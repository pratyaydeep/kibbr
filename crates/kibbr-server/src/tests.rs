// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use std::io::Write;

/// A server over a store in memory: the job registries and the shutdown steps are the
/// same, and nothing is opened.
fn server() -> Arc<Server> {
    let server = Server::new(
        Utc::now(),
        Limits::default(),
        ApiLimits::default(),
        Storage::default(),
        Schema::default(),
        None,
    );
    Arc::new(server.expect("a server in memory"))
}

/// The wait keeps a compaction's last writes inside the directory's lock. A thread told
/// to stop is usually gone already, so only one that takes its time shows the wait.
#[test]
fn shutdown_waits_for_a_compaction_thread_still_running() {
    for by_request in [true, false] {
        let server = server();
        let ended = Arc::new(AtomicBool::new(false));
        let thread = {
            let ended = Arc::clone(&ended);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(200));
                ended.store(true, Ordering::SeqCst);
            })
        };
        {
            let mut jobs = server.compactions.lock().expect("the lock");
            match by_request {
                true => jobs.asked = Some(thread),
                false => jobs.scheduled = Some(thread),
            }
        }

        join_compactions(&server);
        assert!(
            ended.load(Ordering::SeqCst),
            "shutdown went on past a compaction thread still running (asked: {by_request})"
        );
        assert!(server.compact_cancel.load(Ordering::Relaxed));
        let mut jobs = server.compactions.lock().expect("the lock");
        assert!(jobs.is_stopping() && jobs.start().is_none());
        assert!(jobs.asked.is_none() && jobs.scheduled.is_none());
    }
}

/// The scheduler's look is left to the next tick while a load job runs - no thread is
/// started for it - and taken once the load has ended, done or failed.
#[test]
fn a_look_for_due_days_waits_while_a_load_job_runs() {
    let server = server();
    let job = Arc::new(LoadJob {
        control: load::Control::default(),
        state: std::sync::Mutex::new(LoadState::Running),
    });
    server
        .loads
        .lock()
        .expect("the lock")
        .jobs
        .insert(1, Arc::clone(&job));
    let looked = || {
        server
            .compactions
            .lock()
            .expect("the lock")
            .scheduled
            .take()
    };

    assert!(look_waits(&server));
    look_for_due_days(&server);
    assert!(looked().is_none(), "a look was taken beside a running load");

    *job.state.lock().expect("the lock") = LoadState::Failed("why".to_string());
    assert!(!look_waits(&server));
    look_for_due_days(&server);
    let thread = looked().expect("no look was taken once the load had ended");
    thread.join().expect("the look's thread");
}

/// And while the last flush tick could not write: the data directory is made read-only
/// under a row, so the tick that would write it fails.
#[cfg(unix)]
#[test]
fn a_look_for_due_days_waits_while_the_last_flush_could_not_write() {
    use std::os::unix::fs::PermissionsExt;

    let dir = std::env::temp_dir().join(format!("kibbr-serve-look-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let storage = Storage {
        base_path: Some(dir.clone()),
        trie_file: Some(dir.join("trie.json")),
        row_group_timeout: Duration::ZERO,
        ..Storage::default()
    };
    std::fs::create_dir_all(&dir).expect("mkdir");
    let server = Server::new(
        Utc::now(),
        Limits::default(),
        ApiLimits::default(),
        storage,
        Schema::default(),
        None,
    );
    let server = Arc::new(server.expect("a server on disk"));
    let data = dir.join(crate::storage::store::disk::DATA_DIR);
    let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
    mode(0o555).expect("chmod -w");
    if std::fs::create_dir(data.join("probe")).is_ok() {
        // Root, or a filesystem that ignores the mode: nothing here can fail a flush.
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }
    let looked = || {
        server
            .compactions
            .lock()
            .expect("the lock")
            .scheduled
            .take()
    };

    let line = &b"{\"ts\":\"2024-01-15T00:00:00Z\",\"level\":\"info\"}\n"[..];
    let now = Utc::now();
    db::read_lines(
        line,
        &mut |_, parsed| db::ingest_line(&server.trie, Some(&server.stores), &parsed, now),
        &mut |_| {},
    )
    .expect("the line is read");
    let flushed = server.stores.flush_all(FlushKind::Tick);
    let waited = look_waits(&server);
    look_for_due_days(&server);
    let taken = looked();
    let _ = mode(0o755);
    assert_eq!(
        flushed.failed, 1,
        "the flush wrote to a read-only directory"
    );
    assert!(waited, "a store that could not write did not hold the look");
    assert!(
        taken.is_none(),
        "a look was taken on a disk that is refusing"
    );

    let again = server.stores.flush_all(FlushKind::Tick);
    assert_eq!(again.failed, 0, "the flush still fails on a writable disk");
    assert!(!look_waits(&server));
    look_for_due_days(&server);
    let thread = looked().expect("no look was taken once a flush had written");
    thread.join().expect("the look's thread");
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A store whose leaves will not open while `shut` is set: what the disk store never
/// does today, and what the log's applier has to survive if a store ever does.
struct Refusing(Arc<AtomicBool>);

impl crate::storage::store::Store for Refusing {
    fn open(
        &self,
        template_id: u64,
        time_column: &str,
    ) -> Result<Arc<dyn crate::storage::store::LeafStore>, StoreError> {
        if self.0.load(Ordering::Relaxed) {
            return Err(StoreError::Io {
                template_id,
                source: std::io::Error::other("the leaf will not open"),
            });
        }
        crate::storage::store::MemoryStore::new().open(template_id, time_column)
    }

    fn kind(&self) -> &'static str {
        "refusing"
    }

    fn base_path(&self) -> Option<&Path> {
        None
    }
}

/// A row the store refuses after the log has its line is held, and so is every later row
/// of its template: a row group cut around it would say its line was stored.
#[test]
fn a_row_the_store_refuses_is_held_and_its_template_waits_with_it() {
    let shut = Arc::new(AtomicBool::new(true));
    let stores = StoreManager::new(Box::new(Refusing(Arc::clone(&shut))));
    let held = Held::new();
    let placed = |template_id| db::Placed {
        template_id,
        time_column: "ts".to_string(),
        row: crate::storage::store::Row::new(),
    };
    assert_eq!(held.from(), None);

    assert!(!held.apply(&stores, 40, placed(7)));
    assert_eq!((held.from(), held.rows()), (Some(40), 1));

    shut.store(false, Ordering::Relaxed);
    assert!(
        !held.apply(&stores, 41, placed(7)),
        "the template was let back in"
    );
    assert_eq!(stores.rows(), 0);
    assert!(held.apply(&stores, 42, placed(8)));
    assert_eq!(stores.rows(), 1);
    assert_eq!((held.from(), held.rows()), (Some(40), 2));
}

// ── A body of which nothing was taken ─────────────────────────────────────────────
//
// These call the handler itself, with the door's permits made by hand, so the status,
// the headers and the body are the ones a client gets.

/// What `/ingest` answers `body`: status, headers, and the JSON.
fn answer(server: &Arc<Server>, body: &[u8]) -> (StatusCode, HeaderMap, Value) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let permit = server.admit_now().expect("both permits");
        let response = post_ingest(
            State(Arc::clone(server)),
            axum::Extension(permit),
            axum::http::Uri::from_static("/ingest"),
            HeaderMap::new(),
            Ok(Bytes::copy_from_slice(body)),
        )
        .await;
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("the response body");
        let report = serde_json::from_slice(&bytes).expect("the answer is JSON");
        (parts.status, parts.headers, report)
    })
}

/// A server in memory whose catalog refuses every append, as a full disk under
/// `catalog.log` does; `known` is taken first, so its template exists.
fn server_whose_catalog_cannot_write(tag: &str, known: &[u8]) -> Arc<Server> {
    let server = server();
    if !known.is_empty() {
        let (status, _, report) = answer(&server, known);
        assert_eq!(status, StatusCode::OK, "{report}");
    }
    let dir = std::env::temp_dir().join(format!(
        "kibbr-serve-catalog-{tag}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("mkdir");
    server
        .trie
        .attach_catalog(Catalog::read_only(&dir.join("dead.log")).expect("read-only"));
    // The catalog keeps its handle, and nothing reads the file by name again.
    let _ = std::fs::remove_dir_all(&dir);
    server
}

/// No line taken, and every refusal the catalog's write: a 503, a `Retry-After`, and
/// the errors still by line.
#[test]
fn a_body_the_catalog_could_write_none_of_is_a_503_with_its_errors_by_line() {
    let server = server_whose_catalog_cannot_write("none", b"");
    let body = b"{\"ts\":1705314600,\"a\":1}\n{\"ts\":1705314601,\"a\":2}\n\
                 {\"ts\":1705314602,\"b\":3}\n";
    let (status, headers, report) = answer(&server, body);

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "three well-formed lines, none taken: {report}"
    );
    assert_eq!(
        headers.get(header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
        Some(RETRY_AFTER_SECONDS.to_string().as_str()),
        "{headers:?}"
    );
    // The handler has the whole body, and does not itself ask for the connection to be
    // closed: that is `say_close`'s to say, and only of a body left unread.
    assert!(headers.get(header::CONNECTION).is_none(), "{headers:?}");
    assert_eq!(report["ok"], false);
    assert_eq!(report["ingested"], 0);
    assert_eq!(report["retry_after"], RETRY_AFTER_SECONDS, "{report}");
    let errors = report["errors"].as_array().expect("errors");
    assert_eq!(errors.len(), 3, "{report}");
    for (at, error) in errors.iter().enumerate() {
        assert_eq!(error["line"], at + 1, "{report}");
        let msg = error["msg"].as_str().unwrap_or_default();
        assert!(msg.starts_with("not ingested: catalog: "), "{msg}");
        assert!(msg.contains("dead.log"), "{msg}");
    }
    // The shape every other 503 here has, for a client that reads `error` and no
    // further: what to do with the body, and the first line's own reason.
    let said = report["error"].as_str().unwrap_or_default();
    assert!(said.contains("send it again"), "{report}");
    assert!(
        said.ends_with(errors[0]["msg"].as_str().unwrap_or("no message")),
        "{report}"
    );
    assert_eq!(server.trie.template_count(), 0, "a refused template exists");
}

/// Any line taken, or any line the sender got wrong, keeps the 200: a 503 would have a
/// taken line stored again, or a malformed one sent again for ever.
#[test]
fn a_body_with_a_line_taken_or_a_malformed_line_keeps_its_200() {
    let known = b"{\"ts\":1705314600,\"known\":1}\n";

    // A line of a template that exists is taken; the line beside it is not.
    let server = server_whose_catalog_cannot_write("taken", known);
    let (status, headers, report) = answer(
        &server,
        b"{\"ts\":1705314601,\"known\":2}\n{\"ts\":1705314602,\"new\":3}\n",
    );
    assert_eq!(status, StatusCode::OK, "{report}");
    assert!(headers.get(header::RETRY_AFTER).is_none(), "{headers:?}");
    assert_eq!((&report["ok"], &report["ingested"]), (&json!(false), &json!(1)));
    assert_eq!(report["errors"][0]["line"], 2);
    assert!(report.get("error").is_none() && report.get("retry_after").is_none());

    // Nothing taken, and one of the refusals is the line's own fault.
    let server = server_whose_catalog_cannot_write("mixed", b"");
    let (status, headers, report) =
        answer(&server, b"{\"ts\":1705314601,\"new\":2}\nnot json\n");
    assert_eq!(status, StatusCode::OK, "{report}");
    assert!(headers.get(header::RETRY_AFTER).is_none(), "{headers:?}");
    assert_eq!((&report["ok"], &report["ingested"]), (&json!(false), &json!(0)));
    assert_eq!(report["errors"].as_array().map(Vec::len), Some(2));
    assert!(report.get("error").is_none() && report.get("retry_after").is_none());

    // Nothing taken and nothing written for: every line is malformed.
    let (status, _, report) = answer(&server, b"not json\n[1]\n");
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!((&report["ok"], &report["ingested"]), (&json!(false), &json!(0)));

    // And nothing sent: no line, or blank ones. Nothing was refused.
    for empty in [&b""[..], &b"\n\n  \n"[..]] {
        let (status, _, report) = answer(&server, empty);
        assert_eq!(status, StatusCode::OK, "{report}");
        assert_eq!(report, json!({"ok": true, "ingested": 0, "errors": []}));
    }
}

/// The rule counts every refusal and not only those the answer describes: a malformed
/// line past the cap still makes the body a mix.
#[test]
fn the_rule_counts_refusals_past_the_ones_the_answer_describes() {
    let server = server_whose_catalog_cannot_write("cap", b"");
    let mut body = Vec::new();
    for n in 0..=MAX_REPORTED_ERRORS {
        body.extend_from_slice(format!("{{\"ts\":1705314600,\"new\":{n}}}\n").as_bytes());
    }
    let (status, headers, report) = answer(&server, &body);
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(headers.get(header::RETRY_AFTER).is_some());
    assert_eq!(report["ingested"], 0);
    assert_eq!(report["errors"].as_array().map(Vec::len), Some(MAX_REPORTED_ERRORS));
    assert_eq!(report["errors_omitted"], 1);

    body.extend_from_slice(b"not json\n");
    let (status, _, report) = answer(&server, &body);
    assert_eq!(status, StatusCode::OK, "a malformed line past the cap was not counted");
    assert_eq!(report["errors_omitted"], 2);
}

/// A scan is shared out from a thread that may block, never from a runtime worker, where
/// it would stall every request; the control shows the test can fail.
#[test]
fn a_scan_is_shared_out_from_a_blocking_thread_and_never_from_a_worker() {
    let dir = std::env::temp_dir().join(format!("kibbr-serve-fan-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");
    let storage = Storage {
        base_path: Some(dir.clone()),
        trie_file: Some(dir.join("trie.json")),
        row_group_timeout: Duration::ZERO,
        scan_threads: 4,
        scan_fan_rows: 0,
        ..Storage::default()
    };
    let scan: OnceLock<Arc<Fan>> = OnceLock::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");
    let server = Server::new(
        Utc::now(),
        Limits::default(),
        ApiLimits::default(),
        storage,
        Schema::default(),
        None,
    );
    let server = Arc::new(server.expect("a server on disk"));
    let fan = Arc::clone(server.fan.as_ref().expect("the server has scan threads"));
    let _ = scan.set(Arc::clone(&fan));

    // Two row groups in one day: each tick closes what is buffered.
    let now = Utc::now();
    for half in 0..2 {
        let lines: String = (0..50)
            .map(|n| {
                format!(
                    "{{\"ts\":\"2024-01-15T00:0{half}:{n:02}Z\",\"level\":\"info\",\"n\":{n}}}\n"
                )
            })
            .collect();
        db::read_lines(
            lines.as_bytes(),
            &mut |_, parsed| db::ingest_line(&server.trie, Some(&server.stores), &parsed, now),
            &mut |_| {},
        )
        .expect("the lines are read");
        let flushed = server.stores.flush_all(FlushKind::Tick);
        assert_eq!((flushed.failed, flushed.row_groups), (0, 1));
    }

    let text = r#"COUNT LOG {"level":"info"}"#;
    let me = std::thread::current().id();
    let worker = runtime
        .block_on(async { tokio::spawn(async { std::thread::current().id() }).await })
        .expect("the task ran");
    assert_ne!(worker, me);

    // As a request: the handler, on the worker.
    let asked = Bytes::from(json!({ "q": text }).to_string());
    let answer: Value = runtime.block_on(async {
        let response = tokio::spawn(post_query(State(Arc::clone(&server)), asked))
            .await
            .expect("the handler ran");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the response body");
        serde_json::from_slice(&bytes).expect("the answer is JSON")
    });
    assert_eq!(answer["count"], 100, "{answer}");
    assert_eq!(answer["meta"]["row_groups_read"], 2, "{answer}");
    let entered = fan.entered();
    assert_eq!(entered.len(), 1, "{entered:?}");
    assert!(
        entered[0] != worker && entered[0] != me,
        "the scan was shared out from a runtime thread"
    );

    // The control: the engine called from a task, on the worker.
    let query = query::from_trieql(text, Utc::now(), server.limits).expect("a query");
    let held = Arc::clone(&server);
    let count = runtime
        .block_on(async {
            tokio::spawn(async move {
                engine::engine::execute(&held.trie, &held.stores, &query).map(|result| result.count)
            })
            .await
        })
        .expect("the task ran")
        .expect("the query answers");
    assert_eq!(count, 100);
    assert_eq!(fan.entered(), [entered[0], worker]);
    assert_eq!(fan.fanned(), 2);

    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A server says what its queries may use, in `/stats` and its banner: the default is
/// half the cores, and the threads said are the ones a scan can use, not those resolved.
#[test]
fn a_server_says_the_scan_threads_it_resolved_in_stats_and_in_its_banner() {
    let dir = std::env::temp_dir().join(format!("kibbr-serve-scan-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let on_disk = |name: &str, scan_threads: usize, scan_fan_rows: u64| {
        let base = dir.join(name);
        std::fs::create_dir_all(&base).expect("mkdir");
        Storage {
            trie_file: Some(base.join("trie.json")),
            base_path: Some(base),
            scan_threads,
            scan_fan_rows,
            ..Storage::default()
        }
    };
    // In memory, with the four threads `with_scan` resolves on a machine of eight
    // cores still in its settings.
    let in_memory = Storage {
        scan_threads: 4,
        ..Storage::default()
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    // Each server's settings, and the threads and the rows it must then say.
    let cases = [
        (in_memory, 1, crate::storage::store::DEFAULT_FAN_ROWS),
        (on_disk("none", 0, 7), 1, 7),
        (on_disk("one", 1, 7), 1, 7),
        (
            on_disk("two", 2, crate::storage::store::DEFAULT_FAN_ROWS),
            2,
            32_768,
        ),
        (on_disk("four", 4, 0), 4, 0),
    ];
    for (storage, threads, fan_rows) in cases {
        let server = Server::new(
            Utc::now(),
            Limits::default(),
            ApiLimits::default(),
            storage,
            Schema::default(),
            None,
        );
        let server = Arc::new(server.expect("a server"));
        assert_eq!(server.fan.is_some(), threads > 1);
        let stats: Value = runtime.block_on(async {
            let response = get_stats(State(Arc::clone(&server))).await;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("the response body");
            serde_json::from_slice(&bytes).expect("/stats is JSON")
        });
        let queries = &stats["queries"];
        assert_eq!(queries["scan_threads"], threads, "{queries}");
        assert_eq!(queries["scan_fan_rows"], fan_rows, "{queries}");
        assert_eq!(queries["scan_fan_outs"], 0, "{queries}");

        let line = scan_line(&server);
        if threads == 1 {
            assert_eq!(
                line,
                "  scan        1 thread to a query - no scan is shared out"
            );
            continue;
        }
        let first = format!("  scan        {threads} threads to a query, its own among them;");
        assert!(line.starts_with(&first), "{line}");
        let rows = format!("under {} rows is never shared out", commas(fan_rows));
        assert!(line.contains(&rows), "{line}");
        // And when else none is: for as long as a body is at the door, not only
        // while it is parsed.
        assert!(
            line.ends_with("any while an /ingest body is between the door and its answer"),
            "{line}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Backpressure ──────────────────────────────────────────────────────────────────

/// A data directory of one test's own, removed when the test lets it go.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir()
            .join(format!("kibbr-serve-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A server on `scratch`, with these limits on a request and these settings of the
/// store over the defaults.
fn server_on(scratch: &Scratch, api: ApiLimits, storage: Storage) -> Arc<Server> {
    let storage = Storage {
        base_path: Some(scratch.0.clone()),
        trie_file: Some(scratch.0.join("trie.json")),
        ..storage
    };
    let server = Server::new(
        Utc::now(),
        Limits::default(),
        api,
        storage,
        Schema::default(),
        None,
    );
    Arc::new(server.expect("a server on disk"))
}

/// `/stats`, as a client reads it.
fn stats_of(server: &Arc<Server>) -> Value {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let response = get_stats(State(Arc::clone(server))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the response body");
        serde_json::from_slice(&bytes).expect("/stats is JSON")
    })
}

/// `/stats` says what is queued, what waited and what was shed, by cause; `requests_shed`
/// is the sum of the causes, so a reader of the one number loses nothing.
#[test]
fn stats_say_what_is_queued_and_count_every_shed_under_its_cause() {
    let scratch = Scratch::new("stats-shed");
    let server = server_on(&scratch, ApiLimits::default(), Storage::default());
    let stats = stats_of(&server);
    let ingest = &stats["ingest"];
    assert_eq!(ingest["requests_shed"], 0, "{ingest}");
    assert_eq!(ingest["ingest_wait_ms"], 100, "{ingest}");
    assert_eq!(ingest["parsing"], 0, "{ingest}");
    assert_eq!(ingest["bodies_inside"], 0, "{ingest}");
    assert_eq!(
        ingest["door"],
        json!({"waiting": 0, "waits": 0, "waited_ms": 0, "line_max": null}),
        "{ingest}"
    );
    assert_eq!(
        ingest["shed"],
        json!({"permit": 0, "place": 0, "line": 0, "buffers": 0, "log": 0, "stopping": 0}),
        "{ingest}"
    );
    let wal = &stats["storage"]["wal"];
    assert_eq!(wal["queue_max_bytes"], 256 * 1024 * 1024, "{wal}");
    assert_eq!(wal["queue_bytes"], 0, "{wal}");
    assert_eq!(wal["unapplied_bytes"], 0, "{wal}");
    assert_eq!(wal["queue_peak_bytes"], 0, "{wal}");
    assert_eq!(wal["queue_sheds"], 0, "{wal}");
    assert_eq!(wal["queue_waiting"], 0, "{wal}");
    assert_eq!(wal["queue_waits"], 0, "{wal}");
    assert_eq!(wal["queue_left"], 0, "{wal}");

    // A permit and a place in use are said, each under its own name.
    let parse = Arc::clone(&server.ingest_permits)
        .try_acquire_owned()
        .expect("a parse permit");
    let places = Arc::clone(&server.ingest_bodies)
        .try_acquire_many_owned(3)
        .expect("three places");
    let ingest = stats_of(&server)["ingest"].clone();
    assert_eq!((&ingest["parsing"], &ingest["bodies_inside"]), (&json!(1), &json!(3)));
    drop((parse, places));

    // Each cause counted apart, and the total their sum.
    let shed = &server.door.shed;
    let causes = [
        (&shed.permit, "permit", 1u64),
        (&shed.place, "place", 20),
        (&shed.line, "line", 300),
        (&shed.buffers, "buffers", 4_000),
        (&shed.log, "log", 50_000),
        (&shed.stopping, "stopping", 600_000),
    ];
    for (count, _, by) in &causes {
        count.fetch_add(*by, Ordering::Relaxed);
    }
    let ingest = stats_of(&server)["ingest"].clone();
    for (_, cause, by) in &causes {
        assert_eq!(ingest["shed"][cause], *by, "{ingest}");
    }
    assert_eq!(ingest["requests_shed"], 654_321, "{ingest}");
    assert_eq!(ingest["shed"].as_object().map(|causes| causes.len()), Some(6));

    // A server in memory has a door and no log.
    let in_memory = stats_of(&self::server());
    assert_eq!(in_memory["storage"]["wal"], Value::Null);
    assert_eq!(in_memory["ingest"]["requests_shed"], 0);
    assert_eq!(in_memory["ingest"]["shed"]["log"], 0);
}

// ── The door and the log, with a slow hop held ────────────────────────────────────
//
// A server on a data directory and a port of the test's own, with plain sockets for
// clients. Nothing sleeps to let a thing happen: a state is waited for, from the gauges.

/// How long a body that a test expects to be let go may wait, and how long the test
/// gives it: far apart, so that being woken is the only way out in time.
const LONG_WAIT: Duration = Duration::from_secs(60);
const WOKEN_WITHIN: Duration = Duration::from_secs(10);

/// The lines in each body these tests send.
const PER_BODY: usize = 20;

/// Body `nth`: lines of one template and one day, each as long as every other, so
/// that every body counts the same against the log's bound.
fn lines(nth: usize) -> Vec<u8> {
    lines_at(1_705_314_600, nth)
}

/// The same, of the day `ts` is in.
fn lines_at(ts: u64, nth: usize) -> Vec<u8> {
    (0..PER_BODY)
        .map(|n| format!("{{\"ts\":{ts},\"n\":{}}}\n", 1_000_000 + nth * PER_BODY + n))
        .collect::<String>()
        .into_bytes()
}

/// What one such body counts for against the log's bound.
fn one_body() -> usize {
    wal::counted(&lines(0))
}

/// Wait for a state, looking every few milliseconds. The limit fails the test: it is
/// far past what any machine needs, and nothing passes by reaching it.
fn until(what: &str, mut reached: impl FnMut() -> bool) {
    let began = Instant::now();
    while !reached() {
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// A server answering on a port of its own.
struct Listening {
    server: Arc<Server>,
    addr: SocketAddr,
    runtime: Option<tokio::runtime::Runtime>,
    _scratch: Scratch,
}

impl Listening {
    /// `permits` bodies parsed at once, `wait` at the door and for room in the log. No flush
    /// task and no signal: a test flushes by hand and stops by `begin_stop`.
    fn start(tag: &str, permits: usize, wait: Duration, storage: Storage) -> Self {
        let api = ApiLimits {
            ingest_concurrency: permits,
            ingest_wait: wait,
            ..ApiLimits::default()
        };
        Self::start_with(tag, api, storage)
    }

    /// The same, with every limit on a request said by the test.
    fn start_with(tag: &str, api: ApiLimits, storage: Storage) -> Self {
        let scratch = Scratch::new(tag);
        let server = server_on(&scratch, api, storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let app = router(Arc::clone(&server));
        let addr = runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("the bound address");
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            addr
        });
        Self {
            server,
            addr,
            runtime: Some(runtime),
            _scratch: scratch,
        }
    }

    fn wal(&self) -> wal::Stats {
        self.server.wal.as_ref().expect("a log").stats()
    }

    /// Permits for the parse in use.
    fn parsing(&self) -> usize {
        self.server.api.ingest_concurrency - self.server.ingest_permits.available_permits()
    }

    /// Places between the door and an answer in use.
    fn inside(&self) -> usize {
        ingest_bodies(&self.server.api) - self.server.ingest_bodies.available_permits()
    }

    /// Bodies waiting at the door now.
    fn at_the_door(&self) -> u64 {
        self.server.door.waiting.load(Ordering::Relaxed)
    }

    fn shed(&self) -> u64 {
        self.server.door.shed.total()
    }

    /// `COUNT LOG {}`: every row a query can see.
    fn count(&self) -> u64 {
        let server = &self.server;
        let query = query::from_trieql("COUNT LOG {}", Utc::now(), server.limits)
            .expect("a query");
        let result = engine::engine::execute(&server.trie, &server.stores, &query);
        result.expect("the query answers").count
    }

    /// Hold the log's applier still, as a replace-a-day does: bodies are written to the log
    /// and none is stored or answered until the guard is let go.
    fn still(&self) -> std::sync::RwLockWriteGuard<'_, ()> {
        let disk = self.server.stores.disk_store().expect("a disk store");
        disk.still()
    }

    /// A body sent on a connection and a thread of its own: its answer when joined.
    fn post(&self, body: Vec<u8>) -> std::thread::JoinHandle<Answer> {
        let addr = self.addr;
        std::thread::spawn(move || Client::connect(addr).post(&body))
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        // Bounded: a test that failed with a body parked must fail, not hang here.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

/// One answer: its status, its headers in lower case, and its JSON.
#[derive(Debug)]
struct Answer {
    status: u16,
    headers: String,
    body: Value,
}

impl Answer {
    /// Whether the answer says the connection closes after it.
    fn closes(&self) -> bool {
        self.headers
            .lines()
            .filter_map(|line| line.strip_prefix("connection:"))
            .any(|value| value.split(',').any(|option| option.trim() == "close"))
    }

    fn retry_after(&self) -> bool {
        self.headers.lines().any(|line| line.starts_with("retry-after:"))
    }

    fn error(&self) -> &str {
        self.body["error"].as_str().unwrap_or_default()
    }
}

/// What `log_busy` says, and what `busy` says: how a test tells the two refusals.
const SAYS_LOG_BUSY: &str = "the write-ahead log has more waiting";
const SAYS_BUSY: &str = "ingest is at capacity";

/// One connection, kept between requests.
struct Client(std::net::TcpStream);

impl Client {
    fn connect(addr: SocketAddr) -> Self {
        let socket = std::net::TcpStream::connect(addr).expect("connect");
        // Past any wait a test stages: a server that never answers fails the test.
        socket
            .set_read_timeout(Some(Duration::from_secs(120)))
            .expect("a read timeout");
        Self(socket)
    }

    /// A request's head, saying a body of `length` bytes follows.
    fn head(&mut self, method: &str, path: &str, length: usize) {
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: kibbr\r\nContent-Length: {length}\r\n\r\n"
        );
        self.0.write_all(head.as_bytes()).expect("the head");
    }

    /// Bytes of a body. A body the door refuses is never read, so its write may
    /// break; the answer is what a test reads.
    fn write(&mut self, bytes: &[u8]) {
        let _ = self.0.write_all(bytes);
        let _ = self.0.flush();
    }

    /// The next answer on the connection, read in as few reads as it comes in: the door
    /// answers with the body unread and then closes, and a reset socket may not give a second.
    fn answer(&mut self) -> Answer {
        let mut raw = Vec::new();
        let mut chunk = [0u8; 4096];
        let head = loop {
            if let Some(at) = raw.windows(4).position(|four| four == b"\r\n\r\n") {
                break at + 4;
            }
            let read = self.0.read(&mut chunk).expect("the answer's head");
            assert!(read > 0, "the connection ended before an answer's head did");
            raw.extend_from_slice(&chunk[..read]);
        };
        let headers = String::from_utf8_lossy(&raw[..head]).to_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or_else(|| panic!("no Content-Length in the answer:\n{headers}"));
        while raw.len() < head + length {
            let read = self.0.read(&mut chunk).expect("the answer's body");
            assert!(read > 0, "the connection ended before an answer's body did");
            raw.extend_from_slice(&chunk[..read]);
        }
        // One request at a time on a connection, so nothing follows the answer.
        assert_eq!(raw.len(), head + length, "bytes after the answer");
        let body = raw[head..].to_vec();
        let status = headers
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("no status in the answer:\n{headers}"));
        let body = serde_json::from_slice(&body).expect("the answer is JSON");
        Answer {
            status,
            headers,
            body,
        }
    }

    fn post(&mut self, body: &[u8]) -> Answer {
        self.head("POST", "/ingest", body.len());
        self.write(body);
        self.answer()
    }

    fn get(&mut self, path: &str) -> Answer {
        self.head("GET", path, 0);
        self.answer()
    }
}

/// A wait for room that runs out is a `503` on a connection that is kept: the body was
/// read before the log was found full. The applier is held, so no speed makes it a `200`.
#[test]
fn a_wait_for_room_that_runs_out_is_a_503_on_a_kept_connection() {
    let bound = 2 * one_body();
    let storage = Storage {
        wal_queue_bytes: bound,
        ..Storage::default()
    };
    let at = Listening::start("room-503", 2, Duration::from_millis(50), storage);
    let still = at.still();
    let parked = [at.post(lines(0)), at.post(lines(1))];
    // Both written, and neither stored: nothing is waiting for the log's disk here,
    // and the bound is full all the same.
    until("two bodies to fill the log", || {
        let wal = at.wal();
        (wal.queue_bytes, wal.unapplied_bytes) == (0, bound) && at.parsing() == 0
    });

    let mut client = Client::connect(at.addr);
    let answer = client.post(&lines(2));
    assert_eq!(answer.status, 503, "{answer:?}");
    assert!(answer.retry_after(), "{answer:?}");
    assert!(answer.error().contains(SAYS_LOG_BUSY), "{answer:?}");
    // So the refusal may not put it on the log's disk alone: what is behind here is
    // the store, and the answer names both places a line can be waiting.
    assert!(
        answer.error().contains("to be written or stored")
            && answer.error().contains("its disk or the store is behind"),
        "{answer:?}"
    );
    let wal = at.wal();
    assert_eq!((wal.queue_bytes, wal.unapplied_bytes), (0, bound));
    assert!(!answer.closes(), "the body was read: {answer:?}");
    let health = client.get("/health");
    assert_eq!(health.status, 200, "{health:?}");

    let shed = &at.server.door.shed;
    assert_eq!(shed.log.load(Ordering::Relaxed), 1);
    assert_eq!(at.shed(), 1);
    let wal = at.wal();
    assert_eq!((wal.queue_sheds, wal.queue_waits), (1, 1));
    // Read and refused: in the two counters a body the log sheds has always been in.
    let read = at.server.lines_read.load(Ordering::Relaxed);
    let rejected = at.server.lines_rejected.load(Ordering::Relaxed);
    assert_eq!((read, rejected), (PER_BODY as u64, PER_BODY as u64));

    drop(still);
    for body in parked {
        let answer = body.join().expect("a sender");
        assert_eq!(answer.status, 200, "{answer:?}");
    }
    assert_eq!(at.count(), 2 * PER_BODY as u64);
    assert_eq!(at.wal().queue_peak_bytes, bound);
}

/// A slow applier makes senders wait, and none is shed: two bodies in the log, two parsed
/// and waiting for room, four at the door, is the one state the eight can settle in.
#[test]
fn senders_wait_for_a_slow_applier_and_none_is_shed() {
    const SENDERS: usize = 8;
    const BODIES: usize = 3;
    let bound = 2 * one_body();
    let storage = Storage {
        wal_queue_bytes: bound,
        ..Storage::default()
    };
    let at = Listening::start("door-wait", 2, LONG_WAIT, storage);
    let still = at.still();
    let senders: Vec<_> = (0..SENDERS)
        .map(|sender| {
            let addr = at.addr;
            std::thread::spawn(move || {
                let mut client = Client::connect(addr);
                (0..BODIES)
                    .map(|nth| client.post(&lines(sender * BODIES + nth)).status)
                    .collect::<Vec<u16>>()
            })
        })
        .collect();

    until("two in the log, two waiting for room, four at the door", || {
        let wal = at.wal();
        wal.queue_waiting == 2
            && at.at_the_door() == 4
            && wal.last_seq == 2 * PER_BODY as u64
    });
    let wal = at.wal();
    assert_eq!(at.shed(), 0);
    assert_eq!(at.server.lines_ingested.load(Ordering::Relaxed), 0);
    assert_eq!(wal.applied, 0);
    assert!(wal.queue_bytes + wal.unapplied_bytes <= bound, "{wal:?}");
    assert_eq!((at.parsing(), at.inside()), (2, 4));

    drop(still);
    for sender in senders {
        assert_eq!(sender.join().expect("a sender"), [200; BODIES]);
    }
    assert_eq!(at.shed(), 0);
    assert_eq!(at.count(), (SENDERS * BODIES * PER_BODY) as u64);
    let wal = at.wal();
    assert!(wal.queue_peak_bytes <= bound, "{wal:?}");
    assert_eq!((wal.queue_sheds, wal.queue_left), (0, 0));
    assert_eq!((at.parsing(), at.inside(), at.at_the_door()), (0, 0, 0));
}

/// Every shed answer is one of two shapes, whichever way the timing fell: the log's
/// refusal on a kept connection, or the door's on a closed one. The causes are not asserted.
#[test]
fn every_shed_answer_is_one_of_two_shapes() {
    const SENDERS: usize = 8;
    let bound = 2 * one_body();
    let storage = Storage {
        wal_queue_bytes: bound,
        ..Storage::default()
    };
    let at = Listening::start("door-shapes", 2, Duration::from_millis(50), storage);
    let still = at.still();
    let (said, answers) = std::sync::mpsc::channel::<Answer>();
    for sender in 0..SENDERS {
        let (addr, said) = (at.addr, said.clone());
        std::thread::spawn(move || {
            let _ = said.send(Client::connect(addr).post(&lines(sender)));
        });
    }
    drop(said);

    until("six of the eight to be shed", || at.shed() == 6);
    for _ in 0..6 {
        let answer = answers
            .recv_timeout(Duration::from_secs(30))
            .expect("a shed body was not answered");
        assert_eq!(answer.status, 503, "{answer:?}");
        assert!(answer.retry_after(), "{answer:?}");
        if answer.closes() {
            let said = answer.error();
            assert!(
                said.contains(SAYS_BUSY) || said.contains(SAYS_LOG_BUSY),
                "{answer:?}"
            );
        } else {
            assert!(answer.error().contains(SAYS_LOG_BUSY), "{answer:?}");
        }
    }
    assert_eq!(at.shed(), 6);
    let causes: u64 = at.server.door.shed.each().iter().map(|(_, n)| n).sum();
    assert_eq!(causes, 6);
    assert_eq!(at.server.lines_ingested.load(Ordering::Relaxed), 0);

    drop(still);
    for _ in 0..2 {
        let answer = answers
            .recv_timeout(Duration::from_secs(30))
            .expect("a body in the log was not answered");
        assert_eq!(answer.status, 200, "{answer:?}");
    }
    assert_eq!(at.count(), 2 * PER_BODY as u64);
}

/// One body in the log, held at the applier, and a second holding the one permit while
/// it waits for room: what the door looks like to whoever comes third.
fn one_in_the_log_one_waiting_for_room(
    at: &Listening,
) -> [std::thread::JoinHandle<Answer>; 2] {
    let first = at.post(lines(0));
    until("the first body to be in the log", || {
        at.wal().unapplied_bytes == one_body() && at.parsing() == 0
    });
    let second = at.post(lines(1));
    until("the second body to wait for room", || {
        at.wal().queue_waiting == 1
    });
    [first, second]
}

/// A server with one permit whose log holds one body.
fn one_at_a_time(tag: &str, wait: Duration) -> Listening {
    let storage = Storage {
        wal_queue_bytes: one_body(),
        ..Storage::default()
    };
    Listening::start(tag, 1, wait, storage)
}

/// A request's timeout ends a body that does not arrive, and not an `/ingest` body that
/// was read whole: a `504` for that one would be for lines the log had taken.
#[test]
fn a_body_that_does_not_arrive_is_timed_out_and_one_read_whole_is_not() {
    const TIMEOUT: Duration = Duration::from_millis(200);
    let api = ApiLimits {
        ingest_concurrency: 1,
        ingest_wait: LONG_WAIT,
        request_timeout: Some(TIMEOUT),
        ..ApiLimits::default()
    };
    let at = Listening::start_with("timeout-body", api, Storage::default());
    let still = at.still();
    let whole = at.post(lines(0));
    until("the first body to be in the log", || {
        at.wal().unapplied_bytes == one_body() && at.parsing() == 0
    });

    let body = lines(1);
    let mut half = Client::connect(at.addr);
    let began = Instant::now();
    half.head("POST", "/ingest", body.len());
    half.write(&body[..body.len() / 2]);
    let answer = half.answer();
    assert_eq!(answer.status, 408, "{answer:?}");
    assert!(began.elapsed() >= TIMEOUT, "{:?}", began.elapsed());
    assert!(answer.closes(), "{answer:?}");
    assert!(answer.error().contains("had not arrived 200ms"), "{answer:?}");

    // The first body's time ran out before the second's, and it is where it was.
    until("the permit of the body that never arrived to come back", || {
        at.parsing() == 0 && at.inside() == 1
    });
    assert!(!whole.is_finished());
    let ingest = stats_of(&at.server)["ingest"].clone();
    assert_eq!(ingest["request_timeout_ms"], 200, "{ingest}");
    assert_eq!(ingest["requests_timed_out"], 1, "{ingest}");
    assert_eq!(at.wal().last_seq, PER_BODY as u64);

    drop(still);
    let answer = whole.join().expect("a sender");
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(at.count(), PER_BODY as u64);
    assert_eq!(at.server.timed_out.load(Ordering::Relaxed), 1);
}

/// A query whose handler is dropped before its task has returned is told to stop, and
/// whoever made the stop hears of it once, when a scan first finds it set.
#[test]
fn a_query_nobody_waits_for_is_told_to_stop_and_says_so_once() {
    let heard = Arc::new(AtomicU64::new(0));
    let stop = {
        let heard = Arc::clone(&heard);
        Stop::watched(move || {
            heard.fetch_add(1, Ordering::Relaxed);
        })
    };
    drop(Unwanted {
        stop: &stop,
        armed: false,
    });
    assert!(!stop.is_set());
    assert!(stop.check().is_ok());
    assert_eq!(heard.load(Ordering::Relaxed), 0);

    drop(Unwanted {
        stop: &stop,
        armed: true,
    });
    assert!(stop.is_set());
    assert_eq!(heard.load(Ordering::Relaxed), 0, "set is not found");
    for _ in 0..3 {
        assert!(matches!(stop.clone().check(), Err(StoreError::Stopped)));
    }
    assert_eq!(heard.load(Ordering::Relaxed), 1);

    assert_eq!(shortened("COUNT LOG {}"), "COUNT LOG {}");
    let long = format!("MATCH LOG {{\"msg\":\"{}\"}}\nLIMIT 5", "x".repeat(200));
    let short = shortened(&long);
    assert_eq!(short.chars().count(), 123, "{short}");
    assert!(short.ends_with("xxx..."), "{short}");
    assert_eq!(shortened("A\nB"), "A B");
}

/// A sender that hangs up at the door holds nothing, no permit and no place; when its
/// turn comes, reading its body fails and the permit goes straight back.
#[test]
fn a_sender_that_hangs_up_at_the_door_holds_nothing() {
    let at = one_at_a_time("door-left", LONG_WAIT);
    let still = at.still();
    let inside = one_in_the_log_one_waiting_for_room(&at);
    {
        let body = lines(2);
        let mut third = Client::connect(at.addr);
        third.head("POST", "/ingest", body.len());
        third.write(&body[..body.len() / 2]);
        until("the third body to wait at the door", || at.at_the_door() == 1);
    }

    drop(still);
    for body in inside {
        let answer = body.join().expect("a sender");
        assert_eq!(answer.status, 200, "{answer:?}");
    }
    until("the door to be empty", || {
        at.parsing() == 0
            && at.inside() == 0
            && at.at_the_door() == 0
            && at.server.in_flight.load(Ordering::Relaxed) == 0
    });
    assert_eq!(at.count(), 2 * PER_BODY as u64);
    assert_eq!(at.wal().last_seq, 2 * PER_BODY as u64);
    assert_eq!(at.shed(), 0);
}

/// A stop refuses whoever is waiting and finishes whoever is inside. The third body shows
/// the order of the door's waits: a door that looked at its permit first would let it in.
#[test]
fn shutdown_refuses_whoever_is_waiting_and_finishes_whoever_is_inside() {
    let at = one_at_a_time("door-stop", LONG_WAIT);
    let still = at.still();
    let [first, second] = one_in_the_log_one_waiting_for_room(&at);
    let third = at.post(lines(2));
    until("the third body to wait at the door", || at.at_the_door() == 1);

    let began = Instant::now();
    let runtime = at.runtime.as_ref().expect("the runtime");
    runtime.block_on(async { begin_stop(&at.server) });
    let second = second.join().expect("the second sender");
    let third = third.join().expect("the third sender");
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "a stop waited out a body that was waiting"
    );
    assert_eq!(second.status, 503, "{second:?}");
    assert!(second.error().contains(SAYS_LOG_BUSY), "{second:?}");
    assert!(second.retry_after() && !second.closes(), "{second:?}");
    assert_eq!(third.status, 503, "{third:?}");
    assert!(third.error().contains("the server is stopping"), "{third:?}");
    assert!(third.retry_after() && third.closes(), "{third:?}");
    let shed = &at.server.door.shed;
    assert_eq!(shed.log.load(Ordering::Relaxed), 1);
    assert_eq!(shed.stopping.load(Ordering::Relaxed), 1);
    assert_eq!(at.shed(), 2);
    assert!(!first.is_finished(), "the body in the log was not waited for");

    drop(still);
    let first = first.join().expect("the first sender");
    assert_eq!(first.status, 200, "{first:?}");
    // What the stop waits on before its final flush: every place back.
    let places = ingest_bodies(&at.server.api) as u32;
    let all = runtime.block_on(async {
        tokio::time::timeout(WOKEN_WITHIN, at.server.ingest_bodies.acquire_many(places)).await
    });
    assert!(matches!(all, Ok(Ok(_))), "a place was never given back");
    assert_eq!(at.count(), PER_BODY as u64);
}

/// A body still being parsed when a stop begins is taken if the log has room, and shed at
/// once if not, as the log's shed and not the door's "stopping", which is for a body unread.
#[test]
fn a_body_being_parsed_at_a_stop_is_shed_if_the_log_has_no_room_for_it() {
    let at = one_at_a_time("stop-parsing", LONG_WAIT);
    let still = at.still();
    let first = at.post(lines(0));
    until("the first body to be in the log", || {
        at.wal().unapplied_bytes == one_body() && at.parsing() == 0
    });
    let body = lines(1);
    let mut second = Client::connect(at.addr);
    second.head("POST", "/ingest", body.len());
    second.write(&body[..body.len() / 2]);
    until("the second body to hold the permit", || at.parsing() == 1);

    let runtime = at.runtime.as_ref().expect("the runtime");
    runtime.block_on(async { begin_stop(&at.server) });
    let began = Instant::now();
    second.write(&body[body.len() / 2..]);
    let answer = second.answer();
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "a stop let a body wait for room"
    );
    assert_eq!(answer.status, 503, "{answer:?}");
    assert!(answer.error().contains(SAYS_LOG_BUSY), "{answer:?}");
    assert!(answer.retry_after() && !answer.closes(), "{answer:?}");
    let shed = &at.server.door.shed;
    assert_eq!(shed.log.load(Ordering::Relaxed), 1);
    assert_eq!(shed.stopping.load(Ordering::Relaxed), 0);
    assert_eq!(at.shed(), 1);
    assert_eq!(at.wal().queue_sheds, 1);
    assert!(
        !first.is_finished(),
        "the body in the log was not waited for"
    );

    drop(still);
    let first = first.join().expect("the first sender");
    assert_eq!(first.status, 200, "{first:?}");
    let third = at.post(lines(2)).join().expect("the third sender");
    assert_eq!(third.status, 200, "{third:?}");
    assert_eq!(at.shed(), 1);
    assert_eq!(at.count(), 2 * PER_BODY as u64);
    let rejected = at.server.lines_rejected.load(Ordering::Relaxed);
    assert_eq!(rejected, PER_BODY as u64);
}

/// A log that fails answers every waiter with its reason, in less time than a waiter never
/// woken would take. The fourth body's connection may be closed or kept, and is not asserted.
#[test]
#[cfg(unix)]
fn a_failed_log_answers_every_waiter_with_its_reason() {
    use std::os::unix::fs::PermissionsExt;

    let storage = Storage {
        wal_queue_bytes: one_body(),
        // A segment a frame long: the flush after the first needs a new file.
        wal_segment_bytes: 1,
        ..Storage::default()
    };
    let at = Listening::start("door-failed", 2, LONG_WAIT, storage);
    let still = at.still();
    let first = at.post(lines(0));
    until("the first body to be in the log", || {
        at.wal().unapplied_bytes == one_body() && at.parsing() == 0
    });
    let mut waiting = vec![at.post(lines(1)), at.post(lines(2))];
    until("two bodies to wait for room", || at.wal().queue_waiting == 2);
    waiting.push(at.post(lines(3)));
    until("the fourth body to wait at the door", || at.at_the_door() == 1);

    let dir = at.server.wal.as_ref().expect("a log").dir().to_path_buf();
    let mode = |mode| std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode));
    mode(0o555).expect("chmod -w");
    let began = Instant::now();
    drop(still);
    let first = first.join().expect("the first sender");
    let refused: Vec<Answer> = waiting
        .into_iter()
        .map(|body| body.join().expect("a waiting sender"))
        .collect();
    let took = began.elapsed();
    let degraded = at.server.degraded();
    mode(0o755).expect("chmod +w");

    assert_eq!(first.status, 200, "{first:?}");
    let why = at.wal().last_error.expect("the log says why it cannot write");
    for answer in &refused {
        assert_eq!(answer.status, 503, "{answer:?}");
        assert!(answer.retry_after(), "{answer:?}");
        assert!(answer.error().contains(&why), "{answer:?}\n{why}");
    }
    assert!(
        took < WOKEN_WITHIN,
        "a waiter was not told that the log had failed"
    );
    assert!(degraded.is_some_and(|said| said.contains(&why)));
    assert_eq!(at.count(), PER_BODY as u64);

    // And the door opens again at the first write that succeeds.
    until("the log to write again", || {
        move_checkpoint(&at.server);
        at.server.wal.as_ref().is_some_and(Wal::is_healthy)
    });
    let answer = at.post(lines(4)).join().expect("a sender");
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(at.count(), 2 * PER_BODY as u64);
}

/// A server whose buffers hold next to nothing, so that one body leaves them past
/// twice their limit, and which no task flushes: the test does.
fn small_buffers(tag: &str, permits: usize) -> Listening {
    let storage = Storage {
        max_buffer_bytes: 1024,
        ..Storage::default()
    };
    Listening::start(tag, permits, LONG_WAIT, storage)
}

/// Flush as the tick does, and say so to whoever is waiting at the door for it.
fn tick(at: &Listening) -> crate::storage::store::FlushSummary {
    let summary = at.server.stores.flush_all(FlushKind::Tick);
    at.server.flushed.notify_waiters();
    summary
}

/// A body waits at the door while the buffers are past twice their limit, and the flush
/// lets it in; on a store that cannot write the wait could not end, so it is refused at once.
#[test]
#[cfg(unix)]
fn a_body_waits_at_the_door_while_the_buffers_are_past_twice_their_limit() {
    use std::os::unix::fs::PermissionsExt;

    let at = small_buffers("door-buffers", 2);
    let first = at.post(lines(0)).join().expect("a sender");
    assert_eq!(first.status, 200, "{first:?}");
    assert!(at.server.stores.buffers_full());

    let second = at.post(lines(1));
    until("the second body to wait at the door", || at.at_the_door() == 1);
    assert_eq!(at.parsing(), 0, "a body waiting for the flush holds a permit");
    let began = Instant::now();
    assert_eq!(tick(&at).failed, 0);
    assert!(!at.server.stores.buffers_full());
    let second = second.join().expect("the second sender");
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "the body at the door was not let in by the flush"
    );
    assert_eq!(second.status, 200, "{second:?}");
    let door = &at.server.door;
    assert_eq!(door.shed.buffers.load(Ordering::Relaxed), 0);
    assert_eq!(door.waits.load(Ordering::Relaxed), 1);

    // The store stops writing: a day with no file yet, and a directory that will
    // not take one.
    assert_eq!(tick(&at).failed, 0);
    let data = at.server.storage.base_path.as_ref().expect("on disk");
    let data = data.join(crate::storage::store::disk::DATA_DIR);
    let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
    mode(0o555).expect("chmod -w");
    let third = at.post(lines_at(1_705_401_000, 2)).join().expect("a sender");
    let failed = tick(&at).failed;
    let full = at.server.stores.buffers_full() && at.server.write_health.is_degraded();
    let began = Instant::now();
    let fourth = at.post(lines(3)).join().expect("a sender");
    let took = began.elapsed();
    mode(0o755).expect("chmod +w");

    assert_eq!(third.status, 200, "{third:?}");
    assert!(failed > 0 && full, "the store wrote to a read-only directory");
    assert_eq!(fourth.status, 503, "{fourth:?}");
    assert!(fourth.retry_after() && fourth.closes(), "{fourth:?}");
    assert!(fourth.error().contains("the store cannot write"), "{fourth:?}");
    assert!(took < WOKEN_WITHIN, "a refusal that could not change was waited for");
    assert_eq!(door.shed.buffers.load(Ordering::Relaxed), 1);
    assert_eq!(at.shed(), 1);
}

/// A body given its permit onto full buffers goes back to wait: a permit can be a long
/// wait, and without a second look the buffers may have filled meanwhile.
#[test]
fn a_body_given_its_permit_onto_full_buffers_goes_back_to_wait() {
    let at = small_buffers("door-second-look", 1);
    let still = at.still();
    let first = at.post(lines(0));
    until("the first body to be in the log", || {
        at.wal().unapplied_bytes == one_body() && at.parsing() == 0
    });
    let body = lines(1);
    let mut second = Client::connect(at.addr);
    second.head("POST", "/ingest", body.len());
    second.write(&body[..body.len() / 2]);
    until("the second body to hold the permit", || at.parsing() == 1);
    let third = at.post(lines(2));
    until("the third body to wait at the door", || at.at_the_door() == 1);
    assert!(!at.server.stores.buffers_full());

    drop(still);
    let first = first.join().expect("the first sender");
    assert_eq!(first.status, 200, "{first:?}");
    assert!(at.server.stores.buffers_full());
    second.write(&body[body.len() / 2..]);
    let second = second.answer();
    assert_eq!(second.status, 200, "{second:?}");
    until("the third body to give its permit back", || {
        at.parsing() == 0 && at.at_the_door() == 1
    });
    assert!(!third.is_finished());

    let began = Instant::now();
    assert_eq!(tick(&at).failed, 0);
    let third = third.join().expect("the third sender");
    assert!(began.elapsed() < WOKEN_WITHIN);
    assert_eq!(third.status, 200, "{third:?}");
    assert_eq!(at.shed(), 0);
    assert_eq!(at.count(), 3 * PER_BODY as u64);
}

/// The line at the door has no bound at the default wait, where nobody is in it for long;
/// a wait an operator has raised keeps each waiter's socket, and is bounded by the places.
#[test]
fn the_line_has_no_bound_at_the_default_wait() {
    let with_wait = |ms: u64| {
        let api = ApiLimits {
            ingest_wait: Duration::from_millis(ms),
            ..ApiLimits::default()
        };
        let server = Server::new(
            Utc::now(),
            Limits::default(),
            api,
            Storage::default(),
            Schema::default(),
            None,
        );
        Arc::new(server.expect("a server in memory"))
    };
    let places = ingest_bodies(&ApiLimits::default());
    let default = crate::settings::DEFAULT_INGEST_WAIT_MS as u64;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");

    for ms in [0, 1, default] {
        let server = with_wait(ms);
        assert_eq!(server.line_max(), None, "{ms}ms");
        assert_eq!(stats_of(&server)["ingest"]["door"]["line_max"], Value::Null);
        // As many at the door as there are places, and one more is let wait: here
        // it has nothing to wait for, and is let in.
        server.door.waiting.store(places as u64, Ordering::Relaxed);
        let admitted = runtime.block_on(server.admit_waiting());
        assert!(admitted.is_ok(), "{ms}ms");
        assert_eq!(server.door.shed.total(), 0, "{ms}ms");
    }
    for ms in [default + 1, 30_000] {
        let server = with_wait(ms);
        assert_eq!(server.line_max(), Some(places), "{ms}ms");
        assert_eq!(stats_of(&server)["ingest"]["door"]["line_max"], places);
        // One short of the bound waits; at the bound the next is shed at once.
        server.door.waiting.store(places as u64 - 1, Ordering::Relaxed);
        assert!(runtime.block_on(server.admit_waiting()).is_ok(), "{ms}ms");
        server.door.waiting.store(places as u64, Ordering::Relaxed);
        let refused = runtime.block_on(server.admit_waiting());
        let refused = refused.err().expect("the line was full");
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(refused.headers().get(header::RETRY_AFTER).is_some());
        assert_eq!(server.door.shed.line.load(Ordering::Relaxed), 1, "{ms}ms");
        assert_eq!(server.door.shed.total(), 1, "{ms}ms");
    }
}

/// The flush task says so when it has flushed, which lets in a body waiting at the door;
/// the tick is an hour long, so the flush it runs is the one it was rung for.
#[test]
fn the_flush_task_lets_in_a_body_waiting_at_the_door() {
    let storage = Storage {
        max_buffer_bytes: 1024,
        flush_interval: Duration::from_secs(3600),
        ..Storage::default()
    };
    let at = Listening::start("door-flush-task", 2, LONG_WAIT, storage);
    let first = at.post(lines(0)).join().expect("a sender");
    assert_eq!(first.status, 200, "{first:?}");
    assert!(at.server.stores.buffers_full());
    let second = at.post(lines(1));
    until("the second body to wait at the door", || at.at_the_door() == 1);

    let runtime = at.runtime.as_ref().expect("the runtime");
    let token = CancellationToken::new();
    let began = Instant::now();
    let flusher = runtime.spawn(flush_loop(Arc::clone(&at.server), token.clone()));
    let second = second.join().expect("the second sender");
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "the body at the door was not told that the buffers had been flushed"
    );
    assert_eq!(second.status, 200, "{second:?}");
    assert_eq!(at.shed(), 0);
    assert_eq!(at.count(), 2 * PER_BODY as u64);
    token.cancel();
    runtime.block_on(flusher).expect("the flush task");
}

// ── The door's own decision, a look at a time ─────────────────────────────────────
//
// The tests below ask the door directly and turn it by hand: a body at the door is a
// future looked at only when the test looks, so nothing is raced against the deadline.

/// A runtime for a body at the door that the test turns by hand.
fn by_hand() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

/// One look by a body at the door, and no more: what it had come to by the end of
/// it. Nothing wakes it between two of these.
fn look<F: std::future::Future>(
    runtime: &tokio::runtime::Runtime,
    door: &mut Pin<Box<F>>,
) -> Poll<F::Output> {
    runtime.block_on(std::future::poll_fn(|cx| {
        Poll::Ready(door.as_mut().poll(cx))
    }))
}

/// What a refusal at the door says: its status, whether it carries a `Retry-After`,
/// and its error.
fn refusal(
    runtime: &tokio::runtime::Runtime,
    refused: Result<Permit, Box<Response>>,
) -> (StatusCode, bool, String) {
    let Err(response) = refused else {
        panic!("the body was let in");
    };
    let status = response.status();
    let retry = response.headers().get(header::RETRY_AFTER).is_some();
    let bytes = runtime
        .block_on(axum::body::to_bytes(response.into_body(), usize::MAX))
        .expect("the refusal's body");
    let body: Value = serde_json::from_slice(&bytes).expect("a refusal is JSON");
    let error = body["error"].as_str().unwrap_or_default().to_string();
    (status, retry, error)
}

/// A server whose buffers one body fills past twice their limit, which no task
/// flushes, and whose door keeps a body for `wait`. The one body is in it.
fn full_buffers(tag: &str, wait: Duration) -> Listening {
    let storage = Storage {
        max_buffer_bytes: 1024,
        ..Storage::default()
    };
    let at = Listening::start(tag, 2, wait, storage);
    let first = at.post(lines(0)).join().expect("a sender");
    assert_eq!(first.status, 200, "{first:?}");
    assert!(at.server.stores.buffers_full());
    at
}

/// A wait for the buffers that runs out looks at them once more: a flush empties them
/// and only then tells the door, so a wait can run out beside buffers that have room.
#[test]
fn a_wait_for_the_buffers_that_runs_out_looks_at_them_once_more() {
    let at = full_buffers("door-last-look", Duration::from_millis(20));
    let runtime = by_hand();
    let door = &at.server.door;

    // Still full when the wait runs out: a shed, and the bytes in it are the ones
    // that are held.
    let (held, limit) = at.server.stores.buffer_bytes();
    assert!(held > 2 * limit, "{held} of {limit}");
    let mut waiting = Box::pin(at.server.admit_waiting());
    assert!(look(&runtime, &mut waiting).is_pending());
    assert_eq!(at.at_the_door(), 1);
    let (status, retry, error) = refusal(&runtime, runtime.block_on(&mut waiting));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(retry, "{error}");
    let says = format!(
        "the buffers hold {} bytes of memory, past twice the {} byte limit: the flush \
         is behind what is arriving",
        commas(held),
        commas(limit)
    );
    assert!(error.starts_with(&says), "{error}");
    assert_eq!(door.shed.buffers.load(Ordering::Relaxed), 1);
    assert_eq!(at.shed(), 1);

    // Emptied while the body waits, with no word to the door: let in when its wait
    // runs out, and not shed for buffers that have room.
    let mut waiting = Box::pin(at.server.admit_waiting());
    assert!(look(&runtime, &mut waiting).is_pending());
    let flushed = at.server.stores.flush_all(FlushKind::Tick);
    assert_eq!(flushed.failed, 0);
    assert!(!at.server.stores.buffers_full());
    let admitted = runtime.block_on(&mut waiting);
    assert!(admitted.is_ok(), "shed beside buffers that had room");
    assert_eq!(at.shed(), 1, "the one shed is the first half's");
    assert_eq!(door.waits.load(Ordering::Relaxed), 2);
    assert_eq!(at.at_the_door(), 0);
    assert_eq!(
        (at.parsing(), at.inside()),
        (1, 1),
        "the permit it was given"
    );
}

/// A store that stops writing while a body waits for the buffers is the reason the body
/// is given, not "the flush is behind what is arriving".
#[test]
#[cfg(unix)]
fn a_store_that_fails_during_a_wait_for_the_buffers_is_the_reason_given() {
    use std::os::unix::fs::PermissionsExt;

    let at = full_buffers("door-last-look-store", Duration::from_millis(20));
    let runtime = by_hand();
    // The first body's day gets its file, and the buffers are empty again. Then a
    // day with no file yet, and a directory that will not take one.
    assert_eq!(at.server.stores.flush_all(FlushKind::Tick).failed, 0);
    let data = at.server.storage.base_path.as_ref().expect("on disk");
    let data = data.join(crate::storage::store::disk::DATA_DIR);
    let mode = |mode| std::fs::set_permissions(&data, std::fs::Permissions::from_mode(mode));
    mode(0o555).expect("chmod -w");
    let second = at
        .post(lines_at(1_705_401_000, 1))
        .join()
        .expect("a sender");
    let full = at.server.stores.buffers_full();

    let mut waiting = Box::pin(at.server.admit_waiting());
    let parked = look(&runtime, &mut waiting).is_pending();
    let failed = at.server.stores.flush_all(FlushKind::Tick).failed;
    let still_full = at.server.stores.buffers_full() && at.server.write_health.is_degraded();
    let answered = runtime.block_on(&mut waiting);
    mode(0o755).expect("chmod +w");

    assert_eq!(second.status, 200, "{second:?}");
    assert!(full, "one body did not fill the buffers");
    assert!(
        parked,
        "a body was not kept waiting for a flush that could come"
    );
    assert!(
        failed > 0 && still_full,
        "the store wrote to a read-only directory"
    );
    let (status, retry, error) = refusal(&runtime, answered);
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(retry, "{error}");
    assert!(error.contains("the store cannot write"), "{error}");
    assert!(!error.contains("the flush is behind"), "{error}");
    assert_eq!(at.server.door.shed.buffers.load(Ordering::Relaxed), 1);
    assert_eq!(at.shed(), 1);
}

/// With no wait to give, a body at the door is let in or shed at its first look, not
/// parked on a timer for a moment that has already come.
#[test]
fn with_no_wait_a_body_at_the_door_is_answered_at_its_first_look() {
    let at = full_buffers("door-no-wait", Duration::ZERO);
    let runtime = by_hand();
    let server = &at.server;
    let shed = &server.door.shed;
    let first_look = |what: &str| {
        let mut asking = Box::pin(server.admit_waiting());
        match look(&runtime, &mut asking) {
            Poll::Ready(answer) => answer,
            Poll::Pending => panic!("{what}: a body with no wait to give was kept waiting"),
        }
    };

    // The buffers past twice their limit.
    let (status, retry, error) = refusal(&runtime, first_look("full buffers"));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(retry && error.starts_with("the buffers hold"), "{error}");
    assert!(!error.contains("hold 0 bytes"), "{error}");
    assert_eq!(shed.buffers.load(Ordering::Relaxed), 1);
    assert_eq!(server.stores.flush_all(FlushKind::Tick).failed, 0);
    assert!(!server.stores.buffers_full());

    // No permit for the parse.
    let permits = Arc::clone(&server.ingest_permits)
        .try_acquire_many_owned(server.api.ingest_concurrency as u32)
        .expect("every permit");
    let (status, retry, error) = refusal(&runtime, first_look("no permit"));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(retry && error.contains(SAYS_BUSY), "{error}");
    assert_eq!(shed.permit.load(Ordering::Relaxed), 1);
    drop(permits);

    // No place, and the permit that was taken on the way to it goes back.
    let places = Arc::clone(&server.ingest_bodies)
        .try_acquire_many_owned(ingest_bodies(&server.api) as u32)
        .expect("every place");
    let (status, retry, error) = refusal(&runtime, first_look("no place"));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(retry && error.contains(SAYS_LOG_BUSY), "{error}");
    assert_eq!(shed.place.load(Ordering::Relaxed), 1);
    assert_eq!(at.parsing(), 0);
    drop(places);

    // And with nothing missing it is let in, at that same first look.
    let admitted = first_look("nothing missing");
    assert!(admitted.is_ok());
    assert_eq!((at.parsing(), at.inside()), (1, 1));
    drop(admitted);
    assert_eq!((at.parsing(), at.inside()), (0, 0));
    assert_eq!(at.shed(), 3);
    assert_eq!(at.at_the_door(), 0);
}

/// A sender that hangs up while its body waits for room frees its slot: the body gives up
/// its wait and its permit, and nothing of it is in the log.
#[test]
fn a_sender_that_hangs_up_while_waiting_for_room_frees_its_slot() {
    let at = one_at_a_time("room-hangup", LONG_WAIT);
    let still = at.still();
    let first = at.post(lines(0));
    until("the first body to be in the log", || {
        at.wal().unapplied_bytes == one_body() && at.parsing() == 0
    });
    let began = {
        // Written whole, and the connection closed with no answer read.
        let body = lines(1);
        let mut second = Client::connect(at.addr);
        second.head("POST", "/ingest", body.len());
        second.write(&body);
        until("the second body to wait for room", || {
            at.wal().queue_waiting == 1
        });
        Instant::now()
    };
    until("the second body to give up its wait", || {
        at.wal().queue_left == 1 && at.parsing() == 0
    });
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "a body whose sender had hung up went on holding its permit"
    );
    assert_eq!(at.shed(), 0, "a sender that left is not a shed");

    let third = at.post(lines(2));
    until("the third body to take the permit", || {
        at.wal().queue_waiting == 1
    });
    drop(still);
    for body in [first, third] {
        let answer = body.join().expect("a sender");
        assert_eq!(answer.status, 200, "{answer:?}");
    }
    until("every place to come back", || at.inside() == 0);
    // The first and the third, and no line of the second: not stored, not logged,
    // and not counted as taken.
    assert_eq!(at.count(), 2 * PER_BODY as u64);
    assert_eq!(at.wal().last_seq, 2 * PER_BODY as u64);
    let server = &at.server;
    assert_eq!(
        server.lines_ingested.load(Ordering::Relaxed),
        2 * PER_BODY as u64
    );
    assert_eq!(
        server.lines_rejected.load(Ordering::Relaxed),
        PER_BODY as u64
    );
}
