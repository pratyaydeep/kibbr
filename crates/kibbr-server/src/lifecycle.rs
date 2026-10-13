// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Lifecycle: the runtime, the flush clock, signals, shutdown and the banner.

use super::*;

// ── Lifecycle ──────────────────────────────────────────────────────────────────────

/// How a server's run ended, once it has stopped and wound down; never a failure, since
/// a stop that got this far lost nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Where it listened: the port the system gave, where the settings asked for 0.
    pub addr: SocketAddr,
    /// Whether the shutdown deadline passed with requests still in flight, each then
    /// answered 503 where it stood and let go.
    pub cut: bool,
}

/// Build the runtime and run the server until `cancel` is cancelled, by the caller or
/// by SIGTERM or SIGINT; the runtime is built here because the settings size it.
pub fn run(settings: Settings, cancel: CancellationToken) -> Result<Report> {
    // The scan threads' last owner. Declared before the runtime, so let go after it, on
    // the calling thread when no runtime thread exists (see `store::fan`).
    let scan: OnceLock<Arc<Fan>> = OnceLock::new();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(ingest_bodies(&settings.api) + OTHER_BLOCKING_THREADS)
        .build()
        .map_err(Error::Runtime)?;
    let report = runtime.block_on(serve(settings, cancel, &scan))?;
    // Dropping a runtime waits for every blocking thread. After a cut, one may be a query's
    // that is not asked to stop again: it writes nothing, so it is given a moment and left.
    if report.cut {
        runtime.shutdown_timeout(CUT_OFF_GRACE);
    }
    Ok(report)
}

async fn serve(
    settings: Settings,
    token: CancellationToken,
    scan: &OnceLock<Arc<Fan>>,
) -> Result<Report> {
    let Settings {
        host,
        port,
        limits,
        api,
        storage,
        schema,
        banner: how,
    } = settings;
    // Read-only, so it runs before the lock and the port: a data directory holding a
    // segment from another format version is not served at all.
    if let Some(base) = storage.base_path.as_deref() {
        kibbr_storage::store::disk::refuse_other_versions(base).map_err(storage_fatal)?;
    }
    // Taken before anything is written and before the port: two servers on one directory
    // would each append to the catalog and the manifest over the other's records.
    let mut lock = match storage.base_path.as_deref() {
        Some(base) => {
            let mut lock = kibbr_storage::lock::DataLock::acquire(base)?;
            let _ = lock.announce(&kibbr_storage::lock::Holder::server(None, None));
            Some(lock)
        }
        None => None,
    };

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|source| Error::Bind {
            addr: addr.clone(),
            source,
        })?;
    let bound: SocketAddr = listener
        .local_addr()
        .map_err(|source| Error::Bind { addr, source })?;

    // Written to `LOCK` beside the address: a `/load` shows it or is refused. See lock.rs.
    let load_token = match &lock {
        Some(_) => Some(
            kibbr_storage::store::v3::file::new_file_id()
                .map(|id| kibbr_storage::store::manifest::hex(&id))
                .map_err(|e| Error::Storage {
                    detail: format!("drawing the load token: {e}"),
                })?,
        ),
        None => None,
    };
    let server = Arc::new(Server::new(
        Utc::now(),
        limits,
        api,
        storage,
        schema,
        load_token.clone(),
    )?);
    // Handed to `run` before anything below can fail or answer a query, so that on every
    // way out of here the scan threads are let go by `run`, after the runtime.
    if let Some(fan) = &server.fan {
        let _ = scan.set(Arc::clone(fan));
    }
    // Where `kibbr load` finds this server - written once it can answer. An address of
    // all interfaces is reached on loopback, which is where the directory is.
    if let Some(lock) = lock.as_mut() {
        let reach = if bound.ip().is_unspecified() {
            SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), bound.port())
        } else {
            bound
        };
        let holder = kibbr_storage::lock::Holder::server(Some(reach.to_string()), load_token);
        if let Err(e) = lock.announce(&holder) {
            log_warn(&format!(
                "{e}; `kibbr load` and `kibbr compact` will not find this server"
            ));
        }
    }
    // Installed before the banner: a server that cannot hear SIGTERM is killed by the
    // SIGKILL that follows. The token is the caller's, so both stop it the same way.
    let signals = signals(token.clone())?;

    banner(bound, &server, how);
    tokio::spawn(signals);

    // The clock the stores deliberately do not own. Held rather than detached: a tick
    // after the final flush would write behind the `trie.json` snapshot.
    let flusher = tokio::spawn(flush_loop(Arc::clone(&server), token.clone()));
    // And the clock compaction is looked for by. Held for the same reason: a look after
    // the wind-down would start a compaction nothing waits for.
    let compactor = tokio::spawn(compact_loop(Arc::clone(&server), token.clone()));

    let shutdown = {
        let token = token.clone();
        let server = Arc::clone(&server);
        async move {
            token.cancelled().await;
            // Whoever is waiting - at the door, or for room in the log - is refused now,
            // and a compaction is told to stop.
            begin_stop(&server);
            // Shutdown, step one. Everything after this is the wind-down; the log names each
            // step so an operator can see which ones this build actually has.
            log(&format!(
                "stop accepting - {} request(s) in flight",
                server.in_flight.load(Ordering::Relaxed)
            ));
        }
    };

    let mut serving = std::pin::pin!(axum::serve(listener, router(Arc::clone(&server)))
        .with_graceful_shutdown(shutdown)
        .into_future());
    // The deadline's clock starts at the signal, and with no deadline it never runs out.
    let past_deadline = {
        let token = token.clone();
        let deadline = server.api.shutdown_deadline;
        async move {
            token.cancelled().await;
            match deadline {
                Some(deadline) => tokio::time::sleep(deadline).await,
                None => std::future::pending().await,
            }
        }
    };
    let left = tokio::select! {
        served = &mut serving => {
            served.map_err(Error::Serve)?;
            None
        }
        () = past_deadline => Some(server.in_flight.load(Ordering::Relaxed)),
    };
    // Past the deadline each request in flight is answered 503 where it stands (`track`); a
    // connection that will not take even that within the grace is left for the exit to close.
    let cut = left.is_some();
    if let Some(left) = left {
        log_warn(&format!(
            "shutdown deadline - {left} request(s) still in flight after {}; each is \
             answered 503 and let go",
            crate::settings::duration_words(server.api.shutdown_deadline.unwrap_or_default())
        ));
        server.cut_off.cancel();
        match tokio::time::timeout(CUT_OFF_GRACE, &mut serving).await {
            Ok(served) => served.map_err(Error::Serve)?,
            Err(_) => {
                log_warn("shutdown deadline - a connection did not take its answer; left open")
            }
        }
    }

    // `axum::serve` returns once every connection has finished, but a body whose client hung
    // up still has its blocking task: every permit taken back is every such task returned.
    let _ingest_done = server
        .ingest_bodies
        .acquire_many(ingest_bodies(&server.api) as u32)
        .await;
    // Loads stop next, so the final flush and the trie snapshot below see the directory as
    // it will stay.
    stop_loads(&server);

    // Joined before the wind-down: a tick between the final flush and the trie snapshot
    // would write rows the snapshot does not describe.
    let _ = flusher.await;
    // No look starts a compaction from here on; the one running, if any, has been told to
    // stop and is waited for inside the sequence, behind the final flush.
    let _ = compactor.await;
    let result = shutdown_sequence(&server, cut);
    drop(lock);
    // Whether requests were cut off: `run` then does not wait on their threads.
    result.map(|()| Report { addr: bound, cut })
}

/// Ask every store whether a row group is due, each interval or when woken. The
/// flush is `spawn_blocking` so `/health` still answers; `Delay` so a stall is not a burst.
pub(super) async fn flush_loop(server: Arc<Server>, token: CancellationToken) {
    if !server.storage.is_durable() {
        return;
    }
    let mut tick = tokio::time::interval(server.storage.flush_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately; consumed here so the loop's first real wait is
    // a whole interval rather than none.
    tick.tick().await;
    // Whether the last flush was closing buffers early for the memory limit: said once when
    // it starts and once when it stops, not on every tick in between.
    let mut forcing = false;
    loop {
        tokio::select! {
            () = token.cancelled() => return,
            _ = tick.tick() => {}
            () = server.flush_now.notified() => {}
        }
        let held = Arc::clone(&server);
        let flushed = tokio::task::spawn_blocking(move || {
            let summary = held.stores.flush_all(FlushKind::Tick);
            // Behind the flush, never ahead of it: the checkpoint moves past a row only
            // once the row group holding it has been flushed and published.
            move_checkpoint(&held);
            summary
        });
        let flushed = flushed.await;
        // Said to whoever is waiting at the door for the buffers, whatever the flush
        // did: each looks again, and one that finds them still full goes on waiting.
        server.flushed.notify_waiters();
        match flushed {
            Ok(summary) => {
                if summary.failed > 0 {
                    log_warn(&format!(
                        "flush - {} leaf/leaves could not be written; their rows stay \
                         buffered and the next tick retries",
                        summary.failed
                    ));
                }
                if (summary.forced > 0) != forcing {
                    forcing = summary.forced > 0;
                    let (_, limit) = server.stores.buffer_bytes();
                    if forcing {
                        log_warn(&format!(
                            "buffers - past the {} byte limit; closing the largest early \
                             ({} this flush), which makes small row groups",
                            commas(limit),
                            summary.forced
                        ));
                    } else {
                        log(&format!(
                            "buffers - back under the {} byte limit; {} closed early in all",
                            commas(limit),
                            commas(server.stores.forced_closes())
                        ));
                    }
                }
            }
            Err(join) => log_warn(&format!("flush task failed: {join}")),
        }
    }
}

/// After a flush: ask a log that cannot write to try again, and move its checkpoint up
/// behind the oldest row still only in memory, every tick, or the log would grow.
pub(super) fn move_checkpoint(server: &Server) -> Option<wal::Advanced> {
    let wal = server.wal.as_ref()?;
    wal.probe();
    let (moved, problem) =
        wal.advance(
            || match (server.stores.oldest_position(), server.held.from()) {
                (Some(buffered), Some(held)) => Some(buffered.min(held)),
                (buffered, held) => buffered.or(held),
            },
        );
    // Told whatever else happened: the checkpoint is where it is whether or not its file
    // could be written, and the scheduler's compaction waits on it.
    if let (Some(moved), Some(disk)) = (moved, server.stores.disk_store()) {
        disk.note_checkpoint(moved.checkpoint);
    }
    match problem {
        None => server.checkpoint_failing.store(false, Ordering::Relaxed),
        // Said once, not every tick: nothing is lost by it - a start reads a little more
        // of the log, or finds a segment this could not delete and deletes it.
        Some(e) => {
            if !server.checkpoint_failing.swap(true, Ordering::Relaxed) {
                log_warn(&format!(
                    "wal - the checkpoint moved and could not be kept: {e}"
                ));
            }
        }
    }
    moved
}

/// SIGTERM and SIGINT, both cancelling the one token. Returned rather than spawned, so
/// a handler that could not be installed is an error at startup.
fn signals(token: CancellationToken) -> Result<impl std::future::Future<Output = ()>, Error> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).map_err(|source| Error::Signal {
            signal: "SIGTERM",
            source,
        })?;
        let mut int = signal(SignalKind::interrupt()).map_err(|source| Error::Signal {
            signal: "SIGINT",
            source,
        })?;
        Ok(async move {
            let name = tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = int.recv() => "SIGINT",
            };
            log(&format!("{name} received - shutting down"));
            token.cancel();
        })
    }
    #[cfg(not(unix))]
    {
        Ok(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                log("interrupt received - shutting down");
            }
            token.cancel();
        })
    }
}

/// The fixed shutdown order, the steps a memory server lacks printed as skipped
/// so an operator sees which durability they have. No row depends on it finishing.
fn shutdown_sequence(server: &Server, cut: bool) -> Result<(), Error> {
    log(if cut {
        "in-flight requests cut off at the shutdown deadline"
    } else {
        "in-flight requests complete"
    });

    if server.storage.is_durable() {
        let summary = server.stores.flush_all(FlushKind::Final);
        // Rows still in a buffer once the final flush has done all it can. Not lost: they
        // are in the log, and the next start puts them back.
        let unwritten = match summary.failed {
            0 => None,
            leaves => Some((leaves, server.stores.buffered())),
        };
        // Bounded by the open days and not by the templates: one flush and one seal per day
        // file, however many templates' row groups went into it.
        log(&format!(
            "final flush - {} row(s) into {} row group(s) in {} file(s), {} sealed, {} \
             byte(s), {:.2}s{}",
            commas(summary.rows),
            commas(summary.row_groups as u64),
            summary.syncs,
            summary.sealed,
            commas(summary.bytes),
            summary.took.as_secs_f64(),
            match unwritten {
                None => String::new(),
                Some((leaves, rows)) => format!(
                    ", {leaves} leaf/leaves FAILED: {} row(s) were not written to a row \
                     group. They are in the write-ahead log, and the next start puts them back",
                    commas(rows)
                ),
            },
        ));
    } else {
        log("final flush - skipped, storage is memory (--data-dir puts the stores on disk)");
    }

    // Not one of the six steps: it writes nothing a restart needs. Here because a
    // compaction's own flush belongs behind the one above.
    join_compactions(server);

    // Behind the final flush, so that a clean stop leaves the checkpoint at the end of the
    // log and the next start with nothing to read back.
    match &server.wal {
        Some(wal) => {
            let moved = move_checkpoint(server);
            let stats = wal.stats();
            log(&format!(
                "WAL checkpoint - position {} of {}{}; {} segment(s), {} byte(s)",
                commas(stats.checkpoint),
                commas(stats.last_seq),
                match moved {
                    Some(moved) if moved.deleted > 0 =>
                        format!(", {} segment(s) behind it deleted", moved.deleted),
                    _ => String::new(),
                },
                stats.segments,
                commas(stats.bytes),
            ));
        }
        None => log("WAL checkpoint - skipped, storage is memory"),
    }

    match server.storage.trie_file.as_deref() {
        Some(path) => match kibbr_storage::triefile::save(&server.trie, path) {
            Ok(bytes) => log(&format!(
                "trie.json - {} {} byte(s), {} template(s)",
                path.display(),
                commas(bytes),
                commas(server.trie.template_count())
            )),
            // Not a reason to fail the exit. Nothing loads the snapshot while the catalog
            // exists: it is there for a human to read.
            Err(e) => log_warn(&format!("trie.json - {}", error::chain(&e))),
        },
        None => log("trie.json - skipped, storage is memory"),
    }

    let stores = &server.stores;
    // Rows still buffered never reached a file, so they do not belong in a count of what
    // was written.
    let held = stores.rows();
    let unwritten = stores.buffered();
    let kept = if server.storage.is_durable() {
        "written"
    } else {
        "discarded, they were only in memory"
    };
    log(&format!(
        "close stores - {} store(s), {} row(s) {kept}{}{}",
        commas(stores.open() as u64),
        commas(held.saturating_sub(unwritten)),
        match (server.storage.is_durable(), unwritten) {
            (true, 0) => String::new(),
            (true, n) => format!(
                ", {} row(s) still buffered: in the write-ahead log, put back at the next start",
                commas(n)
            ),
            (false, _) => String::new(),
        },
        match server.held.rows() {
            0 => String::new(),
            n => format!(
                ", {} row(s) the store refused: in the write-ahead log, stored at the next start",
                commas(n)
            ),
        },
    ));

    match &server.wal {
        Some(wal) => {
            wal.close();
            log("close WAL - closed");
        }
        None => log("close WAL - skipped, storage is memory"),
    }

    // Always 0 once it gets here: a stop that leaves every acknowledged line durable in
    // the log has lost nothing, and an orchestrator told otherwise would be told a lie.
    log("exit 0");
    Ok(())
}

/// The threads one query of this server may choose a day's rows on, its own among them;
/// 1 where there is nobody to share with - the memory store, or a disk store told 0 or 1.
pub(super) fn scan_threads(server: &Server) -> usize {
    server.fan.as_ref().map_or(1, |_| server.storage.scan_threads)
}

/// The banner's line about the scan threads. The default depends on the machine, and
/// nothing else at startup says what it came to.
pub(super) fn scan_line(server: &Server) -> String {
    match scan_threads(server) {
        1 => "  scan        1 thread to a query - no scan is shared out".to_string(),
        threads => format!(
            "  scan        {threads} threads to a query, its own among them; a run of a\n\
             \x20             day's row groups under {} rows is never shared out, nor\n\
             \x20             any while an /ingest body is between the door and its answer",
            commas(server.storage.scan_fan_rows),
        ),
    }
}

/// The banner's lines about `/ingest`: the limits on a body, how long one may be kept
/// waiting and for what, and the bound on the line at the door when it has one.
fn ingest_line(server: &Server) -> String {
    let api = server.api;
    let wait = api.ingest_wait.as_millis();
    let waits = match &server.wal {
        Some(_) => format!(
            "a body waits up to {wait}ms for a slot, for a flush of full buffers or\n\
             \x20             for room in the log, and is then answered 503 with a Retry-After"
        ),
        None => format!(
            "a body waits up to {wait}ms for a slot, and is then answered 503 with\n\
             \x20             a Retry-After rather than left to time out"
        ),
    };
    let line = match server.line_max() {
        Some(max) => format!(
            "\n\x20             at most {max} wait at the door at once; one more is answered 503"
        ),
        None => String::new(),
    };
    format!(
        "  ingest      {} byte body, {} byte decompressed, {} at once\n\
         \x20             {waits}{line}",
        commas(api.max_body_bytes as u64),
        commas(api.max_decompressed_bytes as u64),
        api.ingest_concurrency,
    )
}

/// The startup banner, an event under `BANNER` that the binary writes bare to stderr, so
/// `kibbr serve >/dev/null` still prints the paragraph about losing data.
fn banner(addr: SocketAddr, server: &Server, how: Banner) {
    let limits = server.limits;
    if how == Banner::Event {
        let data_dir = server.storage.base_path.as_ref();
        tracing::info!(
            addr = %addr,
            storage = if data_dir.is_some() { "disk" } else { "memory" },
            data_dir = %data_dir.map_or_else(String::new, |path| path.display().to_string()),
            max_rows = limits.max_rows,
            ingest_concurrency = server.api.ingest_concurrency,
            ingest_wait_ms = server.api.ingest_wait.as_millis() as u64,
            shutdown_deadline_ms =
                server.api.shutdown_deadline.map_or(0, |d| d.as_millis() as u64),
            request_timeout_ms =
                server.api.request_timeout.map_or(0, |d| d.as_millis() as u64),
            scan_threads = server.storage.scan_threads,
            "listening on http://{addr}"
        );
        if data_dir.is_none() {
            tracing::warn!(
                "storage is in memory: everything ingested is lost when this process exits"
            );
        }
        return;
    }
    let lines = [
        String::new(),
        "kibbr serve".to_string(),
        String::new(),
        format!("  listening   http://{addr}"),
        String::new(),
        // Two paragraphs, one per storage mode: a warning that softens once a feature lands
        // beside it is a warning nobody reads the second time.
        match (&server.storage.base_path, &server.storage.trie_file) {
            (Some(base), trie_file) => format!(
                "  storage     {}\n\
                 \x20             trie      {}\n\
                 \x20             segments  a row is queryable the moment it is ingested; it\n\
                 \x20                       reaches a file when its row group closes - 8,192\n\
                 \x20                       rows, 8MB, or {}ms, whichever comes first. The\n\
                 \x20                       thresholds are checked every {}ms. At most {} day\n\
                 \x20                       files are open for writing at once; past that a\n\
                 \x20                       row for a day with none open seals the least\n\
                 \x20                       recently written and opens its own day's. Keep\n\
                 \x20                       it under `ulimit -n`.\n\
                 \x20                       SIGTERM closes and seals everything before exiting.\n\
                 \x20             compact   a day's small row groups are rebuilt into full ones,\n\
                 \x20                       in finished files, the day after its last live\n\
                 \x20                       row. {}\n\
                 \x20             catalog   {}/catalog.log - a template is written here, and\n\
                 \x20                       fsynced, before the line that made it counts as\n\
                 \x20                       ingested; trie.json is a readable snapshot nothing\n\
                 \x20                       loads while the catalog exists.\n\
                 \x20             wal       {}/wal - a line is flushed here before /ingest\n\
                 \x20                       acknowledges it. A kill -9 or a power cut loses no\n\
                 \x20                       acknowledged line: the next start puts each back, once.\n\
                 \x20                       At most {} bytes of lines are in the log and\n\
                 \x20                       not yet in the store; a body past that waits for room.",
                base.display(),
                trie_file
                    .as_ref()
                    .map_or_else(|| "none".to_string(), |p| p.display().to_string()),
                server.storage.row_group_timeout.as_millis(),
                server.storage.flush_interval.as_millis(),
                commas(server.storage.max_open_segments as u64),
                match server.storage.compact_interval.as_millis() {
                    0 => "Not looked for (--compact-interval-ms 0):\n\
                          \x20                       `kibbr compact` does it when asked."
                        .to_string(),
                    ms => format!(
                        "Looked for every {ms}ms; `kibbr compact`\n\
                         \x20                       does it when asked."
                    ),
                },
                base.display(),
                base.display(),
                commas(server.storage.wal_queue_bytes as u64),
            ),
            (None, _) => {
                "  STORAGE IS IN MEMORY. Everything ingested is lost when this process exits.\n\
                 \x20 Nothing is written to disk: there is no write-ahead log and no snapshot of\n\
                 \x20 the trie. A restart starts an empty database. Do not point anything at this\n\
                 \x20 that you would be unhappy to lose."
                    .to_string()
            }
        },
        String::new(),
        "  endpoints   GET  /            the log viewer, in a browser".to_string(),
        "              POST /ingest      NDJSON body, one log line per line; gzip ok".to_string(),
        "              POST /query       {\"q\": \"<trieql>\"}".to_string(),
        "              POST /load        a bulk load job: what `kibbr load` sends".to_string(),
        "              POST /compact     a compaction job: what `kibbr compact` sends".to_string(),
        "              GET  /templates   every template, its columns and row count".to_string(),
        "              GET  /stats       throughput, template count, storage size".to_string(),
        "              GET  /health      {\"ok\":true}".to_string(),
        String::new(),
        match limits.max_rows {
            0 => "  max rows    none - a MATCH with no LIMIT materialises every matching row"
                .to_string(),
            n => format!(
                "  max rows    {n} - a MATCH with no LIMIT is answered a page at a time,\n\
                 \x20             truncated with a next_cursor to continue from"
            ),
        },
        match (limits.max_buckets, limits.max_groups) {
            (0, 0) => "  max buckets none - a GROUP BY time(...) emits whatever the range holds"
                .to_string(),
            (buckets, groups) => format!(
                "  max buckets {buckets} buckets, {groups} groups - a wider GROUP BY time(...)\n\
                 \x20             is refused rather than drawn with its tail missing"
            ),
        },
        ingest_line(server),
        scan_line(server),
        // On every start: the cost of learning it later is a fleet of shipper configs that
        // all have to change at once. `post_ingest` has the long form.
        "  tenancy     single tenant - /ingest takes no stream or tenant identifier,\n\
         \x20             and adding one later changes every shipper config in the\n\
         \x20             fleet. Deliberate; revisit with the deployment work."
            .to_string(),
        match server.api.request_timeout {
            Some(timeout) => format!(
                "  requests    answered within {} or timed out: 408 for a body that has not\n\
                 \x20             arrived, 504 for a query, whose scan is stopped. Not an\n\
                 \x20             /ingest body read whole (--request-timeout).",
                crate::settings::duration_words(timeout)
            ),
            None => "  requests    given as long as they take (--request-timeout 0s)."
                .to_string(),
        },
        String::new(),
        match server.api.shutdown_deadline {
            Some(deadline) => format!(
                "  shutdown    SIGINT (Ctrl-C) or SIGTERM. In-flight requests finish first; one\n\
                 \x20             still in flight {} later is answered 503 and let go.",
                crate::settings::duration_words(deadline)
            ),
            None => "  shutdown    SIGINT (Ctrl-C) or SIGTERM. In-flight requests finish first,\n\
                     \x20             however long they take (--shutdown-deadline 0s)."
                .to_string(),
        },
        String::new(),
    ];
    tracing::info!(target: BANNER, "{}", lines.join("\n"));
}

/// One lifecycle line, an `info` event and never `eprintln!`, which panics when stderr
/// is gone and would end the flush, load or compaction that called it.
pub(super) fn log(message: &str) {
    tracing::info!("{message}");
}

/// A line an operator should read: something was skipped, delayed or left as it was.
pub(super) fn log_warn(message: &str) {
    tracing::warn!("{message}");
}

/// A line that says the server cannot do what it is for.
pub(super) fn log_error(message: &str) {
    tracing::error!("{message}");
}
