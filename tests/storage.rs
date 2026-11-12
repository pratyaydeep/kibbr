// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What the storage format is observable as, from outside the process, and what the *next*
//! process sees of it:
//!
//! - **Row groups.** Is a row in the answer before it is on disk, does a schema change cost
//!   anything, does a query say what it pruned, and does a data directory from an earlier
//!   layout stop the server.
//! - **Restarts.** Every test ingests under one server, stops it, and starts another on the
//!   same directory. None of it is observable from inside a single process - a store that
//!   never reopened its files can hold the right answer in memory.
//! - **Other builds.** What this build makes of a data directory another build wrote.
//!   `tests/data/compat/ae7d8ca/data` is a data directory a kibbr built from commit ae7d8ca
//!   left behind - before a time column read its own forms and before an epoch and an
//!   integer made a column of integers: row groups from a graceful stop, and lines that are
//!   only in its write-ahead log because the server was killed. `expected.json` is what that
//!   same build answered on it, after replaying its own log (`make.py`, beside them, wrote
//!   both). Every test starts this build on a copy. `tests/data/compat/ca6d867` is the same
//!   of commit ca6d867, the last build that stored an array as the string of its JSON text
//!   and gave an empty object no key: lines that hold arrays and empty objects, stored and
//!   logged.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::harness::*;

/// Lines in the restart tests' ingest body. No corruptions: what a bad line does is
/// `serve.rs`'s subject, and here it would only make the expected counts arithmetic.
const LINES: usize = CORPUS_LINES;

impl Db {
    /// Every data file under the data directory, in path order. By its extension: the
    /// write-ahead log's segments are `seg_` files too, and are not what a row group is in.
    fn segments(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![self.base()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if entry.file_name().to_string_lossy().starts_with("seg_")
                    && path.extension().is_some_and(|e| e == "col")
                {
                    out.push(path);
                }
            }
        }
        out.sort();
        out
    }

    fn trie_json(&self) -> PathBuf {
        self.base().join("trie.json")
    }

    fn catalog(&self) -> PathBuf {
        self.base().join("catalog.log")
    }
}

// ══ Row groups ══════════════════════════════════════════════════════════════════════

/// `count` lines of one shape, so they land in one template and one day partition.
fn one_template(count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..count {
        let second = i % 3600;
        let line = format!(
            "{{\"ts\":\"2024-01-15T{:02}:{:02}:{:02}Z\",\"level\":\"info\",\"service\":\"auth\",\
             \"region\":\"eu\",\"host\":\"h{}\",\"path\":\"/v1/resource\",\"bytes\":{},\"n\":{}}}\n",
            10 + i / 3600,
            second / 60,
            second % 60,
            i % 8,
            1000 + i,
            i
        );
        out.extend_from_slice(line.as_bytes());
    }
    out
}

// ── Visibility ─────────────────────────────────────────────────────────────────────

/// One line is nowhere near any row-group threshold and still has to be in the answer
/// immediately: the thresholds say when bytes reach the disk, not when a row exists.
#[test]
fn one_line_is_queryable_before_any_row_group_closes() {
    let db = Db::new();
    let server = db.start();

    let report = server.ingest(b"{\"ts\":\"2024-01-15T10:00:00Z\",\"event\":\"one\"}\n");
    assert_eq!(report["ingested"], 1);

    let answer = server.query(r#"MATCH LOG { "event": "one" }"#);
    assert_eq!(answer["count"], 1, "the row was not visible: {answer}");
    assert_eq!(answer["rows"][0]["event"], "one");

    // Visible, and in no data file: in the write-ahead log and in memory. `/stats` says
    // so, because that is what the next start would have to put back.
    assert!(db.segments().is_empty(), "a row group closed for one row");
    assert_eq!(server.stats()["storage"]["buffered_rows"], 1);
    assert_eq!(server.stats()["storage"]["wal"]["last_seq"], 1);
}

// ── A schema change ────────────────────────────────────────────────────────────────

/// A row group records the type each of its columns actually used, so a column that
/// widens costs no file, and each row keeps the type and the spelling it arrived with.
#[test]
fn a_widening_column_costs_no_file_and_each_row_keeps_its_type() {
    let db = Db::new();
    let first = db.start();

    // One key set, so one template - and two types in the `code` column.
    first.ingest(
        b"{\"ts\":\"2024-01-15T10:00:00Z\",\"event\":\"w\",\"code\":7}\n\
          {\"ts\":\"2024-01-15T10:00:01Z\",\"event\":\"w\",\"code\":8}\n",
    );
    first.ingest(b"{\"ts\":\"2024-01-15T10:00:02Z\",\"event\":\"w\",\"code\":\"N/A\"}\n");
    first.shutdown();

    assert_eq!(
        db.segments().len(),
        1,
        "one day's rows are in more than one file: {:?}",
        db.segments()
    );

    let second = db.start();
    let rows = second.query(r#"MATCH LOG { "event": "w" } LIMIT 10"#);
    assert_eq!(rows["count"], 3, "a row was lost: {rows}");

    // Newest first, so the string is row 0 and the two integers follow it.
    let codes: Vec<String> = rows["rows"]
        .as_array()
        .expect("rows is an array")
        .iter()
        .map(|row| row["code"].to_string())
        .collect();
    assert_eq!(codes, vec!["\"N/A\"", "8", "7"], "types did not survive");

    assert_eq!(second.query(r#"COUNT LOG { "code": "7" }"#)["count"], 1);
    assert_eq!(second.query(r#"COUNT LOG { "code": "N/A" }"#)["count"], 1);
}

// ── Pruning, as the answer reports it ──────────────────────────────────────────────

/// A window that covers one row group reads one, and says so in `meta`: a prune nobody
/// can see is a prune nobody notices stopping.
#[test]
fn a_window_over_one_row_group_reads_one_and_says_so() {
    let db = Db::new();
    // No tick and no timeout while the body is applied: the shutdown's final flush is the
    // one close.
    let first = db.serve(&[
        "--flush-interval-ms",
        "3600000",
        "--row-group-timeout-ms",
        "3600000",
    ]);
    // 20,000 rows of one shape: two full row groups at 8,192 and a third holding the rest,
    // all in one day partition. The shutdown closes and seals them.
    first.ingest(&one_template(20_000));
    first.shutdown();

    let second = db.start();
    let whole = second.query("COUNT LOG {}");
    assert_eq!(whole["count"], 20_000);
    let groups = whole["meta"]["row_groups_read"]
        .as_u64()
        .expect("row_groups_read");
    assert_eq!(
        groups, 3,
        "one close of 20,000 rows did not make three row groups"
    );

    // The first row group holds the oldest 8,192 rows, which are the first 8,192 seconds
    // of the day: a window inside the last one must not read the other two.
    let last =
        second.query(r#"COUNT LOG {} BETWEEN "2024-01-15T14:40:00Z" AND "2024-01-15T15:40:00Z""#);
    assert_eq!(
        last["meta"]["row_groups_read"], 1,
        "a window inside one row group read more: {}",
        last["meta"]
    );
    assert_eq!(last["meta"]["row_groups_pruned"], 2);
    assert!(
        last["count"].as_u64().is_some_and(|n| n > 0),
        "the window matched nothing, so the prune proves nothing"
    );
}

/// An answer with how long it took left out: everything else in it is the same from one
/// asking to the next, and from one server on a directory to the next.
fn but_the_duration(mut answer: serde_json::Value) -> serde_json::Value {
    let taken = answer["meta"]
        .as_object_mut()
        .and_then(|meta| meta.remove("duration_ms"));
    assert!(taken.is_some(), "an answer with no duration: {answer}");
    answer
}

/// What the machine reports as its cores, as the server counts them.
fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// **A query answers the same whether its scan is shared between threads or not**, and
/// `/stats` says how many runs were shared out and what each server resolved.
#[test]
fn a_scan_shared_between_threads_answers_what_one_thread_answers() {
    let db = Db::new();
    // One close, at the shutdown: two full row groups at 8,192 rows and a third.
    let first = db.serve(&[
        "--flush-interval-ms",
        "3600000",
        "--row-group-timeout-ms",
        "3600000",
    ]);
    first.ingest(&one_template(20_000));
    first.shutdown();

    let asked = |extra: &[&str]| -> (Vec<serde_json::Value>, u64) {
        let said = |server: &Server, key: &str| -> u64 {
            server.stats()["queries"][key]
                .as_u64()
                .unwrap_or_else(|| panic!("no queries.{key} in /stats"))
        };
        let server = db.serve(extra);
        let mut answers: Vec<serde_json::Value> = [
            r#"COUNT LOG {"host":"h3"}"#,
            // The first row group ends at 12:16:31, before this, and is pruned.
            r#"COUNT LOG {} BETWEEN "2024-01-15T12:30:00Z" AND "2024-01-15T15:40:00Z""#,
            r#"COUNT LOG {"level":"info"} GROUP BY time(1h), "host""#,
        ]
        .iter()
        .map(|q| but_the_duration(server.query(q)))
        .collect();
        let mut cursor: Option<String> = None;
        for page in 0.. {
            assert!(page < 10, "a fetch that never ends");
            let after = cursor.map_or(String::new(), |c| format!(r#" AFTER "{c}""#));
            let answer = server.query(&format!(r#"MATCH LOG {{"host":"h3"}} LIMIT 900{after}"#));
            cursor = answer["meta"]["next_cursor"].as_str().map(str::to_string);
            answers.push(but_the_duration(answer));
            if cursor.is_none() {
                break;
            }
        }
        let shared_out = server.stats()["queries"]["scan_fan_outs"]
            .as_u64()
            .expect("queries.scan_fan_outs");
        let (threads, fan_rows) = (
            said(&server, "scan_threads"),
            said(&server, "scan_fan_rows"),
        );
        // What was asked for, where anything was; and otherwise the default, unless the
        // environment this test runs in sets it.
        let flag = |name: &str| -> Option<u64> {
            let at = extra.iter().position(|arg| *arg == name)?;
            extra[at + 1].parse().ok()
        };
        let set = |var: &str| std::env::var(var).ok().and_then(|v| v.trim().parse().ok());
        let default = ((cores() / 2).clamp(1, 4)) as u64;
        let want_threads = flag("--scan-threads")
            .or_else(|| set("KIBBR__QUERY__SCAN_THREADS"))
            .unwrap_or(default);
        let want_rows = flag("--scan-fan-rows")
            .or_else(|| set("KIBBR__QUERY__SCAN_FAN_ROWS"))
            .unwrap_or(32_768);
        // None and one are both the query's own thread and no other.
        assert_eq!(threads, want_threads.max(1), "{extra:?}");
        assert_eq!(fan_rows, want_rows, "{extra:?}");
        // The banner's line is past what `start_with` reads of it: it is in the rest.
        let mut banner = server.banner.clone();
        banner.push_str(&server.shutdown());
        let line = if threads == 1 {
            "  scan        1 thread to a query - no scan is shared out".to_string()
        } else {
            format!("  scan        {threads} threads to a query, its own among them")
        };
        assert!(banner.contains(&line), "no `{line}` in:\n{banner}");
        (answers, shared_out)
    };

    let (plain, _) = asked(&[]);
    assert_eq!(plain[0]["count"], 2_500);
    assert_eq!(plain[1]["meta"]["row_groups_pruned"], 1, "{}", plain[1]);
    assert_eq!(plain[1]["meta"]["row_groups_read"], 2, "{}", plain[1]);
    assert!(plain[2]["groups"].as_array().is_some_and(|g| g.len() > 8));
    // 2,500 rows at 900 a page.
    assert_eq!(plain.len(), 3 + 3);
    assert_eq!(plain[3]["rows"].as_array().map(Vec::len), Some(900));

    let (alone, shared_out) = asked(&["--scan-threads", "1"]);
    assert_eq!(
        shared_out, 0,
        "a server with one scan thread shared a scan out"
    );
    assert_eq!(alone, plain);

    let threads = cores().min(4);
    if threads < 2 {
        return;
    }
    let (together, shared_out) = asked(&[
        "--scan-threads",
        &threads.to_string(),
        "--scan-fan-rows",
        "0",
    ]);
    // Each of the six questions has a run of two or three row groups to share out.
    assert_eq!(shared_out, 6, "runs of row groups shared out");
    assert_eq!(together, plain);
}

/// More scan threads than the machine has cores would only take turns on the same cores:
/// the server says so and does not start, by the flag and by the variable.
#[test]
fn more_scan_threads_than_the_machine_has_cores_are_refused_at_the_start() {
    let db = Db::new();
    let many = (cores() + 1).to_string();
    let refused = |flag: bool| {
        let mut command = kibbr();
        command
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &db.base().display().to_string()]);
        if flag {
            command.args(["--scan-threads", &many]);
        } else {
            command.env("KIBBR__QUERY__SCAN_THREADS", &many);
        }
        let out = command.output().expect("failed to run kibbr serve");
        let said = String::from_utf8_lossy(&out.stderr).into_owned();
        assert_eq!(out.status.code(), Some(1), "{said}");
        let want = format!(
            "scan threads ({many}) is above the {} core(s) this machine has",
            cores()
        );
        assert!(said.contains(&want), "{said}");
        assert!(!said.contains("listening"), "{said}");
    };
    refused(true);
    refused(false);
}

/// The row groups a query reads, and the rows still buffered: what the tick has closed so
/// far, as a client sees it.
fn closed(server: &Server) -> (u64, u64) {
    let whole = server.query("COUNT LOG {}");
    assert_eq!(whole["count"], 20_000, "a row went missing: {whole}");
    let groups = whole["meta"]["row_groups_read"]
        .as_u64()
        .expect("row_groups_read");
    let buffered = server.stats()["storage"]["buffered_rows"]
        .as_u64()
        .expect("buffered_rows");
    (groups, buffered)
}

/// Poll `closed` until it says `want`, or give up after `within` and return what it last
/// said.
fn closed_by(server: &Server, want: (u64, u64), within: std::time::Duration) -> (u64, u64) {
    let deadline = std::time::Instant::now() + within;
    loop {
        let now = closed(server);
        if now == want || std::time::Instant::now() >= deadline {
            return now;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// A tick closes as many full row groups as a buffer holds and no more: the rows past the
/// last full one wait to fill a row group of their own or for the timeout.
#[test]
fn a_tick_closes_only_full_row_groups_and_the_rest_waits_for_the_timeout() {
    // A tick every millisecond, landing all through the body, and a timeout that never
    // comes: only the 8,192-row threshold may close anything.
    let db = Db::new();
    let server = db.serve(&[
        "--flush-interval-ms",
        "1",
        "--row-group-timeout-ms",
        "3600000",
    ]);
    server.ingest(&one_template(20_000));
    let ticked = closed_by(&server, (2, 3_616), std::time::Duration::from_secs(10));
    // Hundreds of ticks more, none of which may close the rest.
    std::thread::sleep(std::time::Duration::from_millis(300));
    let settled = closed(&server);
    assert_eq!(
        (ticked, settled),
        ((2, 3_616), (2, 3_616)),
        "the ticks did not close exactly two full row groups and leave 20,000 - 2 x 8,192 \
         buffered"
    );
    drop(server);

    // The same, with a timeout that does come: it closes the rest as the third.
    let db = Db::new();
    let server = db.serve(&[
        "--flush-interval-ms",
        "1",
        "--row-group-timeout-ms",
        "15000",
    ]);
    // The rest's oldest row arrives after this, so what is seen before 15s from it is seen
    // before the timeout.
    let started = std::time::Instant::now();
    server.ingest(&one_template(20_000));
    let timeout = std::time::Duration::from_secs(15);
    let ticked = closed_by(
        &server,
        (2, 3_616),
        timeout.saturating_sub(started.elapsed()),
    );
    assert!(
        started.elapsed() < timeout,
        "too slow to tell the ticks from the timeout"
    );
    assert_eq!(
        ticked,
        (2, 3_616),
        "the ticks did not close exactly the full row groups"
    );
    let timed_out = closed_by(&server, (3, 0), std::time::Duration::from_secs(20));
    assert_eq!(
        timed_out,
        (3, 0),
        "the timeout did not close the rest as one row group"
    );
}

/// Seven columns of one template whose every value is new, so no encoding makes one column
/// much smaller than another, and a time column beside them.
fn seven_even_columns(count: usize) -> Vec<u8> {
    let mix = |mut z: u64| {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let mut out = Vec::new();
    for i in 0..count {
        let second = i % 3600;
        let mut line = format!(
            "{{\"ts\":\"2024-01-15T{:02}:{:02}:{:02}Z\"",
            10 + i / 3600,
            second / 60,
            second % 60
        );
        for column in 1..=7u64 {
            let value = mix((i as u64) << 3 | column);
            line.push_str(&format!(",\"k{column}\":\"x{value:016x}\""));
        }
        line.push_str("}\n");
        out.extend_from_slice(line.as_bytes());
    }
    out
}

/// Naming one column of eight reads its chunks and the timestamp's, nothing else, and
/// `bytes_read` is the only place that is visible.
#[test]
fn naming_one_column_reads_far_fewer_bytes_than_the_whole_row() {
    let db = Db::new();
    let first = db.start();
    let lines = seven_even_columns(20_000);
    first.ingest(&lines);
    first.shutdown();

    // Row 5's value of k3, read back off the line it is in.
    let line = lines.split(|b| *b == b'\n').nth(5).expect("a sixth line");
    let k3 = json(std::str::from_utf8(line).expect("UTF-8"))["k3"].clone();
    let second = db.start();
    let counted = second.query(&format!(r#"COUNT LOG {{ "k3": {k3} }}"#));
    let whole = second.query("MATCH LOG {} LIMIT 1");

    let narrow = counted["meta"]["bytes_read"].as_u64().expect("bytes_read");
    let wide = whole["meta"]["bytes_read"].as_u64().expect("bytes_read");
    assert!(narrow > 0, "a count that matched rows read no bytes");
    assert_eq!(counted["count"], 1);
    assert!(
        narrow * 3 < wide,
        "naming one column of eight read {narrow} bytes against {wide} for the whole row"
    );
}

// ── The layout this replaces ───────────────────────────────────────────────────────

/// A data directory from before format v3 - a directory per template - is refused by path
/// with a non-zero exit: there is no migration, and answering `count: 0` over data is worse.
#[test]
fn a_per_template_data_directory_is_refused_by_path() {
    let db = Db::new();
    let day = db.base().join("leaf_101").join("2024-01-15");
    fs::create_dir_all(&day).expect("create the day directory");
    fs::write(day.join("seg_000001.col"), b"KBBR\x02\x00\x00\x00").expect("write a v2 segment");

    let out = kibbr()
        .args(["serve", "--port", "0"])
        .args(["--data-dir", &db.base().display().to_string()])
        .output()
        .expect("failed to run kibbr serve");

    assert!(
        !out.status.success(),
        "a per-template data directory started anyway"
    );
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("before format v3"),
        "the error does not say what it found:\n{text}"
    );
    assert!(
        text.contains("leaf_101"),
        "the error does not name the path:\n{text}"
    );
}

/// A data file of another format version, where a v3 one belongs, is refused by path
/// before anything is served.
#[test]
fn a_data_file_of_another_version_is_refused_by_path() {
    let db = Db::new();
    let day = db.base().join("data").join("2024-01-15");
    fs::create_dir_all(&day).expect("create the day directory");
    fs::write(day.join("seg_0_000001.col"), b"KBBR\x02\x00\x00\x00").expect("write");

    let out = kibbr()
        .args(["serve", "--port", "0"])
        .args(["--data-dir", &db.base().display().to_string()])
        .output()
        .expect("failed to run kibbr serve");

    assert!(
        !out.status.success(),
        "a version 2 data file started anyway"
    );
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("version 2") && text.contains("seg_0_000001.col"),
        "the error does not name the file and its version:\n{text}"
    );
}

/// **A data file of another version stops the start before the listener opens, and the
/// same file at this version starts clean** (was `scripts/fault/bad_version.sh`): byte 4
/// of a data file a server wrote is set to 2, and a start with its port already held by
/// another socket exits non-zero naming the file - so the check ran before the bind, or
/// the bind's own error would have come first. Set back to 3, the server starts and every
/// row is there.
#[test]
fn a_data_file_of_another_version_stops_the_start_before_the_listener() {
    use std::io::{Seek, SeekFrom, Write};

    let db = Db::new();
    let first = db.start();
    first.ingest(&hostile(100, 42));
    let before = first.count("COUNT LOG {}");
    first.shutdown();
    let segment = db
        .segments()
        .into_iter()
        .next()
        .expect("no data file was written");
    let flip = |byte: u8| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&segment)
            .expect("open the data file");
        file.seek(SeekFrom::Start(4)).expect("seek");
        file.write_all(&[byte]).expect("write the version byte");
    };

    flip(2);
    let held = std::net::TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let port = held.local_addr().expect("the held port").port().to_string();
    let mut child = kibbr()
        .args(["serve", "--port", &port, "--data-dir", &db.base_arg()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr serve");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while child.try_wait().expect("try_wait").is_none() {
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("still running 5s after starting on a version 2 data file");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let out = child.wait_with_output().expect("the exit");
    drop(held);
    let said = text(&out.stderr);
    assert!(
        !out.status.success(),
        "a version 2 data file started anyway:\n{said}"
    );
    assert!(
        said.contains(&segment.display().to_string()),
        "stderr does not name {}:\n{said}",
        segment.display()
    );
    let lower = said.to_lowercase();
    assert!(
        !lower.contains("cannot listen") && !lower.contains("in use"),
        "the start failed on its port before it read the data file:\n{said}"
    );

    flip(3);
    let restored = db.start();
    assert_eq!(restored.count("COUNT LOG {}"), before, "a row was lost");
    restored.shutdown();
}

/// A data file written by a pre-rename build - the old magic, a version this build does
/// read - is refused by path like another version, and says why.
#[test]
fn a_data_file_from_a_pre_rename_build_is_refused_by_path() {
    let db = Db::new();
    let day = db.base().join("data").join("2024-01-15");
    fs::create_dir_all(&day).expect("create the day directory");
    // The pre-rename magic, then version 3.
    fs::write(
        day.join("seg_0_000001.col"),
        b"\x54\x52\x44\x42\x03\x00\x00\x00",
    )
    .expect("write");

    let out = kibbr()
        .args(["serve", "--port", "0"])
        .args(["--data-dir", &db.base().display().to_string()])
        .output()
        .expect("failed to run kibbr serve");

    assert!(
        !out.status.success(),
        "a pre-rename data file started anyway"
    );
    let text = String::from_utf8_lossy(&out.stderr);
    assert!(
        text.contains("written by a pre-rename build") && text.contains("seg_0_000001.col"),
        "the error does not name the file and say what wrote it:\n{text}"
    );
}

// ── The banner ─────────────────────────────────────────────────────────────────────

/// What the operator is told a crash costs: nothing that was acknowledged. A banner
/// promising the flush interval would be describing something nothing implements.
#[test]
fn the_banner_states_the_row_group_window_and_not_the_flush_interval() {
    let db = Db::new();
    let server = db.serve(&["--row-group-timeout-ms", "4000"]);
    assert!(
        server.banner.contains("8,192") && server.banner.contains("4000ms"),
        "the banner does not state the row group thresholds:\n{}",
        server.banner
    );
    assert!(
        server.banner.contains("loses no")
            && server.banner.contains("acknowledged line")
            && !server.banner.contains("NO WRITE-AHEAD LOG"),
        "the banner does not say what a kill -9 costs:\n{}",
        server.banner
    );
    assert_eq!(server.stats()["storage"]["row_group_timeout_ms"], 4000);
    // One server to a data directory: the second would be refused its lock.
    drop(server);

    // Floored at the flush interval: the tick is what notices the timeout expired.
    let floored = db.serve(&["--row-group-timeout-ms", "10", "--flush-interval-ms", "700"]);
    assert_eq!(floored.stats()["storage"]["row_group_timeout_ms"], 700);
}

/// **No seal flapping, and no `mixed`** (was `scripts/check/date_forms.sh`): a template
/// whose date column alternates between RFC 3339 and a ten-digit epoch 1,000 times,
/// ingested in bodies small enough that many row groups close, ends with at most 2 data
/// files for its day; after a restart every row is inside a window over its day and
/// every `ts` comes back in the form it arrived in. Before a second date form turned the
/// column to text, every change of form sealed the file: 130 partitions became 34,892.
#[test]
fn a_date_column_of_two_forms_seals_once_and_keeps_each_form() {
    const LINES: usize = 1_000;
    const BODIES: usize = 20;
    let form = |i: usize| {
        if i.is_multiple_of(2) {
            "2024-01-15T10:30:00Z"
        } else {
            "1705314600"
        }
    };
    let db = Db::new();
    let quick = ["--flush-interval-ms", "50", "--row-group-timeout-ms", "50"];
    let first = db.serve(&quick);
    let per = LINES / BODIES;
    for b in 0..BODIES {
        let body: String = (b * per..(b + 1) * per)
            .map(|i| format!("{{\"event\":\"tick\",\"ts\":\"{}\",\"n\":{i}}}\n", form(i)))
            .collect();
        first.ingest(body.as_bytes());
        // Long enough for the row group to close before the next body arrives.
        std::thread::sleep(std::time::Duration::from_millis(150));
    }
    let log = first.banner.clone() + &first.shutdown();
    let files = db
        .segments()
        .iter()
        .filter(|path| path.parent().is_some_and(|p| p.ends_with("2024-01-15")))
        .count();
    assert!(files <= 2, "the alternating forms made {files} data files");
    assert!(!log.contains("mixed"), "a mixed chunk was written:\n{log}");

    let second = db.serve(&quick);
    let day =
        r#"COUNT LOG {"event":"tick"} BETWEEN "2024-01-15T00:00:00Z" AND "2024-01-16T00:00:00Z""#;
    assert_eq!(
        second.count(day),
        LINES as u64,
        "the window over the day lost rows"
    );
    let rows = second.query(r#"MATCH LOG {"event":"tick"} LIMIT 100000"#)["rows"].clone();
    let mut got: Vec<(u64, String)> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            (
                r["n"].as_u64().expect("n"),
                r["ts"].as_str().expect("ts").to_string(),
            )
        })
        .collect();
    got.sort();
    let want: Vec<(u64, String)> = (0..LINES)
        .map(|i| (i as u64, form(i).to_string()))
        .collect();
    assert_eq!(got, want, "a ts came back in a form it did not arrive in");
}

/// **Every row has a time** (was `scripts/check/time_columns.sh`): over 20,000 `--seed
/// 42` lines, `COUNT LOG {}` equals the count over a window wide enough to hold
/// everything, and no `undated` directory exists once the rows are on disk. When the time
/// column was the literal name `ts`, 148 of 870 templates had none, their rows went to
/// `undated`, and every time-range query skipped them.
#[test]
fn every_row_has_a_time_and_nothing_is_undated() {
    let db = Db::new();
    let server = db.start();
    let report = server.ingest(&hostile(20_000, 42));
    assert_eq!(report["ingested"], 20_000, "{report}");
    let all = server.count("COUNT LOG {}");
    let windowed =
        server.count(r#"COUNT LOG {} BETWEEN "2020-01-01T00:00:00Z" AND "2030-01-01T00:00:00Z""#);
    assert_eq!(all, windowed, "the window loses {} rows", all - windowed);
    // A clean stop writes every buffered row out, so the directory check sees the
    // partitions the rows were cut into rather than an empty leaf directory.
    server.shutdown();
    let mut stack = vec![db.base()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                assert_ne!(entry.file_name(), "undated", "{} exists", path.display());
                stack.push(path);
            }
        }
    }
}

// ══ Restarts ════════════════════════════════════════════════════════════════════════

/// The queries compared across a restart, chosen to read through different paths: an
/// answer that survived in one and not the others is a store that wrote some of its columns.
const BATTERY: [&str; 5] = [
    "COUNT LOG {}",
    "MATCH LOG {} LIMIT 100000",
    r#"MATCH LOG { "level": "error" } LIMIT 100000"#,
    r#"COUNT LOG {} GROUP BY "level""#,
    "COUNT LOG {} GROUP BY time(1h)",
];

/// Everything about an answer that a restart must not change. `meta` is excluded: its
/// duration and files opened rightly differ between the write buffer and flushed files.
fn answer(server: &Server, q: &str) -> String {
    server.answer(q).to_string()
}

// ── The rows come back ─────────────────────────────────────────────────────────────

/// Ingest, stop, start, ask again.
#[test]
fn a_restart_answers_every_query_identically() {
    let db = Db::new();

    let first = db.start();
    let report = first.ingest(&corpus());
    assert_eq!(report["ingested"], LINES as u64);
    let before: Vec<String> = BATTERY.iter().map(|q| answer(&first, q)).collect();
    // A corpus that answered nothing would make every assertion below vacuous.
    assert_eq!(first.query("COUNT LOG {}")["count"], LINES as u64);
    let templates = json(&first.get("/templates").1)["count"].clone();
    assert!(templates.as_u64().unwrap_or(0) > 0, "no templates learned");

    let log = first.shutdown();
    assert!(
        log.contains("trie.json - ") && !log.contains("WARN trie.json"),
        "the snapshot was not written on a graceful exit:\n{log}"
    );

    let second = db.start();
    assert!(
        second.banner.contains("restored"),
        "the restart did not restore the snapshot:\n{}",
        second.banner
    );
    assert_eq!(
        json(&second.get("/templates").1)["count"],
        templates,
        "the restart learned a different number of templates"
    );

    let after: Vec<String> = BATTERY.iter().map(|q| answer(&second, q)).collect();
    for (q, (before, after)) in BATTERY.iter().zip(before.iter().zip(after.iter())) {
        assert_eq!(before, after, "`{q}` answered differently after a restart");
    }
}

// ── A damaged snapshot ─────────────────────────────────────────────────────────────

/// `trie.json` is a snapshot nothing depends on: deleted or truncated, with a catalog
/// beside it the next start warns about nothing and answers every query as before.
#[test]
fn a_damaged_or_missing_trie_json_changes_no_answer_when_a_catalog_exists() {
    let db = Db::new();

    let first = db.start();
    first.ingest(&corpus());
    first.shutdown();
    let before: Vec<String> = BATTERY.iter().map(|q| answer(&db.start(), q)).collect();

    // Truncated, then absent. Two damages, one contract.
    let whole = fs::read(db.trie_json()).expect("no trie.json after a graceful exit");
    fs::write(db.trie_json(), &whole[..whole.len() / 2]).expect("failed to truncate");
    let second = db.start();
    assert!(
        !second.banner.contains("WARN"),
        "a truncated snapshot warned although the catalog exists:\n{}",
        second.banner
    );
    assert!(
        second.banner.contains("catalog.log - ") && second.banner.contains("restored"),
        "the catalog was not what the restart read:\n{}",
        second.banner
    );
    let truncated: Vec<String> = BATTERY.iter().map(|q| answer(&second, q)).collect();
    assert_eq!(before, truncated, "a truncated trie.json changed an answer");
    second.shutdown();

    fs::remove_file(db.trie_json()).expect("no trie.json to remove");
    let third = db.start();
    assert!(!third.banner.contains("WARN"), "{}", third.banner);
    let missing: Vec<String> = BATTERY.iter().map(|q| answer(&third, q)).collect();
    assert_eq!(before, missing, "a missing trie.json changed an answer");

    // And a clean exit puts the snapshot back, for whoever wants to read it.
    let log = third.shutdown();
    assert!(
        log.contains("trie.json - ") && !log.contains("WARN trie.json"),
        "the snapshot was not rewritten:\n{log}"
    );
    assert!(db.trie_json().exists());
}

/// No catalog and a truncated `trie.json`, as a pre-catalog directory is after power loss
/// mid-write: a WARN and an empty trie, never a crash loop an operator cannot fix.
#[test]
fn no_catalog_and_a_truncated_trie_json_warns_and_starts_clean() {
    let db = Db::new();

    let first = db.start();
    first.ingest(&corpus());
    first.shutdown();

    fs::remove_file(db.catalog()).expect("no catalog.log after a graceful exit");
    let whole = fs::read(db.trie_json()).expect("no trie.json after a graceful exit");
    assert!(whole.len() > 40, "trie.json is too small to truncate");
    fs::write(db.trie_json(), &whole[..whole.len() / 2]).expect("failed to truncate");

    let second = db.start();
    assert!(
        second.banner.contains("WARN trie.json"),
        "a truncated snapshot did not warn:\n{}",
        second.banner
    );
    assert!(
        second.banner.contains("starting with an empty trie"),
        "the warning does not say what it did instead:\n{}",
        second.banner
    );

    // Started, not merely warned: it is serving, and `status: "ok"` because a lost snapshot
    // is not a store that cannot write.
    let (status, body) = second.get("/health");
    assert_eq!(status, 200);
    let health = json(&body);
    assert_eq!(
        (&health["ok"], &health["status"]),
        (&true.into(), &"ok".into())
    );
    assert!(health.get("detail").is_none(), "{health}");
    assert_eq!(json(&second.get("/templates").1)["count"], 0);

    // The leaf directories the lost snapshot described are still on disk, and the server
    // says so rather than leaving an operator to find them with `du`.
    assert!(
        second.banner.contains("WARN storage -") && second.banner.contains("no template"),
        "the orphaned leaf directories were not reported:\n{}",
        second.banner
    );

    // And it relearns. The rows ingested now are queryable, so the empty start is a
    // recovery and not a server that is up but permanently useless.
    let report = second.ingest(&corpus());
    assert_eq!(report["ingested"], LINES as u64);
    // `LINES`, not twice it: the relearned templates take fresh ids and so fresh
    // directories, leaving the orphans unread.
    assert_eq!(second.query("COUNT LOG {}")["count"], LINES as u64);

    // The next start has a catalog to read: the damage does not persist. The orphan
    // warning does, and should - those directories are still there.
    second.shutdown();
    let third = db.start();
    assert!(
        !third.banner.contains("WARN trie.json") && !third.banner.contains("WARN catalog.log"),
        "the damage survived a clean shutdown:\n{}",
        third.banner
    );
    assert_eq!(third.query("COUNT LOG {}")["count"], LINES as u64);
}

/// A data directory from before the catalog, `trie.json` and no `catalog.log`: the first
/// start writes the catalog from the snapshot, once, and the second needs no snapshot.
#[test]
fn a_trie_json_with_no_catalog_is_migrated_once() {
    let db = Db::new();

    let first = db.start();
    first.ingest(&corpus());
    first.shutdown();
    let before: Vec<String> = BATTERY.iter().map(|q| answer(&db.start(), q)).collect();
    let ids_before = template_ids(&db.start());

    fs::remove_file(db.catalog()).expect("no catalog.log after a graceful exit");

    let second = db.start();
    assert!(
        second.banner.contains("catalog.log - none found; wrote"),
        "the migration was not logged:\n{}",
        second.banner
    );
    assert!(!second.banner.contains("WARN"), "{}", second.banner);
    assert!(db.catalog().exists(), "no catalog was written");
    assert_eq!(
        template_ids(&second),
        ids_before,
        "migration changed the template ids"
    );
    let migrated: Vec<String> = BATTERY.iter().map(|q| answer(&second, q)).collect();
    assert_eq!(before, migrated);
    second.shutdown();

    // From the catalog now, and the snapshot is not consulted: remove it to prove it.
    fs::remove_file(db.trie_json()).expect("no trie.json to remove");
    let third = db.start();
    assert!(
        third.banner.contains("catalog.log - ") && third.banner.contains("restored"),
        "{}",
        third.banner
    );
    assert!(
        !third.banner.contains("wrote"),
        "migrated twice:\n{}",
        third.banner
    );
    let again: Vec<String> = BATTERY.iter().map(|q| answer(&third, q)).collect();
    assert_eq!(before, again);
}

/// `Uuid` and `Ip` exist only in the catalog: the segment type byte has no value for them.
#[test]
fn uuid_and_ip_column_types_survive_a_restart() {
    let db = Db::new();
    let line: &[u8] = b"{\"ts\":\"2024-01-15T10:00:00Z\",\"event\":\"login\",\
                       \"session\":\"123e4567-e89b-12d3-a456-426614174000\",\
                       \"client\":\"203.0.113.9\",\"note\":null}\n";

    let first = db.start();
    assert_eq!(first.ingest(line)["ingested"], 1);
    let before = column_types(&first);
    assert_eq!(before["session"], "Uuid", "{before:?}");
    assert_eq!(before["client"], "Ip", "{before:?}");
    assert_eq!(before["ts"], "Datetime", "{before:?}");
    // Only ever null: no type yet, and that is a different fact from `String`.
    assert_eq!(before["note"], serde_json::Value::Null, "{before:?}");
    first.shutdown();

    let second = db.start();
    assert_eq!(
        column_types(&second),
        before,
        "a column type changed across the restart"
    );
}

/// `{name: type}` over every column of every template, as `/templates` reports them.
fn column_types(server: &Server) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for template in json(&server.get("/templates").1)["templates"]
        .as_array()
        .expect("templates is an array")
    {
        for col in template["columns"].as_array().expect("columns is an array") {
            out.insert(
                col["name"]
                    .as_str()
                    .expect("a column has a name")
                    .to_string(),
                col["type"].clone(),
            );
        }
    }
    out
}

// ── A cursor across a restart ──────────────────────────────────────────────────────

/// Cursors are **durable**, not session-scoped: a cursor must resolve to the same row in the
/// next process, since one resolving *nearly* right silently skips or repeats rows.
#[test]
fn a_cursor_issued_before_a_restart_resolves_identically_after_one() {
    let db = Db::new();

    let first = db.serve(&["--max-rows", "50"]);
    first.ingest(&corpus());

    let page = first.query("MATCH LOG {}");
    assert_eq!(page["meta"]["truncated"], true);
    let cursor = page["meta"]["next_cursor"]
        .as_str()
        .expect("a backstopped page carried no cursor")
        .to_string();

    // Paged to completion rather than one page: a cursor that resolves to the right row and
    // then reads the partitions in a different order is still wrong.
    let uninterrupted = page_to_completion(&first, &cursor);
    assert!(!uninterrupted.is_empty(), "the cursor resumed nothing");

    first.shutdown();
    let second = db.serve(&["--max-rows", "50"]);

    let resumed = page_to_completion(&second, &cursor);
    assert_eq!(
        resumed, uninterrupted,
        "the same cursor named a different answer after a restart"
    );

    // Paging that agrees with itself but not with the unpaged query is agreement on a wrong
    // answer.
    let mut whole_by_paging = rows_as_text(&page);
    whole_by_paging.extend(resumed);
    assert_eq!(
        whole_by_paging,
        rows_as_text(&second.query("MATCH LOG {} LIMIT 100000")),
        "paging across a restart did not reconstruct the whole answer"
    );
}

/// Pages from `cursor` to the end, returning the rows in the order they were handed out.
fn page_to_completion(server: &Server, cursor: &str) -> Vec<String> {
    let mut rows = Vec::new();
    let mut cursor = Some(cursor.to_string());
    for _ in 0..40 {
        let Some(c) = cursor.take() else { break };
        let page = server.query(&format!(r#"MATCH LOG {{}} AFTER "{c}""#));
        rows.extend(rows_as_text(&page));
        cursor = page["meta"]["next_cursor"].as_str().map(str::to_string);
    }
    assert_eq!(cursor, None, "paging did not finish");
    rows
}

/// A cursor is still read against the query that issued it, restart or not: durability
/// widens what a cursor may outlive, not what it may be pointed at.
#[test]
fn a_cursor_read_against_another_query_is_refused_after_a_restart() {
    let db = Db::new();

    let first = db.serve(&["--max-rows", "50"]);
    first.ingest(&corpus());
    let cursor = first.query("MATCH LOG {}")["meta"]["next_cursor"]
        .as_str()
        .expect("a backstopped page carried no cursor")
        .to_string();
    first.shutdown();

    let second = db.serve(&["--max-rows", "50"]);
    let (status, body) = second.query_raw(&format!(
        r#"MATCH LOG {{ "level": "error" }} AFTER "{cursor}""#
    ));
    assert_eq!(
        status, 400,
        "a cursor from another query was accepted after a restart: {body}"
    );
}

/// A key holding `=` is the same template after a restart as before it: the trie's edge
/// is escaped (`k\=v`) and the catalog holds the key, so replay must escape it again.
#[test]
fn a_key_holding_an_equals_sign_is_the_same_template_after_a_restart() {
    const LINES: &[u8] = concat!(
        r#"{"event=login":1,"ts":"2024-01-15T10:00:01Z"}"#,
        "\n",
        r#"{"event":"login","ts":"2024-01-15T10:00:02Z"}"#,
        "\n",
        r#"{"k=v":1,"ts":"2024-01-15T10:00:03Z"}"#,
        "\n",
        r#"{"a=b":{"event":"login"},"ts":"2024-01-15T10:00:04Z"}"#,
        "\n",
    )
    .as_bytes();
    const QUERIES: [&str; 5] = [
        "MATCH LOG {}",
        r#"COUNT LOG {"event":"login"}"#,
        r#"COUNT LOG {"event=login":*}"#,
        r#"COUNT LOG {"k=v":"1"}"#,
        r#"COUNT LOG {"a=b.event":"login"}"#,
    ];
    let db = Db::new();

    let first = db.start();
    first.ingest(LINES);
    let ids = template_ids(&first);
    assert_eq!(ids.len(), 4, "four shapes, four templates");
    let before: Vec<String> = QUERIES.iter().map(|q| answer(&first, q)).collect();
    first.shutdown();

    // The same lines again after each of two restarts: they find the templates they
    // made, and every row ingested so far is still counted.
    for round in 2..=3u64 {
        let server = db.start();
        assert_eq!(template_ids(&server), ids, "start {round}");
        if round == 2 {
            let after: Vec<String> = QUERIES.iter().map(|q| answer(&server, q)).collect();
            assert_eq!(after, before, "the restart changed an answer");
        }
        server.ingest(LINES);
        assert_eq!(template_ids(&server), ids, "start {round}, after ingest");
        for q in &QUERIES[1..] {
            assert_eq!(server.query(q)["count"], round, "start {round}: {q}");
        }
        server.shutdown();
    }
}

// ── Ids a lost snapshot leaves spent ───────────────────────────────────────────────

/// A relearned template must never be handed an id a leaf directory already holds: two key
/// sets merged into one directory answer `MATCH LOG {}` right and a filtered query wrong.
#[test]
fn a_relearned_template_never_reuses_a_leaf_directory() {
    let db = Db::new();

    // Hand-written rather than generated: the mechanism is which key set is *seen first*,
    // and that is exactly what a seeded generator holds fixed.
    let alpha_then_beta: &[u8] = b"{\"ts\":\"2024-01-15T10:00:00Z\",\"alpha\":\"a1\"}\n\
                                   {\"ts\":\"2024-01-15T10:00:01Z\",\"beta\":42}\n";
    let beta_then_alpha: &[u8] = b"{\"ts\":\"2024-01-15T10:00:02Z\",\"beta\":43}\n\
                                   {\"ts\":\"2024-01-15T10:00:03Z\",\"alpha\":\"a2\"}\n";

    let first = db.start();
    assert_eq!(first.ingest(alpha_then_beta)["ingested"], 2);
    let before: Vec<u64> = template_ids(&first);
    assert_eq!(before.len(), 2, "the two shapes did not make two templates");
    first.shutdown();

    // Both files: with either one the templates come back and nothing is relearned.
    fs::remove_file(db.trie_json()).expect("no trie.json to remove");
    fs::remove_file(db.catalog()).expect("no catalog.log to remove");

    let second = db.start();
    assert_eq!(second.ingest(beta_then_alpha)["ingested"], 2);

    // Fresh ids: nothing relearned may land on a directory that the templates from before
    // the loss are still using.
    let after = template_ids(&second);
    assert_eq!(after.len(), 2);
    for id in &after {
        assert!(
            !before.contains(id),
            "relearned template {id} reused an id from before the snapshot was lost \
             (before {before:?}, after {after:?})"
        );
    }

    // The filtered query, where reuse goes quiet: `a2` was ingested by this server and must
    // be found by a query that names its column.
    let hit = second.query(r#"MATCH LOG { "alpha": "a2" } LIMIT 100"#);
    assert_eq!(hit["count"], 1, "a filtered query missed a row it ingested");

    // And the unfiltered one agrees with it: two rows, this server's, with neither of the
    // orphaned leaves read back in.
    let all = second.query("MATCH LOG {} LIMIT 100");
    assert_eq!(all["count"], 2, "rows from the orphaned leaves leaked in");

    // Each row still carries its own key set. A directory holding two of them shows up
    // here as a row with the other shape's column on it, present and null.
    for row in all["rows"].as_array().expect("rows is an array") {
        let row = row.as_object().expect("a row is an object");
        assert!(
            row.contains_key("alpha") != row.contains_key("beta"),
            "a row carries both key sets, so two templates share a directory: {row:?}"
        );
    }
}

/// The template ids the server currently knows, ascending.
fn template_ids(server: &Server) -> Vec<u64> {
    let mut ids: Vec<u64> = json(&server.get("/templates").1)["templates"]
        .as_array()
        .expect("templates is an array")
        .iter()
        .map(|t| t["template_id"].as_u64().expect("a template has an id"))
        .collect();
    ids.sort_unstable();
    ids
}

// ══ Other builds ════════════════════════════════════════════════════════════════════

/// The fixture a kibbr built from ae7d8ca wrote, and the one from ca6d867.
const AE7D8CA: &str = "ae7d8ca";
const CA6D867: &str = "ca6d867";

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/compat")
        .join(name)
}

fn expected_of(name: &str) -> Value {
    let text = fs::read_to_string(fixture(name).join("expected.json")).expect("expected.json");
    serde_json::from_str(&text).expect("expected.json is JSON")
}

fn expected() -> Value {
    expected_of(AE7D8CA)
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create a directory");
    for entry in fs::read_dir(from).expect("read a directory") {
        let entry = entry.expect("a directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("a file type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("copy a file");
        }
    }
}

/// A copy of the fixture's data directory, removed when the test is done.
struct Copied {
    db: Db,
    fixture: &'static str,
}

impl Copied {
    fn new() -> Self {
        Self::of(AE7D8CA)
    }

    fn of(fixture_name: &'static str) -> Self {
        let db = Db::new();
        copy_dir(&fixture(fixture_name).join("data"), &db.base());
        Self {
            db,
            fixture: fixture_name,
        }
    }

    /// This build, on the directory, under the time columns the fixture was written with.
    fn start(&self) -> Server {
        let time_columns = expected_of(self.fixture)["time_columns"]
            .as_str()
            .expect("time_columns")
            .to_string();
        self.db.serve(&["--time-columns", &time_columns])
    }
}

impl Server {
    /// Every template as `make.py` kept it: its id, its time source, its row count and
    /// `{column: type}`.
    fn templates(&self) -> Value {
        let mut out: Vec<Value> = self.call("GET", "/templates", b"")["templates"]
            .as_array()
            .expect("templates is an array")
            .iter()
            .map(|t| {
                let columns: serde_json::Map<String, Value> = t["columns"]
                    .as_array()
                    .expect("columns is an array")
                    .iter()
                    .map(|c| {
                        let name = c["name"].as_str().expect("a column has a name");
                        (name.to_string(), c["type"].clone())
                    })
                    .collect();
                json!({
                    "template_id": t["template_id"],
                    "time_source": t["time_source"],
                    "rows": t["rows"],
                    "columns": columns,
                })
            })
            .collect();
        out.sort_by_key(|t| t["template_id"].as_u64());
        Value::Array(out)
    }

    /// `{column: type}` of one template.
    fn types(&self, template_id: u64) -> Value {
        self.templates()
            .as_array()
            .expect("an array")
            .iter()
            .find(|t| t["template_id"] == template_id)
            .unwrap_or_else(|| panic!("no template {template_id}"))["columns"]
            .clone()
    }
}

/// Every query `make.py` asked of ae7d8ca, asked of `server`, held to what ae7d8ca said.
fn assert_answers_as_ae7d8ca(server: &Server, when: &str) {
    let expected = expected();
    let queries = expected["queries"].as_object().expect("queries");
    assert!(queries.len() >= 30, "the fixture lost its queries");
    for (q, want) in queries {
        assert_eq!(
            &server.answer(q),
            want,
            "{when}, `{q}` is answered otherwise than ae7d8ca answered it"
        );
    }
}

// ── The tests ──────────────────────────────────────────────────────────────────────

/// **A data directory ae7d8ca wrote opens, and every query is answered as ae7d8ca
/// answered it**: rows, counts, groups and times, over the row groups it stored and the
/// lines this build replays from its log. A row whose time column holds a form only this
/// build reads stays at the arrival time it was filed under.
#[test]
fn a_data_directory_written_by_ae7d8ca_answers_every_query_as_ae7d8ca_did() {
    let db = Copied::new();
    let server = db.start();
    assert_answers_as_ae7d8ca(&server, "on the directory as ae7d8ca left it");
}

/// **Replaying ae7d8ca's log gives no stored column another type, and leaves no line
/// behind**: `/templates` says what it said under ae7d8ca - each column's type, each
/// template's time source and row count - and all seven logged lines are put back.
#[test]
fn replaying_the_log_ae7d8ca_wrote_changes_no_column_type_and_holds_no_line() {
    let db = Copied::new();
    let server = db.start();
    assert_eq!(
        server.templates(),
        expected()["templates"],
        "the templates after this build replayed ae7d8ca's log"
    );
    let replay = server.call("GET", "/stats", b"")["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["applied"], 7, "{replay}\n{}", server.banner);
    assert_eq!(replay["held"], 0, "{replay}\n{}", server.banner);
    assert!(
        !server.banner.contains("now reads as"),
        "a replayed line moved in time:\n{}",
        server.banner
    );
}

/// **The same again once this build has stored the replayed lines itself**: a graceful
/// stop writes them into row groups, and the next start - with nothing left to replay -
/// answers as ae7d8ca did and reports the same types.
#[test]
fn the_lines_replayed_from_ae7d8ca_are_stored_and_read_back_the_same() {
    let db = Copied::new();
    db.start().shutdown();
    let server = db.start();
    let replay = server.call("GET", "/stats", b"")["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["applied"], 0, "{replay}");
    assert_eq!(replay["held"], 0, "{replay}");
    assert_eq!(server.templates(), expected()["templates"]);
    assert_answers_as_ae7d8ca(&server, "after this build stored the replayed lines");
}

/// **What ae7d8ca decided stays decided; only new evidence is read the new way.** A
/// column it widened to `String` is still `String`, and a template it put on arrival time
/// stays there though this build would read its time column. A `Datetime` column of epoch
/// digits that now meets an integer, and an `Int64` column that now meets epoch digits,
/// are `Int64` - and the rows ae7d8ca stored come back as the digits they arrived as.
#[test]
fn new_lines_on_a_directory_from_ae7d8ca_widen_to_int64_and_keep_what_was_decided() {
    let db = Copied::new();
    let server = db.start();
    let before = server.types(105);
    assert_eq!(before["seen"], "Datetime");
    assert_eq!(before["count"], "Int64");

    server.ingest_lines(&[
        // `seen`, a Datetime of epoch digits, takes an integer.
        json!({"ts": "2024-01-15T11:00:03Z", "seen": 999999999, "count": 45}),
        // `count`, an Int64, takes ten digits inside the epoch window.
        json!({"ts": "2024-01-15T11:00:04Z", "seen": 1705316404u64, "count": 1705316404u64}),
        // Columns ae7d8ca made String take more of the same.
        json!({"ts": "2024-01-15T10:32:00Z", "level": "info", "msg": "i", "avail": 1705312320u64, "bytes": 7}),
        // A template on arrival time, whose time column this build reads: still arrival.
        json!({"timestamp": "1679711333.5", "channel": "OPS", "goalloc": 11}),
    ]);

    let after = server.types(105);
    assert_eq!(after["seen"], "Int64", "Datetime and an integer are Int64");
    assert_eq!(after["count"], "Int64", "Int64 and epoch digits are Int64");
    assert_eq!(after["ts"], "Datetime");
    let old = server.types(101);
    assert_eq!(old["avail"], "String", "a String column never narrows");
    assert_eq!(old["bytes"], "String", "a String column never narrows");

    // The rows of `seen`, old and new, are the integers they arrived as: found by their
    // digits, grouped by them, and printed as the numbers the lines held.
    let seen = server.answer(r#"COUNT LOG {} GROUP BY "seen""#);
    let mut labels: Vec<(String, u64)> = seen["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .map(|g| {
            let label = g["keys"]["seen"].as_str().expect("a label").to_string();
            (label, g["count"].as_u64().expect("a count"))
        })
        .filter(|(label, _)| label != "(null)")
        .collect();
    labels.sort();
    assert_eq!(
        labels,
        [
            ("1705316400".to_string(), 1),
            ("1705316401".to_string(), 1),
            ("1705316402".to_string(), 1),
            ("1705316404".to_string(), 1),
            ("999999999".to_string(), 1),
        ]
    );
    for (digits, row_group) in [
        ("1705316400", "stored by ae7d8ca"),
        ("1705316402", "replayed"),
    ] {
        let found = server.answer(&format!(r#"MATCH LOG {{ "seen": "{digits}" }} LIMIT 10"#));
        assert_eq!(found["count"], 1, "{digits}, {row_group}: {found}");
        assert_eq!(
            found["rows"][0]["seen"],
            digits.parse::<u64>().expect("digits"),
            "{digits}, {row_group}: the number the line held"
        );
    }
    let small = server.answer(r#"MATCH LOG { "seen": "999999999" } LIMIT 10"#);
    assert_eq!(small["count"], 1, "{small}");
    // The same instant written as a date is not that integer any more.
    let dated = server.answer(r#"MATCH LOG { "seen": "2024-01-15T11:00:00Z" } LIMIT 10"#);
    assert_eq!(dated["count"], 0, "{dated}");
    let counted = server.answer(r#"MATCH LOG { "count": "1705316404" } LIMIT 10"#);
    assert_eq!(counted["count"], 1, "{counted}");
    assert_eq!(counted["rows"][0]["count"], 1705316404u64);

    // Template 102 was made on arrival time, and a template keeps its time source.
    let templates = server.templates();
    let cockroach = templates
        .as_array()
        .expect("an array")
        .iter()
        .find(|t| t["template_id"] == 102)
        .expect("template 102");
    assert_eq!(cockroach["time_source"], "arrival");
    let row = server.answer(r#"MATCH LOG { "goalloc": "11" } LIMIT 10"#);
    assert!(
        row["rows"][0]["_received_at"].is_string(),
        "a row of a template on arrival time carries its arrival time: {row}"
    );
}

// ── ca6d867: arrays stored as strings, and empty objects with no key ───────────────

/// The rows of an answer with every array and object in them as the string of its JSON
/// text: how ca6d867 gave them.
fn as_text(answer: &Value) -> Value {
    let mut answer = answer.clone();
    for row in answer["rows"].as_array_mut().expect("rows") {
        for value in row.as_object_mut().expect("a row").values_mut() {
            if value.is_array() || value.is_object() {
                *value = Value::String(value.to_string());
            }
        }
    }
    answer
}

const STORED: &str = r#"MATCH LOG {} BETWEEN "2024-01-15T10:30:00Z" AND "2024-01-15T10:31:00Z" ORDER BY time ASC LIMIT 1000"#;
const LOGGED: &str = r#"MATCH LOG {} BETWEEN "2024-01-15T10:31:00Z" AND "2024-01-15T10:32:00Z" ORDER BY time ASC LIMIT 1000"#;

/// Every query `make.py` asked of ca6d867, held to what it said. The rows ca6d867 stored
/// are what they were, to the byte: an array is the string it was stored as, and a line
/// that held an empty object has no key for it. A line that was only in its log is read
/// by this build when it is put back, so an array in it is the array - the same row,
/// found, counted and grouped as before, and the same text once the array is printed.
fn assert_answers_as_ca6d867(server: &Server, when: &str) {
    let expected = expected_of(CA6D867);
    let queries = expected["queries"].as_object().expect("queries");
    assert!(queries.len() >= 29, "the fixture lost its queries");
    for (q, want) in queries {
        let got = server.answer(q);
        assert_eq!(
            &as_text(&got),
            want,
            "{when}, `{q}` is answered otherwise than ca6d867 answered it"
        );
    }
    assert_eq!(
        server.answer(STORED),
        queries[STORED],
        "{when}: a row ca6d867 stored is not what it was"
    );
    let stored = server.answer(STORED);
    for row in stored["rows"].as_array().expect("rows") {
        for (key, value) in row.as_object().expect("a row") {
            assert!(
                !value.is_array() && !value.is_object(),
                "{when}: `{key}` of a row ca6d867 stored came back as {value}"
            );
        }
    }
    // The four arrays ca6d867 only logged, as arrays; its three stored ones are above.
    let logged = server.answer(LOGGED);
    let arrays: Vec<&Value> = logged["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .flat_map(|row| row.as_object().expect("a row").values())
        .filter(|value| value.is_array())
        .collect();
    assert_eq!(
        arrays,
        [&json!(["late"]), &json!([1, 2]), &json!(["a"]), &json!([])],
        "{when}"
    );
}

/// **A data directory ca6d867 wrote answers every query as ca6d867 answered it.** Its
/// stored arrays stay the strings they were stored as; nothing is rewritten and no row
/// gains a key.
#[test]
fn a_data_directory_written_by_ca6d867_answers_every_query_as_ca6d867_did() {
    let db = Copied::of(CA6D867);
    let server = db.start();
    assert_answers_as_ca6d867(&server, "on the directory as ca6d867 left it");
}

/// **Replaying ca6d867's log gives no stored column another type, no template another
/// key, and leaves no line behind.** A logged line that held an empty object goes back
/// into the template ca6d867 filed it under, which has no key for one.
#[test]
fn replaying_the_log_ca6d867_wrote_changes_no_column_type_and_no_key_set() {
    let db = Copied::of(CA6D867);
    let server = db.start();
    assert_eq!(
        server.templates(),
        expected_of(CA6D867)["templates"],
        "the templates after this build replayed ca6d867's log"
    );
    let replay = server.call("GET", "/stats", b"")["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["applied"], 7, "{replay}\n{}", server.banner);
    assert_eq!(replay["held"], 0, "{replay}\n{}", server.banner);
}

/// **The same again once this build has stored the replayed lines itself**, and after it
/// has compacted the day: the rows ca6d867 stored are rebuilt as the strings they were,
/// and the arrays this build put back from the log stay arrays.
#[test]
fn the_lines_replayed_from_ca6d867_are_stored_compacted_and_read_back_the_same() {
    let db = Copied::of(CA6D867);
    db.start().shutdown();
    let server = db.start();
    let replay = server.call("GET", "/stats", b"")["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["applied"], 0, "{replay}");
    assert_eq!(replay["held"], 0, "{replay}");
    assert_eq!(server.templates(), expected_of(CA6D867)["templates"]);
    assert_answers_as_ca6d867(&server, "after this build stored the replayed lines");
    server.shutdown();

    let compacted = kibbr()
        .args(["compact", "--day", "2024-01-15", "--json"])
        .args(["--data-dir", &db.db.base().display().to_string()])
        .output()
        .expect("failed to run kibbr compact");
    let report = String::from_utf8_lossy(&compacted.stdout).into_owned();
    assert!(compacted.status.success(), "{report}");
    assert!(report.contains(r#""compacted":true"#), "{report}");
    let server = db.start();
    assert_eq!(server.templates(), expected_of(CA6D867)["templates"]);
    assert_answers_as_ca6d867(&server, "after this build compacted the day");
}

/// **What ca6d867 stored stays as it is; a new line is stored the new way.** A line with
/// an array and an empty object, in a shape ca6d867 already holds rows of, comes back
/// with both - under a template of its own where the empty object is a key - while the
/// old rows beside it, and the types of their columns, are what they were.
#[test]
fn new_lines_on_a_directory_from_ca6d867_keep_their_arrays_and_empty_objects() {
    let db = Copied::of(CA6D867);
    let server = db.start();
    let before = server.templates();
    server.ingest_lines(&[
        json!({"ts": "2024-01-15T10:40:00Z", "level": "info", "tags": ["new"], "attr": {"storage": {}}}),
        // The shape of template 101, with nothing that was left out of it: one more row there.
        json!({"ts": "2024-01-15T10:40:01Z", "level": "info", "tags": ["new", "again"]}),
        json!({"ts": "2024-01-15T10:40:02Z", "k": {}}),
    ]);
    let new = server.answer(
        r#"MATCH LOG {} BETWEEN "2024-01-15T10:40:00Z" AND "2024-01-15T10:41:00Z" ORDER BY time ASC LIMIT 10"#,
    );
    assert_eq!(
        new["rows"],
        json!([
            {"ts": "2024-01-15T10:40:00Z", "level": "info", "tags": ["new"], "attr.storage": {}},
            {"ts": "2024-01-15T10:40:01Z", "level": "info", "tags": ["new", "again"]},
            {"ts": "2024-01-15T10:40:02Z", "k": {}},
        ])
    );
    assert_eq!(
        server.answer(STORED),
        expected_of(CA6D867)["queries"][STORED],
        "the rows ca6d867 stored, beside the new ones"
    );

    // Every template ca6d867 made has the columns and types it had; 101 and 104 have a
    // row more, and one template is new: the line whose empty object is now a key.
    let after = server.templates();
    let (before, after) = (
        before.as_array().expect("an array"),
        after.as_array().expect("an array"),
    );
    assert_eq!(after.len(), before.len() + 1);
    for old in before {
        let now = after
            .iter()
            .find(|t| t["template_id"] == old["template_id"])
            .expect("a template ca6d867 made is still there");
        assert_eq!(now["columns"], old["columns"], "{}", old["template_id"]);
        assert_eq!(now["time_source"], old["time_source"]);
    }
    let added = after
        .iter()
        .find(|t| {
            !before
                .iter()
                .any(|old| old["template_id"] == t["template_id"])
        })
        .expect("a new template");
    assert_eq!(
        added["columns"],
        json!({"attr.storage": "String", "level": "String", "tags": "String", "ts": "Datetime"})
    );
}
