// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Timestamps and epoch-like integers from end to end, on the values a preflight over
//! real datasets found mistyped: MongoDB's `attr.Available`, CockroachDB's
//! `event.GoAllocBytes` and decimal `timestamp`, Bluesky's `time_us`, PostgreSQL's zoned
//! `timestamp`. Through the server - in memory, in row groups, and replayed from the
//! write-ahead log - and through `kibbr query`, where the engine is held to the oracle.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};

const TIME_COLUMNS: &str = "ts,timestamp,time_us";

/// The lines, in the order they arrive: each column's first value, and then the value
/// that used to turn it to `String`.
const LINES: &[&str] = &[
    // MongoDB: a column of epoch seconds, then an integer below the window.
    r#"{"ts":"2023-03-25T02:28:50Z","c":"STORAGE","attr":{"Available":1679711330}}"#,
    r#"{"ts":"2023-03-25T02:28:51Z","c":"STORAGE","attr":{"Available":999999999}}"#,
    r#"{"ts":"2023-03-25T02:28:52Z","c":"STORAGE","attr":{"Available":"1679711332"}}"#,
    // CockroachDB: decimal seconds as the time, and integers that then take ten digits
    // inside the epoch window.
    r#"{"timestamp":"1679711330.570420890","channel":"OPS","event":{"GoAllocBytes":999999999}}"#,
    r#"{"timestamp":"1679711331.000000001","channel":"OPS","event":{"GoAllocBytes":1220305536}}"#,
    r#"{"timestamp":1679711332.5,"channel":"OPS","event":{"GoAllocBytes":7}}"#,
    // Bluesky: sixteen digits of microseconds.
    r#"{"time_us":1732206349000167,"did":"did:plc:a","kind":"commit"}"#,
    r#"{"time_us":1732206349000999,"did":"did:plc:b","kind":"identity"}"#,
    // PostgreSQL jsonlog: a date and time with a named zone.
    r#"{"timestamp":"2023-03-27 00:26:35.719 EDT","pid":27,"message":"starting"}"#,
    r#"{"timestamp":"2023-03-27 00:26:36 EST","pid":28,"message":"ready"}"#,
    // A sixteen-digit id under a key that is no time column, beside a real time.
    r#"{"ts":"2023-03-25T02:28:53Z","order_id":1732206349000167}"#,
];

/// More of the same templates, for the lines a test leaves in the write-ahead log.
const LATER: &[&str] = &[
    r#"{"ts":"2023-03-25T02:29:00Z","c":"NETWORK","attr":{"Available":5}}"#,
    r#"{"timestamp":"1679711340.25","channel":"DEV","event":{"GoAllocBytes":1220305537}}"#,
    r#"{"time_us":1732206350000001,"did":"did:plc:c","kind":"commit"}"#,
    r#"{"timestamp":"2023-03-27 00:27:00.5 EDT","pid":29,"message":"listening"}"#,
];

/// A query with what it counts over `LINES`, and over `LINES` and `LATER`.
const COUNTS: &[(&str, u64, u64)] = &[
    ("COUNT LOG {}", 11, 15),
    // The nanosecond CockroachDB wrote, and no other.
    (
        r#"COUNT LOG {} BETWEEN "2023-03-25T02:28:50.570420890Z" AND "2023-03-25T02:28:50.570420891Z""#,
        1,
        1,
    ),
    (
        r#"COUNT LOG {} BETWEEN "2023-03-25T02:28:50.570420891Z" AND "2023-03-25T02:28:51Z""#,
        0,
        0,
    ),
    (
        r#"COUNT LOG { "channel": * } BETWEEN "2023-03-25T02:28:50Z" AND "2023-03-25T02:28:53Z""#,
        3,
        3,
    ),
    // A JSON number of seconds.
    (
        r#"COUNT LOG {} BETWEEN "2023-03-25T02:28:52.5Z" AND "2023-03-25T02:28:52.500000001Z""#,
        1,
        1,
    ),
    // Bluesky's microsecond.
    (
        r#"COUNT LOG {} BETWEEN "2024-11-21T16:25:49.000167Z" AND "2024-11-21T16:25:49.000168Z""#,
        1,
        1,
    ),
    (
        r#"COUNT LOG { "kind": * } BETWEEN "2024-11-21T00:00:00Z" AND "2024-11-22T00:00:00Z""#,
        2,
        3,
    ),
    // PostgreSQL's EDT is four hours behind UTC, EST five.
    (
        r#"COUNT LOG {} BETWEEN "2023-03-27T04:26:35.719Z" AND "2023-03-27T04:26:35.720Z""#,
        1,
        1,
    ),
    (
        r#"COUNT LOG {} BETWEEN "2023-03-27T05:26:36Z" AND "2023-03-27T05:26:37Z""#,
        1,
        1,
    ),
    (
        r#"COUNT LOG { "pid": * } BETWEEN "2023-03-27T00:00:00Z" AND "2023-03-28T00:00:00Z""#,
        2,
        3,
    ),
    // The columns that are integers now, found by the digits they arrived as.
    (r#"COUNT LOG { "attr.Available": "1679711330" }"#, 1, 1),
    (r#"COUNT LOG { "attr.Available": "999999999" }"#, 1, 1),
    (r#"COUNT LOG { "attr.Available": "1679711332" }"#, 1, 1),
    (
        r#"COUNT LOG { "attr.Available": "2023-03-25T02:28:50Z" }"#,
        0,
        0,
    ),
    (r#"COUNT LOG { "event.GoAllocBytes": "1220305536" }"#, 1, 1),
    (r#"COUNT LOG { "event.GoAllocBytes": "999999999" }"#, 1, 1),
    (r#"COUNT LOG { "event.GoAllocBytes": "7" }"#, 1, 1),
    (r#"COUNT LOG { "order_id": "1732206349000167" }"#, 1, 1),
    (r#"COUNT LOG { "time_us": "1732206349000167" }"#, 1, 1),
    (r#"COUNT LOG { "timestamp": "1679711330.570420890" }"#, 1, 1),
    (
        r#"COUNT LOG { "timestamp": "2023-03-27 00:26:35.719 EDT" }"#,
        1,
        1,
    ),
];

/// Queries whose whole answer the engine and the oracle, and a server before and after
/// a restart, must give alike.
const BATTERY: &[&str] = &[
    "MATCH LOG {} LIMIT 1000",
    "MATCH LOG {} ORDER BY time ASC LIMIT 1000",
    r#"MATCH LOG { "channel": "OPS" } ORDER BY time ASC LIMIT 1000"#,
    r#"MATCH LOG { "attr.Available": * } LIMIT 1000"#,
    r#"COUNT LOG {} GROUP BY "attr.Available""#,
    r#"COUNT LOG {} GROUP BY "event.GoAllocBytes""#,
    r#"COUNT LOG {} GROUP BY "time_us""#,
    r#"COUNT LOG {} GROUP BY "timestamp""#,
    r#"COUNT LOG { "channel": * } BETWEEN "2023-03-25T02:00:00Z" AND "2023-03-25T04:00:00Z" GROUP BY time(1h)"#,
    r#"COUNT LOG { "kind": * } BETWEEN "2024-11-21T16:25:49Z" AND "2024-11-21T16:25:51Z" GROUP BY time(1s)"#,
    r#"COUNT LOG { "pid": * } BETWEEN "2023-03-27T04:00:00Z" AND "2023-03-27T06:00:00Z" GROUP BY time(30m)"#,
];

fn body(lines: &[&str]) -> Vec<u8> {
    lines
        .iter()
        .flat_map(|l| format!("{l}\n").into_bytes())
        .collect()
}

// ── A server ───────────────────────────────────────────────────────────────────────

struct Db {
    dir: PathBuf,
}

impl Db {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kibbr-time-types-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        Self { dir }
    }

    fn start(&self) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &self.dir.display().to_string()])
            .args(["--time-columns", TIME_COLUMNS])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn kibbr serve");
        let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
        let mut port = None;
        let mut banner = String::new();
        for _ in 0..60 {
            let mut line = String::new();
            if stderr.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            banner.push_str(&line);
            if let Some(at) = line.find("http://127.0.0.1:") {
                port = line[at + "http://127.0.0.1:".len()..].trim().parse().ok();
                break;
            }
        }
        let port = port.unwrap_or_else(|| panic!("no listening line in banner:\n{banner}"));
        Server {
            child,
            port,
            stderr,
            banner,
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
    banner: String,
}

impl Server {
    fn call(&self, method: &str, path: &str, body: &[u8]) -> Value {
        let mut socket = TcpStream::connect(("127.0.0.1", self.port)).expect("failed to connect");
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n",
            self.port,
            body.len()
        );
        socket.write_all(head.as_bytes()).expect("failed to write");
        socket.write_all(body).expect("failed to write the body");
        let mut response = Vec::new();
        socket.read_to_end(&mut response).expect("failed to read");
        let split = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("no header/body split in the response");
        let head = String::from_utf8_lossy(&response[..split]).into_owned();
        let text = String::from_utf8_lossy(&response[split + 4..]).into_owned();
        assert!(
            head.starts_with("HTTP/1.1 200"),
            "{method} {path} answered:\n{head}\n{text}"
        );
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
    }

    fn ingest(&self, lines: &[&str]) {
        let report = self.call("POST", "/ingest", &body(lines));
        assert_eq!(report["ingested"], lines.len() as u64, "{report}");
    }

    fn query(&self, q: &str) -> Value {
        self.call("POST", "/query", json!({ "q": q }).to_string().as_bytes())
    }

    /// Everything of an answer a restart must not change.
    fn answer(&self, q: &str) -> Value {
        let full = self.query(q);
        json!({
            "exists": full["exists"],
            "count": full["count"],
            "rows": full["rows"],
            "groups": full["groups"],
        })
    }

    /// `(time source, {column: type})` of the one template that has every one of `keys`.
    fn template(&self, keys: &[&str]) -> (String, serde_json::Map<String, Value>) {
        let all = self.call("GET", "/templates", b"");
        let mut found = Vec::new();
        for t in all["templates"].as_array().expect("templates") {
            let columns: serde_json::Map<String, Value> = t["columns"]
                .as_array()
                .expect("columns")
                .iter()
                .map(|c| {
                    (
                        c["name"].as_str().expect("a name").to_string(),
                        c["type"].clone(),
                    )
                })
                .collect();
            if keys.iter().all(|key| columns.contains_key(*key)) {
                let source = t["time_source"].as_str().expect("a time source");
                found.push((source.to_string(), columns));
            }
        }
        assert_eq!(found.len(), 1, "templates with {keys:?}: {found:?}");
        found.remove(0)
    }

    fn shutdown(mut self) {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill -TERM failed");
        let status = self.child.wait().expect("failed to wait for kibbr serve");
        let mut log = String::new();
        let _ = self.stderr.read_to_string(&mut log);
        assert_eq!(status.code(), Some(0), "the stop did not exit 0:\n{log}");
        self.child = Command::new("true").spawn().expect("failed to spawn true");
        let _ = self.child.wait();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn assert_counts(server: &Server, later: bool, when: &str) {
    for (q, first, both) in COUNTS {
        let want = if later { *both } else { *first };
        assert_eq!(server.query(q)["count"], want, "{when}: `{q}`");
    }
}

/// What every template of `LINES` must be: the time source chosen, and the types the
/// ladder and the widening gave - never `String` for an epoch beside an integer.
fn assert_templates(server: &Server, when: &str) {
    let (source, columns) = server.template(&["attr.Available"]);
    assert_eq!(source, "column:ts", "{when}");
    assert_eq!(
        columns["attr.Available"], "Int64",
        "{when}: MongoDB's attr.Available"
    );
    assert_eq!(columns["ts"], "Datetime", "{when}");

    let (source, columns) = server.template(&["event.GoAllocBytes"]);
    assert_eq!(
        source, "column:timestamp",
        "{when}: CockroachDB's timestamp"
    );
    assert_eq!(columns["event.GoAllocBytes"], "Int64", "{when}");
    assert_eq!(columns["timestamp"], "Float64", "{when}");

    let (source, columns) = server.template(&["time_us"]);
    assert_eq!(source, "column:time_us", "{when}: Bluesky's time_us");
    assert_eq!(columns["time_us"], "Int64", "{when}");

    let (source, columns) = server.template(&["pid"]);
    assert_eq!(source, "column:timestamp", "{when}: PostgreSQL's timestamp");
    assert_eq!(columns["timestamp"], "String", "{when}");

    let (source, columns) = server.template(&["order_id"]);
    assert_eq!(source, "column:ts", "{when}");
    assert_eq!(columns["order_id"], "Int64", "{when}: a sixteen-digit id");
}

/// No row of `LINES` took its arrival time, and each shows the value its line held.
fn assert_rows(server: &Server, when: &str) {
    let all = server.query("MATCH LOG {} ORDER BY time ASC LIMIT 1000");
    let rows = all["rows"].as_array().expect("rows");
    assert!(rows.len() >= LINES.len(), "{when}: {all}");
    for row in rows {
        assert!(
            row.get("_received_at").is_none(),
            "{when}: a row took its arrival time: {row}"
        );
    }
    // In time order: March 25th's lines, then the 27th's, then November 2024's.
    let first = &rows[0];
    assert_eq!(first["ts"], "2023-03-25T02:28:50Z", "{when}");
    assert_eq!(first["attr.Available"], 1679711330u64, "{when}");
    assert_eq!(rows[1]["timestamp"], "1679711330.570420890", "{when}");
    assert_eq!(rows[1]["event.GoAllocBytes"], 999999999u64, "{when}");
    let last = &rows[rows.len() - 1];
    assert!(
        last["time_us"]
            .as_u64()
            .is_some_and(|t| t >= 1732206349000999),
        "{when}: the newest row is Bluesky's: {last}"
    );

    // The integers a group is labelled by are the digits that arrived.
    let groups = server.query(r#"COUNT LOG { "attr.Available": * } GROUP BY "attr.Available""#);
    let mut labels: Vec<String> = groups["groups"]
        .as_array()
        .expect("groups")
        .iter()
        .map(|g| {
            g["keys"]["attr.Available"]
                .as_str()
                .expect("a label")
                .to_string()
        })
        .collect();
    labels.sort();
    assert_eq!(&labels[..2], ["1679711330", "1679711332"], "{when}");
    assert!(
        labels.contains(&"999999999".to_string()),
        "{when}: {labels:?}"
    );
}

// ── Through the server ─────────────────────────────────────────────────────────────

/// **The five mistyped columns of the preflight are typed, and timed, as they should
/// be** - in memory, then in row groups after a stop, with every answer unchanged.
#[test]
fn the_preflight_values_are_typed_and_timed_in_memory_and_in_row_groups() {
    let db = Db::new();
    let first = db.start();
    first.ingest(LINES);
    assert_templates(&first, "in memory");
    assert_counts(&first, false, "in memory");
    assert_rows(&first, "in memory");
    let before: Vec<Value> = BATTERY.iter().map(|q| first.answer(q)).collect();
    first.shutdown();

    let second = db.start();
    assert_templates(&second, "in row groups");
    assert_counts(&second, false, "in row groups");
    assert_rows(&second, "in row groups");
    for (q, before) in BATTERY.iter().zip(&before) {
        assert_eq!(&second.answer(q), before, "`{q}` across a restart");
    }
}

/// **Lines this build logged replay under the times it filed them at**: a server killed
/// with rows only in its log puts every one back, holds none, and answers as before.
#[test]
fn a_time_column_form_replays_from_the_log_at_the_time_it_was_filed_under() {
    let db = Db::new();
    let first = db.start();
    first.ingest(LINES);
    first.shutdown();

    let second = db.start();
    second.ingest(LATER);
    assert_counts(&second, true, "before the kill");
    let before: Vec<Value> = BATTERY.iter().map(|q| second.answer(q)).collect();
    drop(second);

    let third = db.start();
    let replay = third.call("GET", "/stats", b"")["storage"]["wal"]["replay"].clone();
    assert_eq!(
        replay["applied"],
        LATER.len() as u64,
        "{replay}\n{}",
        third.banner
    );
    assert_eq!(replay["held"], 0, "{replay}\n{}", third.banner);
    assert_templates(&third, "after a replay");
    assert_counts(&third, true, "after a replay");
    assert_rows(&third, "after a replay");
    for (q, before) in BATTERY.iter().zip(&before) {
        assert_eq!(&third.answer(q), before, "`{q}` across a kill");
    }
}

// ── Through `kibbr query`, against the oracle ──────────────────────────────────────

fn cli(q: &str, reference: bool) -> Value {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
    command
        .args(["query", "-", q, "--format", "json"])
        .args(["--time-columns", TIME_COLUMNS]);
    if reference {
        command.arg("--reference");
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    let lines: Vec<&str> = LINES.iter().chain(LATER).copied().collect();
    let input = body(&lines);
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(&input);
    });
    let out = child.wait_with_output().expect("failed to wait on kibbr");
    writer.join().expect("stdin writer thread panicked");
    assert!(
        out.status.success(),
        "kibbr query `{q}` exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let full: Value = serde_json::from_slice(&out.stdout).expect("query did not emit JSON");
    json!({
        "exists": full["exists"],
        "count": full["count"],
        "rows": full["rows"],
        "groups": full["groups"],
    })
}

/// **The oracle, which works the forms and the widening out for itself, answers as the
/// engine does**: every count above, and the whole answer of every battery query.
#[test]
fn the_engine_and_the_oracle_agree_on_the_preflight_values() {
    for (q, _, both) in COUNTS {
        let engine = cli(q, false);
        assert_eq!(engine["count"], *both, "the engine: `{q}`");
        assert_eq!(cli(q, true), engine, "the oracle and the engine: `{q}`");
    }
    // The oracle has one order, newest first: `ORDER BY time ASC` is the engine's alone.
    for q in BATTERY.iter().filter(|q| !q.contains("ORDER BY")) {
        let engine = cli(q, false);
        assert!(
            engine["count"].as_u64().is_some_and(|n| n > 0),
            "`{q}`: {engine}"
        );
        assert_eq!(cli(q, true), engine, "the oracle and the engine: `{q}`");
    }
}
