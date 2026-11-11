// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What this build makes of a data directory another build wrote.
//!
//! `tests/fixtures/compat-ae7d8ca/data` is a data directory a kibbr built from commit
//! ae7d8ca left behind - before a time column read its own forms and before an epoch
//! and an integer made a column of integers: row groups from a graceful stop, and lines
//! that are only in its write-ahead log because the server was killed. `expected.json`
//! is what that same build answered on it, after replaying its own log (`make.py`,
//! beside them, wrote both). Every test starts this build on a copy.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};

// ── The fixture, and a server on a copy of it ──────────────────────────────────────

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compat-ae7d8ca")
}

fn expected() -> Value {
    let text = fs::read_to_string(fixture().join("expected.json")).expect("expected.json");
    serde_json::from_str(&text).expect("expected.json is JSON")
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
struct Db {
    dir: PathBuf,
}

impl Db {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kibbr-compat-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        copy_dir(&fixture().join("data"), &dir);
        Self { dir }
    }

    /// This build, on the directory, under the time columns the fixture was written with.
    fn start(&self) -> Server {
        let time_columns = expected()["time_columns"]
            .as_str()
            .expect("time_columns")
            .to_string();
        let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &self.dir.display().to_string()])
            .args(["--time-columns", &time_columns])
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
    /// What the start said before it listened: where a replay that left a line in the
    /// log says so.
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

    /// What `make.py` kept of an answer: everything a build must not change.
    fn answer(&self, q: &str) -> Value {
        let full = self.call("POST", "/query", json!({ "q": q }).to_string().as_bytes());
        json!({
            "exists": full["exists"],
            "count": full["count"],
            "rows": full["rows"],
            "groups": full["groups"],
        })
    }

    fn ingest(&self, lines: &[Value]) {
        let body: String = lines.iter().map(|line| format!("{line}\n")).collect();
        let report = self.call("POST", "/ingest", body.as_bytes());
        assert_eq!(report["ingested"], lines.len() as u64, "{report}");
    }

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

    /// SIGTERM, and the exit: the stop that writes every buffered row into a row group.
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
    let db = Db::new();
    let server = db.start();
    assert_answers_as_ae7d8ca(&server, "on the directory as ae7d8ca left it");
}

/// **Replaying ae7d8ca's log gives no stored column another type, and leaves no line
/// behind**: `/templates` says what it said under ae7d8ca - each column's type, each
/// template's time source and row count - and all seven logged lines are put back.
#[test]
fn replaying_the_log_ae7d8ca_wrote_changes_no_column_type_and_holds_no_line() {
    let db = Db::new();
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
    let db = Db::new();
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
    let db = Db::new();
    let server = db.start();
    let before = server.types(105);
    assert_eq!(before["seen"], "Datetime");
    assert_eq!(before["count"], "Int64");

    server.ingest(&[
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
