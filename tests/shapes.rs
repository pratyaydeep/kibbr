// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The JSON values a line held are the JSON values a query gives back: an array as that
//! array and an empty object as `{}`, beside the strings that spell them, through both
//! doors - `kibbr load` and a server's `/ingest` - in memory, on disk, after a restart and
//! after `kibbr compact`.
//!
//! Every test runs the real binary. A line's rows are compared with the line itself,
//! flattened as the store flattens it: nested objects joined with `.`, and an array or an
//! object with no members left whole at its key.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Map, Value};

const DAY: &str = "2024-01-15";

/// Every row there is, oldest first: each line below has a second of its own.
const ALL: &str = "MATCH LOG {} ORDER BY time ASC LIMIT 1000";

/// The values of `k`, one line each: every kind of value one key can hold, and each of
/// `{}`, `[]`, `null`, `""`, `"{}"` and `"[]"`. A missing key is the line with `other`.
const KINDS: &[&str] = &[
    r#"["en"]"#,
    r#""[\"en\"]""#,
    "[]",
    r#""[]""#,
    "{}",
    r#""{}""#,
    "null",
    r#""""#,
    "7",
    "1.5",
    "true",
    r#""plain""#,
    r#""2024-01-15T09:00:00Z""#,
    r#"[124]"#,
    r#""[124]""#,
    r#"[{"a":1.5,"b":[null,{}]},null,true,"x"]"#,
    "[[1,[2,[]]],[]]",
    "[null,null]",
    r#"[{"b":1,"a":2},{}]"#,
    r#"["en"]"#,
];

/// The lines of the tests: `KINDS` under one key, then the same kinds in other shapes.
fn lines() -> Vec<String> {
    let mut bodies: Vec<String> = KINDS.iter().map(|v| format!(r#""k":{v}"#)).collect();
    bodies.extend(
        [
            // No `k` at all.
            r#""other":1"#,
            // An empty object nested, alone and beside a key that is not empty.
            r#""k":{"b":{}}"#,
            r#""k":{"b":{"c":{}},"d":1}"#,
            r#""k":{"b":{"c":1},"d":{}}"#,
            // One template, where `tags` is an array, a string, empty, null, and `a.b`
            // is an empty object, a string and a number.
            r#""a":{"b":{}},"tags":["x","y"],"n":1"#,
            r#""a":{"b":"s"},"tags":"x,y","n":2"#,
            r#""a":{"b":3},"tags":[],"n":3"#,
            r#""a":{"b":{}},"tags":null,"n":4"#,
            r#""a":{"b":[]},"tags":"[\"x\",\"y\"]","n":5"#,
            // Under a key that is promoted when it holds a word.
            r#""event":["a"],"n":1"#,
            r#""event":"a","n":1"#,
            r#""event":{},"n":1"#,
        ]
        .map(str::to_string),
    );
    bodies
        .iter()
        .enumerate()
        .map(|(n, body)| format!(r#"{{"ts":"{DAY}T10:{:02}:{:02}Z",{body}}}"#, n / 60, n % 60))
        .collect()
}

/// A line as its row: nested objects joined with `.`; an array, and an object with no
/// members, whole.
fn flattened(line: &str) -> Value {
    fn walk(prefix: &str, object: &Map<String, Value>, out: &mut Map<String, Value>) {
        for (key, value) in object {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            match value {
                Value::Object(inner) if !inner.is_empty() => walk(&path, inner, out),
                other => {
                    out.insert(path, other.clone());
                }
            }
        }
    }
    let Value::Object(object) = serde_json::from_str(line).expect("a line is JSON") else {
        panic!("a line is an object");
    };
    let mut out = Map::new();
    walk("", &object, &mut out);
    Value::Object(out)
}

fn expected() -> Vec<Value> {
    lines().iter().map(|line| flattened(line)).collect()
}

// ── A scratch directory, the binary, and a server ──────────────────────────────────

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kibbr-shapes-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create the scratch directory");
        Self(dir)
    }

    fn data(&self) -> String {
        self.0.join("data").display().to_string()
    }

    fn file(&self, name: &str, lines: &[String]) -> String {
        let path = self.0.join(name);
        let body: String = lines.iter().map(|line| format!("{line}\n")).collect();
        fs::write(&path, body).expect("write an input file");
        path.display().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// `kibbr <args>`, which must exit 0; its stdout as JSON.
fn kibbr(args: &[&str]) -> Value {
    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(args)
        .env_remove("KIBBR__STORAGE__BASE_PATH")
        .env_remove("KIBBR__SCHEMA__TIME_COLUMNS")
        .output()
        .expect("failed to run kibbr");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "kibbr {args:?}:\n{stdout}\n{stderr}");
    serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("not JSON ({e}): {stdout}"))
}

struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
}

/// `kibbr serve <args>` on a port of its own, compacting only when asked.
fn serve(args: &[&str]) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["serve", "--port", "0", "--compact-interval-ms", "0"])
        .args(args)
        .env_remove("KIBBR__STORAGE__BASE_PATH")
        .env_remove("KIBBR__SCHEMA__TIME_COLUMNS")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr serve");
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
    let mut banner = String::new();
    let mut port = None;
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
    }
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

    fn query(&self, q: &str) -> Value {
        self.call("POST", "/query", json!({ "q": q }).to_string().as_bytes())
    }

    /// The rows of a query, the system column left out: no line here has that key.
    fn rows(&self, q: &str) -> Vec<Value> {
        let answer = self.query(q);
        answer["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("no rows in {answer}"))
            .iter()
            .map(|row| {
                let mut row = row.as_object().expect("a row is an object").clone();
                row.remove("_received_at");
                Value::Object(row)
            })
            .collect()
    }

    fn ingest(&self, lines: &[String]) {
        let body: String = lines.iter().map(|line| format!("{line}\n")).collect();
        let report = self.call("POST", "/ingest", body.as_bytes());
        assert_eq!(report["ingested"], lines.len() as u64, "{report}");
        assert_eq!(report["errors"], json!([]), "{report}");
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
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Every line, row for row, as itself.
fn assert_rows_are_the_lines(server: &Server, when: &str) {
    let got = server.rows(ALL);
    let want = expected();
    assert_eq!(got.len(), want.len(), "{when}: {} rows", got.len());
    for (n, (g, w)) in got.iter().zip(&want).enumerate() {
        assert_eq!(g, w, "{when}: line {} came back otherwise", n + 1);
    }
}

/// `kibbr compact` of the tests' day; `rebuilt` is whether it had row groups to rebuild.
fn compact(dir: &Path, rebuilt: bool) {
    let data = dir.display().to_string();
    let report = kibbr(&["compact", "--data-dir", &data, "--day", DAY, "--json"]);
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["days"][0]["compacted"], rebuilt, "{report}");
}

// ── The tests ──────────────────────────────────────────────────────────────────────

/// **The store flattens a line as these tests say it does**: the expectation is checked
/// against what it must be for three lines written out by hand.
#[test]
fn the_expectation_is_the_line_flattened_with_arrays_and_empty_objects_whole() {
    assert_eq!(
        flattened(r#"{"ts":"t","k":{"b":{"c":{}},"d":1},"e":[],"f":{},"g":"[]"}"#),
        json!({"ts": "t", "k.b.c": {}, "k.d": 1, "e": [], "f": {}, "g": "[]"})
    );
    assert_eq!(flattened(r#"{"a":{"b":{"c":1}}}"#), json!({"a.b.c": 1}));
    assert_eq!(flattened(r#"{"k":[{"a":{}}]}"#), json!({"k": [{"a": {}}]}));
}

/// **Through `/ingest`, held in memory**: every line comes back as itself.
#[test]
fn every_kind_comes_back_as_itself_from_memory() {
    let server = serve(&["--storage", "memory"]);
    server.ingest(&lines());
    assert_rows_are_the_lines(&server, "in memory");
}

/// **Through `/ingest`, on disk**: while the rows are in buffers and row groups of the
/// live door, after a restart reads them from files, and after `kibbr compact` has
/// rebuilt the day - which a second restart reads.
#[test]
fn every_kind_comes_back_as_itself_through_ingest_before_and_after_compaction() {
    let scratch = Scratch::new();
    let live = [
        "--data-dir",
        &scratch.data(),
        "--flush-interval-ms",
        "50",
        "--row-group-timeout-ms",
        "50",
    ];
    let all = lines();
    let server = serve(&live);
    // Three bodies with a wait between, so each template's rows lie in several row
    // groups and a compaction has something to rebuild.
    for part in all.chunks(all.len().div_ceil(3)) {
        server.ingest(part);
        std::thread::sleep(std::time::Duration::from_millis(400));
    }
    assert_rows_are_the_lines(&server, "on disk, warm");
    server.shutdown();

    let server = serve(&live);
    assert_rows_are_the_lines(&server, "on disk, restarted");
    server.shutdown();

    compact(Path::new(&scratch.data()), true);
    let server = serve(&live);
    assert_rows_are_the_lines(&server, "on disk, compacted");
}

/// **Through `kibbr load`**: as loaded, in two loads so the day is two files; after
/// `kibbr compact`, which leaves a loaded day's finished files as they are; and after the
/// live door has added the same lines to the day and a compaction has rebuilt those.
#[test]
fn every_kind_comes_back_as_itself_through_load_before_and_after_compaction() {
    let scratch = Scratch::new();
    let all = lines();
    let (first, second) = all.split_at(all.len() / 2);
    for (name, part) in [("first.ndjson", first), ("second.ndjson", second)] {
        let file = scratch.file(name, part);
        let report = kibbr(&["load", "--data-dir", &scratch.data(), "--json", &file]);
        assert_eq!(report["rows"], part.len() as u64, "{report}");
        assert_eq!(report["rejected"], 0, "{report}");
    }
    let server = serve(&["--data-dir", &scratch.data()]);
    assert_rows_are_the_lines(&server, "loaded");
    server.shutdown();

    compact(Path::new(&scratch.data()), false);
    let server = serve(&["--data-dir", &scratch.data()]);
    assert_rows_are_the_lines(
        &server,
        "loaded, after a compaction with nothing to rebuild",
    );

    // The same lines again through the live door, in three bodies: the day then holds
    // every line twice, the second copy in row groups a compaction rebuilds.
    for part in all.chunks(all.len().div_ceil(3)) {
        server.ingest(part);
    }
    server.shutdown();
    compact(Path::new(&scratch.data()), true);
    let server = serve(&["--data-dir", &scratch.data()]);
    let got = server.rows(ALL);
    let want = expected();
    assert_eq!(got.len(), want.len() * 2);
    // Two rows a line, at one time: both are the line, whichever store each came from.
    for (n, pair) in got.chunks(2).enumerate() {
        assert_eq!(pair, [want[n].clone(), want[n].clone()], "line {}", n + 1);
    }
}

/// **A query compares an array as its JSON text, as it always did**: the text finds the
/// array and the string that spells it, each given back as what it is; `*` finds an array
/// and an empty object, which are present, and not a null; and a group is labelled by the
/// text. In memory and from compacted files alike.
#[test]
fn an_array_is_matched_and_grouped_by_its_text() {
    let scratch = Scratch::new();
    let file = scratch.file("all.ndjson", &lines());
    kibbr(&["load", "--data-dir", &scratch.data(), "--json", &file]);
    let disk = serve(&["--data-dir", &scratch.data()]);
    let memory = serve(&["--storage", "memory"]);
    memory.ingest(&lines());

    for (server, store) in [(&memory, "memory"), (&disk, "disk")] {
        let found = server.rows(r#"MATCH LOG { "k": "[\"en\"]" } ORDER BY time ASC LIMIT 100"#);
        let values: Vec<&Value> = found.iter().map(|row| &row["k"]).collect();
        assert_eq!(
            values,
            [&json!(["en"]), &json!("[\"en\"]"), &json!(["en"])],
            "{store}"
        );

        let empty = server.rows(r#"MATCH LOG { "k": "{}" } ORDER BY time ASC LIMIT 100"#);
        let values: Vec<&Value> = empty.iter().map(|row| &row["k"]).collect();
        assert_eq!(values, [&json!({}), &json!("{}")], "{store}");

        let present = server.query(r#"COUNT LOG { "k": * }"#);
        let nulls = KINDS.iter().filter(|v| **v == "null").count();
        assert_eq!(present["count"], (KINDS.len() - nulls) as u64, "{store}");

        let nested = server.rows(r#"MATCH LOG { "k.b": * } LIMIT 100"#);
        assert_eq!(nested.len(), 1, "{store}: {nested:?}");
        assert_eq!(nested[0]["k.b"], json!({}), "{store}");

        let groups = server.query(r#"COUNT LOG { "n": * } GROUP BY "tags""#);
        let mut labels: Vec<(String, u64)> = groups["groups"]
            .as_array()
            .expect("groups")
            .iter()
            .map(|g| {
                let label = g["keys"]["tags"].as_str().expect("a label").to_string();
                (label, g["count"].as_u64().expect("a count"))
            })
            .collect();
        labels.sort();
        assert_eq!(
            labels,
            [
                // Three lines of another template have `n` and no `tags`, and one a null.
                ("(null)".to_string(), 4),
                ("[\"x\",\"y\"]".to_string(), 2),
                ("[]".to_string(), 1),
                ("x,y".to_string(), 1),
            ],
            "{store}"
        );
    }
}
