//! `triedb serve` from outside the binary: every endpoint answers, a body with three bad
//! lines in it reports exactly those three, and a signal mid-request exits 0.
//!
//! HTTP is spoken over a bare `TcpStream` rather than through a client crate. The
//! contract is a status line and a JSON body, and a test that asserts on
//! those directly is a test that cannot be satisfied by a client library's idea of what
//! the server meant. `Connection: close` makes the body "read to EOF", which is the whole
//! of the parsing needed.
//!
//! The corpus comes from `gen-logs` - real generated data, seeded, with the
//! three corruptions written in at known line numbers.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, ChildStderr, Command, Stdio};

/// Lines in the ingest body, and the three that are broken.
const LINES: usize = 500;
const BAD: [usize; 3] = [137, 400, 461];

// ── A running server ───────────────────────────────────────────────────────────────

struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
}

impl Server {
    /// Start on port 0 and read back the port the OS chose.
    ///
    /// Not a fixed port: two of these tests run concurrently under `cargo test`, and
    /// 7700 is also where a developer's own `triedb serve` is. The banner prints the
    /// bound address precisely so something can read it.
    fn start() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_triedb"))
            .args(["serve", "--port", "0"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn triedb serve");

        let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
        let mut port = None;
        let mut banner = String::new();
        for _ in 0..40 {
            let mut line = String::new();
            if stderr.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            banner.push_str(&line);
            if let Some(at) = line.find("http://127.0.0.1:") {
                let tail = &line[at + "http://127.0.0.1:".len()..];
                port = tail.trim().parse().ok();
            }
            // Read to the end of the banner, not just to the port: the assertions below
            // are about what the whole banner says, and the address is on its third line.
            if line.contains("shutdown ") {
                break;
            }
        }
        let port = port.unwrap_or_else(|| panic!("no listening line in banner:\n{banner}"));

        // The banner is the one place a person is told the data is not durable, so its
        // saying so is part of the contract and not decoration.
        assert!(
            banner.contains("STORAGE IS IN MEMORY"),
            "banner does not say storage is in memory:\n{banner}"
        );
        assert!(
            banner
                .to_lowercase()
                .contains("lost when this process exits"),
            "banner does not say the data is lost on exit:\n{banner}"
        );

        Self {
            child,
            port,
            stderr,
        }
    }

    fn get(&self, path: &str) -> (u16, String) {
        request(self.port, "GET", path, b"")
    }

    fn post(&self, path: &str, body: &[u8]) -> (u16, String) {
        request(self.port, "POST", path, body)
    }

    /// A TrieQL query, as `/query` takes it.
    fn query(&self, q: &str) -> serde_json::Value {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = self.post("/query", body.as_bytes());
        assert_eq!(status, 200, "query `{q}` answered {status}: {text}");
        json(&text)
    }

    fn signal(&self, name: &str) {
        let ok = Command::new("kill")
            .args([name, &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill {name} failed");
    }

    /// Wait for exit, and return the code plus everything left on stderr.
    fn wait(mut self) -> (Option<i32>, String) {
        let status = self.child.wait().expect("failed to wait for triedb serve");
        let mut log = String::new();
        let _ = self.stderr.read_to_string(&mut log);
        // Already reaped; stop `Drop` from killing a pid that may have been reused.
        self.child = Command::new("true").spawn().expect("failed to spawn true");
        let _ = self.child.wait();
        (status.code(), log)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One request, one connection, read to EOF. Returns the status code and the body.
fn request(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    socket.write_all(body).expect("failed to write body");
    socket.flush().expect("failed to flush");

    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).expect("failed to read");
    let text = String::from_utf8_lossy(&raw).into_owned();

    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{text}"));
    let body = match text.find("\r\n\r\n") {
        Some(at) => text[at + 4..].to_string(),
        None => panic!("no header/body split in response:\n{text}"),
    };
    (status, body)
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

// ── The corpus ─────────────────────────────────────────────────────────────────────

/// `LINES` generated lines with `BAD` corrupted, one per failure kind.
///
/// Three kinds rather than three copies of one: the endpoint has to report the *line*
/// correctly whatever went wrong on it, and a truncated document, a non-object and a
/// bad byte take three different routes to `DetectorError`.
fn corpus() -> Vec<u8> {
    let out = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .args(["gen-logs", "--lines", &LINES.to_string(), "--seed", "42"])
        .output()
        .expect("failed to run gen-logs");
    assert!(out.status.success(), "gen-logs failed");

    let mut lines: Vec<Vec<u8>> = out
        .stdout
        .split(|b| *b == b'\n')
        .map(<[u8]>::to_vec)
        .collect();
    // `split` leaves an empty element after the final newline.
    assert_eq!(lines.pop().as_deref(), Some(&b""[..]));
    assert_eq!(
        lines.len(),
        LINES,
        "gen-logs wrote the wrong number of lines"
    );

    lines[BAD[0] - 1] = br#"{"event": "checkout", "user": "#.to_vec();
    lines[BAD[1] - 1] = br#"["not", "an", "object"]"#.to_vec();
    lines[BAD[2] - 1] = b"{\"event\": \"login\", \"msg\": \"caf\xe9\"}".to_vec();

    let mut body = lines.join(&b'\n');
    body.push(b'\n');
    body
}

// ── The endpoints ──────────────────────────────────────────────────────────────────

#[test]
fn every_endpoint_answers() {
    let server = Server::start();

    let (status, body) = server.get("/health");
    assert_eq!(status, 200);
    assert_eq!(json(&body), serde_json::json!({ "ok": true }));

    // Empty, before anything is ingested: a fresh database is a real state and every
    // endpoint has to have an answer for it rather than a division by zero.
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["storage"]["rows"], 0);
    assert_eq!(stats["storage"]["durable"], false);
    assert_eq!(stats["ingest"]["lines_per_second"], 0.0);
    assert_eq!(json(&server.get("/templates").1)["count"], 0);

    let (status, body) = server.post("/ingest", &corpus());
    assert_eq!(status, 200);
    let report = json(&body);
    assert_eq!(report["ingested"], (LINES - BAD.len()) as u64);

    let templates = json(&server.get("/templates").1);
    assert!(templates["count"].as_u64().unwrap_or(0) > 0);
    let first = &templates["templates"][0];
    for field in [
        "template_id",
        "path",
        "columns",
        "rows",
        "first_seen",
        "last_seen",
    ] {
        assert!(
            !first[field].is_null(),
            "/templates entry has no {field}: {first}"
        );
    }

    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["storage"]["rows"], (LINES - BAD.len()) as u64);
    assert_eq!(stats["ingest"]["lines_read"], LINES as u64);
    assert_eq!(stats["ingest"]["lines_rejected"], BAD.len() as u64);
    // Still says so as a field, not only in a banner nobody scrolls back to.
    assert_eq!(stats["storage"]["kind"], "memory");
    assert_eq!(stats["storage"]["durable"], false);

    let answer = server.query(r#"COUNT LOG {}"#);
    assert_eq!(answer["count"], (LINES - BAD.len()) as u64);
    assert_eq!(answer["exists"], true);
}

/// The criterion, exactly: three bad lines out of five hundred report those three,
/// with the line numbers they actually sit on.
#[test]
fn three_bad_lines_report_exactly_those_three() {
    let server = Server::start();
    let (status, body) = server.post("/ingest", &corpus());

    // 200, not 4xx: the body was accepted and 497 lines of it stored.
    assert_eq!(status, 200);
    let report = json(&body);

    assert_eq!(report["ok"], false, "ok should be false when a line failed");
    assert_eq!(report["ingested"], 497);

    let errors = report["errors"].as_array().expect("errors is not an array");
    let lines: Vec<u64> = errors
        .iter()
        .map(|e| e["line"].as_u64().expect("no line number"))
        .collect();
    assert_eq!(
        lines,
        BAD.map(|n| n as u64).to_vec(),
        "wrong lines reported"
    );

    // Every rejection says something about what was wrong with it. The wording is the
    // error type's and is not pinned here; that it is not empty is the contract.
    for e in errors {
        let msg = e["msg"].as_str().unwrap_or("");
        assert!(!msg.is_empty(), "rejection has no message: {e}");
    }

    // The isolation half of the contract: the other 497 are queryable, not lost with them.
    assert_eq!(server.query(r#"COUNT LOG {}"#)["count"], 497);
}

/// A clean body reports no errors at all - the `{"ok":true,...,"errors":[]}` half of the
/// shape, which a test that only ever sends broken input would never check.
#[test]
fn a_clean_body_reports_no_errors() {
    let server = Server::start();
    let out = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .args(["gen-logs", "--lines", "200", "--seed", "7"])
        .output()
        .expect("failed to run gen-logs");

    let (status, body) = server.post("/ingest", &out.stdout);
    assert_eq!(status, 200);
    assert_eq!(
        json(&body),
        serde_json::json!({ "ok": true, "ingested": 200, "errors": [] })
    );
}

/// A query that is not a query is a 400 with a message, not a 500 and not a panic.
#[test]
fn a_bad_query_is_a_400() {
    let server = Server::start();

    for (body, why) in [
        (r#"{"q": "SELECT * FROM logs"}"#, "not TrieQL"),
        (r#"{"q": "COUNT LOG {} SINCE 9z"}"#, "bad duration"),
        (r#"{"nope": 1}"#, "no q field"),
        ("not json at all", "not a JSON body"),
    ] {
        let (status, text) = server.post("/query", body.as_bytes());
        assert_eq!(status, 400, "{why}: expected 400, got {status}: {text}");
        let answer = json(&text);
        assert_eq!(answer["ok"], false, "{why}");
        assert!(
            !answer["error"].as_str().unwrap_or("").is_empty(),
            "{why}: no message"
        );
    }

    // Still serving afterwards - a rejected request must not have cost the process.
    assert_eq!(server.get("/health").0, 200);
}

// ── Shutdown ───────────────────────────────────────────────────────────────────────

/// Graceful shutdown: the signal stops new work, the request already running finishes, exit 0.
///
/// The body is large enough that it cannot complete inside the window before the signal,
/// so the request really is in flight when SIGINT lands. If it were not, the test would
/// still pass - and would be testing nothing - which is why the size is not marginal.
#[test]
fn a_signal_lets_the_in_flight_request_finish() {
    let server = Server::start();
    let port = server.port;

    let out = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .args(["gen-logs", "--lines", "20000", "--seed", "3"])
        .output()
        .expect("failed to run gen-logs");
    let body = out.stdout;

    let ingest = std::thread::spawn(move || request(port, "POST", "/ingest", &body));
    std::thread::sleep(std::time::Duration::from_millis(400));
    server.signal("-INT");

    let (status, text) = ingest.join().expect("the ingest thread panicked");
    assert_eq!(status, 200, "in-flight request did not complete: {text}");
    assert_eq!(json(&text)["ingested"], 20000);

    let (code, log) = server.wait();
    assert_eq!(code, Some(0), "did not exit 0\n{log}");
    assert!(
        log.contains("exit 0"),
        "shutdown log has no exit line:\n{log}"
    );
    // The steps this milestone does not have are named rather than omitted, so an
    // operator can see which durability they do not yet have.
    assert!(
        log.contains("WAL"),
        "shutdown log does not mention the WAL:\n{log}"
    );
}

/// SIGTERM is the one a container runtime sends, and it has to work the same way.
#[test]
fn sigterm_exits_zero() {
    let server = Server::start();
    assert_eq!(server.get("/health").0, 200);
    server.signal("-TERM");

    let (code, log) = server.wait();
    assert_eq!(code, Some(0), "did not exit 0\n{log}");
    assert!(
        log.contains("SIGTERM"),
        "shutdown log does not name the signal:\n{log}"
    );
}
