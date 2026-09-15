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

use flate2::write::GzEncoder;
use flate2::Compression;

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
        Self::start_with(&[])
    }

    /// The same, with extra flags - `--max-rows` is the one this needs, and a backstop the
    /// corpus can actually reach has to be smaller than the 10,000-row default.
    fn start_with(extra: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_triedb"))
            .args(["serve", "--port", "0"])
            .args(extra)
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

        // The backstop changes what an unlimited `MATCH` means on this surface, so the
        // banner names it beside the endpoints rather than leaving it to be discovered
        // from a truncated answer.
        assert!(
            banner.contains("max rows"),
            "banner does not name the backstop:\n{banner}"
        );
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
    let (status, _, body) = request_raw(port, method, path, body);
    (status, body)
}

/// The same, with the response headers kept.
///
/// Only the viewer needs them: `/` is the one endpoint whose content type is part of what
/// it promises, because a browser that is handed `application/json` renders the page as
/// text rather than as a page.
fn request_raw(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String, String) {
    request_with(port, method, path, body, &[])
}

/// The same again with extra request headers - `Content-Encoding` is the one that needs
/// them, and it is a header rather than anything a body can carry.
fn request_with(
    port: u16,
    method: &str,
    path: &str,
    body: &[u8],
    extra: &[(&str, &str)],
) -> (u16, String, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let extra: String = extra
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\n{extra}\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    // Write errors are not failures here. A server that refuses a body before reading it -
    // 413 on the wire cap, 503 on a shed - answers and closes while this is still
    // writing, and the write end breaks. Whether the *answer* survives that is the thing
    // being tested, so the read below is what decides it.
    let _ = socket.write_all(body);
    let _ = socket.flush();

    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).expect("failed to read");
    let text = String::from_utf8_lossy(&raw).into_owned();

    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{text}"));
    let (headers, body) = match text.find("\r\n\r\n") {
        Some(at) => (text[..at].to_lowercase(), text[at + 4..].to_string()),
        None => panic!("no header/body split in response:\n{text}"),
    };
    (status, headers, body)
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

/// A request whose answer may arrive *before* the body has finished being sent.
///
/// `request_with` cannot do this, for a reason worth writing down: `Connection: close`
/// makes the server close the socket as soon as it has answered, and closing one with
/// unread bytes still in its receive buffer is a TCP reset - which discards the response
/// already sitting in the client's buffer. The client sees a connection error where the
/// server sent a perfectly good 503.
///
/// So this speaks `keep-alive`, as real clients do, and reads by `Content-Length` rather
/// than to EOF. It also tolerates the write failing: a refused body is never read, so the
/// write end breaks mid-send. That is the shape of a refusal, not a failure of one.
fn request_early(port: u16, path: &str, body: &[u8]) -> (u16, String, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    // Before the write, not after: macOS refuses the option on a socket whose write end
    // has broken, and that breaking is the normal course of this call.
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(60)))
        .expect("failed to set a read timeout");

    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: keep-alive\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    let _ = socket.write_all(body);
    let _ = socket.flush();

    // Read to the end of the headers, then exactly the body the length promises. Reading
    // to EOF would hang: a keep-alive connection has no EOF until the server gives up on
    // it, which is long after the answer arrived.
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    let split = loop {
        if socket.read(&mut byte).unwrap_or(0) == 0 {
            panic!("connection ended before the headers did:\n{:?}", raw);
        }
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n") {
            break raw.len();
        }
    };
    let headers = String::from_utf8_lossy(&raw[..split]).to_lowercase();
    let length: usize = headers
        .split("content-length:")
        .nth(1)
        .and_then(|tail| tail.split("\r\n").next())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or_else(|| panic!("no Content-Length in the answer:\n{headers}"));

    let mut rest = vec![0u8; length];
    socket
        .read_exact(&mut rest)
        .expect("failed to read the body");
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{headers}"));
    (status, headers, String::from_utf8_lossy(&rest).into_owned())
}

/// `bytes`, gzipped, as a shipper with `compression = "gzip"` would send them.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("failed to gzip");
    encoder.finish().expect("failed to finish gzip")
}

// ── The endpoints ──────────────────────────────────────────────────────────────────

#[test]
fn every_endpoint_answers() {
    let server = Server::start();

    let (status, headers, body) = request_raw(server.port, "GET", "/", b"");
    assert_eq!(status, 200);
    assert!(
        headers.contains("content-type: text/html"),
        "GET / is not served as HTML:\n{headers}"
    );
    assert!(body.contains("<title>"), "GET / answered no page:\n{body}");

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

// ── The backstop, over HTTP ────────────────────────────────────────────────────────

/// A client that names no `LIMIT` gets a page and a way to continue it, not an answer
/// quietly short of the truth. This is the surface the backstop exists for: the CLI's
/// caller is one person at one terminal, and this one's is anything that can open a socket.
#[test]
fn an_unlimited_query_is_backstopped_and_pages_to_completion() {
    let server = Server::start_with(&["--max-rows", "50"]);
    let (status, _) = server.post("/ingest", &corpus());
    assert_eq!(status, 200);

    let first = server.query("MATCH LOG {}");
    assert_eq!(first["meta"]["rows_returned"], 50);
    assert_eq!(first["meta"]["truncated"], true);
    assert_eq!(first["meta"]["limit_source"], "backstop");
    // The total is the answer's, not the page's: 500 lines less the 3 corrupted ones.
    assert_eq!(first["count"], (LINES - BAD.len()) as u64);

    // Every page unlimited, as the client that never learned to page would send them.
    let mut paged: Vec<String> = Vec::new();
    let mut cursor = first["meta"]["next_cursor"].as_str().map(str::to_string);
    paged.extend(rows_as_text(&first));
    for _ in 0..20 {
        let Some(c) = cursor.take() else { break };
        let page = server.query(&format!(r#"MATCH LOG {{}} AFTER "{c}""#));
        paged.extend(rows_as_text(&page));
        cursor = page["meta"]["next_cursor"].as_str().map(str::to_string);
    }
    assert_eq!(cursor, None, "paging did not finish");

    // The same rows the answer holds when it is asked for whole, in the same order.
    let whole = server.query("MATCH LOG {} LIMIT 100000");
    assert_eq!(whole["meta"]["limit_source"], "caller");
    assert_eq!(paged, rows_as_text(&whole));
}

fn rows_as_text(result: &serde_json::Value) -> Vec<String> {
    result["rows"]
        .as_array()
        .expect("rows is an array")
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// The viewer is one file, and this is the claim.
///
/// "Nothing to deploy alongside" is the whole reason it is `include_str!`'d, and it stops
/// being true the moment a stylesheet, a script or a font is fetched from somewhere else -
/// at which point the tool works on the machine it was written on and fails on the network
/// it was needed on. Asserted here rather than trusted, because adding a `<script src>` is
/// a one-line change that nothing else would notice.
#[test]
fn the_viewer_fetches_nothing_else() {
    let server = Server::start();
    let (status, _, body) = request_raw(server.port, "GET", "/", b"");
    assert_eq!(status, 200);

    let page = body.to_lowercase();
    for forbidden in [
        "src=\"http",
        "src='http",
        "src=\"//",
        "href=\"http",
        "@import",
        "<link",
    ] {
        assert!(
            !page.contains(forbidden),
            "the viewer reaches outside itself: found `{forbidden}`"
        );
    }
    // The two it *is* allowed to fetch, and the only two: its own server's endpoints.
    assert!(page.contains("fetch(\"/query\""));
    assert!(page.contains("fetch(\"/templates\""));
}

// ── Compressed bodies ──────────────────────────────────────────────────────────────

/// gzip in, the same answer out - line numbers included.
///
/// The line numbers are the point, not the count: a decoder that dropped a member or
/// mangled a boundary would still report a plausible `ingested`. Only the three known-bad
/// *positions* pin that the reader saw the body that was sent.
#[test]
fn a_gzipped_body_ingests_identically() {
    let corpus = corpus();
    let compressed = gzip(&corpus);
    assert!(
        compressed.len() * 4 < corpus.len(),
        "the corpus did not compress, so this test is not testing what it says"
    );

    let plain = Server::start();
    let (plain_status, plain_body) = plain.post("/ingest", &corpus);

    let zipped = Server::start();
    let (status, _, body) = request_with(
        zipped.port,
        "POST",
        "/ingest",
        &compressed,
        &[("Content-Encoding", "gzip")],
    );

    assert_eq!(status, plain_status);
    assert_eq!(
        json(&body),
        json(&plain_body),
        "a gzipped body answered differently from the same body uncompressed"
    );
    assert_eq!(zipped.query(r#"COUNT LOG {}"#)["count"], 497);
}

/// `x-gzip` is the same encoding under the name older clients use for it.
#[test]
fn x_gzip_is_gzip() {
    let server = Server::start();
    let (status, _, body) = request_with(
        server.port,
        "POST",
        "/ingest",
        &gzip(&corpus()),
        &[("Content-Encoding", "x-gzip")],
    );
    assert_eq!(status, 200);
    assert_eq!(json(&body)["ingested"], 497);
}

/// A body that says gzip and is not is a 400 - and the server is still there afterwards.
///
/// The second half is the half worth having. `/ingest` is reachable by anything that can
/// open a socket, and a decoder that unwinds on malformed input turns one bad shipper
/// config into everyone's outage. So each body is followed by a `/health`.
#[test]
fn a_corrupt_gzip_body_is_a_400_and_not_a_panic() {
    let server = Server::start();
    let good = gzip(&corpus());

    let bodies: [(&str, Vec<u8>); 4] = [
        ("not gzip at all", b"{\"event\":\"login\"}\n".to_vec()),
        ("right magic, wrong rest", {
            let mut bytes = vec![0x1f, 0x8b, 0x08, 0x00];
            bytes.extend_from_slice(b"and then nonsense");
            bytes
        }),
        // What a shipper killed mid-send produces - the one of these four that is an
        // operational event rather than a hostile one.
        ("truncated", good[..good.len() / 2].to_vec()),
        // Valid gzip whose trailer was tampered with - decodes, then fails the CRC.
        ("bad crc", {
            let mut bytes = good.clone();
            let last = bytes.len() - 5;
            bytes[last] ^= 0xff;
            bytes
        }),
    ];

    for (why, body) in bodies {
        let (status, _, text) = request_with(
            server.port,
            "POST",
            "/ingest",
            &body,
            &[("Content-Encoding", "gzip")],
        );
        assert_eq!(status, 400, "`{why}` answered {status}: {text}");
        let report = json(&text);
        assert_eq!(report["ok"], false);
        assert!(
            report["error"].as_str().is_some_and(|e| e.contains("gzip")),
            "`{why}` does not name gzip, so a shipper cannot tell what to fix: {report}"
        );

        let (status, health) = server.get("/health");
        assert_eq!(status, 200, "the server did not survive `{why}`: {health}");
    }

    // Nothing from any of the four was stored - they failed before a line was read.
    assert_eq!(json(&server.get("/stats").1)["storage"]["rows"], 0);
}

/// An encoding this server cannot decode is a 415, and the message says what it can.
#[test]
fn an_unknown_content_encoding_is_415() {
    let server = Server::start();
    for encoding in ["br", "deflate", "zstd", "gzip, gzip"] {
        let (status, _, body) = request_with(
            server.port,
            "POST",
            "/ingest",
            &corpus(),
            &[("Content-Encoding", encoding)],
        );
        assert_eq!(status, 415, "`{encoding}` answered {status}: {body}");
        let error = json(&body)["error"].to_string();
        assert!(
            error.contains("gzip"),
            "the refusal does not say what is accepted: {error}"
        );
    }

    // `identity` is not an unknown encoding, it is the explicit spelling of "none".
    let (status, _, body) = request_with(
        server.port,
        "POST",
        "/ingest",
        &corpus(),
        &[("Content-Encoding", "identity")],
    );
    assert_eq!(status, 200, "identity was refused: {body}");
}

// ── The two caps ───────────────────────────────────────────────────────────────────

/// The wire cap, measured on what arrived.
#[test]
fn a_body_over_the_wire_cap_is_413() {
    let server = Server::start_with(&["--max-body-bytes", "4096"]);

    // `request_early`, because this is the other answer that arrives before the body has
    // finished being sent - see its note on why `Connection: close` loses it.
    let (status, _, body) = request_early(server.port, "/ingest", &corpus());
    assert_eq!(status, 413, "an oversized body answered {status}: {body}");
    let report = json(&body);
    assert_eq!(report["ok"], false);
    // The cap itself, not just that one was hit: a shipper has to split its batch, and
    // cannot pick a new size from a refusal that names no number.
    assert!(
        report["error"].as_str().is_some_and(|e| e.contains("4096")),
        "the refusal does not say what the limit is: {report}"
    );

    // Under the cap still works, so the test is about the cap and not about the endpoint.
    let (status, _) = server.post("/ingest", b"{\"event\":\"login\"}\n");
    assert_eq!(status, 200);
}

/// The decompressed cap, measured while inflating - the check the wire cap cannot make.
///
/// ~8 KiB on the wire, 8 MiB inflated. It passes the wire cap by three orders of
/// magnitude, which is the shape of the attack: the first check cannot see it, and without
/// the second the process allocates until it dies.
#[test]
fn a_gzip_bomb_is_refused_while_inflating() {
    let server = Server::start_with(&["--max-decompressed-bytes", "1048576"]);

    let line = b"{\"event\":\"bomb\",\"pad\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}\n";
    let mut huge = Vec::with_capacity(8 * 1024 * 1024);
    while huge.len() < 8 * 1024 * 1024 {
        huge.extend_from_slice(line);
    }
    let bomb = gzip(&huge);
    assert!(
        bomb.len() < 64 * 1024,
        "the bomb is {} bytes on the wire, which is not a bomb",
        bomb.len()
    );

    let (status, _, body) = request_with(
        server.port,
        "POST",
        "/ingest",
        &bomb,
        &[("Content-Encoding", "gzip")],
    );
    assert_eq!(status, 413, "the bomb answered {status}: {body}");

    // Refused *while* inflating: nothing reached a store. A 413 alone would also be
    // printed by a server that ingested every line and then complained.
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["storage"]["rows"], 0);
    assert_eq!(stats["ingest"]["lines_read"], 0);

    // A body that inflates to under the cap is untouched by any of this.
    let (status, _, body) = request_with(
        server.port,
        "POST",
        "/ingest",
        &gzip(&corpus()),
        &[("Content-Encoding", "gzip")],
    );
    assert_eq!(status, 200, "a legitimate gzipped body was refused: {body}");
}

// ── Backpressure ───────────────────────────────────────────────────────────────────

/// Past capacity, `/ingest` answers 503 with a `Retry-After` - it does not go quiet.
///
/// The status is what makes a shipper back off. What this replaces is not a different
/// status but *no answer*: the body is accepted, the request runs long, the shipper times
/// out, keeps the batch and opens another connection - so load on an already-slow server
/// goes up. That failure has no assertion that catches it after the fact, which is why
/// this pins the answer rather than a latency.
#[test]
fn past_capacity_ingest_sheds_with_retry_after() {
    // One permit, and bodies big enough that holding it is measurable.
    let server = Server::start_with(&["--ingest-concurrency", "1", "--max-body-bytes", "67108864"]);
    let port = server.port;

    let out = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .args(["gen-logs", "--lines", "60000", "--seed", "11"])
        .output()
        .expect("failed to run gen-logs");
    let body = std::sync::Arc::new(out.stdout);

    let threads: Vec<_> = (0..8)
        .map(|_| {
            let body = std::sync::Arc::clone(&body);
            std::thread::spawn(move || request_early(port, "/ingest", &body))
        })
        .collect();
    let answers: Vec<(u16, String, String)> =
        threads.into_iter().map(|t| t.join().unwrap()).collect();

    let shed: Vec<_> = answers
        .iter()
        .filter(|(status, ..)| *status == 503)
        .collect();
    assert!(
        !shed.is_empty(),
        "no body was shed with one permit and eight concurrent senders: {:?}",
        answers.iter().map(|(s, ..)| *s).collect::<Vec<_>>()
    );

    for (_, headers, text) in &shed {
        assert!(
            headers.contains("retry-after:"),
            "a 503 carries no Retry-After, so the shipper has to guess:\n{headers}"
        );
        let report = json(text);
        assert_eq!(report["ok"], false);
        assert!(
            report["error"].as_str().is_some_and(|e| !e.is_empty()),
            "the 503 body is not the shape every other failure here has: {report}"
        );
    }

    // Everything not shed succeeded: the permit is a queue, not a coin toss.
    for (status, _, text) in &answers {
        assert!(
            *status == 200 || *status == 503,
            "unexpected {status}: {text}"
        );
    }

    // A shed body is in none of the line counters - it was never read - so without this
    // it leaves no trace at all.
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["ingest"]["requests_shed"], shed.len() as u64);
    assert_eq!(stats["ingest"]["ingest_concurrency"], 1);
}
