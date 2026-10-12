//! `kibbr serve` from outside: every endpoint answers, a body with three bad lines in it
//! reports exactly those three, and a signal mid-request exits 0.
//!
//! **Two ways a server is started here.** A test that only asks a server things over
//! HTTP runs one in this process (`Served`): the server crate's `run`, on a thread, given
//! the settings the command line would have resolved and a token to stop it by. A test
//! that needs a process starts the binary (`Server`): one that sends a signal, kills it,
//! reads its exit code or what it wrote to stderr as it stopped, or starts it again on
//! the directory it left.
//!
//! HTTP is spoken over a bare `TcpStream` rather than through a client crate. The
//! contract is a status line and a JSON body, and a test that asserts on
//! those directly is a test that cannot be satisfied by a client library's idea of what
//! the server meant. `Connection: close` makes the body "read to EOF", which is the whole
//! of the parsing needed.
//!
//! The corpus comes from `gen-logs` - real generated data, seeded, with the
//! three corruptions written in at known line numbers.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use flate2::write::GzEncoder;
use flate2::Compression;
use kibbr::config::{self, ServeFlags};
use tokio_util::sync::CancellationToken;

/// Lines in the ingest body, and the three that are broken.
const LINES: usize = 500;
const BAD: [usize; 3] = [137, 400, 461];

// ── A running server: the binary, as a process ─────────────────────────────────────

struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
    data_dir: Option<PathBuf>,
    /// Everything the server said before it listened: what a start found, and the banner.
    banner: String,
}

/// A data directory of this test's own.
///
/// Storage defaults to disk, so a server started with no `--data-dir` would write into
/// whatever directory `cargo test` happened to run from - and every test in this file
/// would then share one database. Each gets its own, removed when the server is dropped.
fn scratch_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kibbr-serve-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

impl Server {
    /// Start on port 0 and read back the port the OS chose.
    ///
    /// Not a fixed port: two of these tests run concurrently under `cargo test`, and
    /// 7700 is also where a developer's own `kibbr serve` is. The banner prints the
    /// bound address precisely so something can read it.
    fn start() -> Self {
        Self::start_with(&[])
    }

    /// The same, with extra flags - `--max-rows` is the one this needs, and a backstop the
    /// corpus can actually reach has to be smaller than the 10,000-row default.
    fn start_with(extra: &[&str]) -> Self {
        let memory = extra.windows(2).any(|pair| pair == ["--storage", "memory"]);
        Self::start_on((!memory).then(scratch_dir), extra)
    }

    /// The same, on a data directory an earlier server of this test left: a restart.
    fn start_on(data_dir: Option<PathBuf>, extra: &[&str]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
        command.args(["serve", "--port", "0"]);
        if let Some(dir) = &data_dir {
            command.args(["--data-dir", &dir.display().to_string()]);
        }
        let mut child = command
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn kibbr serve");

        let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
        let mut port = None;
        let mut banner = String::new();
        // Past the banner's own length: a start after a kill says what it read back from
        // the write-ahead log before the banner.
        for _ in 0..80 {
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
        // The banner is the one place a person is told what survives, so its saying so is
        // part of the contract and not decoration. The two modes say different things and
        // both have to say theirs - see `the_banner_names_what_survives_in_either_mode`.
        assert!(
            banner.contains("STORAGE IS IN MEMORY") || banner.contains("storage     "),
            "banner does not say where storage is:\n{banner}"
        );

        Self {
            child,
            port,
            stderr,
            data_dir,
            banner,
        }
    }

    /// `kill -9`, and the data directory as it was left: nothing flushed, nothing sealed,
    /// no checkpoint moved.
    fn kill(mut self) -> PathBuf {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.data_dir.take().expect("a server with a directory")
    }

    /// `wait`, keeping the data directory for a restart.
    fn wait_keeping(mut self) -> (Option<i32>, String, PathBuf) {
        let dir = self.data_dir.take().expect("a server with a directory");
        let (code, log) = self.wait();
        (code, log, dir)
    }

    fn data_dir(&self) -> &Path {
        self.data_dir
            .as_deref()
            .expect("this server was started in memory")
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
        let status = self.child.wait().expect("failed to wait for kibbr serve");
        let mut log = String::new();
        let _ = self.stderr.read_to_string(&mut log);
        // Already reaped; stop `Drop` from killing a pid that may have been reused. The
        // data directory is left on `self`, so `Drop` still removes it.
        self.child = Command::new("true").spawn().expect("failed to spawn true");
        let _ = self.child.wait();
        (status.code(), log)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(dir) = &self.data_dir {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

// ── A running server: the library, in this process ─────────────────────────────────

/// A server run by `kibbr_server::run` on a thread of this process, for a test that only
/// asks it things over HTTP: what `Server` is, without the process.
///
/// **Its settings are the ones the command line resolves**: `config::settings`, given
/// the flags a test would have typed, so a default here is the binary's default and a
/// flag is refused or floored as the binary would.
///
/// **It is stopped as a signal stops one** - the token cancelled, the in-flight requests
/// waited for, the final flush, the checkpoint - and a stop that fails fails the test.
/// `Server`'s drop is a `kill -9`.
struct Served {
    port: u16,
    data_dir: Option<PathBuf>,
    /// Everything the server said before it listened: what a start found, and the banner.
    banner: String,
    cancel: CancellationToken,
    run: Option<std::thread::JoinHandle<kibbr_server::Result<kibbr_server::Report>>>,
}

/// What one thread's events say, each as its message. A server says its start and its
/// stop on the thread `run` was called on, and the banner is one of those events: this
/// is how a test hears the port, where the binary's own subscriber writes it to stderr.
struct Hear(std::sync::Mutex<std::sync::mpsc::Sender<String>>);

impl tracing::Subscriber for Hear {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        // Nobody listens once the banner has been read, and that is not an error.
        if let Ok(said) = self.0.lock() {
            let _ = said.send(message.0);
        }
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

impl Served {
    /// On a data directory of its own, and whichever port the system gives.
    fn start() -> Self {
        Self::start_with(ServeFlags::default())
    }

    /// The same, with flags as `kibbr serve` takes them. The port and the directory are
    /// this function's: `--storage memory` is the one way to have no directory.
    fn start_with(flags: ServeFlags) -> Self {
        let data_dir = (!flags.memory).then(scratch_dir);
        let flags = ServeFlags {
            port: Some(0),
            data_dir: data_dir.clone(),
            ..flags
        };
        let settings = config::settings(&config::File::default(), flags, config::Log::default())
            .expect("the flags were refused");
        let cancel = CancellationToken::new();
        let (said, heard) = std::sync::mpsc::channel();
        let run = {
            let cancel = cancel.clone();
            std::thread::spawn(move || {
                let _hearing = tracing::subscriber::set_default(Hear(std::sync::Mutex::new(said)));
                kibbr_server::run(settings, cancel)
            })
        };

        let mut banner = String::new();
        let port = loop {
            // Ends without a banner only when `run` has: the sender is the thread's.
            let Ok(line) = heard.recv_timeout(std::time::Duration::from_secs(60)) else {
                panic!(
                    "no listening line in banner:\n{banner}\nthe run: {:?}",
                    run.is_finished().then(|| run.join())
                );
            };
            banner.push_str(&line);
            banner.push('\n');
            if let Some(at) = line.find("http://127.0.0.1:") {
                let tail = &line[at + "http://127.0.0.1:".len()..];
                let digits = tail.bytes().take_while(u8::is_ascii_digit).count();
                break tail[..digits].parse::<u16>().expect("the port");
            }
        };
        // As `Server::start_on` holds the binary's banner to.
        assert!(
            banner.contains("max rows"),
            "banner does not name the backstop:\n{banner}"
        );
        assert!(
            banner.contains("STORAGE IS IN MEMORY") || banner.contains("storage     "),
            "banner does not say where storage is:\n{banner}"
        );

        Self {
            port,
            data_dir,
            banner,
            cancel,
            run: Some(run),
        }
    }

    fn data_dir(&self) -> &Path {
        self.data_dir
            .as_deref()
            .expect("this server was started in memory")
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
}

impl Drop for Served {
    fn drop(&mut self) {
        self.cancel.cancel();
        let ended = self.run.take().map(std::thread::JoinHandle::join);
        if let Some(dir) = &self.data_dir {
            let _ = fs::remove_dir_all(dir);
        }
        // A test that is already failing says why itself.
        if !std::thread::panicking() {
            match ended {
                Some(Ok(Ok(report))) => assert_eq!(report.addr.port(), self.port),
                other => panic!("the server did not stop cleanly: {other:?}"),
            }
        }
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
    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
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
    let server = Served::start();

    let (status, headers, body) = request_raw(server.port, "GET", "/", b"");
    assert_eq!(status, 200);
    assert!(
        headers.contains("content-type: text/html"),
        "GET / is not served as HTML:\n{headers}"
    );
    assert!(body.contains("<title>"), "GET / answered no page:\n{body}");

    // `ok` for liveness and `status` for whether the store can write - two fields,
    // because a process that cannot write is still a process worth leaving alive.
    let (status, body) = server.get("/health");
    assert_eq!(status, 200);
    assert_eq!(
        json(&body),
        serde_json::json!({ "ok": true, "status": "ok" })
    );

    // Empty, before anything is ingested: a fresh database is a real state and every
    // endpoint has to have an answer for it rather than a division by zero.
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["storage"]["rows"], 0);
    assert_eq!(stats["storage"]["durable"], true);
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
    // Still says where the rows live as a field, not only in a banner nobody scrolls
    // back to.
    assert_eq!(stats["storage"]["kind"], "disk");
    assert_eq!(stats["storage"]["durable"], true);
    assert!(
        !stats["storage"]["base_path"].is_null(),
        "a durable server does not report where it writes: {stats}"
    );

    let answer = server.query(r#"COUNT LOG {}"#);
    assert_eq!(answer["count"], (LINES - BAD.len()) as u64);
    assert_eq!(answer["exists"], true);
}

/// The criterion, exactly: three bad lines out of five hundred report those three,
/// with the line numbers they actually sit on.
#[test]
fn three_bad_lines_report_exactly_those_three() {
    let server = Served::start();
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

    // **In line order, and that is a contract now rather than an accident.** It used to
    // be one pass numbering its rejections in the order it met them; parsing moved out of
    // the store lock, so a body is now met twice - once to parse and once to apply -
    // and the report has to put the two runs back into the order an operator would count
    // them in.
    assert!(
        lines.windows(2).all(|pair| pair[0] < pair[1]),
        "the report is not in line order: {lines:?}"
    );

    // The isolation half of the contract: the other 497 are queryable, not lost with them.
    assert_eq!(server.query(r#"COUNT LOG {}"#)["count"], 497);
}

/// A clean body reports no errors at all - the `{"ok":true,...,"errors":[]}` half of the
/// shape, which a test that only ever sends broken input would never check.
#[test]
fn a_clean_body_reports_no_errors() {
    let server = Served::start();
    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
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
    let server = Served::start();

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

    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
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
    // Every step of the shutdown is named, the write-ahead log's two among them, so an
    // operator can see what was done on the way out.
    assert!(
        log.contains("WAL checkpoint") && log.contains("close WAL"),
        "shutdown log does not name the WAL's steps:\n{log}"
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
    let server = Served::start_with(ServeFlags {
        max_rows: Some(50),
        ..ServeFlags::default()
    });
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
    let server = Served::start();
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

    let plain = Served::start();
    let (plain_status, plain_body) = plain.post("/ingest", &corpus);

    let zipped = Served::start();
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
    let server = Served::start();
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
    let server = Served::start();
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
    let server = Served::start();
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
    let server = Served::start_with(ServeFlags {
        max_body_bytes: Some(4096),
        ..ServeFlags::default()
    });

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
    let server = Served::start_with(ServeFlags {
        max_decompressed_bytes: Some(1_048_576),
        ..ServeFlags::default()
    });

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
    let server = Served::start_with(ServeFlags {
        ingest_concurrency: Some(1),
        max_body_bytes: Some(67_108_864),
        ..ServeFlags::default()
    });
    let port = server.port;

    // Big enough that one body holds the permit for far longer than the 100ms a queued
    // sender waits, and small enough that the one body that gets the permit still answers
    // inside `request_early`'s read timeout - a debug build walking hostile lines is the
    // slowest this ever runs, and these lines got slower: the generator draws ten-digit
    // ids across the whole ten-digit range, so those columns now hold both the type a
    // value inside the epoch window gets and the type one outside it gets, and a column
    // carrying two types seals a segment every time the type changes.
    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["gen-logs", "--lines", "20000", "--seed", "11"])
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

/// With no wait at all the shed is the one it was with a hundred milliseconds, and at
/// once: a body that finds no permit free is answered `503` with a `Retry-After`, every
/// body is answered one way or the other, and the count of sheds is the count of `503`s.
#[test]
fn with_no_wait_a_body_past_capacity_is_shed_at_once() {
    let server = Served::start_with(ServeFlags {
        ingest_concurrency: Some(1),
        ingest_wait_ms: Some(0),
        max_body_bytes: Some(67_108_864),
        ..ServeFlags::default()
    });
    let port = server.port;
    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["gen-logs", "--lines", "20000", "--seed", "11"])
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

    let statuses: Vec<u16> = answers.iter().map(|(status, ..)| *status).collect();
    let shed: Vec<_> = answers
        .iter()
        .filter(|(status, ..)| *status == 503)
        .collect();
    assert!(
        !shed.is_empty(),
        "no body was shed with one permit, no wait and eight concurrent senders: {statuses:?}"
    );
    assert!(
        statuses.iter().all(|status| [200, 503].contains(status)),
        "{statuses:?}"
    );
    for (_, headers, text) in &shed {
        assert!(headers.contains("retry-after:"), "{headers}");
        let report = json(text);
        assert_eq!(report["ok"], false);
        assert!(
            report["error"].as_str().is_some_and(|e| !e.is_empty()),
            "{report}"
        );
    }

    let stats = json(&server.get("/stats").1);
    let ingest = &stats["ingest"];
    assert_eq!(ingest["requests_shed"], shed.len() as u64, "{ingest}");
    assert_eq!(ingest["ingest_wait_ms"], 0, "{ingest}");
    // Each under its cause, and the causes add up to the total.
    let causes: u64 = ingest["shed"]
        .as_object()
        .expect("the causes")
        .values()
        .filter_map(serde_json::Value::as_u64)
        .sum();
    assert_eq!(causes, shed.len() as u64, "{ingest}");
}

/// A wait past thirty seconds would keep a body longer than its sender waits for an
/// answer, and a log that may hold nothing would refuse every body after the first: the
/// server says so and does not start, by the flag and by the variable.
#[test]
fn a_wait_past_thirty_seconds_and_a_log_bound_of_nothing_are_refused_at_the_start() {
    let refused = |flag: &[&str], variable: Option<(&str, &str)>, want: &str| {
        let dir = scratch_dir();
        let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
        command
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &dir.display().to_string()])
            .args(flag);
        if let Some((name, value)) = variable {
            command.env(name, value);
        }
        let out = command.output().expect("failed to run kibbr serve");
        let _ = fs::remove_dir_all(&dir);
        let said = String::from_utf8_lossy(&out.stderr).into_owned();
        assert_eq!(out.status.code(), Some(1), "{said}");
        assert!(said.contains(want), "{said}");
        assert!(!said.contains("listening"), "{said}");
    };
    let too_long = "ingest wait (30001ms) is above the 30000ms limit";
    refused(&["--ingest-wait-ms", "30001"], None, too_long);
    refused(&[], Some(("KIBBR__API__INGEST_WAIT_MS", "30001")), too_long);
    let nothing = "wal queue bytes must be at least 1 byte";
    refused(&["--wal-queue-bytes", "0"], None, nothing);
    refused(&[], Some(("KIBBR__STORAGE__WAL_QUEUE_BYTES", "0")), nothing);
}

/// What was resolved is said where an operator looks for it: `/stats`, and the banner.
/// With nothing set, the hundred milliseconds and the 256 MiB a server always had, and no
/// bound on the line at the door, which never had one; with the wait raised, the bound
/// that comes with it - as many as may be between the door and their answer.
#[test]
fn stats_and_the_banner_say_the_wait_and_the_logs_bound_that_were_resolved() {
    let server = Served::start();
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["ingest"]["ingest_wait_ms"], 100, "{stats}");
    assert_eq!(
        stats["storage"]["wal"]["queue_max_bytes"], 268_435_456,
        "{stats}"
    );
    assert_eq!(
        stats["ingest"]["door"]["line_max"],
        serde_json::Value::Null,
        "{stats}"
    );
    assert!(
        server.banner.contains("100ms") && server.banner.contains("268,435,456 bytes"),
        "{}",
        server.banner
    );
    assert!(
        !server.banner.contains("wait at the door"),
        "{}",
        server.banner
    );

    let server = Served::start_with(ServeFlags {
        ingest_wait_ms: Some(250),
        wal_queue_bytes: Some(1_048_576),
        ..ServeFlags::default()
    });
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["ingest"]["ingest_wait_ms"], 250, "{stats}");
    assert_eq!(
        stats["storage"]["wal"]["queue_max_bytes"], 1_048_576,
        "{stats}"
    );
    let places = stats["ingest"]["ingest_bodies"].as_u64().expect("places");
    assert_eq!(stats["ingest"]["door"]["line_max"], places, "{stats}");
    assert!(
        server.banner.contains("250ms") && server.banner.contains("1,048,576 bytes"),
        "{}",
        server.banner
    );
    assert!(
        server
            .banner
            .contains(&format!("at most {places} wait at the door at once")),
        "{}",
        server.banner
    );
}

/// Bodies back to back onto buffers that hold next to nothing, with a wait long enough to
/// ride out any flush: every one is taken, and none is shed.
///
/// Each body leaves the buffers past twice their limit, so the one behind it finds them
/// full at the door. It used to be refused there at once, though the flush that would let
/// it in was already running; now it waits for that flush, and the flush says when it is
/// done. Were that never said, a body at the door would wait out its thirty seconds and
/// be refused, and this would not see fifty `200`s.
#[test]
fn bodies_wait_at_the_door_for_the_flush_and_every_one_lands() {
    const BODIES: usize = 50;
    const PER_BODY: usize = 20;
    let server = Served::start_with(ServeFlags {
        max_buffer_bytes: Some(1024),
        ingest_wait_ms: Some(30_000),
        ..ServeFlags::default()
    });
    for nth in 0..BODIES {
        let body: String = (0..PER_BODY)
            .map(|n| format!("{{\"ts\": 1705314600, \"n\": {}}}\n", nth * PER_BODY + n))
            .collect();
        let (status, text) = server.post("/ingest", body.as_bytes());
        assert_eq!(status, 200, "body {nth}: {text}");
    }
    let stats = json(&server.get("/stats").1);
    let ingest = &stats["ingest"];
    assert_eq!(ingest["requests_shed"], 0, "{ingest}");
    assert_eq!(ingest["lines_ingested"], (BODIES * PER_BODY) as u64);
    // How many of the fifty had to wait is the flush's speed against the sender's, and
    // is said and not asserted.
    eprintln!("the door, after {BODIES} bodies: {}", ingest["door"]);
    assert_eq!(
        server.query("COUNT LOG {}")["count"],
        (BODIES * PER_BODY) as u64
    );
}

// ── When the store cannot write ────────────────────────────────────────────────────

/// `/stats` waited on until a predicate holds, or the timeout runs out.
fn wait_for_stats(
    port: u16,
    what: &str,
    mut ready: impl FnMut(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    for _ in 0..200 {
        last = json(&request(port, "GET", "/stats", b"").1);
        if ready(&last) {
            return last;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("timed out waiting for {what}; last /stats was {last}");
}

/// `data/` and every day directory in it read-only, returned so they can be unlocked.
#[cfg(unix)]
fn lock_data(base: &Path) -> Vec<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let data = base.join("data");
    let mut locked: Vec<PathBuf> = fs::read_dir(&data)
        .expect("data dir")
        .flatten()
        .map(|day| day.path())
        .filter(|path| path.is_dir())
        .collect();
    locked.push(data);
    for path in &locked {
        fs::set_permissions(path, fs::Permissions::from_mode(0o555)).expect("chmod");
    }
    locked
}

/// A store that cannot write refuses nothing and loses nothing.
///
/// **The rule that is easy to get backwards.** A line is in the write-ahead log, flushed,
/// before it is acknowledged, so a store that then cannot take its row has failed at
/// nothing the sender needs to hear about: `/ingest` goes on answering `200`, `/health`
/// says degraded, the shutdown exits 0, and the next start puts the rows back from the
/// log. Before the log it was the other way round, and had to be - a `200` meant
/// *buffered*, and on a full disk every batch got one and `COUNT LOG {}` was 0 after a
/// restart - so this used to assert a `503` and an exit code of 1.
///
/// The disk is made unwritable rather than full, which is the same errno's worth of
/// difference: `data/` and its day directories are set read-only after the first day's file
/// is open, and the second line is of the next day - whose file needs a new directory in
/// `data/`, which will not take one. The log is beside `data/`, and still writes.
#[test]
#[cfg(unix)]
fn a_store_that_cannot_write_refuses_nothing_and_loses_nothing() {
    use std::os::unix::fs::PermissionsExt;

    // A row group closes on the timeout, so one line per flush: the point is to get a
    // segment open before the directory is locked.
    let server = Server::start_with(&["--row-group-timeout-ms", "500"]);

    // One line, one template, one day. `x` is an integer here.
    let first = br#"{"ts": 1705314600, "x": 1}"#;
    assert_eq!(server.post("/ingest", first).0, 200);
    let stats = wait_for_stats(server.port, "the first line to reach a segment", |s| {
        s["storage"]["buffered_rows"] == 0
    });
    assert_eq!(stats["storage"]["degraded"], false);
    assert_eq!(json(&server.get("/health").1)["status"], "ok");

    // `data/` and every day directory read-only. The open file's descriptor still works;
    // what cannot happen any more is *creating* another day's file.
    let locked = lock_data(server.data_dir());
    assert!(locked.len() > 1, "no day directory was written");

    // The next day, which has no file and needs a directory `data/` will not take.
    let second = br#"{"ts": 1705401001, "x": "N/A"}"#;
    assert_eq!(server.post("/ingest", second).0, 200);
    let stats = wait_for_stats(server.port, "the flush tick to fail", |s| {
        s["storage"]["degraded"] == true
    });
    assert!(
        stats["storage"]["write_failures"].as_u64().unwrap_or(0) > 0,
        "degraded without a write failure behind it: {stats}"
    );

    // 1. `/health` says degraded and stays alive: a process that cannot write is not one
    //    worth restarting, and the rows already on the disk are still queryable.
    let (status, body) = server.get("/health");
    assert_eq!(status, 200);
    let health = json(&body);
    assert_eq!(health["status"], "degraded");
    assert_eq!(health["ok"], true);
    assert!(
        health["detail"].as_str().unwrap_or("").contains("leaf"),
        "/health does not say what the write failed with: {health}"
    );

    // 2. `/ingest` does not refuse. The line is durable in the log, so `200` is true.
    let third = br#"{"ts": 1705401002, "x": 3}"#;
    let (status, body) = server.post("/ingest", third);
    assert_eq!(
        status, 200,
        "a line the log took was refused because the store cannot write: {body}"
    );
    assert_eq!(
        body.trim(),
        r#"{"ok":true,"ingested":1,"errors":[]}"#,
        "the store's failure reached the sender"
    );

    // 3. Queries still answer, and see every line: the rows are in memory.
    assert_eq!(server.query("COUNT LOG {}")["count"], 3);

    // 4. The exit code is 0: nothing acknowledged was lost. The log says where the rows
    //    the final flush could not write are.
    server.signal("-TERM");
    let (code, log, dir) = server.wait_keeping();
    for path in &locked {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o755));
    }
    assert_eq!(
        code,
        Some(0),
        "a shutdown that lost nothing exited {code:?}\n{log}"
    );
    assert!(
        log.contains("FAILED") && log.contains("write-ahead log") && log.contains("exit 0"),
        "the shutdown log does not say where the unwritten rows are:\n{log}"
    );

    // 5. And the next start puts them back: every line, once.
    let server = Server::start_on(Some(dir), &["--row-group-timeout-ms", "500"]);
    assert_eq!(server.query("COUNT LOG {}")["count"], 3);
    let replay = json(&server.get("/stats").1)["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["applied"], 2, "the two unwritten rows: {replay}");
    assert_eq!(replay["covered"], 0, "{replay}");
    let mut xs: Vec<String> = server.query("MATCH LOG {}")["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["x"].to_string())
        .collect();
    xs.sort();
    assert_eq!(xs, ["\"N/A\"", "1", "3"]);
}

/// A sender that hangs up does not take its body's place with it. The body's task runs
/// on, and the permits are the task's: a second body is shed while the first is still
/// being parsed - where they used to go back at the hang-up, and a sender that timed out
/// and tried again had a second copy of its body parsed beside the first. And a stop
/// waits for such a body: the final flush stores it whole, where an exit half way
/// through would have stored whatever part of it had got that far.
#[test]
fn a_body_whose_sender_hung_up_keeps_its_place_and_is_stored_whole() {
    // Enough lines that the body is in hand for several times as long as the second
    // one takes to be refused: were it done first, this would test nothing.
    const ORPHANED: usize = 200_000;
    let server = Server::start_with(&["--ingest-concurrency", "1"]);
    let body: Vec<u8> = (0..ORPHANED)
        .flat_map(|n| format!("{{\"ts\": 1705314600, \"n\": {n}}}\n").into_bytes())
        .collect();
    {
        // Sent whole, and the socket closed with no answer read - a moment later, as a
        // sender that gives up waiting does. One that closes with the last byte is not
        // this case: the server drops such a request before it has read the body out.
        let mut socket = TcpStream::connect(("127.0.0.1", server.port)).expect("connect");
        let head = format!(
            "POST /ingest HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n",
            server.port,
            body.len()
        );
        socket.write_all(head.as_bytes()).expect("write");
        socket.write_all(&body).expect("write");
        socket.flush().expect("flush");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // The one permit is with that body's task, whoever is still listening for it.
    let (status, answer) = server.post("/ingest", br#"{"ts": 1705314600, "x": 1}"#);
    assert_eq!(
        status,
        503,
        "a body was let in beside the one in hand: {answer}\n{}",
        server.get("/stats").1
    );

    // Stopped now, with the task still at work: the stop waits for it, and its lines are
    // in row groups before the exit - not left in the log for the next start.
    server.signal("-TERM");
    let (code, log, dir) = server.wait_keeping();
    assert_eq!(code, Some(0), "{log}");
    let server = Server::start_on(Some(dir), &[]);
    assert_eq!(server.query("COUNT LOG {}")["count"], ORPHANED);
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["storage"]["wal"]["replay"]["records"], 0, "{stats}");
}

/// A body whose lines, as log records, are more than one write of the log holds is the
/// sender's to split: a `413` that says so, decided from the bytes before a line of it is
/// parsed. Twenty-six million `{}` lines are 78MB on the wire and 1.09GB as records; an
/// answer after the parse would have cost gigabytes of memory to refuse.
#[test]
fn a_body_past_what_the_log_writes_at_once_is_a_413_and_is_not_parsed() {
    let server = Served::start_with(ServeFlags {
        max_body_bytes: Some(100_000_000),
        ..ServeFlags::default()
    });
    let big = b"{}\n".repeat(26_000_000);
    let (status, body) = server.post("/ingest", &big);
    assert_eq!(status, 413, "{body}");
    let error = json(&body)["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        error.contains("write-ahead log") && error.contains("smaller bodies"),
        "the refusal does not say what to do: {error}"
    );
    let stats = json(&server.get("/stats").1);
    assert_eq!(stats["ingest"]["lines_read"], 0, "the body was parsed");
    assert_eq!(stats["ingest"]["requests_shed"], 0, "a 413 is not a shed");

    // And the log is as it was: the next body is taken.
    assert_eq!(
        server.post("/ingest", br#"{"ts": 1705314600, "x": 1}"#).0,
        200
    );
    assert_eq!(json(&server.get("/health").1)["status"], "ok");
}

/// The refusal that is left: a body the write-ahead log cannot take. Nothing of it is
/// stored. And the first write that succeeds clears it.
///
/// The log is made unable to write the way the store was: its directory read-only, with a
/// segment size of one byte so the next flush has to make a new segment there. The open
/// one still takes the first line.
#[test]
#[cfg(unix)]
fn a_log_that_cannot_write_refuses_and_the_first_write_clears_it() {
    use std::os::unix::fs::PermissionsExt;

    let server = Server::start_with(&["--wal-segment-bytes", "1"]);
    assert_eq!(
        server.post("/ingest", br#"{"ts": 1705314600, "x": 1}"#).0,
        200
    );
    let wal = server.data_dir().join("wal");
    fs::set_permissions(&wal, fs::Permissions::from_mode(0o555)).expect("chmod");

    // 1. The body that meets the failure: a 503, a Retry-After, and the log's error.
    let (status, headers, body) = request_raw(
        server.port,
        "POST",
        "/ingest",
        br#"{"ts": 1705314601, "x": 2}"#,
    );
    let _ = fs::set_permissions(&wal, fs::Permissions::from_mode(0o755));
    assert_eq!(
        status, 503,
        "a line the log could not take got {status}: {body}"
    );
    assert!(
        headers.to_ascii_lowercase().contains("retry-after:"),
        "the 503 carried no Retry-After:\n{headers}"
    );
    let refusal = json(&body);
    assert_eq!(refusal["ok"], false);
    let error = refusal["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("write-ahead log cannot write") && error.contains("seg_"),
        "the refusal does not carry the log's write error: {error}"
    );
    // Nothing has the line: not the log, and so not the store.
    assert_eq!(server.query("COUNT LOG {}")["count"], 1);

    // 2. The directory is writable again, and the next flush tick finds out: the door
    //    opens without a body having to be refused to learn it.
    let stats = wait_for_stats(server.port, "the log to write again", |s| {
        s["storage"]["wal"]["healthy"] == true
    });
    assert!(
        !stats["storage"]["wal"]["last_error"].is_null(),
        "the log's last error was forgotten once it could write: {stats}"
    );
    assert_eq!(json(&server.get("/health").1)["status"], "ok");
    assert_eq!(
        server.post("/ingest", br#"{"ts": 1705314602, "x": 3}"#).0,
        200,
        "still refusing after the log could write again"
    );
    assert_eq!(server.query("COUNT LOG {}")["count"], 2);

    server.signal("-TERM");
    let (code, log) = server.wait();
    assert_eq!(code, Some(0), "exited {code:?}\n{log}");
}

/// While the log cannot write, the door refuses a body before it is read - and says why.
#[test]
#[cfg(unix)]
fn while_the_log_cannot_write_the_door_refuses_and_health_says_why() {
    use std::os::unix::fs::PermissionsExt;

    // A tick every ten seconds: the log stays as the failed flush left it for the whole
    // test, where the default tick would ask it to try again.
    let server = Server::start_with(&["--wal-segment-bytes", "1", "--flush-interval-ms", "10000"]);
    let line = br#"{"ts": 1705314600, "x": 1}"#;
    assert_eq!(server.post("/ingest", line).0, 200);
    let wal = server.data_dir().join("wal");
    fs::set_permissions(&wal, fs::Permissions::from_mode(0o555)).expect("chmod");
    assert_eq!(server.post("/ingest", line).0, 503);
    // That body was read, and its line counted: it was the flush that failed.
    let read = json(&server.get("/stats").1)["ingest"]["lines_read"].clone();
    assert_eq!(read, 2);

    let (status, headers, body) = request_raw(server.port, "POST", "/ingest", line);
    let health = json(&server.get("/health").1);
    let after = json(&server.get("/stats").1)["ingest"]["lines_read"].clone();
    let _ = fs::set_permissions(&wal, fs::Permissions::from_mode(0o755));
    assert_eq!(status, 503);
    // This one was not: the door refused it before a byte of it was looked at. The same
    // 503 from the handler would have read the line first.
    assert_eq!(after, read, "the door let the body in to be refused");
    assert!(headers.to_ascii_lowercase().contains("retry-after:"));
    let error = json(&body)["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        error.contains("the write-ahead log cannot write") && error.contains("seg_"),
        "the refusal does not carry the log's write error: {error}"
    );
    assert_eq!(health["status"], "degraded");
    assert_eq!(health["ok"], true);
    assert!(
        health["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("write-ahead log"),
        "{health}"
    );
}

/// A server whose stderr the test holds both ends of, where `Server` holds the end it
/// reads. Killed, and its log's directory made writable again so that it can be removed,
/// however the test ends.
#[cfg(unix)]
struct Unheard {
    child: Child,
    dir: PathBuf,
}

#[cfg(unix)]
impl Drop for Unheard {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;

        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::set_permissions(self.dir.join("wal"), fs::Permissions::from_mode(0o755));
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// One request on a thread of its own, and where its answer will arrive: for a caller
/// that waits only so long, so that a server which does not answer is a finding and not a
/// test that never ends.
#[cfg(unix)]
fn request_apart(
    port: u16,
    method: &'static str,
    path: &'static str,
    body: &'static [u8],
) -> std::sync::mpsc::Receiver<(u16, String)> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(request(port, method, path, body));
    });
    rx
}

/// The log's first failure is named from the first refusal on: by the door's `503`, by
/// `/stats`, by `/health`, and to the body that met it. None of them is told the log
/// cannot write before there is a reason to give, and none waits for the line about it
/// to reach stderr.
///
/// So stderr is made unable to take a byte before the log fails. Whatever the server
/// does only once it has written that line has then not happened, for as long as this
/// looks: a reason stored after it is not there to be read, and a body refused after it
/// is not answered.
#[test]
#[cfg(unix)]
fn the_first_503_names_the_logs_write_error_and_so_do_stats_and_health() {
    use std::io::ErrorKind;
    use std::os::fd::OwnedFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    /// How long an answer is waited for. With stderr stopped, an answer that does not
    /// come is what this test is looking for.
    const ANSWER: Duration = Duration::from_secs(5);

    // A socket pair where `Server` has a pipe: a socket can be told not to wait, so the
    // test learns that it is full from the write that would have had to, and not from
    // how long a write has been taking.
    let (ours, theirs) = UnixStream::pair().expect("socket pair");
    let dir = scratch_dir();
    let child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["serve", "--port", "0", "--data-dir"])
        .arg(&dir)
        // One byte, so the second flush has to make a new segment; and a tick every ten
        // seconds, so nothing asks the log to try again while this looks at it.
        .args(["--wal-segment-bytes", "1", "--flush-interval-ms", "10000"])
        .stdout(Stdio::null())
        .stderr(OwnedFd::from(theirs.try_clone().expect("clone")))
        .spawn()
        .expect("failed to spawn kibbr serve");
    let server = Unheard {
        child,
        dir: dir.clone(),
    };
    // This test holds the server's end too, so a server that died would not end the
    // banner: the wait for it has to.
    ours.set_read_timeout(Some(Duration::from_secs(60)))
        .expect("timeout");
    let mut stderr = BufReader::new(ours);
    let mut port = None;
    let mut banner = String::new();
    for _ in 0..80 {
        let mut line = String::new();
        if stderr.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        banner.push_str(&line);
        if let Some(at) = line.find("http://127.0.0.1:") {
            port = line[at + "http://127.0.0.1:".len()..].trim().parse().ok();
        }
        if line.contains("shutdown ") {
            break;
        }
    }
    let port: u16 = port.unwrap_or_else(|| panic!("no listening line in banner:\n{banner}"));

    let line = br#"{"ts": 1705314600, "x": 1}"#;
    assert_eq!(request(port, "POST", "/ingest", line).0, 200);
    let wal = dir.join("wal");
    fs::set_permissions(&wal, fs::Permissions::from_mode(0o555)).expect("chmod");

    // Stderr, filled to the last byte: in blocks, and then a byte at a time, until a
    // write says it would have to wait. The server's end is this same socket, and waits
    // again once this has stopped asking it not to.
    theirs.set_nonblocking(true).expect("nonblocking");
    for block in [&[b'\n'; 1024][..], b"\n"] {
        loop {
            match (&theirs).write(block) {
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => panic!("filling the server's stderr: {e}"),
            }
        }
    }
    theirs.set_nonblocking(false).expect("blocking");

    // The body that meets the failure.
    let met = request_apart(port, "POST", "/ingest", line);
    let ask = |method, path, body| {
        request_apart(port, method, path, body)
            .recv_timeout(ANSWER)
            .unwrap_or_else(|_| panic!("{method} {path} was not answered in {ANSWER:?}"))
    };
    let named = |text: &str| text.contains("seg_") && text.contains("os error 13");
    let mut wrong = Vec::new();

    // 1. The moment `/stats` says the log cannot write, it says why.
    let deadline = Instant::now() + Duration::from_secs(20);
    let stats = loop {
        let stats = json(&ask("GET", "/stats", b"").1);
        if stats["storage"]["wal"]["healthy"] == false {
            break stats;
        }
        assert!(Instant::now() < deadline, "the log never failed: {stats}");
        std::thread::sleep(Duration::from_millis(5));
    };
    let reason = &stats["storage"]["wal"]["last_error"];
    if !reason.as_str().is_some_and(named) {
        wrong.push(format!(
            "/stats: wal.healthy is false and wal.last_error is {reason}"
        ));
    }
    // 2. The door, which refuses a body before it is read.
    let (status, body) = ask("POST", "/ingest", line);
    if status != 503 || !named(&body) {
        wrong.push(format!("the door: {status} {body}"));
    }
    // 3. `/health`.
    let health = ask("GET", "/health", b"").1;
    if !(health.contains("degraded") && named(&health)) {
        wrong.push(format!("/health: {health}"));
    }
    // 4. And the body that met the failure has its answer, with stderr still not moving.
    let answered = met.recv_timeout(ANSWER);
    match &answered {
        Ok((503, body)) if named(body) => {}
        Ok((status, body)) => wrong.push(format!("the body that met the failure: {status} {body}")),
        Err(_) => wrong.push(format!(
            "the body that met the failure was not answered in {ANSWER:?}: its 503 waits on \
             stderr"
        )),
    }

    // Stderr is let go, whatever was found, so a server still holding that body can
    // answer it before it is killed.
    drop(theirs);
    let drain = std::thread::spawn(move || {
        let _ = std::io::copy(&mut stderr, &mut std::io::sink());
    });
    if answered.is_err() {
        let _ = met.recv_timeout(ANSWER);
    }
    drop(server);
    let _ = drain.join();
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

// ── After a kill ───────────────────────────────────────────────────────────────────

/// `kill -9` loses no acknowledged line and stores none twice - whichever side of the
/// checkpoint the line is on. `scripts/fault/crash_test.sh` is the full version of this, twenty
/// kills at drawn moments; this is the two cases it rests on, each made on purpose.
///
/// **Rows in a row group, and a checkpoint that does not know.** Every line is flushed
/// into a row group, the server is killed, and the checkpoint file is deleted: the log
/// still holds every line, and nothing but the row groups' own ranges says they are
/// stored. A replay that went by the checkpoint would store all of them again.
///
/// **Rows in memory only.** Then three lines more, under the default wait so no row group
/// takes them, and a kill: they come back from the log, and the rest are skipped again.
/// The log's newest segment is given a torn record first - a frame whose length says more
/// than is there, as a write the kill cut short leaves - and the start must warn and
/// carry on.
#[test]
fn a_kill_loses_no_acknowledged_line_and_stores_none_twice() {
    let fast = ["--row-group-timeout-ms", "100", "--flush-interval-ms", "50"];
    let good = (LINES - BAD.len()) as u64;

    let server = Server::start_with(&fast);
    assert_eq!(server.post("/ingest", &corpus()).0, 200);
    wait_for_stats(server.port, "every row to reach a row group", |s| {
        s["storage"]["buffered_rows"] == 0
    });
    let dir = server.kill();
    let _ = fs::remove_file(dir.join("wal").join("checkpoint"));

    let server = Server::start_on(Some(dir), &[]);
    assert_eq!(
        server.query("COUNT LOG {}")["count"],
        good,
        "rows already in a row group were stored again by the replay"
    );
    let replay = json(&server.get("/stats").1)["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["records"], good, "{replay}");
    assert_eq!(replay["covered"], good, "{replay}");
    assert_eq!(replay["applied"], 0, "{replay}");

    let more = b"{\"ts\": 1705314600, \"late\": 1}\n{\"ts\": 1705314601, \"late\": 2}\n\
                 {\"ts\": 1705314602, \"late\": 3}\n";
    assert_eq!(server.post("/ingest", more).0, 200);
    assert_eq!(json(&server.get("/stats").1)["storage"]["buffered_rows"], 3);
    let dir = server.kill();
    let newest = fs::read_dir(dir.join("wal"))
        .expect("wal")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .max()
        .expect("a segment");
    let mut torn = fs::OpenOptions::new()
        .append(true)
        .open(&newest)
        .expect("open");
    torn.write_all(&[0x00, 0x10, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3])
        .expect("write");
    drop(torn);

    let server = Server::start_on(Some(dir), &[]);
    assert!(
        server.banner.contains("WARN wal") && server.banner.contains("torn record"),
        "the start did not warn about the torn record:\n{}",
        server.banner
    );
    assert_eq!(server.query("COUNT LOG {}")["count"], good + 3);
    let mut late: Vec<String> = server.query("MATCH LOG {}")["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .filter(|row| !row["late"].is_null())
        .map(|row| row["late"].to_string())
        .collect();
    late.sort();
    assert_eq!(
        late,
        ["1", "2", "3"],
        "the three lines that were only in memory did not come back once each"
    );
    let replay = json(&server.get("/stats").1)["storage"]["wal"]["replay"].clone();
    assert_eq!(replay["applied"], 3, "{replay}");
    assert_eq!(replay["torn"], 1, "{replay}");

    // A clean stop leaves nothing to read back, and the same rows.
    server.signal("-TERM");
    let (code, log, dir) = server.wait_keeping();
    assert_eq!(code, Some(0), "{log}");
    let server = Server::start_on(Some(dir), &[]);
    assert_eq!(server.query("COUNT LOG {}")["count"], good + 3);
    let replay = json(&server.get("/stats").1)["storage"]["wal"]["replay"].clone();
    assert_eq!(
        replay["records"], 0,
        "a clean stop left lines to replay: {replay}"
    );
}

/// The cap on open segment files is a setting, and `/stats` reports both halves of it.
#[test]
fn the_open_segment_cap_is_reported() {
    let server = Served::start_with(ServeFlags {
        max_open_segments: Some(4),
        row_group_timeout_ms: Some(500),
        ..ServeFlags::default()
    });
    let (status, _) = server.post("/ingest", &corpus());
    assert_eq!(status, 200);

    let stats = wait_for_stats(server.port, "the corpus to reach the disk", |s| {
        s["storage"]["buffered_rows"] == 0
    });
    assert_eq!(stats["storage"]["max_open_segments"], 4);
    let open = stats["storage"]["open_segments"]
        .as_u64()
        .expect("a number");
    assert!(
        open <= 4,
        "{open} segment files open against a cap of 4: {stats}"
    );

    // And every line the corpus had is still there, across however many files it took.
    let want = (LINES - BAD.len()) as u64;
    assert_eq!(server.query("COUNT LOG {}")["count"], want);
}

/// A query's `meta` counts segment files read, and leaves under the name that used to
/// mean files.
///
/// `files_opened` reported 870 where 16,694 segment files were read.
#[test]
fn files_opened_counts_segment_files_and_leaves_opened_takes_the_old_meaning() {
    let server = Served::start_with(ServeFlags {
        row_group_timeout_ms: Some(500),
        ..ServeFlags::default()
    });
    assert_eq!(server.post("/ingest", &corpus()).0, 200);
    wait_for_stats(server.port, "the corpus to reach the disk", |s| {
        s["storage"]["buffered_rows"] == 0
    });

    let meta = server.query("COUNT LOG {}")["meta"].clone();
    let leaves = meta["leaves_opened"].as_u64().expect("leaves_opened");
    let files = meta["files_opened"].as_u64().expect("files_opened");
    assert!(leaves > 0, "no leaf was opened: {meta}");
    assert!(
        files >= leaves,
        "{files} file(s) for {leaves} leaf/leaves - files_opened is still counting leaves: {meta}"
    );
    assert_eq!(
        leaves,
        meta["templates_matched"]
            .as_u64()
            .expect("templates_matched"),
        "every matched template has rows here, so the two should agree"
    );
}

// ── A connection the server is about to close ──────────────────────────────────────

/// One connection kept across requests, as a shipper keeps one: each answer is read by its
/// `Content-Length`, and the next request goes down the same socket.
///
/// `honour` is whether it does what `Connection: close` in an answer asks - opens a new
/// connection for the next request. Either way it opens one after a request that got no
/// answer, which is all a client can do then.
struct Kept {
    port: u16,
    honour: bool,
    /// HTTP/1.0, which keeps a connection only where both ends say `keep-alive`.
    old: bool,
    socket: Option<TcpStream>,
    /// Connections opened so far.
    opened: usize,
}

impl Kept {
    fn new(port: u16, honour: bool) -> Self {
        Self {
            port,
            honour,
            old: false,
            socket: None,
            opened: 0,
        }
    }

    /// A client of HTTP/1.0 that asks for its connection to be kept, and keeps it where
    /// the answer says it may.
    fn old(port: u16) -> Self {
        Self {
            old: true,
            ..Self::new(port, true)
        }
    }

    /// One request down the kept connection: the status, the headers lowercased, and the
    /// body. `Err` is no answer, and says what happened in its place.
    fn send(
        &mut self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> Result<(u16, String, String), String> {
        let mut socket = match self.socket.take() {
            Some(socket) => socket,
            None => {
                self.opened += 1;
                let socket =
                    TcpStream::connect(("127.0.0.1", self.port)).expect("failed to connect");
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(60)))
                    .expect("failed to set a read timeout");
                socket
            }
        };
        let protocol = if self.old {
            "HTTP/1.0\r\nConnection: keep-alive"
        } else {
            "HTTP/1.1"
        };
        let head = format!(
            "{method} {path} {protocol}\r\nHost: 127.0.0.1:{}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            self.port,
            body.len()
        );
        // A write that fails is not yet no answer: a body refused before it is read breaks
        // the write end, and the refusal is in the receive buffer all the same.
        let wrote = socket
            .write_all(head.as_bytes())
            .and_then(|()| socket.write_all(body))
            .and_then(|()| socket.flush());

        let mut raw = Vec::new();
        let mut byte = [0u8; 1];
        while !raw.ends_with(b"\r\n\r\n") {
            match socket.read(&mut byte) {
                Ok(1) => raw.push(byte[0]),
                Ok(_) => {
                    return Err(format!(
                        "the connection ended {} bytes into the answer (write: {wrote:?})",
                        raw.len()
                    ))
                }
                Err(e) => {
                    return Err(format!(
                        "{e} {} bytes into the answer (write: {wrote:?})",
                        raw.len()
                    ))
                }
            }
        }
        let headers = String::from_utf8_lossy(&raw).to_lowercase();
        let length: usize = headers
            .split("content-length:")
            .nth(1)
            .and_then(|tail| tail.split("\r\n").next())
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        let mut rest = vec![0u8; length];
        socket
            .read_exact(&mut rest)
            .map_err(|e| format!("{e} in the answer's body"))?;
        let status = headers
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| format!("no status line in the answer:\n{headers}"))?;
        let closed = if self.old {
            !says(&headers, "keep-alive")
        } else {
            self.honour && says_close(&headers)
        };
        if !closed {
            self.socket = Some(socket);
        }
        Ok((status, headers, String::from_utf8_lossy(&rest).into_owned()))
    }
}

/// Whether an answer's headers, lowercased, carry `Connection: close`.
fn says_close(headers: &str) -> bool {
    says(headers, "close")
}

/// Whether an answer's headers, lowercased, carry `option` in a `Connection` header.
fn says(headers: &str, option: &str) -> bool {
    headers.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.trim() == "connection" && value.split(',').any(|v| v.trim() == option)
        })
    })
}

/// A body of `bytes` bytes or a little over: whole NDJSON lines.
fn lines_of(bytes: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(bytes + 128);
    let mut n = 0u64;
    while body.len() < bytes {
        n += 1;
        body.extend_from_slice(
            format!(
                "{{\"ts\": 1705314600, \"x\": {n}, \"msg\": \"{:0>96}\"}}\n",
                n * 7919
            )
            .as_bytes(),
        );
    }
    body
}

/// A directory a test made read-only, made writable again when this goes: however the
/// test ends, the scratch directory around it can then be removed.
#[cfg(unix)]
struct Writable(PathBuf);

#[cfg(unix)]
impl Drop for Writable {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;

        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
    }
}

/// A refusal after which the server closes the connection says `Connection: close`.
///
/// The door refuses a body before it is read, and a connection with a request half read
/// cannot carry another: the server closes it. A shipper that is not told sends its next
/// batch into the closed socket and gets no answer at all - every other body, with the log
/// failing, where each should have been a 503 it knows what to do with.
#[test]
#[cfg(unix)]
fn a_refusal_that_closes_the_connection_says_so() {
    use std::os::unix::fs::PermissionsExt;

    let server = Served::start_with(ServeFlags {
        wal_segment_bytes: Some(1),
        flush_interval_ms: Some(10_000),
        ..ServeFlags::default()
    });
    let line = br#"{"ts": 1705314600, "x": 0}"#;
    assert_eq!(server.post("/ingest", line).0, 200);
    let wal = server.data_dir().join("wal");
    // Declared after the server, so it goes first: the directory is writable again by the
    // time the server's own drop removes what is around it.
    let _writable = Writable(wal.clone());
    fs::set_permissions(&wal, fs::Permissions::from_mode(0o555)).expect("chmod");
    assert_eq!(server.post("/ingest", line).0, 503);
    let read = json(&server.get("/stats").1)["ingest"]["lines_read"].clone();

    let body = lines_of(100 * 1024);
    // 1. A client that does as `Connection: close` asks.
    let mut kept = Kept::new(server.port, true);
    let answers: Vec<_> = (0..12)
        .map(|_| kept.send("POST", "/ingest", &body))
        .collect();
    // 2. And one that does not. What it loses is reported and not asserted: the server
    //    said what it would do, and a client that sends into a closed socket anyway is not
    //    this server's to fix.
    let mut deaf = Kept::new(server.port, false);
    let unanswered = (0..12)
        .filter(|_| deaf.send("POST", "/ingest", &body).is_err())
        .count();
    let after = json(&server.get("/stats").1)["ingest"]["lines_read"].clone();
    eprintln!(
        "a client that does not honour `Connection: close`: {unanswered} of 12 bodies got no \
         answer, over {} connections",
        deaf.opened
    );

    // All twelve are answered: this is the assertion the missing header fails.
    let lost: Vec<String> = answers
        .iter()
        .enumerate()
        .filter_map(|(nth, a)| {
            a.as_ref()
                .err()
                .map(|why| format!("body {}: {why}", nth + 1))
        })
        .collect();
    assert!(
        lost.is_empty(),
        "{} of 12 bodies sent down a kept connection got no answer:\n{}",
        lost.len(),
        lost.join("\n")
    );
    for (nth, answer) in answers.iter().enumerate() {
        let (status, headers, text) = answer.as_ref().expect("answered");
        assert_eq!(*status, 503, "body {}: {text}", nth + 1);
        assert!(headers.contains("retry-after:"), "{headers}");
        assert!(
            says_close(headers),
            "body {}: a 503 for a body that was not read, on a connection the server then \
             closes, without `Connection: close`:\n{headers}",
            nth + 1
        );
    }
    // One connection each: the client was told, every time.
    assert_eq!(kept.opened, 12);
    // None of the twenty-four was stored, or so much as read.
    assert_eq!(after, read, "a refused body was read");
    assert_eq!(server.query("COUNT LOG {}")["count"], 1);
}

/// And an answer that says nothing of the kind leaves the connection open: the header is
/// the truth both ways. Every request here goes down a kept connection whose client does
/// as it is told, so each is answered - whatever was refused before it, read or unread.
///
/// The bodies left unread are far past what the server reads in one go (about 400KB): one
/// that is a little over its limit can be read to its end by the read that crosses the
/// limit, and then the connection stays open and the answer rightly does not say `close`.
#[test]
fn a_kept_connection_is_closed_only_where_the_answer_says_so() {
    let server = Served::start_with(ServeFlags {
        max_body_bytes: Some(65_536),
        ..ServeFlags::default()
    });
    let small = br#"{"ts": 1705314600, "x": 0}"#;
    let big = lines_of(100 * 1024);
    let far = lines_of(3 * 1024 * 1024);

    // (method, path, body, status, whether the body is read to its end before the answer)
    let asks: [(&str, &str, &[u8], u16, bool); 14] = [
        ("POST", "/ingest", small, 200, true),
        ("POST", "/ingest", small, 200, true),
        ("POST", "/query", b"not json", 400, true),
        ("POST", "/query", &big, 400, true),
        ("POST", "/query", &far, 413, false),
        ("POST", "/ingest", &far, 413, false),
        ("POST", "/ingest", small, 200, true),
        ("POST", "/nope", &big, 404, false),
        ("POST", "/health", &big, 405, false),
        ("GET", "/health", &big, 200, false),
        ("POST", "/load", &big, 403, true),
        ("POST", "/compact", b"{}", 403, true),
        ("POST", "/compact", &far, 413, false),
        ("GET", "/health", b"", 200, true),
    ];
    let mut kept = Kept::new(server.port, true);
    // Each is answered: none went into a connection the server had closed without a word.
    let answers: Vec<_> = asks
        .iter()
        .enumerate()
        .map(|(nth, (method, path, body, ..))| {
            kept.send(method, path, body).unwrap_or_else(|why| {
                panic!(
                    "ask {nth}, {method} {path} with {} bytes, got no answer: {why}",
                    body.len()
                )
            })
        })
        .collect();
    let mut closes = 0;
    for (nth, ((method, path, body, want, read), (status, headers, text))) in
        asks.iter().zip(&answers).enumerate()
    {
        assert_eq!(status, want, "ask {nth}, {method} {path}: {text}");
        assert_eq!(
            says_close(headers),
            !*read,
            "ask {nth}, {method} {path} with {} bytes: {headers}",
            body.len()
        );
        closes += usize::from(!*read);
    }
    assert_eq!(
        kept.opened,
        closes + 1,
        "a connection was closed that no answer said would be"
    );
}

/// A client of HTTP/1.0 that asked for its connection to be kept is told the same.
///
/// It is the one client an answer has to say `keep-alive` to, and the answer to a body
/// left unread said it too - on a connection the server then closed. Here it says `close`
/// in place of `keep-alive`, and `keep-alive` wherever the connection is in fact kept.
#[test]
fn a_kept_connection_of_http_1_0_is_told_the_same() {
    let server = Served::start();
    let big = lines_of(100 * 1024);

    // (method, path, body, status, whether the body is read to its end before the answer)
    let asks: [(&str, &str, &[u8], u16, bool); 6] = [
        ("POST", "/query", b"not json", 400, true),
        ("POST", "/nope", &big, 404, false),
        ("POST", "/query", &big, 400, true),
        ("GET", "/health", &big, 200, false),
        ("POST", "/health", &big, 405, false),
        ("GET", "/health", b"", 200, true),
    ];
    let mut kept = Kept::old(server.port);
    let mut closes = 0;
    for (nth, (method, path, body, want, read)) in asks.iter().enumerate() {
        let (status, headers, text) = kept.send(method, path, body).unwrap_or_else(|why| {
            panic!(
                "ask {nth}, {method} {path} with {} bytes, got no answer: {why}",
                body.len()
            )
        });
        assert_eq!(status, *want, "ask {nth}, {method} {path}: {text}");
        assert_eq!(
            (says_close(&headers), says(&headers, "keep-alive")),
            (!*read, *read),
            "ask {nth}, {method} {path} with {} bytes: {headers}",
            body.len()
        );
        closes += usize::from(!*read);
    }
    assert_eq!(
        kept.opened,
        closes + 1,
        "a connection was closed that no answer said would be"
    );
}
