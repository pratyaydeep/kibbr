//! What an operator reaches for: the settings file, and the deadline on a stop.
//!
//! All of it from outside, as the other files here: the binary's flags, its stdout and
//! stderr, its exit code, and HTTP over a bare `TcpStream`. The arithmetic of where a
//! setting comes from is in `src/config.rs`; this file holds that the commands are
//! wired to it.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;

fn kibbr() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
    // The variables are settings too, and would answer for the file under test.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("KIBBR__") {
            command.env_remove(name);
        }
    }
    command
}

/// A directory of this test's own, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kibbr-operations-{}-{}-{name}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        Self(dir)
    }

    /// A settings file of `text` in the directory, by its path.
    fn file(&self, name: &str, text: &str) -> String {
        let path = self.0.join(name);
        fs::write(&path, text).expect("a file");
        path.display().to_string()
    }

    fn path(&self, name: &str) -> String {
        self.0.join(name).display().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

/// A running `kibbr serve`, killed when dropped.
struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
}

impl Server {
    /// `kibbr <before> serve --port 0 <after>`, once its banner has said where it listens.
    fn start(before: &[&str], after: &[&str], env: &[(&str, &str)]) -> Self {
        let mut command = kibbr();
        command
            .args(before)
            .args(["serve", "--port", "0"])
            .args(after);
        for (name, value) in env {
            command.env(name, value);
        }
        let mut child = command
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn kibbr serve");
        let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
        let mut port = None;
        let mut said = String::new();
        for _ in 0..80 {
            let mut line = String::new();
            if stderr.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            said.push_str(&line);
            if let Some(at) = line.find("http://127.0.0.1:") {
                let rest = &line[at + "http://127.0.0.1:".len()..];
                let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                port = digits.parse().ok();
            }
            // The banner's last line.
            if line.contains("shutdown ") {
                break;
            }
        }
        let port = port.unwrap_or_else(|| panic!("no listening line:\n{said}"));
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

    fn stats(&self) -> Value {
        let (status, body) = self.get("/stats");
        assert_eq!(status, 200, "{body}");
        json(&body)
    }

    fn signal(&self) {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("kill")
            .success();
        assert!(ok, "kill -TERM failed");
    }

    /// SIGTERM, then the exit code and everything stderr said after the banner. Fails if
    /// the process is still there after `within`.
    fn stop(mut self, within: Duration) -> (Option<i32>, String) {
        self.signal();
        let asked = Instant::now();
        let code = loop {
            match self.child.try_wait().expect("try_wait") {
                Some(status) => break status.code(),
                None if asked.elapsed() > within => {
                    let _ = self.child.kill();
                    panic!("still running {within:?} after SIGTERM");
                }
                None => std::thread::sleep(Duration::from_millis(10)),
            }
        };
        let mut said = String::new();
        let _ = self.stderr.read_to_string(&mut said);
        (code, said)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn request(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    let _ = socket.write_all(body);
    let _ = socket.flush();
    answer(&mut socket)
}

/// The status and the body of whatever the server wrote before it closed the connection.
fn answer(socket: &mut TcpStream) -> (u16, String) {
    let mut response = Vec::new();
    let _ = socket.read_to_end(&mut response);
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no answer: {:?}", text(&response)));
    let status = text(&response[..split])
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status line");
    (status, text(&response[split + 4..]))
}

fn run(args: &[&str]) -> Output {
    kibbr().args(args).output().expect("failed to run kibbr")
}

/// A command's stdout with what a clock wrote taken out: a number before `µs` or `ms`,
/// and the `duration_ms` a JSON answer ends on. Everything else is compared as it is.
fn timeless(out: &[u8]) -> String {
    let said = text(out);
    let said = match said.find("\"duration_ms\"") {
        Some(at) => said[..at].to_string(),
        None => said,
    };
    let mut kept = String::new();
    let mut number = String::new();
    let mut rest = said.as_str();
    while let Some(c) = rest.chars().next() {
        if c.is_ascii_digit() || (c == '.' && !number.is_empty()) {
            number.push(c);
        } else {
            if !(rest.starts_with("µs") || rest.starts_with("ms")) {
                kept.push_str(&number);
            }
            number.clear();
            kept.push(c);
        }
        rest = &rest[c.len_utf8()..];
    }
    kept + &number
}

// ── The settings file ──────────────────────────────────────────────────────────────

/// **Every command takes `--config`, before its name or after it, and one that names a
/// file this build cannot use does nothing.** The file is read before the command runs:
/// a key with no setting behind it stops `gen-logs`, which has no settings at all, as it
/// stops `serve` - with the key, its line and the keys its section does have.
#[test]
fn every_command_takes_config_and_a_file_it_cannot_use_stops_it() {
    let scratch = Scratch::new("every-command");
    let bad = scratch.file("bad.toml", "[storage]\nwal_dir = \"/var/lib/kibbr/wal\"\n");
    let good = scratch.file("good.toml", "# every key absent\n[query]\n[schema]\n");
    let input = scratch.file("in.ndjson", "{\"ts\":\"2024-01-15T10:00:00Z\",\"a\":1}\n");
    let data = scratch.path("data");
    let commands: Vec<Vec<&str>> = vec![
        vec!["gen-logs", "--lines", "2"],
        vec!["parse", &input],
        vec!["demo", &input],
        vec!["query", &input, "--count"],
        vec!["explain", "COUNT LOG {\"a\": \"1\"}"],
        vec!["load", &input, "--data-dir", &data],
        vec!["compact", "--data-dir", &data],
        vec!["bench-storage", &input],
        vec!["bench", "--plan-only", "--samples", "1"],
        vec!["serve", "--port", "0", "--storage", "memory"],
    ];
    for command in &commands {
        for after in [false, true] {
            let mut args: Vec<&str> = Vec::new();
            if after {
                args.extend(command);
                args.extend(["--config", &bad]);
            } else {
                args.extend(["--config", &bad]);
                args.extend(command);
            }
            let out = run(&args);
            let said = text(&out.stderr);
            assert_eq!(out.status.code(), Some(1), "{args:?}: {said}");
            assert!(
                out.stdout.is_empty(),
                "{args:?} wrote {:?}",
                text(&out.stdout)
            );
            assert!(
                said.starts_with(&format!("kibbr: --config {bad}: "))
                    && said.contains("line 2")
                    && said.contains("unknown field `wal_dir`")
                    && said.contains("`base_path`"),
                "{args:?}: {said}"
            );
        }
    }
    // Nothing was made under a file that stopped the command.
    assert!(!Path::new(&data).exists());

    // A file that is named and is not there is not "no file".
    let gone = scratch.path("gone.toml");
    let out = run(&["gen-logs", "--lines", "2", "--config", &gone]);
    assert_eq!(out.status.code(), Some(1));
    assert!(text(&out.stderr).starts_with(&format!("kibbr: --config {gone}: ")));

    // And a file every key of which is absent changes nothing a command prints.
    for command in &commands[..5] {
        let plain = run(command);
        let mut with: Vec<&str> = command.clone();
        with.extend(["--config", &good]);
        let filed = run(&with);
        assert_eq!(plain.status.code(), Some(0), "{command:?}");
        assert_eq!(
            timeless(&plain.stdout),
            timeless(&filed.stdout),
            "{command:?}"
        );
    }
}

/// **A server runs with what the file says, a variable wins over the file, and a flag
/// over both** - seen where a server says what it runs with, `/stats`, and in where it
/// put its data.
#[test]
fn a_server_takes_the_file_and_a_variable_and_a_flag_each_win_over_it() {
    let scratch = Scratch::new("serve-file");
    let data = scratch.path("data");
    let config = scratch.file(
        "kibbr.toml",
        &format!(
            "[storage]\nbase_path = \"{data}\"\n\n\
             [api]\ningest_wait_ms = 250\nshutdown_deadline = \"3s\"\n\n\
             [query]\nmax_rows = 2\n"
        ),
    );
    let waits = |server: &Server| {
        let ingest = &server.stats()["ingest"];
        (
            ingest["ingest_wait_ms"].as_u64(),
            ingest["shutdown_deadline_ms"].as_u64(),
        )
    };

    // The file alone.
    let server = Server::start(&[], &["--config", &config], &[]);
    assert_eq!(waits(&server), (Some(250), Some(3000)));
    assert!(Path::new(&data).join("catalog.log").is_file());
    // `[query] max_rows` is the page an unlimited MATCH is cut at.
    let lines: String = (0..5)
        .map(|n| format!("{{\"ts\":\"2024-01-15T10:00:0{n}Z\",\"a\":{n}}}\n"))
        .collect();
    assert_eq!(server.post("/ingest", lines.as_bytes()).0, 200);
    let (status, body) = server.post("/query", br#"{"q":"MATCH LOG {\"a\": *}"}"#);
    assert_eq!(status, 200, "{body}");
    let answer = json(&body);
    assert_eq!(answer["rows"].as_array().map(Vec::len), Some(2), "{answer}");
    assert_eq!(answer["meta"]["limit_source"], "backstop", "{answer}");
    let (code, said) = server.stop(Duration::from_secs(20));
    assert_eq!(code, Some(0), "{said}");

    // A variable over the file, key by key: the deadline is still the file's.
    let server = Server::start(
        &["--config", &config],
        &[],
        &[("KIBBR__API__INGEST_WAIT_MS", "300")],
    );
    assert_eq!(waits(&server), (Some(300), Some(3000)));
    let (code, said) = server.stop(Duration::from_secs(20));
    assert_eq!(code, Some(0), "{said}");

    // A flag over both.
    let server = Server::start(
        &["--config", &config],
        &["--ingest-wait-ms", "400", "--shutdown-deadline", "0s"],
        &[
            ("KIBBR__API__INGEST_WAIT_MS", "300"),
            ("KIBBR__API__SHUTDOWN_DEADLINE", "7s"),
        ],
    );
    assert_eq!(waits(&server), (Some(400), Some(0)));
    let (code, said) = server.stop(Duration::from_secs(20));
    assert_eq!(code, Some(0), "{said}");

    // With no file, no variable and no flag: 100ms and 25s.
    let server = Server::start(&[], &["--storage", "memory"], &[]);
    assert_eq!(waits(&server), (Some(100), Some(25_000)));
}

/// **`query` reads the file too**: its backstop and its time columns are the file's, a
/// flag still wins, and stdout holds the answer and nothing else.
#[test]
fn query_takes_its_limits_and_time_columns_from_the_file() {
    let scratch = Scratch::new("query-file");
    let lines: String = (0..5)
        .map(|n| format!("{{\"when\":\"2024-01-15T10:00:0{n}Z\",\"a\":{n}}}\n"))
        .collect();
    let input = scratch.file("in.ndjson", &lines);
    let config = scratch.file(
        "kibbr.toml",
        "[query]\nmax_rows = 2\n\n[schema]\ntime_columns = [\"when\"]\n",
    );
    let fetch = |extra: &[&str]| {
        let mut args = vec!["query", input.as_str(), "--fetch", "--format", "json"];
        args.extend(extra);
        let out = run(&args);
        assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
        json(&text(&out.stdout))
    };
    let filed = fetch(&["--config", &config]);
    assert_eq!(filed["rows"].as_array().map(Vec::len), Some(2));
    assert_eq!(filed["meta"]["limit_source"], "backstop");
    // Newest first by the file's time column: without it `when` is a column like any
    // other and the rows come back in the order they were read.
    assert_eq!(filed["rows"][0]["a"], 4, "{filed}");

    let flagged = fetch(&["--config", &config, "--max-rows", "4"]);
    assert_eq!(flagged["rows"].as_array().map(Vec::len), Some(4));

    let plain = fetch(&[]);
    assert_eq!(plain["rows"].as_array().map(Vec::len), Some(5));
    assert_eq!(plain["meta"]["truncated"], false);
}

// ── The shutdown deadline ──────────────────────────────────────────────────────────

/// A `POST /ingest` whose sender stops halfway through its body: the head, a
/// `Content-Length` of a thousand, ten bytes, and then nothing.
fn stalled_body(port: u16) -> TcpStream {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let head = format!(
        "POST /ingest HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: 1000\r\n\r\n{{\"a\":1}}\n\n\n"
    );
    socket.write_all(head.as_bytes()).expect("the head");
    socket.flush().expect("flushed");
    socket
}

/// **A stop does not wait past its deadline for a request that will not finish.** A
/// sender that stopped halfway through a body used to hold the server until whatever
/// sent the SIGTERM sent a SIGKILL; with a deadline the request is answered 503 where it
/// stands, the wind-down runs, and the exit is 0 - every step of it in the log.
#[test]
fn a_stop_cuts_off_what_is_still_in_flight_at_the_deadline() {
    let scratch = Scratch::new("deadline");
    let data = scratch.path("data");
    let server = Server::start(
        &[],
        &["--data-dir", &data, "--shutdown-deadline", "400ms"],
        &[],
    );
    let lines = "{\"ts\":\"2024-01-15T10:00:00Z\",\"a\":1}\n";
    assert_eq!(server.post("/ingest", lines.as_bytes()).0, 200);
    let mut stalled = stalled_body(server.port);
    // In flight before the signal, or the stop has nothing to wait for.
    let seen = Instant::now();
    while server.stats()["ingest"]["bodies_inside"] != 1 {
        assert!(
            seen.elapsed() < Duration::from_secs(10),
            "the body never arrived"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let asked = Instant::now();
    let (code, said) = server.stop(Duration::from_secs(15));
    let took = asked.elapsed();
    assert_eq!(code, Some(0), "{said}");
    assert!(
        took >= Duration::from_millis(400),
        "stopped in {took:?}, before the deadline"
    );
    let logged: Vec<&str> = said.lines().filter(|l| l.starts_with("kibbr: ")).collect();
    assert!(
        logged.contains(
            &"kibbr: WARN shutdown deadline - 1 request(s) still in flight after 400ms; each \
              is answered 503 and let go"
        ),
        "{said}"
    );
    let at = |starts: &str| {
        logged
            .iter()
            .position(|line| line.starts_with(starts))
            .unwrap_or_else(|| panic!("no `{starts}` in:\n{said}"))
    };
    // The whole wind-down, in its order, after the cut.
    let steps = [
        "kibbr: WARN shutdown deadline - ",
        "kibbr: in-flight requests cut off at the shutdown deadline",
        "kibbr: final flush - ",
        "kibbr: WAL checkpoint - ",
        "kibbr: trie.json - ",
        "kibbr: close stores - ",
        "kibbr: close WAL - ",
        "kibbr: exit 0",
    ];
    let order: Vec<usize> = steps.iter().map(|step| at(step)).collect();
    assert!(order.windows(2).all(|pair| pair[0] < pair[1]), "{said}");

    // The sender that stalled was answered, and told why.
    let (status, body) = answer(&mut stalled);
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("shutdown deadline"), "{body}");

    // And what was acknowledged before the stop is there after it, with nothing to replay.
    let server = Server::start(&[], &["--data-dir", &data], &[]);
    assert_eq!(server.stats()["ingest"]["lines_ingested"], 0);
    let (status, body) = server.post("/query", br#"{"q":"COUNT LOG {\"a\": *}"}"#);
    assert_eq!(status, 200, "{body}");
    assert_eq!(json(&body)["count"], 1, "{body}");
}

/// **With no deadline a stop waits for every request, as it did** - and with nothing in
/// flight a deadline is never reached: the stop is as quick as it was.
#[test]
fn a_stop_with_no_deadline_waits_and_one_with_nothing_in_flight_does_not() {
    let server = Server::start(
        &[],
        &["--storage", "memory", "--shutdown-deadline", "0s"],
        &[],
    );
    let mut stalled = stalled_body(server.port);
    let seen = Instant::now();
    while server.stats()["ingest"]["bodies_inside"] != 1 {
        assert!(
            seen.elapsed() < Duration::from_secs(10),
            "the body never arrived"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    server.signal();
    std::thread::sleep(Duration::from_millis(1500));
    let mut server = server;
    assert!(
        server.child.try_wait().expect("try_wait").is_none(),
        "stopped with a request in flight and no deadline"
    );
    // The sender finishes its body, and the stop goes on.
    stalled.write_all(&vec![b'\n'; 990]).expect("the rest");
    let (status, body) = answer(&mut stalled);
    assert!(status == 200 || status == 503, "{status}: {body}");
    let asked = Instant::now();
    let code = loop {
        match server.child.try_wait().expect("try_wait") {
            Some(status) => break status.code(),
            None if asked.elapsed() > Duration::from_secs(15) => panic!("still running"),
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    };
    assert_eq!(code, Some(0));

    // Nothing in flight: a long deadline costs a stop nothing.
    let server = Server::start(
        &[],
        &["--storage", "memory", "--shutdown-deadline", "1h"],
        &[],
    );
    let asked = Instant::now();
    let (code, said) = server.stop(Duration::from_secs(10));
    assert_eq!(code, Some(0), "{said}");
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "{:?}",
        asked.elapsed()
    );
    assert!(
        said.contains("kibbr: in-flight requests complete"),
        "{said}"
    );
}
