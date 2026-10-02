//! What the storage format is observable as, from outside the process.
//!
//! The bytes are checked where they are written - `src/store/v3/` - and what the store
//! does with them in `src/store/disk.rs`. This file asks only what a client can ask: is a
//! row in the answer before it is on disk, does a schema change cost anything, does a
//! query say what it pruned, and does a data directory from an earlier layout stop the
//! server rather than being half-read.
//!
//! HTTP over a bare `TcpStream`, as in `serve.rs` and `restart.rs` and for the same
//! reason: the contract is a status line and a JSON body.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

// ── A database, and the servers started on it ──────────────────────────────────────

struct Db {
    dir: PathBuf,
}

impl Db {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "kibbr-segment-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        Self { dir }
    }

    fn start(&self) -> Server {
        self.start_with(&[])
    }

    fn start_with(&self, extra: &[&str]) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &self.dir.display().to_string()])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn kibbr serve");

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
                port = line[at + "http://127.0.0.1:".len()..].trim().parse().ok();
            }
            if line.contains("shutdown ") {
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

    /// Every segment file under the data directory, in path order.
    fn segments(&self) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![self.dir.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if entry.file_name().to_string_lossy().starts_with("seg_") {
                    out.push(path);
                }
            }
        }
        out.sort();
        out
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
    fn query(&self, q: &str) -> serde_json::Value {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = request(self.port, "POST", "/query", body.as_bytes());
        assert_eq!(status, 200, "query `{q}` answered {status}: {text}");
        json(&text)
    }

    fn ingest(&self, body: &[u8]) -> serde_json::Value {
        let (status, text) = request(self.port, "POST", "/ingest", body);
        assert_eq!(status, 200, "ingest answered {status}: {text}");
        json(&text)
    }

    fn stats(&self) -> serde_json::Value {
        json(&request(self.port, "GET", "/stats", b"").1)
    }

    /// SIGTERM, then wait for the exit - the graceful path, which closes every row group
    /// and seals every segment.
    fn shutdown(mut self) -> String {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill -TERM failed");
        let status = self.child.wait().expect("failed to wait for kibbr serve");
        let mut log = String::new();
        let _ = self.stderr.read_to_string(&mut log);
        assert_eq!(
            status.code(),
            Some(0),
            "a graceful shutdown did not exit 0:\n{log}"
        );
        self.child = Command::new("true").spawn().expect("failed to spawn true");
        let _ = self.child.wait();
        log
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
         Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    let _ = socket.write_all(body);
    let _ = socket.flush();

    let mut response = Vec::new();
    socket
        .read_to_end(&mut response)
        .expect("failed to read the response");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body split in the response");
    let headers = String::from_utf8_lossy(&response[..split]).into_owned();
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{headers}"));
    (
        status,
        String::from_utf8_lossy(&response[split + 4..]).into_owned(),
    )
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

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

/// A row group closes at 8,192 rows, 8MB or the timeout - one line is nowhere near any of
/// them, and still has to be in the answer immediately. The thresholds say when bytes
/// reach the disk, not when a row exists.
#[test]
fn one_line_is_queryable_before_any_row_group_closes() {
    let db = Db::new();
    let server = db.start();

    let report = server.ingest(b"{\"ts\":\"2024-01-15T10:00:00Z\",\"event\":\"one\"}\n");
    assert_eq!(report["ingested"], 1);

    let answer = server.query(r#"MATCH LOG { "event": "one" }"#);
    assert_eq!(answer["count"], 1, "the row was not visible: {answer}");
    assert_eq!(answer["rows"][0]["event"], "one");

    // Visible, and on nobody's disk. `/stats` says so, because that is what a kill -9
    // would cost.
    assert!(db.segments().is_empty(), "a row group closed for one row");
    assert_eq!(server.stats()["storage"]["buffered_rows"], 1);
}

// ── A schema change ────────────────────────────────────────────────────────────────

/// A row group records the type each of its columns actually used, so a column that
/// widens costs nothing: no file is sealed and none is opened, and each row keeps the type
/// and the spelling it arrived with rather than the first ones being rewritten as text.
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

/// A window that covers one row group reads one, and says so.
///
/// Reported in `meta` because a prune nobody can see is a prune nobody notices stopping.
#[test]
fn a_window_over_one_row_group_reads_one_and_says_so() {
    let db = Db::new();
    // No tick while the body is applied: one landing on a buffer past 8,192 rows closes
    // all of it, the short rest included, and the count would be the clock's and not this
    // test's. The shutdown's final flush is the one close.
    let first = db.start_with(&[
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

/// Naming one column of eight reads its chunks and the timestamp's, nothing else.
/// `bytes_read` is the only place that is visible.
#[test]
fn naming_one_column_reads_far_fewer_bytes_than_the_whole_row() {
    let db = Db::new();
    let first = db.start();
    first.ingest(&one_template(20_000));
    first.shutdown();

    let second = db.start();
    let counted = second.query(r#"COUNT LOG { "host": "h3" }"#);
    let whole = second.query("MATCH LOG {} LIMIT 1");

    let narrow = counted["meta"]["bytes_read"].as_u64().expect("bytes_read");
    let wide = whole["meta"]["bytes_read"].as_u64().expect("bytes_read");
    assert!(narrow > 0, "a count that matched rows read no bytes");
    assert_eq!(counted["count"], 2_500);
    assert!(
        narrow * 3 < wide,
        "naming one column of eight read {narrow} bytes against {wide} for the whole row"
    );
}

// ── The layout this replaces ───────────────────────────────────────────────────────

/// A data directory from before format v3 - a directory per template - is refused, by
/// path, with a non-zero exit.
///
/// There is no migration, so the alternative to refusing is reading bytes whose meaning
/// has to be guessed. Non-zero exit: a server that starts and answers `count: 0` over
/// somebody's data is the worse outcome.
#[test]
fn a_per_template_data_directory_is_refused_by_path() {
    let db = Db::new();
    let day = db.dir.join("leaf_101").join("2024-01-15");
    fs::create_dir_all(&day).expect("create the day directory");
    fs::write(day.join("seg_000001.col"), b"KBBR\x02\x00\x00\x00").expect("write a v2 segment");

    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["serve", "--port", "0"])
        .args(["--data-dir", &db.dir.display().to_string()])
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
    let day = db.dir.join("data").join("2024-01-15");
    fs::create_dir_all(&day).expect("create the day directory");
    fs::write(day.join("seg_0_000001.col"), b"KBBR\x02\x00\x00\x00").expect("write");

    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["serve", "--port", "0"])
        .args(["--data-dir", &db.dir.display().to_string()])
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

/// A data file written by a pre-rename build - the old magic, a version this build does
/// read - is refused by path like another version, and says why.
#[test]
fn a_data_file_from_a_pre_rename_build_is_refused_by_path() {
    let db = Db::new();
    let day = db.dir.join("data").join("2024-01-15");
    fs::create_dir_all(&day).expect("create the day directory");
    // The pre-rename magic, then version 3.
    fs::write(day.join("seg_0_000001.col"), b"\x54\x52\x44\x42\x03\x00\x00\x00").expect("write");

    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["serve", "--port", "0"])
        .args(["--data-dir", &db.dir.display().to_string()])
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

/// What the operator is told a crash costs. It changed with the format - rows wait for a
/// row group, not for the next tick - and a banner still promising the flush interval
/// would be promising something nothing implements.
#[test]
fn the_banner_states_the_row_group_window_and_not_the_flush_interval() {
    let db = Db::new();
    let server = db.start_with(&["--row-group-timeout-ms", "4000"]);
    assert!(
        server.banner.contains("8,192") && server.banner.contains("4000ms"),
        "the banner does not state the row group thresholds:\n{}",
        server.banner
    );
    assert!(
        server.banner.contains("one row group of rows"),
        "the banner does not say what a kill -9 costs:\n{}",
        server.banner
    );
    assert_eq!(server.stats()["storage"]["row_group_timeout_ms"], 4000);
    // One server to a data directory: the second would be refused its lock.
    drop(server);

    // Floored at the flush interval: the tick is what notices the timeout expired.
    let floored = db.start_with(&["--row-group-timeout-ms", "10", "--flush-interval-ms", "700"]);
    assert_eq!(floored.stats()["storage"]["row_group_timeout_ms"], 700);
}
