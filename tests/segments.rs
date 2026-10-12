//! What the storage format is observable as, from outside the process: is a row in
//! the answer before it is on disk, does a schema change cost anything, does a query say
//! what it pruned, and does a data directory from an earlier layout stop the server.

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

    /// Every data file under the data directory, in path order. By its extension: the
    /// write-ahead log's segments are `seg_` files too, and are not what a row group is in.
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
    let first = db.start_with(&[
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
        let server = db.start_with(extra);
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
        command
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &db.dir.display().to_string()]);
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
    let server = db.start_with(&[
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
    let server = db.start_with(&[
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
    fs::write(
        day.join("seg_0_000001.col"),
        b"\x54\x52\x44\x42\x03\x00\x00\x00",
    )
    .expect("write");

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

/// What the operator is told a crash costs: nothing that was acknowledged. A banner
/// promising the flush interval would be describing something nothing implements.
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
    let floored = db.start_with(&["--row-group-timeout-ms", "10", "--flush-interval-ms", "700"]);
    assert_eq!(floored.stats()["storage"]["row_group_timeout_ms"], 700);
}
