//! `triedb load`, the bulk door, from outside the process.
//!
//! The loader and the server are separate processes on one data directory, and almost
//! everything worth checking is what one of them leaves for the other: a load run alone
//! must be served by the next server, a load sent to a running server must be in its
//! answers at once, and a refused load must leave nothing - no rows, no files, no staging
//! directory, and not its id either, or the corrected retry is refused as a repeat. So
//! every test runs the real binary, usually more than once.
//!
//! Two files of the data directory are read directly. `manifest.log`, because compaction
//! skips a file by its `finished` flag and startup deletes every file the manifest does not
//! name, so which files are live and flagged is the fact itself. And `data/<day>/`, because
//! a retired file still there after a restart is bytes nothing will ever read or delete.
//!
//! Every command runs against a deadline. A client polling a job that never finishes, or a
//! second server that starts when it should have refused, would otherwise hang the suite
//! rather than fail it.
//!
//! HTTP over a bare `TcpStream`, as in `restart.rs` and `segments.rs` and for the same
//! reason: the contract is a status line and a JSON body.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde_json::Value;

/// Hostile lines for most tests: thirty days and a few hundred templates, so a load spans
/// many day files and a partial commit has somewhere to show.
const LINES: usize = 2_000;

/// Farm lines for the replace tests: one day, every line carrying its own time, so a
/// `--replace-day` over it has no row outside the day to be refused for.
const FARM_LINES: usize = 3_000;

const DAY: &str = "2024-01-15";
const ALL: &str = "COUNT LOG {}";
const DAY_COUNT: &str = r#"COUNT LOG {} BETWEEN "2024-01-15T00:00:00Z" AND "2024-01-16T00:00:00Z""#;

/// Long enough for a debug build to load a few thousand lines several times over.
const DEADLINE: Duration = Duration::from_secs(60);

/// A command that should exit at once - a refusal - and is failed rather than waited on.
const REFUSAL_DEADLINE: Duration = Duration::from_secs(15);

// ── A scratch root: inputs, a data directory, and captured output ──────────────────

/// `in/` holds the inputs, `store/` is the data directory, `runs/` each command's output.
///
/// Inputs sit outside the data directory so a directory load cannot pick up the store's
/// own files, and the root outlives every process started on it, as in `restart.rs`.
struct Db {
    root: PathBuf,
}

impl Db {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "triedb-load-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("in")).expect("create the input directory");
        fs::create_dir_all(root.join("runs")).expect("create the output directory");
        Self { root }
    }

    fn base(&self) -> PathBuf {
        self.root.join("store")
    }

    fn base_arg(&self) -> String {
        self.base().display().to_string()
    }

    /// `bytes` at `in/<rel>`, parent directories created.
    fn input(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let path = self.root.join("in").join(rel);
        fs::create_dir_all(path.parent().expect("an input has a parent"))
            .expect("create the input's directory");
        fs::write(&path, bytes).expect("write the input");
        path
    }

    // ── Processes ──

    /// `triedb <args>`, stdout and stderr to files: a pipe nobody reads until the exit
    /// can fill and stall the child, and a stalled child looks like a slow one.
    fn spawn(&self, args: &[String], stdin: Option<&Path>) -> Running {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let out = self.root.join("runs").join(format!("{n}.out"));
        let err = self.root.join("runs").join(format!("{n}.err"));
        let stdin = match stdin {
            Some(path) => Stdio::from(File::open(path).expect("open the stdin input")),
            None => Stdio::null(),
        };
        let child = Command::new(env!("CARGO_BIN_EXE_triedb"))
            .args(args)
            .stdin(stdin)
            .stdout(File::create(&out).expect("create the stdout file"))
            .stderr(File::create(&err).expect("create the stderr file"))
            .spawn()
            .expect("failed to spawn triedb");
        Running {
            child,
            out,
            err,
            what: format!("triedb {}", args.join(" ")),
        }
    }

    fn load_args(&self, args: &[&str], inputs: &[&Path]) -> Vec<String> {
        let mut all = vec!["load".to_string(), "--data-dir".into(), self.base_arg()];
        all.extend(args.iter().map(|a| a.to_string()));
        all.extend(inputs.iter().map(|p| p.display().to_string()));
        all
    }

    fn load(&self, args: &[&str], inputs: &[&Path]) -> Ran {
        self.spawn(&self.load_args(args, inputs), None)
            .finish(DEADLINE)
    }

    /// `triedb load <args> -`, with `stdin` as the input.
    fn load_stdin(&self, args: &[&str], stdin: &Path) -> Ran {
        let mut all = self.load_args(args, &[]);
        all.push("-".into());
        self.spawn(&all, Some(stdin)).finish(DEADLINE)
    }

    fn start(&self) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_triedb"))
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &self.base_arg()])
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
        }
    }

    /// One count from a server started for it and then killed - fine for a process that
    /// ingested nothing, and the point where the question is what the *next* process sees.
    fn count(&self, q: &str) -> u64 {
        self.start().count(q)
    }

    // ── The directory's own files ──

    /// Whatever is under `staging/`. Empty or absent after any load has exited: a
    /// leftover is either a move the loader never finished - rows the next startup, not
    /// this command, puts in place - or bytes nothing will ever delete.
    fn staging(&self) -> Vec<PathBuf> {
        fs::read_dir(self.base().join("staging"))
            .map(|entries| entries.flatten().map(|e| e.path()).collect())
            .unwrap_or_default()
    }

    /// Every record in `manifest.log`, in order.
    ///
    /// Frames are `[len u32 LE][crc u32 LE][JSON]` from offset 0. The CRC is not rechecked:
    /// a payload that is not whole JSON ends the walk, which is what a torn tail looks
    /// like, and these tests never tear one.
    fn manifest(&self) -> Vec<Value> {
        let bytes = fs::read(self.base().join("manifest.log")).unwrap_or_default();
        let mut out = Vec::new();
        let mut at = 0;
        while let Some(head) = bytes.get(at..at + 8) {
            let len = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
            let Some(payload) = bytes.get(at + 8..at + 8 + len).filter(|p| !p.is_empty()) else {
                break;
            };
            let Ok(record) = serde_json::from_slice::<Value>(payload) else {
                break;
            };
            out.push(record);
            at += 8 + len;
        }
        out
    }

    /// The live files by path, after every record is applied: the last word on a path
    /// wins, a removed path is gone, and a replace removes before it adds.
    fn live(&self) -> BTreeMap<String, Value> {
        let mut files = BTreeMap::new();
        for record in self.manifest() {
            let (added, removed) = match record["kind"].as_str() {
                Some("add") | Some("seal") => (record["files"].clone(), Value::Null),
                Some("remove") => (Value::Null, record["files"].clone()),
                Some("replace") => (record["add"].clone(), record["remove"].clone()),
                _ => continue,
            };
            for path in strings(&removed) {
                files.remove(&path);
            }
            for file in added.as_array().into_iter().flatten() {
                let path = file["path"].as_str().expect("a file record has a path");
                files.insert(path.to_string(), file.clone());
            }
        }
        files
    }

    /// The live paths of one day, as the manifest names them (`2024-01-15/seg_…`).
    fn live_on(&self, day: &str) -> BTreeSet<String> {
        let prefix = format!("{day}/");
        self.live()
            .into_keys()
            .filter(|p| p.starts_with(&prefix))
            .collect()
    }

    /// The `.col` files under `data/<day>/`, spelled the way the manifest spells them.
    fn on_disk(&self, day: &str) -> BTreeSet<String> {
        fs::read_dir(self.base().join("data").join(day))
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|name| name.ends_with(".col"))
                    .map(|name| format!("{day}/{name}"))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// `day` is exactly `files`: on disk, in the manifest, and every one flagged finished.
    /// Read after a server has started, since startup is what deletes retired files.
    fn assert_day_is(&self, day: &str, files: &BTreeSet<String>) {
        assert_eq!(
            &self.on_disk(day),
            files,
            "data/{day} does not hold exactly the load's files"
        );
        assert_eq!(
            &self.live_on(day),
            files,
            "the manifest's live files for {day} are not exactly the load's"
        );
        for (path, file) in self.live() {
            if files.contains(&path) {
                assert_eq!(file["finished"], true, "{path} is not flagged finished");
            }
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// A command still running, its output going to two files.
struct Running {
    child: Child,
    out: PathBuf,
    err: PathBuf,
    what: String,
}

impl Running {
    fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Waits for the exit, killing the command at `limit`.
    fn finish(mut self, limit: Duration) -> Ran {
        let deadline = Instant::now() + limit;
        let mut timed_out = false;
        let status = loop {
            match self.child.try_wait().expect("failed to poll triedb") {
                Some(status) => break status,
                None if Instant::now() >= deadline => {
                    timed_out = true;
                    let _ = self.child.kill();
                    break self.child.wait().expect("failed to wait for triedb");
                }
                None => sleep(Duration::from_millis(20)),
            }
        };
        Ran {
            what: self.what.clone(),
            code: status.code(),
            timed_out,
            stdout: fs::read_to_string(&self.out).unwrap_or_default(),
            stderr: fs::read_to_string(&self.err).unwrap_or_default(),
        }
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        // Only a child not yet reaped: once `try_wait` has seen the exit, the pid may
        // already belong to another process.
        if self.is_running() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// An exited command.
struct Ran {
    what: String,
    code: Option<i32>,
    timed_out: bool,
    stdout: String,
    stderr: String,
}

impl Ran {
    /// Exit 0 and the `--json` report, which must say `ok`.
    fn report(&self) -> Value {
        assert!(
            !self.timed_out,
            "`{}` did not finish in time:\n{}",
            self.what, self.stderr
        );
        assert_eq!(
            self.code,
            Some(0),
            "`{}` did not commit:\n{}",
            self.what,
            self.stderr
        );
        let report = json(self.stdout.trim());
        assert_eq!(report["ok"], true, "{report}");
        report
    }

    /// Exit 1, with every one of `phrases` in stderr.
    fn refused(&self, phrases: &[&str]) {
        assert!(
            !self.timed_out,
            "`{}` ran on instead of being refused:\n{}",
            self.what, self.stderr
        );
        assert_eq!(
            self.code,
            Some(1),
            "`{}` was not refused\nstdout: {}\nstderr: {}",
            self.what,
            self.stdout,
            self.stderr
        );
        for phrase in phrases {
            assert!(
                self.stderr.contains(phrase),
                "`{}` was refused without saying `{phrase}`:\n{}",
                self.what,
                self.stderr
            );
        }
    }
}

// ── A server ───────────────────────────────────────────────────────────────────────

struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
}

impl Server {
    fn query(&self, q: &str) -> Value {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = request(self.port, "POST", "/query", body.as_bytes());
        assert_eq!(status, 200, "query `{q}` answered {status}: {text}");
        json(&text)
    }

    fn count(&self, q: &str) -> u64 {
        let answer = self.query(q);
        answer["count"]
            .as_u64()
            .unwrap_or_else(|| panic!("`{q}` answered no count: {answer}"))
    }

    fn ingest(&self, body: &[u8]) -> Value {
        let (status, text) = request(self.port, "POST", "/ingest", body);
        assert_eq!(status, 200, "ingest answered {status}: {text}");
        json(&text)
    }

    /// SIGTERM, then wait for the exit - the graceful path, which closes every row group
    /// and seals every file.
    fn shutdown(mut self) -> String {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill -TERM failed");
        let status = self.child.wait().expect("failed to wait for triedb serve");
        let mut log = String::new();
        let _ = self.stderr.read_to_string(&mut log);
        assert_eq!(
            status.code(),
            Some(0),
            "a graceful shutdown did not exit 0:\n{log}"
        );
        // Already reaped; stop `Drop` from killing a pid that may have been reused.
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
    request_with(port, method, path, "", body)
}

/// `request`, with extra header lines - each ending in CRLF.
fn request_with(port: u16, method: &str, path: &str, extra: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\n{extra}\
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

fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

/// A JSON array of strings as a set; anything else is the empty set.
fn strings(v: &Value) -> BTreeSet<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|s| s.as_str().expect("an array of strings").to_string())
        .collect()
}

// ── Inputs ─────────────────────────────────────────────────────────────────────────

fn gen(args: &[&str]) -> Vec<u8> {
    let out = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .arg("gen-logs")
        .args(args)
        .output()
        .expect("failed to run gen-logs");
    assert!(out.status.success(), "gen-logs {args:?} failed");
    out.stdout
}

fn hostile(lines: usize, seed: u64) -> Vec<u8> {
    gen(&["--lines", &lines.to_string(), "--seed", &seed.to_string()])
}

fn farm(lines: usize, seed: u64) -> Vec<u8> {
    gen(&[
        "--profile",
        "farm",
        "--lines",
        &lines.to_string(),
        "--seed",
        &seed.to_string(),
    ])
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("gzip");
    encoder.finish().expect("gzip")
}

/// Through the crate the binary reads zstd with, so the input does not depend on which
/// `zstd` is on the machine, or on there being one.
fn zstd(from: &Path, to: &Path) {
    fs::create_dir_all(to.parent().expect("a parent")).expect("create the zstd directory");
    let bytes = fs::read(from).expect("read the zstd input");
    fs::write(to, zstd::stream::encode_all(&bytes[..], 3).expect("zstd")).expect("write zstd");
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The load id of one input: FNV-1a 64 over its file name, a 0 byte, the decompressed
/// length and the FNV-1a 64 of the decompressed bytes, both u64 LE; 0 is taken as 1.
/// Nothing about the compression or the directory enters it.
fn load_id(name: &str, content: &[u8]) -> String {
    let mut input = name.as_bytes().to_vec();
    input.push(0);
    input.extend_from_slice(&(content.len() as u64).to_le_bytes());
    input.extend_from_slice(&fnv1a(content).to_le_bytes());
    format!("{:016x}", fnv1a(&input).max(1))
}

/// `count` lines of one shape on one day, so they are one template and one file.
fn one_template(count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..count {
        let second = i % 3600;
        let line = format!(
            "{{\"ts\":\"2024-01-15T{:02}:{:02}:{:02}Z\",\"level\":\"info\",\"service\":\"auth\",\
             \"host\":\"h{}\",\"bytes\":{},\"n\":{}}}\n",
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

// ── Alone ──────────────────────────────────────────────────────────────────────────

/// No server holds the directory, so the command takes it, commits and exits. The proof
/// is a server started afterwards: a different process reading only what the load left.
#[test]
fn a_load_alone_commits_and_the_next_server_answers_every_row() {
    let db = Db::new();
    let input = db.input("hostile.ndjson", &hostile(LINES, 42));

    let report = db.load(&["--json"], &[&input]).report();
    assert_eq!(report["lines"], LINES as u64);
    assert_eq!(report["rows"], LINES as u64);
    assert_eq!(report["rejected"], 0);
    let id = report["load_id"]
        .as_str()
        .expect("the report names no load id");
    assert!(
        id.len() == 16
            && id
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "the load id is not 16 lowercase hex digits: {id}"
    );

    // Thirty days of rows, each reported file under its day's directory, and the report's
    // day count agreeing with those directories.
    let files = strings(&report["files"]);
    let days: BTreeSet<&str> = files.iter().filter_map(|f| f.split('/').next()).collect();
    assert!(days.len() >= 30, "thirty days of rows made {days:?}");
    assert_eq!(report["days"], days.len() as u64, "{report}");
    for file in &files {
        assert!(
            db.base().join("data").join(file).is_file(),
            "{file} is in the report and not in data/"
        );
    }
    assert!(db.staging().is_empty(), "left: {:?}", db.staging());

    // One record commits the whole load, so a crash lands on one side of it. Two would
    // leave a window in which a restart serves half the days.
    let records: Vec<Value> = db
        .manifest()
        .into_iter()
        .filter(|r| r["source"] == format!("load:{id}"))
        .collect();
    assert_eq!(records.len(), 1, "the load was not one manifest record");
    assert_eq!(records[0]["kind"], "add");

    // Every file live and finished: its row groups are already full, so compaction must
    // skip it rather than spend the IO rewriting it into the same thing.
    let live = db.live();
    assert_eq!(
        live.keys().cloned().collect::<BTreeSet<_>>(),
        files,
        "the manifest does not name exactly the load's files"
    );
    for (path, file) in &live {
        assert_eq!(file["finished"], true, "{path} is not flagged finished");
    }

    assert_eq!(db.count(ALL), LINES as u64);
}

/// The whole of a template's day in 8,192-row groups: the loader sees its input to the
/// end, so it has no reason to close a group early the way the live door must.
#[test]
fn a_load_writes_full_row_groups() {
    let db = Db::new();
    let input = db.input("one.ndjson", &one_template(20_000));

    let report = db.load(&["--json"], &[&input]).report();
    // 8,192 + 8,192 + 3,616.
    assert_eq!(report["row_groups"], 3, "{report}");

    let server = db.start();
    let answer = server.query(ALL);
    assert_eq!(answer["count"], 20_000);
    assert_eq!(answer["meta"]["row_groups_read"], 3, "{}", answer["meta"]);
}

// ── Repeats ────────────────────────────────────────────────────────────────────────

/// TrieDB keeps no row identity, so a file loaded twice would be every row twice with
/// nothing to tell them apart. The id is derived from the input's names and bytes, and a
/// known one is refused before anything is written.
///
/// The first load prints the summary line rather than `--json`: that line is what an
/// operator reads, and the id it prints is the one the refusal must name.
#[test]
fn a_repeated_load_is_refused_and_names_the_load() {
    let db = Db::new();
    let input = db.input("hostile.ndjson", &hostile(LINES, 42));

    let first = db.load(&[], &[&input]);
    assert_eq!(first.code, Some(0), "{}", first.stderr);
    let prefix = "loaded 2,000 rows from 2,000 lines (0 rejected) as load ";
    assert!(
        first.stdout.starts_with(prefix),
        "not the summary line: {}",
        first.stdout
    );
    let id = first
        .stdout
        .get(prefix.len()..prefix.len() + 16)
        .expect("the summary line names no load id");

    db.load(&["--json"], &[&input])
        .refused(&["was loaded before", id]);
    assert!(db.staging().is_empty(), "left: {:?}", db.staging());
    assert_eq!(db.count(ALL), LINES as u64, "the repeat added rows");
}

/// Same file name, same decompressed bytes: the same load, whatever the compression and
/// whichever directory it sits in - an archive re-fetched and recompressed under the name
/// it had. (Renamed, it would be a new load: the id is over names.)
#[test]
fn the_same_lines_gzipped_under_the_same_name_are_the_same_load() {
    let db = Db::new();
    let bytes = hostile(LINES, 42);
    let plain = db.input("plain/x.ndjson", &bytes);
    let packed = db.input("gzip/x.ndjson", &gzip(&bytes));

    let report = db.load(&["--json"], &[&plain]).report();
    let id = report["load_id"].as_str().expect("load_id").to_string();
    assert_eq!(
        id,
        load_id("x.ndjson", &bytes),
        "the load id is not the documented function of name and content"
    );

    db.load(&[], &[&packed])
        .refused(&["was loaded before", &id]);
    assert_eq!(db.count(ALL), LINES as u64, "the gzipped copy added rows");
}

// ── Through a server ───────────────────────────────────────────────────────────────

/// A server holds the directory, so the command is a client: it reads the address from
/// LOCK, posts the job, polls and prints the same report.
///
/// The count straight after is what proves the server ran it. A load that ignored the
/// lock and wrote the directory itself would reach this server only at its next start,
/// and would be a second writer of the catalog besides.
#[test]
fn a_load_through_a_running_server_is_answered_at_once() {
    let db = Db::new();
    let input = db.input("hostile.ndjson", &hostile(LINES, 42));
    let server = db.start();

    let report = db.load(&["--json"], &[&input]).report();
    assert_eq!(report["rows"], LINES as u64);
    let id = report["load_id"].as_str().expect("load_id").to_string();
    assert_eq!(
        server.count(ALL),
        LINES as u64,
        "the rows are not visible yet"
    );

    // The repeat check reads the running server's manifest, not a copy from its start.
    db.load(&[], &[&input]).refused(&["was loaded before", &id]);
    assert_eq!(server.count(ALL), LINES as u64, "the repeat added rows");

    // Committed to disk, not held in the server's memory.
    server.shutdown();
    assert_eq!(db.count(ALL), LINES as u64);
}

/// Stdin is spooled to a temp file before it is sent to a server. The id must still be
/// the stream's, not the spool file's: named after the temp file, the same archive piped
/// twice would be two loads and every row twice.
#[test]
fn the_same_stream_piped_twice_through_a_server_is_refused_the_second_time() {
    let db = Db::new();
    let bytes = hostile(LINES, 42);
    let packed = db.input("stream.gz", &gzip(&bytes));
    let server = db.start();

    let report = db.load_stdin(&["--json"], &packed).report();
    assert_eq!(report["rows"], LINES as u64);
    let id = report["load_id"].as_str().expect("load_id").to_string();
    assert_eq!(id, load_id("-", &bytes), "stdin is not named `-` in the id");

    db.load_stdin(&[], &packed)
        .refused(&["was loaded before", &id]);
    assert_eq!(server.count(ALL), LINES as u64);
}

// ── Replace-a-day ──────────────────────────────────────────────────────────────────

/// One source per day, and `--replace-day` is how the source changes: the day as it
/// arrived live, swapped for a clean load of the same lines from the archive. The count
/// must not move; twice it would be both sources counted.
///
/// The live door's files are sealed first, by a graceful restart, so the retired set is
/// known before the load runs. A replace would also write and seal the day's rows still in
/// memory before retiring them - the next test - and sealing first keeps `replaced`
/// comparable to what the restart left.
#[test]
fn replace_day_through_a_server_leaves_the_day_count_unchanged() {
    let db = Db::new();
    let bytes = farm(FARM_LINES, 1);
    let input = db.input("farm.ndjson", &bytes);

    let live = db.start();
    assert_eq!(live.ingest(&bytes)["ingested"], FARM_LINES as u64);
    live.shutdown();

    let server = db.start();
    let before = server.count(DAY_COUNT);
    assert_eq!(
        before, FARM_LINES as u64,
        "the ingest did not put every farm row on {DAY}"
    );
    let sealed = db.live_on(DAY);
    assert!(!sealed.is_empty(), "the ingest left no file on {DAY}");

    let report = db
        .load(&["--replace-day", DAY, "--json"], &[&input])
        .report();
    let replaced = strings(&report["replaced"]);
    assert_eq!(
        replaced, sealed,
        "the replace did not retire exactly the day's other files"
    );
    assert_eq!(server.count(DAY_COUNT), before, "the day's count moved");
    server.shutdown();

    // The next start deletes the retired files, and what is left is the load alone.
    let after = db.start();
    assert_eq!(
        after.count(DAY_COUNT),
        before,
        "the day's count moved on restart"
    );
    db.assert_day_is(DAY, &strings(&report["files"]));
}

/// The same, with the live door still holding the day: rows in its buffers and its day
/// file open. The replace writes and seals them first, then retires them with the rest, so
/// the day is the load's alone - counted once, before a restart and after.
#[test]
fn replace_day_on_a_warm_server_retires_the_rows_still_in_memory() {
    let db = Db::new();
    let bytes = farm(FARM_LINES, 1);
    let input = db.input("farm.ndjson", &bytes);

    let server = db.start();
    assert_eq!(server.ingest(&bytes)["ingested"], FARM_LINES as u64);
    let before = server.count(DAY_COUNT);
    assert_eq!(before, FARM_LINES as u64);

    let report = db
        .load(&["--replace-day", DAY, "--json"], &[&input])
        .report();
    assert!(
        !strings(&report["replaced"]).is_empty(),
        "the live rows were not retired: {report}"
    );
    assert_eq!(server.count(DAY_COUNT), before, "the day's count moved");
    server.shutdown();
    let after = db.start();
    assert_eq!(after.count(DAY_COUNT), before, "the count moved on restart");
    db.assert_day_is(DAY, &strings(&report["files"]));
}

/// A replace with nothing to publish would empty the day: an empty file, or a pipe from a
/// command that failed, is refused and changes nothing.
#[test]
fn replace_day_with_no_rows_is_refused_and_keeps_the_day() {
    let db = Db::new();
    let bytes = farm(FARM_LINES, 1);
    let input = db.input("farm.ndjson", &bytes);
    let empty = db.input("empty.ndjson", b"");
    db.load(&["--json"], &[&input]).report();

    db.load(&["--replace-day", DAY, "--json"], &[&empty])
        .refused(&["no rows", "Nothing was loaded"]);
    assert_eq!(
        db.count(DAY_COUNT),
        FARM_LINES as u64,
        "the day was emptied"
    );
    db.load(&["--json"], &[&empty])
        .refused(&["no rows", "Nothing was loaded"]);
}

/// One file reached twice - named twice, or through a link to its directory - is one input,
/// or every row of it would be stored twice under a single load id.
#[test]
#[cfg(unix)]
fn a_file_reached_twice_is_loaded_once() {
    let db = Db::new();
    let input = db.input("logs/2024-01-15/app.ndjson", &farm(500, 3));
    let logs = input
        .parent()
        .and_then(Path::parent)
        .expect("the logs directory")
        .to_path_buf();
    std::os::unix::fs::symlink(logs.join("2024-01-15"), logs.join("latest")).expect("symlink");
    std::os::unix::fs::symlink(".", logs.join("self")).expect("a link back to itself");
    let report = db.load(&["--json"], &[&logs, &input]).report();
    assert_eq!(report["rows"], 500, "{report}");
    assert_eq!(db.count(ALL), 500);

    // A directory given twice, inside another argument, or through a link to one already
    // given is its files once - not a directory refused as empty because it was seen.
    let day = logs.join("2024-01-15");
    let latest = logs.join("latest");
    for args in [
        vec![logs.clone(), logs.clone()],
        vec![logs.clone(), day.clone()],
        vec![day.clone(), latest.clone()],
    ] {
        let fresh = Db::new();
        let given: Vec<&Path> = args.iter().map(PathBuf::as_path).collect();
        let report = fresh.load(&["--json"], &given).report();
        assert_eq!(report["rows"], 500, "{args:?}: {report}");
    }
}

/// `/load` reads files with the server's permissions and serves their rows: it is taken
/// only with the token the server wrote to its `LOCK`. Without it, the answer is the same
/// whether the path named exists or not. With it, a file named twice is loaded once.
#[test]
fn a_load_request_without_the_lock_token_is_refused() {
    let db = Db::new();
    let input = db.input("farm.ndjson", &farm(300, 5));
    let server = db.start();
    let body = |path: &Path| {
        serde_json::json!({"inputs": [
            {"path": path, "name": "farm.ndjson"},
            {"path": path, "name": "farm.ndjson"},
        ]})
        .to_string()
    };
    let missing = db.root.join("in").join("no-such.ndjson");
    for path in [input.as_path(), missing.as_path()] {
        let (status, text) = request(server.port, "POST", "/load", body(path).as_bytes());
        assert_eq!(status, 403, "{text}");
        assert!(
            !text.contains("No such file"),
            "the refusal said whether it exists: {text}"
        );
    }
    let lock: Value = json(&fs::read_to_string(db.base().join("LOCK")).expect("LOCK"));
    let token = lock["token"].as_str().expect("a token in LOCK").to_string();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(db.base().join("LOCK"))
            .expect("LOCK")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "LOCK is readable by others: {mode:o}");
    }
    let shown = format!("x-triedb-load-token: {token}\r\n");
    let (status, text) = request_with(
        server.port,
        "POST",
        "/load",
        &shown,
        body(&input).as_bytes(),
    );
    assert_eq!(status, 202, "{text}");
    let job = json(&text)["job"].as_u64().expect("a job");
    let started = Instant::now();
    let state = loop {
        let (status, text) = request_with(server.port, "GET", &format!("/load/{job}"), &shown, b"");
        assert_eq!(status, 200, "{text}");
        let answer = json(&text);
        if answer["state"] != "running" {
            break answer;
        }
        assert!(
            started.elapsed() < DEADLINE,
            "load job {job} never finished"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(state["state"], "done", "{state}");
    assert_eq!(
        state["report"]["rows"], 300,
        "the file named twice was loaded twice"
    );
    assert_eq!(server.count(ALL), 300);
}

/// A pipe gives its bytes once and a load reads its input three times: named, it is
/// refused - `-` is how to pipe a load - rather than committed empty.
#[test]
#[cfg(unix)]
fn a_named_pipe_is_refused_and_standard_input_is_the_way_to_pipe() {
    let db = Db::new();
    let fifo = db.root.join("in").join("fifo");
    fs::create_dir_all(fifo.parent().expect("parent")).expect("mkdir");
    let made = Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo")
        .success();
    assert!(made, "mkfifo failed");
    db.load(&["--json"], &[&fifo])
        .refused(&["not a regular file", "triedb load -"]);
}

/// A repeated id is allowed with `--replace-day`: reloading the day from the same file
/// is exactly how a day loaded with the wrong settings is redone.
#[test]
fn a_repeated_load_is_allowed_with_replace_day_and_leaves_the_count_unchanged() {
    let db = Db::new();
    let input = db.input("farm.ndjson", &farm(FARM_LINES, 1));

    let first = db.load(&["--json"], &[&input]).report();
    let second = db
        .load(&["--replace-day", DAY, "--json"], &[&input])
        .report();
    assert_eq!(second["load_id"], first["load_id"]);
    assert_eq!(
        strings(&second["replaced"]),
        strings(&first["files"]),
        "the second load did not retire the first one's files"
    );

    let server = db.start();
    assert_eq!(server.count(DAY_COUNT), FARM_LINES as u64);
    db.assert_day_is(DAY, &strings(&second["files"]));
}

/// A replace names one day, and a row on another day would be published without
/// replacing anything there - a second source for that day, silently. So it refuses the
/// whole input, and refused means nothing at all: no rows, no staging, no manifest
/// record, and no id, or the same file loaded properly next is called a repeat.
#[test]
fn replace_day_with_a_row_outside_the_day_is_refused_and_loads_nothing() {
    let db = Db::new();
    // Thirty days of rows, most of them not on the day named.
    let input = db.input("hostile.ndjson", &hostile(LINES, 42));

    db.load(&["--replace-day", DAY], &[&input])
        .refused(&["outside --replace-day"]);
    assert!(db.staging().is_empty(), "left: {:?}", db.staging());
    assert!(
        db.live().is_empty(),
        "the refusal named files: {:?}",
        db.live()
    );
    assert_eq!(db.count(ALL), 0);

    let report = db.load(&["--json"], &[&input]).report();
    assert_eq!(report["rows"], LINES as u64);
}

// ── Arrival time ───────────────────────────────────────────────────────────────────

/// A row with no time of its own takes its arrival time, and for a file of old lines that
/// is the wrong day by construction - every row filed under today. The loader samples the
/// head of the input and stops unless told to proceed.
///
/// Checked in both modes, since the check runs in whichever process runs the load, and a
/// client that dropped `--proceed` on its way to the server would refuse forever. The
/// refusals record no id either: the `--proceed` load at the end is not a repeat.
#[test]
fn a_head_of_rows_without_a_time_is_refused_unless_told_to_proceed() {
    let db = Db::new();
    let body: String = (0..200)
        .map(|i| format!("{{\"level\":\"info\",\"msg\":\"m{i}\"}}\n"))
        .collect();
    let input = db.input("untimed.ndjson", body.as_bytes());

    db.load(&[], &[&input])
        .refused(&["arrival time", "--proceed"]);
    assert!(
        db.live().is_empty(),
        "the refusal named files: {:?}",
        db.live()
    );

    let server = db.start();
    assert_eq!(server.count(ALL), 0);
    db.load(&[], &[&input])
        .refused(&["arrival time", "--proceed"]);
    assert_eq!(server.count(ALL), 0);

    // `--received-at` puts them on a known day, which the count below reads back: the
    // rows took the arrival time given, not the clock's.
    let report = db
        .load(
            &[
                "--proceed",
                "--received-at",
                "2024-01-15T12:00:00Z",
                "--json",
            ],
            &[&input],
        )
        .report();
    assert_eq!(report["rows"], 200);
    assert_eq!(report["arrival"]["fell_back"], 200, "{report}");
    assert_eq!(server.count(ALL), 200);
    assert_eq!(server.count(DAY_COUNT), 200);
}

// ── The LOCK ───────────────────────────────────────────────────────────────────────

/// One process owns a data directory, so template ids have one issuer. A second server
/// exits rather than serving, naming the file that stopped it, and leaves the first alone.
#[test]
fn a_second_server_on_a_held_directory_exits_naming_lock() {
    let db = Db::new();
    let server = db.start();

    // The address a client load is sent to comes from here.
    let lock =
        json(&fs::read_to_string(db.base().join("LOCK")).expect("a running server wrote no LOCK"));
    assert_eq!(
        lock["address"],
        format!("127.0.0.1:{}", server.port),
        "{lock}"
    );

    let args = ["serve", "--port", "0", "--data-dir", &db.base_arg()].map(String::from);
    let second = db.spawn(&args, None).finish(REFUSAL_DEADLINE);
    assert!(
        !second.timed_out,
        "a second server started on a directory the first holds:\n{}",
        second.stderr
    );
    assert_eq!(second.code, Some(1), "{}", second.stderr);
    assert!(second.stderr.contains("LOCK"), "{}", second.stderr);

    assert_eq!(request(server.port, "GET", "/health", b"").0, 200);
}

/// A load running alone holds LOCK for its whole run: a server started meanwhile would be
/// a second writer, and a second load has no server to be a client of. Both exit naming
/// LOCK.
///
/// `--max-mb-per-sec` holds the load open about five seconds, which is also the test of
/// the throttle.
#[test]
fn a_load_running_alone_holds_the_directory_against_a_server_and_another_load() {
    let db = Db::new();
    let bytes = hostile(LINES, 42);
    let input = db.input("hostile.ndjson", &bytes);
    let other = db.input("farm.ndjson", &farm(200, 1));

    let rate = bytes.len() as f64 / 5.0 / 1e6;
    let rate_arg = format!("{rate:.3}");
    let mut running = db.spawn(
        &db.load_args(&["--json", "--max-mb-per-sec", &rate_arg], &[&input]),
        None,
    );

    // Taken when the file says so, not when the process exists.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let lock = fs::read_to_string(db.base().join("LOCK")).unwrap_or_default();
        let held = serde_json::from_str::<Value>(&lock).is_ok_and(|v| v["load"] == true);
        if held {
            break;
        }
        assert!(
            running.is_running(),
            "the load exited before LOCK said it held it"
        );
        assert!(
            Instant::now() < deadline,
            "the load never wrote LOCK: {lock:?}"
        );
        sleep(Duration::from_millis(20));
    }

    let args = ["serve", "--port", "0", "--data-dir", &db.base_arg()].map(String::from);
    let serve = db.spawn(&args, None).finish(REFUSAL_DEADLINE);
    let second = db.load(&[], &[&other]);
    // Read after both: a command that waited for the lock instead of refusing outlives the
    // first load, and then this is what tells a wait from a throttle that never held.
    let held = running.is_running();

    assert!(
        !serve.timed_out && serve.code == Some(1) && serve.stderr.contains("LOCK"),
        "a server on a directory a load holds was not refused naming LOCK \
         (first load still running after: {held}), exit {:?}:\n{}",
        serve.code,
        serve.stderr
    );
    assert!(
        second.code == Some(1) && second.stderr.contains("LOCK"),
        "a second load beside one holding LOCK was not refused naming it \
         (first load still running after: {held}), exit {:?}:\n{}",
        second.code,
        second.stderr
    );

    let report = running.finish(DEADLINE).report();
    assert_eq!(report["rows"], LINES as u64);
    // Half the ideal time, to allow for a burst at the start.
    let seconds = report["seconds"].as_f64().expect("seconds");
    assert!(
        seconds >= 2.5,
        "{} bytes at {rate_arg} MB/s took {seconds}s",
        bytes.len()
    );

    assert_eq!(db.count(ALL), LINES as u64, "the refused load added rows");
}

// ── Inputs ─────────────────────────────────────────────────────────────────────────

/// A directory is every regular file under it, recursively, each told apart by its first
/// bytes rather than its name; names starting with `.` are skipped, which is where editors
/// and sync tools leave their own files.
#[test]
fn a_directory_of_plain_and_zstd_files_loads_every_line_of_both() {
    let db = Db::new();
    db.input("batch/a.ndjson", &hostile(1_000, 1));
    let raw = db.input("raw/b.ndjson", &hostile(1_000, 2));
    let batch = db.root.join("in").join("batch");
    zstd(&raw, &batch.join("nested").join("b.ndjson.zst"));
    db.input("batch/.partial.ndjson", &farm(100, 1));

    let report = db.load(&["--json"], &[&batch]).report();
    assert_eq!(report["lines"], 2_000, "{report}");
    assert_eq!(report["rows"], 2_000);
    assert_eq!(db.count(ALL), 2_000);
}

/// A bad line is the line's problem, not the load's. It is reported with the number it
/// sits on - on stderr and in the report - and the rest commit.
#[test]
fn rejected_lines_are_counted_and_do_not_fail_the_load() {
    const BAD: [usize; 2] = [17, 400];
    let db = Db::new();
    let generated = hostile(1_000, 42);
    let mut lines: Vec<&[u8]> = generated
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    // Two kinds rather than two copies of one: a truncated document and a non-object take
    // different routes to a rejection, and both must report the right line.
    lines[BAD[0] - 1] = br#"{"event": "checkout", "user": "#;
    lines[BAD[1] - 1] = br#"["not", "an", "object"]"#;
    let mut body = lines.join(&b'\n');
    body.push(b'\n');
    let input = db.input("dirty.ndjson", &body);

    let ran = db.load(&["--json"], &[&input]);
    let report = ran.report();
    assert_eq!(report["lines"], 1_000);
    assert_eq!(report["rejected"], 2);
    assert_eq!(report["rows"], 998);
    let reported: Vec<u64> = report["errors"]
        .as_array()
        .expect("errors is an array")
        .iter()
        .map(|e| e["line"].as_u64().expect("an error has a line"))
        .collect();
    assert_eq!(reported, BAD.map(|n| n as u64).to_vec());
    for n in BAD {
        assert!(
            ran.stderr.contains(&format!("line {n}")),
            "line {n} is not reported on stderr:\n{}",
            ran.stderr
        );
    }

    assert_eq!(db.count(ALL), 998);
}
