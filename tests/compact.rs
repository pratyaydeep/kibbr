// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr compact`, from outside the process: a compaction changes nothing a query
//! can see, so what is checked is what it leaves on disk and what it says it did.
//! Every test builds a day through the live door and reads `manifest.log` and
//! `data/<day>/` directly, as `load.rs` does; the command is run alone, as a client of a
//! server, and by the server's own timer.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kibbr::storage::store::v3::file::{open_sealed, Reader};
use kibbr::storage::store::v3::FormatError;
use serde_json::Value;

/// Farm lines for most tests: one day, every line carrying its own time.
const LINES: usize = 3_000;

/// Farm lines for the tests that need a compaction still running when they act: a debug
/// build takes over a second to rebuild them.
const MANY_LINES: usize = 20_000;

/// The farm's one day, and the midnight that ended it, in seconds since the epoch.
const DAY: &str = "2024-01-15";
const DAY_ENDED: u64 = 1_705_363_200;

/// The day before it, for the tests that need two.
const OLDER: &str = "2024-01-14";

const ALL: &str = "COUNT LOG {}";
const BY_HOUR: &str = "COUNT LOG {} GROUP BY time(1h)";

/// Long enough for a debug build to rebuild the larger day several times over.
const DEADLINE: Duration = Duration::from_secs(60);

/// A command that should exit at once - a refusal - and is failed rather than waited on.
const REFUSAL_DEADLINE: Duration = Duration::from_secs(15);

/// How the live door is run to leave a day worth compacting: a row group closed every
/// 50 ms, and a file sealed every 64 KiB.
const LIVE: &[&str] = &[
    "--flush-interval-ms",
    "50",
    "--row-group-timeout-ms",
    "50",
    "--segment-max-bytes",
    "65536",
    "--compact-interval-ms",
    "0",
];

/// A server that compacts only when asked.
const NO_SCHEDULE: &[&str] = &["--compact-interval-ms", "0"];

/// The environment in which no day can be compacted: a compaction is refused unless the
/// volume has room for the day again beside the buffer limit, and none has beside this.
const NO_ROOM: &[(&str, &str)] = &[("KIBBR__STORAGE__MAX_BUFFER_BYTES", "900000000000000")];

// ── A scratch root: a data directory, and captured output ──────────────────────────

struct Db {
    root: PathBuf,
}

impl Db {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "kibbr-compact-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("runs")).expect("create the output directory");
        Self { root }
    }

    /// A data directory holding `lines` farm lines as the live door leaves them: many short
    /// row groups over several files, sealed and unfinished - a row group a body per template.
    fn with_live_day(lines: usize) -> Self {
        Self::with_live(&[farm(lines, 7)])
    }

    /// As `with_live_day`, of each input in turn: a day apiece, when each is one day's
    /// lines.
    fn with_live(inputs: &[Vec<u8>]) -> Self {
        let db = Self::new();
        let server = db.start(LIVE);
        let mut sent = 0;
        for input in inputs {
            let lines = input.split_inclusive(|b| *b == b'\n').count();
            let mut body = Vec::new();
            for (n, line) in input.split_inclusive(|b| *b == b'\n').enumerate() {
                body.extend_from_slice(line);
                if (n + 1) % 1_000 == 0 || n + 1 == lines {
                    let report = server.ingest(&body);
                    assert_eq!(report["errors"], serde_json::json!([]), "{report}");
                    body.clear();
                    server.wait_until_written();
                }
            }
            sent += lines as u64;
        }
        assert_eq!(server.count(ALL), sent);
        server.shutdown();
        db
    }

    fn base(&self) -> PathBuf {
        self.root.join("store")
    }

    fn base_arg(&self) -> String {
        self.base().display().to_string()
    }

    // ── Processes ──

    /// `kibbr <args>`, stdout and stderr to files: a pipe nobody reads until the exit
    /// can fill and stall the child, and a stalled child looks like a slow one.
    fn spawn(&self, args: &[String]) -> Running {
        self.spawn_with(args, &[], None)
    }

    /// As `spawn`, with these variables set and, if one is given, from this directory.
    fn spawn_with(&self, args: &[String], env: &[(&str, &str)], cwd: Option<&Path>) -> Running {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let out = self.root.join("runs").join(format!("{n}.out"));
        let err = self.root.join("runs").join(format!("{n}.err"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let child = command
            .args(args)
            // The schedule, the size cap and the directory are each test's own to set.
            .env_remove("KIBBR__STORAGE__COMPACT_INTERVAL_MS")
            .env_remove("KIBBR__STORAGE__SEGMENT_MAX_BYTES")
            .env_remove("KIBBR__STORAGE__BASE_PATH")
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(File::create(&out).expect("create the stdout file"))
            .stderr(File::create(&err).expect("create the stderr file"))
            .spawn()
            .expect("failed to spawn kibbr");
        Running {
            child,
            out,
            err,
            what: format!("kibbr {}", args.join(" ")),
        }
    }

    /// `kibbr compact --data-dir <store> <args>`, to its exit.
    fn compact(&self, args: &[&str]) -> Ran {
        self.compact_with(args, &[])
    }

    fn compact_with(&self, args: &[&str], env: &[(&str, &str)]) -> Ran {
        let mut all = vec!["compact".to_string(), "--data-dir".into(), self.base_arg()];
        all.extend(args.iter().map(|a| a.to_string()));
        self.spawn_with(&all, env, None).finish(DEADLINE)
    }

    fn start(&self, flags: &[&str]) -> Server {
        self.start_with(flags, &[])
    }

    fn start_with(&self, flags: &[&str], env: &[(&str, &str)]) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &self.base_arg()])
            .args(flags)
            .env_remove("KIBBR__STORAGE__COMPACT_INTERVAL_MS")
            .env_remove("KIBBR__STORAGE__SEGMENT_MAX_BYTES")
            .envs(env.iter().copied())
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
            }
            if line.contains("shutdown ") {
                break;
            }
        }
        let port = port.unwrap_or_else(|| panic!("no listening line in banner:\n{banner}"));
        let token = json(&fs::read_to_string(self.base().join("LOCK")).expect("LOCK"))["token"]
            .as_str()
            .expect("a running server wrote no token to LOCK")
            .to_string();
        Server {
            child,
            port,
            stderr: Some(stderr),
            banner,
            token,
        }
    }

    // ── The directory's own files ──

    fn manifest_bytes(&self) -> Vec<u8> {
        fs::read(self.base().join("manifest.log")).unwrap_or_default()
    }

    /// Every record in `manifest.log`, in order, as `load.rs` walks them.
    fn manifest(&self) -> Vec<Value> {
        let bytes = self.manifest_bytes();
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

    /// The day's live files by path, after every record is applied: the last word on a
    /// path wins, a removed path is gone, and a replace removes before it adds.
    fn live(&self) -> BTreeMap<String, Value> {
        self.live_of(DAY)
    }

    fn live_of(&self, day: &str) -> BTreeMap<String, Value> {
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
        files.retain(|path, _| path.starts_with(&format!("{day}/")));
        files
    }

    /// The `.col` files under `data/<day>/`, spelled the way the manifest spells them.
    fn on_disk(&self) -> BTreeSet<String> {
        self.on_disk_of(DAY)
    }

    fn on_disk_of(&self, day: &str) -> BTreeSet<String> {
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

    /// A day as the live door left it, for a test with more than one: its files, which
    /// are several, sealed, unfinished and exactly what its directory holds.
    fn live_files_of(&self, day: &str) -> BTreeSet<String> {
        let live = self.live_of(day);
        assert!(live.len() >= 2, "{day} is {} file(s)", live.len());
        for (path, file) in &live {
            assert_eq!(file["sealed"], true, "{path} was left unsealed");
            assert_ne!(file["finished"], true, "{path} is finished already");
        }
        let paths: BTreeSet<String> = live.into_keys().collect();
        assert_eq!(self.on_disk_of(day), paths);
        paths
    }

    /// The bytes on disk of the files named, together.
    fn bytes_of(&self, paths: &BTreeSet<String>) -> u64 {
        paths
            .iter()
            .map(|p| {
                fs::metadata(self.base().join("data").join(p))
                    .unwrap_or_else(|e| panic!("{p}: {e}"))
                    .len()
            })
            .sum()
    }

    /// The day as the live door left it: its files, and the sum of their sizes. Asserts
    /// it is a day compaction has something to do to.
    fn live_day(&self) -> (BTreeSet<String>, u64) {
        let live = self.live();
        assert!(live.len() >= 2, "the live day is {} file(s)", live.len());
        for (path, file) in &live {
            assert_eq!(file["sealed"], true, "{path} was left unsealed");
            assert_ne!(file["finished"], true, "{path} is finished already");
        }
        let paths: BTreeSet<String> = live.into_keys().collect();
        assert_eq!(self.on_disk(), paths);
        let bytes = self.bytes_of(&paths);
        (paths, bytes)
    }

    /// The day was compacted once since `before`: one `replace` record, retiring exactly the
    /// live files for finished ones under new paths, and the directory holds only those.
    fn assert_compacted(
        &self,
        records_before: usize,
        before: &BTreeSet<String>,
    ) -> BTreeSet<String> {
        let records = self.manifest();
        assert_eq!(
            records.len(),
            records_before + 1,
            "a compaction is one manifest record: {:?}",
            &records[records_before..]
        );
        let replace = &records[records_before];
        assert_eq!(replace["kind"], "replace", "{replace}");
        assert_eq!(replace["source"], "compact", "{replace}");
        assert_eq!(&strings(&replace["remove"]), before, "{replace}");
        let live = self.live();
        let added: BTreeSet<String> = replace["add"]
            .as_array()
            .expect("a replace adds a list")
            .iter()
            .map(|f| f["path"].as_str().expect("a path").to_string())
            .collect();
        assert!(!added.is_empty(), "the replace adds no file");
        assert!(
            added.is_disjoint(before),
            "a path was used again: {added:?}"
        );
        assert_eq!(live.keys().cloned().collect::<BTreeSet<_>>(), added);
        for (path, file) in &live {
            assert_eq!(file["sealed"], true, "{path} is not sealed");
            assert_eq!(file["finished"], true, "{path} is not flagged finished");
        }
        assert_eq!(
            self.on_disk(),
            added,
            "data/{DAY} is not the manifest's live files"
        );
        assert!(
            !self.base().join("staging").exists()
                || fs::read_dir(self.base().join("staging")).is_ok_and(|mut d| d.next().is_none()),
            "a compaction left something in staging/"
        );
        added
    }

    /// Every data file given the modification time of the midnight that ended the farm's
    /// day, which is what a start reads "the last live row" from.
    fn backdate(&self) {
        let ended = UNIX_EPOCH + Duration::from_secs(DAY_ENDED);
        for path in self.on_disk_of(OLDER).into_iter().chain(self.on_disk()) {
            let path = self.base().join("data").join(path);
            File::options()
                .write(true)
                .open(&path)
                .and_then(|f| f.set_modified(ended))
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
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
            match self.child.try_wait().expect("failed to poll kibbr") {
                Some(status) => break status,
                None if Instant::now() >= deadline => {
                    timed_out = true;
                    let _ = self.child.kill();
                    break self.child.wait().expect("failed to wait for kibbr");
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
    /// Exit 0, and what it printed.
    fn ok(&self) -> &str {
        assert!(
            !self.timed_out,
            "`{}` did not finish in time:\n{}",
            self.what, self.stderr
        );
        assert_eq!(
            self.code,
            Some(0),
            "`{}` failed:\n{}",
            self.what,
            self.stderr
        );
        &self.stdout
    }

    /// Exit 0 and the `--json` report, which must say `ok`.
    fn report(&self) -> Value {
        let report = json(self.ok().trim());
        assert_eq!(report["ok"], true, "{report}");
        report
    }

    /// Exit 1, nothing on stdout, and every one of `phrases` in stderr.
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
        assert_eq!(self.stdout, "", "`{}` was refused and printed", self.what);
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
    /// `None` once the test has closed its end of the pipe.
    stderr: Option<BufReader<ChildStderr>>,
    banner: String,
    /// What `LOCK` says a `/compact` is asked with.
    token: String,
}

impl Server {
    fn query(&self, q: &str) -> Value {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = request(self.port, "POST", "/query", "", body.as_bytes());
        assert_eq!(status, 200, "query `{q}` answered {status}: {text}");
        json(&text)
    }

    fn count(&self, q: &str) -> u64 {
        let answer = self.query(q);
        answer["count"]
            .as_u64()
            .unwrap_or_else(|| panic!("`{q}` answered no count: {answer}"))
    }

    /// How many row groups the store holds: every one is read or pruned by a count of
    /// everything. Never compared as an answer - changing it is what compaction is for.
    fn row_groups(&self) -> u64 {
        let meta = &self.query(ALL)["meta"];
        meta["row_groups_read"].as_u64().expect("row_groups_read")
            + meta["row_groups_pruned"]
                .as_u64()
                .expect("row_groups_pruned")
    }

    /// What a query can see of the day, in a form two servers' answers compare by: the
    /// count, the count by the hour, and every template's row count.
    fn answers(&self) -> Value {
        let (status, text) = request(self.port, "GET", "/templates", "", b"");
        assert_eq!(status, 200, "{text}");
        let templates: BTreeMap<String, Value> = json(&text)["templates"]
            .as_array()
            .expect("templates")
            .iter()
            .map(|t| (t["template_id"].to_string(), t["rows"].clone()))
            .collect();
        serde_json::json!({
            "count": self.count(ALL),
            "by_hour": self.query(BY_HOUR)["groups"],
            "templates": templates,
        })
    }

    fn ingest(&self, body: &[u8]) -> Value {
        let (status, text) = request(self.port, "POST", "/ingest", "", body);
        assert_eq!(status, 200, "ingest answered {status}: {text}");
        json(&text)
    }

    fn stats(&self) -> Value {
        let (status, text) = request(self.port, "GET", "/stats", "", b"");
        assert_eq!(status, 200, "{text}");
        json(&text)
    }

    /// `/stats`'s `storage.compaction`.
    fn compaction(&self) -> Value {
        self.stats()["storage"]["compaction"].clone()
    }

    /// Until no row is still in a buffer: every row ingested is in a closed row group.
    fn wait_until_written(&self) {
        let deadline = Instant::now() + DEADLINE;
        while self.stats()["storage"]["buffered_rows"] != 0 {
            assert!(Instant::now() < deadline, "rows were still buffered");
            sleep(Duration::from_millis(10));
        }
    }

    /// A request showing the token.
    fn asked(&self, method: &str, path: &str, body: &str) -> (u16, Value) {
        let shown = format!("x-kibbr-load-token: {}\r\n", self.token);
        let (status, text) = request(self.port, method, path, &shown, body.as_bytes());
        (status, json(&text))
    }

    /// `GET /compact/<job>` until it is no longer running.
    fn job(&self, job: u64) -> Value {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let (status, answer) = self.asked("GET", &format!("/compact/{job}"), "");
            assert_eq!(status, 200, "{answer}");
            if answer["state"] != "running" {
                return answer;
            }
            assert!(
                Instant::now() < deadline,
                "compaction job {job} never finished"
            );
            sleep(Duration::from_millis(20));
        }
    }

    /// Close this end of the server's stderr, as a log collector that went away would:
    /// whatever the server writes there from now on fails.
    fn close_stderr(&mut self) {
        self.stderr = None;
    }

    /// SIGTERM, then wait for the exit - the graceful path, which closes every row group
    /// and seals every file. Returns the server's stderr after its banner.
    fn shutdown(mut self) -> String {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill -TERM failed");
        let status = self.child.wait().expect("failed to wait for kibbr serve");
        let mut log = String::new();
        if let Some(stderr) = self.stderr.as_mut() {
            let _ = stderr.read_to_string(&mut log);
        }
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

/// One request, with extra header lines - each ending in CRLF.
fn request(port: u16, method: &str, path: &str, extra: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    // A server that never answers fails the test at the deadline; with no timeout the read
    // below waits on it for good, and nothing between two requests ever looks at a clock.
    socket
        .set_read_timeout(Some(DEADLINE))
        .expect("set the read timeout");
    socket
        .set_write_timeout(Some(DEADLINE))
        .expect("set the write timeout");
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

fn farm(lines: usize, seed: u64) -> Vec<u8> {
    farm_on(DAY, lines, seed)
}

/// Farm lines of `day`, YYYY-MM-DD.
fn farm_on(day: &str, lines: usize, seed: u64) -> Vec<u8> {
    let out = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["gen-logs", "--profile", "farm", "--day", day])
        .args(["--lines", &lines.to_string(), "--seed", &seed.to_string()])
        .output()
        .expect("failed to run gen-logs");
    assert!(out.status.success(), "gen-logs failed");
    out.stdout
}

/// The UTC day it is, as days since the epoch.
fn today() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_secs()
        / 86_400
}

/// `1,234,567` as the number it is.
fn number(text: &str) -> u64 {
    text.replace(',', "")
        .parse()
        .unwrap_or_else(|_| panic!("`{text}` is not a count"))
}

/// The seven counts of the line a compacted day prints, in the order it gives them; the
/// line must be exactly that shape.
fn counts_of(line: &str) -> [u64; 7] {
    let shape = |rest: &str| -> Option<[u64; 7]> {
        let rest = rest.strip_prefix(&format!("compacted {DAY}: "))?;
        let (rows, rest) = rest.split_once(" rows, ")?;
        let (groups_in, rest) = rest.split_once(" row groups in ")?;
        let (files_in, rest) = rest.split_once(" file(s) (")?;
        let (bytes_in, rest) = rest.split_once(" bytes) -> ")?;
        let (groups_out, rest) = rest.split_once(" row groups in ")?;
        let (files_out, rest) = rest.split_once(" file(s) (")?;
        let (bytes_out, rest) = rest.split_once(" bytes), in ")?;
        let seconds = rest.strip_suffix('s')?;
        seconds.parse::<f64>().ok()?;
        Some(
            [
                rows, groups_in, files_in, bytes_in, groups_out, files_out, bytes_out,
            ]
            .map(number),
        )
    };
    shape(line).unwrap_or_else(|| panic!("not the line a compacted day prints: {line:?}"))
}

// ── Alone ──────────────────────────────────────────────────────────────────────────

/// No server holds the directory, so the command takes it and compacts. Everything is
/// read before any server starts, since a start deletes what the manifest does not name.
#[test]
fn a_compaction_alone_rebuilds_a_live_day_and_a_second_one_changes_nothing() {
    let db = Db::with_live_day(LINES);
    let (before, bytes_before) = db.live_day();
    let records = db.manifest().len();
    let (answers, groups_before) = {
        let server = db.start(NO_SCHEDULE);
        let seen = (server.answers(), server.row_groups());
        server.shutdown();
        seen
    };
    assert_eq!(
        db.manifest().len(),
        records,
        "a start and a stop wrote a record"
    );

    let ran = db.compact(&["--day", DAY]);
    let printed = ran.ok();
    assert_eq!(
        printed.matches('\n').count(),
        1,
        "more than one line: {printed:?}"
    );
    let [rows, groups_in, files_in, bytes_in, groups_out, files_out, bytes_out] =
        counts_of(printed.trim_end_matches('\n'));

    let added = db.assert_compacted(records, &before);
    assert_eq!(rows, LINES as u64);
    assert_eq!(files_in, before.len() as u64);
    assert_eq!(bytes_in, bytes_before);
    assert_eq!(groups_in, groups_before);
    assert_eq!(files_out, added.len() as u64);
    assert_eq!(bytes_out, db.bytes_of(&added));
    assert!(
        groups_out < groups_in,
        "{groups_out} row groups from {groups_in}"
    );
    let live_rows: u64 = db
        .live()
        .values()
        .map(|f| f["rows"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(live_rows, LINES as u64, "the rebuilt files' row counts");

    // Again, each way of asking: nothing to do, said, and not a byte written.
    let manifest = db.manifest_bytes();
    assert_eq!(
        db.compact(&["--day", DAY]).ok(),
        format!("{DAY}: nothing to compact\n")
    );
    let report = db.compact(&["--day", DAY, "--json"]).report();
    assert_eq!(
        report["days"],
        serde_json::json!([{
            "day": DAY, "compacted": false, "rows": 0, "files_in": 0, "files_out": 0,
            "row_groups_in": 0, "row_groups_out": 0, "bytes_in": 0, "bytes_out": 0,
            "seconds": 0.0, "added": [], "removed": [],
        }])
    );
    assert_eq!(db.compact(&[]).ok(), "", "no day is due");
    assert_eq!(
        db.compact(&["--json"]).report(),
        serde_json::json!({"ok": true, "days": []})
    );
    assert_eq!(
        db.manifest_bytes(),
        manifest,
        "a no-op wrote to manifest.log"
    );

    // A different process, reading only what the compaction left.
    let server = db.start(NO_SCHEDULE);
    assert_eq!(
        server.answers(),
        answers,
        "the day does not answer as it did"
    );
    assert_eq!(server.row_groups(), groups_out);
    server.shutdown();
    assert_eq!(
        db.manifest_bytes(),
        manifest,
        "a start and a stop wrote a record"
    );
}

/// With no `--day`, the days that are due: a day whose last live row was written before
/// today. The day here is due only once its files are dated the midnight it ended.
#[test]
fn compact_with_no_day_takes_a_day_whose_last_live_row_was_before_today() {
    let started_on = today();
    let db = Db::with_live_day(LINES);
    let (before, _) = db.live_day();
    let records = db.manifest().len();

    let ran = db.compact(&[]);
    // Midnight UTC since the day was written makes it due as it stands: the command was
    // right to take it, and there is then nothing to hold either half to.
    if today() != started_on {
        return;
    }
    assert_eq!(ran.ok(), "", "a day written today was taken as due");
    assert_eq!(db.manifest().len(), records);

    db.backdate();
    let report = db.compact(&["--json"]).report();
    let days = report["days"].as_array().expect("days");
    assert_eq!(days.len(), 1, "{report}");
    assert_eq!(days[0]["day"], DAY);
    assert_eq!(days[0]["compacted"], true);
    assert_eq!(days[0]["rows"], LINES as u64);
    let added = db.assert_compacted(records, &before);
    assert_eq!(strings(&days[0]["added"]), added);
    assert_eq!(strings(&days[0]["removed"]), before);
}

/// A refused command makes no directory, takes no lock, runs no recovery and writes no
/// manifest - alone, or beside a server it would have asked.
#[test]
fn a_day_that_is_not_a_date_is_refused_before_anything_is_touched() {
    let db = Db::new();
    for day in ["yesterday", "2024-13-45", "20240115"] {
        let ran =
            db.spawn(&["compact", "--data-dir", &db.base_arg(), "--day", day].map(String::from));
        ran.finish(REFUSAL_DEADLINE)
            .refused(&[&format!("--day {day}: not a YYYY-MM-DD day")]);
    }
    assert!(
        !db.base().exists(),
        "a refused command made the directory it was pointed at"
    );

    let _server = db.start(NO_SCHEDULE);
    let ran = db.compact(&["--day", "2024-13-45"]);
    ran.refused(&["--day 2024-13-45: not a YYYY-MM-DD day"]);
    // A server refuses the same day in words of its own, which the command would have
    // passed on: not seeing them is what says the request was never sent.
    assert!(
        !ran.stderr.contains("day `2024-13-45` is not"),
        "the server was asked:\n{}",
        ran.stderr
    );
}

/// A compaction makes nothing new, so a path that is not a data directory is refused
/// before anything is made there: opened as a start opens one, its `data/` would be deleted.
#[test]
fn a_path_that_is_not_a_data_directory_is_refused_and_nothing_is_made_or_deleted() {
    let db = Db::new();
    let not_one = "not a kibbr data directory (no manifest.log); nothing was compacted";
    let names = |dir: &Path| -> BTreeSet<String> {
        fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect()
    };

    // A path that is not there, with a day and without.
    for args in [&[][..], &["--day", DAY], &["--json"]] {
        db.compact(args).refused(&[&db.base_arg(), not_one]);
        assert!(!db.base().exists(), "a refused command made its directory");
    }

    // The default path, from a directory that holds no `kibbr-data` - where a command
    // run beside a server that keeps its rows in memory finds itself.
    let elsewhere = db.root.join("elsewhere");
    fs::create_dir_all(&elsewhere).expect("create a working directory");
    db.spawn_with(&["compact".to_string()], &[], Some(&elsewhere))
        .finish(REFUSAL_DEADLINE)
        .refused(&["kibbr-data", not_one]);
    assert_eq!(names(&elsewhere), BTreeSet::new(), "it made something");

    // A directory with a `data/` of someone else's files.
    let project = db.root.join("project");
    fs::create_dir_all(project.join("data").join("sub")).expect("create the project");
    let theirs = [
        project.join("data").join("important.csv"),
        project.join("data").join("sub").join("x.txt"),
    ];
    for path in &theirs {
        fs::write(path, "theirs\n").expect("write a file of theirs");
    }
    let dir = project.display().to_string();
    for args in [
        &["compact", "--data-dir", &dir][..],
        &["compact", "--data-dir", &dir, "--day", DAY],
    ] {
        db.spawn(&args.iter().map(|a| a.to_string()).collect::<Vec<_>>())
            .finish(REFUSAL_DEADLINE)
            .refused(&[&dir, not_one]);
        for path in &theirs {
            assert_eq!(
                fs::read_to_string(path).ok().as_deref(),
                Some("theirs\n"),
                "{} did not survive",
                path.display()
            );
        }
        assert_eq!(names(&project), BTreeSet::from(["data".to_string()]));
    }

    // And inside a real one: its `data/` is not a data directory either, and nothing is
    // planted there for the next start to find.
    db.start(NO_SCHEDULE).shutdown();
    let inside = db.base().join("data");
    let was = names(&inside);
    let dir = inside.display().to_string();
    db.spawn(&["compact", "--data-dir", &dir].map(String::from))
        .finish(REFUSAL_DEADLINE)
        .refused(&[&dir, not_one]);
    assert_eq!(names(&inside), was);
    // The directory itself is one, with no day in it: none is due, and that is exit 0.
    assert_eq!(db.compact(&[]).ok(), "");
}

/// Every row group of a sealed data file, read as a rebuild reads it: the index through
/// the footer, then each header frame and every chunk, CRCs checked and chunks decoded.
fn read_whole(path: &Path) -> Result<(), FormatError> {
    let index = open_sealed(path)?.unwrap_or_else(|| panic!("{} is not sealed", path.display()));
    let mut reader = Reader::open(path)?;
    for entry in &index.entries {
        let columns = index.schemas[entry.schema_id as usize].len();
        let header = reader.header(entry, columns)?;
        let wanted: Vec<usize> = (0..header.chunks.len()).collect();
        reader.chunks(entry, &header, &wanted)?;
    }
    Ok(())
}

/// Turn over 8 bytes inside the first column chunk of a data file's first row group, so
/// its CRC no longer holds; found through the footer, since mid-file may be a schema record.
fn damage(path: &Path) {
    let said = |e| panic!("{}: {e}", path.display());
    read_whole(path).unwrap_or_else(|e| panic!("refused before it was damaged: {e}"));
    let index = open_sealed(path).expect("open").expect("sealed");
    let entry = index.entries.first().expect("a row group");
    let columns = index.schemas[entry.schema_id as usize].len();
    let header = Reader::open(path)
        .and_then(|mut r| r.header(entry, columns))
        .unwrap_or_else(|e| panic!("{e}"));
    let (chunk, start) = header
        .chunks
        .iter()
        .zip(header.chunk_offsets())
        .find(|(chunk, _)| chunk.stored_len >= 8)
        .expect("a chunk of 8 bytes");
    // 16 bytes into the chunk, or as far in as a shorter one has room for.
    let at = entry.offset
        + u64::from(entry.frame_len)
        + start
        + u64::from(chunk.stored_len - 8).min(16);
    let mut file = File::options()
        .read(true)
        .write(true)
        .open(path)
        .unwrap_or_else(said);
    let mut bytes = [0u8; 8];
    file.seek(SeekFrom::Start(at)).expect("seek");
    file.read_exact(&mut bytes).expect("read");
    file.seek(SeekFrom::Start(at)).expect("seek");
    file.write_all(&bytes.map(|b| !b)).expect("overwrite");
}

/// With no `--day` and two days due, both are compacted, the older first: a record a
/// day, each taking out that day's files and no other's.
#[test]
fn compact_with_no_day_takes_every_day_that_is_due() {
    let db = Db::with_live(&[farm_on(OLDER, LINES, 11), farm(LINES, 7)]);
    let before = [OLDER, DAY].map(|day| db.live_files_of(day));
    let records = db.manifest().len();
    db.backdate();

    let report = db.compact(&["--json"]).report();
    let days = report["days"].as_array().expect("days");
    assert_eq!(days.len(), 2, "{report}");
    let written = db.manifest();
    assert_eq!(written.len(), records + 2, "a record a day");
    for (n, day) in [OLDER, DAY].into_iter().enumerate() {
        assert_eq!(days[n]["day"], day, "{report}");
        assert_eq!(days[n]["compacted"], true, "{report}");
        assert_eq!(days[n]["rows"], LINES as u64, "{report}");
        assert_eq!(strings(&days[n]["removed"]), before[n], "{report}");
        let replace = &written[records + n];
        assert_eq!(replace["kind"], "replace", "{replace}");
        assert_eq!(replace["source"], "compact", "{replace}");
        assert_eq!(strings(&replace["remove"]), before[n], "{replace}");
        let live = db.live_of(day);
        assert!(live.values().all(|file| file["finished"] == true));
        let live: BTreeSet<String> = live.into_keys().collect();
        assert_eq!(strings(&days[n]["added"]), live, "{report}");
        assert_eq!(db.on_disk_of(day), live, "data/{day} is not its live files");
    }

    assert_eq!(db.compact(&[]).ok(), "", "a compacted day is due again");
    assert_eq!(db.manifest().len(), records + 2);
}

/// Of two days due, the older cannot be rebuilt. The day after it is compacted all the
/// same and the command is exit 1: stopping at the first failure would starve later days.
#[test]
fn a_day_that_fails_does_not_stop_the_days_after_it() {
    let db = Db::with_live(&[farm_on(OLDER, LINES, 11), farm(LINES, 7)]);
    let before = [OLDER, DAY].map(|day| db.live_files_of(day));
    let records = db.manifest().len();
    let first = before[0].iter().next().expect("a file");
    let damaged = db.base().join("data").join(first);
    damage(&damaged);
    assert!(
        read_whole(&damaged).is_err(),
        "{} was damaged and still reads whole",
        damaged.display()
    );
    db.backdate();

    let ran = db.compact(&[]);
    assert!(!ran.timed_out, "{}", ran.stderr);
    assert_eq!(
        ran.code,
        Some(1),
        "a day failed and the command did not\nstdout: {}\nstderr: {}",
        ran.stdout,
        ran.stderr
    );
    let printed: Vec<&str> = ran.stdout.lines().collect();
    assert_eq!(printed.len(), 1, "{:?}", ran.stdout);
    assert_eq!(counts_of(printed[0])[0], LINES as u64);
    assert!(
        ran.stderr.contains(&format!("compact {OLDER}: "))
            && ran.stderr.contains("1 of the 2 day(s)"),
        "{}",
        ran.stderr
    );
    db.assert_compacted(records, &before[1]);
    let older: BTreeSet<String> = db.live_of(OLDER).into_keys().collect();
    assert_eq!(older, before[0], "the day that failed is not as it was");
    assert_eq!(db.on_disk_of(OLDER), before[0], "a failed day left a file");

    // Again: the one day still due still fails, and is now the whole of the count.
    let manifest = db.manifest_bytes();
    db.compact(&[])
        .refused(&[&format!("compact {OLDER}: "), "1 of the 1 day(s)"]);
    assert_eq!(db.manifest_bytes(), manifest);
}

/// A day that cannot be compacted is exit 1 with nothing on stdout and nothing changed,
/// alone and through a server: exit 0 is a cron job reporting success on a day never rebuilt.
#[test]
fn a_day_that_cannot_be_compacted_is_exit_1_alone_and_through_a_server() {
    let db = Db::with_live_day(LINES);
    let (before, _) = db.live_day();
    let manifest = db.manifest_bytes();
    let said = format!("compact {DAY}: ");

    for args in [&["--day", DAY][..], &["--day", DAY, "--json"]] {
        db.compact_with(args, NO_ROOM)
            .refused(&[&said, "Nothing was compacted"]);
    }
    assert_eq!(db.manifest_bytes(), manifest);
    assert_eq!(db.on_disk(), before);

    let server = db.start_with(NO_SCHEDULE, NO_ROOM);
    for args in [&["--day", DAY][..], &["--day", DAY, "--json"]] {
        db.compact(args).refused(&[&said, "Nothing was compacted"]);
    }
    let stats = server.compaction();
    assert_eq!(stats["runs"], 2, "{stats}");
    assert_eq!(stats["days_compacted"], 0, "{stats}");
    assert_eq!(stats["last"], Value::Null, "{stats}");
    assert!(
        stats["last_error"]
            .as_str()
            .is_some_and(|e| e.starts_with(&said)),
        "{stats}"
    );

    let body = serde_json::json!({ "day": DAY }).to_string();
    let (status, answer) = server.asked("POST", "/compact", &body);
    assert_eq!(status, 202, "{answer}");
    let state = server.job(answer["job"].as_u64().expect("a job number"));
    assert_eq!(state["state"], "failed", "{state}");
    assert!(
        state["error"]
            .as_str()
            .is_some_and(|e| e.starts_with(&said)),
        "{state}"
    );
    assert_eq!(
        state["report"],
        serde_json::json!({"ok": false, "days": []}),
        "{state}"
    );
    assert_eq!(server.count(ALL), LINES as u64);
    server.shutdown();
    assert_eq!(db.manifest_bytes(), manifest);
    assert_eq!(db.on_disk(), before);
}

// ── Through a server ───────────────────────────────────────────────────────────────

/// A server holds the directory, so the command asks it and prints its report: its
/// numbers are the manifest's and the directory's, and `/stats` counts the day once.
#[test]
fn a_compaction_through_a_server_reports_what_the_manifest_and_stats_say() {
    let db = Db::with_live_day(LINES);
    let (before, bytes_before) = db.live_day();
    let records = db.manifest().len();
    let server = db.start(NO_SCHEDULE);
    let answers = server.answers();
    let groups_before = server.row_groups();
    assert_eq!(
        server.compaction(),
        serde_json::json!({
            "running": false, "runs": 0, "days_compacted": 0, "last": null, "last_error": null,
        })
    );

    let report = db.compact(&["--day", DAY, "--json"]).report();
    let days = report["days"].as_array().expect("days");
    assert_eq!(days.len(), 1, "{report}");
    let day = &days[0];
    let added = db.assert_compacted(records, &before);
    let groups_after = server.row_groups();
    assert!(
        groups_after < groups_before,
        "{groups_after} from {groups_before}"
    );
    assert_eq!(day["day"], DAY);
    assert_eq!(day["compacted"], true);
    assert_eq!(day["rows"], LINES as u64);
    assert_eq!(day["files_in"], before.len() as u64);
    assert_eq!(day["files_out"], added.len() as u64);
    assert_eq!(day["row_groups_in"], groups_before);
    assert_eq!(day["row_groups_out"], groups_after);
    assert_eq!(day["bytes_in"], bytes_before);
    assert_eq!(day["bytes_out"], db.bytes_of(&added));
    assert_eq!(strings(&day["added"]), added);
    assert_eq!(strings(&day["removed"]), before);
    assert!(day["seconds"].as_f64().is_some_and(|s| s >= 0.0), "{day}");

    assert_eq!(
        server.answers(),
        answers,
        "the day does not answer as it did"
    );
    let stats = server.compaction();
    assert_eq!(stats["running"], false, "{stats}");
    assert_eq!(stats["runs"], 1, "{stats}");
    assert_eq!(stats["days_compacted"], 1, "{stats}");
    assert_eq!(&stats["last"], day, "{stats}");
    assert_eq!(stats["last_error"], Value::Null, "{stats}");

    // Again through the server: a job that tried a day, and compacted none.
    let manifest = db.manifest_bytes();
    assert_eq!(
        db.compact(&["--day", DAY]).ok(),
        format!("{DAY}: nothing to compact\n")
    );
    assert_eq!(db.compact(&[]).ok(), "", "no day is due");
    assert_eq!(
        db.manifest_bytes(),
        manifest,
        "a no-op wrote to manifest.log"
    );
    let stats = server.compaction();
    assert_eq!(stats["runs"], 2, "{stats}");
    assert_eq!(stats["days_compacted"], 1, "{stats}");
    assert_eq!(&stats["last"], day, "{stats}");

    // The size cap is the server's: said, not obeyed and not an error.
    let ran = db.compact(&["--day", DAY, "--segment-max-bytes", "1"]);
    assert_eq!(ran.ok(), format!("{DAY}: nothing to compact\n"));
    assert!(
        ran.stderr.contains("--segment-max-bytes is ignored"),
        "{}",
        ran.stderr
    );

    server.shutdown();
    let server = db.start(NO_SCHEDULE);
    assert_eq!(
        server.answers(),
        answers,
        "the day does not answer as it did"
    );
    assert_eq!(server.row_groups(), groups_after);
}

/// `/compact` rewrites a day on the server's cores: it is taken only with the token from
/// `LOCK`, only for the directory the server serves, and only for a day that is one.
#[test]
fn a_compact_request_needs_the_lock_token_and_the_servers_own_directory() {
    let db = Db::with_live_day(LINES);
    let records = db.manifest().len();
    let server = db.start(NO_SCHEDULE);
    assert!(server.banner.contains("POST /compact"), "{}", server.banner);
    let ours = fs::canonicalize(db.base()).expect("canonicalize");
    let body = |dir: &std::path::Path, day: &str| {
        serde_json::json!({"day": day, "data_dir": dir, "threads": null}).to_string()
    };

    let (status, text) = request(
        server.port,
        "POST",
        "/compact",
        "",
        body(&ours, DAY).as_bytes(),
    );
    assert_eq!(status, 403, "{text}");
    let wrong = "x-kibbr-load-token: 00000000000000000000000000000000\r\n";
    let (status, text) = request(
        server.port,
        "POST",
        "/compact",
        wrong,
        body(&ours, DAY).as_bytes(),
    );
    assert_eq!(status, 403, "{text}");
    let (status, text) = request(server.port, "GET", "/compact/1", "", b"");
    assert_eq!(status, 403, "{text}");

    let (status, answer) = server.asked("POST", "/compact", &body(&db.root, DAY));
    assert_eq!(status, 409, "{answer}");
    let said = answer["error"].as_str().expect("an error");
    assert!(
        said.contains(&ours.display().to_string()) && said.contains("nothing was compacted"),
        "{said}"
    );
    let (status, answer) = server.asked("POST", "/compact", &body(&ours, "2024-13-45"));
    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"], "day `2024-13-45` is not a YYYY-MM-DD day");
    let (status, answer) = server.asked("POST", "/compact", "{not json");
    assert_eq!(status, 400, "{answer}");
    let (status, answer) = server.asked("GET", "/compact/1", "");
    assert_eq!(status, 404, "{answer}");

    assert_eq!(db.manifest().len(), records, "a refused request compacted");
    assert_eq!(server.compaction()["runs"], 0);

    // And with all three right, it is a job.
    let (status, answer) = server.asked("POST", "/compact", &body(&ours, DAY));
    assert_eq!(status, 202, "{answer}");
    assert_eq!(answer["ok"], true);
    let job = answer["job"].as_u64().expect("a job number");
    let state = server.job(job);
    assert_eq!(state["state"], "done", "{state}");
    assert_eq!(state["report"]["ok"], true, "{state}");
    assert_eq!(state["report"]["days"][0]["rows"], LINES as u64, "{state}");
    assert_eq!(db.manifest().len(), records + 1);
}

/// A store in memory has no day files to rebuild, and says so rather than running a job
/// that finds nothing.
#[test]
fn a_server_with_no_data_directory_refuses_a_compaction() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .args(["serve", "--port", "0", "--storage", "memory"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr serve");
    let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
    let mut port = None;
    for _ in 0..10 {
        let mut line = String::new();
        if stderr.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if let Some(at) = line.find("http://127.0.0.1:") {
            port = line[at + "http://127.0.0.1:".len()..]
                .trim()
                .parse::<u16>()
                .ok();
            break;
        }
    }
    let answer = port.map(|port| request(port, "POST", "/compact", "", b"{}"));
    let _ = child.kill();
    let _ = child.wait();
    let (status, text) = answer.expect("no listening line in the banner");
    assert_eq!(status, 503, "{text}");
    assert!(text.contains("keeps its rows in memory"), "{text}");
}

/// One compaction at a time. Nothing outside the process can hold one open, so the second
/// is asked mid-rebuild: a 409 names the running job; a 202 means the first had ended.
#[test]
fn a_second_compaction_is_refused_while_one_is_running() {
    let db = Db::with_live_day(MANY_LINES);
    let (before, _) = db.live_day();
    let records = db.manifest().len();
    let server = db.start(NO_SCHEDULE);
    let body = serde_json::json!({ "day": DAY }).to_string();

    let (status, answer) = server.asked("POST", "/compact", &body);
    assert_eq!(status, 202, "{answer}");
    let first = answer["job"].as_u64().expect("a job number");
    let (status, answer) = server.asked("POST", "/compact", &body);
    match status {
        409 => {
            let said = answer["error"].as_str().expect("an error");
            assert!(
                said.contains(&format!("compaction job {first} is still running"))
                    && said.contains("one compaction runs at a time"),
                "{said}"
            );
        }
        202 => {
            let (_, state) = server.asked("GET", &format!("/compact/{first}"), "");
            assert_ne!(
                state["state"], "running",
                "a second compaction was started beside one still running"
            );
            let second = answer["job"].as_u64().expect("a job number");
            assert_eq!(server.job(second)["state"], "done");
        }
        other => panic!("a second compaction answered {other}: {answer}"),
    }

    // Refused or not, the day was compacted once.
    let state = server.job(first);
    assert_eq!(state["state"], "done", "{state}");
    assert_eq!(state["report"]["days"][0]["rows"], MANY_LINES as u64);
    db.assert_compacted(records, &before);
    assert_eq!(server.compaction()["days_compacted"], 1);
    assert_eq!(server.count(ALL), MANY_LINES as u64);
}

/// A server whose stderr has gone still compacts, and its job still ends: a log line that
/// cannot be written is dropped, not left to end the job's thread with the job `running`.
#[test]
fn a_compaction_job_ends_when_the_servers_stderr_is_closed() {
    let db = Db::with_live_day(LINES);
    let (before, _) = db.live_day();
    let records = db.manifest().len();
    let mut server = db.start(NO_SCHEDULE);
    server.close_stderr();

    let body = serde_json::json!({ "day": DAY }).to_string();
    let (status, answer) = server.asked("POST", "/compact", &body);
    assert_eq!(status, 202, "{answer}");
    let state = server.job(answer["job"].as_u64().expect("a job number"));
    assert_eq!(state["state"], "done", "{state}");
    assert_eq!(state["report"]["days"][0]["rows"], LINES as u64, "{state}");
    db.assert_compacted(records, &before);

    let (status, answer) = server.asked("POST", "/compact", &body);
    assert_eq!(status, 202, "the first job is still in the way: {answer}");
    let state = server.job(answer["job"].as_u64().expect("a job number"));
    assert_eq!(state["state"], "done", "{state}");
    assert_eq!(server.compaction()["running"], false);
    // And it stops as it would have: every line of the shutdown is one it cannot write.
    server.shutdown();
}

// ── By the server's own clock ──────────────────────────────────────────────────────

/// A day is due the day after its last live row, read from its newest unfinished file:
/// a backdated day is compacted with nobody asking, and one written today is left alone.
#[test]
fn the_scheduler_compacts_a_day_that_ended_and_leaves_one_written_today() {
    let ended = Db::with_live_day(LINES);
    let (before, _) = ended.live_day();
    let records = ended.manifest().len();
    let (answers, groups_before) = {
        let server = ended.start(NO_SCHEDULE);
        let seen = (server.answers(), server.row_groups());
        server.shutdown();
        seen
    };
    ended.backdate();
    // `0` is never: the day is due now, and a server told not to look leaves it.
    {
        let idle = ended.start(NO_SCHEDULE);
        sleep(Duration::from_millis(500));
        assert_eq!(
            idle.compaction(),
            serde_json::json!({
                "running": false, "runs": 0, "days_compacted": 0, "last": null,
                "last_error": null,
            }),
            "a server run with --compact-interval-ms 0 looked for a day that was due"
        );
        idle.shutdown();
        assert_eq!(ended.manifest().len(), records);
    }
    let control = Db::with_live_day(LINES);
    let manifest = control.manifest_bytes();
    let started_on = today();

    let looking = ["--compact-interval-ms", "100"];
    let control_server = control.start(&looking);
    let control_since = Instant::now();
    let control_groups = control_server.row_groups();
    let server = ended.start(&looking);
    assert_eq!(server.stats()["storage"]["compact_interval_ms"], 100);

    let deadline = Instant::now() + DEADLINE;
    while server.compaction()["days_compacted"] == 0 {
        assert!(
            Instant::now() < deadline,
            "a day that ended was not compacted: {}",
            server.compaction()
        );
        sleep(Duration::from_millis(20));
    }
    let added = ended.assert_compacted(records, &before);
    assert_eq!(
        server.answers(),
        answers,
        "the day does not answer as it did"
    );
    assert!(server.row_groups() < groups_before);
    // A second's worth of looks later: compacted once, and not looked at as a job again.
    sleep(Duration::from_secs(1));
    let stats = server.compaction();
    assert_eq!(stats["running"], false, "{stats}");
    assert_eq!(stats["days_compacted"], 1, "{stats}");
    assert_eq!(
        stats["runs"], 1,
        "a look that found nothing due was counted: {stats}"
    );
    assert_eq!(stats["last"]["day"], DAY, "{stats}");
    assert_eq!(stats["last"]["rows"], LINES as u64, "{stats}");
    assert_eq!(strings(&stats["last"]["added"]), added, "{stats}");
    assert_eq!(stats["last_error"], Value::Null, "{stats}");
    assert_eq!(ended.manifest().len(), records + 1);
    let log = server.shutdown();
    assert!(log.contains(&format!("compacted {DAY}: ")), "{log}");

    while control_since.elapsed() < Duration::from_millis(1_500) {
        sleep(Duration::from_millis(50));
    }
    // Midnight UTC during the run makes the control's day due, and there is then nothing
    // to hold it to.
    if today() == started_on {
        let stats = control_server.compaction();
        assert_eq!(
            stats,
            serde_json::json!({
                "running": false, "runs": 0, "days_compacted": 0, "last": null,
                "last_error": null,
            })
        );
        assert_eq!(control_server.row_groups(), control_groups);
        assert_eq!(
            control.manifest_bytes(),
            manifest,
            "a day whose last live row was today was compacted"
        );
    }
    control_server.shutdown();
}

/// A day that is due and cannot be compacted is left for the interval doubled per
/// failure, and the server takes lines as it did: a failed compaction is not a failed write.
#[test]
fn a_day_that_keeps_failing_is_left_longer_each_time() {
    let db = Db::with_live_day(LINES);
    db.live_day();
    db.backdate();
    let manifest = db.manifest_bytes();
    let server = db.start_with(&["--compact-interval-ms", "100"], NO_ROOM);

    let deadline = Instant::now() + DEADLINE;
    while server.compaction()["runs"] == 0 {
        assert!(
            Instant::now() < deadline,
            "a day that ended was never tried: {}",
            server.compaction()
        );
        sleep(Duration::from_millis(10));
    }
    sleep(Duration::from_millis(1_500));
    let stats = server.compaction();
    let runs = stats["runs"].as_u64().expect("runs");
    assert!(
        (1..=5).contains(&runs),
        "{runs} tries in a second and a half of looks every 100 ms: {stats}"
    );
    assert_eq!(stats["days_compacted"], 0, "{stats}");
    assert!(
        stats["last_error"]
            .as_str()
            .is_some_and(|e| e.starts_with(&format!("compact {DAY}: "))),
        "{stats}"
    );
    assert_eq!(db.manifest_bytes(), manifest);

    let (status, health) = request(server.port, "GET", "/health", "", b"");
    assert_eq!(status, 200, "{health}");
    let line = farm(1, 3);
    assert_eq!(server.ingest(&line)["errors"], serde_json::json!([]));
    assert_eq!(server.count(ALL), LINES as u64 + 1);
    let log = server.shutdown();
    assert!(log.contains("; the day is left for "), "{log}");
}

/// `/stats`'s `last_error` is the days still failing: a failed day loaded again whole has
/// nothing left to compact, so the next look drops its error, not a later success.
#[test]
fn last_error_clears_once_the_day_has_nothing_left_to_compact() {
    let db = Db::with_live_day(LINES);
    db.live_day();
    db.backdate();
    let server = db.start_with(&["--compact-interval-ms", "100"], NO_ROOM);
    let deadline = Instant::now() + DEADLINE;
    while !server.compaction()["last_error"].is_string() {
        assert!(Instant::now() < deadline, "a day with no room never failed");
        sleep(Duration::from_millis(10));
    }

    let input = db.root.join("day.ndjson");
    fs::write(&input, farm(LINES, 7)).expect("write the day's lines");
    let load = ["load", "--data-dir", &db.base_arg(), "--replace-day", DAY];
    let mut load: Vec<String> = load.map(String::from).into();
    load.push(input.display().to_string());
    db.spawn(&load).finish(DEADLINE).ok();
    assert!(
        db.live().values().all(|file| file["finished"] == true),
        "the load did not leave the day finished"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    while server.compaction()["last_error"] != Value::Null {
        assert!(
            Instant::now() < deadline,
            "a day with nothing left to compact is still the last error: {}",
            server.compaction()
        );
        sleep(Duration::from_millis(20));
    }
    let stats = server.compaction();
    assert_eq!(stats["days_compacted"], 0, "{stats}");
    assert_eq!(server.count(ALL), LINES as u64);
    server.shutdown();
}

/// `serve --compact-interval-ms` overrides its variable, so it is not refused for what the
/// variable holds; without the flag a variable that is not a number is refused.
#[test]
fn the_compact_interval_flag_wins_over_a_malformed_variable() {
    let db = Db::new();
    let malformed = [("KIBBR__STORAGE__COMPACT_INTERVAL_MS", "abc")];

    let server = db.start_with(&["--compact-interval-ms", "500"], &malformed);
    assert_eq!(server.stats()["storage"]["compact_interval_ms"], 500);
    server.shutdown();

    let serve = ["serve", "--port", "0", "--data-dir", &db.base_arg()].map(String::from);
    db.spawn_with(&serve, &malformed, None)
        .finish(REFUSAL_DEADLINE)
        .refused(&["KIBBR__STORAGE__COMPACT_INTERVAL_MS=`abc` is not a whole number"]);
}

// ── Shutdown ───────────────────────────────────────────────────────────────────────

/// SIGTERM while a compaction is rebuilding a day: the server stops it and exits 0, and
/// the directory holds the day's old files or its new ones, with every row in it once.
#[test]
fn a_server_told_to_stop_mid_compaction_exits_0_and_keeps_every_row_once() {
    let db = Db::with_live_day(MANY_LINES);
    let (before, _) = db.live_day();
    let records = db.manifest().len();
    let server = db.start(NO_SCHEDULE);
    let answers = server.answers();

    let body = serde_json::json!({ "day": DAY }).to_string();
    let (status, answer) = server.asked("POST", "/compact", &body);
    assert_eq!(status, 202, "{answer}");
    let job = answer["job"].as_u64().expect("a job number");
    let log = server.shutdown();

    // Before any start has had the chance to tidy up.
    let appended = db.manifest().len() - records;
    if appended == 0 {
        assert!(
            log.contains(&format!("compaction job {job} - compact {DAY}: cancelled")),
            "the day was not compacted, and the job did not say it was stopped:\n{log}"
        );
        let live: BTreeSet<String> = db.live().into_keys().collect();
        assert_eq!(live, before, "the day's files are not as they were");
        assert_eq!(
            db.on_disk(),
            before,
            "a stopped compaction left a file behind"
        );
    } else {
        // Compacted, so it had finished before the signal: a compaction that ran on to
        // its record after being told to stop is a shutdown that waited out the rebuild.
        let done = log.find(&format!("compaction job {job} - compacted {DAY}: "));
        let told = log.find("SIGTERM received - shutting down");
        assert!(
            matches!((done, told), (Some(done), Some(told)) if done < told),
            "the compaction was not stopped:\n{log}"
        );
        db.assert_compacted(records, &before);
    }
    assert!(log.contains("exit 0"), "{log}");

    let server = db.start(NO_SCHEDULE);
    assert_eq!(
        server.answers(),
        answers,
        "the day does not answer as it did"
    );
    assert_eq!(server.count(ALL), MANY_LINES as u64);
}
