// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What every module of the `e2e` binary shares: the binary as a command, a server as a
//! process, a scratch data directory, one HTTP request, and the inputs the tests feed.
//! A module takes it all with `use crate::harness::*;` and keeps only what is its own.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use serde_json::Value;

// ── The library items the tests reach past the binary for ──────────────────────────
//
// Every `kibbr` library path a test names is here and nowhere else, so a change to the
// crate's layout is a change to this block, and to `kibbr::testing` in src/lib.rs.

pub use kibbr::testing::config::{settings, File as ConfigFile, Log as ConfigLog, ServeFlags};
pub use kibbr::testing::server::{
    run as run_server, Report as ServerReport, Result as ServerResult,
};
pub use kibbr::testing::{open_sealed, FormatError, Reader};

// ── Deadlines ──────────────────────────────────────────────────────────────────────

/// Long enough for a debug build to load a few thousand lines several times over.
pub const DEADLINE: Duration = Duration::from_secs(60);

/// A command that should exit at once - a refusal - and is failed rather than waited on.
pub const REFUSAL_DEADLINE: Duration = Duration::from_secs(15);

// ── The binary ─────────────────────────────────────────────────────────────────────

/// The binary, with no `KIBBR__` variable of the caller's: the variables are settings
/// too, and one exported in the shell that ran `cargo test` would answer for the flag or
/// the file under test. A test that wants one sets it on the command it is given.
pub fn kibbr() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("KIBBR__") {
            command.env_remove(name);
        }
    }
    command
}

/// `kibbr <args>`, which must exit 0; its stdout.
pub fn run(args: &[&str]) -> String {
    let out = kibbr().args(args).output().expect("failed to run kibbr");
    assert!(
        out.status.success(),
        "kibbr {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

/// `kibbr <args>`, which must fail; its stderr.
pub fn refused(args: &[&str]) -> String {
    let out = kibbr().args(args).output().expect("failed to run kibbr");
    assert!(!out.status.success(), "kibbr {args:?} was accepted");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Feeds stdin from a separate thread. Writing inline deadlocks as soon as the input
/// outgrows a pipe buffer: the child blocks writing stdout while we block writing stdin.
pub fn feed(child: &mut Child, input: &[u8]) -> std::thread::JoinHandle<()> {
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    let input = input.to_vec();
    std::thread::spawn(move || {
        // A short `--limit` makes the reader exit early; that closed pipe is the
        // expected end of the write, not a failure.
        let _ = stdin.write_all(&input);
    })
}

/// `kibbr <args>` with `input` on stdin and these variables set, to its exit, whatever
/// the exit: stdout and stderr apart.
pub fn run_piped(args: &[&str], input: &[u8], env: &[(&str, &str)]) -> Output {
    let mut child = kibbr()
        .args(args)
        .envs(env.iter().copied())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let writer = feed(&mut child, input);
    let out = child.wait_with_output().expect("failed to wait on kibbr");
    writer.join().expect("stdin writer thread panicked");
    out
}

/// `kibbr <args>` over `input` on stdin, which must exit 0; its stdout.
pub fn run_stdin(args: &[&str], input: &str) -> String {
    let out = run_piped(args, input.as_bytes(), &[]);
    assert!(
        out.status.success(),
        "kibbr {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

/// `kibbr query - <args>` over `input`, which must exit 0; its stdout.
pub fn query_over(input: &str, args: &[&str]) -> String {
    let mut all = vec!["query", "-"];
    all.extend_from_slice(args);
    let out = run_piped(&all, input.as_bytes(), &[]);
    assert!(
        out.status.success(),
        "kibbr query {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

// ── Inputs ─────────────────────────────────────────────────────────────────────────

/// A file under `tests/data/`.
pub fn data(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data")
        .join(name)
}

/// The worked example: three templates, one promoted dimension (`event`), and `user_id`
/// on all three, so each answer has a promoted and a non-promoted route.
pub const SAMPLE_LOGS: &str = include_str!("data/sample_logs.ndjson");

/// Lines in `corpus()`.
pub const CORPUS_LINES: usize = 500;

/// `gen-logs <args>`, which must exit 0.
pub fn gen_logs(args: &[&str]) -> Vec<u8> {
    let out = kibbr()
        .arg("gen-logs")
        .args(args)
        .output()
        .expect("failed to run gen-logs");
    assert!(out.status.success(), "gen-logs {args:?} failed");
    out.stdout
}

/// `lines` hostile lines of `seed`: thirty days and a few hundred templates.
pub fn hostile(lines: usize, seed: u64) -> Vec<u8> {
    gen_logs(&["--lines", &lines.to_string(), "--seed", &seed.to_string()])
}

/// `lines` farm lines of `seed`, on the farm's own day, 2024-01-15.
pub fn farm(lines: usize, seed: u64) -> Vec<u8> {
    gen_logs(&[
        "--profile",
        "farm",
        "--lines",
        &lines.to_string(),
        "--seed",
        &seed.to_string(),
    ])
}

/// `CORPUS_LINES` generated lines, seeded so two runs ingest the same bytes.
pub fn corpus() -> Vec<u8> {
    hostile(CORPUS_LINES, 42)
}

/// `bytes`, gzipped, as a shipper with `compression = "gzip"` would send them.
pub fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("failed to gzip");
    encoder.finish().expect("failed to finish gzip")
}

pub fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

// ── Answers ────────────────────────────────────────────────────────────────────────

pub fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

/// A JSON array of strings as a set; anything else is the empty set.
pub fn strings(v: &Value) -> BTreeSet<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|s| s.as_str().expect("an array of strings").to_string())
        .collect()
}

/// Every row of an answer as its JSON text.
pub fn rows_as_text(result: &Value) -> Vec<String> {
    result["rows"]
        .as_array()
        .expect("rows is an array")
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// Everything of an answer that a restart, a replay or another build must not change.
/// `meta` is left out: its duration and files opened rightly differ.
pub fn answer_of(full: &Value) -> Value {
    serde_json::json!({
        "exists": full["exists"],
        "count": full["count"],
        "rows": full["rows"],
        "groups": full["groups"],
    })
}

// ── Scratch directories ────────────────────────────────────────────────────────────

/// A path under the system's temporary directory that no other test of this run has,
/// with nothing at it.
pub fn scratch_dir(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "kibbr-e2e-{}-{}-{name}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// A directory of this test's own, made now and removed when the test ends.
pub struct Scratch(pub PathBuf);

impl Scratch {
    pub fn new(name: &str) -> Self {
        let dir = scratch_dir(name);
        fs::create_dir_all(&dir).expect("create the scratch directory");
        Self(dir)
    }

    /// A file of `text` in the directory, by its path.
    pub fn file(&self, name: &str, text: &str) -> String {
        let path = self.0.join(name);
        fs::write(&path, text).expect("write a file");
        path.display().to_string()
    }

    pub fn path(&self, name: &str) -> String {
        self.0.join(name).display().to_string()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A scratch root: `in/` holds inputs, `store/` is the data directory, `runs/` each
/// command's output. Inputs sit outside the store so a directory load cannot pick up the
/// store's own files. The data directory outlives every server started on it.
pub struct Db {
    pub root: PathBuf,
}

impl Db {
    pub fn new() -> Self {
        let root = scratch_dir("db");
        fs::create_dir_all(root.join("in")).expect("create the input directory");
        fs::create_dir_all(root.join("runs")).expect("create the output directory");
        Self { root }
    }

    pub fn base(&self) -> PathBuf {
        self.root.join("store")
    }

    pub fn base_arg(&self) -> String {
        self.base().display().to_string()
    }

    /// `bytes` at `in/<rel>`, parent directories created.
    pub fn input(&self, rel: &str, bytes: &[u8]) -> PathBuf {
        let path = self.root.join("in").join(rel);
        fs::create_dir_all(path.parent().expect("an input has a parent"))
            .expect("create the input's directory");
        fs::write(&path, bytes).expect("write the input");
        path
    }

    // ── Processes ──

    /// `kibbr <args>`, stdout and stderr to files: a pipe nobody reads until the exit
    /// can fill and stall the child, and a stalled child looks like a slow one.
    pub fn spawn(&self, args: &[String]) -> Running {
        self.spawn_with(args, None, &[], None)
    }

    /// As `spawn`, with `stdin` as the input if one is given, these variables set and,
    /// if one is given, from this directory.
    pub fn spawn_with(
        &self,
        args: &[String],
        stdin: Option<&Path>,
        env: &[(&str, &str)],
        cwd: Option<&Path>,
    ) -> Running {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let out = self.root.join("runs").join(format!("{n}.out"));
        let err = self.root.join("runs").join(format!("{n}.err"));
        let stdin = match stdin {
            Some(path) => Stdio::from(File::open(path).expect("open the stdin input")),
            None => Stdio::null(),
        };
        let mut command = kibbr();
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let child = command
            .args(args)
            .envs(env.iter().copied())
            .stdin(stdin)
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

    /// `kibbr serve` on the data directory.
    pub fn start(&self) -> Server {
        self.serve(&[])
    }

    /// The same, with these flags after the directory.
    pub fn serve(&self, flags: &[&str]) -> Server {
        self.serve_env(flags, &[])
    }

    /// The same, with these variables set too.
    pub fn serve_env(&self, flags: &[&str], env: &[(&str, &str)]) -> Server {
        Server::spawn(&[], Some(&self.base()), flags, env)
    }

    /// One count from a server started for it and then killed - fine for a process that
    /// ingested nothing, and the point where the question is what the *next* process sees.
    pub fn count(&self, q: &str) -> u64 {
        self.start().count(q)
    }

    // ── The directory's own files ──

    /// Whatever is under `staging/`. Empty or absent after any load has exited: a leftover
    /// is a move the loader never finished, or bytes nothing will ever delete.
    pub fn staging(&self) -> Vec<PathBuf> {
        fs::read_dir(self.base().join("staging"))
            .map(|entries| entries.flatten().map(|e| e.path()).collect())
            .unwrap_or_default()
    }

    pub fn manifest_bytes(&self) -> Vec<u8> {
        fs::read(self.base().join("manifest.log")).unwrap_or_default()
    }

    /// Every record in `manifest.log`, in order. The CRC is not rechecked: a payload
    /// that is not whole JSON ends the walk, as a torn tail would.
    pub fn manifest(&self) -> Vec<Value> {
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

    /// The live files by path, after every record is applied: the last word on a path
    /// wins, a removed path is gone, and a replace removes before it adds.
    pub fn live_files(&self) -> BTreeMap<String, Value> {
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

    /// The live files of one day, as the manifest names them (`2024-01-15/seg_…`).
    pub fn live_of(&self, day: &str) -> BTreeMap<String, Value> {
        let prefix = format!("{day}/");
        let mut files = self.live_files();
        files.retain(|path, _| path.starts_with(&prefix));
        files
    }

    /// The `.col` files under `data/<day>/`, spelled the way the manifest spells them.
    pub fn on_disk(&self, day: &str) -> BTreeSet<String> {
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
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

// ── A command, running and then ran ────────────────────────────────────────────────

/// A command still running, its output going to two files.
pub struct Running {
    pub child: Child,
    pub out: PathBuf,
    pub err: PathBuf,
    pub what: String,
}

impl Running {
    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Waits for the exit, killing the command at `limit`.
    pub fn finish(mut self, limit: Duration) -> Ran {
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
pub struct Ran {
    pub what: String,
    pub code: Option<i32>,
    pub timed_out: bool,
    pub stdout: String,
    pub stderr: String,
}

impl Ran {
    /// Exit 0, and what it printed.
    pub fn ok(&self) -> &str {
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
    pub fn report(&self) -> Value {
        let report = json(self.ok().trim());
        assert_eq!(report["ok"], true, "{report}");
        report
    }

    /// Exit 1, nothing on stdout, and every one of `phrases` in stderr.
    pub fn refused(&self, phrases: &[&str]) {
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

// ── A server: the binary, as a process ─────────────────────────────────────────────

pub struct Server {
    pub child: Child,
    pub port: u16,
    /// `None` once the test has closed its end of the pipe, or read it to the end.
    pub stderr: Option<BufReader<ChildStderr>>,
    /// Everything the server said before it listened: what a start found, and the banner.
    pub banner: String,
    pub data_dir: Option<PathBuf>,
    /// Whether `data_dir` is the server's own, removed when it is dropped.
    pub owns_dir: bool,
    /// Already waited for: `Drop` must not kill a pid that may have been reused.
    reaped: bool,
}

impl Server {
    /// `kibbr <before> serve --port 0 [--data-dir <dir>] <flags>`, once its banner has
    /// said where it listens. Port 0, and the port read back: tests run concurrently, and
    /// 7700 is where a developer's own `kibbr serve` is.
    pub fn spawn(
        before: &[&str],
        data_dir: Option<&Path>,
        flags: &[&str],
        env: &[(&str, &str)],
    ) -> Self {
        let mut command = kibbr();
        command.args(before).args(["serve", "--port", "0"]);
        if let Some(dir) = data_dir {
            command.args(["--data-dir", &dir.display().to_string()]);
        }
        let mut child = command
            .args(flags)
            .envs(env.iter().copied())
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
                let rest = &line[at + "http://127.0.0.1:".len()..];
                let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                port = digits.parse().ok();
            }
            // Read to the end of the banner, not just to the port: tests hold what the
            // whole banner says. Its last line as text, and its only line as JSON.
            if line.contains("shutdown ") || line.starts_with('{') {
                break;
            }
        }
        let port = port.unwrap_or_else(|| panic!("no listening line in banner:\n{banner}"));
        Self {
            child,
            port,
            stderr: Some(stderr),
            banner,
            data_dir: data_dir.map(Path::to_path_buf),
            owns_dir: false,
            reaped: false,
        }
    }

    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn data_dir(&self) -> &Path {
        self.data_dir
            .as_deref()
            .expect("this server was started in memory")
    }

    pub fn get(&self, path: &str) -> (u16, String) {
        request(self.port, "GET", path, b"")
    }

    pub fn post(&self, path: &str, body: &[u8]) -> (u16, String) {
        request(self.port, "POST", path, body)
    }

    /// `method path`, which must answer 200 with JSON.
    pub fn call(&self, method: &str, path: &str, body: &[u8]) -> Value {
        let (status, text) = request(self.port, method, path, body);
        assert_eq!(status, 200, "{method} {path} answered {status}: {text}");
        json(&text)
    }

    /// A TrieQL query, as `/query` takes it, which must be answered.
    pub fn query(&self, q: &str) -> Value {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = self.post("/query", body.as_bytes());
        assert_eq!(status, 200, "query `{q}` answered {status}: {text}");
        json(&text)
    }

    /// A query that is expected not to be answered, kept as its status and body.
    pub fn query_raw(&self, q: &str) -> (u16, Value) {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = self.post("/query", body.as_bytes());
        (status, json(&text))
    }

    pub fn count(&self, q: &str) -> u64 {
        let answer = self.query(q);
        answer["count"]
            .as_u64()
            .unwrap_or_else(|| panic!("`{q}` answered no count: {answer}"))
    }

    /// `answer_of` the query.
    pub fn answer(&self, q: &str) -> Value {
        answer_of(&self.query(q))
    }

    pub fn ingest(&self, body: &[u8]) -> Value {
        let (status, text) = self.post("/ingest", body);
        assert_eq!(status, 200, "ingest answered {status}: {text}");
        json(&text)
    }

    /// Every one of `lines`, a line apiece, which must all be stored.
    pub fn ingest_lines<L: std::fmt::Display>(&self, lines: &[L]) {
        let body: String = lines.iter().map(|line| format!("{line}\n")).collect();
        let report = self.call("POST", "/ingest", body.as_bytes());
        assert_eq!(report["ingested"], lines.len() as u64, "{report}");
        assert_eq!(report["errors"], serde_json::json!([]), "{report}");
    }

    pub fn stats(&self) -> Value {
        let (status, text) = self.get("/stats");
        assert_eq!(status, 200, "{text}");
        json(&text)
    }

    /// `kill <name>`: `-TERM` is the graceful stop.
    pub fn signal(&self, name: &str) {
        let ok = Command::new("kill")
            .args([name, &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill {name} failed");
    }

    /// Close this end of the server's stderr, as a log collector that went away would:
    /// whatever the server writes there from now on fails.
    pub fn close_stderr(&mut self) {
        self.stderr = None;
    }

    fn rest_of_stderr(&mut self) -> String {
        let mut log = String::new();
        if let Some(stderr) = self.stderr.as_mut() {
            let _ = stderr.read_to_string(&mut log);
        }
        log
    }

    /// Wait for exit, and return the code plus everything left on stderr.
    pub fn wait(mut self) -> (Option<i32>, String) {
        let status = self.child.wait().expect("failed to wait for kibbr serve");
        self.reaped = true;
        (status.code(), self.rest_of_stderr())
    }

    /// SIGTERM, then wait for the exit - the graceful path, which closes every row group,
    /// seals every file and writes the snapshots. It must exit 0; returns stderr after
    /// the banner.
    pub fn shutdown(self) -> String {
        self.signal("-TERM");
        let (code, log) = self.wait();
        assert_eq!(code, Some(0), "a graceful shutdown did not exit 0:\n{log}");
        log
    }

    /// SIGTERM, then the exit code and everything stderr said after the banner. Fails if
    /// the process is still there after `within`.
    pub fn stop(mut self, within: Duration) -> (Option<i32>, String) {
        self.signal("-TERM");
        let asked = Instant::now();
        let code = loop {
            match self.child.try_wait().expect("try_wait") {
                Some(status) => break status.code(),
                None if asked.elapsed() > within => {
                    let _ = self.child.kill();
                    panic!("still running {within:?} after SIGTERM");
                }
                None => sleep(Duration::from_millis(10)),
            }
        };
        self.reaped = true;
        (code, self.rest_of_stderr())
    }

    /// `kill -9`, and the data directory as it was left: nothing flushed, nothing sealed,
    /// no checkpoint moved. The directory is the caller's from now on.
    pub fn kill(mut self) -> PathBuf {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.reaped = true;
        self.owns_dir = false;
        self.data_dir.take().expect("a server with a directory")
    }

    /// `wait`, keeping the data directory for a restart: the caller's from now on.
    pub fn wait_keeping(mut self) -> (Option<i32>, String, PathBuf) {
        self.owns_dir = false;
        let dir = self.data_dir.clone().expect("a server with a directory");
        let (code, log) = self.wait();
        (code, log, dir)
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if self.owns_dir {
            if let Some(dir) = &self.data_dir {
                let _ = fs::remove_dir_all(dir);
            }
        }
    }
}

// ── The wire ───────────────────────────────────────────────────────────────────────

/// One answer: its status, its headers lowercased, and its body.
pub struct Response {
    pub status: u16,
    pub headers: String,
    pub body: String,
}

/// One request, one connection, read to EOF, with extra header lines - each ending in
/// CRLF. A server that never answers fails the test at the deadline; with no timeout the
/// read would wait on it for good.
pub fn send(port: u16, method: &str, path: &str, extra: &str, body: &[u8]) -> Response {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
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
    // Write errors are not failures here: a server that refuses a body before reading it
    // answers and closes mid-write, and whether the answer survives is what the read decides.
    let _ = socket.write_all(body);
    let _ = socket.flush();

    let mut response = Vec::new();
    socket
        .read_to_end(&mut response)
        .expect("failed to read the response");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no header/body split in the response:\n{}", text(&response)));
    let headers = text(&response[..split]).to_lowercase();
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{headers}"));
    Response {
        status,
        headers,
        body: text(&response[split + 4..]),
    }
}

/// One request: the status code and the body.
pub fn request(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    request_with(port, method, path, "", body)
}

/// `request`, with extra header lines - each ending in CRLF.
pub fn request_with(
    port: u16,
    method: &str,
    path: &str,
    extra: &str,
    body: &[u8],
) -> (u16, String) {
    let answer = send(port, method, path, extra, body);
    (answer.status, answer.body)
}

/// The same, with the response headers kept, lowercased.
pub fn request_raw(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String, String) {
    let answer = send(port, method, path, "", body);
    (answer.status, answer.headers, answer.body)
}

/// Header lines, each ending in CRLF, as `request_with` takes them.
pub fn headers(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect()
}

/// The status and the body of whatever the server wrote before it closed the connection,
/// on a socket the caller wrote its own request to. A read that fails part way keeps what
/// it had: what matters is whether an answer arrived.
pub fn read_answer(socket: &mut TcpStream) -> (u16, String) {
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
