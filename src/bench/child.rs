// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! One child `kibbr serve`, on a copy of the loaded directory, for one unit of measuring.
//! A server of its own for every unit: whatever one took or cached would be in the next
//! number. Killed, not stopped: a clean stop would write its buffers out, and it ran on a
//! copy. Up means it printed its address (`--port 0`) and answered `/health` with a 200;
//! spawn to that answer is what a result records as its start.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::corpus::{self, Corpus, FileDigest};
use super::report::{Health, ServerUnit};
use super::wire::{self, Answer, Conn};
use super::{SpeedError, MAX_ROWS, SEGMENT_MAX_BYTES, START_LIMIT, START_POLL, TIME_COLUMNS};

/// Variables no child may inherit: each changes a limit or a default behind the flags.
const SCRUBBED: &str = "KIBBR__";

/// Variables a child keeps, and a result records with their values: a later build's
/// thread pools and the allocator read them.
const NOTED: [&str; 3] = ["RAYON_", "TOKIO_", "MIMALLOC_"];

/// What stands for a server's data directory where its arguments are recorded: every
/// server has a copy of its own, and a result must not differ by a copy's name.
pub const COPY: &str = "<copy>";

/// What the address is read after, in what a server writes as it starts.
const BANNER: &[u8] = b"http://127.0.0.1:";

/// Lines of a server's standard error that an error about its start quotes.
const QUOTED: usize = 40;

/// Servers this process has started, so that each copy and each log has a name no other
/// has.
static STARTED: AtomicU64 = AtomicU64::new(0);

/// A running server; dropping it kills the process, reaps it and removes its copy.
pub struct Server {
    pub pid: u32,
    pub addr: SocketAddr,
    /// From spawn to the first 200 of `/health` whose status was `ok`.
    pub start_s: f64,
    /// Copying the loaded directory and reading the copy back against the marker.
    pub copy_ms: f64,
    pub copy_bytes: u64,
    /// The server's standard error, a file and never a pipe: the farm widens columns on
    /// every start, and a pipe nobody drains blocks the server.
    pub log: PathBuf,
    child: Child,
    copy: PathBuf,
}

/// What the bench reads of `/stats`: the first two fields are required of every server,
/// each of the rest is `None` where the answer does not hold it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stats {
    /// The server's own monotonic clock, read in the same answer as its counters.
    pub uptime_seconds: f64,
    /// Lines acknowledged: in the log and applied.
    pub lines_ingested: u64,
    pub lines_rejected: Option<u64>,
    pub requests_shed: Option<u64>,
    pub ingest_concurrency: Option<u64>,
    pub rows: Option<u64>,
    pub buffered_rows: Option<u64>,
    pub buffered_bytes: Option<u64>,
    pub max_buffer_bytes: Option<u64>,
    pub forced_closes: Option<u64>,
    /// Writes to the store that failed since the server started; while it cannot write,
    /// `/ingest` goes on answering 200 and no row reaches a file.
    pub write_failures: Option<u64>,
    pub degraded: Option<bool>,
    pub wal_last_seq: Option<u64>,
    pub wal_applied: Option<u64>,
    pub wal_flushes: Option<u64>,
    pub templates: Option<u64>,
    /// The whole answer, less `storage.note`, as a result keeps it.
    pub body: Value,
}

/// A copy that was made and read back, with where its server's standard error will go.
struct Made {
    copy: PathBuf,
    log: PathBuf,
    copy_ms: f64,
    copy_bytes: u64,
}

impl Server {
    /// Copy the loaded directory under `work`, verify it against the marker, start `exe serve`
    /// on it and wait for a 200 `ok` from `/health`; an error leaves nothing running and no copy.
    pub fn start(
        exe: &Path,
        loaded: &Corpus,
        work: &Path,
        cores: usize,
        threads: Option<usize>,
    ) -> Result<Server, SpeedError> {
        let number = STARTED.fetch_add(1, Ordering::Relaxed) + 1;
        let copy = work.join(format!("copy-{number}"));
        let log = work.join(format!("server-{number}.log"));

        let began = Instant::now();
        let (_, copy_bytes) = match copied(loaded, &copy) {
            Ok(counts) => counts,
            Err(error) => {
                let _ = fs::remove_dir_all(&copy);
                return Err(error);
            }
        };
        let made = Made {
            copy_ms: began.elapsed().as_secs_f64() * 1e3,
            copy_bytes,
            log,
            copy,
        };
        launch(command(exe, &made.copy, cores, threads), made, START_LIMIT)
    }

    /// `GET /stats`; a required field that is missing or mistyped, or an answer that is not a
    /// 200, is `SpeedError::Surface`.
    pub fn stats(&self, conn: &mut Conn) -> Result<Stats, SpeedError> {
        let answer = self.ask_stats(conn)?;
        self.read_stats(conn, &answer)
    }

    /// The first half of `stats`, its answer left unread in `conn`: for the `/stats` that
    /// opens a window, where reading the answer can wait until the senders are released.
    pub fn ask_stats(&self, conn: &mut Conn) -> Result<Answer, SpeedError> {
        self.get(conn, "GET /stats", "/stats")
    }

    /// The second half: read the answer `ask_stats` left in `conn`, down which nothing may
    /// have been sent since.
    pub fn read_stats(&self, conn: &mut Conn, answer: &Answer) -> Result<Stats, SpeedError> {
        let stats = parse_stats(conn.body(answer))?;
        self.keep(conn, answer, "GET /stats")?;
        Ok(stats)
    }

    /// `GET /health`: its `status` and `detail`, where a status other than `ok` is the
    /// caller's to judge.
    pub fn health(&self, conn: &mut Conn) -> Result<Health, SpeedError> {
        let answer = self.get(conn, "GET /health", "/health")?;
        let health = parse_health(conn.body(&answer))?;
        self.keep(conn, &answer, "GET /health")?;
        Ok(health)
    }

    /// This server as a result records it.
    pub fn unit(&self) -> ServerUnit {
        ServerUnit {
            pid: self.pid,
            start_s: self.start_s,
            copy_ms: self.copy_ms,
            copy_bytes: self.copy_bytes,
        }
    }

    /// One `GET` whose answer must be a 200. The body is `conn`'s until its next request.
    fn get(
        &self,
        conn: &mut Conn,
        surface: &'static str,
        path: &str,
    ) -> Result<Answer, SpeedError> {
        let answer = conn
            .send(&wire::get_request(path), &[])
            .map_err(|error| self.wrong(surface, error.to_string()))?;
        if answer.status != 200 {
            let said = wire::excerpt(conn.body(&answer));
            return Err(self.wrong(surface, format!("status {}: {said}", answer.status)));
        }
        Ok(answer)
    }

    /// After an answer that said the socket is done, connect again: the caller keeps
    /// this connection for its next question and was not shown the answer's head.
    fn keep(
        &self,
        conn: &mut Conn,
        answer: &Answer,
        surface: &'static str,
    ) -> Result<(), SpeedError> {
        if answer.close {
            conn.reopen().map_err(|error| {
                self.wrong(
                    surface,
                    format!("its answer closed the connection, and the next was refused: {error}"),
                )
            })?;
        }
        Ok(())
    }

    /// An answer this server gave that cannot be read, with where to look for why.
    fn wrong(&self, surface: &'static str, detail: String) -> SpeedError {
        SpeedError::Surface {
            surface,
            detail: format!(
                "{detail} (server pid {}, whose standard error is in {})",
                self.pid,
                self.log.display()
            ),
        }
    }

    /// Wait for the address and then for `/health`, until `limit` after `spawned`.
    fn come_up(&mut self, spawned: Instant, limit: Duration) -> Result<(), SpeedError> {
        let deadline = spawned + limit;

        // The address. The file is read whole each time: it is a few lines long, and
        // the port must be taken from its first mention, not its latest.
        let port = loop {
            if let Some(port) = banner_port(&fs::read(&self.log).unwrap_or_default()) {
                break port;
            }
            self.alive()?;
            if Instant::now() >= deadline {
                return Err(self.not_up("it had not written its address when the wait ended"));
            }
            thread::sleep(START_POLL);
        };
        self.addr = SocketAddr::from(([127, 0, 0, 1], port));

        // The first 200 of `/health`. A connection made while the store is still being
        // opened waits to be accepted, so one question can take the whole of the start.
        let request = wire::get_request("/health");
        let mut conn: Option<Conn> = None;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(self.not_up("it had not answered /health with 200 when the wait ended"));
            }
            if conn.is_none() {
                conn = Conn::open(self.addr, left).ok();
            }
            if let Some(open) = conn.as_mut() {
                match open.send(&request, &[]) {
                    Ok(answer) if answer.status == 200 => {
                        let health = parse_health(open.body(&answer))?;
                        if health.status != "ok" {
                            let detail = health.detail.unwrap_or_default();
                            return Err(self.wrong(
                                "GET /health",
                                format!(
                                    "a server that had just started said `{}` ({detail}), \
                                     and what a server in that state does is not measured",
                                    health.status
                                ),
                            ));
                        }
                        break;
                    }
                    Ok(answer) if !answer.close => {}
                    _ => conn = None,
                }
            }
            self.alive()?;
            thread::sleep(START_POLL);
        }
        self.start_s = spawned.elapsed().as_secs_f64();
        Ok(())
    }

    /// An error if the process has ended: a start that is being waited for cannot come.
    fn alive(&mut self) -> Result<(), SpeedError> {
        match self.child.try_wait() {
            Ok(None) => Ok(()),
            Ok(Some(status)) => {
                Err(self.not_up(&format!("the process ended before it was up: {status}")))
            }
            Err(error) => Err(self.not_up(&format!("the process could not be asked: {error}"))),
        }
    }

    /// A start that failed: the end of what the server wrote, and under it, in brackets
    /// because it is not the server's line, what the bench saw.
    fn not_up(&self, saw: &str) -> SpeedError {
        let written = fs::read(&self.log).unwrap_or_default();
        let mut tail = super::tail(&String::from_utf8_lossy(&written), QUOTED);
        if !tail.is_empty() {
            tail.push('\n');
        }
        tail.push_str(&format!("[{saw}]"));
        SpeedError::ServerStart {
            log: self.log.clone(),
            tail,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // SIGKILL, then reaped, so that no server outlives its unit and the next check for
        // other kibbr processes does not meet this one.
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.copy);
    }
}

/// Make `copy` from the loaded directory and read it back: the files and bytes copied.
/// The read proves the server starts on the loaded bytes, and puts them in the file cache.
fn copied(loaded: &Corpus, copy: &Path) -> Result<(u64, u64), SpeedError> {
    let on = |path: &Path| {
        let path = path.to_path_buf();
        move |source| SpeedError::Io { path, source }
    };
    let counts = corpus::copy_tree(&loaded.dir, &loaded.marker.files, copy).map_err(on(copy))?;
    let read = corpus::listing(copy).map_err(on(copy))?;
    match difference(&read.files, &loaded.marker.files) {
        None => Ok(counts),
        Some(what) => Err(SpeedError::Corpus(format!(
            "a copy of the loaded directory is not the bytes that were loaded ({what}). \
             {} was copied to {} and the copy read back against {}: the loaded directory \
             changed while the bench ran, or the volume gave back other bytes than it was \
             given. No server was started on it",
            loaded.dir.display(),
            copy.display(),
            loaded.marker_path.display()
        ))),
    }
}

/// The first place a copy's files differ from the recorded ones, in words; both are in
/// path order.
fn difference(copy: &[FileDigest], recorded: &[FileDigest]) -> Option<String> {
    for (have, want) in copy.iter().zip(recorded) {
        if have.path != want.path {
            // In path order, the smaller name is the one the other list does not have.
            return Some(if have.path < want.path {
                format!("{} is not in the recorded list", have.path)
            } else {
                format!("{} is missing", want.path)
            });
        }
        if have.bytes != want.bytes {
            return Some(format!(
                "{}: {} bytes, recorded {}",
                have.path, have.bytes, want.bytes
            ));
        }
        if have.crc32c != want.crc32c {
            return Some(format!(
                "{}: crc32c {}, recorded {}",
                have.path, have.crc32c, want.crc32c
            ));
        }
    }
    match copy.len().cmp(&recorded.len()) {
        std::cmp::Ordering::Equal => None,
        std::cmp::Ordering::Greater => Some(format!(
            "{} is not in the recorded list",
            copy[recorded.len()].path
        )),
        std::cmp::Ordering::Less => Some(format!("{} is missing", recorded[copy.len()].path)),
    }
}

/// `exe` with a server's arguments, the copy's path standing where they name it.
fn command(exe: &Path, copy: &Path, cores: usize, threads: Option<usize>) -> Command {
    let mut command = Command::new(exe);
    for argument in argv(COPY, cores, threads) {
        if argument == COPY {
            // As the system spells it, which need not be text.
            command.arg(copy);
        } else {
            command.arg(argument);
        }
    }
    command
}

/// Start `command` with standard error to the log, and wait until it is up. From the spawn
/// on a `Server` owns the process and the copy, so an error kills one and removes the other.
fn launch(mut command: Command, made: Made, limit: Duration) -> Result<Server, SpeedError> {
    let spawn = |command: &mut Command| {
        let on = |path: &Path| {
            let path = path.to_path_buf();
            move |source| SpeedError::Io { path, source }
        };
        let errors = File::create(&made.log).map_err(on(&made.log))?;
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(errors);
        scrub(command);
        let spawned = Instant::now();
        let child = command
            .spawn()
            .map_err(on(Path::new(command.get_program())))?;
        Ok((child, spawned))
    };
    let (child, spawned) = match spawn(&mut command) {
        Ok(started) => started,
        Err(error) => {
            let _ = fs::remove_dir_all(&made.copy);
            return Err(error);
        }
    };
    let mut server = Server {
        pid: child.id(),
        // Until the server has said which port it was given.
        addr: SocketAddr::from(([127, 0, 0, 1], 0)),
        start_s: 0.0,
        copy_ms: made.copy_ms,
        copy_bytes: made.copy_bytes,
        log: made.log,
        child,
        copy: made.copy,
    };
    server.come_up(spawned, limit)?;
    Ok(server)
}

/// The port of the first `http://127.0.0.1:<port>` a server has written. `None` until a
/// non-digit follows: the file is read while written, and half a line is another port.
fn banner_port(written: &[u8]) -> Option<u16> {
    let mut from = 0;
    while let Some(at) = written[from..]
        .windows(BANNER.len())
        .position(|window| window == BANNER)
    {
        let digits = from + at + BANNER.len();
        let after = written[digits..]
            .iter()
            .position(|byte| !byte.is_ascii_digit())?;
        let port = std::str::from_utf8(&written[digits..digits + after])
            .ok()
            .and_then(|text| text.parse::<u16>().ok())
            .filter(|port| *port != 0);
        if port.is_some() {
            return port;
        }
        from = digits;
    }
    None
}

/// Every server's arguments, each flag typed out so that "the same flags" survives a moved
/// default: port 0 loses no port, compaction is off because a copy's past day is due for
/// it under the measurement, and `--scan-threads` is there only when `--threads` is given.
pub fn argv(data_dir: &str, cores: usize, threads: Option<usize>) -> Vec<String> {
    let cores = cores.to_string();
    let max_rows = MAX_ROWS.to_string();
    let segment_max_bytes = SEGMENT_MAX_BYTES.to_string();
    let time_columns = TIME_COLUMNS.join(",");
    let flags: [(&str, &str); 19] = [
        ("--host", "127.0.0.1"),
        ("--port", "0"),
        ("--data-dir", data_dir),
        ("--storage", "disk"),
        ("--max-rows", &max_rows),
        ("--max-buckets", "10000"),
        ("--max-groups", "100000"),
        ("--max-body-bytes", "10485760"),
        ("--max-decompressed-bytes", "67108864"),
        ("--ingest-concurrency", &cores),
        ("--flush-interval-ms", "500"),
        ("--row-group-timeout-ms", "300000"),
        ("--wal-segment-bytes", "67108864"),
        ("--batch-size", "500"),
        ("--max-open-segments", "512"),
        ("--segment-max-bytes", &segment_max_bytes),
        ("--max-buffer-bytes", "536870912"),
        ("--compact-interval-ms", "0"),
        ("--time-columns", &time_columns),
    ];
    let mut argv = vec!["serve".to_string()];
    for (flag, value) in flags {
        argv.push(flag.to_string());
        argv.push(value.to_string());
    }
    if let Some(threads) = threads {
        argv.push("--scan-threads".to_string());
        argv.push(threads.to_string());
    }
    argv
}

/// Read a `/stats` body.
pub fn parse_stats(body: &[u8]) -> Result<Stats, SpeedError> {
    let surface = |detail: String| SpeedError::Surface {
        surface: "GET /stats",
        detail,
    };
    let mut whole: Value = serde_json::from_slice(body).map_err(|error| {
        surface(format!(
            "its body is not JSON ({error}): {}",
            wire::excerpt(body)
        ))
    })?;

    // The two a rate is made of. Without either there is no ingest number, so neither
    // is guessed at.
    let required = |path: &str, wanted: &str| {
        let name = path[1..].replace('/', ".");
        match whole.pointer(path) {
            None => surface(format!("it holds no `{name}`")),
            Some(found) => surface(format!(
                "its `{name}` is {found}, and {wanted} is read there"
            )),
        }
    };
    let uptime_seconds = whole
        .pointer("/uptime_seconds")
        .and_then(Value::as_f64)
        .ok_or_else(|| required("/uptime_seconds", "a number of seconds"))?;
    let lines_ingested = whole
        .pointer("/ingest/lines_ingested")
        .and_then(Value::as_u64)
        .ok_or_else(|| required("/ingest/lines_ingested", "a whole number"))?;

    let count = |path: &str| whole.pointer(path).and_then(Value::as_u64);
    let mut stats = Stats {
        uptime_seconds,
        lines_ingested,
        lines_rejected: count("/ingest/lines_rejected"),
        requests_shed: count("/ingest/requests_shed"),
        ingest_concurrency: count("/ingest/ingest_concurrency"),
        rows: count("/storage/rows"),
        buffered_rows: count("/storage/buffered_rows"),
        buffered_bytes: count("/storage/buffered_bytes"),
        max_buffer_bytes: count("/storage/max_buffer_bytes"),
        forced_closes: count("/storage/forced_closes"),
        write_failures: count("/storage/write_failures"),
        degraded: whole.pointer("/storage/degraded").and_then(Value::as_bool),
        wal_last_seq: count("/storage/wal/last_seq"),
        wal_applied: count("/storage/wal/applied"),
        wal_flushes: count("/storage/wal/flushes"),
        templates: count("/trie/templates"),
        body: Value::Null,
    };

    // The note is a paragraph of prose about the store, the same in every answer; the
    // rest is kept as the server gave it, in its order.
    if let Some(storage) = whole.get_mut("storage").and_then(Value::as_object_mut) {
        storage.shift_remove("note");
    }
    stats.body = whole;
    Ok(stats)
}

/// Read a `/health` body: `status`, and `detail` where the server gives a reason.
fn parse_health(body: &[u8]) -> Result<Health, SpeedError> {
    let surface = |detail: String| SpeedError::Surface {
        surface: "GET /health",
        detail,
    };
    let said: Value = serde_json::from_slice(body).map_err(|error| {
        surface(format!(
            "its body is not JSON ({error}): {}",
            wire::excerpt(body)
        ))
    })?;
    let status = said
        .get("status")
        .and_then(Value::as_str)
        .ok_or_else(|| surface(format!("it holds no `status`: {}", wire::excerpt(body))))?;
    let detail = match said.get("detail") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => Some(other.to_string()),
    };
    Ok(Health {
        status: status.to_string(),
        detail,
    })
}

/// Take every `KIBBR__` variable out of a child's environment and return their names, so
/// that no child reads a limit the result does not record.
pub fn scrub(command: &mut Command) -> Vec<String> {
    let mut removed = Vec::new();
    for (name, _) in std::env::vars_os() {
        if begins(&name, &[SCRUBBED]) {
            command.env_remove(&name);
            removed.push(name.to_string_lossy().into_owned());
        }
    }
    removed.sort();
    removed
}

/// The variables a child inherits that a server may read, with their values.
pub fn env_noted() -> BTreeMap<String, String> {
    std::env::vars_os()
        .filter(|(name, _)| begins(name, &NOTED))
        .map(|(name, value)| {
            (
                name.to_string_lossy().into_owned(),
                value.to_string_lossy().into_owned(),
            )
        })
        .collect()
}

/// Whether a variable's name begins with one of `prefixes`.
fn begins(name: &OsStr, prefixes: &[&str]) -> bool {
    let name = name.to_string_lossy();
    prefixes.iter().any(|prefix| name.starts_with(prefix))
}

#[cfg(test)]
#[path = "child_tests.rs"]
mod tests;
