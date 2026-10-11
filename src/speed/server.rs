//! One child `kibbr serve`, on a copy of the loaded directory, for one unit of measuring.
//!
//! This file holds the server's command line - every flag typed out, the same for every
//! server - the scrub of a child's environment, the start (copy, verify, spawn, standard
//! error to a file, the port from the banner, the first `/health`), and the reading of
//! `/stats` and `/health`.
//!
//! **A server of its own for every unit.** Whatever a server took or cached would be in
//! the next number, so none outlives its unit, and each starts from the loaded bytes.
//! **Killed, not stopped.** What it holds is not wanted, and a clean stop would write its
//! buffers out; that is safe only because it ran on a copy, which goes with it.
//!
//! **Up means it said where and answered.** The server is given `--port 0` and writes the
//! address it was given to standard error, which goes to a file, so the file is read
//! until the address is in it. The socket is bound before the store is opened, so a
//! connection made then is only waiting: a server is up at its first `/health` that is a
//! 200, and the time from spawn to that answer is what a result records as its start.

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

/// A running server. Dropping it kills the process, reaps it and removes its copy.
pub struct Server {
    pub pid: u32,
    pub addr: SocketAddr,
    /// From spawn to the first 200 of `/health` whose status was `ok`.
    pub start_s: f64,
    /// Copying the loaded directory and reading the copy back against the marker.
    pub copy_ms: f64,
    pub copy_files: u64,
    pub copy_bytes: u64,
    /// The server's standard error. A file and never a pipe: the farm widens columns on
    /// every start, and a pipe nobody drains blocks the server.
    pub log: PathBuf,
    child: Child,
    copy: PathBuf,
}

/// What the bench reads of `/stats`. The first two fields are required of every server;
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
    /// Writes to the store that failed since the server started, and whether its last
    /// flush did. While the store cannot write, `/ingest` goes on answering 200 - the
    /// lines are in the log - and no row reaches a file.
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
    copy_files: u64,
    copy_bytes: u64,
}

impl Server {
    /// Copy the loaded directory under `work`, read the copy back against the marker,
    /// start `exe serve` on it with `argv` and a scrubbed environment, and wait until it
    /// has printed its address and answered `/health` with 200 and `ok`.
    ///
    /// A child that exits first, or is not ready in `START_LIMIT`, is
    /// `SpeedError::ServerStart` with the end of its standard error; a copy that is not
    /// the marker's list is `SpeedError::Corpus`. Nothing is left running on an error,
    /// and no copy is left behind; the server's standard error is.
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
        let (copy_files, copy_bytes) = match copied(loaded, &copy) {
            Ok(counts) => counts,
            Err(error) => {
                let _ = fs::remove_dir_all(&copy);
                return Err(error);
            }
        };
        let made = Made {
            copy_ms: began.elapsed().as_secs_f64() * 1e3,
            copy_files,
            copy_bytes,
            log,
            copy,
        };
        launch(command(exe, &made.copy, cores, threads), made, START_LIMIT)
    }

    /// `GET /stats`. A required field that is missing or of another type is
    /// `SpeedError::Surface`, and so is an answer that is not a 200, or none.
    pub fn stats(&self, conn: &mut Conn) -> Result<Stats, SpeedError> {
        let answer = self.ask_stats(conn)?;
        self.read_stats(conn, &answer)
    }

    /// The first half of `stats`: the question, and its answer still unread in `conn`.
    /// For the `/stats` that opens a window, where what follows the answer's last byte is
    /// inside the window and reading the answer can wait until the senders are released.
    pub fn ask_stats(&self, conn: &mut Conn) -> Result<Answer, SpeedError> {
        self.get(conn, "GET /stats", "/stats")
    }

    /// The second half: read the answer `ask_stats` left in `conn`. Nothing may have been
    /// sent down `conn` since.
    pub fn read_stats(&self, conn: &mut Conn, answer: &Answer) -> Result<Stats, SpeedError> {
        let stats = parse_stats(conn.body(answer))?;
        self.keep(conn, answer, "GET /stats")?;
        Ok(stats)
    }

    /// `GET /health`: its `status`, and its `detail` when it has one. An answer that is
    /// not 200 is `SpeedError::Surface`; a status other than `ok` is the caller's to judge.
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
/// The read proves the server starts on the loaded bytes, and puts them in the file
/// cache - a clone is a new file, and its pages are not the original's.
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

/// Start `command` with standard error to the log, and wait until it is up. From the
/// spawn on, a `Server` owns the process and the copy, so every way out of here that is
/// an error kills the one and removes the other.
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
        copy_files: made.copy_files,
        copy_bytes: made.copy_bytes,
        log: made.log,
        child,
        copy: made.copy,
    };
    server.come_up(spawned, limit)?;
    Ok(server)
}

/// The port of the first `http://127.0.0.1:<port>` in what a server has written so far.
/// `None` until the digits are followed by something that is not one: the file is read
/// while it is being written, and a port read off half a line would be another port.
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

/// The arguments of every server, after the binary's name: each flag at the value that was
/// its default when the first result was saved, typed out, so that "the same flags"
/// survives a default that moves. `--port 0`, so that no port is picked and then lost to
/// another process; `--compact-interval-ms 0`, because a copy holding live rows of a past
/// day is due for compaction by the wall clock's date, and a compaction under a
/// measurement is not the measurement. A result records them with `<copy>` as `data_dir`.
///
/// A flag a later build adds is not here and takes that build's default.
///
/// **`--scan-threads` is the one flag that is asked for and not typed out**: it is the
/// bench's own `--threads`, last on the line, and with none given it is not on the line
/// at all - the build's default then, which follows the machine, as it was for every
/// result saved before the flag. No variable can stand in for it: `KIBBR__` is scrubbed.
/// The arguments are recorded and two results compare only where they are equal, so a
/// result at four threads is held to one at four.
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

/// Take every `KIBBR__` variable out of a child's environment, and return their names in
/// order, for the log. Called for every child the bench starts - a server, the generator,
/// the load, the plan step - so that none reads a limit the result does not record.
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
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::io::{Read, Write};
    #[cfg(unix)]
    use std::net::TcpListener;

    #[test]
    fn a_child_loses_every_kibbr_variable_and_keeps_the_rest() {
        let scrubbed = |name: &str| begins(OsStr::new(name), &[SCRUBBED]);
        assert!(scrubbed("KIBBR__STORAGE__BASE_PATH"));
        assert!(scrubbed("KIBBR__SCHEMA__EPOCH_WINDOW"));
        assert!(!scrubbed("KIBBR_HOME"));
        assert!(!scrubbed("PATH"));

        let noted = |name: &str| begins(OsStr::new(name), &NOTED);
        assert!(noted("RAYON_NUM_THREADS"));
        assert!(noted("TOKIO_WORKER_THREADS"));
        assert!(noted("MIMALLOC_VERBOSE"));
        assert!(!noted("HOME"));
        assert!(!noted("KIBBR__API__INGEST_CONCURRENCY"));
    }

    #[test]
    fn every_server_is_started_with_every_flag_typed_out() {
        let expected = "serve --host 127.0.0.1 --port 0 --data-dir <copy> --storage disk \
             --max-rows 10000 --max-buckets 10000 --max-groups 100000 \
             --max-body-bytes 10485760 --max-decompressed-bytes 67108864 \
             --ingest-concurrency 8 \
             --flush-interval-ms 500 --row-group-timeout-ms 300000 \
             --wal-segment-bytes 67108864 --batch-size 500 \
             --max-open-segments 512 --segment-max-bytes 536870912 \
             --max-buffer-bytes 536870912 \
             --compact-interval-ms 0 \
             --time-columns @timestamp,timestamp,time,ts,ts_ms,ts_unix,datetime,event_time,created_at";
        let argv = argv(COPY, 8, None);
        assert_eq!(argv, expected.split(' ').collect::<Vec<_>>());

        // Only the copy and the cores differ between two servers, and the copy's path
        // reaches the child as one argument whatever it holds.
        let copy = Path::new("testdata/bench/work-1.noindex/copy 7");
        let command = command(Path::new("/bin/kibbr"), copy, 4, None);
        let passed: Vec<&OsStr> = command.get_args().collect();
        assert_eq!(passed.len(), argv.len());
        for (at, (passed, recorded)) in passed.iter().zip(&argv).enumerate() {
            match recorded.as_str() {
                COPY => assert_eq!(*passed, copy.as_os_str()),
                "8" => assert_eq!(*passed, "4", "argument {at}"),
                recorded => assert_eq!(*passed, recorded, "argument {at}"),
            }
        }
        assert_eq!(command.get_program(), "/bin/kibbr");
    }

    /// **`--threads` reaches every server as `--scan-threads`, and with none given the
    /// command line is the one every earlier result recorded.**
    #[test]
    fn the_scan_threads_asked_for_are_the_last_flag_and_none_is_no_flag() {
        let without = argv(COPY, 8, None);
        assert!(!without.iter().any(|argument| argument == "--scan-threads"));

        let with = argv(COPY, 8, Some(4));
        assert_eq!(with[..without.len()], without[..]);
        assert_eq!(with[without.len()..], ["--scan-threads", "4"]);

        let command = command(Path::new("/bin/kibbr"), Path::new("copy"), 8, Some(4));
        let passed: Vec<&OsStr> = command.get_args().collect();
        assert_eq!(passed[passed.len() - 2..], ["--scan-threads", "4"]);
        assert_eq!(passed.len(), with.len());
    }

    #[test]
    fn the_port_is_the_first_one_a_server_wrote_whole() {
        let banner =
            b"kibbr: catalog.log - /d/catalog.log restored 80 template(s) from 1,204 record(s)\n\
                       kibbr serve\n  listening   http://127.0.0.1:24611\n  storage     disk\n";
        assert_eq!(banner_port(banner), Some(24_611));
        // Read while the line was being written: 2461 is not the port.
        assert_eq!(banner_port(&banner[..banner.len() - 20]), None);
        assert_eq!(banner_port(b"  listening   http://127.0.0.1:2461"), None);
        assert_eq!(banner_port(b"  listening   http://127.0.0.1:"), None);
        assert_eq!(banner_port(b"kibbr: nothing yet\n"), None);
        assert_eq!(banner_port(b""), None);
        // The first mention, and one that is not a port is passed over.
        assert_eq!(
            banner_port(
                b"see http://127.0.0.1:/docs\n http://127.0.0.1:7700/ and http://127.0.0.1:1 \n"
            ),
            Some(7_700)
        );
        assert_eq!(
            banner_port(b"http://127.0.0.1:99999 http://127.0.0.1:0 \n"),
            None
        );
    }

    /// `/stats` as a server of this build answers it over a loaded farm day, with the
    /// note cut to its first words and the counters the bench reads set apart from one
    /// another, so that one read from another's place shows.
    const STATS: &str = r#"{"uptime_seconds":0.026272959,"started_at":"2026-10-07T15:24:22.940221Z","storage":{"kind":"disk","durable":true,"note":"a line is written to the write-ahead log under wal/ and flushed before /ingest acknowledges it","base_path":"/s/data","trie_file":"/s/data/trie.json","catalog":"/s/data/catalog.log","flush_interval_ms":500,"row_group_timeout_ms":300000,"wal":{"dir":"/s/data/wal","stream":"ecca12cf6193f716","segments":1,"first_segment":1,"bytes":28,"last_seq":600,"applied":598,"checkpoint":0,"flushes":2,"healthy":true,"last_error":null,"segment_bytes":67108864,"batch_size":500,"held_rows":0,"replay":{"records":0,"applied":0,"covered":0,"held":0,"replaced":0,"behind_checkpoint":0,"torn":0,"corrupt":0,"seconds":0.0}},"stores_open":80,"open_segments":0,"max_open_segments":512,"segment_max_bytes":536870912,"rows":2000,"buffered_rows":7,"buffered_bytes":1536352,"max_buffer_bytes":536870912,"compact_interval_ms":0,"compaction":{"running":false,"runs":0,"days_compacted":0,"last":null,"last_error":null},"forced_closes":3,"write_failures":5,"last_write_error":null,"degraded":true,"approx_bytes":145294},"trie":{"templates":80,"nodes":780,"tokens":1044},"ingest":{"lines_read":600,"lines_ingested":600,"lines_rejected":1,"lines_per_second":0.0,"requests_shed":4,"ingest_concurrency":8,"ingest_bodies":256},"queries":{"served":0,"per_second":0.0}}"#;

    #[test]
    fn stats_are_the_two_required_fields_and_whatever_else_is_there() {
        let stats = parse_stats(STATS.as_bytes()).unwrap();
        let body = stats.body.clone();
        assert_eq!(
            stats,
            Stats {
                uptime_seconds: 0.026272959,
                lines_ingested: 600,
                lines_rejected: Some(1),
                requests_shed: Some(4),
                ingest_concurrency: Some(8),
                rows: Some(2_000),
                buffered_rows: Some(7),
                buffered_bytes: Some(1_536_352),
                max_buffer_bytes: Some(536_870_912),
                forced_closes: Some(3),
                write_failures: Some(5),
                degraded: Some(true),
                wal_last_seq: Some(600),
                wal_applied: Some(598),
                wal_flushes: Some(2),
                templates: Some(80),
                body: body.clone(),
            }
        );

        // The body is the answer in the answer's order, less the note and nothing else.
        let note = r#""note":"a line is written to the write-ahead log under wal/ and flushed before /ingest acknowledges it","#;
        assert!(STATS.contains(note));
        assert_eq!(body.to_string(), STATS.replace(note, ""));

        // An optional field that is gone, or is another type, is missing; a field the
        // bench does not know is kept in the body and not read.
        let stats = parse_stats(
            br#"{"uptime_seconds":3,"ingest":{"lines_ingested":0,"requests_shed":"none"},"scan":{"threads":4}}"#,
        )
        .unwrap();
        assert_eq!((stats.uptime_seconds, stats.lines_ingested), (3.0, 0));
        assert_eq!(
            (
                stats.requests_shed,
                stats.rows,
                stats.wal_last_seq,
                stats.templates
            ),
            (None, None, None, None)
        );
        assert_eq!((stats.write_failures, stats.degraded), (None, None));
        assert_eq!(stats.body["scan"]["threads"], 4);

        // Whether the store could write is read as a count and a flag, and a word in the
        // flag's place is not taken for one.
        let stats = parse_stats(
            br#"{"uptime_seconds":3,"ingest":{"lines_ingested":0},"storage":{"write_failures":0,"degraded":"no"}}"#,
        )
        .unwrap();
        assert_eq!((stats.write_failures, stats.degraded), (Some(0), None));
    }

    #[test]
    fn stats_without_a_required_field_are_not_read() {
        for (body, says) in [
            (
                r#"{"ingest":{"lines_ingested":0}}"#,
                "it holds no `uptime_seconds`",
            ),
            (
                r#"{"uptime_seconds":"1.5","ingest":{"lines_ingested":0}}"#,
                "its `uptime_seconds` is \"1.5\", and a number of seconds is read there",
            ),
            (
                r#"{"uptime_seconds":1.5,"ingest":{}}"#,
                "it holds no `ingest.lines_ingested`",
            ),
            (
                r#"{"uptime_seconds":1.5,"ingest":{"lines_ingested":12.5}}"#,
                "its `ingest.lines_ingested` is 12.5, and a whole number is read there",
            ),
            (r#"{"uptime_seconds":1.5"#, "its body is not JSON"),
        ] {
            let error = parse_stats(body.as_bytes()).unwrap_err();
            assert!(
                matches!(
                    error,
                    SpeedError::Surface {
                        surface: "GET /stats",
                        ..
                    }
                ),
                "{error}"
            );
            assert!(error.to_string().contains(says), "{error}");
        }
    }

    #[test]
    fn health_is_its_status_and_the_reason_it_gives() {
        assert_eq!(
            parse_health(br#"{"ok":true,"status":"ok"}"#).unwrap(),
            Health {
                status: "ok".to_string(),
                detail: None
            }
        );
        assert_eq!(
            parse_health(br#"{"ok":true,"status":"degraded","detail":"the log cannot write"}"#)
                .unwrap(),
            Health {
                status: "degraded".to_string(),
                detail: Some("the log cannot write".to_string())
            }
        );
        for body in [&br#"{"ok":true}"#[..], br#"{"status":1}"#, b"ok"] {
            let error = parse_health(body).unwrap_err();
            assert!(
                matches!(
                    error,
                    SpeedError::Surface {
                        surface: "GET /health",
                        ..
                    }
                ),
                "{error}"
            );
        }
    }

    #[test]
    fn a_copy_that_differs_is_named_by_its_first_difference() {
        let file = |path: &str, bytes: u64, crc32c: &str| FileDigest {
            path: path.to_string(),
            bytes,
            crc32c: crc32c.to_string(),
        };
        let recorded = [
            file("catalog.log", 10, "17c0a2e1"),
            file("data/2024-01-15/seg_0_000001.col", 20, "b40d91f3"),
            file("manifest.log", 30, "0e5a7c22"),
        ];
        assert_eq!(difference(&recorded, &recorded), None);

        let mut copy = recorded.to_vec();
        copy[1].crc32c = "00000000".to_string();
        assert_eq!(
            difference(&copy, &recorded).unwrap(),
            "data/2024-01-15/seg_0_000001.col: crc32c 00000000, recorded b40d91f3"
        );
        copy[1].bytes = 19;
        assert_eq!(
            difference(&copy, &recorded).unwrap(),
            "data/2024-01-15/seg_0_000001.col: 19 bytes, recorded 20"
        );
        assert_eq!(
            difference(&recorded[..2], &recorded).unwrap(),
            "manifest.log is missing"
        );
        assert_eq!(
            difference(&[recorded[0].clone(), recorded[2].clone()], &recorded).unwrap(),
            "data/2024-01-15/seg_0_000001.col is missing"
        );
        let mut more = recorded.to_vec();
        more.insert(1, file("catalog.log.tmp", 1, "00000001"));
        assert_eq!(
            difference(&more, &recorded).unwrap(),
            "catalog.log.tmp is not in the recorded list"
        );
        more = recorded.to_vec();
        more.push(file("trie.json", 1, "00000001"));
        assert_eq!(
            difference(&more, &recorded).unwrap(),
            "trie.json is not in the recorded list"
        );
    }

    /// A directory of this test's own, with a copy in it for a server to be started on.
    #[cfg(unix)]
    fn scratch(name: &str) -> (PathBuf, Made) {
        let dir =
            std::env::temp_dir().join(format!("kibbr-speed-server-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let copy = dir.join("copy-1");
        fs::create_dir_all(copy.join("data")).unwrap();
        fs::write(copy.join("catalog.log"), b"x").unwrap();
        let made = Made {
            log: dir.join("server-1.log"),
            copy,
            copy_ms: 4.0,
            copy_files: 1,
            copy_bytes: 1,
        };
        (dir, made)
    }

    /// Something that starts the way a server does, as far as the bench can see: a
    /// shell that writes `script`'s lines to standard error and then stays.
    #[cfg(unix)]
    fn stand_in(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]);
        command
    }

    /// Whether a process is there, running or not yet reaped.
    #[cfg(unix)]
    fn exists(pid: u32) -> bool {
        Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    }

    /// A listener that answers every request on every connection with the next of
    /// `answers`, and the last of them from then on. Returns its port.
    #[cfg(unix)]
    fn answering(answers: Vec<String>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        thread::spawn(move || {
            let mut asked = 0;
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut request = Vec::new();
                let mut byte = [0u8; 1];
                while stream.read(&mut byte).unwrap_or(0) == 1 {
                    request.push(byte[0]);
                    if request.ends_with(b"\r\n\r\n") {
                        let answer = &answers[asked.min(answers.len() - 1)];
                        asked += 1;
                        request.clear();
                        if stream.write_all(answer.as_bytes()).is_err()
                            || answer.contains("connection: close")
                        {
                            break;
                        }
                    }
                }
            }
        });
        port
    }

    #[cfg(unix)]
    fn answer(status: &str, extra: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{extra}content-length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    #[cfg(unix)]
    #[test]
    fn a_server_is_up_at_its_first_200_of_health_and_goes_with_its_copy() {
        // Not ready twice - once on a socket it then closes - and then ready; after
        // that `/stats`, and a `/health` that is no longer `ok`.
        let port = answering(vec![
            answer("503 Service Unavailable", "connection: close\r\n", "{}"),
            answer("404 Not Found", "", "{}"),
            answer("200 OK", "", r#"{"ok":true,"status":"ok"}"#),
            answer("200 OK", "", STATS),
            answer(
                "200 OK",
                "connection: close\r\n",
                r#"{"ok":true,"status":"degraded","detail":"the log cannot write"}"#,
            ),
            answer("500 Internal Server Error", "", "no"),
        ]);
        let (dir, made) = scratch("up");
        let (copy, log) = (made.copy.clone(), made.log.clone());
        let script = format!(
            "echo 'kibbr serve' >&2; sleep 0.1; \
             echo '  listening   http://127.0.0.1:{port}' >&2; exec sleep 60"
        );
        let server = launch(stand_in(&script), made, Duration::from_secs(30)).unwrap();

        assert_eq!(server.addr, SocketAddr::from(([127, 0, 0, 1], port)));
        assert!(
            server.start_s >= 0.1 && server.start_s < 30.0,
            "{}",
            server.start_s
        );
        assert_eq!(
            server.unit(),
            ServerUnit {
                pid: server.pid,
                start_s: server.start_s,
                copy_ms: 4.0,
                copy_bytes: 1
            }
        );
        assert!(exists(server.pid));

        let mut conn = Conn::open(server.addr, Duration::from_secs(20)).unwrap();
        assert_eq!(server.stats(&mut conn).unwrap().lines_ingested, 600);
        // This answer said `connection: close`; the next question still goes through.
        assert_eq!(server.health(&mut conn).unwrap().status, "degraded");
        let error = server.stats(&mut conn).unwrap_err().to_string();
        assert!(
            error.starts_with(
                "GET /stats did not answer the way kibbr bench reads it: status 500: no (server pid "
            ) && error.ends_with("server-1.log)"),
            "{error}"
        );

        let pid = server.pid;
        drop(server);
        assert!(!exists(pid), "killed and reaped");
        assert!(!copy.exists(), "the copy goes with its server");
        assert!(log.exists(), "what it wrote stays");
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_server_that_ends_before_it_is_up_is_quoted_and_leaves_no_copy() {
        let (dir, made) = scratch("ends");
        let (copy, log) = (made.copy.clone(), made.log.clone());
        let Err(error) = launch(
            stand_in("echo 'kibbr: the data directory is held by another server' >&2; exit 3"),
            made,
            Duration::from_secs(30),
        ) else {
            panic!("a process that exited is not a server");
        };
        let SpeedError::ServerStart { log: named, tail } = &error else {
            panic!("{error}");
        };
        assert_eq!(named, &log);
        assert_eq!(
            tail,
            "kibbr: the data directory is held by another server\n\
             [the process ended before it was up: exit status: 3]"
        );
        assert!(!copy.exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_server_that_never_says_where_or_never_answers_is_killed_when_the_wait_ends() {
        // Nothing written at all.
        let (dir, made) = scratch("silent");
        let copy = made.copy.clone();
        let began = Instant::now();
        let Err(error) = launch(stand_in("exec sleep 60"), made, Duration::from_millis(300)) else {
            panic!("nothing was written");
        };
        assert!(began.elapsed() < Duration::from_secs(20));
        let SpeedError::ServerStart { tail, .. } = &error else {
            panic!("{error}");
        };
        assert_eq!(tail, "[it had not written its address when the wait ended]");
        assert!(!copy.exists());
        fs::remove_dir_all(dir).unwrap();

        // An address, and behind it something that is never a 200.
        let port = answering(vec![answer("503 Service Unavailable", "", "{}")]);
        let (dir, made) = scratch("unready");
        let script = format!("echo '  listening   http://127.0.0.1:{port}' >&2; exec sleep 60");
        let Err(error) = launch(stand_in(&script), made, Duration::from_millis(300)) else {
            panic!("it never answered 200");
        };
        let SpeedError::ServerStart { tail, .. } = &error else {
            panic!("{error}");
        };
        assert_eq!(
            tail,
            &format!(
                "  listening   http://127.0.0.1:{port}\n\
                 [it had not answered /health with 200 when the wait ended]"
            )
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_server_that_starts_degraded_is_not_measured() {
        let port = answering(vec![answer(
            "200 OK",
            "",
            r#"{"ok":true,"status":"degraded","detail":"the log cannot write"}"#,
        )]);
        let (dir, made) = scratch("degraded");
        let copy = made.copy.clone();
        let script = format!("echo '  listening   http://127.0.0.1:{port}' >&2; exec sleep 60");
        let Err(error) = launch(stand_in(&script), made, Duration::from_secs(30)) else {
            panic!("a degraded server is not up");
        };
        let text = error.to_string();
        assert!(
            matches!(
                error,
                SpeedError::Surface {
                    surface: "GET /health",
                    ..
                }
            ),
            "{text}"
        );
        assert!(
            text.contains("said `degraded` (the log cannot write)"),
            "{text}"
        );
        assert!(!copy.exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_binary_that_cannot_be_started_is_an_error_about_that_file() {
        let dir = std::env::temp_dir().join(format!(
            "kibbr-speed-server-{}-nobinary",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let copy = dir.join("copy-1");
        fs::create_dir_all(&copy).unwrap();
        let made = Made {
            log: dir.join("server-1.log"),
            copy: copy.clone(),
            copy_ms: 0.0,
            copy_files: 0,
            copy_bytes: 0,
        };
        let missing = dir.join("no-such-kibbr");
        let Err(error) = launch(
            command(&missing, &copy, 8, None),
            made,
            Duration::from_secs(30),
        ) else {
            panic!("there is no such binary");
        };
        assert!(
            matches!(&error, SpeedError::Io { path, .. } if path == &missing),
            "{error}"
        );
        assert!(!copy.exists());
        fs::remove_dir_all(dir).unwrap();
    }
}
