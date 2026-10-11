//! `kibbr bench --lines N --profile farm`: this build's speed on one farm day, measured the
//! same way every time, so that the number a change is held to is one command's and not a
//! script's.
//!
//! **What is measured.** Eight rows, each number the middle of an odd count of runs and
//! never a percentile over their pooled samples:
//!
//! - `ingest lines/s`: the whole live stream, once, from four senders into a server that
//!   is asked nothing else - the lines the server says it acknowledged between two
//!   `/stats` answers, over its own clock in the same two answers. No client clock and no
//!   count of lines sent is in it.
//! - `S1` to `S4`, p50 and p99: each query asked `--samples` times back to back down one
//!   open connection to a server that has taken no line, timed on this process's clock
//!   from the request's first byte written to the answer's last byte read.
//! - `scan rows/s/core`: a block of asks of one count over two wide templates - the rows
//!   that hold its key, read from an answer, times the asks, over the CPU-seconds the
//!   server process used across exactly those asks. Per CPU-second and not per second, so
//!   that a scan spread over four threads still reads as what one core did; the same block
//!   by the wall clock is printed under the table.
//! - `plan step at 10,000 templates`: the p50 of `engine::plan` on a trie of its own, in a
//!   process of its own, since the farm cannot have that many templates.
//! - the same four queries under four-sender ingest, p99: one sample a server, each query
//!   asked when a fixed share of the live stream has been acknowledged.
//!
//! **A unit is one server's work, and no server is shared.** The plan step, ingest alone,
//! the idle server and each lap under ingest are units. Each of the three kinds that need
//! a server starts one on its own copy of the loaded directory - read back against the
//! digests taken when it was loaded - and kills it when its work is done. Whatever a
//! server took, cached or wrote would otherwise be in the next number; and a query under
//! ingest reads a store that grows at the ingest rate, so a hundred of them on one server
//! would each read another store. With a server a sample, every sample of a query starts
//! from the loaded bytes and is asked at the same place in the same stream.
//!
//! **The order of a run** is: a pause and the checks, the plan step, ingest alone, the
//! idle server, the laps. Ingest is one number on every core and the most sensitive to
//! heat, so what stands before it must be the same in every build: a pause and a few
//! seconds of one core planning. After the query phases it would follow minutes of one
//! core in one build and seconds of eight in the next.
//!
//! **What the bench touches, and nothing else.** Three children of this same binary -
//! `gen-logs`, `load` and `serve`, with every flag a number depends on typed out and every
//! `KIBBR__` variable removed - and itself as `bench --plan-only`; of a server, the
//! address it prints, `/health`, `/stats`, `/query` and `/ingest`; of the system, a
//! child's CPU time, the swap counter and the process list; and in this crate, for the
//! plan step only, the generator, the parser, the trie and `engine::plan`. `--threads T`
//! is the one thing a caller sets on a server: `serve --scan-threads T` on every one, and
//! with none given no such flag, as before it was added. Each of them is
//! asked once before the first run, on a server started for that, so that one that
//! answers in another form stops the command there and not an hour in. A field the bench
//! only records may be missing and is then `null`; the two a rate is made of may not. How
//! the machine is powered is asked of the system too, beside the references of each run,
//! and only recorded: a system that does not say is `null` and stops nothing.
//!
//! **Two gates, and what a disturbed unit costs.** A run starts on a machine with no
//! other kibbr process and a swap-out counter that is not moving, and waits for one.
//! Both are read again after each unit, between two servers and never beside one: a unit
//! during which either moved is thrown away with none of its numbers kept, and done
//! again - a lap costs seconds to repeat, where a run costs minutes. Everything else
//! about the machine is printed and recorded and decides nothing. `--unchecked` skips
//! all of it and marks the result as not a measurement.
//!
//! **The files.** This one holds what the command line reaches - `Args`, `run`, `table`,
//! `plan_only` - the one error type, the constants a result records under `flags`, the
//! texts of the queries, the steps taken once before the first run, and the loop over the
//! runs.
//!
//! - `corpus`: the loaded directory, built once and only ever copied; the live stream.
//! - `server`: one child `kibbr serve` on a copy, killed when its work is done.
//! - `wire`: one kept HTTP/1.1 connection, and the bytes of each request.
//! - `host`: the machine - a child's CPU time, swap, other kibbr processes, the references,
//!   the power state.
//! - `phases`: what is timed - ingest alone, the idle server, one lap under ingest.
//! - `plan`: the plan step on a trie of 10,000 templates, in a process of its own.
//! - `report`: the result file, the raw log, the table, and what makes two results
//!   comparable.
//!
//! **Frozen once a result is saved as a baseline.** A later result is printed as a ratio on
//! the baseline's, and that ratio is of two builds only while the instrument is one. The
//! eight files of this module are digested into every result (`report::SOURCES`), and a
//! table over two digests says so in capitals. So nothing here is tuned after the first
//! saved result - not a query's text, not a place in the stream, not a pause.
//!
//! **Changed once since, at M11: `--threads` was added.** The cloud box has eight cores
//! alike and the default is four, and no variable reaches a server, so the bench could
//! not measure the eight. The digest moved with it, and `testdata/bench/baseline.json`
//! was taken again by this instrument, at `--threads 4`. Nothing that is timed changed.

pub mod corpus;
pub mod host;
pub mod phases;
pub mod plan;
pub mod report;
pub mod server;
pub mod wire;

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use thiserror::Error;

use kibbr_core::fmt::commas;

use corpus::{BuiltBy, Corpus};
use host::Swap;
use phases::Work;
use report::{
    Binary, Checks, Commit, Data, Flags, FlushRef, LiveStreamFacts, Log, Machine, PlanTrie,
    QueryText, ResultFile, Run, Saved, ServerFacts, Seven, Voided,
};

/// Where the loaded directory, each server's copy of it and the default results go, unless
/// `--dir` names another place.
pub const DEFAULT_DIR: &str = "testdata/bench";
/// `--runs`, unless given. Odd, so that the median of the runs is one of them.
pub const DEFAULT_RUNS: usize = 3;
/// `--samples`, unless given: with 100, the p99 is the 99th smallest.
pub const DEFAULT_SAMPLES: usize = 100;
/// `--body-lines`, unless given: what both measuring scripts send.
pub const DEFAULT_BODY_LINES: usize = 2_000;

/// The one profile measured, as a result records it.
pub const PROFILE: &str = "farm";
/// Threads that send the live stream, one kept connection each.
pub const SENDERS: usize = 4;
/// `gen-logs --seed` of the day that is loaded.
pub const LOADED_SEED: u64 = 42;
/// `gen-logs --seed` of the stream that is sent to the servers.
pub const LIVE_SEED: u64 = 43;
/// The arrival time given at both doors - `load --received-at` and `/ingest?received_at=` -
/// so that no stored value depends on the date the bench ran. Noon of the farm's day.
pub const RECEIVED_AT: &str = "2024-01-15T12:00:00Z";
/// Where in the live stream each of the four queries is asked under ingest, as the share
/// of its lines acknowledged: what a result records. `place_lines` is what is waited for.
pub const LOAD_AT: [f64; 4] = [0.02, 0.03, 0.07, 0.14];
/// `LOAD_AT` in whole percent, so that a place is a line count worked out in integers.
const LOAD_AT_PERCENT: [u64; 4] = [2, 3, 7, 14];
/// Asks that are not measured before an idle block and before the scan block, when there
/// are that many samples.
pub const WARMUP: usize = 3;
/// Plans that are not measured before a shape's timed ones, when there are that many.
pub const PLAN_WARMUP: usize = 5;
/// How long a sender waits before it sends a refused body again. The bench's own wait and
/// not the server's `Retry-After`, so that a refusal costs the same on every build.
pub const REFUSAL_PAUSE: Duration = Duration::from_millis(50);
/// Attempts in a row that read no status line, after which a sender gives up.
pub const SILENT_LIMIT: usize = 5;
/// With no body acknowledged for this long, a place in the stream not reached, or a
/// query still refused, the phase ends in an error: a server that refuses for ever must
/// not hold a run for ever.
pub const STALL: Duration = Duration::from_secs(120);
/// The pause before each run.
pub const SETTLE: Duration = Duration::from_secs(20);
/// Between the two reads of the swap counter that must not differ.
pub const SWAP_GAP: Duration = Duration::from_secs(2);
/// How long a check that failed waits before it looks again.
pub const WAIT_STEP: Duration = Duration::from_secs(10);
/// How long the checks wait for a quiet machine before the command gives up.
pub const WAIT_LIMIT: Duration = Duration::from_secs(30 * 60);
/// Disturbed units one run repeats; one more and the command stops.
pub const VOID_LIMIT: usize = 5;
/// The flush reference: this many writes, each followed by a full flush.
pub const FLUSH_REF_WRITES: usize = 20;
/// The flush reference: bytes a write - what the server's log writes for one 2,000-line
/// body.
pub const FLUSH_REF_BYTES: usize = 136_000;
/// How long the server is left alone after the scan block, to see whether it still uses
/// CPU with nothing asked.
pub const SCAN_TAIL: Duration = Duration::from_millis(100);
/// A started server must print its address and answer `/health` within this.
pub const START_LIMIT: Duration = Duration::from_secs(120);
/// Between two looks for a starting server's address, and for its first `/health`.
pub const START_POLL: Duration = Duration::from_millis(5);
/// The read and write timeout of every connection: what bounds one body, or one query.
pub const SOCKET_TIMEOUT: Duration = Duration::from_secs(300);
/// Free bytes the volume must have for each line of `--lines` before anything is built.
pub const FREE_BYTES_PER_LINE: u64 = 4 * 500;
/// The key names a template's time column is chosen from, typed out and not read from
/// `config`: a default that moves must not move what was measured.
pub const TIME_COLUMNS: [&str; 9] = [
    "@timestamp",
    "timestamp",
    "time",
    "ts",
    "ts_ms",
    "ts_unix",
    "datetime",
    "event_time",
    "created_at",
];
/// `--segment-max-bytes`, at the load and on every server, typed out for the same reason.
pub const SEGMENT_MAX_BYTES: u64 = 536_870_912;
/// `--max-rows` of every server: the rows a fetch with no `LIMIT` is cut at. S3 is such a
/// fetch, and must never reach it.
pub const MAX_ROWS: u64 = 10_000;

/// The seven query texts, in the order every result holds them. S1 and S4 are the measuring
/// script's, byte for byte. S2 keeps its key and takes a value a quarter of the scanned
/// rows hold, and S3 takes the farm's own id column: the script's texts match no farm row,
/// and a filter that matches nothing is settled without reading a row. SCAN is the scan
/// rate's own query - two wide templates, most rows matching - and KEY and SCANKEY count
/// the rows S2 and SCAN must look at. S3's text is made by `queries`, around an id that is
/// read from the loaded stream.
pub const S1: &str = r#"COUNT LOG {"event":"login_ok"}"#;
pub const S2: &str = r#"COUNT LOG {"level":"info"}"#;
pub const S4: &str = "MATCH LOG {} LIMIT 500";
pub const KEY: &str = r#"COUNT LOG {"level": *}"#;
pub const SCAN: &str = r#"COUNT LOG {"status":"200"}"#;
pub const SCANKEY: &str = r#"COUNT LOG {"status": *}"#;

/// What `kibbr bench --lines N --profile farm` was asked for. The profile is not here: the
/// command line refuses every one but the farm before it builds this.
pub struct Args {
    pub lines: u64,
    pub out: Option<PathBuf>,
    pub baseline: Option<PathBuf>,
    pub label: Option<String>,
    pub dir: PathBuf,
    pub runs: usize,
    pub samples: usize,
    pub body_lines: usize,
    /// `--scan-threads` of every server measured. `None`: the flag is not passed, and a
    /// server takes its build's default.
    pub threads: Option<usize>,
    pub unchecked: bool,
}

/// What stops `kibbr bench`. Each sentence says what was met and, where there is one, what
/// to do about it: the command is run rarely and by hand, an hour at a time.
#[derive(Debug, Error)]
pub enum SpeedError {
    /// `--profile hostile`. The queries, their expected answers and the places in the
    /// stream are the farm's; over other data they would be numbers about nothing.
    #[error("kibbr bench measures the farm profile only")]
    Profile,

    /// Checked before anything runs, so that an hour of measuring cannot end at a file it
    /// may not write.
    #[error("{0} exists; a saved result is not written over")]
    OutExists(PathBuf),

    #[error(
        "this is a debug build, and what it measures is not this code's speed. Build with \
         `cargo build --release` and run that binary; --unchecked runs this one, and marks \
         its result as not a measurement"
    )]
    DebugBuild,

    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// A child that is not a server: `what` is its command, as `kibbr gen-logs`. `stderr`
    /// is what was wrong where the exit code does not say it, and then the last lines the
    /// child wrote to standard error.
    #[error("`{what}`, which the bench runs as a child, {}: {stderr}", ended(.code))]
    Child {
        what: &'static str,
        code: Option<i32>,
        stderr: String,
    },

    /// The loaded directory, its marker, the live stream or the plan step's trie is not
    /// what every result is measured over. The text is the whole sentence.
    #[error("{0}")]
    Corpus(String),

    #[error(
        "a server the bench started did not come up: within {} s it must write \
         `http://127.0.0.1:<port>` to standard error and answer /health with 200. Its \
         standard error is kept in {log} and ends:\n{tail}",
        START_LIMIT.as_secs()
    )]
    ServerStart { log: PathBuf, tail: String },

    /// Something the bench reads answered in another form than the one it was written
    /// for. Met in the preflight, before a run is spent.
    #[error("{surface} did not answer the way kibbr bench reads it: {detail}")]
    Surface {
        surface: &'static str,
        detail: String,
    },

    /// A query's answer that cannot be a sample: a status that is neither 200 nor a
    /// refusal, a refusal that was still the answer `STALL` after the first try, or a 200
    /// that is not the answer this query has over these bytes.
    #[error("{name}: the server's answer cannot be measured (status {status}): {body}")]
    Query {
        name: &'static str,
        status: u16,
        body: String,
    },

    /// `sender` counts from 0; `SENDERS` itself is the main thread, speaking for all of
    /// them when none has had a body acknowledged in `STALL`.
    #[error("{}: {detail}", who(.sender))]
    Sender { sender: usize, detail: String },

    /// The machine, or the binary on it, is not fit to measure on. The text is the whole
    /// sentence.
    #[error("{0}")]
    Host(String),

    #[error(
        "the machine did not become quiet in {minutes} minutes: {reason}. Nothing was \
         saved; stop what runs beside the bench and start it again"
    )]
    Waited { minutes: u64, reason: String },

    /// `reason` is either gate's: pages swapped out during a unit, or another kibbr
    /// process alive when it ended. The advice is for both.
    #[error(
        "run {run}: {units} of its units were disturbed and thrown away, the last because \
         {reason}. Nothing was saved: stop what runs beside the bench, or make room if the \
         machine swapped, and start again"
    )]
    Disturbed {
        run: usize,
        units: usize,
        reason: String,
    },

    /// The file cannot be read as a saved result, or is a result of other work: `detail`
    /// names each key that differs, with both values.
    #[error("{path} cannot be the baseline of this run: {detail}")]
    Baseline { path: PathBuf, detail: String },

    #[error("JSON that kibbr bench reads or writes is not in the form it has")]
    Json(#[from] serde_json::Error),

    /// Returned after the table is printed - and, by a run, after the result is saved - so
    /// that the command still ends on its table and exits 1. It says nothing of a saved
    /// result: `--table` returns it too, and saves none; a run has said `saved ...` on
    /// the line above its table.
    #[error("the table above carries {lines} FAIL line(s)")]
    Failed { lines: usize },
}

/// How a child ended, for `SpeedError::Child`.
fn ended(code: &Option<i32>) -> String {
    match code {
        Some(0) => "exited 0 without doing what was asked of it".to_string(),
        Some(code) => format!("exited {code}"),
        None => "was ended by a signal".to_string(),
    }
}

/// Who speaks in a `SpeedError::Sender`.
fn who(sender: &usize) -> String {
    if *sender < SENDERS {
        format!("ingest sender {} of {SENDERS}", sender + 1)
    } else {
        format!("the {SENDERS} ingest senders")
    }
}

/// A write to standard output that failed, as the error a file's failure is.
pub fn on_stdout(source: io::Error) -> SpeedError {
    SpeedError::Io {
        path: PathBuf::from("standard output"),
        source,
    }
}

/// A CRC-32C as every result, marker and log line spells it: eight hex digits, lower case.
pub fn crc_hex(crc: u32) -> String {
    format!("{crc:08x}")
}

/// The last `lines` lines of `text`, for an error that quotes a child's standard error.
pub fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Unmeasured asks before a block of `samples` timed ones.
pub fn warmup(samples: usize) -> usize {
    WARMUP.min(samples)
}

/// Unmeasured plans before a shape's `samples` timed ones.
pub fn plan_warmup(samples: usize) -> usize {
    PLAN_WARMUP.min(samples)
}

/// Asks in the scan block: a quarter of the samples and at least one. The scan rate is a
/// sum over the block and not a percentile, so a quarter is enough.
pub fn scan_asks(samples: usize) -> usize {
    (samples / 4).max(1)
}

/// The live lines that must be acknowledged before each of the four queries is asked under
/// ingest: `LOAD_AT` of `lines`, rounded up. In integers, because 0.14 x 2,000 is not 280
/// in floating point and a place must be the same line on every machine.
pub fn place_lines(lines: u64) -> [u64; 4] {
    LOAD_AT_PERCENT.map(|percent| (lines * percent).div_ceil(100))
}

/// The column heading of this result.
pub fn label(args: &Args) -> String {
    match (&args.label, &args.baseline) {
        (Some(label), _) => label.clone(),
        (None, None) => "baseline".to_string(),
        (None, Some(_)) => "this run".to_string(),
    }
}

/// What a result records under `flags`: the command line's values, and every constant a
/// number depends on. Two results compare only where these are equal.
pub fn flags(args: &Args) -> Flags {
    Flags {
        lines: args.lines,
        profile: PROFILE.to_string(),
        runs: args.runs,
        samples: args.samples,
        body_lines: args.body_lines,
        checked: !args.unchecked,
        senders: SENDERS,
        loaded_seed: LOADED_SEED,
        live_seed: LIVE_SEED,
        received_at: RECEIVED_AT.to_string(),
        load_at: LOAD_AT,
        warmup: warmup(args.samples),
        plan_warmup: plan_warmup(args.samples),
        scan_asks: scan_asks(args.samples),
        refusal_pause_ms: REFUSAL_PAUSE.as_millis() as u64,
        stall_s: STALL.as_secs(),
        settle_s: SETTLE.as_secs(),
        flush_ref: FlushRef {
            writes: FLUSH_REF_WRITES,
            bytes: FLUSH_REF_BYTES,
        },
    }
}

/// The seven queries with their labels, S3 around the id read from the loaded stream.
pub fn queries(s3_id: &str) -> Seven<QueryText> {
    let query = |label: &str, text: &str| QueryText {
        label: label.to_string(),
        text: text.to_string(),
    };
    Seven {
        s1: query("promoted value", S1),
        s2: query("unpromoted key", S2),
        s3: query("one id", &format!(r#"MATCH LOG {{"job.id":"{s3_id}"}}"#)),
        s4: query("newest 500", S4),
        key: query("rows holding level", KEY),
        scan: query("scan block", SCAN),
        scan_key: query("rows holding status", SCANKEY),
    }
}

/// A failure of the file system as the bench's own, with the path it was met at.
fn io_at(path: &Path) -> impl FnOnce(io::Error) -> SpeedError + '_ {
    move |source| SpeedError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// The time on the wall, in UTC to the second, as a result records an instant.
fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn as_ms(took: Duration) -> f64 {
    took.as_secs_f64() * 1e3
}

/// Process ids as a sentence names them.
fn pids(pids: &[u32]) -> String {
    let each: Vec<String> = pids.iter().map(u32::to_string).collect();
    format!("pid {}", each.join(", "))
}

/// What is known before anything is made on disk: where the result will go, the baseline
/// if there is one, and the commit, binary, machine and server arguments a result records.
struct Start {
    /// This binary. Every child is it, run again.
    exe: PathBuf,
    out: PathBuf,
    baseline: Option<Saved>,
    commit: Commit,
    binary: Binary,
    machine: Machine,
    server: ServerFacts,
    flags: Flags,
}

/// Where a result goes when `--out` names no file: under the directory the loaded bytes
/// are in, named by when it ran and on which commit, so that two runs do not meet.
fn default_out(dir: &Path, commit: &Commit) -> PathBuf {
    let sha: String = match &commit.sha {
        Some(sha) => sha.chars().take(8).collect(),
        None => "unknown".to_string(),
    };
    dir.join("results").join(format!(
        "{}-{sha}.json",
        Utc::now().format("%Y%m%dT%H%M%SZ")
    ))
}

/// Everything that can be refused before a child is started: a result that would be
/// written over, a baseline that cannot be read or is of other work, a debug build, a
/// volume without room, a clock for a child's CPU time that does not agree with this
/// process's own, and - last, so that every refusal before it makes nothing on disk - a
/// place the result cannot be written to. That last one makes the result's directory.
fn start(args: &Args) -> Result<Start, SpeedError> {
    let commit = host::commit();
    let out = match &args.out {
        Some(out) => out.clone(),
        None => default_out(&args.dir, &commit),
    };
    // Now, and not an hour from now: neither the result nor its log is written over.
    for place in [out.clone(), report::log_path(&out)] {
        if place.try_exists().map_err(io_at(&place))? {
            return Err(SpeedError::OutExists(place));
        }
    }
    let baseline = args.baseline.as_deref().map(report::read).transpose()?;
    if cfg!(debug_assertions) && !args.unchecked {
        return Err(SpeedError::DebugBuild);
    }

    let exe = std::env::current_exe().map_err(io_at(Path::new("this binary's own path")))?;
    let binary = host::binary(&exe, &commit)?;
    let mut machine = host::machine();
    // Now, and by the bench: `serve` refuses it too, but as a server that did not start.
    if let Some(threads) = args
        .threads
        .filter(|threads| *threads > machine.cores_logical)
    {
        return Err(SpeedError::Host(format!(
            "--threads {threads} is above the {} core(s) this machine has, and `kibbr \
             serve` refuses scan threads it has no core for. Nothing was started",
            machine.cores_logical
        )));
    }
    machine.disk_free_bytes = corpus::free_space(&args.dir, args.lines)?;
    machine.cpu_clock_check = host::cpu_clock_check()?;
    let server = ServerFacts {
        argv: server::argv(server::COPY, machine.cores_logical, args.threads),
        // What every child's command has taken out of it, read off one that is not run.
        env_removed: server::scrub(&mut Command::new(&exe)),
        env_noted: server::env_noted(),
    };
    let flags = flags(args);
    if let Some(baseline) = &baseline {
        report::comparable_before(baseline, &flags, &machine, &server, &plan::queries())?;
    }
    // Now, and not an hour from now: a directory that cannot be made, or that no file can
    // be made in, is found when the runs are over if it is not looked for here.
    report::writable(&out)?;
    Ok(Start {
        exe,
        out,
        baseline,
        commit,
        binary,
        machine,
        server,
        flags,
    })
}

/// The two gates of one run - another kibbr process, and swap-outs - with what they have
/// cost it so far.
struct Watch {
    run: usize,
    /// False under `--unchecked`: nothing is waited for, and nothing is read after a unit.
    checked: bool,
    /// The swap counters as they were last read: before the unit that runs next.
    swap: Option<Swap>,
    voided: Vec<Voided>,
    waited_s: u64,
    waited_for: Vec<String>,
}

impl Watch {
    fn new(run: usize, checked: bool) -> Self {
        Self {
            run,
            checked,
            swap: None,
            voided: Vec::new(),
            waited_s: 0,
            waited_for: Vec::new(),
        }
    }

    /// What stands in the way of a quiet machine at this moment: what the wait is for, and
    /// the same with its numbers. The swap-out counter counts from boot, so only a
    /// difference means anything: it is read twice, `SWAP_GAP` apart. A system with no
    /// counter has no such gate.
    fn disturbance(&mut self) -> Result<Option<(&'static str, String)>, SpeedError> {
        let others = host::other_kibbr()?;
        if !others.is_empty() {
            return Ok(Some((
                "another kibbr process",
                format!("another kibbr process is alive ({})", pids(&others)),
            )));
        }
        let Some(first) = host::swap()? else {
            self.swap = None;
            return Ok(None);
        };
        thread::sleep(SWAP_GAP);
        let Some(second) = host::swap()? else {
            self.swap = None;
            return Ok(None);
        };
        self.swap = Some(second);
        if second.outs != first.outs {
            return Ok(Some((
                "the machine to stop swapping",
                format!(
                    "{} pages were swapped out in {:.1} s",
                    commas(second.outs.saturating_sub(first.outs)),
                    SWAP_GAP.as_secs_f64()
                ),
            )));
        }
        Ok(None)
    }

    /// Wait until neither gate is shut, looking again every `WAIT_STEP`, and return the
    /// seconds waited. Each look that fails is said on standard error; a machine that is
    /// not quiet after `WAIT_LIMIT` ends the command.
    fn quiet(&mut self) -> Result<u64, SpeedError> {
        let mut waited = Duration::ZERO;
        loop {
            let Some((what, said)) = self.disturbance()? else {
                return Ok(waited.as_secs());
            };
            if waited >= WAIT_LIMIT {
                return Err(SpeedError::Waited {
                    minutes: WAIT_LIMIT.as_secs() / 60,
                    reason: said,
                });
            }
            eprintln!(
                "waiting: {said}; looking again in {} s",
                WAIT_STEP.as_secs()
            );
            if !self.waited_for.iter().any(|known| known == what) {
                self.waited_for.push(what.to_string());
            }
            thread::sleep(WAIT_STEP);
            waited += WAIT_STEP;
            self.waited_s += WAIT_STEP.as_secs();
        }
    }

    /// Before a run's first unit: the pause, the two gates, and then - with nothing of
    /// kibbr's running - the load average, the memory, how the machine is powered, and
    /// the two references. Under `--unchecked` none of it, and every reading is `null`.
    fn before_run(&mut self, work: &Path) -> Result<Checks, SpeedError> {
        if !self.checked {
            return Ok(Checks::default());
        }
        thread::sleep(SETTLE);
        self.quiet()?;
        let (outs, ins) = match self.swap {
            Some(swap) => (Some(swap.outs), Some(swap.ins)),
            None => (None, None),
        };
        let checks = Checks {
            waited_s: self.waited_s,
            waited_for: self.waited_for.clone(),
            swapouts_before: outs,
            swapins_before: ins,
            load_average: host::load_average(),
            memory_available_bytes: host::memory_available(),
            // Read before the references here and after them at the run's end, so that
            // the two readings stand around everything the run timed.
            power_before: host::power(),
            spin_ms_before: Some(as_ms(host::spin())),
            flush_ms_before: Some(as_ms(host::flush_ref(work).map_err(io_at(work))?)),
            spin_ms_after: None,
            flush_ms_after: None,
            power_after: None,
            after_unit_checks: true,
        };
        // The references took a second: what the first unit is held to is read after them.
        self.swap = host::swap()?;
        Ok(checks)
    }

    /// Why the unit that has just ended is thrown away, if it is: swap-outs moved since it
    /// began, or another kibbr process is alive. Read with the unit's own server gone,
    /// between two units and never inside one.
    fn disturbed(&mut self) -> Result<Option<String>, SpeedError> {
        let mut reasons = Vec::new();
        let now = host::swap()?;
        if let (Some(was), Some(now)) = (self.swap, now) {
            if now.outs != was.outs {
                reasons.push(format!(
                    "{} pages were swapped out during it",
                    commas(now.outs.saturating_sub(was.outs))
                ));
            }
        }
        self.swap = now;
        let others = host::other_kibbr()?;
        if !others.is_empty() {
            reasons.push(format!(
                "another kibbr process was alive when it ended ({})",
                pids(&others)
            ));
        }
        Ok((!reasons.is_empty()).then(|| reasons.join(", and ")))
    }

    /// Run one unit, and run it again for as long as it was disturbed. A disturbed unit
    /// keeps none of its numbers: it is named with its reason, the gates are waited for,
    /// and it is done again. One more than `VOID_LIMIT` of them in a run end the command -
    /// a machine that swaps under the bench's own load needs room made, not a mark in a
    /// table.
    fn unit<T>(
        &mut self,
        log: &mut Log,
        name: &str,
        mut measure: impl FnMut() -> Result<T, SpeedError>,
    ) -> Result<T, SpeedError> {
        loop {
            let measured = measure()?;
            if !self.checked {
                return Ok(measured);
            }
            let Some(reason) = self.disturbed()? else {
                return Ok(measured);
            };
            drop(measured);
            let voided = Voided {
                unit: name.to_string(),
                reason: reason.clone(),
            };
            self.voided.push(voided.clone());
            if self.voided.len() > VOID_LIMIT {
                return Err(SpeedError::Disturbed {
                    run: self.run,
                    units: self.voided.len(),
                    reason,
                });
            }
            let waited = self.quiet()?;
            report::log_voided(log, self.run, &voided, waited)?;
        }
    }
}

/// One run: the checks, the plan step, ingest alone, the idle server, the laps, and the
/// references, the power state and the binary's digest again. Each unit's lines are
/// printed when the unit is over, never while it runs.
fn one_run(
    run: usize,
    start: &Start,
    work: &Work,
    queries: &Seven<QueryText>,
    trie: &mut PlanTrie,
    log: &mut Log,
) -> Result<Run, SpeedError> {
    let flags = &start.flags;
    let began = Instant::now();
    let started_at = now();
    let mut watch = Watch::new(run, flags.checked);

    let mut checks = watch.before_run(work.work)?;
    report::log_checks(log, run, &checks, flags)?;

    let plan = watch.unit(log, "plan", || {
        plan::child(work.exe, work.samples, work.work)
    })?;
    // One trie for every run of every result: the first run's is the result's, and a run
    // that planned on another has timed something else.
    let planned = PlanTrie {
        source: plan::TRIE_SOURCE.to_string(),
        templates: plan.templates,
        nodes: plan.nodes,
        tokens: plan.tokens,
    };
    if run == 1 {
        *trie = planned;
    } else if *trie != planned {
        return Err(SpeedError::Corpus(format!(
            "the plan step's trie had {} templates, {} nodes and {} tokens in run 1 and has \
             {}, {} and {} in run {run}: the same lines did not build the same trie twice",
            commas(trie.templates),
            commas(trie.nodes),
            commas(trie.tokens),
            commas(planned.templates),
            commas(planned.nodes),
            commas(planned.tokens),
        )));
    }
    report::log_plan(log, run, &plan, flags)?;

    let ingest = watch.unit(log, "ingest", || phases::ingest_alone(work))?;
    report::log_ingest(log, run, &ingest)?;

    let (idle, scan) = watch.unit(log, "idle", || phases::idle(work))?;
    report::log_idle(log, run, &idle, &scan, queries, flags)?;

    report::log_laps(log, run, flags)?;
    let mut laps = Vec::with_capacity(work.samples);
    for lap in 1..=work.samples {
        laps.push(watch.unit(log, &format!("lap {lap}"), || phases::lap(work))?);
        report::log_lap(log, run, lap, work.samples)?;
    }
    let loaded = phases::loaded(&laps, work);
    report::log_loaded(log, run, &loaded, flags)?;

    if flags.checked {
        checks.spin_ms_after = Some(as_ms(host::spin()));
        checks.flush_ms_after = Some(as_ms(host::flush_ref(work.work).map_err(io_at(work.work))?));
        checks.power_after = host::power();
    }
    // A build beside the bench replaces the binary being measured; the servers of this
    // run were then not all one build.
    host::binary_unchanged(&start.binary)?;
    // Every wait of the run, the ones after a disturbed unit among them.
    checks.waited_s = watch.waited_s;
    checks.waited_for = watch.waited_for;
    let run = Run {
        run,
        started_at,
        seconds: began.elapsed().as_secs_f64(),
        checks,
        voided: watch.voided,
        plan,
        ingest,
        idle,
        scan,
        loaded,
    };
    report::log_end(log, &run)?;
    Ok(run)
}

/// What a result records of the loaded bytes and the live stream.
fn data(loaded: &Corpus, stream: &corpus::LiveStream) -> Data {
    let marker = &loaded.marker;
    Data {
        dir: loaded.dir.display().to_string(),
        lines: marker.lines,
        seed: marker.seed,
        input: marker.input.clone(),
        load: marker.load.clone(),
        files: marker.files.clone(),
        s3_id: marker.s3_id.clone(),
        built_at: marker.built_at.clone(),
        built_by: marker.built_by.clone(),
        live_stream: LiveStreamFacts {
            lines: stream.lines,
            seed: LIVE_SEED,
            bytes: stream.bytes.len() as u64,
            crc32c: stream.crc32c.clone(),
            bodies: stream.bodies.len(),
        },
    }
}

/// Make this invocation's work directory, which must not be there yet.
///
/// Its name is the process id's, and a process id comes round again - at once in a
/// container, where the bench is the same low one every time. A directory of that name is
/// then an earlier bench's that did not end well: it holds what that bench's children
/// wrote, and its result if it could not be saved where it was told. Used as this run's
/// own it would be removed whole when this run ended well.
fn make_work(work: &Path) -> Result<(), SpeedError> {
    fs::create_dir(work).map_err(|source| match source.kind() {
        io::ErrorKind::AlreadyExists => SpeedError::Host(format!(
            "{} is already there: the work directory of an earlier bench that had this \
             process id and did not end well. It holds what that bench's children wrote to \
             standard error, and its result if it holds unsaved.json. Move it away and run \
             again; the bench removes nothing it did not make",
            work.display()
        )),
        _ => SpeedError::Io {
            path: work.to_path_buf(),
            source,
        },
    })
}

/// A result that could not be saved where it was told is put in the work directory,
/// which a failed command leaves and names, and `said` - standard error - is told where.
/// What was measured is not thrown away with the place it could not go to.
///
/// If it cannot be kept there either, that is said with its own reason, and the result is
/// printed to `said` whole: the terminal is then the last place it can be. Not when
/// either write stopped at a result that would not read back - that one is on the disk
/// under its `.tmp` name, and the error says where.
fn keep_unsaved(
    work: &Path,
    out: &Path,
    file: &ResultFile,
    log: &str,
    first: &SpeedError,
    said: &mut dyn Write,
) {
    let kept = work.join("unsaved.json");
    // Standard error that cannot be written to has no one left to tell.
    let second = match report::write(&kept, file, log) {
        Ok(()) => {
            let _ = writeln!(
                said,
                "kibbr: the result could not be saved to {}; it is in {}, with its log \
                 beside it",
                out.display(),
                kept.display()
            );
            return;
        }
        Err(second) => second,
    };
    let _ = writeln!(
        said,
        "kibbr: the result could not be saved to {}, nor kept in {}: {second}",
        out.display(),
        kept.display()
    );
    let on_disk = |error: &SpeedError| matches!(error, SpeedError::Baseline { .. });
    if on_disk(first) || on_disk(&second) {
        return;
    }
    if let Ok(text) = report::result_text(file) {
        let _ = writeln!(
            said,
            "kibbr: so that what was measured is not lost, the result follows, between two \
             lines of dashes; saved to a file as it is, it is a result\n\
             ----------\n{text}----------"
        );
    }
}

/// Everything from the loaded directory to the saved result, with `work` as the place
/// for this invocation's copies and its children's standard error. `made` is set once
/// `work` is this invocation's own: until then nothing of it is there to keep or remove.
fn measure(
    args: &Args,
    start: &Start,
    work: &Path,
    made: &mut bool,
    out: &mut dyn Write,
) -> Result<(), SpeedError> {
    // The loaded directory: built by this invocation, or held to its marker.
    let to_load = corpus::loaded_dir(&args.dir, args.lines);
    if !to_load.exists() && !corpus::marker_path(&args.dir, args.lines).exists() {
        // Standard output begins once the first server has answered, which for a first
        // invocation is after a day has been generated and loaded.
        eprintln!(
            "kibbr: loading {} farm lines into {}, once; not timed",
            commas(args.lines),
            to_load.display()
        );
    }
    let built_by = BuiltBy {
        sha: start.commit.sha.clone(),
        binary_crc32c: start.binary.crc32c.clone(),
    };
    let loaded = corpus::prepare(&start.exe, &args.dir, args.lines, &built_by)?;

    // The directory `--dir` names is there: the loaded directory is in it.
    let stale = corpus::stale_work(&args.dir);
    make_work(work)?;
    *made = true;

    // The live stream, its bodies and their request heads, and the seven query requests:
    // every byte a server will be sent, before any server starts.
    let stream = corpus::live_stream(&start.exe, args.lines, args.body_lines, work)?;
    let heads = corpus::heads(&stream);
    let queries = queries(&loaded.marker.s3_id);
    let requests = queries.map(|query| wire::query_request(&query.text));

    // One server asked everything a run will ask; its answers are the expected ones.
    let cores = start.machine.cores_logical;
    let (expect, answer_room) = phases::preflight(
        &start.exe,
        &loaded,
        work,
        cores,
        args.threads,
        &stream,
        &heads,
        &requests,
    )?;
    let data = data(&loaded, &stream);
    if let Some(baseline) = &start.baseline {
        report::comparable_data(baseline, &data, &queries, &expect)?;
    }

    let bench = report::source_digest();
    let mut file = ResultFile {
        schema: report::SCHEMA.to_string(),
        label: label(args),
        created_at: now(),
        commit: start.commit.clone(),
        binary: start.binary.clone(),
        baseline: start
            .baseline
            .as_ref()
            .map(|baseline| report::baseline_ref(baseline, &bench)),
        bench,
        flags: start.flags.clone(),
        machine: start.machine.clone(),
        server: start.server.clone(),
        data,
        queries,
        expect,
        plan_queries: plan::queries(),
        plan_trie: PlanTrie {
            source: plan::TRIE_SOURCE.to_string(),
            ..PlanTrie::default()
        },
        runs: Vec::with_capacity(args.runs),
        per_run: report::PerRun::default(),
        medians: report::Medians::default(),
        notes: Vec::new(),
    };

    let mut log = Log::new(out);
    report::log_start(&mut log, &file, &loaded)?;
    report::log_stale(&mut log, &stale)?;

    let unit = Work {
        exe: &start.exe,
        loaded: &loaded,
        work,
        cores,
        threads: args.threads,
        stream: &stream,
        heads: &heads,
        requests: &requests,
        expect: &file.expect,
        answer_room,
        lines: args.lines,
        samples: args.samples,
        body_lines: args.body_lines,
    };
    let mut runs = Vec::with_capacity(args.runs);
    let mut trie = file.plan_trie.clone();
    for run in 1..=args.runs {
        runs.push(one_run(
            run,
            start,
            &unit,
            &file.queries,
            &mut trie,
            &mut log,
        )?);
    }
    file.runs = runs;
    file.plan_trie = trie;
    file.per_run = report::per_run(&file.runs);
    file.medians = report::medians(&file.per_run);
    file.notes = report::notes(&file);
    report::log_summary(&mut log, &file)?;

    // The table is made before the result is saved, so that the log saved beside it ends
    // with the table; and printed after, so that nothing says `saved` of a file that is
    // not there. A table that cannot be made does not cost the result: it is saved first,
    // and the error is told after. Nor does a place that can no longer be written to.
    let mut columns = Vec::with_capacity(2);
    columns.extend(start.baseline.clone());
    columns.push(Saved {
        path: start.out.clone(),
        file,
    });
    let table = report::table(&columns);
    let mut last = vec![report::saved_line(&start.out), String::new()];
    if let Ok(table) = &table {
        last.extend(table.lines.iter().cloned());
    }
    let with = |lines: &[String]| {
        let mut text = log.text().to_string();
        for line in lines {
            text.push_str(line);
            text.push('\n');
        }
        text
    };
    if let Some(this) = columns.last() {
        if let Err(error) = report::write(&start.out, &this.file, &with(&last)) {
            // Without the line that says `saved`: it was not.
            keep_unsaved(
                work,
                &start.out,
                &this.file,
                &with(&last[1..]),
                &error,
                &mut io::stderr(),
            );
            return Err(error);
        }
    }
    for line in &last {
        log.line(line)?;
    }
    let table = table?;
    if table.fails > 0 {
        return Err(SpeedError::Failed { lines: table.fails });
    }
    Ok(())
}

/// Measure, save, and print the table. `Err(SpeedError::Failed)` comes after the table is
/// printed and the result saved.
///
/// The work directory - each server's copy and every child's standard error - is this
/// invocation's own, made by it and refused if it is already there: removed when the
/// result is saved, and left and named when the command fails, since a child's last
/// words are then in it. One this invocation did not make is neither.
pub fn run(args: &Args, out: &mut dyn Write) -> Result<(), SpeedError> {
    let start = start(args)?;
    let work = corpus::work_dir(&args.dir);
    let mut made = false;
    let result = measure(args, &start, &work, &mut made, out);
    match &result {
        _ if !made => {}
        Ok(()) | Err(SpeedError::Failed { .. }) => {
            if let Err(error) = fs::remove_dir_all(&work) {
                eprintln!("kibbr: {} could not be removed: {error}", work.display());
            }
        }
        Err(_) => {
            eprintln!(
                "kibbr: kept {}, which holds what each child wrote to standard error",
                work.display()
            );
        }
    }
    result
}

/// Measure nothing: print the table from saved results, the first as the baseline.
pub fn table(files: &[PathBuf], out: &mut dyn Write) -> Result<(), SpeedError> {
    let mut saved = Vec::with_capacity(files.len());
    for path in files {
        saved.push(report::read(path)?);
    }
    let table = report::table(&saved)?;
    for line in &table.lines {
        writeln!(out, "{line}").map_err(on_stdout)?;
    }
    if table.fails > 0 {
        return Err(SpeedError::Failed { lines: table.fails });
    }
    Ok(())
}

/// What a run starts as a child: build the trie of 10,000 templates in this process, time
/// the plans, and print them as one JSON line.
pub fn plan_only(samples: usize, out: &mut dyn Write) -> Result<(), SpeedError> {
    let line = plan::measure(samples)?;
    writeln!(out, "{}", serde_json::to_string(&line)?).map_err(on_stdout)
}

/// The `bench` arm of `main.rs`: `--plan-only`, `--table`, or the measurement.
#[allow(clippy::too_many_arguments)]
pub fn cli(
    out: &mut dyn Write,
    lines: Option<u64>,
    profile: Option<crate::gen::Profile>,
    save: Option<PathBuf>,
    baseline: Option<PathBuf>,
    label: Option<String>,
    dir: PathBuf,
    runs: usize,
    samples: usize,
    body_lines: usize,
    threads: Option<usize>,
    unchecked: bool,
    table: Vec<PathBuf>,
    plan_only: bool,
) -> Result<(), crate::error::CliError> {
    let result = if plan_only {
        self::plan_only(samples, out)
    } else if !table.is_empty() {
        self::table(&table, out)
    } else {
        match (lines, profile) {
            (Some(lines), Some(crate::gen::Profile::Farm)) => run(
                &Args {
                    lines,
                    out: save,
                    baseline,
                    label,
                    dir,
                    runs,
                    samples,
                    body_lines,
                    threads,
                    unchecked,
                },
                out,
            ),
            // Both are asked for unless `--table` or `--plan-only` is given, so
            // what is left is the other profile. Refused here, before the module
            // is reached: it is handed no profile, and makes nothing under `--dir`.
            _ => Err(SpeedError::Profile),
        }
    };
    result.map_err(crate::error::CliError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_is_a_whole_line_count_and_the_same_one_floating_point_would_miss() {
        assert_eq!(
            place_lines(1_000_000),
            [20_000, 30_000, 70_000, 140_000],
            "the measurement's four places"
        );
        assert_eq!(place_lines(2_000), [40, 60, 140, 280]);
        // A share that is not a whole line waits for the next one.
        assert_eq!(place_lines(33), [1, 1, 3, 5]);
        for (place, share) in LOAD_AT_PERCENT.iter().zip(LOAD_AT) {
            assert_eq!(*place as f64 / 100.0, share, "the two spellings of a place");
        }
    }

    #[test]
    fn a_small_run_has_fewer_unmeasured_asks_and_one_scan_ask_at_least() {
        assert_eq!((warmup(100), plan_warmup(100), scan_asks(100)), (3, 5, 25));
        assert_eq!((warmup(1), plan_warmup(1), scan_asks(1)), (1, 1, 1));
        assert_eq!((warmup(4), plan_warmup(4), scan_asks(7)), (3, 4, 1));
    }

    #[test]
    fn the_seven_texts_are_the_ones_a_saved_result_is_compared_on() {
        let queries = queries("20834abd-1e3d-462e-b89b-fb1d4643e6bb");
        let texts: Vec<&str> = queries.each().iter().map(|q| q.text.as_str()).collect();
        assert_eq!(
            texts,
            [
                r#"COUNT LOG {"event":"login_ok"}"#,
                r#"COUNT LOG {"level":"info"}"#,
                r#"MATCH LOG {"job.id":"20834abd-1e3d-462e-b89b-fb1d4643e6bb"}"#,
                "MATCH LOG {} LIMIT 500",
                r#"COUNT LOG {"level": *}"#,
                r#"COUNT LOG {"status":"200"}"#,
                r#"COUNT LOG {"status": *}"#,
            ]
        );
    }

    #[test]
    fn an_error_quotes_the_end_of_what_a_child_wrote() {
        assert_eq!(tail("a\nb\nc\n", 2), "b\nc");
        assert_eq!(tail("a\nb", 20), "a\nb");
        assert_eq!(tail("", 20), "");
    }

    #[test]
    fn an_error_says_what_was_met_in_one_sentence() {
        assert_eq!(
            SpeedError::Profile.to_string(),
            "kibbr bench measures the farm profile only"
        );
        assert_eq!(
            SpeedError::OutExists(PathBuf::from("testdata/bench/baseline.json")).to_string(),
            "testdata/bench/baseline.json exists; a saved result is not written over"
        );
        // True of a run, which saved its result, and of `--table`, which saves none.
        assert_eq!(
            SpeedError::Failed { lines: 2 }.to_string(),
            "the table above carries 2 FAIL line(s)"
        );
        // Either gate can be the reason, and the advice fits both.
        let disturbed = |reason: &str| {
            SpeedError::Disturbed {
                run: 2,
                units: 6,
                reason: reason.to_string(),
            }
            .to_string()
        };
        assert_eq!(
            disturbed("another kibbr process was alive when it ended (pid 36231)"),
            "run 2: 6 of its units were disturbed and thrown away, the last because another \
             kibbr process was alive when it ended (pid 36231). Nothing was saved: stop what \
             runs beside the bench, or make room if the machine swapped, and start again"
        );
        assert!(disturbed("1,204 pages were swapped out during it")
            .ends_with("or make room if the machine swapped, and start again"));

        let child = |code| {
            SpeedError::Child {
                what: "kibbr gen-logs",
                code,
                stderr: "999 lines where 1,000 were asked for".to_string(),
            }
            .to_string()
        };
        assert_eq!(
            child(Some(0)),
            "`kibbr gen-logs`, which the bench runs as a child, exited 0 without doing what \
             was asked of it: 999 lines where 1,000 were asked for"
        );
        assert!(child(Some(1)).contains("a child, exited 1: "));
        assert!(child(None).contains("a child, was ended by a signal: "));

        let sender = |sender| {
            SpeedError::Sender {
                sender,
                detail: "five attempts in a row read no answer".to_string(),
            }
            .to_string()
        };
        assert_eq!(
            sender(0),
            "ingest sender 1 of 4: five attempts in a row read no answer"
        );
        assert!(sender(SENDERS).starts_with("the 4 ingest senders: "));

        let start = SpeedError::ServerStart {
            log: PathBuf::from("work/server-1.log"),
            tail: "kibbr: cannot listen".to_string(),
        }
        .to_string();
        assert!(start.contains("within 120 s"), "{start}");
        assert!(start.ends_with("kept in work/server-1.log and ends:\nkibbr: cannot listen"));
    }

    /// A directory for one test's files, removed when the test ends.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("kibbr-speed-mod-{}-{name}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("a scratch directory");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn a_work_directory_that_is_already_there_is_an_earlier_bench_s_and_is_refused() {
        let scratch = Scratch::new("work");
        let work = corpus::work_dir(&scratch.0);
        make_work(&work).expect("made");
        assert!(work.is_dir());

        // What a bench that did not end well left in it: the only copy of its result.
        fs::write(work.join("unsaved.json"), "an earlier result").expect("a file");
        let refused = make_work(&work).expect_err("it is someone's");
        assert!(matches!(refused, SpeedError::Host(_)), "{refused:?}");
        let said = refused.to_string();
        assert!(
            said.starts_with(&format!(
                "{} is already there: the work directory of an earlier bench",
                work.display()
            )) && said
                .ends_with("Move it away and run again; the bench removes nothing it did not make"),
            "{said}"
        );
        assert_eq!(
            fs::read_to_string(work.join("unsaved.json")).expect("still there"),
            "an earlier result"
        );

        // A place it cannot be made in is that place's error, by path.
        let nowhere = scratch.0.join("not-there").join("work-1.noindex");
        let error = make_work(&nowhere).expect_err("no parent");
        assert!(
            matches!(&error, SpeedError::Io { path, .. } if *path == nowhere),
            "{error:?}"
        );
    }

    fn a_result() -> ResultFile {
        ResultFile {
            schema: report::SCHEMA.to_string(),
            label: "baseline".to_string(),
            ..ResultFile::default()
        }
    }

    #[test]
    fn a_result_that_cannot_be_saved_where_it_was_told_is_kept_in_the_work_directory() {
        let scratch = Scratch::new("unsaved");
        let work = scratch.0.join("work-1.noindex");
        fs::create_dir_all(&work).expect("the work directory");
        let out = scratch.0.join("results").join("baseline.json");
        let first = SpeedError::OutExists(out.clone());
        let file = a_result();

        let mut said = Vec::new();
        keep_unsaved(&work, &out, &file, "the log\n", &first, &mut said);
        let kept = work.join("unsaved.json");
        assert_eq!(
            String::from_utf8(said).expect("text"),
            format!(
                "kibbr: the result could not be saved to {}; it is in {}, with its log beside \
                 it\n",
                out.display(),
                kept.display()
            )
        );
        assert_eq!(report::read(&kept).expect("a whole result").file, file);
        assert_eq!(
            fs::read_to_string(work.join("unsaved.log")).expect("its log"),
            "the log\n"
        );
    }

    #[test]
    fn a_result_that_can_be_kept_nowhere_is_said_with_both_reasons_and_printed_whole() {
        let scratch = Scratch::new("nowhere");
        let out = scratch.0.join("baseline.json");
        let first = SpeedError::OutExists(out.clone());
        let file = a_result();

        // A work directory that is not there: nothing can be kept in it.
        let work = scratch.0.join("gone").join("work-1.noindex");
        fs::write(scratch.0.join("gone"), "a file where a directory is wanted").expect("a file");
        let mut said = Vec::new();
        keep_unsaved(&work, &out, &file, "the log\n", &first, &mut said);
        let said = String::from_utf8(said).expect("text");
        let kept = work.join("unsaved.json");
        assert!(
            said.starts_with(&format!(
                "kibbr: the result could not be saved to {}, nor kept in {}: ",
                out.display(),
                kept.display()
            )),
            "{said}"
        );
        // The second write's own reason is on that first line, and then the result, as
        // it would have been saved.
        let (reason, rest) = said.split_once('\n').expect("more than one line");
        assert!(
            reason.contains(&scratch.0.join("gone").display().to_string()),
            "{reason}"
        );
        let text = report::result_text(&file).expect("a result is JSON");
        assert!(
            rest.starts_with("kibbr: so that what was measured is not lost, the result follows")
                && rest.ends_with(&format!("\n----------\n{text}----------\n")),
            "{rest}"
        );
        let between: ResultFile = serde_json::from_str(&text).expect("it reads as a result");
        assert_eq!(between, file);

        // A result that would not read back is on the disk under its `.tmp` name, by
        // whichever write met it: it is named, and not printed.
        let work = scratch.0.join("work-2.noindex");
        fs::create_dir_all(&work).expect("the work directory");
        let mut unreadable = a_result();
        unreadable.medians.ingest_lines_per_s = f64::NAN;
        let mut said = Vec::new();
        keep_unsaved(&work, &out, &unreadable, "the log\n", &first, &mut said);
        let said = String::from_utf8(said).expect("text");
        assert_eq!(said.lines().count(), 1, "{said}");
        assert!(
            said.contains("unsaved.json.tmp")
                && said.contains("what was measured is kept in this file"),
            "{said}"
        );
        assert!(work.join("unsaved.json.tmp").exists());
    }

    #[test]
    fn a_label_is_the_one_given_or_says_which_side_of_a_comparison_this_is() {
        let mut args = Args {
            lines: 1,
            out: None,
            baseline: None,
            label: None,
            dir: PathBuf::from(DEFAULT_DIR),
            runs: DEFAULT_RUNS,
            samples: DEFAULT_SAMPLES,
            body_lines: DEFAULT_BODY_LINES,
            threads: None,
            unchecked: false,
        };
        assert_eq!(label(&args), "baseline");
        args.baseline = Some(PathBuf::from("baseline.json"));
        assert_eq!(label(&args), "this run");
        args.label = Some("after 1 (columns)".to_string());
        assert_eq!(label(&args), "after 1 (columns)");
    }
}
