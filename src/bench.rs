// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr-bench --lines N --profile farm`: this build's speed on one farm day, measured the
//! same way every time, so that the number a change is held to is one command's.
//! A unit is one server's work on its own copy of the loaded bytes; no server is shared,
//! and a unit disturbed by swap or another kibbr process is done again. Frozen once a
//! result is saved as a baseline: the module's files are digested into every result.

pub mod child;
pub mod corpus;
pub mod host;
pub mod phases;
pub mod plan;
pub mod report;
pub mod wire;

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use thiserror::Error;

use crate::cli::error::CliError;
use crate::engine::commas;

use corpus::{BuiltBy, Corpus};
use host::Swap;
use phases::Work;
use report::{
    Binary, Checks, Commit, Data, Flags, FlushRef, LiveStreamFacts, Log, Machine, PlanTrie,
    QueryText, ResultFile, Run, Saved, ServerFacts, Seven, Voided,
};

/// Where the loaded directory, each server's copy of it and the default results go, unless
/// `--dir` names another place.
pub const DEFAULT_DIR: &str = "target/scratch/bench";
/// `--runs`, unless given: odd, so that the median of the runs is one of them.
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
/// The arrival time given at both doors, noon of the farm's day, so that no stored value
/// depends on the date the bench ran.
pub const RECEIVED_AT: &str = "2024-01-15T12:00:00Z";
/// Where in the live stream each of the four queries is asked under ingest, as the share
/// of its lines acknowledged; `place_lines` is what is waited for.
pub const LOAD_AT: [f64; 4] = [0.02, 0.03, 0.07, 0.14];
/// `LOAD_AT` in whole percent, so that a place is a line count worked out in integers.
const LOAD_AT_PERCENT: [u64; 4] = [2, 3, 7, 14];
/// Asks that are not measured before an idle block and before the scan block, when there
/// are that many samples.
pub const WARMUP: usize = 3;
/// Plans that are not measured before a shape's timed ones, when there are that many.
pub const PLAN_WARMUP: usize = 5;
/// How long a sender waits before it sends a refused body again: the bench's own wait and
/// not the server's `Retry-After`, so that a refusal costs the same on every build.
pub const REFUSAL_PAUSE: Duration = Duration::from_millis(50);
/// Attempts in a row that read no status line, after which a sender gives up.
pub const SILENT_LIMIT: usize = 5;
/// With nothing acknowledged, reached or answered for this long the phase ends in an
/// error: a server that refuses for ever must not hold a run for ever.
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
/// `--max-rows` of every server, the rows a fetch with no `LIMIT` is cut at, which S3 must
/// never reach.
pub const MAX_ROWS: u64 = 10_000;

/// The seven query texts, in the order every result holds them; S2, S3 and SCAN are not
/// the measuring script's, since a filter that matches no farm row reads no row.
pub const S1: &str = r#"COUNT LOG {"event":"login_ok"}"#;
pub const S2: &str = r#"COUNT LOG {"level":"info"}"#;
pub const S4: &str = "MATCH LOG {} LIMIT 500";
pub const KEY: &str = r#"COUNT LOG {"level": *}"#;
pub const SCAN: &str = r#"COUNT LOG {"status":"200"}"#;
pub const SCANKEY: &str = r#"COUNT LOG {"status": *}"#;

/// What `kibbr-bench --lines N --profile farm` was asked for; the profile is refused by
/// the command line before this is built.
pub struct Args {
    /// The `kibbr` that is measured; `None` is the one beside this binary.
    pub kibbr: Option<PathBuf>,
    pub lines: u64,
    pub out: Option<PathBuf>,
    pub baseline: Option<PathBuf>,
    pub label: Option<String>,
    pub dir: PathBuf,
    pub runs: usize,
    pub samples: usize,
    pub body_lines: usize,
    /// `--scan-threads` of every server measured; with `None` the flag is not passed and a
    /// server takes its build's default.
    pub threads: Option<usize>,
    pub unchecked: bool,
}

/// What stops `kibbr-bench`, each sentence saying what was met and what to do about it:
/// the command is run rarely and by hand.
#[derive(Debug, Error)]
pub enum SpeedError {
    /// `--profile hostile`: the queries, their expected answers and the places in the
    /// stream are the farm's.
    #[error("kibbr-bench measures the farm profile only")]
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

    /// A child that is not a server: `what` is its command, and `stderr` what was wrong
    /// followed by the last lines the child wrote to standard error.
    #[error("`{what}`, which the bench runs as a child, {}: {stderr}", ended(.code))]
    Child {
        what: &'static str,
        code: Option<i32>,
        stderr: String,
    },

    /// The loaded directory, its marker, the live stream or the plan step's trie is not
    /// what every result is measured over.
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
    /// for, met in the preflight before a run is spent.
    #[error("{surface} did not answer the way kibbr-bench reads it: {detail}")]
    Surface {
        surface: &'static str,
        detail: String,
    },

    /// A query's answer that cannot be a sample: an odd status, a refusal still the answer
    /// after `STALL`, or a 200 that is not this query's answer over these bytes.
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

    /// The machine, or the binary on it, is not fit to measure on.
    #[error("{0}")]
    Host(String),

    #[error(
        "the machine did not become quiet in {minutes} minutes: {reason}. Nothing was \
         saved; stop what runs beside the bench and start it again"
    )]
    Waited { minutes: u64, reason: String },

    /// `reason` is either gate's: pages swapped out during a unit, or another kibbr
    /// process alive when it ended.
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

    #[error("JSON that kibbr-bench reads or writes is not in the form it has")]
    Json(#[from] serde_json::Error),

    /// Returned after the table is printed, so that the command still ends on its table and
    /// exits 1; it says nothing of a saved result, since `--table` returns it too.
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

/// Asks in the scan block, a quarter of the samples and at least one: the scan rate is a
/// sum over the block and not a percentile, so a quarter is enough.
pub fn scan_asks(samples: usize) -> usize {
    (samples / 4).max(1)
}

/// The live lines that must be acknowledged before each query is asked under ingest:
/// `LOAD_AT` of `lines` rounded up, in integers so a place is the same line on every machine.
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

/// What a result records under `flags`, the command line's values and every constant a
/// number depends on: two results compare only where these are equal.
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
    /// The `kibbr` measured: every server, load and generator is it.
    exe: PathBuf,
    /// This binary, which the plan step is run as.
    bench: PathBuf,
    out: PathBuf,
    baseline: Option<Saved>,
    commit: Commit,
    binary: Binary,
    machine: Machine,
    server: ServerFacts,
    flags: Flags,
}

/// The `kibbr` that is measured: the one `--kibbr` names, or the one beside this binary, as
/// cargo builds the two; refused when there is no file there.
fn kibbr_beside(bench: &Path, named: Option<&Path>) -> Result<PathBuf, SpeedError> {
    let exe = match named {
        Some(named) => named.to_path_buf(),
        None => bench.with_file_name(format!("kibbr{}", std::env::consts::EXE_SUFFIX)),
    };
    if !exe.is_file() {
        return Err(SpeedError::Host(format!(
            "there is no kibbr binary at {}: build it beside this one (`cargo build --release \
             --features bench`) or name one with --kibbr. Nothing was started",
            exe.display()
        )));
    }
    Ok(exe)
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

/// Everything that can be refused before a child is started. A place the result cannot
/// be written to is last, so that every refusal before it makes nothing on disk.
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

    let bench = std::env::current_exe().map_err(io_at(Path::new("this binary's own path")))?;
    let exe = kibbr_beside(&bench, args.kibbr.as_deref())?;
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
        argv: child::argv(child::COPY, machine.cores_logical, args.threads),
        // What every child's command has taken out of it, read off one that is not run.
        env_removed: child::scrub(&mut Command::new(&exe)),
        env_noted: child::env_noted(),
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
        bench,
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

    /// What stands in the way of a quiet machine at this moment. The swap-out counter counts
    /// from boot, so it is read twice, `SWAP_GAP` apart; with no counter there is no such gate.
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
    /// seconds waited. A machine that is not quiet after `WAIT_LIMIT` ends the command.
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

    /// Before a run's first unit: the pause, the two gates, and then the readings and the
    /// two references. Under `--unchecked` none of it, and every reading is `null`.
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

    /// Why the unit that has just ended is thrown away, if it is. Read with the unit's own
    /// server gone, between two units and never inside one.
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

    /// Run one unit, and run it again for as long as it was disturbed. Past `VOID_LIMIT` the
    /// command ends: a machine that swaps under the bench's own load needs room made.
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

/// One run: the checks, the units, and the references, power state and binary's digest
/// again. Each unit's lines are printed when the unit is over, never while it runs.
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
        plan::child(work.bench, work.samples, work.work)
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

/// Make this invocation's work directory, which must not be there yet. A process id comes
/// round again, and an earlier bench's directory may hold its only unsaved result.
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

/// A result that could not be saved where it was told is put in the work directory, and
/// failing that printed whole to `said`: what was measured is not thrown away.
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

/// Everything from the loaded directory to the saved result. `made` is set once `work`
/// is this invocation's own: until then nothing of it is there to keep or remove.
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
        bench: &start.bench,
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

    // The table is made before the result is saved, so the log saved beside it ends with it,
    // and printed after, so nothing says `saved` of a file that is not there.
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

/// Measure, save and print the table, removing the work directory when the result is
/// saved and leaving and naming it on failure, when a child's last words are in it.
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

/// `--plan-only`, `--table`, or the measurement.
fn command(cli: Cli, out: &mut dyn Write) -> Result<(), SpeedError> {
    if cli.plan_only {
        plan_only(cli.samples, out)
    } else if !cli.table.is_empty() {
        table(&cli.table, out)
    } else {
        match (cli.lines, cli.profile) {
            (Some(lines), Some(crate::cli::gen_logs::Profile::Farm)) => run(
                &Args {
                    kibbr: cli.kibbr,
                    lines,
                    out: cli.out,
                    baseline: cli.baseline,
                    label: cli.label,
                    dir: cli.dir,
                    runs: cli.runs,
                    samples: cli.samples,
                    body_lines: cli.body_lines,
                    threads: cli.threads,
                    unchecked: cli.unchecked,
                },
                out,
            ),
            // Both are asked for unless `--table` or `--plan-only` is given, so what is left is
            // the other profile, refused before anything is made under `--dir`.
            _ => Err(SpeedError::Profile),
        }
    }
}

// ── The command line ───────────────────────────────────────────────────────────────

/// Measure how fast a build is on one farm day: ingest lines a second, the four standard
/// queries idle and under ingest, scan rows per CPU-second, and the plan step.
///
/// A binary of its own, built with the `bench` feature, and not a `kibbr` subcommand: the
/// bench is a quarter of the source, and the build that is edited and tested leaves it
/// out. It measures the `kibbr` beside it, or the one `--kibbr` names, which it runs as
/// every server, load and generator; the plan step is this binary, run again.
#[derive(clap::Parser)]
#[command(name = "kibbr-bench", version, long_about = None)]
struct Cli {
    /// The `kibbr` binary that is measured. [default: `kibbr` in this binary's directory]
    #[arg(long, value_name = "PATH")]
    kibbr: Option<PathBuf>,
    /// As `kibbr --config`: the settings file this process reads; the children it starts
    /// are measured with none.
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,
    /// As `kibbr --log-level`.
    #[arg(long, value_enum, value_name = "LEVEL")]
    log_level: Option<crate::cli::config::LogLevel>,
    /// As `kibbr --log-format`.
    #[arg(long, value_enum, value_name = "FORMAT")]
    log_format: Option<crate::cli::config::LogFormat>,
    /// Lines in the farm day that is loaded (seed 42), and in the live stream sent to
    /// the servers (seed 43). The measurement is `--lines 1000000`.
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        required_unless_present_any = ["table", "plan_only"]
    )]
    lines: Option<u64>,
    /// Only `farm` is measured: the queries and their expected answers are the
    /// farm's. `hostile` is refused.
    #[arg(long, value_enum, required_unless_present_any = ["table", "plan_only"])]
    profile: Option<crate::cli::gen_logs::Profile>,
    /// Where the result is saved, the raw log beside it as `.log`; a file that exists is
    /// refused. [default: <dir>/results/<UTC time>-<commit>.json]
    #[arg(long, value_name = "FILE")]
    out: Option<PathBuf>,
    /// A saved result to compare with, held to be the same work before anything is measured;
    /// each number is printed with its speed-up on it.
    #[arg(long, value_name = "FILE")]
    baseline: Option<PathBuf>,
    /// The heading of this result's column, saved in the file.
    /// [default: baseline, or `this run` with --baseline]
    #[arg(long, value_name = "TEXT")]
    label: Option<String>,
    /// Holds the loaded directory, each server's copy of it, and the default results.
    #[arg(long, value_name = "PATH", default_value = DEFAULT_DIR)]
    dir: PathBuf,
    /// How many times the whole sequence is run; each printed number is the middle
    /// one. Odd. Two results compare only at the same value.
    #[arg(long, value_name = "R", default_value_t = DEFAULT_RUNS, value_parser = odd)]
    runs: usize,
    /// Timed asks of each query, and timed plans of each shape. Two results compare only at
    /// the same value.
    #[arg(
        long,
        value_name = "K",
        default_value_t = DEFAULT_SAMPLES,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    samples: usize,
    /// Lines in one `/ingest` body. Two results compare only at the same value.
    #[arg(
        long,
        value_name = "L",
        default_value_t = DEFAULT_BODY_LINES,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    body_lines: usize,
    /// Scan threads of every server measured: each is started with `--scan-threads T`.
    /// Two results compare only at the same value.
    #[arg(
        long,
        value_name = "T",
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    threads: Option<usize>,
    /// Skip the checks that make a run a measurement, and allow a debug build; the result is
    /// marked `UNCHECKED - not a measurement`. For tests.
    #[arg(long)]
    unchecked: bool,
    /// Measure nothing: print the table from saved results, the first as the baseline
    /// and one column a file, headed by its label.
    #[arg(
        long,
        value_name = "FILE",
        num_args = 2..,
        conflicts_with_all = [
            "lines", "profile", "out", "baseline", "label", "dir", "runs", "samples",
            "body_lines", "unchecked", "plan_only"
        ]
    )]
    table: Vec<PathBuf>,
    // What a run starts as a child, once a run. Hidden, because its one JSON line is read
    // by the bench and not by a person.
    #[arg(
        long,
        hide = true,
        conflicts_with_all = [
            "lines", "profile", "out", "baseline", "label", "dir", "runs", "body_lines",
            "unchecked"
        ]
    )]
    plan_only: bool,
}

/// `bench --runs`: an odd count, so that the median of the runs is one of them and not a
/// number between two.
fn odd(text: &str) -> Result<usize, String> {
    match text.parse::<usize>() {
        Ok(runs) if runs % 2 == 1 => Ok(runs),
        Ok(runs) => Err(format!(
            "{runs} is not odd; each printed number is the middle one of the runs"
        )),
        Err(e) => Err(e.to_string()),
    }
}

/// `kibbr-bench`, the binary: its flags read, the settings and the log as every `kibbr`
/// command has them, then `--plan-only`, `--table` or the measurement.
pub fn main() -> ExitCode {
    let cli = <Cli as clap::Parser>::parse();
    let result =
        crate::cli::settle(cli.config.as_deref(), cli.log_level, cli.log_format).and_then(|_| {
            let stdout = io::stdout();
            let mut out = io::BufWriter::new(stdout.lock());
            let result = command(cli, &mut out).map_err(CliError::from);
            crate::cli::finish(result, out)
        });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            crate::cli::logging::failed(&e);
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
#[path = "bench_tests.rs"]
mod tests;
