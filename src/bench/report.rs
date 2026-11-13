// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a bench leaves behind: the result file (schema `kibbr-bench/1`), the raw log and
//! the table. Reading is lenient where writing is exact, so a result saved by an earlier
//! build of the bench still prints its column. A ratio is always a speed-up, and every
//! table number is the middle of the runs' own numbers, never a percentile over pooled
//! samples. Nothing here is timed: each log line is built after its unit is over.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::engine::commas;

use super::corpus::{BuiltBy, Corpus, FileDigest, InputDigest, LoadFacts};
use super::phases::{median, percentile};
use super::{crc_hex, on_stdout, place_lines, SpeedError, SCAN_TAIL, SWAP_GAP};

/// The `schema` of every result this build writes, and the only one it reads.
pub const SCHEMA: &str = "kibbr-bench/1";

/// The bench's own source, in the order it is digested with ASCII white space left out:
/// two results under two digests were measured by two instruments.
pub const SOURCES: [(&str, &str); 8] = [
    ("bench.rs", include_str!("../bench.rs")),
    ("corpus.rs", include_str!("corpus.rs")),
    ("child.rs", include_str!("child.rs")),
    ("wire.rs", include_str!("wire.rs")),
    ("host.rs", include_str!("host.rs")),
    ("phases.rs", include_str!("phases.rs")),
    ("plan.rs", include_str!("plan.rs")),
    ("report.rs", include_str!("report.rs")),
];

// ── One value for each query ───────────────────────────────────────────────────────

/// One value for each of the four standard queries, under the names a result uses.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Four<T> {
    #[serde(rename = "S1")]
    pub s1: T,
    #[serde(rename = "S2")]
    pub s2: T,
    #[serde(rename = "S3")]
    pub s3: T,
    #[serde(rename = "S4")]
    pub s4: T,
}

impl<T> Four<T> {
    pub const NAMES: [&'static str; 4] = ["S1", "S2", "S3", "S4"];

    pub fn from_array([s1, s2, s3, s4]: [T; 4]) -> Self {
        Self { s1, s2, s3, s4 }
    }

    /// The four in order, beside `NAMES`.
    pub fn each(&self) -> [&T; 4] {
        [&self.s1, &self.s2, &self.s3, &self.s4]
    }

    #[cfg(test)]
    pub fn each_mut(&mut self) -> [&mut T; 4] {
        [&mut self.s1, &mut self.s2, &mut self.s3, &mut self.s4]
    }

    pub fn map<U>(&self, f: impl FnMut(&T) -> U) -> Four<U> {
        Four::from_array(self.each().map(f))
    }
}

/// One value for each query the bench sends: the four, then the count of rows S2 must look
/// at, the scan block's query, and the count of rows that one must look at.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Seven<T> {
    #[serde(rename = "S1")]
    pub s1: T,
    #[serde(rename = "S2")]
    pub s2: T,
    #[serde(rename = "S3")]
    pub s3: T,
    #[serde(rename = "S4")]
    pub s4: T,
    #[serde(rename = "KEY")]
    pub key: T,
    #[serde(rename = "SCAN")]
    pub scan: T,
    #[serde(rename = "SCANKEY")]
    pub scan_key: T,
}

impl<T> Seven<T> {
    pub const NAMES: [&'static str; 7] = ["S1", "S2", "S3", "S4", "KEY", "SCAN", "SCANKEY"];

    pub fn from_array([s1, s2, s3, s4, key, scan, scan_key]: [T; 7]) -> Self {
        Self {
            s1,
            s2,
            s3,
            s4,
            key,
            scan,
            scan_key,
        }
    }

    /// The seven in order, beside `NAMES`.
    pub fn each(&self) -> [&T; 7] {
        [
            &self.s1,
            &self.s2,
            &self.s3,
            &self.s4,
            &self.key,
            &self.scan,
            &self.scan_key,
        ]
    }

    pub fn map<U>(&self, f: impl FnMut(&T) -> U) -> Seven<U> {
        Seven::from_array(self.each().map(f))
    }

    /// The four standard queries' values, by reference.
    pub fn four(&self) -> Four<&T> {
        Four {
            s1: &self.s1,
            s2: &self.s2,
            s3: &self.s3,
            s4: &self.s4,
        }
    }
}

/// One value for each plan shape.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Shapes<T> {
    #[serde(rename = "P1")]
    pub p1: T,
    #[serde(rename = "P2")]
    pub p2: T,
    #[serde(rename = "P3")]
    pub p3: T,
    #[serde(rename = "P4")]
    pub p4: T,
}

impl<T> Shapes<T> {
    pub const NAMES: [&'static str; 4] = ["P1", "P2", "P3", "P4"];

    pub fn from_array([p1, p2, p3, p4]: [T; 4]) -> Self {
        Self { p1, p2, p3, p4 }
    }

    /// The four in order, beside `NAMES`.
    pub fn each(&self) -> [&T; 4] {
        [&self.p1, &self.p2, &self.p3, &self.p4]
    }

    pub fn map<U>(&self, f: impl FnMut(&T) -> U) -> Shapes<U> {
        Shapes::from_array(self.each().map(f))
    }
}

// ── The result file ────────────────────────────────────────────────────────────────

/// A saved result: one invocation's facts, every sample of every run, and the numbers the
/// table prints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResultFile {
    pub schema: String,
    /// The column heading.
    pub label: String,
    pub created_at: String,
    pub commit: Commit,
    pub binary: Binary,
    pub bench: BenchSource,
    pub flags: Flags,
    pub machine: Machine,
    pub server: ServerFacts,
    pub data: Data,
    pub queries: Seven<QueryText>,
    /// Each query's answer on the loaded bytes, taken in the preflight.
    pub expect: Seven<Expect>,
    pub plan_queries: Shapes<QueryText>,
    pub plan_trie: PlanTrie,
    pub runs: Vec<Run>,
    /// One value a run behind each number of `medians`, in run order.
    pub per_run: PerRun,
    /// Exactly the table's numbers and those of the lines under it.
    pub medians: Medians,
    pub notes: Vec<String>,
    pub baseline: Option<BaselineRef>,
}

/// The checkout the bench ran in, where without git `sha` is `null`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Commit {
    pub sha: Option<String>,
    /// Whether `src`, `Cargo.toml` or `Cargo.lock` differs from the commit.
    pub src_dirty: Option<bool>,
    pub committed_at: Option<String>,
    /// How the sha was learned, in words: it is the checkout's, not the binary's.
    pub how: String,
}

/// The binary that measured, which is the binary measured.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Binary {
    pub path: String,
    pub bytes: u64,
    pub crc32c: String,
    pub modified_at: Option<String>,
    /// `release` or `debug`.
    pub build: String,
    /// Built before the commit it is recorded under was made.
    pub stale: Option<bool>,
}

/// The digest of `SOURCES`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BenchSource {
    pub source_crc32c: String,
    pub source_files: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Flags {
    pub lines: u64,
    pub profile: String,
    pub runs: usize,
    pub samples: usize,
    pub body_lines: usize,
    /// False under `--unchecked`: the result is then not a measurement.
    pub checked: bool,
    pub senders: usize,
    pub loaded_seed: u64,
    pub live_seed: u64,
    pub received_at: String,
    pub load_at: [f64; 4],
    pub warmup: usize,
    pub plan_warmup: usize,
    pub scan_asks: usize,
    pub refusal_pause_ms: u64,
    pub stall_s: u64,
    pub settle_s: u64,
    pub flush_ref: FlushRef,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FlushRef {
    pub writes: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Machine {
    pub os: String,
    pub os_version: Option<String>,
    pub arch: String,
    pub cpu: Option<String>,
    pub cores_logical: usize,
    pub cores_performance: Option<usize>,
    pub cores_efficiency: Option<usize>,
    pub memory_bytes: Option<u64>,
    /// Free on the volume that holds `--dir`, before anything was built.
    pub disk_free_bytes: u64,
    /// Where a child's CPU time comes from, in words; `null` where the system has no
    /// source this build reads.
    pub cpu_clock: Option<String>,
    /// This process's CPU time over a spin by that source, over the same by `getrusage`.
    pub cpu_clock_check: Option<f64>,
    /// Where swap counts come from; `null` where there is no counter, and then swap is
    /// not a gate.
    pub swap_counter: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServerFacts {
    /// Every server's arguments, with `<copy>` where its data directory stood.
    pub argv: Vec<String>,
    /// `KIBBR__` variables taken out of every child's environment, by name.
    pub env_removed: Vec<String>,
    /// Variables left in that a server may read, with their values.
    pub env_noted: BTreeMap<String, String>,
}

/// The loaded bytes and the live stream: what the marker says, and the stream's digest.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Data {
    pub dir: String,
    pub lines: u64,
    pub seed: u64,
    pub input: InputDigest,
    pub load: LoadFacts,
    pub files: Vec<FileDigest>,
    pub s3_id: String,
    pub built_at: String,
    pub built_by: BuiltBy,
    pub live_stream: LiveStreamFacts,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LiveStreamFacts {
    pub lines: u64,
    pub seed: u64,
    pub bytes: u64,
    pub crc32c: String,
    pub bodies: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryText {
    pub label: String,
    pub text: String,
}

/// What a query answers, less what moves between two asks of it: the part two answers
/// are compared on.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Expect {
    pub count: u64,
    pub rows_returned: Option<u64>,
    pub templates_matched: Option<u64>,
    pub truncated: Option<bool>,
    pub limit_source: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PlanTrie {
    pub source: String,
    pub templates: u64,
    pub nodes: u64,
    pub tokens: u64,
}

// ── One run ────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Run {
    /// Counted from 1.
    pub run: usize,
    pub started_at: String,
    pub seconds: f64,
    pub checks: Checks,
    /// Units thrown away and done again, with none of their numbers.
    pub voided: Vec<Voided>,
    pub plan: PlanRun,
    pub ingest: IngestRun,
    pub idle: IdleRun,
    pub scan: ScanRun,
    pub loaded: LoadedRun,
}

/// What was looked at before a run and after it, each reading `null` under `--unchecked`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Checks {
    pub waited_s: u64,
    /// What each wait was for, in words.
    pub waited_for: Vec<String>,
    pub swapouts_before: Option<u64>,
    pub swapins_before: Option<u64>,
    pub load_average: Option<f64>,
    pub memory_available_bytes: Option<u64>,
    pub spin_ms_before: Option<f64>,
    pub spin_ms_after: Option<f64>,
    pub flush_ms_before: Option<f64>,
    pub flush_ms_after: Option<f64>,
    /// How the machine was powered when the references were taken before the run, and
    /// again at its end; `null` where there is no reading, or in a result saved without them.
    pub power_before: Option<Power>,
    pub power_after: Option<Power>,
    /// Whether swap and the process list were read after each unit.
    pub after_unit_checks: bool,
}

/// How the machine was powered at one moment: a laptop in Low Power Mode or on its
/// battery holds its cores back, and the same build then reads slower in every row.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Power {
    /// What the machine draws from, as the system names it: `AC Power`, `Battery Power`.
    pub source: Option<String>,
    pub low_power_mode: Option<bool>,
}

/// What the system calls mains power.
const MAINS: &str = "AC Power";

impl Power {
    /// Whether this is known to be another state than mains power with Low Power Mode
    /// off. A part the system did not give is not held against the machine.
    fn off_mains(&self) -> bool {
        self.source.as_deref().is_some_and(|source| source != MAINS)
            || self.low_power_mode == Some(true)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Voided {
    pub unit: String,
    pub reason: String,
}

/// The plan step of one run, and the one JSON line the plan child prints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PlanRun {
    pub templates: u64,
    pub nodes: u64,
    pub tokens: u64,
    /// Generated lines the parser refused, which are in no template.
    pub lines_skipped: u64,
    #[serde(flatten)]
    pub shapes: Shapes<PlanShape>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PlanShape {
    /// Every timed plan, in the order taken.
    pub ms: Vec<f64>,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub min_ms: f64,
    /// Leaves the plan returned: a plan that returns others is not a faster one.
    pub leaves: u64,
}

/// A unit's server: its process, how long it took to answer, and the copy it ran on.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServerUnit {
    pub pid: u32,
    /// From spawn to the first 200 of `/health`.
    pub start_s: f64,
    /// Copying the loaded directory and reading the copy back against the marker.
    pub copy_ms: f64,
    pub copy_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Health {
    pub status: String,
    pub detail: Option<String>,
}

/// Ingest alone: the whole live stream, once, into a server that is asked nothing else.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestRun {
    pub server: ServerUnit,
    pub lines_ingested_before: u64,
    pub lines_ingested_after: u64,
    /// `lines_ingested_after - lines_ingested_before`: the server's count.
    pub lines: u64,
    pub uptime_before_s: f64,
    pub uptime_after_s: f64,
    /// `uptime_after_s - uptime_before_s`: the server's clock.
    pub window_s: f64,
    /// `lines / window_s`: the table's number.
    pub lines_per_s: f64,
    /// The same window on the bench's clock.
    pub window_s_bench: f64,
    pub stream_lines: u64,
    pub bodies: usize,
    pub acked_bodies: u64,
    /// The sum of the `ingested` fields the senders were given.
    pub acked_lines: u64,
    /// Whether `acked_lines` is `lines`.
    pub counters_agree: bool,
    pub refused: Refused,
    /// Attempts that read no status line and whose body was not all written.
    pub no_answer: u64,
    pub retry_after_max_s: Option<u64>,
    pub reconnects: u64,
    /// Bodies answered 200 with fewer lines taken than sent, and the lines missing.
    pub short_bodies: u64,
    pub short_lines: u64,
    /// Attempts that read no status line after the body was written whole.
    pub unknown_outcome: u64,
    pub senders: Vec<SenderFacts>,
    pub acked_lines_by_second: Vec<u64>,
    pub cpu_s: Option<f64>,
    pub cores_busy: Option<f64>,
    pub resident_bytes_at_end: Option<u64>,
    pub pageins: Option<u64>,
    pub at_end: AtEnd,
    pub health_at_end: Health,
    /// The last `/stats` answer, verbatim, less `storage.note`.
    pub stats_at_end: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Refused {
    #[serde(rename = "503")]
    pub s503: u64,
    #[serde(rename = "429")]
    pub s429: u64,
    /// How many of the two came before the body was all written.
    pub before_body_end: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SenderFacts {
    pub bodies: u64,
    pub lines: u64,
    pub refused: u64,
    pub no_answer: u64,
    pub reconnects: u64,
    pub seconds: f64,
}

/// The server when the ingest window closed, from its last `/stats`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AtEnd {
    pub buffered_rows: Option<u64>,
    pub buffered_bytes: Option<u64>,
    pub max_buffer_bytes: Option<u64>,
    pub forced_closes: Option<u64>,
    /// `forced_closes` above 0: the server is new, so the counter is this window's.
    pub reached_buffer_limit: Option<bool>,
    pub requests_shed: Option<u64>,
    pub lines_rejected: Option<u64>,
    /// Writes to the store that failed in the window; the server acknowledges lines while
    /// its store cannot write, so above 0 the rate is not of rows that were stored.
    pub write_failures: Option<u64>,
    /// Lines the log holds that the store does not yet.
    pub wal_behind: Option<u64>,
    pub templates_before: Option<u64>,
    pub templates_after: Option<u64>,
    pub rows: Option<u64>,
}

/// The idle server: the two counts, the scan block, then a block for each query.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IdleRun {
    pub server: ServerUnit,
    /// Rows that hold `level`: what S2 must look at.
    pub key_rows: u64,
    /// Rows that hold `status`: what the scan block's query must look at.
    pub scan_key_rows: u64,
    #[serde(rename = "SCAN")]
    pub scan: ScanBlock,
    #[serde(flatten)]
    pub queries: Four<IdleBlock>,
    pub health_at_end: Health,
    pub stats_at_end: serde_json::Value,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanBlock {
    pub client_ms: Vec<f64>,
    pub server_ms: Vec<Option<f64>>,
    pub answer: Expect,
    /// Whether every timed answer was the expected one.
    pub answers_identical: bool,
    pub asks: usize,
    /// One ask's, from its `meta`.
    pub rows_scanned: Option<u64>,
    pub row_groups_read: Option<u64>,
    pub files_opened: Option<u64>,
    pub leaves_opened: Option<u64>,
    pub rows_scanned_sum: Option<u64>,
    /// User plus system CPU the server process used across the timed asks.
    pub cpu_s: Option<f64>,
    pub cpu_user_s: Option<f64>,
    pub cpu_system_s: Option<f64>,
    pub wall_s: f64,
    /// CPU the server used in `tail_ms` after the block, with nothing asked.
    pub cpu_tail_s: Option<f64>,
    pub tail_ms: u64,
    pub refused: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IdleBlock {
    /// The bench's clock, from the request's first byte written to the answer's last
    /// byte read.
    pub client_ms: Vec<f64>,
    /// The server's own `duration_ms`.
    pub server_ms: Vec<Option<f64>>,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub server_p50_ms: Option<f64>,
    pub server_p99_ms: Option<f64>,
    pub answer: Expect,
    pub answers_identical: bool,
    pub rows_scanned: Option<u64>,
    pub row_groups_read: Option<u64>,
    pub row_groups_pruned: Option<u64>,
    pub bytes_read: Option<u64>,
    pub files_opened: Option<u64>,
    pub leaves_opened: Option<u64>,
    pub rows_scanned_sum: Option<u64>,
    pub cpu_s: Option<f64>,
    pub cpu_user_s: Option<f64>,
    pub cpu_system_s: Option<f64>,
    pub wall_s: f64,
    pub refused: u64,
}

/// The scan rate of one run, and what stands beside it in the log.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanRun {
    /// `SCAN`: which query the rate was read off.
    pub query: String,
    pub key_rows: u64,
    pub asks: usize,
    /// `key_rows x asks`: the data's count, not the server's.
    pub rows: u64,
    pub cpu_s: Option<f64>,
    /// `rows / cpu_s`, the table's number; `null` where the system reported no CPU time
    /// over the block.
    pub rows_per_cpu_s: Option<f64>,
    pub wall_s: f64,
    /// `cpu_s / wall_s`.
    pub cores_busy: Option<f64>,
    pub rows_per_wall_s: Option<f64>,
    pub cpu_tail_s: Option<f64>,
    pub server_rows_scanned_sum: Option<u64>,
    pub beside: Beside,
}

/// The same rate read off the four queries' own blocks, which is not in the table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Beside {
    #[serde(rename = "S2")]
    pub s2: BesideKeyed,
    #[serde(rename = "S1")]
    pub s1: BesideRate,
    #[serde(rename = "S3")]
    pub s3: BesideRate,
    #[serde(rename = "S4")]
    pub s4: BesideRate,
}

/// S2's block, whose rows are known from the data: `key_rows x asks / cpu_s`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BesideKeyed {
    pub key_rows: u64,
    pub asks: usize,
    pub rows: u64,
    pub cpu_s: Option<f64>,
    pub rows_per_cpu_s: Option<f64>,
    pub leaves_opened: Option<u64>,
}

/// Rows the server says it scanned over a block, over the block's CPU-seconds.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BesideRate {
    pub rows_per_cpu_s: Option<f64>,
}

/// The four queries under ingest: one sample a lap, each lap a server of its own.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadedRun {
    pub laps: usize,
    #[serde(flatten)]
    pub queries: Four<LoadedBlock>,
    pub lap: Laps,
    /// The median over laps of the server's lines over its uptime between two `/stats`.
    pub lines_per_s_median: Option<f64>,
    /// Times a body was refused, over every lap: a body refused twice counts twice.
    pub refused_bodies: u64,
    pub wrapped_laps: u64,
    /// Laps that ended with forced closes above 0.
    pub laps_at_buffer_limit: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadedBlock {
    pub client_ms: Vec<f64>,
    pub server_ms: Vec<Option<f64>>,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
    pub server_p50_ms: Option<f64>,
    pub server_p99_ms: Option<f64>,
    /// Live lines that must be acknowledged before the query is asked.
    pub place_lines: u64,
    /// Live lines acknowledged when it was asked.
    pub at_lines: Vec<u64>,
    /// Live lines acknowledged while it ran: load beside the query, not rows it read.
    pub acked_while_running: Vec<u64>,
    /// Samples asked more than one round of bodies past their place.
    pub late: u64,
    pub count: Vec<u64>,
    /// `count` less the expected count: the live rows the query's snapshot held.
    pub count_over_loaded: Vec<u64>,
    pub count_over_loaded_median: u64,
    pub rows_scanned: Vec<Option<u64>>,
    pub refused: u64,
}

/// One value a lap, in lap order.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Laps {
    /// Lines the server took between the lap's two `/stats` reads.
    pub lines: Vec<u64>,
    /// The server's uptime between them.
    pub seconds: Vec<f64>,
    /// Times the lap's senders were refused a body, not bodies: one that was refused
    /// twice counts twice.
    pub refused_bodies: Vec<u64>,
    /// Times a sender reached the end of its share of the stream and began it again.
    pub wraps: Vec<u64>,
    pub start_s: Vec<f64>,
    pub forced_closes: Vec<Option<u64>>,
    pub buffered_bytes: Vec<Option<u64>>,
    pub requests_shed: Vec<Option<u64>>,
    /// Writes to the store that failed in the lap, and whether its last flush had; a
    /// result saved before these were kept reads with none.
    #[serde(default)]
    pub write_failures: Vec<Option<u64>>,
    #[serde(default)]
    pub degraded: Vec<Option<bool>>,
}

// ── Over the runs ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerRun {
    pub ingest_lines_per_s: Vec<f64>,
    pub idle: Four<PerRunPair>,
    pub loaded: Four<PerRunPair>,
    pub scan_rows_per_cpu_s: Vec<Option<f64>>,
    pub scan_rows_per_wall_s: Vec<Option<f64>>,
    pub scan_cores_busy: Vec<Option<f64>>,
    pub scan_cpu_tail_s: Vec<Option<f64>>,
    /// Each shape's p50, a run.
    pub plan_ms: Shapes<Vec<f64>>,
    pub loaded_count_over_loaded_median: Four<Vec<u64>>,
    pub laps_at_buffer_limit: Vec<Option<u64>>,
    /// The references taken before each run.
    pub spin_ms: Vec<Option<f64>>,
    pub flush_ms: Vec<Option<f64>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PerRunPair {
    pub p50_ms: Vec<f64>,
    pub p99_ms: Vec<f64>,
}

/// The middle of the runs for each number, never a percentile over pooled samples.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Medians {
    pub ingest_lines_per_s: f64,
    pub idle: Four<Pair>,
    pub loaded: Four<Pair>,
    pub scan_rows_per_cpu_s: Option<f64>,
    pub scan_rows_per_wall_s: Option<f64>,
    pub scan_cores_busy: Option<f64>,
    pub scan_cpu_tail_s: Option<f64>,
    pub plan_ms: Shapes<f64>,
    /// P2's: the shape a map from a key to its templates is for.
    pub plan_step_ms: f64,
    pub plan_step_over_1_ms: bool,
    pub loaded_count_over_loaded_median: Four<u64>,
    /// Summed over the runs, not their median: laps out of all the laps.
    pub laps_at_buffer_limit: Option<u64>,
    pub spin_ms: Option<f64>,
    pub flush_ms: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Pair {
    pub p50_ms: f64,
    pub p99_ms: f64,
}

/// The baseline a result was measured against; its ratios are not stored but worked
/// out again from the two files whenever a table is printed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BaselineRef {
    pub file: String,
    pub sha: Option<String>,
    pub label: String,
    /// Whether both were measured by the same source of the bench.
    pub same_bench: bool,
}

// ── Reading and writing ────────────────────────────────────────────────────────────

/// A result as it was read, with the path it came from for the errors that name it.
#[derive(Debug, Clone, PartialEq)]
pub struct Saved {
    pub path: PathBuf,
    pub file: ResultFile,
}

/// The table: its lines in order, and how many of them begin `FAIL`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Table {
    pub lines: Vec<String>,
    pub fails: usize,
}

/// The CRC-32C of the texts in order, with ASCII white space left out of each: what
/// `cargo fmt` moves is not in it, and every other byte is.
fn digest_of(sources: &[(&str, &str)]) -> u32 {
    let mut crc = 0;
    let mut kept = Vec::new();
    for (_, text) in sources {
        kept.clear();
        kept.extend(text.bytes().filter(|byte| !byte.is_ascii_whitespace()));
        crc = crc32c::crc32c_append(crc, &kept);
    }
    crc
}

/// The CRC-32C of `SOURCES` with ASCII white space left out, and how many files it is of.
pub fn source_digest() -> BenchSource {
    BenchSource {
        source_crc32c: crc_hex(digest_of(&SOURCES)),
        source_files: SOURCES.len(),
    }
}

/// Read a saved result, where a file that cannot be read, is not JSON, or is of another
/// schema is `SpeedError::Baseline`, by path.
pub fn read(path: &Path) -> Result<Saved, SpeedError> {
    let refused = |detail: String| SpeedError::Baseline {
        path: path.to_path_buf(),
        detail,
    };
    let text = fs::read_to_string(path).map_err(|e| refused(format!("it cannot be read ({e})")))?;
    let value: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| refused(format!("it is not JSON ({e})")))?;
    // The schema is looked at before the rest is typed, so that a result of a later
    // schema is refused for that and not for the first field it spells another way.
    match value.get("schema").and_then(serde_json::Value::as_str) {
        Some(SCHEMA) => {}
        Some(other) => {
            return Err(refused(format!(
                "its schema is `{other}`, and this build reads `{SCHEMA}`"
            )))
        }
        None => {
            return Err(refused(
                "it has no `schema`, so it is not a result kibbr bench saved".to_string(),
            ))
        }
    }
    let file = serde_json::from_value(value)
        .map_err(|e| refused(format!("it is not a whole result of `{SCHEMA}` ({e})")))?;
    Ok(Saved {
        path: path.to_path_buf(),
        file,
    })
}

/// `path` with `suffix` after its whole name: `baseline.json` and `.tmp` are
/// `baseline.json.tmp`.
fn beside(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().map(OsString::from).unwrap_or_default();
    name.push(suffix);
    path.with_file_name(name)
}

/// Where a result's raw log goes: beside it, `.log` for `.json`, or one more `.log`
/// after a result itself named `.log`, so that the two are never one file.
pub fn log_path(out: &Path) -> PathBuf {
    if out.extension().is_some_and(|extension| extension == "log") {
        beside(out, ".log")
    } else {
        out.with_extension("log")
    }
}

/// The line that says where a result went, the last line of the raw log before the
/// table, so the text given to `write` already holds it.
pub fn saved_line(out: &Path) -> String {
    format!("saved {} and {}", out.display(), log_path(out).display())
}

/// What a result records of the baseline it was measured against.
pub fn baseline_ref(baseline: &Saved, bench: &BenchSource) -> BaselineRef {
    BaselineRef {
        file: baseline.path.display().to_string(),
        sha: baseline.file.commit.sha.clone(),
        label: baseline.file.label.clone(),
        same_bench: baseline.file.bench.source_crc32c == bench.source_crc32c,
    }
}

fn io_at(path: &Path) -> impl FnOnce(io::Error) -> SpeedError + '_ {
    move |source| SpeedError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// Whether something stands at `path`. A place that cannot be looked at is an error and
/// not an empty place: a result must not be written over because a directory was shut.
fn taken(path: &Path) -> Result<bool, SpeedError> {
    path.try_exists().map_err(io_at(path))
}

/// Write `bytes` to a new file at `path` and flush them to the disk. A file of that name
/// is replaced: it is only ever a `.tmp` an interrupted bench left.
fn put(path: &Path, bytes: &[u8]) -> Result<(), SpeedError> {
    let mut file = File::create(path).map_err(io_at(path))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(io_at(path))
}

/// Make the directory a result will be saved in and prove a file can be made there, with
/// a file of its own beside the result's place, before a run is spent.
pub fn writable(out: &Path) -> Result<(), SpeedError> {
    if let Some(dir) = out.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        fs::create_dir_all(dir).map_err(io_at(dir))?;
    }
    let probe = beside(out, ".probe");
    match OpenOptions::new().write(true).create_new(true).open(&probe) {
        Ok(_) => fs::remove_file(&probe).map_err(io_at(&probe)),
        Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
            Err(SpeedError::Host(format!(
                "{} is already there, and the bench makes and removes a file of that name \
                 to prove that it can save its result beside it. Move it away and run \
                 again; the bench removes nothing it did not make",
                probe.display()
            )))
        }
        // By the result's own name: it is the result that cannot be made there.
        Err(source) => Err(SpeedError::Io {
            path: out.to_path_buf(),
            source,
        }),
    }
}

/// A result as it is saved, each field on a line of its own and each run on one line, so
/// that a change between two saved results reads as a change of lines.
pub fn result_text(file: &ResultFile) -> Result<String, SpeedError> {
    let value = serde_json::to_value(file)?;
    let serde_json::Value::Object(fields) = &value else {
        return Ok(serde_json::to_string(&value)?);
    };
    let mut text = String::from("{\n");
    for (at, (key, value)) in fields.iter().enumerate() {
        let comma = if at + 1 < fields.len() { "," } else { "" };
        let key = serde_json::to_string(key)?;
        match value {
            serde_json::Value::Array(runs) if key == "\"runs\"" && !runs.is_empty() => {
                text.push_str(&format!("  {key}: [\n"));
                for (at, run) in runs.iter().enumerate() {
                    let comma = if at + 1 < runs.len() { "," } else { "" };
                    text.push_str(&format!("    {}{comma}\n", serde_json::to_string(run)?));
                }
                text.push_str(&format!("  ]{comma}\n"));
            }
            _ => text.push_str(&format!(
                "  {key}: {}{comma}\n",
                serde_json::to_string(value)?
            )),
        }
    }
    text.push_str("}\n");
    Ok(text)
}

/// Save a result and its raw log through `.tmp` names, writing over nothing; a result
/// that would not read back keeps its `.tmp` name so no later run takes it for a baseline.
pub fn write(out: &Path, file: &ResultFile, log: &str) -> Result<(), SpeedError> {
    let log_to = log_path(out);
    for place in [out, log_to.as_path()] {
        if taken(place)? {
            return Err(SpeedError::OutExists(place.to_path_buf()));
        }
    }
    if let Some(dir) = out.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        fs::create_dir_all(dir).map_err(io_at(dir))?;
    }

    let text = result_text(file)?;
    let result_tmp = beside(out, ".tmp");
    let log_tmp = beside(&log_to, ".tmp");
    put(&result_tmp, text.as_bytes())?;
    put(&log_tmp, log.as_bytes())?;
    if let Err(e) = serde_json::from_str::<ResultFile>(&text) {
        return Err(SpeedError::Baseline {
            path: result_tmp,
            detail: format!(
                "what was measured is kept in this file, with its log beside it, and it \
                 does not read back as a result ({e})"
            ),
        });
    }

    // The log first: a result at its place says that its log is beside it.
    for (tmp, place) in [(&log_tmp, log_to.as_path()), (&result_tmp, out)] {
        if taken(place)? {
            return Err(SpeedError::OutExists(place.to_path_buf()));
        }
        fs::rename(tmp, place).map_err(io_at(place))?;
    }
    Ok(())
}

// ── The middle of the runs ─────────────────────────────────────────────────────────

/// One value a run for each table number, read off the runs' own blocks.
pub fn per_run(runs: &[Run]) -> PerRun {
    fn each<T>(runs: &[Run], of: impl Fn(&Run) -> T) -> Vec<T> {
        runs.iter().map(of).collect()
    }
    let places = [0, 1, 2, 3];
    PerRun {
        ingest_lines_per_s: each(runs, |run| run.ingest.lines_per_s),
        idle: Four::from_array(places.map(|at| PerRunPair {
            p50_ms: each(runs, |run| run.idle.queries.each()[at].p50_ms),
            p99_ms: each(runs, |run| run.idle.queries.each()[at].p99_ms),
        })),
        loaded: Four::from_array(places.map(|at| PerRunPair {
            p50_ms: each(runs, |run| run.loaded.queries.each()[at].p50_ms),
            p99_ms: each(runs, |run| run.loaded.queries.each()[at].p99_ms),
        })),
        scan_rows_per_cpu_s: each(runs, |run| run.scan.rows_per_cpu_s),
        scan_rows_per_wall_s: each(runs, |run| run.scan.rows_per_wall_s),
        scan_cores_busy: each(runs, |run| run.scan.cores_busy),
        scan_cpu_tail_s: each(runs, |run| run.scan.cpu_tail_s),
        plan_ms: Shapes::from_array(
            places.map(|at| each(runs, |run| run.plan.shapes.each()[at].p50_ms)),
        ),
        loaded_count_over_loaded_median: Four::from_array(places.map(|at| {
            each(runs, |run| {
                run.loaded.queries.each()[at].count_over_loaded_median
            })
        })),
        laps_at_buffer_limit: each(runs, |run| run.loaded.laps_at_buffer_limit),
        spin_ms: each(runs, |run| run.checks.spin_ms_before),
        flush_ms: each(runs, |run| run.checks.flush_ms_before),
    }
}

/// The values of a list that has one for every run, or `None`: the middle of the runs
/// that happened to give a number is not the middle of the runs.
fn every<T: Copy>(values: &[Option<T>]) -> Option<Vec<T>> {
    values.iter().copied().collect()
}

/// The middle of each of `per_run`'s lists, which `flags.runs` being odd makes one of
/// them; a number that one run could not give is `None`.
pub fn medians(per_run: &PerRun) -> Medians {
    let middle = |values: &[f64]| median(values).unwrap_or_default();
    let middle_of_every = |values: &[Option<f64>]| every(values).and_then(|all| median(&all));
    let pair = |pair: &PerRunPair| Pair {
        p50_ms: middle(&pair.p50_ms),
        p99_ms: middle(&pair.p99_ms),
    };
    let plan_ms = per_run.plan_ms.map(|values| middle(values));
    Medians {
        ingest_lines_per_s: middle(&per_run.ingest_lines_per_s),
        idle: per_run.idle.map(pair),
        loaded: per_run.loaded.map(pair),
        scan_rows_per_cpu_s: middle_of_every(&per_run.scan_rows_per_cpu_s),
        scan_rows_per_wall_s: middle_of_every(&per_run.scan_rows_per_wall_s),
        scan_cores_busy: middle_of_every(&per_run.scan_cores_busy),
        scan_cpu_tail_s: middle_of_every(&per_run.scan_cpu_tail_s),
        plan_step_ms: plan_ms.p2,
        plan_step_over_1_ms: plan_ms.p2 > 1.0,
        plan_ms,
        loaded_count_over_loaded_median: per_run
            .loaded_count_over_loaded_median
            .map(|values| median(values).unwrap_or_default()),
        laps_at_buffer_limit: every(&per_run.laps_at_buffer_limit)
            .filter(|all| !all.is_empty())
            .map(|all| all.iter().sum()),
        spin_ms: middle_of_every(&per_run.spin_ms),
        flush_ms: middle_of_every(&per_run.flush_ms),
    }
}

/// The speed-up of a rate on a baseline's: `this / baseline`, above 1 when faster, and
/// `None` when the baseline's is 0.
pub fn speedup_of_rate(this: f64, baseline: f64) -> Option<f64> {
    quotient(this, baseline)
}

/// The speed-up of a time on a baseline's: `baseline / this`, above 1 when faster, and
/// `None` when this one is 0.
pub fn speedup_of_time(this: f64, baseline: f64) -> Option<f64> {
    quotient(baseline, this)
}

/// `over / under`, where that is a number: nothing is divided by 0, and a quotient that
/// is not finite is not printed as one.
fn quotient(over: f64, under: f64) -> Option<f64> {
    let ratio = over / under;
    (under != 0.0 && ratio.is_finite()).then_some(ratio)
}

/// The smallest and the largest of `values`.
fn range(values: &[f64]) -> Option<(f64, f64)> {
    let first = *values.first()?;
    Some(values.iter().fold((first, first), |(least, most), value| {
        (least.min(*value), most.max(*value))
    }))
}

/// Whether the smallest to the largest of `a` and the same of `b` share a value, ends
/// included: a ratio between two such sets of runs is not a difference.
pub fn overlap(a: &[f64], b: &[f64]) -> bool {
    match (range(a), range(b)) {
        (Some((a_least, a_most)), Some((b_least, b_most))) => {
            a_least <= b_most && b_least <= a_most
        }
        _ => false,
    }
}

// ── The same work ──────────────────────────────────────────────────────────────────

/// A value as a difference quotes it: its JSON, so that a text keeps its quotes and a
/// number that was not there reads `null`.
fn quoted<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "a value that is not JSON".to_string())
}

/// Note `key` where the two differ, with both values. `who` is the other side as the
/// sentence names it: `this run`, or a file.
fn differ<T: PartialEq + Serialize>(
    found: &mut Vec<String>,
    key: &str,
    base: &T,
    this: &T,
    who: &str,
) {
    if base != this {
        found.push(format!(
            "{key}: {} in the baseline, {} in {who}",
            quoted(base),
            quoted(this)
        ));
    }
}

/// What must be equal before the data is looked at.
fn work_differences(
    base: &ResultFile,
    flags: &Flags,
    machine: &Machine,
    server: &ServerFacts,
    plan_queries: &Shapes<QueryText>,
    who: &str,
) -> Vec<String> {
    let mut found = Vec::new();
    differ(&mut found, "schema", &base.schema.as_str(), &SCHEMA, who);
    let was = &base.flags;
    differ(&mut found, "flags.lines", &was.lines, &flags.lines, who);
    differ(
        &mut found,
        "flags.profile",
        &was.profile,
        &flags.profile,
        who,
    );
    differ(&mut found, "flags.runs", &was.runs, &flags.runs, who);
    differ(
        &mut found,
        "flags.samples",
        &was.samples,
        &flags.samples,
        who,
    );
    differ(
        &mut found,
        "flags.body_lines",
        &was.body_lines,
        &flags.body_lines,
        who,
    );
    differ(
        &mut found,
        "flags.senders",
        &was.senders,
        &flags.senders,
        who,
    );
    differ(
        &mut found,
        "flags.load_at",
        &was.load_at,
        &flags.load_at,
        who,
    );
    differ(&mut found, "flags.warmup", &was.warmup, &flags.warmup, who);
    differ(
        &mut found,
        "flags.plan_warmup",
        &was.plan_warmup,
        &flags.plan_warmup,
        who,
    );
    differ(
        &mut found,
        "flags.scan_asks",
        &was.scan_asks,
        &flags.scan_asks,
        who,
    );
    differ(
        &mut found,
        "flags.received_at",
        &was.received_at,
        &flags.received_at,
        who,
    );
    for ((name, was), now) in Shapes::<()>::NAMES
        .iter()
        .zip(base.plan_queries.each())
        .zip(plan_queries.each())
    {
        differ(
            &mut found,
            &format!("plan_queries.{name}.text"),
            &was.text,
            &now.text,
            who,
        );
    }
    differ(
        &mut found,
        "server.argv",
        &base.server.argv.join(" "),
        &server.argv.join(" "),
        who,
    );
    differ(
        &mut found,
        "machine.cpu",
        &base.machine.cpu,
        &machine.cpu,
        who,
    );
    differ(
        &mut found,
        "machine.cores_logical",
        &base.machine.cores_logical,
        &machine.cores_logical,
        who,
    );
    differ(
        &mut found,
        "machine.memory_bytes",
        &base.machine.memory_bytes,
        &machine.memory_bytes,
        who,
    );
    found
}

/// How many differing files a refusal names before it counts the rest.
const FILES_NAMED: usize = 3;

/// The loaded files that differ, each by path with both lengths and digests.
fn file_differences(base: &[FileDigest], this: &[FileDigest], who: &str) -> Option<String> {
    if base == this {
        return None;
    }
    let mut said = Vec::new();
    for was in base {
        match this.iter().find(|now| now.path == was.path) {
            None => said.push(format!("{} is in the baseline and not in {who}", was.path)),
            Some(now) if now != was => said.push(format!(
                "{} is {} bytes, crc32c {} in the baseline and {} bytes, crc32c {} in {who}",
                was.path,
                commas(was.bytes),
                was.crc32c,
                commas(now.bytes),
                now.crc32c
            )),
            Some(_) => {}
        }
    }
    for now in this {
        if !base.iter().any(|was| was.path == now.path) {
            said.push(format!("{} is in {who} and not in the baseline", now.path));
        }
    }
    if said.is_empty() {
        said.push(format!("the same files, listed in another order in {who}"));
    }
    let more = said.len().saturating_sub(FILES_NAMED);
    said.truncate(FILES_NAMED);
    let mut text = format!("data.files: {}", said.join(", "));
    if more > 0 {
        text.push_str(&format!(", and {} more", commas(more as u64)));
    }
    Some(text)
}

/// What must be equal of the bytes and the texts.
fn data_differences(
    base: &ResultFile,
    data: &Data,
    queries: &Seven<QueryText>,
    who: &str,
) -> Vec<String> {
    let mut found = Vec::new();
    found.extend(file_differences(&base.data.files, &data.files, who));
    differ(
        &mut found,
        "data.live_stream.crc32c",
        &base.data.live_stream.crc32c,
        &data.live_stream.crc32c,
        who,
    );
    for ((name, was), now) in Seven::<()>::NAMES
        .iter()
        .zip(base.queries.each())
        .zip(queries.each())
    {
        differ(
            &mut found,
            &format!("queries.{name}.text"),
            &was.text,
            &now.text,
            who,
        );
    }
    found
}

/// A `FAIL` sentence for each query that answers otherwise than the baseline's did. Only
/// asked once the bytes and the texts are the same: a faster wrong answer is not a column.
fn answer_failures(base: &Seven<Expect>, this: &Seven<Expect>) -> Vec<String> {
    const WHY: &str = "the same query over the same bytes has another answer";
    let mut found = Vec::new();
    for ((name, was), now) in Seven::<()>::NAMES.iter().zip(base.each()).zip(this.each()) {
        if was == now {
            continue;
        }
        if was.count != now.count {
            found.push(format!(
                "FAIL {name} counts {} where the baseline counted {}: {WHY}",
                commas(now.count),
                commas(was.count)
            ));
            continue;
        }
        let mut fields = Vec::new();
        let mut field = |key: &str, was: String, now: String| {
            if was != now {
                fields.push(format!("{key} {now} where the baseline's was {was}"));
            }
        };
        field(
            "rows_returned",
            quoted(&was.rows_returned),
            quoted(&now.rows_returned),
        );
        field(
            "templates_matched",
            quoted(&was.templates_matched),
            quoted(&now.templates_matched),
        );
        field("truncated", quoted(&was.truncated), quoted(&now.truncated));
        field(
            "limit_source",
            quoted(&was.limit_source),
            quoted(&now.limit_source),
        );
        found.push(format!("FAIL {name} answers {}: {WHY}", fields.join(", ")));
    }
    found
}

fn refuse(baseline: &Saved, found: Vec<String>) -> Result<(), SpeedError> {
    if found.is_empty() {
        return Ok(());
    }
    Err(SpeedError::Baseline {
        path: baseline.path.clone(),
        detail: found.join("; "),
    })
}

/// What can be compared with a baseline before the data is looked at - schema, flags,
/// plan texts, server arguments, machine - where a difference is `SpeedError::Baseline`.
pub fn comparable_before(
    baseline: &Saved,
    flags: &Flags,
    machine: &Machine,
    server: &ServerFacts,
    plan_queries: &Shapes<QueryText>,
) -> Result<(), SpeedError> {
    let found = work_differences(
        &baseline.file,
        flags,
        machine,
        server,
        plan_queries,
        "this run",
    );
    refuse(baseline, found)
}

/// What can be compared once the preflight has run: the loaded files, the live stream's
/// digest, the seven texts, and each expected answer, where another answer is a `FAIL`.
pub fn comparable_data(
    baseline: &Saved,
    data: &Data,
    queries: &Seven<QueryText>,
    expect: &Seven<Expect>,
) -> Result<(), SpeedError> {
    let mut found = data_differences(&baseline.file, data, queries, "this run");
    if found.is_empty() {
        found = answer_failures(&baseline.file.expect, expect);
    }
    refuse(baseline, found)
}

// ── How a number is printed ────────────────────────────────────────────────────────

/// Milliseconds as the log and the table print them, with more decimals the smaller the
/// value, bounded where a shorter form would round up into the next one.
pub fn ms(value: f64) -> String {
    if value > 0.0 && value < 0.0995 {
        format!("{value:.4}")
    } else if value < 9.995 {
        format!("{value:.2}")
    } else if value < 999.95 {
        format!("{value:.1}")
    } else {
        commas(value.round() as u64)
    }
}

/// A rate, to the whole unit, thousands separated.
fn rate(value: f64) -> String {
    commas(value.round() as u64)
}

/// A speed-up: two decimals while it rounds to under 1.1, where the second is all there
/// is to read, and one from there up.
fn times(speedup: f64) -> String {
    if speedup < 1.095 {
        format!("{speedup:.2}x")
    } else {
        format!("{speedup:.1}x")
    }
}

/// A speed-up, or `n/a` where one side gave nothing to divide by.
fn times_or_na(speedup: Option<f64>) -> String {
    speedup.map_or_else(|| NA.to_string(), times)
}

/// What stands where a number could not be given.
const NA: &str = "n/a";

fn or_na<T>(value: Option<T>, print: impl FnOnce(T) -> String) -> String {
    value.map_or_else(|| NA.to_string(), print)
}

/// `1 lap`, `2 laps`: a count with its noun.
fn counted(count: u64, one: &str, many: &str) -> String {
    format!("{} {}", commas(count), if count == 1 { one } else { many })
}

/// A width in characters, as `{:<width$}` counts it.
fn width(text: &str) -> usize {
    text.chars().count()
}

/// The first twelve characters of a commit's sha, as every line names a commit.
fn sha12(commit: &Commit) -> String {
    commit.sha.as_deref().map_or_else(
        || "unknown".to_string(),
        |sha| sha.chars().take(12).collect(),
    )
}

/// The variables a server inherited and may read, as a line names them: `NAME=value`
/// for each, or `none`.
fn noted(env: &BTreeMap<String, String>) -> String {
    if env.is_empty() {
        return "none".to_string();
    }
    env.iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The key a text such as `COUNT LOG {"status": *}` asks for.
fn key_of(text: &str) -> &str {
    text.split('"').nth(1).unwrap_or(text)
}

// ── The table ──────────────────────────────────────────────────────────────────────

/// Under this many samples the p99 is the largest of them, and the table says so.
const P99_SAMPLES: usize = 100;
/// Spaces between the widest row label and the first column.
const LABEL_GAP: usize = 2;
/// A column is this much wider than its widest cell, and never narrower than
/// `COLUMN_MIN`.
const COLUMN_GAP: usize = 3;
const COLUMN_MIN: usize = 22;
/// Spaces between two columns' cells in the plan row, which is too wide for the columns.
const PLAN_GAP: usize = 6;
/// Spaces between the widest line of loaded p99s and the label that says whose it is.
const LOADED_GAP: usize = 4;
/// Spaces between the widest label and what a line under the table says of its column.
const HEAD_GAP: usize = 3;
/// How far two medians of the spin reference may be apart, as a share of the baseline's.
/// A run's own two readings are held to the same share, of the one taken before it.
const SPIN_APART: f64 = 0.05;
/// The same for the flush reference, which moves more between two identical tries.
const FLUSH_APART: f64 = 0.15;
/// CPU-seconds after the scan block above which its workers are held not to sleep: a
/// tenth of a core for the 100 ms the server is left alone.
const TAIL_BUSY_S: f64 = 0.01;

const LEGEND: &str = "(Nx) is the speed-up on the baseline: this / baseline for the two \
                      rates, baseline / this for every time; above 1 is faster.";

/// The eight row labels, in the order the table prints them: ingest, the four queries,
/// the scan rate, the plan step, the four queries under ingest.
fn row_labels(base: &ResultFile) -> [String; 8] {
    let [s1, s2, s3, s4] = Four::<()>::NAMES;
    let queries = base.queries.four();
    let query = |name: &str, query: &QueryText| format!("{name} {:<17}p50 / p99", query.label);
    [
        "ingest lines/s".to_string(),
        query(s1, queries.s1),
        query(s2, queries.s2),
        query(s3, queries.s3),
        query(s4, queries.s4),
        "scan rows/s/core".to_string(),
        format!(
            "plan step at {} templates:",
            commas(base.plan_trie.templates)
        ),
        format!(
            "same four queries under {}-sender ingest, p99:",
            base.flags.senders
        ),
    ]
}

/// ` (1.01x)` after a number of a later column; nothing after the baseline's own.
fn after(speedup: Option<Option<f64>>) -> String {
    speedup.map_or_else(String::new, |speedup| {
        format!(" ({})", times_or_na(speedup))
    })
}

/// One file's cells for the six rows that stand in columns: ingest, the four idle pairs,
/// the scan rate. `base` is `None` for the baseline's own column.
fn column(file: &ResultFile, base: Option<&ResultFile>) -> [String; 6] {
    let now = &file.medians;
    let ingest = format!(
        "{}{}",
        rate(now.ingest_lines_per_s),
        after(base.map(|base| {
            speedup_of_rate(now.ingest_lines_per_s, base.medians.ingest_lines_per_s)
        }))
    );
    let pair = |at: usize| {
        let pair = now.idle.each()[at];
        let mut cell = format!("{} / {} ms", ms(pair.p50_ms), ms(pair.p99_ms));
        if let Some(base) = base {
            let was = base.medians.idle.each()[at];
            cell.push_str(&format!(
                " ({} / {})",
                times_or_na(speedup_of_time(pair.p50_ms, was.p50_ms)),
                times_or_na(speedup_of_time(pair.p99_ms, was.p99_ms))
            ));
        }
        cell
    };
    let scan = match now.scan_rows_per_cpu_s {
        None => NA.to_string(),
        Some(rows) => format!(
            "{}{}",
            rate(rows),
            after(base.map(|base| {
                base.medians
                    .scan_rows_per_cpu_s
                    .and_then(|was| speedup_of_rate(rows, was))
            }))
        ),
    };
    [ingest, pair(0), pair(1), pair(2), pair(3), scan]
}

/// One file's cell of the plan row.
fn plan_cell(file: &ResultFile, base: Option<&ResultFile>) -> String {
    let now = &file.medians;
    format!(
        "{} ms{}   over 1 ms: {}",
        ms(now.plan_step_ms),
        after(base.map(|base| speedup_of_time(now.plan_step_ms, base.medians.plan_step_ms))),
        if now.plan_step_over_1_ms { "yes" } else { "no" }
    )
}

/// One file's four p99s under ingest, on one line.
fn loaded_cell(file: &ResultFile, base: Option<&ResultFile>) -> String {
    let mut cells = Vec::with_capacity(4);
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        let p99 = file.medians.loaded.each()[at].p99_ms;
        let speedup = base.map(|base| speedup_of_time(p99, base.medians.loaded.each()[at].p99_ms));
        cells.push(format!("{name} {}{}", ms(p99), after(speedup)));
    }
    format!("{} ms", cells.join("  "))
}

/// The line that is always under the rows: the scan block by the wall clock with the
/// cores it kept busy, where a spread scan shows, which a rate per CPU-second cannot.
fn scan_note(base: &ResultFile, others: &[&ResultFile]) -> String {
    let wall = |file: &ResultFile| or_na(file.medians.scan_rows_per_wall_s, rate);
    let cores = |file: &ResultFile| or_na(file.medians.scan_cores_busy, |c| format!("{c:.2}"));
    let mut note = format!(
        "scan rows/s/core is rows holding `{}` x {} asks of {} / CPU-seconds of the server \
         process. By the wall clock: ",
        key_of(&base.queries.scan_key.text),
        base.flags.scan_asks,
        base.queries.scan.text
    );
    if others.is_empty() {
        note.push_str(&format!("{} rows/s on {} cores.", wall(base), cores(base)));
        return note;
    }
    note.push_str(&format!(
        "{} {} rows/s on {} cores",
        base.label,
        wall(base),
        cores(base)
    ));
    for other in others {
        note.push_str(&format!(
            "; {} {} on {}",
            other.label,
            wall(other),
            cores(other)
        ));
        if let (Some(now), Some(was)) = (
            other.medians.scan_rows_per_wall_s,
            base.medians.scan_rows_per_wall_s,
        ) {
            note.push_str(&after(Some(speedup_of_rate(now, was))));
        }
    }
    note.push('.');
    note
}

/// Laps of every run together.
fn laps(file: &ResultFile) -> u64 {
    file.runs.iter().map(|run| run.loaded.laps as u64).sum()
}

/// Laps whose server said a write to its store had failed, or that its last flush had.
/// The count is the surer of the two: the flag is cleared by the next flush that writes.
fn laps_that_could_not_write(laps: &Laps) -> u64 {
    let failed = |at: usize| {
        let failures = laps.write_failures.get(at).copied().flatten();
        let degraded = laps.degraded.get(at).copied().flatten();
        failures.is_some_and(|failures| failures > 0) || degraded == Some(true)
    };
    let said = laps.write_failures.len().max(laps.degraded.len());
    (0..said).filter(|at| failed(*at)).count() as u64
}

/// A server that did not say `ok` when its phase ended, as a line under the table.
fn unwell(phase: &str, run: usize, health: &Health) -> Option<String> {
    if health.status == "ok" {
        return None;
    }
    Some(format!(
        "{phase}: the server was {} at the end of run {run}{}",
        health.status,
        health
            .detail
            .as_deref()
            .map_or_else(String::new, |detail| format!(" ({detail})"))
    ))
}

/// A power state as a line names it: `on AC Power with Low Power Mode off`.
fn power_words(power: &Power) -> String {
    format!(
        "on {} with Low Power Mode {}",
        or_na(power.source.clone(), |source| source),
        match power.low_power_mode {
            Some(true) => "on",
            Some(false) => "off",
            None => NA,
        }
    )
}

/// A power state as a line of the raw log carries it beside the references.
fn power_part(power: Option<&Power>) -> String {
    power.map_or_else(|| format!("power {NA}"), power_words)
}

/// `run 2`, `runs 1 and 3`, `runs 1, 2 and 3`.
fn runs_named(runs: &[usize]) -> String {
    let each: Vec<String> = runs.iter().map(usize::to_string).collect();
    match each.split_last() {
        Some((last, rest)) if !rest.is_empty() => {
            format!("runs {} and {last}", rest.join(", "))
        }
        _ => format!("run {}", each.join("")),
    }
}

/// What a run began or ended as, where either was not mains power with Low Power Mode
/// off. A reading that was on mains, or was not taken, is not named.
fn off_mains(checks: &Checks) -> Option<String> {
    let words = |power: &Option<Power>| {
        power
            .as_ref()
            .filter(|power| power.off_mains())
            .map(power_words)
    };
    match (words(&checks.power_before), words(&checks.power_after)) {
        (Some(began), Some(ended)) if began == ended => Some(format!("began and ended {began}")),
        (Some(began), Some(ended)) => Some(format!("began {began} and ended {ended}")),
        (Some(began), None) => Some(format!("began {began}")),
        (None, Some(ended)) => Some(format!("ended {ended}")),
        (None, None) => None,
    }
}

/// A line for the runs that began or ended on a battery or in Low Power Mode. Nothing is
/// refused for it: only this line says the numbers are a slower machine's.
fn off_mains_lines(runs: &[Run]) -> Vec<String> {
    let mut said: Vec<(String, Vec<usize>)> = Vec::new();
    for run in runs {
        let Some(how) = off_mains(&run.checks) else {
            continue;
        };
        match said.iter_mut().find(|(known, _)| *known == how) {
            Some((_, named)) => named.push(run.run),
            None => said.push((how, vec![run.run])),
        }
    }
    said.iter()
        .map(|(how, named)| {
            format!(
                "{} {how}: the numbers are not the machine's on mains with Low Power Mode off",
                runs_named(named)
            )
        })
        .collect()
}

/// A line for a run whose two readings of the spin reference are too far apart: only the
/// first is set against a baseline's, so a machine that changed part way would not show.
fn spin_moved(run: &Run) -> Option<String> {
    let (before, after) = (run.checks.spin_ms_before?, run.checks.spin_ms_after?);
    apart(after, before, SPIN_APART).then(|| {
        format!(
            "spin {} ms before run {} and {} ms after it: the machine changed state during \
             the run",
            ms(before),
            run.run,
            ms(after)
        )
    })
}

/// The lines that are true of one result whatever it is compared with.
fn own_lines(file: &ResultFile) -> Vec<String> {
    let mut lines = Vec::new();
    if !file.flags.checked {
        lines.push("UNCHECKED - not a measurement".to_string());
    }
    if file.binary.build == "debug" {
        lines.push("debug build - not a measurement".to_string());
    }
    if file.binary.stale == Some(true) {
        lines.push(
            "the commit is the checkout's; the binary was built before it was committed"
                .to_string(),
        );
    }
    if file.commit.src_dirty == Some(true) {
        lines.push(
            "the commit is the checkout's; its source differed from the commit when this \
             ran, so the commit does not name the code measured"
                .to_string(),
        );
    }
    lines.extend(off_mains_lines(&file.runs));
    lines.extend(file.runs.iter().filter_map(spin_moved));
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        let blocks = || file.runs.iter().map(|run| run.loaded.queries.each()[at]);
        let late: u64 = blocks().map(|block| block.late).sum();
        let samples: usize = blocks().map(|block| block.client_ms.len()).sum();
        if late > 0 {
            lines.push(format!(
                "{name} under ingest: {} of {} samples started late",
                commas(late),
                commas(samples as u64)
            ));
        }
    }
    let wrapped: u64 = file.runs.iter().map(|run| run.loaded.wrapped_laps).sum();
    if wrapped > 0 {
        lines.push(format!(
            "{} of {} laps ran past the end of the stream",
            commas(wrapped),
            commas(laps(file))
        ));
    }
    let refused: u64 = file
        .runs
        .iter()
        .map(|run| run.ingest.refused.s503 + run.ingest.refused.s429)
        .sum();
    // A refused body is sent again, so these count refusals and not bodies: one body the
    // server sheds for a second is twenty of them.
    if refused > 0 {
        lines.push(format!(
            "bodies were refused {} in the ingest phase, each followed by a {} ms pause \
             inside the window",
            counted(refused, "time", "times"),
            file.flags.refusal_pause_ms
        ));
    }
    let refused: u64 = file.runs.iter().map(|run| run.loaded.refused_bodies).sum();
    if refused > 0 {
        lines.push(format!(
            "bodies were refused {} in the laps under ingest",
            counted(refused, "time", "times")
        ));
    }
    // A server whose store cannot write goes on acknowledging lines, so its rate and its
    // samples are numbers like any other: these lines are all that tells them apart.
    for run in &file.runs {
        lines.extend(unwell("ingest", run.run, &run.ingest.health_at_end));
        if let Some(failures) = run.ingest.at_end.write_failures.filter(|n| *n > 0) {
            lines.push(format!(
                "ingest: {} to the server's store failed in run {}: it acknowledged lines \
                 while no row reached a file",
                counted(failures, "write", "writes"),
                run.run
            ));
        }
    }
    let unwritten: u64 = file
        .runs
        .iter()
        .map(|run| laps_that_could_not_write(&run.loaded.lap))
        .sum();
    if unwritten > 0 {
        lines.push(format!(
            "{} of {} laps ran on a store that could not write",
            commas(unwritten),
            commas(laps(file))
        ));
    }
    for run in &file.runs {
        lines.extend(unwell("idle", run.run, &run.idle.health_at_end));
    }
    // The same of a query: an ask refused twice before it was answered counts twice.
    let refused: u64 = file.runs.iter().map(|run| run.idle.scan.refused).sum();
    if refused > 0 {
        lines.push(format!(
            "scan: asks of the block were refused {} and sent again; the wait is in its \
             wall clock",
            counted(refused, "time", "times")
        ));
    }
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        let idle: u64 = file
            .runs
            .iter()
            .map(|run| run.idle.queries.each()[at].refused)
            .sum();
        if idle > 0 {
            lines.push(format!(
                "{name}: asks of the idle server were refused {} and sent again; the wait \
                 is in their samples",
                counted(idle, "time", "times")
            ));
        }
        let loaded: u64 = file
            .runs
            .iter()
            .map(|run| run.loaded.queries.each()[at].refused)
            .sum();
        if loaded > 0 {
            lines.push(format!(
                "{name} under ingest: asks were refused {} and sent again; the wait is in \
                 their samples",
                counted(loaded, "time", "times")
            ));
        }
    }
    let voided: usize = file.runs.iter().map(|run| run.voided.len()).sum();
    if voided > 0 {
        lines.push(format!(
            "{} disturbed, thrown away and done again",
            counted(voided as u64, "unit was", "units were")
        ));
    }
    if let Some(tail) = file
        .medians
        .scan_cpu_tail_s
        .filter(|tail| *tail > TAIL_BUSY_S)
    {
        let tail_ms = file
            .runs
            .first()
            .map_or(SCAN_TAIL.as_millis() as u64, |run| run.idle.scan.tail_ms);
        lines.push(format!(
            "scan: the server used {tail:.3} s of CPU in the {tail_ms} ms after the block - \
             workers that do not sleep are inside the scan row"
        ));
    }
    if file.medians.scan_rows_per_cpu_s.is_none() {
        if let Some(run) = file
            .runs
            .iter()
            .find(|run| run.scan.rows_per_cpu_s.is_none())
        {
            let why = if run.scan.cpu_s.is_none() {
                "the system gave no CPU time for the server process"
            } else {
                "the system reported no CPU time over the scan block"
            };
            lines.push(format!(
                "scan rows/s/core is {NA}: {why} in run {}",
                run.run
            ));
        }
    }
    lines
}

/// With no baseline to set it against: the laps that ended past the buffer limit.
fn buffer_limit_alone(file: &ResultFile) -> Option<String> {
    let reached = file.medians.laps_at_buffer_limit.filter(|laps| *laps > 0)?;
    Some(format!(
        "S4 under ingest: the buffer limit was reached in {} of {} laps",
        commas(reached),
        commas(laps(file))
    ))
}

/// The lines under a table of one result: everything that is true of it and is not one of
/// its numbers. What a result keeps under `notes`.
pub fn notes(file: &ResultFile) -> Vec<String> {
    let mut lines = Vec::new();
    if file.flags.samples < P99_SAMPLES {
        lines.push(format!("p99 of {} samples", file.flags.samples));
    }
    lines.extend(own_lines(file));
    lines.extend(buffer_limit_alone(file));
    lines
}

/// One line for a printed ratio whose runs overlap the baseline's, with both ranges.
fn overlap_line(name: &str, now: &[f64], was: &[f64], in_ms: bool) -> Option<String> {
    if !overlap(now, was) {
        return None;
    }
    let (now_least, now_most) = range(now)?;
    let (was_least, was_most) = range(was)?;
    let (print, unit): (fn(f64) -> String, &str) = if in_ms { (ms, " ms") } else { (rate, "") };
    Some(format!(
        "{name}: the runs overlap ({} to {}{unit} against the baseline's {} to {}) - not a \
         difference",
        print(now_least),
        print(now_most),
        print(was_least),
        print(was_most)
    ))
}

/// A line for each ratio the table prints for `file` whose runs overlap the baseline's,
/// in the rows' order.
fn overlap_lines(base: &ResultFile, file: &ResultFile) -> Vec<String> {
    let (was, now) = (&base.per_run, &file.per_run);
    let mut lines = Vec::new();
    lines.extend(overlap_line(
        "ingest lines/s",
        &now.ingest_lines_per_s,
        &was.ingest_lines_per_s,
        false,
    ));
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        let (now, was) = (now.idle.each()[at], was.idle.each()[at]);
        lines.extend(overlap_line(
            &format!("{name} p50"),
            &now.p50_ms,
            &was.p50_ms,
            true,
        ));
        lines.extend(overlap_line(
            &format!("{name} p99"),
            &now.p99_ms,
            &was.p99_ms,
            true,
        ));
    }
    if let (Some(now), Some(was)) = (
        every(&now.scan_rows_per_cpu_s),
        every(&was.scan_rows_per_cpu_s),
    ) {
        lines.extend(overlap_line("scan rows/s/core", &now, &was, false));
    }
    lines.extend(overlap_line(
        "plan step",
        &now.plan_ms.p2,
        &was.plan_ms.p2,
        true,
    ));
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        lines.extend(overlap_line(
            &format!("{name} under ingest p99"),
            &now.loaded.each()[at].p99_ms,
            &was.loaded.each()[at].p99_ms,
            true,
        ));
    }
    if let (Some(now), Some(was)) = (
        every(&now.scan_rows_per_wall_s),
        every(&was.scan_rows_per_wall_s),
    ) {
        lines.extend(overlap_line("scan by the wall clock", &now, &was, false));
    }
    lines
}

/// Whether two references are further apart than `share` of the baseline's.
fn apart(now: f64, was: f64, share: f64) -> bool {
    (now - was).abs() > share * was
}

/// The power states a result's runs went through, in the order they were read, each
/// named once for as long as it lasted. `None` for a result with no reading.
fn power_of(runs: &[Run]) -> Option<String> {
    let mut states: Vec<String> = powers(runs).map(power_words).collect();
    states.dedup();
    (!states.is_empty()).then(|| states.join(", then "))
}

/// Every reading of the power state a result's runs took, in the order they were taken.
fn powers(runs: &[Run]) -> impl Iterator<Item = &Power> {
    runs.iter()
        .flat_map(|run| [&run.checks.power_before, &run.checks.power_after])
        .flatten()
}

/// What one half of the power state went through in a result, over the readings that
/// have that half, each value once for as long as it lasted.
fn power_half<'a, T: PartialEq>(runs: &'a [Run], half: impl Fn(&'a Power) -> Option<T>) -> Vec<T> {
    let mut states: Vec<T> = powers(runs).filter_map(half).collect();
    states.dedup();
    states
}

/// Whether two results are known to have been taken in different power states. Each half
/// is set against the same half, so a part the system did not give is not held against it.
fn power_differs(base: &[Run], file: &[Run]) -> bool {
    fn differ<T: PartialEq>(was: &[T], now: &[T]) -> bool {
        !was.is_empty() && !now.is_empty() && was != now
    }
    let source = |power: &Power| power.source.clone();
    let mode = |power: &Power| power.low_power_mode;
    differ(&power_half(base, source), &power_half(file, source))
        || differ(&power_half(base, mode), &power_half(file, mode))
}

/// The lines that are true of `file` set against the baseline and do not stop the
/// comparison: each says what was not the same, with both numbers.
fn against_lines(base: &ResultFile, file: &ResultFile) -> Vec<String> {
    let mut lines = Vec::new();
    if base.bench.source_crc32c != file.bench.source_crc32c {
        lines.push(format!(
            "NOT THE SAME BENCH: the bench's own source differs from the baseline's (crc32c \
             {}, {}) - these ratios compare two instruments",
            base.bench.source_crc32c, file.bench.source_crc32c
        ));
    }
    if base.machine.os_version != file.machine.os_version {
        let version = |machine: &Machine| or_na(machine.os_version.clone(), |version| version);
        lines.push(format!(
            "the system is {} {} where the baseline's was {}",
            file.machine.os,
            version(&file.machine),
            version(&base.machine)
        ));
    }
    // These reach every server and are in no argument: a pool's width set in the shell
    // of one result and not of the other is two server settings under one command line.
    if base.server.env_noted != file.server.env_noted {
        lines.push(format!(
            "the server's environment held {} where the baseline's held {} - not the same \
             server settings",
            noted(&file.server.env_noted),
            noted(&base.server.env_noted)
        ));
    }
    if let (Some(was), Some(now)) = (
        base.medians.laps_at_buffer_limit,
        file.medians.laps_at_buffer_limit,
    ) {
        if was != now {
            lines.push(format!(
                "S4 under ingest: the buffer limit was reached in {} of {} baseline laps and \
                 {} of {} here",
                commas(was),
                commas(laps(base)),
                commas(now),
                commas(laps(file))
            ));
        }
    }
    let round = (base.flags.senders * base.flags.body_lines) as u64;
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        let was = *base.medians.loaded_count_over_loaded_median.each()[at];
        let now = *file.medians.loaded_count_over_loaded_median.each()[at];
        if was.abs_diff(now) > round {
            lines.push(format!(
                "{name} under ingest: its answers held a median of {} live rows where the \
                 baseline's held {} - not the same store",
                commas(now),
                commas(was)
            ));
        }
    }
    if let (Some(was), Some(now)) = (power_of(&base.runs), power_of(&file.runs)) {
        if power_differs(&base.runs, &file.runs) {
            lines.push(format!(
                "the machine was {now} where the baseline's was {was}: not the same machine \
                 state"
            ));
        }
    }
    if let (Some(was), Some(now)) = (base.medians.spin_ms, file.medians.spin_ms) {
        if apart(now, was, SPIN_APART) {
            lines.push(format!(
                "spin {} ms against the baseline's {} ms: not the same machine state",
                ms(now),
                ms(was)
            ));
        }
    }
    if let (Some(was), Some(now)) = (base.medians.flush_ms, file.medians.flush_ms) {
        if apart(now, was, FLUSH_APART) {
            lines.push(format!(
                "flush {} ms against the baseline's {} ms: not the same disk state - ingest \
                 lines/s leans on it",
                ms(now),
                ms(was)
            ));
        }
    }
    lines.extend(read_lines(base, file));
    lines
}

/// What an idle ask read, where it is not what the baseline's read. A later build may
/// read less for the same answer; the line gives both numbers and stops nothing.
fn read_lines(base: &ResultFile, file: &ResultFile) -> Vec<String> {
    let (Some(was), Some(now)) = (base.runs.first(), file.runs.first()) else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    let mut read = |name: &str, words: (&str, &str, &str), was: Option<u64>, now: Option<u64>| {
        if let (Some(was), Some(now)) = (was, now) {
            if was != now {
                let (does, what, did) = words;
                lines.push(format!(
                    "the server now {does} {} {what} for {name} where the baseline's {did} {}",
                    commas(now),
                    commas(was)
                ));
            }
        }
    };
    const SCANNED: (&str, &str, &str) = ("counts", "rows scanned", "counted");
    const GROUPS: (&str, &str, &str) = ("reads", "row groups", "read");
    const BYTES: (&str, &str, &str) = ("reads", "bytes", "read");
    const FILES: (&str, &str, &str) = ("opens", "files", "opened");
    for (at, name) in Four::<()>::NAMES.iter().enumerate() {
        let (was, now) = (was.idle.queries.each()[at], now.idle.queries.each()[at]);
        read(name, SCANNED, was.rows_scanned, now.rows_scanned);
        read(name, GROUPS, was.row_groups_read, now.row_groups_read);
        read(name, BYTES, was.bytes_read, now.bytes_read);
        read(name, FILES, was.files_opened, now.files_opened);
    }
    let (was, now) = (&was.idle.scan, &now.idle.scan);
    read(
        "the scan block",
        SCANNED,
        was.rows_scanned,
        now.rows_scanned,
    );
    read(
        "the scan block",
        GROUPS,
        was.row_groups_read,
        now.row_groups_read,
    );
    read("the scan block", FILES, was.files_opened, now.files_opened);
    lines
}

/// What makes a column wrong and not slower or faster, each as a sentence that the table
/// prints after `FAIL`: a plan that returns other leaves, or plans timed on another trie.
fn failures(base: &ResultFile, file: &ResultFile) -> Vec<String> {
    let mut found = Vec::new();
    let trie = |what: &str, was: u64, now: u64| {
        (was != now).then(|| {
            format!(
                "the plan step's trie has {} {what} where the baseline's had {}: the plans \
                 were timed on another trie",
                commas(now),
                commas(was)
            )
        })
    };
    let (was, now) = (&base.plan_trie, &file.plan_trie);
    found.extend(trie("templates", was.templates, now.templates));
    found.extend(trie("nodes", was.nodes, now.nodes));
    found.extend(trie("tokens", was.tokens, now.tokens));
    if let Some(was) = base.runs.first() {
        for (at, name) in Shapes::<()>::NAMES.iter().enumerate() {
            let was = was.plan.shapes.each()[at].leaves;
            let other = file
                .runs
                .iter()
                .map(|run| run.plan.shapes.each()[at].leaves)
                .find(|now| *now != was);
            if let Some(now) = other {
                found.push(format!(
                    "{name} plans to {} leaves where the baseline's planned to {}: a plan \
                     that returns other leaves is not a faster one",
                    commas(now),
                    commas(was)
                ));
            }
        }
    }
    found
}

/// The table from saved results, the first as the baseline and one column a file: the
/// eight rows, then the lines under them, or `SpeedError::Baseline` if not the same work.
pub fn table(saved: &[Saved]) -> Result<Table, SpeedError> {
    let Some((first, rest)) = saved.split_first() else {
        return Ok(Table::default());
    };
    for other in rest {
        let who = other.path.display().to_string();
        let file = &other.file;
        let mut found = work_differences(
            &first.file,
            &file.flags,
            &file.machine,
            &file.server,
            &file.plan_queries,
            &who,
        );
        found.extend(data_differences(
            &first.file,
            &file.data,
            &file.queries,
            &who,
        ));
        if found.is_empty() {
            found = answer_failures(&first.file.expect, &file.expect);
        }
        refuse(first, found)?;
    }
    let base = &first.file;
    let others: Vec<&ResultFile> = rest.iter().map(|saved| &saved.file).collect();
    let files: Vec<&ResultFile> = saved.iter().map(|saved| &saved.file).collect();
    let mut lines = Vec::new();

    let mut head = format!(
        "kibbr bench   commit {}",
        sha12(&others.last().copied().unwrap_or(base).commit)
    );
    if !others.is_empty() {
        head.push_str(&format!("   baseline on {}", sha12(&base.commit)));
    }
    head.push_str(&format!(
        "   {} {} lines, median of {} runs of {} samples",
        base.flags.profile,
        commas(base.flags.lines),
        base.flags.runs,
        base.flags.samples
    ));
    lines.push(head);

    let labels = row_labels(base);
    let indent = labels.iter().map(|label| width(label)).max().unwrap_or(0) + LABEL_GAP;
    let columns: Vec<[String; 6]> = files
        .iter()
        .enumerate()
        .map(|(at, file)| column(file, (at > 0).then_some(base)))
        .collect();
    let widths: Vec<usize> = files
        .iter()
        .zip(&columns)
        .map(|(file, cells)| {
            let widest = cells
                .iter()
                .map(|cell| width(cell))
                .chain([width(&file.label)])
                .max()
                .unwrap_or(0);
            (widest + COLUMN_GAP).max(COLUMN_MIN)
        })
        .collect();
    let in_columns = |label: &str, cells: Vec<&str>| {
        let mut line = format!("{label:<indent$}");
        for (cell, width) in cells.iter().zip(&widths) {
            line.push_str(&format!("{cell:<width$}"));
        }
        line.trim_end().to_string()
    };
    lines.push(in_columns(
        "",
        files.iter().map(|file| file.label.as_str()).collect(),
    ));
    for (row, label) in labels.iter().take(6).enumerate() {
        lines.push(in_columns(
            label,
            columns.iter().map(|cells| cells[row].as_str()).collect(),
        ));
    }

    let plan: Vec<String> = files
        .iter()
        .enumerate()
        .map(|(at, file)| plan_cell(file, (at > 0).then_some(base)))
        .collect();
    lines.push(format!(
        "{:<indent$}{}",
        labels[6],
        plan.join(&" ".repeat(PLAN_GAP))
    ));

    lines.push(format!("{:<indent$}{}", labels[7], loaded_cell(base, None)));
    let loaded: Vec<String> = others
        .iter()
        .map(|file| loaded_cell(file, Some(base)))
        .collect();
    let to = loaded.iter().map(|cell| width(cell)).max().unwrap_or(0) + LOADED_GAP;
    for (file, cell) in others.iter().zip(&loaded) {
        lines.push(format!("{:<indent$}{cell:<to$}{}", "", file.label));
    }

    if !others.is_empty() {
        lines.push(LEGEND.to_string());
    }
    lines.push(scan_note(base, &others));

    let head_width = files
        .iter()
        .map(|file| width(&file.label))
        .max()
        .unwrap_or(0)
        + HEAD_GAP;
    let headed = |file: &ResultFile, text: String, head: bool| {
        if head {
            format!("{:<head_width$}{text}", file.label)
        } else {
            text
        }
    };
    if others.is_empty() {
        lines.extend(notes(base));
        return Ok(Table { lines, fails: 0 });
    }
    if base.flags.samples < P99_SAMPLES {
        lines.push(format!("p99 of {} samples", base.flags.samples));
    }
    for text in own_lines(base) {
        lines.push(headed(base, text, true));
    }
    let several = others.len() > 1;
    let mut fails = 0;
    for file in others {
        for text in overlap_lines(base, file)
            .into_iter()
            .chain(against_lines(base, file))
        {
            lines.push(headed(file, text, several));
        }
        for text in own_lines(file) {
            lines.push(headed(file, text, true));
        }
        for text in failures(base, file) {
            fails += 1;
            lines.push(if several {
                format!("FAIL {}: {text}", file.label)
            } else {
                format!("FAIL {text}")
            });
        }
    }
    Ok(Table { lines, fails })
}

// ── The raw log ────────────────────────────────────────────────────────────────────

/// The raw log: each line goes to standard output at once and is kept, so that the file
/// saved beside the result holds what was printed.
pub struct Log<'a> {
    out: &'a mut dyn Write,
    kept: String,
}

impl<'a> Log<'a> {
    pub fn new(out: &'a mut dyn Write) -> Self {
        Self {
            out,
            kept: String::new(),
        }
    }

    /// Print one line, or several, and flush, since a run is watched as it goes; never
    /// called inside a timed window.
    pub fn line(&mut self, text: &str) -> Result<(), SpeedError> {
        writeln!(self.out, "{text}")
            .and_then(|()| self.out.flush())
            .map_err(on_stdout)?;
        self.kept.push_str(text);
        self.kept.push('\n');
        Ok(())
    }

    /// Everything printed so far.
    pub fn text(&self) -> &str {
        &self.kept
    }
}

fn print(log: &mut Log, lines: &[String]) -> Result<(), SpeedError> {
    lines.iter().try_for_each(|line| log.line(line))
}

/// A line before the first run: what it is about, then the fact. A line that goes on
/// from the one above has no tag.
fn fact(tag: &str, text: &str) -> String {
    format!("{tag:<13}{text}")
}

/// A line of a run: the run, the phase, then the fact.
fn of_run(run: usize, phase: &str, text: &str) -> String {
    format!("run {run}  {phase:<10}{text}")
}

/// Bytes as GiB, to a tenth.
fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1u64 << 30) as f64)
}

/// `1st`, `2nd`, `3rd`, `11th`, `99th`.
fn ordinal(n: usize) -> String {
    let suffix = match (n % 10, n % 100) {
        (_, 11..=13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

/// Which of `samples` values, counted from the smallest, the percentile rule takes at
/// `p`: read off the rule itself, so that the log cannot say another rank than was taken.
fn rank(samples: usize, p: f64) -> usize {
    let ranks: Vec<usize> = (1..=samples).collect();
    percentile(&ranks, p).unwrap_or(0)
}

/// What follows a flag in the server's arguments.
fn argument<'a>(argv: &'a [String], flag: &str) -> Option<&'a str> {
    let at = argv.iter().position(|argument| argument == flag)?;
    argv.get(at + 1).map(String::as_str)
}

/// How an answer reads in a line: its count, and its rows where it returned any.
fn answer(expect: &Expect) -> String {
    match expect.rows_returned {
        Some(rows) if rows > 0 => format!(
            "count {}, {}",
            commas(expect.count),
            counted(rows, "row", "rows")
        ),
        _ => format!("count {}", commas(expect.count)),
    }
}

fn start_lines(file: &ResultFile, loaded: &Corpus) -> Vec<String> {
    let flags = &file.flags;
    let mut lines = Vec::new();

    let mut what = format!(
        "{}, {} lines; {} runs of {} samples; {} senders, {}-line bodies; label: {}",
        flags.profile,
        commas(flags.lines),
        flags.runs,
        flags.samples,
        flags.senders,
        commas(flags.body_lines as u64),
        file.label
    );
    if !flags.checked {
        what.push_str("; UNCHECKED - not a measurement");
    }
    lines.push(fact("kibbr bench", &what));

    let commit = &file.commit;
    lines.push(fact(
        "commit",
        &match &commit.sha {
            None => format!("unknown ({})", commit.how),
            Some(_) => format!(
                "{}, src {} ({}; committed {})",
                sha12(commit),
                match commit.src_dirty {
                    Some(false) => "clean",
                    Some(true) => "differs from it",
                    None => "not compared with it",
                },
                commit.how,
                or_na(commit.committed_at.clone(), |at| at)
            ),
        },
    ));

    let binary = &file.binary;
    let mut built = format!(
        "{}  {} bytes  crc32c {}  built {}  {}",
        binary.path,
        commas(binary.bytes),
        binary.crc32c,
        or_na(binary.modified_at.clone(), |at| at),
        binary.build
    );
    if binary.stale == Some(true) {
        built.push_str("  - built before the commit above was made");
    }
    lines.push(fact("binary", &built));

    lines.push(fact(
        "bench code",
        &format!(
            "crc32c {} over {} source files, white space left out",
            file.bench.source_crc32c, file.bench.source_files
        ),
    ));

    let machine = &file.machine;
    let cores = match (machine.cores_performance, machine.cores_efficiency) {
        (Some(performance), Some(efficiency)) => format!(
            "{} cores ({performance} performance, {efficiency} efficiency)",
            machine.cores_logical
        ),
        _ => format!("{} cores", machine.cores_logical),
    };
    lines.push(fact(
        "machine",
        &format!(
            "{}; {cores}; {} of memory; {} {}; {} free on the volume",
            or_na(machine.cpu.clone(), |cpu| cpu),
            or_na(machine.memory_bytes, |bytes| format!(
                "{} bytes",
                commas(bytes)
            )),
            if machine.os == "macos" {
                "macOS"
            } else {
                machine.os.as_str()
            },
            or_na(machine.os_version.clone(), |version| version),
            gib(machine.disk_free_bytes)
        ),
    ));
    lines.push(fact(
        "cpu clock",
        &match (&machine.cpu_clock, machine.cpu_clock_check) {
            (Some(source), Some(check)) => format!(
                "{source}: this process over a spin by that source, over the same spin by \
                 getrusage, {check:.3}"
            ),
            (Some(source), None) => format!("{source}: not held to getrusage"),
            (None, _) => format!(
                "{NA}: this system has no source of a child's CPU time that this build \
                 reads, so scan rows/s/core is {NA}"
            ),
        },
    ));
    lines.push(fact(
        "swap",
        &match &machine.swap_counter {
            Some(source) => format!("swap-outs are read from {source}"),
            None => "this system has no swap counter that this build reads: swap is not a gate"
                .to_string(),
        },
    ));

    let server = &file.server;
    lines.push(fact("server", &format!("kibbr {}", server.argv.join(" "))));
    lines.push(fact(
        "",
        &format!(
            "{}; noted: {}. A new process on a new copy for every unit below, killed when \
             the unit ends",
            if server.env_removed.is_empty() {
                "no KIBBR__ variable was set".to_string()
            } else {
                format!(
                    "taken out of every child's environment: {}",
                    server.env_removed.join(", ")
                )
            },
            noted(&server.env_noted)
        ),
    ));

    let data = &file.data;
    let bytes: u64 = data.files.iter().map(|file| file.bytes).sum();
    lines.push(fact(
        "loaded",
        &format!(
            "{}  {}: {}, {} bytes, each file's crc32c {}",
            data.dir,
            if loaded.reused {
                "reused"
            } else {
                "built by this invocation"
            },
            counted(data.files.len() as u64, "file", "files"),
            commas(bytes),
            if loaded.reused {
                "as recorded when it was loaded".to_string()
            } else {
                format!("recorded in {}", loaded.marker_path.display())
            }
        ),
    ));
    lines.push(fact(
        "",
        &format!(
            "loaded once, {}, by `kibbr load` from gen-logs --profile {} --lines {} --seed \
             {} ({} bytes, crc32c {}): {} rows, {} row groups",
            data.built_at,
            flags.profile,
            data.lines,
            data.seed,
            commas(data.input.bytes),
            data.input.crc32c,
            commas(data.load.rows),
            or_na(data.load.row_groups, commas)
        ),
    ));
    for left in &loaded.left_out {
        lines.push(fact(
            "",
            &format!(
                "not part of the loaded bytes, and not copied: {}",
                left.display()
            ),
        ));
    }

    let live = &data.live_stream;
    lines.push(fact(
        "live stream",
        &format!(
            "gen-logs --profile {} --lines {} --seed {}: {} bytes, crc32c {}; {} of {} \
             lines, built before any server starts",
            flags.profile,
            live.lines,
            live.seed,
            commas(live.bytes),
            live.crc32c,
            counted(live.bodies as u64, "body", "bodies"),
            commas(flags.body_lines as u64)
        ),
    ));

    // The four by name and label, the three beside them by what they count.
    let names: Vec<String> = Seven::<()>::NAMES
        .iter()
        .zip(file.queries.each())
        .enumerate()
        .map(|(at, (name, query))| {
            if at < Four::<()>::NAMES.len() {
                format!("{name} {}", query.label)
            } else {
                query.label.clone()
            }
        })
        .collect();
    let name_width = names.iter().map(|name| width(name)).max().unwrap_or(0) + 1;
    let text_width = file
        .queries
        .each()
        .iter()
        .map(|query| width(&query.text))
        .max()
        .unwrap_or(0)
        + 4;
    let backstop = argument(&server.argv, "--max-rows")
        .and_then(|rows| rows.parse::<u64>().ok())
        .map_or_else(
            || "server's".to_string(),
            |rows| format!("{}-row", commas(rows)),
        );
    for (at, ((name, query), expect)) in names
        .iter()
        .zip(file.queries.each())
        .zip(file.expect.each())
        .enumerate()
    {
        let mut said = answer(expect);
        if at == 2 && expect.limit_source.as_deref() == Some("backstop") {
            said.push_str(&format!(
                "; no LIMIT: the {backstop} backstop applies and is {}",
                if expect.truncated == Some(true) {
                    "reached"
                } else {
                    "not reached"
                }
            ));
        }
        if at == 5 {
            said.push_str(&format!(
                "; not one of the four: asked {} times for the scan rate",
                flags.scan_asks
            ));
        }
        lines.push(fact(
            if at == 0 { "queries" } else { "" },
            &format!("{name:<name_width$}{:<text_width$}{said}", query.text),
        ));
    }

    lines.push(fact(
        "preflight",
        "a server on a copy answered /health, /stats, the seven queries and one /ingest \
         body the way this bench reads them",
    ));
    if let Some(baseline) = &file.baseline {
        lines.push(fact(
            "baseline",
            &format!(
                "{}  label: {}; commit {}; {}. The same flags, bytes, texts, answers, server \
                 arguments and machine",
                baseline.file,
                baseline.label,
                baseline.sha.as_deref().map_or_else(
                    || "unknown".to_string(),
                    |sha| sha.chars().take(12).collect()
                ),
                if baseline.same_bench {
                    "measured by the same bench code"
                } else {
                    "NOT THE SAME BENCH: its bench code has another digest"
                }
            ),
        ));
    }
    lines.push(fact(
        "what is ms",
        "a query's ms: this process's clock from the request's first byte written to the \
         answer's last byte read, one open connection, one query at a time",
    ));
    let ranks = format!(
        "the {} and {} smallest of {} (nearest rank)",
        ordinal(rank(flags.samples, 0.50)),
        ordinal(rank(flags.samples, 0.99)),
        flags.samples
    );
    lines.push(fact(
        "",
        &if flags.samples < P99_SAMPLES {
            format!("p99 of {} samples: p50, p99 are {ranks}", flags.samples)
        } else {
            format!("p50, p99: {ranks}")
        },
    ));
    lines
}

/// The lines before the first run - what is measured, on which commit, binary and machine,
/// over which bytes, each query with its expected answer - from `file` with no run in it.
pub fn log_start(log: &mut Log, file: &ResultFile, loaded: &Corpus) -> Result<(), SpeedError> {
    print(log, &start_lines(file, loaded))
}

/// Work directories an interrupted bench left under `--dir`, each named once: the bench
/// removes nothing it did not make, and uses nothing it finds in them.
pub fn log_stale(log: &mut Log, left: &[PathBuf]) -> Result<(), SpeedError> {
    left.iter().try_for_each(|dir| {
        log.line(&fact(
            "left behind",
            &format!(
                "{}: the work directory of a bench that did not finish; not removed, and \
                 nothing in it is used",
                dir.display()
            ),
        ))
    })
}

fn checks_lines(run: usize, checks: &Checks, flags: &Flags) -> Vec<String> {
    // Every run's lines stand apart from what is above them.
    let mut lines = vec![String::new()];
    if !flags.checked {
        lines.push(of_run(
            run,
            "checks",
            "none (--unchecked): no pause, no wait for a quiet machine, no spin or flush \
             reference, and nothing is looked at after a unit",
        ));
        return lines;
    }
    let mut parts = Vec::new();
    if checks.waited_s > 0 {
        parts.push(format!(
            "waited {} s for {}",
            commas(checks.waited_s),
            if checks.waited_for.is_empty() {
                "a quiet machine".to_string()
            } else {
                checks.waited_for.join(" and ")
            }
        ));
    }
    parts.push(format!("paused {} s", flags.settle_s));
    parts.push("no other kibbr process".to_string());
    parts.push(match checks.swapouts_before {
        Some(outs) => format!(
            "swapouts {} did not move in {:.1} s (swapins {})",
            commas(outs),
            SWAP_GAP.as_secs_f64(),
            or_na(checks.swapins_before, commas)
        ),
        None => "this system has no swap counter (not a gate)".to_string(),
    });
    parts.push(format!(
        "load average {} and {} available (printed, not gates)",
        or_na(checks.load_average, |load| format!("{load:.2}")),
        or_na(checks.memory_available_bytes, gib)
    ));
    parts.push(format!("spin {} ms", or_na(checks.spin_ms_before, ms)));
    parts.push(format!("flush {} ms", or_na(checks.flush_ms_before, ms)));
    parts.push(power_part(checks.power_before.as_ref()));
    lines.push(of_run(run, "checks", &parts.join("; ")));
    // What the two references are is said once: they are the same in every run.
    if run == 1 {
        lines.push(of_run(
            run,
            "checks",
            &format!(
                "spin: 2^27 rounds of arithmetic on one thread, least of 3. flush: {} writes \
                 of {} bytes to a file in the work directory, each followed by sync_all, \
                 least of 3. Neither touches kibbr",
                flags.flush_ref.writes,
                commas(flags.flush_ref.bytes as u64)
            ),
        ));
    }
    lines
}

/// A run's `checks` lines: the pause, each wait, the swap counter, the references, and
/// how the machine was powered.
pub fn log_checks(
    log: &mut Log,
    run: usize,
    checks: &Checks,
    flags: &Flags,
) -> Result<(), SpeedError> {
    print(log, &checks_lines(run, checks, flags))
}

fn plan_lines(run: usize, plan: &PlanRun, flags: &Flags) -> Vec<String> {
    let mut what = format!(
        "engine::plan on a trie of {} templates, {} nodes, {} tokens, in a process of its \
         own, one thread; p50 of {} calls after {} unmeasured (p99; leaves)",
        commas(plan.templates),
        commas(plan.nodes),
        commas(plan.tokens),
        flags.samples,
        flags.plan_warmup
    );
    if plan.lines_skipped > 0 {
        what.push_str(&format!(
            "; {} the parser refused and are in no template",
            counted(plan.lines_skipped, "generated line", "generated lines")
        ));
    }
    let labels = super::plan::queries();
    let shapes: Vec<String> = Shapes::<()>::NAMES
        .iter()
        .zip(labels.each())
        .zip(plan.shapes.each())
        .map(|((name, query), shape)| {
            format!(
                "{name} {} {} ({}; {})",
                query.label,
                ms(shape.p50_ms),
                ms(shape.p99_ms),
                commas(shape.leaves)
            )
        })
        .collect();
    vec![
        of_run(run, "plan", &what),
        of_run(run, "plan", &shapes.join("   ")),
    ]
}

/// A run's `plan` lines.
pub fn log_plan(
    log: &mut Log,
    run: usize,
    plan: &PlanRun,
    flags: &Flags,
) -> Result<(), SpeedError> {
    print(log, &plan_lines(run, plan, flags))
}

/// A unit's server: its process, the copy it ran on, how long it took to answer.
fn server_words(server: &ServerUnit) -> String {
    format!(
        "server pid {} on a verified copy ({} bytes, {} ms), ready {:.2} s after spawn",
        server.pid,
        commas(server.copy_bytes),
        ms(server.copy_ms),
        server.start_s
    )
}

fn health_words(health: &Health) -> String {
    match &health.detail {
        Some(detail) => format!("health {} ({detail})", health.status),
        None => format!("health {}", health.status),
    }
}

/// A share of a number, in percent, for a difference that is said only when it is large.
fn percent_apart(a: f64, b: f64) -> f64 {
    if b == 0.0 {
        return 0.0;
    }
    (a - b).abs() / b * 100.0
}

fn ingest_lines(run: usize, ingest: &IngestRun) -> Vec<String> {
    let line = |text: &str| of_run(run, "ingest", text);
    let mut lines = vec![line(&format!(
        "{}; no query is sent to this server",
        server_words(&ingest.server)
    ))];
    lines.push(line(&format!(
        "the server's ingest.lines_ingested went {} -> {} while its uptime_seconds went \
         {:.3} -> {:.3}: {} lines / {:.3} s = {} lines/s",
        commas(ingest.lines_ingested_before),
        commas(ingest.lines_ingested_after),
        ingest.uptime_before_s,
        ingest.uptime_after_s,
        commas(ingest.lines),
        ingest.window_s,
        rate(ingest.lines_per_s)
    )));

    let mut senders = format!(
        "senders: {} answered 200, their `ingested` fields sum to {} ({}); refused 503 x {}, \
         429 x {} ({} of them before the body was all written); attempts with no answer {}; \
         reconnects {}; short bodies {}",
        counted(ingest.acked_bodies, "body", "bodies"),
        commas(ingest.acked_lines),
        if ingest.counters_agree {
            "the two counts agree".to_string()
        } else {
            format!(
                "THE TWO COUNTS DIFFER: the server counted {}",
                commas(ingest.lines)
            )
        },
        commas(ingest.refused.s503),
        commas(ingest.refused.s429),
        commas(ingest.refused.before_body_end),
        commas(ingest.no_answer),
        commas(ingest.reconnects),
        commas(ingest.short_bodies)
    );
    if ingest.short_lines > 0 {
        senders.push_str(&format!(
            " ({} not taken)",
            counted(ingest.short_lines, "line", "lines")
        ));
    }
    senders.push_str(&format!(
        "; unknown outcomes {}; the same window on this process's clock {:.3} s",
        commas(ingest.unknown_outcome),
        ingest.window_s_bench
    ));
    let apart = percent_apart(ingest.window_s_bench, ingest.window_s);
    if apart > 1.0 {
        senders.push_str(&format!(", {apart:.1}% from the server's"));
    }
    if let Some(wait) = ingest.retry_after_max_s {
        senders.push_str(&format!(
            "; the longest Retry-After was {wait} s, recorded and not obeyed"
        ));
    }
    if ingest.stream_lines != ingest.lines {
        senders.push_str(&format!(
            "; the stream is {} lines",
            commas(ingest.stream_lines)
        ));
    }
    lines.push(line(&senders));

    let end = &ingest.at_end;
    lines.push(line(&format!(
        "server at the end: {} rows buffered in {} bytes of a {}-byte limit; forced closes \
         {}{}; requests shed {}; lines rejected {}; store writes failed {}; log {} lines \
         ahead of the store; templates {} -> {}; {}; cpu {} ({} cores busy); resident {}",
        or_na(end.buffered_rows, commas),
        or_na(end.buffered_bytes, commas),
        or_na(end.max_buffer_bytes, commas),
        or_na(end.forced_closes, commas),
        match end.reached_buffer_limit {
            Some(true) => " (the limit was reached)",
            Some(false) => " (the limit was not reached)",
            None => "",
        },
        or_na(end.requests_shed, commas),
        or_na(end.lines_rejected, commas),
        or_na(end.write_failures, commas),
        or_na(end.wal_behind, commas),
        or_na(end.templates_before, commas),
        or_na(end.templates_after, commas),
        health_words(&ingest.health_at_end),
        or_na(ingest.cpu_s, |cpu| format!("{cpu:.1} s")),
        or_na(ingest.cores_busy, |cores| format!("{cores:.2}")),
        or_na(ingest.resident_bytes_at_end, |bytes| format!(
            "{} MiB",
            commas(bytes >> 20)
        ))
    )));

    // One line a run for what the server did not say, so that an `n/a` above has its why.
    let missing: Vec<&str> = [
        ("storage.buffered_rows", end.buffered_rows.is_none()),
        ("storage.buffered_bytes", end.buffered_bytes.is_none()),
        ("storage.max_buffer_bytes", end.max_buffer_bytes.is_none()),
        ("storage.forced_closes", end.forced_closes.is_none()),
        ("ingest.requests_shed", end.requests_shed.is_none()),
        ("ingest.lines_rejected", end.lines_rejected.is_none()),
        ("storage.write_failures", end.write_failures.is_none()),
        (
            "storage.wal.last_seq or storage.wal.applied",
            end.wal_behind.is_none(),
        ),
        (
            "trie.templates",
            end.templates_before.is_none() || end.templates_after.is_none(),
        ),
    ]
    .iter()
    .filter(|(_, absent)| *absent)
    .map(|(field, _)| *field)
    .collect();
    if !missing.is_empty() {
        lines.push(line(&format!(
            "{NA} above: /stats did not give {}",
            missing.join(", ")
        )));
    }

    let by_second: Vec<String> = ingest
        .acked_lines_by_second
        .iter()
        .map(|lines| commas(*lines))
        .collect();
    lines.push(line(&format!(
        "lines acknowledged by the second, this process's clock: {}",
        by_second.join(" ")
    )));
    lines
}

/// A run's `ingest` lines: what was divided by what, then everything beside it.
pub fn log_ingest(log: &mut Log, run: usize, ingest: &IngestRun) -> Result<(), SpeedError> {
    print(log, &ingest_lines(run, ingest))
}

/// Seconds of a block, CPU or wall, to four figures: `0.151`, `13.02`, `183.9`.
fn cpu_s(seconds: f64) -> String {
    if seconds < 9.9995 {
        format!("{seconds:.3}")
    } else if seconds < 99.995 {
        format!("{seconds:.2}")
    } else {
        format!("{seconds:.1}")
    }
}

/// Client p50, p99, least and most, then the server's own p50 and p99.
fn timings(
    p50_ms: f64,
    p99_ms: f64,
    min_ms: f64,
    max_ms: f64,
    server: (Option<f64>, Option<f64>),
) -> String {
    format!(
        "{}  {}  ({}  {}) | {}  {}",
        ms(p50_ms),
        ms(p99_ms),
        ms(min_ms),
        ms(max_ms),
        or_na(server.0, ms),
        or_na(server.1, ms)
    )
}

fn refused_words(refused: u64) -> String {
    if refused == 0 {
        return String::new();
    }
    format!(
        " | refused {} and sent again",
        counted(refused, "time", "times")
    )
}

fn idle_lines(
    run: usize,
    idle: &IdleRun,
    scan: &ScanRun,
    queries: &Seven<QueryText>,
    flags: &Flags,
) -> Vec<String> {
    let mut lines = vec![of_run(
        run,
        "idle",
        &format!(
            "{}; no line is sent to this server",
            server_words(&idle.server)
        ),
    )];

    let block = &idle.scan;
    let mut divided = format!(
        "{} rows hold {} x {} of {}, back to back after {} unmeasured = {} rows / ",
        commas(scan.key_rows),
        key_of(&queries.scan_key.text),
        counted(scan.asks as u64, "ask", "asks"),
        queries.scan.text,
        flags.warmup,
        commas(scan.rows)
    );
    match scan.cpu_s {
        None => divided.push_str(&format!(
            "{NA}: the system gave no CPU time for pid {} = {NA} rows per CPU-second",
            idle.server.pid
        )),
        Some(cpu) => {
            divided.push_str(&format!(
                "{} s of CPU the server process used over them (user {} + system {}, read \
                 from the OS for pid {}; the whole process: HTTP, the plan and {} file \
                 opens an ask are in it) = ",
                cpu_s(cpu),
                or_na(block.cpu_user_s, |user| format!("{user:.2}")),
                or_na(block.cpu_system_s, |system| format!("{system:.2}")),
                idle.server.pid,
                or_na(block.files_opened, commas)
            ));
            divided.push_str(&match scan.rows_per_cpu_s {
                Some(rows) => format!("{} rows per CPU-second", rate(rows)),
                None => format!("{NA} (the OS reported no CPU time over the block)"),
            });
        }
    }
    lines.push(of_run(run, "scan", &divided));
    let mut wall = format!(
        "the same block by the wall clock: {} s, {} cores busy, {} rows per wall-second; \
         the server says it scanned {}; in the {} ms after the block, nothing asked, it \
         used {} s of CPU",
        cpu_s(scan.wall_s),
        or_na(scan.cores_busy, |cores| format!("{cores:.2}")),
        or_na(scan.rows_per_wall_s, rate),
        or_na(scan.server_rows_scanned_sum, commas),
        block.tail_ms,
        or_na(scan.cpu_tail_s, cpu_s)
    );
    wall.push_str(&refused_words(block.refused).replace(" | ", "; "));
    lines.push(of_run(run, "scan", &wall));

    lines.push(of_run(
        run,
        "idle",
        &format!(
            "{} asks of each query after {} unmeasured; client p50 p99 (min max) | the \
             server's own duration_ms p50 p99 | the answer | CPU the server used over the {}",
            flags.samples, flags.warmup, flags.samples
        ),
    ));
    for (name, block) in Four::<()>::NAMES.iter().zip(idle.queries.each()) {
        let limit = match block.answer.limit_source.as_deref() {
            Some("backstop") => {
                if block.answer.truncated == Some(true) {
                    "backstop, truncated".to_string()
                } else {
                    "backstop, not truncated".to_string()
                }
            }
            Some(source) => source.to_string(),
            None => NA.to_string(),
        };
        lines.push(of_run(
            run,
            &format!("idle {name}"),
            &format!(
                "{} | {}  scanned {}  {}  {}  {limit} | {} CPU-s in {} s{}",
                timings(
                    block.p50_ms,
                    block.p99_ms,
                    block.min_ms,
                    block.max_ms,
                    (block.server_p50_ms, block.server_p99_ms)
                ),
                answer(&block.answer),
                or_na(block.rows_scanned, commas),
                or_na(block.answer.templates_matched, |templates| counted(
                    templates,
                    "template",
                    "templates"
                )),
                or_na(block.row_groups_read, |groups| counted(
                    groups,
                    "row group",
                    "row groups"
                )),
                or_na(block.cpu_s, cpu_s),
                cpu_s(block.wall_s),
                refused_words(block.refused)
            ),
        ));
    }

    let beside = &scan.beside;
    lines.push(of_run(
        run,
        "idle",
        &format!(
            "beside the scan rate, not in the table: S2's own block, {} rows hold {} x {} / \
             {} s of CPU = {} rows per CPU-second, held down by {} file opens an ask; S1 {}; \
             S3 {}; S4 {} (rows the server says it scanned / CPU-seconds)",
            commas(beside.s2.key_rows),
            key_of(&queries.key.text),
            counted(beside.s2.asks as u64, "ask", "asks"),
            or_na(beside.s2.cpu_s, cpu_s),
            or_na(beside.s2.rows_per_cpu_s, rate),
            or_na(beside.s2.leaves_opened, commas),
            or_na(beside.s1.rows_per_cpu_s, rate),
            or_na(beside.s3.rows_per_cpu_s, rate),
            or_na(beside.s4.rows_per_cpu_s, rate)
        ),
    ));
    if idle.health_at_end.status != "ok" {
        lines.push(of_run(
            run,
            "idle",
            &format!(
                "at the end of the phase: {}",
                health_words(&idle.health_at_end)
            ),
        ));
    }
    lines
}

/// A run's `scan` and `idle` lines.
pub fn log_idle(
    log: &mut Log,
    run: usize,
    idle: &IdleRun,
    scan: &ScanRun,
    queries: &Seven<QueryText>,
    flags: &Flags,
) -> Result<(), SpeedError> {
    print(log, &idle_lines(run, idle, scan, queries, flags))
}

fn laps_lines(run: usize, flags: &Flags) -> Vec<String> {
    let places: Vec<String> = place_lines(flags.lines)
        .iter()
        .map(|lines| commas(*lines))
        .collect();
    vec![
        of_run(
            run,
            "loaded",
            &format!(
                "{}. A lap: a new copy, a new server, S1 S2 S3 once unmeasured, {} senders \
                 flat out from their first body; each query asked once when {} live lines \
                 are acknowledged; kill",
                counted(flags.samples as u64, "lap", "laps"),
                flags.senders,
                places.join(" / ")
            ),
        ),
        of_run(
            run,
            "loaded",
            "a query reads one snapshot taken before its scan: `over the loaded answer` is \
             what its count held of the live stream; lines acknowledged while it ran are \
             load beside it, not rows it read",
        ),
    ]
}

/// The `loaded` lines that say what a lap is, printed before the first.
pub fn log_laps(log: &mut Log, run: usize, flags: &Flags) -> Result<(), SpeedError> {
    print(log, &laps_lines(run, flags))
}

/// Whether lap `lap` of `laps`, counted from 1, is one the log marks: a lap that ends
/// another quarter of them. With 100 laps those are 25, 50, 75 and 100.
fn marked(lap: usize, laps: usize) -> bool {
    (1..=laps).contains(&lap) && lap * 4 / laps != (lap - 1) * 4 / laps
}

/// One line as a quarter of a run's laps passes, so that a run of minutes is seen to
/// move; called after every lap, between two servers.
pub fn log_lap(log: &mut Log, run: usize, lap: usize, laps: usize) -> Result<(), SpeedError> {
    if !marked(lap, laps) {
        return Ok(());
    }
    log.line(&of_run(run, "loaded", &format!("lap {lap} of {laps}")))
}

/// The least and the greatest of a list of counts, as `20,000 to 26,000`.
fn span(values: &[u64]) -> String {
    match (values.iter().min(), values.iter().max()) {
        (Some(least), Some(most)) => format!("{} to {}", commas(*least), commas(*most)),
        _ => NA.to_string(),
    }
}

fn loaded_lines(run: usize, loaded: &LoadedRun, flags: &Flags) -> Vec<String> {
    let mut lines = Vec::new();
    for (name, block) in Four::<()>::NAMES.iter().zip(loaded.queries.each()) {
        lines.push(of_run(
            run,
            &format!("loaded {name}"),
            &format!(
                "{} | asked at {} lines; counted {} over the loaded answer; {} late{}",
                timings(
                    block.p50_ms,
                    block.p99_ms,
                    block.min_ms,
                    block.max_ms,
                    (block.server_p50_ms, block.server_p99_ms)
                ),
                span(&block.at_lines),
                span(&block.count_over_loaded),
                commas(block.late),
                refused_words(block.refused)
            ),
        ));
    }
    let laps = commas(loaded.laps as u64);
    lines.push(of_run(
        run,
        "loaded",
        &format!(
            "{} ran past the end of the stream; bodies refused {}; {}; ingest with the \
             queries running {} lines/s (median over laps of the server's lines over its \
             uptime between two /stats reads; not in the table)",
            counted(loaded.wrapped_laps, "lap", "laps"),
            counted(loaded.refused_bodies, "time", "times"),
            match loaded.laps_at_buffer_limit {
                Some(reached) => format!(
                    "the buffer limit was reached in {} of {laps} laps (forced closes above \
                     0 when the lap ended)",
                    commas(reached)
                ),
                None => format!(
                    "the buffer limit: {NA} (/stats did not give storage.forced_closes in \
                     every lap)"
                ),
            },
            or_na(loaded.lines_per_s_median, rate)
        ),
    ));
    let unwritten = laps_that_could_not_write(&loaded.lap);
    if unwritten > 0 {
        lines.push(of_run(
            run,
            "loaded",
            &format!(
                "{} of {laps} laps ran on a store that could not write (in the lap's last \
                 /stats, storage.write_failures above 0 or storage.degraded): their servers \
                 acknowledged lines while no row reached a file",
                commas(unwritten)
            ),
        ));
    }
    let late: u64 = loaded.queries.each().iter().map(|block| block.late).sum();
    if late > 0 {
        lines.push(of_run(
            run,
            "loaded",
            &format!(
                "late: asked more than one round of bodies ({} lines) past its place, \
                 because the query before it was still running",
                commas((flags.senders * flags.body_lines) as u64)
            ),
        ));
    }
    lines
}

/// A run's `loaded` lines, once its laps are done.
pub fn log_loaded(
    log: &mut Log,
    run: usize,
    loaded: &LoadedRun,
    flags: &Flags,
) -> Result<(), SpeedError> {
    print(log, &loaded_lines(run, loaded, flags))
}

fn voided_line(run: usize, voided: &Voided, waited_s: u64) -> String {
    // A lap is named with its number; the three other units by the phase they are.
    let (phase, unit) = if voided.unit.starts_with("lap") {
        ("loaded", voided.unit.clone())
    } else {
        (voided.unit.as_str(), "this unit".to_string())
    };
    of_run(
        run,
        phase,
        &format!(
            "{unit} thrown away: {}; waited {} s; done again",
            voided.reason,
            commas(waited_s)
        ),
    )
}

/// A unit that was thrown away: which, why, and how long the checks then waited.
pub fn log_voided(
    log: &mut Log,
    run: usize,
    voided: &Voided,
    waited_s: u64,
) -> Result<(), SpeedError> {
    log.line(&voided_line(run, voided, waited_s))
}

/// Seconds as a run's length reads: `48 s`, `11 min 02 s`.
fn spent(seconds: f64) -> String {
    let whole = seconds.round() as u64;
    if whole < 60 {
        format!("{whole} s")
    } else {
        format!("{} min {:02} s", whole / 60, whole % 60)
    }
}

fn end_line(run: &Run) -> String {
    of_run(
        run.run,
        "end",
        &format!(
            "{} thrown away; spin {} ms; flush {} ms; {}; the binary is the one that \
             started; {}",
            counted(run.voided.len() as u64, "unit", "units"),
            or_na(run.checks.spin_ms_after, ms),
            or_na(run.checks.flush_ms_after, ms),
            power_part(run.checks.power_after.as_ref()),
            spent(run.seconds)
        ),
    )
}

/// A run's `end` line - units thrown away, the references and the power state again, the
/// binary, the time - printed once the binary is found to be the bytes that started.
pub fn log_end(log: &mut Log, run: &Run) -> Result<(), SpeedError> {
    log.line(&end_line(run))
}

/// The width of the summary's labels, and of each of its columns of numbers.
const SUMMARY_LABEL: usize = 39;
const SUMMARY_COLUMN: usize = 13;

fn summary_lines(file: &ResultFile) -> Vec<String> {
    let (each, middle) = (&file.per_run, &file.medians);
    let row = |label: &str, cells: Vec<String>| {
        let mut line = format!("{label:<SUMMARY_LABEL$}");
        for cell in cells {
            line.push_str(&format!("{cell:>SUMMARY_COLUMN$}"));
        }
        line
    };
    let with = |runs: Vec<String>, median: String| {
        let mut cells = runs;
        cells.push(median);
        cells
    };
    let in_ms = |runs: &[f64], median: f64| with(runs.iter().map(|v| ms(*v)).collect(), ms(median));
    let maybe = |runs: &[Option<f64>], median: Option<f64>, print: fn(f64) -> String| {
        with(
            runs.iter().map(|value| or_na(*value, print)).collect(),
            or_na(median, print),
        )
    };
    let cores: fn(f64) -> String = |cores| format!("{cores:.2}");

    let mut heads: Vec<String> = (1..=each.ingest_lines_per_s.len())
        .map(|run| format!("run {run}"))
        .collect();
    heads.push("median".to_string());
    let mut lines = vec![
        String::new(),
        row("every run, then the median", heads),
        row(
            "ingest lines/s",
            with(
                each.ingest_lines_per_s.iter().map(|v| rate(*v)).collect(),
                rate(middle.ingest_lines_per_s),
            ),
        ),
    ];
    for (phase, each, middle) in [
        ("idle", &each.idle, &middle.idle),
        ("loaded", &each.loaded, &middle.loaded),
    ] {
        for ((name, each), middle) in Four::<()>::NAMES.iter().zip(each.each()).zip(middle.each()) {
            lines.push(row(
                &format!("{phase} {name} p50 ms"),
                in_ms(&each.p50_ms, middle.p50_ms),
            ));
            lines.push(row(
                &format!("{phase} {name} p99 ms"),
                in_ms(&each.p99_ms, middle.p99_ms),
            ));
        }
    }
    lines.push(row(
        "scan rows per CPU-second",
        maybe(&each.scan_rows_per_cpu_s, middle.scan_rows_per_cpu_s, rate),
    ));
    lines.push(row(
        "scan rows per wall-second (not a row)",
        maybe(
            &each.scan_rows_per_wall_s,
            middle.scan_rows_per_wall_s,
            rate,
        ),
    ));
    lines.push(row(
        "scan cores busy (not a row)",
        maybe(&each.scan_cores_busy, middle.scan_cores_busy, cores),
    ));
    for ((name, each), middle) in Shapes::<()>::NAMES
        .iter()
        .zip(each.plan_ms.each())
        .zip(middle.plan_ms.each())
    {
        lines.push(row(&format!("plan {name} p50 ms"), in_ms(each, *middle)));
    }
    lines.push(row(
        "spin ms, before the run (not a row)",
        maybe(&each.spin_ms, middle.spin_ms, ms),
    ));
    lines.push(row(
        "flush ms, before the run (not a row)",
        maybe(&each.flush_ms, middle.flush_ms, ms),
    ));
    lines.push(String::new());
    lines
}

/// After the last run: every run's value for each number, then the median.
pub fn log_summary(log: &mut Log, file: &ResultFile) -> Result<(), SpeedError> {
    print(log, &summary_lines(file))
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
