// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr serve`: the command line of the server, which is a module of its own
//! (`crate::server`). Writes nothing to standard output: the banner and the lifecycle log
//! go to stderr through `logging`.

use std::path::PathBuf;
use std::process::ExitCode;

use tokio_util::sync::CancellationToken;

use crate::cli::config::{self, File, Log};
use crate::cli::error::CliError;

/// A flag rather than inferred from `--data-dir`, so that `--storage memory --data-dir /x`
/// reads as an instruction and not as a contradiction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum StorageKind {
    #[default]
    Disk,
    Memory,
}

/// Run a server on the settings `flags`, `file` and the defaults make, until a signal stops
/// it, and say what the process exits with.
pub fn cli(file: &File, flags: Args, log: Log) -> Result<ExitCode, CliError> {
    let settings = config::settings(file, flags, log)?;
    // Nothing here cancels the token: a server started from the command line is
    // stopped by SIGTERM or SIGINT, which it hears for itself.
    let report = crate::server::run(settings, CancellationToken::new())?;
    Ok(exit_code(&report))
}

/// What `kibbr serve` exits with: 0 for every run that ended in a report, the deadline's
/// cut included, because every acknowledged line is in the write-ahead log.
fn exit_code(report: &crate::server::Report) -> ExitCode {
    // Every field named, so that one added to the report is looked at here.
    let crate::server::Report { addr: _, cut: _ } = report;
    ExitCode::SUCCESS
}

/// The flags of `kibbr serve`, as clap read them: each `None` where the command line did
/// not say, so that its variable, the file and the default are asked in turn.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct Args {
    /// The address to listen on. Loopback by default: `0.0.0.0` publishes a write endpoint with
    /// no authentication. Overrides the host of `KIBBR__API__NATIVE_ADDR`. [default: 127.0.0.1]
    #[arg(long)]
    pub host: Option<String>,
    /// Overrides the port of `KIBBR__API__NATIVE_ADDR` and of `[api] native_addr`.
    /// [default: 7700]
    #[arg(long)]
    pub port: Option<u16>,
    /// Rows a query that names no `LIMIT` is cut at.
    /// `0` disables it. Overrides `KIBBR__QUERY__MAX_ROWS`.
    #[arg(long, value_name = "ROWS")]
    pub max_rows: Option<usize>,
    /// Buckets a `GROUP BY time(...)` may emit before the query is refused.
    /// `0` disables it. Overrides `KIBBR__QUERY__MAX_BUCKETS`.
    #[arg(long, value_name = "BUCKETS")]
    pub max_buckets: Option<usize>,
    /// Groups a bucketed `GROUP BY` may emit before the query is refused.
    /// `0` disables it. Overrides `KIBBR__QUERY__MAX_GROUPS`.
    #[arg(long, value_name = "GROUPS")]
    pub max_groups: Option<usize>,

    // The three below are properties of the server, not of a query: a file has no wire, no
    // encoding and no second caller competing for the machine.
    /// The largest `/ingest` body accepted, measured on the wire *before* any
    /// decompression. Overrides `KIBBR__API__MAX_BODY_BYTES`.
    #[arg(long, value_name = "BYTES")]
    pub max_body_bytes: Option<usize>,
    /// The largest a gzipped `/ingest` body may become once decompressed. Cannot
    /// exceed the 64 MiB line limit. Overrides `KIBBR__API__MAX_DECOMPRESSED_BYTES`.
    #[arg(long, value_name = "BYTES")]
    pub max_decompressed_bytes: Option<usize>,
    /// How many `/ingest` bodies may be parsed at once; past it a body waits --ingest-wait-ms,
    /// then answered `503`. Overrides `KIBBR__API__INGEST_CONCURRENCY`. [default: one a core]
    #[arg(long, value_name = "N")]
    pub ingest_concurrency: Option<usize>,
    /// How long an `/ingest` body may wait, in milliseconds, before it is answered `503`; at
    /// most 30000. Overrides `KIBBR__API__INGEST_WAIT_MS`. [default: 100]
    #[arg(long, value_name = "MS")]
    pub ingest_wait_ms: Option<usize>,
    /// How long after a stop signal requests in flight are given to finish: a number and ms, s,
    /// m or h; `0s` waits for all. Overrides `KIBBR__API__SHUTDOWN_DEADLINE`. [default: 25s]
    #[arg(long, value_name = "DURATION", value_parser = config::parse_duration)]
    pub shutdown_deadline: Option<std::time::Duration>,
    /// How long one request may take: `408` for a body not yet arrived, `504` for a query,
    /// whose scan stops; `0s`: none. Overrides `KIBBR__API__REQUEST_TIMEOUT`. [default: 30s]
    #[arg(long, value_name = "DURATION", value_parser = config::parse_duration)]
    pub request_timeout: Option<std::time::Duration>,
    /// Where rows go: `disk`, day files under --data-dir, or `memory`, which writes nothing.
    #[arg(long, value_name = "KIND", default_value = "disk")]
    pub storage: StorageKind,
    /// The data directory. Created if absent. Overrides `KIBBR__STORAGE__BASE_PATH`.
    #[arg(long, value_name = "PATH")]
    pub data_dir: Option<PathBuf>,
    /// The trie snapshot. Defaults to `trie.json` inside the data directory.
    /// Overrides `KIBBR__STORAGE__TRIE_FILE`.
    #[arg(long, value_name = "PATH")]
    pub trie_file: Option<PathBuf>,
    /// How often the row group thresholds are checked, in milliseconds; a row is queryable at
    /// once either way. Overrides `KIBBR__INGESTOR__FLUSH_INTERVAL_MS`. [default: 500]
    #[arg(long, value_name = "MS")]
    pub flush_interval_ms: Option<usize>,
    /// How long a row group may stay open with rows in it, in milliseconds; they are in the log
    /// meanwhile. Overrides `KIBBR__INGESTOR__ROW_GROUP_TIMEOUT_MS`. [default: 300000]
    #[arg(long, value_name = "MS")]
    pub row_group_timeout_ms: Option<usize>,
    /// A segment of the write-ahead log rolls to the next number at this many bytes.
    /// Overrides `KIBBR__STORAGE__WAL_SEGMENT_BYTES`. [default: 64 MiB]
    #[arg(long, value_name = "BYTES")]
    pub wal_segment_bytes: Option<usize>,
    /// Lines at which one flush of the write-ahead log stops taking more bodies; a body is
    /// never split. Overrides `KIBBR__INGESTOR__BATCH_SIZE`. [default: 500]
    #[arg(long, value_name = "LINES")]
    pub batch_size: Option<usize>,
    /// The most bytes of lines that may be in the write-ahead log and not yet in the store;
    /// past it a body waits. Overrides `KIBBR__STORAGE__WAL_QUEUE_BYTES`. [default: 256 MiB]
    #[arg(long, value_name = "BYTES")]
    pub wal_queue_bytes: Option<usize>,
    /// How many day files may be open for writing at once; keep it under `ulimit -n`.
    /// Overrides `KIBBR__STORAGE__MAX_OPEN_SEGMENTS`. [default: 512]
    #[arg(long, value_name = "N")]
    pub max_open_segments: Option<usize>,
    /// A day's data file is sealed at this many bytes and the next one opened.
    /// Overrides `KIBBR__STORAGE__SEGMENT_MAX_BYTES`. [default: 512 MiB]
    #[arg(long, value_name = "BYTES")]
    pub segment_max_bytes: Option<usize>,
    /// Memory all row buffers together may hold before the largest are closed early; at twice
    /// it `/ingest` waits. Overrides `KIBBR__STORAGE__MAX_BUFFER_BYTES`. [default: 512 MiB]
    #[arg(long, value_name = "BYTES")]
    pub max_buffer_bytes: Option<usize>,
    /// How often to look for a finished day and compact it, in milliseconds; `0` never looks.
    /// Overrides `KIBBR__STORAGE__COMPACT_INTERVAL_MS`. [default: 60000]
    #[arg(long, value_name = "MS")]
    pub compact_interval_ms: Option<usize>,
    /// Threads one query may scan a day on; more than the machine has cores is refused.
    /// Overrides `KIBBR__QUERY__SCAN_THREADS`. [default: half the cores, at most 4]
    #[arg(long, visible_alias = "threads", value_name = "N")]
    pub scan_threads: Option<usize>,
    /// A run of row groups holding fewer rows than this is scanned on one thread.
    /// Overrides `KIBBR__QUERY__SCAN_FAN_ROWS`. [default: 32768]
    #[arg(long, value_name = "ROWS")]
    pub scan_fan_rows: Option<usize>,
    /// The key names a new template's time column is chosen from, in order, comma-separated;
    /// one holding none takes arrival time. Overrides `KIBBR__SCHEMA__TIME_COLUMNS`.
    #[arg(long, value_name = "NAMES", value_delimiter = ',')]
    pub time_columns: Option<Vec<String>>,
}

impl Args {
    /// `--storage memory`: no data directory, whatever else names one.
    pub fn memory(&self) -> bool {
        self.storage == StorageKind::Memory
    }
}
