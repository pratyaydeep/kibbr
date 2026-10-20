// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a server runs on: `Settings`, every value in it plain data with a default.
//!
//! Nothing here reads a flag, an environment variable or a file - where a value came from
//! is the binary's business, and a test's is a struct literal.

use std::path::PathBuf;
use std::time::Duration;

pub use crate::engine::limits::Limits;
pub use crate::storage::trie::Schema;

use crate::DEFAULT_PORT;

/// The largest `/ingest` body accepted off the wire, 10 MiB: above a shipper's routine
/// batch, and bounded because the body is buffered whole.
pub const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// The largest an `/ingest` body may become once decompressed, capped at
/// `lines::MAX_LINE_BYTES` so `LineTooLong` stays unreachable over HTTP.
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = crate::core::lines::MAX_LINE_BYTES;

/// How many `/ingest` bodies may be in the parse-and-walk at once: one per core, as the
/// work is CPU-bound, and 1 where the core count is unknowable.
pub fn default_ingest_concurrency() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// How long an `/ingest` body is kept waiting, in milliseconds, before it is shed: short,
/// so a burst queues and an overload is refused well before the shipper's own timeout.
pub const DEFAULT_INGEST_WAIT_MS: usize = 100;

/// How long after SIGTERM or SIGINT the requests in flight are given to finish: 25
/// seconds, inside the thirty Kubernetes allows before its SIGKILL.
pub const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(25);

/// How long a request may take from its first line to its answer: 30 seconds, except an
/// `/ingest` body read whole, whose `504` would make a shipper resend lines the log took.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Where a server listens when nothing says otherwise: loopback, since every interface
/// would publish an unauthenticated write endpoint.
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// How often the thresholds that close a row group are checked, which bounds how late a
/// fired threshold is noticed and is not a durability interval.
pub const DEFAULT_FLUSH_INTERVAL_MS: usize = 500;

/// How long a row group may stay open with rows in it: five minutes, which costs replay
/// time and no rows, and cannot go below the flush interval whose tick notices it.
pub const DEFAULT_ROW_GROUP_TIMEOUT_MS: usize = 300_000;

/// How often a server looks for a day that has ended and compacts it; `0` switches the
/// look off, and `kibbr compact` still compacts a day when asked.
pub const DEFAULT_COMPACT_INTERVAL_MS: usize = 60_000;

/// A duration as `parse_duration` reads one back: whole seconds as `25s`, and anything
/// finer in milliseconds.
pub fn duration_words(d: Duration) -> String {
    match d.as_millis() {
        ms if ms % 1000 == 0 => format!("{}s", ms / 1000),
        ms => format!("{ms}ms"),
    }
}

/// Where rows and the trie snapshot live, and how often the buffer reaches them.
#[derive(Debug, Clone)]
pub struct Storage {
    /// `None` = memory, and everything ingested is lost at exit.
    pub base_path: Option<PathBuf>,
    /// `None` exactly when `base_path` is.
    pub trie_file: Option<PathBuf>,
    pub flush_interval: Duration,
    /// What a row group closes on, when no size threshold got there first.
    pub row_group_timeout: Duration,
    /// The cap on data files open for writing, a setting because it follows the process's
    /// descriptor limit (`store::DEFAULT_MAX_OPEN_SEGMENTS`).
    pub max_open_segments: usize,
    /// A day's data file is sealed at this size and the next number opened.
    pub segment_max_bytes: u64,
    /// Memory every buffer together may hold before the largest are closed early.
    pub max_buffer_bytes: u64,
    /// How often a server looks for a day that is due for compaction. Zero is never.
    pub compact_interval: Duration,
    /// A segment of the write-ahead log rolls to the next number at this size.
    pub wal_segment_bytes: u64,
    /// Where one flush of the write-ahead log stops taking more bodies: once it holds
    /// this many lines.
    pub wal_batch_lines: usize,
    /// The most bytes of lines that may be in the write-ahead log and not yet in the
    /// store: past it a body waits for room, and is then shed.
    pub wal_queue_bytes: usize,
    /// Threads one query may choose a day's rows on, its own among them; only a server asks
    /// for more than its own (`with_scan`).
    pub scan_threads: usize,
    /// A run of row groups holding fewer rows than this is never shared between those
    /// threads: see `store::DEFAULT_FAN_ROWS`.
    pub scan_fan_rows: u64,
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            base_path: None,
            trie_file: None,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS as u64),
            row_group_timeout: Duration::from_millis(DEFAULT_ROW_GROUP_TIMEOUT_MS as u64),
            max_open_segments: crate::storage::store::DEFAULT_MAX_OPEN_SEGMENTS,
            segment_max_bytes: crate::storage::store::disk::DEFAULT_SEGMENT_MAX_BYTES,
            max_buffer_bytes: crate::storage::store::DEFAULT_MAX_BUFFER_BYTES,
            compact_interval: Duration::from_millis(DEFAULT_COMPACT_INTERVAL_MS as u64),
            wal_segment_bytes: crate::storage::wal::DEFAULT_SEGMENT_BYTES,
            wal_batch_lines: crate::storage::wal::DEFAULT_BATCH_LINES,
            wal_queue_bytes: crate::storage::wal::DEFAULT_QUEUE_BYTES,
            scan_threads: 0,
            scan_fan_rows: crate::storage::store::DEFAULT_FAN_ROWS,
        }
    }
}

impl Storage {
    /// What the write-ahead log is written with.
    pub fn wal_options(&self) -> crate::storage::wal::Options {
        crate::storage::wal::Options {
            segment_bytes: self.wal_segment_bytes,
            batch_lines: self.wal_batch_lines,
            queue_bytes: self.wal_queue_bytes,
        }
    }

    pub fn is_durable(&self) -> bool {
        self.base_path.is_some()
    }

    /// The row group thresholds these settings imply; only the timeout is worth a knob.
    pub fn row_group_limits(&self) -> crate::storage::store::RowGroupLimits {
        crate::storage::store::RowGroupLimits {
            age: self.row_group_timeout,
            ..crate::storage::store::RowGroupLimits::default()
        }
    }

    /// What the disk store is held to.
    pub fn disk_options(&self) -> crate::storage::store::DiskOptions {
        crate::storage::store::DiskOptions {
            row_groups: self.row_group_limits(),
            max_open_files: self.max_open_segments,
            segment_max_bytes: self.segment_max_bytes,
            max_buffer_bytes: self.max_buffer_bytes,
            scan_threads: self.scan_threads,
            fan_rows: self.scan_fan_rows,
        }
    }

    /// What a freshly-stamped `LeafMeta` records as its storage path: documentation
    /// rather than authority, see `trie::DEFAULT_DATA_DIR`.
    pub fn data_dir(&self) -> String {
        self.base_path.as_ref().map_or_else(
            || crate::storage::trie::DEFAULT_DATA_DIR.to_string(),
            |path| path.display().to_string(),
        )
    }
}

/// What a *request* may not exceed; server-only, since a file has no wire and no
/// second caller competing for the machine.
#[derive(Debug, Clone, Copy)]
pub struct ApiLimits {
    /// The largest `/ingest` body accepted, measured on the wire before any decompression.
    pub max_body_bytes: usize,
    /// The largest that body may become once decompressed.
    pub max_decompressed_bytes: usize,
    /// How many `/ingest` bodies may be in the parse-and-walk at once; never 0, which
    /// `load` refuses rather than start a process that 503s every request.
    pub ingest_concurrency: usize,
    /// How long an `/ingest` body may be kept waiting before it is shed, zero waiting for
    /// nothing (`DEFAULT_INGEST_WAIT_MS`).
    pub ingest_wait: Duration,
    /// How long after the signal the requests in flight are given before they are cut off,
    /// `None` waiting for every one (`DEFAULT_SHUTDOWN_DEADLINE`).
    pub shutdown_deadline: Option<Duration>,
    /// How long a request may take from its first line to its answer, `None` being as long
    /// as it takes (`DEFAULT_REQUEST_TIMEOUT`).
    pub request_timeout: Option<Duration>,
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
            ingest_concurrency: default_ingest_concurrency(),
            ingest_wait: Duration::from_millis(DEFAULT_INGEST_WAIT_MS as u64),
            shutdown_deadline: Some(DEFAULT_SHUTDOWN_DEADLINE),
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
        }
    }
}

/// How a server says it has started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Banner {
    /// A paragraph for a person at a terminal, sent as one event under `BANNER`.
    #[default]
    Paragraph,
    /// One event with the address and settings as fields, for whatever reads an object a
    /// line; storage in memory is a warning of its own.
    Event,
}

/// The target of the event that is the banner's paragraph, which a subscriber writes
/// bare whatever its level, so no level can silence it.
pub const BANNER: &str = "crate::server::banner";

/// Everything a server runs on; the default touches nothing: loopback, storage in
/// memory, the default limits.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Where it listens: loopback unless told otherwise, see `DEFAULT_HOST`.
    pub host: String,
    pub port: u16,
    /// What a query may not exceed.
    pub limits: Limits,
    /// What a request may not exceed.
    pub api: ApiLimits,
    /// Where rows go, and how the files and the log are written.
    pub storage: Storage,
    /// Which key names may carry a template's time.
    pub schema: Schema,
    /// How the start is said.
    pub banner: Banner,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            limits: Limits::default(),
            api: ApiLimits::default(),
            storage: Storage::default(),
            schema: Schema::default(),
            banner: Banner::default(),
        }
    }
}
