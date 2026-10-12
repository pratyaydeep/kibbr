//! What a server runs on: `Settings`, every value in it plain data with a default.
//!
//! Nothing here reads a flag, an environment variable or a file - where a value came from
//! is the binary's business (`flag > environment > file > default`, its `config`), and a
//! test's is a struct literal. Each group is declared with what holds a server to it:
//! `Limits` is what a *query* may not exceed and is the engine's, `Schema` is the trie's,
//! `ApiLimits` is what a *request* may not exceed, and `Storage` is where rows go - which
//! a load or a compaction running alone is given too, to open the directory a server
//! would.

use std::path::PathBuf;
use std::time::Duration;

pub use kibbr_engine::limits::Limits;
pub use kibbr_storage::trie::Schema;

use crate::DEFAULT_PORT;

/// The largest `/ingest` body accepted off the wire, 10 MiB.
///
/// axum's own 2 MiB is right for a form and wrong for a log shipper: Vector's default
/// batch is larger, and a 413 on a routine batch reads as kibbr rejecting valid logs. Not
/// unlimited, because the body is buffered whole and this process cannot spill it.
///
/// Measured before decompression, which is the only place it *can* be measured, and the
/// reason it is not the only cap - see `DEFAULT_MAX_DECOMPRESSED_BYTES`.
pub const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;

/// The largest an `/ingest` body may become once decompressed, 64 MiB.
///
/// The wire cap does not bound what the server allocates: a crafted 1 MiB body expands to
/// gigabytes, passes the wire check by three orders of magnitude - it *is* under 10 MiB -
/// and the process dies inflating it. So the size is checked twice, and the second check
/// stops *at* the cap rather than after it.
///
/// **Capped at `lines::MAX_LINE_BYTES`, and the ceiling is load bearing.** That constant's
/// note has the equality: a body cannot exceed the body cap, so a line inside one cannot
/// exceed `MAX_LINE_BYTES`, so `LineTooLong` is unreachable over HTTP. A larger cap breaks
/// that, which is why `ApiLimits::load` refuses one rather than quietly accepting it.
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = kibbr_core::lines::MAX_LINE_BYTES;

/// How many `/ingest` bodies may be in the parse-and-walk at once: one per core.
///
/// CPU-bound work, so the permits say how much of this machine ingest may have, not how
/// deep a queue is. More permits than cores does not make the server faster - it makes
/// every request slower and the shipper's timeout closer, which is the failure the bound
/// exists to prevent.
///
/// Falls back to 1 where the core count is unknowable: one permit is slow, not broken,
/// where a guess of eight on a single core is broken.
pub fn default_ingest_concurrency() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// How long an `/ingest` body is kept waiting, in milliseconds, before it is shed.
///
/// What separates a burst from an overload. Two shippers arriving together on a
/// four-permit server should queue for the millisecond it takes; twenty arriving together
/// every second is not a burst, and waiting will not fix it.
///
/// A setting, and a short one by default: what an operator tunes first is the permit
/// count, which is the statement about the machine. A longer wait moves the refusal
/// closer to the shipper's own timeout, which is the outcome the shed exists to avoid -
/// so the default stays where it was while it was a constant.
pub const DEFAULT_INGEST_WAIT_MS: usize = 100;

/// How long after SIGTERM or SIGINT the requests in flight are given to finish: 25
/// seconds.
///
/// Kubernetes sends SIGKILL thirty seconds after its SIGTERM. A stop used to wait for
/// every connection for as long as it took, so one slow sender - or one that sent half a
/// body and nothing more - held the final flush, the checkpoint and `trie.json` until
/// the kill arrived and none of them ran. With the deadline, what is still in flight
/// then is cut off and the wind-down follows, inside the five seconds that are left:
/// 200,000 buffered rows stop in a second and a half (`scripts/fault/shutdown_time.sh`).
/// Nothing acknowledged is lost either way - it is in the write-ahead log - but a stop
/// that finishes leaves nothing to replay.
pub const DEFAULT_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(25);

/// How long a request may take from its first line to its answer: 30 seconds.
///
/// Two things were unbounded without it. A client that sends a request's headers and
/// then no body, or half of one, kept its handler waiting for as long as it kept its
/// socket; and a query kept its thread, its core and every row it had fetched until it
/// had read the last row group it was asked about, whether or not anyone was still
/// waiting for the answer - `MATCH LOG {}` with `--max-rows 0` on 1,800,000 rows was two
/// minutes of that. Now the first is answered `408` and the second `504`, and the
/// query's scan is told to stop (`store::Stop`): it is not left running.
///
/// **An `/ingest` body that has been read whole is not timed out.** Its waits are
/// `--ingest-wait-ms`'s, and the one after them is the log's flush: a `504` then would
/// be for lines the log may have taken, and a shipper told so sends them again.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Where a server listens when nothing says otherwise. Loopback: an address of every
/// interface publishes an unauthenticated write endpoint, and that is typed, not
/// defaulted into.
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// How often the thresholds that close a row group are checked.
///
/// Not a durability interval, and it stopped being one with the segment format: a tick
/// closes a row group only where a threshold has fired, so a busy leaf is written on the
/// first tick after 8,192 rows and a quiet one waits for `DEFAULT_ROW_GROUP_TIMEOUT_MS`.
/// What the tick bounds is how late either can be noticed.
pub const DEFAULT_FLUSH_INTERVAL_MS: usize = 500;

/// How long a row group may stay open with rows in it: five minutes.
///
/// **It was one minute while it was also what a `kill -9` cost.** Every row that waits is
/// now durable in the write-ahead log, so the wait costs replay time after a crash - a
/// 10,000-line backlog is back in under a second - and no rows; and a quiet template that
/// closes a row group every five minutes makes a fifth of the row groups, each of which
/// pays a header against a few hundred bytes of data. Still a setting: it is also how
/// much of the log a quiet template holds the checkpoint behind. It cannot go below the
/// flush interval - the tick is what notices it has expired.
pub const DEFAULT_ROW_GROUP_TIMEOUT_MS: usize = 300_000;

/// How often a server looks for a day that has ended and compacts it.
///
/// A minute, where the flush tick is half a second: a day becomes due once, at the first
/// look after midnight UTC, and a minute's delay in rebuilding a day nobody is writing
/// costs nothing. `0` is "off", as it is for the query limits - the look is what is
/// switched off, and `kibbr compact` still compacts a day when asked.
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
///
/// `Clone` and not `Copy`, which breaks the pattern the other two config structs set: a
/// `PathBuf` is an allocation. Worth the break - the alternative is a fixed-size path type
/// or an interned string, and neither is a thing this needs.
#[derive(Debug, Clone)]
pub struct Storage {
    /// `None` = memory, and everything ingested is lost at exit.
    pub base_path: Option<PathBuf>,
    /// `None` exactly when `base_path` is.
    pub trie_file: Option<PathBuf>,
    pub flush_interval: Duration,
    /// What a row group closes on, when no size threshold got there first.
    pub row_group_timeout: Duration,
    /// The cap on data files open for writing - one per UTC day being written. Past it,
    /// the least recently written one is sealed - always safe, because the next row for
    /// that day opens the next file number.
    ///
    /// The number and what goes wrong without it are at `store::DEFAULT_MAX_OPEN_SEGMENTS`.
    /// What belongs here is why it is a **setting and not a constant**: it is really a
    /// statement about the process's descriptor limit, which the process does not choose,
    /// so an operator running under a tight `ulimit -n` has to be able to say so.
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
    /// this many lines. Not a target and not a wait - a flush takes what has queued - and
    /// not a bound on a body, which goes in whole.
    pub wal_batch_lines: usize,
    /// The most bytes of lines that may be in the write-ahead log and not yet in the
    /// store: past it a body waits for room, and is then shed.
    pub wal_queue_bytes: usize,
    /// Threads one query may choose a day's rows on, its own among them. 0 and 1: its
    /// own alone. Only a server asks for more (`with_scan`).
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
            max_open_segments: kibbr_storage::store::DEFAULT_MAX_OPEN_SEGMENTS,
            segment_max_bytes: kibbr_storage::store::disk::DEFAULT_SEGMENT_MAX_BYTES,
            max_buffer_bytes: kibbr_storage::store::DEFAULT_MAX_BUFFER_BYTES,
            compact_interval: Duration::from_millis(DEFAULT_COMPACT_INTERVAL_MS as u64),
            wal_segment_bytes: kibbr_storage::wal::DEFAULT_SEGMENT_BYTES,
            wal_batch_lines: kibbr_storage::wal::DEFAULT_BATCH_LINES,
            wal_queue_bytes: kibbr_storage::wal::DEFAULT_QUEUE_BYTES,
            scan_threads: 0,
            scan_fan_rows: kibbr_storage::store::DEFAULT_FAN_ROWS,
        }
    }
}

impl Storage {
    /// What the write-ahead log is written with.
    pub fn wal_options(&self) -> kibbr_storage::wal::Options {
        kibbr_storage::wal::Options {
            segment_bytes: self.wal_segment_bytes,
            batch_lines: self.wal_batch_lines,
            queue_bytes: self.wal_queue_bytes,
        }
    }

    pub fn is_durable(&self) -> bool {
        self.base_path.is_some()
    }

    /// The row group thresholds these settings imply. Only the timeout is configurable;
    /// the two size thresholds are not worth a knob.
    pub fn row_group_limits(&self) -> kibbr_storage::store::RowGroupLimits {
        kibbr_storage::store::RowGroupLimits {
            age: self.row_group_timeout,
            ..kibbr_storage::store::RowGroupLimits::default()
        }
    }

    /// What the disk store is held to.
    pub fn disk_options(&self) -> kibbr_storage::store::DiskOptions {
        kibbr_storage::store::DiskOptions {
            row_groups: self.row_group_limits(),
            max_open_files: self.max_open_segments,
            segment_max_bytes: self.segment_max_bytes,
            max_buffer_bytes: self.max_buffer_bytes,
            scan_threads: self.scan_threads,
            fan_rows: self.scan_fan_rows,
        }
    }

    /// What a freshly-stamped `LeafMeta` records as its storage path. Documentation
    /// rather than authority - see `trie::DEFAULT_DATA_DIR`.
    pub fn data_dir(&self) -> String {
        self.base_path.as_ref().map_or_else(
            || kibbr_storage::trie::DEFAULT_DATA_DIR.to_string(),
            |path| path.display().to_string(),
        )
    }
}

/// What a *request* may not exceed. Server-only: a file has no wire, no encoding and no
/// second caller competing for the machine.
#[derive(Debug, Clone, Copy)]
pub struct ApiLimits {
    /// The largest `/ingest` body accepted, measured on the wire before any decompression.
    pub max_body_bytes: usize,
    /// The largest that body may become once decompressed.
    pub max_decompressed_bytes: usize,
    /// How many `/ingest` bodies may be in the parse-and-walk at once. Never 0 - `load`
    /// refuses it rather than starting a process that 503s every request.
    pub ingest_concurrency: usize,
    /// How long an `/ingest` body may be kept waiting before it is shed: see
    /// `DEFAULT_INGEST_WAIT_MS`. Zero waits for nothing.
    pub ingest_wait: Duration,
    /// How long after the signal the requests in flight are given before they are cut
    /// off and the wind-down begins: see `DEFAULT_SHUTDOWN_DEADLINE`. `None` waits for
    /// every one of them, however long it takes.
    pub shutdown_deadline: Option<Duration>,
    /// How long a request may take from its first line to its answer: see
    /// `DEFAULT_REQUEST_TIMEOUT`. `None` is for as long as it takes.
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
    /// A paragraph for a person at a terminal: the address, what survives a stop and
    /// what does not, the endpoints and every limit. One event under `BANNER`, its
    /// message the paragraph, which the binary's subscriber writes as it is.
    #[default]
    Paragraph,
    /// One event and not a paragraph, for whatever reads an object a line: the address
    /// and the settings an alert would be written on as its fields, and the one sentence
    /// that must not be lost - storage in memory - as a warning of its own.
    Event,
}

/// The target of the event that is the banner's paragraph. A subscriber that writes a
/// line of text for an event writes this one's message bare and whatever its level: it
/// is already the paragraph, and a warning a level can silence is one that will be.
pub const BANNER: &str = "kibbr_server::banner";

/// Everything a server runs on. `run` is given one and reads nothing else.
///
/// **The default is a server that touches nothing**: loopback, storage in memory, the
/// default limits. A data directory is `storage.base_path` and `storage.trie_file`
/// together - the binary's `config` is what makes the directory and names the snapshot
/// inside it - and `port` 0 is whichever port the system gives.
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
