//! Configuration, shared between the CLI and the server so they cannot disagree about
//! the backstop for an unlimited query.
//!
//! One struct, because the CLI and the server disagreeing about a backstop would make the
//! same query mean two things.
//!
//! Three groups: `Limits` is what a *query* may not exceed and both surfaces carry it;
//! `ApiLimits` is what a *request* may not exceed, and only the server has requests;
//! `Storage` is where rows go, which only the server has too - the CLI rebuilds its
//! database from an input file per invocation and has nothing to keep.
//!
//! The flush interval is spelled in **milliseconds as a whole number**, where the
//! configuration file it will eventually be read from spells it `"500ms"`. Deliberate
//! while that file does not exist: the one duration reader in the crate is TrieQL's, whose
//! units are `s/m/h/d`, and teaching it `ms` would add `SINCE 500ms` to the grammar as a
//! side effect of naming a flush interval. The `ms` spelling arrives with the file.

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use crate::infer::EpochWindow;

/// Default page size for an unlimited `MATCH`.
pub const DEFAULT_MAX_ROWS: usize = 10_000;

/// Default ceiling on the buckets a `GROUP BY time(...)` may emit.
///
/// `SINCE 30d GROUP BY time(1s)` is 2.6M buckets. The query is refused rather than
/// truncated: a chart missing its tail looks like data, where a refusal looks like a
/// refusal.
pub const DEFAULT_MAX_BUCKETS: usize = 10_000;

/// Default ceiling on the groups a bucketed `GROUP BY` may emit.
///
/// A second key multiplies the bucket count by the number of distinct labels it has, so
/// the bucket cap alone does not bound the answer: 10,000 buckets against 800 hosts is
/// 8,000,000 groups. Only bucketed grouping is measured against this - a plain
/// `GROUP BY user_id` emits one row per user and always did.
pub const DEFAULT_MAX_GROUPS: usize = 100_000;

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
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = crate::lines::MAX_LINE_BYTES;

/// How many `/ingest` bodies may be in the parse-and-walk at once: one per core.
///
/// CPU-bound work, so the permits say how much of this machine ingest may have, not how
/// deep a queue is. More permits than cores does not make the server faster - it makes
/// every request slower and the shipper's timeout closer, which is the failure the bound
/// exists to prevent.
///
/// Falls back to 1 where the core count is unknowable: one permit is slow, not broken,
/// where a guess of eight on a single core is broken.
fn default_ingest_concurrency() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// The cores this machine reports; one where it reports none.
fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Half the cores, and no more than four: what one query may choose rows on. Half
/// leaves the other half to the runtime and the ingest path. Four is as many fast cores
/// as the machine this was measured on has; more than four was not measured on this scan.
///
/// **What four costs, in replays on eight cores, four of them fast; `kibbr bench` has
/// not yet taken it.** A count over 433,000 rows of one day answers in 2.1 to 2.5 ms
/// where one thread takes 4.5, and a fetch of the newest 500 rows of a million in 41 ms
/// where one takes 45. The same count uses 14 to 34% more CPU than on one thread when it
/// is asked of a server that has been idle (its rows per CPU-second fall 12 to 25%), and
/// 10% more of one that is busy with such counts (a fall of 9%): about 3 points of that
/// are the wake-ups, and the rest is what cores that have just been woken charge for the
/// same work. Two threads keep about half of each gain for about half of that cost,
/// which is what `--scan-threads 2` buys.
fn default_scan_threads() -> usize {
    (cores() / 2).clamp(1, 4)
}

/// Environment variable for the max_rows configuration.
const MAX_ROWS_ENV: &str = "KIBBR__QUERY__MAX_ROWS";
const MAX_BUCKETS_ENV: &str = "KIBBR__QUERY__MAX_BUCKETS";
const MAX_GROUPS_ENV: &str = "KIBBR__QUERY__MAX_GROUPS";
const SCAN_THREADS_ENV: &str = "KIBBR__QUERY__SCAN_THREADS";
const SCAN_FAN_ROWS_ENV: &str = "KIBBR__QUERY__SCAN_FAN_ROWS";
const MAX_BODY_BYTES_ENV: &str = "KIBBR__API__MAX_BODY_BYTES";
const MAX_DECOMPRESSED_BYTES_ENV: &str = "KIBBR__API__MAX_DECOMPRESSED_BYTES";
const INGEST_CONCURRENCY_ENV: &str = "KIBBR__API__INGEST_CONCURRENCY";
const BASE_PATH_ENV: &str = "KIBBR__STORAGE__BASE_PATH";
const TRIE_FILE_ENV: &str = "KIBBR__STORAGE__TRIE_FILE";
const FLUSH_INTERVAL_ENV: &str = "KIBBR__INGESTOR__FLUSH_INTERVAL_MS";
const ROW_GROUP_TIMEOUT_ENV: &str = "KIBBR__INGESTOR__ROW_GROUP_TIMEOUT_MS";
const MAX_OPEN_SEGMENTS_ENV: &str = "KIBBR__STORAGE__MAX_OPEN_SEGMENTS";
const SEGMENT_MAX_BYTES_ENV: &str = "KIBBR__STORAGE__SEGMENT_MAX_BYTES";
const MAX_BUFFER_BYTES_ENV: &str = "KIBBR__STORAGE__MAX_BUFFER_BYTES";
const COMPACT_INTERVAL_ENV: &str = "KIBBR__STORAGE__COMPACT_INTERVAL_MS";
const WAL_SEGMENT_BYTES_ENV: &str = "KIBBR__STORAGE__WAL_SEGMENT_BYTES";
const BATCH_SIZE_ENV: &str = "KIBBR__INGESTOR__BATCH_SIZE";
const TIME_COLUMNS_ENV: &str = "KIBBR__SCHEMA__TIME_COLUMNS";
const EPOCH_WINDOW_ENV: &str = "KIBBR__SCHEMA__EPOCH_WINDOW";

/// The names a template's time column is chosen from, first match wins.
pub const DEFAULT_TIME_COLUMNS: &[&str] = &[
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

/// Where `serve` keeps day partitions when nothing says otherwise.
///
/// Relative, and beside the process rather than under `/var/lib`: a default that needs
/// root to create is a default that fails on the first run on a laptop.
pub const DEFAULT_DATA_DIR: &str = "kibbr-data";
/// The trie snapshot, inside the data directory so one path moves both.
pub const TRIE_FILE_NAME: &str = "trie.json";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{var}=`{value}` is not a whole number")]
    BadEnv { var: &'static str, value: String },

    /// Zero permits is not "no limit", it is a server that refuses everything. The other
    /// limits here read 0 as "off", so this one has to say why it does not.
    #[error("ingest concurrency must be at least 1; 0 would refuse every /ingest body")]
    NoIngestPermits,

    /// Above the line cap, `LineTooLong` becomes reachable over HTTP and a shipper's batch
    /// silently changes meaning. Refused rather than clamped: an operator who asked for
    /// 256 MiB should be told they did not get it.
    #[error(
        "max decompressed bytes ({asked}) is above the {limit} byte line limit; a body \
         cannot hold a line longer than that, so the extra capacity is unreachable"
    )]
    DecompressedAboveLineCap { asked: usize, limit: usize },

    /// Every other limit here reads 0 as "off". This one cannot: a zero interval is not
    /// "never flush", it is a task that wakes with no delay and burns a core.
    #[error("flush interval must be at least 1ms; 0 would spin the flush task")]
    NoFlushInterval,

    /// And this one cannot read 0 as "off" either: with no segment allowed open there is
    /// nowhere for a row group to go, so every flush would seal an empty file and the
    /// buffer would never drain.
    #[error("max open segments must be at least 1; 0 would leave nowhere for a row group to go")]
    NoOpenSegments,

    /// A file sealed before it holds anything would seal on every flush; a limit of 0 on
    /// the buffers would close every buffer on every tick, the thresholds unread.
    #[error("{what} must be at least 1 byte; 0 is not a size anything can be held to")]
    NoBytes { what: &'static str },

    /// The bound on one flush of the write-ahead log is not "off" at 0 either: a flush
    /// that may take no line takes none, and every body waits for ever.
    #[error("batch size must be at least 1 line; 0 would let a flush of the write-ahead log take nothing")]
    NoBatch,

    /// Refused rather than clamped, for the reason `DecompressedAboveLineCap` is. A pool of
    /// 511 threads on eight cores ran a scan at four times its CPU and no faster, and one of
    /// 99,999 held the first query that needed it for sixteen seconds while the system
    /// refused its threads - with every answer still right, so nothing said so.
    #[error(
        "scan threads ({asked}) is above the {cores} core(s) this machine has; the threads \
         past that would only take turns on the same cores"
    )]
    ScanThreadsAboveCores { asked: usize, cores: usize },

    /// Refused rather than defaulted: the window decides what type a value is stored as,
    /// and an operator who set one would believe it was in force.
    #[error(
        "{var}=`{value}` is not an epoch window: {reason}; expected two RFC 3339 instants, \
         start before end, e.g. `2000-01-01T00:00:00Z,2100-01-01T00:00:00Z`"
    )]
    BadEpochWindow {
        var: &'static str,
        value: String,
        reason: String,
    },

    #[error("storage directory `{path}`: {source}")]
    BadDataDir {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// What the type ladder and the trie are told about the data: for now, which key names
/// may carry a template's time.
///
/// Carried by the CLI as well as the server - `kibbr query <file>` builds its trie in
/// the process, and a template created there chooses its time source by the same rule.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Ordered. A template's time source is the first of these that is one of its keys
    /// and whose value on the creating line inferred as `Datetime`.
    pub time_columns: Vec<String>,
}

impl Default for Schema {
    fn default() -> Self {
        Self {
            time_columns: DEFAULT_TIME_COLUMNS.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Schema {
    /// Flag > environment > default. The variable is comma-separated; blanks are dropped
    /// so a trailing comma is not an empty column name.
    pub fn load(time_columns: Option<Vec<String>>) -> Self {
        let from_env = || {
            std::env::var(TIME_COLUMNS_ENV).ok().map(|text| {
                text.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<String>>()
            })
        };
        let mut schema = Self::default();
        if let Some(list) = time_columns.or_else(from_env) {
            schema.time_columns = list;
        }
        schema
    }
}

/// The window ten or thirteen digits must name a moment inside to be read as a timestamp:
/// environment > default. The variable is two RFC 3339 instants, comma-separated -
/// neither contains a comma - and the window runs from the first up to but not including
/// the second.
///
/// Not a field of `Schema`, because the ladder cannot be handed one: it is a free function
/// the flattener, the store and the engine all call. `main` installs this once for the
/// process instead, before any command types a value.
pub fn epoch_window() -> Result<EpochWindow, ConfigError> {
    let Some(raw) = std::env::var_os(EPOCH_WINDOW_ENV) else {
        return Ok(EpochWindow::default());
    };
    // Unset is absence; anything else is a window an operator believes is in force, so a
    // value that is not even UTF-8 is an error rather than a silent fall back to the
    // default.
    let text = raw.to_string_lossy().into_owned();
    let bad = |reason: String| ConfigError::BadEpochWindow {
        var: EPOCH_WINDOW_ENV,
        value: text.clone(),
        reason,
    };
    if raw.to_str().is_none() {
        return Err(bad("the value is not UTF-8".into()));
    }
    let ends: Vec<&str> = text.split(',').map(str::trim).collect();
    let [start, end] = ends[..] else {
        return Err(bad(format!("{} comma-separated values, not 2", ends.len())));
    };
    let instant = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| bad(format!("`{s}` is not an RFC 3339 instant ({e})")))
    };
    EpochWindow::new(instant(start)?, instant(end)?)
        .ok_or_else(|| bad("the start is not before the end, so the window is empty".into()))
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
            max_open_segments: crate::store::DEFAULT_MAX_OPEN_SEGMENTS,
            segment_max_bytes: crate::store::disk::DEFAULT_SEGMENT_MAX_BYTES,
            max_buffer_bytes: crate::store::DEFAULT_MAX_BUFFER_BYTES,
            compact_interval: Duration::from_millis(DEFAULT_COMPACT_INTERVAL_MS as u64),
            wal_segment_bytes: crate::wal::DEFAULT_SEGMENT_BYTES,
            wal_batch_lines: crate::wal::DEFAULT_BATCH_LINES,
            scan_threads: 0,
            scan_fan_rows: crate::store::DEFAULT_FAN_ROWS,
        }
    }
}

impl Storage {
    /// The same three rules as the other two, over the storage settings.
    ///
    /// `durable` is the `--storage` choice rather than a path being present, so that
    /// `--storage memory --data-dir /x` is a clear instruction and not a contradiction
    /// resolved by whichever flag the code happened to read first.
    #[allow(clippy::too_many_arguments)]
    pub fn load(
        durable: bool,
        data_dir: Option<PathBuf>,
        trie_file: Option<PathBuf>,
        flush_ms: Option<usize>,
        row_group_ms: Option<usize>,
        max_open_segments: Option<usize>,
        segment_max_bytes: Option<usize>,
        max_buffer_bytes: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let flush_ms = resolve(flush_ms, FLUSH_INTERVAL_ENV, DEFAULT_FLUSH_INTERVAL_MS)?;
        if flush_ms == 0 {
            return Err(ConfigError::NoFlushInterval);
        }
        let flush_interval = Duration::from_millis(flush_ms as u64);
        // Floored at the flush interval rather than refused below it: the tick is what
        // asks whether the timeout has expired, so a shorter one cannot be honoured and
        // pretending otherwise would put a number in `/stats` that nothing implements.
        let row_group_ms = resolve(
            row_group_ms,
            ROW_GROUP_TIMEOUT_ENV,
            DEFAULT_ROW_GROUP_TIMEOUT_MS,
        )?
        .max(flush_ms);
        let row_group_timeout = Duration::from_millis(row_group_ms as u64);

        let max_open_segments = resolve(
            max_open_segments,
            MAX_OPEN_SEGMENTS_ENV,
            crate::store::DEFAULT_MAX_OPEN_SEGMENTS,
        )?;
        if max_open_segments == 0 {
            return Err(ConfigError::NoOpenSegments);
        }
        let bytes = |flag, var, default: u64, what| match resolve(flag, var, default as usize)? {
            0 => Err(ConfigError::NoBytes { what }),
            n => Ok(n as u64),
        };
        let segment_max_bytes = bytes(
            segment_max_bytes,
            SEGMENT_MAX_BYTES_ENV,
            crate::store::disk::DEFAULT_SEGMENT_MAX_BYTES,
            "segment max bytes",
        )?;
        let max_buffer_bytes = bytes(
            max_buffer_bytes,
            MAX_BUFFER_BYTES_ENV,
            crate::store::DEFAULT_MAX_BUFFER_BYTES,
            "max buffer bytes",
        )?;
        // The default here, and the variable not read: only `serve` looks for days that
        // are due, and it resolves its flag over the variable in `with_compact_interval` -
        // read here, a malformed variable would refuse the flag that overrides it, and
        // every command that never looks.
        let compact_interval = Duration::from_millis(DEFAULT_COMPACT_INTERVAL_MS as u64);

        // The defaults here too, for the reason the compaction interval's is: only `serve`
        // writes a log, and it resolves its flags over the variables in `with_wal`.
        let wal_segment_bytes = crate::wal::DEFAULT_SEGMENT_BYTES;
        let wal_batch_lines = crate::wal::DEFAULT_BATCH_LINES;

        // No scan threads here, for the same reason again: only `serve` answers a query
        // from a disk store, and it asks for its threads in `with_scan`. A load and a
        // compaction start none.
        let scan_threads = 0;
        let scan_fan_rows = crate::store::DEFAULT_FAN_ROWS;

        if !durable {
            return Ok(Self {
                base_path: None,
                trie_file: None,
                flush_interval,
                row_group_timeout,
                max_open_segments,
                segment_max_bytes,
                max_buffer_bytes,
                compact_interval,
                wal_segment_bytes,
                wal_batch_lines,
                scan_threads,
                scan_fan_rows,
            });
        }

        let base = Self::base_named(data_dir);
        // Created and canonicalised now, before a line is accepted: an unwritable data
        // directory should stop the process at startup, where an operator is watching,
        // rather than at the first flush tick half a minute later. Absolute afterwards, so
        // nothing that changes the working directory can move the database.
        std::fs::create_dir_all(&base).map_err(|source| ConfigError::BadDataDir {
            path: base.display().to_string(),
            source,
        })?;
        let base = std::fs::canonicalize(&base).unwrap_or(base);

        let trie_file =
            resolve_path(trie_file, TRIE_FILE_ENV).unwrap_or_else(|| base.join(TRIE_FILE_NAME));

        Ok(Self {
            base_path: Some(base),
            trie_file: Some(trie_file),
            flush_interval,
            row_group_timeout,
            max_open_segments,
            segment_max_bytes,
            max_buffer_bytes,
            compact_interval,
            wal_segment_bytes,
            wal_batch_lines,
            scan_threads,
            scan_fan_rows,
        })
    }

    /// The data directory a command was pointed at - flag > environment > default - as it
    /// was named, and **without creating it**. `load` creates what this names, which is
    /// right for a command that fills a directory; one that only works on a directory
    /// already there asks this first, so a mistyped path is refused and not made.
    pub fn base_named(data_dir: Option<PathBuf>) -> PathBuf {
        resolve_path(data_dir, BASE_PATH_ENV).unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR))
    }

    /// How often `serve` looks for a day that is due: `--compact-interval-ms` >
    /// environment > default. Apart from `load` because only `serve` has the flag and
    /// only `serve` looks: a ninth positional `None` at every other caller would say
    /// nothing, and the variable is read here so that the flag, when given, is never
    /// refused for a variable it overrides.
    pub fn with_compact_interval(mut self, flag_ms: Option<usize>) -> Result<Self, ConfigError> {
        let ms = resolve(flag_ms, COMPACT_INTERVAL_ENV, DEFAULT_COMPACT_INTERVAL_MS)?;
        self.compact_interval = Duration::from_millis(ms as u64);
        Ok(self)
    }

    /// How `serve` writes its write-ahead log: `--wal-segment-bytes` and `--batch-size`,
    /// each over its variable, over the default. Apart from `load` for the reason
    /// `with_compact_interval` is: only `serve` has the flags, and only `serve` writes one.
    pub fn with_wal(
        mut self,
        segment_bytes: Option<usize>,
        batch_lines: Option<usize>,
    ) -> Result<Self, ConfigError> {
        self.wal_segment_bytes = match resolve(
            segment_bytes,
            WAL_SEGMENT_BYTES_ENV,
            crate::wal::DEFAULT_SEGMENT_BYTES as usize,
        )? {
            0 => {
                return Err(ConfigError::NoBytes {
                    what: "wal segment bytes",
                })
            }
            n => n as u64,
        };
        self.wal_batch_lines =
            match resolve(batch_lines, BATCH_SIZE_ENV, crate::wal::DEFAULT_BATCH_LINES)? {
                0 => return Err(ConfigError::NoBatch),
                n => n,
            };
        Ok(self)
    }

    /// What a server's queries may use: `--scan-threads` and `--scan-fan-rows`, each over
    /// its variable, over the default. Apart from `load` for the reason `with_wal` is:
    /// only `serve` answers a query from a disk store, so only it has the flags, and a
    /// load or a compaction starts no scan thread. More threads than the machine has
    /// cores are refused; 0 and 1 are both "the query's own thread and no other".
    pub fn with_scan(
        mut self,
        threads: Option<usize>,
        fan_rows: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let cores = cores();
        self.scan_threads = match resolve(threads, SCAN_THREADS_ENV, default_scan_threads())? {
            asked if asked > cores => {
                return Err(ConfigError::ScanThreadsAboveCores { asked, cores })
            }
            n => n,
        };
        self.scan_fan_rows = resolve(
            fan_rows,
            SCAN_FAN_ROWS_ENV,
            crate::store::DEFAULT_FAN_ROWS as usize,
        )? as u64;
        Ok(self)
    }

    /// What the write-ahead log is written with.
    pub fn wal_options(&self) -> crate::wal::Options {
        crate::wal::Options {
            segment_bytes: self.wal_segment_bytes,
            batch_lines: self.wal_batch_lines,
        }
    }

    pub fn is_durable(&self) -> bool {
        self.base_path.is_some()
    }

    /// The row group thresholds these settings imply. Only the timeout is configurable;
    /// the two size thresholds are not worth a knob.
    pub fn row_group_limits(&self) -> crate::store::RowGroupLimits {
        crate::store::RowGroupLimits {
            age: self.row_group_timeout,
            ..crate::store::RowGroupLimits::default()
        }
    }

    /// What the disk store is held to.
    pub fn disk_options(&self) -> crate::store::DiskOptions {
        crate::store::DiskOptions {
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
            || crate::trie::DEFAULT_DATA_DIR.to_string(),
            |path| path.display().to_string(),
        )
    }
}

/// Flag > environment > absent. The same rule as `resolve`, over a path.
fn resolve_path(flag: Option<PathBuf>, var: &'static str) -> Option<PathBuf> {
    flag.or_else(|| std::env::var_os(var).map(PathBuf::from))
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
}

impl Default for ApiLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
            ingest_concurrency: default_ingest_concurrency(),
        }
    }
}

/// What a query may not exceed, carried on the `Query` itself so the engine reads the
/// same numbers the CLI and the server were configured with.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// The backstop, in rows. `0` = none.
    pub max_rows: usize,
    /// The ceiling on buckets emitted by `GROUP BY time(...)`. `0` = none.
    pub max_buckets: usize,
    /// The ceiling on groups emitted by a bucketed `GROUP BY`. `0` = none.
    pub max_groups: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_ROWS,
            max_buckets: DEFAULT_MAX_BUCKETS,
            max_groups: DEFAULT_MAX_GROUPS,
        }
    }
}

impl Limits {
    /// Load the query limits: flag > environment > default, one rule for each.
    pub fn load(
        max_rows: Option<usize>,
        max_buckets: Option<usize>,
        max_groups: Option<usize>,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            max_rows: resolve(max_rows, MAX_ROWS_ENV, DEFAULT_MAX_ROWS)?,
            max_buckets: resolve(max_buckets, MAX_BUCKETS_ENV, DEFAULT_MAX_BUCKETS)?,
            max_groups: resolve(max_groups, MAX_GROUPS_ENV, DEFAULT_MAX_GROUPS)?,
        })
    }
}

impl ApiLimits {
    /// The same three rules, over the request limits.
    pub fn load(
        max_body_bytes: Option<usize>,
        max_decompressed_bytes: Option<usize>,
        ingest_concurrency: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let max_decompressed_bytes = resolve(
            max_decompressed_bytes,
            MAX_DECOMPRESSED_BYTES_ENV,
            DEFAULT_MAX_DECOMPRESSED_BYTES,
        )?;
        if max_decompressed_bytes > crate::lines::MAX_LINE_BYTES {
            return Err(ConfigError::DecompressedAboveLineCap {
                asked: max_decompressed_bytes,
                limit: crate::lines::MAX_LINE_BYTES,
            });
        }

        let ingest_concurrency = resolve(
            ingest_concurrency,
            INGEST_CONCURRENCY_ENV,
            default_ingest_concurrency(),
        )?;
        if ingest_concurrency == 0 {
            return Err(ConfigError::NoIngestPermits);
        }

        Ok(Self {
            max_body_bytes: resolve(max_body_bytes, MAX_BODY_BYTES_ENV, DEFAULT_MAX_BODY_BYTES)?,
            max_decompressed_bytes,
            ingest_concurrency,
        })
    }
}

fn resolve(flag: Option<usize>, var: &'static str, default: usize) -> Result<usize, ConfigError> {
    match flag {
        Some(n) => Ok(n),
        None => Ok(env_usize(var)?.unwrap_or(default)),
    }
}

/// Unset is absence; unreadable is an error. A limit that silently ignored the number an
/// operator set is worse than not having one - they would believe it was in force.
fn env_usize(var: &'static str) -> Result<Option<usize>, ConfigError> {
    let Ok(text) = std::env::var(var) else {
        return Ok(None);
    };
    text.trim()
        .parse()
        .map(Some)
        .map_err(|_| ConfigError::BadEnv { var, value: text })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two scan variables as a test found them, put back when it ends.
    struct Kept([(&'static str, Option<String>); 2]);

    impl Kept {
        fn cleared() -> Self {
            let kept =
                [SCAN_THREADS_ENV, SCAN_FAN_ROWS_ENV].map(|var| (var, std::env::var(var).ok()));
            for (var, _) in &kept {
                std::env::remove_var(var);
            }
            Self(kept)
        }
    }

    impl Drop for Kept {
        fn drop(&mut self) {
            for (var, was) in &self.0 {
                match was {
                    Some(value) => std::env::set_var(var, value),
                    None => std::env::remove_var(var),
                }
            }
        }
    }

    /// **What a server's queries may use is the flag, or else the variable, or else the
    /// default; and more threads than the machine has cores are refused.**
    ///
    /// One test, because the variables are the process's: nothing else in this crate's
    /// tests reads these two, and this sets and clears them in turn.
    #[test]
    fn scan_threads_are_the_flag_then_the_variable_then_the_default_and_never_above_the_cores() {
        let _kept = Kept::cleared();
        let cores = cores();
        let scan = |threads: Option<usize>, fan_rows: Option<usize>| {
            Storage::default()
                .with_scan(threads, fan_rows)
                .map(|storage| (storage.scan_threads, storage.scan_fan_rows))
        };

        // Nobody but `with_scan` asks for a thread: a load and a compaction have none.
        let untouched = Storage::default();
        assert_eq!(
            (untouched.scan_threads, untouched.scan_fan_rows),
            (0, crate::store::DEFAULT_FAN_ROWS)
        );
        assert_eq!(untouched.disk_options().scan_threads, 0);

        // The default: half the cores, at least one, at most four, never above the cores.
        let default = default_scan_threads();
        assert!(
            (1..=4).contains(&default) && default <= cores,
            "{default} of {cores}"
        );
        assert_eq!(default, (cores / 2).clamp(1, 4));
        assert_eq!(
            scan(None, None).expect("the defaults"),
            (default, crate::store::DEFAULT_FAN_ROWS)
        );

        // The variable over the default, and the flag over the variable.
        std::env::set_var(SCAN_THREADS_ENV, "1");
        std::env::set_var(SCAN_FAN_ROWS_ENV, " 4096 ");
        assert_eq!(scan(None, None).expect("the variables"), (1, 4096));
        assert_eq!(scan(Some(0), None).expect("a flag"), (0, 4096));
        assert_eq!(scan(None, Some(0)).expect("a flag"), (1, 0));
        assert_eq!(scan(Some(cores), Some(7)).expect("both flags"), (cores, 7));

        // An unreadable variable is refused, unless the flag that overrides it is given.
        std::env::set_var(SCAN_THREADS_ENV, "four");
        assert!(matches!(
            scan(None, Some(7)),
            Err(ConfigError::BadEnv { var, value }) if var == SCAN_THREADS_ENV && value == "four"
        ));
        assert_eq!(scan(Some(1), None).expect("the flag"), (1, 4096));
        std::env::set_var(SCAN_THREADS_ENV, "1");
        std::env::set_var(SCAN_FAN_ROWS_ENV, "-1");
        assert!(matches!(
            scan(None, None),
            Err(ConfigError::BadEnv { var, .. }) if var == SCAN_FAN_ROWS_ENV
        ));
        assert_eq!(scan(None, Some(9)).expect("the flag"), (1, 9));
        std::env::remove_var(SCAN_FAN_ROWS_ENV);

        // As many threads as there are cores are taken; one more is refused, by the flag
        // and by the variable, and the refusal names both numbers. None and one are taken.
        std::env::remove_var(SCAN_THREADS_ENV);
        for few in [0, 1, cores] {
            assert_eq!(scan(Some(few), None).expect("taken").0, few);
            std::env::set_var(SCAN_THREADS_ENV, few.to_string());
            assert_eq!(scan(None, None).expect("taken").0, few);
            std::env::remove_var(SCAN_THREADS_ENV);
        }
        let refused = |got: Result<(usize, u64), ConfigError>, asked: usize| match got {
            Err(e @ ConfigError::ScanThreadsAboveCores { .. }) => {
                let said = e.to_string();
                assert!(
                    said.contains(&format!(
                        "scan threads ({asked}) is above the {cores} core(s)"
                    )),
                    "{said}"
                );
            }
            other => panic!("{asked} threads on {cores} cores: {other:?}"),
        };
        for many in [cores + 1, 512, 100_000] {
            refused(scan(Some(many), None), many);
            std::env::set_var(SCAN_THREADS_ENV, many.to_string());
            refused(scan(None, None), many);
            // And the flag, when it is in range, is not refused for the variable.
            assert_eq!(scan(Some(1), None).expect("the flag").0, 1);
            std::env::remove_var(SCAN_THREADS_ENV);
        }

        // What a server opens its disk store with is what was resolved.
        let options = Storage::default()
            .with_scan(Some(1), Some(12))
            .expect("taken")
            .disk_options();
        assert_eq!((options.scan_threads, options.fan_rows), (1, 12));
    }
}
