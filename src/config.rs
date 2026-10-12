//! Configuration: flags, `KIBBR__` variables and the file, resolved into what a command
//! runs on. For `serve` that is the server crate's `Settings` (`settings`), plus what
//! only this binary has - its own log, and the epoch window, which is the process's.
//!
//! The values are declared where they are read. `Limits` is what a *query* may not exceed
//! and both surfaces carry it - the CLI and the server disagreeing about a backstop would
//! make the same query mean two things; `ApiLimits` is what a *request* may not exceed,
//! and only the server has requests; `Storage` is where rows go, which only the server
//! has too - the CLI rebuilds its database from an input file per invocation and has
//! nothing to keep. This module is where each comes from.
//!
//! **Where a setting comes from, for every one of them: flag > environment > file >
//! default.** The file is the one `--config` names, on any command; with no `--config`
//! there is no file. A key's variable is `KIBBR__`, its section and its name in capitals -
//! `[storage] base_path` is `KIBBR__STORAGE__BASE_PATH` - so the file's keys are the
//! names the variables already had, and a duration that was a whole number of
//! milliseconds under a name ending `_ms` is one in the file too. The one duration that
//! arrived with the file, `[api] shutdown_deadline`, is spelled `"25s"`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use kibbr_core::infer::EpochWindow;

// The two groups of settings the crates below this one read: the query limits ride on
// every `Query`, and the time columns are the trie's. They are declared where they are
// read; this module is still where they are loaded and what the commands name them by.
pub use kibbr_engine::limits::{Limits, DEFAULT_MAX_BUCKETS, DEFAULT_MAX_GROUPS, DEFAULT_MAX_ROWS};
pub use kibbr_storage::trie::{Schema, DEFAULT_TIME_COLUMNS};

// What a server runs on is declared in the server crate, where it is read: the two
// groups below, their defaults and the `Settings` they are fields of. This module is
// still where each is loaded and what the commands name them by.
pub use kibbr_server::settings::{
    default_ingest_concurrency, duration_words, ApiLimits, Banner, Settings, Storage,
    DEFAULT_COMPACT_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS, DEFAULT_HOST,
    DEFAULT_INGEST_WAIT_MS, DEFAULT_MAX_BODY_BYTES, DEFAULT_MAX_DECOMPRESSED_BYTES,
    DEFAULT_REQUEST_TIMEOUT, DEFAULT_ROW_GROUP_TIMEOUT_MS, DEFAULT_SHUTDOWN_DEADLINE,
};
pub use kibbr_server::DEFAULT_PORT;

/// The longest that wait may be set to: thirty seconds.
///
/// A body can be kept waiting twice - at the door, and then for room in the write-ahead
/// log - and each wait is at most the setting. Two of thirty seconds are a minute, which
/// leaves the other minute of the two after which an ordinary shipper gives up for the
/// parse, the log's flush and the store.
pub const MAX_INGEST_WAIT_MS: usize = 30_000;


/// The cores this machine reports; one where it reports none.
fn cores() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// Half the cores, and no more than four: what one query may choose rows on. Half
/// leaves the other half to the runtime and the ingest path. Four is as many fast cores
/// as the machine this was measured on has; more than four was not measured on this scan.
///
/// **The four stays, with the file here to change it.** It is the four performance cores
/// of the machine this is developed and measured on (eight cores, four of them fast); the
/// cloud run has eight alike and passes `--threads 8` - `serve`'s `--scan-threads` under
/// its short name - or sets `[query] scan_threads = 8`. A default that followed the core
/// count there would be a default nothing here has measured.
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
const INGEST_WAIT_ENV: &str = "KIBBR__API__INGEST_WAIT_MS";
const NATIVE_ADDR_ENV: &str = "KIBBR__API__NATIVE_ADDR";
const SHUTDOWN_DEADLINE_ENV: &str = "KIBBR__API__SHUTDOWN_DEADLINE";
const REQUEST_TIMEOUT_ENV: &str = "KIBBR__API__REQUEST_TIMEOUT";
const LOG_LEVEL_ENV: &str = "KIBBR__LOG__LEVEL";
const LOG_FORMAT_ENV: &str = "KIBBR__LOG__FORMAT";
const BASE_PATH_ENV: &str = "KIBBR__STORAGE__BASE_PATH";
const TRIE_FILE_ENV: &str = "KIBBR__STORAGE__TRIE_FILE";
const FLUSH_INTERVAL_ENV: &str = "KIBBR__INGESTOR__FLUSH_INTERVAL_MS";
const ROW_GROUP_TIMEOUT_ENV: &str = "KIBBR__INGESTOR__ROW_GROUP_TIMEOUT_MS";
const MAX_OPEN_SEGMENTS_ENV: &str = "KIBBR__STORAGE__MAX_OPEN_SEGMENTS";
const SEGMENT_MAX_BYTES_ENV: &str = "KIBBR__STORAGE__SEGMENT_MAX_BYTES";
const MAX_BUFFER_BYTES_ENV: &str = "KIBBR__STORAGE__MAX_BUFFER_BYTES";
const COMPACT_INTERVAL_ENV: &str = "KIBBR__STORAGE__COMPACT_INTERVAL_MS";
const WAL_SEGMENT_BYTES_ENV: &str = "KIBBR__STORAGE__WAL_SEGMENT_BYTES";
const WAL_QUEUE_BYTES_ENV: &str = "KIBBR__STORAGE__WAL_QUEUE_BYTES";
const BATCH_SIZE_ENV: &str = "KIBBR__INGESTOR__BATCH_SIZE";
const TIME_COLUMNS_ENV: &str = "KIBBR__SCHEMA__TIME_COLUMNS";
const EPOCH_WINDOW_ENV: &str = "KIBBR__SCHEMA__EPOCH_WINDOW";


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

    /// Refused rather than clamped, as `DecompressedAboveLineCap` is: a wait this long
    /// keeps a body past the point where its sender has given up and sent it again.
    #[error(
        "ingest wait ({asked}ms) is above the {limit}ms limit; a body may wait that long \
         twice, at the door and for the write-ahead log, and a sender kept longer gives up"
    )]
    IngestWaitTooLong { asked: usize, limit: usize },

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

    /// `--config` named a file that could not be read. Never skipped: a server started
    /// with a file it did not read runs under defaults its operator believes they changed.
    #[error("--config {path}: {source}")]
    FileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// The file is not TOML, or holds a key this build has no setting for, or a value of
    /// the wrong kind. The parser's own message, which names the line and the keys the
    /// section has. A key is refused and not skipped for the reason an unreadable
    /// variable is: its writer believes it is in force.
    #[error("--config {path}: {detail}")]
    FileSyntax { path: String, detail: String },

    /// A setting that is not a whole number - an address, a duration, a log level - and
    /// could not be read as what it is. `name` is where it was written: the variable, or
    /// the file's section and key.
    #[error("{name} `{value}`: {reason}")]
    BadSetting {
        name: String,
        value: String,
        reason: String,
    },
}

// ── The file ───────────────────────────────────────────────────────────────────────

/// What `--config` named, as it was read: every key optional, and every key a setting
/// this build has.
///
/// One struct a section and one field a key, each `None` where the file does not say.
/// The loaders below read a field only after the flag and the variable have both said
/// nothing, so a file never overrides either and an absent file is this struct's
/// default.
///
/// **A key that is not below is refused** (`deny_unknown_fields`), with the line it is on
/// and the keys its section has. The design was written before most of these were
/// built and names some that never were - `wal_dir`, `[cold_tier]`,
/// the Elasticsearch address, the merge threshold, the promoted keys: a file that sets
/// one is told so, where a key read and dropped would be a setting its writer believes
/// is in force.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
    #[serde(default)]
    pub storage: StorageKeys,
    #[serde(default)]
    pub ingestor: IngestorKeys,
    #[serde(default)]
    pub api: ApiKeys,
    #[serde(default)]
    pub query: QueryKeys,
    #[serde(default)]
    pub schema: SchemaKeys,
    #[serde(default)]
    pub log: LogKeys,
}

/// `[storage]`: where rows go and what the files and the log are held to.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageKeys {
    /// The data directory: `--data-dir`.
    pub base_path: Option<PathBuf>,
    pub trie_file: Option<PathBuf>,
    pub max_open_segments: Option<usize>,
    pub segment_max_bytes: Option<usize>,
    pub max_buffer_bytes: Option<usize>,
    pub compact_interval_ms: Option<usize>,
    pub wal_segment_bytes: Option<usize>,
    pub wal_queue_bytes: Option<usize>,
}

/// `[ingestor]`: the flush tick, the row group's wait, and one flush of the log.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestorKeys {
    pub flush_interval_ms: Option<usize>,
    pub row_group_timeout_ms: Option<usize>,
    pub batch_size: Option<usize>,
}

/// `[api]`: the listener and what a request is held to.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKeys {
    /// `host:port`. `--host` and `--port` each override their half.
    pub native_addr: Option<String>,
    pub max_body_bytes: Option<usize>,
    pub max_decompressed_bytes: Option<usize>,
    pub ingest_concurrency: Option<usize>,
    pub ingest_wait_ms: Option<usize>,
    /// A duration: `"25s"`. `"0s"` is no deadline.
    pub shutdown_deadline: Option<String>,
    /// A duration: `"30s"`. `"0s"` is no timeout.
    pub request_timeout: Option<String>,
}

/// `[query]`: what a query may not exceed, and the threads it may use.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryKeys {
    pub max_rows: Option<usize>,
    pub max_buckets: Option<usize>,
    pub max_groups: Option<usize>,
    pub scan_threads: Option<usize>,
    pub scan_fan_rows: Option<usize>,
}

/// `[schema]`: what the type ladder and the trie are told about the data.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaKeys {
    pub time_columns: Option<Vec<String>>,
    /// Two RFC 3339 instants, start before end.
    pub epoch_window: Option<Vec<String>>,
}

/// `[log]`: kibbr's own log, not the logs it stores.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogKeys {
    pub level: Option<String>,
    pub format: Option<String>,
}

impl File {
    /// The file `--config` named, or - with none named - no file: every key absent.
    ///
    /// There is no path this looks at unasked. A `kibbr.toml` picked up from whatever
    /// directory a command was run in would be a setting nobody typed.
    pub fn read(path: Option<&Path>) -> Result<Self, ConfigError> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::FileRead {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&text).map_err(|detail| ConfigError::FileSyntax {
            path: path.display().to_string(),
            detail,
        })
    }

    /// The file's text, read. The error is the parser's own message.
    pub fn parse(text: &str) -> Result<Self, String> {
        toml::from_str(text).map_err(|e| e.to_string().trim_end().to_string())
    }
}

/// A duration as the file and its variable spell one: a whole number and `ms`, `s`, `m`
/// or `h`, with nothing between - `"25s"`, `"500ms"`. The unit is not optional: a bare
/// `25` would be seconds to one reader and milliseconds to the next.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    let (number, unit) = text.split_at(digits);
    let expected = "expected a whole number and a unit - ms, s, m or h - as in `25s`";
    let number: u64 = number.parse().map_err(|_| expected.to_string())?;
    let millis = match unit {
        "ms" => Some(number),
        "s" => number.checked_mul(1_000),
        "m" => number.checked_mul(60_000),
        "h" => number.checked_mul(3_600_000),
        _ => return Err(expected.to_string()),
    };
    millis
        .map(Duration::from_millis)
        .ok_or_else(|| "longer than a duration can hold".to_string())
}


/// A setting that is text: flag > environment > file, each with the name it was written
/// under, for the error that quotes it.
fn text_setting(
    flag: Option<String>,
    var: &'static str,
    file: Option<&String>,
    key: &'static str,
) -> Option<(String, String)> {
    if let Some(value) = flag {
        return Some((key.to_string(), value));
    }
    if let Ok(value) = std::env::var(var) {
        return Some((var.to_string(), value));
    }
    file.map(|value| (key.to_string(), value.clone()))
}

/// Where `serve` listens: `--host` and `--port`, each over its half of the address in
/// `KIBBR__API__NATIVE_ADDR`, over `[api] native_addr`, over loopback and 7700.
pub fn listen(
    file: &File,
    host: Option<String>,
    port: Option<u16>,
) -> Result<(String, u16), ConfigError> {
    let named = text_setting(
        None,
        NATIVE_ADDR_ENV,
        file.api.native_addr.as_ref(),
        "[api] native_addr",
    );
    let (addr_host, addr_port) = match named {
        None => (None, None),
        Some((name, value)) => {
            let bad = |reason: &str| ConfigError::BadSetting {
                name: name.clone(),
                value: value.clone(),
                reason: reason.to_string(),
            };
            let (host, port) = value
                .rsplit_once(':')
                .ok_or_else(|| bad("expected host:port, as in `0.0.0.0:7700`"))?;
            let port: u16 = port
                .parse()
                .map_err(|_| bad("the port is not a number from 0 to 65535"))?;
            if host.is_empty() {
                return Err(bad("the host is empty"));
            }
            (Some(host.to_string()), Some(port))
        }
    };
    Ok((
        host.or(addr_host)
            .unwrap_or_else(|| DEFAULT_HOST.to_string()),
        port.or(addr_port).unwrap_or(DEFAULT_PORT),
    ))
}

/// How loud kibbr's own log is. The levels are `tracing`'s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// How a log line is written: a line of text for a person at a terminal, or one JSON
/// object a line for whatever collects a container's stderr.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

/// `[log]`, resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Log {
    pub level: LogLevel,
    pub format: LogFormat,
}

impl Default for Log {
    /// `info`, as text. Text and not JSON by default, where the design's sample file
    /// says `json`: the lines a terminal, the scripts and `kibbr bench` read off a
    /// server's stderr are the text ones, and a container says `format = "json"` once.
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Text,
        }
    }
}

/// The log's settings: `--log-level` and `--log-format`, each over its variable, over
/// `[log]`, over `info` and `text`.
pub fn log(
    file: &File,
    level: Option<LogLevel>,
    format: Option<LogFormat>,
) -> Result<Log, ConfigError> {
    fn named<T: clap::ValueEnum>(
        var: &'static str,
        file: Option<&String>,
        key: &'static str,
    ) -> Result<Option<T>, ConfigError> {
        let Some((name, value)) = text_setting(None, var, file, key) else {
            return Ok(None);
        };
        match T::from_str(value.trim(), true) {
            Ok(read) => Ok(Some(read)),
            Err(_) => {
                let known: Vec<String> = T::value_variants()
                    .iter()
                    .filter_map(|v| v.to_possible_value())
                    .map(|v| v.get_name().to_string())
                    .collect();
                Err(ConfigError::BadSetting {
                    name,
                    value,
                    reason: format!("expected one of {}", known.join(", ")),
                })
            }
        }
    }
    let default = Log::default();
    let level = match level {
        Some(level) => level,
        None => {
            named(LOG_LEVEL_ENV, file.log.level.as_ref(), "[log] level")?.unwrap_or(default.level)
        }
    };
    let format = match format {
        Some(format) => format,
        None => named(LOG_FORMAT_ENV, file.log.format.as_ref(), "[log] format")?
            .unwrap_or(default.format),
    };
    Ok(Log { level, format })
}

/// Which key names may carry a template's time: flag > environment > file > default.
/// The variable is comma-separated; blanks are dropped so a trailing comma is not an
/// empty column name.
pub fn schema(file: &File, time_columns: Option<Vec<String>>) -> Schema {
    let from_env = || {
        std::env::var(TIME_COLUMNS_ENV).ok().map(|text| {
            text.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect::<Vec<String>>()
        })
    };
    let from_file = || file.schema.time_columns.clone();
    let mut schema = Schema::default();
    if let Some(list) = time_columns.or_else(from_env).or_else(from_file) {
        schema.time_columns = list;
    }
    schema
}

/// The window ten or thirteen digits must name a moment inside to be read as a timestamp:
/// environment > file > default. The variable is two RFC 3339 instants, comma-separated -
/// neither contains a comma - and the file's key is a list of the two; the window runs
/// from the first up to but not including the second.
///
/// Not a field of `Schema`, because the ladder cannot be handed one: it is a free function
/// the flattener, the store and the engine all call. `main` installs this once for the
/// process instead, before any command types a value.
pub fn epoch_window(file: &File) -> Result<EpochWindow, ConfigError> {
    let (var, text, ends): (&'static str, String, Vec<String>) =
        match std::env::var_os(EPOCH_WINDOW_ENV) {
            // Unset is absence; anything else is a window an operator believes is in
            // force, so a value that is not even UTF-8 is an error rather than a silent
            // fall back to the file or the default.
            Some(raw) => {
                let text = raw.to_string_lossy().into_owned();
                if raw.to_str().is_none() {
                    return Err(ConfigError::BadEpochWindow {
                        var: EPOCH_WINDOW_ENV,
                        value: text,
                        reason: "the value is not UTF-8".into(),
                    });
                }
                let ends = text.split(',').map(|s| s.trim().to_string()).collect();
                (EPOCH_WINDOW_ENV, text, ends)
            }
            None => match &file.schema.epoch_window {
                Some(ends) => ("[schema] epoch_window", ends.join(","), ends.clone()),
                None => return Ok(EpochWindow::default()),
            },
        };
    let bad = |reason: String| ConfigError::BadEpochWindow {
        var,
        value: text.clone(),
        reason,
    };
    let [start, end] = &ends[..] else {
        return Err(bad(format!("{} comma-separated values, not 2", ends.len())));
    };
    let instant = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s)
            .map_err(|e| bad(format!("`{s}` is not an RFC 3339 instant ({e})")))
    };
    EpochWindow::new(instant(start)?, instant(end)?)
        .ok_or_else(|| bad("the start is not before the end, so the window is empty".into()))
}

/// How `Storage` is loaded: flag > environment > file > default.
///
/// The struct is the server crate's - it is what a server runs on - and what resolves it
/// is this module's. A trait, because an inherent `impl` cannot be written here for a
/// type of another crate; `Storage` is its one implementor, and each method is described
/// where it is written, below.
pub trait LoadStorage: Sized {
    #[allow(clippy::too_many_arguments)]
    fn load(
        file: &File,
        durable: bool,
        data_dir: Option<PathBuf>,
        trie_file: Option<PathBuf>,
        flush_ms: Option<usize>,
        row_group_ms: Option<usize>,
        max_open_segments: Option<usize>,
        segment_max_bytes: Option<usize>,
        max_buffer_bytes: Option<usize>,
    ) -> Result<Self, ConfigError>;
    fn base_named(file: &File, data_dir: Option<PathBuf>) -> PathBuf;
    fn with_compact_interval(self, file: &File, flag_ms: Option<usize>)
        -> Result<Self, ConfigError>;
    fn with_wal(
        self,
        file: &File,
        segment_bytes: Option<usize>,
        batch_lines: Option<usize>,
        queue_bytes: Option<usize>,
    ) -> Result<Self, ConfigError>;
    fn with_scan(
        self,
        file: &File,
        threads: Option<usize>,
        fan_rows: Option<usize>,
    ) -> Result<Self, ConfigError>;
}

impl LoadStorage for Storage {
    /// The same four rules as the other two, over the storage settings.
    ///
    /// `durable` is the `--storage` choice rather than a path being present, so that
    /// `--storage memory --data-dir /x` is a clear instruction and not a contradiction
    /// resolved by whichever flag the code happened to read first.
    #[allow(clippy::too_many_arguments)]
    fn load(
        file: &File,
        durable: bool,
        data_dir: Option<PathBuf>,
        trie_file: Option<PathBuf>,
        flush_ms: Option<usize>,
        row_group_ms: Option<usize>,
        max_open_segments: Option<usize>,
        segment_max_bytes: Option<usize>,
        max_buffer_bytes: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let flush_ms = resolve(
            flush_ms,
            FLUSH_INTERVAL_ENV,
            file.ingestor.flush_interval_ms,
            DEFAULT_FLUSH_INTERVAL_MS,
        )?;
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
            file.ingestor.row_group_timeout_ms,
            DEFAULT_ROW_GROUP_TIMEOUT_MS,
        )?
        .max(flush_ms);
        let row_group_timeout = Duration::from_millis(row_group_ms as u64);

        let max_open_segments = resolve(
            max_open_segments,
            MAX_OPEN_SEGMENTS_ENV,
            file.storage.max_open_segments,
            kibbr_storage::store::DEFAULT_MAX_OPEN_SEGMENTS,
        )?;
        if max_open_segments == 0 {
            return Err(ConfigError::NoOpenSegments);
        }
        let bytes = |flag, var, in_file, default: u64, what| match resolve(
            flag,
            var,
            in_file,
            default as usize,
        )? {
            0 => Err(ConfigError::NoBytes { what }),
            n => Ok(n as u64),
        };
        let segment_max_bytes = bytes(
            segment_max_bytes,
            SEGMENT_MAX_BYTES_ENV,
            file.storage.segment_max_bytes,
            kibbr_storage::store::disk::DEFAULT_SEGMENT_MAX_BYTES,
            "segment max bytes",
        )?;
        let max_buffer_bytes = bytes(
            max_buffer_bytes,
            MAX_BUFFER_BYTES_ENV,
            file.storage.max_buffer_bytes,
            kibbr_storage::store::DEFAULT_MAX_BUFFER_BYTES,
            "max buffer bytes",
        )?;
        // The default here, and the variable not read: only `serve` looks for days that
        // are due, and it resolves its flag over the variable in `with_compact_interval` -
        // read here, a malformed variable would refuse the flag that overrides it, and
        // every command that never looks.
        let compact_interval = Duration::from_millis(DEFAULT_COMPACT_INTERVAL_MS as u64);

        // The defaults here too, for the reason the compaction interval's is: only `serve`
        // writes a log, and it resolves its flags over the variables in `with_wal`.
        let wal_segment_bytes = kibbr_storage::wal::DEFAULT_SEGMENT_BYTES;
        let wal_batch_lines = kibbr_storage::wal::DEFAULT_BATCH_LINES;
        let wal_queue_bytes = kibbr_storage::wal::DEFAULT_QUEUE_BYTES;

        // No scan threads here, for the same reason again: only `serve` answers a query
        // from a disk store, and it asks for its threads in `with_scan`. A load and a
        // compaction start none.
        let scan_threads = 0;
        let scan_fan_rows = kibbr_storage::store::DEFAULT_FAN_ROWS;

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
                wal_queue_bytes,
                scan_threads,
                scan_fan_rows,
            });
        }

        let base = Self::base_named(file, data_dir);
        // Created and canonicalised now, before a line is accepted: an unwritable data
        // directory should stop the process at startup, where an operator is watching,
        // rather than at the first flush tick half a minute later. Absolute afterwards, so
        // nothing that changes the working directory can move the database.
        std::fs::create_dir_all(&base).map_err(|source| ConfigError::BadDataDir {
            path: base.display().to_string(),
            source,
        })?;
        let base = std::fs::canonicalize(&base).unwrap_or(base);

        let trie_file = resolve_path(trie_file, TRIE_FILE_ENV, file.storage.trie_file.as_ref())
            .unwrap_or_else(|| base.join(TRIE_FILE_NAME));

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
            wal_queue_bytes,
            scan_threads,
            scan_fan_rows,
        })
    }

    /// The data directory a command was pointed at - flag > environment > file > default - as it
    /// was named, and **without creating it**. `load` creates what this names, which is
    /// right for a command that fills a directory; one that only works on a directory
    /// already there asks this first, so a mistyped path is refused and not made.
    fn base_named(file: &File, data_dir: Option<PathBuf>) -> PathBuf {
        resolve_path(data_dir, BASE_PATH_ENV, file.storage.base_path.as_ref())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR))
    }

    /// How often `serve` looks for a day that is due: `--compact-interval-ms` >
    /// environment > default. Apart from `load` because only `serve` has the flag and
    /// only `serve` looks: a ninth positional `None` at every other caller would say
    /// nothing, and the variable is read here so that the flag, when given, is never
    /// refused for a variable it overrides.
    fn with_compact_interval(
        mut self,
        file: &File,
        flag_ms: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let ms = resolve(
            flag_ms,
            COMPACT_INTERVAL_ENV,
            file.storage.compact_interval_ms,
            DEFAULT_COMPACT_INTERVAL_MS,
        )?;
        self.compact_interval = Duration::from_millis(ms as u64);
        Ok(self)
    }

    /// How `serve` writes its write-ahead log: `--wal-segment-bytes`, `--batch-size` and
    /// `--wal-queue-bytes`, each over its variable, over the default. Apart from `load`
    /// for the reason `with_compact_interval` is: only `serve` has the flags, and only
    /// `serve` writes one.
    fn with_wal(
        mut self,
        file: &File,
        segment_bytes: Option<usize>,
        batch_lines: Option<usize>,
        queue_bytes: Option<usize>,
    ) -> Result<Self, ConfigError> {
        self.wal_segment_bytes = match resolve(
            segment_bytes,
            WAL_SEGMENT_BYTES_ENV,
            file.storage.wal_segment_bytes,
            kibbr_storage::wal::DEFAULT_SEGMENT_BYTES as usize,
        )? {
            0 => {
                return Err(ConfigError::NoBytes {
                    what: "wal segment bytes",
                })
            }
            n => n as u64,
        };
        self.wal_batch_lines = match resolve(
            batch_lines,
            BATCH_SIZE_ENV,
            file.ingestor.batch_size,
            kibbr_storage::wal::DEFAULT_BATCH_LINES,
        )? {
            0 => return Err(ConfigError::NoBatch),
            n => n,
        };
        self.wal_queue_bytes = match resolve(
            queue_bytes,
            WAL_QUEUE_BYTES_ENV,
            file.storage.wal_queue_bytes,
            kibbr_storage::wal::DEFAULT_QUEUE_BYTES,
        )? {
            0 => {
                return Err(ConfigError::NoBytes {
                    what: "wal queue bytes",
                })
            }
            n => n,
        };
        Ok(self)
    }

    /// What a server's queries may use: `--scan-threads` and `--scan-fan-rows`, each over
    /// its variable, over the default. Apart from `load` for the reason `with_wal` is:
    /// only `serve` answers a query from a disk store, so only it has the flags, and a
    /// load or a compaction starts no scan thread. More threads than the machine has
    /// cores are refused; 0 and 1 are both "the query's own thread and no other".
    fn with_scan(
        mut self,
        file: &File,
        threads: Option<usize>,
        fan_rows: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let cores = cores();
        self.scan_threads = match resolve(
            threads,
            SCAN_THREADS_ENV,
            file.query.scan_threads,
            default_scan_threads(),
        )? {
            asked if asked > cores => {
                return Err(ConfigError::ScanThreadsAboveCores { asked, cores })
            }
            n => n,
        };
        self.scan_fan_rows = resolve(
            fan_rows,
            SCAN_FAN_ROWS_ENV,
            file.query.scan_fan_rows,
            kibbr_storage::store::DEFAULT_FAN_ROWS as usize,
        )? as u64;
        Ok(self)
    }
}


/// Flag > environment > file > absent. The same rule as `resolve`, over a path.
fn resolve_path(
    flag: Option<PathBuf>,
    var: &'static str,
    file: Option<&PathBuf>,
) -> Option<PathBuf> {
    flag.or_else(|| std::env::var_os(var).map(PathBuf::from))
        .or_else(|| file.cloned())
}


/// The query limits: flag > environment > file > default, one rule for each.
pub fn limits(
    file: &File,
    max_rows: Option<usize>,
    max_buckets: Option<usize>,
    max_groups: Option<usize>,
) -> Result<Limits, ConfigError> {
    Ok(Limits {
        max_rows: resolve(
            max_rows,
            MAX_ROWS_ENV,
            file.query.max_rows,
            DEFAULT_MAX_ROWS,
        )?,
        max_buckets: resolve(
            max_buckets,
            MAX_BUCKETS_ENV,
            file.query.max_buckets,
            DEFAULT_MAX_BUCKETS,
        )?,
        max_groups: resolve(
            max_groups,
            MAX_GROUPS_ENV,
            file.query.max_groups,
            DEFAULT_MAX_GROUPS,
        )?,
    })
}

/// How `ApiLimits` is loaded: as `LoadStorage` is, and a trait for the same reason.
pub trait LoadApiLimits: Sized {
    fn load(
        file: &File,
        max_body_bytes: Option<usize>,
        max_decompressed_bytes: Option<usize>,
        ingest_concurrency: Option<usize>,
    ) -> Result<Self, ConfigError>;
    fn with_ingest_wait(self, file: &File, flag_ms: Option<usize>) -> Result<Self, ConfigError>;
    fn with_shutdown_deadline(
        self,
        file: &File,
        flag: Option<Duration>,
    ) -> Result<Self, ConfigError>;
    fn with_request_timeout(
        self,
        file: &File,
        flag: Option<Duration>,
    ) -> Result<Self, ConfigError>;
}

impl LoadApiLimits for ApiLimits {
    /// The same four rules, over the request limits.
    fn load(
        file: &File,
        max_body_bytes: Option<usize>,
        max_decompressed_bytes: Option<usize>,
        ingest_concurrency: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let max_decompressed_bytes = resolve(
            max_decompressed_bytes,
            MAX_DECOMPRESSED_BYTES_ENV,
            file.api.max_decompressed_bytes,
            DEFAULT_MAX_DECOMPRESSED_BYTES,
        )?;
        if max_decompressed_bytes > kibbr_core::lines::MAX_LINE_BYTES {
            return Err(ConfigError::DecompressedAboveLineCap {
                asked: max_decompressed_bytes,
                limit: kibbr_core::lines::MAX_LINE_BYTES,
            });
        }

        let ingest_concurrency = resolve(
            ingest_concurrency,
            INGEST_CONCURRENCY_ENV,
            file.api.ingest_concurrency,
            default_ingest_concurrency(),
        )?;
        if ingest_concurrency == 0 {
            return Err(ConfigError::NoIngestPermits);
        }

        Ok(Self {
            max_body_bytes: resolve(
                max_body_bytes,
                MAX_BODY_BYTES_ENV,
                file.api.max_body_bytes,
                DEFAULT_MAX_BODY_BYTES,
            )?,
            max_decompressed_bytes,
            ingest_concurrency,
            // The defaults, and the variables not read: see `with_ingest_wait`,
            // `with_shutdown_deadline` and `with_request_timeout`.
            ingest_wait: Duration::from_millis(DEFAULT_INGEST_WAIT_MS as u64),
            shutdown_deadline: Some(DEFAULT_SHUTDOWN_DEADLINE),
            request_timeout: Some(DEFAULT_REQUEST_TIMEOUT),
        })
    }

    /// How long `serve` keeps a body waiting before it sheds it: `--ingest-wait-ms` >
    /// environment > default. Apart from `load` for the reason `Storage::with_wal` is:
    /// only `serve` has the flag. Zero is taken - wait for nothing, shed at once - and
    /// more than `MAX_INGEST_WAIT_MS` is refused.
    fn with_ingest_wait(
        mut self,
        file: &File,
        flag_ms: Option<usize>,
    ) -> Result<Self, ConfigError> {
        let ms = resolve(
            flag_ms,
            INGEST_WAIT_ENV,
            file.api.ingest_wait_ms,
            DEFAULT_INGEST_WAIT_MS,
        )?;
        if ms > MAX_INGEST_WAIT_MS {
            return Err(ConfigError::IngestWaitTooLong {
                asked: ms,
                limit: MAX_INGEST_WAIT_MS,
            });
        }
        self.ingest_wait = Duration::from_millis(ms as u64);
        Ok(self)
    }

    /// How long a stopping `serve` gives the requests in flight: `--shutdown-deadline` >
    /// environment > `[api] shutdown_deadline` > 25 seconds. Apart from `load` for the
    /// reason `with_ingest_wait` is. A duration of zero, in any unit, is no deadline:
    /// every request is waited for, as it was before there was one.
    fn with_shutdown_deadline(
        mut self,
        file: &File,
        flag: Option<Duration>,
    ) -> Result<Self, ConfigError> {
        let deadline = duration_setting(
            flag,
            SHUTDOWN_DEADLINE_ENV,
            file.api.shutdown_deadline.as_ref(),
            "[api] shutdown_deadline",
            DEFAULT_SHUTDOWN_DEADLINE,
        )?;
        self.shutdown_deadline = (!deadline.is_zero()).then_some(deadline);
        Ok(self)
    }

    /// How long `serve` gives one request: `--request-timeout` > environment > `[api]
    /// request_timeout` > 30 seconds. Apart from `load` for the reason `with_ingest_wait`
    /// is. A duration of zero, in any unit, is no timeout, as it was before there was one.
    fn with_request_timeout(
        mut self,
        file: &File,
        flag: Option<Duration>,
    ) -> Result<Self, ConfigError> {
        let timeout = duration_setting(
            flag,
            REQUEST_TIMEOUT_ENV,
            file.api.request_timeout.as_ref(),
            "[api] request_timeout",
            DEFAULT_REQUEST_TIMEOUT,
        )?;
        self.request_timeout = (!timeout.is_zero()).then_some(timeout);
        Ok(self)
    }
}

/// `serve`'s own flags, as clap read them: each `None` where the command line did not
/// say, so that its variable, the file and the default are asked in turn.
#[derive(Debug, Clone, Default)]
pub struct ServeFlags {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub max_rows: Option<usize>,
    pub max_buckets: Option<usize>,
    pub max_groups: Option<usize>,
    pub max_body_bytes: Option<usize>,
    pub max_decompressed_bytes: Option<usize>,
    pub ingest_concurrency: Option<usize>,
    pub ingest_wait_ms: Option<usize>,
    pub shutdown_deadline: Option<Duration>,
    pub request_timeout: Option<Duration>,
    /// `--storage memory`: no data directory, whatever else names one.
    pub memory: bool,
    pub data_dir: Option<PathBuf>,
    pub trie_file: Option<PathBuf>,
    pub flush_interval_ms: Option<usize>,
    pub row_group_timeout_ms: Option<usize>,
    pub wal_segment_bytes: Option<usize>,
    pub batch_size: Option<usize>,
    pub wal_queue_bytes: Option<usize>,
    pub max_open_segments: Option<usize>,
    pub segment_max_bytes: Option<usize>,
    pub max_buffer_bytes: Option<usize>,
    pub compact_interval_ms: Option<usize>,
    pub scan_threads: Option<usize>,
    pub scan_fan_rows: Option<usize>,
    pub time_columns: Option<Vec<String>>,
}

/// What `kibbr serve` runs on: every flag over its variable, over the file, over the
/// default - the one place the server's `Settings` is made from them. `log` is this
/// binary's own setting and already resolved; the server is told only how to say it has
/// started, since as JSON that is an event and as text a paragraph.
///
/// In the order the settings were always resolved in, so the first one that is refused
/// is the one that was: the address, the query limits, the request limits, the storage -
/// whose directory is made here - and the schema.
pub fn settings(file: &File, flags: ServeFlags, log: Log) -> Result<Settings, ConfigError> {
    let (host, port) = listen(file, flags.host, flags.port)?;
    Ok(Settings {
        host,
        port,
        limits: limits(file, flags.max_rows, flags.max_buckets, flags.max_groups)?,
        api: ApiLimits::load(
            file,
            flags.max_body_bytes,
            flags.max_decompressed_bytes,
            flags.ingest_concurrency,
        )?
        .with_ingest_wait(file, flags.ingest_wait_ms)?
        .with_shutdown_deadline(file, flags.shutdown_deadline)?
        .with_request_timeout(file, flags.request_timeout)?,
        storage: Storage::load(
            file,
            !flags.memory,
            flags.data_dir,
            flags.trie_file,
            flags.flush_interval_ms,
            flags.row_group_timeout_ms,
            flags.max_open_segments,
            flags.segment_max_bytes,
            flags.max_buffer_bytes,
        )?
        .with_compact_interval(file, flags.compact_interval_ms)?
        .with_wal(
            file,
            flags.wal_segment_bytes,
            flags.batch_size,
            flags.wal_queue_bytes,
        )?
        .with_scan(file, flags.scan_threads, flags.scan_fan_rows)?,
        schema: schema(file, flags.time_columns),
        banner: match log.format {
            LogFormat::Text => Banner::Paragraph,
            LogFormat::Json => Banner::Event,
        },
    })
}

/// Flag > environment > file > default, over a duration: the flag already one - clap
/// read it with `parse_duration` - and the variable and the file still its text.
fn duration_setting(
    flag: Option<Duration>,
    var: &'static str,
    file: Option<&String>,
    key: &'static str,
    default: Duration,
) -> Result<Duration, ConfigError> {
    if let Some(flag) = flag {
        return Ok(flag);
    }
    match text_setting(None, var, file, key) {
        None => Ok(default),
        Some((name, value)) => parse_duration(&value).map_err(|reason| ConfigError::BadSetting {
            name,
            value,
            reason,
        }),
    }
}

/// Flag > environment > file > default: the one rule, over a whole number.
fn resolve(
    flag: Option<usize>,
    var: &'static str,
    file: Option<usize>,
    default: usize,
) -> Result<usize, ConfigError> {
    match flag {
        Some(n) => Ok(n),
        None => Ok(env_usize(var)?.or(file).unwrap_or(default)),
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

    /// Variables as a test found them, cleared for it and put back when it ends.
    ///
    /// **And one such test at a time.** The variables are the process's, and a test that
    /// reads every key of a file with none set cannot run beside one that sets a
    /// variable to see it win; each holds this for as long as it has them cleared.
    struct Cleared(
        Vec<(&'static str, Option<String>)>,
        #[allow(dead_code)] std::sync::MutexGuard<'static, ()>,
    );

    static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

    impl Cleared {
        fn of(vars: &[&'static str]) -> Self {
            // Through a poisoning: a test that failed holding it has put its variables
            // back in `drop`, and the next one clears its own.
            let turn = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
            let kept = vars.iter().map(|var| (*var, std::env::var(var).ok()));
            let kept: Vec<_> = kept.collect();
            for (var, _) in &kept {
                std::env::remove_var(var);
            }
            Self(kept, turn)
        }
    }

    impl Drop for Cleared {
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
        let _cleared = Cleared::of(&[SCAN_THREADS_ENV, SCAN_FAN_ROWS_ENV]);
        let cores = cores();
        let scan = |threads: Option<usize>, fan_rows: Option<usize>| {
            Storage::default()
                .with_scan(&File::default(), threads, fan_rows)
                .map(|storage| (storage.scan_threads, storage.scan_fan_rows))
        };

        // Nobody but `with_scan` asks for a thread: a load and a compaction have none.
        let untouched = Storage::default();
        assert_eq!(
            (untouched.scan_threads, untouched.scan_fan_rows),
            (0, kibbr_storage::store::DEFAULT_FAN_ROWS)
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
            (default, kibbr_storage::store::DEFAULT_FAN_ROWS)
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
            .with_scan(&File::default(), Some(1), Some(12))
            .expect("taken")
            .disk_options();
        assert_eq!((options.scan_threads, options.fan_rows), (1, 12));
    }

    /// **How long a body may be kept waiting, and how much may wait on the log, are each
    /// the flag, or else the variable, or else what they were before they could be set.**
    /// A wait of nothing is taken; one past thirty seconds is refused and not clamped. A
    /// bound of nothing on the log is refused: every body after the first would be one
    /// too many.
    ///
    /// One test for the two, because the variables are the process's: nothing else in
    /// this crate's tests reads them, or the five beside them that the functions asked
    /// here read as well - cleared too, so that a shell with one of those set to
    /// something unreadable does not fail a test of these two.
    #[test]
    fn the_ingest_wait_and_the_logs_bound_are_the_flag_then_the_variable_then_the_default() {
        let _cleared = Cleared::of(&[
            INGEST_WAIT_ENV,
            WAL_QUEUE_BYTES_ENV,
            WAL_SEGMENT_BYTES_ENV,
            BATCH_SIZE_ENV,
            MAX_BODY_BYTES_ENV,
            MAX_DECOMPRESSED_BYTES_ENV,
            INGEST_CONCURRENCY_ENV,
        ]);
        let wait = |flag: Option<usize>| {
            ApiLimits::default()
                .with_ingest_wait(&File::default(), flag)
                .map(|api| api.ingest_wait)
        };
        let ms = |ms: usize| Duration::from_millis(ms as u64);

        // Nobody but `with_ingest_wait` reads the variable: `load` gives the default.
        std::env::set_var(INGEST_WAIT_ENV, "7");
        let loaded = ApiLimits::load(&File::default(), None, None, None).expect("the defaults");
        assert_eq!(loaded.ingest_wait, ms(DEFAULT_INGEST_WAIT_MS));
        assert_eq!(ApiLimits::default().ingest_wait, ms(100));
        std::env::remove_var(INGEST_WAIT_ENV);

        assert_eq!(wait(None).expect("the default"), ms(100));
        std::env::set_var(INGEST_WAIT_ENV, " 250 ");
        assert_eq!(wait(None).expect("the variable"), ms(250));
        assert_eq!(wait(Some(5)).expect("the flag"), ms(5));
        // No wait at all is a setting, by the flag and by the variable.
        assert_eq!(wait(Some(0)).expect("the flag"), Duration::ZERO);
        std::env::set_var(INGEST_WAIT_ENV, "0");
        assert_eq!(wait(None).expect("the variable"), Duration::ZERO);

        // Thirty seconds is taken, and a millisecond more is refused with both numbers.
        std::env::remove_var(INGEST_WAIT_ENV);
        assert_eq!(wait(Some(MAX_INGEST_WAIT_MS)).expect("taken"), ms(30_000));
        let refused = |got: Result<Duration, ConfigError>| match got {
            Err(e @ ConfigError::IngestWaitTooLong { .. }) => {
                let said = e.to_string();
                assert!(
                    said.contains("ingest wait (30001ms) is above the 30000ms limit"),
                    "{said}"
                );
            }
            other => panic!("a wait of 30001ms: {other:?}"),
        };
        refused(wait(Some(30_001)));
        std::env::set_var(INGEST_WAIT_ENV, "30001");
        refused(wait(None));
        // The flag, when it is in range, is not refused for the variable it overrides,
        // and an unreadable variable is refused unless the flag is given.
        assert_eq!(wait(Some(1)).expect("the flag"), ms(1));
        std::env::set_var(INGEST_WAIT_ENV, "soon");
        assert!(matches!(
            wait(None),
            Err(ConfigError::BadEnv { var, value }) if var == INGEST_WAIT_ENV && value == "soon"
        ));
        assert_eq!(wait(Some(1)).expect("the flag"), ms(1));
        std::env::remove_var(INGEST_WAIT_ENV);

        // The log's bound.
        let bound = |flag: Option<usize>| {
            Storage::default()
                .with_wal(&File::default(), None, None, flag)
                .map(|storage| (storage.wal_queue_bytes, storage.wal_options().queue_bytes))
        };
        let default = kibbr_storage::wal::DEFAULT_QUEUE_BYTES;
        assert_eq!(default, 256 * 1024 * 1024);
        assert_eq!(Storage::default().wal_queue_bytes, default);
        assert_eq!(bound(None).expect("the default"), (default, default));
        std::env::set_var(WAL_QUEUE_BYTES_ENV, "4096");
        assert_eq!(bound(None).expect("the variable"), (4096, 4096));
        assert_eq!(bound(Some(1)).expect("the flag"), (1, 1));
        let no_bytes = |got: Result<(usize, usize), ConfigError>| match got {
            Err(e @ ConfigError::NoBytes { .. }) => {
                let said = e.to_string();
                assert!(
                    said.starts_with("wal queue bytes must be at least 1 byte"),
                    "{said}"
                );
            }
            other => panic!("a bound of nothing: {other:?}"),
        };
        no_bytes(bound(Some(0)));
        std::env::set_var(WAL_QUEUE_BYTES_ENV, "0");
        no_bytes(bound(None));
        assert_eq!(bound(Some(2)).expect("the flag"), (2, 2));
    }

    /// Every variable a key of the file has, so a test of the file is not answered by
    /// whatever the machine it runs on has set.
    const FILE_VARS: &[&str] = &[
        BASE_PATH_ENV,
        TRIE_FILE_ENV,
        MAX_OPEN_SEGMENTS_ENV,
        SEGMENT_MAX_BYTES_ENV,
        MAX_BUFFER_BYTES_ENV,
        COMPACT_INTERVAL_ENV,
        WAL_SEGMENT_BYTES_ENV,
        WAL_QUEUE_BYTES_ENV,
        FLUSH_INTERVAL_ENV,
        ROW_GROUP_TIMEOUT_ENV,
        BATCH_SIZE_ENV,
        NATIVE_ADDR_ENV,
        MAX_BODY_BYTES_ENV,
        MAX_DECOMPRESSED_BYTES_ENV,
        INGEST_CONCURRENCY_ENV,
        INGEST_WAIT_ENV,
        SHUTDOWN_DEADLINE_ENV,
        REQUEST_TIMEOUT_ENV,
        SCAN_THREADS_ENV,
        SCAN_FAN_ROWS_ENV,
        TIME_COLUMNS_ENV,
        EPOCH_WINDOW_ENV,
        LOG_LEVEL_ENV,
        LOG_FORMAT_ENV,
    ];

    /// **A setting is the flag, or else the variable, or else the file, or else the
    /// default** - the file under both of the others, and over nothing but the default.
    ///
    /// On the query limits, whose three variables no other test of this crate sets.
    #[test]
    fn a_setting_is_the_flag_then_the_variable_then_the_file_then_the_default() {
        let _cleared = Cleared::of(&[MAX_ROWS_ENV, MAX_BUCKETS_ENV, MAX_GROUPS_ENV]);
        let file = File::parse("[query]\nmax_rows = 11\nmax_buckets = 12\n").expect("a file");

        // The file over the default, key by key: `max_groups` is not in it.
        let read = limits(&file, None, None, None).expect("the file");
        assert_eq!(
            (read.max_rows, read.max_buckets, read.max_groups),
            (11, 12, DEFAULT_MAX_GROUPS)
        );

        // The variable over the file.
        std::env::set_var(MAX_ROWS_ENV, "21");
        let read = limits(&file, None, None, None).expect("the variable");
        assert_eq!((read.max_rows, read.max_buckets), (21, 12));

        // The flag over both.
        let read = limits(&file, Some(31), Some(32), None).expect("the flag");
        assert_eq!((read.max_rows, read.max_buckets), (31, 32));

        // And a variable that cannot be read is an error with a file there to fall back
        // on, as it is with none: its writer believes it is in force.
        std::env::set_var(MAX_ROWS_ENV, "many");
        assert!(matches!(
            limits(&file, None, None, None),
            Err(ConfigError::BadEnv { var, .. }) if var == MAX_ROWS_ENV
        ));

        // No file is every key absent.
        std::env::remove_var(MAX_ROWS_ENV);
        let none = File::read(None).expect("no file");
        assert_eq!(
            limits(&none, None, None, None).expect("defaults").max_rows,
            DEFAULT_MAX_ROWS
        );
    }

    /// **Every key of the file reaches the setting it names.** One file holding all of
    /// them, each at a value that is not its default, read with no flag and no variable.
    #[test]
    fn every_key_of_the_file_reaches_its_setting() {
        let _cleared = Cleared::of(FILE_VARS);
        let dir = std::env::temp_dir().join(format!("kibbr-config-keys-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let text = format!(
            r#"
[storage]
base_path = "{base}"
trie_file = "{base}/elsewhere.json"
max_open_segments = 7
segment_max_bytes = 1048576
max_buffer_bytes = 2097152
compact_interval_ms = 0
wal_segment_bytes = 131072
wal_queue_bytes = 4194304

[ingestor]
flush_interval_ms = 20
row_group_timeout_ms = 60
batch_size = 250

[api]
native_addr = "0.0.0.0:7811"
max_body_bytes = 1024
max_decompressed_bytes = 2048
ingest_concurrency = 3
ingest_wait_ms = 250
shutdown_deadline = "1500ms"
request_timeout = "45s"

[query]
scan_threads = 1
scan_fan_rows = 99

[schema]
time_columns = ["when", "ts"]
epoch_window = ["2001-01-01T00:00:00Z", "2002-01-01T00:00:00Z"]

[log]
level = "warn"
format = "json"
"#,
            base = dir.display()
        );
        let file = File::parse(&text).expect("every key is a setting");

        let storage = Storage::load(&file, true, None, None, None, None, None, None, None)
            .and_then(|storage| storage.with_compact_interval(&file, None))
            .and_then(|storage| storage.with_wal(&file, None, None, None))
            .and_then(|storage| storage.with_scan(&file, None, None))
            .expect("the storage settings");
        let base = std::fs::canonicalize(&dir).expect("the directory was made");
        assert_eq!(storage.base_path.as_deref(), Some(base.as_path()));
        assert_eq!(storage.trie_file, Some(dir.join("elsewhere.json")));
        assert_eq!(storage.max_open_segments, 7);
        assert_eq!(storage.segment_max_bytes, 1_048_576);
        assert_eq!(storage.max_buffer_bytes, 2_097_152);
        assert_eq!(storage.compact_interval, Duration::ZERO);
        assert_eq!(storage.wal_segment_bytes, 131_072);
        assert_eq!(storage.wal_queue_bytes, 4_194_304);
        assert_eq!(storage.flush_interval, Duration::from_millis(20));
        assert_eq!(storage.row_group_timeout, Duration::from_millis(60));
        assert_eq!(storage.wal_batch_lines, 250);
        assert_eq!((storage.scan_threads, storage.scan_fan_rows), (1, 99));
        let _ = std::fs::remove_dir_all(&dir);

        let api = ApiLimits::load(&file, None, None, None)
            .and_then(|api| api.with_ingest_wait(&file, None))
            .and_then(|api| api.with_shutdown_deadline(&file, None))
            .and_then(|api| api.with_request_timeout(&file, None))
            .expect("the request settings");
        assert_eq!(api.max_body_bytes, 1024);
        assert_eq!(api.max_decompressed_bytes, 2048);
        assert_eq!(api.ingest_concurrency, 3);
        assert_eq!(api.ingest_wait, Duration::from_millis(250));
        assert_eq!(api.shutdown_deadline, Some(Duration::from_millis(1500)));
        assert_eq!(api.request_timeout, Some(Duration::from_secs(45)));

        assert_eq!(
            listen(&file, None, None).expect("an address"),
            ("0.0.0.0".to_string(), 7811)
        );
        // Each flag overrides its half of the address and leaves the other.
        assert_eq!(
            listen(&file, Some("127.0.0.1".to_string()), None).expect("a host"),
            ("127.0.0.1".to_string(), 7811)
        );
        assert_eq!(
            listen(&file, None, Some(0)).expect("a port"),
            ("0.0.0.0".to_string(), 0)
        );

        assert_eq!(schema(&file, None).time_columns, ["when", "ts"]);
        let window = epoch_window(&file).expect("a window");
        assert!(format!("{window:?}") != format!("{:?}", EpochWindow::default()));

        assert_eq!(
            log(&file, None, None).expect("the log settings"),
            Log {
                level: LogLevel::Warn,
                format: LogFormat::Json
            }
        );
        assert_eq!(
            log(&file, Some(LogLevel::Debug), Some(LogFormat::Text)).expect("the flags"),
            Log {
                level: LogLevel::Debug,
                format: LogFormat::Text
            }
        );
    }

    /// **A key this build has no setting for is refused, by name and by line** - a key
    /// the design names and nothing implements among them - and so is a section, and a
    /// value of the wrong kind. Read and dropped, each would be a setting its writer
    /// believes is in force.
    #[test]
    fn a_key_with_no_setting_behind_it_is_refused() {
        for (text, says) in [
            ("[storage]\nwal_dir = \"/var/lib/kibbr/wal\"\n", "wal_dir"),
            (
                "[api]\nes_compat_addr = \"0.0.0.0:9200\"\n",
                "es_compat_addr",
            ),
            (
                "[schema]\nmerge_threshold_pct = 80\n",
                "merge_threshold_pct",
            ),
            ("[schema]\npromoted_keys = [\"event\"]\n", "promoted_keys"),
            ("[cold_tier]\nenabled = false\n", "cold_tier"),
            ("[ingestor]\nflush_interval = \"500ms\"\n", "flush_interval"),
            ("[query]\nmax_rows = \"many\"\n", "max_rows"),
            ("[query]\nmax_rows = -1\n", "max_rows"),
            ("max_rows = 5\n", "max_rows"),
        ] {
            let Err(detail) = File::parse(text) else {
                panic!("read: {text}");
            };
            assert!(detail.contains(says), "{text}: {detail}");
            assert!(detail.contains("line "), "{text}: {detail}");
        }
        // What is not TOML at all is refused by the same road.
        assert!(File::parse("[storage\n").is_err());
        // And a file of nothing, or of comments, is every key absent.
        assert!(File::parse("# nothing yet\n").is_ok());

        // A file that is named and is not there is an error, never "no file".
        let gone = std::env::temp_dir().join(format!("kibbr-no-such-{}.toml", std::process::id()));
        assert!(matches!(
            File::read(Some(&gone)),
            Err(ConfigError::FileRead { .. })
        ));
    }

    /// **A duration is a whole number and a unit, and reads back as it was written.**
    #[test]
    fn a_duration_is_a_whole_number_and_a_unit() {
        for (text, millis) in [
            ("25s", 25_000),
            ("500ms", 500),
            ("5m", 300_000),
            ("1h", 3_600_000),
            ("0s", 0),
            (" 2s ", 2_000),
        ] {
            assert_eq!(
                parse_duration(text),
                Ok(Duration::from_millis(millis)),
                "{text}"
            );
        }
        for text in [
            "25",
            "s",
            "",
            "2.5s",
            "-1s",
            "25 s",
            "25sec",
            "1d",
            "99999999999999999999s",
        ] {
            assert!(parse_duration(text).is_err(), "{text}");
        }
        assert!(parse_duration("18446744073709551615h").is_err());
        assert_eq!(duration_words(Duration::from_secs(25)), "25s");
        assert_eq!(duration_words(Duration::from_millis(1500)), "1500ms");
        assert_eq!(duration_words(Duration::ZERO), "0s");
    }

    /// **The shutdown deadline is 25 seconds unless something says otherwise; zero is no
    /// deadline; and one that cannot be read is an error that says where it was written.**
    #[test]
    fn the_shutdown_deadline_is_25s_by_default_and_none_at_zero() {
        let _cleared = Cleared::of(&[SHUTDOWN_DEADLINE_ENV]);
        let deadline = |file: &File, flag: Option<Duration>| {
            ApiLimits::default()
                .with_shutdown_deadline(file, flag)
                .map(|api| api.shutdown_deadline)
        };
        let none = File::default();
        assert_eq!(
            ApiLimits::default().shutdown_deadline,
            Some(Duration::from_secs(25))
        );
        assert_eq!(
            deadline(&none, None).expect("default"),
            Some(Duration::from_secs(25))
        );

        let file = File::parse("[api]\nshutdown_deadline = \"40s\"\n").expect("a file");
        assert_eq!(
            deadline(&file, None).expect("file"),
            Some(Duration::from_secs(40))
        );
        std::env::set_var(SHUTDOWN_DEADLINE_ENV, "300ms");
        assert_eq!(
            deadline(&file, None).expect("variable"),
            Some(Duration::from_millis(300))
        );
        let flag = Some(Duration::from_secs(2));
        assert_eq!(deadline(&file, flag).expect("flag"), flag);

        // Zero, in any unit and from anywhere, waits for every request.
        assert_eq!(deadline(&file, Some(Duration::ZERO)).expect("flag 0"), None);
        std::env::set_var(SHUTDOWN_DEADLINE_ENV, "0ms");
        assert_eq!(deadline(&file, None).expect("variable 0"), None);

        std::env::set_var(SHUTDOWN_DEADLINE_ENV, "25");
        let Err(e) = deadline(&file, None) else {
            panic!("a bare number was read");
        };
        assert_eq!(
            e.to_string(),
            "KIBBR__API__SHUTDOWN_DEADLINE `25`: expected a whole number and a unit - ms, s, \
             m or h - as in `25s`"
        );
        std::env::remove_var(SHUTDOWN_DEADLINE_ENV);
        let file = File::parse("[api]\nshutdown_deadline = \"soon\"\n").expect("a file");
        let Err(e) = deadline(&file, None) else {
            panic!("`soon` was read");
        };
        assert!(
            e.to_string()
                .starts_with("[api] shutdown_deadline `soon`: "),
            "{e}"
        );
    }

    /// **A request is given 30 seconds unless something says otherwise; zero is no
    /// timeout; flag > variable > file; and one that cannot be read is an error that says
    /// where it was written.**
    #[test]
    fn the_request_timeout_is_30s_by_default_and_none_at_zero() {
        let _cleared = Cleared::of(&[REQUEST_TIMEOUT_ENV]);
        let timeout = |file: &File, flag: Option<Duration>| {
            ApiLimits::default()
                .with_request_timeout(file, flag)
                .map(|api| api.request_timeout)
        };
        let none = File::default();
        assert_eq!(
            ApiLimits::default().request_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            timeout(&none, None).expect("default"),
            Some(Duration::from_secs(30))
        );

        let file = File::parse("[api]\nrequest_timeout = \"2m\"\n").expect("a file");
        assert_eq!(
            timeout(&file, None).expect("file"),
            Some(Duration::from_secs(120))
        );
        std::env::set_var(REQUEST_TIMEOUT_ENV, "750ms");
        assert_eq!(
            timeout(&file, None).expect("variable"),
            Some(Duration::from_millis(750))
        );
        let flag = Some(Duration::from_secs(5));
        assert_eq!(timeout(&file, flag).expect("flag"), flag);

        assert_eq!(timeout(&file, Some(Duration::ZERO)).expect("flag 0"), None);
        std::env::set_var(REQUEST_TIMEOUT_ENV, "0s");
        assert_eq!(timeout(&file, None).expect("variable 0"), None);

        std::env::set_var(REQUEST_TIMEOUT_ENV, "30");
        let Err(e) = timeout(&file, None) else {
            panic!("a bare number was read");
        };
        assert_eq!(
            e.to_string(),
            "KIBBR__API__REQUEST_TIMEOUT `30`: expected a whole number and a unit - ms, s, \
             m or h - as in `25s`"
        );
        std::env::remove_var(REQUEST_TIMEOUT_ENV);
        let file = File::parse("[api]\nrequest_timeout = \"never\"\n").expect("a file");
        let Err(e) = timeout(&file, None) else {
            panic!("`never` was read");
        };
        assert!(
            e.to_string().starts_with("[api] request_timeout `never`: "),
            "{e}"
        );
    }

    /// **An address or a log setting that cannot be read is an error that names where it
    /// was written and what it may be.**
    #[test]
    fn an_address_or_a_log_setting_that_cannot_be_read_says_where_it_was_written() {
        let _cleared = Cleared::of(&[NATIVE_ADDR_ENV, LOG_LEVEL_ENV, LOG_FORMAT_ENV]);
        let none = File::default();
        assert_eq!(
            listen(&none, None, None).expect("defaults"),
            ("127.0.0.1".to_string(), DEFAULT_PORT)
        );
        assert_eq!(log(&none, None, None).expect("defaults"), Log::default());

        for text in [
            "localhost",
            "localhost:",
            "localhost:http",
            ":7700",
            "localhost:70000",
        ] {
            let file = File {
                api: ApiKeys {
                    native_addr: Some(text.to_string()),
                    ..ApiKeys::default()
                },
                ..File::default()
            };
            let Err(e) = listen(&file, None, None) else {
                panic!("{text} was read");
            };
            assert!(e.to_string().starts_with("[api] native_addr `"), "{e}");
        }
        // An address with colons of its own keeps them: the port is after the last.
        std::env::set_var(NATIVE_ADDR_ENV, "[::1]:7800");
        assert_eq!(
            listen(&none, None, None).expect("an IPv6 address"),
            ("[::1]".to_string(), 7800)
        );
        std::env::remove_var(NATIVE_ADDR_ENV);

        std::env::set_var(LOG_LEVEL_ENV, "loud");
        let Err(e) = log(&none, None, None) else {
            panic!("`loud` was read");
        };
        assert_eq!(
            e.to_string(),
            "KIBBR__LOG__LEVEL `loud`: expected one of trace, debug, info, warn, error"
        );
        // The flag is not refused for a variable it overrides.
        assert_eq!(
            log(&none, Some(LogLevel::Error), None)
                .expect("the flag")
                .level,
            LogLevel::Error
        );
        std::env::set_var(LOG_LEVEL_ENV, "DEBUG");
        assert_eq!(
            log(&none, None, None).expect("any case").level,
            LogLevel::Debug
        );
    }
}
