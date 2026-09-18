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
/// batch is larger, and a 413 on a routine batch reads as TrieDB rejecting valid logs. Not
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

/// Environment variable for the max_rows configuration.
const MAX_ROWS_ENV: &str = "TRIEDB__QUERY__MAX_ROWS";
const MAX_BUCKETS_ENV: &str = "TRIEDB__QUERY__MAX_BUCKETS";
const MAX_GROUPS_ENV: &str = "TRIEDB__QUERY__MAX_GROUPS";
const MAX_BODY_BYTES_ENV: &str = "TRIEDB__API__MAX_BODY_BYTES";
const MAX_DECOMPRESSED_BYTES_ENV: &str = "TRIEDB__API__MAX_DECOMPRESSED_BYTES";
const INGEST_CONCURRENCY_ENV: &str = "TRIEDB__API__INGEST_CONCURRENCY";
const BASE_PATH_ENV: &str = "TRIEDB__STORAGE__BASE_PATH";
const TRIE_FILE_ENV: &str = "TRIEDB__STORAGE__TRIE_FILE";
const FLUSH_INTERVAL_ENV: &str = "TRIEDB__INGESTOR__FLUSH_INTERVAL_MS";
const ROW_GROUP_TIMEOUT_ENV: &str = "TRIEDB__INGESTOR__ROW_GROUP_TIMEOUT_MS";

/// How often the thresholds that close a row group are checked.
///
/// Not a durability interval, and it stopped being one with the segment format: a tick
/// closes a row group only where a threshold has fired, so a busy leaf is written on the
/// first tick after 8,192 rows and a quiet one waits for `DEFAULT_ROW_GROUP_TIMEOUT_MS`.
/// What the tick bounds is how late either can be noticed.
pub const DEFAULT_FLUSH_INTERVAL_MS: usize = 500;

/// How long a row group may stay open with rows in it.
///
/// **Until the write-ahead log lands this is also what a `kill -9` costs**, which is why
/// it is a setting and not a constant: an operator who would rather pay in file count than
/// in rows can turn it down. It cannot go below the flush interval - the tick is what
/// notices it has expired.
pub const DEFAULT_ROW_GROUP_TIMEOUT_MS: usize = 60_000;

/// Where `serve` keeps day partitions when nothing says otherwise.
///
/// Relative, and beside the process rather than under `/var/lib`: a default that needs
/// root to create is a default that fails on the first run on a laptop.
pub const DEFAULT_DATA_DIR: &str = "triedb-data";
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

    #[error("storage directory `{path}`: {source}")]
    BadDataDir {
        path: String,
        #[source]
        source: std::io::Error,
    },
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
}

impl Default for Storage {
    fn default() -> Self {
        Self {
            base_path: None,
            trie_file: None,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS as u64),
            row_group_timeout: Duration::from_millis(DEFAULT_ROW_GROUP_TIMEOUT_MS as u64),
        }
    }
}

impl Storage {
    /// The same three rules as the other two, over the storage settings.
    ///
    /// `durable` is the `--storage` choice rather than a path being present, so that
    /// `--storage memory --data-dir /x` is a clear instruction and not a contradiction
    /// resolved by whichever flag the code happened to read first.
    pub fn load(
        durable: bool,
        data_dir: Option<PathBuf>,
        trie_file: Option<PathBuf>,
        flush_ms: Option<usize>,
        row_group_ms: Option<usize>,
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

        if !durable {
            return Ok(Self {
                base_path: None,
                trie_file: None,
                flush_interval,
                row_group_timeout,
            });
        }

        let base = resolve_path(data_dir, BASE_PATH_ENV)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR));
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
        })
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
