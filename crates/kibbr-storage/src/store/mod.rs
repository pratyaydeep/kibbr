//! Leaf storage: the scan interface, the two implementations behind it, and the registry.
//! `LeafStore` holds one leaf's rows and `Store` writes them out; neither has a clock, which
//! lives in `serve`. No lock is over all of it: each leaf locks itself, and a flush
//! holds no lock an append or a query waits on while it touches the disk
//! (`disk::DiskLeaf::state`).

// Part of this interface still has no caller (`scan`, `NotOpen`, `template_id`, `iter`); kept
// because it is the contract both implementations were written against.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use kibbr_core::infer::infer;
use kibbr_core::types::ColValue;

pub mod columns;
pub mod disk;
pub mod fan;
pub mod manifest;
pub mod memory;
#[cfg(any(test, feature = "test-support"))]
pub mod pool;
pub mod v3;

pub use disk::DiskStore;
pub use memory::MemoryStore;

/// One row: each key's value as the ladder read it and, only where printing that value would
/// not give back what the line said, the JSON the line said.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    values: HashMap<String, ColValue>,
    written: HashMap<String, serde_json::Value>,
}

impl std::ops::Deref for Row {
    type Target = HashMap<String, ColValue>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = (&'a String, &'a ColValue);
    type IntoIter = std::collections::hash_map::Iter<'a, String, ColValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl FromIterator<(String, ColValue)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, ColValue)>>(iter: I) -> Self {
        Self {
            values: iter.into_iter().collect(),
            written: HashMap::new(),
        }
    }
}

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            values: HashMap::with_capacity(n),
            written: HashMap::new(),
        }
    }

    /// A value whose canonical print is what the line said.
    pub fn insert(&mut self, key: String, value: ColValue) -> Option<ColValue> {
        self.written.remove(&key);
        self.values.insert(key, value)
    }

    /// A value, and the JSON the line said where that differs from its canonical print, so
    /// every source of rows ends with the same row for the same line.
    pub fn insert_written(
        &mut self,
        key: String,
        value: ColValue,
        written: Option<serde_json::Value>,
    ) {
        match written {
            Some(json) if json != value.printed() => {
                self.written.insert(key.clone(), json);
            }
            _ => {
                self.written.remove(&key);
            }
        }
        self.values.insert(key, value);
    }

    /// Replace the value a query compares against and keep what the line said - the text a
    /// widened date column is matched as, over the JSON the answer still prints.
    pub fn set_value(&mut self, key: &str, value: ColValue) {
        if let Some(slot) = self.values.get_mut(key) {
            let before = std::mem::replace(slot, value);
            if !self.written.contains_key(key) {
                self.written.insert(key.to_string(), before.printed());
            }
        }
    }

    pub fn clear(&mut self) {
        self.values.clear();
        self.written.clear();
    }

    /// The JSON the line said for `key`, when it is not the value's canonical print.
    pub fn written(&self, key: &str) -> Option<&serde_json::Value> {
        self.written.get(key)
    }

    /// The JSON value the line said for `key`: what an answer prints.
    pub fn json(&self, key: &str) -> Option<serde_json::Value> {
        let value = self.values.get(key)?;
        Some(
            self.written
                .get(key)
                .cloned()
                .unwrap_or_else(|| value.printed()),
        )
    }

    /// Roughly the memory this row holds, tables and allocations included: several times its
    /// payload, which is what a memory limit must count.
    pub fn held_bytes(&self) -> usize {
        fn table<K, V>(map: &HashMap<K, V>) -> usize {
            match map.capacity() {
                0 => 0,
                // A table holds seven items for every eight slots, in a power of two, and
                // a control byte a slot beside them.
                n => (n * 8 / 7).next_power_of_two() * (std::mem::size_of::<(K, V)>() + 1),
            }
        }
        fn heap(len: usize) -> usize {
            len.div_ceil(16) * 16
        }
        let keys: usize = self
            .values
            .keys()
            .chain(self.written.keys())
            .map(|key| heap(key.len()))
            .sum();
        let strings: usize = self
            .values
            .values()
            .map(|value| match value {
                ColValue::String(text) => heap(text.len()),
                _ => 0,
            })
            .chain(self.written.values().map(|json| match json {
                serde_json::Value::String(text) => heap(text.len()),
                _ => 0,
            }))
            .sum();
        std::mem::size_of::<Self>() + table(&self.values) + table(&self.written) + keys + strings
    }
}

/// One term of a pattern that the path could not settle: the key it reads and what it wants
/// there, as data read through the ladder once so a scan only compares.
#[derive(Debug, Clone, PartialEq)]
pub struct Term {
    key: String,
    want: Want,
}

/// What a term wants of the value under its key.
#[derive(Debug, Clone, PartialEq)]
pub enum Want {
    /// `*`: the key is present and not null.
    Any,
    /// One value: the term's text, and what the ladder reads that text as - `infer(text)`,
    /// run where the term is made and nowhere in a scan.
    Exact { text: String, inferred: ColValue },
}

impl Term {
    pub fn any(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            want: Want::Any,
        }
    }

    /// A term for one value: the one place the ladder is run on a term's text.
    pub fn exact(key: impl Into<String>, text: &str) -> Self {
        Self {
            key: key.into(),
            want: Want::Exact {
                text: text.to_string(),
                inferred: infer(text).value,
            },
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn want(&self) -> &Want {
        &self.want
    }

    /// Whether a value, as a rendered row holds it, satisfies the term: the string reading
    /// first, then the ladder's, as `engine::value_matches` does.
    pub fn matches(&self, seen: &ColValue) -> bool {
        match &self.want {
            Want::Any => !matches!(seen, ColValue::Null),
            Want::Exact { text, inferred } => {
                matches!(seen, ColValue::String(s) if s == text) || seen == inferred
            }
        }
    }

    /// `matches`, for one stored value no row was rendered for: in a column that is now `String`
    /// a `Datetime` is compared as the text it arrived as.
    pub fn matches_cell(
        &self,
        value: &ColValue,
        written: Option<&serde_json::Value>,
        text_column: bool,
    ) -> bool {
        match (value, text_column) {
            (ColValue::Datetime(_), true) => {
                render_text(value, written).is_some_and(|seen| self.matches(&seen))
            }
            _ => self.matches(value),
        }
    }

    /// A row already rendered: a row without the term's key matches nothing, `*` included.
    pub fn matches_row(&self, row: &Row) -> bool {
        row.get(&self.key).is_some_and(|have| self.matches(have))
    }

    /// What `matches_row` answers for `render_row` of a stored row, with no row cloned to find
    /// out.
    pub fn matches_in(&self, row: &Row, text_column: bool) -> bool {
        match row.get(&self.key) {
            None => false,
            Some(value @ ColValue::Datetime(_)) if text_column => {
                render_text(value, row.written(&self.key)).is_some_and(|seen| self.matches(&seen))
            }
            Some(value) => self.matches(value),
        }
    }
}

/// The column a time filter reads when none is named: a fallback for a `_leaf.json` that
/// predates the time source.
pub const DEFAULT_TS_COLUMN: &str = "ts";

/// The system column holding a row's arrival time.
pub use kibbr_core::types::RECEIVED_AT_COLUMN;

/// What a query returns the system column as, unless the row has an ingested key of that
/// name - then the ingested one is shown and the system value is left out.
pub const RECEIVED_AT_NAME: &str = "_received_at";

#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Empty = every column; a read hint, not a projection guarantee, which is what lets the
    /// disk store read only the chunks named.
    pub columns: Vec<String>,
    /// 0 = unlimited.
    pub limit: usize,
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
    /// The template's time column: what the window and the order read, falling
    /// back to `RECEIVED_AT_COLUMN` on a row where it holds no `Datetime`.
    pub ts_column: Option<String>,
    /// Columns whose type is now `String`: a `Datetime` stored in one is compared as the text
    /// it arrived as.
    pub text_columns: Vec<String>,
    /// What tells the scan its answer is no longer wanted (`Stop`); the default is never told.
    pub stop: Stop,
}

/// What whoever asked for a scan tells it when the answer is no longer wanted; a scan asks
/// between pieces of work and nobody interrupts it.
#[derive(Default, Clone)]
pub struct Stop(Option<Arc<Told>>);

struct Told {
    set: AtomicBool,
    /// Whether a scan has found it set: `seen` is called by the first that does.
    found: AtomicBool,
    seen: Option<Box<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => f.write_str("Stop(never)"),
            Some(_) => write!(f, "Stop({})", self.is_set()),
        }
    }
}

/// How many rows the memory store reads between two looks at its `Stop`.
pub const STOP_EVERY_ROWS: usize = 4096;

impl Stop {
    /// A stop that can be set.
    pub fn new() -> Self {
        Self(Some(Arc::new(Told {
            set: AtomicBool::new(false),
            found: AtomicBool::new(false),
            seen: None,
        })))
    }

    /// A stop that can be set, and calls `seen` when a scan first finds it so - on the
    /// scan's thread, between two row groups: it should be quick.
    pub fn watched(seen: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Some(Arc::new(Told {
            set: AtomicBool::new(false),
            found: AtomicBool::new(false),
            seen: Some(Box::new(seen)),
        })))
    }

    pub fn set(&self) {
        if let Some(told) = &self.0 {
            told.set.store(true, Ordering::Relaxed);
        }
    }

    pub fn is_set(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|told| told.set.load(Ordering::Relaxed))
    }

    /// `Err(StoreError::Stopped)` once set: what a scan asks between two pieces of work.
    pub fn check(&self) -> Result<(), StoreError> {
        let Some(told) = self.0.as_ref().filter(|told| told.set.load(Ordering::Relaxed)) else {
            return Ok(());
        };
        if let Some(seen) = &told.seen {
            if !told.found.swap(true, Ordering::Relaxed) {
                seen();
            }
        }
        Err(StoreError::Stopped)
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("no store open for leaf {template_id}")]
    NotOpen { template_id: u64 },
    #[error("leaf {template_id}: {source}")]
    Io {
        template_id: u64,
        #[source]
        source: std::io::Error,
    },
    /// A leaf's rows could not be written: the leaf, then why.
    #[error("leaf {template_id}: {source}")]
    Leaf {
        template_id: u64,
        #[source]
        source: Box<StoreError>,
    },
    #[error(transparent)]
    Format(#[from] v3::FormatError),
    /// A file the manifest names that a start could not look for, which may be there and whole;
    /// `why` is in the sentence and not a source, or a start would print it twice.
    #[error(
        "{dir}: {why}: manifest.log names {file}, and that file was not read - whether it \
         is on disk is not known"
    )]
    NamedUnread {
        dir: std::path::PathBuf,
        file: std::path::PathBuf,
        why: std::io::Error,
    },
    /// A file the manifest names that is not on disk, with no source: that error would add
    /// nothing to the sentence.
    #[error("{file}: named by manifest.log and not on disk; its rows are gone")]
    NamedAbsent { file: std::path::PathBuf },
    #[error(transparent)]
    Build(#[from] v3::build::BuildError),
    #[error(transparent)]
    Manifest(#[from] manifest::ManifestError),
    /// A layout from before format v3, which has no migration: the path is named rather than
    /// bytes read whose meaning would be a guess.
    #[error(
        "{path}: this data directory holds an earlier storage layout, which this build \
         cannot read. There is no migration - point --data-dir somewhere else, or remove it \
         and re-ingest."
    )]
    LegacyLayout { path: std::path::PathBuf },
    /// A bulk load the manifest already names: the same inputs - the same file names and
    /// the same bytes - loaded twice would be every row twice.
    #[error(
        "load {load_id:016x} was loaded before: manifest.log names load:{load_id:016x}, so \
         these inputs - the same file names holding the same bytes - were committed once \
         already (and may since have been replaced), and nothing was loaded. To load a day of \
         them again in place of what that day holds, use --replace-day"
    )]
    LoadRepeated { load_id: u64 },
    /// A committed load whose files are still in `staging/`: this process's view is short
    /// of their rows, and another load staging over them would delete the only copy.
    #[error(
        "load {load_id:016x} is committed - or its record may be - and its files are still in \
         staging/; restart to settle it (the start finishes the load if its record is there \
         and removes it if not), then load again"
    )]
    MovePending { load_id: u64 },
    /// The record that commits a load failed partway and may be on disk whole, so the staged
    /// files are kept for the next start to decide.
    #[error(
        "load {load_id:016x}: writing its manifest record failed ({source}), and the record may \
         be on disk. Its files are kept in staging/; the next start finishes the load if the \
         record is there and removes them if not"
    )]
    CommitUncertain {
        load_id: u64,
        #[source]
        source: manifest::ManifestError,
    },
    #[error("--replace-day {day}: {path}: {why}. Nothing was loaded")]
    ReplaceRefused {
        day: String,
        path: String,
        why: String,
    },
    /// Past the record that commits a load, a move out of `staging/` failed: the rows are safe
    /// and the next start finishes the move, but this process cannot show them.
    #[error(
        "load {load_id:016x} is committed, but moving its files into data/ failed ({source}). \
         Its rows are safe in staging/, and the next start moves them into place"
    )]
    NotMoved {
        load_id: u64,
        #[source]
        source: v3::FormatError,
    },
    /// A day compaction will not take as it stands: a file a failed seal left, or a commit only
    /// a restart can settle.
    /// Refused before a byte is written, but for the seal of the day's open file as it begins.
    #[error("compact {day}: {path}: {why}. Nothing was compacted")]
    CompactRefused {
        day: String,
        path: String,
        why: String,
    },
    /// One day is compacted at a time, and `busy` is the day that is.
    #[error(
        "compact {day}: {busy} is being compacted, and one day is compacted at a time. \
         Nothing was compacted"
    )]
    CompactBusy { day: String, busy: String },
    /// Compaction writes a day again beside the files it reads, and the volume has no room
    /// for both and for the live door's next flush.
    #[error(
        "compact {day}: {path} has {free} bytes free, and compacting the day needs {needed}: \
         its files a second time, one more file and the buffer limit. Nothing was compacted"
    )]
    NoSpace {
        day: String,
        path: std::path::PathBuf,
        free: u64,
        needed: u64,
    },
    /// A file the compaction was reading was retired under it by a load replacing the day:
    /// not a failure, since the day is the load's.
    #[error(
        "compact {day}: {path} was retired while the day was being rebuilt - a load replaced \
         the day. The rebuilt files were removed and the day is as the load left it"
    )]
    InputsChanged { day: String, path: String },
    /// A scan told to stop by whoever asked for it (`Stop`): not a failure of the store.
    #[error("the scan was stopped: its answer was no longer wanted")]
    Stopped,
    /// Stopped by its caller before the record: not a failure.
    #[error(
        "compact {day}: cancelled. The rebuilt files were removed and the day's files are as \
         they were"
    )]
    Cancelled { day: String },
    /// The record that commits a compaction failed partway and may be on disk whole, so both
    /// sets of files stay for the next start's manifest to choose between.
    #[error(
        "compact {day}: writing its manifest record failed ({source}), and the record may be \
         on disk. The day's files and the rebuilt ones are both kept - the next start keeps \
         the set manifest.log names and deletes the other - and no day is compacted before \
         manifest.log next takes a record"
    )]
    CompactUncertain {
        day: String,
        #[source]
        source: manifest::ManifestError,
    },
    /// The flush a compaction begins with failed; not `CompactFailed`, since the flush may have
    /// written row groups or named a file before the write that failed.
    #[error(
        "compact {day}: the flush before the compaction failed ({source}). Nothing was \
         compacted and no file was rebuilt; rows the flush could not write are still \
         buffered, and the next flush writes them"
    )]
    CompactFlushFailed {
        day: String,
        #[source]
        source: Box<StoreError>,
    },
    /// A compaction that could not finish: the day, then why.
    #[error(
        "compact {day}: {source}. Nothing was compacted: no rebuilt file is kept and the \
         day's files are as they were"
    )]
    CompactFailed {
        day: String,
        #[source]
        source: Box<StoreError>,
    },
    /// A compacted file and a file it was rebuilt from, both named live: the start stops
    /// rather than answer with rows counted twice.
    #[error(
        "{child}: its footer says it replaced {parent}, and manifest.log names both as live - \
         the rows of {parent} would be stored twice. Nothing was opened"
    )]
    ReplacedLive {
        child: std::path::PathBuf,
        parent: std::path::PathBuf,
    },
}

/// What one scan produced and what it cost: the scan is the only thing that can count
/// what `QueryMeta` reports.
#[derive(Debug, Default)]
pub struct Scan {
    pub rows: Vec<Row>,
    /// Row groups whose chunks were read, on every answer's `meta` so a prune that stops is
    /// noticed.
    pub row_groups_read: u64,
    /// Row groups dropped on their footer timestamps, before any column data was read.
    pub row_groups_pruned: u64,
    /// Bytes of column chunk read off disk: where column pruning shows up.
    pub bytes_read: u64,
    /// Segment files opened and read - not leaves, and not row groups; a file whose every row
    /// group the window pruned is not counted.
    pub files_read: u64,
    /// Rows examined, including those the window and the terms rejected; a partition skipped
    /// unread contributes nothing, so the two backends report different numbers.
    pub scanned: u64,
    /// Rows that passed everything.
    pub matched: u64,
}

// ── The cap on open data files ─────────────────────────────────────────────────────

/// How many data files may be open for writing at once, one per UTC day: at it the least
/// recently written is sealed, and a row group is never filed under another day.
pub const DEFAULT_MAX_OPEN_SEGMENTS: usize = 512;

/// The memory every buffer together may hold before the largest are closed early, and twice
/// it before `/ingest` sheds bodies: an emergency brake, counted as `Row::held_bytes`.
pub const DEFAULT_MAX_BUFFER_BYTES: u64 = 512 * 1024 * 1024;

/// A run of a day's row groups holding fewer rows than this is chosen on the thread that
/// asked: below it waking the scan threads (`fan`) costs more than it saves.
pub const DEFAULT_FAN_ROWS: u64 = 32_768;

/// What the disk store is held to: when a row group closes, how many files may be open,
/// how large one may grow, how much may wait in memory, and what a scan may use.
#[derive(Debug, Clone, Copy)]
pub struct DiskOptions {
    pub row_groups: RowGroupLimits,
    /// The cap on day files open for writing: see `DEFAULT_MAX_OPEN_SEGMENTS`.
    pub max_open_files: usize,
    /// A day's file is sealed once it is this long, and the next number opened.
    pub segment_max_bytes: u64,
    /// See `DEFAULT_MAX_BUFFER_BYTES`.
    pub max_buffer_bytes: u64,
    /// Threads one scan may choose rows on, the asking thread among them; 0 by default, since
    /// only a server answers queries and it asks for its threads (`config::Storage::with_scan`).
    pub scan_threads: usize,
    /// See `DEFAULT_FAN_ROWS`; 0 in a test that wants every run of two row groups shared out.
    pub fan_rows: u64,
}

impl Default for DiskOptions {
    fn default() -> Self {
        Self {
            row_groups: RowGroupLimits::default(),
            max_open_files: DEFAULT_MAX_OPEN_SEGMENTS,
            segment_max_bytes: disk::DEFAULT_SEGMENT_MAX_BYTES,
            max_buffer_bytes: DEFAULT_MAX_BUFFER_BYTES,
            scan_threads: 0,
            fan_rows: DEFAULT_FAN_ROWS,
        }
    }
}

/// What a flush is being asked to do: closing a row group and sealing a segment are two
/// different events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushKind {
    /// The interval: closes a row group only where a threshold says so, the short rest only
    /// once its oldest row has waited the timeout.
    Tick,
    /// Graceful shutdown: closes every buffer and seals every open file.
    Final,
}

impl FlushKind {
    pub fn is_final(self) -> bool {
        self == FlushKind::Final
    }
}

/// When a row group closes: at a size worth encoding, or at a timeout that can be long
/// because every row that waits is already durable in the log.
#[derive(Debug, Clone, Copy)]
pub struct RowGroupLimits {
    pub rows: usize,
    pub bytes: usize,
    pub age: std::time::Duration,
}

impl Default for RowGroupLimits {
    fn default() -> Self {
        Self {
            rows: 8_192,
            bytes: 8 * 1024 * 1024,
            age: std::time::Duration::from_secs(300),
        }
    }
}

/// What one `Store::flush` wrote, and what it could not.
#[derive(Debug, Default)]
pub struct Flushed {
    pub rows: u64,
    /// Buffers closed - one template's rows of one UTC day each.
    pub partitions: usize,
    pub row_groups: usize,
    pub bytes: u64,
    /// Files flushed: once each, however many row groups went into them.
    pub syncs: usize,
    pub sealed: usize,
    /// Buffers closed before a threshold fired, because together they were past the
    /// memory limit.
    pub forced: usize,
    /// Templates whose rows could not be written, each with why; their rows are back in their
    /// buffers, and one failing is never fatal to the others.
    pub failed: Vec<(u64, StoreError)>,
    /// Seals that failed: they cost no row, but are still a store that could not write.
    pub seal_failed: Vec<StoreError>,
}

/// What one flush wrote, and what it could not, as the process reports it.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushSummary {
    pub rows: u64,
    pub partitions: usize,
    pub row_groups: usize,
    pub bytes: u64,
    pub syncs: usize,
    pub sealed: usize,
    pub forced: usize,
    /// Leaves whose rows could not be written: what the shutdown reports rows against.
    pub failed: usize,
    pub took: Duration,
}

// ── The scan interface ─────────────────────────────────────────────────────────────

/// What a visitor reads of a row that passed the window and the terms: a hint that lets
/// the disk store build fewer columns, or no row at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wants {
    /// Nothing: the count is `Scan::matched`, and a store need build no row.
    Nothing,
    /// Rows holding at least these columns, in storage order.
    Columns(Vec<String>),
    /// Every column the scan read, in storage order: the default.
    Rows,
    /// Every column the scan read, and only the rows `wants_row` says yes to: for a visitor
    /// that keeps the newest few and would have the rest built to be dropped.
    Newest,
}

/// Where a scan's surviving rows go: a trait and not a closure, which cannot say "stop"
/// or be asked whether the next partition is worth opening.
pub trait Visitor {
    /// One surviving row, beside its ordinal within its partition.
    fn row(&mut self, row: &Row, seq: u64);

    /// Whether the partition whose greatest possible timestamp is `newest` (`None`: undated) is
    /// worth reading; the default is yes, since a count cannot skip rows.
    fn wants_partition(&mut self, newest: Option<i64>) -> bool {
        let _ = newest;
        true
    }

    /// What `row` reads of the row it is handed (`Wants`); the default is the whole row.
    fn wants(&self) -> Wants {
        Wants::Rows
    }

    /// Whether a row is worth building (`Wants::Newest`): advisory, so `row` must not lean on it;
    /// `placed` is false where the row's time is no longer in the day it is stored under.
    fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
        let _ = (ts, seq, placed);
        true
    }
}

/// The visitor of a count: it reads nothing of a row, and says so, so that a store which
/// would have to build each row out of columns builds none.
pub struct Counted;

impl Visitor for Counted {
    fn row(&mut self, _: &Row, _: u64) {}

    fn wants(&self) -> Wants {
        Wants::Nothing
    }
}

/// A closure as a `Visitor`; not a blanket impl over `FnMut`, which coherence would then
/// hold against every hand-written `Visitor`.
pub struct Rows<F>(pub F);

impl<F: FnMut(&Row, u64)> Visitor for Rows<F> {
    fn row(&mut self, row: &Row, seq: u64) {
        (self.0)(row, seq)
    }
}

/// One leaf's rows, append-only; every method takes `&self` because a leaf locks itself
/// and none of its callers waits on a lock over any other leaf.
pub trait LeafStore: Send + Sync {
    fn template_id(&self) -> u64;

    /// Buffer one row and return its ordinal, with no I/O and no clock: it runs once per
    /// ingested line.
    fn append(&self, row: Row) -> u64;

    /// `append`, for a row that came through the write-ahead log at `position`; positions
    /// arrive in order, which is what makes a row group's recorded range true.
    fn append_at(&self, row: Row, position: u64) -> u64 {
        let _ = position;
        self.append(row)
    }

    /// Hand every surviving row to `v` as far as it reads rows, and count each in
    /// `Scan::matched`: the one place filtering happens, so count, fetch and group-by cannot drift.
    fn visit(
        &self,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError>;

    /// Rows held, flushed and buffered together - what a query would see.
    fn len(&self) -> u64;

    /// Rows not yet in a row group: what the next start would replay from the log, or with
    /// no log what a `kill -9` would cost.
    fn buffered(&self) -> u64;

    /// Roughly what this leaf weighs in bytes.
    fn approx_bytes(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every surviving row, projected through `opts.columns`; defaulted so the projection
    /// rule lives in one place.
    fn scan(&self, opts: &ScanOptions, terms: &[Term]) -> Result<Scan, StoreError> {
        let mut rows = Vec::new();
        let mut scan = {
            let mut sink = Rows(|row: &Row, _: u64| rows.push(project(row, &opts.columns)));
            self.visit(opts, terms, &mut sink)?
        };
        scan.rows = rows;
        Ok(scan)
    }
}

/// What one query reads: the templates it asked for, as the store held them at one moment,
/// so a load or a replace is atomic to it.
pub trait Snapshot: Send {
    /// The store's publication count when this was taken: see `Store::generation`.
    fn generation(&self) -> u64;

    /// One template's rows as `LeafStore::visit` would have handed them over when the snapshot
    /// was taken, each template once; `None` for a template it holds no leaf of.
    fn visit(
        &mut self,
        template_id: u64,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Option<Result<Scan, StoreError>>;
}

/// The snapshot of a store that never publishes more than one row at a time: its leaves,
/// read as they stand when each is reached.
struct Live(BTreeMap<u64, Arc<dyn LeafStore>>);

impl Snapshot for Live {
    fn generation(&self) -> u64 {
        0
    }

    fn visit(
        &mut self,
        template_id: u64,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Option<Result<Scan, StoreError>> {
        self.0
            .remove(&template_id)
            .map(|leaf| leaf.visit(opts, terms, v))
    }
}

/// Where leaves come from, and where their rows are written: the whole of what memory and
/// disk differ by.
pub trait Store: Send + Sync {
    /// `time_column` is the template's time column: what the leaf partitions,
    /// orders and prunes on.
    fn open(&self, template_id: u64, time_column: &str) -> Result<Arc<dyn LeafStore>, StoreError>;

    /// Close every buffer that is due into row groups - all of them on a final flush, which
    /// then seals every open file - synchronously, called by whoever owns the clock.
    fn flush(&self, kind: FlushKind) -> Flushed {
        let _ = kind;
        Flushed::default()
    }

    /// For the banner and `/stats`.
    fn kind(&self) -> &'static str;
    fn base_path(&self) -> Option<&Path>;
    /// Data files open for writing, and the cap they are held under.
    fn open_files(&self) -> (usize, usize) {
        (0, 0)
    }
    /// Template ids that stored rows already name, whether or not a template knows them:
    /// what a restart must not hand out again.
    fn claimed(&self) -> Vec<u64> {
        Vec::new()
    }
    /// Memory held by buffered rows, and the limit past which the largest are closed early.
    fn buffer_bytes(&self) -> (u64, u64) {
        (0, 0)
    }
    /// The lowest write-ahead log position among the rows still only in memory, which the
    /// log's checkpoint is moved up to; `None` when no such row carries a position.
    fn oldest_position(&self) -> Option<u64> {
        None
    }
    /// The disk store, for the bulk door: only it has files to publish.
    fn as_disk(&self) -> Option<&DiskStore> {
        None
    }
    /// The disk store's scan threads, which the door and the server both hold; `None` for every
    /// store but a server's disk store.
    fn fan(&self) -> Option<Arc<fan::Fan>> {
        None
    }

    /// How many times a load or a replace has published here: a query that sees it move
    /// between planning and its snapshot plans again.
    fn generation(&self) -> u64 {
        0
    }

    /// `leaves` - the registry's open leaves of the templates a query planned - as one
    /// `Snapshot`, taken with the query's window (`opts.since`, `opts.until`).
    fn snapshot(
        &self,
        leaves: Vec<(u64, Arc<dyn LeafStore>)>,
        opts: &ScanOptions,
    ) -> Box<dyn Snapshot> {
        let _ = opts;
        Box::new(Live(leaves.into_iter().collect()))
    }
}

// ── Whether the store can write at all ─────────────────────────────────────────────

/// Whether the last flush tick could write, and what stopped it if not; read without the
/// store lock, so `/health` never queues behind a long flush.
#[derive(Debug, Default)]
pub struct WriteHealth {
    degraded: std::sync::atomic::AtomicBool,
    /// The last error a write failed with. Replaced rather than kept, because what a
    /// caller being refused wants is why *now*.
    last_error: std::sync::Mutex<Option<String>>,
}

impl WriteHealth {
    pub fn is_degraded(&self) -> bool {
        self.degraded.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn set_error(&self, detail: String) {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(detail);
    }

    /// A write failed outside a flush tick - a leaf that could not be opened. Set here and cleared only by a tick, so the clearing rule stays in
    /// one place.
    fn degrade(&self) {
        self.degraded
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// What one flush tick did to the verdict. The error text is left alone on a clearing tick,
    /// so `/stats` still says what the last failure was after the disk comes back.
    fn tick(&self, failed: usize) {
        self.degraded
            .store(failed > 0, std::sync::atomic::Ordering::Relaxed);
    }
}

// ── The registry ───────────────────────────────────────────────────────────────────

/// Take a lock, recovering from a poisoning: these locks guard steps that do not stop
/// halfway, and refusing every later request would turn one bad line into a dead server.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The open leaves, and the backend that makes them and writes their rows; the map's own
/// lock is held only to find or add a leaf, never across a leaf's rows or the disk.
pub struct StoreManager {
    backend: Box<dyn Store>,
    stores: RwLock<BTreeMap<u64, Arc<dyn LeafStore>>>,
    write_failures: AtomicU64,
    /// Rows `append` refused, which are in no buffer: behind the write-ahead log the next
    /// start replays them, and with no log they are gone.
    rows_dropped: AtomicU64,
    /// Buffers closed early by the memory limit, over the life of the process.
    forced_closes: AtomicU64,
    /// Leaves `append` could not open, with the time column to open them on. Retried every
    /// flush, and while any is here the store is degraded (`retry_unopened`).
    unopened: Mutex<BTreeMap<u64, String>>,
    health: Arc<WriteHealth>,
}

impl StoreManager {
    pub fn new(backend: Box<dyn Store>) -> Self {
        Self {
            backend,
            stores: RwLock::new(BTreeMap::new()),
            write_failures: AtomicU64::new(0),
            rows_dropped: AtomicU64::new(0),
            forced_closes: AtomicU64::new(0),
            unopened: Mutex::new(BTreeMap::new()),
            health: Arc::new(WriteHealth::default()),
        }
    }

    pub fn memory() -> Self {
        Self::new(Box::new(MemoryStore::new()))
    }

    pub fn disk(base: std::path::PathBuf, options: DiskOptions) -> Result<Self, StoreError> {
        Ok(Self::new(Box::new(DiskStore::open(base, options)?)))
    }

    /// Shared out at startup and read from `/health` without any lock here.
    pub fn health(&self) -> Arc<WriteHealth> {
        Arc::clone(&self.health)
    }

    /// Data files open for writing, and the cap they are held under.
    pub fn open_files(&self) -> (usize, usize) {
        self.backend.open_files()
    }

    /// Template ids stored rows already name (`Store::claimed`).
    pub fn claimed_template_ids(&self) -> Vec<u64> {
        self.backend.claimed()
    }

    /// The disk store underneath, if there is one: what a bulk load commits through.
    pub fn disk_store(&self) -> Option<&DiskStore> {
        self.backend.as_disk()
    }

    /// The scan threads of the store underneath, if it has any (`Store::fan`).
    pub fn fan(&self) -> Option<Arc<fan::Fan>> {
        self.backend.fan()
    }

    /// Open the store on first write, then buffer the row; a failure to open is the row's,
    /// returned and counted, and the row is buffered nowhere.
    pub fn append(&self, template_id: u64, time_column: &str, row: Row) -> Result<(), StoreError> {
        self.append_at(template_id, time_column, row, 0)
    }

    /// `append`, for a row that came through the write-ahead log at `position` (0: none); an
    /// `Err` here is not the sender's, whose line is durable and replayed on the next start.
    pub fn append_at(
        &self,
        template_id: u64,
        time_column: &str,
        row: Row,
        position: u64,
    ) -> Result<(), StoreError> {
        let leaf = match self.lookup(template_id) {
            Some(leaf) => leaf,
            None => self.open_for_write(template_id, time_column)?,
        };
        leaf.append_at(row, position);
        Ok(())
    }

    /// See `Store::oldest_position`.
    pub fn oldest_position(&self) -> Option<u64> {
        self.backend.oldest_position()
    }

    /// The first row of a template this process has not opened a leaf for. Double-checked
    /// under the write lock: two opens would split one template's rows between two leaves.
    fn open_for_write(
        &self,
        template_id: u64,
        time_column: &str,
    ) -> Result<Arc<dyn LeafStore>, StoreError> {
        let mut stores = self.stores.write().unwrap_or_else(|e| e.into_inner());
        if let Some(leaf) = stores.get(&template_id) {
            return Ok(Arc::clone(leaf));
        }
        match self.backend.open(template_id, time_column) {
            Ok(leaf) => {
                stores.insert(template_id, Arc::clone(&leaf));
                drop(stores);
                lock(&self.unopened).remove(&template_id);
                Ok(leaf)
            }
            Err(e) => {
                drop(stores);
                self.note_failure(&e);
                self.rows_dropped.fetch_add(1, Ordering::Relaxed);
                lock(&self.unopened).insert(template_id, time_column.to_string());
                Err(e)
            }
        }
    }

    /// The batch entry point of the ingest path, which stops at the first row the store refuses.
    pub fn append_batch_to(
        &self,
        template_id: u64,
        time_column: &str,
        rows: impl IntoIterator<Item = Row>,
    ) -> Result<(), StoreError> {
        for row in rows {
            self.append(template_id, time_column, row)?;
        }
        Ok(())
    }

    /// Open a leaf the trie already knows about, without writing to it: the restart path, where
    /// a template with no stored rows opens empty and is not an error.
    pub fn open_existing(&self, template_id: u64, time_column: &str) -> Result<(), StoreError> {
        let mut stores = self.stores.write().unwrap_or_else(|e| e.into_inner());
        if stores.contains_key(&template_id) {
            return Ok(());
        }
        let store = self.backend.open(template_id, time_column)?;
        stores.insert(template_id, store);
        Ok(())
    }

    /// Count a failure, record what it was and mark the store degraded. Reported once, since a
    /// dead disk fails on every line; the stored string is the latest, for `/health`.
    fn note_failure(&self, e: &StoreError) {
        if self.write_failures.fetch_add(1, Ordering::Relaxed) == 0 {
            kibbr_core::error::report(e);
        }
        self.health.set_error(kibbr_core::error::chain(e));
        self.health.degrade();
    }

    /// See `Store::generation`.
    pub fn generation(&self) -> u64 {
        self.backend.generation()
    }

    /// The templates a query planned, in one snapshot: see `Snapshot`.
    pub fn snapshot(&self, template_ids: &[u64], opts: &ScanOptions) -> Box<dyn Snapshot> {
        let leaves = template_ids
            .iter()
            .filter_map(|id| self.lookup(*id).map(|leaf| (*id, leaf)))
            .collect();
        self.backend.snapshot(leaves, opts)
    }

    pub fn get(&self, template_id: u64) -> Result<Arc<dyn LeafStore>, StoreError> {
        self.lookup(template_id)
            .ok_or(StoreError::NotOpen { template_id })
    }

    /// A leaf with no store is a template that exists and holds no rows - the second of
    /// the two empty answers, and not an error.
    pub fn lookup(&self, template_id: u64) -> Option<Arc<dyn LeafStore>> {
        self.stores
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&template_id)
            .map(Arc::clone)
    }

    /// Close every buffer that is due and try again to open every leaf `append` could not;
    /// never `Err`, since one leaf failing must not stop the rest being written.
    pub fn flush_all(&self, kind: FlushKind) -> FlushSummary {
        let started = Instant::now();
        let done = self.backend.flush(kind);
        let failed: BTreeSet<u64> = done.failed.iter().map(|(id, _)| *id).collect();
        let mut failures: Vec<StoreError> = done.failed.into_iter().map(|(_, e)| e).collect();
        // Counted apart from `failed`: a seal that failed has lost no row, but it is still a
        // store that could not write, so it reaches the verdict below.
        let seal_failures = done.seal_failed.len();
        failures.extend(done.seal_failed);
        let unopened = self.retry_unopened();
        for e in &failures {
            kibbr_core::error::report(e);
        }
        if let Some(e) = failures.first() {
            self.write_failures
                .fetch_add(failures.len() as u64, Ordering::Relaxed);
            self.health.set_error(kibbr_core::error::chain(e));
        }
        self.forced_closes
            .fetch_add(done.forced as u64, Ordering::Relaxed);
        // The verdict `/health` reads. A final flush sets it too: a shutdown that could
        // not write is degraded for whatever is left of the process.
        self.health.tick(failed.len() + seal_failures + unopened);
        FlushSummary {
            rows: done.rows,
            partitions: done.partitions,
            row_groups: done.row_groups,
            bytes: done.bytes,
            syncs: done.syncs,
            sealed: done.sealed,
            forced: done.forced,
            failed: failed.len(),
            took: started.elapsed(),
        }
    }

    /// Try again to open every leaf `append` could not, and count the ones that still fail: a
    /// leaf that will not open holds no buffer, so no other pass would keep `degraded` set.
    fn retry_unopened(&self) -> usize {
        let pending: Vec<(u64, String)> = std::mem::take(&mut *lock(&self.unopened))
            .into_iter()
            .collect();
        let mut failed = 0;
        for (template_id, time_column) in pending {
            let mut stores = self.stores.write().unwrap_or_else(|e| e.into_inner());
            if stores.contains_key(&template_id) {
                continue;
            }
            match self.backend.open(template_id, &time_column) {
                Ok(store) => {
                    stores.insert(template_id, store);
                }
                Err(e) => {
                    drop(stores);
                    failed += 1;
                    self.note_failure(&e);
                    lock(&self.unopened).insert(template_id, time_column);
                }
            }
        }
        failed
    }

    /// Every open leaf, in template order, as they stand now. Copied out so that nothing
    /// done with them holds the map's lock.
    fn leaves(&self) -> Vec<(u64, Arc<dyn LeafStore>)> {
        self.stores
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(id, leaf)| (*id, Arc::clone(leaf)))
            .collect()
    }

    pub fn open(&self) -> usize {
        self.stores.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Rows held across every open store, summed and not counted on the way in: an append
    /// counter reads 0 after a restart.
    pub fn rows(&self) -> u64 {
        self.leaves().iter().map(|(_, s)| s.len()).sum()
    }

    pub fn buffered(&self) -> u64 {
        self.leaves().iter().map(|(_, s)| s.buffered()).sum()
    }

    /// Every open store's `approx_bytes`, summed: `/stats`'s storage size.
    pub fn approx_bytes(&self) -> u64 {
        self.leaves().iter().map(|(_, s)| s.approx_bytes()).sum()
    }

    /// Memory held by buffered rows, and the limit past which the largest are closed early.
    pub fn buffer_bytes(&self) -> (u64, u64) {
        self.backend.buffer_bytes()
    }

    /// Whether the buffers are past their limit, so the flush should not wait for its next
    /// tick; an atomic and no lock, because it is read on the ingest path.
    pub fn over_buffer_limit(&self) -> bool {
        let (held, limit) = self.backend.buffer_bytes();
        limit > 0 && held > limit
    }

    /// Whether the buffers are past twice their limit, so the door should take nothing more
    /// until the flush catches up (`DEFAULT_MAX_BUFFER_BYTES`).
    pub fn buffers_full(&self) -> bool {
        let (held, limit) = self.backend.buffer_bytes();
        limit > 0 && held > limit.saturating_mul(2)
    }

    /// Open stores in template order, for `/templates` and `/stats`.
    pub fn iter(&self) -> impl Iterator<Item = (u64, Arc<dyn LeafStore>)> {
        self.leaves().into_iter()
    }

    pub fn kind(&self) -> &'static str {
        self.backend.kind()
    }

    pub fn base_path(&self) -> Option<&Path> {
        self.backend.base_path()
    }

    pub fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }

    /// Rows `append` refused over the life of this manager, never cleared.
    pub fn rows_dropped(&self) -> u64 {
        self.rows_dropped.load(Ordering::Relaxed)
    }

    pub fn forced_closes(&self) -> u64 {
        self.forced_closes.load(Ordering::Relaxed)
    }

    pub fn last_write_error(&self) -> Option<String> {
        self.health.last_error()
    }

    pub fn is_degraded(&self) -> bool {
        self.health.is_degraded()
    }
}

impl Default for StoreManager {
    fn default() -> Self {
        Self::memory()
    }
}

// ── Reading a row's place in time ──────────────────────────────────────────────────

/// A value as an instant - a `Datetime`, or text the ladder reads as one - or `None`;
/// apart from `ts_of` because a scan reads instants off a decoded chunk, not a row.
pub fn instant_of(value: &ColValue) -> Option<i64> {
    match value {
        ColValue::Datetime(ns) => Some(*ns),
        ColValue::String(text) => match infer(text).value {
            ColValue::Datetime(ns) => Some(ns),
            _ => None,
        },
        _ => None,
    }
}

/// A row's value under `column` as an instant: `instant_of` it, and `None` for a row
/// that has no such column.
pub fn ts_of(row: &Row, column: &str) -> Option<i64> {
    row.get(column).and_then(instant_of)
}

/// The effective time of a row: the template's time column where it holds an instant, and
/// otherwise its arrival time; `disk::scan` spells the same rule, so change both.
pub fn effective_ts(row: &Row, column: &str) -> Option<i64> {
    ts_of(row, column).or_else(|| ts_of(row, RECEIVED_AT_COLUMN))
}

/// Whether an instant falls in `[since, until)`; a row with no effective time is outside
/// every window.
pub fn in_window(ts: Option<i64>, since: Option<i64>, until: Option<i64>) -> bool {
    let Some(ns) = ts else {
        return false;
    };
    since.is_none_or(|from| ns >= from) && until.is_none_or(|to| ns < to)
}

/// A row as a query shows it, as `(name shown, key in the row)`: the system column under
/// its query name, unless an ingested key has that name or its value is null.
pub fn present(row: &Row) -> BTreeMap<String, &str> {
    let mut out: BTreeMap<String, &str> = row
        .keys()
        .filter(|key| key.as_str() != RECEIVED_AT_COLUMN)
        .map(|key| (key.clone(), key.as_str()))
        .collect();
    if let Some(system) = row.get(RECEIVED_AT_COLUMN) {
        if !matches!(system, ColValue::Null) && !out.contains_key(RECEIVED_AT_NAME) {
            out.insert(RECEIVED_AT_NAME.to_string(), RECEIVED_AT_COLUMN);
        }
    }
    out
}

/// A `Datetime` stored in a column that is now text, as the text a query compares it
/// against: the JSON the line said; `None` for any other value.
pub fn render_text(value: &ColValue, written: Option<&serde_json::Value>) -> Option<ColValue> {
    let ColValue::Datetime(ns) = value else {
        return None;
    };
    Some(ColValue::String(match written {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => kibbr_core::fmt::rfc3339(*ns),
    }))
}

/// A row with every text column's `Datetime` rendered back to the text it arrived as,
/// or `None` when nothing in it needs rendering.
pub fn render_row(row: &Row, text_columns: &[String]) -> Option<Row> {
    let mut out: Option<Row> = None;
    for column in text_columns {
        let Some(value) = row.get(column) else {
            continue;
        };
        if let Some(text) = render_text(value, row.written(column)) {
            out.get_or_insert_with(|| row.clone())
                .set_value(column, text);
        }
    }
    out
}

/// Pre-1970 lands on the far side of the epoch rather than saturating at it.
pub fn system_time(dt: chrono::DateTime<chrono::Utc>) -> SystemTime {
    let secs = dt.timestamp();
    let nanos = dt.timestamp_subsec_nanos();
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH - Duration::new(secs.unsigned_abs(), 0) + Duration::from_nanos(nanos.into())
    }
}

/// Unix nanoseconds, saturating at both ends: a bound outside 1677-2262 is clamped to the
/// near end, never wrapped past the far one.
pub fn nanos_of(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos()).map_or(i64::MIN, |n| -n),
    }
}

/// One value's payload width: a string's bytes, or the fixed-width cell the column store
/// writes.
pub fn value_bytes(value: &ColValue) -> usize {
    match value {
        ColValue::String(s) => s.len(),
        ColValue::Int64(_) | ColValue::Float64(_) | ColValue::Datetime(_) => 8,
        ColValue::Bool(_) => 1,
        ColValue::Null => 0,
    }
}

pub fn project(row: &Row, columns: &[String]) -> Row {
    if columns.is_empty() {
        return row.clone();
    }
    let mut out = Row::with_capacity(columns.len());
    for name in columns {
        if let Some(value) = row.get(name) {
            out.insert_written(name.clone(), value.clone(), row.written(name).cloned());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backend whose leaves fail to open until `shut` is cleared: what a `chmod 000`
    /// directory looks like from the manager, and then the `chmod 755`.
    struct Shut(std::sync::Arc<std::sync::atomic::AtomicBool>);

    impl Store for Shut {
        fn open(&self, template_id: u64, t: &str) -> Result<Arc<dyn LeafStore>, StoreError> {
            if self.0.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(StoreError::Io {
                    template_id,
                    source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
                });
            }
            MemoryStore::new().open(template_id, t)
        }

        fn kind(&self) -> &'static str {
            "shut"
        }

        fn base_path(&self) -> Option<&Path> {
            None
        }
    }

    /// The store stays degraded through any number of ticks until the leaf opens: a tick that
    /// cleared it sooner would let a retrying shipper's next body in, to store lines twice.
    #[test]
    fn a_row_whose_leaf_will_not_open_is_refused_and_the_store_stays_degraded() {
        let shut = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let stores = StoreManager::new(Box::new(Shut(std::sync::Arc::clone(&shut))));
        for n in 0..3 {
            let refused = stores.append(7, DEFAULT_TS_COLUMN, Row::new());
            assert!(
                matches!(refused, Err(StoreError::Io { template_id: 7, .. })),
                "row {n} was not refused: {refused:?}"
            );
        }
        assert_eq!(stores.rows_dropped(), 3);
        assert_eq!(stores.rows(), 0);
        assert!(stores.is_degraded());

        for _ in 0..3 {
            stores.flush_all(FlushKind::Tick);
            assert!(
                stores.is_degraded(),
                "a tick cleared degraded with the leaf still shut"
            );
        }

        shut.store(false, std::sync::atomic::Ordering::Relaxed);
        stores.flush_all(FlushKind::Tick);
        assert!(
            !stores.is_degraded(),
            "the leaf opened and the store is still degraded"
        );
        assert!(
            stores.lookup(7).is_some(),
            "the tick did not keep the leaf it opened"
        );
        // The count is never cleared: those rows are still gone.
        assert_eq!(stores.rows_dropped(), 3);
        stores
            .append(7, DEFAULT_TS_COLUMN, Row::new())
            .expect("the leaf is open now");
        assert_eq!(stores.rows(), 1);
    }

    #[test]
    fn a_row_the_store_takes_is_not_counted_as_dropped() {
        let stores = StoreManager::memory();
        stores
            .append(7, DEFAULT_TS_COLUMN, Row::new())
            .expect("the memory store always opens");
        assert_eq!(stores.rows_dropped(), 0);
        assert_eq!(stores.rows(), 1);
    }

    /// Past twice the buffer limit the buffers are full - what `/ingest` refuses bodies on -
    /// and a flush, closing the largest early, empties them again.
    #[test]
    fn the_buffers_are_full_past_twice_their_limit_until_a_flush() {
        let dir = std::env::temp_dir().join(format!("kibbr-full-{}", std::process::id()));
        let row = |n: i64| {
            let mut row = Row::new();
            row.insert(
                "ts".into(),
                ColValue::Datetime(1_705_314_600_000_000_000 + n),
            );
            row.insert("n".into(), ColValue::Int64(n));
            row
        };
        // What one such row holds, by the store's own count.
        let _ = std::fs::remove_dir_all(&dir);
        let probe = StoreManager::disk(dir.clone(), DiskOptions::default()).expect("open");
        probe.append(7, "ts", row(0)).expect("append");
        let each = probe.buffer_bytes().0;
        drop(probe);

        let _ = std::fs::remove_dir_all(&dir);
        let options = DiskOptions {
            max_buffer_bytes: 5 * each,
            ..DiskOptions::default()
        };
        let stores = StoreManager::disk(dir.clone(), options).expect("open");
        for n in 0..6 {
            stores.append(7, "ts", row(n)).expect("append");
        }
        assert!(stores.over_buffer_limit() && !stores.buffers_full());
        for n in 6..11 {
            stores.append(7, "ts", row(n)).expect("append");
        }
        assert!(stores.buffers_full(), "{:?}", stores.buffer_bytes());
        let done = stores.flush_all(FlushKind::Tick);
        assert_eq!((done.forced, done.rows), (1, 11));
        assert!(!stores.over_buffer_limit() && !stores.buffers_full());
        drop(stores);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The invariant at `disk::DiskLeaf::state`, under load: every snapshot holds every row
    /// appended before it began, once and in place. `KIBBR_SNAPSHOT_SECONDS` sets how long (10).
    #[test]
    fn every_snapshot_sees_every_row_exactly_once_while_row_groups_close() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

        const ID: u64 = 7;
        const T0: i64 = 1_705_314_600_000_000_000;
        const WINDOW: u64 = 2_048;
        let seconds: u64 = std::env::var("KIBBR_SNAPSHOT_SECONDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        let dir = std::env::temp_dir().join(format!("kibbr-snapshot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // Every flush closes whatever is buffered, in row groups of 64: a close is always
        // under way, and a snapshot meets rows in row groups, moved aside and buffered.
        let options = DiskOptions {
            row_groups: RowGroupLimits {
                rows: 64,
                bytes: 1 << 20,
                age: Duration::ZERO,
            },
            ..DiskOptions::default()
        };
        let stores = StoreManager::disk(dir.clone(), options).expect("open");

        let appended = AtomicU64::new(0);
        let stop = AtomicBool::new(false);
        let closes = AtomicU64::new(0);
        let row_groups = AtomicU64::new(0);
        let flushes = AtomicU64::new(0);
        let snapshots = AtomicU64::new(0);
        let twice = AtomicU64::new(0);
        let missing = AtomicU64::new(0);
        let misplaced = AtomicU64::new(0);

        /// One snapshot's rows, `(n, position)`, against every row from `lo` up to at
        /// least `floor`: rows seen twice, rows missing, rows out of place.
        fn check(mut seen: Vec<(i64, u64)>, lo: u64, floor: u64) -> (u64, u64, u64) {
            seen.sort_unstable();
            let (mut twice, mut missing, mut misplaced) = (0, 0, 0);
            let mut next = lo;
            for (n, seq) in seen {
                let n = n as u64;
                if n < next {
                    twice += 1;
                    continue;
                }
                missing += n - next;
                // A single day, so a row's position is its place among all of them.
                if seq != n {
                    misplaced += 1;
                }
                next = n + 1;
            }
            missing += floor.saturating_sub(next);
            (twice, missing, misplaced)
        }

        let row = |n: i64| {
            let mut row = Row::new();
            row.insert("ts".into(), ColValue::Datetime(T0 + n));
            row.insert("n".into(), ColValue::Int64(n));
            row
        };
        let snapshot = |since: Option<u64>| {
            let leaf = stores.lookup(ID)?;
            let opts = ScanOptions {
                ts_column: Some("ts".into()),
                since: since.map(|n| UNIX_EPOCH + Duration::from_nanos((T0 + n as i64) as u64)),
                ..ScanOptions::default()
            };
            let mut seen = Vec::new();
            leaf.visit(
                &opts,
                &[],
                &mut Rows(|row: &Row, seq: u64| {
                    if let Some(ColValue::Int64(n)) = row.get("n") {
                        seen.push((*n, seq));
                    }
                }),
            )
            .expect("visit");
            Some(seen)
        };

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut n = 0i64;
                while !stop.load(Relaxed) {
                    for _ in 0..32 {
                        stores.append(ID, "ts", row(n)).expect("append");
                        n += 1;
                        appended.store(n as u64, Release);
                    }
                    std::thread::sleep(Duration::from_micros(250));
                }
            });
            scope.spawn(|| {
                while !stop.load(Relaxed) {
                    let done = stores.flush_all(FlushKind::Tick);
                    assert_eq!(done.failed, 0, "a flush failed");
                    closes.fetch_add(done.partitions as u64, Relaxed);
                    row_groups.fetch_add(done.row_groups as u64, Relaxed);
                    flushes.fetch_add(done.syncs as u64, Relaxed);
                }
            });
            for _ in 0..4 {
                scope.spawn(|| {
                    while !stop.load(Relaxed) {
                        let floor = appended.load(Acquire);
                        let lo = floor.saturating_sub(WINDOW);
                        let Some(seen) = snapshot(Some(lo)) else {
                            continue;
                        };
                        let (t, m, p) = check(seen, lo, floor);
                        twice.fetch_add(t, Relaxed);
                        missing.fetch_add(m, Relaxed);
                        misplaced.fetch_add(p, Relaxed);
                        snapshots.fetch_add(1, Relaxed);
                    }
                });
            }
            std::thread::sleep(Duration::from_secs(seconds));
            stop.store(true, Relaxed);
        });

        // The whole leaf, once nothing is moving; then written and sealed, and read back.
        let rows = appended.load(Acquire);
        let whole = check(snapshot(None).expect("the leaf"), 0, rows);
        let done = stores.flush_all(FlushKind::Final);
        assert_eq!(done.failed, 0, "the final flush failed");
        drop(stores);
        let reopened = StoreManager::disk(dir.clone(), options).expect("reopen");
        reopened.open_existing(ID, "ts").expect("leaf");
        let after = reopened.lookup(ID).map_or(0, |leaf| leaf.len());
        drop(reopened);
        let _ = std::fs::remove_dir_all(&dir);

        let (twice, missing, misplaced) = (
            twice.load(Relaxed),
            missing.load(Relaxed),
            misplaced.load(Relaxed),
        );
        eprintln!(
            "snapshots under closes: {seconds}s, {rows} rows appended, {} closes into {} row \
             groups under {} flushes, {} snapshots by 4 threads; {twice} rows seen twice, \
             {missing} missing, {misplaced} out of place; the whole leaf at the end {:?}; \
             {after} rows after a restart",
            closes.load(Relaxed),
            row_groups.load(Relaxed),
            flushes.load(Relaxed),
            snapshots.load(Relaxed),
            whole,
        );
        assert!(
            snapshots.load(Relaxed) > 0 && closes.load(Relaxed) > 0,
            "nothing ran"
        );
        assert_eq!(
            (twice, missing, misplaced),
            (0, 0, 0),
            "snapshots saw rows twice, missed them, or out of place"
        );
        assert_eq!(whole, (0, 0, 0), "the whole leaf is not every row once");
        assert_eq!(after, rows, "a restart does not hold every row");
    }

    // ── A term, and a row that was not rendered ────────────────────────────────────

    /// 2024-01-15T10:30:00Z: the instant the pool spells in every form the ladder takes.
    const INSTANT: i64 = 1_705_314_600_000_000_000;

    fn spelling(text: &str) -> Option<serde_json::Value> {
        Some(serde_json::Value::String(text.to_string()))
    }

    /// A row whose one key, `v`, holds `value` beside what a line wrote for it.
    fn holding(value: ColValue, written: Option<serde_json::Value>) -> Row {
        let mut row = Row::new();
        row.insert_written("v".into(), value, written);
        row
    }

    /// A `Datetime` under each kind of spelling `render_text` tells apart, and a value of every
    /// other type with a spelling kept and with none.
    fn spelled() -> Vec<Row> {
        let seconds = serde_json::Value::from(1_705_314_600_i64);
        let millis = serde_json::Value::from(1_705_314_600_000_i64);
        vec![
            holding(ColValue::Datetime(INSTANT), None),
            holding(
                ColValue::Datetime(INSTANT),
                spelling("2024-01-15T16:00:00+05:30"),
            ),
            holding(
                ColValue::Datetime(INSTANT),
                spelling(&kibbr_core::fmt::rfc3339(INSTANT)),
            ),
            holding(ColValue::Datetime(INSTANT), Some(seconds)),
            holding(ColValue::Datetime(INSTANT), Some(millis)),
            holding(ColValue::Datetime(INSTANT), spelling("1705314600")),
            holding(ColValue::String("INFO".into()), None),
            holding(
                ColValue::String("[1,2]".into()),
                Some(serde_json::json!([1, 2])),
            ),
            holding(ColValue::Int64(45), None),
            holding(ColValue::Int64(45), spelling("0045")),
            holding(ColValue::Float64(200.0), None),
            holding(ColValue::Float64(200.0), spelling("2e2")),
            holding(ColValue::Bool(true), None),
            holding(ColValue::Bool(true), spelling("True")),
            holding(ColValue::Null, None),
        ]
    }

    /// Rendering clones the row to change one value; this is what lets a scan use the two
    /// unrendered readings in its place.
    #[test]
    fn a_term_reads_a_stored_row_as_it_reads_the_rendered_one() {
        let mut rows = spelled();
        rows.extend(pool::rows());
        let mut terms = vec![Term::any("v"), Term::any("w"), Term::exact("w", "INFO")];
        terms.extend(pool::TEXTS.iter().map(|text| Term::exact("v", text)));

        let mut matched = [0u32; 2];
        for row in &rows {
            for text_columns in [Vec::new(), vec!["v".to_string()]] {
                let rendered = render_row(row, &text_columns);
                let seen = rendered.as_ref().unwrap_or(row);
                for term in &terms {
                    let is_text = text_columns.iter().any(|column| column == term.key());
                    let want = term.matches_row(seen);
                    assert_eq!(
                        term.matches_in(row, is_text),
                        want,
                        "{term:?} put to {row:?}, text column: {is_text}"
                    );
                    let by_value = row.get(term.key()).is_some_and(|value| {
                        term.matches_cell(value, row.written(term.key()), is_text)
                    });
                    assert_eq!(
                        by_value, want,
                        "{term:?} put to the value of {row:?}, text column: {is_text}"
                    );
                    matched[usize::from(is_text)] += u32::from(want);
                }
            }
        }
        // Not agreement on nothing: both readings said yes, and often.
        assert!(matched[0] > 100 && matched[1] > 100, "{matched:?}");

        // The case the two readings exist for, by hand: in a `Datetime` column every spelling of
        // the instant finds the row; once the column is text, only the one it arrived in does.
        let zulu = Term::exact("v", "2024-01-15T10:30:00Z");
        let arrived = Term::exact("v", "2024-01-15T16:00:00+05:30");
        let digits = Term::exact("v", "1705314600");
        let offset = holding(
            ColValue::Datetime(INSTANT),
            spelling("2024-01-15T16:00:00+05:30"),
        );
        assert!(zulu.matches_in(&offset, false) && arrived.matches_in(&offset, false));
        assert!(!zulu.matches_in(&offset, true) && arrived.matches_in(&offset, true));
        let epoch = holding(
            ColValue::Datetime(INSTANT),
            Some(serde_json::Value::from(1_705_314_600_i64)),
        );
        assert!(zulu.matches_in(&epoch, false) && digits.matches_in(&epoch, false));
        assert!(!zulu.matches_in(&epoch, true) && digits.matches_in(&epoch, true));
        // A row without the key matches nothing, `*` included.
        assert!(!Term::any("w").matches_in(&epoch, false) && !Term::any("w").matches_row(&epoch));
    }
}
