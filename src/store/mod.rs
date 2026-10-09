//! Leaf storage: the scan interface, the two implementations behind it, and the registry.
//!
//! **The traits appear here and not earlier on purpose.** An interface is a compression of
//! what its callers need, and with one implementation it is a guess that gets baked into
//! tests which then resist the first real requirement. Memory and disk both exist now, so
//! `LeafStore` and `Store` are a description rather than a prediction.
//!
//! The split is three ways and it matters:
//!
//! - **`LeafStore`** holds one leaf's rows. **`Store`** makes leaves and writes their rows
//!   out with a synchronous `flush()`: the rows of every template share a day's file, so
//!   the flush that closes their row groups belongs to the store that owns the files and
//!   not to any one leaf. Neither has **a clock**. Neither can know when it should write,
//!   and giving one a timer would make it untestable without a runtime.
//! - **`StoreManager`** owns the registry of open leaves. The interval that calls
//!   `flush()` lives in `serve`, watching the same cancellation token as everything else,
//!   because the clock belongs to the process lifecycle and not to storage.
//! - **The ingest path** has neither. It calls `append` and returns.
//!
//! **There is no lock over all of it.** Each leaf locks itself, the registry is locked
//! only to find or add a leaf, and a flush holds no lock an append or a query waits on
//! while it touches the disk - `disk::DiskLeaf::state` has the invariant that makes that
//! safe. One `RwLock` over every leaf of every template, held by the flush across the
//! encode, the write and the fsync of every row group it closed, measured ingest falling
//! from 44,800 lines a second to 1,645 with queries running, and a count by a promoted
//! value going from 33ms to 1.1s.
//!
//! `StoreManager` is a concrete struct and not a trait, which is a deliberate reading of
//! the rule above: the registry, the lazy open and the flush loop
//! are identical whichever backend they drive, so there is exactly one implementation of
//! a manager and a trait over it would be the guess the rule forbids. The two things that
//! genuinely have two implementations are the two that became traits.

// Part of this interface still has no caller. `scan` is `visit` with a `Vec` under it and
// every caller wants the `Visitor`; `NotOpen` is what `get` returns and the engine goes
// through `lookup`; `template_id` and `iter` are the shape `/templates` will read a leaf
// through once it reports more than a row count. Kept because the contract is what the two
// implementations were written against, and scoped so the allow cannot spread past this
// module.
//
// `ScanOptions::columns` is no longer among them. It was unreachable while a scan's filter
// was a list of opaque `Box<dyn Fn(&Row)>`, which nothing could ask which columns they
// read. A filter is now a list of `Term`s, and a term is data that still has its key: the
// engine names the columns from the same terms it hands the scan. See `engine::execute`.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::infer::infer;
use crate::types::ColValue;

pub mod columns;
pub mod disk;
pub mod fan;
pub mod manifest;
pub mod memory;
#[cfg(test)]
pub(crate) mod pool;
pub mod v3;

pub use disk::DiskStore;
pub use memory::MemoryStore;

/// One row: each key's value as the ladder read it, and - only where printing that value
/// would not give back what the line said - the JSON value the line said.
///
/// **What goes in comes back out.** A query returns the JSON value that was ingested: a
/// string character for character, a number as the same number. `"0045000012"` is an
/// `Int64` to every comparison and the ten characters to every answer; `1705312200` is a
/// `Datetime` to the time window and a JSON number to the answer. The value is what a
/// query compares against, and folding the spelling into it would change what
/// `ts=1705314600` matches; so the spelling rides beside it, and is kept only for the few
/// values whose canonical print differs - a plain string or a JSON integer costs nothing.
///
/// Absence on a line is `ColValue::Null`, not a missing entry - the row is built from the
/// leaf's key set, and the key set is the path.
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

    /// A value, and the JSON the line said where that differs from its canonical print.
    /// Passing JSON equal to the canonical print keeps nothing, so every source of rows -
    /// the parser, the stores, the oracle - ends with the same row for the same line.
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

    /// Roughly the memory this row holds: its two hash tables, and an allocation for every
    /// key and every string value, rounded up as the allocator rounds them.
    ///
    /// **Several times its payload, which is why this exists.** A generated line of 182
    /// bytes of keys and values is 1,537 here and held 1.8KB of resident size as a buffered
    /// row: a hash table of sixteen slots for eight keys, and an allocation per string. A
    /// limit on memory that counted payload would let eight times its figure in.
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

/// One term of a pattern that the path could not settle, as a scan holds it: the key it
/// reads, and what it wants there.
///
/// **Data, and read through the ladder once.** A term's text has two readings - itself,
/// and what the ladder types it as; `engine::value_matches` says why - and neither depends
/// on the row. While a term was a closure over its text both were made again for every
/// row: the text cloned, and the whole ladder run on it wherever the string reading missed,
/// which is every row that does not match. `Term::exact` makes them when the term is made,
/// and a scan only compares.
///
/// Data still has its key, too, which a closure could not be asked for: a store can tell
/// which column a term reads, and put the term to a value before any row holds it.
///
/// Still passed beside `ScanOptions` and not inside it. That began because a boxed closure
/// is not `Clone` and would have cost the options their `Default`; it stays because the
/// options are what a snapshot is taken with, before any leaf's terms exist.
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

    /// A term for one value. The one place the ladder is run on a term's text.
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

    /// Does a value satisfy the term? `seen` is the value as a rendered row holds it.
    ///
    /// `engine::value_matches`, with nothing made per call: the string reading first, so a
    /// JSON string `"149.00"` is matched by its own text, and then the ladder's reading,
    /// which is how `ts=2024-01-15T10:30:00Z` finds a `Datetime`. The oracle still calls
    /// `value_matches` itself, so the two are held to each other by every differential,
    /// and value by value by a test beside it.
    pub fn matches(&self, seen: &ColValue) -> bool {
        match &self.want {
            Want::Any => !matches!(seen, ColValue::Null),
            Want::Exact { text, inferred } => {
                matches!(seen, ColValue::String(s) if s == text) || seen == inferred
            }
        }
    }

    /// `matches`, for one stored value no row was rendered for. `text_column` says the
    /// column's type is now `String`: a `Datetime` stored there is compared as the text it
    /// arrived as, which is what `render_row` would have put in its place.
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

    /// A stored row, not rendered: what `matches_row` answers for `render_row` of it, with
    /// no row cloned to find out. `text_column`: the term's key is a column whose type is
    /// now `String`.
    ///
    /// What the line wrote is looked up only in the one arm that reads it, a `Datetime` in
    /// a text column. Asked for every row it is a second hash of the key wherever a row
    /// keeps any spelling at all, for a value nearly no row needs.
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

/// The column a time filter reads when none is named. A fallback for a `_leaf.json` that
/// predates the time source; every current caller names the column.
pub const DEFAULT_TS_COLUMN: &str = "ts";

/// The system column holding a row's arrival time, on rows whose template's time column
/// gave them none.
///
/// A backslash and an underscore: key escaping writes `\\` for a backslash, so no
/// flattened key can ever be spelled this way - the collision is unreachable rather than
/// detected. It is added after fingerprinting and never enters a key set, a fingerprint or
/// a template id. A query shows it as `RECEIVED_AT_NAME`.
pub const RECEIVED_AT_COLUMN: &str = "\\_received_at";

/// What a query returns the system column as, unless the row has an ingested key of that
/// name - then the ingested one is shown and the system value is left out.
pub const RECEIVED_AT_NAME: &str = "_received_at";

#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Empty = every column.
    ///
    /// **A read hint, not a projection guarantee.** A store may return only these, and a
    /// caller must not depend on anything else being present - which is what lets the disk
    /// store read two chunks out of eight. The in-memory store ignores it: its rows are
    /// already rows, and slicing them would cost an allocation to save nothing.
    pub columns: Vec<String>,
    /// 0 = unlimited.
    pub limit: usize,
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
    /// The template's time column: what the window and the order read, falling
    /// back to `RECEIVED_AT_COLUMN` on a row where it holds no `Datetime`.
    pub ts_column: Option<String>,
    /// Columns whose type is now `String`. A value stored as a `Datetime` in one of them
    /// is compared as the text it arrived as, before anything reads the row.
    pub text_columns: Vec<String>,
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
    /// A file the manifest names that a start could not look for: `dir`, a directory above
    /// it, would not be searched. **Not `NamedAbsent`** - the file may be there and whole,
    /// and nothing here knows.
    ///
    /// `why` is the system's error, written into the sentence and not kept as a source: a
    /// start prints an error and then every cause below it, and a cause that is both would
    /// be said twice.
    #[error(
        "{dir}: {why}: manifest.log names {file}, and that file was not read - whether it \
         is on disk is not known"
    )]
    NamedUnread {
        dir: std::path::PathBuf,
        file: std::path::PathBuf,
        why: std::io::Error,
    },
    /// A file the manifest names that is not on disk: the system looked for it and said
    /// there is no such file. No source - that error would add nothing to the sentence.
    #[error("{file}: named by manifest.log and not on disk; its rows are gone")]
    NamedAbsent { file: std::path::PathBuf },
    #[error(transparent)]
    Build(#[from] v3::build::BuildError),
    #[error(transparent)]
    Manifest(#[from] manifest::ManifestError),
    /// A layout from before format v3 - a `leaf_<id>` directory, or a segment of another
    /// version. There is no upgrade path and no migration: the path is named and the
    /// process exits non-zero, rather than reading bytes it would have to guess the
    /// meaning of.
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
    /// A committed load whose files are still in `staging/`: until a restart moves them,
    /// this process's view is short of their rows, and another load staging over them
    /// would delete the only copy.
    #[error(
        "load {load_id:016x} is committed - or its record may be - and its files are still in \
         staging/; restart to settle it (the start finishes the load if its record is there \
         and removes it if not), then load again"
    )]
    MovePending { load_id: u64 },
    /// The record that commits a load failed partway: it may be on disk whole, in which
    /// case the load is committed. The staged files are kept for the next start to decide.
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
    /// Past the record that commits a load, a move out of `staging/` failed. The rows are
    /// safe - the next start finishes the move - but this process cannot show them.
    #[error(
        "load {load_id:016x} is committed, but moving its files into data/ failed ({source}). \
         Its rows are safe in staging/, and the next start moves them into place"
    )]
    NotMoved {
        load_id: u64,
        #[source]
        source: v3::FormatError,
    },
    /// A day compaction will not take as it stands: a file a failed seal left for the next
    /// start, a file from before every row went into its own day's, a commit only a restart
    /// can settle. Refused before a byte is written, but for the one seal that can fail
    /// inside the compaction - of the day's open file, as it begins - whose refusal carries
    /// the cause: the flush before it wrote what the day had in memory.
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
    /// A file the compaction was reading was retired under it - a load replaced the day.
    /// Not a failure: the day is the load's, and holds nothing left to compact.
    #[error(
        "compact {day}: {path} was retired while the day was being rebuilt - a load replaced \
         the day. The rebuilt files were removed and the day is as the load left it"
    )]
    InputsChanged { day: String, path: String },
    /// Stopped by its caller before the record. Not a failure.
    #[error(
        "compact {day}: cancelled. The rebuilt files were removed and the day's files are as \
         they were"
    )]
    Cancelled { day: String },
    /// The record that commits a compaction failed partway, and could not be shown to be
    /// off the disk: it may be there whole, in which case the rebuilt files are the day.
    /// Both sets stay, each whole, for the next start's manifest to choose between.
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
    /// The flush a compaction begins with - the day's buffered rows written, its open file
    /// sealed - failed. Not `CompactFailed`, which says the day's files are as they were:
    /// the flush may have written row groups, or named a file, before the write that
    /// failed.
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
    /// A compacted file and a file it was rebuilt from, both named live: every row of the
    /// parent is in the child too. No record this build writes leaves the two together, so
    /// the manifest is not what a compaction left, and the start stops rather than answer
    /// with rows counted twice.
    #[error(
        "{child}: its footer says it replaced {parent}, and manifest.log names both as live - \
         the rows of {parent} would be stored twice. Nothing was opened"
    )]
    ReplacedLive {
        child: std::path::PathBuf,
        parent: std::path::PathBuf,
    },
}

/// What one scan produced and what it cost.
///
/// A scan could return a bare `Vec<Row>`, but `QueryMeta` reports `rows_scanned` on every
/// result and the scan is the only thing that can count it. Carrying the counters out is
/// the smallest change that keeps `meta` populated from the start rather than bolted on.
#[derive(Debug, Default)]
pub struct Scan {
    pub rows: Vec<Row>,
    /// Row groups whose chunks were read.
    ///
    /// With `row_groups_pruned` beside it, this is what the segment format buys a query: a
    /// window covering one row group of fifty reads one. On every answer's `meta`, because
    /// a prune nobody can see is a prune nobody notices stopping.
    pub row_groups_read: u64,
    /// Row groups dropped on their footer timestamps, before any column data was read.
    pub row_groups_pruned: u64,
    /// Bytes of column chunk read off disk. Column pruning shows up here and nowhere
    /// else: naming two columns of eight reads two chunks.
    pub bytes_read: u64,
    /// Segment **files** opened and read - not leaves, and not row groups.
    ///
    /// `files_opened` on a query's `meta` used to count leaves and reported 870 where
    /// 16,694 segment files were read. A file whose every row group the
    /// window pruned is not counted: nothing in it was read.
    pub files_read: u64,
    /// Rows examined, including those the time window and the terms rejected.
    ///
    /// A partition the disk store skipped unread contributes nothing, which is the honest
    /// number and the reason the two backends report different ones. Making the disk
    /// store count rows it never opened would be reporting work it did not do.
    pub scanned: u64,
    /// Rows that passed everything.
    pub matched: u64,
}

// ── The cap on open data files ─────────────────────────────────────────────────────

/// How many data files may be open for writing at once. One is open per UTC day that has
/// taken a row since it was last sealed, so this bounds the days a burst of late rows can
/// keep open. At it, a row whose day has no open file seals the least recently written
/// file and opens its own day's: a row group is never filed under another day.
pub const DEFAULT_MAX_OPEN_SEGMENTS: usize = 512;

/// The memory every buffer together may hold before the largest are closed early - and
/// twice it, before `/ingest` refuses bodies until the flush catches up.
///
/// **An emergency brake, not a threshold the normal run of things reaches.** Closing a
/// buffer early makes a small row group, and a small row group compresses badly and costs
/// a header like a full one: modelled on a burst, a 256MB limit cut the peak from 689MB to
/// 423MB and forced 33,880 closes where 870 were needed. It fires when there are too many
/// buffers for the row group thresholds - 8,192 rows or 8MB per template and day - to close
/// them before the timeout, as there are when rows arrive across hundreds of templates and
/// a month of days at once.
///
/// **Closing early is not enough on its own.** Measured with nothing behind it: 108,000
/// lines a second across 27,000 template-days outran the flush, the buffers reached
/// 5,200,000 rows on an 8GB machine, and the process swapped until a query took minutes.
/// So past twice the limit the door sheds bodies with a `503`, and memory is bounded by the
/// limit and not by how fast the shippers are.
///
/// **Memory, as `Row::held_bytes` estimates it, and not payload** - a buffered row holds
/// eight times its keys and values. Measured on 200,000 generated lines: 182 bytes of
/// payload a row, 1,537 by the estimate, 1.8KB of resident size.
pub const DEFAULT_MAX_BUFFER_BYTES: u64 = 512 * 1024 * 1024;

/// A run of a day's row groups holding fewer rows than this, by the index, is chosen on
/// the thread that asked and never shared with the scan threads (`fan`).
///
/// **Four full row groups, one a thread.** Sharing a run out costs a wake-up of each
/// thread, measured at 0.03 to 0.06 ms of a query's time and 0.07 to 0.25 ms of CPU, and
/// choosing a row costs about 8 ns for a count and 14 ns for a fetch that is asked first:
/// four threads break even near 8,000 to 13,000 rows. This is three times that, and it is
/// the least power of two at which no query that was under two milliseconds on a day of a
/// million rows over 145 templates was shared out: at half of it one of them lost 2.5%,
/// and at a quarter 11%, because nearly every chunk it reads is a constant and there was
/// nothing for a second thread to decode.
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
    /// Threads one scan may choose rows on, the asking thread among them. 0 and 1: every
    /// scan chooses in turn. **The default is 0**: only a server answers queries from a
    /// disk store, and it asks for its threads (`config::Storage::with_scan`). A load, a
    /// compaction and a test get none.
    pub scan_threads: usize,
    /// See `DEFAULT_FAN_ROWS`. 0 in a test that wants every run of two row groups shared
    /// out.
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

/// What a flush is being asked to do.
///
/// **Closing a row group and sealing a segment are two different events**: this is the
/// difference between "close whatever a threshold fired on" and "close everything, seal".
///
/// Not a durability switch: a row group is one fsync for up to 8,192 rows of every column
/// at once, so closing always fsyncs and the only question left is whether it is time to
/// close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushKind {
    /// The interval. Closes a row group only where a threshold says so: every full one a
    /// buffer holds, and the short rest only once its oldest row has waited the timeout.
    Tick,
    /// Graceful shutdown. Closes every buffer and seals every open file.
    Final,
}

impl FlushKind {
    pub fn is_final(self) -> bool {
        self == FlushKind::Final
    }
}

/// When a row group closes.
///
/// The size thresholds keep chunks large enough to be worth encoding and to amortise the
/// row group header. The timeout is for low-traffic leaves that would never reach one.
/// Five minutes, where it was one before the write-ahead log: every row that waits is
/// already durable in the log, so what the wait costs is replay time after a crash and
/// not rows, and what it buys is a fifth of the row groups a quiet template makes.
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
    /// Templates whose rows could not be written, each with why. Their rows are back in
    /// their buffers, and the next flush tries them again. One failing is never fatal to
    /// the others: a single unwritable directory must not stop every other template's rows
    /// being persisted.
    pub failed: Vec<(u64, StoreError)>,
    /// Seals that failed. They cost no row - the file is recovered on the next start - but
    /// they are still a store that could not write.
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

/// What a visitor reads of a row that passed the window and the terms.
///
/// Asked once for a leaf, before anything is read. **A hint, as `ScanOptions::columns`
/// is**: a store whose rows are rows already may hand over more than was asked for, and a
/// visitor must not lean on being handed less. What it buys is on the disk store, where a
/// row is built out of columns: a count builds none, and a grouping builds a row of the
/// few columns it labels by.
///
/// Whatever is said here, `Scan::matched` counts every row that passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wants {
    /// Nothing: the count is `Scan::matched`. A store need build no row and call no `row`.
    Nothing,
    /// Rows holding at least these columns, in storage order.
    Columns(Vec<String>),
    /// Every column the scan read, in storage order. The default.
    Rows,
    /// Every column the scan read, in storage order, and only the rows `wants_row` says
    /// yes to: for a visitor that keeps the newest few it is handed and drops the rest,
    /// most of whose rows would be built to be dropped. A store that builds each row out
    /// of columns asks it about a row before building it; a store whose rows are rows
    /// already hands it every row, as it hands `Rows`.
    Newest,
}

/// Where a scan's surviving rows go.
///
/// A trait and not `&mut dyn FnMut(&Row, u64)`, which is what this was while storage was
/// a `Vec`. A closure cannot say "stop", and a store that reads day partitions
/// newest-first needs to ask whether the next one is worth opening at all.
pub trait Visitor {
    /// One surviving row, beside its ordinal within its partition.
    fn row(&mut self, row: &Row, seq: u64);

    /// May the next partition be skipped unread?
    ///
    /// `newest` is the greatest timestamp any row in it can carry; `None` is the undated
    /// partition, whose rows sort last. The default reads everything, which is the only
    /// correct answer for a count - a count cannot skip rows it has to count.
    fn wants_partition(&mut self, newest: Option<i64>) -> bool {
        let _ = newest;
        true
    }

    /// What `row` reads of the row it is handed: see `Wants`. The default is the whole
    /// row, which is what a closure that was never asked must be assumed to read.
    fn wants(&self) -> Wants {
        Wants::Rows
    }

    /// Is this row worth building? Asked of a visitor that said `Wants::Newest`, for a
    /// row that passed the window and the terms, before the row is built, in storage
    /// order.
    ///
    /// `ts` is the row's effective time, as `effective_ts` reads it off the row `row`
    /// would be handed, and `seq` is the ordinal `row` would be handed beside it.
    ///
    /// `placed` says that time lies in the day the store keeps the row under. A row's
    /// day is chosen from its time when it is stored and `ts` is read when it is asked
    /// for, and the two are one reading for as long as the ladder reads the stored value
    /// the way it did then. It is `false` only for a row where it no longer does, which
    /// is a row whose order among the others a visitor can no longer take from its time
    /// and its ordinal alone. A store with no days says `true`.
    ///
    /// `false` means the row cannot change what the visitor keeps. The row is then not
    /// built and not handed over, and it is still counted in `Scan::matched`.
    ///
    /// **Advisory.** A store may hand `row` a row it never asked about - the memory store
    /// asks about none - so `row` must not lean on having been asked: handed a row this
    /// would have refused, it must end with the answer it would have had without it.
    fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
        let _ = (ts, seq, placed);
        true
    }
}

/// The visitor of a count: it reads nothing of a row, and says so, so that a store which
/// would have to build each row out of columns builds none. The count itself is
/// `Scan::matched`, which the scan keeps whether or not anything is handed over.
pub struct Counted;

impl Visitor for Counted {
    fn row(&mut self, _: &Row, _: u64) {}

    fn wants(&self) -> Wants {
        Wants::Nothing
    }
}

/// A closure as a `Visitor`, for the call sites that only want rows.
///
/// Deliberately not a blanket `impl<F: FnMut(&Row, u64)> Visitor for F`: that impl would
/// cover every type in this crate that might later want to implement `Visitor` by hand,
/// and coherence would then refuse the hand-written ones.
pub struct Rows<F>(pub F);

impl<F: FnMut(&Row, u64)> Visitor for Rows<F> {
    fn row(&mut self, row: &Row, seq: u64) {
        (self.0)(row, seq)
    }
}

/// One leaf's rows. Append-only; nothing deletes.
///
/// `Send + Sync`, and every method takes `&self`: a leaf locks itself, so an append, a
/// query and a flush on it are told apart by its own lock, and none of them waits on a
/// lock over any other leaf.
pub trait LeafStore: Send + Sync {
    fn template_id(&self) -> u64;

    /// Buffer one row, and return the ordinal it was given.
    ///
    /// No I/O and no clock: this half has to stay synchronous and cheap, because it runs
    /// once per ingested line.
    fn append(&self, row: Row) -> u64;

    /// `append`, for a row that came through the write-ahead log: `position` is where its
    /// line is in the log, and the row group that takes the row records it. A store whose
    /// rows never leave memory has nothing to record it against. **Positions arrive in
    /// order**: the one applier hands them over as the log holds them, and a row group's
    /// range is only true of a buffer filled that way.
    fn append_at(&self, row: Row, position: u64) -> u64 {
        let _ = position;
        self.append(row)
    }

    /// Hand every surviving row to `v`, as far as `v` reads rows (`Visitor::wants`), and
    /// count every one of them in `Scan::matched` either way. The one place filtering
    /// happens, so the count, fetch and group-by paths cannot drift.
    fn visit(
        &self,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError>;

    /// Rows held, flushed and buffered together - what a query would see.
    fn len(&self) -> u64;

    /// Rows not yet in a row group. With a write-ahead log they are in it, and this is what
    /// the next start would replay; with none it is what a `kill -9` would cost.
    fn buffered(&self) -> u64;

    /// Roughly what this leaf weighs in bytes.
    fn approx_bytes(&self) -> u64;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every surviving row, projected through `opts.columns`.
    ///
    /// Defaulted once rather than written twice: it is `visit` with a `Vec` under it, and
    /// two copies of that would be two places for the projection rule to drift.
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

/// What one query reads: the templates it asked for, as the store held them at one moment.
///
/// **One snapshot per query, taken as it starts.** A load or a replace publishes every
/// template's row groups in one step, but a query that looked at each template as it
/// reached it could straddle that step and count some templates before it and the rest
/// after: measured, a `COUNT LOG {}` in flight across a load's commit answered 240,819 when
/// only 60,000 or 340,000 were ever true, and a count of a day being replaced came out
/// between its old and new counts. Every template is taken in one hold instead, and the
/// answer comes from that.
///
/// A file a replace retires after the snapshot is taken stays readable until the query
/// ends: the snapshot holds the file, and a retired file is deleted by its last holder.
pub trait Snapshot: Send {
    /// The store's publication count when this was taken: see `Store::generation`.
    fn generation(&self) -> u64;

    /// One template's rows as `LeafStore::visit` would have handed them over at the
    /// moment the snapshot was taken. `opts` carries the window the snapshot was taken
    /// with. `None` for a template it holds no leaf of: a known template with no rows.
    ///
    /// **Each template once**: what the snapshot held of it is let go as it is read, so a
    /// long query does not keep the rows and files of the templates it has finished with.
    fn visit(
        &mut self,
        template_id: u64,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Option<Result<Scan, StoreError>>;
}

/// The snapshot of a store that never publishes more than one row at a time: its leaves,
/// read as they stand when each is reached. The memory store takes no loads, so a query
/// against it can only straddle an ingest body - which is not atomic to a query at either
/// store.
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

/// Where leaves come from, and where their rows are written. The registry lives on
/// `StoreManager`, so this trait is the whole of what memory and disk actually differ by.
pub trait Store: Send + Sync {
    /// `time_column` is the template's time column: what the leaf partitions,
    /// orders and prunes on.
    fn open(&self, template_id: u64, time_column: &str) -> Result<Arc<dyn LeafStore>, StoreError>;

    /// Close every buffer a threshold says is due into row groups - every buffer, on a
    /// final flush, which then seals every open file. Synchronous, and called by whoever
    /// owns the clock. Nothing to do for a backend whose rows never leave memory.
    fn flush(&self, kind: FlushKind) -> Flushed {
        let _ = kind;
        Flushed::default()
    }

    /// For the banner and `/stats`.
    fn kind(&self) -> &'static str;
    fn base_path(&self) -> Option<&Path>;
    /// Data files open for writing, and the cap they are held under. A backend that holds
    /// no files has none.
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
    /// The lowest write-ahead log position among the rows still only in memory - in a
    /// buffer, or moved aside by a flush that has not published them. `None` when no such
    /// row carries a position: what the log's checkpoint is moved up to.
    fn oldest_position(&self) -> Option<u64> {
        None
    }
    /// The disk store, for the bulk door: only it has files to publish. `None` for a store
    /// whose rows never leave memory, which a load is refused against.
    fn as_disk(&self) -> Option<&DiskStore> {
        None
    }
    /// The disk store's scan threads: for the door to say a body is in its task, and
    /// for the server to keep them until its runtime is gone. `None` for a store that
    /// has none, which is every store but a server's disk store.
    fn fan(&self) -> Option<Arc<fan::Fan>> {
        None
    }

    /// How many times a load or a replace has published here, bumped in the same step as
    /// the publish: a query that sees it move between planning and its snapshot plans
    /// again, since a load's new templates may have joined the catalog after the plan was
    /// made. A store with no bulk door never publishes, and stays at 0.
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

/// Whether the last flush tick could write, and what stopped it if it could not.
///
/// **Read without the store lock, deliberately.** `/health` is what an orchestrator uses
/// to decide whether to kill the process, and a health check that queues behind a long
/// flush reports the wrong thing at exactly the moment it matters. So this is an atomic
/// and a short-lived mutex beside the registry rather than a field inside it.
///
/// **`/ingest` no longer refuses on it.** Before the write-ahead log a `200` for a line
/// the store could not write was a lie - measured on a full disk, every batch got a `200`,
/// the shutdown log said "90,000 rows written", and after a restart `COUNT LOG {}` was 0 -
/// so while this said degraded, `/ingest` answered `503`. A line is now durable in the log
/// before it is acknowledged, and a store that cannot write delays its row and loses
/// nothing: the refusal moved to the log. This is what `/health` and the compaction look
/// read, and what the door reads only to say why the buffers are full when it sheds.
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

    /// What one flush tick did to the verdict. A tick that wrote everything clears it;
    /// one that could not sets it. The error text is left alone on a clearing tick, so
    /// `/stats` still says what the last failure was after the disk comes back.
    fn tick(&self, failed: usize) {
        self.degraded
            .store(failed > 0, std::sync::atomic::Ordering::Relaxed);
    }
}

// ── The registry ───────────────────────────────────────────────────────────────────

/// Take a lock, recovering from a poisoning rather than propagating it.
///
/// A poisoned lock means something panicked while holding it. What these locks guard is
/// changed under them in steps that do not stop halfway - a push, a take, a swap - so there
/// is no torn state to inherit, and refusing every later request would turn one bad line
/// into a dead server. `PoisonError::into_inner`, never `.unwrap()`: nothing here can end the
/// process.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The open leaves, and the backend that makes them and writes their rows.
///
/// `BTreeMap`, so `open` order is template order and two runs over one file report the
/// same leaves in the same sequence.
///
/// **Every method takes `&self`**, and the server holds this with no lock around it. The
/// map has its own, held only to find a leaf or to add one - never across a leaf's rows,
/// and never across the disk. A leaf locks itself, and a flush takes no lock an append or a
/// query waits on while it writes.
pub struct StoreManager {
    backend: Box<dyn Store>,
    stores: RwLock<BTreeMap<u64, Arc<dyn LeafStore>>>,
    write_failures: AtomicU64,
    /// Rows `append` refused. Apart from `write_failures`, which also counts flushes that
    /// failed and later succeeded: these rows are in no buffer. Behind the write-ahead log
    /// they are still in it, and the next start replays them; with no log they are gone.
    rows_dropped: AtomicU64,
    /// Buffers closed early by the memory limit, over the life of the process.
    forced_closes: AtomicU64,
    /// Leaves `append` could not open, by template id, with the time column to open them
    /// on. Retried every flush, and while any is still here the store is degraded: see
    /// `retry_unopened`.
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

    /// Template ids stored rows already name. See `Store::claimed`.
    pub fn claimed_template_ids(&self) -> Vec<u64> {
        self.backend.claimed()
    }

    /// The disk store underneath, if there is one: what a bulk load commits through.
    pub fn disk_store(&self) -> Option<&DiskStore> {
        self.backend.as_disk()
    }

    /// The scan threads of the store underneath, if it has any. See `Store::fan`.
    pub fn fan(&self) -> Option<Arc<fan::Fan>> {
        self.backend.fan()
    }

    /// Open the store on first write, then buffer the row.
    ///
    /// **A failure to open is the row's, and it is returned.** The row is not buffered
    /// anywhere. A caller with no write-ahead log behind it - the CLI, a load - refuses the
    /// line by number: the row is then nowhere, and counting it as ingested would be a
    /// lie. The live door's applier does the opposite, and has to (`append_at`).
    ///
    /// Counted as well as returned - `write_failures` for `/stats`, `rows_dropped` for
    /// whoever asks - and it marks the store degraded.
    pub fn append(&self, template_id: u64, time_column: &str, row: Row) -> Result<(), StoreError> {
        self.append_at(template_id, time_column, row, 0)
    }

    /// `append`, for a row that came through the write-ahead log at `position`; 0 is a
    /// row that came no such way.
    ///
    /// **An `Err` here is not the sender's.** The line is durable in the log and was, or
    /// will be, acknowledged: the caller logs the failure and keeps the log's checkpoint
    /// behind the position, so the next start's replay stores the row. It must never
    /// become an error for the line - the type the live door answers with has no variant
    /// that can hold one (`error::IngestError`).
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

    /// The first row of a template this process has not opened a leaf for.
    ///
    /// **Double-checked**: the read in `append` missed, but another body carrying the same
    /// new template can have opened the leaf since, so the write lock looks again before the
    /// backend is asked. Two opens would give one template two leaves, and its rows would
    /// split between them - each query reading one of the two.
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

    /// The batch entry point: the ingest path calls this and owns neither buffer nor clock.
    /// Stops at the first row the store refuses; the rows after it are not appended.
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

    /// Open a leaf the trie already knows about, without writing to it.
    ///
    /// The restart path. A template with no stored rows opens empty, which is a known
    /// template holding no rows - the second of the two empty answers, not an error.
    pub fn open_existing(&self, template_id: u64, time_column: &str) -> Result<(), StoreError> {
        let mut stores = self.stores.write().unwrap_or_else(|e| e.into_inner());
        if stores.contains_key(&template_id) {
            return Ok(());
        }
        let store = self.backend.open(template_id, time_column)?;
        stores.insert(template_id, store);
        Ok(())
    }

    /// Count a failure and record what it was.
    ///
    /// **Reported once, recorded every time.** A disk that has gone away fails on every
    /// line, and a message per line is how a log becomes unreadable at the moment it
    /// matters - so the `error!` is the first one only. The *stored* string is the latest,
    /// because it is what `/health` shows, and what its reader wants is why now.
    ///
    /// **It also marks the store degraded**, and not only the flush tick does.
    ///
    /// The rule is written around the tick because that is where a full disk shows up
    /// first, but a leaf that cannot even be *opened* is a store that cannot write by any
    /// reading.
    ///
    /// The clearing rule is still the tick's: the next pass that writes everything clears
    /// it, and a pass has not written everything while a leaf `append` could not open
    /// still will not open - `retry_unopened` is in the pass. So the verdict lasts exactly
    /// as long as the leaf will not open: until the first tick after a disk fault is
    /// fixed, and for bytes the format rejects, until an operator moves them.
    fn note_failure(&self, e: &StoreError) {
        if self.write_failures.fetch_add(1, Ordering::Relaxed) == 0 {
            crate::error::report(e);
        }
        self.health.set_error(crate::error::chain(e));
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

    /// Close every buffer that is due - every one on a final flush, which then seals every
    /// open file - and try again to open every leaf `append` could not.
    ///
    /// Never returns `Err`: one leaf failing is collected into the summary and the rest
    /// are still written. One at a time: the backend's own lock makes a second caller wait
    /// for the first, and nothing else - no append, no query - waits on either.
    pub fn flush_all(&self, kind: FlushKind) -> FlushSummary {
        let started = Instant::now();
        let done = self.backend.flush(kind);
        let failed: BTreeSet<u64> = done.failed.iter().map(|(id, _)| *id).collect();
        let mut failures: Vec<StoreError> = done.failed.into_iter().map(|(_, e)| e).collect();
        // Counted apart from `failed`, which is leaves whose rows could not be written and is
        // what the shutdown reports rows against. A seal that failed has lost no row - the
        // file recovers on the next start - but it is still a store that could not write, so
        // it reaches the verdict below.
        let seal_failures = done.seal_failed.len();
        failures.extend(done.seal_failed);
        let unopened = self.retry_unopened();
        for e in &failures {
            crate::error::report(e);
        }
        if let Some(e) = failures.first() {
            self.write_failures
                .fetch_add(failures.len() as u64, Ordering::Relaxed);
            self.health.set_error(crate::error::chain(e));
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

    /// Try again to open every leaf `append` could not, and count the ones that still fail.
    ///
    /// **Why the flush has to ask.** A leaf that will not open holds no buffer, so a pass
    /// over the open stores never meets it and would end by clearing `degraded` while the
    /// store still cannot take that template's rows. Kept degraded instead, `/health` says
    /// so and names the error for as long as the leaf stays shut.
    ///
    /// Counted apart from `summary.failed` for the reason seal failures are: that is leaves
    /// whose buffered rows could not be written, and these hold none. Their rows were
    /// refused by `append` and are in `rows_dropped`.
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

    /// Rows held across every open store.
    ///
    /// Summed rather than counted on the way in: an append counter reads 0 after a
    /// restart, while the rows it counted are sitting on disk.
    pub fn rows(&self) -> u64 {
        self.leaves().iter().map(|(_, s)| s.len()).sum()
    }

    pub fn buffered(&self) -> u64 {
        self.leaves().iter().map(|(_, s)| s.buffered()).sum()
    }

    /// Every open store's `approx_bytes`, summed. `/stats`'s storage size.
    pub fn approx_bytes(&self) -> u64 {
        self.leaves().iter().map(|(_, s)| s.approx_bytes()).sum()
    }

    /// Memory held by buffered rows, and the limit past which the largest are closed early.
    pub fn buffer_bytes(&self) -> (u64, u64) {
        self.backend.buffer_bytes()
    }

    /// Whether the buffers are past their limit, so the flush should not wait for its
    /// next tick. Read on the ingest path: an atomic, and no lock.
    pub fn over_buffer_limit(&self) -> bool {
        let (held, limit) = self.backend.buffer_bytes();
        limit > 0 && held > limit
    }

    /// Whether the buffers are past twice their limit: the flush, closing the largest
    /// early, is still falling behind what arrives, and the door should take nothing more
    /// until it catches up. See `DEFAULT_MAX_BUFFER_BYTES`.
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

    /// Rows `append` refused over the life of this manager. Never cleared.
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

/// A value as an instant, or `None` for a value that is not one.
///
/// A `Datetime` is one. So is *text* the ladder reads as one: a partition that stores its
/// time column as text after a second date form arrived hands the value back as
/// the string it was ingested as, and the row's place in time is still what that string
/// says. Anything else - null, `"N/A"`, a number - is not an instant.
///
/// Apart from `ts_of` because a value is not always in a row: a scan reads a column's
/// instants off its decoded chunk, and must read each as a row holding it would.
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

/// **The effective time of a row**: the template's time column where it holds an
/// instant, and otherwise the arrival time the row was stored with.
///
/// **One rule, and it is read in two ways.** Of a row it is this function: the result
/// order reads it, `GROUP BY time(...)` reads it, the disk store partitions by it, and the
/// time window reads it of a row held in memory. The disk scan reads the same rule where
/// it has no row to hand this function, as `instant_of` of the time column's value and
/// then of the arrival time's: in `disk::scan::Unit::times`, for the window over a stored
/// row group and for the time a row is asked about before it is built; in
/// `Unit::seen_ts`, for that time where a time column is now text; and in
/// `Ask::seen_ts_of`, for it of a row not yet in a row group, which is not rendered to be
/// asked about. `scan::slow_pick`, which a debug build checks each window with, spells it
/// a fourth time. **Change the rule here and change it there.**
///
/// Two readings that disagree by one type would put a row in a dated directory while
/// ranking it as undated, and the partition holding it would then be skipped by a query
/// that should have found it; and a row asked about with another time than it shows
/// would not be built. Both only under a limit, only once the heap fills, so an unlimited
/// query and a small fixture would both pass. What holds the readings to this function
/// is the debug assertions in `scan::hand` and `scan::tail`, and the tests
/// `a_column_s_instants_are_what_each_row_s_cell_reads_as`,
/// `the_scan_answers_what_the_plain_scan_answers_whatever_is_read` and
/// `a_row_is_asked_about_with_the_time_it_shows_and_in_the_day_it_is_stored_under`. A
/// release build has only the tests.
///
/// `None` only for a row from before the arrival-time column: one with neither an instant
/// in its time column nor a system column, which is a row in an `undated` partition.
pub fn effective_ts(row: &Row, column: &str) -> Option<i64> {
    ts_of(row, column).or_else(|| ts_of(row, RECEIVED_AT_COLUMN))
}

/// Whether an instant falls in `[since, until)`. A row with no effective time is outside
/// every window - the one case left is a row from before the arrival-time column, in
/// `undated`.
pub fn in_window(ts: Option<i64>, since: Option<i64>, until: Option<i64>) -> bool {
    let Some(ns) = ts else {
        return false;
    };
    since.is_none_or(|from| ns >= from) && until.is_none_or(|to| ns < to)
}

/// A row as a query shows it, as `(name shown, key in the row)`: the system column under
/// its query name, unless the row has an ingested key of that name, in which case the
/// ingested one is shown and the system value is left out. A null system value - a column
/// the row never had - is left out too.
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
/// against: the JSON the line said, which for a date the ladder read is its own text or
/// its digits. Every other value is returned as it is.
pub fn render_text(value: &ColValue, written: Option<&serde_json::Value>) -> Option<ColValue> {
    let ColValue::Datetime(ns) = value else {
        return None;
    };
    Some(ColValue::String(match written {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => crate::fmt::rfc3339(*ns),
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

/// Unix nanoseconds, saturating at both ends. i64 nanos run 1677-2262; a bound outside
/// that is clamped to the near end, never wrapped past the far one.
pub fn nanos_of(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos()).map_or(i64::MIN, |n| -n),
    }
}

/// One value's payload width. A string costs its bytes; everything else is the scalar
/// the column store writes as a fixed-width cell.
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

    /// A row whose leaf will not open is refused to the caller and counted where the exit
    /// code reads it, and the store stays degraded - through any number of ticks - until
    /// the leaf opens. A tick that cleared it sooner would let a retrying shipper's next
    /// body in, to store its other lines a second time.
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

    /// Four threads snapshot one template while it takes rows and closes row groups the
    /// whole time. Every snapshot must hold every row appended before it began, each once
    /// and in the place it was given: the invariant at `disk::DiskLeaf::state`, under load.
    /// A row seen twice is a publish and a drop split across two holds of the leaf's lock;
    /// a row missing is the drop done first.
    ///
    /// Each snapshot reads the newest 2,048 rows through a time window, so it costs the
    /// same at the end of the run as at the start and the four threads take thousands; the
    /// whole leaf is read once at the end, and once more after a restart.
    ///
    /// `KIBBR_SNAPSHOT_SECONDS` sets how long it runs; 10 by default.
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

    /// A `Datetime` under each kind of spelling `render_text` tells apart - none, a string
    /// that is not its canonical print, a string that is, a JSON number, a string of digits
    /// - and a value of every other type with a spelling kept and with none.
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
                spelling(&crate::fmt::rfc3339(INSTANT)),
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

    /// A term put to a stored row answers what it answers for that row rendered, and so
    /// does a term put to the row's one value and what was written beside it: over every
    /// value the pool ingests and every spelling above, every term of the pool, in a column
    /// that is still its own type and in one that is now text.
    ///
    /// Rendering clones the row to change one value, which is the cost the two unrendered
    /// readings are there to avoid; this is what lets a scan use them in its place.
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

        // And the case the two readings exist for, by hand. In a `Datetime` column every
        // spelling of the instant finds the row; once the column is text, only the
        // spelling it arrived in does.
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
