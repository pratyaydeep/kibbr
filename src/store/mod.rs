//! Leaf storage: the scan interface, the two implementations behind it, and the registry.
//!
//! **The traits appear here and not earlier on purpose.** An interface is a compression of
//! what its callers need, and with one implementation it is a guess that gets baked into
//! tests which then resist the first real requirement. Memory and disk both exist now, so
//! `LeafStore` and `Store` are a description rather than a prediction.
//!
//! The split is three ways and it matters:
//!
//! - **`LeafStore`** holds one leaf's rows and a synchronous `flush()`. It has **no
//!   clock**. It cannot know when it should be written, and giving it a timer would make
//!   it untestable without a runtime.
//! - **`StoreManager`** owns the registry of open stores. The interval that calls
//!   `flush()` lives in `serve`, watching the same cancellation token as everything else,
//!   because the clock belongs to the process lifecycle and not to storage.
//! - **The ingest path** has neither. It calls `append` and returns.
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
// `ScanOptions::columns` is no longer among them. It was unreachable for a stated reason -
// a `Predicate` is an opaque `Box<dyn Fn(&Row)>`, so nothing could know which columns one
// reads - and the way past it needed no interface change: the engine builds every
// predicate from a `Match` that still has its key. See `engine::execute`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::infer::infer;
use crate::types::ColValue;

pub mod disk;
pub mod manifest;
pub mod memory;
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
}

/// Passed beside `ScanOptions` rather than inside it: `Box<dyn Fn>` is not `Clone`, which
/// would cost `ScanOptions` its `Default`.
pub type Predicate = Box<dyn Fn(&Row) -> bool + Send + Sync>;

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
    /// Rows examined, including those the time window and the predicates rejected.
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
/// keep open; past it the least recently written day's file is sealed, and the next row
/// for that day opens the next file number.
pub const DEFAULT_MAX_OPEN_SEGMENTS: usize = 512;

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
    /// The interval. Closes a row group only where a threshold says so.
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
/// row group header. The timeout is for low-traffic leaves that would never reach one -
/// and until the write-ahead log lands, it is also the longest a row can be queryable
/// without being on disk.
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
            age: std::time::Duration::from_secs(60),
        }
    }
}

/// What one `flush()` wrote.
#[derive(Debug, Default, Clone, Copy)]
pub struct Flushed {
    pub rows: u64,
    pub partitions: usize,
    pub bytes: u64,
}

/// What one pass over every open store wrote, and what it could not.
#[derive(Debug, Default, Clone, Copy)]
pub struct FlushSummary {
    pub rows: u64,
    pub partitions: usize,
    pub bytes: u64,
    /// Leaves whose flush failed. One store failing is logged, never fatal to the others:
    /// a single unwritable directory must not stop every other leaf being persisted.
    pub failed: usize,
}

// ── The scan interface ─────────────────────────────────────────────────────────────

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
/// `Send + Sync` because the server shares the registry across handler threads; `visit`
/// takes `&self` so a query and an append can be told apart by the lock rather than by
/// convention.
pub trait LeafStore: Send + Sync {
    fn template_id(&self) -> u64;

    /// Buffer one row, and return the ordinal it was given.
    ///
    /// No I/O and no clock: this half has to stay synchronous and cheap, because it runs
    /// once per ingested line.
    fn append(&mut self, row: Row) -> u64;

    /// Write the buffer out. Synchronous, and called by whoever owns the clock.
    fn flush(&mut self, kind: FlushKind) -> Result<Flushed, StoreError>;

    /// Hand every surviving row to `v`. The one place filtering happens, so the count,
    /// fetch and group-by paths cannot drift.
    fn visit(
        &self,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError>;

    /// Rows held, flushed and buffered together - what a query would see.
    fn len(&self) -> u64;

    /// Rows not yet written. `/stats` reports it because it is exactly what a `kill -9`
    /// would cost while there is no write-ahead log.
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
    fn scan(&self, opts: &ScanOptions, preds: &[Predicate]) -> Result<Scan, StoreError> {
        let mut rows = Vec::new();
        let mut scan = {
            let mut sink = Rows(|row: &Row, _: u64| rows.push(project(row, &opts.columns)));
            self.visit(opts, preds, &mut sink)?
        };
        scan.rows = rows;
        Ok(scan)
    }
}

/// Where leaves come from. A factory and nothing else - the registry lives on
/// `StoreManager`, so this trait is the whole of what memory and disk actually differ by.
pub trait Store: Send + Sync {
    /// `time_column` is the template's time column: what the leaf partitions,
    /// orders and prunes on.
    fn open(&self, template_id: u64, time_column: &str) -> Result<Box<dyn LeafStore>, StoreError>;
    /// For the banner and `/stats`.
    fn kind(&self) -> &'static str;
    fn base_path(&self) -> Option<&Path>;
    /// Data files open for writing, and the cap they are held under. A backend that holds
    /// no files has none.
    fn open_files(&self) -> (usize, usize) {
        (0, 0)
    }
    /// Seal every open file: the last step of a final flush, once every leaf has closed
    /// its buffers. Nothing to do for a backend that holds no files.
    fn seal(&self) -> Result<(), StoreError> {
        Ok(())
    }
    /// Template ids that stored rows already name, whether or not a template knows them:
    /// what a restart must not hand out again.
    fn claimed(&self) -> Vec<u64> {
        Vec::new()
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
/// Until the write-ahead log lands, a `200` for a line the store cannot write is the
/// lie: measured on a full disk, every batch got a `200`, the shutdown log said
/// "90,000 rows written", and after a restart `COUNT LOG {}` was 0. While this says
/// degraded, `/ingest` answers `503` instead.
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

/// The open stores, and the backend that makes them.
///
/// `BTreeMap`, so `open` order is template order and two runs over one file report the
/// same leaves in the same sequence.
///
/// **No double-checked locking inside here, deliberately.** The obvious shape is a read
/// lock, a miss, a write lock and a re-check - but the server already takes the
/// `RwLock<StoreManager>` write lock once per ingest body, so `entry().or_insert_with()`
/// under that lock *is* the check-and-create, atomically. A second lock in here would
/// guard against a race the caller has already excluded.
pub struct StoreManager {
    backend: Box<dyn Store>,
    stores: BTreeMap<u64, Box<dyn LeafStore>>,
    write_failures: u64,
    /// Rows `append` refused. Apart from `write_failures`, which also counts flushes that
    /// failed and later succeeded: these rows are gone for good, and the shutdown exits
    /// non-zero on them.
    rows_dropped: u64,
    /// Leaves `append` could not open, by template id, with the time column to open them
    /// on. Retried every flush, and while any is still here the store is degraded: see
    /// `retry_unopened`.
    unopened: BTreeMap<u64, String>,
    health: std::sync::Arc<WriteHealth>,
}

impl StoreManager {
    pub fn new(backend: Box<dyn Store>) -> Self {
        Self {
            backend,
            stores: BTreeMap::new(),
            write_failures: 0,
            rows_dropped: 0,
            unopened: BTreeMap::new(),
            health: std::sync::Arc::new(WriteHealth::default()),
        }
    }

    pub fn memory() -> Self {
        Self::new(Box::new(MemoryStore::new()))
    }

    pub fn disk(
        base: std::path::PathBuf,
        limits: RowGroupLimits,
        max_open_files: usize,
        segment_max_bytes: u64,
    ) -> Result<Self, StoreError> {
        Ok(Self::new(Box::new(DiskStore::open(
            base,
            limits,
            max_open_files,
            segment_max_bytes,
        )?)))
    }

    /// Shared out at startup and read from `/health` and `/ingest` without this lock.
    pub fn health(&self) -> std::sync::Arc<WriteHealth> {
        std::sync::Arc::clone(&self.health)
    }

    /// Data files open for writing, and the cap they are held under.
    pub fn open_files(&self) -> (usize, usize) {
        self.backend.open_files()
    }

    /// Template ids stored rows already name. See `Store::claimed`.
    pub fn claimed_template_ids(&self) -> Vec<u64> {
        self.backend.claimed()
    }

    /// Open the store on first write, then buffer the row.
    ///
    /// **A failure to open is the row's, and it is returned.** The row is not buffered
    /// anywhere, and with no write-ahead log that makes it nowhere: this used to count the
    /// failure and return nothing, and `/ingest` answered `200` with the line counted in
    /// `ingested`. The caller refuses the line by number instead. The body is not failed:
    /// the other templates' rows in it did land, and in this body the leaf costs its own
    /// lines and no one else's. The bodies after it are refused whole at the door until
    /// the leaf opens; see `retry_unopened`.
    ///
    /// Counted as well as returned - `write_failures` for `/stats`, `rows_dropped` for the
    /// exit code - and it marks the store degraded.
    pub fn append(
        &mut self,
        template_id: u64,
        time_column: &str,
        row: Row,
    ) -> Result<(), StoreError> {
        if !self.stores.contains_key(&template_id) {
            match self.backend.open(template_id, time_column) {
                Ok(store) => {
                    self.stores.insert(template_id, store);
                    self.unopened.remove(&template_id);
                }
                Err(e) => {
                    self.note_failure(&e);
                    self.rows_dropped += 1;
                    self.unopened.insert(template_id, time_column.to_string());
                    return Err(e);
                }
            }
        }
        if let Some(store) = self.stores.get_mut(&template_id) {
            store.append(row);
        }
        Ok(())
    }

    /// The batch entry point: the ingest path calls this and owns neither buffer nor clock.
    /// Stops at the first row the store refuses; the rows after it are not appended.
    pub fn append_batch_to(
        &mut self,
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
    /// The restart path. A leaf whose directory is absent opens empty, which is a known
    /// template holding no rows - the second of the two empty answers, not an error.
    pub fn open_existing(&mut self, template_id: u64, time_column: &str) -> Result<(), StoreError> {
        if self.stores.contains_key(&template_id) {
            return Ok(());
        }
        let store = self.backend.open(template_id, time_column)?;
        self.stores.insert(template_id, store);
        Ok(())
    }

    /// Count a failure and record what it was.
    ///
    /// **Reported once, recorded every time.** A disk that has gone away fails on every
    /// line, and a message per line is how a log becomes unreadable at the moment it
    /// matters - so the `error!` is the first one only. The *stored* string is the latest,
    /// because it is what a caller being refused with a 503 is shown, and what that caller
    /// wants is why now.
    ///
    /// **It also marks the store degraded**, and not only the flush tick does.
    ///
    /// The rule is written around the tick because that is where a full disk shows up
    /// first, but a leaf that cannot even be *opened* - a directory `create_dir_all` could
    /// not make - is a store that cannot write by any reading. `append` refuses that row
    /// to its line; this is what refuses the *next* body at the door, before it is read.
    ///
    /// The clearing rule is still the tick's: the next pass that writes everything clears
    /// it, and a pass has not written everything while a leaf `append` could not open
    /// still will not open - `retry_unopened` is in the pass. So the refusal lasts exactly
    /// as long as the leaf will not open: until the first tick after a disk fault is
    /// fixed, and for bytes the format rejects, until an operator moves them.
    fn note_failure(&mut self, e: &StoreError) {
        if self.write_failures == 0 {
            crate::error::report(e);
        }
        self.write_failures += 1;
        self.health.set_error(crate::error::chain(e));
        self.health.degrade();
    }

    pub fn get(&self, template_id: u64) -> Result<&dyn LeafStore, StoreError> {
        self.lookup(template_id)
            .ok_or(StoreError::NotOpen { template_id })
    }

    /// A leaf with no store is a template that exists and holds no rows - the second of
    /// the two empty answers, and not an error.
    pub fn lookup(&self, template_id: u64) -> Option<&dyn LeafStore> {
        self.stores.get(&template_id).map(AsRef::as_ref)
    }

    /// Write every open store's buffer; on a final flush, then seal every open file.
    ///
    /// Never returns `Err`: one leaf failing is collected into the summary and the rest
    /// are still written.
    pub fn flush_all(&mut self, kind: FlushKind) -> FlushSummary {
        let mut summary = FlushSummary::default();
        let mut failures = Vec::new();
        for (id, store) in self.stores.iter_mut() {
            match store.flush(kind) {
                Ok(done) => {
                    summary.rows += done.rows;
                    summary.partitions += done.partitions;
                    summary.bytes += done.bytes;
                }
                Err(e) => {
                    summary.failed += 1;
                    failures.push((*id, e));
                }
            }
        }
        // Counted apart from `summary.failed`, which is leaves whose rows could not be
        // written and is what the shutdown reports rows against. A seal that failed has
        // lost no row - the file recovers on the next start - but it is still a store
        // that could not write, so it reaches the verdict below.
        let mut seal_failures = 0usize;
        if kind.is_final() {
            if let Err(e) = self.backend.seal() {
                seal_failures += 1;
                failures.push((0, e));
            }
        }
        let unopened = self.retry_unopened();
        for (_, e) in &failures {
            crate::error::report(e);
        }
        if let Some((_, e)) = failures.first() {
            self.write_failures += failures.len() as u64;
            self.health.set_error(crate::error::chain(e));
        }
        // The verdict `/health` and `/ingest` read. A final flush sets it too: a shutdown
        // that could not write is degraded for whatever is left of the process.
        self.health.tick(summary.failed + seal_failures + unopened);
        summary
    }

    /// Try again to open every leaf `append` could not, and count the ones that still fail.
    ///
    /// **Why the flush has to ask.** A leaf that will not open holds no buffer, so a pass
    /// over the open stores never meets it and would end by clearing `degraded` - and the
    /// next body through the door would store its other lines again and be refused again
    /// for this leaf's. A shipper retrying that `503` duplicates every good line of its
    /// batch once per retry, for as long as the leaf stays shut. Kept degraded instead, the
    /// retry is refused at the door until the leaf opens. The bodies already past the door
    /// when the fault was found are each partly stored once; none after them is.
    ///
    /// **What it costs: every template's ingest, not only this one's.** The door cannot
    /// tell which bodies need the leaf without parsing them, so while it will not open no
    /// body is taken, and a leaf whose bytes the format rejects never opens by itself.
    /// That is the stance a full disk already gets, and the alternative is the unbounded
    /// duplication above. `/health` says degraded and names the error meanwhile. The retry
    /// is a full `DiskLeaf::open` under the store lock each tick: nothing for a directory
    /// that will not list, a footer read per segment before the bad one for a corrupt leaf.
    ///
    /// Counted apart from `summary.failed` for the reason seal failures are: that is leaves
    /// whose buffered rows could not be written, which the shutdown reports as lost, and
    /// these hold none. Their rows were refused by line and are in `rows_dropped`.
    fn retry_unopened(&mut self) -> usize {
        let pending: Vec<(u64, String)> = std::mem::take(&mut self.unopened).into_iter().collect();
        let mut failed = 0;
        for (template_id, time_column) in pending {
            if self.stores.contains_key(&template_id) {
                continue;
            }
            match self.backend.open(template_id, &time_column) {
                Ok(store) => {
                    self.stores.insert(template_id, store);
                }
                Err(e) => {
                    failed += 1;
                    self.note_failure(&e);
                    self.unopened.insert(template_id, time_column);
                }
            }
        }
        failed
    }

    pub fn open(&self) -> usize {
        self.stores.len()
    }

    /// Rows held across every open store.
    ///
    /// Summed rather than counted on the way in: an append counter reads 0 after a
    /// restart, while the rows it counted are sitting on disk.
    pub fn rows(&self) -> u64 {
        self.stores.values().map(|s| s.len()).sum()
    }

    pub fn buffered(&self) -> u64 {
        self.stores.values().map(|s| s.buffered()).sum()
    }

    /// Every open store's `approx_bytes`, summed. `/stats`'s storage size.
    pub fn approx_bytes(&self) -> u64 {
        self.stores.values().map(|s| s.approx_bytes()).sum()
    }

    /// Open stores in template order, for `/templates` and `/stats`.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &dyn LeafStore)> {
        self.stores.iter().map(|(id, store)| (*id, store.as_ref()))
    }

    pub fn kind(&self) -> &'static str {
        self.backend.kind()
    }

    pub fn base_path(&self) -> Option<&Path> {
        self.backend.base_path()
    }

    pub fn write_failures(&self) -> u64 {
        self.write_failures
    }

    /// Rows `append` refused over the life of this manager. Never cleared.
    pub fn rows_dropped(&self) -> u64 {
        self.rows_dropped
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

/// A column's value as an instant, or `None` for a value that is not one.
///
/// A `Datetime` is one. So is *text* the ladder reads as one: a partition that stores its
/// time column as text after a second date form arrived hands the value back as
/// the string it was ingested as, and the row's place in time is still what that string
/// says. Anything else - absent, null, `"N/A"` - is not an instant.
pub fn ts_of(row: &Row, column: &str) -> Option<i64> {
    match row.get(column) {
        Some(ColValue::Datetime(ns)) => Some(*ns),
        Some(ColValue::String(text)) => match infer(text).value {
            ColValue::Datetime(ns) => Some(ns),
            _ => None,
        },
        _ => None,
    }
}

/// **The effective time of a row**: the template's time column where it holds an
/// instant, and otherwise the arrival time the row was stored with.
///
/// **One function, four callers**, and that is the point: the time window reads it, the
/// result order reads it, `GROUP BY time(...)` reads it and the disk store partitions by
/// it. Two readings that disagree by one type would put a row in a dated directory while
/// ranking it as undated, and the partition holding it would then be skipped by a query
/// that should have found it - only under a limit, only once the heap fills, so an
/// unlimited query and a small fixture would both pass.
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
        fn open(&self, template_id: u64, t: &str) -> Result<Box<dyn LeafStore>, StoreError> {
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
        let mut stores = StoreManager::new(Box::new(Shut(std::sync::Arc::clone(&shut))));
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
        let mut stores = StoreManager::memory();
        stores
            .append(7, DEFAULT_TS_COLUMN, Row::new())
            .expect("the memory store always opens");
        assert_eq!(stores.rows_dropped(), 0);
        assert_eq!(stores.rows(), 1);
    }
}
