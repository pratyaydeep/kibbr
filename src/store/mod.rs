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
//! the milestone against the rule above: the registry, the lazy open and the flush loop
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

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::infer::infer;
use crate::types::{ColValue, DtForm};

pub mod disk;
pub mod memory;
pub mod segment;

pub use disk::DiskStore;
pub use memory::MemoryStore;

/// Absence on a line is `ColValue::Null`, not a missing entry - the row is built from the
/// leaf's key set, and the key set is the path.
pub type Row = std::collections::HashMap<String, ColValue>;

/// Passed beside `ScanOptions` rather than inside it: `Box<dyn Fn>` is not `Clone`, which
/// would cost `ScanOptions` its `Default`.
pub type Predicate = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// The `dt_form` evidence one row carries, per Datetime column. Beside the `Row` rather
/// than inside it, because `ColValue` is what a query compares against and a rung tag in
/// there would change what `ts=1705314600` matches. See `types::DtForm`.
pub type DtForms = [(String, DtForm)];

/// The column a time filter reads when none is named. A fallback for a `_leaf.json` that
/// predates the time source; every caller from M5.6b on names the column.
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
    /// is rendered back to the text it arrived as, through the chunk's own `dt_form`
    /// before anything reads the row.
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
    #[error(transparent)]
    Segment(#[from] segment::SegError),
    /// A row group with no segment to go in. Not reachable: the write path opens one
    /// where there is none, and nothing between the two can close it. Here because it was
    /// a panic on an unreachable branch, and an assumption that turns out to be wrong
    /// should cost one flush rather than the process.
    #[error("leaf {template_id}: partition {partition} has rows to write and no open segment")]
    NoOpenSegment { template_id: u64, partition: String },
    /// One file per (template, day, column), which this build does not read. There is no
    /// upgrade path and no migration: the path is named and the process exits non-zero,
    /// rather than reading bytes it would have to guess the meaning of.
    #[error(
        "{path}: this data directory was written by the per-column layout, which this \
         build cannot read. There is no migration - point --data-dir somewhere else, or \
         remove it and re-ingest."
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

// ── The cap on open segment files ──────────────────────────────────────────────────

/// How many segment files may be open for writing at once, across every leaf.
///
/// A segment stays open after its row group closes, so on the as-built store the count is
/// "partitions that have taken a row and not been sealed" - one per (template, day), with
/// nothing bounding it. 20,000 generated lines are 823 templates over 31 days, which is
/// about 20,000 of them; the operating system's descriptor limit arrives long before
/// that, and what arrives with it is a flush that fails, a half-created file that makes
/// every retry fail with `File exists`, and rows that never leave memory.
pub const DEFAULT_MAX_OPEN_SEGMENTS: usize = 512;

/// One open segment file: the leaf, and the partition inside it.
///
/// `day` is `Some(days since the epoch)` or `None` for the undated partition - the two
/// shapes the disk store has. `Copy`, because the most recently written segment moves to
/// the back of the order once per row group and an allocation there would buy nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentKey {
    pub template_id: u64,
    pub day: Option<i64>,
}

/// The open segments, in least recently written order, and the cap on how many there may
/// be.
///
/// **It counts and orders; it does not seal.** Sealing needs `&mut` on the leaf that owns
/// the segment, and a registry that held the writers would have to hand the sealed index
/// back to that leaf for a query to find its rows. So the registry answers "which leaf
/// holds the oldest one" and the two callers that can act on that do:
///
/// - a leaf about to open a segment seals **its own** oldest first, which is always
///   possible once it has one and is what keeps a single leaf's flush from overshooting
///   while it works through thirty-one days;
/// - `StoreManager::flush_all` reaps between leaves, where it has `&mut` on all of them.
///
/// A `Vec` and a linear scan rather than a heap: it holds at most `max` entries and is
/// touched once per row group, not once per row.
pub struct OpenSegments {
    max: usize,
    order: std::sync::Mutex<Vec<SegmentKey>>,
}

impl OpenSegments {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            order: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn max(&self) -> usize {
        self.max
    }

    /// A poisoned lock is recovered from rather than propagated, as every other lock in
    /// this crate is: the contents are an ordering hint, and there is no torn state in a
    /// `Vec` of `Copy` keys to inherit.
    fn order(&self) -> std::sync::MutexGuard<'_, Vec<SegmentKey>> {
        self.order.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn len(&self) -> usize {
        self.order().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Would one more pass the cap?
    pub fn full(&self) -> bool {
        self.len() >= self.max
    }

    /// Record a write to this segment - the first one, which opened it, or a later one.
    /// Either way it becomes the most recently written.
    pub fn wrote(&self, key: SegmentKey) {
        let mut order = self.order();
        order.retain(|held| *held != key);
        order.push(key);
    }

    pub fn closed(&self, key: SegmentKey) {
        self.order().retain(|held| *held != key);
    }

    /// The least recently written open segment.
    pub fn oldest(&self) -> Option<SegmentKey> {
        self.order().first().copied()
    }

    /// The least recently written open segment belonging to one leaf.
    ///
    /// Which is also the global victim whenever the global victim is that leaf's: the
    /// leaf's own order is a subsequence of this one.
    pub fn oldest_of(&self, template_id: u64) -> Option<SegmentKey> {
        self.order()
            .iter()
            .find(|key| key.template_id == template_id)
            .copied()
    }
}

/// What a flush is being asked to do.
///
/// **Closing a row group and sealing a segment are two different events**: this is the
/// difference between "close whatever a threshold fired on" and "close everything, seal".
///
/// Not a durability switch, and that changed with the format. The per-column layout could
/// not afford an fsync per tick - one per column file is tens of milliseconds, and a
/// half-second tick over a few hundred templates spent minutes in the kernel and never
/// caught up. A row group is one fsync for up to 8,192 rows of every column at once, so
/// closing always fsyncs and the only question left is whether it is time to close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushKind {
    /// The interval. Closes a row group only where a threshold says so.
    Tick,
    /// Graceful shutdown. Closes every buffer and seals every open segment.
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
    fn append(&mut self, row: Row, forms: &DtForms) -> u64;

    /// Write the buffer out. Synchronous, and called by whoever owns the clock.
    fn flush(&mut self, kind: FlushKind) -> Result<Flushed, StoreError>;

    /// Seal this leaf's least recently written open segment, and say whether there was
    /// one. What `StoreManager` calls to get back under `OpenSegments::max`.
    ///
    /// **Always safe**, which is what makes the cap a cap rather than a refusal: the next
    /// row for that partition opens the next segment number, and a sealed file is a
    /// complete file. The cost is one more file, paid where the alternative is a
    /// descriptor the process does not have.
    ///
    /// Defaulted to "there was none" for a store that holds no files. That is not a
    /// guess about a second implementation - the memory store is the second
    /// implementation, and it has no segments to seal.
    fn seal_lru(&mut self) -> Result<bool, StoreError> {
        Ok(false)
    }

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
    /// The registry every leaf this backend makes shares. A backend that holds no files
    /// hands back an empty one rather than an `Option`, so the manager has one thing to
    /// read rather than two cases.
    fn open_segments(&self) -> std::sync::Arc<OpenSegments> {
        std::sync::Arc::new(OpenSegments::new(0))
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

    /// A write failed outside a flush tick - a leaf that could not be opened, a reap that
    /// could not seal. Set here and cleared only by a tick, so the clearing rule stays in
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
    /// The open segment files, shared with every leaf the backend makes. The manager is
    /// the only thing that can seal one leaf's segment to make room for another's.
    open_segments: std::sync::Arc<OpenSegments>,
    health: std::sync::Arc<WriteHealth>,
}

impl StoreManager {
    pub fn new(backend: Box<dyn Store>) -> Self {
        let open_segments = backend.open_segments();
        Self {
            backend,
            stores: BTreeMap::new(),
            write_failures: 0,
            open_segments,
            health: std::sync::Arc::new(WriteHealth::default()),
        }
    }

    pub fn memory() -> Self {
        Self::new(Box::new(MemoryStore::new()))
    }

    pub fn disk(
        base: std::path::PathBuf,
        limits: RowGroupLimits,
        max_open_segments: usize,
    ) -> Result<Self, StoreError> {
        Ok(Self::new(Box::new(DiskStore::new(
            base,
            limits,
            max_open_segments,
        )?)))
    }

    /// Shared out at startup and read from `/health` and `/ingest` without this lock.
    pub fn health(&self) -> std::sync::Arc<WriteHealth> {
        std::sync::Arc::clone(&self.health)
    }

    /// Segment files open for writing, and the cap they are held under.
    pub fn open_segments(&self) -> (usize, usize) {
        (self.open_segments.len(), self.open_segments.max())
    }

    /// Open the store on first write, then buffer the row.
    ///
    /// A failure to open is **counted and reported, not propagated**. Ingest returns a
    /// per-line verdict to a client and a directory that cannot be created is not that
    /// line's fault; failing the body would turn one bad mount into five hundred
    /// identical error messages. What must not happen is silence, so the count is on
    /// `/stats` - a disk failing behind a 200 on every `/ingest` is the worst outcome
    /// available here.
    pub fn append(&mut self, template_id: u64, time_column: &str, row: Row, forms: &DtForms) {
        if !self.stores.contains_key(&template_id) {
            match self.backend.open(template_id, time_column) {
                Ok(store) => {
                    self.stores.insert(template_id, store);
                }
                Err(e) => return self.note_failure(&e),
            }
        }
        if let Some(store) = self.stores.get_mut(&template_id) {
            store.append(row, forms);
        }
    }

    /// The batch entry point: the ingest path calls this and owns neither buffer nor clock.
    pub fn append_batch_to(
        &mut self,
        template_id: u64,
        time_column: &str,
        rows: impl IntoIterator<Item = (Row, Vec<(String, DtForm)>)>,
    ) {
        for (row, forms) in rows {
            self.append(template_id, time_column, row, &forms);
        }
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
    /// not make - is a store that cannot write by any reading, and `append` drops that row
    /// on the floor. Without this, a disk that fills before a template's first line
    /// answers `200` for rows that were never anywhere, which is the exact lie the
    /// refusal exists to stop.
    ///
    /// The clearing rule is still the tick's: the next pass that writes everything clears
    /// it. If the disk is still bad the next body fails again and sets it again, so the
    /// worst case is one refused-then-retried body per flush interval rather than a
    /// server stuck refusing after the disk came back.
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

    /// Write every open store's buffer, and get back under the open-segment cap.
    ///
    /// Never returns `Err`: one leaf failing is collected into the summary and the rest
    /// are still written.
    ///
    /// **Iterated over a snapshot of the ids rather than over the map.** Reaping between
    /// leaves needs `&mut` on a leaf other than the one just flushed, which a
    /// `&mut self.stores` iterator would not allow - and reaping only at the end would let
    /// a pass of five hundred leaves overshoot the cap by five hundred before anything
    /// noticed.
    pub fn flush_all(&mut self, kind: FlushKind) -> FlushSummary {
        let mut summary = FlushSummary::default();
        let mut failures = Vec::new();
        // Counted apart from `summary.failed`, which is leaves whose flush failed and is
        // what the shutdown reports rows against. A reap that could not seal is still a
        // store that cannot write, so it reaches the verdict below - without that, a pass
        // whose only failure was a reap would end by *clearing* `degraded`.
        let mut reap_failures = 0usize;
        let ids: Vec<u64> = self.stores.keys().copied().collect();
        for id in ids {
            if let Some(store) = self.stores.get_mut(&id) {
                match store.flush(kind) {
                    Ok(done) => {
                        summary.rows += done.rows;
                        summary.partitions += done.partitions;
                        summary.bytes += done.bytes;
                    }
                    Err(e) => {
                        summary.failed += 1;
                        failures.push((id, e));
                    }
                }
            }
            reap_failures += self.reap_open_segments();
        }
        for (id, e) in &failures {
            crate::error::report(e);
            let _ = id;
        }
        if let Some((_, e)) = failures.first() {
            self.write_failures += failures.len() as u64;
            self.health.set_error(crate::error::chain(e));
        }
        // The verdict `/health` and `/ingest` read. A final flush sets it too: a shutdown
        // that could not write is degraded for whatever is left of the process.
        self.health.tick(summary.failed + reap_failures);
        summary
    }

    /// Seal least recently written open segments until the count is back under the cap.
    ///
    /// **A failed seal does not stop this, and cannot make it spin.** Every path through
    /// `seal_lru` takes the entry out of the registry - a seal that fails has still closed
    /// the file - so each turn removes one and the loop is bounded by the overshoot. The
    /// earlier shape returned on the first error, which left the count above the cap and
    /// let the rest of the pass add one more per leaf, unbounded, which is the descriptor
    /// exhaustion the cap exists to prevent.
    ///
    /// Each failure is counted once, where it happened, and not once per leaf afterwards.
    /// The count comes back so the pass's verdict includes it: a reap that could not seal
    /// is a store that cannot write, and a pass that only failed there must not end by
    /// clearing `degraded`.
    fn reap_open_segments(&mut self) -> usize {
        let mut failed = 0;
        while self.open_segments.len() > self.open_segments.max() {
            let Some(key) = self.open_segments.oldest() else {
                return failed;
            };
            let Some(store) = self.stores.get_mut(&key.template_id) else {
                // A key for a leaf the registry no longer has. Dropped rather than left
                // to be chosen again on every pass.
                self.open_segments.closed(key);
                continue;
            };
            match store.seal_lru() {
                Ok(_) => {}
                Err(e) => {
                    failed += 1;
                    self.note_failure(&e);
                }
            }
            // Whatever happened, that entry has to be gone or this loop is the spin the
            // early return was avoiding. `seal_lru` clears it on every path; this is the
            // belt for a future implementation that forgets to.
            if self.open_segments.oldest() == Some(key) {
                self.open_segments.closed(key);
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
/// `None` only for a row from before M5.6b: one with neither an instant in its time
/// column nor a system column, which is a row in an `undated` partition.
pub fn effective_ts(row: &Row, column: &str) -> Option<i64> {
    ts_of(row, column).or_else(|| ts_of(row, RECEIVED_AT_COLUMN))
}

/// Whether an instant falls in `[since, until)`. A row with no effective time is outside
/// every window - the one case left is a row from before M5.6b, in `undated`.
pub fn in_window(ts: Option<i64>, since: Option<i64>, until: Option<i64>) -> bool {
    let Some(ns) = ts else {
        return false;
    };
    since.is_none_or(|from| ns >= from) && until.is_none_or(|to| ns < to)
}

/// A row as a query shows it: the system column under its query name, unless the row has
/// an ingested key of that name, in which case the ingested one is shown and the system
/// value is left out. A null system value - a segment column the row never had -
/// is left out too.
pub fn present(row: &Row) -> BTreeMap<String, &ColValue> {
    let mut out: BTreeMap<String, &ColValue> = row
        .iter()
        .filter(|(key, _)| key.as_str() != RECEIVED_AT_COLUMN)
        .map(|(key, value)| (key.clone(), value))
        .collect();
    if let Some(system) = row.get(RECEIVED_AT_COLUMN) {
        if !matches!(system, ColValue::Null) && !out.contains_key(RECEIVED_AT_NAME) {
            out.insert(RECEIVED_AT_NAME.to_string(), system);
        }
    }
    out
}

/// A `Datetime` stored in a column that is now text, rendered back to what was ingested
/// through the rung that read it. Every other value is returned as it is.
pub fn render_text(value: ColValue, form: Option<DtForm>) -> ColValue {
    match value {
        ColValue::Datetime(ns) => ColValue::String(form.unwrap_or(DtForm::Mixed).render(ns)),
        other => other,
    }
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
    columns
        .iter()
        .filter_map(|name| row.get(name).map(|v| (name.clone(), v.clone())))
        .collect()
}
