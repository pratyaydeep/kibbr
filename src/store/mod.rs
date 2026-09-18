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

/// The column a time filter reads when none is named.
pub const DEFAULT_TS_COLUMN: &str = "ts";

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
    pub ts_column: Option<String>,
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
    /// Rows examined, including those the time window and the predicates rejected.
    ///
    /// A partition the disk store skipped unread contributes nothing, which is the honest
    /// number and the reason the two backends report different ones. Making the disk
    /// store count rows it never opened would be reporting work it did not do.
    pub scanned: u64,
    /// Rows that passed everything.
    pub matched: u64,
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
    fn open(&self, template_id: u64) -> Result<Box<dyn LeafStore>, StoreError>;
    /// For the banner and `/stats`.
    fn kind(&self) -> &'static str;
    fn base_path(&self) -> Option<&Path>;
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
    last_write_error: Option<String>,
}

impl StoreManager {
    pub fn new(backend: Box<dyn Store>) -> Self {
        Self {
            backend,
            stores: BTreeMap::new(),
            write_failures: 0,
            last_write_error: None,
        }
    }

    pub fn memory() -> Self {
        Self::new(Box::new(MemoryStore::new()))
    }

    pub fn disk(base: std::path::PathBuf, limits: RowGroupLimits) -> Result<Self, StoreError> {
        Ok(Self::new(Box::new(DiskStore::new(base, limits)?)))
    }

    /// Open the store on first write, then buffer the row.
    ///
    /// A failure to open is **counted and reported, not propagated**. Ingest returns a
    /// per-line verdict to a client and a directory that cannot be created is not that
    /// line's fault; failing the body would turn one bad mount into five hundred
    /// identical error messages. What must not happen is silence, so the count is on
    /// `/stats` - a disk failing behind a 200 on every `/ingest` is the worst outcome
    /// available here.
    pub fn append(&mut self, template_id: u64, row: Row, forms: &DtForms) {
        if !self.stores.contains_key(&template_id) {
            match self.backend.open(template_id) {
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
        rows: impl IntoIterator<Item = (Row, Vec<(String, DtForm)>)>,
    ) {
        for (row, forms) in rows {
            self.append(template_id, row, &forms);
        }
    }

    /// Open a leaf the trie already knows about, without writing to it.
    ///
    /// The restart path. A leaf whose directory is absent opens empty, which is a known
    /// template holding no rows - the second of the two empty answers, not an error.
    pub fn open_existing(&mut self, template_id: u64) -> Result<(), StoreError> {
        if self.stores.contains_key(&template_id) {
            return Ok(());
        }
        let store = self.backend.open(template_id)?;
        self.stores.insert(template_id, store);
        Ok(())
    }

    fn note_failure(&mut self, e: &StoreError) {
        self.write_failures += 1;
        // Once, on the first failure. A disk that has gone away fails on every line, and
        // a message per line is how a log becomes unreadable at the moment it matters.
        if self.last_write_error.is_none() {
            crate::error::report(e);
            self.last_write_error = Some(crate::error::chain(e));
        }
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

    /// Write every open store's buffer.
    ///
    /// Never returns `Err`: one leaf failing is collected into the summary and the rest
    /// are still written.
    pub fn flush_all(&mut self, kind: FlushKind) -> FlushSummary {
        let mut summary = FlushSummary::default();
        let mut failures = Vec::new();
        for (id, store) in &mut self.stores {
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
        for (id, e) in &failures {
            crate::error::report(e);
            let _ = id;
        }
        if let Some((_, e)) = failures.first() {
            self.write_failures += failures.len() as u64;
            if self.last_write_error.is_none() {
                self.last_write_error = Some(crate::error::chain(e));
            }
        }
        summary
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

    pub fn last_write_error(&self) -> Option<&str> {
        self.last_write_error.as_deref()
    }
}

impl Default for StoreManager {
    fn default() -> Self {
        Self::memory()
    }
}

// ── Reading a row's place in time ──────────────────────────────────────────────────

/// A row's timestamp, or `None` for a row that cannot be placed in time.
///
/// **One function, three callers**, and that is the point: the time window reads it, the
/// result order reads it, and the disk store partitions by it. Two readings that disagree
/// by one type would put a row in a dated directory while ranking it as undated, and the
/// partition holding it would then be skipped by a query that should have found it - only
/// under a limit, only once the heap fills, so an unlimited query and a small fixture
/// would both pass.
pub fn ts_of(row: &Row, column: &str) -> Option<i64> {
    match row.get(column) {
        Some(ColValue::Datetime(ns)) => Some(*ns),
        _ => None,
    }
}

/// Whether a row's timestamp falls in `[since, until)`.
///
/// A column holding anything but a `Datetime` - absent, null, or widened to `String` by
/// one bad line - cannot be placed in time, so it is out. That is the cost of the
/// lattice's monotonicity, and it should show as an empty result rather than as rows
/// silently admitted on a timestamp nobody could read.
///
/// Which settles the open case: a **template with no `ts` column at all**, under a time
/// filter, answers `exists: true, count: 0`. The structure was found; no row in it can
/// satisfy the predicate. Deliberately not the two alternatives:
///
/// - **Not an error.** The pattern is well-formed and the template is real, and "found,
///   and nothing matched" is an answer rather than a failure.
/// - **Not "ignore the time clause".** Admitting those rows would make a filtered query
///   return rows outside its own window, with no way for the operator to see it happened.
///
/// This reaches further than it looks. Kibana puts a mandatory time filter on every search,
/// so once it is the client every ts-less template is queried this way on every request:
/// erroring would break Discover outright, and ignoring the clause would fill a time
/// histogram with rows that have no place on it.
pub fn in_window(value: Option<&ColValue>, since: Option<i64>, until: Option<i64>) -> bool {
    let Some(ColValue::Datetime(ns)) = value else {
        return false;
    };
    since.is_none_or(|from| *ns >= from) && until.is_none_or(|to| *ns < to)
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
