// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Leaf storage: the scan interface, the two implementations behind it, and the registry.
//! `LeafStore` holds one leaf's rows and `Store` writes them out; neither has a clock, which
//! lives in the server (`src/server/runtime.rs`). No lock is over all of it: each leaf locks itself, and a flush
//! holds no lock an append or a query waits on while it touches the disk
//! (`disk::DiskLeaf::state`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::engine::model::types::ColValue;

pub(crate) mod disk;
pub(crate) mod filter;
pub(crate) mod manifest;
pub(crate) mod memory;
pub(crate) mod scan_threads;
pub(crate) mod visit;

pub(crate) use disk::DiskStore;
pub(crate) use memory::MemoryStore;
pub use visit::{present, Scan, ScanOptions, Shown, Term, Visitor, Want, Wants};
pub(crate) use visit::{
    in_window, render_text, shown_row, Counted, DEFAULT_TS_COLUMN, RECEIVED_AT_COLUMN,
};
#[cfg(test)]
pub(crate) use visit::{project, Rows};
// A row, and its place in time, are the model's: the format builds from them too.
pub(crate) use crate::engine::model::row::{
    effective_ts, instant_of, own_ts, said_instant, said_ts, ts_of, Row,
};

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
    Format(#[from] crate::engine::format::FormatError),
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
    Build(#[from] crate::engine::format::build::BuildError),
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
        source: crate::engine::format::FormatError,
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
    #[cfg(test)]
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

/// One leaf's rows, append-only; every method takes `&self` because a leaf locks itself
/// and none of its callers waits on a lock over any other leaf.
pub trait LeafStore: Send + Sync {
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

    /// Every surviving row, projected through `opts.columns`; defaulted so the projection
    /// rule lives in one place.
    #[cfg(test)]
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
    fn fan(&self) -> Option<Arc<scan_threads::Fan>> {
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
    pub fn fan(&self) -> Option<Arc<scan_threads::Fan>> {
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
            crate::engine::error::report(e);
        }
        self.health.set_error(crate::engine::error::chain(e));
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

    #[cfg(test)]
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
            crate::engine::error::report(e);
        }
        if let Some(e) = failures.first() {
            self.write_failures
                .fetch_add(failures.len() as u64, Ordering::Relaxed);
            self.health.set_error(crate::engine::error::chain(e));
        }
        self.forced_closes
            .fetch_add(done.forced as u64, Ordering::Relaxed);
        // The verdict `/health` reads. A final flush sets it too: a shutdown that could
        // not write is degraded for whatever is left of the process.
        self.health.tick(failed.len() + seal_failures + unopened);
        FlushSummary {
            rows: done.rows,
            #[cfg(test)]
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
    #[cfg(test)]
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
pub(crate) fn nanos_of(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos()).map_or(i64::MIN, |n| -n),
    }
}

/// One value's payload width: a string's bytes, or the fixed-width cell the column store
/// writes.
pub(crate) fn value_bytes(value: &ColValue) -> usize {
    match value {
        ColValue::String(s) | ColValue::Json(s) => s.len(),
        ColValue::Int64(_) | ColValue::Float64(_) | ColValue::Datetime(_) => 8,
        ColValue::Bool(_) => 1,
        ColValue::Null => 0,
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
