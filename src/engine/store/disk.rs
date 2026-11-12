// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The disk store: one shared file per UTC day, named in `manifest.log`.
//!
//! One file per day because every cost of the per-template layout came from the file
//! count. A leaf holds a template's row groups by day and its rows not yet in one;
//! closing a row group holds no lock across the disk (`DiskLeaf::state`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::time::{Instant, SystemTime};

use chrono::{DateTime, NaiveDate};

use super::fan::Fan;
use super::manifest::{self, FileRecord, Manifest, Record};
use crate::engine::format::build::{self, schema_of, Encoded, Level};
use crate::engine::format::file::{self, Entry, Index, Mark, Reader, Writer};
use crate::engine::format::{day_of, FormatError, Source, DAY_NS};
use super::{
    effective_ts, lock, nanos_of, value_bytes, DiskOptions, FlushKind, Flushed, LeafStore, Row,
    RowGroupLimits, Scan, ScanOptions, Snapshot, Store, StoreError, Term, Visitor,
};

mod rebuild;
mod scan;

// Named through here by whoever runs a compaction: the command, the server's job and its
// scheduler, the bench.
pub use rebuild::{CompactOptions, Compacted, Due};

pub const DATA_DIR: &str = "data";
pub const STAGING_DIR: &str = "staging";

/// The size a day's file is sealed at; an unsealed file this large is still quick to
/// walk at startup.
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 512 * 1024 * 1024;

fn warn(message: String) {
    tracing::warn!("{message}");
}

// ── Names ──────────────────────────────────────────────────────────────────────────

/// `2024-01-15` for a day since the epoch.
pub fn day_name(day: i64) -> String {
    DateTime::from_timestamp(day * 86_400, 0).map_or_else(
        || format!("day_{day}"),
        |dt| dt.format("%Y-%m-%d").to_string(),
    )
}

pub fn day_of_name(name: &str) -> Option<i64> {
    let date = NaiveDate::parse_from_str(name, "%Y-%m-%d").ok()?;
    Some(
        date.and_hms_opt(0, 0, 0)?
            .and_utc()
            .timestamp()
            .div_euclid(86_400),
    )
}

/// Today, as days since the epoch, UTC: the `today` a caller with no clock of its own
/// hands `DiskStore::due_days`.
pub(crate) fn today() -> i64 {
    day_of(nanos_of(SystemTime::now()))
}

fn file_name(number: u32) -> String {
    format!("seg_0_{number:06}.col")
}

/// The number in `seg_0_NNNNNN.col`, and `None` for any other name.
fn number_of(name: &str) -> Option<u32> {
    let digits = name.strip_prefix("seg_0_")?.strip_suffix(".col")?;
    if digits.len() < 6 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

// ── Refusing what came before ──────────────────────────────────────────────────────

/// Refuse a data directory holding an earlier layout, or a data file of another format
/// version, by path; it reads header bytes only, so it can run before `LOCK` is taken.
pub fn refuse_other_versions(base: &Path) -> Result<(), StoreError> {
    let mut leaves: Vec<PathBuf> = fs::read_dir(base)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|e| e.file_name().to_string_lossy().starts_with("leaf_"))
        .map(|e| e.path())
        .collect();
    leaves.sort();
    if let Some(path) = leaves.into_iter().next() {
        return Err(StoreError::LegacyLayout { path });
    }
    let mut files = Vec::new();
    for day in dirs(&base.join(DATA_DIR)) {
        for entry in fs::read_dir(&day).into_iter().flatten().flatten() {
            if entry.file_type().is_ok_and(|t| t.is_file()) {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    for path in files {
        match file::version_of(&path) {
            Ok(Some(found)) if found != crate::engine::format::VERSION => {
                return Err(FormatError::Version { path, found }.into());
            }
            Err(e @ FormatError::PreRename { .. }) => return Err(e.into()),
            _ => {}
        }
    }
    Ok(())
}

/// Whether a file `manifest.log` names is on disk, `false` only when the system says
/// there is no such file: `Path::exists` is false for any error at all.
fn named_is_there(path: &Path) -> Result<bool, StoreError> {
    match fs::metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(why) => Err(StoreError::NamedUnread {
            dir: refused_at(path),
            file: path.to_path_buf(),
            why,
        }),
    }
}

/// The directory that would not be searched for `path`: the nearest one above it that
/// can itself be looked at.
fn refused_at(path: &Path) -> PathBuf {
    let mut at = path;
    while let Some(above) = at.parent() {
        if fs::metadata(above).is_ok() {
            return above.to_path_buf();
        }
        at = above;
    }
    path.parent().unwrap_or(path).to_path_buf()
}

fn dirs(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

// ── Files ──────────────────────────────────────────────────────────────────────────

/// A data file a row group lives in, deleted once retired when its last reader lets go:
/// a query that copied a day's row groups opens their file only when it reaches them.
#[derive(Debug)]
pub(crate) struct DataFile {
    pub path: PathBuf,
    retired: AtomicBool,
}

impl DataFile {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            retired: AtomicBool::new(false),
        }
    }

    fn retire(&self) {
        self.retired.store(true, Ordering::Relaxed);
    }

    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Relaxed)
    }
}

impl Drop for DataFile {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Relaxed) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// One row group a leaf can read: its file, its index entry, and its schema's columns.
#[derive(Debug, Clone)]
struct Stored {
    file: Arc<DataFile>,
    entry: Entry,
    schema: Arc<Vec<String>>,
}

/// A day's file being written.
struct Open {
    writer: Writer,
    file: Arc<DataFile>,
    rel: String,
    /// Each schema's columns, shared by every `Stored` that names it.
    schemas: HashMap<u32, Arc<Vec<String>>>,
    /// The flush that last wrote to it. At the cap on open files the least recently
    /// written is sealed to make room.
    written: u64,
    /// Where the running flush began writing to it: what a failed flush cuts it back to.
    began: Option<Mark>,
}

impl Open {
    /// Cut the file back to `mark`, and forget the schemas written since with it.
    fn rollback(&mut self, mark: Mark) -> Result<(), FormatError> {
        let cut = self.writer.rollback(mark);
        self.schemas.retain(|id, _| (*id as usize) < mark.schemas());
        cut
    }
}

/// The files being written, and the manifest that names them. Behind the flush's own
/// lock, which a load's commit takes too; no append and no query ever waits on it.
struct Files {
    data: PathBuf,
    manifest: Manifest,
    /// The manifest's live files, kept current with every record appended: what
    /// replace-a-day takes its day's directory from and retires.
    live: BTreeMap<String, FileRecord>,
    /// Days a file found at startup shares with another day, and that file: see `day_files`.
    /// Only a directory from before every row went into its own day's file has one.
    mixed: BTreeMap<i64, String>,
    /// Every load a record names as its source. A repeat is refused by its id.
    loads: BTreeSet<u64>,
    /// Loads committed whose files could not all be moved out of staging: no load commits
    /// beside them until a restart finishes the move.
    unmoved: BTreeSet<u64>,
    open: BTreeMap<i64, Open>,
    /// The next file number per day directory.
    next: BTreeMap<i64, u32>,
    max_open: usize,
    max_bytes: u64,
    /// Flushes so far: the clock `Open::written` is kept by.
    flushes: u64,
    /// The UTC day on which each day's directory last took a row group from the live door.
    /// In memory; a start takes the newest modification time among a day's unfinished files.
    last_live: BTreeMap<i64, i64>,
    /// The day a compaction has begun and not yet committed or given up: one at a time.
    compacting: Option<i64>,
    /// Files a compaction wrote whose `replace` record may or may not be on disk: kept, and
    /// no day is compacted, until the next record the manifest takes overwrites its place.
    uncertain: Vec<PathBuf>,
    /// The highest write-ahead log position this process has seen in each day; a day with no
    /// entry was wholly behind the checkpoint this process started from.
    wal_through: BTreeMap<i64, u64>,
    /// Every replace-a-day's word to the write-ahead log, as the manifest holds them and
    /// as this process adds to them: what a replay must not bring back.
    replaced: Vec<manifest::WalMark>,
}

impl Files {
    /// Append records to the manifest - durable once this returns - and apply them to the
    /// live set this store keeps.
    fn record(&mut self, records: Vec<Record>) -> Result<(), manifest::ManifestError> {
        self.manifest.append(&records)?;
        // These records are durable, and they start at the offset a compaction's uncertain
        // record was cut back to: that record is not on disk, so its files are nobody's.
        for path in std::mem::take(&mut self.uncertain) {
            let _ = fs::remove_file(&path);
        }
        self.loads.extend(manifest::load_ids(&records));
        for record in &records {
            manifest::apply(&mut self.live, record);
        }
        Ok(())
    }

    /// Whether a file must be sealed before `day` can open one: it has none open, and the
    /// cap on open files is reached.
    fn full_for(&self, day: i64) -> bool {
        !self.open.contains_key(&day) && self.open.len() >= self.max_open.max(1)
    }

    /// The open file to seal to make room: one no batch of this flush still wants, then the
    /// least recently written, the older day first.
    fn least_recent(&self, wanted: &BTreeMap<i64, usize>) -> Option<i64> {
        self.open
            .iter()
            .min_by_key(|(day, open)| (wanted.contains_key(day), open.written, **day))
            .map(|(day, _)| *day)
    }

    /// The open file in `dir`'s directory, created and named in the manifest if there is
    /// none. The caller has made room for it under the cap first (`full_for`).
    fn open_in(&mut self, dir: i64) -> Result<&mut Open, StoreError> {
        if !self.open.contains_key(&dir) {
            let path_dir = self.data.join(day_name(dir));
            fs::create_dir_all(&path_dir).map_err(|source| FormatError::Io {
                path: path_dir.clone(),
                source,
            })?;
            // A new directory's entry is made durable before a file in it is named.
            crate::engine::format::frame::sync_dir(&path_dir)
                .and_then(|()| crate::engine::format::frame::sync_dir(&self.data))
                .map_err(|source| FormatError::Io {
                    path: path_dir.clone(),
                    source,
                })?;
            let number = self.next.get(&dir).copied().unwrap_or(1);
            let rel = format!("{}/{}", day_name(dir), file_name(number));
            let path = self.data.join(&rel);
            let writer = Writer::create(&path)?;
            // Named before its first row group, so a crash cannot leave durable row groups
            // in a file the next start would delete as unnamed.
            let named = self.record(vec![Record::Add {
                files: vec![FileRecord::open(rel.clone(), &writer.index().file_id)],
                source: None,
            }]);
            if let Err(e) = named {
                drop(writer);
                let _ = fs::remove_file(&path);
                return Err(e.into());
            }
            self.next.insert(dir, number + 1);
            self.open.insert(
                dir,
                Open {
                    writer,
                    file: Arc::new(DataFile::new(path)),
                    rel,
                    schemas: HashMap::new(),
                    written: self.flushes,
                    began: None,
                },
            );
        }
        self.open.get_mut(&dir).ok_or_else(|| StoreError::Io {
            template_id: 0,
            source: std::io::Error::other("a day file vanished while it was being opened"),
        })
    }

    /// Append one moved-aside batch's row groups, encoded, to its day's file. Not flushed:
    /// a flush flushes each file it wrote to once.
    fn write(
        &mut self,
        moved: &Moved,
        encoded: Vec<EncodedRowGroup>,
    ) -> Result<Vec<Stored>, StoreError> {
        // A batch that did not encode whole touches no file.
        let encoded = encoded.into_iter().collect::<Result<Vec<_>, _>>()?;
        let flushes = self.flushes;
        let open = self.open_in(moved.day)?;
        open.written = flushes;
        let mark = open.writer.mark();
        open.began.get_or_insert(mark);
        match append_row_groups(open, moved.leaf.template_id, encoded) {
            Ok(stored) => Ok(stored),
            Err(e) => {
                // Whatever of this batch reached the file is cut back: one bad batch costs its own
                // rows a retry, and not the day's. A cut that fails leaves the writer torn.
                let _ = open.rollback(mark);
                Err(e)
            }
        }
    }

    /// Flush the file in `dir`'s directory: once, for every row group a flush appended to
    /// it.
    fn sync(&mut self, dir: i64) -> Result<(), StoreError> {
        let Some(open) = self.open.get_mut(&dir) else {
            return Ok(());
        };
        let began = open.began.take();
        match open.writer.sync() {
            Ok(()) => Ok(()),
            Err(e) => {
                // Nothing this flush appended here is known to be durable: the file is cut back and
                // its row groups go back to their buffers, or a retry would write those rows twice.
                if let Some(mark) = began {
                    let _ = open.rollback(mark);
                }
                Err(e.into())
            }
        }
    }

    /// After a flush: seal every file past the size cap, returning what was sealed and what
    /// could not be.
    fn settle(&mut self) -> (usize, Vec<StoreError>) {
        let mut errors = Vec::new();
        for open in self.open.values_mut() {
            open.began = None;
        }
        // A writer that could not cut a failed write back cannot be sealed: it is let go named
        // and unsealed, for the next start to recover.
        let torn: Vec<i64> = self
            .open
            .iter()
            .filter(|(_, open)| open.writer.is_torn())
            .map(|(day, _)| *day)
            .collect();
        for day in torn {
            if let Some(open) = self.open.remove(&day) {
                errors.push(StoreError::from(FormatError::Io {
                    path: open.file.path.clone(),
                    source: std::io::Error::other(
                        "a failed write could not be cut back; the file takes nothing more and \
                         the next start recovers it",
                    ),
                }));
            }
        }
        let seal: Vec<i64> = self
            .open
            .iter()
            .filter(|(_, open)| open.writer.len() >= self.max_bytes)
            .map(|(day, _)| *day)
            .collect();
        let (sealed, failed) = self.seal_days(seal);
        errors.extend(failed);
        (sealed, errors)
    }

    fn seal_all(&mut self) -> (usize, Vec<StoreError>) {
        let days: Vec<i64> = self.open.keys().copied().collect();
        self.seal_days(days)
    }

    /// Seal these days' files, and record every seal in one manifest append - so sealing
    /// every open day costs the manifest one flush, not one per file.
    fn seal_days(&mut self, days: Vec<i64>) -> (usize, Vec<StoreError>) {
        let mut records = Vec::new();
        let mut errors = Vec::new();
        for day in days {
            // The file leaves the open set whatever happens: a seal that failed has still stopped
            // taking rows, and the next start recovers it.
            let Some(open) = self.open.remove(&day) else {
                continue;
            };
            match open.writer.seal() {
                Ok(index) => records.push(FileRecord::sealed(open.rel, &index)),
                Err(e) => errors.push(e.into()),
            }
        }
        let sealed = records.len();
        if !records.is_empty() {
            // A seal the manifest never heard of is found sealed at the next start, which
            // flushes the file again and records it then.
            if let Err(e) = self.record(vec![Record::Seal { files: records }]) {
                errors.push(e.into());
            }
        }
        (sealed, errors)
    }
}

impl Files {
    /// Every live file in `day`'s directory: what replace-a-day retires. Refused for a day a
    /// file found at startup shares with another, whose rows the directory would take too.
    fn day_files(&self, day: i64) -> Result<Vec<String>, StoreError> {
        if let Some(rel) = self.mixed.get(&day) {
            return Err(StoreError::ReplaceRefused {
                day: day_name(day),
                path: rel.clone(),
                why: "it holds row groups of more than one day - written before every row went \
                      into its own day's file - so retiring the day's directory would take \
                      another day's rows or leave some of this day's"
                    .to_string(),
            });
        }
        let prefix = format!("{}/", day_name(day));
        Ok(self
            .live
            .keys()
            .filter(|rel| rel.starts_with(&prefix))
            .cloned()
            .collect())
    }
}

/// A batch's row groups, numbered in `open`'s schema records and appended in order.
fn append_row_groups(
    open: &mut Open,
    template_id: u64,
    encoded: Vec<(Vec<String>, Encoded)>,
) -> Result<Vec<Stored>, StoreError> {
    let mut out = Vec::with_capacity(encoded.len());
    for (columns, rows) in encoded {
        let schema_id = open.writer.schema_id(&columns)?;
        let entry = open.writer.append(template_id, &rows.frame(schema_id))?;
        let schema = Arc::clone(
            open.schemas
                .entry(schema_id)
                .or_insert_with(|| Arc::new(columns)),
        );
        out.push(Stored {
            file: Arc::clone(&open.file),
            entry,
            schema,
        });
    }
    Ok(out)
}

/// One row group's columns and encoding, or why it has none.
type EncodedRowGroup = Result<(Vec<String>, Encoded), StoreError>;

/// One row group to encode: rows `from..to` of the `moved`th batch.
struct Job {
    moved: usize,
    from: usize,
    to: usize,
}

/// Every batch cut into row groups of at most `limits.rows` rows and about `limits.bytes`
/// of column data, in order.
fn jobs_of(moved: &[Moved], limits: &RowGroupLimits) -> Vec<Job> {
    let mut jobs = Vec::new();
    for (at, m) in moved.iter().enumerate() {
        let rows = &m.batch.rows;
        let mut from = 0;
        while from < rows.len() {
            let mut to = from;
            let mut bytes = 0;
            while to < rows.len()
                && (to == from || (to - from < limits.rows && bytes < limits.bytes))
            {
                bytes += rows[to].bytes;
                to += 1;
            }
            jobs.push(Job {
                moved: at,
                from,
                to,
            });
            from = to;
        }
    }
    jobs
}

/// Where a row group's rows came from in the write-ahead log `stream`: the range replay
/// skips by. Not recorded for a row with no position, which would promise lines.
fn source_of(stream: u64, rows: &[Arc<Buffered>]) -> Source {
    if stream == 0 || rows.iter().any(|b| b.pos == 0) {
        return Source::NotRecorded;
    }
    let first = rows.iter().map(|b| b.pos).min();
    let last = rows.iter().map(|b| b.pos).max();
    match (first, last) {
        (Some(first), Some(last)) => Source::Wal {
            stream,
            first,
            last,
        },
        _ => Source::NotRecorded,
    }
}

/// Every job's row group, encoded on up to `threads` threads at once, in job order. An
/// encoder that panics fails its batch, not the flush, so its rows go back to their buffer.
fn encode_all(moved: &[Moved], jobs: &[Job], threads: usize, stream: u64) -> Vec<EncodedRowGroup> {
    let encode = |job: &Job| {
        let m = &moved[job.moved];
        let rows = &m.batch.rows[job.from..job.to];
        let refs: Vec<&Row> = rows.iter().map(|b| &b.row).collect();
        let times: Vec<i64> = rows.iter().map(|b| b.time).collect();
        let source = source_of(stream, rows);
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            let columns = schema_of(refs.iter().copied());
            let encoded = build::encode(
                m.leaf.template_id,
                &columns,
                &refs,
                &times,
                source,
                Level::Live,
            )?;
            Ok((columns, encoded))
        }));
        encoded.unwrap_or_else(|_| {
            Err(StoreError::Io {
                template_id: m.leaf.template_id,
                source: std::io::Error::other("the row group encoder panicked"),
            })
        })
    };
    let threads = threads.min(jobs.len());
    if threads <= 1 {
        return jobs.iter().map(encode).collect();
    }
    let next = AtomicUsize::new(0);
    let done: Vec<OnceLock<EncodedRowGroup>> = jobs.iter().map(|_| OnceLock::new()).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| loop {
                let at = next.fetch_add(1, Ordering::Relaxed);
                let Some(job) = jobs.get(at) else {
                    break;
                };
                let _ = done[at].set(encode(job));
            });
        }
    });
    done.into_iter()
        .map(|slot| {
            slot.into_inner().unwrap_or_else(|| {
                Err(StoreError::Io {
                    template_id: 0,
                    source: std::io::Error::other("a row group was never encoded"),
                })
            })
        })
        .collect()
}

// ── What a load hands the store ────────────────────────────────────────────────────

/// A sealed, finished file a load wrote into its staging directory.
#[derive(Debug)]
pub struct StagedFile {
    /// The UTC day its rows are on, as days since the epoch.
    pub day: i64,
    pub path: PathBuf,
    pub index: Index,
}

/// A load, staged and ready to publish.
#[derive(Debug)]
pub struct LoadCommit {
    pub load_id: u64,
    /// In storage order: each day's files in the order they were written.
    pub files: Vec<StagedFile>,
    /// Retire every other file of this day, as days since the epoch, in the same record.
    pub replace_day: Option<i64>,
}

/// What a commit published and retired, by path under `data/`.
#[derive(Debug, Default)]
pub struct Committed {
    pub added: Vec<String>,
    pub replaced: Vec<String>,
}

// ── The store ──────────────────────────────────────────────────────────────────────

pub struct DiskStore {
    base: PathBuf,
    limits: RowGroupLimits,
    max_open: usize,
    max_buffer_bytes: u64,
    segment_max_bytes: u64,
    /// Threads a flush encodes row groups on: one per core, up to eight.
    encoders: usize,
    /// The threads a scan may choose a day's rows on beside its own, handed to each leaf as
    /// it opens: see `fan`.
    fan: Option<Arc<Fan>>,
    files: Mutex<Files>,
    /// Every leaf this store has opened, by template: for a flush to walk, and for a query
    /// to take its snapshot from in the hold a load publishes under.
    leaves: RwLock<BTreeMap<u64, Arc<DiskLeaf>>>,
    /// Held in write by a load's publish and in read by a query's snapshot while it freezes
    /// its leaves. Apart from `leaves`, where a wide query's freezes would stall a first row.
    publish: RwLock<()>,
    /// Loads and replaces published so far: see `Store::generation`. Bumped under the
    /// write hold of `publish`, and read under a read hold.
    generation: AtomicU64,
    /// Row groups on disk with no leaf open yet, by template, in storage order. A leaf takes
    /// its own when it opens; a template no catalog record names keeps them here, unread.
    stored: Mutex<BTreeMap<u64, Vec<Stored>>>,
    claimed: Vec<u64>,
    /// Memory held by every leaf's buffered rows and by the closes in flight, as
    /// `Row::held_bytes` estimates it: what the memory limit is held against.
    buffered: Arc<AtomicU64>,
    /// Files open for writing as the last flush left them. Read by `/stats`, without the
    /// files lock a flush holds across the disk.
    open_count: AtomicUsize,
    /// The write-ahead log's id, once one is attached; 0 with none. What a row group
    /// written from positioned rows names as its stream.
    stream: AtomicU64,
    /// The log's checkpoint as this store was last told it: what a day is held to before
    /// the scheduler compacts it (`due_days`).
    wal_checkpoint: AtomicU64,
    /// The highest log position the applier has handed over, a leaf has taken, or a
    /// start has met in the log: see `note_applied`.
    wal_applied: Arc<AtomicU64>,
    /// Held in read by the log's applier across each batch, and in write by a replace-a-day
    /// while it retires its day: see `manifest::WalMark`.
    applying: RwLock<()>,
}

impl DiskStore {
    /// Open the data directory the caller holds the `LOCK` of: recover what a crash left, index
    /// every row group, and delete nothing under `data/` until every named file has opened.
    pub fn open(base: PathBuf, options: DiskOptions) -> Result<Self, StoreError> {
        fs::create_dir_all(&base).map_err(|source| StoreError::Io {
            template_id: 0,
            source,
        })?;
        refuse_other_versions(&base)?;
        let (mut manifest, records) = Manifest::open(&manifest::path_in(&base), &mut |m| {
            warn(format!("manifest.log - {m}"))
        })?;
        let mut live = manifest::live(&records);
        let loads = manifest::load_ids(&records);
        let replaced: Vec<manifest::WalMark> = records
            .iter()
            .filter_map(|record| match record {
                Record::Replace { wal, .. } => wal.clone(),
                _ => None,
            })
            .collect();

        let data = base.join(DATA_DIR);
        fs::create_dir_all(&data).map_err(|source| FormatError::Io {
            path: data.clone(),
            source,
        })?;
        // Finished before `staging/` goes: a load whose record is written is committed,
        // and its files are the only copy of its rows.
        let staging = base.join(STAGING_DIR);
        finish_moves(
            &staging,
            &data,
            &live,
            &manifest::published_by_load(&records),
        )?;
        if staging.exists() {
            // A file still here belongs to a load that never committed - bar a spool of standard
            // input, which is only ever a copy of what a load was reading.
            let uncommitted = files_under(&staging)
                .map_err(|source| FormatError::Io {
                    path: staging.clone(),
                    source,
                })?
                .into_iter()
                .filter(|p| p.extension().is_none_or(|e| e != "spool"))
                .count();
            if uncommitted > 0 {
                warn(format!(
                    "{}: {uncommitted} file(s) of a bulk load that never committed; deleted",
                    staging.display()
                ));
            }
            fs::remove_dir_all(&staging).map_err(|source| FormatError::Io {
                path: staging.clone(),
                source,
            })?;
        }

        let mut next: BTreeMap<i64, u32> = BTreeMap::new();
        let mut stored: BTreeMap<u64, Vec<Stored>> = BTreeMap::new();
        let mut mixed: BTreeMap<i64, String> = BTreeMap::new();
        let mut last_live: BTreeMap<i64, i64> = BTreeMap::new();
        let mut seals = Vec::new();
        for (rel, record) in &live {
            let path = data.join(rel);
            let (day, number) = rel
                .split_once('/')
                .and_then(|(d, n)| Some((day_of_name(d)?, number_of(n)?)))
                .ok_or_else(|| FormatError::Corrupt {
                    path: manifest::path_in(&base),
                    offset: 0,
                    what: format!("names `{rel}`, which is not a day's data file"),
                })?;
            // The number after this one is the day's next file, and the last number there is has
            // none: refused by name, like any path that is not a day's data file.
            let after = number.checked_add(1).ok_or_else(|| FormatError::Corrupt {
                path: manifest::path_in(&base),
                offset: 0,
                what: format!("names `{rel}`, whose number leaves none for the day's next file"),
            })?;
            let slot = next.entry(day).or_insert(1);
            *slot = (*slot).max(after);
            if !named_is_there(&path)? {
                return Err(StoreError::NamedAbsent { file: path });
            }
            let index = if record.sealed {
                file::open_sealed(&path)?.ok_or_else(|| FormatError::Corrupt {
                    path: path.clone(),
                    offset: 0,
                    what: "manifest.log says this file is sealed and its footer does not \
                           check out"
                        .into(),
                })?
            } else {
                let index = match file::open_sealed(&path)? {
                    // Sealed, but the manifest never heard, and the footer may be in the page cache
                    // alone: flushed first, or a power cut leaves a record of a footer not on disk.
                    Some(index) => {
                        fs::File::open(&path)
                            .and_then(|f| f.sync_all())
                            .map_err(|source| FormatError::Io {
                                path: path.clone(),
                                source,
                            })?;
                        index
                    }
                    None => {
                        let recovered = file::recover(&path)?;
                        if let Some((at, len)) = recovered.torn_at {
                            warn(format!(
                                "{}: torn at offset {at} of {len}; truncated to the {} row \
                                 group(s) before it and sealed",
                                path.display(),
                                recovered.index.entries.len()
                            ));
                        }
                        recovered.index
                    }
                };
                seals.push(FileRecord::sealed(rel.clone(), &index));
                index
            };
            if manifest::hex(&index.file_id) != record.file_id {
                return Err(FormatError::Corrupt {
                    path,
                    offset: 8,
                    what: format!(
                        "file id {} is not the {} manifest.log names",
                        manifest::hex(&index.file_id),
                        record.file_id
                    ),
                }
                .into());
            }
            // A compacted file names its parents, and the record that added it removed them: a
            // parent still live under its id means the rows are here twice.
            for (parent, id) in &index.replaced {
                if live
                    .get(parent)
                    .is_some_and(|named| named.file_id == manifest::hex(id))
                {
                    return Err(StoreError::ReplacedLive {
                        child: path,
                        parent: data.join(parent),
                    });
                }
            }
            if !index.finished {
                let written = fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .map_or_else(|_| today(), |at| day_of(nanos_of(at)));
                let last = last_live.entry(day).or_insert(written);
                *last = (*last).max(written);
            }
            // A row group of another day than its directory's, from a build that sent a late row
            // into today's file: both days are noted, with the file, for `day_files` to refuse.
            for e in &index.entries {
                let other = day_of(e.min_time);
                if other != day {
                    mixed.entry(day).or_insert_with(|| rel.clone());
                    mixed.entry(other).or_insert_with(|| rel.clone());
                }
            }
            index_rows(&mut stored, Arc::new(DataFile::new(path)), &index);
        }
        // Every named file is there and opens. Still before this returns, so no file is
        // ever created under a number an unnamed one holds.
        delete_unnamed(&data, &live)?;
        if !seals.is_empty() {
            let record = Record::Seal { files: seals };
            manifest.append(std::slice::from_ref(&record))?;
            manifest::apply(&mut live, &record);
        }
        let claimed = stored.keys().copied().collect();
        Ok(Self {
            base,
            limits: options.row_groups,
            max_open: options.max_open_files,
            max_buffer_bytes: options.max_buffer_bytes,
            segment_max_bytes: options.segment_max_bytes,
            encoders: std::thread::available_parallelism().map_or(1, |n| n.get().min(8)),
            // A struct and no thread: the first scan that has a use for them starts them.
            fan: Fan::new(options.scan_threads, options.fan_rows),
            files: Mutex::new(Files {
                data,
                manifest,
                live,
                mixed,
                loads,
                unmoved: BTreeSet::new(),
                open: BTreeMap::new(),
                next,
                max_open: options.max_open_files,
                max_bytes: options.segment_max_bytes,
                flushes: 0,
                last_live,
                compacting: None,
                uncertain: Vec::new(),
                wal_through: BTreeMap::new(),
                replaced,
            }),
            leaves: RwLock::new(BTreeMap::new()),
            publish: RwLock::new(()),
            generation: AtomicU64::new(0),
            stored: Mutex::new(stored),
            claimed,
            buffered: Arc::new(AtomicU64::new(0)),
            open_count: AtomicUsize::new(0),
            stream: AtomicU64::new(0),
            wal_checkpoint: AtomicU64::new(0),
            wal_applied: Arc::new(AtomicU64::new(0)),
            applying: RwLock::new(()),
        })
    }

    // ── The write-ahead log ────────────────────────────────────────────────────────

    /// Attach the write-ahead log, its id and the checkpoint it starts from, so a row group
    /// cut from positioned rows records their range under this stream.
    pub fn attach_wal(&self, stream: u64, checkpoint: u64) {
        self.stream.store(stream, Ordering::Relaxed);
        self.wal_checkpoint.store(checkpoint, Ordering::Relaxed);
        self.wal_applied.fetch_max(checkpoint, Ordering::Relaxed);
    }

    /// What the log's applier holds while it hands a batch of rows over: a replace-a-day
    /// waits for it, and no row is applied while one retires its day.
    pub fn applying(&self) -> std::sync::RwLockReadGuard<'_, ()> {
        self.applying.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Hold the log's applier still, as a replace-a-day does while it retires its day:
    /// for a test of what waits behind a store that is slow to take rows.
    #[cfg(test)]
    pub fn still(&self) -> std::sync::RwLockWriteGuard<'_, ()> {
        self.applying.write().unwrap_or_else(|e| e.into_inner())
    }

    /// The days a replace-a-day retired under `stream`, each with the highest position
    /// retired: a replay stores no line of the day at or below it.
    pub fn replaced_days(&self, stream: u64) -> BTreeMap<i64, u64> {
        let stream = format!("{stream:016x}");
        let mut out: BTreeMap<i64, u64> = BTreeMap::new();
        for mark in lock(&self.files)
            .replaced
            .iter()
            .filter(|m| m.stream == stream)
        {
            // The day as `day_name` writes it and no other spelling: a mark is not typed, and it
            // says which lines a replay drops.
            let day = day_of_name(&mark.day).filter(|day| day_name(*day) == mark.day);
            if let Some(day) = day {
                let through = out.entry(day).or_insert(mark.through);
                *through = (*through).max(mark.through);
            }
        }
        out
    }

    /// Every position up to this one came before whatever happens next: what a replace-a-day
    /// retires through, said by the applier for each batch and by a start for the log it read.
    pub fn note_applied(&self, position: u64) {
        self.wal_applied.fetch_max(position, Ordering::Relaxed);
    }

    /// Every row now buffered has already waited its wait, so the next tick closes them all:
    /// restarted from the replay, a server stopped often would never close them.
    pub fn age_buffers(&self) {
        let Some(past) = Instant::now().checked_sub(self.limits.age) else {
            return;
        };
        let leaves: Vec<Arc<DiskLeaf>> = self
            .leaves
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        for leaf in leaves {
            for part in leaf.state().days.values_mut() {
                for group in &mut part.groups {
                    group.opened_at = group.opened_at.min(past);
                }
            }
        }
    }

    /// The log's checkpoint moved: every position up to it is in a row group.
    pub fn note_checkpoint(&self, checkpoint: u64) {
        self.wal_checkpoint.fetch_max(checkpoint, Ordering::Relaxed);
    }

    /// Replay met position `position` in `day`, stored by this start or already in a row
    /// group: the day is not behind the checkpoint until the checkpoint passes it.
    pub fn note_position(&self, day: i64, position: u64) {
        self.wal_applied.fetch_max(position, Ordering::Relaxed);
        let mut files = lock(&self.files);
        let seen = files.wal_through.entry(day).or_insert(position);
        *seen = (*seen).max(position);
    }

    /// The positions of `stream` that row groups of one template and one day hold, merged:
    /// what replay skips, read from the row groups' own headers, which alone hold a range.
    pub fn wal_ranges(
        &self,
        template_id: u64,
        day: i64,
        stream: u64,
    ) -> Result<Vec<(u64, u64)>, StoreError> {
        let leaf = self
            .leaves
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&template_id)
            .cloned();
        let stored = match leaf {
            Some(leaf) => match leaf.state().days.get(&day) {
                Some(part) => Arc::clone(&part.stored),
                None => return Ok(Vec::new()),
            },
            // No leaf has taken the template's row groups, and they cover what they cover all the
            // same: read as "nothing is stored", a replay would put back every line they hold.
            None => Arc::new(
                lock(&self.stored)
                    .get(&template_id)
                    .map(|all| {
                        all.iter()
                            .filter(|s| day_of(s.entry.min_time) == day)
                            .cloned()
                            .collect::<Vec<Stored>>()
                    })
                    .unwrap_or_default(),
            ),
        };
        let mut ranges = Vec::new();
        let mut reader: Option<(&Path, Reader)> = None;
        for s in stored.iter() {
            let path = s.file.path.as_path();
            let open = match &mut reader {
                Some((of, open)) if *of == path => open,
                slot => &mut slot.insert((path, Reader::open(path)?)).1,
            };
            let header = open.header(&s.entry, s.schema.len())?;
            if let Source::Wal {
                stream: of,
                first,
                last,
            } = header.source
            {
                if of == stream {
                    ranges.push((first, last));
                }
            }
        }
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (first, last) in ranges {
            match merged.last_mut() {
                Some(run) if first <= run.1.saturating_add(1) => run.1 = run.1.max(last),
                _ => merged.push((first, last)),
            }
        }
        Ok(merged)
    }

    // ── The bulk door ──────────────────────────────────────────────────────────────

    /// When a row group closes: a load closes its own at the same thresholds.
    pub fn row_group_limits(&self) -> RowGroupLimits {
        self.limits
    }

    /// Where a load starts its next file.
    pub fn segment_max_bytes(&self) -> u64 {
        self.segment_max_bytes
    }

    /// The memory a load's batches may hold before the largest close early.
    pub fn max_buffer_bytes(&self) -> u64 {
        self.max_buffer_bytes
    }

    /// Whether the manifest names this load as the source of a commit.
    pub fn was_loaded(&self, load_id: u64) -> bool {
        lock(&self.files).loads.contains(&load_id)
    }

    /// An empty `staging/<load id>/`, durable before a file is written into it; refused while
    /// any committed load is unmoved, whose rows are in `staging/` and nowhere else.
    pub fn stage(&self, load_id: u64) -> Result<PathBuf, StoreError> {
        let io = |path: &Path, source| {
            StoreError::from(FormatError::Io {
                path: path.to_path_buf(),
                source,
            })
        };
        let files = lock(&self.files);
        if let Some(pending) = files.unmoved.iter().next() {
            return Err(StoreError::MovePending { load_id: *pending });
        }
        let staging = self.base.join(STAGING_DIR);
        let dir = staging.join(format!("{load_id:016x}"));
        if dir.exists() {
            if files.loads.contains(&load_id) {
                return Err(StoreError::MovePending { load_id });
            }
            fs::remove_dir_all(&dir).map_err(|e| io(&dir, e))?;
        }
        drop(files);
        fs::create_dir_all(&dir).map_err(|e| io(&dir, e))?;
        crate::engine::format::frame::sync_dir(&staging)
            .and_then(|()| crate::engine::format::frame::sync_dir(&self.base))
            .map_err(|e| io(&staging, e))?;
        Ok(dir)
    }

    /// Remove a load's staging directory: what a load that will not commit leaves behind.
    pub fn unstage(&self, load_id: u64) {
        let _ = fs::remove_dir_all(self.base.join(STAGING_DIR).join(format!("{load_id:016x}")));
    }

    /// Publish a load's staged files: one manifest record, which is the commit, then the
    /// move, then the rows.
    /// Numbered here, under the flush's lock, so a load and the live door can never want one name.
    pub fn commit_load(&self, commit: LoadCommit) -> Result<Committed, StoreError> {
        let mut files = lock(&self.files);
        if let Some(pending) = files.unmoved.iter().next() {
            return Err(StoreError::MovePending { load_id: *pending });
        }
        let stream = self.stream.load(Ordering::Relaxed);
        // After the flush's lock, as nothing takes the two the other way round: the
        // applier takes no lock of the files'.
        let _still = (commit.replace_day.is_some() && stream != 0)
            .then(|| self.applying.write().unwrap_or_else(|e| e.into_inner()));
        let mark = commit.replace_day.filter(|_| stream != 0).map(|day| {
            manifest::WalMark::new(
                stream,
                day_name(day),
                self.wal_applied.load(Ordering::Relaxed),
            )
        });
        if commit.replace_day.is_none() && files.loads.contains(&commit.load_id) {
            return Err(StoreError::LoadRepeated {
                load_id: commit.load_id,
            });
        }
        let mut retired = Vec::new();
        if let Some(day) = commit.replace_day {
            // The loader checks this too; a file of another day published as this day's
            // replacement would add to that day, and nothing later would say so.
            if let Some(other) = commit.files.iter().find(|f| f.day != day) {
                return Err(StoreError::ReplaceRefused {
                    day: day_name(day),
                    path: other.path.display().to_string(),
                    why: format!("it holds rows of {}", day_name(other.day)),
                });
            }
            let flushed = self.flush_locked(&mut files, FlushKind::Final, Some(day));
            if let Some((_, e)) = flushed.failed.into_iter().next() {
                return Err(e);
            }
            if let Some(e) = flushed.seal_failed.into_iter().next() {
                return Err(e);
            }
            retired = files.day_files(day)?;
        }

        let mut named = Vec::with_capacity(commit.files.len());
        for staged in &commit.files {
            let next = files.next.entry(staged.day).or_insert(1);
            named.push(format!("{}/{}", day_name(staged.day), file_name(*next)));
            *next += 1;
        }
        let added: Vec<FileRecord> = named
            .iter()
            .zip(&commit.files)
            .map(|(rel, staged)| FileRecord::sealed(rel.clone(), &staged.index))
            .collect();
        let source = Some(manifest::load_source(commit.load_id));
        let recorded = files.record(vec![match commit.replace_day {
            Some(_) => Record::Replace {
                add: added,
                remove: retired.clone(),
                source,
                wal: mark.clone(),
            },
            None => Record::Add {
                files: added,
                source,
            },
        }]);
        match recorded {
            Ok(()) => {}
            // Refused before a byte was written: nothing is committed.
            Err(e @ manifest::ManifestError::Serialize { .. })
            | Err(e @ manifest::ManifestError::Log(crate::engine::format::frame::LogError::Torn { .. })) => {
                return Err(e.into())
            }
            // A write or a flush failed partway: the record may be on disk whole, and then
            // it is the commit. Only the next start can tell, so the staged files stay.
            Err(source) => {
                files.unmoved.insert(commit.load_id);
                return Err(StoreError::CommitUncertain {
                    load_id: commit.load_id,
                    source,
                });
            }
        }
        files.replaced.extend(mark);

        // Committed. What fails from here is a move the next start finishes.
        for (rel, staged) in named.iter().zip(&commit.files) {
            if let Err(source) = move_into(&staged.path, &files.data, rel) {
                files.unmoved.insert(commit.load_id);
                return Err(StoreError::NotMoved {
                    load_id: commit.load_id,
                    source,
                });
            }
        }
        self.unstage(commit.load_id);
        self.publish_load(&files.data, &named, &commit.files, &retired);
        Ok(Committed {
            added: named,
            replaced: retired,
        })
    }

    /// A committed load's row groups into the leaves, or `stored`, and the retired files' out.
    /// One write hold of `publish`, so a query counts all of a load or none of it: see `Snapshot`.
    fn publish_load(
        &self,
        data: &Path,
        named: &[String],
        staged: &[StagedFile],
        retired: &[String],
    ) {
        let gone: BTreeSet<PathBuf> = retired.iter().map(|rel| data.join(rel)).collect();
        let mut new: BTreeMap<u64, Vec<Stored>> = BTreeMap::new();
        for (rel, f) in named.iter().zip(staged) {
            index_rows(&mut new, Arc::new(DataFile::new(data.join(rel))), &f.index);
        }
        let mut taken: Vec<Arc<DataFile>> = Vec::new();
        {
            let _publishing = self.publish.write().unwrap_or_else(|e| e.into_inner());
            let leaves = self.leaves.write().unwrap_or_else(|e| e.into_inner());
            self.generation.fetch_add(1, Ordering::Relaxed);
            for leaf in leaves.values() {
                let rows = new.remove(&leaf.template_id).unwrap_or_default();
                if !gone.is_empty() || !rows.is_empty() {
                    taken.extend(leaf.swap(&gone, rows));
                }
            }
            let mut stored = lock(&self.stored);
            if !gone.is_empty() {
                for list in stored.values_mut() {
                    list.retain(|s| {
                        let keep = !gone.contains(&s.file.path);
                        if !keep {
                            taken.push(Arc::clone(&s.file));
                        }
                        keep
                    });
                }
            }
            for (id, rows) in new {
                stored.entry(id).or_default().extend(rows);
            }
        }
        let mut marked = BTreeSet::new();
        for file in taken {
            file.retire();
            marked.insert(file.path.clone());
        }
        // A file no row group named - one that held no rows - has no reader to wait for.
        for path in gone.difference(&marked) {
            let _ = fs::remove_file(path);
        }
    }

    /// One flush, in the three steps the invariant at `DiskLeaf::state` needs, then the seals;
    /// with `only`, one day's rows and then its file, as replace-a-day does first.
    fn flush_locked(&self, files: &mut Files, kind: FlushKind, only: Option<i64>) -> Flushed {
        files.flushes += 1;
        let leaves: Vec<Arc<DiskLeaf>> = self
            .leaves
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        let now = Instant::now();
        let today = today();
        let mut done = Flushed::default();

        // ── 1. Move the rows that are due aside: one short hold of each leaf's lock ──
        let mut moved: Vec<Moved> = Vec::new();
        let relieving = !kind.is_final() && only.is_none();
        let whole = relieving && self.buffered.load(Ordering::Relaxed) > self.max_buffer_bytes;
        for leaf in &leaves {
            leaf.move_due(kind, now, only, whole, &mut moved);
        }
        if relieving {
            done.forced = self.relieve(&leaves, &mut moved);
        }

        // ── 2. Encode, write and flush them, holding no leaf's lock ──
        //
        // Encoded side by side first, which is nearly all of the cost and needs no file, then
        // appended one at a time, day by day.
        let jobs = jobs_of(&moved, &self.limits);
        let stream = self.stream.load(Ordering::Relaxed);
        let mut encoded = encode_all(&moved, &jobs, self.encoders, stream).into_iter();
        let mut per_batch: Vec<Vec<EncodedRowGroup>> = Vec::with_capacity(moved.len());
        let mut first_job = 0;
        for at in 0..moved.len() {
            let count = jobs[first_job..]
                .iter()
                .take_while(|j| j.moved == at)
                .count();
            first_job += count;
            per_batch.push(encoded.by_ref().take(count).collect());
        }
        let mut written: Vec<Result<Vec<Stored>, StoreError>> =
            (0..moved.len()).map(|_| Ok(Vec::new())).collect();
        let mut by_file: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
        let mut seal_failed = Vec::new();
        // Days with a file open first: by the time a day needs room every open file this flush
        // writes to has been written, so the one sealed for it is never one a later batch wants.
        let mut order: Vec<usize> = (0..moved.len()).collect();
        order.sort_by_key(|at| {
            let day = moved[*at].day;
            (!files.open.contains_key(&day), day)
        });
        let mut wanted: BTreeMap<i64, usize> = BTreeMap::new();
        for m in &moved {
            *wanted.entry(m.day).or_default() += 1;
        }
        for at in order {
            let day = moved[at].day;
            if let Some(left) = wanted.get_mut(&day) {
                *left -= 1;
                if *left == 0 {
                    wanted.remove(&day);
                }
            }
            // At the cap, the least recently written file is sealed and the row's own day opened,
            // never another day's file: a day split across two directories cannot be replaced.
            if files.full_for(day) {
                if let Some(victim) = files.least_recent(&wanted) {
                    // Written by this flush and not yet flushed: flushed first, so nothing
                    // sealed holds row groups a failed flush would still cut back.
                    if let Some(ats) = by_file.remove(&victim) {
                        self.sync_into(files, victim, ats, &moved, &mut written, &mut done);
                    }
                    let (sealed, failed) = files.seal_days(vec![victim]);
                    done.sealed += sealed;
                    seal_failed.extend(failed);
                }
            }
            written[at] = files.write(&moved[at], std::mem::take(&mut per_batch[at]));
            if written[at].is_ok() {
                by_file.entry(day).or_default().push(at);
            }
        }
        for (dir, ats) in by_file {
            self.sync_into(files, dir, ats, &moved, &mut written, &mut done);
        }

        // ── 3. Publish each row group and drop the rows it holds: one more short hold ──
        for (m, result) in moved.into_iter().zip(written) {
            match result {
                Ok(stored) => {
                    done.rows += m.batch.rows.len() as u64;
                    done.partitions += 1;
                    done.row_groups += stored.len();
                    done.bytes += stored
                        .iter()
                        .map(|s| u64::from(s.entry.frame_len) + u64::from(s.entry.chunks_len))
                        .sum::<u64>();
                    files.last_live.insert(m.day, today);
                    if let Some(through) = m.batch.rows.iter().map(|b| b.pos).max() {
                        if through > 0 {
                            let seen = files.wal_through.entry(m.day).or_insert(through);
                            *seen = (*seen).max(through);
                        }
                    }
                    m.leaf.publish(m.day, stored);
                }
                Err(e) => {
                    // Back where they were, ahead of anything that arrived since: the next
                    // flush tries them again, and until then queries read them there.
                    m.leaf.restore(m.day);
                    done.failed.push((
                        m.leaf.template_id,
                        StoreError::Leaf {
                            template_id: m.leaf.template_id,
                            source: Box::new(e),
                        },
                    ));
                }
            }
        }

        // ── 4. Seal: every file on a final flush, and otherwise those past the size cap ──
        let (sealed, failed) = match only {
            Some(day) => files.seal_days(vec![day]),
            None if kind.is_final() => files.seal_all(),
            None => files.settle(),
        };
        done.sealed += sealed;
        seal_failed.extend(failed);
        done.seal_failed = seal_failed;
        self.open_count.store(files.open.len(), Ordering::Relaxed);
        done
    }

    /// Flush one file this flush wrote to, and if that fails, fail every batch written to
    /// it: the file was cut back, so their rows go back to their buffers.
    fn sync_into(
        &self,
        files: &mut Files,
        dir: i64,
        ats: Vec<usize>,
        moved: &[Moved],
        written: &mut [Result<Vec<Stored>, StoreError>],
        done: &mut Flushed,
    ) {
        match files.sync(dir) {
            Ok(()) => done.syncs += 1,
            Err(e) => {
                let why = e.to_string();
                let mut first = Some(e);
                for at in ats {
                    written[at] = Err(first.take().unwrap_or_else(|| StoreError::Io {
                        template_id: moved[at].leaf.template_id,
                        source: std::io::Error::other(why.clone()),
                    }));
                }
            }
        }
    }

    /// Past the memory limit, move aside the largest buffers too, until three quarters of the
    /// limit stays: stopping a hair under would force more small row groups on every tick.
    fn relieve(&self, leaves: &[Arc<DiskLeaf>], moved: &mut Vec<Moved>) -> usize {
        let moving: u64 = moved.iter().map(|m| m.batch.held as u64).sum();
        let mut staying = self.buffered.load(Ordering::Relaxed).saturating_sub(moving);
        if staying <= self.max_buffer_bytes {
            return 0;
        }
        let target = self.max_buffer_bytes / 4 * 3;
        let mut sizes: Vec<(usize, usize, i64)> = Vec::new();
        for (at, leaf) in leaves.iter().enumerate() {
            for (day, bytes) in leaf.buffer_sizes() {
                sizes.push((bytes, at, day));
            }
        }
        sizes.sort_unstable_by_key(|(bytes, _, _)| std::cmp::Reverse(*bytes));
        let mut forced = 0;
        for (_, at, day) in sizes {
            if staying <= target {
                break;
            }
            if let Some(m) = leaves[at].move_day(day) {
                staying = staying.saturating_sub(m.batch.held as u64);
                moved.push(m);
                forced += 1;
            }
        }
        forced
    }
}

/// Move a sealed file into `data/<rel>` and make the move durable, a new day directory
/// synced with its parent first so the next start finds it.
fn move_into(from: &Path, data: &Path, rel: &str) -> Result<(), FormatError> {
    let io = |path: &Path, source| FormatError::Io {
        path: path.to_path_buf(),
        source,
    };
    let to = data.join(rel);
    let dir = crate::engine::format::frame::parent_of(&to);
    if !dir.exists() {
        fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        crate::engine::format::frame::sync_dir(data).map_err(|e| io(data, e))?;
    }
    fs::rename(from, &to).map_err(|e| io(from, e))?;
    crate::engine::format::frame::sync_dir(dir).map_err(|e| io(dir, e))
}

/// Every regular file under `dir`, at any depth, in path order; none when `dir` is absent.
fn files_under(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut todo = vec![dir.to_path_buf()];
    while let Some(at) = todo.pop() {
        let entries = match fs::read_dir(&at) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && at == dir => break,
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                todo.push(entry.path());
            } else if kind.is_file() {
                out.push(entry.path());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Files a load committed and a stop left in `staging/<load id>/`, moved into place and
/// matched by header id. Fails closed: staging may hold the only copy of a load's rows.
fn finish_moves(
    staging: &Path,
    data: &Path,
    live: &BTreeMap<String, FileRecord>,
    published: &BTreeMap<String, u64>,
) -> Result<(), StoreError> {
    let io = |path: &Path, source| {
        StoreError::from(FormatError::Io {
            path: path.to_path_buf(),
            source,
        })
    };
    let mut loads: BTreeMap<u64, Vec<(&String, &FileRecord)>> = BTreeMap::new();
    for (rel, record) in live {
        if let Some(id) = published.get(rel) {
            loads.entry(*id).or_default().push((rel, record));
        }
    }
    for (id, named) in loads {
        let dir = staging.join(format!("{id:016x}"));
        if !dir.exists() {
            continue;
        }
        // Only a file not already in place is looked for: `staging/<id>/` can also hold a later
        // run of the same inputs, whose files no record names and may be cut short.
        let mut wanted = Vec::new();
        for (rel, record) in named {
            let at = data.join(rel);
            let there = named_is_there(&at)?;
            if !(there && manifest::hex(&file::file_id_of(&at)?) == record.file_id) {
                wanted.push((rel, record, at));
            }
        }
        if wanted.is_empty() {
            continue;
        }
        let mut by_id: HashMap<String, PathBuf> = HashMap::new();
        let mut unreadable = Vec::new();
        for path in files_under(&dir).map_err(|e| io(&dir, e))? {
            match file::file_id_of(&path) {
                Ok(file_id) => {
                    by_id.insert(manifest::hex(&file_id), path);
                }
                Err(e @ FormatError::PreRename { .. }) => return Err(e.into()),
                Err(e) => unreadable.push(e.to_string()),
            }
        }
        let mut moves = Vec::new();
        for (rel, record, at) in wanted {
            match by_id.get(&record.file_id) {
                Some(from) => moves.push((from, rel)),
                None => {
                    return Err(io(
                        &at,
                        std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!(
                                "named by manifest.log as part of load {id:016x}, and neither \
                                 here with its id nor in {}{}; staging is kept",
                                dir.display(),
                                if unreadable.is_empty() {
                                    String::new()
                                } else {
                                    format!(" (unreadable there: {})", unreadable.join("; "))
                                }
                            ),
                        ),
                    ))
                }
            }
        }
        for (from, rel) in &moves {
            move_into(from, data, rel)?;
        }
        if !moves.is_empty() {
            warn(format!(
                "load {id:016x} was committed and stopped before its files were moved; moved \
                 {} file(s) out of {}",
                moves.len(),
                dir.display()
            ));
        }
    }
    Ok(())
}

/// Every file under `data/` the manifest does not name, deleted, and every directory
/// left empty: a file nobody named is not part of the database.
fn delete_unnamed(data: &Path, live: &BTreeMap<String, FileRecord>) -> Result<(), StoreError> {
    let io = |path: &Path, source| FormatError::Io {
        path: path.to_path_buf(),
        source,
    };
    for entry in fs::read_dir(data).map_err(|e| io(data, e))?.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            warn(format!(
                "{}: not named by manifest.log; deleted",
                path.display()
            ));
            fs::remove_file(&path).map_err(|e| io(&path, e))?;
            continue;
        }
        let mut kept = 0;
        for inner in fs::read_dir(&path).map_err(|e| io(&path, e))?.flatten() {
            let file = inner.path();
            let rel = format!("{name}/{}", inner.file_name().to_string_lossy());
            if live.contains_key(&rel) && inner.file_type().is_ok_and(|t| t.is_file()) {
                kept += 1;
                continue;
            }
            warn(format!(
                "{}: not named by manifest.log; deleted",
                file.display()
            ));
            if inner.file_type().is_ok_and(|t| t.is_dir()) {
                fs::remove_dir_all(&file).map_err(|e| io(&file, e))?;
            } else {
                fs::remove_file(&file).map_err(|e| io(&file, e))?;
            }
        }
        if kept == 0 {
            fs::remove_dir(&path).map_err(|e| io(&path, e))?;
        }
    }
    Ok(())
}

/// A file's row groups into the per-template lists, in file order.
fn index_rows(stored: &mut BTreeMap<u64, Vec<Stored>>, file: Arc<DataFile>, index: &Index) {
    let schemas: Vec<Arc<Vec<String>>> = index.schemas.iter().cloned().map(Arc::new).collect();
    for entry in &index.entries {
        stored.entry(entry.template_id).or_default().push(Stored {
            file: Arc::clone(&file),
            entry: *entry,
            schema: Arc::clone(&schemas[entry.schema_id as usize]),
        });
    }
}

impl Store for DiskStore {
    fn open(&self, template_id: u64, time_column: &str) -> Result<Arc<dyn LeafStore>, StoreError> {
        // Held from taking the row groups to joining the list, so a load committing meanwhile
        // finds this template in `stored` or in the leaf, never between the two.
        let mut leaves = self.leaves.write().unwrap_or_else(|e| e.into_inner());
        let stored = lock(&self.stored).remove(&template_id).unwrap_or_default();
        let mut days: BTreeMap<i64, Day> = BTreeMap::new();
        for s in stored {
            days.entry(day_of(s.entry.min_time)).or_default().note(s);
        }
        let leaf = Arc::new(DiskLeaf {
            template_id,
            time_column: time_column.to_string(),
            limits: self.limits,
            held: Arc::clone(&self.buffered),
            applied: Arc::clone(&self.wal_applied),
            fan: self.fan.clone(),
            state: Mutex::new(LeafState { days, buffered: 0 }),
        });
        leaves.insert(template_id, Arc::clone(&leaf));
        Ok(leaf)
    }

    fn flush(&self, kind: FlushKind) -> Flushed {
        let mut files = lock(&self.files);
        self.flush_locked(&mut files, kind, None)
    }

    fn as_disk(&self) -> Option<&DiskStore> {
        Some(self)
    }

    fn fan(&self) -> Option<Arc<Fan>> {
        self.fan.clone()
    }

    /// One short hold of each leaf's lock. A buffer is in position order, so a day's oldest
    /// is the first row a flush moved aside, or the buffer's first.
    fn oldest_position(&self) -> Option<u64> {
        let leaves: Vec<Arc<DiskLeaf>> = self
            .leaves
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        leaves
            .iter()
            .filter_map(|leaf| leaf.oldest_position())
            .min()
    }

    fn kind(&self) -> &'static str {
        "disk"
    }

    fn base_path(&self) -> Option<&Path> {
        Some(&self.base)
    }

    fn open_files(&self) -> (usize, usize) {
        (self.open_count.load(Ordering::Relaxed), self.max_open)
    }

    fn claimed(&self) -> Vec<u64> {
        self.claimed.clone()
    }

    fn buffer_bytes(&self) -> (u64, u64) {
        (self.buffered.load(Ordering::Relaxed), self.max_buffer_bytes)
    }

    fn generation(&self) -> u64 {
        let _publishing = self.publish.read().unwrap_or_else(|e| e.into_inner());
        self.generation.load(Ordering::Relaxed)
    }

    /// Every planned template's leaf, frozen in one read hold of `publish`, so the snapshot
    /// is wholly before a commit or wholly after it.
    fn snapshot(
        &self,
        leaves: Vec<(u64, Arc<dyn LeafStore>)>,
        opts: &ScanOptions,
    ) -> Box<dyn Snapshot> {
        let since = opts.since.map(nanos_of);
        let until = opts.until.map(nanos_of);
        let _publishing = self.publish.read().unwrap_or_else(|e| e.into_inner());
        let mine: Vec<(u64, Arc<DiskLeaf>)> = {
            let held = self.leaves.read().unwrap_or_else(|e| e.into_inner());
            leaves
                .into_iter()
                .filter_map(|(id, _)| held.get(&id).map(|leaf| (id, Arc::clone(leaf))))
                .collect()
        };
        let frozen = mine
            .into_iter()
            .map(|(id, leaf)| (id, leaf.freeze(since, until)))
            .collect();
        Box::new(DiskSnapshot {
            generation: self.generation.load(Ordering::Relaxed),
            leaves: frozen,
        })
    }
}

/// A query's snapshot of the disk store: see `Snapshot`.
struct DiskSnapshot {
    generation: u64,
    leaves: BTreeMap<u64, Frozen>,
}

impl Snapshot for DiskSnapshot {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn visit(
        &mut self,
        template_id: u64,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Option<Result<Scan, StoreError>> {
        self.leaves
            .remove(&template_id)
            .map(|frozen| frozen.visit(opts, terms, v))
    }
}

// ── One leaf ───────────────────────────────────────────────────────────────────────

struct Buffered {
    row: Row,
    /// Where the row's line is in the write-ahead log; 0 for a row that came by no log.
    pos: u64,
    time: i64,
    /// Payload: key text, string values, a scalar's width. What the 8MB row group
    /// threshold and `/stats` count.
    bytes: usize,
    /// Memory: what the buffer limit counts. See `Row::held_bytes`.
    held: usize,
}

/// Rows a flush moved out of a buffer: shared by the leaf, which still answers queries from
/// them, and the flush writing them.
struct Batch {
    rows: Vec<Arc<Buffered>>,
    bytes: usize,
    held: usize,
    span: Option<(i64, i64)>,
    /// The row groups the rows were cut into while buffered, for `restore` to put back.
    groups: Vec<Group>,
}

/// One row group a buffer is filling. Cut as `jobs_of` cuts, so the groups a tick counts
/// as full are the row groups the flush then writes.
#[derive(Clone, Copy)]
struct Group {
    start: usize,
    /// When the row group's oldest row arrived: what the timeout is measured from once the
    /// rows before it are written. A clock read per row group, not per row.
    opened_at: Instant,
    bytes: usize,
    held: usize,
    span: Option<(i64, i64)>,
}

/// A batch on its way to a file: whose, and which day.
struct Moved {
    leaf: Arc<DiskLeaf>,
    day: i64,
    batch: Arc<Batch>,
}

fn widen(span: &mut Option<(i64, i64)>, lo: i64, hi: i64) {
    *span = Some(span.map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))));
}

fn union(a: Option<(i64, i64)>, b: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (a, b) {
        (Some((a0, a1)), Some((b0, b1))) => Some((a0.min(b0), a1.max(b1))),
        (a, b) => a.or(b),
    }
}

/// One template's rows of one UTC day: its row groups in storage order, the rows a flush
/// has moved aside, and the rows not yet in either.
#[derive(Default)]
struct Day {
    /// Row groups in storage order. The open file's are its index, published here as each
    /// closes because it has no footer on disk; behind an `Arc`, so a snapshot is a count.
    stored: Arc<Vec<Stored>>,
    stored_rows: u64,
    stored_bytes: u64,
    stored_span: Option<(i64, i64)>,
    /// Rows a flush has moved out of the buffer and is writing: answered from here until
    /// their row groups are published.
    closing: Option<Arc<Batch>>,
    /// Grows on demand and never starts sized: sized for a full row group per template it
    /// costs far more memory and ingest rate.
    buffer: Vec<Arc<Buffered>>,
    buffer_bytes: usize,
    buffer_held: usize,
    buffer_span: Option<(i64, i64)>,
    /// The row groups the buffer is cut into, oldest first. Read from a clock but never
    /// driven by one: whoever owns the tick asks.
    groups: Vec<Group>,
}

impl Group {
    /// Whether the row group is full with the buffer `end` rows long: `jobs_of` would end
    /// it here rather than add the next row.
    fn is_full(&self, end: usize, limits: &RowGroupLimits) -> bool {
        end - self.start >= limits.rows || self.bytes >= limits.bytes
    }
}

impl Day {
    /// A row group found at startup, in storage order.
    fn note(&mut self, s: Stored) {
        self.stored_rows += u64::from(s.entry.row_count);
        self.stored_bytes += u64::from(s.entry.frame_len) + u64::from(s.entry.chunks_len);
        widen(&mut self.stored_span, s.entry.min_time, s.entry.max_time);
        Arc::make_mut(&mut self.stored).push(s);
    }

    fn in_memory(&self) -> u64 {
        self.closing.as_ref().map_or(0, |b| b.rows.len() as u64) + self.buffer.len() as u64
    }

    fn span(&self) -> Option<(i64, i64)> {
        union(
            union(self.stored_span, self.closing.as_ref().and_then(|b| b.span)),
            self.buffer_span,
        )
    }

    /// Buffer one row: into the last row group, or the first of a new one if that is full.
    /// `arrived` is asked only when a row group opens.
    fn push(
        &mut self,
        row: Arc<Buffered>,
        limits: &RowGroupLimits,
        arrived: impl FnOnce() -> Instant,
    ) {
        let at = self.buffer.len();
        if self.groups.last().is_none_or(|g| g.is_full(at, limits)) {
            self.groups.push(Group {
                start: at,
                opened_at: arrived(),
                bytes: 0,
                held: 0,
                span: None,
            });
        }
        if let Some(g) = self.groups.last_mut() {
            g.bytes += row.bytes;
            g.held += row.held;
            widen(&mut g.span, row.time, row.time);
        }
        self.buffer_bytes += row.bytes;
        self.buffer_held += row.held;
        widen(&mut self.buffer_span, row.time, row.time);
        self.buffer.push(row);
    }

    /// How many of the buffer's row groups are full: all but the last, and the last too
    /// once it reaches a threshold.
    fn full_groups(&self, limits: &RowGroupLimits) -> usize {
        match self.groups.last() {
            Some(last) if !last.is_full(self.buffer.len(), limits) => self.groups.len() - 1,
            _ => self.groups.len(),
        }
    }

    /// Hand the buffer's first `groups` row groups to a flush and keep the rest buffered.
    /// The rows moved stay readable here, as `closing`, until the flush publishes them.
    fn move_aside(&mut self, groups: usize) -> Arc<Batch> {
        let rest = self.groups.split_off(groups);
        let moved = std::mem::replace(&mut self.groups, rest);
        let rows = match self.groups.first() {
            Some(g) => {
                let rest = self.buffer.split_off(g.start);
                std::mem::replace(&mut self.buffer, rest)
            }
            // `take` leaves a `Vec` with no capacity at all: the next buffer grows from
            // nothing, as the first did.
            None => std::mem::take(&mut self.buffer),
        };
        let bytes: usize = moved.iter().map(|g| g.bytes).sum();
        let held: usize = moved.iter().map(|g| g.held).sum();
        let span = moved.iter().fold(None, |s, g| union(s, g.span));
        let cut = rows.len();
        self.buffer_bytes -= bytes;
        self.buffer_held -= held;
        self.buffer_span = self.groups.iter().fold(None, |s, g| union(s, g.span));
        for g in &mut self.groups {
            g.start -= cut;
        }
        let batch = Arc::new(Batch {
            rows,
            bytes,
            held,
            span,
            groups: moved,
        });
        self.closing = Some(Arc::clone(&batch));
        batch
    }

    /// Everything a query reads of this day. `day` is the day itself: the key this is
    /// filed under, which it does not hold.
    fn view(&self, day: i64) -> View {
        View {
            day,
            stored: Arc::clone(&self.stored),
            closing: self.closing.clone(),
            buffer: self.buffer.clone(),
            rows: self.stored_rows + self.in_memory(),
            span: self.span(),
        }
    }
}

/// A day as one query found it: copied under the leaf's lock, read with it released.
struct View {
    /// The UTC day these rows are kept under: the day each one's time was in when it was
    /// stored. A scan measures the time it reads off a row against it.
    day: i64,
    stored: Arc<Vec<Stored>>,
    closing: Option<Arc<Batch>>,
    buffer: Vec<Arc<Buffered>>,
    rows: u64,
    span: Option<(i64, i64)>,
}

#[derive(Default)]
struct LeafState {
    days: BTreeMap<i64, Day>,
    /// Rows in memory: in a buffer, or moved aside by a flush that has not published them.
    buffered: u64,
}

pub(crate) struct DiskLeaf {
    template_id: u64,
    /// The template's time column: what a row's day, and its row group's time range, are
    /// read from, falling back to the arrival time a row without one carries.
    time_column: String,
    limits: RowGroupLimits,
    /// The store's count of memory held by buffered rows, across every leaf.
    held: Arc<AtomicU64>,
    /// The store's `wal_applied`, raised by every positioned row a leaf takes.
    applied: Arc<AtomicU64>,
    /// The store's scan threads, if it has any.
    fan: Option<Arc<Fan>>,
    state: Mutex<LeafState>,
}

impl DiskLeaf {
    /// The leaf's lock. Whenever it is free, every row is in exactly one of `Day::stored`,
    /// `Day::closing` or the buffer; nothing is done under it but moving rows between them.
    fn state(&self) -> MutexGuard<'_, LeafState> {
        lock(&self.state)
    }

    /// How many of a day's row groups are due to close, if any. A size threshold closes only
    /// the full ones; a final flush, the timeout or `whole` (the memory limit) closes them all.
    fn due(&self, part: &Day, kind: FlushKind, now: Instant, whole: bool) -> Option<usize> {
        if part.closing.is_some() || part.buffer.is_empty() {
            return None;
        }
        let timed_out = part
            .groups
            .first()
            .is_some_and(|g| now.saturating_duration_since(g.opened_at) >= self.limits.age);
        if kind.is_final() || timed_out {
            return Some(part.groups.len());
        }
        match part.full_groups(&self.limits) {
            0 => None,
            _ if whole => Some(part.groups.len()),
            full => Some(full),
        }
    }

    /// Move aside every row group a threshold says is due, of every day or of `only`: every
    /// buffer whole, on a final flush. `whole`: see `due`.
    fn move_due(
        self: &Arc<Self>,
        kind: FlushKind,
        now: Instant,
        only: Option<i64>,
        whole: bool,
        out: &mut Vec<Moved>,
    ) {
        let mut state = self.state();
        for (day, part) in state.days.iter_mut() {
            if only.is_some_and(|d| d != *day) {
                continue;
            }
            if let Some(groups) = self.due(part, kind, now, whole) {
                out.push(Moved {
                    leaf: Arc::clone(self),
                    day: *day,
                    batch: part.move_aside(groups),
                });
            }
        }
    }

    /// Move aside one day's buffer, due or not: what the memory limit closes.
    fn move_day(self: &Arc<Self>, day: i64) -> Option<Moved> {
        let mut state = self.state();
        let part = state.days.get_mut(&day)?;
        if part.closing.is_some() || part.buffer.is_empty() {
            return None;
        }
        let groups = part.groups.len();
        Some(Moved {
            leaf: Arc::clone(self),
            day,
            batch: part.move_aside(groups),
        })
    }

    /// The lowest log position among this leaf's rows that are not in a row group yet.
    fn oldest_position(&self) -> Option<u64> {
        self.state()
            .days
            .values()
            .filter_map(|part| {
                let moved = part.closing.as_ref().and_then(|batch| batch.rows.first());
                moved.or(part.buffer.first()).map(|b| b.pos)
            })
            .filter(|pos| *pos > 0)
            .min()
    }

    /// The days this leaf holds a row of that is still only in memory.
    fn days_in_memory(&self) -> Vec<i64> {
        self.state()
            .days
            .iter()
            .filter(|(_, part)| part.in_memory() > 0)
            .map(|(day, _)| *day)
            .collect()
    }

    /// The memory each day's buffer holds, for the limit to pick the largest from.
    fn buffer_sizes(&self) -> Vec<(i64, usize)> {
        self.state()
            .days
            .iter()
            .filter(|(_, part)| part.closing.is_none() && !part.buffer.is_empty())
            .map(|(day, part)| (*day, part.buffer_held))
            .collect()
    }

    /// The flush wrote a day's moved rows as `stored`: publish those row groups and drop the
    /// rows, in one hold of the lock.
    fn publish(&self, day: i64, stored: Vec<Stored>) {
        let freed = {
            let mut state = self.state();
            let Some(part) = state.days.get_mut(&day) else {
                return;
            };
            let Some(batch) = part.closing.take() else {
                return;
            };
            for s in &stored {
                part.stored_rows += u64::from(s.entry.row_count);
                part.stored_bytes += u64::from(s.entry.frame_len) + u64::from(s.entry.chunks_len);
                widen(&mut part.stored_span, s.entry.min_time, s.entry.max_time);
            }
            // A query holding the old list keeps it; this copies it only then.
            Arc::make_mut(&mut part.stored).extend(stored);
            state.buffered -= batch.rows.len() as u64;
            batch.held
        };
        self.held.fetch_sub(freed as u64, Ordering::Relaxed);
    }

    /// A committed load's row groups into their days, and every row group of the files at
    /// `gone` out, in one hold of the lock; appended after everything stored in their day.
    fn swap(&self, gone: &BTreeSet<PathBuf>, stored: Vec<Stored>) -> Vec<Arc<DataFile>> {
        let mut out = Vec::new();
        let mut state = self.state();
        if !gone.is_empty() {
            for part in state.days.values_mut() {
                if !part.stored.iter().any(|s| gone.contains(&s.file.path)) {
                    continue;
                }
                let (left, taken): (Vec<Stored>, Vec<Stored>) = part
                    .stored
                    .iter()
                    .cloned()
                    .partition(|s| !gone.contains(&s.file.path));
                out.extend(taken.into_iter().map(|s| s.file));
                // A query holding the old list keeps it; this leaf starts a new one.
                part.stored = Arc::new(Vec::new());
                part.stored_rows = 0;
                part.stored_bytes = 0;
                part.stored_span = None;
                for s in left {
                    part.note(s);
                }
            }
        }
        for s in stored {
            state
                .days
                .entry(day_of(s.entry.min_time))
                .or_default()
                .note(s);
        }
        out
    }

    /// A compaction's row groups into `day` by path, where the files they replace were, so
    /// no position in the day moves; the files at `gone` go out in the same hold.
    fn replace(&self, day: i64, gone: &BTreeSet<PathBuf>, stored: Vec<Stored>) {
        let mut state = self.state();
        let held = state
            .days
            .get(&day)
            .is_some_and(|part| part.stored.iter().any(|s| gone.contains(&s.file.path)));
        if !held && stored.is_empty() {
            return;
        }
        let part = state.days.entry(day).or_default();
        let list = spliced(&part.stored, gone, stored);
        // A query holding the old list keeps it; this leaf starts a new one.
        part.stored = Arc::new(Vec::new());
        part.stored_rows = 0;
        part.stored_bytes = 0;
        part.stored_span = None;
        for s in list {
            part.note(s);
        }
    }

    /// The flush could not write a day's moved rows: put them back at the head of the
    /// buffer, so every row's position is what it would have been had they never moved.
    fn restore(&self, day: i64) {
        let mut state = self.state();
        let Some(part) = state.days.get_mut(&day) else {
            return;
        };
        let Some(batch) = part.closing.take() else {
            return;
        };
        // Cut into row groups again from the head. Each row group opens when the one its first
        // row was in did before - never later than that row arrived.
        let marks: Vec<(usize, Instant)> = batch
            .groups
            .iter()
            .map(|g| (g.start, g.opened_at))
            .chain(
                part.groups
                    .iter()
                    .map(|g| (g.start + batch.rows.len(), g.opened_at)),
            )
            .collect();
        let rows: Vec<Arc<Buffered>> = batch
            .rows
            .iter()
            .cloned()
            .chain(std::mem::take(&mut part.buffer))
            .collect();
        part.groups.clear();
        part.buffer_bytes = 0;
        part.buffer_held = 0;
        part.buffer_span = None;
        let mut mark = 0;
        for (at, row) in rows.into_iter().enumerate() {
            while marks.get(mark + 1).is_some_and(|m| m.0 <= at) {
                mark += 1;
            }
            part.push(row, &self.limits, || {
                marks.get(mark).map_or_else(Instant::now, |m| m.1)
            });
        }
    }

    /// Every day a query over `[since, until)` can use, newest first, copied in one hold of
    /// the lock as counts and pointers; the other days' row groups are counted as pruned.
    fn freeze(&self, since: Option<i64>, until: Option<i64>) -> Frozen {
        let windowed = since.is_some() || until.is_some();
        let state = self.state();
        let mut frozen = Frozen {
            fan: self.fan.clone(),
            ..Frozen::default()
        };
        for (day, part) in state.days.iter().rev() {
            if windowed && !Self::overlaps(*day, part.span(), since, until) {
                frozen.pruned += part.stored.len() as u64;
                continue;
            }
            frozen.views.push(part.view(*day));
        }
        frozen
    }

    /// How many rows this day contributes to `count`, if that can be known without reading
    /// it: no term could reject a row, and the window holds the whole day's range.
    fn countable(
        view: &View,
        terms: &[Term],
        since: Option<i64>,
        until: Option<i64>,
    ) -> Option<u64> {
        if !terms.is_empty() {
            return None;
        }
        let Some((lo, hi)) = view.span else {
            return Some(view.rows);
        };
        let contained = since.is_none_or(|from| lo >= from) && until.is_none_or(|to| hi < to);
        contained.then_some(view.rows)
    }

    fn overlaps(
        day: i64,
        span: Option<(i64, i64)>,
        since: Option<i64>,
        until: Option<i64>,
    ) -> bool {
        let start = day.saturating_mul(DAY_NS);
        let lo = span.map_or(start, |(lo, _)| lo).max(start);
        let hi = span.map_or(start.saturating_add(DAY_NS - 1), |(_, hi)| hi);
        since.is_none_or(|from| hi >= from) && until.is_none_or(|to| lo < to)
    }
}

/// One leaf as a query found it: see `DiskLeaf::freeze`. Read with no lock held.
#[derive(Default)]
struct Frozen {
    views: Vec<View>,
    pruned: u64,
    /// The store's scan threads, if it has any: a frozen leaf carries them to its scan.
    fan: Option<Arc<Fan>>,
}

impl Frozen {
    /// The frozen days' rows, newest day first. `opts` must carry the window it was frozen
    /// with: the days it ruled out were not copied.
    fn visit(
        &self,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        let since = opts.since.map(nanos_of);
        let until = opts.until.map(nanos_of);
        let mut out = Scan {
            row_groups_pruned: self.pruned,
            ..Scan::default()
        };
        // What the visitor reads of a row is asked once, here, and not of every row.
        let ask = scan::Ask::new(opts, terms, v.wants());
        let mut readers = HashMap::new();
        for view in &self.views {
            if !v.wants_partition(view.span.map(|(_, hi)| hi)) {
                // The visitor wants no row from here, but a limit bounds the answer and not the
                // count: the day is still counted, free when nothing in it could reject a row.
                if let Some(matched) = DiskLeaf::countable(view, terms, since, until) {
                    out.matched += matched;
                    continue;
                }
            }
            if !scan::day(view, &ask, v, &mut out, &mut readers, self.fan.as_deref())? {
                break;
            }
        }
        Ok(out)
    }
}

impl LeafStore for DiskLeaf {
    fn template_id(&self) -> u64 {
        self.template_id
    }

    /// The ordinal is the row's position within its template's UTC day, counted from the
    /// index so it survives a restart.
    fn append(&self, row: Row) -> u64 {
        self.append_at(row, 0)
    }

    fn append_at(&self, row: Row, pos: u64) -> u64 {
        // A row with no effective time, its own or its arrival's, has no day; it is placed at
        // the epoch rather than dropped.
        let time = effective_ts(&row, &self.time_column).unwrap_or(0);
        let bytes = row
            .iter()
            .map(|(key, value)| key.len() + value_bytes(value))
            .sum();
        // The row, the `Arc` around it and its slot in the buffer.
        let held = row.held_bytes() + std::mem::size_of::<Buffered>() + 24;
        let row = Arc::new(Buffered {
            row,
            pos,
            time,
            bytes,
            held,
        });
        let seq = {
            let mut state = self.state();
            state.buffered += 1;
            let part = state.days.entry(day_of(time)).or_default();
            let seq = part.stored_rows + part.in_memory();
            part.push(row, &self.limits, Instant::now);
            seq
        };
        self.held.fetch_add(held as u64, Ordering::Relaxed);
        if pos > 0 {
            self.applied.fetch_max(pos, Ordering::Relaxed);
        }
        seq
    }

    /// Every day this query can use, copied in one hold of the lock, then read with it
    /// released, so a long read holds up no append and no flush.
    fn visit(
        &self,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        self.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of))
            .visit(opts, terms, v)
    }

    fn len(&self) -> u64 {
        let state = self.state();
        state.days.values().map(|p| p.stored_rows).sum::<u64>() + state.buffered
    }

    fn buffered(&self) -> u64 {
        self.state().buffered
    }

    fn approx_bytes(&self) -> u64 {
        self.state()
            .days
            .values()
            .map(|p| {
                p.stored_bytes + (p.buffer_bytes + p.closing.as_ref().map_or(0, |b| b.bytes)) as u64
            })
            .sum()
    }
}

/// `list` less every row group of the files at `gone`, with `new` put in by path, so a
/// day's row groups stay in path order.
fn spliced(list: &[Stored], gone: &BTreeSet<PathBuf>, new: Vec<Stored>) -> Vec<Stored> {
    let mut out = Vec::with_capacity(list.len() + new.len());
    let mut new = new.into_iter().peekable();
    for s in list {
        if gone.contains(&s.file.path) {
            continue;
        }
        while let Some(n) = new.next_if(|n| n.file.path < s.file.path) {
            out.push(n);
        }
        out.push(s.clone());
    }
    out.extend(new);
    out
}

/// Every data file under `base`, in path order: what `find <data> -type f` would count
/// among the rows' files.
pub fn data_files(base: &Path) -> BTreeSet<PathBuf> {
    let mut out = BTreeSet::new();
    for day in dirs(&base.join(DATA_DIR)) {
        for entry in fs::read_dir(&day).into_iter().flatten().flatten() {
            out.insert(entry.path());
        }
    }
    out
}

#[cfg(test)]
mod tests;
