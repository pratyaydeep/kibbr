//! The disk store: one shared file per UTC day, named in `manifest.log`.
//!
//! ```text
//! <base>/
//! ├── catalog.log                 the truth about templates
//! ├── manifest.log                the truth about files
//! ├── staging/                    bulk loads in flight; at startup a committed one is
//! │                               moved into data/ and the rest deleted
//! └── data/
//!     └── 2024-01-15/
//!         ├── seg_0_000001.col    seg_<group>_<number>; group is always 0 for now
//!         └── seg_0_000002.col    the next file, once the size cap is reached
//! ```
//!
//! **Why one file per day and not per template.** Every cost the per-template layout had -
//! a 4KB block rounded up per file, a header and footer per file, a descriptor per
//! partition, a shutdown that sealed thousands of files - came from the file count, which
//! now depends on days and size and not on templates. A row group still belongs to one
//! template; only the file is shared.
//!
//! **What a leaf holds.** A template's row groups, by UTC day, in storage order - the
//! sealed files' by path, then the open file's - and its rows not yet in a row group. A
//! query reads a day's row groups and then its rows in memory, newest day first, and prunes
//! on each row group's time range before a byte of column data is read.
//!
//! **Closing a row group holds no lock across the disk.** Each leaf has a lock of its own,
//! and a flush takes it twice, briefly: once to move the rows that are due aside - where
//! queries still see them - leaving an empty buffer, and once, after they have been encoded,
//! written and flushed with it released, to publish their row groups and drop them in one
//! step. `DiskLeaf::state` has the invariant this rests on. Every row group that is due is
//! appended before anything is flushed, and then each file written is flushed once: a tick
//! costs one flush per day it wrote to, not one per row group, which capped closes at about
//! 270 a second - 870 quiet templates timing out together were three seconds of flushing.
//! Modelled against holding the lock throughout: worst 1% query wait 164µs against 8.8ms,
//! ingest 803,000 rows a second against 551,000, and neither ever showed a snapshot a row
//! twice or not at all.
//!
//! **The file being written has no index on disk** - its footer is written when it is
//! sealed - so a query reads it through the entries its writer holds in memory, published
//! to each leaf as its row groups close: the same entries, template by template, that the
//! footer will hold. Forgetting them would lose the newest rows from every answer.
//!
//! **Startup.** The manifest names the live files. A file a bulk load committed and a stop
//! left in `staging/` is moved into place - the commit happened, so it is finished rather
//! than undone - and then `staging/` is deleted, taking any load that never committed with
//! it. A file the manifest does not name is deleted. A named file that is sealed opens
//! through its footer and nothing else; one that is not was left by a crash, and is
//! recovered - walked, cut at the tear, sealed - and the seal recorded. A directory holding
//! the per-template layout is refused by path.
//!
//! **The bulk door.** A load writes whole, finished files into `staging/<load id>/`, and
//! `commit_load` publishes them: one manifest record, then the move into `data/`, then the
//! row groups into the leaves - under the flush's lock, so a load and a flush never
//! interleave. See `commit_load`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::time::Instant;

use chrono::{DateTime, NaiveDate};

use super::manifest::{self, FileRecord, Manifest, Record};
use super::v3::build::{self, schema_of, Encoded};
use super::v3::file::{self, Entry, Index, Mark, Reader, Writer};
use super::v3::{day_of, FormatError, Source, DAY_NS};
use super::{
    effective_ts, in_window, lock, nanos_of, render_row, value_bytes, DiskOptions, FlushKind,
    Flushed, LeafStore, Predicate, Row, RowGroupLimits, Scan, ScanOptions, Snapshot, Store,
    StoreError, Visitor, DEFAULT_TS_COLUMN,
};

pub const DATA_DIR: &str = "data";
pub const STAGING_DIR: &str = "staging";

/// Seal a day's file at 512MB and start the next number. Walking an unsealed 1GB file at
/// startup and checking every CRC takes about 0.3 seconds, so either is fine.
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 512 * 1024 * 1024;

fn warn(message: String) {
    eprintln!("triedb: WARN {message}");
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
/// version, by path.
///
/// **Header bytes only.** This writes nothing, so it can run before the directory's
/// `LOCK` is taken and before the port; everything that writes - recovery, deleting
/// unnamed files - waits until the caller holds `LOCK`. Paths are checked in order,
/// so the one named is the same on every start.
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
        if let Ok(Some(found)) = file::version_of(&path) {
            if found != super::v3::VERSION {
                return Err(FormatError::Version { path, found }.into());
            }
        }
    }
    Ok(())
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

/// A data file a row group lives in.
///
/// **A retired file is deleted when its last reader lets go.** Replace-a-day takes a file
/// out of the manifest and out of every leaf in one step, but a query that copied a day's
/// row groups a moment earlier still holds them, and opens their file only when it reaches
/// them: unlinked then, that query would fail halfway through an answer. So the file goes
/// when the last `Arc` naming it drops. A process that stops first leaves it unnamed, and
/// the next start deletes it.
#[derive(Debug)]
pub struct DataFile {
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

/// The files being written, and the manifest that names them: one open file per UTC day
/// that has taken a row since its last was sealed.
///
/// **Behind the flush's own lock.** A flush takes it - the tick, the final flush - and so
/// does a load's commit, so a load and a flush never interleave. It is held across the
/// encode, the write and the fsync, and no append and no query ever waits on it.
struct Files {
    data: PathBuf,
    manifest: Manifest,
    /// The manifest's live files, kept current with every record appended: what
    /// replace-a-day takes its day's directory from and retires.
    live: BTreeMap<String, FileRecord>,
    /// Days a file found at startup shares with another day, and that file: see
    /// `day_files`. Only a directory written before a row always went into its own day's
    /// file can have one, and nothing this process writes adds to it.
    mixed: BTreeMap<i64, String>,
    /// Every load a record names as its source. A repeat is refused by its id.
    loads: BTreeSet<u64>,
    /// Loads committed whose files could not all be moved out of staging: this process's
    /// view of the directory is short of their rows until a restart finishes the move, so
    /// no load commits beside them.
    unmoved: BTreeSet<u64>,
    open: BTreeMap<i64, Open>,
    /// The next file number per day directory.
    next: BTreeMap<i64, u32>,
    max_open: usize,
    max_bytes: u64,
    /// Flushes so far: the clock `Open::written` is kept by.
    flushes: u64,
}

impl Files {
    /// Append records to the manifest - durable once this returns - and apply them to the
    /// live set this store keeps.
    fn record(&mut self, records: Vec<Record>) -> Result<(), manifest::ManifestError> {
        self.manifest.append(&records)?;
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

    /// The open file to seal to make room: one no batch still to be written in this flush
    /// wants, if there is one - sealing a day about to be written would only open its next
    /// number a moment later - and then the least recently written, the older day first
    /// between two written by the same flush.
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
            crate::frame::sync_dir(&path_dir)
                .and_then(|()| crate::frame::sync_dir(&self.data))
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

    /// Append one moved-aside batch's row groups, encoded, to its day's file. Not
    /// flushed: a flush appends every batch that is due first, and then flushes each file
    /// it wrote to once.
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
                // Whatever of this batch reached the file is cut back, so the batches either
                // side of it are still written and flushed: one bad batch costs its own rows
                // a retry, and not the day's. A cut that fails leaves the writer torn, and
                // every later batch for this file fails at its first write.
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
                // Nothing this flush appended here is known to be durable, so none of it is
                // kept: the file is cut back to where the flush began, and every row group
                // it held goes back to its buffer for the next flush to write again. Kept,
                // those rows would be in the file twice once the retry landed. If the cut
                // fails as well, the file is left torn for the next start, whose recovery
                // keeps whatever of them did reach the disk beside their retry: a disk that
                // fails an fsync and then a truncate is past what a flush can promise.
                if let Some(mark) = began {
                    let _ = open.rollback(mark);
                }
                Err(e.into())
            }
        }
    }

    /// After a flush: seal every file past the size cap. What was sealed, and what could
    /// not be. The cap on open files needs nothing here: the flush made room under it
    /// before it opened each file.
    fn settle(&mut self) -> (usize, Vec<StoreError>) {
        let mut errors = Vec::new();
        for open in self.open.values_mut() {
            open.began = None;
        }
        // A writer that could not cut a failed write back takes nothing more, and cannot be
        // sealed. It is let go named and unsealed: the next start recovers it, cut at the
        // tear, and the next row for its day opens the next number.
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
            // The file leaves the open set whatever happens: a seal that failed has still
            // stopped taking rows, and the next start recovers it.
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
            // records it then.
            if let Err(e) = self.record(vec![Record::Seal { files: records }]) {
                errors.push(e.into());
            }
        }
        (sealed, errors)
    }
}

impl Files {
    /// Every live file in `day`'s directory: what replace-a-day retires.
    ///
    /// The directory is the day: a row group goes into its own day's file and no other's,
    /// whatever the cap on open files, so retiring the directory takes every row of the
    /// day and nothing of any other. **Refused, naming the file, for a day a file found at
    /// startup shares with another day** - a directory written while a late row still went
    /// into today's file. Retiring the directory would then take the other day's rows with
    /// it, or leave some of this day's beside the new ones, and the day's count would not
    /// be what the load holds.
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

/// Every job's row group, encoded on up to `threads` threads at once, in job order.
///
/// **The rows are read, never moved or locked.** They are the batches' shared rows, which
/// their leaves go on answering queries from the whole time. An encoder that panics fails
/// its batch - its rows go back to their buffer - rather than the flush: a flush that died
/// halfway would leave every batch it had moved aside neither written nor put back.
fn encode_all(moved: &[Moved], jobs: &[Job], threads: usize) -> Vec<EncodedRowGroup> {
    let encode = |job: &Job| {
        let m = &moved[job.moved];
        let rows = &m.batch.rows[job.from..job.to];
        let refs: Vec<&Row> = rows.iter().map(|b| &b.row).collect();
        let times: Vec<i64> = rows.iter().map(|b| b.time).collect();
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            let columns = schema_of(refs.iter().copied());
            let encoded = build::encode(
                m.leaf.template_id,
                &columns,
                &refs,
                &times,
                Source::NotRecorded,
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
    files: Mutex<Files>,
    /// Every leaf this store has opened, by template: for a flush to walk, and for a query
    /// to take its snapshot from in the hold a load publishes under.
    leaves: RwLock<BTreeMap<u64, Arc<DiskLeaf>>>,
    /// Held in write by a load's publish, and in read by a query's snapshot while it
    /// freezes its leaves: a snapshot is wholly before a publish or wholly after it. Apart
    /// from `leaves`, which a template's first row takes in write to add its leaf: a wide
    /// query's freezes held there would stall that row - and, behind it, every ingest body
    /// and every query waiting on the registry - for as long as they took.
    publish: RwLock<()>,
    /// Loads and replaces published so far: see `Store::generation`. Bumped under the
    /// write hold of `publish`, and read under a read hold.
    generation: AtomicU64,
    /// Row groups on disk with no leaf open yet - found at startup, or committed by a load
    /// since - by template, in storage order. A leaf takes its own
    /// when it opens; a template no catalog record names keeps them here, unread.
    stored: Mutex<BTreeMap<u64, Vec<Stored>>>,
    claimed: Vec<u64>,
    /// Memory held by every leaf's buffered rows and by the closes in flight, as
    /// `Row::held_bytes` estimates it: what the memory limit is held against.
    buffered: Arc<AtomicU64>,
    /// Files open for writing as the last flush left them. Read by `/stats`, without the
    /// files lock a flush holds across the disk.
    open_count: AtomicUsize,
}

impl DiskStore {
    /// Open the data directory: refuse an earlier layout, read the manifest, finish moving
    /// any load it names as committed out of staging, delete what it does not name and the
    /// rest of staging, recover every file a crash left unsealed, and index every row group
    /// by template. The caller holds the directory's `LOCK`.
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
            // Every committed file has been moved out by now, so a file still here belongs
            // to a load that never committed - bar a spool of standard input, which is only
            // ever a copy of what a load was reading.
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
        delete_unnamed(&data, &live)?;

        let mut next: BTreeMap<i64, u32> = BTreeMap::new();
        let mut stored: BTreeMap<u64, Vec<Stored>> = BTreeMap::new();
        let mut mixed: BTreeMap<i64, String> = BTreeMap::new();
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
            let slot = next.entry(day).or_insert(1);
            *slot = (*slot).max(number + 1);
            if !path.exists() {
                return Err(FormatError::Io {
                    path,
                    source: std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "named by manifest.log and not on disk; its rows are gone",
                    ),
                }
                .into());
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
                    // Sealed, and the process stopped before the manifest heard.
                    Some(index) => index,
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
            // A row group of another day than its directory's: filed there, past the cap on
            // open days, by a build that sent a late row into today's file. Both days are
            // noted, with the file, for `day_files` to refuse.
            for e in &index.entries {
                let other = day_of(e.min_time);
                if other != day {
                    mixed.entry(day).or_insert_with(|| rel.clone());
                    mixed.entry(other).or_insert_with(|| rel.clone());
                }
            }
            index_rows(&mut stored, Arc::new(DataFile::new(path)), &index);
        }
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
            }),
            leaves: RwLock::new(BTreeMap::new()),
            publish: RwLock::new(()),
            generation: AtomicU64::new(0),
            stored: Mutex::new(stored),
            claimed,
            buffered: Arc::new(AtomicU64::new(0)),
            open_count: AtomicUsize::new(0),
        })
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

    /// An empty `staging/<load id>/`, durable before a file is written into it: a stop
    /// after this leaves a directory the next start deletes, and nothing else.
    ///
    /// **Never over a committed load's files.** A load whose record is written and whose
    /// move failed has its rows in `staging/<load id>/` and nowhere else; the same inputs
    /// run again - with `--replace-day`, which a repeat is allowed - would stage into the
    /// same directory. So while any committed load is unmoved, no load stages at all.
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
        crate::frame::sync_dir(&staging)
            .and_then(|()| crate::frame::sync_dir(&self.base))
            .map_err(|e| io(&staging, e))?;
        Ok(dir)
    }

    /// Remove a load's staging directory: what a load that will not commit leaves behind.
    pub fn unstage(&self, load_id: u64) {
        let _ = fs::remove_dir_all(self.base.join(STAGING_DIR).join(format!("{load_id:016x}")));
    }

    /// Publish a load's staged files: one manifest record, then the move, then the rows.
    ///
    /// **The record is the commit.** Every staged file is sealed and flushed before this
    /// is called, and each is named in the one record - `add`, or with a day to replace,
    /// `replace` - so a load spanning a hundred days is in the database whole or not at
    /// all. A stop before the record leaves only `staging/`, which the next start deletes;
    /// a stop after it leaves files to move, which the next start moves (`finish_moves`).
    ///
    /// **Numbered here, under the flush's lock.** A load stages under names of its own and
    /// takes each file's final number from the same counter the live door opens files by,
    /// so the two can never want one name - and a load's files come after every file of
    /// their day, which is where their row groups are in storage order.
    ///
    /// **Replace-a-day** first writes the day's rows still in memory into its file and
    /// seals it, so the day it retires is wholly on disk, then retires every file in the
    /// day's directory in the same record that adds the new ones. The retired files leave
    /// every leaf in the same hold that adds the new row groups, and are deleted when the
    /// last query reading them lets go. A repeated load id is not refused here: the load
    /// is replacing its day, not adding to it.
    pub fn commit_load(&self, commit: LoadCommit) -> Result<Committed, StoreError> {
        let mut files = lock(&self.files);
        if let Some(pending) = files.unmoved.iter().next() {
            return Err(StoreError::MovePending { load_id: *pending });
        }
        if commit.replace_day.is_none() && files.loads.contains(&commit.load_id) {
            return Err(StoreError::LoadRepeated {
                load_id: commit.load_id,
            });
        }
        let mut retired = Vec::new();
        if let Some(day) = commit.replace_day {
            // The loader refuses a row outside the day before it stages anything; checked
            // again here because a file of another day published as this day's replacement
            // would add to that day rather than replace it, and nothing later would say so.
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
            | Err(e @ manifest::ManifestError::Log(crate::frame::LogError::Torn { .. })) => {
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

    /// A committed load's row groups into the leaves - and into `stored`, for a template
    /// with no leaf open - and the retired files' out, in one hold of the leaves' lock.
    ///
    /// **One step, across every template**: the swap and the `generation` bump happen in
    /// one write hold of `publish`, the lock a query's snapshot freezes its leaves under in
    /// read - so a query counts all of a load or none of it, and a replaced day as it was or
    /// as the load left it, never some templates' rows of it and not others'. See
    /// `Snapshot`.
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

    /// One flush, in the three steps the invariant at `DiskLeaf::state` needs, and then the
    /// seals - of every day, or with `only`, of one day's rows and then its file: what
    /// replace-a-day does first, so the day it retires holds no row still in memory.
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
        let mut done = Flushed::default();

        // ── 1. Move the rows that are due aside: one short hold of each leaf's lock ──
        let mut moved: Vec<Moved> = Vec::new();
        for leaf in &leaves {
            leaf.move_due(kind, now, only, &mut moved);
        }
        if !kind.is_final() && only.is_none() {
            done.forced = self.relieve(&leaves, &mut moved);
        }

        // ── 2. Encode, write and flush them, holding no leaf's lock ──
        //
        // Encoded side by side first - nearly all of the cost, and none of it needs the
        // file - then numbered, framed and appended one at a time, day by day: a day's
        // file is opened once per flush however many templates wrote to it.
        let jobs = jobs_of(&moved, &self.limits);
        let mut encoded = encode_all(&moved, &jobs, self.encoders).into_iter();
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
        // Days with a file open first: by the time a day needs room, every open file this
        // flush will write to has been written, so the one sealed for it is never one a
        // later batch wants - a day is sealed and opened again as its next number only when
        // more days than the cap want a file in one flush.
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
            // **At the cap, the least recently written file is sealed and the row's own day
            // opened** - never another day's file. A late row once went into today's file
            // instead, as a row group of its own day: its day was then split across two
            // directories in an order a restart read differently from the order it was
            // written in - tied rows swapped places, and a cursor from before the restart
            // repeated one and skipped the other - and no replace could retire the day
            // without taking another's rows or leaving some of its own. What this costs is
            // a seal and an open per late day past the cap, where the spill cost a row
            // group in a file already open.
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
    /// it: the file was cut back to where the flush began, so their rows go back to their
    /// buffers.
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

    /// Past the memory limit, move aside the largest buffers left as well, largest first,
    /// until what stays in memory is three quarters of the limit. How many were moved.
    ///
    /// To three quarters and not just under: every close forced here makes a row group
    /// smaller than the thresholds would have, and stopping a hair under the limit would buy
    /// another round of them on every tick for as long as the burst lasted.
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

/// Move a sealed file into `data/<rel>`, and make the move durable: the day directory is
/// made first, and synced with its parent when new, so the rename lands in a directory the
/// next start will find.
fn move_into(from: &Path, data: &Path, rel: &str) -> Result<(), FormatError> {
    let io = |path: &Path, source| FormatError::Io {
        path: path.to_path_buf(),
        source,
    };
    let to = data.join(rel);
    let dir = crate::frame::parent_of(&to);
    if !dir.exists() {
        fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        crate::frame::sync_dir(data).map_err(|e| io(data, e))?;
    }
    fs::rename(from, &to).map_err(|e| io(from, e))?;
    crate::frame::sync_dir(dir).map_err(|e| io(dir, e))
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

/// Files a load committed and a stop left in `staging/<load id>/`, moved into place.
///
/// A load appends its one manifest record and then moves its files out of staging, so a
/// stop between the two leaves the manifest naming files that are not in `data/` yet. The
/// record is the commit: the move is finished here, never undone. A staged file is matched
/// to its record by the id in its header and not by its name - staging names are the
/// load's own, and a file's number is only chosen when it is committed.
///
/// **Fails closed.** `staging/` is deleted after this returns, and for a committed load it
/// may hold the only copy of its rows: a file the record names and that is neither in
/// `data/` with its id nor readable in staging - or a staging directory that cannot be
/// read - stops the start here, with staging untouched, rather than after the only copy
/// is gone.
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
        // Only a file not already in place is looked for: `staging/<id>/` can also hold a
        // later run of the same inputs - a `--replace-day` of them, stopped part way - whose
        // files no record names, and a file of that cut short at its header is no reason
        // for a start to fail.
        let mut wanted = Vec::new();
        for (rel, record) in named {
            let at = data.join(rel);
            if !(at.exists() && manifest::hex(&file::file_id_of(&at)?) == record.file_id) {
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

/// Every file under `data/` the manifest does not name, deleted; and every directory left
/// empty. A file nobody named is not part of the database: a crash between creating a file
/// and naming it, or a load that never committed.
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
        // Held from taking the row groups to joining the list, so a load committing
        // meanwhile finds this template's in one place or the other: in `stored` before, in
        // the leaf after. Between the two, its row groups would go to `stored` and wait
        // there, unread, until a restart.
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

    /// Every planned template's leaf, frozen in one read hold of `publish` - the hold a
    /// load's publish waits for in write, and bumps `generation` under - so the snapshot is
    /// wholly before a commit or wholly after it. The registry's leaves say which templates
    /// have one; this store's own are the ones it can freeze.
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
        preds: &[Predicate],
        v: &mut dyn Visitor,
    ) -> Option<Result<Scan, StoreError>> {
        self.leaves
            .remove(&template_id)
            .map(|frozen| frozen.visit(opts, preds, v))
    }
}

// ── One leaf ───────────────────────────────────────────────────────────────────────

struct Buffered {
    row: Row,
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
    opened_at: Option<Instant>,
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
    /// Row groups in storage order: the sealed files' by path, then the open file's in the
    /// order they were published. **The open file's are its index**: this template's run of
    /// the entries its writer holds in memory - the entries its footer will hold - published
    /// here as each row group closes, because the file has no footer on disk to read them
    /// from. Behind an `Arc`, so a query's snapshot is a count and not a copy.
    stored: Arc<Vec<Stored>>,
    stored_rows: u64,
    stored_bytes: u64,
    stored_span: Option<(i64, i64)>,
    /// Rows a flush has moved out of the buffer and is writing: answered from here until
    /// the row groups holding them are published, in the same hold of the lock that drops
    /// them.
    closing: Option<Arc<Batch>>,
    /// Grows on demand and never starts sized: at 10,000 templates, a buffer sized for a
    /// full row group each was 1.06GB against 146MB grown as rows arrived, and a fifth of
    /// the ingest rate.
    buffer: Vec<Arc<Buffered>>,
    buffer_bytes: usize,
    buffer_held: usize,
    buffer_span: Option<(i64, i64)>,
    /// When the oldest buffered row arrived, which is when the current row group opened.
    /// Read from a clock but never driven by one: whoever owns the tick asks.
    opened_at: Option<Instant>,
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

    /// Hand the buffer to a flush and start an empty one. The rows stay readable here, as
    /// `closing`, until the flush publishes their row groups.
    fn move_aside(&mut self) -> Arc<Batch> {
        let batch = Arc::new(Batch {
            // `take` leaves a `Vec` with no capacity at all: the next buffer grows from
            // nothing, as the first did.
            rows: std::mem::take(&mut self.buffer),
            bytes: std::mem::take(&mut self.buffer_bytes),
            held: std::mem::take(&mut self.buffer_held),
            span: self.buffer_span.take(),
            opened_at: self.opened_at.take(),
        });
        self.closing = Some(Arc::clone(&batch));
        batch
    }

    /// Everything a query reads of this day.
    fn view(&self) -> View {
        View {
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

pub struct DiskLeaf {
    template_id: u64,
    /// The template's time column: what a row's day, and its row group's time range, are
    /// read from, falling back to the arrival time a row without one carries.
    time_column: String,
    limits: RowGroupLimits,
    /// The store's count of memory held by buffered rows, across every leaf.
    held: Arc<AtomicU64>,
    state: Mutex<LeafState>,
}

impl DiskLeaf {
    /// The leaf's lock, and the invariant that lets a flush write with it released.
    ///
    /// **Whenever this lock is free, every row of the leaf is in exactly one of three
    /// places: a published row group (`Day::stored`), the rows a flush has moved aside
    /// (`Day::closing`), or the buffer.** A query copies all three in one hold and reads them
    /// after it lets go, so it counts each row once whatever a flush is doing. A flush takes
    /// the lock twice: once to move the buffer to `closing`, and once, after the rows are
    /// written and flushed, to publish their row groups and drop `closing` - together, in
    /// that one hold. Split the second - publish, let go, drop - and a query in between sees
    /// those rows twice: a count occasionally too high, which nothing reports and nobody
    /// reproduces. Drop first and it sees them not at all.
    ///
    /// Nothing is done under it but moving rows between the three: no encoding, no I/O.
    fn state(&self) -> MutexGuard<'_, LeafState> {
        lock(&self.state)
    }

    fn due(&self, part: &Day, kind: FlushKind, now: Instant) -> bool {
        part.closing.is_none()
            && !part.buffer.is_empty()
            && (kind.is_final()
                || part.buffer.len() >= self.limits.rows
                || part.buffer_bytes >= self.limits.bytes
                || part
                    .opened_at
                    .is_some_and(|t| now.saturating_duration_since(t) >= self.limits.age))
    }

    /// Move aside every buffer a threshold says is due - every buffer, on a final flush -
    /// of every day, or of `only`.
    fn move_due(
        self: &Arc<Self>,
        kind: FlushKind,
        now: Instant,
        only: Option<i64>,
        out: &mut Vec<Moved>,
    ) {
        let mut state = self.state();
        for (day, part) in state.days.iter_mut() {
            if only.is_none_or(|d| d == *day) && self.due(part, kind, now) {
                out.push(Moved {
                    leaf: Arc::clone(self),
                    day: *day,
                    batch: part.move_aside(),
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
        Some(Moved {
            leaf: Arc::clone(self),
            day,
            batch: part.move_aside(),
        })
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
    /// `gone` out, in one hold of the lock - so no query sees the day between the two. The
    /// files taken out are handed back, for the caller to retire once nothing else names
    /// them.
    ///
    /// The new row groups go after everything stored in their day, and not by path: a day
    /// whose file is still open has published that file's row groups here already, and the
    /// load's files - numbered after it - come after them in the list as they will by path
    /// once it is sealed. Only row groups the open file takes after this moment sort
    /// differently across a restart, and so only the position between two rows of one
    /// template, one day and one timestamp, one live and one loaded - which one source per
    /// day rules out.
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

    /// The flush could not write a day's moved rows: put them back at the head of the
    /// buffer, before anything that arrived since - the order, and so every row's position,
    /// is what it would have been had they never moved.
    fn restore(&self, day: i64) {
        let mut state = self.state();
        let Some(part) = state.days.get_mut(&day) else {
            return;
        };
        let Some(batch) = part.closing.take() else {
            return;
        };
        let mut rows = batch.rows.clone();
        rows.append(&mut part.buffer);
        part.buffer = rows;
        part.buffer_bytes += batch.bytes;
        part.buffer_held += batch.held;
        part.buffer_span = union(part.buffer_span, batch.span);
        part.opened_at = match (batch.opened_at, part.opened_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }

    /// Every day a query over `[since, until)` can use, newest first, copied in one hold of
    /// the lock - `View`s share the row groups and rows, so this is counts and pointers,
    /// not rows - and the row groups of the days the window rules out, counted as pruned.
    fn freeze(&self, since: Option<i64>, until: Option<i64>) -> Frozen {
        let windowed = since.is_some() || until.is_some();
        let state = self.state();
        let mut frozen = Frozen::default();
        for (day, part) in state.days.iter().rev() {
            if windowed && !Self::overlaps(*day, part.span(), since, until) {
                frozen.pruned += part.stored.len() as u64;
                continue;
            }
            frozen.views.push(part.view());
        }
        frozen
    }

    /// How many rows this day contributes to `count`, if that can be known without reading
    /// it: no predicate could reject a row, and the window holds the whole day's range.
    fn countable(
        view: &View,
        preds: &[Predicate],
        since: Option<i64>,
        until: Option<i64>,
    ) -> Option<u64> {
        if !preds.is_empty() {
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

    /// One day's rows - stored, then in memory - handed to the visitor. `false` once the
    /// limit is reached.
    fn visit_day(
        view: &View,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
        out: &mut Scan,
        readers: &mut HashMap<PathBuf, Reader>,
    ) -> Result<bool, StoreError> {
        let ts_name = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(nanos_of);
        let until = opts.until.map(nanos_of);
        let windowed = since.is_some() || until.is_some();

        // A row's position is the rows of its template and day before it in storage order,
        // counted from the index: the pruned row groups' counts are added unread.
        let mut seq = 0u64;
        for s in view.stored.iter() {
            let rows = u64::from(s.entry.row_count);
            if windowed
                && !(since.is_none_or(|from| s.entry.max_time >= from)
                    && until.is_none_or(|to| s.entry.min_time < to))
            {
                out.row_groups_pruned += 1;
                seq += rows;
                continue;
            }
            out.row_groups_read += 1;
            // Empty means every column: a `MATCH` returns whole rows. A window always reads
            // the time column, because it filters on it.
            let wanted: Vec<usize> = (0..s.schema.len())
                .filter(|at| {
                    let name = &s.schema[*at];
                    opts.columns.is_empty()
                        || opts.columns.iter().any(|w| w == name)
                        || (windowed && (name == ts_name || name == super::RECEIVED_AT_COLUMN))
                })
                .collect();
            let reader = match readers.entry(s.file.path.clone()) {
                std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
                // The one place a data file is opened for reading, so the one place that
                // counts them: a file whose every row group the window pruned is not.
                std::collections::hash_map::Entry::Vacant(slot) => {
                    out.files_read += 1;
                    slot.insert(Reader::open(&s.file.path)?)
                }
            };
            let header = reader.header(&s.entry, s.schema.len())?;
            let (columns, read) = reader.chunks(&s.entry, &header, &wanted)?;
            out.bytes_read += read;

            let mut row = Row::with_capacity(wanted.len());
            for r in 0..header.row_count as usize {
                out.scanned += 1;
                row.clear();
                for (at, cells) in wanted.iter().zip(&columns) {
                    let (value, written) = &cells[r];
                    row.insert_written(s.schema[*at].clone(), value.clone(), written.clone());
                }
                let position = seq;
                seq += 1;
                if windowed && !in_window(effective_ts(&row, ts_name), since, until) {
                    continue;
                }
                let rendered = render_row(&row, &opts.text_columns);
                let seen = rendered.as_ref().unwrap_or(&row);
                if !preds.iter().all(|pred| pred(seen)) {
                    continue;
                }
                out.matched += 1;
                v.row(seen, position);
                if opts.limit != 0 && out.matched as usize >= opts.limit {
                    return Ok(false);
                }
            }
        }

        // Then the rows not yet in a row group - moved aside first, then buffered - after the
        // stored ones, because that is the position they will have once written.
        let in_memory = view
            .closing
            .iter()
            .flat_map(|batch| batch.rows.iter())
            .chain(view.buffer.iter());
        for b in in_memory {
            out.scanned += 1;
            let position = seq;
            seq += 1;
            if windowed && !in_window(effective_ts(&b.row, ts_name), since, until) {
                continue;
            }
            let rendered = render_row(&b.row, &opts.text_columns);
            let seen = rendered.as_ref().unwrap_or(&b.row);
            if !preds.iter().all(|pred| pred(seen)) {
                continue;
            }
            out.matched += 1;
            v.row(seen, position);
            if opts.limit != 0 && out.matched as usize >= opts.limit {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// One leaf as a query found it: see `DiskLeaf::freeze`. Read with no lock held.
#[derive(Default)]
struct Frozen {
    views: Vec<View>,
    pruned: u64,
}

impl Frozen {
    /// The frozen days' rows, newest day first. `opts` must carry the window it was frozen
    /// with: the days it ruled out were not copied.
    fn visit(
        &self,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        let since = opts.since.map(nanos_of);
        let until = opts.until.map(nanos_of);
        let mut out = Scan {
            row_groups_pruned: self.pruned,
            ..Scan::default()
        };
        let mut readers = HashMap::new();
        for view in &self.views {
            if !v.wants_partition(view.span.map(|(_, hi)| hi)) {
                // The visitor has no use for a row from here, but a limit bounds the rows in
                // the answer and not the rows that matched: the day is still counted, free
                // when nothing inside it could reject a row, and read otherwise.
                if let Some(matched) = DiskLeaf::countable(view, preds, since, until) {
                    out.matched += matched;
                    continue;
                }
            }
            if !DiskLeaf::visit_day(view, opts, preds, v, &mut out, &mut readers)? {
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

    /// The ordinal is **the row's position within its template's UTC day**: the rows of
    /// that template and day before it in storage order. It survives a restart with nothing
    /// stored, because it is counted from the index. `Rank` compares ordinals only between
    /// rows of one template with one timestamp, which are on one day.
    fn append(&self, row: Row) -> u64 {
        // Every row the ingest path hands over has an effective time - its own, or the
        // arrival time it was stamped with. A row with neither has no day; it is placed
        // at the epoch rather than dropped.
        let time = effective_ts(&row, &self.time_column).unwrap_or(0);
        let bytes = row
            .iter()
            .map(|(key, value)| key.len() + value_bytes(value))
            .sum();
        // The row, the `Arc` around it and its slot in the buffer.
        let held = row.held_bytes() + std::mem::size_of::<Buffered>() + 24;
        let row = Arc::new(Buffered {
            row,
            time,
            bytes,
            held,
        });
        let seq = {
            let mut state = self.state();
            state.buffered += 1;
            let part = state.days.entry(day_of(time)).or_default();
            let seq = part.stored_rows + part.in_memory();
            if part.buffer.is_empty() {
                part.opened_at = Some(Instant::now());
            }
            part.buffer.push(row);
            part.buffer_bytes += bytes;
            part.buffer_held += held;
            widen(&mut part.buffer_span, time, time);
            seq
        };
        self.held.fetch_add(held as u64, Ordering::Relaxed);
        seq
    }

    /// Every day this query can use, copied in one hold of the lock; then read, newest day
    /// first, with the lock released - so a query reading a thousand row groups off the disk
    /// holds up no append and no flush.
    fn visit(
        &self,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        self.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of))
            .visit(opts, preds, v)
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
mod tests {
    use super::*;
    use crate::store::v3::Source;
    use crate::store::Rows;
    use crate::types::ColValue;
    use std::time::Duration;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "triedb-disk-{name}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("mkdir");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const T0: i64 = 1_705_314_600_000_000_000;

    /// A row group closes on every flush: nothing waits for a threshold.
    fn eager() -> DiskOptions {
        DiskOptions {
            row_groups: RowGroupLimits {
                age: Duration::ZERO,
                ..RowGroupLimits::default()
            },
            max_open_files: 8,
            ..DiskOptions::default()
        }
    }

    fn store(scratch: &Scratch) -> DiskStore {
        store_with(scratch, eager())
    }

    fn store_with(scratch: &Scratch, options: DiskOptions) -> DiskStore {
        DiskStore::open(scratch.0.clone(), options).expect("open")
    }

    fn row(ts: i64, n: i64) -> Row {
        let mut row = Row::new();
        row.insert("ts".into(), ColValue::Datetime(ts));
        row.insert_written(
            "n".into(),
            ColValue::Int64(n),
            Some(serde_json::Value::String(format!("{n:04}"))),
        );
        row
    }

    fn all(leaf: &dyn LeafStore) -> Vec<Row> {
        leaf.scan(
            &ScanOptions {
                ts_column: Some("ts".into()),
                ..ScanOptions::default()
            },
            &[],
        )
        .expect("scan")
        .rows
    }

    /// Each row's `n`, sorted: what a leaf holds, however it is split between the disk and
    /// memory.
    fn ns(leaf: &dyn LeafStore) -> Vec<i64> {
        let mut out: Vec<i64> = all(leaf)
            .iter()
            .map(|r| match r.get("n") {
                Some(ColValue::Int64(n)) => *n,
                other => panic!("n is {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn rows_come_back_as_written_before_and_after_a_restart() {
        let scratch = Scratch::new("restart");
        let want: Vec<Row> = (0..5).map(|n| row(T0 + n * 1_000_000_000, n)).collect();
        {
            let s = store_with(&scratch, DiskOptions::default());
            let leaf = s.open(7, "ts").expect("leaf");
            for r in &want {
                leaf.append(r.clone());
            }
            assert_eq!(all(leaf.as_ref()).len(), 5, "buffered rows are visible");
            let done = s.flush(FlushKind::Final);
            assert!(done.failed.is_empty(), "{:?}", done.failed);
            assert_eq!((done.rows, done.sealed), (5, 1));
            assert_eq!(all(leaf.as_ref()), want);
        }
        let s = store(&scratch);
        assert_eq!(s.claimed(), vec![7]);
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(all(leaf.as_ref()), want);
        assert_eq!(data_files(&scratch.0).len(), 1, "one file for one day");
    }

    #[test]
    fn a_file_the_manifest_does_not_name_is_deleted_and_so_is_staging() {
        let scratch = Scratch::new("unnamed");
        let day = scratch.0.join(DATA_DIR).join("2024-01-15");
        fs::create_dir_all(&day).expect("mkdir");
        fs::write(day.join("seg_0_000001.col"), b"TRDB\x03\0\0\0").expect("write");
        fs::create_dir_all(scratch.0.join(STAGING_DIR).join("abc")).expect("mkdir");
        let _ = store(&scratch);
        assert!(!day.exists(), "an unnamed file survived startup");
        assert!(
            !scratch.0.join(STAGING_DIR).exists(),
            "staging survived startup"
        );
    }

    #[test]
    fn a_leaf_directory_is_refused_by_path() {
        let scratch = Scratch::new("legacy");
        let leaf = scratch.0.join("leaf_101");
        fs::create_dir_all(&leaf).expect("mkdir");
        match DiskStore::open(scratch.0.clone(), DiskOptions::default()) {
            Err(StoreError::LegacyLayout { path }) => assert_eq!(path, leaf),
            Err(other) => panic!("refused for another reason: {other}"),
            Ok(_) => panic!("opened a directory holding the per-template layout"),
        }
    }

    /// A crash after row groups were flushed and before the seal: the next start recovers
    /// the file, records the seal, and every flushed row is there.
    #[test]
    fn an_unsealed_file_is_recovered_and_its_rows_kept() {
        let scratch = Scratch::new("crash");
        {
            let s = store(&scratch);
            let leaf = s.open(7, "ts").expect("leaf");
            for n in 0..3 {
                leaf.append(row(T0 + n, n));
            }
            s.flush(FlushKind::Tick);
            // No seal: the process dies here.
        }
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(all(leaf.as_ref()).len(), 3);
        drop(leaf);
        drop(s);
        // And that seal is durable: the next start opens it through its footer.
        let (_, records) =
            Manifest::open(&manifest::path_in(&scratch.0), &mut |_| {}).expect("manifest");
        let live = manifest::live(&records);
        assert!(live.values().all(|f| f.sealed), "{live:?}");
    }

    /// The file being written has no footer, and a query still reads its row groups - from
    /// disk, through the entries its writer holds, and not from the buffer they left.
    #[test]
    fn the_open_file_is_read_through_its_writers_index() {
        let scratch = Scratch::new("open-index");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..10 {
            leaf.append(row(T0 + n, n));
        }
        s.flush(FlushKind::Tick);
        assert_eq!(leaf.buffered(), 0, "the rows are still buffered");
        let path = data_files(&scratch.0)
            .into_iter()
            .next()
            .expect("a data file");
        assert!(
            file::open_sealed(&path).expect("readable").is_none(),
            "the open file has a footer"
        );
        let scan = leaf
            .visit(
                &ScanOptions::default(),
                &[],
                &mut Rows(|_: &Row, _: u64| {}),
            )
            .expect("scan");
        assert_eq!((scan.matched, scan.row_groups_read), (10, 1));
        assert_eq!(ns(leaf.as_ref()), (0..10).collect::<Vec<_>>());
    }

    /// Two templates over two days: four row groups, two files, and one flush for each
    /// file - not one per row group.
    #[test]
    fn every_row_group_that_is_due_goes_out_before_one_flush_per_file() {
        let scratch = Scratch::new("one-flush");
        let s = store(&scratch);
        let a = s.open(7, "ts").expect("leaf");
        let b = s.open(8, "ts").expect("leaf");
        for n in 0..4 {
            a.append(row(T0 + n, n));
            b.append(row(T0 + DAY_NS + n, n));
            a.append(row(T0 + DAY_NS + n, 10 + n));
            b.append(row(T0 + n, 10 + n));
        }
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!(
            (done.rows, done.row_groups, done.syncs, done.sealed),
            (16, 4, 2, 0)
        );
        assert_eq!(data_files(&scratch.0).len(), 2);
        let done = s.flush(FlushKind::Final);
        assert_eq!((done.rows, done.syncs, done.sealed), (0, 0, 2));
    }

    /// The UTC day each data file's directory names, in path order.
    fn file_days(scratch: &Scratch) -> Vec<String> {
        data_files(&scratch.0)
            .iter()
            .map(|p| {
                let day = p.parent().and_then(|d| d.file_name()).unwrap_or_default();
                day.to_string_lossy().into_owned()
            })
            .collect()
    }

    /// Every row group of every sealed data file lies in the day its directory names.
    fn every_row_group_in_its_own_day(scratch: &Scratch) {
        for path in data_files(&scratch.0) {
            let dir = path
                .parent()
                .and_then(|d| d.file_name())
                .unwrap_or_default();
            let day = day_of_name(&dir.to_string_lossy()).expect("a day directory");
            let index = file::open_sealed(&path).expect("readable").expect("sealed");
            for e in &index.entries {
                assert_eq!(
                    (day_of(e.min_time), day_of(e.max_time)),
                    (day, day),
                    "{} holds a row group of another day",
                    path.display()
                );
            }
        }
    }

    /// Past the cap on open days, the least recently written file is sealed and a row whose
    /// day has no open file opens its own day's: no row group is ever in another day's
    /// file, before a restart or after.
    #[test]
    fn past_the_open_day_cap_a_late_row_opens_its_own_days_file() {
        let scratch = Scratch::new("late");
        let options = DiskOptions {
            max_open_files: 2,
            ..eager()
        };
        let first = day_of(T0);
        {
            let s = store_with(&scratch, options);
            let leaf = s.open(7, "ts").expect("leaf");
            for d in 0..3 {
                leaf.append(row(T0 + d * DAY_NS, d));
            }
            let done = s.flush(FlushKind::Tick);
            assert!(done.failed.is_empty(), "{:?}", done.failed);
            // The first day's file is flushed and sealed to make room for the third's.
            assert_eq!((done.syncs, done.sealed), (3, 1));
            assert_eq!(s.open_files(), (2, 2), "the cap was passed");
            assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
            s.flush(FlushKind::Final);
        }
        assert_eq!(
            file_days(&scratch),
            vec![day_name(first), day_name(first + 1), day_name(first + 2)],
            "a row went into another day's file"
        );
        every_row_group_in_its_own_day(&scratch);
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
    }

    /// Rows of one template and one timestamp keep their order across a restart when their
    /// day's file was sealed for the cap in between.
    ///
    /// Filed into today's file past the cap, the first went into `<today>/seg_0_000001.col`
    /// and the second, once a size seal had freed a slot, into its own day's
    /// `<day>/seg_0_000001.col` - which sorts first by path. The order a restart reads was
    /// then the reverse of the order they were published in: a cursor taken between them
    /// before the restart repeated one row and never returned the other.
    #[test]
    fn tied_rows_keep_their_order_across_a_restart_past_the_open_day_cap() {
        let scratch = Scratch::new("tied");
        let options = DiskOptions {
            max_open_files: 1,
            segment_max_bytes: 700,
            ..eager()
        };
        let tied = T0 + DAY_NS;
        let positions = |leaf: &dyn LeafStore| {
            let mut out = Vec::new();
            leaf.visit(
                &ScanOptions::default(),
                &[],
                &mut Rows(|r: &Row, seq: u64| out.push((r.get("n").cloned(), seq))),
            )
            .expect("scan");
            out.sort_by_key(|(_, seq)| *seq);
            out.into_iter().map(|(n, _)| n).collect::<Vec<_>>()
        };
        let before = {
            let s = store_with(&scratch, options);
            let a = s.open(7, "ts").expect("leaf");
            let b = s.open(8, "ts").expect("leaf");
            let c = s.open(9, "ts").expect("leaf");
            a.append(row(T0, 0));
            s.flush(FlushKind::Tick);
            b.append(row(tied, 1));
            s.flush(FlushKind::Tick);
            // Wide enough to take its file past the size cap.
            let mut wide = row(T0 + 2 * DAY_NS, 0);
            wide.insert("pad".into(), ColValue::String("f".repeat(600)));
            c.append(wide);
            s.flush(FlushKind::Tick);
            b.append(row(tied, 2));
            let done = s.flush(FlushKind::Tick);
            assert!(done.failed.is_empty(), "{:?}", done.failed);
            let before = positions(b.as_ref());
            s.flush(FlushKind::Final);
            before
        };
        assert_eq!(
            before,
            vec![Some(ColValue::Int64(1)), Some(ColValue::Int64(2))]
        );
        let s = store_with(&scratch, options);
        let b = s.open(8, "ts").expect("leaf");
        assert_eq!(
            positions(b.as_ref()),
            before,
            "a restart reordered tied rows"
        );
        every_row_group_in_its_own_day(&scratch);
    }

    /// A day's file past the size cap is sealed after the flush that took it there, and the
    /// next row for the day opens the next number.
    #[test]
    fn a_file_past_the_size_cap_is_sealed_and_the_next_number_opened() {
        let scratch = Scratch::new("size-cap");
        let options = DiskOptions {
            segment_max_bytes: 1,
            ..eager()
        };
        let s = store_with(&scratch, options);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..3 {
            leaf.append(row(T0 + n, n));
            let done = s.flush(FlushKind::Tick);
            assert_eq!((done.syncs, done.sealed), (1, 1));
        }
        let files: Vec<PathBuf> = data_files(&scratch.0).into_iter().collect();
        assert_eq!(files.len(), 3);
        for path in &files {
            assert!(file::open_sealed(path).expect("readable").is_some());
        }
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
    }

    /// Past the memory limit the largest buffer closes first, though no threshold has
    /// fired, and the small one waits for its own.
    #[test]
    fn past_the_buffer_limit_the_largest_buffer_closes_first() {
        let scratch = Scratch::new("limit");
        // What one row holds, by the store's own count.
        let each = {
            let probe = store_with(&scratch, DiskOptions::default());
            probe.open(9, "ts").expect("leaf").append(row(T0, 0));
            probe.buffer_bytes().0
        };
        let options = DiskOptions {
            max_buffer_bytes: 20 * each,
            ..DiskOptions::default()
        };
        let s = store_with(&scratch, options);
        let big = s.open(7, "ts").expect("leaf");
        let small = s.open(8, "ts").expect("leaf");
        for n in 0..40 {
            big.append(row(T0 + n, n));
        }
        small.append(row(T0, 0));
        assert_eq!(s.buffer_bytes().0, 41 * each);
        let done = s.flush(FlushKind::Tick);
        assert_eq!((done.forced, done.rows), (1, 40));
        assert_eq!((big.buffered(), small.buffered()), (0, 1));
        assert_eq!(s.buffer_bytes().0, each);
    }

    /// A flush that cannot write puts the rows back where they were: still buffered, still
    /// answered once each, and written by the first flush that can.
    #[test]
    #[cfg(unix)]
    fn rows_a_flush_cannot_write_go_back_to_their_buffer() {
        use std::os::unix::fs::PermissionsExt;
        let scratch = Scratch::new("restore");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..5 {
            leaf.append(row(T0 + n, n));
        }
        let data = scratch.0.join(DATA_DIR);
        fs::set_permissions(&data, fs::Permissions::from_mode(0o555)).expect("chmod");
        let done = s.flush(FlushKind::Tick);
        fs::set_permissions(&data, fs::Permissions::from_mode(0o755)).expect("chmod");
        assert_eq!(done.failed.len(), 1, "the flush did not fail");
        assert_eq!(leaf.buffered(), 5);
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4]);

        leaf.append(row(T0 + 5, 5));
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!((done.rows, leaf.buffered()), (6, 0));
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 5]);
        // In their order: the restored rows went back ahead of the one that came after.
        let mut seqs = Vec::new();
        leaf.visit(
            &ScanOptions::default(),
            &[],
            &mut Rows(|r: &Row, seq: u64| seqs.push((r.get("n").cloned(), seq))),
        )
        .expect("scan");
        for (n, seq) in seqs {
            assert_eq!(n, Some(ColValue::Int64(seq as i64)));
        }
    }

    /// A load's file for template `id`, rows `ns` on T0's day: staged under the load's own
    /// name, sealed and flagged finished, as the loader leaves one.
    fn staged(dir: &Path, name: &str, id: u64, ns: std::ops::Range<i64>) -> StagedFile {
        staged_at(dir, name, id, T0, ns)
    }

    /// `staged`, with rows at `base + n` and on `base`'s day.
    fn staged_at(
        dir: &Path,
        name: &str,
        id: u64,
        base: i64,
        ns: std::ops::Range<i64>,
    ) -> StagedFile {
        let path = dir.join(name);
        let mut writer = Writer::create(&path).expect("create");
        writer.finish();
        let rows: Vec<Row> = ns.clone().map(|n| row(base + n, n)).collect();
        let refs: Vec<&Row> = rows.iter().collect();
        let times: Vec<i64> = ns.clone().map(|n| base + n).collect();
        let columns = schema_of(refs.iter().copied());
        let encoded = build::encode(
            id,
            &columns,
            &refs,
            &times,
            Source::Load {
                stream: 9,
                first: ns.start as u64 + 1,
                last: ns.end as u64,
            },
        )
        .expect("encode");
        let schema_id = writer.schema_id(&columns).expect("schema");
        writer
            .append(id, &encoded.frame(schema_id))
            .expect("append");
        StagedFile {
            day: day_of(base),
            path,
            index: writer.seal().expect("seal"),
        }
    }

    /// A commit lands in an open leaf at once, behind its live rows; a second commit of the
    /// same load is refused by its id; and it is all there after a restart.
    #[test]
    fn a_committed_load_is_read_at_once_and_after_a_restart() {
        let scratch = Scratch::new("commit");
        {
            let s = store(&scratch);
            let leaf = s.open(7, "ts").expect("leaf");
            leaf.append(row(T0 + 100, 100));
            let dir = s.stage(9).expect("stage");
            let file = staged(&dir, "a.col", 7, 0..5);
            assert!(!s.was_loaded(9));
            let done = s
                .commit_load(LoadCommit {
                    load_id: 9,
                    files: vec![file],
                    replace_day: None,
                })
                .expect("commit");
            assert_eq!(
                done.added,
                vec![format!("{}/seg_0_000001.col", day_name(day_of(T0)))]
            );
            assert!(!dir.exists(), "staging outlived its commit");
            assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 100]);
            assert!(s.was_loaded(9));

            let again = s.stage(9).expect("stage");
            let refused = s.commit_load(LoadCommit {
                load_id: 9,
                files: vec![staged(&again, "a.col", 7, 0..5)],
                replace_day: None,
            });
            assert!(matches!(
                refused,
                Err(StoreError::LoadRepeated { load_id: 9 })
            ));
            s.unstage(9);
            s.flush(FlushKind::Final);
        }
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 100]);
        let (_, records) =
            Manifest::open(&manifest::path_in(&scratch.0), &mut |_| {}).expect("manifest");
        let finished = manifest::live(&records)
            .into_values()
            .filter(|f| f.finished)
            .count();
        assert_eq!(finished, 1, "the load's file is not flagged finished");
    }

    /// A template no leaf is open for takes the load's row groups when it opens.
    #[test]
    fn a_load_for_a_template_with_no_leaf_waits_for_the_leaf() {
        let scratch = Scratch::new("no-leaf");
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![staged(&dir, "a.col", 8, 0..3)],
            replace_day: None,
        })
        .expect("commit");
        let leaf = s.open(8, "ts").expect("leaf");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
    }

    /// The record is the commit: a stop after it and before the move leaves the files in
    /// staging, and the next start moves them rather than deleting them.
    #[test]
    fn a_load_stopped_between_its_record_and_its_move_is_moved_at_the_next_start() {
        let scratch = Scratch::new("finish-move");
        let rel = format!("{}/seg_0_000001.col", day_name(day_of(T0)));
        {
            let s = store(&scratch);
            let dir = s.stage(9).expect("stage");
            let file = staged(&dir, "a.col", 7, 0..4);
            // `commit_load`'s record, and no move: the process stops here.
            let mut files = lock(&s.files);
            files
                .record(vec![Record::Add {
                    files: vec![FileRecord::sealed(rel.clone(), &file.index)],
                    source: Some(manifest::load_source(9)),
                }])
                .expect("record");
        }
        assert!(!scratch.0.join(DATA_DIR).join(&rel).exists());
        let s = store(&scratch);
        assert!(scratch.0.join(DATA_DIR).join(&rel).exists(), "not moved");
        assert!(!scratch.0.join(STAGING_DIR).exists(), "staging survived");
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3]);
        assert!(s.was_loaded(9));
    }

    /// A committed file found neither in `data/` nor in staging stops the start with
    /// staging untouched - never after the only copy of a load's rows is deleted.
    #[test]
    fn a_committed_load_missing_from_staging_stops_the_start_and_keeps_staging() {
        let scratch = Scratch::new("fail-closed");
        let rel = format!("{}/seg_0_000001.col", day_name(day_of(T0)));
        {
            let s = store(&scratch);
            let dir = s.stage(9).expect("stage");
            let file = staged(&dir, "a.col", 7, 0..4);
            let other = staged(&dir, "b.col", 7, 4..6);
            let mut files = lock(&s.files);
            files
                .record(vec![Record::Add {
                    files: vec![
                        FileRecord::sealed(rel.clone(), &file.index),
                        FileRecord::sealed(
                            format!("{}/seg_0_000002.col", day_name(day_of(T0))),
                            &other.index,
                        ),
                    ],
                    source: Some(manifest::load_source(9)),
                }])
                .expect("record");
            // One of the two is lost - a bad sector, a hand that tidied up.
            fs::remove_file(&other.path).expect("remove");
        }
        assert!(DiskStore::open(scratch.0.clone(), eager()).is_err());
        assert!(
            files_under(&scratch.0.join(STAGING_DIR))
                .expect("staging")
                .iter()
                .any(|p| p.ends_with("a.col")),
            "the start deleted the staged copy it could still have moved"
        );
    }

    /// A committed load whose files are all in place is not held up by what else sits in
    /// its staging directory: a later run of the same inputs cut short at a file's header.
    #[test]
    fn a_stray_file_in_a_moved_loads_staging_does_not_stop_the_start() {
        let scratch = Scratch::new("stray");
        {
            let s = store(&scratch);
            let dir = s.stage(9).expect("stage");
            s.commit_load(LoadCommit {
                load_id: 9,
                files: vec![staged(&dir, "a.col", 7, 0..3)],
                replace_day: None,
            })
            .expect("commit");
            // The same inputs again, with --replace-day, stopped inside a file's header.
            let again = scratch.0.join(STAGING_DIR).join(format!("{:016x}", 9));
            fs::create_dir_all(again.join("2024-01-15")).expect("mkdir");
            fs::write(again.join("2024-01-15").join("000001.col"), b"").expect("write");
        }
        let s = store(&scratch);
        assert_eq!(ns(s.open(7, "ts").expect("leaf").as_ref()), vec![0, 1, 2]);
        assert!(!scratch.0.join(STAGING_DIR).exists(), "staging survived");
    }

    /// While a committed load's files are still in staging, no load stages - the same load
    /// again would stage into the same directory, over the only copy.
    #[test]
    fn nothing_stages_over_a_committed_load_left_in_staging() {
        let scratch = Scratch::new("move-pending");
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        let file = staged(&dir, "a.col", 7, 0..4);
        // The move fails: something that is not a directory sits where the day's is.
        let day_dir = scratch.0.join(DATA_DIR).join(day_name(day_of(T0)));
        fs::write(&day_dir, b"in the way").expect("block");
        let failed = s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![file],
            replace_day: None,
        });
        assert!(matches!(
            failed,
            Err(StoreError::NotMoved { load_id: 9, .. })
        ));
        fs::remove_file(&day_dir).expect("unblock");
        assert!(matches!(
            s.stage(9),
            Err(StoreError::MovePending { load_id: 9 })
        ));
        assert!(matches!(
            s.stage(10),
            Err(StoreError::MovePending { load_id: 9 })
        ));
        assert!(dir.join("a.col").exists(), "the only copy went");
        drop(s);
        // The next start finishes the move.
        let s = store(&scratch);
        assert_eq!(
            ns(s.open(7, "ts").expect("leaf").as_ref()),
            vec![0, 1, 2, 3]
        );
    }

    /// Before the record, a load is only its staging directory, which the next start
    /// deletes: no row of it, and no file.
    #[test]
    fn a_load_stopped_before_its_record_leaves_nothing() {
        let scratch = Scratch::new("undo");
        {
            let s = store(&scratch);
            let dir = s.stage(9).expect("stage");
            staged(&dir, "a.col", 7, 0..4);
        }
        let s = store(&scratch);
        assert!(!scratch.0.join(STAGING_DIR).exists(), "staging survived");
        assert!(data_files(&scratch.0).is_empty());
        assert!(!s.was_loaded(9));
        assert!(ns(s.open(7, "ts").expect("leaf").as_ref()).is_empty());
    }

    /// Replace-a-day writes the day's rows still in memory, retires every file of the day
    /// in the record that adds the load's, and deletes a retired file only once the last
    /// query holding it lets go.
    #[test]
    fn replace_a_day_retires_the_day_and_deletes_its_files_after_the_last_reader() {
        let scratch = Scratch::new("replace");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..3 {
            leaf.append(row(T0 + n, 50 + n));
        }
        s.flush(FlushKind::Tick);
        leaf.append(row(T0 + 9, 59));
        let old: Vec<PathBuf> = data_files(&scratch.0).into_iter().collect();
        assert_eq!(old.len(), 1);
        // A query's snapshot of the day, taken before the replace.
        let held = {
            let leaves = s.leaves.read().expect("leaves");
            let state = leaves[&7].state();
            Arc::clone(&state.days[&day_of(T0)].stored)
        };
        let dir = s.stage(9).expect("stage");
        let done = s
            .commit_load(LoadCommit {
                load_id: 9,
                files: vec![staged(&dir, "a.col", 7, 0..5)],
                replace_day: Some(day_of(T0)),
            })
            .expect("replace");
        assert_eq!(done.replaced.len(), 1, "{done:?}");
        assert_eq!(
            ns(leaf.as_ref()),
            vec![0, 1, 2, 3, 4],
            "the day holds only the load"
        );
        assert!(old[0].exists(), "deleted under a reader");
        drop(held);
        assert!(!old[0].exists(), "a retired file outlived its last reader");
        drop(leaf);
        drop(s);
        let s = store(&scratch);
        assert_eq!(
            ns(s.open(7, "ts").expect("leaf").as_ref()),
            vec![0, 1, 2, 3, 4]
        );
    }

    /// A day that passed through the cap on open days - its file sealed to make room for
    /// another day's, and opened again - is replaced like any other: every file of it is in
    /// its own directory, so retiring the directory retires the day and nothing else.
    #[test]
    fn replace_a_day_after_the_open_day_cap_retires_that_day_and_no_other() {
        let scratch = Scratch::new("replace-capped");
        let options = DiskOptions {
            max_open_files: 1,
            ..eager()
        };
        let s = store_with(&scratch, options);
        let leaf = s.open(7, "ts").expect("leaf");
        let late = day_of(T0 + DAY_NS);
        leaf.append(row(T0, 0));
        leaf.append(row(T0 + DAY_NS, 1));
        s.flush(FlushKind::Tick);
        leaf.append(row(T0 + 1, 2));
        s.flush(FlushKind::Tick);
        leaf.append(row(T0 + DAY_NS + 1, 3));
        s.flush(FlushKind::Tick);
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3]);
        let dir = s.stage(9).expect("stage");
        let file = staged_at(&dir, "a.col", 7, T0 + DAY_NS, 10..12);
        let done = s
            .commit_load(LoadCommit {
                load_id: 9,
                files: vec![file],
                replace_day: Some(late),
            })
            .expect("a day that passed through the cap is replaced");
        assert!(
            done.replaced
                .iter()
                .all(|rel| rel.starts_with(&format!("{}/", day_name(late)))),
            "{done:?}"
        );
        assert_eq!(
            ns(leaf.as_ref()),
            vec![0, 2, 10, 11],
            "the other day lost rows, or the replaced day kept some"
        );
        s.flush(FlushKind::Final);
        every_row_group_in_its_own_day(&scratch);
    }

    /// Each row's `n` from one template of a snapshot, sorted.
    fn snapshot_ns(snap: &mut dyn Snapshot, id: u64) -> Vec<i64> {
        let mut out = Vec::new();
        let opts = ScanOptions {
            ts_column: Some("ts".into()),
            ..ScanOptions::default()
        };
        snap.visit(
            id,
            &opts,
            &[],
            &mut Rows(|r: &Row, _: u64| match r.get("n") {
                Some(ColValue::Int64(n)) => out.push(*n),
                other => panic!("n is {other:?}"),
            }),
        )
        .expect("a leaf")
        .expect("scan");
        out.sort_unstable();
        out
    }

    /// A snapshot answers as the store stood when it was taken, for every template in it: a
    /// load committed after it - over two templates - shows in neither, and a snapshot
    /// taken after shows it in both. A load publishes every template in one step, and the
    /// snapshot is taken in one step too, so no query can see one template's share of a
    /// load and not another's.
    #[test]
    fn a_snapshot_sees_a_load_in_every_template_or_in_none() {
        let scratch = Scratch::new("snapshot");
        let s = store(&scratch);
        let a = s.open(7, "ts").expect("leaf");
        let b = s.open(8, "ts").expect("leaf");
        a.append(row(T0 + 100, 100));
        b.append(row(T0 + 200, 200));
        let leaves =
            || -> Vec<(u64, Arc<dyn LeafStore>)> { vec![(7, Arc::clone(&a)), (8, Arc::clone(&b))] };
        let opts = ScanOptions::default();
        let mut before = s.snapshot(leaves(), &opts);
        let generation = s.generation();
        assert_eq!(before.generation(), generation);

        let dir = s.stage(9).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![
                staged(&dir, "a.col", 7, 0..3),
                staged(&dir, "b.col", 8, 10..12),
            ],
            replace_day: None,
        })
        .expect("commit");
        assert_eq!(s.generation(), generation + 1, "a publish is not counted");

        assert_eq!(snapshot_ns(before.as_mut(), 7), vec![100]);
        assert_eq!(snapshot_ns(before.as_mut(), 8), vec![200]);
        let mut after = s.snapshot(leaves(), &opts);
        assert_eq!(snapshot_ns(after.as_mut(), 7), vec![0, 1, 2, 100]);
        assert_eq!(snapshot_ns(after.as_mut(), 8), vec![10, 11, 200]);
        assert!(before
            .visit(9, &opts, &[], &mut Rows(|_: &Row, _| {}))
            .is_none());
    }

    /// A day a replace retires after a snapshot was taken is still read, whole, through
    /// that snapshot - its file stays on disk while the snapshot holds it - and the file
    /// goes when the snapshot does.
    #[test]
    fn a_snapshot_reads_a_replaced_day_until_it_is_dropped() {
        let scratch = Scratch::new("snapshot-replace");
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..3 {
            leaf.append(row(T0 + n, 50 + n));
        }
        s.flush(FlushKind::Final);
        let old: Vec<PathBuf> = data_files(&scratch.0).into_iter().collect();
        let opts = ScanOptions::default();
        let mut held = s.snapshot(vec![(7, Arc::clone(&leaf))], &opts);
        let dir = s.stage(9).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![staged(&dir, "a.col", 7, 0..5)],
            replace_day: Some(day_of(T0)),
        })
        .expect("replace");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4]);
        assert!(old[0].exists(), "deleted under a snapshot");
        assert_eq!(
            snapshot_ns(held.as_mut(), 7),
            vec![50, 51, 52],
            "the snapshot lost the day it was taken with"
        );
        drop(held);
        assert!(
            !old[0].exists(),
            "a retired file outlived its last snapshot"
        );
    }

    /// A day a file found at startup shares with another day - a directory written while a
    /// late row still went into today's file - is not replaced: retiring its directory
    /// would take the other day's rows, or leave some of its own. Both days are refused,
    /// naming the file; any other day is replaced as usual.
    #[test]
    fn replace_a_day_refuses_a_day_a_file_from_before_shares_with_another() {
        let scratch = Scratch::new("replace-mixed");
        let first = day_of(T0);
        let late = first + 1;
        {
            let s = store(&scratch);
            // What the spill left: a file under the first day's directory holding a row
            // group of the next day's.
            let dir = s.stage(9).expect("stage");
            let file = StagedFile {
                day: first,
                ..staged_at(&dir, "a.col", 7, T0 + DAY_NS, 0..2)
            };
            s.commit_load(LoadCommit {
                load_id: 9,
                files: vec![file],
                replace_day: None,
            })
            .expect("commit");
            let dir = s.stage(10).expect("stage");
            s.commit_load(LoadCommit {
                load_id: 10,
                files: vec![staged_at(&dir, "b.col", 7, T0 + 2 * DAY_NS, 5..6)],
                replace_day: None,
            })
            .expect("commit");
        }
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for (load_id, day) in [(11, first), (12, late)] {
            let dir = s.stage(load_id).expect("stage");
            let refused = s.commit_load(LoadCommit {
                load_id,
                files: vec![staged_at(&dir, "c.col", 7, day * DAY_NS, 20..21)],
                replace_day: Some(day),
            });
            match refused {
                Err(StoreError::ReplaceRefused { path, .. }) => {
                    assert!(path.starts_with(&day_name(first)), "{path}")
                }
                other => panic!("replaced a day a mixed file shares: {other:?}"),
            }
            s.unstage(load_id);
        }
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 5], "a refused replace changed rows");
        let dir = s.stage(13).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 13,
            files: vec![staged_at(&dir, "d.col", 7, T0 + 2 * DAY_NS, 30..31)],
            replace_day: Some(first + 2),
        })
        .expect("a day no mixed file touches is replaced");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 30]);
    }

    /// At the cap, the file sealed to make room is one no later batch of the flush wants:
    /// a flush writing no more days than the cap never seals a day and opens it again as its
    /// next number.
    #[test]
    fn at_the_cap_a_day_the_flush_still_wants_is_not_the_one_sealed() {
        let scratch = Scratch::new("victim");
        let options = DiskOptions {
            max_open_files: 2,
            ..eager()
        };
        let s = store_with(&scratch, options);
        let leaf = s.open(7, "ts").expect("leaf");
        leaf.append(row(T0 + 9 * DAY_NS, 9));
        s.flush(FlushKind::Tick);
        leaf.append(row(T0 + 3 * DAY_NS, 3));
        s.flush(FlushKind::Tick);
        // The ninth day, open since the first flush, is the least recently written - and
        // this flush wants it again. The third day is the one to seal.
        leaf.append(row(T0 + DAY_NS, 1));
        leaf.append(row(T0 + 9 * DAY_NS + 1, 10));
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!(done.sealed, 1, "a day was sealed and opened again");
        s.flush(FlushKind::Final);
        let names: Vec<String> = data_files(&scratch.0)
            .iter()
            .map(|p| p.file_name().unwrap_or_default().to_string_lossy().into_owned())
            .collect();
        assert!(
            names.iter().all(|n| n == "seg_0_000001.col"),
            "a day opened a second file: {names:?}"
        );
        assert_eq!(ns(leaf.as_ref()), vec![1, 3, 9, 10]);
        every_row_group_in_its_own_day(&scratch);
    }

    /// The final flush seals one file per open day, however many templates wrote to it.
    #[test]
    fn the_final_flush_seals_one_file_per_open_day() {
        let scratch = Scratch::new("final");
        let s = store_with(&scratch, DiskOptions::default());
        for id in 0..50u64 {
            let leaf = s.open(id, "ts").expect("leaf");
            for d in 0..3 {
                leaf.append(row(T0 + d * DAY_NS, id as i64));
            }
        }
        let done = s.flush(FlushKind::Final);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!(
            (done.rows, done.row_groups, done.syncs, done.sealed),
            (150, 150, 3, 3)
        );
        assert_eq!(s.open_files().0, 0);
    }
}
