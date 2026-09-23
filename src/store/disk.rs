//! The disk store: one shared file per UTC day, named in `manifest.log`.
//!
//! ```text
//! <base>/
//! ├── catalog.log                 the truth about templates
//! ├── manifest.log                the truth about files
//! ├── staging/                    bulk loads in flight; deleted at startup
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
//! **Startup.** The manifest names the live files. A file it does not name is deleted, and
//! so is `staging/`. A named file that is sealed opens through its footer and nothing else;
//! one that is not was left by a crash, and is recovered - walked, cut at the tear, sealed
//! - and the seal recorded. A directory holding the per-template layout is refused by path.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::time::{Instant, SystemTime};

use chrono::{DateTime, NaiveDate};

use super::manifest::{self, FileRecord, Manifest, Record};
use super::v3::build::{self, schema_of, Encoded};
use super::v3::file::{self, Entry, Index, Mark, Reader, Writer};
use super::v3::{day_of, FormatError, Source, DAY_NS};
use super::{
    effective_ts, in_window, lock, nanos_of, render_row, value_bytes, DiskOptions, FlushKind,
    Flushed, LeafStore, Predicate, Row, RowGroupLimits, Scan, ScanOptions, Store, StoreError,
    Visitor, DEFAULT_TS_COLUMN,
};

pub const DATA_DIR: &str = "data";
pub const STAGING_DIR: &str = "staging";

/// Seal a day's file at 512MB and start the next number. Walking an unsealed 1GB file at
/// startup and checking every CRC takes about 0.3 seconds, so either is fine.
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 512 * 1024 * 1024;

fn warn(message: String) {
    eprintln!("triedb: WARN {message}");
}

/// Today, as days since the epoch, by the system clock.
fn today_utc() -> i64 {
    day_of(nanos_of(SystemTime::now()))
}

// ── Names ──────────────────────────────────────────────────────────────────────────

/// `2024-01-15` for a day since the epoch.
fn day_name(day: i64) -> String {
    DateTime::from_timestamp(day * 86_400, 0).map_or_else(
        || format!("day_{day}"),
        |dt| dt.format("%Y-%m-%d").to_string(),
    )
}

fn day_of_name(name: &str) -> Option<i64> {
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
/// **Header bytes only, and before the port is taken.** This writes nothing, so it can
/// run before the bind; everything that writes - recovery, deleting unnamed files - waits
/// until the bind has shown no other server is running here. Paths are checked in order,
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
#[derive(Debug)]
pub struct DataFile {
    pub path: PathBuf,
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
    /// The flush that last wrote to it. Past the cap on open files the least recently
    /// written is sealed first.
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
/// **Behind the flush's own lock.** Only a flush takes it - the tick, the final flush - so
/// it is held across the encode, the write and the fsync, and no append and no query ever
/// waits on it.
struct Files {
    data: PathBuf,
    manifest: Manifest,
    open: BTreeMap<i64, Open>,
    /// The next file number per day directory.
    next: BTreeMap<i64, u32>,
    max_open: usize,
    max_bytes: u64,
    /// Flushes so far: the clock `Open::written` is kept by.
    flushes: u64,
}

impl Files {
    /// The day directory whose file a row group of `day` goes into.
    ///
    /// Its own day's, when that is open or there is room to open it. **Past the cap on open
    /// days, today's**, as a row group of its own day among today's. A reader skips a file
    /// by the time range its index holds and never by its directory's name, so a late row is
    /// found wherever it went; and a burst of rows from a hundred old days costs today's
    /// file a hundred row groups instead of costing the process a hundred open files.
    fn target(&self, day: i64, today: i64) -> i64 {
        if self.open.contains_key(&day) || self.open.len() < self.max_open.max(1) {
            day
        } else {
            today
        }
    }

    /// The open file in `dir`'s directory, created and named in the manifest if there is
    /// none. Never refused for the cap: `settle` brings the count back under it once the
    /// flush has written, so no file this flush wrote to is sealed before it is flushed.
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
            let named = self.manifest.append(&[Record::Add {
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
                    file: Arc::new(DataFile { path }),
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

    /// Append one moved-aside batch's row groups, encoded, to its day's file - or today's -
    /// and the directory it went to. Not flushed: a flush appends every batch that is due
    /// first, and then flushes each file it wrote to once.
    fn write(
        &mut self,
        moved: &Moved,
        encoded: Vec<EncodedRowGroup>,
        today: i64,
    ) -> Result<(i64, Vec<Stored>), StoreError> {
        // A batch that did not encode whole touches no file.
        let encoded = encoded.into_iter().collect::<Result<Vec<_>, _>>()?;
        let dir = self.target(moved.day, today);
        let flushes = self.flushes;
        let open = self.open_in(dir)?;
        open.written = flushes;
        let mark = open.writer.mark();
        open.began.get_or_insert(mark);
        match append_row_groups(open, moved.leaf.template_id, encoded) {
            Ok(stored) => Ok((dir, stored)),
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

    /// After a flush: seal every file past the size cap, and then the least recently
    /// written until no more than the cap are open - never today's, which is where late rows
    /// go. What was sealed, and what could not be.
    fn settle(&mut self, today: i64) -> (usize, Vec<StoreError>) {
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
        let mut seal: Vec<i64> = self
            .open
            .iter()
            .filter(|(_, open)| open.writer.len() >= self.max_bytes)
            .map(|(day, _)| *day)
            .collect();
        let cap = self.max_open.max(1);
        let mut left = self.open.len() - seal.len();
        if left > cap {
            let mut idle: Vec<(u64, i64)> = self
                .open
                .iter()
                .filter(|(day, _)| **day != today && !seal.contains(day))
                .map(|(day, open)| (open.written, *day))
                .collect();
            idle.sort_unstable();
            for (_, day) in idle {
                if left <= cap {
                    break;
                }
                seal.push(day);
                left -= 1;
            }
        }
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
            if let Err(e) = self.manifest.append(&[Record::Seal { files: records }]) {
                errors.push(e.into());
            }
        }
        (sealed, errors)
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

// ── The store ──────────────────────────────────────────────────────────────────────

pub struct DiskStore {
    base: PathBuf,
    limits: RowGroupLimits,
    max_open: usize,
    max_buffer_bytes: u64,
    /// Threads a flush encodes row groups on: one per core, up to eight.
    encoders: usize,
    files: Mutex<Files>,
    /// Every leaf this store has opened, for a flush to walk.
    leaves: RwLock<Vec<Arc<DiskLeaf>>>,
    /// Row groups on disk at startup, by template, in storage order. A leaf takes its own
    /// when it opens; a template no catalog record names keeps them here, unread.
    stored: Mutex<BTreeMap<u64, Vec<Stored>>>,
    claimed: Vec<u64>,
    /// Memory held by every leaf's buffered rows and by the closes in flight, as
    /// `Row::held_bytes` estimates it: what the memory limit is held against.
    buffered: Arc<AtomicU64>,
    /// Files open for writing as the last flush left them. Read by `/stats`, without the
    /// files lock a flush holds across the disk.
    open_count: AtomicUsize,
    /// Today, as days since the epoch: where a late row goes once the open days are
    /// capped. A function and not a call to the clock, so a test can say when today is.
    today: fn() -> i64,
}

impl DiskStore {
    /// Open the data directory: refuse an earlier layout, read the manifest, delete what it
    /// does not name and any leftover staging, recover every file a crash left unsealed,
    /// and index every row group by template.
    pub fn open(base: PathBuf, options: DiskOptions) -> Result<Self, StoreError> {
        fs::create_dir_all(&base).map_err(|source| StoreError::Io {
            template_id: 0,
            source,
        })?;
        refuse_other_versions(&base)?;
        let (mut manifest, records) = Manifest::open(&manifest::path_in(&base), &mut |m| {
            warn(format!("manifest.log - {m}"))
        })?;
        let live = manifest::live(&records);

        let staging = base.join(STAGING_DIR);
        if staging.exists() {
            warn(format!(
                "{}: a bulk load that never committed; deleted",
                staging.display()
            ));
            fs::remove_dir_all(&staging).map_err(|source| FormatError::Io {
                path: staging.clone(),
                source,
            })?;
        }

        let data = base.join(DATA_DIR);
        fs::create_dir_all(&data).map_err(|source| FormatError::Io {
            path: data.clone(),
            source,
        })?;
        delete_unnamed(&data, &live)?;

        let mut next: BTreeMap<i64, u32> = BTreeMap::new();
        let mut stored: BTreeMap<u64, Vec<Stored>> = BTreeMap::new();
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
            index_rows(&mut stored, Arc::new(DataFile { path }), &index);
        }
        if !seals.is_empty() {
            manifest.append(&[Record::Seal { files: seals }])?;
        }
        let claimed = stored.keys().copied().collect();
        Ok(Self {
            base,
            limits: options.row_groups,
            max_open: options.max_open_files,
            max_buffer_bytes: options.max_buffer_bytes,
            encoders: std::thread::available_parallelism().map_or(1, |n| n.get().min(8)),
            files: Mutex::new(Files {
                data,
                manifest,
                open: BTreeMap::new(),
                next,
                max_open: options.max_open_files,
                max_bytes: options.segment_max_bytes,
                flushes: 0,
            }),
            leaves: RwLock::new(Vec::new()),
            stored: Mutex::new(stored),
            claimed,
            buffered: Arc::new(AtomicU64::new(0)),
            open_count: AtomicUsize::new(0),
            today: today_utc,
        })
    }

    /// The same store, with `today` saying which day is today.
    #[cfg(test)]
    fn with_today(mut self, today: fn() -> i64) -> Self {
        self.today = today;
        self
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
        self.leaves
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::clone(&leaf));
        Ok(leaf)
    }

    /// One flush, in the three steps the invariant at `DiskLeaf::state` needs, and then the
    /// seals.
    fn flush(&self, kind: FlushKind) -> Flushed {
        let mut files = lock(&self.files);
        files.flushes += 1;
        let leaves: Vec<Arc<DiskLeaf>> = self
            .leaves
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let now = Instant::now();
        let mut done = Flushed::default();

        // ── 1. Move the rows that are due aside: one short hold of each leaf's lock ──
        let mut moved: Vec<Moved> = Vec::new();
        for leaf in &leaves {
            leaf.move_due(kind, now, &mut moved);
        }
        if !kind.is_final() {
            done.forced = self.relieve(&leaves, &mut moved);
        }

        // ── 2. Encode, write and flush them, holding no leaf's lock ──
        //
        // Encoded side by side first - nearly all of the cost, and none of it needs the
        // file - then numbered, framed and appended one at a time, in order.
        let today = (self.today)();
        let jobs = jobs_of(&moved, &self.limits);
        let mut encoded = encode_all(&moved, &jobs, self.encoders).into_iter();
        let mut written: Vec<Result<Vec<Stored>, StoreError>> = Vec::with_capacity(moved.len());
        let mut by_file: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
        let mut first_job = 0;
        for (at, m) in moved.iter().enumerate() {
            let count = jobs[first_job..]
                .iter()
                .take_while(|j| j.moved == at)
                .count();
            first_job += count;
            let mine: Vec<_> = encoded.by_ref().take(count).collect();
            written.push(match files.write(m, mine, today) {
                Ok((dir, stored)) => {
                    by_file.entry(dir).or_default().push(at);
                    Ok(stored)
                }
                Err(e) => Err(e),
            });
        }
        for (dir, ats) in by_file {
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

        // ── 4. Seal: every file on a final flush, and otherwise those a cap says to ──
        let (sealed, seal_failed) = if kind.is_final() {
            files.seal_all()
        } else {
            files.settle(today)
        };
        done.sealed = sealed;
        done.seal_failed = seal_failed;
        self.open_count.store(files.open.len(), Ordering::Relaxed);
        done
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

    /// Move aside every buffer a threshold says is due - every buffer, on a final flush.
    fn move_due(self: &Arc<Self>, kind: FlushKind, now: Instant, out: &mut Vec<Moved>) {
        let mut state = self.state();
        for (day, part) in state.days.iter_mut() {
            if self.due(part, kind, now) {
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
        let since = opts.since.map(nanos_of);
        let until = opts.until.map(nanos_of);
        let windowed = since.is_some() || until.is_some();
        let mut out = Scan::default();
        let views: Vec<View> = {
            let state = self.state();
            let mut views = Vec::new();
            for (day, part) in state.days.iter().rev() {
                if windowed && !Self::overlaps(*day, part.span(), since, until) {
                    out.row_groups_pruned += part.stored.len() as u64;
                    continue;
                }
                views.push(part.view());
            }
            views
        };
        let mut readers = HashMap::new();
        for view in &views {
            if !v.wants_partition(view.span.map(|(_, hi)| hi)) {
                // The visitor has no use for a row from here, but a limit bounds the rows in
                // the answer and not the rows that matched: the day is still counted, free
                // when nothing inside it could reject a row, and read otherwise.
                if let Some(matched) = Self::countable(view, preds, since, until) {
                    out.matched += matched;
                    continue;
                }
            }
            if !Self::visit_day(view, opts, preds, v, &mut out, &mut readers)? {
                break;
            }
        }
        Ok(out)
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

    /// Past the cap on open days, a row whose day has no open file goes into today's file,
    /// as a row group of its own day - and is found there, before a restart and after.
    #[test]
    fn past_the_open_day_cap_a_late_row_goes_into_todays_file() {
        const TODAY: i64 = 20_000;
        let scratch = Scratch::new("late");
        let options = DiskOptions {
            max_open_files: 2,
            ..eager()
        };
        let first = day_of(T0);
        {
            let s = store_with(&scratch, options).with_today(|| TODAY);
            let leaf = s.open(7, "ts").expect("leaf");
            for d in 0..3 {
                leaf.append(row(T0 + d * DAY_NS, d));
            }
            let done = s.flush(FlushKind::Tick);
            assert!(done.failed.is_empty(), "{:?}", done.failed);
            assert_eq!(done.syncs, 3);
            assert_eq!(s.open_files(), (2, 2), "the cap was not restored");
            assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
            s.flush(FlushKind::Final);
        }
        let names: Vec<String> = data_files(&scratch.0)
            .iter()
            .map(|p| {
                let day = p.parent().and_then(|d| d.file_name()).unwrap_or_default();
                day.to_string_lossy().into_owned()
            })
            .collect();
        assert_eq!(
            names,
            vec![day_name(first), day_name(first + 1), day_name(TODAY)],
            "the third day did not go into today's file"
        );
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
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
