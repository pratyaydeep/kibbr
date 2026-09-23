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
//! query reads a day's row groups and then its buffer, newest day first, and prunes on
//! each row group's time range before a byte of column data is read.
//!
//! **Startup.** The manifest names the live files. A file it does not name is deleted, and
//! so is `staging/`. A named file that is sealed opens through its footer and nothing else;
//! one that is not was left by a crash, and is recovered - walked, cut at the tear, sealed
//! - and the seal recorded. A directory holding the per-template layout is refused by path.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use chrono::{DateTime, NaiveDate};

use super::manifest::{self, FileRecord, Manifest, Record};
use super::v3::build::{self, schema_of};
use super::v3::file::{self, Entry, Index, Reader, Writer};
use super::v3::{day_of, FormatError, Source, DAY_NS};
use super::{
    effective_ts, in_window, render_row, value_bytes, FlushKind, Flushed, LeafStore, Predicate,
    Row, RowGroupLimits, Scan, ScanOptions, Store, StoreError, Visitor, DEFAULT_TS_COLUMN,
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
}

/// The files being written, shared by every leaf: one per day that has taken a row since
/// it was last sealed, and the manifest that names them.
struct Files {
    data: PathBuf,
    manifest: Manifest,
    open: BTreeMap<i64, Open>,
    /// Days in least recently written order.
    order: Vec<i64>,
    /// The next file number per day directory.
    next: BTreeMap<i64, u32>,
    max_open: usize,
    max_bytes: u64,
}

impl Files {
    /// The day's open file, created and named in the manifest if there is none. Past the
    /// cap on open files the least recently written day's file is sealed first.
    fn writer_for(&mut self, day: i64) -> Result<&mut Open, StoreError> {
        if !self.open.contains_key(&day) {
            while self.open.len() >= self.max_open.max(1) {
                let Some(oldest) = self.order.first().copied() else {
                    break;
                };
                self.seal_day(oldest)?;
            }
            let dir = self.data.join(day_name(day));
            fs::create_dir_all(&dir).map_err(|source| FormatError::Io {
                path: dir.clone(),
                source,
            })?;
            // A new directory's entry is made durable before a file in it is named.
            crate::frame::sync_dir(&dir)
                .and_then(|()| crate::frame::sync_dir(&self.data))
                .map_err(|source| FormatError::Io {
                    path: dir.clone(),
                    source,
                })?;
            let number = self.next.get(&day).copied().unwrap_or(1);
            let rel = format!("{}/{}", day_name(day), file_name(number));
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
            self.next.insert(day, number + 1);
            self.open.insert(
                day,
                Open {
                    writer,
                    file: Arc::new(DataFile { path }),
                    rel,
                    schemas: HashMap::new(),
                },
            );
        }
        self.order.retain(|d| *d != day);
        self.order.push(day);
        self.open.get_mut(&day).ok_or_else(|| StoreError::Io {
            template_id: 0,
            source: std::io::Error::other("a day file vanished while it was being opened"),
        })
    }

    /// Seal one day's file and record it. The file leaves the open set whatever happens:
    /// a seal that failed has still stopped taking rows, and the next start recovers it.
    fn seal_day(&mut self, day: i64) -> Result<(), StoreError> {
        self.order.retain(|d| *d != day);
        let Some(open) = self.open.remove(&day) else {
            return Ok(());
        };
        let index = open.writer.seal()?;
        self.manifest.append(&[Record::Seal {
            files: vec![FileRecord::sealed(open.rel, &index)],
        }])?;
        Ok(())
    }

    fn seal_all(&mut self) -> Result<(), StoreError> {
        let mut first = None;
        let days: Vec<i64> = self.open.keys().copied().collect();
        for day in days {
            if let Err(e) = self.seal_day(day) {
                first = first.or(Some(e));
            }
        }
        first.map_or(Ok(()), Err)
    }
}

// ── The store ──────────────────────────────────────────────────────────────────────

pub struct DiskStore {
    base: PathBuf,
    limits: RowGroupLimits,
    files: Arc<Mutex<Files>>,
    /// Row groups on disk at startup, by template, in storage order. A leaf takes its own
    /// when it opens; a template no catalog record names keeps them here, unread.
    stored: Mutex<BTreeMap<u64, Vec<Stored>>>,
    claimed: Vec<u64>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl DiskStore {
    /// Open the data directory: refuse an earlier layout, read the manifest, delete what it
    /// does not name and any leftover staging, recover every file a crash left unsealed,
    /// and index every row group by template.
    pub fn open(
        base: PathBuf,
        limits: RowGroupLimits,
        max_open: usize,
        max_bytes: u64,
    ) -> Result<Self, StoreError> {
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
            limits,
            files: Arc::new(Mutex::new(Files {
                data,
                manifest,
                open: BTreeMap::new(),
                order: Vec::new(),
                next,
                max_open,
                max_bytes,
            })),
            stored: Mutex::new(stored),
            claimed,
        })
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
    fn open(&self, template_id: u64, time_column: &str) -> Result<Box<dyn LeafStore>, StoreError> {
        let stored = lock(&self.stored).remove(&template_id).unwrap_or_default();
        let mut days: BTreeMap<i64, Day> = BTreeMap::new();
        for s in stored {
            days.entry(day_of(s.entry.min_time)).or_default().note(s);
        }
        Ok(Box::new(DiskLeaf {
            template_id,
            time_column: time_column.to_string(),
            limits: self.limits,
            files: Arc::clone(&self.files),
            days,
            buffered: 0,
        }))
    }

    fn kind(&self) -> &'static str {
        "disk"
    }

    fn base_path(&self) -> Option<&Path> {
        Some(&self.base)
    }

    fn open_files(&self) -> (usize, usize) {
        let files = lock(&self.files);
        (files.open.len(), files.max_open)
    }

    fn seal(&self) -> Result<(), StoreError> {
        lock(&self.files).seal_all()
    }

    fn claimed(&self) -> Vec<u64> {
        self.claimed.clone()
    }
}

// ── One leaf ───────────────────────────────────────────────────────────────────────

struct Buffered {
    row: Row,
    time: i64,
    bytes: usize,
}

/// One template's rows of one UTC day: its row groups in storage order, then the rows not
/// yet in one.
#[derive(Default)]
struct Day {
    stored: Vec<Stored>,
    buffer: Vec<Buffered>,
    /// When the oldest buffered row arrived, which is when the current row group opened.
    /// Read from a clock but never driven by one: whoever owns the tick asks.
    opened_at: Option<Instant>,
    buffer_bytes: usize,
    rows: u64,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    bytes: u64,
}

impl Day {
    fn note(&mut self, s: Stored) {
        self.rows += u64::from(s.entry.row_count);
        self.bytes += u64::from(s.entry.frame_len) + u64::from(s.entry.chunks_len);
        self.min_ts = Some(
            self.min_ts
                .map_or(s.entry.min_time, |m| m.min(s.entry.min_time)),
        );
        self.max_ts = Some(
            self.max_ts
                .map_or(s.entry.max_time, |m| m.max(s.entry.max_time)),
        );
        self.stored.push(s);
    }

    fn newest(&self) -> Option<i64> {
        let buffered = self.buffer.iter().map(|b| b.time).max();
        match (self.max_ts, buffered) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    fn oldest(&self) -> Option<i64> {
        let buffered = self.buffer.iter().map(|b| b.time).min();
        match (self.min_ts, buffered) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

pub struct DiskLeaf {
    template_id: u64,
    /// The template's time column: what a row's day, and its row group's time range, are
    /// read from, falling back to the arrival time a row without one carries.
    time_column: String,
    limits: RowGroupLimits,
    files: Arc<Mutex<Files>>,
    days: BTreeMap<i64, Day>,
    buffered: u64,
}

impl DiskLeaf {
    /// Close the day's buffer into row groups of at most `limits.rows` rows and about
    /// `limits.bytes` of column data, each appended to the day's file and flushed before
    /// its rows leave the buffer - so a row is always in exactly one of the two.
    fn close(&mut self, day: i64, done: &mut Flushed) -> Result<(), StoreError> {
        let Some(part) = self.days.get_mut(&day) else {
            return Ok(());
        };
        while !part.buffer.is_empty() {
            let mut take = 0;
            let mut bytes = 0;
            for b in &part.buffer {
                if take > 0 && (take >= self.limits.rows || bytes >= self.limits.bytes) {
                    break;
                }
                take += 1;
                bytes += b.bytes;
            }
            let rows: Vec<&Row> = part.buffer[..take].iter().map(|b| &b.row).collect();
            let times: Vec<i64> = part.buffer[..take].iter().map(|b| b.time).collect();
            let columns = schema_of(rows.iter().copied());

            let mut files = lock(&self.files);
            let max_bytes = files.max_bytes;
            let open = files.writer_for(day)?;
            let schema_id = open.writer.schema_id(&columns)?;
            let rg = build::build(
                self.template_id,
                schema_id,
                &columns,
                &rows,
                &times,
                Source::NotRecorded,
            )?;
            let entry = open.writer.append(self.template_id, &rg)?;
            open.writer.sync()?;
            let schema = Arc::clone(
                open.schemas
                    .entry(schema_id)
                    .or_insert_with(|| Arc::new(columns)),
            );
            let stored = Stored {
                file: Arc::clone(&open.file),
                entry,
                schema,
            };
            if open.writer.len() >= max_bytes {
                files.seal_day(day)?;
            }
            drop(files);

            part.note(stored);
            part.buffer.drain(..take);
            part.buffer_bytes = part.buffer.iter().map(|b| b.bytes).sum();
            self.buffered -= take as u64;
            done.rows += take as u64;
            done.bytes += u64::from(entry.frame_len) + u64::from(entry.chunks_len);
        }
        part.opened_at = None;
        Ok(())
    }

    fn due(&self, part: &Day, kind: FlushKind) -> bool {
        !part.buffer.is_empty()
            && (kind.is_final()
                || part.buffer.len() >= self.limits.rows
                || part.buffer_bytes >= self.limits.bytes
                || part
                    .opened_at
                    .is_some_and(|t| t.elapsed() >= self.limits.age))
    }

    /// How many rows this day contributes to `count`, if that can be known without opening
    /// it: no predicate could reject a row, and the window holds the whole day's range.
    fn countable(
        &self,
        day: i64,
        preds: &[Predicate],
        since: Option<i64>,
        until: Option<i64>,
    ) -> Option<u64> {
        if !preds.is_empty() {
            return None;
        }
        let part = self.days.get(&day)?;
        let total = part.rows + part.buffer.len() as u64;
        let (Some(lo), Some(hi)) = (part.oldest(), part.newest()) else {
            return Some(total);
        };
        let contained = since.is_none_or(|from| lo >= from) && until.is_none_or(|to| hi < to);
        contained.then_some(total)
    }

    fn overlaps(&self, day: i64, since: Option<i64>, until: Option<i64>) -> bool {
        let start = day.saturating_mul(DAY_NS);
        let part = self.days.get(&day);
        let lo = part.and_then(Day::oldest).unwrap_or(start).max(start);
        let hi = part
            .and_then(Day::newest)
            .unwrap_or(start.saturating_add(DAY_NS - 1));
        since.is_none_or(|from| hi >= from) && until.is_none_or(|to| lo < to)
    }

    /// One day's rows, stored then buffered, handed to the visitor. `false` once the limit
    /// is reached.
    fn visit_day(
        &self,
        day: i64,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
        out: &mut Scan,
        readers: &mut HashMap<PathBuf, Reader>,
    ) -> Result<bool, StoreError> {
        let Some(part) = self.days.get(&day) else {
            return Ok(true);
        };
        let ts_name = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();

        // A row's position is the rows of its template and day before it in storage order,
        // counted from the index: the pruned row groups' counts are added unread.
        let mut seq = 0u64;
        for s in &part.stored {
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

        // Then the rows not yet in a row group, after the stored ones because that is the
        // position they will have once written.
        for b in &part.buffer {
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
    fn append(&mut self, row: Row) -> u64 {
        // Every row the ingest path hands over has an effective time - its own, or the
        // arrival time it was stamped with. A row with neither has no day; it is placed
        // at the epoch rather than dropped.
        let time = effective_ts(&row, &self.time_column).unwrap_or(0);
        let bytes = row
            .iter()
            .map(|(key, value)| key.len() + value_bytes(value))
            .sum();
        let part = self.days.entry(day_of(time)).or_default();
        let seq = part.rows + part.buffer.len() as u64;
        if part.buffer.is_empty() {
            part.opened_at = Some(Instant::now());
        }
        part.buffer.push(Buffered { row, time, bytes });
        part.buffer_bytes += bytes;
        self.buffered += 1;
        seq
    }

    /// `Tick` closes only the days a threshold has fired on; `Final` closes every buffer.
    /// Sealing the files is the store's, once every leaf has closed.
    fn flush(&mut self, kind: FlushKind) -> Result<Flushed, StoreError> {
        let days: Vec<i64> = self
            .days
            .iter()
            .filter(|(_, part)| self.due(part, kind))
            .map(|(day, _)| *day)
            .collect();
        let mut done = Flushed::default();
        let mut failure = None;
        for day in days {
            let before = done.rows;
            // One day that cannot be written must not hold up the rest; its rows stay
            // buffered and the next tick retries.
            if let Err(e) = self.close(day, &mut done) {
                failure = failure.or(Some(StoreError::Leaf {
                    template_id: self.template_id,
                    source: Box::new(e),
                }));
            }
            if done.rows > before {
                done.partitions += 1;
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(done),
        }
    }

    fn visit(
        &self,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();
        let mut out = Scan::default();
        let mut readers = HashMap::new();
        for day in self.days.keys().rev().copied() {
            if windowed && !self.overlaps(day, since, until) {
                out.row_groups_pruned += self.days.get(&day).map_or(0, |p| p.stored.len() as u64);
                continue;
            }
            if !v.wants_partition(self.days.get(&day).and_then(Day::newest)) {
                // The visitor has no use for a row from here, but a limit bounds the rows in
                // the answer and not the rows that matched: the day is still counted, free
                // when nothing inside it could reject a row, and read otherwise.
                if let Some(matched) = self.countable(day, preds, since, until) {
                    out.matched += matched;
                    continue;
                }
            }
            if !self.visit_day(day, opts, preds, v, &mut out, &mut readers)? {
                break;
            }
        }
        Ok(out)
    }

    fn len(&self) -> u64 {
        self.days.values().map(|p| p.rows).sum::<u64>() + self.buffered
    }

    fn buffered(&self) -> u64 {
        self.buffered
    }

    fn approx_bytes(&self) -> u64 {
        self.days
            .values()
            .map(|p| p.bytes + p.buffer_bytes as u64)
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
    use crate::types::ColValue;

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

    fn store(scratch: &Scratch) -> DiskStore {
        DiskStore::open(
            scratch.0.clone(),
            RowGroupLimits::default(),
            8,
            DEFAULT_SEGMENT_MAX_BYTES,
        )
        .expect("open")
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

    #[test]
    fn rows_come_back_as_written_before_and_after_a_restart() {
        let scratch = Scratch::new("restart");
        let want: Vec<Row> = (0..5).map(|n| row(T0 + n * 1_000_000_000, n)).collect();
        {
            let s = store(&scratch);
            let mut leaf = s.open(7, "ts").expect("leaf");
            for r in &want {
                leaf.append(r.clone());
            }
            assert_eq!(all(leaf.as_ref()).len(), 5, "buffered rows are visible");
            leaf.flush(FlushKind::Final).expect("flush");
            s.seal().expect("seal");
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
        match DiskStore::open(scratch.0.clone(), RowGroupLimits::default(), 8, 1 << 20) {
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
            let mut leaf = s.open(7, "ts").expect("leaf");
            for n in 0..3 {
                leaf.append(row(T0 + n, n));
            }
            leaf.flush(FlushKind::Final).expect("flush");
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
}
