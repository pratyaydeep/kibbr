//! The disk store: one directory per leaf, partitioned by day, one **segment** per batch.
//!
//! ```text
//! <base>/leaf_101/
//! ├── _leaf.json          template_id, the column the days were cut on, the key set
//! ├── 2024-01-15/         seg_000001.col, seg_000002.col …
//! ├── 2024-01-16/         seg_000001.col
//! └── undated/            rows whose timestamp column is not a readable Datetime
//! ```
//!
//! A partition is a **directory of numbered segments**, not a single file. Lexical name
//! order is write order, and numbering restarts in each day directory.
//!
//! Nothing about the shape is TrieDB-specific, which is worth protecting: a day-partitioned
//! directory of columnar files is what Athena, DuckDB and S3 Select read directly, with
//! TrieDB not running.
//!
//! **Why days, and why descending.** Rows come back newest first, so a limited query wants
//! the newest partitions and nothing else; descending order lets a scan stop once
//! everything it holds outranks anything the next partition could contain. A time window
//! prunes harder and needs no cooperation from the caller: `SINCE 1h` opens one directory
//! out of a year, and inside a segment the footer's per-row-group timestamps prune again
//! before a byte of column data is read.
//!
//! **Closing a row group and sealing a segment are two different events**, and conflating
//! them is the main way this format goes wrong. Separate thresholds, separate functions:
//! `close_row_groups` and `seal_open`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use chrono::{DateTime, NaiveDate};
use serde::{Deserialize, Serialize};

use super::segment::{self, ColKind, ColumnSpec, SegmentIndex, SegmentWriter};
use super::{
    in_window, ts_of, value_bytes, DtForms, FlushKind, Flushed, LeafStore, Predicate, Row,
    RowGroupLimits, Scan, ScanOptions, Store, StoreError, Visitor, DEFAULT_TS_COLUMN,
};
use crate::types::{ColValue, DtForm};

const DAY_NS: i64 = 86_400 * 1_000_000_000;
const LEAF_FILE: &str = "_leaf.json";
/// The partition holding rows whose timestamp column is not a readable `Datetime`.
const UNDATED: &str = "undated";
/// The manifest the per-column layout wrote. Its presence is how that layout is
/// recognised and refused; see `refuse_legacy_layout`.
const LEGACY_MANIFEST: &str = "_columns.json";

/// Seal a segment at 128MB. The other three triggers - the day boundary, graceful
/// shutdown, and the leaf's column set changing - are structural and handled where they
/// happen.
const SEGMENT_MAX_BYTES: u64 = 128 * 1024 * 1024;

pub struct DiskStore {
    base: PathBuf,
    limits: RowGroupLimits,
}

impl DiskStore {
    /// Opening the base is also where the **old layout is refused**. There is no upgrade
    /// path from one file per (template, day, column): a server that finds those files
    /// names the path and exits non-zero rather than reading them.
    pub fn new(base: PathBuf, limits: RowGroupLimits) -> Result<Self, StoreError> {
        fs::create_dir_all(&base).map_err(|source| StoreError::Io {
            template_id: 0,
            source,
        })?;
        refuse_legacy_layout(&base)?;
        Ok(Self { base, limits })
    }
}

/// The path of the first thing under `base` belonging to the per-column layout.
///
/// Read from names, not contents: that layout wrote `_columns.json` into every partition
/// directory and named its files after escaped column names, so either signature is proof
/// on its own - a `.col` file that is not `seg_NNNNNN.col` is the second.
fn legacy_path(base: &Path) -> Option<PathBuf> {
    let leaves = fs::read_dir(base).ok()?;
    for leaf in leaves.flatten() {
        if !leaf.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Ok(days) = fs::read_dir(leaf.path()) else {
            continue;
        };
        for day in days.flatten() {
            if !day.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let Ok(files) = fs::read_dir(day.path()) else {
                continue;
            };
            for file in files.flatten() {
                let name = file.file_name();
                let name = name.to_string_lossy();
                if name == LEGACY_MANIFEST {
                    return Some(file.path());
                }
                if name.ends_with(".col") && segment::segment_seq(&name).is_none() {
                    return Some(file.path());
                }
            }
        }
    }
    None
}

fn refuse_legacy_layout(base: &Path) -> Result<(), StoreError> {
    match legacy_path(base) {
        Some(path) => Err(StoreError::LegacyLayout { path }),
        None => Ok(()),
    }
}

impl Store for DiskStore {
    /// The leaf's directory is derived from the template id, not read from
    /// `LeafMeta::storage_path`.
    ///
    /// That field is persisted as an absolute path, so a restart under a different data
    /// directory would send every leaf back to the old one. It stays in `trie.json` as
    /// the documentation the format asks for; the authority is the configured base.
    fn open(&self, template_id: u64) -> Result<Box<dyn LeafStore>, StoreError> {
        let dir = self.base.join(format!("leaf_{template_id}"));
        Ok(Box::new(DiskLeaf::open(template_id, dir, self.limits)?))
    }

    fn kind(&self) -> &'static str {
        "disk"
    }

    fn base_path(&self) -> Option<&Path> {
        Some(&self.base)
    }
}

/// Every template id that a leaf directory under `base` has already claimed.
///
/// Read from the directory *names*, not from `_leaf.json` inside them: a directory whose
/// manifest is missing or unreadable has still taken its name, and the whole purpose here
/// is to find the ids that are spoken for.
///
/// Unreadable base, odd names and non-directories are all skipped rather than refused.
/// This answers "what is taken", and anything it cannot read is one more reason to hand
/// out a fresh id rather than a reason to stop.
pub fn claimed_template_ids(base: &Path) -> Vec<u64> {
    let Ok(entries) = fs::read_dir(base) else {
        return Vec::new();
    };
    let mut ids: Vec<u64> = entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()?
                .strip_prefix("leaf_")?
                .parse::<u64>()
                .ok()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Whether `_leaf.json` at `path` already records exactly `keys`.
///
/// What `keys_on_disk` starts as: true only for a file that is there and agrees, so a
/// leaf opened with no file, or with a file from before a key was added, writes on its
/// first flush and an unchanged one never rewrites.
fn recorded_keys_match(path: &Path, keys: &BTreeSet<String>) -> bool {
    let Ok(bytes) = fs::read(path) else {
        return false;
    };
    let Ok(file) = serde_json::from_slice::<LeafFile>(&bytes) else {
        return false;
    };
    file.keys.len() == keys.len() && file.keys.iter().all(|k| keys.contains(k))
}

// ── Partitions ─────────────────────────────────────────────────────────────────────

/// Which directory a row belongs in.
///
/// `Day` holds days since the epoch rather than a formatted string, so the ordering is
/// arithmetic and the undated partition sorts last by construction. A descending sort over
/// the directory *names* would put `undated` first, which is the wrong end - it holds the
/// rows that rank last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PartKey {
    Day(i64),
    Undated,
}

impl PartKey {
    fn of(ts: Option<i64>) -> Self {
        match ts {
            Some(ns) => PartKey::Day(ns.div_euclid(DAY_NS)),
            None => PartKey::Undated,
        }
    }

    fn dir_name(self) -> String {
        match self {
            PartKey::Undated => UNDATED.to_string(),
            PartKey::Day(day) => DateTime::from_timestamp(day * 86_400, 0).map_or_else(
                || format!("day_{day}"),
                |dt| dt.format("%Y-%m-%d").to_string(),
            ),
        }
    }

    fn from_dir_name(name: &str) -> Option<Self> {
        if name == UNDATED {
            return Some(PartKey::Undated);
        }
        let date = NaiveDate::parse_from_str(name, "%Y-%m-%d").ok()?;
        let start = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp();
        Some(PartKey::Day(start.div_euclid(86_400)))
    }

    /// The widest span a row in this partition can carry. Used only to prune, so it has
    /// to be a true bound: the *end* of the day, not its start.
    fn bounds(self) -> Option<(i64, i64)> {
        match self {
            PartKey::Undated => None,
            PartKey::Day(day) => {
                let start = day.saturating_mul(DAY_NS);
                Some((start, start.saturating_add(DAY_NS - 1)))
            }
        }
    }
}

struct Buffered {
    row: Row,
    forms: Vec<(String, DtForm)>,
    bytes: usize,
}

impl Buffered {
    fn form_of(&self, column: &str) -> Option<DtForm> {
        self.forms
            .iter()
            .find(|(key, _)| key == column)
            .map(|(_, form)| *form)
    }

    /// The row's columns, and what each one claims about its type.
    fn shape(&self) -> BTreeMap<&str, (Option<ColKind>, Option<DtForm>)> {
        self.row
            .iter()
            .map(|(name, value)| {
                let kind = ColKind::of(value);
                let form = (kind == Some(ColKind::Dt))
                    .then(|| self.form_of(name))
                    .flatten();
                (name.as_str(), (kind, form))
            })
            .collect()
    }
}

/// One day directory: the segments in it, the one still open, and the rows not yet in a
/// row group.
struct Partition {
    /// Sealed, in write order - which is also the order their rows arrived in.
    sealed: Vec<SegmentIndex>,
    open: Option<SegmentWriter>,
    next_seq: u32,
    buffer: Vec<Buffered>,
    /// When the oldest buffered row arrived, which is when the current row group opened.
    /// Read from a clock but never driven by one: `LeafStore` has no timer, and whoever
    /// owns the tick is the one that asks whether this has expired.
    opened_at: Option<Instant>,
    buffer_bytes: usize,
    /// Rows in segments - sealed and open. The buffer is counted separately.
    rows: u64,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    bytes: u64,
}

impl Default for Partition {
    fn default() -> Self {
        Self {
            sealed: Vec::new(),
            open: None,
            // Numbered from one, restarting in each day directory.
            next_seq: 1,
            buffer: Vec::new(),
            opened_at: None,
            buffer_bytes: 0,
            rows: 0,
            min_ts: None,
            max_ts: None,
            bytes: 0,
        }
    }
}

impl Partition {
    /// Every segment index, oldest first, the open one last.
    fn indexes(&self) -> impl Iterator<Item = &SegmentIndex> {
        self.sealed
            .iter()
            .chain(self.open.as_ref().map(SegmentWriter::index))
    }

    fn note(&mut self, ts: Option<i64>) {
        if let Some(ns) = ts {
            self.min_ts = Some(self.min_ts.map_or(ns, |m: i64| m.min(ns)));
            self.max_ts = Some(self.max_ts.map_or(ns, |m: i64| m.max(ns)));
        }
    }
}

#[derive(Serialize, Deserialize)]
struct LeafFile {
    template_id: u64,
    /// The column the days were cut on. Recorded because skipping a partition is only
    /// sound against *this* column: a query ordering by `created_at` while the directories
    /// were cut on `ts` cannot read "newest directory first" as "newest rows first".
    partition_column: String,
    /// **The leaf's key set**, and it is here because of a bug found at M5: a trie
    /// relearned in a different order can point template *n* at a directory holding a
    /// different key set, and the rows in it become unreachable with no error - the
    /// filtered query prunes to one template and opens another. Without the key set on
    /// disk the trie cannot be rebuilt from what is on disk, so those rows stay orphaned.
    #[serde(default)]
    keys: Vec<String>,
}

// ── One leaf ───────────────────────────────────────────────────────────────────────

pub struct DiskLeaf {
    template_id: u64,
    dir: PathBuf,
    partition_column: String,
    limits: RowGroupLimits,
    /// Every key this leaf has held, which for a template is its key set: a row is built
    /// from the leaf's key set, so the row's keys *are* it.
    keys: BTreeSet<String>,
    keys_on_disk: bool,
    parts: BTreeMap<PartKey, Partition>,
    buffered: u64,
}

impl DiskLeaf {
    pub fn open(
        template_id: u64,
        dir: PathBuf,
        limits: RowGroupLimits,
    ) -> Result<Self, StoreError> {
        let io = |source| StoreError::Io {
            template_id,
            source,
        };
        fs::create_dir_all(&dir).map_err(io)?;

        let leaf_path = dir.join(LEAF_FILE);
        let recorded: Option<LeafFile> = fs::read(&leaf_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok());
        if let Some(found) = recorded.as_ref().map(|f| f.template_id) {
            if found != template_id {
                // The directory name and the file inside it disagree about whose rows
                // these are. Reported rather than acted on: the directory is what the
                // template id resolves to, and silently reading another leaf's columns is
                // the one outcome worth refusing to reach quietly.
                eprintln!(
                    "triedb: WARN {}: records template {found}, but it is leaf {template_id}'s \
                     directory",
                    leaf_path.display()
                );
            }
        }
        let keys: BTreeSet<String> = recorded
            .as_ref()
            .map(|f| f.keys.iter().cloned().collect())
            .unwrap_or_default();
        let partition_column =
            recorded.map_or_else(|| DEFAULT_TS_COLUMN.to_string(), |f| f.partition_column);

        let mut parts: BTreeMap<PartKey, Partition> = BTreeMap::new();
        for entry in fs::read_dir(&dir).map_err(io)? {
            let Ok(entry) = entry else { continue };
            if !entry.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(key) = PartKey::from_dir_name(&name) else {
                // Not a partition this store wrote. Left alone rather than deleted: a
                // directory nobody can explain is a thing to look at, not to destroy.
                continue;
            };
            parts.insert(key, open_partition(template_id, &entry.path())?);
        }

        // Not written here. A leaf opened before its first row has no keys to record,
        // and an empty key list on disk is worse than none: it is the file the M5 bug
        // was found through, saying a template has no columns. The first flush with keys
        // writes it; until then the catalog is the record of the template.
        let keys_on_disk = recorded_keys_match(&leaf_path, &keys);
        Ok(Self {
            template_id,
            dir,
            partition_column,
            limits,
            keys,
            keys_on_disk,
            parts,
            buffered: 0,
        })
    }

    /// `_leaf.json`, written so that a reader sees the old file or the new one and never
    /// less than either: `.tmp` → `sync_all` → rename → directory fsync.
    ///
    /// Two things it will not do. It will not write when serialisation failed - the old
    /// code wrote `unwrap_or_default()`, a zero-byte file over a good one. And it will not
    /// write an empty key list over a non-empty one: a leaf with no keys has nothing to
    /// say yet, and a file already there says more.
    fn write_leaf_file(&mut self) -> Result<(), StoreError> {
        let path = self.dir.join(LEAF_FILE);
        if self.keys.is_empty() {
            return Ok(());
        }
        let io = |source| StoreError::Io {
            template_id: self.template_id,
            source,
        };
        let body = serde_json::to_vec_pretty(&LeafFile {
            template_id: self.template_id,
            partition_column: self.partition_column.clone(),
            keys: self.keys.iter().cloned().collect(),
        })
        .map_err(|e| io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;

        let tmp = self.dir.join(format!("{LEAF_FILE}.tmp"));
        {
            let mut handle = fs::File::create(&tmp).map_err(io)?;
            std::io::Write::write_all(&mut handle, &body).map_err(io)?;
            handle.sync_all().map_err(io)?;
        }
        fs::rename(&tmp, &path).map_err(io)?;
        fs::File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(io)?;
        self.keys_on_disk = true;
        Ok(())
    }

    /// Dated partitions newest first, then the undated one.
    ///
    /// That is exactly `Rank`'s order over partitions: `Reverse(ts)` puts the newest first
    /// and `Reverse(None)` - the greatest `Option` reversed - puts the rows that cannot be
    /// placed in time at the end.
    fn partitions_newest_first(&self) -> Vec<PartKey> {
        let mut keys: Vec<PartKey> = self.parts.keys().copied().collect();
        keys.reverse();
        keys
    }

    /// The greatest timestamp any row in this partition carries, buffer included.
    fn newest_in(&self, key: PartKey) -> Option<i64> {
        let part = self.parts.get(&key)?;
        let buffered = part
            .buffer
            .iter()
            .filter_map(|b| ts_of(&b.row, &self.partition_column))
            .max();
        match (part.max_ts, buffered) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    fn oldest_in(&self, key: PartKey) -> Option<i64> {
        let part = self.parts.get(&key)?;
        let buffered = part
            .buffer
            .iter()
            .filter_map(|b| ts_of(&b.row, &self.partition_column))
            .min();
        match (part.min_ts, buffered) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn part_dir(&self, key: PartKey) -> PathBuf {
        self.dir.join(key.dir_name())
    }
}

/// What one day directory holds, read off the segments in it.
///
/// A crash can only have left the last one unsealed, but every one goes through
/// `segment::open`, which recovers whatever needs it. Recovery is a WARN and never a
/// failed startup: the normal cause is a process killed mid-write, the rows past the tear
/// come back from the write-ahead log once there is one, and refusing to open the leaf
/// would cost every row before the tear too.
fn open_partition(template_id: u64, dir: &Path) -> Result<Partition, StoreError> {
    let io = |source| StoreError::Io {
        template_id,
        source,
    };
    let mut names: Vec<(u32, PathBuf)> = Vec::new();
    for entry in fs::read_dir(dir).map_err(io)? {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(seq) = segment::segment_seq(&name) {
            names.push((seq, entry.path()));
        }
    }
    names.sort_unstable();

    let mut part = Partition::default();
    for (seq, path) in names {
        let (index, opened) = segment::open(&path)?;
        if let segment::Opened::Recovered { .. } = opened {
            eprintln!(
                "triedb: WARN leaf {template_id}: {} was not sealed - {opened}",
                path.display()
            );
        }
        part.next_seq = part.next_seq.max(seq + 1);
        part.rows += index.total_rows;
        part.bytes += index.bytes;
        for group in &index.groups {
            if let Some((lo, hi)) = group.span() {
                part.min_ts = Some(part.min_ts.map_or(lo, |m: i64| m.min(lo)));
                part.max_ts = Some(part.max_ts.map_or(hi, |m: i64| m.max(hi)));
            }
        }
        part.sealed.push(index);
    }
    Ok(part)
}

// ── Writing ────────────────────────────────────────────────────────────────────────

/// The schema one run of rows needs: every column, its type, and its datetime rung.
type Shape = BTreeMap<String, (Option<ColKind>, Option<DtForm>)>;

/// Can `row` go into a segment whose columns are `spec`?
///
/// Three ways it cannot, and each one seals the segment:
///
/// - **The column set differs.** The segment's column order is fixed.
/// - **A column's type would widen.** The footer names one type per column for the whole
///   segment, so a wider value cannot go in. A *narrower* one can, coerced up - which is
///   what stops a column that has widened once from sealing on every row after it.
/// - **A datetime column's rung changes.** `dt_form` lives in the row group and in the
///   footer and the two must agree, so two rungs cannot share a chunk.
fn row_fits(spec: &[ColumnSpec], row: &Buffered) -> bool {
    let shape = row.shape();
    if shape.len() != spec.len() {
        return false;
    }
    spec.iter().all(|column| {
        let Some((kind, form)) = shape.get(column.name.as_str()) else {
            return false;
        };
        let Some(kind) = kind else {
            // A null claims nothing about the column, here as in the type lattice.
            return true;
        };
        if ColKind::join(column.kind, *kind) != column.kind {
            return false;
        }
        column.kind != ColKind::Dt || DtForm::merge(column.dt_form, *form) == column.dt_form
    })
}

/// Fold one row into the schema a new segment would need, or refuse it.
///
/// Refused for the same three reasons `row_fits` refuses, with one difference: nothing is
/// fixed yet, so a column's type may still *widen* here - but only from nothing. A type
/// that changes after a value has been seen ends the run, so the common shape (a column
/// that is `I64` for a thousand rows and `Str` for one) writes a thousand ints and then a
/// string, rather than a thousand ints rendered as text.
fn extend_shape(shape: &mut Shape, row: &Buffered) -> bool {
    let incoming = row.shape();
    if !shape.is_empty() && shape.len() != incoming.len() {
        return false;
    }
    if !shape.is_empty() && incoming.keys().any(|name| !shape.contains_key(*name)) {
        return false;
    }
    for (name, (kind, form)) in &incoming {
        let slot = shape.entry((*name).to_string()).or_insert((None, None));
        match (slot.0, kind) {
            (_, None) => {}
            (None, Some(k)) => slot.0 = Some(*k),
            (Some(have), Some(k)) if have == *k => {}
            (Some(_), Some(_)) => return false,
        }
        if slot.0 == Some(ColKind::Dt) {
            match (slot.1, form) {
                (_, None) => {}
                (None, Some(f)) => slot.1 = Some(*f),
                (Some(have), Some(f)) if have == *f => {}
                (Some(_), Some(_)) => return false,
            }
        }
    }
    true
}

/// A shape, as the columns a segment is created with. A column that has only ever held
/// nulls is `Str`: every cell in it is absent, and the first real value would have said
/// otherwise.
fn columns_of(shape: &Shape) -> Vec<ColumnSpec> {
    shape
        .iter()
        .map(|(name, (kind, form))| ColumnSpec {
            name: name.clone(),
            kind: kind.unwrap_or(ColKind::Str),
            dt_form: (kind == &Some(ColKind::Dt)).then(|| form.unwrap_or(DtForm::Mixed)),
        })
        .collect()
}

impl DiskLeaf {
    /// How many rows from the front of the buffer can go into the currently open segment,
    /// and what schema they need if there is none.
    fn run_end(&self, part: &Partition) -> (usize, Option<Vec<ColumnSpec>>) {
        if let Some(open) = &part.open {
            let spec = open.columns();
            let end = part
                .buffer
                .iter()
                .position(|row| !row_fits(spec, row))
                .unwrap_or(part.buffer.len());
            // A row that does not fit the open segment seals it, and the next run
            // establishes its own schema - so a zero-length run is the signal to seal.
            return (end, None);
        }
        let mut shape = Shape::new();
        let mut end = 0;
        for row in &part.buffer {
            let mut next = shape.clone();
            if !extend_shape(&mut next, row) {
                break;
            }
            shape = next;
            end += 1;
        }
        (end.max(1), Some(columns_of(&shape)))
    }

    /// Close row groups out of one partition's buffer until its thresholds are quiet.
    ///
    /// **A row leaves the buffer only once the row group holding it is fsynced.** The
    /// tempting shape - take the rows, release the lock, write them - opens a window where
    /// a row is in neither the buffer nor a file, and a query landing in it gets a wrong
    /// count with nothing reporting an error.
    fn close_row_groups(
        &mut self,
        key: PartKey,
        kind: FlushKind,
        done: &mut Flushed,
    ) -> Result<(), StoreError> {
        let dir = self.part_dir(key);
        let limits = self.limits;
        let ts_column = self.partition_column.clone();
        let template_id = self.template_id;

        loop {
            let part = self.parts.entry(key).or_default();
            if part.buffer.is_empty() {
                part.opened_at = None;
                return Ok(());
            }
            // The three thresholds. The size ones keep chunks large enough to be worth
            // encoding and to amortise the row group header; the timeout is for
            // low-traffic leaves that would otherwise never reach one.
            let due = kind == FlushKind::Final
                || part.buffer.len() >= limits.rows
                || part.buffer_bytes >= limits.bytes
                || part.opened_at.is_some_and(|at| at.elapsed() >= limits.age);
            if !due {
                return Ok(());
            }

            let (run, fresh) = match self.parts.get(&key) {
                Some(part) => self.run_end(part),
                None => return Ok(()),
            };
            let part = self.parts.entry(key).or_default();
            if run == 0 {
                // The next row needs a segment this one cannot hold - a column set
                // change, a widening type, or a changed datetime rung. Sealing, which is
                // not the same event as closing a row group.
                seal_open(part, &mut done.bytes)?;
                continue;
            }
            if part.open.is_none() {
                fs::create_dir_all(&dir).map_err(|source| StoreError::Io {
                    template_id,
                    source,
                })?;
                let path = dir.join(segment::segment_name(part.next_seq));
                let columns = fresh.unwrap_or_default();
                part.open = Some(SegmentWriter::create(path, columns)?);
                part.next_seq += 1;
            }

            // One row group: the run, cut at the row and byte thresholds.
            let mut take = run.min(limits.rows);
            let mut bytes = 0usize;
            for (at, row) in part.buffer.iter().take(take).enumerate() {
                bytes += row.bytes;
                if bytes >= limits.bytes {
                    take = at + 1;
                    break;
                }
            }

            let writer = part.open.as_mut().unwrap_or_else(|| unreachable!());
            let columns = writer.columns().to_vec();
            let rows = &part.buffer[..take];
            let mut chunks: Vec<Vec<ColValue>> = Vec::with_capacity(columns.len());
            for column in &columns {
                chunks.push(
                    rows.iter()
                        .map(|row| {
                            let value =
                                row.row.get(&column.name).cloned().unwrap_or(ColValue::Null);
                            // The rung travels with the *row*, not with the file:
                            // promoting a datetime to text has to invert the rung that
                            // read it, and a column holding two rungs would render one of
                            // them wrong.
                            segment::coerce(value, column.kind, row.form_of(&column.name))
                        })
                        .collect(),
                );
            }
            let mut span: Option<(i64, i64)> = None;
            for row in rows {
                if let Some(ns) = ts_of(&row.row, &ts_column) {
                    span = Some(span.map_or((ns, ns), |(lo, hi)| (lo.min(ns), hi.max(ns))));
                }
            }

            let before = writer.bytes();
            writer.append_row_group(&chunks, take as u64, span)?;
            let written = writer.bytes() - before;
            let full = writer.bytes() >= SEGMENT_MAX_BYTES;

            // Only now do the rows leave the buffer.
            let drained: usize = part.buffer.drain(..take).map(|row| row.bytes).sum();
            part.buffer_bytes = part.buffer_bytes.saturating_sub(drained);
            part.rows += take as u64;
            part.bytes += written;
            if let Some((lo, hi)) = span {
                part.note(Some(lo));
                part.note(Some(hi));
            }
            self.buffered -= take as u64;
            done.rows += take as u64;
            done.bytes += written;

            if full {
                seal_open(self.parts.entry(key).or_default(), &mut done.bytes)?;
            }
        }
    }
}

/// Write the footer, fsync, close. After sealing, the next write opens the next segment
/// number, and the sealed file is never reopened for writing.
fn seal_open(part: &mut Partition, bytes: &mut u64) -> Result<(), StoreError> {
    let Some(writer) = part.open.take() else {
        return Ok(());
    };
    let before = writer.bytes();
    let index = writer.seal()?;
    *bytes += index.bytes.saturating_sub(before);
    part.bytes += index.bytes.saturating_sub(before);
    part.sealed.push(index);
    Ok(())
}

// ── Reading ────────────────────────────────────────────────────────────────────────

impl DiskLeaf {
    /// One partition's rows on disk, handed to the visitor.
    ///
    /// Row groups are pruned on their footer timestamps **before any column data is
    /// read**, and only the chunks the query names are read at all. A surviving row group
    /// costs one decode per needed column, not per row - the chunk is columnar and comes
    /// back as a column.
    fn visit_partition(
        &self,
        key: PartKey,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
        out: &mut Scan,
    ) -> Result<bool, StoreError> {
        let Some(part) = self.parts.get(&key) else {
            return Ok(true);
        };
        let ts_name = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();

        let mut seq = 0u64;
        for index in part.indexes() {
            // Which columns this query actually reads. Empty means every column - a
            // `MATCH` returns whole rows and cannot project. A window always reads the
            // timestamp column, whatever else was asked for, because it filters on it.
            let wanted: Vec<usize> = if opts.columns.is_empty() {
                (0..index.columns.len()).collect()
            } else {
                (0..index.columns.len())
                    .filter(|at| {
                        let name = &index.columns[*at].name;
                        opts.columns.iter().any(|want| want == name)
                            || (windowed && name == ts_name)
                    })
                    .collect()
            };

            let mut file = None;
            for (g, group) in index.groups.iter().enumerate() {
                if group.row_count == 0 {
                    continue;
                }
                if windowed && !group.overlaps(since, until) {
                    out.row_groups_pruned += 1;
                    seq += group.row_count;
                    continue;
                }
                out.row_groups_read += 1;

                let handle = match &mut file {
                    Some(handle) => handle,
                    none => none.insert(index.open_file()?),
                };
                let mut decoded: Vec<(&str, Vec<ColValue>)> = Vec::with_capacity(wanted.len());
                for at in &wanted {
                    out.bytes_read += u64::from(group.chunks[*at].len);
                    decoded.push((
                        index.columns[*at].name.as_str(),
                        index.values(handle, g, *at)?,
                    ));
                }

                let mut row: Row = Row::with_capacity(decoded.len());
                for r in 0..group.row_count as usize {
                    out.scanned += 1;
                    row.clear();
                    for (name, values) in &decoded {
                        row.insert((*name).to_string(), values[r].clone());
                    }
                    if windowed && !in_window(row.get(ts_name), since, until) {
                        seq += 1;
                        continue;
                    }
                    if !preds.iter().all(|pred| pred(&row)) {
                        seq += 1;
                        continue;
                    }
                    out.matched += 1;
                    v.row(&row, seq);
                    seq += 1;
                    if opts.limit != 0 && out.matched as usize >= opts.limit {
                        return Ok(false);
                    }
                }
            }
        }

        // Then the rows not yet in a row group. A row group threshold affects
        // *visibility on disk*, not visibility at all - an unwritten row is still in the
        // answer - and it goes after the segments' rows because that is the position it
        // will have once written.
        for buffered in &part.buffer {
            out.scanned += 1;
            if windowed && !in_window(buffered.row.get(ts_name), since, until) {
                seq += 1;
                continue;
            }
            if !preds.iter().all(|pred| pred(&buffered.row)) {
                seq += 1;
                continue;
            }
            out.matched += 1;
            v.row(&buffered.row, seq);
            seq += 1;
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

    /// The ordinal is **the row's position within its partition**, which is what makes it
    /// survive a restart with nothing stored: it is where the row sits in the segments.
    ///
    /// Narrower than "the ordinal this leaf gave it", and enough. `Rank` compares ordinals
    /// only between rows that tied on timestamp *and* leaf - and two rows with the same
    /// timestamp are in the same day, so the same partition. Undated rows all tie, and all
    /// live in the one undated partition.
    fn append(&mut self, row: Row, forms: &DtForms) -> u64 {
        let key = PartKey::of(ts_of(&row, &self.partition_column));
        for name in row.keys() {
            if !self.keys.contains(name) {
                self.keys.insert(name.clone());
                self.keys_on_disk = false;
            }
        }
        let bytes = row
            .iter()
            .map(|(key, value)| key.len() + value_bytes(value))
            .sum();
        let part = self.parts.entry(key).or_default();
        let seq = part.rows + part.buffer.len() as u64;
        if part.buffer.is_empty() {
            part.opened_at = Some(Instant::now());
        }
        part.buffer.push(Buffered {
            row,
            forms: forms.to_vec(),
            bytes,
        });
        part.buffer_bytes += bytes;
        self.buffered += 1;
        seq
    }

    /// `FlushKind::Tick` closes only the row groups a threshold has fired on;
    /// `FlushKind::Final` closes everything and seals every segment - graceful shutdown.
    fn flush(&mut self, kind: FlushKind) -> Result<Flushed, StoreError> {
        let keys: Vec<PartKey> = self.parts.keys().copied().collect();
        let mut done = Flushed::default();
        let mut failure = None;

        for key in keys {
            let before = done.rows;
            match self.close_row_groups(key, kind, &mut done) {
                Ok(()) => {
                    if kind == FlushKind::Final {
                        if let Err(e) =
                            seal_open(self.parts.entry(key).or_default(), &mut done.bytes)
                        {
                            failure = failure.or(Some(e));
                        }
                    }
                    if done.rows > before {
                        done.partitions += 1;
                    }
                }
                // Kept buffered, and the other partitions still attempted: one unwritable
                // day must not hold up the rest of the leaf, and the next tick retries.
                Err(e) => failure = failure.or(Some(e)),
            }
        }

        if !self.keys_on_disk {
            if let Err(e) = self.write_leaf_file() {
                failure = failure.or(Some(e));
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
        let ts_name = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        // Skipping a partition is a claim about the order the directories are in, and the
        // directories were cut on one column. Ordering by any other makes "newest
        // directory first" mean nothing, so the pruning is off entirely. Nothing reaches
        // this today - a query's ts column is always the default - which is exactly why
        // the guard is written before something depends on it.
        let skippable = ts_name == self.partition_column;
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();

        let mut out = Scan::default();

        for key in self.partitions_newest_first() {
            if skippable && windowed && !self.overlaps(key, since, until) {
                out.row_groups_pruned += self
                    .parts
                    .get(&key)
                    .map_or(0, |p| p.indexes().map(|i| i.groups.len() as u64).sum());
                continue;
            }
            if skippable && !v.wants_partition(self.newest_in(key)) {
                // The visitor has no use for a *row* from here - every one of them ranks
                // below what it is already holding. It still needs the partition
                // **counted**: a limit bounds the rows in the answer and not the rows
                // that matched, so a `count` that shrank when a `LIMIT` was added would
                // be a different number for the same question.
                //
                // Free exactly when nothing inside the partition could reject a row: no
                // predicates, and a window that contains the whole partition rather than
                // cutting through it. Then the count is the footer's, and no chunk is
                // read. Otherwise the skip is refused and the partition is read after
                // all - correct first, and the window prune above has usually already
                // done the larger saving.
                if let Some(matched) = self.countable(key, preds, since, until, windowed) {
                    out.matched += matched;
                    continue;
                }
            }

            if !self.visit_partition(key, opts, preds, v, &mut out)? {
                break;
            }
        }
        Ok(out)
    }

    fn len(&self) -> u64 {
        self.parts.values().map(|p| p.rows).sum::<u64>() + self.buffered
    }

    fn buffered(&self) -> u64 {
        self.buffered
    }

    /// Bytes on disk, plus what the buffer would add. Measured from the segment indexes
    /// rather than by walking the tree, so `/stats` costs nothing per request.
    fn approx_bytes(&self) -> u64 {
        self.parts
            .values()
            .map(|p| p.bytes + p.buffer_bytes as u64)
            .sum()
    }
}

impl DiskLeaf {
    /// How many rows this partition contributes to `count`, if that can be known without
    /// opening it.
    ///
    /// `None` means "read it": either a predicate could reject rows, or the window cuts
    /// through the partition rather than containing it, and in both cases the only way to
    /// know how many rows match is to look at them.
    fn countable(
        &self,
        key: PartKey,
        preds: &[Predicate],
        since: Option<i64>,
        until: Option<i64>,
        windowed: bool,
    ) -> Option<u64> {
        if !preds.is_empty() {
            return None;
        }
        let part = self.parts.get(&key)?;
        let total = part.rows + part.buffer.len() as u64;

        if !windowed {
            return Some(total);
        }
        // A dated partition holds only rows whose timestamp read as a `Datetime` in that
        // day - that is what put them there - so the extremes bound every row in it.
        // The undated partition has no such bound and, under a window, no matching rows
        // either; the prune above has already dropped it.
        let (Some(lo), Some(hi)) = (self.oldest_in(key), self.newest_in(key)) else {
            return (total == 0).then_some(0);
        };
        let contained = since.is_none_or(|from| lo >= from) && until.is_none_or(|to| hi < to);
        contained.then_some(total)
    }

    /// Can any row in this partition fall inside `[since, until)`?
    ///
    /// The undated partition never can: a row with no readable timestamp is outside every
    /// window, which is the same rule `in_window` applies one row at a time. Skipping the
    /// whole directory is that rule read once instead of a million times.
    fn overlaps(&self, key: PartKey, since: Option<i64>, until: Option<i64>) -> bool {
        let Some((day_start, day_end)) = key.bounds() else {
            return false;
        };
        // The recorded extremes are tighter than the day when they are known, and the day
        // is the fallback that is always true.
        let lo = self.oldest_in(key).unwrap_or(day_start).max(day_start);
        let hi = self.newest_in(key).unwrap_or(day_end).min(day_end);
        since.is_none_or(|from| hi >= from) && until.is_none_or(|to| lo < to)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// What the leaf does with segments, as against what the format does with bytes - those
/// are in `segment.rs`, and here for the same reason: the crate is a binary, so there is
/// no library to import from `tests/`. The pruning tests have end-to-end counterparts a
/// query can actually reach in `tests/segments.rs`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Rows, Scan};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "triedb-disk-{}-{}-{}",
                name,
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&dir);
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// A day in January 2024, in nanoseconds, so every row lands in one partition.
    const DAY: i64 = 1_705_276_800_000_000_000;

    fn leaf(scratch: &Scratch, limits: RowGroupLimits) -> DiskLeaf {
        DiskLeaf::open(101, scratch.0.join("leaf_101"), limits).expect("open the leaf")
    }

    fn row(pairs: &[(&str, ColValue)]) -> Row {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect()
    }

    fn at(ns: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(ns as u64)
    }

    fn scan(leaf: &DiskLeaf, opts: ScanOptions) -> (Scan, Vec<Row>) {
        let mut rows = Vec::new();
        let mut out = {
            let mut sink = Rows(|row: &Row, _: u64| rows.push(row.clone()));
            leaf.visit(&opts, &[], &mut sink).expect("visit")
        };
        out.rows = Vec::new();
        (out, rows)
    }

    /// Every segment file under the leaf, in path order.
    fn segments(scratch: &Scratch) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![scratch.0.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if segment::segment_seq(&entry.file_name().to_string_lossy()).is_some() {
                    out.push(path);
                }
            }
        }
        out.sort();
        out
    }

    // ── Visibility ─────────────────────────────────────────────────────────────────

    /// A row group now closes on size, so a quiet leaf's rows can sit unwritten for up to
    /// the timeout. They are still in the answer the moment they are appended - the
    /// threshold is about when bytes reach the disk, not about when a row exists.
    #[test]
    fn a_row_is_visible_before_its_row_group_closes() {
        let scratch = Scratch::new("visible");
        let mut leaf = leaf(&scratch, RowGroupLimits::default());
        leaf.append(row(&[("ts", ColValue::Datetime(DAY))]), &[]);

        assert_eq!(leaf.len(), 1);
        assert_eq!(leaf.buffered(), 1);
        assert!(segments(&scratch).is_empty(), "a row group closed early");
        let (out, rows) = scan(&leaf, ScanOptions::default());
        assert_eq!(rows.len(), 1);
        assert_eq!(out.matched, 1);

        // And once it is written the count does not double: the row leaves the buffer
        // only as the row group that holds it lands, under the one lock.
        leaf.flush(FlushKind::Final).expect("flush");
        assert_eq!(leaf.buffered(), 0);
        assert_eq!(leaf.len(), 1);
        assert_eq!(scan(&leaf, ScanOptions::default()).1.len(), 1);
    }

    // ── Sealing ────────────────────────────────────────────────────────────────────

    /// The footer names one type per column for the whole segment, so a column that
    /// widens cannot stay in it. The rows on both sides of the widening keep the type
    /// they arrived with, rather than the earlier thousand being rewritten as text.
    #[test]
    fn a_column_that_widens_seals_the_segment() {
        let scratch = Scratch::new("widen");
        let mut leaf = leaf(&scratch, RowGroupLimits::default());
        for n in 0..3i64 {
            leaf.append(
                row(&[
                    ("ts", ColValue::Datetime(DAY + n)),
                    ("x", ColValue::Int64(n)),
                ]),
                &[],
            );
        }
        leaf.append(
            row(&[
                ("ts", ColValue::Datetime(DAY + 9)),
                ("x", ColValue::String("N/A".into())),
            ]),
            &[],
        );
        leaf.flush(FlushKind::Final).expect("flush");

        let files = segments(&scratch);
        assert_eq!(files.len(), 2, "the widening did not seal the segment");
        let (first, _) = segment::open(&files[0]).expect("open the first segment");
        let (second, _) = segment::open(&files[1]).expect("open the second segment");
        assert_eq!(
            first.column_at("x").map(|at| first.columns[at].kind),
            Some(ColKind::I64)
        );
        assert_eq!(
            second.column_at("x").map(|at| second.columns[at].kind),
            Some(ColKind::Str)
        );

        // Both stay queryable, and each row still carries the type it arrived with.
        let (_, rows) = scan(&leaf, ScanOptions::default());
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0]["x"], ColValue::Int64(0));
        assert_eq!(rows[3]["x"], ColValue::String("N/A".into()));
    }

    /// A rung change seals rather than writing two forms into one chunk. `Mixed` stays a
    /// value this build *reads* - it is the promotion fallback - but never one it writes.
    #[test]
    fn a_changed_rung_seals_the_segment_rather_than_mixing_forms() {
        let scratch = Scratch::new("rung");
        let mut leaf = leaf(&scratch, RowGroupLimits::default());
        for (n, form) in [
            DtForm::EpochSecs,
            DtForm::EpochSecs,
            DtForm::Rfc3339,
            DtForm::Rfc3339,
        ]
        .into_iter()
        .enumerate()
        {
            leaf.append(
                row(&[
                    ("ts", ColValue::Datetime(DAY)),
                    ("when", ColValue::Datetime(DAY + n as i64)),
                ]),
                &[("when".to_string(), form)],
            );
        }
        leaf.flush(FlushKind::Final).expect("flush");

        let files = segments(&scratch);
        assert_eq!(files.len(), 2, "the rung change did not seal the segment");
        for (path, want) in files.iter().zip([DtForm::EpochSecs, DtForm::Rfc3339]) {
            let (index, _) = segment::open(path).expect("open the segment");
            let at = index.column_at("when").expect("the column is there");
            assert_eq!(
                index.columns[at].dt_form,
                Some(want),
                "{} carries the wrong rung",
                path.display()
            );
        }
    }

    /// A column set change seals, and is also when the key set in `_leaf.json` changes.
    #[test]
    fn a_column_set_change_seals_the_segment_and_grows_the_key_set() {
        let scratch = Scratch::new("columns");
        let mut leaf = leaf(&scratch, RowGroupLimits::default());
        leaf.append(row(&[("ts", ColValue::Datetime(DAY))]), &[]);
        leaf.append(
            row(&[
                ("ts", ColValue::Datetime(DAY + 1)),
                ("extra", ColValue::Bool(true)),
            ]),
            &[],
        );
        leaf.flush(FlushKind::Final).expect("flush");

        assert_eq!(segments(&scratch).len(), 2);
        let recorded: LeafFile = serde_json::from_slice(
            &fs::read(scratch.0.join("leaf_101").join(LEAF_FILE)).expect("read _leaf.json"),
        )
        .expect("parse _leaf.json");
        assert_eq!(recorded.keys, vec!["extra".to_string(), "ts".to_string()]);
        assert_eq!(recorded.template_id, 101);
    }

    /// The other half of the widening rule: a value the open segment can already hold is
    /// coerced into it rather than sealing again. Without this an alternating stream
    /// would seal on every row, which is the file count problem coming back.
    #[test]
    fn a_narrower_value_is_coerced_rather_than_sealing_again() {
        let scratch = Scratch::new("coerce");
        let mut leaf = leaf(&scratch, RowGroupLimits::default());
        leaf.append(
            row(&[("ts", ColValue::Datetime(DAY)), ("x", ColValue::Int64(1))]),
            &[],
        );
        for n in 0..6 {
            // Str, then ints again: the column has widened and cannot narrow back.
            let value = if n % 2 == 0 {
                ColValue::String(format!("s{n}"))
            } else {
                ColValue::Int64(n)
            };
            leaf.append(
                row(&[("ts", ColValue::Datetime(DAY + 1)), ("x", value)]),
                &[],
            );
        }
        leaf.flush(FlushKind::Final).expect("flush");

        assert_eq!(
            segments(&scratch).len(),
            2,
            "a column that has already widened sealed a segment again"
        );
        let (_, rows) = scan(&leaf, ScanOptions::default());
        assert_eq!(rows.len(), 7);
        assert_eq!(rows[2]["x"], ColValue::String("1".into()), "not coerced");
    }

    // ── Pruning ────────────────────────────────────────────────────────────────────

    /// Fifty row groups, a window inside one of them.
    #[test]
    fn a_window_covering_one_row_group_of_fifty_reads_one() {
        let scratch = Scratch::new("prune");
        let limits = RowGroupLimits {
            rows: 10,
            ..RowGroupLimits::default()
        };
        let mut leaf = leaf(&scratch, limits);
        for r in 0..500i64 {
            leaf.append(row(&[("ts", ColValue::Datetime(DAY + r * 1_000_000))]), &[]);
            if r % 10 == 9 {
                leaf.flush(FlushKind::Tick).expect("flush");
            }
        }
        leaf.flush(FlushKind::Final).expect("flush");

        let (out, _) = scan(
            &leaf,
            ScanOptions {
                since: Some(at(DAY + 255 * 1_000_000)),
                until: Some(at(DAY + 256 * 1_000_000)),
                ..ScanOptions::default()
            },
        );
        assert_eq!(
            out.row_groups_read, 1,
            "a window inside one row group read more"
        );
        assert_eq!(out.row_groups_pruned, 49);
        assert_eq!(out.matched, 1);
    }

    /// Naming two columns of eight reads two chunks and not eight, and `bytes_read` is
    /// exactly those two chunks' lengths - which is the only way to show that the other
    /// six were never touched.
    #[test]
    fn naming_two_columns_of_eight_reads_two_chunks() {
        let scratch = Scratch::new("columns-read");
        let mut leaf = leaf(&scratch, RowGroupLimits::default());
        for r in 0..50i64 {
            let mut pairs = vec![("ts".to_string(), ColValue::Datetime(DAY + r))];
            for c in 0..7 {
                pairs.push((
                    format!("c{c}"),
                    ColValue::String("padding".repeat(c as usize + 1)),
                ));
            }
            leaf.append(pairs.into_iter().collect(), &[]);
        }
        leaf.flush(FlushKind::Final).expect("flush");

        let files = segments(&scratch);
        let (index, _) = segment::open(&files[0]).expect("open the segment");
        assert_eq!(index.columns.len(), 8);
        let want: u64 = ["c3", "ts"]
            .iter()
            .map(|name| {
                let at = index.column_at(name).expect("the column is there");
                u64::from(index.groups[0].chunks[at].len)
            })
            .sum();

        let (out, _) = scan(
            &leaf,
            ScanOptions {
                columns: vec!["c3".to_string(), "ts".to_string()],
                ..ScanOptions::default()
            },
        );
        assert_eq!(
            out.bytes_read, want,
            "more than the two named chunks was read"
        );

        // And the whole row, for comparison: eight chunks is strictly more.
        let (all, _) = scan(&leaf, ScanOptions::default());
        assert!(
            all.bytes_read > out.bytes_read * 2,
            "projecting saved nothing: {} against {}",
            out.bytes_read,
            all.bytes_read
        );
    }

    // ── Restart ────────────────────────────────────────────────────────────────────

    /// Reopening a leaf reads its rows back out of the footers, and the next write starts
    /// a new segment rather than reopening a sealed one.
    #[test]
    fn a_reopened_leaf_reads_its_segments_and_appends_a_new_one() {
        let scratch = Scratch::new("reopen");
        {
            let mut first = leaf(&scratch, RowGroupLimits::default());
            for r in 0..20i64 {
                first.append(row(&[("ts", ColValue::Datetime(DAY + r))]), &[]);
            }
            first.flush(FlushKind::Final).expect("flush");
        }

        let mut second = leaf(&scratch, RowGroupLimits::default());
        assert_eq!(second.len(), 20);
        second.append(row(&[("ts", ColValue::Datetime(DAY + 99))]), &[]);
        second.flush(FlushKind::Final).expect("flush");

        assert_eq!(second.len(), 21);
        assert_eq!(segments(&scratch).len(), 2, "a sealed segment was reopened");
        assert_eq!(scan(&second, ScanOptions::default()).1.len(), 21);
    }

    /// The old layout is recognised from names alone and refused by path.
    #[test]
    fn the_per_column_layout_is_refused_by_path() {
        let scratch = Scratch::new("legacy");
        let day = scratch.0.join("leaf_101").join("2024-01-15");
        fs::create_dir_all(&day).expect("create the day directory");
        fs::write(day.join("ts.col"), b"TRDB").expect("write a per-column file");

        match DiskStore::new(scratch.0.clone(), RowGroupLimits::default()).err() {
            Some(StoreError::LegacyLayout { path }) => {
                assert!(
                    path.ends_with("ts.col"),
                    "the wrong path was named: {path:?}"
                );
            }
            other => panic!("the per-column layout was not refused: {other:?}"),
        }
    }
}
