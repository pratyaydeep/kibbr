//! The disk store: one directory per leaf, partitioned by day.
//!
//! ```text
//! <base>/leaf_101/
//! ├── _leaf.json          which column the days were cut on
//! ├── 2024-01-15/         <column>.col …  _columns.json
//! ├── 2024-01-16/
//! └── undated/            rows whose timestamp column is not a readable Datetime
//! ```
//!
//! Nothing about the shape is TrieDB-specific, which is a property worth protecting: a
//! day-partitioned directory of columnar files is what Athena, DuckDB and S3 Select read
//! directly, with TrieDB not running.
//!
//! **Why days, and why descending.** Rows come back newest first, so a query with a limit
//! wants the newest partitions and nothing else. Reading directories in descending date
//! order lets a scan stop once the rows it is holding are all newer than anything the next
//! partition could contain - the payoff for having fixed the order before there was a disk
//! to read it off. A time window prunes harder still and needs no cooperation from the
//! caller at all: `SINCE 1h` opens one directory out of a year.
//!
//! **What partitioning does not buy.** Rows *inside* a day are in ingest order, not
//! timestamp order, so a partition that is opened has to be finished. The saving is whole
//! directories skipped, never a row-level early stop.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate};
use serde::{Deserialize, Serialize};

use super::col::{self, ColFile, ColKind, ColReader};
use super::manifest::{self, ColumnEntry, Manifest, UNDATED};
use super::{
    in_window, ts_of, value_bytes, DtForms, Durability, Flushed, LeafStore, Predicate, Row, Scan,
    ScanOptions, Store, StoreError, Visitor, DEFAULT_TS_COLUMN,
};
use crate::types::{ColValue, DtForm};

const DAY_NS: i64 = 86_400 * 1_000_000_000;
const LEAF_FILE: &str = "_leaf.json";

pub struct DiskStore {
    base: PathBuf,
}

impl DiskStore {
    pub fn new(base: PathBuf) -> Result<Self, StoreError> {
        fs::create_dir_all(&base).map_err(|source| StoreError::Io {
            template_id: 0,
            source,
        })?;
        Ok(Self { base })
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

impl Store for DiskStore {
    /// The leaf's directory is derived from the template id, not read from
    /// `LeafMeta::storage_path`.
    ///
    /// That field is persisted as an absolute path, so a restart under a different data
    /// directory would send every leaf back to the old one. It stays in `trie.json` as
    /// the documentation the format asks for; the authority is the configured base.
    fn open(&self, template_id: u64) -> Result<Box<dyn LeafStore>, StoreError> {
        let dir = self.base.join(format!("leaf_{template_id}"));
        Ok(Box::new(DiskLeaf::open(template_id, dir)?))
    }

    fn kind(&self) -> &'static str {
        "disk"
    }

    fn base_path(&self) -> Option<&Path> {
        Some(&self.base)
    }
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

/// What one partition directory holds, as of the last flush.
#[derive(Debug, Default, Clone)]
struct PartState {
    row_count: u64,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
    bytes: u64,
}

struct Buffered {
    row: Row,
    forms: Vec<(String, DtForm)>,
}

#[derive(Serialize, Deserialize)]
struct LeafFile {
    template_id: u64,
    /// The column the days were cut on. Recorded because skipping a partition is only
    /// sound against *this* column: a query ordering by `created_at` while the directories
    /// were cut on `ts` cannot read "newest directory first" as "newest rows first".
    partition_column: String,
}

// ── One leaf ───────────────────────────────────────────────────────────────────────

pub struct DiskLeaf {
    template_id: u64,
    dir: PathBuf,
    partition_column: String,
    parts: BTreeMap<PartKey, PartState>,
    /// Unflushed rows, grouped by the partition they will land in. Grouping at append
    /// time is what makes the ordinal assignable without touching the disk, and what makes
    /// the scan hand them out in the order they will have once written.
    buffer: BTreeMap<PartKey, Vec<Buffered>>,
    buffered: u64,
}

impl DiskLeaf {
    pub fn open(template_id: u64, dir: PathBuf) -> Result<Self, StoreError> {
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
        let partition_column =
            recorded.map_or_else(|| DEFAULT_TS_COLUMN.to_string(), |f| f.partition_column);
        if !leaf_path.exists() {
            let body = serde_json::to_vec_pretty(&LeafFile {
                template_id,
                partition_column: partition_column.clone(),
            })
            .unwrap_or_default();
            fs::write(&leaf_path, body).map_err(io)?;
        }

        let mut parts = BTreeMap::new();
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
            let path = entry.path();
            let Some(m) = Manifest::read(&path) else {
                // A directory with no manifest is a flush that did not reach its last
                // step. Nothing in it is addressable - the row count lives in the file
                // that is missing - so it is skipped and reported rather than guessed at.
                eprintln!(
                    "triedb: WARN leaf {template_id}: {} has no {}, skipping the partition",
                    path.display(),
                    manifest::MANIFEST
                );
                continue;
            };
            for found in m.repair(&path) {
                eprintln!("triedb: WARN leaf {template_id}: {found}");
            }
            parts.insert(
                key,
                PartState {
                    row_count: m.row_count,
                    min_ts: m.min_ts,
                    max_ts: m.max_ts,
                    bytes: m.columns.iter().map(|c| c.bytes).sum(),
                },
            );
        }

        Ok(Self {
            template_id,
            dir,
            partition_column,
            parts,
            buffer: BTreeMap::new(),
            buffered: 0,
        })
    }

    /// Dated partitions newest first, then the undated one.
    ///
    /// That is exactly `Rank`'s order over partitions: `Reverse(ts)` puts the newest first
    /// and `Reverse(None)` - the greatest `Option` reversed - puts the rows that cannot be
    /// placed in time at the end.
    fn partitions_newest_first(&self) -> Vec<PartKey> {
        let mut keys: Vec<PartKey> = self
            .parts
            .keys()
            .chain(self.buffer.keys())
            .copied()
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys.reverse();
        keys
    }

    /// The greatest timestamp any row in this partition carries, buffer included.
    fn newest_in(&self, key: PartKey) -> Option<i64> {
        let flushed = self.parts.get(&key).and_then(|p| p.max_ts);
        let buffered = self.buffer.get(&key).and_then(|rows| {
            rows.iter()
                .filter_map(|b| ts_of(&b.row, &self.partition_column))
                .max()
        });
        match (flushed, buffered) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    fn oldest_in(&self, key: PartKey) -> Option<i64> {
        let flushed = self.parts.get(&key).and_then(|p| p.min_ts);
        let buffered = self.buffer.get(&key).and_then(|rows| {
            rows.iter()
                .filter_map(|b| ts_of(&b.row, &self.partition_column))
                .min()
        });
        match (flushed, buffered) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    fn part_dir(&self, key: PartKey) -> PathBuf {
        self.dir.join(key.dir_name())
    }

    /// Write one partition's buffered rows, returning its new state.
    fn write_partition(
        &self,
        key: PartKey,
        rows: &[Buffered],
        durability: Durability,
    ) -> Result<PartState, StoreError> {
        let dir = self.part_dir(key);
        let io = |source| StoreError::Io {
            template_id: self.template_id,
            source,
        };
        fs::create_dir_all(&dir).map_err(io)?;

        let existing = Manifest::read(&dir).unwrap_or_default();
        let existing_rows = existing.row_count;
        let before: BTreeMap<String, ColumnEntry> = existing
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.clone()))
            .collect();

        // The column set is the partition's, not any one row's: every column the
        // partition already has gets a cell for every new row, null where the row does
        // not carry it, and a column a row brings for the first time is created and
        // back-filled with nulls for the rows already there. The alternative - a file
        // whose length depends on which rows happened to mention it - has no way to line
        // its cells up with anyone else's.
        let mut target: BTreeMap<String, ColKind> = BTreeMap::new();
        for (name, entry) in &before {
            if let Some(kind) = manifest::kind_of_name(&entry.col_type) {
                target.insert(name.clone(), kind);
            }
        }
        for buffered in rows {
            for (name, value) in &buffered.row {
                let Some(kind) = ColKind::of(value) else {
                    // A null claims nothing about the column - here as in the lattice.
                    // The column is still created, so the cell has somewhere to go.
                    target.entry(name.clone()).or_insert(ColKind::Str);
                    continue;
                };
                target
                    .entry(name.clone())
                    .and_modify(|have| *have = ColKind::join(*have, kind))
                    .or_insert(kind);
            }
        }
        // A column that has only ever held nulls was defaulted to `Str` above, which is a
        // claim the lattice would not make. Harmless: every cell in it is the sentinel,
        // and the first real value widens or confirms it.

        let mut columns = Vec::with_capacity(target.len());
        let mut bytes = 0;
        let mut min_ts = existing.min_ts;
        let mut max_ts = existing.max_ts;

        for (name, kind) in &target {
            let file_name = manifest::file_name(name);
            let path = dir.join(&file_name);

            // Promote before opening: the file's header names one type for the whole
            // partition, and `open_append` keeps whatever it finds. Widening is what
            // `types::join` does to a column part-way through a day.
            let existing_form = if path.exists() {
                let header = col::header_of(&path)?;
                if header.kind != *kind {
                    col::promote(&path, *kind)?;
                }
                header.dt_form
            } else {
                None
            };

            let mut file = ColFile::open_append(&path, *kind)?;
            file.observe_form(existing_form);

            // Back-fill a column the partition did not have. `open_append` on a new file
            // starts at zero rows, so this is the gap between it and everyone else.
            for _ in file.row_count()..existing_rows {
                file.push(&ColValue::Null)?;
            }

            for buffered in rows {
                let value = buffered.row.get(name).cloned().unwrap_or(ColValue::Null);
                let form = buffered
                    .forms
                    .iter()
                    .find(|(key, _)| key == name)
                    .map(|(_, form)| *form);
                if *kind == ColKind::Dt {
                    file.observe_form(form);
                }
                // The rung travels with the *row*, not with the file: promoting a
                // datetime to text has to invert the rung that read it, and a partition
                // whose rows came from two rungs would otherwise render one of them wrong.
                file.push(&col::coerce(value, *kind, form))?;
            }

            let (len, dt_form) = file.finish(durability)?;
            bytes += len;
            columns.push(ColumnEntry {
                name: name.clone(),
                file: file_name,
                col_type: manifest::kind_name(*kind),
                dt_form,
                bytes: len,
            });
        }

        for buffered in rows {
            if let Some(ns) = ts_of(&buffered.row, &self.partition_column) {
                min_ts = Some(min_ts.map_or(ns, |m: i64| m.min(ns)));
                max_ts = Some(max_ts.map_or(ns, |m: i64| m.max(ns)));
            }
        }

        let written = Manifest {
            row_count: existing_rows + rows.len() as u64,
            min_ts,
            max_ts,
            columns,
        };
        written.write(&dir, durability).map_err(io)?;

        Ok(PartState {
            row_count: written.row_count,
            min_ts,
            max_ts,
            bytes,
        })
    }

    /// One partition's rows on disk, handed to the visitor.
    ///
    /// Column-major on disk, row-major to the caller: one reader per column, all opened at
    /// the first cell and advanced in lockstep. A `Str` cell is variable-width so there is
    /// no arithmetic that finds row *k*, and lockstep is what makes the file readable a
    /// row at a time without holding the partition in memory.
    fn visit_partition(
        &self,
        key: PartKey,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
        out: &mut Scan,
    ) -> Result<(), StoreError> {
        let dir = self.part_dir(key);
        let Some(m) = Manifest::read(&dir) else {
            return Ok(());
        };
        if m.row_count == 0 {
            return Ok(());
        }

        let ts_name = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();

        let names: Vec<String> = m.columns.iter().map(|c| c.name.clone()).collect();
        let mut readers = Vec::with_capacity(m.columns.len());
        for entry in &m.columns {
            readers.push(ColReader::open(&dir.join(&entry.file))?);
        }
        let ts_at = names.iter().position(|name| name == ts_name);

        // One allocation for the partition, not one per row. Rows that survive are cloned
        // by whoever keeps them; rows that do not never leave this buffer.
        let mut row: Row = Row::with_capacity(names.len());

        for seq in 0..m.row_count {
            out.scanned += 1;
            row.clear();

            // The timestamp first, so a row outside the window costs one decode and a
            // seek per remaining column rather than a decode - which for a string column
            // is the difference between an allocation and none.
            let stamp = match ts_at {
                Some(at) => match readers[at].cell() {
                    Ok(value) => Some(value),
                    Err(e) => return torn(&dir, seq, &e, out),
                },
                None => None,
            };
            let admitted = !windowed || in_window(stamp.as_ref(), since, until);

            let mut torn_at = None;
            for (at, reader) in readers.iter_mut().enumerate() {
                if Some(at) == ts_at {
                    continue;
                }
                let step = if admitted {
                    reader.cell().map(|value| {
                        row.insert(names[at].clone(), value);
                    })
                } else {
                    reader.skip_cell()
                };
                if let Err(e) = step {
                    torn_at = Some(e);
                    break;
                }
            }
            if let Some(e) = torn_at {
                return torn(&dir, seq, &e, out);
            }
            if !admitted {
                continue;
            }
            if let (Some(at), Some(stamp)) = (ts_at, stamp) {
                row.insert(names[at].clone(), stamp);
            }

            if !preds.iter().all(|pred| pred(&row)) {
                continue;
            }
            out.matched += 1;
            v.row(&row, seq);
            if opts.limit != 0 && out.matched as usize >= opts.limit {
                break;
            }
        }
        Ok(())
    }
}

/// A partition that ends before its manifest said it would.
///
/// **A warning and a short read, not a failed query.** With no write-ahead log yet, an
/// unclean exit can leave cells the manifest already counted, and the honest answer is the
/// rows that are actually there - the same "up to one flush interval" the banner promises.
/// Failing the query instead would turn a few lost rows into a template nobody can read
/// again, which is the larger loss and the harder one to notice.
///
/// Every row *before* the tear was read and handed over, so `out` is returned as it stands
/// rather than discarded.
fn torn(dir: &Path, seq: u64, e: &col::ColError, out: &mut Scan) -> Result<(), StoreError> {
    eprintln!(
        "triedb: WARN {}: partition ends at row {seq}: {e}",
        dir.display()
    );
    // The row that could not be read was counted as scanned and did not match.
    out.scanned = out.scanned.saturating_sub(1);
    Ok(())
}

impl LeafStore for DiskLeaf {
    fn template_id(&self) -> u64 {
        self.template_id
    }

    /// The ordinal is **the row's position within its partition**, which is what makes it
    /// survive a restart with nothing stored: it is where the row sits in the file.
    ///
    /// That is narrower than "the ordinal this leaf gave it", and it is enough. `Rank`
    /// only ever compares ordinals between rows that tied on timestamp *and* leaf - and
    /// two rows with the same timestamp are in the same day, so they are in the same
    /// partition. Undated rows all tie, and all live in the one undated partition.
    fn append(&mut self, row: Row, forms: &DtForms) -> u64 {
        let key = PartKey::of(ts_of(&row, &self.partition_column));
        let flushed = self.parts.get(&key).map_or(0, |p| p.row_count);
        let pending = self.buffer.get(&key).map_or(0, Vec::len) as u64;
        self.buffer.entry(key).or_default().push(Buffered {
            row,
            forms: forms.to_vec(),
        });
        self.buffered += 1;
        flushed + pending
    }

    /// **A row is dropped from the buffer only once its write has returned.** The
    /// tempting shape - take the rows, release the lock, write them - opens a window
    /// where a row is in neither the buffer nor a file, and a query landing in it gets a
    /// wrong count with nothing anywhere reporting an error.
    fn flush(&mut self, durability: Durability) -> Result<Flushed, StoreError> {
        let keys: Vec<PartKey> = self.buffer.keys().copied().collect();
        let mut done = Flushed::default();
        let mut failure = None;

        for key in keys {
            let Some(rows) = self.buffer.get(&key) else {
                continue;
            };
            if rows.is_empty() {
                self.buffer.remove(&key);
                continue;
            }
            match self.write_partition(key, rows, durability) {
                Ok(state) => {
                    let count = rows.len() as u64;
                    done.rows += count;
                    done.partitions += 1;
                    done.bytes += state.bytes;
                    self.buffered -= count;
                    self.parts.insert(key, state);
                    self.buffer.remove(&key);
                }
                // Kept buffered, and the other partitions still attempted: one unwritable
                // day must not hold up the rest of the leaf, and the next tick retries.
                Err(e) => failure = failure.or(Some(e)),
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
                // cutting through it. Then the count is the manifest's, and the directory
                // is never opened. Otherwise the skip is refused and the partition is
                // read after all - correct first, and the window prune above has usually
                // already done the larger saving.
                if let Some(matched) = self.countable(key, preds, since, until, windowed) {
                    out.matched += matched;
                    continue;
                }
            }

            if self.parts.get(&key).is_some_and(|p| p.row_count > 0) {
                self.visit_partition(key, opts, preds, v, &mut out)?;
                if opts.limit != 0 && out.matched as usize >= opts.limit {
                    break;
                }
            }

            // Then the rows not yet written. A flush interval affects *visibility*, not
            // durability - an unflushed row is still in the answer - and it goes after the
            // partition's own rows because that is the position it will have once written.
            let flushed = self.parts.get(&key).map_or(0, |p| p.row_count);
            if let Some(rows) = self.buffer.get(&key) {
                for (offset, buffered) in rows.iter().enumerate() {
                    out.scanned += 1;
                    if windowed && !in_window(buffered.row.get(ts_name), since, until) {
                        continue;
                    }
                    if !preds.iter().all(|pred| pred(&buffered.row)) {
                        continue;
                    }
                    out.matched += 1;
                    v.row(&buffered.row, flushed + offset as u64);
                    if opts.limit != 0 && out.matched as usize >= opts.limit {
                        break;
                    }
                }
            }
            if opts.limit != 0 && out.matched as usize >= opts.limit {
                break;
            }
        }
        Ok(out)
    }

    fn len(&self) -> u64 {
        self.parts.values().map(|p| p.row_count).sum::<u64>() + self.buffered
    }

    fn buffered(&self) -> u64 {
        self.buffered
    }

    /// Bytes on disk, plus what the buffer would add. Measured from the manifests rather
    /// than by walking the tree, so `/stats` costs nothing per request.
    fn approx_bytes(&self) -> u64 {
        let on_disk: u64 = self.parts.values().map(|p| p.bytes).sum();
        let pending: u64 = self
            .buffer
            .values()
            .flat_map(|rows| rows.iter())
            .map(|b| {
                b.row
                    .iter()
                    .map(|(key, value)| (key.len() + value_bytes(value)) as u64)
                    .sum::<u64>()
            })
            .sum();
        on_disk + pending
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
        let flushed = self.parts.get(&key).map_or(0, |p| p.row_count);
        let pending = self.buffer.get(&key).map_or(0, Vec::len) as u64;
        let total = flushed + pending;

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
        // The manifest's extremes are tighter than the day when they are known, and the
        // day is the fallback that is always true.
        let lo = self.oldest_in(key).unwrap_or(day_start).max(day_start);
        let hi = self.newest_in(key).unwrap_or(day_end).min(day_end);
        since.is_none_or(|from| hi >= from) && until.is_none_or(|to| lo < to)
    }
}
