//! In-memory leaf storage and the scan interface over it.
//!
//! One `Vec<Row>` per leaf. No partitions, no disk, no flush timer: the flush clock lives
//! in the store registry, and there is neither a clock nor anywhere to flush to yet. What
//! has to be right now is the *shape*, because the disk and Parquet stores swap the
//! implementation in behind it.

// Parts of this interface have no in-memory caller: nothing projects columns yet, nothing
// fails to open a store that cannot be closed, and `template_id` is a file header nothing
// writes. Written now because the disk store replaces the body of this module and not its
// callers - the retrofit it exists to avoid. Scoped so the allow stays here.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::types::ColValue;

/// Absence on a line is `ColValue::Null`, not a missing entry - the row is built from the
/// leaf's key set, and the key set is the path.
pub type Row = HashMap<String, ColValue>;

/// Passed beside `ScanOptions` rather than inside it: `Box<dyn Fn>` is not `Clone`, which
/// would cost `ScanOptions` its `Default`.
pub type Predicate = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// The column a time filter reads when none is named.
pub const DEFAULT_TS_COLUMN: &str = "ts";

#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Empty = every column.
    pub columns: Vec<String>,
    /// 0 = unlimited.
    pub limit: usize,
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
    pub ts_column: Option<String>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    /// Unreachable in memory, and here because a disk store will reach it.
    #[error("no store open for leaf {template_id}")]
    NotOpen { template_id: u64 },
}

/// What one scan produced and what it cost.
///
/// A scan could return a bare `Vec<Row>`, but `QueryMeta` reports `rows_scanned` on every
/// result and the scan is the only thing that can count it. Carrying the counters out is
/// the smallest change that keeps `meta` populated from the start rather than bolted on.
#[derive(Debug, Default)]
pub struct Scan {
    pub rows: Vec<Row>,
    /// Rows examined, including those the time window and the predicates rejected.
    pub scanned: u64,
    /// Rows that passed everything.
    pub matched: u64,
}

/// One leaf's rows. Append-only; nothing deletes.
pub struct LeafStore {
    pub template_id: u64,
    rows: Vec<Row>,
}

impl LeafStore {
    pub fn new(template_id: u64) -> Self {
        Self {
            template_id,
            rows: Vec::new(),
        }
    }

    pub fn append(&mut self, row: Row) {
        self.rows.push(row);
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Hand every surviving row to `f`. The one place filtering happens - `scan` and the
    /// engine's count and group-by paths all come through here, so they cannot drift.
    ///
    /// `scanned` counts rows looked at, so a time window that rejects everything still
    /// reports the work it did. Under a limit the loop stops early and `scanned` stops
    /// with it, which is the honest number.
    pub fn visit(
        &self,
        opts: &ScanOptions,
        preds: &[Predicate],
        f: &mut dyn FnMut(&Row),
    ) -> Result<Scan, StoreError> {
        let ts = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(nanos_of);
        let until = opts.until.map(nanos_of);
        let windowed = since.is_some() || until.is_some();

        let mut out = Scan::default();
        for row in &self.rows {
            out.scanned += 1;
            if windowed && !in_window(row.get(ts), since, until) {
                continue;
            }
            if !preds.iter().all(|pred| pred(row)) {
                continue;
            }
            out.matched += 1;
            f(row);
            if opts.limit != 0 && out.matched as usize >= opts.limit {
                break;
            }
        }
        Ok(out)
    }

    /// Every surviving row, projected through `opts.columns`.
    pub fn scan(&self, opts: &ScanOptions, preds: &[Predicate]) -> Result<Scan, StoreError> {
        let mut rows = Vec::new();
        let mut scan = self.visit(opts, preds, &mut |row| rows.push(project(row, &opts.columns)))?;
        scan.rows = rows;
        Ok(scan)
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
fn in_window(value: Option<&ColValue>, since: Option<i64>, until: Option<i64>) -> bool {
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

fn project(row: &Row, columns: &[String]) -> Row {
    if columns.is_empty() {
        return row.clone();
    }
    columns
        .iter()
        .filter_map(|name| row.get(name).map(|v| (name.clone(), v.clone())))
        .collect()
}

/// The registry of open stores, minus the flush clock that will own it later.
///
/// `BTreeMap`, so `open` order is template order and two runs over one file report the
/// same leaves in the same sequence.
#[derive(Default)]
pub struct Stores {
    stores: BTreeMap<u64, LeafStore>,
    rows: u64,
}

impl Stores {
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens the store on first write.
    pub fn append(&mut self, template_id: u64, row: Row) {
        self.stores
            .entry(template_id)
            .or_insert_with(|| LeafStore::new(template_id))
            .append(row);
        self.rows += 1;
    }

    pub fn get(&self, template_id: u64) -> Result<&LeafStore, StoreError> {
        self.stores
            .get(&template_id)
            .ok_or(StoreError::NotOpen { template_id })
    }

    /// A leaf with no store is a template that exists and holds no rows - the second of
    /// the two empty answers, and not an error.
    pub fn lookup(&self, template_id: u64) -> Option<&LeafStore> {
        self.stores.get(&template_id)
    }

    pub fn open(&self) -> usize {
        self.stores.len()
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }
}
