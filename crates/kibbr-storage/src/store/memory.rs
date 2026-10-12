//! The in-memory store: one `Vec<Row>` per leaf.
//!
//! Kept as what `kibbr query <file>` builds, and as the other half of the differential
//! that checks the disk store. A query holds its leaf's read lock while it scans,
//! and the disk store's three row-group counters stay zero: no work is reported undone.

use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use super::{
    effective_ts, in_window, render_row, value_bytes, LeafStore, Row, Scan, ScanOptions, Store,
    StoreError, Term, Visitor, DEFAULT_TS_COLUMN,
};

pub struct MemoryStore;

impl MemoryStore {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for MemoryStore {
    fn open(&self, template_id: u64, _time_column: &str) -> Result<Arc<dyn LeafStore>, StoreError> {
        Ok(Arc::new(MemoryLeaf::new(template_id)))
    }

    fn kind(&self) -> &'static str {
        "memory"
    }

    fn base_path(&self) -> Option<&Path> {
        None
    }
}

pub struct MemoryLeaf {
    template_id: u64,
    rows: RwLock<Vec<Row>>,
}

impl MemoryLeaf {
    pub fn new(template_id: u64) -> Self {
        Self {
            template_id,
            rows: RwLock::new(Vec::new()),
        }
    }

    fn rows(&self) -> RwLockReadGuard<'_, Vec<Row>> {
        self.rows.read().unwrap_or_else(|e| e.into_inner())
    }
}

impl LeafStore for MemoryLeaf {
    fn template_id(&self) -> u64 {
        self.template_id
    }

    /// The ordinal is the `Vec` index, the order the row arrived in; nothing deletes, so it
    /// is stable for the life of the process.
    fn append(&self, row: Row) -> u64 {
        let mut rows = self.rows.write().unwrap_or_else(|e| e.into_inner());
        rows.push(row);
        rows.len() as u64 - 1
    }

    /// The limit is honoured, but these rows are in insertion order, so a fetch that wants
    /// the newest must not lean on it: the engine passes 0 (see `execute`).
    fn visit(
        &self,
        opts: &ScanOptions,
        terms: &[Term],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        let ts = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();

        let mut out = Scan::default();
        for (seq, stored) in self.rows().iter().enumerate() {
            if seq % super::STOP_EVERY_ROWS == 0 {
                opts.stop.check()?;
            }
            out.scanned += 1;
            // A column that has widened to text since this row arrived: its `Datetime` is compared
            // as the text it was ingested as. The effective time is read first, off the instant.
            let rendered = render_row(stored, &opts.text_columns);
            let row = rendered.as_ref().unwrap_or(stored);
            if windowed && !in_window(effective_ts(stored, ts), since, until) {
                continue;
            }
            if !terms.iter().all(|term| term.matches_row(row)) {
                continue;
            }
            out.matched += 1;
            v.row(row, seq as u64);
            if opts.limit != 0 && out.matched as usize >= opts.limit {
                break;
            }
        }
        Ok(out)
    }

    fn len(&self) -> u64 {
        self.rows().len() as u64
    }

    /// Zero: these rows are only ever in memory, so none is waiting to be written, and
    /// `len()` would tell an operator a `kill -9` costs only the buffer.
    fn buffered(&self) -> u64 {
        0
    }

    /// Roughly what this leaf's rows weigh in bytes, payload only: counted, not estimated,
    /// because templates differ widely in row width.
    fn approx_bytes(&self) -> u64 {
        self.rows()
            .iter()
            .map(|row| {
                row.iter()
                    .map(|(key, value)| (key.len() + value_bytes(value)) as u64)
                    .sum::<u64>()
            })
            .sum()
    }
}
