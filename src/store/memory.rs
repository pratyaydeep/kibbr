//! The in-memory store: one `Vec<Row>` per leaf.
//!
//! Kept, not replaced. It is what `triedb query <file>` builds - a database rebuilt per
//! invocation has nothing to persist to - and it is the other half of the differential
//! that checks the disk store: two implementations of one interface that must return the
//! same answers, so a disagreement is a bug found with no hand-written assertion.
//!
//! It has no partitions, so `Visitor::wants_partition` is never consulted, and its
//! `flush()` is the honest no-op rather than an error - "written" and "never needed
//! writing" are the same state here. It has no row groups either, so the three counters
//! the disk store fills stay zero: reporting work it did not do would make the two
//! implementations agree about the wrong thing.
//!
//! It keeps each row's `dt_form` evidence beside the row for the same reason the disk
//! store writes it: a `Datetime` in a column that later widens to `String` has to come
//! back as the text it arrived as, and the rung is the only way to spell it.

use std::path::Path;

use super::{
    effective_ts, in_window, render_text, value_bytes, DtForms, FlushKind, Flushed, LeafStore,
    Predicate, Row, Scan, ScanOptions, Store, StoreError, Visitor, DEFAULT_TS_COLUMN,
};
use crate::types::DtForm;

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
    fn open(&self, template_id: u64, _time_column: &str) -> Result<Box<dyn LeafStore>, StoreError> {
        Ok(Box::new(MemoryLeaf::new(template_id)))
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
    rows: Vec<(Row, Vec<(String, DtForm)>)>,
}

impl MemoryLeaf {
    pub fn new(template_id: u64) -> Self {
        Self {
            template_id,
            rows: Vec::new(),
        }
    }
}

impl LeafStore for MemoryLeaf {
    fn template_id(&self) -> u64 {
        self.template_id
    }

    /// The ordinal is the `Vec` index, which is the order the row arrived in. Nothing
    /// deletes, so it is stable for the life of the process - and this store has no next
    /// process to be stable into.
    fn append(&mut self, row: Row, forms: &DtForms) -> u64 {
        let seq = self.rows.len() as u64;
        self.rows.push((row, forms.to_vec()));
        seq
    }

    fn flush(&mut self, _kind: FlushKind) -> Result<Flushed, StoreError> {
        Ok(Flushed::default())
    }

    /// `scanned` counts rows looked at, so a time window that rejects everything still
    /// reports the work it did. Under a limit the loop stops early and `scanned` stops
    /// with it, which is the honest number.
    ///
    /// **The limit is honoured here and the caller still must not lean on it for a
    /// fetch.** These rows are in insertion order, not timestamp order, so stopping at
    /// the first `limit` matches would return the *oldest* rows under an order that
    /// promises the newest. The engine passes 0 for that reason; see `execute`.
    fn visit(
        &self,
        opts: &ScanOptions,
        preds: &[Predicate],
        v: &mut dyn Visitor,
    ) -> Result<Scan, StoreError> {
        let ts = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
        let since = opts.since.map(super::nanos_of);
        let until = opts.until.map(super::nanos_of);
        let windowed = since.is_some() || until.is_some();

        let mut out = Scan::default();
        let mut rendered = Row::new();
        for (seq, (stored, forms)) in self.rows.iter().enumerate() {
            out.scanned += 1;
            // A column that has widened to text since this row arrived: its `Datetime`
            // is rendered back through the row's own rung before the predicates see it,
            // so `order_id=4500000101` matches the text that was ingested and a `MATCH`
            // returns it. The effective time is read first, off the instant.
            let row = if opts.text_columns.is_empty() {
                stored
            } else {
                rendered.clear();
                for (key, value) in stored {
                    let value = if opts.text_columns.iter().any(|c| c == key) {
                        let form = forms.iter().find(|(k, _)| k == key).map(|(_, f)| *f);
                        render_text(value.clone(), form)
                    } else {
                        value.clone()
                    };
                    rendered.insert(key.clone(), value);
                }
                &rendered
            };
            if windowed && !in_window(effective_ts(stored, ts), since, until) {
                continue;
            }
            if !preds.iter().all(|pred| pred(row)) {
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
        self.rows.len() as u64
    }

    /// Everything and nothing: these rows are only ever in memory, so none of them is
    /// *waiting* to be written. Reporting `len()` here would tell an operator a `kill -9`
    /// costs the buffer, when what it costs is the database.
    fn buffered(&self) -> u64 {
        0
    }

    /// Roughly what this leaf's rows weigh, in bytes.
    ///
    /// `/stats` promises storage sizes, and while storage is a `Vec<Row>` there is no
    /// file to `stat`. Counted rather than estimated from a row average, because the
    /// number's whole job is to tell an operator how close an in-memory database is to
    /// the memory it has - and templates differ by an order of magnitude in row width.
    ///
    /// Payload only: key text, string values, and the fixed width of a scalar. Allocator
    /// slack and the `HashMap` table are excluded, so the true resident size is larger.
    /// It is labelled an estimate on the endpoint for exactly that reason.
    fn approx_bytes(&self) -> u64 {
        self.rows
            .iter()
            .map(|(row, _)| {
                row.iter()
                    .map(|(key, value)| (key.len() + value_bytes(value)) as u64)
                    .sum::<u64>()
            })
            .sum()
    }
}
