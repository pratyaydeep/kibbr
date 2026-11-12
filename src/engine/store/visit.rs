// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The scan side of a store: the terms a pattern leaves for the rows to settle, the
//! options a scan is run with, the visitor its surviving rows go to, and how a row is shown.

use std::collections::BTreeMap;
use std::time::SystemTime;

use crate::engine::model::infer::{epoch_digits, infer};
use crate::engine::model::types::ColValue;

use super::Row;
use crate::engine::stop::Stop;

/// One term of a pattern that the path could not settle: the key it reads and what it wants
/// there, as data read through the ladder once so a scan only compares.
#[derive(Debug, Clone, PartialEq)]
pub struct Term {
    key: String,
    want: Want,
}

/// What a term wants of the value under its key.
#[derive(Debug, Clone, PartialEq)]
pub enum Want {
    /// `*`: the key is present and not null.
    Any,
    /// One value: the term's text, and what the ladder reads that text as - `infer(text)`,
    /// run where the term is made and nowhere in a scan. `count` is the integer the text
    /// spells where rule 2 read it as an instant: what it is in a column of integers.
    Exact {
        text: String,
        inferred: ColValue,
        count: Option<i64>,
    },
}

/// How a stored `Datetime` is shown in its column, by what the column's type is now: as
/// stored, as the text it arrived as (`String`), or as the count it arrived as (`Int64`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Shown {
    #[default]
    Stored,
    Text,
    Count,
}

impl From<bool> for Shown {
    /// `true` for a text column: how the question was put before a column could be `Count`.
    fn from(text_column: bool) -> Self {
        if text_column {
            Shown::Text
        } else {
            Shown::Stored
        }
    }
}

impl Term {
    pub fn any(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            want: Want::Any,
        }
    }

    /// A term for one value: the one place the ladder is run on a term's text.
    pub fn exact(key: impl Into<String>, text: &str) -> Self {
        Self {
            key: key.into(),
            want: Want::Exact {
                text: text.to_string(),
                inferred: infer(text).value,
                count: epoch_digits(text),
            },
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn want(&self) -> &Want {
        &self.want
    }

    /// The integer the term's text is in a column of integers: what the ladder read, or the
    /// count rule 2 read as an instant.
    pub(crate) fn whole(&self) -> Option<i64> {
        match &self.want {
            Want::Exact {
                inferred: ColValue::Int64(i),
                ..
            } => Some(*i),
            Want::Exact { count, .. } => *count,
            Want::Any => None,
        }
    }

    /// Whether a value, as a rendered row holds it, satisfies the term: the string reading
    /// first, then the ladder's, as `engine::value_matches` does.
    pub fn matches(&self, seen: &ColValue) -> bool {
        match &self.want {
            Want::Any => !matches!(seen, ColValue::Null),
            Want::Exact { text, inferred, .. } => {
                matches!(seen, ColValue::String(s) | ColValue::Json(s) if s == text)
                    || seen == inferred
            }
        }
    }

    /// `matches`, for one stored value no row was rendered for: in a column that is now `String`
    /// a `Datetime` is compared as the text it arrived as, and in one that is now `Int64` as
    /// the count it arrived as.
    pub fn matches_cell(
        &self,
        value: &ColValue,
        written: Option<&serde_json::Value>,
        shown: impl Into<Shown>,
    ) -> bool {
        match (value, shown.into()) {
            (ColValue::Datetime(_), Shown::Text) => {
                render_text(value, written).is_some_and(|seen| self.matches(&seen))
            }
            (ColValue::Datetime(_), Shown::Count) => match render_count(value, written) {
                Some(count) => self.matches_count(count),
                None => self.matches(value),
            },
            _ => self.matches(value),
        }
    }

    /// Whether a `Datetime` shown as the count `count` satisfies the term: `*`, or a text
    /// that is the same integer, whichever rule the ladder read it by.
    fn matches_count(&self, count: i64) -> bool {
        matches!(self.want, Want::Any) || self.whole() == Some(count)
    }

    /// A row already rendered: a row without the term's key matches nothing, `*` included.
    /// An integer the line wrote as exactly its digits, where the ladder reads those digits
    /// as an instant, is a `Datetime` shown as a count, and is matched as one.
    pub fn matches_row(&self, row: &Row) -> bool {
        row.get(&self.key).is_some_and(|have| {
            self.matches(have)
                || shown_count(have, row.written(&self.key))
                    .is_some_and(|count| self.matches_count(count))
        })
    }

    /// What `matches_row` answers for `render_row` of a stored row, with no row cloned to find
    /// out.
    pub fn matches_in(&self, row: &Row, shown: impl Into<Shown>) -> bool {
        match row.get(&self.key) {
            None => false,
            Some(value) => self.matches_cell(value, row.written(&self.key), shown),
        }
    }
}

/// The column a time filter reads when none is named: a fallback for a `_leaf.json` that
/// predates the time source.
pub(crate) const DEFAULT_TS_COLUMN: &str = "ts";

/// The system column holding a row's arrival time.
pub(crate) use crate::engine::model::types::RECEIVED_AT_COLUMN;

/// What a query returns the system column as, unless the row has an ingested key of that
/// name - then the ingested one is shown and the system value is left out.
pub(crate) const RECEIVED_AT_NAME: &str = "_received_at";

#[derive(Debug, Default, Clone)]
pub struct ScanOptions {
    /// Empty = every column; a read hint, not a projection guarantee, which is what lets the
    /// disk store read only the chunks named.
    pub columns: Vec<String>,
    /// 0 = unlimited.
    pub limit: usize,
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
    /// The template's time column: what the window and the order read, falling
    /// back to `RECEIVED_AT_COLUMN` on a row where it holds no `Datetime`.
    pub ts_column: Option<String>,
    /// Columns whose type is now `String`: a `Datetime` stored in one is compared as the text
    /// it arrived as.
    pub text_columns: Vec<String>,
    /// Columns whose type is now `Int64`: a `Datetime` stored in one as epoch digits is
    /// compared as the integer it arrived as.
    pub int_columns: Vec<String>,
    /// What tells the scan its answer is no longer wanted (`Stop`); the default is never told.
    pub stop: Stop,
}

/// What one scan produced and what it cost: the scan is the only thing that can count
/// what `QueryMeta` reports.
#[derive(Debug, Default)]
pub struct Scan {
    #[cfg(test)]
    pub rows: Vec<Row>,
    /// Row groups whose chunks were read, on every answer's `meta` so a prune that stops is
    /// noticed.
    pub row_groups_read: u64,
    /// Row groups dropped on their footer timestamps, before any column data was read.
    pub row_groups_pruned: u64,
    /// Bytes of column chunk read off disk: where column pruning shows up.
    pub bytes_read: u64,
    /// Segment files opened and read - not leaves, and not row groups; a file whose every row
    /// group the window pruned is not counted.
    pub files_read: u64,
    /// Rows examined, including those the window and the terms rejected; a partition skipped
    /// unread contributes nothing, so the two backends report different numbers.
    pub scanned: u64,
    /// Rows that passed everything.
    pub matched: u64,
}

// ── The scan interface ─────────────────────────────────────────────────────────────

/// What a visitor reads of a row that passed the window and the terms: a hint that lets
/// the disk store build fewer columns, or no row at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wants {
    /// Nothing: the count is `Scan::matched`, and a store need build no row.
    Nothing,
    /// Rows holding at least these columns, in storage order.
    Columns(Vec<String>),
    /// Every column the scan read, in storage order: the default.
    Rows,
    /// Every column the scan read, and only the rows `wants_row` says yes to: for a visitor
    /// that keeps the newest few and would have the rest built to be dropped.
    Newest,
}

/// Where a scan's surviving rows go: a trait and not a closure, which cannot say "stop"
/// or be asked whether the next partition is worth opening.
pub trait Visitor {
    /// One surviving row, beside its ordinal within its partition.
    fn row(&mut self, row: &Row, seq: u64);

    /// Whether the partition whose greatest possible timestamp is `newest` (`None`: undated) is
    /// worth reading; the default is yes, since a count cannot skip rows.
    fn wants_partition(&mut self, newest: Option<i64>) -> bool {
        let _ = newest;
        true
    }

    /// What `row` reads of the row it is handed (`Wants`); the default is the whole row.
    fn wants(&self) -> Wants {
        Wants::Rows
    }

    /// Whether a row is worth building (`Wants::Newest`): advisory, so `row` must not lean on it;
    /// `placed` is false where the row's time is no longer in the day it is stored under.
    fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
        let _ = (ts, seq, placed);
        true
    }
}

/// The visitor of a count: it reads nothing of a row, and says so, so that a store which
/// would have to build each row out of columns builds none.
pub(crate) struct Counted;

impl Visitor for Counted {
    fn row(&mut self, _: &Row, _: u64) {}

    fn wants(&self) -> Wants {
        Wants::Nothing
    }
}

/// A closure as a `Visitor`; not a blanket impl over `FnMut`, which coherence would then
/// hold against every hand-written `Visitor`.
#[cfg(test)]
pub(crate) struct Rows<F>(pub F);

#[cfg(test)]
impl<F: FnMut(&Row, u64)> Visitor for Rows<F> {
    fn row(&mut self, row: &Row, seq: u64) {
        (self.0)(row, seq)
    }
}

/// Whether an instant falls in `[since, until)`; a row with no effective time is outside
/// every window.
pub(crate) fn in_window(ts: Option<i64>, since: Option<i64>, until: Option<i64>) -> bool {
    let Some(ns) = ts else {
        return false;
    };
    since.is_none_or(|from| ns >= from) && until.is_none_or(|to| ns < to)
}

/// A row as a query shows it, as `(name shown, key in the row)`: the system column under
/// its query name, unless an ingested key has that name or its value is null.
pub fn present(row: &Row) -> BTreeMap<String, &str> {
    let mut out: BTreeMap<String, &str> = row
        .keys()
        .filter(|key| key.as_str() != RECEIVED_AT_COLUMN)
        .map(|key| (key.clone(), key.as_str()))
        .collect();
    if let Some(system) = row.get(RECEIVED_AT_COLUMN) {
        if !matches!(system, ColValue::Null) && !out.contains_key(RECEIVED_AT_NAME) {
            out.insert(RECEIVED_AT_NAME.to_string(), RECEIVED_AT_COLUMN);
        }
    }
    out
}

/// A `Datetime` stored in a column that is now text, as the text a query compares it
/// against: the JSON the line said; `None` for any other value.
pub(crate) fn render_text(
    value: &ColValue,
    written: Option<&serde_json::Value>,
) -> Option<ColValue> {
    let ColValue::Datetime(ns) = value else {
        return None;
    };
    Some(ColValue::String(match written {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => crate::engine::display::rfc3339(*ns),
    }))
}

/// A row with every text column's `Datetime` rendered back to the text it arrived as,
/// or `None` when nothing in it needs rendering.
pub(crate) fn render_row(row: &Row, text_columns: &[String]) -> Option<Row> {
    let mut out: Option<Row> = None;
    for column in text_columns {
        let Some(value) = row.get(column) else {
            continue;
        };
        if let Some(text) = render_text(value, row.written(column)) {
            out.get_or_insert_with(|| row.clone())
                .set_value(column, text);
        }
    }
    out
}

/// A `Datetime` that arrived as epoch digits, as the integer those digits spell: what a
/// query compares it as in a column that is now `Int64`. `None` for any other value, and
/// for a `Datetime` that arrived as RFC 3339 text, which is no integer.
///
/// Exact, because the digits are read from what the line said and never worked back from
/// the instant: a row holds them beside the value, and a row group stores them as the
/// instant in whole seconds or milliseconds only when that prints the same digits.
pub(crate) fn render_count(value: &ColValue, written: Option<&serde_json::Value>) -> Option<i64> {
    let ColValue::Datetime(_) = value else {
        return None;
    };
    match written? {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(text) => plain_digits(text),
        _ => None,
    }
}

/// Text that is an integer in plain decimal and nothing else: no sign, no leading zero.
fn plain_digits(text: &str) -> Option<i64> {
    let padded = text.len() > 1 && text.starts_with('0');
    let plain = !text.is_empty() && !padded && text.bytes().all(|b| b.is_ascii_digit());
    if plain {
        text.parse().ok()
    } else {
        None
    }
}

/// The count a rendered value is a `Datetime` shown as: an integer in the epoch window's
/// ten or thirteen digits that the line wrote as exactly those digits. The ladder reads
/// such a value as an instant, so a row holds it as an integer only by `render_count`.
pub(crate) fn shown_count(seen: &ColValue, written: Option<&serde_json::Value>) -> Option<i64> {
    let ColValue::Int64(have) = seen else {
        return None;
    };
    let said = match written? {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(text) => plain_digits(text),
        _ => None,
    }?;
    (said == *have && epoch_digits(&said.to_string()) == Some(said)).then_some(said)
}

/// `render_row` for a scan: its text columns, and then a `Datetime` in each column that is
/// now `Int64` as the count it arrived as. `None` when nothing in the row needs rendering.
pub(crate) fn shown_row(row: &Row, opts: &ScanOptions) -> Option<Row> {
    let mut out = render_row(row, &opts.text_columns);
    for column in &opts.int_columns {
        let Some(value) = row.get(column) else {
            continue;
        };
        if let Some(count) = render_count(value, row.written(column)) {
            out.get_or_insert_with(|| row.clone())
                .set_value(column, ColValue::Int64(count));
        }
    }
    out
}

#[cfg(test)]
pub(crate) fn project(row: &Row, columns: &[String]) -> Row {
    if columns.is_empty() {
        return row.clone();
    }
    let mut out = Row::with_capacity(columns.len());
    for name in columns {
        if let Some(value) = row.get(name) {
            out.insert_written(name.clone(), value.clone(), row.written(name).cloned());
        }
    }
    out
}
