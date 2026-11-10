// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The query engine: a plan, a scan, and the result shape every surface above reports.
//!
//! The plan chooses templates from the trie and touches no data. The scan opens only the
//! leaves the plan chose. The split is enforced by the types: `plan` cannot see a store,
//! so `--explain` cannot accidentally read a row.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use thiserror::Error;

pub mod answer;
pub mod limits;
pub mod trieql;

use crate::query::limits::Limits;
use crate::model::tokens::{promoted_of, promoted_token};
use crate::fmt::{col_text, commas, duration_text};
use crate::model::infer::infer;
use crate::model::types::ColValue;
use crate::rows::{
    effective_ts, nanos_of, Counted, Row, ScanOptions, Stop, StoreError, StoreManager, Term,
    Visitor, Wants, RECEIVED_AT_COLUMN,
};
use crate::templates::trie::{KeyIndex, Node, Posting, Template, Trie};

/// The column a `time(...)` bucket is reported under, on every surface.
pub const BUCKET_COLUMN: &str = "bucket";

/// One key of a `GROUP BY`, as the clause named it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKey {
    Column(String),
    /// `time(<width>)` - the bucket width; at most one per clause, because the zero-fill has
    /// one time axis.
    Time(Duration),
}

impl GroupKey {
    /// How the key is written back - a table header, an NDJSON field, `--explain`.
    pub fn name(&self) -> &str {
        match self {
            Self::Column(key) => key,
            Self::Time(_) => BUCKET_COLUMN,
        }
    }
}

/// The `time(...)` width and the column keys, split out of what the clause named: one
/// decides the time axis, the others the label.
pub fn split_keys(keys: &[GroupKey]) -> (Option<Duration>, Vec<String>) {
    let mut width = None;
    let mut columns = Vec::new();
    for key in keys {
        match key {
            GroupKey::Column(name) => columns.push(name.clone()),
            GroupKey::Time(w) => width = Some(*w),
        }
    }
    (width, columns)
}

/// The HTTP `/query` surface and the Elasticsearch translation layer both consume this
/// shape unchanged, so changing it later means changing both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMode {
    Exists,
    Fetch,
    Count,
    GroupBy(Vec<GroupKey>),
}

/// One group of a `GROUP BY` and the rows in it; a struct, not a map entry, because a map
/// cannot say "this bucket exists and is empty".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// The bucket's start in Unix nanoseconds, or `None` for rows that cannot be placed in
    /// time and whenever the clause named no `time(...)`.
    pub bucket: Option<i64>,
    /// The column values, in the order the keys were written.
    pub values: Vec<String>,
    pub count: u64,
}

#[derive(Debug, Default)]
pub struct QueryResult {
    /// `false` = no such template, which is distinct from `exists: true, count: 0`.
    pub exists: bool,
    pub count: u64,
    pub rows: Vec<Row>,
    /// Each row's time, beside it: the effective timestamp (nanoseconds since the epoch) the
    /// row is ranked by, `None` for a row with none. Always as long as `rows`.
    pub times: Vec<Option<i64>>,
    /// Ordered by bucket ascending - the unplaceable `None` first - then by values; the order
    /// is part of the answer because `scripts/check/agree.sh` diffs the JSON as text.
    pub groups: Vec<Group>,
    pub meta: QueryMeta,
}

#[derive(Debug, Default)]
pub struct QueryMeta {
    pub templates_matched: usize,
    pub template_ids: Vec<u64>,
    /// **Segment files** read, not leaves; a file whose every row group a window pruned is
    /// not in it.
    pub files_opened: u64,
    /// Leaves with a store, opened for this query.
    pub leaves_opened: usize,
    /// Row groups whose chunks were read; `row_groups_pruned` are those dropped on their
    /// footer timestamps before any column data was.
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    /// Bytes of column chunk read off disk - where column pruning shows up.
    pub bytes_read: u64,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    /// The limit dropped rows that matched (`rows_returned < count`); its own field because
    /// every surface has to say so.
    pub truncated: bool,
    /// Who set the page size; only `backstop` *and* `truncated` together mean "narrow your
    /// query".
    pub limit_source: LimitSource,
    /// Hand this back as `AFTER` for the next page; `None` unless a `LIMIT` stopped short of
    /// the end, so no cursor yields an empty page.
    pub next_cursor: Option<String>,
    /// Walk plus scan; loading the trie is not query time.
    pub duration: Duration,
}

/// Who set the cap on rows returned; read with `truncated`, see `QueryMeta::limit_source`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum LimitSource {
    /// Nothing capped this answer - no `LIMIT`, no backstop, or a mode returning no rows.
    #[default]
    Unlimited,
    /// The caller's own `LIMIT`.
    Caller,
    /// The server's max_rows limit, because the query named no `LIMIT`.
    Backstop,
}

impl LimitSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unlimited => "unlimited",
            Self::Caller => "caller",
            Self::Backstop => "backstop",
        }
    }
}

/// Which end of time a `Fetch` answer starts at. Rows with no time come last either way.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    #[default]
    NewestFirst,
    OldestFirst,
}

impl Order {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewestFirst => "newest_first",
            Self::OldestFirst => "oldest_first",
        }
    }

    /// A row's time as the first component of its `Rank`: oldest first ranks by the bitwise
    /// complement, which reverses the order with no overflow at either end of `i64`.
    pub fn rank_ts(self, ts: Option<i64>) -> Reverse<Option<i64>> {
        match self {
            Self::NewestFirst => Reverse(ts),
            Self::OldestFirst => Reverse(ts.map(|t| !t)),
        }
    }

    /// `rank_ts` undone: the row's own time back out of its `Rank`.
    pub fn time_of(self, rank: Rank) -> Option<i64> {
        let Reverse(ts) = rank.0;
        match self {
            Self::NewestFirst => ts,
            Self::OldestFirst => ts.map(|t| !t),
        }
    }
}

/// One term: a `--match KEY=VALUE` flag, or one `"key": "value"` pair of a pattern.
#[derive(Debug, Clone, PartialEq)]
pub struct Match {
    pub key: String,
    pub value: MatchValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MatchValue {
    /// `*` - the key is present and not null.
    Any,
    Exact(String),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TimeRange {
    pub since: Option<SystemTime>,
    pub until: Option<SystemTime>,
}

impl TimeRange {
    pub fn is_open(&self) -> bool {
        self.since.is_none() && self.until.is_none()
    }
}

/// A query, already built: the flags and `crate::query::trieql` build the same struct, so the
/// engine has one input whichever surface asked.
#[derive(Debug)]
pub struct Query {
    pub matches: Vec<Match>,
    pub mode: QueryMode,
    pub time: TimeRange,
    /// `None` = every matching row; `Query::new` supplies `max_rows` as a page size where
    /// the caller named none, so an unlimited `MATCH` cannot materialise every row.
    pub limit: Option<usize>,
    /// Who supplied `limit`.
    pub limit_source: LimitSource,
    /// Where the last page stopped, from `QueryMeta::next_cursor`; `None` = start at the
    /// first row in `order`.
    pub cursor: Option<Cursor>,
    /// Which end of time the rows start at; only a `Fetch` returns rows to order.
    pub order: Order,
    /// The instant the query was built against - the edge `GROUP BY time(...)` zero-fills to
    /// with no `until` - kept out of `Cursor::guard` so pages a moment apart agree.
    pub now: SystemTime,
    /// What this query may not exceed, carried so the engine reads the number the CLI was
    /// configured with.
    pub limits: Limits,
}

impl Query {
    /// The one constructor, so the flag surface and TrieQL resolve the limit identically: it
    /// caps rows returned, not counted, and goes with the cursor in a mode returning no rows.
    pub fn new(
        matches: Vec<Match>,
        mode: QueryMode,
        time: TimeRange,
        limit: Option<usize>,
        cursor: Option<Cursor>,
        now: SystemTime,
        limits: Limits,
    ) -> Self {
        let fetching = mode == QueryMode::Fetch;
        let (limit, limit_source) = match (fetching, limit, limits.max_rows) {
            (false, ..) => (None, LimitSource::Unlimited),
            (true, Some(n), _) => (Some(n), LimitSource::Caller),
            (true, None, 0) => (None, LimitSource::Unlimited),
            (true, None, cap) => (Some(cap), LimitSource::Backstop),
        };
        Self {
            matches,
            mode,
            time,
            limit,
            limit_source,
            cursor: if fetching { cursor } else { None },
            order: Order::NewestFirst,
            now,
            limits,
        }
    }

    /// The query answering oldest first, or newest first. Kept out of `new` so every caller
    /// that never orders reads as it did; a mode returning no rows ignores it, as it does a
    /// cursor.
    pub fn with_order(mut self, order: Order) -> Self {
        if self.mode == QueryMode::Fetch {
            self.order = order;
        }
        self
    }

    /// The `time(...)` width this query groups by, or `None` for every other mode.
    pub fn bucket_width(&self) -> Option<Duration> {
        match &self.mode {
            QueryMode::GroupBy(keys) => split_keys(keys).0,
            _ => None,
        }
    }

    /// The pattern as trie tokens, for `--explain`.
    pub fn tokens(&self) -> Vec<String> {
        self.matches
            .iter()
            .map(|m| match &m.value {
                MatchValue::Any => format!("{}=*", m.key),
                MatchValue::Exact(v) => format!("{}={v}", m.key),
            })
            .collect()
    }

    pub fn scan_options(&self) -> ScanOptions {
        ScanOptions {
            columns: Vec::new(),
            // The storage layer's spelling of "unlimited", which is not this one's.
            limit: self.limit.unwrap_or(0),
            since: self.time.since,
            until: self.time.until,
            ts_column: None,
            text_columns: Vec::new(),
            // Whoever executes the query says: see `execute_until`.
            stop: Stop::default(),
        }
    }

    /// A cursor of this query at a rank it produced - the one way to mint one, so a cursor
    /// cannot be built without a query to answer for it.
    pub fn cursor_at(&self, rank: Rank) -> Cursor {
        Cursor::new(rank, self)
    }
}

mod buckets;
mod cursor;
mod execute;
mod plan;

pub use buckets::*;
pub use cursor::*;
pub use execute::*;
pub use plan::*;

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// The bucket arithmetic, which the CLI surface can only reach one query at a time: its
/// properties hold for any range and width, so they are checked over generated ones.
#[cfg(test)]
mod tests;
