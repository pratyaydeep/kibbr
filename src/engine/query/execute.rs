// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The scan: the rows of the leaves a plan chose, in the order an answer gives them, and
//! the counts and groups it makes of them.

use super::*;

// ── Result order ───────────────────────────────────────────────────────────────────

/// Where a row sits in the answer, smaller is earlier: newest first, then the leaf's key
/// hash and the store's ordinal - all facts about the row, so a cursor can name it.
pub type Rank = (Reverse<Option<i64>>, u64, u64);

/// A leaf's place in the order, hashed from its key set so it survives a restart; a NUL
/// inside a token is hashed as `0xFF`, so no two paths spell the same bytes.
pub fn leaf_key(path: &[String]) -> u64 {
    let mut h = FNV_OFFSET;
    for token in path {
        let mut parts = token.split('\0');
        if let Some(first) = parts.next() {
            h = fnv(h, first.as_bytes());
        }
        for part in parts {
            h = fnv(h, &[0xFF]);
            h = fnv(h, part.as_bytes());
        }
        h = fnv(h, &[0]);
    }
    h
}

pub(super) const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a, one more slice folded in.
pub(super) fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// One candidate row, ordered by `Rank` and by nothing else - `Row` is a `HashMap` and has
/// no ordering of its own.
pub(super) struct Ranked {
    pub(super) rank: Rank,
    pub(super) row: Row,
}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.rank.cmp(&other.rank)
    }
}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.rank == other.rank
    }
}
impl Eq for Ranked {}

/// The best `keep` rows by `Rank` in a max-heap of `keep + 1`, or all of them when nothing
/// was capped. The one extra row only answers whether there is a next page.
pub(super) struct Newest {
    /// `None` = keep everything.
    pub(super) keep: Option<usize>,
    pub(super) heap: BinaryHeap<Ranked>,
    /// Whether a store may be told a row is not worth building (`Fetch::wants_row`): `false`
    /// from the first row stored outside its own day, since two rows may then share a `Rank`.
    pub(super) gate: bool,
}

impl Newest {
    pub(super) fn new(keep: Option<usize>) -> Self {
        Self {
            keep,
            heap: BinaryHeap::new(),
            gate: true,
        }
    }

    /// A heap that is offered every row from the first, kept for the tests to hold the
    /// answer to.
    #[cfg(test)]
    pub(super) fn offered_every_row(keep: Option<usize>) -> Self {
        Self {
            gate: false,
            ..Self::new(keep)
        }
    }

    pub(super) fn offer(&mut self, rank: Rank, row: Row) {
        self.heap.push(Ranked { rank, row });
        if let Some(keep) = self.keep {
            // Counted down from a length the push made at least one, not up from `keep`: a client
            // can write `LIMIT 18446744073709551615`, and there is no one more than it.
            if self.heap.len() - 1 > keep {
                // The greatest `Rank` is the last row in result order, so this drops the
                // oldest candidate and never one that belongs in the answer.
                self.heap.pop();
            }
        }
    }

    /// The rank one more `offer` would drop, which a partition is measured against; `None`
    /// while there is still room, and for an unlimited query.
    pub(super) fn worst(&self) -> Option<Rank> {
        let keep = self.keep?;
        if self.heap.len() > keep {
            self.heap.peek().map(|ranked| ranked.rank)
        } else {
            None
        }
    }

    /// In `Rank` order, and the `Rank` the next page should resume after: the overflow row is
    /// the proof that a next page exists, and the row before it is where this one stopped.
    #[cfg(test)]
    pub(super) fn finish(self) -> (Vec<Row>, Option<Rank>) {
        let (ranked, next) = self.finish_ranked();
        (ranked.into_iter().map(|r| r.row).collect(), next)
    }

    /// `finish`, each row still beside its `Rank`, which holds its time.
    pub(super) fn finish_ranked(self) -> (Vec<Ranked>, Option<Rank>) {
        let keep = self.keep;
        let mut ranked = self.heap.into_sorted_vec();
        let more = keep.is_some_and(|n| ranked.len() > n);
        if let Some(n) = keep {
            ranked.truncate(n);
        }
        let next = more.then(|| ranked.last().map(|r| r.rank)).flatten();
        (ranked, next)
    }
}

/// The `Fetch` mode's sink: rank each row, drop the ones an earlier page served, and let
/// the heap keep the best. A struct, because a closure cannot say what is worth opening.
pub(super) struct Fetch<'a> {
    pub(super) keep: &'a mut Newest,
    pub(super) key: u64,
    pub(super) ts: &'a str,
    pub(super) after: Option<Rank>,
    pub(super) order: Order,
}

impl Visitor for Fetch<'_> {
    fn row(&mut self, row: &Row, seq: u64) {
        // `seq` comes from the store, so it is the row's own ordinal and not a count of what
        // this scan admitted.
        let rank = (self.order.rank_ts(effective_ts(row, self.ts)), self.key, seq);
        // Served on an earlier page. `Rank` is a total order, so rows sharing one nanosecond
        // split across the page boundary rather than being repeated or dropped.
        if self.after.is_some_and(|stop| rank <= stop) {
            return;
        }
        self.keep.offer(rank, row.clone());
    }

    /// The best `Rank` any row in this partition could carry, against the worst one already
    /// held: `seq: 0` makes it a lower bound, and `Newest` is shared so the cut tightens.
    fn wants_partition(&mut self, newest: Option<i64>) -> bool {
        // A store says only how new a partition can be, which bounds nothing oldest first:
        // every partition in the window is read.
        if self.order == Order::OldestFirst {
            return true;
        }
        let best = (Reverse(newest), self.key, 0u64);
        self.keep.worst().is_none_or(|worst| best < worst)
    }

    /// Asked before each row while there is a limit to fill, and until the gate is off.
    /// With no limit every row is kept and there is nothing to ask.
    fn wants(&self) -> Wants {
        if self.keep.keep.is_some() && self.keep.gate && self.order == Order::NewestFirst {
            Wants::Newest
        } else {
            Wants::Rows
        }
    }

    /// Whether `row` could change what the heap holds. Refusing one leaves the heap as offering
    /// it would have while all ranks held differ, so the first unplaced row turns the gate off.
    fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
        if !placed {
            self.keep.gate = false;
        }
        if !self.keep.gate {
            return true;
        }
        let rank = (self.order.rank_ts(ts), self.key, seq);
        if self.after.is_some_and(|stop| rank <= stop) {
            return false;
        }
        self.keep.worst().is_none_or(|worst| rank <= worst)
    }
}

/// The `GroupBy` mode's sink: each row counted under its bucket and its label. A struct,
/// because it says which columns it reads, so a store builds only those.
struct Grouped<'a> {
    tally: &'a mut Tally,
    columns: &'a [String],
    /// What `row` reads: `columns`, and under `time(...)` the leaf's time column and the
    /// arrival time a row falls back on.
    reads: Vec<String>,
    ts: &'a str,
    bucket_ns: Option<i64>,
}

impl<'a> Grouped<'a> {
    fn new(
        tally: &'a mut Tally,
        columns: &'a [String],
        ts: &'a str,
        bucket_ns: Option<i64>,
    ) -> Self {
        let mut reads = columns.to_vec();
        if bucket_ns.is_some() {
            reads.push(ts.to_string());
            reads.push(RECEIVED_AT_COLUMN.to_string());
        }
        Self {
            tally,
            columns,
            reads,
            ts,
            bucket_ns,
        }
    }
}

impl Visitor for Grouped<'_> {
    fn row(&mut self, row: &Row, _: u64) {
        // `None` twice over: no `time(...)` was asked for, or this row has no timestamp to
        // place it by. The renderers tell the two apart from the width.
        let bucket = self
            .bucket_ns
            .and_then(|w| effective_ts(row, self.ts).map(|ns| bucket_of(ns, w)));
        *self
            .tally
            .entry((bucket, group_values(row, self.columns)))
            .or_insert(0) += 1;
    }

    fn wants(&self) -> Wants {
        Wants::Columns(self.reads.clone())
    }
}

// ── The scan ───────────────────────────────────────────────────────────────────────

/// Errors an execution can end on: a `StoreError` is the storage layer failing, a
/// `BucketError` is the client's to fix.
/// `Exists` is two questions: no time clause reads no data, a time clause scans to the first row.
#[derive(Debug, Error)]
pub enum ExecError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Buckets(#[from] BucketError),
}

pub fn execute(
    trie: &Trie,
    stores: &StoreManager,
    query: &Query,
) -> Result<QueryResult, ExecError> {
    execute_until(trie, stores, query, &Stop::default())
}

/// `execute` for a caller that may stop wanting the answer; once `stop` is set the query
/// ends with `StoreError::Stopped`, and a stopped query has no partial answer.
pub fn execute_until(
    trie: &Trie,
    stores: &StoreManager,
    query: &Query,
    stop: &Stop,
) -> Result<QueryResult, ExecError> {
    answer(trie, stores, query, Newest::new(query.limit), stop)
}

/// `execute`, handed the heap a fetch keeps its rows in, so a test can hand over one that
/// is offered every row.
pub(super) fn answer(
    trie: &Trie,
    stores: &StoreManager,
    query: &Query,
    mut newest: Newest,
    stop: &Stop,
) -> Result<QueryResult, ExecError> {
    let exists_only = query.mode == QueryMode::Exists;
    let mut opts = query.scan_options();
    opts.stop = stop.clone();
    let started = Instant::now();
    // One snapshot of every planned template (`Snapshot`). Planned again if a load published
    // between the plan and the snapshot, so a load's rows are counted whole or not at all.
    let (plan, snapshot) = loop {
        let before = stores.generation();
        let plan = plan(trie, query);
        if exists_only && query.time.is_open() {
            break (plan, None);
        }
        let ids: Vec<u64> = plan.leaves.iter().map(|l| l.template_id).collect();
        let snapshot = stores.snapshot(&ids, &opts);
        if snapshot.generation() == before {
            break (plan, Some(snapshot));
        }
    };
    let mut result = QueryResult {
        exists: !plan.leaves.is_empty(),
        meta: QueryMeta {
            templates_matched: plan.leaves.len(),
            template_ids: plan.leaves.iter().map(|l| l.template_id).collect(),
            duration: plan.duration,
            ..QueryMeta::default()
        },
        ..QueryResult::default()
    };
    let Some(mut snapshot) = snapshot else {
        return Ok(result);
    };
    // Split once rather than per leaf: the width decides the time axis and the columns
    // decide the label, and neither changes between leaves.
    let (width, columns) = match &query.mode {
        QueryMode::GroupBy(keys) => split_keys(keys),
        _ => (None, Vec::new()),
    };
    let bucket_ns = width.map(width_nanos);
    // Every template has a time source, so a matched template is a time axis; with none
    // matched the answer carries no buckets - `exists: false`.
    let timed = !plan.leaves.is_empty();
    let mut tally = Tally::new();

    for leaf in &plan.leaves {
        // Between two templates, as each scan asks between two row groups: ten thousand
        // templates of a few rows each are ten thousand scans that never get to ask.
        stop.check()?;
        let terms: Vec<Term> = leaf.residual.iter().map(term).collect();
        let ts = leaf.template.time_column.as_str();
        opts.ts_column = Some(leaf.template.time_column.clone());
        opts.text_columns = leaf.template.text_columns.clone();
        opts.int_columns = leaf.template.int_columns.clone();

        // Column pruning: any mode but `Fetch` reads only the residual keys, the group keys and
        // the time columns, so a store may skip the rest. Empty is every column.
        opts.columns = match &query.mode {
            QueryMode::Fetch => Vec::new(),
            _ => {
                let mut wanted: Vec<String> =
                    leaf.residual.iter().map(|term| term.key.clone()).collect();
                wanted.extend(columns.iter().cloned());
                wanted.push(ts.to_string());
                wanted.push(RECEIVED_AT_COLUMN.to_string());
                wanted.sort();
                wanted.dedup();
                wanted
            }
        };

        // The store's own limit is not used for `Fetch`: rows are in arrival order within a
        // partition, so the first `limit` matches are not the newest. `Newest` bounds the memory.
        opts.limit = if exists_only { 1 } else { 0 };

        let id = leaf.template_id;
        let scan = match &query.mode {
            // One row is the whole answer, and `visit` stops on it. Neither mode reads a
            // row: the answer is how many passed, which the scan counts.
            QueryMode::Exists | QueryMode::Count => snapshot.visit(id, &opts, &terms, &mut Counted),
            QueryMode::Fetch => {
                let mut fetch = Fetch {
                    keep: &mut newest,
                    key: leaf_key(&leaf.template.path),
                    ts,
                    after: query.cursor.map(|c| c.rank),
                    order: query.order,
                };
                snapshot.visit(id, &opts, &terms, &mut fetch)
            }
            QueryMode::GroupBy(_) => {
                let mut grouped = Grouped::new(&mut tally, &columns, ts, bucket_ns);
                snapshot.visit(id, &opts, &terms, &mut grouped)
            }
        };
        // A planned template with no leaf is a known template holding no rows: the second
        // of the two empty answers, reached without an error.
        let Some(scan) = scan else {
            continue;
        };
        let scan = scan?;
        result.meta.leaves_opened += 1;

        result.meta.files_opened += scan.files_read;
        result.meta.rows_scanned += scan.scanned;
        result.meta.row_groups_read += scan.row_groups_read;
        result.meta.row_groups_pruned += scan.row_groups_pruned;
        result.meta.bytes_read += scan.bytes_read;
        result.count += scan.matched;
        if exists_only && result.count > 0 {
            break;
        }
    }

    if query.mode == QueryMode::Fetch {
        let (ranked, next) = newest.finish_ranked();
        result.times = ranked.iter().map(|r| query.order.time_of(r.rank)).collect();
        result.rows = ranked.into_iter().map(|r| r.row).collect();
        result.meta.next_cursor = next.map(|rank| Cursor::new(rank, query).encode());
    }
    if matches!(query.mode, QueryMode::GroupBy(_)) {
        result.groups = zero_fill(
            tally,
            width.filter(|_| timed),
            window_span(query),
            columns.len(),
            query.limits,
        )?;
    }
    // Rows in the answer, which for every mode but `Fetch` is none: `count` is what those
    // modes return and it is its own field.
    result.meta.rows_returned = result.rows.len() as u64;
    // The limit dropped rows that matched; only `Fetch` can reach this. Counted against
    // `count`, which the cursor does not reduce: a limit is a statement about this response.
    result.meta.truncated =
        query.mode == QueryMode::Fetch && result.meta.rows_returned < result.count;
    result.meta.limit_source = query.limit_source;
    // From before the plan: a plan made again for a publish that landed under it counts.
    result.meta.duration = started.elapsed();
    Ok(result)
}

/// One residual term as the scan holds it: its key, and its text read through the ladder
/// here, once for the leaf.
pub(super) fn term(m: &Match) -> Term {
    match &m.value {
        MatchValue::Any => Term::any(m.key.clone()),
        MatchValue::Exact(text) => Term::exact(m.key.clone(), text),
    }
}

/// Whether a stored value satisfies `KEY=VALUE`, by its own text first and then through
/// the inference ladder: the contract the oracle calls and `store::Term::matches` is held to.
pub fn value_matches(want: &MatchValue, stored: &ColValue) -> bool {
    match want {
        MatchValue::Any => !matches!(stored, ColValue::Null),
        MatchValue::Exact(text) => {
            matches!(stored, ColValue::String(s) | ColValue::Json(s) if s == text)
                || *stored == infer(text).value
        }
    }
}

/// A row's group label, one value per column key in the order written; missing and null
/// share one label, and the oracle shares this function.
pub fn group_values(row: &Row, keys: &[String]) -> Vec<String> {
    keys.iter()
        .map(|key| {
            row.get(key)
                .map_or_else(|| col_text(&ColValue::Null), col_text)
        })
        .collect()
}
