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

use crate::limits::Limits;
use kibbr_core::flatten::{promoted_of, promoted_token};
use kibbr_core::fmt::{col_text, commas, duration_text};
use kibbr_core::infer::infer;
use kibbr_core::types::ColValue;
use kibbr_storage::store::{
    effective_ts, nanos_of, Counted, Row, ScanOptions, Stop, StoreError, StoreManager, Term,
    Visitor, Wants, RECEIVED_AT_COLUMN,
};
use kibbr_storage::trie::{KeyIndex, Node, Posting, Template, Trie};

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

/// A query, already built: the flags and `crate::trieql` build the same struct, so the
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
    /// newest row.
    pub cursor: Option<Cursor>,
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
            now,
            limits,
        }
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

// ── Time buckets ───────────────────────────────────────────────────────────────────
//
// Boundaries are multiples of the width from the Unix epoch, so overlapping queries agree
// on where the bars go. A range emits every bucket it touches, so unaligned edges add one.

/// What a bucketed `GROUP BY` was refused for; a refusal, not a truncation, and it names
/// both the requested and the permitted count because only the pair is actionable.
#[derive(Debug, PartialEq, Eq)]
pub enum BucketError {
    TooManyBuckets {
        width: String,
        requested: u64,
        permitted: usize,
    },
    TooManyGroups {
        width: String,
        buckets: u64,
        series: usize,
        requested: u64,
        permitted: usize,
    },
}

// Hand-written rather than derived, because every number in these sentences is rendered
// with thousands separators.
impl std::fmt::Display for BucketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyBuckets {
                width,
                requested,
                permitted,
            } => write!(
                f,
                "`GROUP BY time({width})` over this range needs {} buckets; the limit is \
                 {} - widen the bucket or narrow the range",
                commas(*requested),
                commas(*permitted as u64)
            ),
            Self::TooManyGroups {
                width,
                buckets,
                series,
                requested,
                permitted,
            } => write!(
                f,
                "`GROUP BY time({width})` over this range needs {} buckets across {} \
                 distinct groups, which is {} rows; the limit is {} - drop a group key, \
                 widen the bucket, or narrow the range",
                commas(*buckets),
                commas(*series as u64),
                commas(*requested),
                commas(*permitted as u64)
            ),
        }
    }
}

impl std::error::Error for BucketError {}

/// A bucket width in nanoseconds, saturating: `time(1000000000d)` fits no `i64`, and one
/// bucket covering every representable instant is the honest answer to it.
fn width_nanos(width: Duration) -> i64 {
    i64::try_from(width.as_nanos()).unwrap_or(i64::MAX).max(1)
}

/// A timestamp's bucket start in Unix nanoseconds, floored on the epoch's grid and not the
/// query's, so a chart does not shift its bars when the range is nudged.
pub fn bucket_of(ns: i64, width: i64) -> i64 {
    ns.div_euclid(width).saturating_mul(width)
}

/// The first bucket start at or after `ns` - the exclusive end of a span covering
/// `[.., ns)`.
fn bucket_after(ns: i64, width: i64) -> i64 {
    bucket_of(ns.saturating_sub(1), width).saturating_add(width)
}

/// The half-open span of bucket starts a query's own window implies, `[lo, hi)`; `None`
/// with no `since`. The right edge is `until`, or `now`, which is not a filter.
fn window_span(query: &Query) -> Option<(i64, i64)> {
    let width = width_nanos(query.bucket_width()?);
    let since = query.time.since?;
    let lo = bucket_of(nanos_of(since), width);
    let end = nanos_of(query.time.until.unwrap_or(query.now));
    Some((lo, bucket_after(end, width).max(lo)))
}

/// The window's span widened to hold every bucket that received a row, which keeps
/// `sum(groups) == count` true for a row ahead of `now` or a window with no lower bound.
fn span_with(base: Option<(i64, i64)>, seen: Option<(i64, i64)>, width: i64) -> Option<(i64, i64)> {
    match (base, seen) {
        (Some((lo, hi)), Some((min, max))) => {
            Some((lo.min(min), hi.max(max.saturating_add(width))))
        }
        (Some(span), None) => Some(span),
        (None, Some((min, max))) => Some((min, max.saturating_add(width))),
        (None, None) => None,
    }
}

/// How many buckets a span holds, rounded up so the saturated bottom bucket's rows are
/// not dropped out of the sum.
fn span_len(span: (i64, i64), width: i64) -> u64 {
    let (lo, hi) = span;
    u64::try_from(hi.saturating_sub(lo))
        .unwrap_or(0)
        .div_ceil(width.unsigned_abs().max(1))
}

/// The bucket count against `max_buckets`, before anything is read, so a refused query
/// reads no rows.
pub fn check_buckets(query: &Query) -> Result<(), BucketError> {
    let Some(width) = query.bucket_width() else {
        return Ok(());
    };
    let Some(span) = window_span(query) else {
        return Ok(());
    };
    check_span(span, width, query.limits.max_buckets)
}

/// How many buckets this query's own window implies, for `--explain`; `None` where it is
/// not bucketed or its window has no edge.
pub fn planned_buckets(query: &Query) -> Option<u64> {
    let width = query.bucket_width()?;
    Some(span_len(window_span(query)?, width_nanos(width)))
}

fn check_span(span: (i64, i64), width: Duration, permitted: usize) -> Result<(), BucketError> {
    let requested = span_len(span, width_nanos(width));
    if permitted > 0 && requested > permitted as u64 {
        return Err(BucketError::TooManyBuckets {
            width: duration_text(width),
            requested,
            permitted,
        });
    }
    Ok(())
}

/// What one bucketed group is keyed by while the scan is running: its bucket, and the
/// values of the other keys.
type Tally = HashMap<(Option<i64>, Vec<String>), u64>;

/// The tally as the answer, with a zero in every bucket of the span for each observed
/// label combination: a quiet bucket is an observation. Unplaceable rows are not filled.
fn zero_fill(
    tally: Tally,
    width: Option<Duration>,
    span: Option<(i64, i64)>,
    columns: usize,
    limits: Limits,
) -> Result<Vec<Group>, BucketError> {
    let Some(width) = width else {
        // No time axis: the groups are what was seen, and nothing is filled in.
        let mut groups: Vec<Group> = tally
            .into_iter()
            .map(|((bucket, values), count)| Group {
                bucket,
                values,
                count,
            })
            .collect();
        groups.sort_by(|a, b| (&a.bucket, &a.values).cmp(&(&b.bucket, &b.values)));
        return Ok(groups);
    };
    let w = width_nanos(width);

    // The rows that could not be placed. Held aside because they are not on the time
    // axis and must not be multiplied across it.
    let mut groups: Vec<Group> = tally
        .iter()
        .filter(|((bucket, _), _)| bucket.is_none())
        .map(|((_, values), count)| Group {
            bucket: None,
            values: values.clone(),
            count: *count,
        })
        .collect();
    groups.sort_by(|a, b| a.values.cmp(&b.values));

    let placed: Vec<(i64, &Vec<String>, u64)> = tally
        .iter()
        .filter_map(|((bucket, values), count)| bucket.map(|b| (b, values, *count)))
        .collect();
    let seen = placed
        .iter()
        .fold(None, |acc: Option<(i64, i64)>, (b, ..)| match acc {
            Some((min, max)) => Some((min.min(*b), max.max(*b))),
            None => Some((*b, *b)),
        });

    let Some(span) = span_with(span, seen, w) else {
        // No window edge and no placed row: nothing names a time axis, so there are no
        // buckets to emit. Not an error - see `execute`.
        return Ok(groups);
    };
    check_span(span, width, limits.max_buckets)?;

    // One series per observed label combination. With no other key there is exactly one,
    // the bare time series.
    let mut series: BTreeSet<&Vec<String>> = placed.iter().map(|(_, values, _)| *values).collect();
    let empty = Vec::new();
    if columns == 0 {
        series.insert(&empty);
    }

    let buckets = span_len(span, w);
    let requested = buckets.saturating_mul(series.len() as u64);
    if limits.max_groups > 0 && requested > limits.max_groups as u64 {
        return Err(BucketError::TooManyGroups {
            width: duration_text(width),
            buckets,
            series: series.len(),
            requested,
            permitted: limits.max_groups,
        });
    }

    let counts: HashMap<(i64, &Vec<String>), u64> = placed
        .iter()
        .map(|(bucket, values, count)| ((*bucket, *values), *count))
        .collect();
    groups.reserve(requested as usize);
    for n in 0..buckets {
        let start = span.0.saturating_add((n as i64).saturating_mul(w));
        for values in &series {
            groups.push(Group {
                bucket: Some(start),
                values: (*values).clone(),
                count: counts.get(&(start, *values)).copied().unwrap_or(0),
            });
        }
    }
    Ok(groups)
}

// ── The plan ───────────────────────────────────────────────────────────────────────

/// What the plan decided, and nothing else: no rows, no store handles.
#[derive(Debug, Default)]
pub struct Plan {
    pub leaves: Vec<PlannedLeaf>,
    /// Root subtrees a walk of the trie skips without looking inside: the root's edges
    /// that hold another value of a key the pattern names exactly.
    pub skipped: usize,
    pub duration: Duration,
}

/// One template a plan chose, and what is left for the scan to ask of its rows.
#[derive(Debug, PartialEq)]
pub struct PlannedLeaf {
    pub template_id: u64,
    /// The template's path, storage path, time column and text columns as they were when
    /// the plan was made, shared and not copied.
    pub template: Arc<Template>,
    /// Terms the path could not settle - the scan's predicates; empty means every row in the
    /// leaf matches by its shape alone.
    pub residual: Vec<Match>,
}

/// Choose the templates that could hold a matching row from the trie's key index, reading
/// no data: the plan `plan_by_walk` makes, at the cost of the lists a pattern names.
pub fn plan(trie: &Trie, query: &Query) -> Plan {
    let started = Instant::now();
    let refused = refusals(query);
    let (skipped, edges) = pruned(trie, &refused);
    // The index's lock is let go at the end of this block and before anything else is
    // done: nothing below may wait on a node while it is held.
    let (leaves, edits) = {
        let keys = trie.keys();
        (chosen(&keys, query, &refused), keys.edits())
    };
    let duration = started.elapsed();
    if cfg!(debug_assertions) {
        hold_to_the_walk(trie, query, &leaves, (skipped, edges), edits);
    }
    Plan {
        leaves,
        skipped,
        duration,
    }
}

/// A debug build's check of every plan against the walk. Each half is compared only where
/// no other thread moved it in between: the leaves by `edits`, `skipped` by root edges.
fn hold_to_the_walk(
    trie: &Trie,
    query: &Query,
    leaves: &[PlannedLeaf],
    (skipped, edges): (usize, usize),
    edits: u64,
) {
    let walked = plan_by_walk(trie, query);
    if trie.keys().edits() == edits {
        assert_eq!(
            leaves,
            walked.leaves.as_slice(),
            "the plan read from the key index is not the plan a walk of the trie makes, \
             for the pattern {:?}",
            query.tokens()
        );
    }
    let edges_now = trie
        .root()
        .children
        .read()
        .map_or(0, |children| children.len());
    if edges_now == edges {
        assert_eq!(
            skipped,
            walked.skipped,
            "the root subtrees counted as skipped are not the ones a walk of the trie \
             skips, for the pattern {:?}",
            query.tokens()
        );
    }
}

/// Each exact term's promoted token and the length of its `KEY=` prefix: what the root
/// filter reads, made once for a query rather than for every edge.
fn refusals(query: &Query) -> Vec<(String, usize)> {
    query
        .matches
        .iter()
        .filter_map(|m| match &m.value {
            MatchValue::Exact(v) => {
                let token = promoted_token(&m.key, v);
                let prefix = token.len() - v.len();
                Some((token, prefix))
            }
            MatchValue::Any => None,
        })
        .collect()
}

/// Whether the root filter refuses a root edge with this label. The one predicate both the
/// count of skipped edges and the choice of templates use, so the two cannot disagree.
fn refuses(label: &str, refused: &[(String, usize)]) -> bool {
    refused
        .iter()
        .any(|(token, prefix)| label != token && label.starts_with(&token[..*prefix]))
}

/// `Plan::skipped` and the count of the root's edges, in one hold of the root's lock.
/// Counted at the root because the walk also counts an edge whose subtree has no template.
fn pruned(trie: &Trie, refused: &[(String, usize)]) -> (usize, usize) {
    let root = trie.root();
    let Ok(children) = root.children.read() else {
        return (0, 0);
    };
    let skipped = children
        .iter()
        .filter(|edge| refuses(&edge.label, refused))
        .count();
    (skipped, children.len())
}

/// The templates the pattern could match, in id order, with the terms left to the scan.
/// Applies both of the walk's filters, since `select` does not imply `keeps`.
fn chosen(keys: &KeyIndex, query: &Query, refused: &[(String, usize)]) -> Vec<PlannedLeaf> {
    // One list a term, with how far into it the merge has come. A key no template has
    // is a pattern no template can match.
    let mut lists: Vec<(&[Posting], usize)> = Vec::with_capacity(query.matches.len());
    for term in &query.matches {
        match keys.of(&term.key) {
            Some(list) => lists.push((list, 0)),
            None => return Vec::new(),
        }
    }
    // With no term, every template: the root as a leaf has no key and is found here and
    // by no term.
    let driver = lists
        .iter()
        .map(|(list, _)| *list)
        .min_by_key(|list| list.len())
        .unwrap_or(keys.all());

    let mut leaves = Vec::new();
    'next: for (id, template) in driver {
        for (list, at) in &mut lists {
            while *at < list.len() && list[*at].0 < *id {
                *at += 1;
            }
            if list.get(*at).map(|posting| posting.0) != Some(*id) {
                continue 'next;
            }
        }
        // The root as a leaf has no first label and is kept, as the walk keeps it.
        if !refused.is_empty()
            && template
                .path
                .first()
                .is_some_and(|first| refuses(first, refused))
        {
            continue 'next;
        }
        let mut residual = Vec::new();
        for term in &query.matches {
            match template.promoted(&term.key) {
                // The value is in the path, so the template settles the term now and the scan has
                // nothing to check.
                Some(have) => match &term.value {
                    MatchValue::Any => {}
                    MatchValue::Exact(want) if want == have => {}
                    MatchValue::Exact(_) => continue 'next,
                },
                None => residual.push(term.clone()),
            }
        }
        leaves.push(PlannedLeaf {
            template_id: *id,
            template: Arc::clone(template),
            residual,
        });
    }
    leaves
}

/// The plan as a walk of the trie makes it, kept as what `plan` is held to: a change to
/// `keeps`, `select` or `promoted_of` is one to `refuses`, `chosen` and `Template::of` too.
fn plan_by_walk(trie: &Trie, query: &Query) -> Plan {
    let started = Instant::now();
    let root = trie.root();
    let mut found = Vec::new();
    let mut skipped = 0;

    // A line with no keys at all (`{}`) walks zero tokens, so the root itself can be a
    // leaf. Rare, and the hostile corpus has it.
    if root.is_leaf.load(Ordering::Acquire) {
        found.push((Vec::new(), Arc::clone(&root)));
    }
    if let Ok(children) = root.children.read() {
        for edge in children.iter() {
            if !keeps(&edge.label, query) {
                skipped += 1;
                continue;
            }
            let mut path = vec![edge.label.clone()];
            collect(&edge.child, &mut path, &mut found);
        }
    }

    let mut leaves = Vec::new();
    for (path, node) in found {
        let Ok(slot) = node.leaf_meta.read() else {
            continue;
        };
        let Some(meta) = slot.as_ref() else {
            continue;
        };
        let columns: BTreeSet<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();
        if let Some(residual) = select(&path, &columns, query) {
            leaves.push(PlannedLeaf {
                template_id: meta.template_id,
                template: Arc::new(Template::of(path, meta)),
                residual,
            });
        }
    }
    leaves.sort_by_key(|leaf| leaf.template_id);

    Plan {
        leaves,
        skipped,
        duration: started.elapsed(),
    }
}

/// Whether a root subtree is kept: promoted tokens are hoisted to the front of the path,
/// so a root label `KEY=` with another value cannot hold an exact match on that key.
fn keeps(label: &str, query: &Query) -> bool {
    query.matches.iter().all(|m| match &m.value {
        MatchValue::Exact(v) => {
            let token = promoted_token(&m.key, v);
            label == token || !label.starts_with(&token[..token.len() - v.len()])
        }
        MatchValue::Any => true,
    })
}

fn collect(node: &Arc<Node>, path: &mut Vec<String>, out: &mut Vec<(Vec<String>, Arc<Node>)>) {
    if node.is_leaf.load(Ordering::Acquire) {
        out.push((path.clone(), Arc::clone(node)));
    }
    let Ok(children) = node.children.read() else {
        return;
    };
    for edge in children.iter() {
        path.push(edge.label.clone());
        collect(&edge.child, path, out);
        path.pop();
    }
}

/// Test one leaf against the pattern. `None` = this leaf cannot match; `Some(residual)` =
/// it can, once the scan has applied the terms listed.
fn select(path: &[String], columns: &BTreeSet<&str>, query: &Query) -> Option<Vec<Match>> {
    let promoted = promoted_of(path, columns);
    let mut residual = Vec::new();
    for term in &query.matches {
        // The key set is the path, so a template without the key cannot hold a row with
        // it. This is the whole of the walk's pruning for non-promoted keys.
        if !columns.contains(term.key.as_str()) {
            return None;
        }
        match promoted.get(term.key.as_str()) {
            // The value is in the path, so the leaf settles the term now and every row
            // in it agrees - no predicate, and the scan has nothing to check.
            Some(&have) => match &term.value {
                MatchValue::Any => {}
                MatchValue::Exact(want) if want == have => {}
                MatchValue::Exact(_) => return None,
            },
            None => residual.push(term.clone()),
        }
    }
    Some(residual)
}

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

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a, one more slice folded in.
fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

// ── Cursors ────────────────────────────────────────────────────────────────────────

/// Where a page stopped: the `Rank` of its last row, which names a row and so is keyset
/// and stateless, plus a guard that refuses it against another pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    rank: Rank,
    query: u32,
}

/// Bumped when `Rank`'s components change meaning, and read first in `decode`: a cursor
/// from an older layout would answer confidently with the wrong rows.
const CURSOR_VERSION: &str = "v3";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("not a cursor: expected `{CURSOR_VERSION}.` followed by {HEX_LEN} hex digits")]
    Malformed,
    #[error("cursor version `{found}` is not `{CURSOR_VERSION}` - it was issued before the page order changed; re-run the query without `AFTER`")]
    Version { found: String },
    #[error("this cursor came from a different query - a cursor is only valid for the pattern and order that produced it")]
    WrongQuery,
}

/// `[1 present][8 ts][8 leaf_key][8 seq][4 guard]`, big-endian, as hex.
const CURSOR_BYTES: usize = 1 + 8 + 8 + 8 + 4;
const HEX_LEN: usize = CURSOR_BYTES * 2;

impl Cursor {
    /// The fingerprint a cursor is checked against - the pattern and the order, not the time
    /// window, because `SINCE 1h` resolves against `now` and would refuse its own cursors.
    pub fn guard(query: &Query) -> u32 {
        let h = fnv(FNV_OFFSET, &Self::spelling(query));
        // Folded to 32 bits: this is a guard against a mistake, not against an attacker,
        // and the cursor is already long.
        ((h >> 32) ^ h) as u32
    }

    /// The bytes `guard` hashes, length-prefixed so that no two patterns spell the same bytes;
    /// `Query::tokens` is not this spelling and must not become it.
    pub fn spelling(query: &Query) -> Vec<u8> {
        fn text(out: &mut Vec<u8>, text: &str) {
            out.extend_from_slice(&(text.len() as u64).to_be_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        let mut out = Vec::new();
        out.extend_from_slice(&(query.matches.len() as u64).to_be_bytes());
        for term in &query.matches {
            text(&mut out, &term.key);
            match &term.value {
                MatchValue::Any => out.push(0),
                MatchValue::Exact(value) => {
                    out.push(1);
                    text(&mut out, value);
                }
            }
        }
        // The order is by the effective time, whatever a template calls its column, so a cursor
        // survives a template being created with another name between two pages.
        out.extend_from_slice(b"effective_time");
        out
    }

    /// The `Rank` a page stopped at, read by the scan and by the oracle.
    pub fn rank(&self) -> Rank {
        self.rank
    }

    fn new(rank: Rank, query: &Query) -> Self {
        Self {
            rank,
            query: Self::guard(query),
        }
    }

    /// The cursor as opaque text: anyone who can write one by hand will depend on the layout
    /// `CURSOR_VERSION` exists to change.
    pub fn encode(&self) -> String {
        let (Reverse(ts), leaf, seq) = self.rank;
        let mut out = String::with_capacity(CURSOR_VERSION.len() + 1 + HEX_LEN);
        out.push_str(CURSOR_VERSION);
        out.push('.');
        let mut hex = |bytes: &[u8]| {
            for b in bytes {
                out.push_str(&format!("{b:02x}"));
            }
        };
        // `ts` is an `Option`, and the absent case is a real one: the flag carries it, and the
        // eight bytes beside it are then zero and mean nothing.
        match ts {
            Some(ns) => {
                hex(&[1]);
                hex(&ns.to_be_bytes());
            }
            None => {
                hex(&[0]);
                hex(&0i64.to_be_bytes());
            }
        }
        hex(&leaf.to_be_bytes());
        hex(&seq.to_be_bytes());
        hex(&self.query.to_be_bytes());
        out
    }

    /// The text back into a cursor; the guard is *not* checked here, because the query it
    /// belongs to is not built yet at parse time - see `check`.
    pub fn decode(text: &str) -> Result<Self, CursorError> {
        let (version, body) = text.split_once('.').ok_or(CursorError::Malformed)?;
        if version != CURSOR_VERSION {
            return Err(CursorError::Version {
                found: version.to_string(),
            });
        }
        if body.len() != HEX_LEN || !body.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(CursorError::Malformed);
        }
        let mut bytes = [0u8; CURSOR_BYTES];
        for (n, slot) in bytes.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&body[n * 2..n * 2 + 2], 16).map_err(|_| {
                // Unreachable - every byte was checked above. Not an `unwrap`, because
                // `decode` is fed by the CLI and "never panics" is the parser's rule too.
                CursorError::Malformed
            })?;
        }
        let word = |at: usize| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[at..at + 8]);
            buf
        };
        let ts = match bytes[0] {
            0 => None,
            _ => Some(i64::from_be_bytes(word(1))),
        };
        let mut guard = [0u8; 4];
        guard.copy_from_slice(&bytes[25..29]);
        Ok(Self {
            rank: (
                Reverse(ts),
                u64::from_be_bytes(word(9)),
                u64::from_be_bytes(word(17)),
            ),
            query: u32::from_be_bytes(guard),
        })
    }

    /// Whether this cursor belongs to this query, checked once before the scan.
    pub fn check(&self, query: &Query) -> Result<(), CursorError> {
        if self.query == Self::guard(query) {
            return Ok(());
        }
        Err(CursorError::WrongQuery)
    }
}

/// One candidate row, ordered by `Rank` and by nothing else - `Row` is a `HashMap` and has
/// no ordering of its own.
struct Ranked {
    rank: Rank,
    row: Row,
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
struct Newest {
    /// `None` = keep everything.
    keep: Option<usize>,
    heap: BinaryHeap<Ranked>,
    /// Whether a store may be told a row is not worth building (`Fetch::wants_row`): `false`
    /// from the first row stored outside its own day, since two rows may then share a `Rank`.
    gate: bool,
}

impl Newest {
    fn new(keep: Option<usize>) -> Self {
        Self {
            keep,
            heap: BinaryHeap::new(),
            gate: true,
        }
    }

    /// A heap that is offered every row from the first, kept for the tests to hold the
    /// answer to.
    #[cfg(test)]
    fn offered_every_row(keep: Option<usize>) -> Self {
        Self {
            gate: false,
            ..Self::new(keep)
        }
    }

    fn offer(&mut self, rank: Rank, row: Row) {
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
    fn worst(&self) -> Option<Rank> {
        let keep = self.keep?;
        if self.heap.len() > keep {
            self.heap.peek().map(|ranked| ranked.rank)
        } else {
            None
        }
    }

    /// Newest first, and the `Rank` the next page should resume after: the overflow row is
    /// the proof that a next page exists, and the row before it is where this one stopped.
    fn finish(self) -> (Vec<Row>, Option<Rank>) {
        let keep = self.keep;
        let mut ranked = self.heap.into_sorted_vec();
        let more = keep.is_some_and(|n| ranked.len() > n);
        if let Some(n) = keep {
            ranked.truncate(n);
        }
        let next = more.then(|| ranked.last().map(|r| r.rank)).flatten();
        (ranked.into_iter().map(|r| r.row).collect(), next)
    }
}

/// The `Fetch` mode's sink: rank each row, drop the ones an earlier page served, and let
/// the heap keep the best. A struct, because a closure cannot say what is worth opening.
struct Fetch<'a> {
    keep: &'a mut Newest,
    key: u64,
    ts: &'a str,
    after: Option<Rank>,
}

impl Visitor for Fetch<'_> {
    fn row(&mut self, row: &Row, seq: u64) {
        // `seq` comes from the store, so it is the row's own ordinal and not a count of what
        // this scan admitted.
        let rank = (Reverse(effective_ts(row, self.ts)), self.key, seq);
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
        let best = (Reverse(newest), self.key, 0u64);
        self.keep.worst().is_none_or(|worst| best < worst)
    }

    /// Asked before each row while there is a limit to fill, and until the gate is off.
    /// With no limit every row is kept and there is nothing to ask.
    fn wants(&self) -> Wants {
        if self.keep.keep.is_some() && self.keep.gate {
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
        let rank = (Reverse(ts), self.key, seq);
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
fn answer(
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
        let (rows, next) = newest.finish();
        result.rows = rows;
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
fn term(m: &Match) -> Term {
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
            *stored == ColValue::String(text.clone()) || *stored == infer(text).value
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

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// The bucket arithmetic, which the CLI surface can only reach one query at a time: its
/// properties hold for any range and width, so they are checked over generated ones.
#[cfg(test)]
mod tests {
    use super::*;

    /// A promoted edge reads back as the column it was promoted from, escapes resolved,
    /// and a key's own token reads back as nothing.
    #[test]
    fn a_promoted_edge_reads_back_as_its_column() {
        let path = [
            "a\\=b.event=login".to_string(),
            "k\\=v".to_string(),
            "n".to_string(),
        ];
        let columns = BTreeSet::from(["a=b.event", "k=v", "n"]);
        let promoted = promoted_of(&path, &columns);
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted.get("a=b.event"), Some(&"login"));
    }

    /// A leaf whose label `event=login` is also one of its columns, as old on-disk data can
    /// hold: the path settles nothing there and the scan reads the column.
    #[test]
    fn a_label_that_is_also_a_column_settles_nothing() {
        let path = ["event=login".to_string()];
        let columns = BTreeSet::from(["event", "event=login"]);
        assert!(promoted_of(&path, &columns).is_empty());
    }
    use crate::limits::Limits;
    use kibbr_core::rng::Rng;
    use kibbr_core::types::ColumnType;
    use kibbr_storage::db;
    use kibbr_storage::store::nanos_of;
    use std::collections::BTreeMap;
    use std::time::UNIX_EPOCH;

    /// 2024-01-15T00:00:00Z, and the corpus runs a day forward from it.
    const EPOCH: i64 = 1_705_276_800;
    const DAY: i64 = 86_400;
    /// The hour deliberately left with no rows in it, as an offset from `EPOCH`.
    const GAP_HOUR: i64 = 5;

    /// Seconds as the Unix nanoseconds a `Group::bucket` is in.
    fn ns(secs: i64) -> i64 {
        secs * 1_000_000_000
    }

    fn at(secs: i64) -> SystemTime {
        if secs >= 0 {
            UNIX_EPOCH + Duration::from_secs(secs as u64)
        } else {
            UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs())
        }
    }

    /// A day of lines: a timestamp every few seconds, one hour of it empty, and a handful
    /// of rows on a template with no `ts` column at all.
    fn corpus() -> String {
        let mut rng = Rng::new(11);
        let mut out = String::new();
        let mut secs = 0;
        while secs < DAY {
            if !(GAP_HOUR * 3_600..(GAP_HOUR + 1) * 3_600).contains(&secs) {
                let level = ["ERROR", "WARN", "INFO"][rng.below(3) as usize];
                let ts = chrono::DateTime::from_timestamp(EPOCH + secs, 0)
                    .expect("a representable instant")
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
                out.push_str(&format!(
                    "{{\"ts\":\"{ts}\",\"level\":\"{level}\",\"event\":\"tick\"}}\n"
                ));
            }
            secs += 1 + rng.below(40) as i64;
        }
        // A second template, sharing `level` and holding no timestamp: under a window it
        // contributes nothing, and with none it is the `bucket: null` group.
        for n in 0..7 {
            out.push_str(&format!(
                "{{\"level\":\"ERROR\",\"event\":\"untimed\",\"n\":{n}}}\n"
            ));
        }
        out
    }

    fn db() -> db::Db {
        db::load(
            corpus().as_bytes(),
            true,
            chrono::Utc::now(),
            &kibbr_storage::trie::Schema::default(),
        )
        .expect("the corpus loads")
    }

    fn query(mode: QueryMode, since: Option<i64>, until: Option<i64>) -> Query {
        Query::new(
            Vec::new(),
            mode,
            TimeRange {
                since: since.map(at),
                until: until.map(at),
            },
            None,
            None,
            at(EPOCH + DAY),
            Limits {
                max_rows: 0,
                max_buckets: 0,
                max_groups: 0,
            },
        )
    }

    /// A query told to stop has no answer, in any mode; the stop is asked before the first
    /// template is read, so a query told before it began reads nothing.
    #[test]
    fn a_query_told_to_stop_ends_with_no_answer() {
        let db = db();
        for mode in [
            QueryMode::Count,
            QueryMode::Fetch,
            QueryMode::GroupBy(vec![GroupKey::Column("level".to_string())]),
        ] {
            let q = query(mode, None, None);
            let stop = Stop::new();
            let whole = execute(&db.trie, &db.stores, &q).expect("an answer");
            let untold = execute_until(&db.trie, &db.stores, &q, &stop).expect("an answer");
            assert_eq!(untold.count, whole.count);
            assert_eq!(untold.rows.len(), whole.rows.len());
            assert!(whole.count > 0);

            stop.set();
            let told = execute_until(&db.trie, &db.stores, &q, &stop);
            assert!(
                matches!(told, Err(ExecError::Store(StoreError::Stopped))),
                "{told:?}"
            );
        }
    }

    /// A load that publishes between a query's plan and its snapshot can have added a
    /// template the plan never saw; the query sees the generation move and plans again.
    #[test]
    fn a_publish_between_the_plan_and_the_snapshot_plans_again() {
        use kibbr_storage::store::{
            LeafStore, MemoryStore, Scan, ScanOptions, Snapshot, Store, StoreError, Term, Visitor,
        };
        use std::sync::atomic::{AtomicBool, AtomicU64};

        /// A store whose first snapshot finds that a load has just published a new
        /// template, as one landing in that gap would.
        struct Publishing {
            inner: MemoryStore,
            trie: Arc<Trie>,
            generation: AtomicU64,
            published: AtomicBool,
        }
        struct Taken(u64);
        impl Snapshot for Taken {
            fn generation(&self) -> u64 {
                self.0
            }
            fn visit(
                &mut self,
                _: u64,
                _: &ScanOptions,
                _: &[Term],
                _: &mut dyn Visitor,
            ) -> Option<Result<Scan, StoreError>> {
                None
            }
        }
        impl Store for Publishing {
            fn open(&self, id: u64, column: &str) -> Result<Arc<dyn LeafStore>, StoreError> {
                self.inner.open(id, column)
            }
            fn kind(&self) -> &'static str {
                "publishing"
            }
            fn base_path(&self) -> Option<&std::path::Path> {
                None
            }
            fn generation(&self) -> u64 {
                self.generation.load(Ordering::SeqCst)
            }
            fn snapshot(
                &self,
                _: Vec<(u64, Arc<dyn LeafStore>)>,
                _: &ScanOptions,
            ) -> Box<dyn Snapshot> {
                if !self.published.swap(true, Ordering::SeqCst) {
                    let line = br#"{"ts":"2024-01-15T12:00:00Z","loaded":"yes","batch":7}"#;
                    let parsed = kibbr_core::flatten::parse_line(line, 1).expect("a line");
                    self.trie
                        .ingest(&parsed, chrono::Utc::now())
                        .expect("a new template");
                    self.generation.fetch_add(1, Ordering::SeqCst);
                }
                Box::new(Taken(self.generation.load(Ordering::SeqCst)))
            }
        }

        let trie = Arc::new(db().trie);
        let stores = StoreManager::new(Box::new(Publishing {
            inner: MemoryStore::new(),
            trie: Arc::clone(&trie),
            generation: AtomicU64::new(0),
            published: AtomicBool::new(false),
        }));
        let q = query(QueryMode::Count, None, None);
        let planned = plan(&trie, &q).leaves.len();
        let result = execute(&trie, &stores, &q).expect("execute");
        assert_eq!(
            result.meta.templates_matched,
            planned + 1,
            "the template published under the plan is not in the answer"
        );
    }

    fn bucketed(width: u64) -> QueryMode {
        QueryMode::GroupBy(vec![GroupKey::Time(Duration::from_secs(width))])
    }

    /// The property, over randomised ranges and widths. Both endpoints are snapped to the
    /// width, which is what makes `ceil(range / width)` the exact bucket count.
    #[test]
    fn buckets_sum_to_the_count_and_tile_the_range() {
        let db = db();
        let mut rng = Rng::new(7);
        // Widths that divide a day and widths that do not, so alignment is exercised
        // against boundaries that fall inside the range as well as on its edges.
        const WIDTHS: [u64; 8] = [1, 30, 60, 300, 900, 3_600, 7_200, 4_500];

        for _ in 0..400 {
            let width = WIDTHS[rng.below(WIDTHS.len() as u64) as usize];
            let w = width as i64;
            // Aligned to the width from the epoch, which is where the boundaries are.
            let lo = (EPOCH + rng.below(DAY as u64) as i64).div_euclid(w) * w;
            let hi = lo + w * (1 + rng.below(400) as i64);

            let grouped = execute(
                &db.trie,
                &db.stores,
                &query(bucketed(width), Some(lo), Some(hi)),
            )
            .expect("the bucketed query answers");
            let plain = execute(
                &db.trie,
                &db.stores,
                &query(QueryMode::Count, Some(lo), Some(hi)),
            )
            .expect("the unbucketed query answers");

            let case = format!("[{lo}, {hi}) at {width}s");
            let summed: u64 = grouped.groups.iter().map(|g| g.count).sum();
            assert_eq!(
                summed, plain.count,
                "{case}: buckets do not sum to the count"
            );
            assert_eq!(
                summed, grouped.count,
                "{case}: the answer disagrees with its own groups"
            );

            // `ceil(range / width)`, exact because both edges are on a boundary.
            let expected = ((hi - lo) as u64).div_ceil(width);
            assert_eq!(
                grouped.groups.len() as u64,
                expected,
                "{case}: wrong number of buckets"
            );

            // Contiguous multiples of the width from the range's own edge: a floor against the
            // query start still tiles the range, but not where a neighbouring query tiles it.
            for (n, group) in grouped.groups.iter().enumerate() {
                let start = lo + w * n as i64;
                assert_eq!(
                    group.bucket,
                    Some(ns(start)),
                    "{case}: bucket {n} is misplaced"
                );
                assert_eq!(start.rem_euclid(w), 0, "{case}: bucket {n} is off the grid");
            }
        }
    }

    /// The reason the boundaries are floored against the epoch and not against the query:
    /// two queries over overlapping ranges have to agree about where the bars go.
    #[test]
    fn overlapping_ranges_put_their_boundaries_in_the_same_places() {
        let db = db();
        let width: i64 = 300;
        let starts = |since: i64, until: i64| -> Vec<(i64, u64)> {
            execute(
                &db.trie,
                &db.stores,
                &query(bucketed(width as u64), Some(since), Some(until)),
            )
            .expect("the query answers")
            .groups
            .into_iter()
            .filter_map(|g| g.bucket.map(|b| (b, g.count)))
            .collect()
        };

        // Same data, three ranges nudged off each other by amounts smaller than a bucket.
        let wide = starts(EPOCH, EPOCH + 4 * 3_600);
        let mut compared = 0;
        for offset in [1, 7, 137, 299] {
            let nudged = starts(EPOCH + offset, EPOCH + 4 * 3_600 - offset);
            assert!(!nudged.is_empty());
            for (start, count) in &nudged {
                let same = wide.iter().find(|(at, _)| at == start).unwrap_or_else(|| {
                    panic!("offset {offset}: bucket {start} is not on the grid")
                });
                // Only the two partial buckets at the edges may hold fewer rows; the ones
                // between them are the same observation whichever range asked.
                if *start > ns(EPOCH + offset)
                    && *start + ns(width) < ns(EPOCH + 4 * 3_600 - offset)
                {
                    assert_eq!(*count, same.1, "offset {offset}: bucket {start} disagrees");
                    compared += 1;
                }
            }
        }
        // The comparison above is inside an `if`, so it has to be shown to have run.
        assert!(compared > 100, "only {compared} buckets were compared");
    }

    /// An hour with no rows in it is 12 zeros at 5m, not twelve missing keys.
    #[test]
    fn an_empty_hour_is_zeros() {
        let db = db();
        let result = execute(
            &db.trie,
            &db.stores,
            &query(bucketed(300), Some(EPOCH), Some(EPOCH + DAY)),
        )
        .expect("the query answers");

        let gap = ns(EPOCH + GAP_HOUR * 3_600);
        let inside: Vec<&Group> = result
            .groups
            .iter()
            .filter(|g| {
                g.bucket
                    .is_some_and(|b| (gap..gap + ns(3_600)).contains(&b))
            })
            .collect();
        assert_eq!(inside.len(), 12, "the empty hour is 12 buckets at 5m");
        assert!(
            inside.iter().all(|g| g.count == 0),
            "the empty hour is not all zeros: {inside:?}"
        );
        // And the hours either side are not, so the corpus is actually exercising this.
        assert!(result.groups.iter().any(|g| g.count > 0));
    }

    /// Every series gets a point in every bucket, because a chart with three lines has
    /// three lines in every bucket - a quiet one is a zero on its line, not a gap in it.
    #[test]
    fn a_second_key_is_zero_filled_across_every_bucket() {
        let db = db();
        let mode = QueryMode::GroupBy(vec![
            GroupKey::Time(Duration::from_secs(300)),
            GroupKey::Column("level".to_string()),
        ]);
        let result = execute(
            &db.trie,
            &db.stores,
            &query(mode, Some(EPOCH), Some(EPOCH + 3_600)),
        )
        .expect("the query answers");

        let series: BTreeSet<&Vec<String>> = result.groups.iter().map(|g| &g.values).collect();
        assert_eq!(series.len(), 3, "ERROR, WARN and INFO");
        assert_eq!(result.groups.len(), 12 * 3);
        // Exactly one row per (bucket, series) pair, and the total still holds.
        let pairs: BTreeSet<(Option<i64>, &Vec<String>)> = result
            .groups
            .iter()
            .map(|g| (g.bucket, &g.values))
            .collect();
        assert_eq!(pairs.len(), result.groups.len());
        assert_eq!(
            result.groups.iter().map(|g| g.count).sum::<u64>(),
            result.count
        );
    }

    /// A template with no time column is on arrival time, which is `now` and outside the
    /// window: the buckets are there, zero-filled, and the count is zero.
    #[test]
    fn a_template_with_no_ts_column_is_on_arrival_time() {
        let db = db();
        let mut q = query(bucketed(300), Some(EPOCH), Some(EPOCH + DAY));
        q.matches = vec![Match {
            key: "n".to_string(),
            value: MatchValue::Any,
        }];
        let result = execute(&db.trie, &db.stores, &q).expect("the query answers");
        assert!(result.exists, "the template is real");
        assert_eq!(result.count, 0);
        assert_eq!(
            result.groups.len() as i64,
            DAY / 300,
            "{:?}",
            result.groups.len()
        );
        assert!(result
            .groups
            .iter()
            .all(|g| g.count == 0 && g.bucket.is_some()));
    }

    /// With no window the seven rows on arrival time are counted, and they land in the
    /// bucket their arrival falls in.
    #[test]
    fn rows_on_arrival_time_are_bucketed_at_their_arrival() {
        let db = db();
        let result = execute(&db.trie, &db.stores, &query(bucketed(3_600), None, None))
            .expect("the query answers");

        assert!(
            result.groups.iter().all(|g| g.bucket.is_some()),
            "a row without a bucket: {:?}",
            result.groups.iter().find(|g| g.bucket.is_none())
        );
        let newest = result.groups.iter().map(|g| g.bucket).max().flatten();
        let arrivals: u64 = result
            .groups
            .iter()
            .filter(|g| g.bucket == newest)
            .map(|g| g.count)
            .sum();
        assert_eq!(arrivals, 7, "the seven rows with no ts column, at `now`");
        assert_eq!(
            result.groups.iter().map(|g| g.count).sum::<u64>(),
            result.count,
            "the groups still sum to the count"
        );
    }

    /// The refusal, and both numbers in it.
    #[test]
    fn too_many_buckets_is_refused_rather_than_truncated() {
        let mut q = query(bucketed(1), Some(EPOCH), Some(EPOCH + 30 * DAY));
        q.limits.max_buckets = 10_000;
        let err = check_buckets(&q).expect_err("2.6M buckets is over the cap");
        assert_eq!(
            err,
            BucketError::TooManyBuckets {
                width: "1s".to_string(),
                requested: 2_592_000,
                permitted: 10_000,
            }
        );
        let text = err.to_string();
        assert!(text.contains("2,592,000"), "{text}");
        assert!(text.contains("10,000"), "{text}");

        // And the one under it is not refused.
        q.limits.max_buckets = 3_000_000;
        check_buckets(&q).expect("under the cap");
    }

    /// Flooring against the epoch, at the edges an `i64` of nanoseconds reaches.
    #[test]
    fn bucket_starts_are_multiples_of_the_width_at_both_ends_of_time() {
        for width in [1, 1_000, 1_000_000_000, 300 * 1_000_000_000] {
            for at in [
                0,
                1,
                -1,
                i64::MAX,
                i64::MIN,
                1_705_276_800_000_000_000,
                -1_705_276_800_000_000_000,
            ] {
                let start = bucket_of(at, width);
                // Every bucket is on the grid but the bottom one, whose true boundary is
                // below `i64::MIN` and has no spelling - see `bucket_of`.
                if start != i64::MIN {
                    assert_eq!(start.rem_euclid(width), 0, "{at} at {width}");
                }
                // The bucket contains its instant, except at both ends of the range, where the
                // arithmetic saturates rather than leaving the representable range.
                assert!(start <= at || start == i64::MIN, "{at} at {width}");
                let end = start.saturating_add(width);
                assert!(
                    end > at || end == i64::MAX || start == i64::MIN,
                    "{at} at {width}"
                );
            }
        }
    }

    /// A `MATCH` over `terms`, a value of `None` being the wildcard.
    fn pattern(terms: &[(&str, Option<&str>)]) -> Query {
        let mut q = query(QueryMode::Fetch, None, None);
        q.matches = terms
            .iter()
            .map(|(key, value)| Match {
                key: key.to_string(),
                value: match value {
                    None => MatchValue::Any,
                    Some(text) => MatchValue::Exact(text.to_string()),
                },
            })
            .collect();
        q
    }

    /// Three pairs of patterns that `Query::tokens` cannot tell apart and that are two
    /// queries all the same: each has to refuse a cursor of the other.
    #[test]
    fn the_guard_tells_apart_patterns_that_print_the_same_tokens() {
        let pairs = [
            // The wildcard and the one-character value `*`, two things in the grammar.
            (pattern(&[("host", None)]), pattern(&[("host", Some("*"))])),
            // Two terms, and one term whose value runs on through a NUL into the second.
            (
                pattern(&[("a", Some("b")), ("c", Some("d"))]),
                pattern(&[("a", Some("b\0c=d"))]),
            ),
            // A `=` that ends the key, and the same `=` inside the value.
            (
                pattern(&[("a=b", Some("c"))]),
                pattern(&[("a", Some("b=c"))]),
            ),
        ];
        let rank = (Reverse(Some(ns(EPOCH))), 3, 17);
        for (one, other) in &pairs {
            assert_eq!(
                one.tokens().join("\0"),
                other.tokens().join("\0"),
                "the pair is one the tokens cannot tell apart"
            );
            assert_ne!(Cursor::spelling(one), Cursor::spelling(other));
            for (issued_by, sent_to) in [(one, other), (other, one)] {
                let cursor = issued_by.cursor_at(rank);
                assert_eq!(cursor.check(issued_by), Ok(()));
                assert_eq!(
                    cursor.check(sent_to),
                    Err(CursorError::WrongQuery),
                    "a cursor of {:?} is taken by {:?}",
                    issued_by.matches,
                    sent_to.matches
                );
            }
        }
    }

    /// The spelling reads back to the pattern, and an encoding that reads back one way is
    /// one that no second pattern shares.
    #[test]
    fn the_guard_spelling_reads_back_to_the_pattern() {
        fn word(bytes: &mut &[u8]) -> usize {
            let (head, rest) = bytes.split_at(8);
            *bytes = rest;
            u64::from_be_bytes(head.try_into().expect("eight bytes")) as usize
        }
        fn text(bytes: &mut &[u8]) -> String {
            let len = word(bytes);
            let (head, rest) = bytes.split_at(len);
            *bytes = rest;
            String::from_utf8(head.to_vec()).expect("a term's text")
        }
        for q in [
            pattern(&[]),
            pattern(&[("host", None)]),
            pattern(&[("host", Some("*"))]),
            pattern(&[("", Some(""))]),
            pattern(&[
                ("a=b", Some("c\0d")),
                ("effective_time", None),
                ("e", Some("=")),
            ]),
        ] {
            let spelled = Cursor::spelling(&q);
            let mut bytes = spelled.as_slice();
            let mut read = Vec::new();
            for _ in 0..word(&mut bytes) {
                let key = text(&mut bytes);
                let (tag, rest) = bytes.split_first().expect("a tag");
                bytes = rest;
                let value = match tag {
                    0 => MatchValue::Any,
                    _ => MatchValue::Exact(text(&mut bytes)),
                };
                read.push(Match { key, value });
            }
            assert_eq!(read, q.matches);
            assert_eq!(bytes, b"effective_time");
        }
    }

    /// What the guard leaves out stays out: a cursor is good under another window and
    /// another limit of its own pattern, as `Cursor::guard` says and why.
    #[test]
    fn the_guard_is_the_pattern_and_not_the_window_or_the_limit() {
        let issued_by = pattern(&[("level", Some("ERROR"))]);
        let mut sent_to = pattern(&[("level", Some("ERROR"))]);
        sent_to.time.since = Some(at(EPOCH));
        sent_to.limit = Some(3);
        let cursor = issued_by.cursor_at((Reverse(None), 0, 0));
        assert_eq!(cursor.check(&sent_to), Ok(()));
    }

    /// The largest limit a client can write is every row: nothing is dropped to make room
    /// for one more than it, and no next page is promised.
    #[test]
    fn the_largest_limit_is_every_row_and_no_next_page() {
        let db = db();
        let mut q = query(QueryMode::Fetch, None, None);
        let all = execute(&db.trie, &db.stores, &q).expect("the unlimited query answers");
        assert!(all.count > 0 && all.rows.len() as u64 == all.count);

        q.limit = Some(usize::MAX);
        q.limit_source = LimitSource::Caller;
        let capped = execute(&db.trie, &db.stores, &q).expect("the limited query answers");
        assert_eq!(capped.count, all.count);
        assert_eq!(capped.rows, all.rows);
        assert!(!capped.meta.truncated);
        assert_eq!(capped.meta.next_cursor, None);
    }

    /// `Newest` at every limit around the number of rows offered and at both ends of `usize`:
    /// the best `keep` rows, and a next page exactly when a row was left over.
    #[test]
    fn the_page_heap_keeps_the_limit_and_one_more_at_any_limit() {
        const OFFERED: usize = 3;
        for keep in [0, 1, 2, 3, 4, usize::MAX - 1, usize::MAX] {
            let mut newest = Newest::new(Some(keep));
            for seq in 0..OFFERED as u64 {
                newest.offer((Reverse(None), 0, seq), Row::new());
                assert!(newest.heap.len() <= OFFERED.min(keep.saturating_add(1)));
            }
            let (rows, next) = newest.finish();
            assert_eq!(rows.len(), keep.min(OFFERED), "LIMIT {keep}");
            let last = keep.min(OFFERED) as u64;
            let more = keep > 0 && keep < OFFERED;
            assert_eq!(
                next,
                more.then(|| (Reverse(None), 0, last - 1)),
                "LIMIT {keep}"
            );
        }
    }

    fn path(tokens: &[&str]) -> Vec<String> {
        tokens.iter().map(|token| token.to_string()).collect()
    }

    /// Paths that are the same bytes once a NUL is put after each token are different key
    /// sets and need two keys; a path with no NUL keeps its key, so no stored row moves.
    #[test]
    fn a_leaf_key_tells_apart_paths_that_join_to_the_same_bytes() {
        for (one, other) in [
            (path(&["a\0b"]), path(&["a", "b"])),
            (
                path(&["event=tick", "a\0b", "n", "ts"]),
                path(&["event=tick", "a", "b", "n", "ts"]),
            ),
            (path(&["a\0"]), path(&["a", ""])),
            (path(&["\0"]), path(&["", ""])),
            (path(&[]), path(&[""])),
        ] {
            assert_ne!(leaf_key(&one), leaf_key(&other), "{one:?} and {other:?}");
        }
        assert_eq!(
            leaf_key(&path(&["event=hit", "n", "ts"])),
            0xe84c_a476_c2e8_c3e6
        );
    }

    /// Two such templates with one row each at one instant: a page of one row stops
    /// between them, and the next page is the other row and not an empty one.
    #[test]
    fn a_page_that_stops_between_two_leaves_of_one_instant_resumes_on_the_other() {
        let lines = concat!(
            "{\"ts\":\"2024-01-15T10:00:00Z\",\"a\\u0000b\":1}\n",
            "{\"ts\":\"2024-01-15T10:00:00Z\",\"a\":1,\"b\":1}\n",
        );
        let db = db::load(
            lines.as_bytes(),
            true,
            chrono::Utc::now(),
            &kibbr_storage::trie::Schema::default(),
        )
        .expect("the two lines load");
        let mut q = query(QueryMode::Fetch, None, None);
        q.limit = Some(1);
        q.limit_source = LimitSource::Caller;
        let first = execute(&db.trie, &db.stores, &q).expect("the first page answers");
        assert_eq!((first.count, first.rows.len()), (2, 1));
        let next = first.meta.next_cursor.expect("a row is left");

        q.cursor = Some(Cursor::decode(&next).expect("the engine's own cursor"));
        let second = execute(&db.trie, &db.stores, &q).expect("the second page answers");
        assert_eq!((second.count, second.rows.len()), (2, 1));
        assert_ne!(second.rows, first.rows);
        assert_eq!(second.meta.next_cursor, None);
    }

    /// `nanos_of` is the engine's reading of a window edge; the buckets are floored off
    /// the same instants, so the two cannot drift.
    #[test]
    fn a_window_edge_lands_in_the_bucket_that_holds_it() {
        let width = ns(300);
        let edge = nanos_of(at(EPOCH + 137));
        assert_eq!(bucket_of(edge, width), nanos_of(at(EPOCH)));
        assert_eq!(nanos_of(at(EPOCH)), ns(EPOCH));
    }

    /// A term read through the ladder once gives `value_matches`' answer for every value it
    /// can be put to: this is what says the scan's side asks the oracle's question.
    #[test]
    fn a_term_read_once_matches_what_value_matches_does() {
        use kibbr_storage::store::pool;

        let mut values = vec![ColValue::Null];
        for row in pool::rows() {
            values.extend(row.get("v").cloned());
        }
        for text in pool::TEXTS {
            values.push(ColValue::String(text.to_string()));
        }
        let mut wants = vec![MatchValue::Any];
        wants.extend(
            pool::TEXTS
                .iter()
                .map(|text| MatchValue::Exact(text.to_string())),
        );

        let (mut by_text, mut by_ladder) = (0, 0);
        for value in wants {
            let m = Match {
                key: "v".to_string(),
                value,
            };
            let held = term(&m);
            assert_eq!(held.key(), "v");
            for seen in &values {
                let want = value_matches(&m.value, seen);
                assert_eq!(held.matches(seen), want, "{m:?} put to {seen:?}");
                // Which of the two readings said yes: a string value can only be the
                // text itself, and any other value only what the ladder made of it.
                match (want, &m.value, seen) {
                    (true, MatchValue::Exact(_), ColValue::String(_)) => by_text += 1,
                    (true, MatchValue::Exact(_), _) => by_ladder += 1,
                    _ => {}
                }
            }
        }
        // Not agreement on nothing: every text found itself as a string, and the ladder's
        // reading of one found a value that is not a string over a hundred times.
        assert!(
            by_text >= pool::TEXTS.len() && by_ladder > 100,
            "{by_text} {by_ladder}"
        );
    }

    /// What filtering on columns stands on: a term put to a decoded column selects the rows
    /// `value_matches` selects from rendered rows, over generated columns of every type.
    #[test]
    fn a_term_put_to_a_column_selects_the_rows_value_matches_does() {
        use kibbr_storage::store::columns::{hits, hits_by_row, Sel};
        use kibbr_storage::store::v3::column::{Column, ValuesRef};
        use kibbr_storage::store::v3::{read, samples};
        use kibbr_storage::store::{pool, render_row};

        let mut wants = vec![MatchValue::Any];
        wants.extend(pool::terms().into_iter().map(MatchValue::Exact));
        let matches: Vec<Match> = wants
            .into_iter()
            .map(|value| Match {
                key: "v".to_string(),
                value,
            })
            .collect();

        // Rows a term for one value matched, by the type of the piece that held the row
        // and by whether the column was text; and what the rounds were made of.
        let mut matched: BTreeMap<&'static str, [u64; 2]> = BTreeMap::new();
        let (mut present, mut null, mut two_pieces, mut one_piece) = (0u64, 0u64, 0, 0);
        let mut constants = 0;
        for seed in 0..200u64 {
            let mut rng = Rng::new(seed);
            let rows = rng.between(1, 300) as usize;
            let single = rng.chance(15);
            let kinds: Vec<&[&str]> = (0..rng.between(1, 3))
                .map(|_| {
                    let kind: &[&str] = rng.pick(pool::KINDS);
                    if single {
                        let at = rng.below(kind.len() as u64) as usize;
                        &kind[at..=at]
                    } else {
                        kind
                    }
                })
                .collect();
            let majority = rng.chance(50);
            let nulls = rng.chance(33);
            let values: Vec<&str> = (0..rows)
                .map(|_| {
                    if nulls && rng.chance(20) {
                        return "null";
                    }
                    let kind = if majority && rng.chance(85) {
                        kinds[0]
                    } else {
                        *rng.pick(&kinds)
                    };
                    *rng.pick(kind)
                })
                .collect();
            let lines = samples::lines_of(&values);
            let sample = samples::of_lines("generated", &lines);
            let chunk = &sample.chunks[sample.place("v").expect("a column v")];

            let cells = read::chunk(&chunk.bytes, chunk.at, &chunk.dir, rows)
                .expect("a chunk the builder made decodes");
            let column = Column::decode(&chunk.bytes, chunk.at, &chunk.dir, rows)
                .expect("a chunk the builder made decodes typed");
            let col = column.over(&chunk.bytes).expect("over its own bytes");

            // Which piece holds each row: bookkeeping for the counts below, not part of
            // either side.
            let pieces = col.pieces().expect("pieces");
            let piece_of: Vec<Option<&'static str>> = (0..rows)
                .map(|row| {
                    let (in_b, _) = match col.slots() {
                        None => (false, row),
                        Some(slots) => slots[row].get()?,
                    };
                    let piece = pieces[usize::from(in_b)].as_ref()?;
                    Some(piece.form.column_type().as_str())
                })
                .collect();
            match &pieces {
                [Some(_), Some(_)] => two_pieces += 1,
                [Some(_), None] => one_piece += 1,
                _ => {}
            }
            constants += pieces
                .iter()
                .flatten()
                .filter(|piece| {
                    !matches!(
                        piece.values,
                        ValuesRef::Ints(_)
                            | ValuesRef::Floats(_)
                            | ValuesRef::Times(_)
                            | ValuesRef::Bools(_)
                            | ValuesRef::Words { .. }
                            | ValuesRef::Texts(_)
                            | ValuesRef::Fixed { .. }
                    )
                })
                .count();

            for is_text in [false, true] {
                let text_columns: Vec<String> = if is_text {
                    vec!["v".to_string()]
                } else {
                    Vec::new()
                };
                // What a rendered row holds under `v`, for each row.
                let seen: Vec<Option<ColValue>> = cells
                    .iter()
                    .map(|(value, written)| {
                        let mut row = Row::new();
                        row.insert_written("v".to_string(), value.clone(), written.clone());
                        let rendered = render_row(&row, &text_columns);
                        rendered.as_ref().unwrap_or(&row).get("v").cloned()
                    })
                    .collect();
                // `value_matches` runs the ladder on the term's text at every call, so it is asked
                // once for each value that differs, which is all it can tell apart.
                let mut distinct: Vec<&ColValue> = Vec::new();
                let which: Vec<Option<usize>> = seen
                    .iter()
                    .map(|have| {
                        let have = have.as_ref()?;
                        Some(match distinct.iter().position(|known| *known == have) {
                            Some(at) => at,
                            None => {
                                distinct.push(have);
                                distinct.len() - 1
                            }
                        })
                    })
                    .collect();

                for m in &matches {
                    let said: Vec<bool> = distinct
                        .iter()
                        .map(|have| value_matches(&m.value, have))
                        .collect();
                    let mut expected = Sel::none(rows);
                    for (row, which) in which.iter().enumerate() {
                        if which.is_some_and(|at| said[at]) {
                            expected.set(row);
                        }
                    }

                    let held = term(m);
                    let typed = hits(col, &held, is_text).expect("a typed compare");
                    assert_eq!(
                        typed,
                        expected,
                        "seed {seed}, {m:?}, text column: {is_text}, by typed compare, over:\n{}",
                        lines.join("\n")
                    );
                    let printed = hits_by_row(col, &held, is_text).expect("each row printed");
                    assert_eq!(
                        printed,
                        expected,
                        "seed {seed}, {m:?}, text column: {is_text}, row by row, over:\n{}",
                        lines.join("\n")
                    );

                    if matches!(m.value, MatchValue::Any) {
                        present += expected.count();
                        null += rows as u64 - expected.count();
                        continue;
                    }
                    for row in expected.iter() {
                        let kind = piece_of[row].expect("a term matched a null");
                        matched.entry(kind).or_default()[usize::from(is_text)] += 1;
                    }
                }
            }
        }
        // Not agreement on nothing: a piece of every type matched terms, as its own type and as
        // text; columns of one piece and of two; nulls; and pieces that are one value stored once.
        let kinds: Vec<&str> = matched.keys().copied().collect();
        assert_eq!(
            kinds,
            ["Bool", "Datetime", "Float64", "Int64", "Ip", "String", "Uuid"],
            "{matched:?}"
        );
        assert!(
            matched
                .values()
                .all(|[own, text]| *own > 1_000 && *text > 1_000),
            "{matched:?}"
        );
        assert!(
            present > 10_000 && null > 1_000 && two_pieces > 20 && one_piece > 20,
            "{present} {null} {two_pieces} {one_piece}"
        );
        assert!(constants > 20, "{constants} constant pieces");
    }

    /// A day of two templates for both stores: one with a time column in several spellings
    /// and sometimes missing, a `when` that widens to text and a mixed `status`; one untimed.
    fn two_store_lines() -> Vec<String> {
        two_templates(900, 1)
    }

    /// `two_store_lines`, of `count` lines of the first template, with each run of
    /// `tied` of them written at one second: rows only their position tells apart.
    fn two_templates(count: i64, tied: i64) -> Vec<String> {
        let mut rng = Rng::new(23);
        let mut out = Vec::new();
        for n in 0..count {
            let secs = EPOCH + (n - n % tied) * 37;
            let at = chrono::DateTime::from_timestamp(secs, 0).expect("a representable instant");
            let offset = at
                .with_timezone(&chrono::FixedOffset::east_opt(19_800).expect("an offset"))
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, false);
            let zulu = at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            let ts = match n % 7 {
                _ if n % 11 == 4 => "null".to_string(),
                3 => secs.to_string(),
                5 => format!(r#""{offset}""#),
                _ => format!(r#""{zulu}""#),
            };
            let when = match n % 50 {
                7 => "N/A",
                _ if n % 4 == 1 => offset.as_str(),
                _ => zulu.as_str(),
            };
            let level = ["ERROR", "WARN", "INFO"][rng.below(3) as usize];
            let status = ["200", "200", "404", r#""N/A""#, "null"][rng.below(5) as usize];
            out.push(format!(
                r#"{{"ts":{ts},"level":"{level}","status":{status},"when":"{when}","n":{n}}}"#
            ));
            if n % 3 == 0 {
                out.push(format!(r#"{{"level":"ERROR","event":"untimed","n":{n}}}"#));
            }
        }
        out
    }

    /// The lines through `db::ingest_line` into a trie and a store of their own, with a tick
    /// after `flush_after` lines so that a disk store holds row groups and rows in memory.
    fn stored(stores: StoreManager, lines: &[String], flush_after: Option<usize>) -> db::Db {
        let schema = kibbr_storage::trie::Schema::default();
        let trie = Trie::new().with_time_columns(schema.time_columns.clone());
        let now = chrono::DateTime::from_timestamp(EPOCH + DAY / 2, 0).expect("an instant");
        for (at, line) in lines.iter().enumerate() {
            let parsed = kibbr_core::flatten::parse_line(line.as_bytes(), at + 1).expect("a line");
            db::ingest_line(&trie, Some(&stores), &parsed, now).expect("ingested");
            if flush_after == Some(at + 1) {
                let done = stores.flush_all(kibbr_storage::store::FlushKind::Tick);
                assert_eq!(done.failed, 0);
            }
        }
        db::Db {
            trie,
            stores,
            stats: db::LoadStats::default(),
        }
    }

    /// A count, a grouping and a fetch answer from columns what they answer from rows: every
    /// mode is asked of the memory store and of a part-flushed disk store, and must agree.
    #[test]
    fn a_count_a_grouping_and_a_fetch_answer_from_columns_what_they_answer_from_rows() {
        use kibbr_storage::store::{DiskOptions, RowGroupLimits};

        let dir = Scratch::new("two-stores");
        let lines = two_store_lines();
        let memory = stored(StoreManager::memory(), &lines, None);
        let options = DiskOptions {
            row_groups: RowGroupLimits {
                rows: 128,
                ..RowGroupLimits::default()
            },
            ..DiskOptions::default()
        };
        let disk = stored(
            StoreManager::disk(dir.0.clone(), options).expect("a disk store"),
            &lines,
            Some(lines.len() * 2 / 3),
        );
        assert!(disk.stores.buffered() > 100, "nothing was left in memory");
        assert!(disk.stores.rows() - disk.stores.buffered() > 400);

        let exact = |key: &str, text: &str| Match {
            key: key.into(),
            value: MatchValue::Exact(text.into()),
        };
        let any = |key: &str| Match {
            key: key.into(),
            value: MatchValue::Any,
        };
        let patterns = [
            vec![],
            vec![exact("level", "ERROR")],
            vec![exact("status", "200")],
            vec![exact("status", "N/A")],
            vec![any("status")],
            vec![exact("level", "WARN"), exact("status", "404")],
            // The time column is a date however a line spelled it: line 149 by an offset
            // and by its instant, and line 150 by the epoch seconds it wrote.
            vec![exact("ts", "2024-01-15T07:01:53+05:30")],
            vec![exact("ts", "2024-01-15T01:31:53Z")],
            vec![exact("ts", "1705282350")],
            // `when` is text: line 149 by the offset it wrote and not by its instant,
            // line 150 by its own text, and the lines that say `N/A`.
            vec![exact("when", "2024-01-15T07:01:53+05:30")],
            vec![exact("when", "2024-01-15T01:31:53Z")],
            vec![exact("when", "2024-01-15T01:32:30Z")],
            vec![exact("when", "N/A")],
            vec![any("event")],
            vec![exact("n", "120")],
        ];
        let column = |name: &str| GroupKey::Column(name.to_string());
        let hour = GroupKey::Time(Duration::from_secs(3_600));
        let modes = [
            (QueryMode::Count, None),
            (QueryMode::Exists, None),
            (QueryMode::GroupBy(vec![hour.clone()]), None),
            (QueryMode::GroupBy(vec![column("level")]), None),
            (QueryMode::GroupBy(vec![hour, column("status")]), None),
            (
                QueryMode::GroupBy(vec![column("event"), column("level")]),
                None,
            ),
            (QueryMode::GroupBy(vec![column("when")]), None),
            (QueryMode::Fetch, None),
            (QueryMode::Fetch, Some(7)),
        ];
        let windows = [
            (None, None),
            (Some(EPOCH + 3_000), Some(EPOCH + 9_000)),
            (Some(EPOCH + 30_000), None),
        ];

        let (mut counted, mut grouped, mut fetched, mut read) = (0u64, 0usize, 0usize, 0u64);
        for pattern in &patterns {
            for (mode, limit) in &modes {
                for (since, until) in windows {
                    let ask = || {
                        Query::new(
                            pattern.clone(),
                            mode.clone(),
                            TimeRange {
                                since: since.map(at),
                                until: until.map(at),
                            },
                            *limit,
                            None,
                            at(EPOCH + DAY),
                            Limits {
                                max_rows: 0,
                                max_buckets: 0,
                                max_groups: 0,
                            },
                        )
                    };
                    let said = format!("{pattern:?} as {mode:?} in {since:?}..{until:?}");
                    let rows = execute(&memory.trie, &memory.stores, &ask()).expect("memory");
                    let columns = execute(&disk.trie, &disk.stores, &ask()).expect("disk");
                    assert_eq!(columns.exists, rows.exists, "{said}");
                    assert_eq!(columns.count, rows.count, "{said}");
                    assert_eq!(shown(&columns.rows), shown(&rows.rows), "{said}");
                    assert_eq!(columns.groups, rows.groups, "{said}");
                    assert_eq!(
                        (
                            columns.meta.templates_matched,
                            columns.meta.rows_returned,
                            columns.meta.truncated,
                            &columns.meta.next_cursor,
                        ),
                        (
                            rows.meta.templates_matched,
                            rows.meta.rows_returned,
                            rows.meta.truncated,
                            &rows.meta.next_cursor,
                        ),
                        "{said}"
                    );
                    counted += columns.count;
                    grouped += columns.groups.iter().filter(|g| g.count > 0).count();
                    fetched += columns.rows.len();
                    read += columns.meta.row_groups_read;
                }
            }
        }
        // Not agreement on nothing, and not one store asked twice.
        assert!(
            counted > 10_000 && grouped > 400 && fetched > 2_000,
            "{counted} {grouped} {fetched}"
        );
        assert!(read > 500, "{read} row groups read");
        let count_of = |pattern: &Vec<Match>| {
            let q = Query::new(
                pattern.clone(),
                QueryMode::Count,
                TimeRange::default(),
                None,
                None,
                at(EPOCH + DAY),
                Limits {
                    max_rows: 0,
                    max_buckets: 0,
                    max_groups: 0,
                },
            );
            execute(&disk.trie, &disk.stores, &q).expect("disk").count
        };
        // Every pattern finds a row but the one that asks a text column for an instant.
        for pattern in &patterns[1..] {
            let found = count_of(pattern);
            let instant = *pattern == vec![exact("when", "2024-01-15T01:31:53Z")];
            assert_eq!(found == 0, instant, "{pattern:?} found {found}");
        }
        // And `when` is text and the time column is not, as the lines were written to
        // make them.
        let q = Query::new(
            vec![any("when")],
            QueryMode::Count,
            TimeRange::default(),
            None,
            None,
            at(EPOCH + DAY),
            Limits {
                max_rows: 0,
                max_buckets: 0,
                max_groups: 0,
            },
        );
        let planned = plan(&disk.trie, &q);
        assert_eq!(planned.leaves.len(), 1);
        let text = &planned.leaves[0].template.text_columns;
        assert!(
            text.iter().any(|c| c == "when") && !text.iter().any(|c| c == "ts"),
            "{text:?}"
        );
    }

    // ── A fetch that is asked before a row is built ────────────────────────────────

    /// A row that is only its number, at a time when it has one.
    fn numbered(id: u64, ts: Option<i64>) -> Row {
        let mut row = Row::new();
        row.insert("id".to_string(), ColValue::Int64(id as i64));
        if let Some(at) = ts {
            row.insert("ts".to_string(), ColValue::Datetime(at));
        }
        row
    }

    /// One page as these tests compare it: the ranks the heap held when the scan ended,
    /// in the order it held them, then the rows and the rank to resume after.
    type Page = (Vec<Rank>, Vec<Row>, Option<Rank>);

    /// Rows offered to one heap, and how many the fetch refused. With `ask`, the fetch is
    /// asked about each row first, as a row that lies in its day, and handed it only on a yes.
    fn offered(
        rows: &[(u64, u64, Option<i64>, Row)],
        keep: Option<usize>,
        after: Option<Rank>,
        ask: bool,
    ) -> (Page, usize) {
        let mut newest = Newest::new(keep);
        let mut refused = 0;
        for (key, seq, ts, row) in rows {
            let mut fetch = Fetch {
                keep: &mut newest,
                key: *key,
                ts: "ts",
                after,
            };
            if ask && !fetch.wants_row(*ts, *seq, true) {
                refused += 1;
                continue;
            }
            fetch.row(row, *seq);
        }
        let held = newest.heap.iter().map(|ranked| ranked.rank).collect();
        let (rows, next) = newest.finish();
        ((held, rows, next), refused)
    }

    /// Asking first changes no page, and leaves the heap itself the same, while no two rows
    /// carry one `Rank` - a property the standard library does not promise and this holds.
    #[test]
    fn a_fetch_asked_first_keeps_the_rows_it_keeps_when_handed_every_row() {
        let (mut pages, mut refused, mut shared) = (0usize, 0usize, 0usize);
        for seed in 0..300u64 {
            let mut rng = Rng::new(seed);
            let instants: Vec<i64> = (0..rng.between(1, 5))
                .map(|_| ns(EPOCH + rng.between(0, 20)))
                .collect();
            let mut rows = Vec::new();
            for key in [11u64, 5, 23].into_iter().take(rng.between(1, 3) as usize) {
                for seq in 0..rng.between(1, 25) as u64 {
                    let ts = (!rng.chance(10)).then(|| *rng.pick(&instants));
                    rows.push((key, seq, ts, numbered(rows.len() as u64, ts)));
                }
            }
            let ranks: BTreeSet<Rank> = rows
                .iter()
                .map(|(key, seq, ts, _)| (Reverse(*ts), *key, *seq))
                .collect();
            assert_eq!(ranks.len(), rows.len(), "two rows of one rank were drawn");
            let times: BTreeSet<Option<i64>> = rows.iter().map(|row| row.2).collect();
            shared += rows.len() - times.len();
            let mut shuffled = rows.clone();
            rng.shuffle(&mut shuffled);

            let few = rng.between(2, 9) as usize;
            for keep in [
                None,
                Some(0),
                Some(1),
                Some(few),
                Some(rows.len()),
                Some(usize::MAX),
            ] {
                // Every cursor a page of this limit ends on, and a few that are a row's
                // rank or between two.
                let mut cursors = vec![None];
                cursors.extend((0..3).map(|_| {
                    let (key, seq, ts, _) = rng.pick(&rows);
                    Some((Reverse(*ts), *key, *seq + rng.below(2)))
                }));
                while let Some(after) = cursors.pop() {
                    let said = format!("seed {seed}, limit {keep:?}, after {after:?}");
                    let (every, _) = offered(&rows, keep, after, false);
                    let (asked, turned_away) = offered(&rows, keep, after, true);
                    assert_eq!(asked, every, "{said}");
                    let (mixed, _) = offered(&shuffled, keep, after, true);
                    assert_eq!((&mixed.1, &mixed.2), (&every.1, &every.2), "{said}");
                    pages += 1;
                    refused += turned_away;
                    if let (Some(next), None) = (every.2, after) {
                        cursors.push(Some(next));
                    } else if let (Some(next), Some(was)) = (every.2, after) {
                        assert!(was < next, "{said}: a page ended where it began");
                        cursors.push(Some(next));
                    }
                }
            }
        }
        // Not agreement on nothing: thousands of pages, most rows sharing their time
        // with another, and rows turned away by the ten thousand.
        assert!(
            pages > 5_000 && refused > 50_000 && shared > 5_000,
            "{pages} {refused} {shared}"
        );
    }

    /// One day of one template in a model of a store that keeps days: the newest time stored
    /// in it, and its rows in storage order with the time each shows when read.
    struct ModelDay {
        day: i64,
        newest: Option<i64>,
        rows: Vec<(Option<i64>, Row)>,
    }

    /// How a store offers a fetch its rows.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Offers {
        /// Every row handed over and none asked about.
        Every,
        /// Each row asked about first, with whether its time lies in its day.
        Asked,
        /// Each row asked about first and said to lie in its day whether or not it does:
        /// what asking first would be if it did not look.
        AskedAsIfPlaced,
    }

    /// One page of a fetch over the model, driven as a disk leaf drives its visitor; a day
    /// declined by `wants_partition` is skipped, or read all the same under `reads_declined`.
    fn page_of(
        leaves: &[(u64, Vec<ModelDay>)],
        keep: Option<usize>,
        after: Option<Rank>,
        offers: Offers,
        reads_declined: bool,
    ) -> Page {
        let mut newest = Newest::new(keep);
        for (key, days) in leaves {
            let mut fetch = Fetch {
                keep: &mut newest,
                key: *key,
                ts: "ts",
                after,
            };
            let asks = offers != Offers::Every && fetch.wants() == Wants::Newest;
            for day in days {
                if !fetch.wants_partition(day.newest) && !reads_declined {
                    continue;
                }
                for (seq, (ts, row)) in day.rows.iter().enumerate() {
                    let placed = offers == Offers::AskedAsIfPlaced
                        || kibbr_storage::store::v3::day_of(ts.unwrap_or(0)) == day.day;
                    if asks && !fetch.wants_row(*ts, seq as u64, placed) {
                        continue;
                    }
                    fetch.row(row, seq as u64);
                }
            }
        }
        let held = newest.heap.iter().map(|ranked| ranked.rank).collect();
        let (rows, next) = newest.finish();
        (held, rows, next)
    }

    /// Every page of a fetch over the model, each from the cursor the one before ended
    /// on.
    fn pages_of(
        leaves: &[(u64, Vec<ModelDay>)],
        keep: Option<usize>,
        offers: Offers,
        reads_declined: bool,
    ) -> Vec<Page> {
        let mut pages: Vec<Page> = Vec::new();
        let mut after = None;
        loop {
            let page = page_of(leaves, keep, after, offers, reads_declined);
            let next = page.2;
            pages.push(page);
            match next {
                Some(rank) => {
                    assert!(
                        after.is_none_or(|was| was < rank),
                        "a page ended where it began"
                    );
                    after = Some(rank);
                }
                None => return pages,
            }
        }
    }

    /// A model day of rows written at `stored` seconds into day `day`, each shown at
    /// the time `shown` gives for it.
    fn model_day(
        day: i64,
        ids: &mut u64,
        stored: &[i64],
        mut shown: impl FnMut(i64) -> Option<i64>,
    ) -> ModelDay {
        let start = day * kibbr_storage::store::v3::DAY_NS;
        ModelDay {
            day,
            newest: stored.iter().map(|secs| start + ns(*secs)).max(),
            rows: stored
                .iter()
                .map(|secs| {
                    let ts = shown(start + ns(*secs));
                    *ids += 1;
                    (ts, numbered(*ids, ts))
                })
                .collect(),
        }
    }

    /// Asking first changes no page when rows tie either, because the fetch stops asking at
    /// the first row whose time is not in its day; the third run does not look, and differs.
    #[test]
    fn a_fetch_stops_asking_at_the_first_row_that_is_not_in_its_day() {
        let none = |_: i64| None;
        let mut ids = 0;
        let smallest = vec![(
            7u64,
            vec![
                model_day(19_739, &mut ids, &[5], Some),
                model_day(19_738, &mut ids, &[1, 2], none),
                model_day(19_737, &mut ids, &[1, 2], none),
            ],
        )];
        let every = pages_of(&smallest, Some(1), Offers::Every, false);
        assert_eq!(pages_of(&smallest, Some(1), Offers::Asked, false), every);
        assert_ne!(
            pages_of(&smallest, Some(1), Offers::AskedAsIfPlaced, false),
            every,
            "asking first without looking served the rows it serves when it looks"
        );

        let (mut pages, mut tied_sets, mut moved, mut moved_healthy) = (0usize, 0, 0, 0);
        for seed in 0..1_500u64 {
            let mut rng = Rng::new(seed);
            let odd = rng.chance(50);
            let days = [19_739i64, 19_738, 19_737];
            let mut ids = 0;
            let mut leaves = Vec::new();
            for key in [7u64, 3, 12].into_iter().take(rng.between(1, 3) as usize) {
                let mut held = Vec::new();
                for day in days {
                    if rng.chance(25) {
                        continue;
                    }
                    let stored: Vec<i64> =
                        (0..rng.between(1, 5)).map(|_| rng.between(0, 3)).collect();
                    held.push(model_day(day, &mut ids, &stored, |at| {
                        match rng.below(10) {
                            0..=3 if odd => None,
                            4 if odd => {
                                Some(*rng.pick(&days) * kibbr_storage::store::v3::DAY_NS + ns(1))
                            }
                            _ => Some(at),
                        }
                    }));
                }
                leaves.push((key, held));
            }
            let mut ranks = BTreeSet::new();
            let mut rows = 0;
            for (key, held) in &leaves {
                for day in held {
                    for (seq, (ts, _)) in day.rows.iter().enumerate() {
                        ranks.insert((Reverse(*ts), *key, seq as u64));
                        rows += 1;
                    }
                }
            }
            let tied = ranks.len() < rows;
            assert!(
                odd || !tied,
                "seed {seed}: a healthy store held two rows of one rank"
            );
            tied_sets += usize::from(tied);

            for keep in [
                None,
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(5),
                Some(usize::MAX),
            ] {
                for reads_declined in [false, true] {
                    let said = format!(
                        "seed {seed}, limit {keep:?}, reads declined days: {reads_declined}"
                    );
                    let every = pages_of(&leaves, keep, Offers::Every, reads_declined);
                    let asked = pages_of(&leaves, keep, Offers::Asked, reads_declined);
                    assert_eq!(asked, every, "{said}");
                    pages += every.len();
                    let blind = pages_of(&leaves, keep, Offers::AskedAsIfPlaced, reads_declined);
                    if blind != every {
                        moved += 1;
                        moved_healthy += usize::from(!odd);
                    }
                }
            }
        }
        // Not agreement on nothing: sets with tied rows, pages that move when the fetch does not
        // look, and none of them in a store whose rows are all in their days.
        assert!(pages > 20_000 && tied_sets > 300, "{pages} {tied_sets}");
        assert!(moved > 20 && moved_healthy == 0, "{moved} {moved_healthy}");
    }

    /// A directory of a test's own under the system's, removed when the test ends.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "kibbr-engine-{name}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Rows as an answer shows them: a disk row can hold a null arrival time an answer leaves
    /// out, so two stores' rows are compared as they are shown and not as they are held.
    fn shown(rows: &[Row]) -> Vec<BTreeMap<String, Option<serde_json::Value>>> {
        rows.iter()
            .map(|row| {
                kibbr_storage::store::present(row)
                    .into_iter()
                    .map(|(name, key)| (name, row.json(key)))
                    .collect()
            })
            .collect()
    }

    /// Every page of a fetch, first to last, each asked with the cursor the one before ended
    /// on. `limit` is the caller's and `max_rows` the server's backstop.
    fn pages(
        db: &db::Db,
        pattern: &[Match],
        (since, until): (Option<i64>, Option<i64>),
        (limit, max_rows): (Option<usize>, usize),
        run: &dyn Fn(&db::Db, &Query) -> QueryResult,
    ) -> Vec<QueryResult> {
        let mut out = Vec::new();
        let mut cursor = None;
        loop {
            let q = Query::new(
                pattern.to_vec(),
                QueryMode::Fetch,
                TimeRange {
                    since: since.map(at),
                    until: until.map(at),
                },
                limit,
                cursor,
                at(EPOCH + 7 * DAY),
                Limits {
                    max_rows,
                    max_buckets: 0,
                    max_groups: 0,
                },
            );
            let page = run(db, &q);
            let next = page.meta.next_cursor.clone();
            out.push(page);
            assert!(out.len() < 10_000, "a fetch that never ends");
            match next {
                Some(text) => {
                    cursor = Some(Cursor::decode(&text).expect("the engine's own cursor"));
                }
                None => return out,
            }
        }
    }

    /// Two runs of pages are one answer: page for page the same rows in the same order,
    /// the same count, cut or not alike, and the same cursor.
    fn assert_same_pages(one: &[QueryResult], other: &[QueryResult], said: &str) {
        assert_eq!(one.len(), other.len(), "{said}: pages");
        for (n, (a, b)) in one.iter().zip(other).enumerate() {
            assert_eq!(shown(&a.rows), shown(&b.rows), "{said}: page {n}");
            assert_eq!(
                (
                    a.count,
                    a.meta.rows_returned,
                    a.meta.truncated,
                    &a.meta.next_cursor
                ),
                (
                    b.count,
                    b.meta.rows_returned,
                    b.meta.truncated,
                    &b.meta.next_cursor
                ),
                "{said}: page {n}"
            );
        }
    }

    fn executed(db: &db::Db, q: &Query) -> QueryResult {
        execute(&db.trie, &db.stores, q).expect("the query answers")
    }

    /// A fetch pages through columns as it pages through rows: most rows here are told apart
    /// by position alone, so a position counted wrong across a row group's end shows up.
    #[test]
    fn a_fetch_pages_through_columns_as_it_pages_through_rows() {
        use kibbr_storage::store::{DiskOptions, RowGroupLimits};

        let dir = Scratch::new("pages");
        let lines = two_templates(300, 4);
        let memory = stored(StoreManager::memory(), &lines, None);
        let options = DiskOptions {
            row_groups: RowGroupLimits {
                rows: 64,
                ..RowGroupLimits::default()
            },
            ..DiskOptions::default()
        };
        let disk = stored(
            StoreManager::disk(dir.0.clone(), options).expect("a disk store"),
            &lines,
            Some(lines.len() * 2 / 3),
        );
        assert!(disk.stores.buffered() > 50, "nothing was left in memory");
        assert!(disk.stores.rows() - disk.stores.buffered() > 150);

        let exact = |key: &str, text: &str| Match {
            key: key.into(),
            value: MatchValue::Exact(text.into()),
        };
        let patterns = [
            vec![],
            vec![exact("level", "ERROR")],
            vec![exact("status", "200")],
        ];
        let windows = [(None, None), (Some(EPOCH + 2_000), Some(EPOCH + 9_000))];
        let limits = [
            (Some(0), 0),
            (Some(1), 0),
            (Some(2), 0),
            (Some(7), 0),
            (Some(500), 0),
            (None, 150),
        ];
        let (mut asked, mut served) = (0usize, 0u64);
        for pattern in &patterns {
            for window in windows {
                for limit in limits {
                    let said = format!("{pattern:?} in {window:?}, limit {limit:?}");
                    let rows = pages(&memory, pattern, window, limit, &executed);
                    let columns = pages(&disk, pattern, window, limit, &executed);
                    assert_same_pages(&columns, &rows, &said);
                    let count = columns[0].count;
                    assert!(count > 20, "{said}: {count} rows matched");
                    let handed: u64 = columns.iter().map(|page| page.meta.rows_returned).sum();
                    if limit.0 == Some(0) {
                        assert_eq!((columns.len(), handed), (1, 0), "{said}");
                    } else {
                        assert_eq!(handed, count, "{said}");
                    }
                    asked += columns.len();
                    served += handed;
                }
            }
        }
        assert!(asked > 1_000 && served > 4_000, "{asked} {served}");
    }

    /// A fetch's visitor with a record kept of what a store asked it and handed it.
    struct Watched<'a> {
        fetch: Fetch<'a>,
        /// Each row it was asked about: its time, its position, whether the store said
        /// the time is in the row's day, and what the fetch answered.
        asked: Vec<(Option<i64>, u64, bool, bool)>,
        handed: u64,
    }

    impl Visitor for Watched<'_> {
        fn row(&mut self, row: &Row, seq: u64) {
            self.handed += 1;
            self.fetch.row(row, seq);
        }

        fn wants_partition(&mut self, newest: Option<i64>) -> bool {
            self.fetch.wants_partition(newest)
        }

        fn wants(&self) -> Wants {
            self.fetch.wants()
        }

        fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
            let said = self.fetch.wants_row(ts, seq, placed);
            self.asked.push((ts, seq, placed, said));
            said
        }
    }

    /// Rows that tie across two days page the same whether or not a store asks first: one
    /// row out of its day turns the asking off for the rest of the query.
    #[test]
    fn rows_that_tie_across_two_days_page_as_they_did_before_a_store_could_ask() {
        use kibbr_storage::store::{DiskOptions, FlushKind};

        let dir = Scratch::new("tied");
        let mut lines = vec![
            r#"{"ts":"2024-01-15T10:00:00Z","v":"old"}"#.to_string(),
            // What makes the time column text. It goes by its arrival time.
            r#"{"ts":"N/A","v":"undated"}"#.to_string(),
        ];
        // The newest day, newest row first: a full heap then has no use for the rest.
        for n in 0..6 {
            let second = 5 - n;
            lines.push(format!(
                r#"{{"ts":"2113-01-01T10:00:0{second}Z","v":"new {n}"}}"#
            ));
        }
        for n in 0..4 {
            lines.push(format!(
                r#"{{"ts":"2024-01-16T09:00:0{n}Z","u":{n},"v":"other {n}"}}"#
            ));
        }
        let disk = stored(
            StoreManager::disk(dir.0.clone(), DiskOptions::default()).expect("a disk store"),
            &lines,
            None,
        );

        let any = |key: &str| Match {
            key: key.into(),
            value: MatchValue::Any,
        };
        let planned = |pattern: Vec<Match>| {
            let q = Query::new(
                pattern,
                QueryMode::Fetch,
                TimeRange::default(),
                Some(1),
                None,
                at(EPOCH + 7 * DAY),
                Limits {
                    max_rows: 0,
                    max_buckets: 0,
                    max_groups: 0,
                },
            );
            plan(&disk.trie, &q).leaves
        };
        let leaves = planned(vec![any("v")]);
        assert_eq!(leaves.len(), 2);
        let (tied, other) = (&leaves[0], &leaves[1]);
        assert!(
            tied.template.text_columns.iter().any(|c| c == "ts"),
            "{tied:?}"
        );
        assert!(
            other.template.path.iter().any(|token| token == "u"),
            "{other:?}"
        );
        assert!(!tied.residual.is_empty() && !other.residual.is_empty());

        // By hand, into the first template: a row stored under the day of its instant, in a
        // column that shows it as the digits it is said to have arrived as.
        let adrift_days = [52_083i64, 52_084];
        let adrift_at = |day: i64, k: i64| day * DAY + 36_000 + k;
        for day in adrift_days {
            for k in 0..3 {
                let secs = adrift_at(day, k);
                assert!(infer(&secs.to_string()).value == ColValue::Int64(secs));
                let mut row = Row::new();
                row.insert_written(
                    "ts".to_string(),
                    ColValue::Datetime(ns(secs)),
                    Some(serde_json::Value::Number(secs.into())),
                );
                row.insert(
                    "v".to_string(),
                    ColValue::String(format!("day {day} row {k}")),
                );
                disk.stores
                    .append(tied.template_id, "ts", row)
                    .expect("appended");
            }
        }

        let every_row = |db: &db::Db, q: &Query| {
            answer(
                &db.trie,
                &db.stores,
                q,
                Newest::offered_every_row(q.limit),
                &Stop::default(),
            )
                .expect("the query answers")
        };
        let check = |state: &str| {
            let mut asked_pages = 0;
            for pattern in [vec![], vec![any("v")]] {
                for limit in [1, 2, 3] {
                    let said = format!("{state}: {pattern:?}, limit {limit}");
                    let limit = (Some(limit), 0);
                    let asked = pages(&disk, &pattern, (None, None), limit, &executed);
                    let every = pages(&disk, &pattern, (None, None), limit, &every_row);
                    assert_same_pages(&asked, &every, &said);
                    for (a, b) in asked.iter().zip(&every) {
                        assert_eq!(a.rows, b.rows, "{said}");
                    }
                    assert_eq!(asked[0].count, 18, "{said}");
                    asked_pages += asked.len();
                }
            }
            assert!(asked_pages > 30, "{state}: {asked_pages} pages");

            // One page of two rows, driven by hand so that what the store asks is seen.
            let mut opts = ScanOptions::default();
            let ids = [tied.template_id, other.template_id];
            let mut snapshot = disk.stores.snapshot(&ids, &opts);
            let mut newest = Newest::new(Some(2));
            let mut seen = Vec::new();
            for leaf in [tied, other] {
                opts.ts_column = Some(leaf.template.time_column.clone());
                opts.text_columns = leaf.template.text_columns.clone();
                let terms: Vec<Term> = leaf.residual.iter().map(term).collect();
                let mut watched = Watched {
                    fetch: Fetch {
                        keep: &mut newest,
                        key: leaf_key(&leaf.template.path),
                        ts: leaf.template.time_column.as_str(),
                        after: None,
                    },
                    asked: Vec::new(),
                    handed: 0,
                };
                let first = watched.wants();
                let scan = snapshot
                    .visit(leaf.template_id, &opts, &terms, &mut watched)
                    .expect("the template has a leaf")
                    .expect("scan");
                seen.push((first, watched.asked, watched.handed, scan.matched));
            }
            let (first, asked, handed, matched) = &seen[0];
            assert_eq!((first, *matched), (&Wants::Newest, 14), "{state}");
            assert_eq!(asked.len() as u64, *matched, "{state}");
            let lost = asked
                .iter()
                .position(|(_, _, placed, _)| !placed)
                .expect("no row out of its day was asked about");
            // Before it, the newest day's six rows: asked about, and some refused.
            assert_eq!(lost, 6, "{state}: {asked:?}");
            assert!(asked[..lost].iter().any(|(.., said)| !said), "{state}");
            // From it on nothing is refused, in its day or out of it.
            assert!(asked[lost..].iter().all(|(.., said)| *said), "{state}");
            let turned_away = asked.iter().filter(|(.., said)| !said).count() as u64;
            assert_eq!(*handed, matched - turned_away, "{state}");
            // And the rows out of their days are the tied ones: no time, and each
            // position twice.
            let adrift: Vec<(Option<i64>, u64)> = asked
                .iter()
                .filter(|(_, _, placed, _)| !placed)
                .map(|(ts, seq, ..)| (*ts, *seq))
                .collect();
            let nowhere = |seq: u64| (None, seq);
            assert_eq!(adrift, [0, 1, 2, 0, 1, 2].map(nowhere), "{state}");

            let (first, asked, handed, matched) = &seen[1];
            assert_eq!((first, asked.len()), (&Wants::Rows, 0), "{state}");
            assert_eq!((*handed, *matched), (4, 4), "{state}");
        };

        check("in memory");
        // Every buffer written out, short as each is.
        let done = disk.stores.flush_all(FlushKind::Final);
        assert_eq!(done.failed, 0);
        assert_eq!(disk.stores.buffered(), 0);
        // The writer took the six rows as the instants they are: read with the time
        // column not text, each is still its date.
        let as_dates = ScanOptions {
            ts_column: Some("ts".to_string()),
            ..ScanOptions::default()
        };
        let leaf = disk
            .stores
            .lookup(tied.template_id)
            .expect("the template has a leaf");
        let held = leaf.scan(&as_dates, &[]).expect("scan").rows;
        let adrift = held
            .iter()
            .filter(|row| {
                matches!(row.get("ts"), Some(ColValue::Datetime(at))
                    if adrift_days.contains(&kibbr_storage::store::v3::day_of(*at)))
            })
            .count();
        assert_eq!((held.len(), adrift), (14, 6));
        check("in row groups");
    }

    // ── A scan shared between threads ──────────────────────────────────────────────

    /// Three templates over two days for a store whose row groups close at two rows. Two are
    /// timed and tie on their seconds; the third's time column is text and takes `adrift`.
    fn three_templates() -> (Vec<String>, Vec<String>) {
        let mut rng = Rng::new(31);
        let mut line = |day: i64, n: i64| {
            let secs = EPOCH + day * DAY + 3_600 + (n - n % 2) * 613;
            let ts = chrono::DateTime::from_timestamp(secs, 0)
                .expect("a representable instant")
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            let level = ["ERROR", "WARN", "INFO"][rng.below(3) as usize];
            let status = ["200", "404", r#""N/A""#, "null"][rng.below(4) as usize];
            [
                format!(r#"{{"ts":"{ts}","level":"{level}","status":{status},"n":{n}}}"#),
                format!(
                    r#"{{"ts":"{ts}","level":"{level}","job":"j{}","n":{n}}}"#,
                    n % 3
                ),
            ]
        };
        let (mut stored, mut in_memory) = (Vec::new(), Vec::new());
        for day in 0..2 {
            for n in 0..14 {
                stored.extend(line(day, n));
            }
            for n in 14..17 {
                in_memory.extend(line(day, n));
            }
        }
        // The third template after the other two, so that a query reads it last: its first row
        // out of its day ends the asking for the rest of a fetch.
        stored.push(r#"{"ts":"2024-01-15T10:00:00Z","v":"old"}"#.to_string());
        // What makes its time column text.
        stored.push(r#"{"ts":"N/A","v":"undated"}"#.to_string());
        for n in 0..6 {
            stored.push(format!(
                r#"{{"ts":"2113-01-01T10:00:0{}Z","v":"new {n}"}}"#,
                5 - n
            ));
        }
        in_memory.push(r#"{"ts":"2113-01-01T10:00:09Z","v":"late"}"#.to_string());
        (stored, in_memory)
    }

    /// Into the template whose time column is text, by hand: rows in two days of 2112 that
    /// show no time, so the rows at one position of the two days are one `Rank`.
    fn adrift(stores: &StoreManager, template_id: u64) {
        for day in [52_083i64, 52_084] {
            for k in 0..5 {
                let secs = day * DAY + 36_000 + k;
                let mut row = Row::new();
                row.insert_written(
                    "ts".to_string(),
                    ColValue::Datetime(ns(secs)),
                    Some(serde_json::Value::Number(secs.into())),
                );
                row.insert(
                    "v".to_string(),
                    ColValue::String(format!("day {day} row {k}")),
                );
                stores.append(template_id, "ts", row).expect("appended");
            }
        }
    }

    /// A query with no limit of the server's.
    fn asking(
        pattern: &[Match],
        mode: QueryMode,
        (since, until): (Option<i64>, Option<i64>),
    ) -> Query {
        Query::new(
            pattern.to_vec(),
            mode,
            TimeRange {
                since: since.map(at),
                until: until.map(at),
            },
            None,
            None,
            at(EPOCH + 7 * DAY),
            Limits {
                max_rows: 0,
                max_buckets: 0,
                max_groups: 0,
            },
        )
    }

    /// A directory another store wrote, opened again: the templates of `known` learnt, every
    /// leaf opened on the row groups it holds, and `in_memory` ingested and left unwritten.
    fn reopened(
        dir: &Scratch,
        options: kibbr_storage::store::DiskOptions,
        known: &[String],
        in_memory: &[String],
    ) -> db::Db {
        let stores = StoreManager::disk(dir.0.clone(), options).expect("a disk store");
        let schema = kibbr_storage::trie::Schema::default();
        let trie = Trie::new().with_time_columns(schema.time_columns.clone());
        let now = chrono::DateTime::from_timestamp(EPOCH + DAY / 2, 0).expect("an instant");
        for (at, line) in known.iter().enumerate() {
            let parsed = kibbr_core::flatten::parse_line(line.as_bytes(), at + 1).expect("a line");
            db::ingest_line(&trie, None, &parsed, now).expect("learnt");
        }
        for leaf in plan(&trie, &asking(&[], QueryMode::Count, (None, None))).leaves {
            stores
                .open_existing(leaf.template_id, &leaf.template.time_column)
                .expect("opened");
        }
        for (at, line) in in_memory.iter().enumerate() {
            let parsed = kibbr_core::flatten::parse_line(line.as_bytes(), at + 1).expect("a line");
            db::ingest_line(&trie, Some(&stores), &parsed, now).expect("ingested");
        }
        db::Db {
            trie,
            stores,
            stats: db::LoadStats::default(),
        }
    }

    /// Everything an answer says but how long it took: what a caller reads, and every
    /// counter of the scan behind it.
    fn whole(r: &QueryResult) -> impl PartialEq + std::fmt::Debug + '_ {
        (
            (r.exists, r.count, &r.rows, &r.groups),
            (&r.meta.templates_matched, &r.meta.template_ids),
            (
                r.meta.files_opened,
                r.meta.leaves_opened,
                r.meta.row_groups_read,
                r.meta.row_groups_pruned,
                r.meta.bytes_read,
                r.meta.rows_scanned,
                r.meta.rows_returned,
            ),
            (r.meta.truncated, r.meta.limit_source, &r.meta.next_cursor),
        )
    }

    /// The same questions two hundred times with every scan shared between threads: one
    /// answer and one set of counters, whichever thread chose a row group's rows.
    #[test]
    fn the_same_questions_answer_the_same_every_time_with_their_scans_shared_out() {
        use kibbr_storage::store::{DiskOptions, FlushKind, RowGroupLimits};

        const TIMES: u64 = 200;
        /// Every row: the two timed templates' seventeen lines a day each, the third's
        /// nine lines, and the ten put into it by hand.
        const ROWS: u64 = 2 * 2 * 17 + 9 + 10;
        let dir = Scratch::new("shared-out");
        let (lines, in_memory) = three_templates();
        let small = DiskOptions {
            row_groups: RowGroupLimits {
                rows: 2,
                ..RowGroupLimits::default()
            },
            ..DiskOptions::default()
        };
        let any = |key: &str| Match {
            key: key.into(),
            value: MatchValue::Any,
        };
        let exact = |key: &str, text: &str| Match {
            key: key.into(),
            value: MatchValue::Exact(text.into()),
        };
        {
            let written = stored(
                StoreManager::disk(dir.0.clone(), small).expect("a disk store"),
                &lines,
                None,
            );
            let tied = plan(
                &written.trie,
                &asking(&[any("v")], QueryMode::Count, (None, None)),
            )
            .leaves;
            assert_eq!(tied.len(), 1);
            assert!(
                tied[0].template.text_columns.iter().any(|c| c == "ts"),
                "{tied:?}"
            );
            adrift(&written.stores, tied[0].template_id);
            let done = written.stores.flush_all(FlushKind::Final);
            assert_eq!((done.failed, written.stores.buffered()), (0, 0));
        }
        let in_turn = reopened(&dir, small, &lines, &in_memory);
        let shared = reopened(
            &dir,
            DiskOptions {
                scan_threads: 4,
                fan_rows: 0,
                ..small
            },
            &lines,
            &in_memory,
        );
        assert!(in_turn.stores.fan().is_none());
        let fan = shared.stores.fan().expect("the store has scan threads");
        for db in [&in_turn, &shared] {
            assert_eq!(db.stores.rows(), ROWS);
            assert_eq!(db.stores.buffered(), in_memory.len() as u64);
        }

        let column = |name: &str| GroupKey::Column(name.to_string());
        let hour = GroupKey::Time(Duration::from_secs(3_600));
        // The second day from its third hour: of each timed template's seven row groups
        // that day the first three are before it, and pruned, and four are read.
        let later = (Some(EPOCH + DAY + 2 * 3_600), Some(EPOCH + 2 * DAY));
        let one = |pattern: Vec<Match>, mode: QueryMode, window| {
            move |db: &db::Db| vec![executed(db, &asking(&pattern, mode.clone(), window))]
        };
        let paged = |pattern: Vec<Match>, window, limit: (Option<usize>, usize)| {
            move |db: &db::Db| pages(db, &pattern, window, limit, &executed)
        };
        type Asked<'a> = Box<dyn Fn(&db::Db) -> Vec<QueryResult> + 'a>;
        let questions: Vec<(&str, Asked<'_>)> = vec![
            (
                "a count",
                Box::new(one(vec![], QueryMode::Count, (None, None))),
            ),
            (
                "a count under a window that prunes",
                Box::new(one(vec![], QueryMode::Count, later)),
            ),
            (
                "a count by a term",
                Box::new(one(
                    vec![exact("level", "ERROR")],
                    QueryMode::Count,
                    (None, None),
                )),
            ),
            (
                "a count by a term under the window",
                Box::new(one(vec![exact("status", "200")], QueryMode::Count, later)),
            ),
            (
                "a fetch under the window, a row a page",
                Box::new(paged(vec![], later, (Some(1), 0))),
            ),
            (
                "a fetch, five rows a page",
                Box::new(paged(vec![], (None, None), (Some(5), 0))),
            ),
            (
                "a fetch at the server's page size",
                Box::new(paged(vec![], (None, None), (None, 40))),
            ),
            (
                "a fetch by a term, a row a page",
                Box::new(paged(
                    vec![exact("level", "WARN")],
                    (None, None),
                    (Some(1), 0),
                )),
            ),
            (
                "a fetch under the window, five rows a page",
                Box::new(paged(vec![any("n")], later, (Some(5), 0))),
            ),
            (
                "a fetch through the rows that tie, a row a page",
                Box::new(paged(vec![any("v")], (None, None), (Some(1), 0))),
            ),
            (
                "a fetch through the rows that tie, two rows a page",
                Box::new(paged(vec![any("v")], (None, None), (Some(2), 0))),
            ),
            (
                "a grouping by a column",
                Box::new(one(
                    vec![any("n")],
                    QueryMode::GroupBy(vec![column("level")]),
                    (None, None),
                )),
            ),
            (
                "a grouping by the hour and a column",
                Box::new(one(
                    vec![any("n")],
                    QueryMode::GroupBy(vec![hour, column("status")]),
                    (Some(EPOCH), Some(EPOCH + 2 * DAY)),
                )),
            ),
            (
                "whether a row exists, in a window that has one",
                Box::new(one(vec![], QueryMode::Exists, later)),
            ),
            (
                "whether a row exists, in a window that has none",
                Box::new(one(
                    vec![any("n")],
                    QueryMode::Exists,
                    (Some(EPOCH + 3 * DAY), Some(EPOCH + 4 * DAY)),
                )),
            ),
        ];

        let mut each_time = None;
        for (said, ask) in &questions {
            let first = ask(&in_turn);
            let before = fan.fanned();
            for time in 0..TIMES {
                let again = ask(&shared);
                assert_eq!(again.len(), first.len(), "{said}, time {time}: pages");
                for (n, (a, b)) in again.iter().zip(&first).enumerate() {
                    assert!(
                        whole(a) == whole(b),
                        "{said}, time {time}, page {n}:\n{:?}\nin turn:\n{:?}",
                        whole(a),
                        whole(b)
                    );
                }
                // The scans of one asking are shared out as often as of the next.
                let shared_out = fan.fanned() - before;
                assert_eq!(shared_out % (time + 1), 0, "{said}, time {time}");
                each_time = Some(shared_out / (time + 1));
            }
            let each_time = each_time.take().expect("asked at least once");
            // Not agreement between two scans in turn: runs of row groups were shared out for every
            // question but the count of every row and the two that stop at their first row.
            let in_turn_only = *said == "a count" || said.starts_with("whether");
            assert_eq!(each_time == 0, in_turn_only, "{said}: {each_time}");
            // And not agreement on nothing.
            match *said {
                "a count" => assert_eq!(first[0].count, ROWS),
                "a count under a window that prunes" => {
                    let meta = &first[0].meta;
                    assert!(
                        first[0].count > 10 && meta.row_groups_pruned > 10,
                        "{meta:?}"
                    );
                    assert!(meta.row_groups_read > 5, "{meta:?}");
                }
                "a fetch, five rows a page" => {
                    assert_eq!(
                        (first.len() as u64, first[0].count),
                        (ROWS.div_ceil(5), ROWS)
                    );
                }
                "a fetch under the window, a row a page" => {
                    assert_eq!(first.len() as u64, first[0].count);
                    assert!(first.len() > 20, "{} pages", first.len());
                }
                "a fetch through the rows that tie, a row a page" => {
                    // A cursor cannot stand between two rows of one `Rank`: a page that ends on one
                    // of a pair is followed by the page after both, so there are fewer pages than rows.
                    assert_eq!(first[0].count, 19);
                    assert!(first.len() < 19, "{} pages", first.len());
                    let tied = first
                        .iter()
                        .flat_map(|page| &page.rows)
                        .filter(|row| {
                            matches!(row.get("v"), Some(ColValue::String(v)) if v.starts_with("day "))
                        })
                        .count();
                    assert!((5..10).contains(&tied), "{tied} of the rows that tie");
                }
                "whether a row exists, in a window that has one" => {
                    assert_eq!((first[0].exists, first[0].count), (true, 1));
                }
                "whether a row exists, in a window that has none" => {
                    assert_eq!((first[0].exists, first[0].count), (true, 0));
                }
                _ => assert!(first[0].count > 0, "{said}: no row"),
            }
        }

        // The rows that tie were themselves read shared out, and are still told apart from rows
        // in their days: the scan asks about each position of the two days twice with no time.
        let leaves = plan(
            &shared.trie,
            &asking(&[any("v")], QueryMode::Fetch, (None, None)),
        )
        .leaves;
        let tied = &leaves[0];
        let opts = ScanOptions {
            ts_column: Some(tied.template.time_column.clone()),
            text_columns: tied.template.text_columns.clone(),
            ..ScanOptions::default()
        };
        let terms: Vec<Term> = tied.residual.iter().map(term).collect();
        assert!(!terms.is_empty());
        let mut newest = Newest::new(Some(2));
        let mut watched = Watched {
            fetch: Fetch {
                keep: &mut newest,
                key: leaf_key(&tied.template.path),
                ts: tied.template.time_column.as_str(),
                after: None,
            },
            asked: Vec::new(),
            handed: 0,
        };
        let before = fan.fanned();
        shared
            .stores
            .snapshot(&[tied.template_id], &opts)
            .visit(tied.template_id, &opts, &terms, &mut watched)
            .expect("the template has a leaf")
            .expect("scan");
        // Its newest day's three row groups, and each of the two days of rows that tie.
        assert_eq!(fan.fanned() - before, 3);
        let mut out_of_its_day: Vec<(Option<i64>, u64)> = watched
            .asked
            .iter()
            .filter(|(_, _, placed, _)| !placed)
            .map(|(ts, seq, ..)| (*ts, *seq))
            .collect();
        out_of_its_day.sort_unstable();
        let twice: Vec<(Option<i64>, u64)> = (0..5).flat_map(|seq| [(None, seq); 2]).collect();
        assert_eq!(out_of_its_day, twice);
    }

    // ── The plan read from the key index, held to the walk ────────────────────────────

    fn wants(key: &str, value: &str) -> Match {
        Match {
            key: key.to_string(),
            value: MatchValue::Exact(value.to_string()),
        }
    }

    fn has(key: &str) -> Match {
        Match {
            key: key.to_string(),
            value: MatchValue::Any,
        }
    }

    fn counting(terms: &[Match]) -> Query {
        asking(terms, QueryMode::Count, (None, None))
    }

    fn ids_of(plan: &Plan) -> Vec<u64> {
        plan.leaves.iter().map(|leaf| leaf.template_id).collect()
    }

    /// One line into a trie: the id of the template it went to, and whether it made it.
    fn taken(trie: &Trie, text: &str) -> Result<(u64, bool), kibbr_storage::trie::TrieError> {
        let parsed = kibbr_core::flatten::parse_line(text.as_bytes(), 1).expect("a valid line");
        let (leaf, is_new) = trie.ingest(&parsed, chrono::Utc::now())?;
        let id = leaf
            .leaf_meta
            .read()
            .expect("meta")
            .as_ref()
            .expect("a template")
            .template_id;
        Ok((id, is_new))
    }

    /// One pattern planned from the index and by the walk, and the two held equal: leaves,
    /// order, residual terms and the count of skipped root subtrees. Hands back the index's.
    fn both(trie: &Trie, terms: &[Match]) -> Plan {
        let q = counting(terms);
        let indexed = plan(trie, &q);
        let walked = plan_by_walk(trie, &q);
        assert_eq!(indexed.leaves, walked.leaves, "leaves of {:?}", q.tokens());
        assert_eq!(
            indexed.skipped,
            walked.skipped,
            "skipped of {:?}",
            q.tokens()
        );
        indexed
    }

    fn template_record(
        id: u64,
        tokens: Vec<kibbr_storage::catalog::Token>,
    ) -> kibbr_storage::catalog::Record {
        kibbr_storage::catalog::Record::Template {
            id,
            created_at: chrono::DateTime::from_timestamp(EPOCH, 0).expect("a valid timestamp"),
            tokens,
        }
    }

    fn column_record(
        id: u64,
        name: &str,
        col_type: Option<ColumnType>,
    ) -> kibbr_storage::catalog::Record {
        kibbr_storage::catalog::Record::Column {
            template_id: id,
            name: name.to_string(),
            col_type,
            widened_from: None,
            at: None,
        }
    }

    fn promoted_token_of(key: &str, value: &str) -> kibbr_storage::catalog::Token {
        kibbr_storage::catalog::Token::Promoted {
            promoted: key.to_string(),
            value: value.to_string(),
        }
    }

    fn replayed(records: &[kibbr_storage::catalog::Record]) -> Trie {
        Trie::from_catalog("data", records, &mut |_| {}).expect("the records replay")
    }

    /// A line holding `event` and the literal key its promoted token is spelled as: the path
    /// settles nothing, and neither the root filter nor the plan opens it for another value.
    #[test]
    fn a_template_under_a_root_edge_of_another_value_is_not_planned() {
        let trie = Trie::new();
        taken(&trie, r#"{"event":"login","event=login":1}"#).expect("a new template");

        let other = both(&trie, &[wants("event", "logout")]);
        assert!(other.leaves.is_empty(), "{other:?}");
        assert_eq!(other.skipped, 1);

        for term in [wants("event", "login"), has("event")] {
            let planned = both(&trie, std::slice::from_ref(&term));
            assert_eq!(ids_of(&planned), [101]);
            assert_eq!(planned.leaves[0].residual, [term]);
            assert_eq!(planned.skipped, 0);
        }

        // Beside a template that does hold the other value: that one and no more.
        taken(&trie, r#"{"event":"logout","user":7}"#).expect("a new template");
        let planned = both(&trie, &[wants("event", "logout")]);
        assert_eq!(ids_of(&planned), [102]);
        assert!(planned.leaves[0].residual.is_empty());
        assert_eq!(planned.skipped, 1);
    }

    /// The same leaf as a replay of an old catalog brings it back: one label, `event=login`,
    /// and the columns `event` and `event=login`.
    #[test]
    fn a_replayed_leaf_whose_label_is_its_column_is_not_planned_for_another_value() {
        let trie = replayed(&[
            template_record(101, vec![promoted_token_of("event", "login")]),
            column_record(101, "event", Some(ColumnType::String)),
            column_record(101, "event=login", Some(ColumnType::Int64)),
        ]);

        let other = both(&trie, &[wants("event", "logout")]);
        assert!(other.leaves.is_empty(), "{other:?}");
        assert_eq!(other.skipped, 1);

        let term = wants("event", "login");
        let planned = both(&trie, std::slice::from_ref(&term));
        assert_eq!(ids_of(&planned), [101]);
        assert_eq!(planned.leaves[0].residual, [term]);
    }

    /// A path no build writes and a replay accepts: two promoted tokens of one key. An exact
    /// term plans the template for neither value - the walk's answer, and the index's too.
    #[test]
    fn a_replayed_path_with_two_values_of_one_key_is_planned_for_neither() {
        let trie = replayed(&[
            template_record(
                101,
                vec![
                    promoted_token_of("event", "a"),
                    promoted_token_of("event", "b"),
                ],
            ),
            column_record(101, "event", Some(ColumnType::String)),
        ]);

        let second = both(&trie, &[wants("event", "b")]);
        assert!(second.leaves.is_empty(), "{second:?}");
        assert_eq!(second.skipped, 1);

        let first = both(&trie, &[wants("event", "a")]);
        assert!(first.leaves.is_empty(), "{first:?}");
        assert_eq!(first.skipped, 0);

        let any = both(&trie, &[has("event")]);
        assert_eq!(ids_of(&any), [101]);
        assert!(any.leaves[0].residual.is_empty());
    }

    /// A line refused after its nodes were made leaves a root edge with no template under it,
    /// which a walk counts as skipped - so the plan reads the count at the root.
    #[test]
    fn a_refused_lines_root_edge_is_counted_as_skipped() {
        let trie = Trie::new();
        taken(&trie, r#"{"event":"login","user":7}"#).expect("a new template");
        let before = both(&trie, &[wants("event", "login")]);
        assert_eq!((ids_of(&before), before.skipped), (vec![101], 0));
        let edits = trie.keys().edits();

        // Every id spent: the next new template has its path made and is then refused.
        trie.reserve_template_ids_through(kibbr_storage::trie::LAST_TEMPLATE_ID);
        let refused = taken(&trie, r#"{"event":"logout","user":7}"#);
        assert!(
            matches!(
                refused,
                Err(kibbr_storage::trie::TrieError::TemplateIdsSpent)
            ),
            "{refused:?}"
        );
        assert_eq!(
            trie.keys().edits(),
            edits,
            "a refused line reached the index"
        );

        let after = both(&trie, &[wants("event", "login")]);
        assert_eq!((ids_of(&after), after.skipped), (vec![101], 1));
        assert!(both(&trie, &[wants("event", "logout")]).leaves.is_empty());
        assert_eq!(trie.keys_agree(), Ok(()));
    }

    /// A column that widens to text is text in the next plan, and a plan made before goes
    /// on reading the template as it was when it was made.
    #[test]
    fn a_widened_column_is_text_in_the_next_plan_and_not_in_one_already_made() {
        let trie = Trie::new();
        taken(&trie, r#"{"event":"login","user":7}"#).expect("a new template");
        let before = both(&trie, &[]);
        assert_eq!(before.leaves[0].template.text_columns, ["event"]);

        // Int64 ⊔ String.
        let (id, is_new) = taken(&trie, r#"{"event":"login","user":"u1"}"#).expect("a widening");
        assert_eq!((id, is_new), (101, false));
        let after = both(&trie, &[]);
        assert_eq!(after.leaves[0].template.text_columns, ["event", "user"]);
        assert_eq!(before.leaves[0].template.text_columns, ["event"]);
    }

    /// A column added to a leaf that exists is a key its template is planned by from then on.
    /// The leaf is an old one whose label reads as a promoted `event` but is a literal key.
    #[test]
    fn a_column_added_to_a_leaf_is_a_key_its_template_is_planned_by() {
        let trie = replayed(&[
            template_record(101, vec![promoted_token_of("event", "login")]),
            column_record(101, "event=login", Some(ColumnType::Int64)),
        ]);
        assert!(both(&trie, &[has("event")]).leaves.is_empty());
        assert_eq!(ids_of(&both(&trie, &[has("event=login")])), [101]);

        let (id, is_new) = taken(&trie, r#"{"event":"login"}"#).expect("a line of that leaf");
        assert_eq!((id, is_new), (101, false));
        let term = has("event");
        let planned = both(&trie, std::slice::from_ref(&term));
        assert_eq!(ids_of(&planned), [101]);
        assert_eq!(planned.leaves[0].residual, [term]);
        assert_eq!(trie.keys_agree(), Ok(()));
    }

    /// A column added to a leaf can be the key its path holds a value for: the template is
    /// made again whole, so the path answers a term on the key from then on.
    #[test]
    fn a_column_added_to_a_leaf_can_be_one_its_path_settles() {
        let trie = replayed(&[
            template_record(101, vec![promoted_token_of("event", "ghost")]),
            column_record(101, "other", Some(ColumnType::Int64)),
        ]);
        assert!(both(&trie, &[wants("event", "ghost")]).leaves.is_empty());
        let before = both(&trie, &[]);
        assert_eq!(ids_of(&before), [101]);
        assert_eq!(before.leaves[0].template.promoted("event"), None);

        let (id, is_new) = taken(&trie, r#"{"event":"ghost"}"#).expect("a line of that leaf");
        assert_eq!((id, is_new), (101, false));
        assert_eq!(trie.keys_agree(), Ok(()));
        for term in [wants("event", "ghost"), has("event")] {
            let planned = both(&trie, std::slice::from_ref(&term));
            assert_eq!(ids_of(&planned), [101]);
            assert!(planned.leaves[0].residual.is_empty(), "{planned:?}");
            assert_eq!(planned.leaves[0].template.promoted("event"), Some("ghost"));
        }
        let other = both(&trie, &[wants("event", "login")]);
        assert!(other.leaves.is_empty(), "{other:?}");
        assert_eq!(other.skipped, 1);
        // A plan made before the line goes on reading the template as it was.
        assert_eq!(before.leaves[0].template.promoted("event"), None);
    }

    /// A line with no keys makes the root a leaf. It has no key to be found by, so the
    /// empty pattern plans it and no term does.
    #[test]
    fn the_root_as_a_leaf_is_planned_by_the_empty_pattern_and_by_no_term() {
        let trie = Trie::new();
        taken(&trie, "{}").expect("the root as a leaf");
        taken(&trie, r#"{"user":7}"#).expect("a new template");

        let all = both(&trie, &[]);
        assert_eq!(ids_of(&all), [101, 102]);
        assert!(all.leaves[0].template.path.is_empty());
        assert_eq!(ids_of(&both(&trie, &[has("user")])), [102]);
        assert_eq!(ids_of(&both(&trie, &[wants("user", "7")])), [102]);
        assert!(both(&trie, &[has("")]).leaves.is_empty());
        // An exact term refuses no root edge it could hang under: it has none.
        assert!(both(&trie, &[wants("event", "login")]).leaves.is_empty());
    }

    /// The shapes a pattern takes, on one small trie: several terms, one key twice, a wildcard,
    /// a key no template has, and a promoted key asked for a value no template holds.
    #[test]
    fn the_shapes_of_a_pattern_plan_as_the_walk_plans_them() {
        let trie = Trie::new();
        for text in [
            r#"{"event":"login","level":"error","user":7}"#,
            r#"{"event":"login","user":7}"#,
            r#"{"event":"logout","level":"warn"}"#,
            r#"{"level":"error","host":"a"}"#,
            r#"{"event":"an event too long to be promoted into a path","level":"error"}"#,
        ] {
            taken(&trie, text).expect("a new template");
        }

        // The shorter list is read and the other merged against it. The first's path settles
        // `event`; the last holds its `event` in its rows, so both terms are the scan's there.
        let terms = [wants("level", "error"), wants("event", "login")];
        let planned = both(&trie, &terms);
        assert_eq!(ids_of(&planned), [101, 105]);
        assert_eq!(planned.leaves[0].residual, [wants("level", "error")]);
        assert_eq!(planned.leaves[1].residual, terms);
        assert_eq!(planned.skipped, 1);

        // Unpromoted, so every template with the key and the term for the scan.
        let level = both(&trie, &[wants("level", "error")]);
        assert_eq!(ids_of(&level), [101, 103, 104, 105]);
        assert!(level.leaves.iter().all(|leaf| leaf.residual.len() == 1));
        assert_eq!(level.skipped, 0);

        // One key twice: both terms left, in the order written.
        let twice = [wants("level", "error"), wants("level", "warn")];
        let planned = both(&trie, &twice);
        assert_eq!(ids_of(&planned), [101, 103, 104, 105]);
        assert_eq!(planned.leaves[0].residual, twice);

        // A wildcard on a promoted key: settled by a path that holds it, the scan's where
        // the value was too long to promote.
        let any = both(&trie, &[has("event")]);
        assert_eq!(ids_of(&any), [101, 102, 103, 105]);
        assert!(any.leaves[..3].iter().all(|leaf| leaf.residual.is_empty()));
        assert_eq!(any.leaves[3].residual, [has("event")]);

        assert!(both(&trie, &[wants("nosuch", "1")]).leaves.is_empty());
        assert!(both(&trie, &[wants("level", "error"), has("nosuch")])
            .leaves
            .is_empty());

        // A promoted key and a value no path holds: only the template whose value is in
        // its rows and not in its path is left to be scanned.
        let none = both(&trie, &[wants("event", "no_such")]);
        assert_eq!(ids_of(&none), [105]);
        assert_eq!(none.skipped, 2);
    }

    /// What `event` is given in a generated line, as JSON: words that are promoted into the
    /// path, and values that are not.
    const EVENT_VALUES: [&str; 9] = [
        r#""login""#,
        r#""logout""#,
        r#""x""#,
        r#""pay""#,
        "5",
        r#""has space""#,
        r#""0123456789012345678901234567890123456789X""#,
        r#""""#,
        "null",
    ];

    /// One generated line, with the values its `event`s took added to `seen`. Key sets overlap,
    /// and some lines hold the literal key `event=<value>` the walk's two filters differ on.
    fn generated_line(rng: &mut Rng, seen: &mut Vec<String>) -> String {
        const NAMES: [&str; 14] = [
            "event",
            "event",
            "event",
            "a",
            "x",
            "lvl",
            "ts",
            "a=b",
            "event=x",
            "event=login",
            "event=",
            "a.b",
            r"k\\",
            "",
        ];
        const VALUES: [&str; 5] = ["7", r#""word""#, "null", "2.5", r#""2024-01-15T12:00:00Z""#];
        if rng.chance(4) {
            return "{}".to_string();
        }
        let mut used: Vec<String> = Vec::new();
        let mut members: Vec<String> = Vec::new();
        for _ in 0..1 + rng.below(4) {
            let name = *rng.pick(&NAMES);
            if used.iter().any(|have| have == name) {
                continue;
            }
            used.push(name.to_string());
            match name {
                "event" => {
                    let value = *rng.pick(&EVENT_VALUES);
                    members.push(format!(r#""event":{value}"#));
                    let bare = value.trim_matches('"');
                    seen.push(bare.to_string());
                    let literal = format!("event={bare}");
                    if rng.chance(30) && !used.contains(&literal) {
                        members.push(format!(r#""{literal}":1"#));
                        used.push(literal);
                    }
                }
                "a" | "x" => {
                    let value = *rng.pick(&EVENT_VALUES);
                    members.push(format!(r#""{name}":{{"event":{value}}}"#));
                    seen.push(value.trim_matches('"').to_string());
                }
                _ => members.push(format!(r#""{name}":{}"#, rng.pick(&VALUES))),
            }
        }
        format!("{{{}}}", members.join(","))
    }

    /// Catalog records for one to four templates, including what `ingest` never writes and a
    /// replay accepts: a label that is also a column, two promoted tokens of one key, and more.
    fn generated_records(rng: &mut Rng) -> Vec<kibbr_storage::catalog::Record> {
        use kibbr_storage::catalog::Token;
        const KEYS: [&str; 10] = [
            "event",
            "a.event",
            "lvl",
            "ts",
            "a=b",
            "event=x",
            "event=login",
            "k\\",
            "",
            "x",
        ];
        const VALUES: [&str; 6] = ["x", "login", "logout", "", "a=b", "b"];
        const TYPES: [Option<ColumnType>; 3] =
            [Some(ColumnType::String), Some(ColumnType::Int64), None];
        let mut out = Vec::new();
        for n in 0..1 + rng.below(4) {
            let id = if n > 0 && rng.chance(10) {
                100 + n
            } else {
                101 + n
            };
            let mut tokens = Vec::new();
            for _ in 0..rng.below(4) {
                if rng.chance(50) {
                    tokens.push(promoted_token_of(rng.pick(&KEYS), rng.pick(&VALUES)));
                } else {
                    tokens.push(Token::Key {
                        key: rng.pick(&KEYS).to_string(),
                    });
                }
            }
            let labels: Vec<String> = tokens.iter().map(Token::label).collect();
            out.push(template_record(id, tokens.clone()));
            for token in &tokens {
                // Mostly the column a line of this template would have; sometimes none.
                if !rng.chance(20) {
                    let name = match token {
                        Token::Promoted { promoted, .. } => promoted,
                        Token::Key { key } => key,
                    };
                    out.push(column_record(id, name, *rng.pick(&TYPES)));
                }
            }
            if rng.chance(33) && !labels.is_empty() {
                out.push(column_record(
                    id,
                    rng.pick(&labels),
                    Some(ColumnType::Int64),
                ));
            }
            if rng.chance(25) {
                out.push(column_record(id, rng.pick(&KEYS), Some(ColumnType::String)));
            }
        }
        out
    }

    /// A pattern of no term to three. Half the keys are columns the trie holds, and a value is
    /// often some *other* line's `event`, which finds a template under another root edge.
    fn generated_pattern(rng: &mut Rng, held: &[String], seen: &[String]) -> Vec<Match> {
        const KEYS: [&str; 18] = [
            "event",
            "event",
            "event",
            "event",
            "a.event",
            "x.event",
            "lvl",
            "ts",
            "a=b",
            "event=x",
            "event=login",
            "event=",
            "a\\.b",
            "k\\\\",
            "k\\",
            "",
            "nosuch",
            "a",
        ];
        const VALUES: [&str; 8] = [
            "x",
            "login",
            "logout",
            "",
            "a=b",
            "b",
            "7",
            "no-path-holds-this",
        ];
        let mut terms = Vec::new();
        for _ in 0..rng.below(4) {
            let key = if !held.is_empty() && rng.chance(50) {
                rng.pick(held).as_str()
            } else {
                *rng.pick(&KEYS)
            };
            terms.push(match rng.below(4) {
                0 => has(key),
                1 | 2 if !seen.is_empty() => wants(key, rng.pick(seen)),
                _ => wants(key, rng.pick(&VALUES)),
            });
            if rng.chance(10) {
                terms.push(wants(key, rng.pick(&VALUES)));
            }
        }
        terms
    }

    /// Generated tries, built every way a trie is built - ingest, replay, `trie.json`, and
    /// catalog records - with four generated patterns put to each after every line and record.
    fn over_generated_tries(name: &str, seeds: u64, mut ask: impl FnMut(&Trie, &Query)) {
        let now = chrono::Utc::now();
        let dir =
            std::env::temp_dir().join(format!("kibbr-engine-plans-{}-{name}", std::process::id()));
        let mut put = |trie: &Trie, rng: &mut Rng, seen: &[String]| {
            let mut held: Vec<String> = Vec::new();
            for leaf in trie.all_leaves() {
                let slot = leaf.leaf_meta.read().expect("meta");
                let columns = slot.iter().flat_map(|meta| &meta.columns);
                held.extend(columns.map(|column| column.name.clone()));
            }
            held.sort();
            held.dedup();
            for _ in 0..4 {
                ask(trie, &counting(&generated_pattern(rng, &held, seen)));
            }
        };
        for seed in 0..seeds {
            let mut rng = Rng::new(seed);
            let mut seen = Vec::new();

            let trie = Trie::new();
            for _ in 0..1 + rng.below(8) {
                let text = generated_line(&mut rng, &mut seen);
                let Ok(parsed) = kibbr_core::flatten::parse_line(text.as_bytes(), 1) else {
                    continue;
                };
                if trie.ingest(&parsed, now).is_err() {
                    continue;
                }
                put(&trie, &mut rng, &seen);
            }
            put(&replayed(&trie.catalog_records()), &mut rng, &seen);
            if seed % 4 == 0 {
                let path = dir.join("trie.json");
                kibbr_storage::triefile::save(&trie, &path).expect("the trie saves");
                let (loaded, error) = kibbr_storage::triefile::load_reporting(&path, "data");
                assert!(error.is_none(), "{error:?}");
                put(&loaded, &mut rng, &seen);
            }

            let records = generated_records(&mut rng);
            for upto in 1..=records.len() {
                put(&replayed(&records[..upto]), &mut rng, &seen);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The plan read from the key index is the plan the walk makes, on generated tries after
    /// every line and record, with the index itself held to the nodes at each.
    #[test]
    fn the_plan_read_from_the_index_is_the_walks_plan_on_generated_tries() {
        let (mut asked, mut chose, mut skipped, mut scanned) = (0u64, 0u64, 0u64, 0u64);
        over_generated_tries("same", 300, |trie, q| {
            assert_eq!(trie.keys_agree(), Ok(()));
            let indexed = plan(trie, q);
            let walked = plan_by_walk(trie, q);
            assert_eq!(indexed.leaves, walked.leaves, "leaves of {:?}", q.tokens());
            assert_eq!(
                indexed.skipped,
                walked.skipped,
                "skipped of {:?}",
                q.tokens()
            );
            asked += 1;
            chose += u64::from(!indexed.leaves.is_empty() && !q.matches.is_empty());
            skipped += u64::from(indexed.skipped > 0);
            scanned += u64::from(indexed.leaves.iter().any(|leaf| !leaf.residual.is_empty()));
        });
        // A generator whose patterns all miss compares empty plans and holds nothing.
        assert!(
            asked > 10_000 && chose > asked / 8 && skipped > asked / 16 && scanned > asked / 8,
            "{asked} plans: {chose} chose a template by a term, {skipped} skipped a root \
             subtree, {scanned} left a term to the scan"
        );
    }

    /// Holds the generator to making tries the walk's two filters differ on: the choice with
    /// the root filter left out has to differ from the walk somewhere.
    #[test]
    fn the_generated_tries_tell_a_plan_without_the_root_filter_from_the_walk() {
        let (mut asked, mut differs) = (0u64, 0u64);
        over_generated_tries("bites", 300, |trie, q| {
            let walked = plan_by_walk(trie, q).leaves;
            let unfiltered = chosen(&trie.keys(), q, &[]);
            asked += 1;
            if unfiltered != walked {
                differs += 1;
                // Never fewer templates and never another order: only more.
                let mut walked = walked.iter().peekable();
                for leaf in &unfiltered {
                    if walked.peek().is_some_and(|w| **w == *leaf) {
                        walked.next();
                    }
                }
                assert!(walked.next().is_none(), "{:?}", q.tokens());
            }
        });
        assert!(
            differs > 0,
            "{asked} plans and none tells the plan without the root filter from the walk"
        );
    }

    /// One thread ingests while two plan beside it, from the index and by the walk; the two
    /// are compared only on what the writer did not move, and both comparisons must be made.
    #[test]
    fn plans_made_beside_inserts_are_the_walks_plans() {
        use std::sync::atomic::{AtomicBool, AtomicU64};

        /// Tells the readers to stop when the writer is done, however it ends: a writer
        /// that panicked would otherwise leave them planning for ever.
        struct Done<'a>(&'a AtomicBool);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let trie = Trie::new();
        let done = AtomicBool::new(false);
        let (leaves_compared, skipped_compared) = (AtomicU64::new(0), AtomicU64::new(0));
        let root_edges = |trie: &Trie| trie.root().children.read().map_or(0, |edges| edges.len());
        let patterns = [vec![], vec![has("lvl")], vec![wants("event", "e0")]];

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let _done = Done(&done);
                let mut rng = Rng::new(21);
                for n in 0..600u64 {
                    // More values of `event` so new root edges keep coming, a small-set field so new
                    // templates come under old edges, and `n` sometimes a word so a template widens.
                    let event = rng.below(1 + n / 8);
                    let field = rng.below(6);
                    let number = if rng.chance(20) { r#""word""# } else { "7" };
                    let lvl = if rng.chance(50) {
                        r#","lvl":"warn""#
                    } else {
                        ""
                    };
                    let text = format!(r#"{{"event":"e{event}","f{field}":1,"n":{number}{lvl}}}"#);
                    let parsed =
                        kibbr_core::flatten::parse_line(text.as_bytes(), 1).expect("a valid line");
                    let (leaf, is_new) = trie
                        .ingest(&parsed, chrono::Utc::now())
                        .expect("the line is taken");
                    if is_new {
                        let id = leaf
                            .leaf_meta
                            .read()
                            .expect("meta")
                            .as_ref()
                            .expect("a template")
                            .template_id;
                        let mut asks = vec![Vec::new()];
                        asks.extend(parsed.keys.iter().map(|key| vec![has(key)]));
                        for terms in asks {
                            let planned = plan(&trie, &counting(&terms));
                            assert!(
                                ids_of(&planned).contains(&id),
                                "template {id} is not in the plan of {terms:?} made after \
                                 the line that made it"
                            );
                        }
                    }
                    if n % 32 == 31 {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            });
            for _ in 0..2 {
                scope.spawn(|| {
                    let mut all: Vec<u64> = Vec::new();
                    loop {
                        // Read before the pass, so the last pass is made with the writer
                        // gone and compares everything.
                        let finished = done.load(Ordering::Acquire);
                        for terms in &patterns {
                            let q = counting(terms);
                            // Two statements, the root first: the index's guard lives to the end of its
                            // statement, and no node's lock is waited for while it is held.
                            let edges = root_edges(&trie);
                            let edits = trie.keys().edits();
                            let indexed = plan(&trie, &q);
                            let walked = plan_by_walk(&trie, &q);
                            if trie.keys().edits() == edits {
                                assert_eq!(indexed.leaves, walked.leaves, "{:?}", q.tokens());
                                leaves_compared.fetch_add(1, Ordering::Relaxed);
                            }
                            if root_edges(&trie) == edges {
                                assert_eq!(indexed.skipped, walked.skipped, "{:?}", q.tokens());
                                skipped_compared.fetch_add(1, Ordering::Relaxed);
                            }
                            let ids = ids_of(&indexed);
                            assert!(
                                ids.windows(2).all(|pair| pair[0] < pair[1]),
                                "the ids of a plan do not ascend: {ids:?}"
                            );
                            if terms.is_empty() {
                                assert!(
                                    all.iter().all(|id| ids.binary_search(id).is_ok()),
                                    "a template planned before is not planned now"
                                );
                                all = ids;
                            }
                        }
                        if finished {
                            break;
                        }
                    }
                });
            }
        });

        assert!(leaves_compared.load(Ordering::Relaxed) > 0);
        assert!(skipped_compared.load(Ordering::Relaxed) > 0);
        assert_eq!(trie.keys_agree(), Ok(()));
        for terms in &patterns {
            both(&trie, terms);
        }
        let all = both(&trie, &[]);
        assert!(all.leaves.len() > 100, "{} templates", all.leaves.len());
        // The writer's exact term skips every root edge but its own.
        let exact = both(&trie, &[wants("event", "e0")]);
        assert_eq!(exact.skipped, root_edges(&trie) - 1);
        let mut rng = Rng::new(22);
        for _ in 0..200 {
            let terms = [
                wants("event", &format!("e{}", rng.below(80))),
                has(&format!("f{}", rng.below(7))),
            ];
            both(&trie, &terms[..1 + rng.below(2) as usize]);
        }
    }

    /// What a plan gives for one term on `key`: that value, or any where none is named.
    fn planned(trie: &Trie, key: &str, value: Option<&str>) -> Plan {
        let term = Match {
            key: key.to_string(),
            value: value.map_or(MatchValue::Any, |text| MatchValue::Exact(text.to_string())),
        };
        let query = Query::new(
            vec![term],
            QueryMode::Count,
            TimeRange::default(),
            None,
            None,
            UNIX_EPOCH,
            Limits::default(),
        );
        plan(trie, &query)
    }

    /// The trie's write rule, as a plan sees it: a template the catalog refused is not
    /// planned, and a refused widening leaves the plan the columns as they were.
    #[test]
    fn a_plan_holds_nothing_the_catalog_refused() {
        use kibbr_storage::catalog::{self, Catalog};
        use kibbr_storage::trie::TrieError;
        let dir = std::env::temp_dir().join(format!(
            "kibbr-engine-plans-{}-failed-append",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let parsed =
            |line: &str| kibbr_core::flatten::parse_line(line.as_bytes(), 1).expect("a valid line");
        let trie = Trie::new_in(&dir.display().to_string());
        let good = Catalog::open(&catalog::path_in(&dir), &mut |_| {}).expect("open");
        trie.attach_catalog(good.0);
        let now = chrono::Utc::now();
        trie.ingest(&parsed(r#"{"event":"login","user":7}"#), now)
            .expect("first line");

        // Swap in a catalog that cannot be written to.
        trie.attach_catalog(Catalog::read_only(&dir.join("dead.log")).expect("read-only"));

        // A new template: refused, and not planned.
        let refused = trie.ingest(&parsed(r#"{"event":"logout","user":7}"#), now);
        assert!(matches!(refused, Err(TrieError::Catalog(_))));
        let logout = planned(&trie, "event", Some("logout"));
        assert!(logout.leaves.is_empty(), "{logout:?}");
        assert_eq!(planned(&trie, "event", None).leaves.len(), 1);

        // A widening on the existing template (Int64 ⊔ String): refused, and a plan is
        // handed the columns as they were - `user` is not text.
        let widened = trie.ingest(&parsed(r#"{"event":"login","user":"u1"}"#), now);
        assert!(matches!(widened, Err(TrieError::Catalog(_))));
        let users = planned(&trie, "user", None);
        assert_eq!(users.leaves.len(), 1);
        assert_eq!(users.leaves[0].template.text_columns, ["event"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A poisoned key index still answers plans: a template taken through the poisoned lock
    /// is planned, and a widening of it shows in the next plan.
    #[test]
    fn a_plan_reads_through_a_poisoned_key_index() {
        let parsed =
            |line: &str| kibbr_core::flatten::parse_line(line.as_bytes(), 1).expect("a valid line");
        let trie = Trie::new();
        let now = chrono::Utc::now();
        trie.ingest(&parsed(r#"{"user":7}"#), now)
            .expect("a new template");
        trie.poison_keys();

        trie.ingest(&parsed(r#"{"host":"a","n":1}"#), now)
            .expect("a new template through a poisoned lock");
        let hosts = planned(&trie, "host", Some("a"));
        assert_eq!(hosts.leaves.len(), 1);
        assert_eq!(hosts.leaves[0].template_id, 102);
        assert_eq!(hosts.leaves[0].template.text_columns, ["host"]);

        trie.ingest(&parsed(r#"{"host":"a","n":"x"}"#), now)
            .expect("a widening through a poisoned lock");
        let widened = planned(&trie, "n", None);
        assert_eq!(widened.leaves.len(), 1);
        assert_eq!(widened.leaves[0].template.text_columns, ["host", "n"]);
        assert_eq!(trie.keys_agree(), Ok(()));
    }
}
