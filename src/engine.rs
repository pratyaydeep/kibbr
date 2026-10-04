//! The query engine: a walk, a scan, and the result shape every surface above reports.
//!
//! The walk crosses the trie and touches no data. The scan opens only the leaves the walk
//! chose. The split is enforced by the types: `plan` cannot see a store, so `--explain`
//! cannot accidentally read a row.

use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use thiserror::Error;

use crate::config::Limits;
use crate::flatten::{promoted_token, split_token};
use crate::fmt::{col_text, commas, duration_text};
use crate::infer::infer;
use crate::store::{
    effective_ts, nanos_of, Predicate, Row, Rows, ScanOptions, StoreError, StoreManager, Visitor,
    RECEIVED_AT_COLUMN,
};
use crate::trie::{Node, Trie};
use crate::types::{ColValue, ColumnType};

/// The column a `time(...)` bucket is reported under, on every surface.
pub const BUCKET_COLUMN: &str = "bucket";

/// One key of a `GROUP BY`, as the clause named it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKey {
    Column(String),
    /// `time(<width>)` - the bucket width. At most one per clause: the zero-fill below
    /// has one time axis, and two of them would silently produce nested buckets.
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

/// The `time(...)` width and the column keys, split out of what the clause named.
///
/// The two are read separately everywhere below - one decides the time axis, the others
/// the label - while `Vec<GroupKey>` keeps the order they were written in, which is the
/// order every surface prints its columns.
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

/// One group of a `GROUP BY`, and the rows in it.
///
/// A struct rather than an entry in a `HashMap<String, u64>`, because a map cannot say
/// "this bucket exists and is empty" - an absent key is an absent bucket, and a gap in a
/// chart and a zero in a chart mean different things to whoever is reading it at 3am. A
/// 6h range at 5m is 72 observations, some of which are zero.
///
/// It also retires the separator a map key needed. The label used to be the values joined
/// by a unit separator, split again three different ways by the three renderers; the
/// values are a list here and nothing joins them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// The bucket's start, in Unix nanoseconds, or `None` for rows that cannot be placed
    /// in time. `None` throughout when the clause named no `time(...)`.
    pub bucket: Option<i64>,
    /// The column values, in the order the keys were written.
    pub values: Vec<String>,
    pub count: u64,
}

#[derive(Debug, Default)]
pub struct QueryResult {
    /// `false` = no such template. Distinct from `exists: true, count: 0`, and
    /// every layer above depends on the difference.
    pub exists: bool,
    pub count: u64,
    pub rows: Vec<Row>,
    /// Ordered by bucket ascending - the unplaceable `None` first - then by values.
    /// Every surface prints them in the order they arrive, and `scripts/agree.sh` diffs
    /// the JSON as text, so the order is part of the answer.
    pub groups: Vec<Group>,
    pub meta: QueryMeta,
}

#[derive(Debug, Default)]
pub struct QueryMeta {
    pub templates_matched: usize,
    pub template_ids: Vec<u64>,
    /// **Segment files** read, not leaves. It counted leaves and reported 870 where
    /// 16,694 files were read; `leaves_opened` below took that meaning.
    /// A file whose every row group a window pruned is not in it, because nothing in it
    /// was read.
    pub files_opened: u64,
    /// Leaves with a store, opened for this query. What `files_opened` used to mean.
    pub leaves_opened: usize,
    /// Row groups whose chunks were read, and row groups dropped on their footer
    /// timestamps before any column data was. Together they are what the segment format
    /// buys: `SINCE 1h` over fifty row groups reads one.
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    /// Bytes of column chunk read off disk - where column pruning shows up.
    pub bytes_read: u64,
    pub rows_scanned: u64,
    pub rows_returned: u64,
    /// The limit dropped rows that matched. `count` is the total that matched and
    /// `rows_returned` is how many survived the cap, so this is `rows_returned < count` -
    /// carried as its own field because every surface above has to say so, and a reader
    /// comparing two numbers is a reader who can miss it.
    pub truncated: bool,
    /// Who set the page size; `truncated` says whether it fired. Read together: only
    /// `backstop` *and* `truncated` means "narrow your query", and a caller that asked for
    /// ten rows and got ten needs telling nothing.
    pub limit_source: LimitSource,
    /// Hand this back as `AFTER` to get the next page, or `None` because there is no next
    /// page. Present only when a `LIMIT` both applied and actually stopped short of the
    /// end - `rows_returned == limit` is not the same fact, and a cursor that yields an
    /// empty page is a cursor that lied.
    pub next_cursor: Option<String>,
    /// Walk plus scan. Loading the file is not query time - the CLI has to build the
    /// trie first because nothing persists it yet.
    pub duration: Duration,
}

/// Who set the cap on rows returned. Two fields, not one: see `QueryMeta::limit_source`.
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

/// A query, already built. Nothing here parses text: the flags build this directly and
/// `crate::trieql` builds the same struct from a query string, so the engine has one
/// input whichever surface asked.
#[derive(Debug)]
pub struct Query {
    pub matches: Vec<Match>,
    pub mode: QueryMode,
    pub time: TimeRange,
    /// `None` = every matching row. Resolved by `Query::new`, which drops a limit the mode
    /// cannot use and supplies `max_rows` where the caller named none.
    ///
    /// An unlimited `MATCH` materialises every matching row in the server before a byte of
    /// the response is written: a denial-of-service path on a shared daemon, reachable by
    /// accident from a terminal. The backstop is a *page size* and not a truncation - the
    /// answer says so and carries a cursor - which is what makes capping it an answer
    /// rather than a refusal.
    pub limit: Option<usize>,
    /// Who supplied `limit`.
    pub limit_source: LimitSource,
    /// Where the last page stopped, from `QueryMeta::next_cursor`. `None` = start at the
    /// newest row.
    pub cursor: Option<Cursor>,
    /// The instant the query was built against, carried for the same reason `now` is a
    /// parameter to `query::build` and `trieql::parse` rather than a call inside them: a
    /// caller that wants a reproducible answer has to be able to fix it.
    ///
    /// `GROUP BY time(...)` needs it. A window with no `until` still has a right-hand
    /// edge to zero-fill out to, and that edge is `now`. It is deliberately *not* folded
    /// into `Cursor::guard`, for the reason the window itself is not: two calls a
    /// millisecond apart would then refuse each other's cursors.
    pub now: SystemTime,
    /// What this query may not exceed. Carried rather than passed to `execute`, so the
    /// number the CLI was configured with is the number the engine reads.
    pub limits: Limits,
}

impl Query {
    /// The one constructor, so the flag surface and TrieQL resolve the limit identically.
    ///
    /// `limit` caps *rows returned*, not rows counted: `MATCH … LIMIT 10` answers with the
    /// newest ten and a `count` of every row that matched, as SQL's LIMIT bounds the result
    /// set and not `COUNT(*)`. `Count` and `GroupBy` return no rows, so a limit has nothing
    /// to cap there and is normalised away rather than quietly halving a total; `cursor`
    /// goes with it, since a mode returning no rows has no sequence to page. `--explain`
    /// prints what survived.
    ///
    /// An `Option`, not a 0-means-absent sentinel: `LIMIT 0` asks for no rows and has to
    /// stay distinguishable from asking for nothing. `ScanOptions.limit` keeps the sentinel
    /// - it is the storage interface, where "unlimited" is a real setting.
    ///
    /// `limits.max_rows` is the server's cap on an unlimited `Fetch`, in rows; `0` means
    /// none. The bucket caps beside it are read by `check_buckets`, not here.
    /// The caller's own limit stands above it and is not clamped; `COUNT`/`GROUP BY`
    /// return no rows for it to bound.
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
        }
    }

    /// A cursor of this query, at a rank this query produced. The one way to mint one -
    /// `Cursor::guard` is the query's, so a cursor cannot be built without a query to
    /// answer for it.
    pub fn cursor_at(&self, rank: Rank) -> Cursor {
        Cursor::new(rank, self)
    }
}

// ── Time buckets ───────────────────────────────────────────────────────────────────
//
// Boundaries sit on a fixed grid: multiples of the width counted from the **Unix epoch**,
// so 5-minute buckets always begin at :00, :05, :10 … :55 of every hour. The grid is a
// property of the clock and not of the query - which is the whole of `bucket_of` below,
// and the reason two overlapping queries agree on where the bars go.
//
// **A range emits every bucket it touches, including the two it only partly covers.** A
// row anywhere in the range has to land in some emitted bucket or it disappears from the
// chart while still being counted in `count`. So the bucket count is `range / width` only
// when both edges are already on the grid, and one more when they are not:
//
//     aligned - [10:00:00, 16:00:00) at 5m
//
//     grid    10:00   10:05   10:10    ...    15:50   15:55   16:00
//             ├───────┼───────┼─────────────────┼───────┼───────┤
//     range   ●───────────────────────────────────────────────○
//             ^since                                      ^until
//                                                → 72 buckets = 6h / 5m
//
//     unaligned - [10:03:17, 16:03:17), the same six hours
//
//     grid    10:00   10:05   10:10    ...    15:55   16:00   16:05
//             ├───────┼───────┼─────────────────┼───────┼───────┤
//     range       ●───────────────────────────────────────○
//                 ^since                              ^until
//                                                → 73 buckets
//
// A row at 10:03:20 is in range and its bucket starts at 10:00, before the range does; a
// row at 16:02:00 is in range and its bucket starts at 16:00, after it. Both buckets are
// real observations and both are emitted, so the ends straddle rather than land and the
// count goes up by one. `SINCE 6h` is unaligned nearly always - `now` is whatever instant
// it is - so this is the ordinary case rather than the exception. It is also where
// `SINCE 30d GROUP BY time(1s)` gets its 2,592,001 rather than 2,592,000.
//
// `ceil(range / width)` is therefore the exact count only for a range whose edges are
// already multiples of the width, which is what the property test in this file generates.
//
// `bucket_of` is the left edge of this and `bucket_after` the right; `window_span` puts
// the two together, and `zero_fill` fills in what the scan did not reach.

/// What a bucketed `GROUP BY` was refused for.
///
/// A refusal and not a truncation. A chart cut off partway through still looks like a
/// chart, and nothing on it says which half is missing; an error says so in one sentence.
/// Both numbers are named because only the pair is actionable - "too many" without the
/// permitted count leaves the writer guessing how much narrower to go.
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
// with thousands separators - `2,592,000 buckets` is the figure that makes the refusal
// land, and `2592000` is one an operator has to count digits on.
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

/// A bucket width in nanoseconds.
///
/// Saturating: `time(1000000000d)` is a width no `i64` of nanoseconds can hold, and one
/// bucket covering every representable instant is the honest answer to it rather than a
/// panic. `0` is unreachable - the parser rejects a zero width, which has no boundaries
/// to floor against.
fn width_nanos(width: Duration) -> i64 {
    i64::try_from(width.as_nanos()).unwrap_or(i64::MAX).max(1)
}

/// A timestamp's bucket start, in Unix nanoseconds.
///
/// The grid, and the section header above says why it is the epoch's and not the query's:
/// a chart that shifts its bars when you nudge the time range is a bug people do not
/// report clearly, and flooring against the query start is how it happens.
///
/// `div_euclid` floors toward negative infinity, so a pre-1970 timestamp lands in the
/// bucket that contains it rather than in the one after it.
///
/// The multiply saturates, because flooring `i64::MIN` to a width that does not divide it
/// lands *below* `i64::MIN` - there is no representable boundary there. That bottom bucket
/// is therefore short and off the grid, alone among all of them. It is the year 1677 and
/// reachable only by a timestamp column holding `i64::MIN` itself; containment still
/// holds, which is what the ordering and the sum depend on.
pub fn bucket_of(ns: i64, width: i64) -> i64 {
    ns.div_euclid(width).saturating_mul(width)
}

/// The first bucket start at or after `ns` - the exclusive end of a span covering
/// `[.., ns)`.
fn bucket_after(ns: i64, width: i64) -> i64 {
    bucket_of(ns.saturating_sub(1), width).saturating_add(width)
}

/// The half-open span of bucket starts a query's own window implies, `[lo, hi)`.
///
/// `None` where the window has no edge to read from: with no `SINCE` and no `BETWEEN`
/// there is no range to enumerate, and the span comes from the rows instead.
///
/// The right edge is `until`, or `now` where the window is open-ended - `SINCE 6h` means
/// the last six hours, and the sixth of them is a bucket like any other. `now` is not a
/// filter: a row whose clock ran ahead still counts and still gets a bucket, which is
/// what `span_with` is for.
fn window_span(query: &Query) -> Option<(i64, i64)> {
    let width = width_nanos(query.bucket_width()?);
    let since = query.time.since?;
    let lo = bucket_of(nanos_of(since), width);
    let end = nanos_of(query.time.until.unwrap_or(query.now));
    Some((lo, bucket_after(end, width).max(lo)))
}

/// The window's span widened to hold every bucket that actually received a row.
///
/// The widening is what keeps `sum(groups) == count` true. A row ahead of `now` by a
/// skewed clock, or any row at all when the window has no lower bound, would otherwise
/// fall in a bucket nothing emitted - counted in `count` and invisible in the chart.
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

/// How many buckets a span holds.
///
/// Rounded up. Both edges come out of `bucket_of`, so the span is a whole number of widths
/// and the rounding never fires - except against the saturated bottom bucket, where losing
/// the remainder would drop the rows in it out of the sum.
fn span_len(span: (i64, i64), width: i64) -> u64 {
    let (lo, hi) = span;
    u64::try_from(hi.saturating_sub(lo))
        .unwrap_or(0)
        .div_ceil(width.unsigned_abs().max(1))
}

/// The bucket count against `max_buckets`, before anything is read.
///
/// Called from `query::finish` on both surfaces, so `SINCE 30d GROUP BY time(1s)` is
/// refused without reading thirty days of rows, and called again inside `execute` for the
/// span a query with no lower bound derives from its rows.
pub fn check_buckets(query: &Query) -> Result<(), BucketError> {
    let Some(width) = query.bucket_width() else {
        return Ok(());
    };
    let Some(span) = window_span(query) else {
        return Ok(());
    };
    check_span(span, width, query.limits.max_buckets)
}

/// How many buckets this query's own window implies, for `--explain`. `None` where the
/// query is not bucketed, or where its window has no edge and the span comes from the
/// rows instead - which `--explain` cannot know, having read none.
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

/// The tally as the answer: every observed label combination given a count in every
/// bucket of the span, zero where nothing landed.
///
/// **Zeros, not gaps.** A bucket with no rows is a real observation - the thing was
/// quiet - and it is not the same statement as a bucket nobody asked about. The cross
/// product is over the label combinations that were *observed*, because a column's domain
/// is not something the engine can know: three levels seen means three series, each with
/// a point in all 72 buckets.
///
/// Rows that cannot be placed in time keep their count under `bucket: None`, so the
/// groups always sum to `count`. They are never zero-filled - there is one of them or
/// there is not.
fn zero_fill(
    tally: Tally,
    width: Option<Duration>,
    span: Option<(i64, i64)>,
    columns: usize,
    limits: Limits,
) -> Result<Vec<Group>, BucketError> {
    let Some(width) = width else {
        // No time axis: the groups are what was seen, and nothing is filled in. This is
        // the pre-existing `GROUP BY level`, whose row count is the column's domain and
        // not a product of anything.
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
    // and it is the bare time series - which is what makes `GROUP BY time(5m)` alone
    // produce 72 rows rather than none.
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

// ── The walk ───────────────────────────────────────────────────────────────────────

/// What the walk decided, and nothing else. No rows, no store handles.
#[derive(Debug, Default)]
pub struct Plan {
    pub leaves: Vec<PlannedLeaf>,
    /// Root subtrees the trie let us skip without looking inside.
    pub skipped: usize,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct PlannedLeaf {
    pub template_id: u64,
    /// The leaf's key set as walk tokens - what `ParsedLog::walk_tokens` produces for any
    /// line in it.
    ///
    /// Carried because it is the leaf's *content-derived* identity, where `template_id` is
    /// a counter handed out in the order templates were first seen. The result order needs
    /// a tiebreak for rows a timestamp cannot separate, and a tiebreak the reference
    /// scanner cannot recompute is one the two sides will disagree about - which they did,
    /// on every template whose time column is not called `ts`.
    pub path: Vec<String>,
    /// Collected by the walk; nothing opens one while the store is a `Vec<Row>`. It is
    /// what the scan will hand to the file layer once there is one.
    #[allow(dead_code)]
    pub storage_path: String,
    /// Terms the path could not settle - the scan's predicates. Empty means the leaf
    /// answers the pattern by its shape alone and every row in it matches.
    pub residual: Vec<Match>,
    /// Whether this template carries the column a time filter reads.
    ///
    /// The template's time column: what its rows are ordered, windowed and
    /// bucketed by, falling back to the system column on a row where it holds no
    /// instant. Every template has one, so every leaf has a time axis.
    pub time_column: String,
    /// Columns whose type is now `String`. A `Datetime` stored in one of them is rendered
    /// back to its ingested text by the store before the row is read.
    pub text_columns: Vec<String>,
}

/// Walk the trie, collect the leaves that could hold a matching row. Reads no data.
///
/// Cost is the trie's size, not the file's - the property `--explain` is timed on.
pub fn plan(trie: &Trie, query: &Query) -> Plan {
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
                path: path.clone(),
                storage_path: meta.storage_path.clone(),
                residual,
                time_column: meta.time_source.column().to_string(),
                text_columns: meta
                    .columns
                    .iter()
                    .filter(|c| c.col_type == Some(ColumnType::String))
                    .map(|c| c.name.clone())
                    .collect(),
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

/// Whether a root subtree can be skipped outright.
///
/// Promoted tokens are hoisted to the front of the path (`ParsedLog::walk_tokens`), so a
/// root label `event=login_ok` is the only thing an exact `--match event=…` can reach.
/// A label that starts with `KEY=` and is not the wanted token is another promoted value
/// of that key, and nothing else: the key is spelled as its token spells it, `=` escaped
/// (`flatten::promoted_token`), so the `=` after it is the bare one promotion wrote. It is
/// not a literal key named `KEY=…`, whose token has no bare `=`, and not a promoted value
/// that happens to begin with the rest of a longer key.
///
/// A pattern over non-promoted keys prunes nothing and costs one `starts_with` per root
/// child. Correctness never rests on this: `select` re-checks every leaf that survives.
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

/// The `key=value` tokens on this path, split back into pairs.
///
/// A promoted token has a bare `=` and a plain key's token has none
/// (`flatten::split_token`), so the label tells the two apart without consulting the
/// promotion rules, and the key it reads back is the column's name, unescaped.
fn promoted_of<'a>(
    path: &'a [String],
    columns: &BTreeSet<&str>,
) -> BTreeMap<Cow<'a, str>, &'a str> {
    let mut out = BTreeMap::new();
    for label in path {
        // A leaf made before `=` was escaped can hold a column named as one of its own
        // labels reads: the literal key `event=login` and the promoted `event=login` were
        // one edge then, and one leaf took the rows of both. The label does not speak
        // for every row of such a leaf, so the term is left to the scan.
        if columns.contains(label.as_str()) {
            continue;
        }
        if let Some((key, value)) = split_token(label) {
            if columns.contains(key.as_ref()) {
                out.insert(key, value);
            }
        }
    }
    out
}

// ── Result order ───────────────────────────────────────────────────────────────────

/// Where a row sits in the answer. Smaller is earlier.
///
/// Rows come back **newest first**: "the last 100 errors" is what a limit is nearly always
/// asked for, and an ascending order would hand back the oldest hundred instead. Leaving
/// this unspecified was not a neutral position - it made LIMIT meaningless.
///
/// `Reverse(ts)` puts the newest first and, because `None` is the least `Option`, puts a
/// row with no readable timestamp last. A template with no `ts` column at all has nothing
/// to sort by, so those rows fall back to **arrival order**: `seq` is where the row sits
/// among the rows it was stored beside. The same pair settles a tie between two rows
/// sharing a timestamp.
///
/// Both tiebreak components are facts about the *row*, not about the query that found it -
/// which is what lets a cursor built from one still name the same row later (see `Cursor`).
/// `leaf_key` is the leaf's key set hashed; `seq` is the ordinal its store gave it. The
/// order is total and computed from the data alone, so the differential compares the
/// answer exactly instead of as an unordered set.
///
/// **`seq` is only ever compared within a tie**, which is what lets a disk store define it
/// as the row's position in its day partition rather than a number it has to write down.
/// Equal timestamps are in the same day, so a tie group never spans two partitions; rows
/// that cannot be placed in time all tie, and all live in the one undated partition. The
/// in-memory store counts per leaf instead, so the two mint different cursor *bytes* for
/// one row - and the same row *order*, because within a partition the two counts run in
/// the same direction.
///
/// This order is also what lets a disk store skip whole partitions once the limit fills:
/// read newest-first, a day whose newest row is older than the worst row already held
/// cannot contain an answer. It does not license stopping mid-partition - rows inside a
/// day are in arrival order, not timestamp order - so `Newest` stays.
pub type Rank = (Reverse<Option<i64>>, u64, u64);

/// A leaf's place in the order, from its key set alone.
///
/// Shared with the oracle as `value_matches` is: which number a leaf gets is a contract
/// both sides must land on exactly, not a mechanism worth reimplementing.
///
/// It replaced the leaf's rank among the leaves *this query* matched, which was a fact
/// about the query and not the leaf: one new template matching the pattern renumbered every
/// leaf after it, and a cursor issued before that landed between rows. A key-set hash is
/// content-derived, so it is the same number in the next process and off disk.
///
/// A NUL ends each token, and a NUL *inside* a token is hashed as `0xFF`, a byte no UTF-8
/// text holds. A key may contain a NUL - `{"a\u0000b": 1}` is a line - and hashed as it
/// stands that one key spelled the bytes of the two keys `a` and `b`, so two templates got
/// one key by construction and not by a 64-bit accident. Their rows of one instant then
/// carried one `Rank`, and a page that stopped on the first skipped the second for good:
/// the scan drops every row at or before the cursor's rank. The same defect as the one
/// `Cursor::spelling` describes, in the other hash a cursor carries.
///
/// Escaped rather than length-prefixed so that a path with no NUL in it - every path there
/// has been but those - keeps the key it had, and with it the order of its tied rows. The
/// key of a path that does hold one moved, in the build that changed the guard's spelling
/// too: every cursor issued before it is refused by the guard, so none carrying an old key
/// is resolved against the new order.
///
/// Two paths colliding on 64 bits still give their tied rows one `Rank`. That is the
/// accident this accepts; what it may not do is hash the same bytes for two paths.
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

/// Where a page stopped: the `Rank` of its last row, plus a guard.
///
/// Keyset, not offset. A cursor names a *row*, so nothing ingested elsewhere can shift
/// it; `OFFSET n` names a position in a sequence that moves, so it re-serves rows when
/// anything lands at the head between two calls - and pays for the n rows it skips.
///
/// Nothing stores one. A cursor is a function of the last row handed out, so there is no
/// session and no expiry and the server may restart between two pages. That is the line
/// between `search_after`, which Kibana uses, and `scroll` or a Splunk job, which park a
/// frozen result set server-side.
///
/// **What it names, and what a restart does to it.** All three components are properties of
/// the row - its timestamp, its leaf's key set hashed, the ordinal its store gave it - and
/// none is a position in a structure a restart rebuilds. So it resolves to the same row in
/// the next process as long as that row is still stored. On disk that ordinal is the row's
/// position inside its day partition, which is not written down anywhere because it does
/// not have to be: it is where the row sits in the file.
///
/// It still does not freeze the data. A row ingested ahead of the cursor between two pages
/// is not shown until you start again without one, as under `Cursor::guard`.
///
/// The guard is why `query` is in here. A cursor read against a different pattern is a
/// cursor into a different answer - it would resume from a row that answer never contained
/// and skip everything ranked above it, without complaining. Four bytes is the difference
/// between an error and a wrong answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    rank: Rank,
    query: u32,
}

/// Bumped when `Rank`'s components change meaning, and read before anything else in
/// `decode`: a cursor from an older layout would resolve against a layout it never
/// described and answer confidently with the wrong rows. Nothing reads a superseded version
/// and nothing translates one - this check is the whole compatibility story.
///
/// A change to how the guard is computed does not bump it. A cursor whose guard was
/// computed another way names the same row in the same order and is refused as
/// `WrongQuery`, which is a refusal and not a wrong page; the version is for the case the
/// guard cannot catch.
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
    /// The fingerprint a cursor is checked against: what was matched, and in what order.
    /// Anything that would renumber the leaves or reorder the rows is in here, and nothing
    /// that would not.
    ///
    /// The time window is deliberately *out*. It renumbers nothing - `select` chooses
    /// leaves from the pattern alone - and putting it in makes the common case impossible:
    /// `SINCE 1h` resolves against `now`, so two calls a millisecond apart produce two
    /// different windows and every cursor from a relative window would be refused by the
    /// query that issued it.
    ///
    /// What that costs, stated rather than hidden: pages are a moving view over live data.
    /// A row ingested between two pages lands ahead of the cursor and is not shown until
    /// you start again without one. Widening the window mid-page has the same shape. This
    /// is how `search_after` and Loki behave; freezing the window into the cursor instead
    /// would make it a snapshot, with the expiry and server-side state that implies.
    pub fn guard(query: &Query) -> u32 {
        let h = fnv(FNV_OFFSET, &Self::spelling(query));
        // Folded to 32 bits: this is a guard against a mistake, not against an attacker,
        // and the cursor is already long.
        ((h >> 32) ^ h) as u32
    }

    /// The bytes `guard` hashes: the pattern, then the name of the order.
    ///
    /// Written so that no two patterns spell the same bytes, because a hash of bytes two
    /// patterns share cannot refuse either's cursor at any width. The count of terms comes
    /// first; a key and an exact value are each eight bytes of length and then the text,
    /// so a `=` or a NUL inside one ends nothing; and a tag after the key says whether a
    /// value follows, so the wildcard is not the one-character value `*`, which the
    /// grammar keeps apart from it.
    ///
    /// `Query::tokens` is not this spelling and must not become it: it prints `key=*` for
    /// both of those and is what `--explain` shows a person. The guard hashed it once, and
    /// `{"a": "b", "c": "d"}` handed its cursor to `{"a": "b\u0000c=d"}`.
    ///
    /// Terms are spelled in the order they were written, as `tokens` has them, so a
    /// pattern with its terms the other way round is still another query to the guard.
    ///
    /// Visible to the crate so that `fuzzing::cursor` can ask, of two patterns sharing a
    /// guard, whether they were ever different to it.
    pub(crate) fn spelling(query: &Query) -> Vec<u8> {
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
        // The order is by the effective time, whatever a template calls its
        // column; the guard names that rather than a column, so a cursor survives a
        // template being created with another name between two pages.
        out.extend_from_slice(b"effective_time");
        out
    }

    /// The `Rank` a page stopped at. Read by the scan, and by the oracle, which reaches
    /// the same answer by sorting rather than by filtering as it goes.
    pub fn rank(&self) -> Rank {
        self.rank
    }

    fn new(rank: Rank, query: &Query) -> Self {
        Self {
            rank,
            query: Self::guard(query),
        }
    }

    /// Opaque on purpose. Nobody should be able to write one by hand, because anyone who
    /// can will depend on the layout that `CURSOR_VERSION` exists to change.
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
        // `ts` is an `Option`, and the absent case is a real one - a template with no `ts`
        // column pages like any other. The flag carries it; the eight bytes beside it are
        // then zero and mean nothing.
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

    /// The text back into a cursor. The guard is *not* checked here - the query it belongs
    /// to is not built yet at parse time - which is what `check` is for.
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

    /// Does this cursor belong to this query? Checked once, before the scan.
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

/// The best `keep` rows by `Rank`, held in `keep + 1` rows of memory - or all of them,
/// in order, when nothing was capped.
///
/// The bounded case is a max-heap, worst row on top, so one push and one pop per row keeps
/// the best `keep` and drops the rest as they arrive. Collecting every matching row into a
/// `Vec` and sorting afterwards would allocate exactly what the cap was asked to avoid.
///
/// One more than asked for, deliberately: `rows_returned == limit` does not mean there is
/// another page, and a `next_cursor` that yields an empty page is a cursor that lied. The
/// extra row is held only to answer that question and dropped before the answer leaves.
struct Newest {
    /// `None` = keep everything.
    keep: Option<usize>,
    heap: BinaryHeap<Ranked>,
}

impl Newest {
    fn new(keep: Option<usize>) -> Self {
        Self {
            keep,
            heap: BinaryHeap::new(),
        }
    }

    fn offer(&mut self, rank: Rank, row: Row) {
        self.heap.push(Ranked { rank, row });
        if let Some(keep) = self.keep {
            // One more than `keep` is held, counted down from a length the push above
            // made at least one and not up from `keep`: `LIMIT 18446744073709551615` is a
            // limit a client can write, and there is no one more than it.
            if self.heap.len() - 1 > keep {
                // The greatest `Rank` is the last row in result order, so this drops the
                // oldest candidate and never one that belongs in the answer.
                self.heap.pop();
            }
        }
    }

    /// The rank one more `offer` would drop, or `None` while there is still room.
    ///
    /// What a partition is measured against: a partition whose best possible row is worse
    /// than this one holds nothing that belongs in the answer. `None` while the heap is
    /// filling, because nothing can be ruled out until it is full - and `None` for an
    /// unlimited query, which has to read everything by definition.
    fn worst(&self) -> Option<Rank> {
        let keep = self.keep?;
        if self.heap.len() > keep {
            self.heap.peek().map(|ranked| ranked.rank)
        } else {
            None
        }
    }

    /// Newest first, and the `Rank` the next page should resume after.
    ///
    /// `into_sorted_vec` is ascending by `Ord`, and `Rank` is smallest-first. The overflow
    /// row - if the scan produced one - is the proof that a next page exists, and the row
    /// before it is where this one stopped.
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
/// the heap keep the best.
///
/// A struct rather than a closure because it answers `wants_partition` too, and that is
/// the whole reason `Visitor` is a trait.
struct Fetch<'a> {
    keep: &'a mut Newest,
    key: u64,
    ts: &'a str,
    after: Option<Rank>,
}

impl Visitor for Fetch<'_> {
    fn row(&mut self, row: &Row, seq: u64) {
        // `seq` comes from the store, so it is the row's own ordinal and not a count of
        // what this scan admitted. A row leaving a relative `SINCE` window between two
        // pages used to renumber every row behind it in its leaf.
        let rank = (Reverse(effective_ts(row, self.ts)), self.key, seq);
        // Served on an earlier page. `Rank` is a total order, so this is exact where a
        // `ts` comparison alone is not: rows sharing one nanosecond split across the page
        // boundary rather than being repeated by an inclusive bound or dropped by an
        // exclusive one.
        if self.after.is_some_and(|stop| rank <= stop) {
            return;
        }
        self.keep.offer(rank, row.clone());
    }

    /// The best `Rank` any row in this partition could carry, against the worst one
    /// already held.
    ///
    /// `seq: 0` is the floor for this leaf at this timestamp, so the tuple really is a
    /// lower bound and a partition rejected here cannot contain an answer. The undated
    /// partition reports `None`, and `Reverse(None)` is the greatest `Rank` there is - so
    /// it is skipped exactly when the heap is full of dated rows and read when the worst
    /// row held is itself undated. That rule is the existing ordering, not a special case.
    ///
    /// `Newest` is shared across the leaves of one query, so the cut tightens as the scan
    /// goes on rather than restarting at each leaf.
    fn wants_partition(&mut self, newest: Option<i64>) -> bool {
        let best = (Reverse(newest), self.key, 0u64);
        self.keep.worst().is_none_or(|worst| best < worst)
    }
}

// ── The scan ───────────────────────────────────────────────────────────────────────

/// Both halves.
///
/// `QueryMode::Exists` is two questions wearing one name, and which one was asked is
/// decided by the time clause:
///
/// - **No time clause** - "does this template exist at all?". Answered by the walk, and it
///   reads no data: `leaves_opened` and `rows_scanned` both stay 0, which is the mode's
///   whole claim.
/// - **With a time clause** - "is this firing *now*?". `--exists --since 5m` is a cheap
///   liveness check, not a contradiction, so it scans - and stops dead on the first row
///   inside the window. `count` is 0 or 1 and `rows_scanned` is what it actually cost.
///
/// Reading `rows_scanned` is how the two are told apart, so both paths report it honestly
/// rather than one of them reporting a plausible zero.
///
/// `exists` keeps its meaning throughout: the template was found. A template that exists
/// and is not firing is `exists: true, count: 0` - already distinct from `exists: false`,
/// and distinguished the same way.
/// Errors an execution can end on. A `StoreError` is the storage layer failing; a
/// `BucketError` is the query asking for more chart than anyone can read, which is the
/// client's to fix and not the server's.
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
    let exists_only = query.mode == QueryMode::Exists;
    let mut opts = query.scan_options();
    let started = Instant::now();
    // **One snapshot of every planned template**, and nothing read outside it: see
    // `Snapshot`. Planned again if a load published between the plan and the snapshot - its
    // new templates can have joined the catalog after the plan was made, and a snapshot
    // holding the load's rows of the planned templates but not of those would count part of
    // it. A publish is a load's commit, so this goes round again once per commit that lands
    // in that gap, and no more.
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
    let mut newest = Newest::new(query.limit);
    // Split once rather than per leaf: the width decides the time axis and the columns
    // decide the label, and neither changes between leaves.
    let (width, columns) = match &query.mode {
        QueryMode::GroupBy(keys) => split_keys(keys),
        _ => (None, Vec::new()),
    };
    let bucket_ns = width.map(width_nanos);
    // Every template has a time source, so a matched template is a time axis.
    // With no template matched there is no axis to fill in and the answer carries no
    // buckets - `exists: false`, the first of the two empty answers.
    let timed = !plan.leaves.is_empty();
    let mut tally = Tally::new();

    for leaf in &plan.leaves {
        let preds: Vec<Predicate> = leaf.residual.iter().map(predicate).collect();
        let ts = leaf.time_column.as_str();
        opts.ts_column = Some(leaf.time_column.clone());
        opts.text_columns = leaf.text_columns.clone();

        // **Column pruning**, without touching `Predicate`: every predicate here was
        // built from a `Match` two lines up, so its key is known even though a
        // `Box<dyn Fn(&Row)>` cannot be asked. Those keys, the group keys and the
        // timestamp column are all any mode but `Fetch` reads, so a store may skip the
        // rest. `Fetch` returns whole rows and names nothing - empty is every column.
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

        // The store's own limit stops the read once `limit` rows have matched. Not used
        // for `Fetch`, and a disk store does not change that: rows are in arrival order
        // *within* a partition as much as within a `Vec`, so stopping at the first `limit`
        // matches returns the oldest of them under an order that promises the newest.
        // `Newest` bounds the memory instead, and what partitioning buys is one level up -
        // whole days skipped before they are opened, through `wants_partition`.
        opts.limit = if exists_only { 1 } else { 0 };

        let id = leaf.template_id;
        let scan = match &query.mode {
            // One row is the whole answer, and `visit` stops on it.
            QueryMode::Exists | QueryMode::Count => {
                snapshot.visit(id, &opts, &preds, &mut Rows(|_: &Row, _| {}))
            }
            QueryMode::Fetch => {
                let mut fetch = Fetch {
                    keep: &mut newest,
                    key: leaf_key(&leaf.path),
                    ts,
                    after: query.cursor.map(|c| c.rank),
                };
                snapshot.visit(id, &opts, &preds, &mut fetch)
            }
            QueryMode::GroupBy(_) => {
                let (tally, columns) = (&mut tally, &columns);
                snapshot.visit(
                    id,
                    &opts,
                    &preds,
                    &mut Rows(|row: &Row, _| {
                        // `None` twice over: no `time(...)` was asked for, or one was and this
                        // row has no timestamp to place it by. The renderers tell the two
                        // apart from the width, which they already hold.
                        let bucket = bucket_ns
                            .and_then(|w| effective_ts(row, ts).map(|ns| bucket_of(ns, w)));
                        *tally
                            .entry((bucket, group_values(row, columns)))
                            .or_insert(0) += 1;
                    }),
                )
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
    // modes return and it is its own field. This used to be a second copy of `count`,
    // which was harmless while nothing could make the two differ - a limit can, and
    // `truncated` is only readable next to a `rows_returned` that means what it says.
    result.meta.rows_returned = result.rows.len() as u64;
    // The limit dropped rows that matched. Only `Fetch` can reach this: the other modes
    // return no rows, so `Query::new` normalised their limit away.
    //
    // Counted against `count`, which is every row matching the pattern and window on every
    // page - it is not reduced by the cursor. That is the point: page 7 of 483 still says
    // how many rows there are in total, so a limit is a statement about this response and
    // never about the answer.
    result.meta.truncated =
        query.mode == QueryMode::Fetch && result.meta.rows_returned < result.count;
    result.meta.limit_source = query.limit_source;
    // From before the plan: a plan made again for a publish that landed under it counts.
    result.meta.duration = started.elapsed();
    Ok(result)
}

/// One residual term as a scan predicate.
fn predicate(term: &Match) -> Predicate {
    let key = term.key.clone();
    let want = term.value.clone();
    Box::new(move |row: &Row| row.get(&key).is_some_and(|have| value_matches(&want, have)))
}

/// Does a stored value satisfy `KEY=VALUE`?
///
/// The contract, shared verbatim with the reference scanner - a differential test is only
/// worth running if both sides are answering the same question.
///
/// Two readings of the term's text, because a `Row` holds `ColValue` and not the raw line.
/// A `Datetime` column kept nanoseconds and threw the text away, so `ts=2024-01-15T10:30:00Z`
/// can only be matched by putting the flag through the same ladder the value went through.
/// The string reading comes first so a JSON string `"149.00"` is matched by its own text
/// rather than by the float it would infer to.
pub fn value_matches(want: &MatchValue, stored: &ColValue) -> bool {
    match want {
        MatchValue::Any => !matches!(stored, ColValue::Null),
        MatchValue::Exact(text) => {
            *stored == ColValue::String(text.clone()) || *stored == infer(text).value
        }
    }
}

/// A row's group label, one value per column key, in the order the keys were written.
///
/// Missing and null share one label: neither is a value, and splitting them would put two
/// labels on one absent column. Shared verbatim with the oracle, as `value_matches` is -
/// which label a row gets is a contract both sides must land on, not a mechanism worth
/// reimplementing. The bucket is *not* here, for the opposite reason: see the note at the
/// top of `reference::naive`.
pub fn group_values(row: &Row, keys: &[String]) -> Vec<String> {
    keys.iter()
        .map(|key| {
            row.get(key)
                .map_or_else(|| col_text(&ColValue::Null), col_text)
        })
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// The bucket arithmetic, which the CLI surface can only reach one query at a time.
///
/// The properties below hold for *any* range and width, so they are checked over hundreds
/// of them rather than over a corpus somebody thought of - a boundary-alignment bug does
/// not produce an error, it produces a plausible chart with the bars in the wrong places,
/// and no hand-written case is likely to be the one that notices.
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

    /// A leaf a build before the escape merged: the label `event=login` is also one of
    /// its columns, so some of its rows have no `event` at all. The path settles nothing
    /// there and the scan reads the column.
    #[test]
    fn a_label_that_is_also_a_column_settles_nothing() {
        let path = ["event=login".to_string()];
        let columns = BTreeSet::from(["event", "event=login"]);
        assert!(promoted_of(&path, &columns).is_empty());
    }
    use crate::config::Limits;
    use crate::db;
    use crate::rng::Rng;
    use crate::store::nanos_of;
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
            &crate::config::Schema::default(),
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

    /// A load that publishes between a query's plan and its snapshot can have added a
    /// template the plan never saw - its rows in the snapshot for the planned templates, and
    /// not for that one. The query sees the publish count move and plans again, so the new
    /// template is in the answer.
    #[test]
    fn a_publish_between_the_plan_and_the_snapshot_plans_again() {
        use crate::store::{
            LeafStore, MemoryStore, Predicate, Scan, ScanOptions, Snapshot, Store, StoreError,
            Visitor,
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
                _: &[Predicate],
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
                    let parsed = crate::flatten::parse_line(line, 1).expect("a line");
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

    /// The property, over randomised ranges and widths.
    ///
    /// Both endpoints are snapped to a multiple of the width, which is what makes
    /// `ceil(range / width)` the exact bucket count rather than an approximation of it -
    /// an unaligned range straddles a boundary at each end and touches one bucket more.
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

            // Contiguous multiples of the width, starting at the range's own edge. This
            // is the assertion an alignment bug fails: a floor against the query start
            // instead of the epoch still tiles the range, but not at the same places a
            // neighbouring query tiles it.
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

    /// Every template has a time axis: one with no time column is on arrival
    /// time, which is `now` and outside a window over 1970 - so the buckets are there,
    /// zero-filled across the window, and the count is zero.
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
    /// bucket their arrival falls in - no row is without a bucket any more.
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
                // The bucket contains its instant: it is at or below `at`, and the next
                // boundary is above it. Both ends of the range are the exceptions, where
                // the arithmetic saturates rather than leaving the representable range.
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

    /// Three pairs of patterns that `Query::tokens` cannot tell apart - joined by the NUL
    /// the guard once put between them, each pair is the same bytes - and that are two
    /// queries all the same. Each has to refuse a cursor of the other.
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

    /// The spelling reads back: every term's key, tag and value sit where the lengths
    /// before them say, and what is left is the order's name. An encoding that reads back
    /// one way is one that no second pattern shares.
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

    /// `Newest` at every limit around the number of rows offered, and at both ends of
    /// `usize`: the best `keep` rows, and a next page exactly when a row was left over
    /// and the page is not empty.
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

    /// Paths that are the same bytes once a NUL is put after each token, and are different
    /// key sets all the same: each pair is two leaves and has to be two keys. And a path
    /// with no NUL in it has the key it always had, so no stored row changed places.
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
            &crate::config::Schema::default(),
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
}
