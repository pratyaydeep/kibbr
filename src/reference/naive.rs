//! The oracle. Every line, a closure, a counter. Obviously correct, hopelessly slow,
//! never ships.
//!
//! It shares the *contract* with the engine - `value_matches` and how a `Row` is built
//! from a line - because a differential test in which the two sides answer different
//! questions finds nothing. It shares none of the *mechanism*: no trie, no walk order, no
//! leaves, no pruning, no plan, and its own arithmetic for the time window. Sharing that
//! comparison too was tried and reverted: it made `[since, until)` unfalsifiable, because
//! a mutation to the one rule moved both answers together.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::row_of;
use crate::engine::{
    group_values, leaf_key, split_keys, value_matches, ExecError, Group, MatchValue, Query,
    QueryMeta, QueryMode, QueryResult,
};
use crate::flatten::ParsedLog;
use crate::store::{Row, DEFAULT_TS_COLUMN};
use crate::types::ColValue;

/// `engine::Rank`, spelled again rather than imported - see the note at the top.
type Rank = (Reverse<Option<i64>>, u64, u64);

pub fn run(lines: &[ParsedLog], query: &Query) -> Result<QueryResult, ExecError> {
    let started = std::time::Instant::now();

    // Brute force: a template exists when some line has the pattern's keys and
    // agrees with it on the keys that line promoted into the fingerprint. Nothing about
    // rows, timestamps or the terms the trie cannot settle.
    let known = |line: &ParsedLog| {
        query.matches.iter().all(|m| {
            line.keys.contains(&m.key)
                && match (&m.value, line.promoted.contains(&m.key)) {
                    (MatchValue::Exact(want), true) => line.values.get(&m.key) == Some(want),
                    _ => true,
                }
        })
    };

    let templates: HashSet<u64> = lines
        .iter()
        .filter(|line| known(line))
        .map(|line| line.template_id)
        .collect();

    let mut result = QueryResult {
        exists: !templates.is_empty(),
        meta: QueryMeta {
            templates_matched: templates.len(),
            rows_scanned: lines.len() as u64,
            ..QueryMeta::default()
        },
        ..QueryResult::default()
    };
    // `--exists` with no window is the template question and reads no line, the same
    // short-circuit `engine::execute` takes. With a window it is the "is this firing?"
    // question and has to look, so it falls through to the loop below and stops on the
    // first row that lands inside.
    let exists_only = query.mode == QueryMode::Exists;
    if exists_only && query.time.is_open() {
        result.meta.duration = started.elapsed();
        return Ok(result);
    }

    // `engine::Rank`'s tiebreak. Two parts, both for rows a timestamp cannot separate:
    //
    // - the leaf's key, from `engine::leaf_key`. Called rather than reimplemented, unlike
    //   the window arithmetic: which number a key set hashes to is a contract, and two
    //   independent hashes agreeing on it would be luck rather than evidence.
    // - insertion order within a template, which is file order seen from this side and the
    //   ingest ordinal seen from the engine's. Counted over every line of the template, not
    //   over the ones that matched - the engine numbers rows when they land.
    let mut seq: HashMap<u64, u64> = HashMap::new();
    let mut ranked: Vec<(Rank, Row)> = Vec::new();
    // The group keys, split the way the engine splits them - which is contract, not
    // mechanism: both sides have to agree on what the clause named. Everything the bucket
    // arithmetic below does with the width is this file's own.
    let (width, columns) = match &query.mode {
        QueryMode::GroupBy(keys) => split_keys(keys),
        _ => (None, Vec::new()),
    };
    let timed = width.is_some()
        && lines
            .iter()
            .filter(|line| known(line))
            .any(|line| line.keys.iter().any(|k| k == DEFAULT_TS_COLUMN));
    let mut tally: BTreeMap<(Option<i64>, Vec<String>), u64> = BTreeMap::new();

    for line in lines.iter().filter(|line| known(line)) {
        let row = row_of(line);
        let key = leaf_key(&line.walk_tokens());
        let n = seq.entry(line.template_id).or_insert(0);
        let at = *n;
        *n += 1;

        let hit = in_window(&row, query)
            && query.matches.iter().all(|m| {
                row.get(&m.key)
                    .is_some_and(|have| value_matches(&m.value, have))
            });
        if !hit {
            continue;
        }
        result.count += 1;
        match &query.mode {
            QueryMode::Fetch => {
                // `count` is incremented above, before this: a row served on an earlier
                // page is still a row that matched, so the total is the same on every page.
                let rank = (Reverse(ts_of(&row)), key, at);
                if query.cursor.is_none_or(|c| rank > c.rank()) {
                    ranked.push((rank, row));
                }
            }
            QueryMode::GroupBy(_) => {
                let bucket = width
                    .filter(|_| timed)
                    .and_then(|w| ts_of(&row).map(|ns| floor_to(ns, w)));
                *tally
                    .entry((bucket, group_values(&row, &columns)))
                    .or_insert(0) += 1;
            }
            QueryMode::Count => {}
            QueryMode::Exists => break,
        }
    }

    if query.mode == QueryMode::Fetch {
        // Sort the whole thing and take the front, then look at what is left to know
        // whether there is another page. The oracle is allowed to be the hopelessly slow
        // one - `engine::Newest` is where the bounded version lives, and an oracle that
        // shared it would stop being an independent answer.
        ranked.sort_by_key(|(rank, _)| *rank);
        if let Some(n) = query.limit {
            if ranked.len() > n {
                result.meta.next_cursor = ranked
                    .get(n - 1)
                    .map(|(rank, _)| query.cursor_at(*rank).encode());
            }
            ranked.truncate(n);
        }
        result.rows = ranked.into_iter().map(|(_, row)| row).collect();
    }
    if matches!(query.mode, QueryMode::GroupBy(_)) {
        result.groups = fill(tally, width.filter(|_| timed), query, columns.len());
    }
    result.meta.rows_returned = result.rows.len() as u64;
    result.meta.truncated =
        query.mode == QueryMode::Fetch && result.meta.rows_returned < result.count;
    result.meta.limit_source = query.limit_source;
    result.meta.files_opened = result.meta.templates_matched;
    result.meta.duration = started.elapsed();
    Ok(result)
}

/// A timestamp's bucket start, worked out from scratch.
///
/// Written again rather than calling `engine::bucket_of`, for the reason `in_window` is:
/// where a boundary falls is exactly what `scripts/agree.sh` is checking, and a mutation
/// to one flooring rule must not move both answers together. Where the engine floors with
/// `div_euclid`, this counts whole widths from the epoch and steps back one for an instant
/// before it - the same rule, reached a different way.
fn floor_to(ns: i64, width: Duration) -> i64 {
    let w = i64::try_from(width.as_nanos()).unwrap_or(i64::MAX).max(1);
    let whole = ns / w;
    let start = if ns < 0 && ns % w != 0 {
        whole - 1
    } else {
        whole
    };
    start.saturating_mul(w)
}

/// The tally as groups, zero-filled across the span. The engine's `zero_fill` reaches the
/// same answer from a `HashMap` and a cap check; this one sorts and counts.
fn fill(
    tally: BTreeMap<(Option<i64>, Vec<String>), u64>,
    width: Option<Duration>,
    query: &Query,
    columns: usize,
) -> Vec<Group> {
    let Some(width) = width else {
        return tally
            .into_iter()
            .map(|((bucket, values), count)| Group {
                bucket,
                values,
                count,
            })
            .collect();
    };
    let w = i64::try_from(width.as_nanos()).unwrap_or(i64::MAX).max(1);

    let mut groups: Vec<Group> = tally
        .iter()
        .filter(|((bucket, _), _)| bucket.is_none())
        .map(|((_, values), count)| Group {
            bucket: None,
            values: values.clone(),
            count: *count,
        })
        .collect();

    // The span: the window's own edges where it has them, widened to hold every bucket a
    // row actually landed in.
    let mut edges: Option<(i64, i64)> = query.time.since.map(|since| {
        let lo = floor_to(nanos(since), width);
        let end = nanos(query.time.until.unwrap_or(query.now));
        let hi = floor_to(end.saturating_sub(1), width).saturating_add(w);
        (lo, hi.max(lo))
    });
    for (bucket, _) in tally.keys() {
        let Some(at) = bucket else { continue };
        edges = Some(match edges {
            Some((lo, hi)) => (lo.min(*at), hi.max(at.saturating_add(w))),
            None => (*at, at.saturating_add(w)),
        });
    }
    let Some((lo, hi)) = edges else {
        return groups;
    };

    let mut series: BTreeSet<Vec<String>> = tally
        .iter()
        .filter(|((bucket, _), _)| bucket.is_some())
        .map(|((_, values), _)| values.clone())
        .collect();
    if columns == 0 {
        series.insert(Vec::new());
    }

    let mut at = lo;
    while at < hi {
        for values in &series {
            groups.push(Group {
                bucket: Some(at),
                values: values.clone(),
                count: tally.get(&(Some(at), values.clone())).copied().unwrap_or(0),
            });
        }
        at = at.saturating_add(w);
    }
    groups
}

/// A row's timestamp, or `None`. Worked out here rather than shared with the engine, for
/// the reason at the top of this file - a mutation to the one reading must not move both
/// answers together.
fn ts_of(row: &Row) -> Option<i64> {
    match row.get(DEFAULT_TS_COLUMN) {
        Some(ColValue::Datetime(ns)) => Some(*ns),
        _ => None,
    }
}

/// `ts` inside `[since, until)`, worked out from scratch. A row with no readable
/// timestamp is outside every window.
fn in_window(row: &Row, query: &Query) -> bool {
    if query.time.is_open() {
        return true;
    }
    let Some(ColValue::Datetime(ns)) = row.get(DEFAULT_TS_COLUMN) else {
        return false;
    };
    let after = query.time.since.is_none_or(|t| *ns >= nanos(t));
    let before = query.time.until.is_none_or(|t| *ns < nanos(t));
    after && before
}

fn nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos()).map_or(i64::MIN, |n| -n),
    }
}
