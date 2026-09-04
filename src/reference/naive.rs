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
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::row_of;
use crate::engine::{
    group_key, leaf_key, value_matches, MatchValue, Query, QueryMeta, QueryMode, QueryResult,
};
use crate::flatten::ParsedLog;
use crate::store::{Row, DEFAULT_TS_COLUMN};
use crate::types::ColValue;

/// `engine::Rank`, spelled again rather than imported - see the note at the top.
type Rank = (Reverse<Option<i64>>, u64, u64);

pub fn run(lines: &[ParsedLog], query: &Query) -> QueryResult {
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
        return result;
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
            QueryMode::GroupBy(keys) => {
                *result.group_counts.entry(group_key(&row, keys)).or_insert(0) += 1;
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
    result.meta.rows_returned = result.rows.len() as u64;
    result.meta.truncated =
        query.mode == QueryMode::Fetch && result.meta.rows_returned < result.count;
    result.meta.limit_source = query.limit_source;
    result.meta.files_opened = result.meta.templates_matched;
    result.meta.duration = started.elapsed();
    result
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
