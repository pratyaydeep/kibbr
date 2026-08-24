//! The oracle. Every line, a closure, a counter. Obviously correct, hopelessly slow,
//! never ships.
//!
//! It shares the *contract* with the engine - `value_matches` and how a `Row` is built
//! from a line - because a differential test in which the two sides answer different
//! questions finds nothing. It shares none of the *mechanism*: no trie, no walk order, no
//! leaves, no pruning, no plan, and its own arithmetic for the time window. Sharing that
//! comparison too was tried and reverted: it made `[since, until)` unfalsifiable, because
//! a mutation to the one rule moved both answers together.

use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::row_of;
use crate::engine::{group_key, value_matches, MatchValue, Query, QueryMeta, QueryMode, QueryResult};
use crate::flatten::ParsedLog;
use crate::store::{Row, DEFAULT_TS_COLUMN};
use crate::types::ColValue;

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
    if query.mode == QueryMode::Exists {
        result.meta.duration = started.elapsed();
        return result;
    }

    for line in lines.iter().filter(|line| known(line)) {
        let row = row_of(line);
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
            QueryMode::Fetch => result.rows.push(row),
            QueryMode::GroupBy(keys) => {
                *result.group_counts.entry(group_key(&row, keys)).or_insert(0) += 1;
            }
            QueryMode::Count | QueryMode::Exists => {}
        }
        if query.limit != 0 && result.count as usize >= query.limit {
            break;
        }
    }

    result.meta.rows_returned = result.count;
    result.meta.files_opened = result.meta.templates_matched;
    result.meta.duration = started.elapsed();
    result
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
