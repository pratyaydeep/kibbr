//! The oracle. Every line, a closure, a counter. Obviously correct, hopelessly slow,
//! never ships.
//!
//! It shares the *contract* with the engine - `value_matches` and how a `Row` is built
//! from a line - because a differential test in which the two sides answer different
//! questions finds nothing. It shares none of the *mechanism*: no trie, no walk order, no
//! leaves, no pruning, no plan, and its own arithmetic for the time window. Sharing that
//! comparison too was tried and reverted: it made `[since, until)` unfalsifiable, because
//! a mutation to the one rule moved both answers together.
//!
//! **The time source and the read-back of a widened date column are worked out here
//! independently, not taken from the engine.** An oracle that shares a rule shares its
//! bugs: the worst query bug this codebase has had - the time column was the literal
//! name `ts`, so 41.6% of measured rows were invisible to every time-range query -
//! passed every differential, because this file was written to the same "no `ts`, no
//! time" rule the engine was. So: a template's time source is decided by the *first
//! line of that template in file order*, from the configured list, by this file's own
//! reading of the rule; a row's effective time is worked out here; the system column is
//! added here; and a `Datetime` in a column whose joined type is `String` is rendered
//! back through its own rung here. None of it calls the engine or the store.

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
use crate::store::Row;
use crate::types::{ColValue, ColumnType};

/// The system column, spelled out here rather than imported: a backslash and an
/// underscore.
const RECEIVED_AT: &str = "\\_received_at";

/// Where a template gets its time from, by this file's own reading of the rule.
#[derive(Debug, Clone, PartialEq)]
enum Source {
    Column(String),
    Arrival,
}

/// The rule: on the line that creates the template, the first configured name that is
/// one of its keys and whose value the ladder called a `Datetime`. Otherwise arrival.
fn source_of(line: &ParsedLog, time_columns: &[String]) -> Source {
    for name in time_columns {
        if !line.keys.contains(name) {
            continue;
        }
        let is_datetime = line
            .typed_values
            .get(name)
            .is_some_and(|t| t.ty == Some(ColumnType::Datetime));
        if is_datetime {
            return Source::Column(name.clone());
        }
    }
    Source::Arrival
}

/// The effective time of one line, and whether the row needs the system column.
///
/// The time column's value where the ladder read it as a `Datetime`; otherwise the
/// arrival time, which for the CLI is the instant the query was built against - every
/// line of the file arrived "now", the same `now` `db::load` stamps.
fn effective(line: &ParsedLog, source: &Source, arrival: i64) -> (i64, bool) {
    if let Source::Column(name) = source {
        if let Some(t) = line.typed_values.get(name) {
            if let (Some(ColumnType::Datetime), ColValue::Datetime(ns)) = (t.ty, &t.value) {
                return (*ns, false);
            }
        }
    }
    (arrival, true)
}

/// `engine::Rank`, spelled again rather than imported - see the note at the top.
type Rank = (Reverse<Option<i64>>, u64, u64);

pub fn run(
    lines: &[ParsedLog],
    query: &Query,
    time_columns: &[String],
) -> Result<QueryResult, ExecError> {
    let started = std::time::Instant::now();
    let arrival = nanos(query.now);

    // Per template, in file order: the first line decides the time source, and every
    // line folds its types into the column types - `types::join`, applied here by this
    // file's own reading of the rule, so a column whose join is `String` renders its
    // stored datetimes back as text. Both are facts about the template, settled before
    // any row is read.
    let mut sources: HashMap<u64, Source> = HashMap::new();
    let mut col_types: HashMap<(u64, String), Option<ColumnType>> = HashMap::new();
    for line in lines {
        sources
            .entry(line.template_id)
            .or_insert_with(|| source_of(line, time_columns));
        for key in &line.keys {
            let incoming = line.column_type(key);
            let slot = col_types
                .entry((line.template_id, key.clone()))
                .or_insert(None);
            *slot = crate::types::join(*slot, incoming);
        }
    }

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
    // Every template has a time source, so a matched template is a time axis.
    let timed = width.is_some() && lines.iter().any(&known);
    let mut tally: BTreeMap<(Option<i64>, Vec<String>), u64> = BTreeMap::new();

    for line in lines.iter().filter(|line| known(line)) {
        let source = sources.get(&line.template_id).unwrap_or(&Source::Arrival);
        let (at_ns, needs_system) = effective(line, source, arrival);
        let mut row = row_of(line);
        if needs_system {
            row.insert(RECEIVED_AT.to_string(), ColValue::Datetime(at_ns));
        }
        // A column that has widened to text: this line's own datetime, if the ladder
        // read one, comes back as the text it arrived as.
        for key in &line.keys {
            let joined = col_types
                .get(&(line.template_id, key.clone()))
                .copied()
                .flatten();
            if joined != Some(ColumnType::String) {
                continue;
            }
            // The line's own text, read straight off the line: a string's characters, a
            // number's digits.
            if let Some(ColValue::Datetime(_)) = row.get(key) {
                let text = line.values.get(key).cloned().unwrap_or_default();
                row.set_value(key, ColValue::String(text));
            }
        }
        let key = leaf_key(&line.walk_tokens());
        let n = seq.entry(line.template_id).or_insert(0);
        let at = *n;
        *n += 1;

        let hit = in_window(at_ns, query)
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
                let rank = (Reverse(Some(at_ns)), key, at);
                if query.cursor.is_none_or(|c| rank > c.rank()) {
                    ranked.push((rank, row));
                }
            }
            QueryMode::GroupBy(_) => {
                let bucket = width.filter(|_| timed).map(|w| floor_to(at_ns, w));
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
    // The oracle opens no file and reads every line it was handed, so the file count is
    // honestly zero. The leaf count is every template it matched.
    result.meta.files_opened = 0;
    result.meta.leaves_opened = result.meta.templates_matched;
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

/// An instant inside `[since, until)`, worked out from scratch. Every row has one now
/// now, so nothing here is outside every window by construction any more.
fn in_window(ns: i64, query: &Query) -> bool {
    if query.time.is_open() {
        return true;
    }
    let after = query.time.since.is_none_or(|t| ns >= nanos(t));
    let before = query.time.until.is_none_or(|t| ns < nanos(t));
    after && before
}

fn nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos()).map_or(i64::MIN, |n| -n),
    }
}
