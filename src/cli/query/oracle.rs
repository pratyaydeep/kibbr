// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The oracle: every line, a closure, a counter; obviously correct, never ships.
//! It shares the contract with the engine (`value_matches`, how a `Row` is built) and
//! none of the mechanism, or a mutation to one rule would move both answers together.
//! The time source, the forms only a time column reads, the window arithmetic and the
//! read-back of a widened date column are worked out here independently: an oracle that
//! shares a rule shares its bugs.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::ParsedLog;
use crate::engine::model::infer::in_epoch_window;
use crate::engine::model::types::{ColValue, ColumnType, DtForm};
use crate::engine::query::{
    group_values, leaf_key, split_keys, value_matches, ExecError, Group, MatchValue, Query,
    QueryMeta, QueryMode, QueryResult,
};
use crate::engine::{row_of, Row};

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
/// one of its keys and whose value is a time there - one the ladder called a `Datetime`,
/// or one in a form only a time column reads. Otherwise arrival.
fn source_of(line: &ParsedLog, time_columns: &[String]) -> Source {
    for name in time_columns {
        if line.keys.contains(name) && time_in(line, name).is_some() {
            return Source::Column(name.clone());
        }
    }
    Source::Arrival
}

/// The instant a line's value under a time column is: the ladder's `Datetime`, or the
/// line's own text in one of the time-column forms. A null and a promoted key are neither.
fn time_in(line: &ParsedLog, name: &str) -> Option<i64> {
    let typed = line.typed_values.get(name)?;
    match (typed.ty, &typed.value) {
        (Some(ColumnType::Datetime), ColValue::Datetime(ns)) => Some(*ns),
        (Some(_), _) => time_form(line.values.get(name)?),
        (None, _) => None,
    }
}

/// The three forms a time column reads beyond the ladder, by this file's own arithmetic:
/// seconds with a point and up to nine digits, sixteen or nineteen digits, and a date and
/// time with a named zone. Whole numbers throughout - a float would round the nanoseconds.
fn time_form(text: &str) -> Option<i64> {
    let count = |digits: &str| -> Option<i128> {
        let plain = !digits.is_empty()
            && !digits.starts_with('0')
            && digits.chars().all(|c| c.is_ascii_digit());
        if plain {
            digits.parse().ok()
        } else {
            None
        }
    };
    let fraction = |digits: &str| -> Option<i128> {
        if digits.is_empty() || digits.len() > 9 || !digits.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        // Padded on the right to nine digits: `.5` is five hundred million nanoseconds.
        format!("{digits:0<9}").parse().ok()
    };
    let in_window = |ns: i128| -> Option<i64> {
        if in_epoch_window(ns) {
            i64::try_from(ns).ok()
        } else {
            None
        }
    };

    // 1679711330.570420890
    if let Some((whole, part)) = text.split_once('.') {
        if whole.len() == 10 {
            if let (Some(secs), Some(sub)) = (count(whole), fraction(part)) {
                return in_window(secs * 1_000_000_000 + sub);
            }
        }
    }
    // 1679711330570420 and 1679711330570420890
    match text.len() {
        16 => return in_window(count(text)? * 1_000),
        19 => return in_window(count(text)?),
        _ => {}
    }
    // 2023-03-27 00:26:35.719 EDT
    let (stamp, zone) = text.rsplit_once(' ')?;
    let west_hours = match zone {
        "UTC" | "GMT" | "Z" => 0,
        "EDT" => 4,
        "EST" | "CDT" => 5,
        "CST" | "MDT" => 6,
        "MST" | "PDT" => 7,
        "PST" => 8,
        _ => return None,
    };
    let (clock, sub) = match stamp.split_once('.') {
        Some((clock, part)) => (clock, fraction(part)?),
        None => (stamp, 0),
    };
    let shaped = clock.len() == 19
        && clock
            .chars()
            .zip("dddd-dd-dd dd:dd:dd".chars())
            .all(|(c, want)| {
                if want == 'd' {
                    c.is_ascii_digit()
                } else {
                    c == want
                }
            });
    if !shaped {
        return None;
    }
    let local = chrono::NaiveDateTime::parse_from_str(clock, "%Y-%m-%d %H:%M:%S").ok()?;
    if chrono::Timelike::second(&local) > 59 || chrono::Timelike::nanosecond(&local) != 0 {
        return None;
    }
    let zone = chrono::FixedOffset::west_opt(west_hours * 3600)?;
    let at = local.and_local_timezone(zone).single()?;
    let ns = i128::from(at.timestamp()) * 1_000_000_000 + sub;
    i64::try_from(ns).ok()
}

/// The effective time of one line, and whether the row needs the system column.
/// Arrival is `query.now`: for the CLI every line of the file arrived at that instant.
fn effective(line: &ParsedLog, source: &Source, arrival: i64) -> (i64, bool) {
    if let Source::Column(name) = source {
        if let Some(ns) = time_in(line, name) {
            return (ns, false);
        }
    }
    (arrival, true)
}

/// Whether the ladder read a line's value as an instant from ten or thirteen digits.
fn is_count(line: &ParsedLog, key: &str) -> bool {
    line.typed_values
        .get(key)
        .is_some_and(|t| matches!(t.dt_form, Some(DtForm::EpochSecs | DtForm::EpochMillis)))
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

    // Facts about the template, settled before any row is read: the first line decides
    // the time source, and every line folds its types into the column types.
    let mut sources: HashMap<u64, Source> = HashMap::new();
    let mut col_types: HashMap<(u64, String), Option<ColumnType>> = HashMap::new();
    // The columns that have held a date written as RFC 3339 text, which no integer is.
    let mut dated_in_text: HashSet<(u64, String)> = HashSet::new();
    for line in lines {
        sources
            .entry(line.template_id)
            .or_insert_with(|| source_of(line, time_columns));
        for key in &line.keys {
            let incoming = line.column_type(key);
            let column = (line.template_id, key.clone());
            let counts = is_count(line, key);
            if incoming == Some(ColumnType::Datetime) && !counts {
                dated_in_text.insert(column.clone());
            }
            let all_counts = !dated_in_text.contains(&column);
            let slot = col_types.entry(column).or_insert(None);
            // The join, but digits read as an instant and an integer are one column of
            // integers: either way round, the column is `Int64` and not text - so long as
            // every date the column has held was digits.
            *slot = match (*slot, incoming) {
                (Some(ColumnType::Int64), Some(ColumnType::Datetime)) if counts => {
                    Some(ColumnType::Int64)
                }
                (Some(ColumnType::Datetime), Some(ColumnType::Int64)) if all_counts => {
                    Some(ColumnType::Int64)
                }
                (have, incoming) => crate::engine::model::types::join(have, incoming),
            };
        }
    }

    // Brute force: a template exists when some line has the pattern's keys and agrees with
    // it on the keys that line promoted into the fingerprint.
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
    // `--exists` with no window is the template question and reads no line; with a window
    // it has to look, and the loop below stops on the first row inside.
    let exists_only = query.mode == QueryMode::Exists;
    if exists_only && query.time.is_open() {
        result.meta.duration = started.elapsed();
        return Ok(result);
    }

    // `engine::Rank`'s tiebreak: `engine::leaf_key`, called because which number a key set
    // hashes to is contract, then file order over every line of the template.
    let mut seq: HashMap<u64, u64> = HashMap::new();
    // Each row kept with its own time: the instant `effective` read off this line, its time
    // column or its arrival, and never anything the engine computed.
    let mut ranked: Vec<(Rank, Row, i64)> = Vec::new();
    // The group keys, split the way the engine splits them: what the clause named is
    // contract, not mechanism.
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
        // read one, comes back as the text it arrived as. One that is now integers: a
        // datetime read from digits comes back as the integer those digits spell.
        let mut counted: HashSet<&str> = HashSet::new();
        for key in &line.keys {
            let joined = col_types
                .get(&(line.template_id, key.clone()))
                .copied()
                .flatten();
            let Some(ColValue::Datetime(_)) = row.get(key) else {
                continue;
            };
            // The line's own text, read straight off the line: a string's characters, a
            // number's digits.
            let text = line.values.get(key).cloned().unwrap_or_default();
            match joined {
                Some(ColumnType::String) => row.set_value(key, ColValue::String(text)),
                Some(ColumnType::Int64) if is_count(line, key) => {
                    if let Ok(count) = text.parse::<i64>() {
                        row.set_value(key, ColValue::Int64(count));
                        counted.insert(key.as_str());
                    }
                }
                _ => {}
            }
        }
        let key = leaf_key(&line.walk_tokens());
        let n = seq.entry(line.template_id).or_insert(0);
        let at = *n;
        *n += 1;

        let hit = in_window(at_ns, query)
            && query.matches.iter().all(|m| {
                row.get(&m.key).is_some_and(|have| {
                    value_matches(&m.value, have)
                        // A date shown as the count it arrived as is found by its digits,
                        // which the ladder reads as a date and `value_matches` as no integer.
                        || (counted.contains(m.key.as_str())
                            && matches!(&m.value, MatchValue::Exact(want)
                                if line.values.get(&m.key) == Some(want)))
                })
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
                    ranked.push((rank, row, at_ns));
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
        // Sort everything and take the front: an oracle that shared `engine::Newest`'s bounded
        // version would stop being an independent answer.
        ranked.sort_by_key(|(rank, _, _)| *rank);
        if let Some(n) = query.limit {
            if ranked.len() > n {
                // `LIMIT 0` has no last row to resume after and so no cursor, as the
                // engine answers it - and no row zero-minus-one to look for.
                result.meta.next_cursor = n
                    .checked_sub(1)
                    .and_then(|last| ranked.get(last))
                    .map(|(rank, _, _)| query.cursor_at(*rank).encode());
            }
            ranked.truncate(n);
        }
        // `times` beside `rows`, index for index. Every line has a time here: a line with no
        // time column takes its arrival, so no entry is `None`.
        (result.rows, result.times) = ranked
            .into_iter()
            .map(|(_, row, at_ns)| (row, Some(at_ns)))
            .unzip();
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

/// A timestamp's bucket start, written again rather than calling `engine::bucket_of`: a
/// mutation to one flooring rule must not move both answers together.
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

/// An instant inside `[since, until)`, worked out from scratch.
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
