//! `triedb query`. Flags or TrieQL in, `QueryResult` out.
//!
//! Two ways in and one `Query` out. The flags construct it directly; a TrieQL string goes
//! through `trieql` and arrives at the same struct, carrying nothing the flags could not
//! also say. That is what makes `--reference` and `scripts/parser_equivalence.sh` honest -
//! every path builds the one struct, so a differential compares answers, not plumbing.
//!
//! `--limit` and the output flags are not in the grammar and stay flags on both.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde_json::{Map, Value};

use crate::db;
use crate::engine::{
    self, Match, MatchValue, Plan, Query, QueryMode, QueryResult, TimeRange, GROUP_SEP,
};
use crate::error::{CliError, QueryError};
use crate::fmt::{col_text, commas, millis, rfc3339};
use crate::trieql;
use crate::types::ColValue;

/// Blank columns between two cells of a rendered table.
const GUTTER: usize = 2;

/// Leaf ids `--explain` names before it starts counting instead.
const EXPLAIN_LEAVES: usize = 12;

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Table,
    Json,
    /// One JSON object per line and no envelope: `triedb query … | triedb query -` is the
    /// point, so what comes out has to be what goes in. `jsonl` is the same thing under
    /// the name the file extension uses.
    #[value(alias = "jsonl")]
    Ndjson,
}

/// The flags, straight off the command line and not yet validated.
pub struct Args {
    /// Present or absent, never both: clap keeps it exclusive with the flags below, so
    /// the two surfaces cannot half-describe one query between them.
    pub trieql: Option<String>,
    pub matches: Vec<String>,
    pub count: bool,
    pub fetch: bool,
    pub exists: bool,
    pub group_by: Option<String>,
    pub since: Option<String>,
    pub between: Option<Vec<String>>,
    pub format: Format,
    pub explain: bool,
    pub reference: bool,
    pub limit: usize,
}

pub fn run<R: BufRead, W: Write>(input: R, out: &mut W, args: &Args) -> Result<(), CliError> {
    let query = build(args, Utc::now())?;

    // `--explain` executes nothing, so it never asks for rows. The load is still O(lines)
    // because there is no persisted trie yet - what is constant is `Plan::duration`, which
    // is what the plan line reports.
    if args.explain {
        let db = db::load(input, false)?;
        note(db.stats);
        let plan = engine::plan(&db.trie, &query);
        return explain(out, &query, &plan);
    }

    let result = if args.reference {
        let (lines, stats) = db::load_lines(input)?;
        note(stats);
        crate::reference::naive::run(&lines, &query)
    } else {
        let db = db::load(input, true)?;
        note(db.stats);
        engine::execute(&db.trie, &db.stores, &query)?
    };

    match args.format {
        Format::Table => table(out, &query, &result),
        Format::Json => json(out, &result),
        Format::Ndjson => ndjson(out, &query, &result),
    }
}

/// stderr, so it never lands in the JSON on stdout. The answer stands - it just does not
/// cover the lines that could not be read, and an operator should not have to infer that
/// from a row count.
fn note(stats: db::LoadStats) {
    if stats.rejected > 0 {
        eprintln!(
            "triedb query: {} of {} lines rejected; the answer covers the other {}",
            commas(stats.rejected),
            commas(stats.read),
            commas(stats.ingested)
        );
    }
}

// ── Text and flags to a Query ──────────────────────────────────────────────────────

/// `now` is a parameter, not a call: `--since` and `SINCE` are both relative to it, and a
/// caller that wants a reproducible answer needs to be able to fix it. Both surfaces take
/// the same one, which is what lets `scripts/parser_equivalence.sh` compare them at all.
fn build(args: &Args, now: DateTime<Utc>) -> Result<Query, CliError> {
    // TrieQL builds the same `Query` the flags do and then stops: `--limit` and the
    // output flags are not in the grammar.
    if let Some(text) = &args.trieql {
        let parsed = trieql::parse(text, now)?;
        return Ok(Query {
            matches: parsed.matches,
            mode: parsed.mode,
            time: parsed.time,
            limit: args.limit,
        });
    }

    let matches = args
        .matches
        .iter()
        .map(|arg| parse_match(arg))
        .collect::<Result<Vec<_>, _>>()?;

    // clap keeps the four verbs mutually exclusive, so the order here only settles which
    // arm reads which flag. `--count` and no verb at all land together: counting is the
    // cheapest thing a bare query can mean.
    let mode = match (&args.group_by, args.fetch, args.exists, args.count) {
        (Some(keys), ..) => QueryMode::GroupBy(group_keys(keys)?),
        (None, true, ..) => QueryMode::Fetch,
        (None, false, true, _) => QueryMode::Exists,
        (None, false, false, _) => QueryMode::Count,
    };

    let time = match (&args.since, &args.between) {
        (Some(d), _) => TimeRange {
            since: ago(system_time(now), parse_duration(d)?),
            until: None,
        },
        (None, Some(pair)) => {
            let from = parse_datetime(&pair[0])?;
            let to = parse_datetime(&pair[1])?;
            if from > to {
                return Err(QueryError::BackwardsRange {
                    from: pair[0].clone(),
                    to: pair[1].clone(),
                }
                .into());
            }
            TimeRange {
                since: Some(system_time(from)),
                until: Some(system_time(to)),
            }
        }
        (None, None) => TimeRange::default(),
    };

    Ok(Query {
        matches,
        mode,
        time,
        limit: args.limit,
    })
}

/// Split on the *first* `=`: a value may contain one, a key may not - `a=b=c` is the key
/// `a` matched against `b=c`.
fn parse_match(arg: &str) -> Result<Match, QueryError> {
    let (key, value) = arg.split_once('=').ok_or_else(|| QueryError::BadMatch {
        arg: arg.to_string(),
    })?;
    if key.is_empty() {
        return Err(QueryError::EmptyKey {
            arg: arg.to_string(),
        });
    }
    Ok(Match {
        key: key.to_string(),
        value: if value == "*" {
            MatchValue::Any
        } else {
            MatchValue::Exact(value.to_string())
        },
    })
}

fn group_keys(spec: &str) -> Result<Vec<String>, QueryError> {
    let keys: Vec<String> = spec.split(',').map(|k| k.trim().to_string()).collect();
    if keys.iter().any(String::is_empty) {
        return Err(QueryError::EmptyGroupKey {
            arg: spec.to_string(),
        });
    }
    Ok(keys)
}

/// A duration: a number and one of s, m, h, d.
fn parse_duration(text: &str) -> Result<Duration, QueryError> {
    let bad = || QueryError::BadDuration {
        arg: text.to_string(),
    };
    // By character, not by `len() - 1`: `len` is bytes and `split_at` panics on an index
    // inside a multibyte character. This is a flag value straight off the command line,
    // and no input may make this panic.
    let (at, unit) = text.char_indices().next_back().ok_or_else(bad)?;
    let n: u64 = text[..at].parse().map_err(|_| bad())?;
    let secs = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        _ => return Err(bad()),
    };
    Ok(Duration::from_secs(n.saturating_mul(secs)))
}

fn parse_datetime(text: &str) -> Result<DateTime<Utc>, QueryError> {
    DateTime::parse_from_rfc3339(text)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| QueryError::BadDatetime {
            arg: text.to_string(),
        })
}

/// `now - d`, or no lower bound at all when the subtraction leaves the representable
/// range: `SystemTime - Duration` panics on overflow, and `1000000000000d` is a duration
/// a user is entitled to write on either surface. A window reaching further back than
/// time can be expressed is an open one, which is what `None` already means.
pub(crate) fn ago(now: SystemTime, d: Duration) -> Option<SystemTime> {
    now.checked_sub(d)
}

/// Pre-1970 lands on the far side of the epoch rather than saturating at it.
pub(crate) fn system_time(dt: DateTime<Utc>) -> SystemTime {
    let secs = dt.timestamp();
    let nanos = dt.timestamp_subsec_nanos();
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH - Duration::new(secs.unsigned_abs(), 0) + Duration::from_nanos(nanos.into())
    }
}

/// A window edge as RFC 3339 - the inverse of `system_time`, and fallible where that one
/// is not.
///
/// Separate from `fmt::rfc3339`, which takes the `i64` nanoseconds a stored `Datetime`
/// always is and so cannot fail. A window edge is not stored and not bounded by that: it
/// comes off `--between` or out of `now - SINCE`, and reaches years no count of
/// nanoseconds can address.
///
/// `SystemTime` reaches far further than `DateTime<Utc>` does and chrono's `From` panics
/// rather than saying so - which `SINCE 1000000000000d` is enough to reach. A window edge
/// with no RFC 3339 spelling is still a window edge the engine can compare against, so it
/// is named rather than rejected.
fn edge_text(t: SystemTime) -> String {
    let stamp = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs()).ok().map(|s| (s, d.subsec_nanos())),
        // Below the epoch: chrono counts its nanoseconds forward from a whole second, so
        // a fractional second down here borrows one.
        Err(back) => {
            let d = back.duration();
            i64::try_from(d.as_secs())
                .ok()
                .and_then(|s| match d.subsec_nanos() {
                    0 => Some((-s, 0)),
                    n => (-s).checked_sub(1).map(|s| (s, 1_000_000_000 - n)),
                })
        }
    };
    stamp
        .and_then(|(secs, nanos)| DateTime::from_timestamp(secs, nanos))
        .map_or_else(
            || "out of range".to_string(),
            |dt: DateTime<Utc>| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        )
}

// ── Output ─────────────────────────────────────────────────────────────────────────

/// The plan, and nothing executed.
fn explain<W: Write>(out: &mut W, query: &Query, plan: &Plan) -> Result<(), CliError> {
    let tokens: Vec<String> = query.tokens().iter().map(|t| format!("{t:?}")).collect();
    // A wildcard over a wide dimension selects hundreds of leaves, and a plan nobody can
    // read is not a plan. The count is on the scan line either way.
    let mut ids: Vec<String> = plan
        .leaves
        .iter()
        .take(EXPLAIN_LEAVES)
        .map(|leaf| format!("#{}", leaf.template_id))
        .collect();
    if plan.leaves.len() > EXPLAIN_LEAVES {
        ids.push(format!("… {} more", plan.leaves.len() - EXPLAIN_LEAVES));
    }

    writeln!(out, "plan:").map_err(CliError::Write)?;
    writeln!(
        out,
        "  phase 1  walk [{}]  → {}",
        tokens.join(", "),
        if ids.is_empty() {
            "no such template".to_string()
        } else {
            format!("leaves {}", ids.join(" "))
        }
    )
    .map_err(CliError::Write)?;

    if plan.leaves.is_empty() {
        writeln!(out, "  phase 2  nothing to scan").map_err(CliError::Write)?;
    } else {
        let residual: Vec<String> = plan
            .leaves
            .iter()
            .flat_map(|leaf| leaf.residual.iter())
            .map(term_text)
            .collect();
        writeln!(
            out,
            "  phase 2  scan {} {}, {}, {}",
            plan.leaves.len(),
            plural(plan.leaves.len(), "leaf", "leaves"),
            window_text(query),
            if residual.is_empty() {
                "no predicate".to_string()
            } else {
                format!("predicate {}", dedup(residual).join(" "))
            }
        )
        .map_err(CliError::Write)?;
    }
    if plan.skipped > 0 {
        writeln!(
            out,
            "  pruned   {} root {} unopened",
            plan.skipped,
            plural(plan.skipped, "subtree", "subtrees")
        )
        .map_err(CliError::Write)?;
    }
    writeln!(
        out,
        "  phase 1 took {} and read no rows; mode {}",
        millis(plan.duration),
        mode_text(&query.mode)
    )
    .map_err(CliError::Write)
}

/// `triedb explain '<trieql>'` - what the parser made of the string, and nothing else.
///
/// Ideally this would print the execution plan, but a plan needs a trie and this command
/// takes no file: nothing persists a trie yet, so the only one that exists is the one
/// `triedb query <file>` builds for the length of a run. `query --explain` is the half
/// that plans; this one reports the `Query` the text denotes, which is the whole of what
/// the parser is answerable for. Persisting the trie would let this grow the plan without
/// the parser changing.
pub fn describe<W: Write>(out: &mut W, text: &str) -> Result<(), CliError> {
    let parsed = trieql::parse(text, Utc::now())?;
    let query = Query {
        matches: parsed.matches,
        mode: parsed.mode,
        time: parsed.time,
        limit: 0,
    };
    writeln!(out, "query:").map_err(CliError::Write)?;
    writeln!(out, "  mode     {}", mode_text(&query.mode)).map_err(CliError::Write)?;
    writeln!(out, "  pattern  [{}]", query.tokens().join(", ")).map_err(CliError::Write)?;
    writeln!(out, "  window   {}", window_text(&query)).map_err(CliError::Write)
}

fn table<W: Write>(out: &mut W, query: &Query, result: &QueryResult) -> Result<(), CliError> {
    // The distinction, in the one place an operator looks. "unknown template" is not a
    // count of zero, and the two lines do not resemble each other on purpose.
    if !result.exists {
        writeln!(
            out,
            "unknown template   no template matches [{}]   ({})",
            query.tokens().join(", "),
            summary(result)
        )
        .map_err(CliError::Write)?;
        return Ok(());
    }

    match &query.mode {
        // `rows scanned: 0` is the mode's whole point: the answer came off the trie walk
        // having opened nothing.
        QueryMode::Exists => writeln!(
            out,
            "exists  true   (templates matched: {} → {}, rows scanned: {}, {})",
            result.meta.templates_matched,
            ids_text(result),
            result.meta.rows_scanned,
            millis(result.meta.duration)
        )
        .map_err(CliError::Write)?,
        QueryMode::Count => writeln!(
            out,
            "count  {}   ({})",
            commas(result.count),
            summary(result)
        )
        .map_err(CliError::Write)?,
        QueryMode::Fetch => {
            let rows = canonical(result);
            render_rows(out, &rows)?;
            writeln!(out, "{} rows   ({})", commas(result.count), summary(result))
                .map_err(CliError::Write)?;
        }
        QueryMode::GroupBy(keys) => {
            let mut grid: Vec<Vec<String>> = vec![keys
                .iter()
                .cloned()
                .chain(std::iter::once("count".to_string()))
                .collect()];
            for (label, n) in sorted_groups(result) {
                let mut row: Vec<String> = label.split(GROUP_SEP).map(str::to_string).collect();
                row.push(commas(n));
                grid.push(row);
            }
            grid_write(out, &grid)?;
            writeln!(
                out,
                "{} groups, {} rows   ({})",
                commas(result.group_counts.len() as u64),
                commas(result.count),
                summary(result)
            )
            .map_err(CliError::Write)?;
        }
    }
    Ok(())
}

/// One compact line, so a differential script can diff two runs without a JSON parser.
///
/// Keys are sorted everywhere - `Row` and `group_counts` are `HashMap`s, and a result
/// that serialises in a different order each run is not comparable to anything.
fn json<W: Write>(out: &mut W, result: &QueryResult) -> Result<(), CliError> {
    let rows: Vec<Value> = canonical(result).into_iter().map(row_json).collect();

    let mut groups = Map::new();
    for (label, n) in sorted_groups(result) {
        groups.insert(label.replace(GROUP_SEP, " | "), Value::from(n));
    }

    let meta = &result.meta;
    let body = serde_json::json!({
        "exists": result.exists,
        "count": result.count,
        "rows": rows,
        "group_counts": Value::Object(groups),
        "meta": {
            "templates_matched": meta.templates_matched,
            "template_ids": meta.template_ids,
            "files_opened": meta.files_opened,
            "rows_scanned": meta.rows_scanned,
            "rows_returned": meta.rows_returned,
            "duration_ms": meta.duration.as_secs_f64() * 1000.0,
        }
    });
    writeln!(out, "{body}").map_err(CliError::Write)
}

/// One row as one JSON object. Shared with `ndjson`, so a row serialises identically
/// whether it is a line of its own or an element of `rows`.
fn row_json(row: &crate::store::Row) -> Value {
    let mut obj = Map::new();
    for (key, value) in row.iter().collect::<BTreeMap<_, _>>() {
        obj.insert(key.clone(), value_json(value));
    }
    Value::Object(obj)
}

/// A row's value as JSON.
///
/// `ColValue::to_json` everywhere except a `Datetime`, which is spelled RFC 3339 through
/// the one renderer in `fmt`. A row leaving here is a log line: it goes to something that
/// will read `ts` as a time, and it re-ingests through the type ladder as a `Datetime`
/// rather than as the 19-digit integer raw nanoseconds would be read as.
///
/// That leaves `parse --json`'s `typed_values` as the only surface still carrying the
/// machine instant, which is what it is for - see `fmt::rfc3339`.
fn value_json(value: &ColValue) -> Value {
    match value {
        ColValue::Datetime(ns) => Value::String(rfc3339(*ns)),
        other => other.to_json(),
    }
}

/// One JSON object per line and nothing else on stdout.
///
/// The envelope `--format json` prints - `exists`, `count`, `meta` - has no place in a
/// stream whose every line is a record, so none of it is here. The output feeds straight
/// back into `triedb query -`, and into anything else that reads NDJSON: a row goes
/// through `row_json`, so a `Datetime` leaves as RFC 3339 and comes back as one.
///
/// The two empty answers stay apart on stderr. "No such template" prints no lines and
/// says so there; "that template exists and nothing matched" prints no lines and says
/// nothing. Neither puts an odd-shaped object in the middle of a stream something
/// downstream is parsing row by row.
fn ndjson<W: Write>(out: &mut W, query: &Query, result: &QueryResult) -> Result<(), CliError> {
    if !result.exists {
        eprintln!(
            "triedb query: no template matches [{}]",
            query.tokens().join(", ")
        );
        return Ok(());
    }

    // Every mode prints its own records rather than only `Fetch` printing anything: a
    // format flag that silently emits nothing for three of the four modes is a trap.
    match &query.mode {
        QueryMode::Exists => line(out, &serde_json::json!({ "exists": true }))?,
        QueryMode::Count => line(out, &serde_json::json!({ "count": result.count }))?,
        QueryMode::Fetch => {
            for row in canonical(result) {
                line(out, &row_json(row))?;
            }
        }
        QueryMode::GroupBy(keys) => {
            for (label, n) in sorted_groups(result) {
                let mut obj = Map::new();
                // The label is the group's values joined by `GROUP_SEP`, in the order the
                // keys were asked for - the same split `table` does, given field names.
                for (key, value) in keys.iter().zip(label.split(GROUP_SEP)) {
                    obj.insert(key.clone(), Value::from(value));
                }
                obj.insert("count".to_string(), Value::from(n));
                line(out, &Value::Object(obj))?;
            }
        }
    }
    Ok(())
}

fn line<W: Write>(out: &mut W, value: &Value) -> Result<(), CliError> {
    writeln!(out, "{value}").map_err(CliError::Write)
}

/// Rows in a fixed order.
///
/// The engine returns them leaf by leaf and the oracle in file order; neither is wrong
/// and there is no ORDER BY to override it. Sorting by the rendered row is what lets the
/// two be compared at all, and it costs an ordering nothing was promising.
fn canonical(result: &QueryResult) -> Vec<&crate::store::Row> {
    let mut rows: Vec<&crate::store::Row> = result.rows.iter().collect();
    rows.sort_by_cached_key(|row| {
        row.iter()
            .collect::<BTreeMap<_, _>>()
            .iter()
            .map(|(k, v)| format!("{k}\u{1}{}", col_text(v)))
            .collect::<Vec<_>>()
            .join("\u{2}")
    });
    rows
}

fn sorted_groups(result: &QueryResult) -> BTreeMap<&String, u64> {
    result.group_counts.iter().map(|(k, v)| (k, *v)).collect()
}

fn render_rows<W: Write>(out: &mut W, rows: &[&crate::store::Row]) -> Result<(), CliError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut columns: Vec<&String> = rows.iter().flat_map(|row| row.keys()).collect();
    columns.sort_unstable();
    columns.dedup();

    let mut grid = vec![columns.iter().map(|c| (*c).clone()).collect::<Vec<_>>()];
    for row in rows {
        grid.push(
            columns
                .iter()
                // `-` is "this template has no such column", which is not the same fact
                // as `(null)` - a column the template has and this row left empty.
                .map(|c| row.get(*c).map_or_else(|| "-".to_string(), col_text))
                .collect(),
        );
    }
    grid_write(out, &grid)
}

/// Left-aligned columns, widths from the content. Row 0 is the header.
fn grid_write<W: Write>(out: &mut W, grid: &[Vec<String>]) -> Result<(), CliError> {
    let cols = grid.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..cols)
        .map(|i| {
            grid.iter()
                .filter_map(|row| row.get(i))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    for row in grid {
        let mut line = String::new();
        for (i, cell) in row.iter().enumerate() {
            let pad = widths[i].saturating_sub(cell.chars().count());
            line.push_str(cell);
            if i + 1 < row.len() {
                line.push_str(&" ".repeat(pad + GUTTER));
            }
        }
        writeln!(out, "{}", line.trim_end()).map_err(CliError::Write)?;
    }
    Ok(())
}

fn summary(result: &QueryResult) -> String {
    let meta = &result.meta;
    format!(
        "templates matched: {}, rows scanned: {}, {}",
        commas(meta.templates_matched as u64),
        commas(meta.rows_scanned),
        millis(meta.duration)
    )
}

fn ids_text(result: &QueryResult) -> String {
    result
        .meta
        .template_ids
        .iter()
        .map(|id| format!("#{id}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn term_text(term: &Match) -> String {
    match &term.value {
        MatchValue::Any => format!("{}=*", term.key),
        MatchValue::Exact(v) => format!("{}={v}", term.key),
    }
}

fn window_text(query: &Query) -> String {
    if query.time.is_open() {
        return "no time filter".to_string();
    }
    let show = |t: Option<SystemTime>| t.map_or("-".to_string(), edge_text);
    format!("ts in [{}, {})", show(query.time.since), show(query.time.until))
}

fn mode_text(mode: &QueryMode) -> String {
    match mode {
        QueryMode::Exists => "exists".to_string(),
        QueryMode::Count => "count".to_string(),
        QueryMode::Fetch => "fetch".to_string(),
        QueryMode::GroupBy(keys) => format!("group by {}", keys.join(", ")),
    }
}

fn plural(n: usize, one: &'static str, many: &'static str) -> &'static str {
    if n == 1 {
        one
    } else {
        many
    }
}

fn dedup(mut items: Vec<String>) -> Vec<String> {
    items.sort();
    items.dedup();
    items
}
