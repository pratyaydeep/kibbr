// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr query`. Flags or TrieQL in, `QueryResult` out. Both ways in build the one
//! `Query`, which is what makes `--reference` and
//! `scripts/check/parser_equivalence.sh` honest: a differential compares answers, not
//! plumbing. Only the output flags - `--format`, `--explain`, `--reference` - stay flags
//! on both surfaces, because none of them changes the answer.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde_json::{Map, Value};

use crate::config::{Limits, Schema};
use crate::error::{CliError, QueryError};
// A TrieQL string into a `Query` and an answer into JSON are the server crate's, since
// `POST /query` is their other caller: here under the names this module always had.
use kibbr_server::query::{self as wire, canonical, row_json};
pub use kibbr_server::query::{result_json, result_json_until};
use kibbr_core::fmt::{col_text, commas, duration_text, millis, rfc3339, NULL_TEXT};
use kibbr_engine::engine::{
    self, Cursor, Group, GroupKey, LimitSource, Match, MatchValue, Plan, Query, QueryMode,
    QueryResult, TimeRange,
};
use kibbr_engine::trieql::{self, ago};
use kibbr_storage::db;
use kibbr_storage::store::system_time;

/// Blank columns between two cells of a rendered table.
const GUTTER: usize = 2;

/// Leaf ids `--explain` names before it starts counting instead.
const EXPLAIN_LEAVES: usize = 12;

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Table,
    Json,
    /// One JSON object per line and no envelope, so `kibbr query … | kibbr query -` works.
    /// `jsonl` is the same.
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
    /// `None` = no limit asked for, which is not the same request as `--limit 0`.
    pub limit: Option<usize>,
    pub after: Option<String>,
    /// The caps, already resolved by `config`.
    pub limits: Limits,
    /// The names a template's time column is chosen from, already resolved by `config`.
    pub schema: Schema,
}

pub fn run<R: BufRead, W: Write>(input: R, out: &mut W, args: &Args) -> Result<(), CliError> {
    // One instant for the query *and* for every line's arrival, read by the engine and
    // the oracle alike, which is what lets `scripts/check/agree.sh` compare their rows.
    let now = Utc::now();
    let schema = &args.schema;
    let query = build(args, now)?;

    // `--explain` executes nothing, so it never asks for rows; the plan line reports
    // `Plan::duration`, not the load.
    if args.explain {
        let db = db::load(input, false, now, schema)?;
        note(db.stats);
        let plan = engine::plan(&db.trie, &query);
        return explain(out, &query, &plan);
    }

    let result = if args.reference {
        let (lines, stats) = db::load_lines(input)?;
        note(stats);
        crate::reference::naive::run(&lines, &query, &schema.time_columns)?
    } else {
        let db = db::load(input, true, now, schema)?;
        note(db.stats);
        engine::execute(&db.trie, &db.stores, &query)?
    };

    match args.format {
        Format::Table => table(out, &query, &result),
        Format::Json => json(out, &query, &result),
        Format::Ndjson => ndjson(out, &query, &result),
    }
}

/// stderr, so it never lands in the JSON on stdout: the answer does not cover the lines
/// that could not be read, and an operator should not have to infer that from a row count.
fn note(stats: db::LoadStats) {
    if stats.rejected > 0 {
        eprintln!(
            "kibbr query: {} of {} lines rejected; the answer covers the other {}",
            commas(stats.rejected),
            commas(stats.read),
            commas(stats.ingested)
        );
    }
}

// ── Text and flags to a Query ──────────────────────────────────────────────────────

/// `now` is a parameter, not a call, so a caller can fix it and both surfaces take the
/// same one. Visible to the crate so `fuzzing::cursor` can hand `--after` a token.
pub(crate) fn build(args: &Args, now: DateTime<Utc>) -> Result<Query, CliError> {
    // TrieQL builds the same `Query` the flags do. Both routes end at `Query::new`, the
    // one place the limit's default and its per-mode meaning are decided.
    if let Some(text) = &args.trieql {
        return from_trieql(text, now, args.limits);
    }

    let matches = args
        .matches
        .iter()
        .map(|arg| parse_match(arg))
        .collect::<Result<Vec<_>, _>>()?;

    // clap keeps the four verbs mutually exclusive. `--count` and no verb at all land
    // together: counting is the cheapest thing a bare query can mean.
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

    finish(Query::new(
        matches,
        mode,
        time,
        args.limit,
        cursor(args.after.as_deref())?,
        system_time(now),
        args.limits,
    ))
}

/// A TrieQL string to a `Query`: the server crate's `from_trieql`, which is what
/// `POST /query` accepts, with what it refuses said as this command's own error.
pub fn from_trieql(text: &str, now: DateTime<Utc>, limits: Limits) -> Result<Query, CliError> {
    wire::from_trieql(text, now, limits).map_err(CliError::from)
}

/// The cursor text as a `Cursor`, before there is a `Query` to check it against.
fn cursor(text: Option<&str>) -> Result<Option<Cursor>, CliError> {
    wire::cursor(text).map_err(CliError::from)
}

/// The last thing `build` does on either surface: does this cursor belong to this query,
/// and may it bucket as it asks? The server crate's `finish`.
fn finish(query: Query) -> Result<Query, CliError> {
    wire::finish(query).map_err(CliError::from)
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

/// `--group-by KEY[,KEY]`, where a key may be `time(<duration>)`: a clause TrieQL can
/// say and the flags cannot is one `scripts/check/parser_equivalence.sh` stops covering.
fn group_keys(spec: &str) -> Result<Vec<GroupKey>, QueryError> {
    let bad = || QueryError::EmptyGroupKey {
        arg: spec.to_string(),
    };
    let mut keys: Vec<GroupKey> = Vec::new();
    for text in spec.split(',').map(str::trim) {
        if text.is_empty() {
            return Err(bad());
        }
        let key = match bucket_spec(text) {
            Some(width) => {
                let width = duration_of(width).filter(|w| !w.is_zero()).ok_or_else(|| {
                    QueryError::BadBucket {
                        arg: text.to_string(),
                    }
                })?;
                if keys.iter().any(|k| matches!(k, GroupKey::Time(_))) {
                    return Err(QueryError::TwoTimeBuckets {
                        arg: spec.to_string(),
                    });
                }
                GroupKey::Time(width)
            }
            None => GroupKey::Column(text.to_string()),
        };
        keys.push(key);
    }
    Ok(keys)
}

/// The duration inside `time(...)`, or `None` for a key that is not one. Case-insensitive
/// on the name, as every keyword is; the duration inside keeps its own rules.
fn bucket_spec(text: &str) -> Option<&str> {
    let rest = text.strip_suffix(')')?;
    let (name, width) = rest.split_once('(')?;
    name.trim().eq_ignore_ascii_case("time").then_some(width)
}

/// A duration: a number and one of s, m, h, d.
fn parse_duration(text: &str) -> Result<Duration, QueryError> {
    duration_of(text).ok_or_else(|| QueryError::BadDuration {
        arg: text.to_string(),
    })
}

/// The grammar's `duration` and the only reading of one in the crate, split by character
/// so that no flag value with a multibyte ending can make it panic.
pub(crate) fn duration_of(text: &str) -> Option<Duration> {
    let (at, unit) = text.char_indices().next_back()?;
    let n: u64 = text[..at].parse().ok()?;
    let secs = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3_600,
        'd' => 86_400,
        _ => return None,
    };
    Some(Duration::from_secs(n.saturating_mul(secs)))
}

fn parse_datetime(text: &str) -> Result<DateTime<Utc>, QueryError> {
    DateTime::parse_from_rfc3339(text)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| QueryError::BadDatetime {
            arg: text.to_string(),
        })
}

/// A window edge as RFC 3339 - the inverse of `system_time`. An edge chrono cannot
/// spell is named `out of range` rather than left to chrono's `From`, which panics.
fn edge_text(t: SystemTime) -> String {
    let stamp = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_secs())
            .ok()
            .map(|s| (s, d.subsec_nanos())),
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

/// The plan, and nothing executed. Each line names the step it reports rather than
/// numbering it: an operator wants what the query is about to do.
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
        "  walk     [{}]  → {}",
        tokens.join(", "),
        if ids.is_empty() {
            "no such template".to_string()
        } else {
            format!("leaves {}", ids.join(" "))
        }
    )
    .map_err(CliError::Write)?;

    if plan.leaves.is_empty() {
        writeln!(out, "  scan     nothing to scan").map_err(CliError::Write)?;
    } else {
        let residual: Vec<String> = plan
            .leaves
            .iter()
            .flat_map(|leaf| leaf.residual.iter())
            .map(term_text)
            .collect();
        writeln!(
            out,
            "  scan     {} {}, {}, {}",
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
    // The limit and the order belong on the plan: a limit that is `Query::new`'s default
    // is the one an operator most needs to see before the query runs.
    if let Some(width) = query.bucket_width() {
        writeln!(out, "  buckets  {}", bucket_text(query, width)).map_err(CliError::Write)?;
    }
    if query.mode == QueryMode::Fetch {
        writeln!(
            out,
            "  limit    {}, {}",
            match (query.limit, query.limit_source) {
                // Which of the two capped it is the thing an operator most needs off a
                // plan: one is theirs to change in the query, the other in the config.
                (Some(n), LimitSource::Backstop) => format!("{n} rows - the max_rows backstop"),
                (Some(n), _) => format!("{n} rows"),
                (None, _) => "every matching row".to_string(),
            },
            ORDER_TEXT
        )
        .map_err(CliError::Write)?;
        if query.cursor.is_some() {
            writeln!(out, "  after    resuming from a cursor of this query")
                .map_err(CliError::Write)?;
        }
    }
    writeln!(
        out,
        "  the walk took {} and read no rows; mode {}",
        millis(plan.duration),
        mode_text(&query.mode)
    )
    .map_err(CliError::Write)
}

/// `kibbr explain '<trieql>'`: the `Query` the text denotes and nothing else, since a
/// plan needs a trie and this command takes no file.
pub fn describe<W: Write>(out: &mut W, text: &str, limits: Limits) -> Result<(), CliError> {
    let now = Utc::now();
    let parsed = trieql::parse(text, now)?;
    let asked = parsed.limit;
    let paged = parsed.after.is_some();
    let query = Query::new(
        parsed.matches,
        parsed.mode,
        parsed.time,
        parsed.limit,
        // Not decoded against a `Query` here: `kibbr explain` takes no file, so there is
        // no answer for a cursor to be a page of.
        None,
        system_time(now),
        limits,
    );
    // The same refusal `query` makes: a plan for a query that will be refused is not the
    // answer an operator came for.
    engine::check_buckets(&query).map_err(engine::ExecError::from)?;
    writeln!(out, "query:").map_err(CliError::Write)?;
    writeln!(out, "  mode     {}", mode_text(&query.mode)).map_err(CliError::Write)?;
    writeln!(out, "  pattern  [{}]", query.tokens().join(", ")).map_err(CliError::Write)?;
    writeln!(out, "  window   {}", window_text(&query)).map_err(CliError::Write)?;
    writeln!(out, "  order    {}", ORDER_TEXT).map_err(CliError::Write)?;
    if let Some(width) = query.bucket_width() {
        writeln!(out, "  buckets  {}", bucket_text(&query, width)).map_err(CliError::Write)?;
    }
    writeln!(out, "  limit    {}", limit_text(&query, asked)).map_err(CliError::Write)?;
    if paged {
        writeln!(out, "  after    a cursor; resumes where its page stopped")
            .map_err(CliError::Write)?;
    }
    Ok(())
}

/// The fixed order, printed rather than assumed - it is what makes a limit mean anything,
/// and it is not in the grammar for a reader to have inferred from the query.
const ORDER_TEXT: &str = "ts descending (newest first); rows with no ts last, in insertion order";

/// The time axis, on the plan: where the bucket boundaries fall is what an operator
/// cannot see from the query text.
fn bucket_text(query: &Query, width: std::time::Duration) -> String {
    let w = duration_text(width);
    let count = match engine::planned_buckets(query) {
        Some(n) => format!("{} buckets of {w}", commas(n)),
        // No window edge to count from: the span comes from the rows, and this has read
        // none.
        None => format!("{w} buckets over whatever range the rows cover"),
    };
    let permitted = match query.limits.max_buckets {
        0 => "no limit".to_string(),
        n => format!("max {}", commas(n as u64)),
    };
    format!("{count}, aligned to {w} boundaries in UTC from the epoch, never from this query's start ({permitted})")
}

/// What the limit resolved to, and why - `Query::new` may have supplied it or dropped it,
/// and neither should happen where only the source can see it.
fn limit_text(query: &Query, asked: Option<usize>) -> String {
    match (query.limit, query.limit_source, asked) {
        (Some(n), LimitSource::Backstop, _) => {
            format!("{n} - the max_rows backstop; this query names none")
        }
        (Some(n), ..) => n.to_string(),
        (None, _, None) if query.mode == QueryMode::Fetch => {
            "none - every matching row".to_string()
        }
        (None, _, None) => "none (this verb returns no rows)".to_string(),
        (None, _, Some(n)) => format!("none - LIMIT {n} does not apply, this verb returns no rows"),
    }
}

/// What to add to a footer when the server did the cutting. Empty otherwise: a caller that
/// asked for ten rows and got ten needs telling nothing.
fn backstop_note(result: &QueryResult) -> &'static str {
    match result.meta.limit_source {
        LimitSource::Backstop => "; cut at the max_rows backstop, not by this query",
        _ => "",
    }
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
        // Two answers, because `--exists` asks two questions (see `engine::execute`): with
        // no window the walk opened nothing, with one a matching row was looked for.
        QueryMode::Exists => writeln!(
            out,
            "exists  true{}   (templates matched: {} → {}, rows scanned: {}, {})",
            if query.time.is_open() {
                String::new()
            } else {
                format!(
                    "   in window  {}",
                    if result.count > 0 { "yes" } else { "no" }
                )
            },
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
            // `count` is every row that matched and `rows_returned` is how many came back; a
            // limit must not print the smaller number as the answer.
            if result.meta.truncated {
                writeln!(
                    out,
                    "{} of {} rows   (newest first{}; {})",
                    commas(result.meta.rows_returned),
                    commas(result.count),
                    backstop_note(result),
                    summary(result)
                )
                .map_err(CliError::Write)?;
            } else {
                writeln!(out, "{} rows   ({})", commas(result.count), summary(result))
                    .map_err(CliError::Write)?;
            }
            // Its own line, and the last one: it is the only thing on this screen meant to
            // be copied, and a cursor buried in a summary is a cursor selected by hand.
            if let Some(c) = &result.meta.next_cursor {
                writeln!(out, "next page:  AFTER \"{c}\"").map_err(CliError::Write)?;
            }
        }
        QueryMode::GroupBy(keys) => {
            let mut grid: Vec<Vec<String>> = vec![keys
                .iter()
                .map(|key| key.name().to_string())
                .chain(std::iter::once("count".to_string()))
                .collect()];
            grid.extend(result.groups.iter().map(|group| group_cells(keys, group)));
            grid_write(out, &grid)?;
            writeln!(
                out,
                "{} groups, {} rows   ({})",
                commas(result.groups.len() as u64),
                commas(result.count),
                summary(result)
            )
            .map_err(CliError::Write)?;
        }
    }
    Ok(())
}

/// One compact line, keys sorted, so a differential script can diff two runs without a
/// JSON parser.
fn json<W: Write>(out: &mut W, query: &Query, result: &QueryResult) -> Result<(), CliError> {
    let body = result_json(query, result);
    writeln!(out, "{body}").map_err(CliError::Write)
}

/// One JSON object per line and nothing else on stdout, so it feeds back into
/// `kibbr query -`; what is not a record goes to stderr. Not a stream: every row is ranked
/// before the first is written (`scripts/measure/backstop_rss.sh`).
fn ndjson<W: Write>(out: &mut W, query: &Query, result: &QueryResult) -> Result<(), CliError> {
    if !result.exists {
        eprintln!(
            "kibbr query: no template matches [{}]",
            query.tokens().join(", ")
        );
        return Ok(());
    }

    // Every mode prints its own records rather than only `Fetch` printing anything: a
    // format flag that silently emits nothing for three of the four modes is a trap.
    match &query.mode {
        // Under a window the mode answers "is this firing?", so the record carries that.
        // Without one, a `matched: false` would read as "checked, and no".
        QueryMode::Exists if query.time.is_open() => {
            line(out, &serde_json::json!({ "exists": true }))?
        }
        QueryMode::Exists => line(
            out,
            &serde_json::json!({ "exists": true, "matched": result.count > 0 }),
        )?,
        QueryMode::Count => line(out, &serde_json::json!({ "count": result.count }))?,
        QueryMode::Fetch => {
            for row in canonical(result) {
                line(out, &row_json(row))?;
            }
        }
        QueryMode::GroupBy(keys) => {
            for group in &result.groups {
                // One flat record per group, its fields named by the keys in the order
                // they were asked for - the same cells `table` prints, given field names.
                let mut obj = Map::new();
                for (key, cell) in keys.iter().zip(group_cells(keys, group)) {
                    obj.insert(key.name().to_string(), bucket_json(key, group, cell));
                }
                obj.insert("count".to_string(), Value::from(group.count));
                line(out, &Value::Object(obj))?;
            }
        }
    }
    if let Some(c) = &result.meta.next_cursor {
        eprintln!(
            "kibbr query: more rows{}; next page: AFTER \"{c}\"",
            backstop_note(result)
        );
    }
    Ok(())
}

fn line<W: Write>(out: &mut W, value: &Value) -> Result<(), CliError> {
    writeln!(out, "{value}").map_err(CliError::Write)
}

/// One group as its cells, in the order the keys were written, with the count last. An
/// unplaceable row's bucket reads `(null)`: absence is not a value.
fn group_cells(keys: &[GroupKey], group: &Group) -> Vec<String> {
    let mut values = group.values.iter();
    let mut cells: Vec<String> = keys
        .iter()
        .map(|key| match key {
            GroupKey::Time(_) => group.bucket.map_or_else(|| NULL_TEXT.to_string(), rfc3339),
            GroupKey::Column(_) => values.next().cloned().unwrap_or_default(),
        })
        .collect();
    cells.push(commas(group.count));
    cells
}

/// An NDJSON cell. Every field is its rendered text except an unplaceable bucket, which
/// is `null` - a record in a stream is parsed, and `"(null)"` would come back a string.
fn bucket_json(key: &GroupKey, group: &Group, cell: String) -> Value {
    match key {
        GroupKey::Time(_) if group.bucket.is_none() => Value::Null,
        _ => Value::from(cell),
    }
}

fn render_rows<W: Write>(out: &mut W, rows: &[&kibbr_storage::store::Row]) -> Result<(), CliError> {
    if rows.is_empty() {
        return Ok(());
    }
    // The same presentation `row_json` gives: the system column under its query name.
    let shown: Vec<BTreeMap<String, String>> = rows
        .iter()
        .map(|row| {
            kibbr_storage::store::present(row)
                .into_iter()
                .map(|(name, key)| (name, cell_text(row, key)))
                .collect()
        })
        .collect();
    let mut columns: Vec<&String> = shown.iter().flat_map(|row| row.keys()).collect();
    columns.sort_unstable();
    columns.dedup();

    let mut grid = vec![columns.iter().map(|c| (*c).clone()).collect::<Vec<_>>()];
    for row in &shown {
        grid.push(
            columns
                .iter()
                // `-` is "this template has no such column", which is not the same fact
                // as `(null)` - a column the template has and this row left empty.
                .map(|c| row.get(*c).cloned().unwrap_or_else(|| "-".to_string()))
                .collect(),
        );
    }
    grid_write(out, &grid)
}

/// A value as the table shows it: as the line wrote it where the row records that, and
/// through `col_text` otherwise.
fn cell_text(row: &kibbr_storage::store::Row, key: &str) -> String {
    match row.written(key) {
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => row.get(key).map_or_else(String::new, col_text),
    }
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
    format!(
        "ts in [{}, {})",
        show(query.time.since),
        show(query.time.until)
    )
}

fn mode_text(mode: &QueryMode) -> String {
    match mode {
        QueryMode::Exists => "exists".to_string(),
        QueryMode::Count => "count".to_string(),
        QueryMode::Fetch => "fetch".to_string(),
        QueryMode::GroupBy(keys) => format!(
            "group by {}",
            keys.iter()
                .map(|key| match key {
                    GroupKey::Column(name) => name.clone(),
                    GroupKey::Time(w) => format!("time({})", duration_text(*w)),
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
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

/// The `query` arm of `main.rs`: the limits settled, the input opened, then `run`.
#[allow(clippy::too_many_arguments)]
pub fn cli<W: Write>(
    file: &crate::config::File,
    out: &mut W,
    input: &str,
    trieql: Option<String>,
    matches: Vec<String>,
    count: bool,
    fetch: bool,
    exists: bool,
    group_by: Option<String>,
    since: Option<String>,
    between: Option<Vec<String>>,
    format: Format,
    explain: bool,
    reference: bool,
    limit: Option<usize>,
    after: Option<String>,
    max_rows: Option<usize>,
    max_buckets: Option<usize>,
    max_groups: Option<usize>,
    time_columns: Option<Vec<String>>,
) -> Result<(), CliError> {
    let limits = crate::config::limits(file, max_rows, max_buckets, max_groups)?;
    let reader = super::open(input)?;
    let args = Args {
        trieql,
        matches,
        count,
        fetch,
        exists,
        group_by,
        since,
        between,
        format,
        explain,
        reference,
        limit,
        after,
        limits,
        schema: crate::config::schema(file, time_columns),
    };
    run(reader, out, &args)
}
