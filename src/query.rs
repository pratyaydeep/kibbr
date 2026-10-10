//! `kibbr query`. Flags or TrieQL in, `QueryResult` out.
//!
//! Two ways in and one `Query` out. The flags construct it directly; a TrieQL string goes
//! through `trieql` and arrives at the same struct, carrying nothing the flags could not
//! also say. That is what makes `--reference` and `scripts/parser_equivalence.sh` honest -
//! every path builds the one struct, so a differential compares answers, not plumbing.
//!
//! The grammar reaches every part of a `Query`: `LIMIT` and `AFTER` are in it, so `--limit`
//! and `--after` are exclusive with a TrieQL string the way `--since` already is. Only the
//! output flags - `--format`, `--explain`, `--reference` - stay flags on both surfaces,
//! because none of them changes the answer.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde_json::{Map, Value};

use crate::config::{Limits, Schema};
use crate::error::{CliError, QueryError};
use kibbr_core::fmt::{col_text, commas, duration_text, millis, rfc3339, NULL_TEXT};
use kibbr_engine::engine::{
    self, Cursor, Group, GroupKey, LimitSource, Match, MatchValue, Plan, Query, QueryMode,
    QueryResult, TimeRange, BUCKET_COLUMN,
};
use kibbr_engine::trieql::{self, ago};
use kibbr_storage::db;
use kibbr_storage::store::{system_time, Stop, STOP_EVERY_ROWS};

/// Blank columns between two cells of a rendered table.
const GUTTER: usize = 2;

/// Leaf ids `--explain` names before it starts counting instead.
const EXPLAIN_LEAVES: usize = 12;

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum Format {
    Table,
    Json,
    /// One JSON object per line and no envelope: `kibbr query … | kibbr query -` is the
    /// point, so what comes out has to be what goes in. `jsonl` is the same thing under
    /// the name the file extension uses.
    ///
    /// An encoding, and not a way to ask for more rows than fit in memory: the answer is
    /// ranked into a `Vec<Row>` before this writes a byte. See `fn ndjson`.
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
    // One instant for the query *and* for every line's arrival: a `SINCE` is
    // relative to it, and a row on arrival time sits at it. The engine and the oracle
    // read the same `now`, which is what lets `scripts/agree.sh` compare their rows.
    let now = Utc::now();
    let schema = &args.schema;
    let query = build(args, now)?;

    // `--explain` executes nothing, so it never asks for rows. The load is still O(lines)
    // because there is no persisted trie yet - what is constant is `Plan::duration`, which
    // is what the plan line reports.
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

/// stderr, so it never lands in the JSON on stdout. The answer stands - it just does not
/// cover the lines that could not be read, and an operator should not have to infer that
/// from a row count.
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

/// `now` is a parameter, not a call: `--since` and `SINCE` are both relative to it, and a
/// caller that wants a reproducible answer needs to be able to fix it. Both surfaces take
/// the same one, which is what lets `scripts/parser_equivalence.sh` compare them at all.
///
/// Visible to the crate so that `fuzzing::cursor` can hand `--after` a token at a fixed
/// `now`, without a file to load: `run` is the only other caller.
pub(crate) fn build(args: &Args, now: DateTime<Utc>) -> Result<Query, CliError> {
    // TrieQL builds the same `Query` the flags do and then stops: the output flags are not
    // in the grammar and do not change the answer. Both routes end at `Query::new`, which
    // is the one place the limit's default and its per-mode meaning are decided.
    if let Some(text) = &args.trieql {
        return from_trieql(text, now, args.limits);
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

/// A TrieQL string to a `Query`. The only thing `POST /query` accepts, and the same
/// three lines `--query '<trieql>'` runs.
///
/// Public so the HTTP surface reaches the grammar through this and not through
/// `trieql::parse` directly - `Query::new`'s per-mode limit rules and `finish`'s cursor
/// guard are part of what a TrieQL string means, and a second caller assembling the
/// struct itself would be a second dialect.
pub fn from_trieql(text: &str, now: DateTime<Utc>, limits: Limits) -> Result<Query, CliError> {
    let parsed = trieql::parse(text, now)?;
    finish(Query::new(
        parsed.matches,
        parsed.mode,
        parsed.time,
        parsed.limit,
        cursor(parsed.after.as_deref())?,
        system_time(now),
        limits,
    ))
}

/// The cursor text as a `Cursor`, before there is a `Query` to check it against.
fn cursor(text: Option<&str>) -> Result<Option<Cursor>, CliError> {
    text.map(Cursor::decode).transpose().map_err(CliError::from)
}

/// The last thing `build` does on either surface: does this cursor belong to this query?
///
/// After construction, not during: the guard is over the pattern and the order
/// (`Cursor::guard`), so there is nothing to check against until the whole `Query` exists.
/// Refusing here is the difference between an error and a silently wrong page - `Rank`'s
/// leaf ordinal is relative to the leaves *this* pattern matched, so the same cursor read
/// against a different pattern names a different leaf and answers confidently.
fn finish(query: Query) -> Result<Query, CliError> {
    if let Some(c) = query.cursor {
        c.check(&query)?;
    }
    // The bucket count, against a window that is now resolved. Here rather than in the
    // engine so `SINCE 30d GROUP BY time(1s)` is refused before thirty days of rows are
    // read - the engine checks again for the span a query with no lower bound derives
    // from its rows, which is the only one that cannot be known this early.
    engine::check_buckets(&query).map_err(engine::ExecError::from)?;
    if let QueryMode::GroupBy(keys) = &query.mode {
        // `bucket` is the column a `time(...)` reports under, so a key of that name
        // beside one would put two columns of the same name in the table and collide in
        // the NDJSON object. Refused rather than silently letting one win.
        let (width, columns) = engine::split_keys(keys);
        if width.is_some() && columns.iter().any(|c| c == BUCKET_COLUMN) {
            return Err(QueryError::BucketKeyClash.into());
        }
    }
    Ok(query)
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

/// `--group-by KEY[,KEY]`, where a key may be `time(<duration>)`.
///
/// The flag surface reaches the bucket too. The two surfaces build one `Query`
/// (see the module header), and a clause TrieQL can say and the flags cannot is a clause
/// `scripts/parser_equivalence.sh` stops covering the moment it is added.
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

/// The grammar's `duration`, and the only reading of one in the crate.
///
/// `--since 1h`, `SINCE 1h`, `--group-by 'time(1h)'` and `GROUP BY time(1h)` all land
/// here. It used to be written twice, once per surface, which is exactly the drift
/// `scripts/parser_equivalence.sh` exists to catch - each surface still wraps the `None`
/// in its own error, because a flag names itself and a query string names a byte offset.
///
/// By character, not by `len() - 1`: `len` is bytes and slicing at an index inside a
/// multibyte character panics. This reads a flag value straight off the command line, and
/// no input may make it panic.
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

/// The plan, and nothing executed.
///
/// Each line names the step it reports - `walk`, `scan`, `pruned`, `limit` - rather than
/// numbering them. A number told a reader which half of the source to open, not what the
/// query is about to do, and an operator reading a plan wants the second.
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
    // The limit and the order belong on the plan, not only in `kibbr explain`: a limit
    // that is `Query::new`'s default rather than the operator's is the one they most need
    // to see before the query runs, and the order is what decides which rows it keeps.
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

/// `kibbr explain '<trieql>'` - what the parser made of the string, and nothing else.
///
/// Ideally this would print the execution plan, but a plan needs a trie and this command
/// takes no file: nothing persists a trie yet, so the only one that exists is the one
/// `kibbr query <file>` builds for the length of a run. `query --explain` is the half
/// that plans; this one reports the `Query` the text denotes, which is the whole of what
/// the parser is answerable for. Persisting the trie would let this grow the plan without
/// the parser changing.
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
        // no answer for a cursor to be a page of. `AFTER`'s shape was checked by the
        // parser; whether it belongs to this query is `query --explain`'s to say.
        None,
        system_time(now),
        limits,
    );
    // The same refusal `query` makes, at the same point: `kibbr explain` is where an
    // operator checks a query before running it, and a plan for a query that will be
    // refused is not the answer they came for.
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

/// The time axis, on the plan and before it runs.
///
/// Where the boundaries fall is the thing an operator cannot see from the query text, and
/// the thing that makes two overlapping charts comparable - so the plan says it rather
/// than leaving it to be inferred from the answer. A query with no window has no count to
/// give here: the span comes from the rows, and `--explain` has read none.
fn bucket_text(query: &Query, width: std::time::Duration) -> String {
    let w = duration_text(width);
    let count = match engine::planned_buckets(query) {
        Some(n) => format!("{} buckets of {w}", commas(n)),
        // No window edge to count from: the span comes from the rows, and this has read
        // none. Naming the width is still worth a line - it is what the answer will be
        // shaped by.
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
        // Two answers, because `--exists` asks two questions (see `engine::execute`).
        // Without a window, `rows scanned: 0` is the mode's whole point: the answer came
        // off the trie walk having opened nothing. With one, the extra column is the
        // question actually asked - "is this firing?" - and `rows scanned` is what
        // stopping on the first matching row cost.
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
            // `count` is every row that matched and `rows_returned` is how many came back.
            // A limit that quietly printed the smaller number and called it the answer is
            // how someone concludes there were 10 errors when there were 40,000.
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

/// One compact line, so a differential script can diff two runs without a JSON parser.
///
/// Keys are sorted everywhere - `Row` is a `HashMap` and `groups` arrives already
/// ordered, because a result that serialises differently each run is not comparable to
/// anything.
fn json<W: Write>(out: &mut W, query: &Query, result: &QueryResult) -> Result<(), CliError> {
    let body = result_json(query, result);
    writeln!(out, "{body}").map_err(CliError::Write)
}

/// A `QueryResult` as the JSON both `--format json` and `POST /query` answer with.
///
/// One function, because `/query` is specified as answering with "query result JSON" and the
/// two surfaces answering the same question differently is the drift this module's
/// header is about. `engine::QueryMode`'s own comment makes the same promise from the
/// other end: the shape is consumed unchanged, so it is built in one place.
pub fn result_json(query: &Query, result: &QueryResult) -> Value {
    let rows: Vec<Value> = canonical(result).into_iter().map(row_json).collect();
    shaped(query, result, rows)
}

/// `result_json`, for a caller that may stop wanting the answer: `None` once `stop` is
/// set, asked every `STOP_EVERY_ROWS` rows.
///
/// A row is put into JSON at about a third of what it cost to fetch, so a fetch of every
/// row that was stopped only while it scanned would run on for half as long again after
/// it was told - and for a query that had just finished its scan, for all of that.
pub fn result_json_until(query: &Query, result: &QueryResult, stop: &Stop) -> Option<Value> {
    let mut rows = Vec::with_capacity(result.rows.len());
    for (n, row) in canonical(result).into_iter().enumerate() {
        if n % STOP_EVERY_ROWS == 0 && stop.check().is_err() {
            return None;
        }
        rows.push(row_json(row));
    }
    Some(shaped(query, result, rows))
}

/// The answer around its rows: what `result_json` and `result_json_until` share.
fn shaped(query: &Query, result: &QueryResult, rows: Vec<Value>) -> Value {
    let keys = group_keys_of(query);
    let groups: Vec<Value> = result
        .groups
        .iter()
        .map(|group| group_json(keys, group))
        .collect();

    let meta = &result.meta;
    serde_json::json!({
        "exists": result.exists,
        "count": result.count,
        "rows": rows,
        "groups": Value::Array(groups),
        "meta": {
            "templates_matched": meta.templates_matched,
            "template_ids": meta.template_ids,
            // Segment files read, and leaves opened. Two numbers because they are two
            // facts and one of them used to wear the other's name: 870 leaves against
            // 16,694 files read.
            "files_opened": meta.files_opened,
            "leaves_opened": meta.leaves_opened,
            // What the segment format buys, per answer: row groups read against row
            // groups dropped on their timestamps, and the bytes of column chunk that
            // reading them cost. A prune nobody can see is a prune nobody notices
            // stopping.
            "row_groups_read": meta.row_groups_read,
            "row_groups_pruned": meta.row_groups_pruned,
            "bytes_read": meta.bytes_read,
            "rows_scanned": meta.rows_scanned,
            "rows_returned": meta.rows_returned,
            "truncated": meta.truncated,
            // Two fields on purpose: `truncated` says the page is short of the answer,
            // this says who decided that.
            "limit_source": meta.limit_source.as_str(),
            "next_cursor": meta.next_cursor,
            "duration_ms": meta.duration.as_secs_f64() * 1000.0,
        }
    })
}

/// One row as one JSON object. Shared with `ndjson`, so a row serialises identically
/// whether it is a line of its own or an element of `rows`.
///
/// Through `store::present`, so the system column leaves as `_received_at` - or
/// not at all, on a row that ingested a key of that name.
///
/// **Each value is the JSON value that was ingested**: `Row::json`, which is the line's own
/// spelling wherever the stored value would print another - `"0045000012"`, `1705312200`,
/// `"2024-01-15T16:00:00+05:30"` - and the value's canonical print everywhere else. The
/// system column was not ingested, and prints as RFC 3339.
fn row_json(row: &kibbr_storage::store::Row) -> Value {
    let mut obj = Map::new();
    for (name, key) in kibbr_storage::store::present(row) {
        obj.insert(name, row.json(key).unwrap_or(Value::Null));
    }
    Value::Object(obj)
}

/// One JSON object per line and nothing else on stdout.
///
/// The envelope `--format json` prints - `exists`, `count`, `meta` - has no place in a
/// stream whose every line is a record, so none of it is here. The output feeds straight
/// back into `kibbr query -`, and into anything else that reads NDJSON: a row goes
/// through `row_json`, so a `Datetime` leaves as RFC 3339 and comes back as one.
///
/// The two empty answers stay apart on stderr. "No such template" prints no lines and
/// says so there; "that template exists and nothing matched" prints no lines and says
/// nothing. Neither puts an odd-shaped object in the middle of a stream something
/// downstream is parsing row by row.
///
/// **This does not stream, and it is not a memory mitigation.** Every row is ranked into a
/// `Vec<Row>` before the first line is written, so the format changes the encoding and not
/// the memory - measured rather than argued: peak RSS grows with the row count here, where
/// a streaming writer's would not (`scripts/backstop_rss.sh`). Nor could it: newest-first
/// over a file needs the last row read before the first row out. The server's day
/// partitions are ordered and can be walked descending; a flat file cannot. So the bound
/// here is the backstop and paging, which is why an unlimited fetch gets a cursor.
///
/// `next_cursor` goes to stderr because it is not a record.
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
        // Under a window the mode answers "is this firing?", so the record carries that
        // and not only the template's existence. Without one there is no such answer to
        // give - the walk read nothing - and inventing a `matched: false` would read as
        // "checked, and no", which is the one thing it does not mean.
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

/// The rows, in the order the answer already has them.
///
/// This used to sort by the rendered row, because the engine returned rows leaf by leaf,
/// the oracle in file order, and nothing promised either. `engine::Rank` promises one now,
/// newest first, so re-sorting here would throw away the ordering the limit depends on and
/// hand back the alphabetically-first rows instead of the newest. Both sides produce the
/// same sequence, so the differential still compares them exactly.
fn canonical(result: &QueryResult) -> Vec<&kibbr_storage::store::Row> {
    result.rows.iter().collect()
}

/// The keys a query grouped by, or none for every other mode.
///
/// `result_json` takes the query for this alone: a `Group` carries its values and not the
/// names they go under, because the names are the same for every group in the answer.
fn group_keys_of(query: &Query) -> &[GroupKey] {
    match &query.mode {
        QueryMode::GroupBy(keys) => keys,
        _ => &[],
    }
}

/// One group as its cells, in the order the keys were written, with the count last.
///
/// The bucket is pulled from `Group::bucket` and every other cell from `values` in turn,
/// so `GROUP BY level, time(5m)` prints its columns in that order rather than in one this
/// function chose. An unplaceable row's bucket reads `(null)` - the same spelling a
/// missing column value gets, and for the same reason: absence is not a value.
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

/// One group as JSON: the bucket beside the labels rather than among them, because it is
/// the one key that was computed rather than read off a row.
///
/// `bucket` is absent when the clause named no `time(...)` and `null` when it named one
/// and this row could not be placed in time. Two different facts, and a client drawing a
/// chart has to tell them apart.
fn group_json(keys: &[GroupKey], group: &Group) -> Value {
    let mut obj = Map::new();
    if keys.iter().any(|key| matches!(key, GroupKey::Time(_))) {
        obj.insert(
            BUCKET_COLUMN.to_string(),
            group
                .bucket
                .map_or(Value::Null, |ns| Value::from(rfc3339(ns))),
        );
    }
    let mut values = group.values.iter();
    let mut labels = Map::new();
    for key in keys {
        if let GroupKey::Column(name) = key {
            let value = values.next().cloned().unwrap_or_default();
            labels.insert(name.clone(), Value::from(value));
        }
    }
    obj.insert("keys".to_string(), Value::Object(labels));
    obj.insert("count".to_string(), Value::from(group.count));
    Value::Object(obj)
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
