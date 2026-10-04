//! Loading a file into a trie and a set of leaf stores.
//!
//! For now this is what "the database" is: no persistence, so every query rebuilds it.
//! Kept apart from `engine` so the engine can be handed a trie and stores from anywhere
//! once something does persist them.
//!
//! `kibbr serve` is the caller that made that separation pay: it holds one trie and one
//! set of stores for the process lifetime and appends to them a body at a time, where the
//! CLI builds a fresh `Db` per query. Both parse a line and route it by the functions
//! here - the CLI through `ingest_line`, the server through `parse_and_prepare` and
//! `place_in` with its write-ahead log between the routing and the append - so the two
//! surfaces cannot disagree about what putting a line in the database means.

use std::io::BufRead;

use chrono::{DateTime, Utc};

use crate::error::{CliError, DetectorError, DetectorErrorKind};
use crate::flatten::{parse_line, ParsedLog};
use crate::lines::{read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES};
use crate::store::{Row, StoreManager, RECEIVED_AT_COLUMN};
use crate::trie::{Trie, TrieError};

/// How a line handed to a `read_lines` sink can fail to land.
///
/// Two outcomes that must not share a type: a `Line` rejection is counted against its
/// number and the loop continues, a `Fatal` one ends the read. Before the catalog every
/// post-parse failure was fatal, which was right - a poisoned lock fails every line after
/// it. A catalog append that fails is the other thing: it costs the line that needed the
/// record and nothing else.
#[derive(Debug)]
pub enum SinkError {
    Line(DetectorErrorKind),
    Fatal(CliError),
}

impl From<CliError> for SinkError {
    fn from(e: CliError) -> Self {
        SinkError::Fatal(e)
    }
}

impl From<TrieError> for SinkError {
    fn from(e: TrieError) -> Self {
        match e {
            TrieError::Catalog(_) => SinkError::Line(DetectorErrorKind::NotDurable {
                detail: crate::error::chain(&e),
            }),
            e => SinkError::Fatal(e.into()),
        }
    }
}
use crate::types::ColValue;

#[derive(Debug, Default, Clone, Copy)]
pub struct LoadStats {
    pub read: u64,
    pub ingested: u64,
    pub blank: u64,
    pub rejected: u64,
}

pub struct Db {
    pub trie: Trie,
    pub stores: StoreManager,
    pub stats: LoadStats,
}

/// One parsed line as a `Row`.
///
/// Every key on the path gets an entry, so a row's key set is the leaf's key set. A
/// promoted key carries its value in the path rather than in `typed_values`, and
/// promotion already required the ladder to say `String`. Each value keeps the JSON its
/// line wrote wherever that would not print back from the value alone.
pub fn row_of(parsed: &ParsedLog) -> Row {
    let mut row = Row::with_capacity(parsed.keys.len());
    for key in &parsed.keys {
        let value = match parsed.typed_values.get(key) {
            Some(typed) => typed.value.clone(),
            None => ColValue::String(parsed.values.get(key).cloned().unwrap_or_default()),
        };
        row.insert_written(key.clone(), value, parsed.written.get(key).cloned());
    }
    row
}

/// One parsed line into the trie, and into its leaf's store unless `stores` is `None`.
///
/// The whole write path of a store with no write-ahead log behind it: the CLI's, and the
/// bench's. `/ingest` does these steps with the log between them - `place_in`, the log,
/// then the append by the log's one applier (`serve::ingest`).
///
/// It holds no clock and no store lifecycle, which is the third of the three-way split:
/// the registry opens the leaf on first write and something else decides when the buffer
/// is written out.
///
/// `now` is a parameter for the reason it is one in `Trie::ingest`: `first_seen` and
/// `last_seen` are what `/templates` reports, and a test that pins them needs to be able
/// to fix the clock.
pub fn ingest_line(
    trie: &Trie,
    stores: Option<&StoreManager>,
    parsed: &ParsedLog,
    now: DateTime<Utc>,
) -> Result<(), SinkError> {
    let prepared = Prepared::of(parsed);
    ingest_prepared(trie, stores, parsed, prepared, now)
}

/// A line's row, built before anything is locked.
///
/// Splitting this off is what lets `/ingest` build five hundred rows before it touches a
/// leaf, and then only append them. It does not need the trie: a row is built from the
/// line's own keys and values.
pub struct Prepared {
    pub row: Row,
}

impl Prepared {
    pub fn of(parsed: &ParsedLog) -> Self {
        Self {
            row: row_of(parsed),
        }
    }
}

/// The rest of `ingest_line`, with the row already built.
pub fn ingest_prepared(
    trie: &Trie,
    stores: Option<&StoreManager>,
    parsed: &ParsedLog,
    prepared: Prepared,
    now: DateTime<Utc>,
) -> Result<(), SinkError> {
    let Some(stores) = stores else {
        trie.ingest(parsed, now)?;
        return Ok(());
    };
    if let Some(placed) = place(trie, parsed, prepared, now)? {
        // Refused by line, as a catalog append that fails is: the template exists, the row
        // is nowhere, and the line must not be counted as ingested. `to_string` and not
        // `error::chain`: every `StoreError` already prints its cause, and a chain would
        // print it twice.
        stores
            .append(placed.template_id, &placed.time_column, placed.row)
            .map_err(|e| {
                SinkError::Line(DetectorErrorKind::NotStored {
                    detail: e.to_string(),
                })
            })?;
    }
    Ok(())
}

/// A line's template, the template's time column, and the row it stores.
pub struct Placed {
    pub template_id: u64,
    pub time_column: String,
    pub row: Row,
}

/// A line through the trie - its template made or widened, durably - and its row stamped
/// with an arrival time if it has no time of its own: everything the write path does but
/// the append.
///
/// **Both doors call this**, the bulk loader here and `/ingest` through `place_in`, which
/// is what makes a line's template, types and stored row the same whichever door it came
/// through. `None` is a poisoned leaf lock and nothing else: `ingest` just wrote the meta.
pub fn place(
    trie: &Trie,
    parsed: &ParsedLog,
    prepared: Prepared,
    now: DateTime<Utc>,
) -> Result<Option<Placed>, SinkError> {
    Ok(place_in(trie, parsed, prepared, now)?)
}

/// `place`, failing as the trie fails and as nothing else. What the live door calls: its
/// error type has no room for a store's (`error::IngestError`), and `SinkError` does.
///
/// **This is the step that comes before the write-ahead log.** A line's log record names
/// the template it was routed to, so the template - and any widening the line caused -
/// is in the catalog, flushed, by the time this returns: an id is never in the log ahead
/// of the record that defines it.
pub fn place_in(
    trie: &Trie,
    parsed: &ParsedLog,
    prepared: Prepared,
    now: DateTime<Utc>,
) -> Result<Option<Placed>, TrieError> {
    let (leaf, _) = trie.ingest(parsed, now)?;
    let placed = leaf.leaf_meta.read().ok().and_then(|slot| {
        slot.as_ref()
            .map(|meta| (meta.template_id, meta.time_source.clone()))
    });
    let Some((id, source)) = placed else {
        return Ok(None);
    };
    let Prepared { mut row } = prepared;
    // Left here rather than moved out with the row: which column holds the time is the
    // *template's* choice, and nothing outside the trie knows it yet.
    stamp(&mut row, source.column(), arrival_nanos(now));
    Ok(Some(Placed {
        template_id: id,
        time_column: source.column().to_string(),
        row,
    }))
}

/// An arrival time as a row stores it, and as the write-ahead log records it.
pub fn arrival_nanos(now: DateTime<Utc>) -> i64 {
    crate::store::nanos_of(crate::query::system_time(now))
}

/// The effective time: the time column where it holds an instant, and otherwise the
/// arrival time - stored in the system column, **after** fingerprinting, so it is not a
/// key and makes no template. A row with its own time does not get the column.
fn stamp(row: &mut Row, time_column: &str, received_at: i64) {
    if crate::store::ts_of(row, time_column).is_none() {
        row.insert(
            RECEIVED_AT_COLUMN.to_string(),
            ColValue::Datetime(received_at),
        );
    }
}

/// A line read back from the write-ahead log, as the row it was first stored as.
///
/// **Routed by the record and not decided again**: the caller has the template's id from
/// the record and its time column from the catalog, so this only rebuilds the row - the
/// same parse, and the arrival time the record carries where the line has no time of its
/// own. Stamped with the time of the replay instead, such a row would move to another
/// instant and, across a midnight, to another day: its position would then be held
/// against the wrong day's row groups, and the line stored twice.
pub fn row_from_log(
    line: &[u8],
    time_column: &str,
    received_at: i64,
) -> Result<Row, DetectorError> {
    let parsed = parse_line(line, 0)?;
    let mut row = row_of(&parsed);
    stamp(&mut row, time_column, received_at);
    Ok(row)
}

/// Build the trie, and the stores unless `with_rows` is false.
///
/// `--explain` passes `false`: the walk needs the schema and nothing else, and skipping
/// row materialisation applies the walk/scan separation to the load.
///
/// Always in memory, and it has to be: the CLI rebuilds the whole database from its input
/// file per invocation, so there is nothing for it to persist and nothing it should read
/// back. A `kibbr query` that picked up a `trie.json` would answer about rows its input
/// file does not contain.
///
/// `now` is every line's arrival time, one instant for the whole file: the CLI has no
/// per-line clock worth the name, and the oracle it is compared against has to be
/// able to give each line the same arrival time this did.
pub fn load<R: BufRead>(
    input: R,
    with_rows: bool,
    now: DateTime<Utc>,
    schema: &crate::config::Schema,
) -> Result<Db, CliError> {
    let trie = Trie::new().with_time_columns(schema.time_columns.clone());
    let stores = StoreManager::memory();
    let stats = read_lines(
        input,
        &mut |_, parsed| ingest_line(&trie, with_rows.then_some(&stores), &parsed, now),
        &mut report_rejected,
    )?;
    Ok(Db {
        trie,
        stores,
        stats,
    })
}

/// Every parsed line, in file order. What the oracle scans.
pub fn load_lines<R: BufRead>(input: R) -> Result<(Vec<ParsedLog>, LoadStats), CliError> {
    let (lines, stats) = parse_only(input, &mut report_rejected)?;
    Ok((lines.into_iter().map(|(_, parsed)| parsed).collect(), stats))
}

/// Split and parse, taking no lock and touching no trie.
///
/// **This is the first half of the write path**, and it is a separate function because
/// `/ingest` runs it before it touches a leaf. Parsing is most of the cost of a line, and
/// running it under the one global write lock the stores used to have is why ingest
/// measured flat at 30,000 lines a second from one sender to eight.
///
/// The line number comes back beside each `ParsedLog` rather than inside it. A rejection
/// found in the second half has to be reported against the number an operator would count
/// in an editor, and after the split the two halves no longer share a counter.
///
/// `stats.ingested` is **not** filled in here - nothing has been ingested yet. The caller
/// applies the lines and counts what landed.
pub fn parse_only<R: BufRead>(
    input: R,
    on_error: &mut dyn FnMut(DetectorError),
) -> Result<(Vec<(usize, ParsedLog)>, LoadStats), CliError> {
    let mut lines = Vec::new();
    let stats = read_lines(
        input,
        &mut |at, parsed| {
            lines.push((at, parsed));
            Ok(())
        },
        on_error,
    )?;
    Ok((lines, stats))
}

/// One parsed line, its number, and the row built from it.
pub type PreparedLine = (usize, ParsedLog, Prepared);

/// The same, with each line's row built as well.
///
/// What `/ingest` uses: a row is an allocation per key, built here with the parse so the
/// second half has nothing left to do but walk the trie and append.
pub fn parse_and_prepare<R: BufRead>(
    input: R,
    on_error: &mut dyn FnMut(DetectorError),
) -> Result<(Vec<PreparedLine>, LoadStats), CliError> {
    let mut lines = Vec::new();
    let stats = read_lines(
        input,
        &mut |at, parsed| {
            let prepared = Prepared::of(&parsed);
            lines.push((at, parsed, prepared));
            Ok(())
        },
        on_error,
    )?;
    Ok((lines, stats))
}

/// What the CLI does with a rejected line: name it on stderr and carry on.
///
/// `/ingest` passes a collector instead - it owes the client the same rejections as JSON
/// in the response body, where stderr is not somewhere an HTTP client can look.
pub fn report_rejected(e: DetectorError) {
    crate::error::report(&e);
}

/// The read loop: strip the line ending and any BOM, skip blanks, report bad lines
/// against their number and carry on.
///
/// A rejected line does not fail a query, unlike `parse` and `demo`. A query is a read,
/// and one malformed line out of a million must not be the reason there is no answer -
/// the count it reports is the count over the lines that were readable, and stderr says
/// how many were not.
///
/// This is also the per-line error isolation `/ingest` needs, and it is the same loop: a bad line
/// goes to `on_error` and the loop continues, so line 400 of 500 costs the body line 400
/// and nothing else. What differs between the CLI and `/ingest` is only where the
/// rejection is written, which is why that is the parameter.
///
/// `stats.read` is the line number handed to `parse_line`, and it counts *every*
/// physical line - blanks included. An operator opening the body in an editor and
/// jumping to the reported line has to land on the line that was rejected.
pub fn read_lines<R: BufRead>(
    mut input: R,
    sink: &mut dyn FnMut(usize, ParsedLog) -> Result<(), SinkError>,
    on_error: &mut dyn FnMut(DetectorError),
) -> Result<LoadStats, CliError> {
    let mut stats = LoadStats::default();
    let mut raw = Vec::new();
    loop {
        raw.clear();
        let read = read_line(&mut input, &mut raw).map_err(|source| CliError::Read {
            path: "<input>".to_string(),
            source,
        })?;
        match read {
            Line::Eof => return Ok(stats),
            // Counted and numbered like any other rejection, so the lines after it keep
            // the numbers an operator would count in an editor. Unreachable through
            // `/ingest`, whose body cap is `MAX_LINE_BYTES` - see the constant.
            Line::TooLong => {
                stats.read += 1;
                stats.rejected += 1;
                // Reported before the skip, never after: the skip may be the last thing
                // this process does, and a rejection nobody saw is the bug being fixed.
                on_error(DetectorError::new(
                    stats.read as usize,
                    DetectorErrorKind::LineTooLong {
                        limit: MAX_LINE_BYTES,
                    },
                ));
                match skip_to_eol(&mut input).map_err(|source| CliError::Read {
                    path: "<input>".to_string(),
                    source,
                })? {
                    Resync::Done => continue,
                    Resync::NotDelimited { scanned } => {
                        return Err(CliError::NotNewlineDelimited {
                            line: stats.read,
                            scanned,
                            limit: MAX_LINE_BYTES,
                        })
                    }
                }
            }
            Line::Read => {}
        }
        stats.read += 1;
        let line = strip_bom(strip_eol(&raw));

        if std::str::from_utf8(line).is_ok_and(|s| s.trim().is_empty()) {
            stats.blank += 1;
            continue;
        }
        match parse_line(line, stats.read as usize) {
            Ok(parsed) => match sink(stats.read as usize, parsed) {
                Ok(()) => stats.ingested += 1,
                // The line parsed and still did not land. Counted and reported exactly
                // as a line that did not parse: to the client both are "this one is
                // not in the database", and the message says which.
                Err(SinkError::Line(kind)) => {
                    stats.rejected += 1;
                    on_error(DetectorError::new(stats.read as usize, kind));
                }
                Err(SinkError::Fatal(e)) => return Err(e),
            },
            Err(e) => {
                stats.rejected += 1;
                on_error(e);
            }
        }
    }
}
