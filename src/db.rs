//! Loading a file into a trie and a set of leaf stores.
//!
//! For now this is what "the database" is: no persistence, so every query rebuilds it.
//! Kept apart from `engine` so the engine can be handed a trie and stores from anywhere
//! once something does persist them.
//!
//! `triedb serve` is the caller that made that separation pay: it holds one `Db` for the
//! process lifetime and appends to it a body at a time, where the CLI builds a fresh one
//! per query. Both go through `ingest_line`, so the two surfaces cannot disagree about
//! what putting a line in the database means.

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
use crate::types::{ColValue, ColumnType, DtForm};

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
/// promotion already required the ladder to say `String`.
pub fn row_of(parsed: &ParsedLog) -> Row {
    parsed
        .keys
        .iter()
        .map(|key| {
            let value = match parsed.typed_values.get(key) {
                Some(typed) => typed.value.clone(),
                None => ColValue::String(parsed.values.get(key).cloned().unwrap_or_default()),
            };
            (key.clone(), value)
        })
        .collect()
}

/// Which rung read each of this line's datetimes.
///
/// Beside the `Row` rather than inside it: a `Row` holds `ColValue`, and a query compares
/// a stored `ColValue` against one the ladder just built from the query's own text. Tagging
/// the value with its rung would stop `ts=1705314600` matching a row ingested as RFC 3339.
///
/// Datetime columns only, so this is empty for most lines and one entry for most of the
/// rest. The column store folds them per partition; see `types::DtForm`.
pub fn dt_forms_of(parsed: &ParsedLog) -> Vec<(String, DtForm)> {
    parsed
        .typed_values
        .iter()
        .filter(|(_, typed)| typed.ty == Some(ColumnType::Datetime))
        .filter_map(|(key, typed)| typed.dt_form.map(|form| (key.clone(), form)))
        .collect()
}

/// One parsed line into the trie, and into its leaf's store unless `stores` is `None`.
///
/// The whole write path. A future `Ingestor` would sit between the API and the trie; this
/// is a plain function instead, because there is one implementation of it and an interface
/// with one caller is a guess at what the second one will need. The WAL and the detector
/// cache are the arrows that will make it a type, and neither exists yet.
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
    stores: Option<&mut StoreManager>,
    parsed: &ParsedLog,
    now: DateTime<Utc>,
) -> Result<(), SinkError> {
    let prepared = Prepared::of(parsed);
    ingest_prepared(trie, stores, parsed, prepared, now)
}

/// A line's row and datetime rungs, built before anything is locked.
///
/// Splitting this off is what lets `/ingest` build five hundred rows without the store
/// lock and then only take it to append them. Neither half needs the trie: a row is
/// built from the line's own keys, and the rungs from its own values.
pub struct Prepared {
    pub row: Row,
    pub forms: Vec<(String, DtForm)>,
}

impl Prepared {
    pub fn of(parsed: &ParsedLog) -> Self {
        Self {
            row: row_of(parsed),
            forms: dt_forms_of(parsed),
        }
    }
}

/// The rest of `ingest_line`, with the row already built.
pub fn ingest_prepared(
    trie: &Trie,
    stores: Option<&mut StoreManager>,
    parsed: &ParsedLog,
    prepared: Prepared,
    now: DateTime<Utc>,
) -> Result<(), SinkError> {
    let (leaf, _) = trie.ingest(parsed, now)?;
    let Some(stores) = stores else {
        return Ok(());
    };
    let placed = leaf.leaf_meta.read().ok().and_then(|slot| {
        slot.as_ref()
            .map(|meta| (meta.template_id, meta.time_source.clone()))
    });
    // `ingest` just wrote the meta, so `None` is a poisoned lock and nothing else.
    // Dropping the row is the only option left and it is counted nowhere.
    if let Some((id, source)) = placed {
        let Prepared { mut row, mut forms } = prepared;
        // The effective time: the time column where it holds an instant, and
        // otherwise the arrival time - stored in the system column, **after**
        // fingerprinting, so it is not a key and makes no template. A row with its own
        // time does not get the column.
        //
        // Left here rather than moved out with the row: which column holds the time is
        // the *template's* choice, and nothing outside the trie knows it yet.
        if crate::store::ts_of(&row, source.column()).is_none() {
            row.insert(
                RECEIVED_AT_COLUMN.to_string(),
                ColValue::Datetime(crate::store::nanos_of(crate::query::system_time(now))),
            );
            forms.push((RECEIVED_AT_COLUMN.to_string(), DtForm::Rfc3339));
        }
        stores.append(id, source.column(), row, &forms);
    }
    Ok(())
}

/// Build the trie, and the stores unless `with_rows` is false.
///
/// `--explain` passes `false`: the walk needs the schema and nothing else, and skipping
/// row materialisation applies the walk/scan separation to the load.
///
/// Always in memory, and it has to be: the CLI rebuilds the whole database from its input
/// file per invocation, so there is nothing for it to persist and nothing it should read
/// back. A `triedb query` that picked up a `trie.json` would answer about rows its input
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
    let mut stores = StoreManager::memory();
    let stats = read_lines(
        input,
        &mut |_, parsed| ingest_line(&trie, with_rows.then_some(&mut stores), &parsed, now),
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
/// `/ingest` has to run it before it takes the store lock. Parsing is most of the cost of
/// a line, and running it under one global write lock is why ingest measured flat at
/// 30,000 lines a second from one sender to eight.
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
/// What `/ingest` uses: a row is an allocation per key, and five hundred of them under one
/// global write lock is five hundred allocations nothing else in the process may overlap.
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
