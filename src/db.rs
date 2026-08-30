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
use crate::store::{Row, Stores};
use crate::trie::Trie;
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
    pub stores: Stores,
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

/// One parsed line into the trie, and into its leaf's store unless `stores` is `None`.
///
/// The whole write path as of M3. A future `Ingestor` would sit between the API and the
/// trie; this is a plain function instead, because there is one implementation of it and
/// an interface with one caller is a guess at what the second one will need. The
/// WAL, the detector cache and the flush timer are the arrows that will make it a type,
/// and none of them exists yet.
///
/// `now` is a parameter for the reason it is one in `Trie::ingest`: `first_seen` and
/// `last_seen` are what `/templates` reports, and a test that pins them needs to be able
/// to fix the clock.
pub fn ingest_line(
    trie: &Trie,
    stores: Option<&mut Stores>,
    parsed: &ParsedLog,
    now: DateTime<Utc>,
) -> Result<(), CliError> {
    let (leaf, _) = trie.ingest(parsed, now)?;
    let Some(stores) = stores else {
        return Ok(());
    };
    let id = leaf
        .leaf_meta
        .read()
        .ok()
        .and_then(|slot| slot.as_ref().map(|meta| meta.template_id));
    // `ingest` just wrote the meta, so `None` is a poisoned lock and nothing else.
    // Dropping the row is the only option left and it is counted nowhere.
    if let Some(id) = id {
        stores.append(id, row_of(parsed));
    }
    Ok(())
}

/// Build the trie, and the stores unless `with_rows` is false.
///
/// `--explain` passes `false`: the walk needs the schema and nothing else, and skipping
/// row materialisation applies the walk/scan separation to the load.
pub fn load<R: BufRead>(input: R, with_rows: bool) -> Result<Db, CliError> {
    let trie = Trie::new();
    let mut stores = Stores::new();
    let stats = read_lines(
        input,
        &mut |parsed| ingest_line(&trie, with_rows.then_some(&mut stores), &parsed, Utc::now()),
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
    let mut lines = Vec::new();
    let stats = read_lines(
        input,
        &mut |parsed| {
            lines.push(parsed);
            Ok(())
        },
        &mut report_rejected,
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
    sink: &mut dyn FnMut(ParsedLog) -> Result<(), CliError>,
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
            Ok(parsed) => {
                sink(parsed)?;
                stats.ingested += 1;
            }
            Err(e) => {
                stats.rejected += 1;
                on_error(e);
            }
        }
    }
}
