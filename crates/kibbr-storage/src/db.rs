// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The write path: one parsed line into a trie and a set of leaf stores.
//!
//! The CLI and the server both parse and route a line by the functions here, so the two
//! surfaces cannot disagree about what putting a line in the database means.

use std::io::BufRead;

use chrono::{DateTime, Utc};

use thiserror::Error;

use crate::core::error::{DetectorError, DetectorErrorKind};
use crate::core::flatten::{parse_line, ParsedLog};
use crate::core::lines::{
    read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES,
};
use crate::core::types::ColValue;

use crate::store::{Row, StoreManager, RECEIVED_AT_COLUMN};
use crate::trie::{Schema, Trie, TrieError};

/// What ends a read of lines, where a rejected line does not: the input could not be
/// read, it is not lines at all, or the trie failed in a way that fails every line after.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// No newline within `MAX_SKIP_BYTES` of an over-long line, so the input is not
    /// newline-delimited and there is no next line to carry on to.
    #[error(
        "line {line}: no newline in the {scanned} bytes after the {limit} byte line limit \
         - the input does not look like newline-delimited JSON"
    )]
    NotNewlineDelimited {
        line: u64,
        scanned: u64,
        limit: usize,
    },

    #[error(transparent)]
    Trie(#[from] TrieError),

    /// What a caller's own sink ended the read with - a flush between two lines that
    /// could not write, say.
    #[error(transparent)]
    Sink(Box<dyn std::error::Error + Send + Sync>),
}

/// How a line handed to a `read_lines` sink can fail to land: a `Line` rejection is
/// counted and the loop continues, a `Fatal` one ends the read.
#[derive(Debug)]
pub enum SinkError {
    Line(DetectorErrorKind),
    Fatal(ReadError),
}

impl From<ReadError> for SinkError {
    fn from(e: ReadError) -> Self {
        SinkError::Fatal(e)
    }
}

impl From<TrieError> for SinkError {
    fn from(e: TrieError) -> Self {
        match e {
            TrieError::Catalog(_) => SinkError::Line(DetectorErrorKind::NotDurable {
                detail: crate::core::error::chain(&e),
            }),
            e => SinkError::Fatal(e.into()),
        }
    }
}

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

/// One parsed line as a `Row`, with an entry for every key on the path so a row's key
/// set is the leaf's key set.
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

/// One parsed line into the trie, and into its leaf's store unless `stores` is `None`:
/// the whole write path of a store with no write-ahead log behind it.
pub fn ingest_line(
    trie: &Trie,
    stores: Option<&StoreManager>,
    parsed: &ParsedLog,
    now: DateTime<Utc>,
) -> Result<(), SinkError> {
    let prepared = Prepared::of(parsed);
    ingest_prepared(trie, stores, parsed, prepared, now)
}

/// A line's row, built before anything is locked, so `/ingest` builds its rows first and
/// then only appends them.
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
        // Refused by line, as a failed catalog append is: the template exists, the row is nowhere.
        // `to_string` and not `error::chain`: every `StoreError` already prints its cause.
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

/// A line through the trie and its row stamped with an arrival time if it has none of
/// its own; both doors call this, so a line is stored the same through either.
pub fn place(
    trie: &Trie,
    parsed: &ParsedLog,
    prepared: Prepared,
    now: DateTime<Utc>,
) -> Result<Option<Placed>, SinkError> {
    Ok(place_in(trie, parsed, prepared, now)?)
}

/// `place`, failing only as the trie fails; the template is in the catalog, flushed, when
/// this returns, so an id is never in the write-ahead log ahead of its record.
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
    crate::store::nanos_of(crate::store::system_time(now))
}

/// The effective time: the time column where it holds an instant, otherwise the arrival
/// time, stored after fingerprinting so it is not a key and makes no template.
fn stamp(row: &mut Row, time_column: &str, received_at: i64) {
    if crate::store::ts_of(row, time_column).is_none() {
        row.insert(
            RECEIVED_AT_COLUMN.to_string(),
            ColValue::Datetime(received_at),
        );
    }
}

/// A line read back from the write-ahead log as the row it was first stored as, with the
/// record's arrival time: stamped at replay it could move to another day.
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

/// Build the trie, and the stores unless `with_rows` is false, always in memory: the CLI
/// rebuilds the database from its input per invocation, `now` being every line's arrival.
pub fn load<R: BufRead>(
    input: R,
    with_rows: bool,
    now: DateTime<Utc>,
    schema: &Schema,
) -> Result<Db, ReadError> {
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

/// Every parsed line, in file order, which is what the oracle scans.
pub fn load_lines<R: BufRead>(input: R) -> Result<(Vec<ParsedLog>, LoadStats), ReadError> {
    let (lines, stats) = parse_only(input, &mut report_rejected)?;
    Ok((lines.into_iter().map(|(_, parsed)| parsed).collect(), stats))
}

/// Split and parse, taking no lock and touching no trie: the first half of the write
/// path, with each line's number beside it and `stats.ingested` left to the caller.
pub fn parse_only<R: BufRead>(
    input: R,
    on_error: &mut dyn FnMut(DetectorError),
) -> Result<(Vec<(usize, ParsedLog)>, LoadStats), ReadError> {
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

/// The same, with each line's row built as well, so `/ingest` has nothing left to do but
/// walk the trie and append.
pub fn parse_and_prepare<R: BufRead>(
    input: R,
    on_error: &mut dyn FnMut(DetectorError),
) -> Result<(Vec<PreparedLine>, LoadStats), ReadError> {
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
pub fn report_rejected(e: DetectorError) {
    crate::core::error::report(&e);
}

/// The read loop: strip the line ending and any BOM, skip blanks, report bad lines against
/// their number - every physical line counted, as an editor would - and carry on.
pub fn read_lines<R: BufRead>(
    mut input: R,
    sink: &mut dyn FnMut(usize, ParsedLog) -> Result<(), SinkError>,
    on_error: &mut dyn FnMut(DetectorError),
) -> Result<LoadStats, ReadError> {
    let mut stats = LoadStats::default();
    let mut raw = Vec::new();
    loop {
        raw.clear();
        let read = read_line(&mut input, &mut raw).map_err(|source| ReadError::Read {
            path: "<input>".to_string(),
            source,
        })?;
        match read {
            Line::Eof => return Ok(stats),
            // Counted and numbered like any other rejection, so the lines after it keep the
            // numbers an operator would count in an editor.
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
                match skip_to_eol(&mut input).map_err(|source| ReadError::Read {
                    path: "<input>".to_string(),
                    source,
                })? {
                    Resync::Done => continue,
                    Resync::NotDelimited { scanned } => {
                        return Err(ReadError::NotNewlineDelimited {
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
                // The line parsed and still did not land: counted and reported as a line that did
                // not parse, and the message says which.
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
