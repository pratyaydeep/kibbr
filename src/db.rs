//! Loading a file into a trie and a set of leaf stores.
//!
//! For now this is what "the database" is: no persistence, so every query rebuilds it.
//! Kept apart from `engine` so the engine can be handed a trie and stores from anywhere
//! once something does persist them.

use std::io::BufRead;

use chrono::Utc;

use crate::error::CliError;
use crate::flatten::{parse_line, ParsedLog};
use crate::lines::{strip_bom, strip_eol};
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

/// Build the trie, and the stores unless `with_rows` is false.
///
/// `--explain` passes `false`: the walk needs the schema and nothing else, and skipping
/// row materialisation applies the walk/scan separation to the load.
pub fn load<R: BufRead>(input: R, with_rows: bool) -> Result<Db, CliError> {
    let trie = Trie::new();
    let mut stores = Stores::new();
    let stats = read_lines(input, &mut |parsed| {
        let (leaf, _) = trie.ingest(&parsed, Utc::now())?;
        if with_rows {
            let id = leaf
                .leaf_meta
                .read()
                .ok()
                .and_then(|slot| slot.as_ref().map(|meta| meta.template_id));
            // `ingest` just wrote the meta, so `None` is a poisoned lock and nothing
            // else. Dropping the row is the only option left and it is counted nowhere -
            // single-threaded for now, so it cannot happen yet.
            if let Some(id) = id {
                stores.append(id, row_of(&parsed));
            }
        }
        Ok(())
    })?;
    Ok(Db {
        trie,
        stores,
        stats,
    })
}

/// Every parsed line, in file order. What the oracle scans.
pub fn load_lines<R: BufRead>(input: R) -> Result<(Vec<ParsedLog>, LoadStats), CliError> {
    let mut lines = Vec::new();
    let stats = read_lines(input, &mut |parsed| {
        lines.push(parsed);
        Ok(())
    })?;
    Ok((lines, stats))
}

/// The read loop: strip the line ending and any BOM, skip blanks, report bad lines
/// against their number and carry on.
///
/// A rejected line does not fail a query, unlike `parse` and `demo`. A query is a read,
/// and one malformed line out of a million must not be the reason there is no answer -
/// the count it reports is the count over the lines that were readable, and stderr says
/// how many were not.
pub fn read_lines<R: BufRead>(
    mut input: R,
    sink: &mut dyn FnMut(ParsedLog) -> Result<(), CliError>,
) -> Result<LoadStats, CliError> {
    let mut stats = LoadStats::default();
    let mut raw = Vec::new();
    loop {
        raw.clear();
        // Bytes, not `lines()`: that iterator turns one non-UTF-8 line into an
        // `io::Error` and kills the stream.
        let read = input
            .read_until(b'\n', &mut raw)
            .map_err(|source| CliError::Read {
                path: "<input>".to_string(),
                source,
            })?;
        if read == 0 {
            return Ok(stats);
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
                crate::error::report(&e);
            }
        }
    }
}
