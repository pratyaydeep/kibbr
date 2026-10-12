//! `kibbr parse` - the observable surface. Per line: the fingerprint, then
//! every value with the type the ladder gave it.

use std::collections::HashSet;
use std::io::{BufRead, Write};

use crate::error::{CliError, DetectorError, DetectorErrorKind};
use kibbr_core::flatten::parse_line;
use kibbr_core::fmt::rfc3339;
use kibbr_core::lines::{
    read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES,
};
use kibbr_core::types::{ColValue, ColumnType};

/// Width of the widest fixed label, so `fingerprint` and the keys line up.
const MIN_KEY_WIDTH: usize = 11;
const TYPE_WIDTH: usize = 9;

/// Print each line's fingerprint and typed values; a bad line is reported and stepped
/// over, and a run that rejected anything still exits non-zero.
pub fn run<R: BufRead, W: Write>(
    mut input: R,
    out: &mut W,
    limit: Option<u64>,
    json: bool,
) -> Result<(), CliError> {
    let mut seen_templates: HashSet<u64> = HashSet::new();
    let mut printed = 0u64;
    let mut skipped_blank = 0u64;
    let mut rejected = 0u64;
    let mut read_lines = 0u64;
    // Reused across lines; `read_until` appends, so it is cleared each time.
    let mut raw = Vec::new();

    loop {
        if limit.is_some_and(|n| printed >= n) {
            break;
        }
        raw.clear();
        let read = read_line(&mut input, &mut raw).map_err(|source| CliError::Read {
            path: "<input>".to_string(),
            source,
        })?;
        match read {
            Line::Eof => break,
            // A framing fault, reported and stepped over like a syntax one. `--limit` cannot
            // stand in for the cap: a stream with no newline prints no record to count.
            Line::TooLong => {
                read_lines += 1;
                rejected += 1;
                // Flushed and reported before the skip, never after: the skip may be the
                // last thing this process does.
                out.flush().map_err(CliError::Write)?;
                crate::error::report(&DetectorError::new(
                    read_lines as usize,
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
                            line: read_lines,
                            scanned,
                            limit: MAX_LINE_BYTES,
                        })
                    }
                }
            }
            Line::Read => {}
        }
        read_lines += 1;
        let line_no = read_lines as usize;
        let line = strip_bom(strip_eol(&raw));

        // Blank means blank as text: bytes that do not decode are a bad line. Checked after
        // the mark is stripped, so a line holding nothing but one counts as blank.
        if std::str::from_utf8(line).is_ok_and(|s| s.trim().is_empty()) {
            skipped_blank += 1;
            continue;
        }

        let mut parsed = match parse_line(line, line_no) {
            Ok(parsed) => parsed,
            Err(e) => {
                rejected += 1;
                // Flush first: stdout is block-buffered when piped and stderr is not, so
                // without this the rejections all arrive before the records they sit between.
                out.flush().map_err(CliError::Write)?;
                // stderr, so the rejection never lands in the middle of the NDJSON
                // that `--json` promises on stdout.
                crate::error::report(&e);
                continue;
            }
        };
        parsed.is_new_template = seen_templates.insert(parsed.template_id);
        printed += 1;

        if json {
            render_json(&parsed, out)?;
        } else {
            render(&parsed, line_no, out)?;
        }
    }

    if !json {
        writeln!(out).map_err(CliError::Write)?;
        writeln!(
            out,
            "{printed} lines, {} templates{}{}",
            seen_templates.len(),
            if skipped_blank > 0 {
                format!(", {skipped_blank} blank lines skipped")
            } else {
                String::new()
            },
            if rejected > 0 {
                format!(", {rejected} lines rejected")
            } else {
                String::new()
            }
        )
        .map_err(CliError::Write)?;
    }

    if rejected > 0 {
        return Err(CliError::RejectedLines {
            count: rejected,
            total: read_lines,
        });
    }
    Ok(())
}

/// The parsed row as NDJSON, every map emitted in `keys` order: `HashMap` order is not
/// stable between runs, and a diff must mean a real change.
fn render_json<W: Write>(
    parsed: &kibbr_core::flatten::ParsedLog,
    out: &mut W,
) -> Result<(), CliError> {
    let mut values = serde_json::Map::new();
    let mut typed_values = serde_json::Map::new();
    let mut types = serde_json::Map::new();
    let json_values = parsed.typed_values_json();

    for key in &parsed.keys {
        if let Some(raw) = parsed.values.get(key) {
            values.insert(key.clone(), serde_json::Value::from(raw.as_str()));
        }
        if let Some(typed) = parsed.typed_values.get(key) {
            let value = json_values.get(key).cloned().unwrap_or_default();
            typed_values.insert(key.clone(), value);
            types.insert(key.clone(), serde_json::Value::from(typed.label()));
        }
    }

    let record = serde_json::json!({
        "template_id": parsed.template_id,
        "fingerprint": parsed.fingerprint,
        "keys": parsed.keys,
        "values": values,
        "typed_values": typed_values,
        "types": types,
        "is_new_template": parsed.is_new_template,
        "raw_line": String::from_utf8_lossy(&parsed.raw_line),
    });
    writeln!(out, "{record}").map_err(CliError::Write)
}

/// `line_no` is the physical line, the same number a rejection is reported against, so
/// one stream never holds two numbering schemes.
fn render<W: Write>(
    parsed: &kibbr_core::flatten::ParsedLog,
    line_no: usize,
    out: &mut W,
) -> Result<(), CliError> {
    let key_width = parsed
        .value_keys()
        .map(String::len)
        .chain(std::iter::once(MIN_KEY_WIDTH))
        .max()
        .unwrap_or(MIN_KEY_WIDTH);

    writeln!(out, "line {line_no}").map_err(CliError::Write)?;
    writeln!(
        out,
        "  {:<kw$}  {}",
        "fingerprint",
        parsed.fingerprint,
        kw = key_width
    )
    .map_err(CliError::Write)?;

    for key in parsed.value_keys() {
        let Some(typed) = parsed.typed_values.get(key) else {
            continue;
        };
        let raw = parsed.values.get(key).map_or("", String::as_str);
        writeln!(
            out,
            "  {:<kw$}  {:<tw$}  {}",
            key,
            typed.label(),
            display_value(typed.ty, &typed.value, raw),
            kw = key_width,
            tw = TYPE_WIDTH
        )
        .map_err(CliError::Write)?;
    }
    Ok(())
}

/// The raw text, plus whatever the ladder turned it into when the two differ: the epoch
/// rule's trade made visible instead of buried.
fn display_value(ty: Option<ColumnType>, value: &ColValue, raw: &str) -> String {
    match (ty, value) {
        (_, ColValue::Null) => "null".to_string(),
        (Some(ColumnType::Datetime), ColValue::Datetime(ns)) => {
            let text = rfc3339(*ns);
            if text == raw {
                raw.to_string()
            } else {
                format!("{raw}  (= {text})")
            }
        }
        // An empty string is a value, not a missing one; say so.
        (Some(ColumnType::String), ColValue::String(s)) if s.is_empty() => "\"\"".to_string(),
        _ => raw.to_string(),
    }
}

/// The `parse` arm of `main.rs`: the input opened, then `run`.
pub fn cli<W: Write>(
    input: &str,
    out: &mut W,
    limit: Option<u64>,
    json: bool,
) -> Result<(), CliError> {
    let reader = super::open(input)?;
    run(reader, out, limit, json)
}
