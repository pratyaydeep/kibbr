//! `triedb parse` - the M0 observable surface. Per line: the fingerprint, then
//! every value with the type the ladder gave it.

use std::collections::HashSet;
use std::io::{BufRead, Write};

use chrono::DateTime;

use crate::error::CliError;
use crate::flatten::parse_line;
use crate::types::{ColValue, ColumnType};

/// Width of the widest fixed label, so `fingerprint` and the keys line up.
const MIN_KEY_WIDTH: usize = 11;
const TYPE_WIDTH: usize = 9;

pub fn run<R: BufRead, W: Write>(
    input: R,
    out: &mut W,
    limit: Option<u64>,
    json: bool,
) -> Result<(), CliError> {
    let mut seen_templates: HashSet<u64> = HashSet::new();
    let mut printed = 0u64;
    let mut skipped_blank = 0u64;

    for (index, line) in input.lines().enumerate() {
        if limit.is_some_and(|n| printed >= n) {
            break;
        }
        let line = line.map_err(|source| CliError::Read {
            path: "<input>".to_string(),
            source,
        })?;
        if line.trim().is_empty() {
            skipped_blank += 1;
            continue;
        }

        let mut parsed = parse_line(line.as_bytes(), index + 1)?;
        parsed.is_new_template = seen_templates.insert(parsed.template_id);
        printed += 1;

        if json {
            render_json(&parsed, out)?;
        } else {
            render(&parsed, printed, out)?;
        }
    }

    if json {
        return Ok(());
    }

    writeln!(out).map_err(CliError::Write)?;
    writeln!(
        out,
        "{printed} lines, {} templates{}",
        seen_templates.len(),
        if skipped_blank > 0 {
            format!(", {skipped_blank} blank lines skipped")
        } else {
            String::new()
        }
    )
    .map_err(CliError::Write)?;
    Ok(())
}

/// The parsed row as NDJSON - one `ParsedLog` per line, for anything downstream of a human.
///
/// `ParsedLog` stores its maps as `HashMap`, whose iteration order is not stable between
/// runs. Every map here is emitted in `keys` order instead, so two runs over the same
/// input produce the same bytes and a diff means a real change.
fn render_json<W: Write>(parsed: &crate::flatten::ParsedLog, out: &mut W) -> Result<(), CliError> {
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

fn render<W: Write>(
    parsed: &crate::flatten::ParsedLog,
    line_no: u64,
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

/// The raw text, plus whatever the ladder turned it into when the two differ.
/// A ten-digit order ID showing `1700000000  (= 2023-11-14T22:13:20Z)` is the
/// rule-2 trade made visible instead of buried.
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

fn rfc3339(nanos: i64) -> String {
    // AutoSi, not Secs: a value that carried sub-second precision keeps it, so the
    // rendering never implies the ladder threw precision away.
    DateTime::from_timestamp_nanos(nanos).to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}
