//! Rendering shared by the commands. One definition each, because a value that prints
//! two ways in two places is a bug an operator has to notice first.

use chrono::DateTime;

use crate::types::ColValue;

/// A column that has no value on this row - absent, or an explicit JSON null.
pub const NULL_TEXT: &str = "(null)";

/// A value as a human reads it. Not a round-trip encoding: `--match` compares against the
/// stored `ColValue` (see `engine::value_matches`), not against this.
pub fn col_text(value: &ColValue) -> String {
    match value {
        ColValue::String(s) => s.clone(),
        ColValue::Int64(i) => i.to_string(),
        ColValue::Float64(f) => f.to_string(),
        // Nanoseconds are unreadable and a timestamp is the one column an operator scans
        // by eye. `ColValue::to_json` keeps the nanos, for the one surface that needs the
        // machine instant - see `rfc3339` below.
        ColValue::Datetime(ns) => rfc3339(*ns),
        ColValue::Bool(b) => b.to_string(),
        ColValue::Null => NULL_TEXT.to_string(),
    }
}

/// An instant as RFC 3339, and the only place this crate spells one.
///
/// One definition, per the note at the top of this file: the table, `triedb parse`, and
/// the rows in `--format json` and `--format ndjson` all come through here, so a
/// timestamp cannot read one way on the terminal and another in the JSON.
///
/// `ColValue::to_json` is the deliberate exception and keeps the nanoseconds. It feeds
/// `parse --json`'s `typed_values`, a view of what the type ladder decided: the boundary
/// cases there - year 9999, `i64::MAX`, epoch 0 - are only checkable as exact instants
/// (`tests/fixture.rs`).
///
/// AutoSi, not Secs: a value carrying sub-second precision keeps it, so the rendering
/// never implies the ladder threw precision away. `Z`, not `+00:00`, because that is what
/// the corpus is written in and what this re-ingests as.
pub fn rfc3339(nanos: i64) -> String {
    rfc3339_at(DateTime::from_timestamp_nanos(nanos))
}

/// The same spelling, for an instant that is already a `DateTime`.
///
/// `/templates` reports `first_seen` and `last_seen`, which `LeafMeta` holds as
/// `DateTime<Utc>` rather than as nanoseconds. Routed through one set of options rather
/// than calling `to_rfc3339` at the call site, so the rule above - one spelling, no
/// exceptions beyond the documented one - survives a second kind of input.
pub fn rfc3339_at(t: DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// Thousands separators. Row counts here are read, not parsed.
pub fn commas(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// A bucket width as the grammar spells it: the largest unit that divides it evenly.
///
/// The inverse of the `duration` both surfaces parse, and here rather than at the call
/// site for the reason `rfc3339` is: `time(5m)` has to read back as `5m` in the error
/// that refuses it and in the `--explain` line above it, or the two disagree about the
/// query the operator wrote.
pub fn duration_text(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    for (unit, size) in [('d', 86_400), ('h', 3_600), ('m', 60)] {
        if secs >= size && secs.is_multiple_of(size) {
            return format!("{}{unit}", secs / size);
        }
    }
    format!("{secs}s")
}

/// Elapsed time, in whichever unit has digits in it.
///
/// The walk runs in microseconds and the scan in milliseconds, and that gap is what
/// `--explain` is claiming - printing both to two decimal places of a millisecond renders
/// the claim as `0.00ms`.
pub fn millis(d: std::time::Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:.0}µs", ms * 1000.0)
    } else {
        format!("{ms:.2}ms")
    }
}
