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
        // by eye. `to_json` keeps the nanos for machines.
        ColValue::Datetime(ns) => DateTime::from_timestamp_nanos(*ns).to_rfc3339(),
        ColValue::Bool(b) => b.to_string(),
        ColValue::Null => NULL_TEXT.to_string(),
    }
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

/// Elapsed time, in whichever unit has digits in it.
///
/// Phase 1 runs in microseconds and phase 2 in milliseconds, and that gap is the thing
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
