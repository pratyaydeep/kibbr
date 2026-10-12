//! Rendering shared by the commands. One definition each, because a value that prints
//! two ways in two places is a bug an operator has to notice first.

use chrono::DateTime;

use crate::types::ColValue;

/// A column that has no value on this row - absent, or an explicit JSON null.
pub const NULL_TEXT: &str = "(null)";

/// A value as a human reads it, not a round-trip encoding.
pub fn col_text(value: &ColValue) -> String {
    match value {
        ColValue::String(s) => s.clone(),
        ColValue::Int64(i) => i.to_string(),
        ColValue::Float64(f) => f.to_string(),
        // Nanoseconds are unreadable, and a timestamp is the one column an operator scans by eye.
        ColValue::Datetime(ns) => rfc3339(*ns),
        ColValue::Bool(b) => b.to_string(),
        ColValue::Null => NULL_TEXT.to_string(),
    }
}

/// An instant as RFC 3339, the one place it is spelled: AutoSi so sub-second precision
/// is kept, `Z` because that is what re-ingests.
pub fn rfc3339(nanos: i64) -> String {
    rfc3339_at(DateTime::from_timestamp_nanos(nanos))
}

/// The same spelling, for an instant that is already a `DateTime`.
pub fn rfc3339_at(t: DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// Thousands separators, for row counts that are read, not parsed.
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

/// A bucket width as the grammar spells it, the largest unit that divides it evenly, so
/// `time(5m)` reads back as `5m`.
pub fn duration_text(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    for (unit, size) in [('d', 86_400), ('h', 3_600), ('m', 60)] {
        if secs >= size && secs.is_multiple_of(size) {
            return format!("{}{unit}", secs / size);
        }
    }
    format!("{secs}s")
}

/// Elapsed time in whichever unit has digits in it, so a microsecond walk does not print
/// as `0.00ms`.
pub fn millis(d: std::time::Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{:.0}µs", ms * 1000.0)
    } else {
        format!("{ms:.2}ms")
    }
}
