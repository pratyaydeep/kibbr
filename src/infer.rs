//! Type inference. First match wins; the order is the contract.
//!
//! ```text
//! 1.  parses as RFC 3339                          -> Datetime
//! 2.  exactly 10 or 13 ASCII digits               -> Datetime  (seconds / millis)
//! 3.  matches the UUID pattern                    -> Uuid
//! 4.  parses as IpAddr (v4 or v6)                 -> Ip
//! 5.  contains @ and matches email                -> String
//! 6.  parses as i64                               -> Int64
//! 7.  parses as f64                               -> Float64
//! 8.  true / True / false / False                 -> Bool
//! 9.  anything else                               -> String
//! ```
//!
//! Rule 2 sits above rule 6 on purpose. A ten-digit number is a Unix timestamp before
//! it is an integer, because getting that wrong turns every time-range query into a
//! full scan. It misclassifies ten-digit order IDs; that is the accepted trade.

use std::net::IpAddr;

use chrono::DateTime;

use crate::types::{ColValue, ColumnType, Typed};

/// Run the ladder against a raw value. Every rung is written out separately and in
/// order; do not collapse them, the order is the thing being specified.
pub fn infer(raw: &str) -> Typed {
    // 1. RFC 3339.
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        let ns = dt.timestamp_nanos_opt().unwrap_or(i64::MAX);
        return typed(ColumnType::Datetime, ColValue::Datetime(ns));
    }

    // 2. Exactly 10 or 13 ASCII digits -> seconds / milliseconds since the epoch.
    //    Above rule 6 deliberately.
    if is_epoch_digits(raw) {
        let digits: i64 = match raw.parse() {
            Ok(v) => v,
            // Unreachable: 13 ASCII digits is at most 9_999_999_999_999 < i64::MAX.
            Err(_) => return typed(ColumnType::String, ColValue::String(raw.to_string())),
        };
        let per_unit = if raw.len() == 10 { 1_000_000_000 } else { 1_000_000 };
        // 9_999_999_999 seconds in nanoseconds overflows i64 (i64 nanos run out in
        // 2262). Saturate rather than wrap: the type call stands, the instant is
        // clamped, and nothing silently becomes a negative timestamp.
        let ns = digits.saturating_mul(per_unit);
        return typed(ColumnType::Datetime, ColValue::Datetime(ns));
    }

    // 3. UUID.
    if is_uuid(raw) {
        return typed(ColumnType::Uuid, ColValue::String(raw.to_string()));
    }

    // 4. IP address, v4 or v6.
    if raw.parse::<IpAddr>().is_ok() {
        return typed(ColumnType::Ip, ColValue::String(raw.to_string()));
    }

    // 5. Email. Nothing containing `@` could reach a later rung anyway; the rule is
    //    here so the ladder reads in its documented order.
    if is_email(raw) {
        return typed(ColumnType::String, ColValue::String(raw.to_string()));
    }

    // 6. i64.
    if let Ok(i) = raw.parse::<i64>() {
        return typed(ColumnType::Int64, ColValue::Int64(i));
    }

    // 7. f64. `inf`, `-inf` and `NaN` parse here in Rust but have no JSON encoding
    //    (`typed_values`), so they are held to finite values and fall to rule 9.
    if let Ok(f) = raw.parse::<f64>() {
        if f.is_finite() {
            return typed(ColumnType::Float64, ColValue::Float64(f));
        }
    }

    // 8. Bool, in exactly the four spellings the ladder allows.
    if matches!(raw, "true" | "True" | "false" | "False") {
        let b = matches!(raw, "true" | "True");
        return typed(ColumnType::Bool, ColValue::Bool(b));
    }

    // 9. Everything else.
    typed(ColumnType::String, ColValue::String(raw.to_string()))
}

fn typed(ty: ColumnType, value: ColValue) -> Typed {
    Typed { ty: Some(ty), value }
}

fn is_epoch_digits(s: &str) -> bool {
    (s.len() == 10 || s.len() == 13) && s.bytes().all(|b| b.is_ascii_digit())
}

/// 8-4-4-4-12 hex, case-insensitive. Version and variant nibbles are not checked:
/// a value shaped like a UUID is a UUID for storage purposes.
fn is_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    s.bytes().enumerate().all(|(i, b)| match i {
        8 | 13 | 18 | 23 => b == b'-',
        _ => b.is_ascii_hexdigit(),
    })
}

/// Deliberately conservative: one `@`, a non-empty local part, and a domain of
/// non-empty dot-separated labels with no whitespace.
fn is_email(s: &str) -> bool {
    let Some((local, domain)) = s.split_once('@') else {
        return false;
    };
    if local.is_empty() || domain.contains('@') || !domain.contains('.') {
        return false;
    }
    if s.chars().any(char::is_whitespace) {
        return false;
    }
    domain.split('.').all(|label| !label.is_empty())
}
