// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Type inference. First match wins; the order is the contract.
//!
//! Rule 2 (epoch digits) sits above rule 6 (i64): a timestamp read as an integer turns
//! every time-range query into a full scan. A value the typed form cannot hold exactly
//! falls through to the rungs below; nothing is clamped, rounded or truncated.

use std::net::IpAddr;
use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset};

use crate::types::{ColValue, ColumnType, DtForm, Typed};

/// Run the ladder against a raw value, every rung written out in order because the order
/// is the specification.
pub fn infer(raw: &str) -> Typed {
    // 1. RFC 3339, where the instant it names is one the type holds exactly.
    if let Some(ns) = rfc3339_nanos(raw) {
        return dated(ColValue::Datetime(ns), DtForm::Rfc3339);
    }

    // 2. 10 or 13 digits inside the epoch window. Above rule 6 deliberately; outside the
    //    window they fall through to it.
    if is_epoch_digits(raw) {
        let digits: i64 = match raw.parse() {
            Ok(v) => v,
            // Unreachable: 13 ASCII digits is at most 9_999_999_999_999 < i64::MAX.
            Err(_) => return typed(ColumnType::String, ColValue::String(raw.to_string())),
        };
        let (per_unit, form) = if raw.len() == 10 {
            (1_000_000_000, DtForm::EpochSecs)
        } else {
            (1_000_000, DtForm::EpochMillis)
        };
        if epoch_window().contains(digits, per_unit) {
            // Nanoseconds run out in 2262; past it the digits fall through, since a clamped
            // instant would be another moment.
            if let Some(ns) = digits.checked_mul(per_unit) {
                return dated(ColValue::Datetime(ns), form);
            }
        }
    }

    // 3. UUID.
    if is_uuid(raw) {
        return typed(ColumnType::Uuid, ColValue::String(raw.to_string()));
    }

    // 4. IP address, v4 or v6.
    if raw.parse::<IpAddr>().is_ok() {
        return typed(ColumnType::Ip, ColValue::String(raw.to_string()));
    }

    // 5. Email. Nothing containing `@` reaches a later rung anyway; the rule is here so
    //    the ladder reads in its documented order.
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
    Typed {
        ty: Some(ty),
        value,
        dt_form: None,
    }
}

/// A `Datetime`, tagged with the rung that read it - see `DtForm`.
fn dated(value: ColValue, form: DtForm) -> Typed {
    Typed {
        ty: Some(ColumnType::Datetime),
        value,
        dt_form: Some(form),
    }
}

/// The instant an RFC 3339 value names, or `None` where the type cannot hold it exactly:
/// a second of 60, more than nine fraction digits, a year outside 1677 to 2262.
fn rfc3339_nanos(raw: &str) -> Option<i64> {
    let dt = DateTime::parse_from_rfc3339(raw).ok()?;
    // How chrono holds a leap second: a sub-second part of a full second or more.
    if dt.timestamp_subsec_nanos() >= 1_000_000_000 {
        return None;
    }
    // The fraction is the only `.` in a value that parsed: the offset is `Z` or `+hh:mm`.
    let fraction = raw
        .split_once('.')
        .map_or(0, |(_, rest)| rest.bytes().take_while(u8::is_ascii_digit).count());
    if fraction > 9 {
        return None;
    }
    dt.timestamp_nanos_opt()
}

/// Ten or thirteen ASCII digits, the first not a zero: a leading zero is a fixed-width
/// id, not a count since 1970.
fn is_epoch_digits(s: &str) -> bool {
    (s.len() == 10 || s.len() == 13)
        && !s.starts_with('0')
        && s.bytes().all(|b| b.is_ascii_digit())
}

/// The moments rule 2 may read ten or thirteen digits as, `[start, end)`, by default 2000
/// to 2100; it decides the type stored, and a column never narrows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochWindow {
    start_ns: i128,
    end_ns: i128,
}

const NANOS_PER_SEC: i128 = 1_000_000_000;

impl EpochWindow {
    /// `None` unless `start` is before `end`: an empty window would type every epoch
    /// timestamp as an integer.
    pub fn new(start: DateTime<FixedOffset>, end: DateTime<FixedOffset>) -> Option<Self> {
        let nanos = |dt: DateTime<FixedOffset>| {
            i128::from(dt.timestamp()) * NANOS_PER_SEC + i128::from(dt.timestamp_subsec_nanos())
        };
        let (start_ns, end_ns) = (nanos(start), nanos(end));
        (start_ns < end_ns).then_some(Self { start_ns, end_ns })
    }

    /// Does `digits` counted in `per_unit` nanoseconds name a moment inside the window?
    fn contains(&self, digits: i64, per_unit: i64) -> bool {
        let ns = i128::from(digits) * i128::from(per_unit);
        self.start_ns <= ns && ns < self.end_ns
    }
}

impl Default for EpochWindow {
    /// `[2000-01-01T00:00:00Z, 2100-01-01T00:00:00Z)`.
    fn default() -> Self {
        Self {
            start_ns: 946_684_800 * NANOS_PER_SEC,
            end_ns: 4_102_444_800 * NANOS_PER_SEC,
        }
    }
}

static EPOCH_WINDOW: OnceLock<EpochWindow> = OnceLock::new();

/// Fix the window for the rest of the process, first call wins: the ladder is re-run on
/// stored text, and a moving window would give one text two types.
pub fn set_epoch_window(window: EpochWindow) {
    // Ignoring a second call keeps the window every earlier `infer` already used.
    let _ = EPOCH_WINDOW.set(window);
}

fn epoch_window() -> &'static EpochWindow {
    EPOCH_WINDOW.get_or_init(EpochWindow::default)
}

/// 8-4-4-4-12 hex, case-insensitive. Version and variant nibbles are not checked: a value
/// shaped like a UUID is a UUID for storage purposes.
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
