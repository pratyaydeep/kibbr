// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Type inference. First match wins; the order is the contract.
//!
//! Rule 2 (epoch digits) sits above rule 6 (i64): a timestamp read as an integer turns
//! every time-range query into a full scan. A value the typed form cannot hold exactly
//! falls through to the rungs below; nothing is clamped, rounded or truncated.
//!
//! The ladder types a value whatever its key. A template's time column reads more
//! (`time_nanos`): forms that are a time there and a number or a string anywhere else, so
//! they decide a row's time and never a column's type.

use std::net::IpAddr;
use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset, NaiveDate};

use crate::engine::model::types::{ColValue, ColumnType, DtForm, Typed};

/// Run the ladder against a raw value, every rung written out in order because the order
/// is the specification.
pub fn infer(raw: &str) -> Typed {
    // 1. RFC 3339, where the instant it names is one the type holds exactly.
    if let Some(ns) = rfc3339_nanos(raw) {
        return dated(ColValue::Datetime(ns), DtForm::Rfc3339);
    }

    // 2. 10 or 13 digits inside the epoch window. Above rule 6 deliberately; outside the
    //    window they fall through to it.
    if let Some((_, ns, form)) = epoch_reading(raw) {
        return dated(ColValue::Datetime(ns), form);
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
    (s.len() == 10 || s.len() == 13) && is_count(s)
}

/// ASCII digits, the first not a zero.
fn is_count(s: &str) -> bool {
    !s.is_empty() && !s.starts_with('0') && s.bytes().all(|b| b.is_ascii_digit())
}

const NANOS_PER_SEC_I64: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;

/// `digits` counted in `per_unit` nanoseconds, as nanoseconds, where that moment is inside
/// the epoch window. Nanoseconds run out in 2262; past it there is no instant, since a
/// clamped one would be another moment.
fn windowed(digits: i64, per_unit: i64) -> Option<i64> {
    if epoch_window().contains(digits, per_unit) {
        digits.checked_mul(per_unit)
    } else {
        None
    }
}

/// Rule 2: the count `raw` spells, the instant it names in nanoseconds, and the unit it
/// was read in. Thirteen ASCII digits are at most 9_999_999_999_999, so the parse holds.
fn epoch_reading(raw: &str) -> Option<(i64, i64, DtForm)> {
    if !is_epoch_digits(raw) {
        return None;
    }
    let digits: i64 = raw.parse().ok()?;
    let (per_unit, form) = if raw.len() == 10 {
        (NANOS_PER_SEC_I64, DtForm::EpochSecs)
    } else {
        (NANOS_PER_MILLI, DtForm::EpochMillis)
    };
    windowed(digits, per_unit).map(|ns| (digits, ns, form))
}

/// The integer rule 2 reads `raw` as a count of, when it reads it at all: what a column
/// of integers holds those digits as, exactly - the instant is that count times a whole
/// number of nanoseconds, so dividing gives the digits back.
pub fn epoch_digits(raw: &str) -> Option<i64> {
    epoch_reading(raw).map(|(digits, _, _)| digits)
}

// ── What a time column reads ───────────────────────────────────────────────────────

/// The instant a template's time column reads `raw` as: rule 1 and rule 2 as the ladder
/// reads them, and then the forms only a time column reads (`time_form_nanos`).
pub fn time_nanos(raw: &str) -> Option<i64> {
    rfc3339_nanos(raw)
        .or_else(|| epoch_reading(raw).map(|(_, ns, _)| ns))
        .or_else(|| time_form_nanos(raw))
}

/// `time_nanos` of an integer written in plain decimal, with no text made: ten, thirteen,
/// sixteen or nineteen digits are seconds, milliseconds, microseconds or nanoseconds.
pub fn time_nanos_of_int(value: i64) -> Option<i64> {
    if value <= 0 {
        return None;
    }
    let per_unit = match value.ilog10() + 1 {
        10 => NANOS_PER_SEC_I64,
        13 => NANOS_PER_MILLI,
        16 => NANOS_PER_MICRO,
        19 => 1,
        _ => return None,
    };
    windowed(value, per_unit)
}

/// The forms a time column reads and the ladder does not, each exact or not read at all:
///
/// - epoch seconds with a point and one to nine fraction digits, `1679711330.570420890`;
/// - sixteen digits as epoch microseconds and nineteen as nanoseconds;
/// - `YYYY-MM-DD HH:MM:SS[.fraction] ZONE`, the zone one of `ZONES`.
///
/// The epoch forms are held to the epoch window, as rule 2 is. Nothing here is on the
/// ladder: under any other key `1679711330.57` is a `Float64`, sixteen digits an `Int64`
/// and a zoned date a `String`, and under a time column they still are - only the row's
/// time is read from them.
pub fn time_form_nanos(raw: &str) -> Option<i64> {
    decimal_seconds(raw)
        .or_else(|| long_epoch(raw))
        .or_else(|| zoned(raw))
}

/// Ten digits of seconds, a point, one to nine digits of fraction: read digit by digit,
/// never through a float, so the ninth digit is the nanosecond that was written.
fn decimal_seconds(raw: &str) -> Option<i64> {
    let (whole, fraction) = raw.split_once('.')?;
    if whole.len() != 10 || !is_count(whole) {
        return None;
    }
    let sub = fraction_nanos(fraction)?;
    let secs: i64 = whole.parse().ok()?;
    let ns = secs.checked_mul(NANOS_PER_SEC_I64)?.checked_add(sub)?;
    windowed(ns, 1)
}

/// One to nine fraction digits as nanoseconds; a tenth digit is more than the type holds.
fn fraction_nanos(fraction: &str) -> Option<i64> {
    if fraction.is_empty() || fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let digits: i64 = fraction.parse().ok()?;
    Some(digits * 10i64.pow(9 - fraction.len() as u32))
}

/// Sixteen digits as microseconds, nineteen as nanoseconds, the first not a zero.
fn long_epoch(raw: &str) -> Option<i64> {
    let per_unit = match raw.len() {
        16 => NANOS_PER_MICRO,
        19 => 1,
        _ => return None,
    };
    if !is_count(raw) {
        return None;
    }
    // Nineteen digits can be past `i64::MAX`, which is past 2262 and no instant here.
    windowed(raw.parse().ok()?, per_unit)
}

/// The zone abbreviations a time column reads, with the hours each is from UTC. Only the
/// ones that name one offset in the logs this was built for: `IST` is India, Israel and
/// Ireland, and is not read.
pub const ZONES: [(&str, i64); 11] = [
    ("UTC", 0),
    ("GMT", 0),
    ("Z", 0),
    ("EST", -5),
    ("EDT", -4),
    ("CST", -6),
    ("CDT", -5),
    ("MST", -7),
    ("MDT", -6),
    ("PST", -8),
    ("PDT", -7),
];

/// `YYYY-MM-DD HH:MM:SS[.fraction] ZONE`: one space each side of the time, two-digit
/// fields, a second below 60, and an instant the type holds exactly.
fn zoned(raw: &str) -> Option<i64> {
    let (stamp, zone) = raw.rsplit_once(' ')?;
    let hours = ZONES.iter().find(|(name, _)| *name == zone)?.1;
    let b = stamp.as_bytes();
    if b.len() < 19 || !stamp.is_ascii() {
        return None;
    }
    let marks = b[4] == b'-' && b[7] == b'-' && b[10] == b' ' && b[13] == b':' && b[16] == b':';
    if !marks {
        return None;
    }
    let field = |range: std::ops::Range<usize>| -> Option<u32> {
        let text = &stamp[range];
        text.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| text.parse().ok())
            .flatten()
    };
    let sub = match b.get(19) {
        None => 0,
        Some(b'.') => fraction_nanos(&stamp[20..])?,
        Some(_) => return None,
    };
    let date = NaiveDate::from_ymd_opt(field(0..4)? as i32, field(5..7)?, field(8..10)?)?;
    let local = date.and_hms_opt(field(11..13)?, field(14..16)?, field(17..19)?)?;
    let secs = local.and_utc().timestamp().checked_sub(hours * 3600)?;
    secs.checked_mul(NANOS_PER_SEC_I64)?.checked_add(sub)
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

/// Whether an instant, in nanoseconds since 1970, is inside the epoch window: for a
/// reader that works the instant out for itself, as the oracle does.
pub fn in_epoch_window(ns: i128) -> bool {
    let window = epoch_window();
    window.start_ns <= ns && ns < window.end_ns
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

#[cfg(test)]
mod tests;
