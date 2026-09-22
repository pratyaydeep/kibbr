//! Type inference. First match wins; the order is the contract.
//!
//! ```text
//! 1.  parses as RFC 3339                          -> Datetime
//! 2.  exactly 10 or 13 ASCII digits, naming a
//!     moment inside the epoch window              -> Datetime  (seconds / millis)
//! 3.  matches the UUID pattern                    -> Uuid
//! 4.  parses as IpAddr (v4 or v6)                 -> Ip
//! 5.  contains @ and matches email                -> String
//! 6.  parses as i64                               -> Int64
//! 7.  parses as f64                               -> Float64
//! 8.  true / True / false / False                 -> Bool
//! 9.  anything else                               -> String
//! ```
//!
//! Rule 2 sits above rule 6 on purpose: a ten-digit number is a Unix timestamp before it
//! is an integer, because getting that wrong turns every time-range query into a full
//! scan. The window keeps it to numbers that name a moment a log line could carry, so an
//! order id naming 2112 falls through to rule 6. One naming a moment this century is
//! still taken for a date - the accepted trade, and the column join corrects it the first
//! time an id of another shape arrives.

use std::net::IpAddr;
use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset};

use crate::types::{ColValue, ColumnType, DtForm, Typed};

/// Run the ladder against a raw value. Every rung is written out separately and in
/// order; do not collapse them, the order is the thing being specified.
pub fn infer(raw: &str) -> Typed {
    // 1. RFC 3339.
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        // i64 nanoseconds run from 1677 to 2262; RFC 3339 runs from year 0 to 9999. Both
        // ends overflow, and `timestamp_nanos_opt` reports them alike, so the direction
        // has to come from the seconds - in range for every datetime chrono can hold.
        // Clamping both ends to `i64::MAX` would file the pre-1677 era in 2262, the far
        // end of the range it fell off.
        let ns = dt.timestamp_nanos_opt().unwrap_or(if dt.timestamp() < 0 {
            i64::MIN
        } else {
            i64::MAX
        });
        return dated(ColValue::Datetime(ns), DtForm::Rfc3339);
    }

    // 2. Exactly 10 or 13 ASCII digits -> seconds / milliseconds since the epoch, if the
    //    moment they name is inside the window. Above rule 6 deliberately. Outside it the
    //    digits fall through, neither refused nor text: rule 6 reads them as the integer
    //    they are.
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
            // 9_999_999_999 seconds in nanoseconds overflows i64 (nanos run out in
            // 2262), which only a window reaching past 2262 admits. Saturate rather than
            // wrap: the type call stands, nothing silently becomes a negative timestamp,
            // and every value past that end lands on the same clamped instant - the price
            // of a window wider than the type the instant is stored in.
            let ns = digits.saturating_mul(per_unit);
            return dated(ColValue::Datetime(ns), form);
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

/// A `Datetime`, tagged with the rung that read it. Only rungs 1 and 2 produce one, and
/// the tag is what lets a later promotion to `String` write back the shape that arrived
/// rather than a canonical timestamp - see `DtForm`.
fn dated(value: ColValue, form: DtForm) -> Typed {
    Typed {
        ty: Some(ColumnType::Datetime),
        value,
        dt_form: Some(form),
    }
}

fn is_epoch_digits(s: &str) -> bool {
    (s.len() == 10 || s.len() == 13) && s.bytes().all(|b| b.is_ascii_digit())
}

/// The moments rule 2 may read ten or thirteen digits as: `[start, end)`.
///
/// By default 2000-01-01 up to but not including 2100-01-01, which costs real timestamps
/// nothing - no log line is stamped before 2000 or after 2100 - and sends most ten-digit
/// ids to `Int64`: an order id naming 2112, a phone number naming 2282, ten digits with a
/// leading zero naming the 1970s.
///
/// It decides what type is *stored*, and a column never narrows: a window turned on over
/// data typed without one makes every misread id column `Datetime` joined with `Int64`,
/// which is `String`, for good.
///
/// Nanoseconds in an `i128`, because either end may be any RFC 3339 instant and an `i64`
/// of nanoseconds runs out in 2262.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochWindow {
    start_ns: i128,
    end_ns: i128,
}

const NANOS_PER_SEC: i128 = 1_000_000_000;

impl EpochWindow {
    /// `None` unless `start` is before `end`. An empty window is refused rather than read
    /// as "rule 2 off": it would type every epoch timestamp as an integer and send every
    /// time-range query on it to a full scan, the failure rule 2 exists to prevent.
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

/// Fix the window for the rest of the process. The first call wins, and a process that
/// never makes one reads the default.
///
/// Once, because the ladder is re-run on stored text - to match a query term, to read a
/// row's place in time - and a window that moved while the process ran would give one
/// text two types, so a stored value would stop matching itself.
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
