//! Type inference. First match wins; the order is the contract.
//!
//! ```text
//! 1.  parses as RFC 3339, and the type holds
//!     that instant exactly                        -> Datetime
//! 2.  exactly 10 or 13 ASCII digits, no leading
//!     zero, naming a moment inside the epoch
//!     window that the type holds                  -> Datetime  (seconds / millis)
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
//!
//! A value the typed form cannot hold exactly is not a member of that type: it falls
//! through to the rungs below, and for anything shaped like a date that means text.
//! Nothing is clamped, rounded, rolled forward or truncated, because each of those can
//! store a different value from the one ingested, not a different spelling of it.

use std::net::IpAddr;
use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset};

use crate::types::{ColValue, ColumnType, DtForm, Typed};

/// Run the ladder against a raw value. Every rung is written out separately and in
/// order; do not collapse them, the order is the thing being specified.
pub fn infer(raw: &str) -> Typed {
    // 1. RFC 3339, where the instant it names is one the type holds exactly.
    if let Some(ns) = rfc3339_nanos(raw) {
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
            // Nanoseconds run out in 2262, which only a window reaching past it admits.
            // Past that the digits fall through like any outside the window: a clamped
            // instant would be another moment, and every such value the same one.
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

/// The instant an RFC 3339 value names, in the `i64` nanoseconds a `Datetime` holds, or
/// `None` when it is not RFC 3339 or the type cannot hold that instant exactly.
///
/// The parser accepts three things the type cannot keep, each of which would store
/// another value rather than another spelling of this one:
///
/// - a second of 60, held as a full second past `:59`, which comes back as the next
///   minute - on any day, leap second or not;
/// - more than nine fraction digits, the ones past the ninth dropped. Dropping zeros
///   loses nothing, but the rule counts digits rather than reading them, so whether a
///   value is a `Datetime` never turns on which digit would have been lost;
/// - any year from 0 to 9999, where i64 nanoseconds run only from 1677 to 2262.
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

/// Ten or thirteen ASCII digits, the first of them not a zero.
///
/// A count of seconds or milliseconds since 1970 is not written with a leading zero; ten
/// digits that start with one are an id kept at a fixed width, and read as an epoch they
/// come back as a date nobody wrote. They land on rule 6 instead. Under the default window
/// the ones it would have admitted name 2000-01-01 to 2001-09-09, so its lower edge is
/// out of reach of ten and thirteen digits.
fn is_epoch_digits(s: &str) -> bool {
    (s.len() == 10 || s.len() == 13)
        && !s.starts_with('0')
        && s.bytes().all(|b| b.is_ascii_digit())
}

/// The moments rule 2 may read ten or thirteen digits as: `[start, end)`.
///
/// By default 2000-01-01 up to but not including 2100-01-01, which costs real timestamps
/// nothing - no log line is stamped before 2000 or after 2100 - and sends most ten-digit
/// ids to `Int64`: an order id naming 2112, a phone number naming 2282.
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
