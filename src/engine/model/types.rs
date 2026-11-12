// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use serde::{Deserialize, Serialize};

/// A column has one type; absence on a line is `ColValue::Null`, not a `Null` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    String,
    Int64,
    Float64,
    Datetime,
    Bool,
    Uuid,
    Ip,
}

impl ColumnType {
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnType::String => "String",
            ColumnType::Int64 => "Int64",
            ColumnType::Float64 => "Float64",
            ColumnType::Datetime => "Datetime",
            ColumnType::Bool => "Bool",
            ColumnType::Uuid => "Uuid",
            ColumnType::Ip => "Ip",
        }
    }

    /// The short form the trie tree prints; a leaf carries its whole key set, and the
    /// full names would not fit beside it.
    pub fn short(self) -> &'static str {
        match self {
            ColumnType::String => "Str",
            ColumnType::Int64 => "I64",
            ColumnType::Float64 => "F64",
            ColumnType::Datetime => "DT",
            ColumnType::Bool => "Bool",
            ColumnType::Uuid => "Uuid",
            ColumnType::Ip => "Ip",
        }
    }
}

/// The type join that turns per-value types into one column type: commutative,
/// associative and monotonic, so arrival order cannot matter.
pub fn join(a: Option<ColumnType>, b: Option<ColumnType>) -> Option<ColumnType> {
    match (a, b) {
        // `None` is the identity of ⊔, so a column of only nulls keeps type `None`, which is
        // not `String`.
        (None, other) | (other, None) => other,
        (Some(x), Some(y)) if x == y => Some(x),
        (Some(ColumnType::Int64), Some(ColumnType::Float64))
        | (Some(ColumnType::Float64), Some(ColumnType::Int64)) => Some(ColumnType::Float64),
        _ => Some(ColumnType::String),
    }
}

/// What one more value makes of its column: the type, and for a `Datetime` column whether
/// every value so far was rule 2's digits. It is `join`, except that such digits and an
/// integer are one column of integers, where the join alone would make them `String`.
///
/// `digits` is what the column says of itself: `Some(true)` while every value has been ten
/// or thirteen digits, `Some(false)` once one was RFC 3339 text - which no integer column
/// holds - and `None` where it was never recorded: a column that is not `Datetime`, or one
/// from a catalog written before this was kept. An integer arriving in a `Datetime` column
/// makes it `Int64` unless the column is known to hold RFC 3339 dates; for a column that
/// does not say, the integer is taken as the sign that its values are counts.
/// `epoch_digits` says the same of the incoming value.
pub fn widen(
    column: Option<ColumnType>,
    digits: Option<bool>,
    incoming: Option<ColumnType>,
    epoch_digits: bool,
) -> (Option<ColumnType>, Option<bool>) {
    use ColumnType::{Datetime, Int64};
    match (column, incoming) {
        // A null claims nothing about the column.
        (_, None) => (column, digits),
        (None, Some(Datetime)) => (incoming, Some(epoch_digits)),
        // One RFC 3339 date settles it for good; digits leave it as it was.
        (Some(Datetime), Some(Datetime)) if epoch_digits => (column, digits),
        (Some(Datetime), Some(Datetime)) => (column, Some(false)),
        (Some(Datetime), Some(Int64)) if digits != Some(false) => (Some(Int64), None),
        (Some(Int64), Some(Datetime)) if epoch_digits => (Some(Int64), None),
        _ => (join(column, incoming), None),
    }
}

/// The decoded value behind a `ColumnType`.
#[derive(Debug, Clone, PartialEq)]
pub enum ColValue {
    String(String),
    /// An array, or an object with no members, as its JSON text: a value of its own kind,
    /// so `["a"]` is not the string `"[\"a\"]"`. Its column is a `String` column, and a
    /// query compares it as its text.
    Json(String),
    Int64(i64),
    Float64(f64),
    /// Unix nanoseconds.
    Datetime(i64),
    Bool(bool),
    /// Optional column absent on this line, or an explicit JSON `null`.
    Null,
}

impl ColValue {
    /// The `typed_values` form.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::Value;
        match self {
            ColValue::String(s) => Value::String(s.clone()),
            // The text is JSON this build wrote or checked on reading; text that is not
            // is shown as the string it is.
            ColValue::Json(text) => {
                serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.clone()))
            }
            ColValue::Int64(i) => Value::from(*i),
            // Rule 7 only produces finite floats, so this cannot fall through in
            // practice; `Null` is the honest answer if it ever does.
            ColValue::Float64(f) => {
                serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number)
            }
            ColValue::Datetime(ns) => Value::from(*ns),
            ColValue::Bool(b) => Value::Bool(*b),
            ColValue::Null => Value::Null,
        }
    }

    /// The value as a query prints it: `to_json`, except a `Datetime` is RFC 3339 through
    /// `display`.
    pub fn printed(&self) -> serde_json::Value {
        match self {
            ColValue::Datetime(ns) => {
                serde_json::Value::String(crate::engine::display::rfc3339(*ns))
            }
            other => other.to_json(),
        }
    }
}

/// Which rung of the ladder read a `Datetime`, so promoting the column to `String` can
/// write back the text that arrived, and to `Int64` the count it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DtForm {
    /// Rows in this partition came from more than one rung. Promotion falls back to
    /// canonical RFC 3339.
    Mixed,
    Rfc3339,
    EpochSecs,
    EpochMillis,
}

/// One value on one line: the ladder's type, `None` for a JSON null, and the decoded
/// value; `dt_form` rides outside `ColValue` so `engine::value_matches` ignores the rung.
#[derive(Debug, Clone, PartialEq)]
pub struct Typed {
    pub ty: Option<ColumnType>,
    pub value: ColValue,
    pub dt_form: Option<DtForm>,
}

impl Typed {
    /// Whether rule 2 read this value: ten or thirteen digits, which a column of integers
    /// holds as the integer they spell.
    pub fn is_epoch_digits(&self) -> bool {
        matches!(self.dt_form, Some(DtForm::EpochSecs | DtForm::EpochMillis))
    }

    pub fn label(&self) -> &'static str {
        match self.ty {
            Some(t) => t.as_str(),
            None => "Null",
        }
    }
}

/// Where a template's rows get their time from, decided once at creation and recorded in
/// the catalog so a restart orders rows the same way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeSource {
    Column(String),
    Arrival,
}

/// The system column holding a row's arrival time, spelled with `\_` so no flattened key
/// can collide with it.
pub const RECEIVED_AT_COLUMN: &str = "\\_received_at";

impl TimeSource {
    /// The catalog's spelling: `column:<name>` or `arrival`.
    pub fn as_record(&self) -> String {
        match self {
            TimeSource::Column(name) => format!("column:{name}"),
            TimeSource::Arrival => "arrival".to_string(),
        }
    }

    /// `as_record` read back. `None` for a spelling this build does not know.
    pub fn from_record(text: &str) -> Option<Self> {
        match text {
            "arrival" => Some(TimeSource::Arrival),
            _ => text
                .strip_prefix("column:")
                .map(|n| TimeSource::Column(n.to_string())),
        }
    }

    /// The rule, over the types a template holds per key: the first configured name that
    /// is a `Datetime`. What a catalog with no `time_source` record is read by.
    pub fn choose<'a>(
        time_columns: &[String],
        type_of: impl Fn(&str) -> Option<ColumnType> + 'a,
    ) -> Self {
        Self::choose_by(time_columns, |name| {
            type_of(name) == Some(ColumnType::Datetime)
        })
    }

    /// The rule, over the line that makes a template: the first configured name whose
    /// value on that line `is_time` - a `Datetime`, or a form only a time column reads.
    pub fn choose_by(time_columns: &[String], is_time: impl Fn(&str) -> bool) -> Self {
        time_columns
            .iter()
            .find(|name| is_time(name))
            .map_or(TimeSource::Arrival, |name| TimeSource::Column(name.clone()))
    }

    /// The column the store partitions, orders and windows on: the named one, or the
    /// system column a row on arrival time carries.
    pub fn column(&self) -> &str {
        match self {
            TimeSource::Column(name) => name,
            TimeSource::Arrival => RECEIVED_AT_COLUMN,
        }
    }
}

#[cfg(test)]
mod tests;
