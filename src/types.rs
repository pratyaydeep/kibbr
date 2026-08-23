use serde::{Deserialize, Serialize};

/// Note there is no `Null` column type: a column has one type, and absence
/// on a given line is expressed by `ColValue::Null`, not by a different type.
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

    /// The short form the trie tree prints, where a leaf carries its whole key set and
    /// the full names would not fit beside it.
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

/// The type join. **The ladder types a value; a column has one type**, and this is the
/// rule that turns the second into the first:
///
/// ```text
/// ⊥ = None                        no evidence; a null claims nothing about the column
/// None  ⊔ T        = T
/// T     ⊔ T        = T
/// Int64 ⊔ Float64  = Float64
/// _     ⊔ _        = String       ⊤
/// ```
///
/// **Commutative and associative**, which is the entire point - the column type must not
/// depend on the order lines arrived in, and the trie is checked by feeding one file's
/// lines in several orders and diffing the tree. A rule that read differently forwards
/// and backwards would not merely fail that check, it would make it unmeasurable.
///
/// **Monotonic - a column never narrows.** Narrowing is what would reintroduce order
/// dependence, so it stays forbidden even where it looks obviously right. The price is
/// that one `"N/A"` in a field of ten million clean timestamps turns that column to
/// String permanently and sends every time-range query on it to a full scan. It is also
/// how rung 2's known misclassification corrects itself: a real timestamp field is
/// consistently ten or thirteen digits and stays `Datetime`, while a ten-digit order id
/// meets its first `AX-99213` and widens back to `String` on its own - no heuristic, no
/// second pass, no sampling.
///
/// `Int64 ⊔ Float64 = Float64` loses precision above 2^53. Accepted: the alternative is
/// String, which is worse for every query anyone would actually run.
pub fn join(a: Option<ColumnType>, b: Option<ColumnType>) -> Option<ColumnType> {
    match (a, b) {
        // `None` does two jobs and this is the second: the identity element of ⊔. Both
        // sides `None` lands here too, and a column that has only ever held nulls keeps
        // type `None` - a different statement from `String`.
        (None, other) | (other, None) => other,
        (Some(x), Some(y)) if x == y => Some(x),
        (Some(ColumnType::Int64), Some(ColumnType::Float64))
        | (Some(ColumnType::Float64), Some(ColumnType::Int64)) => Some(ColumnType::Float64),
        _ => Some(ColumnType::String),
    }
}

/// The decoded value behind a `ColumnType`.
#[derive(Debug, Clone, PartialEq)]
pub enum ColValue {
    String(String),
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
            ColValue::Int64(i) => Value::from(*i),
            // Inference only produces finite floats (rule 7), so this cannot
            // fall through in practice; `Null` is the honest answer if it ever does.
            ColValue::Float64(f) => serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number),
            ColValue::Datetime(ns) => Value::from(*ns),
            ColValue::Bool(b) => Value::Bool(*b),
            ColValue::Null => Value::Null,
        }
    }
}

/// One value on one line: the type the type ladder chose, plus the decoded value.
///
/// `ty` is `None` only for an explicit JSON `null`. A null carries no evidence about
/// the column's type, so the ladder is not run on it and nothing is claimed.
#[derive(Debug, Clone, PartialEq)]
pub struct Typed {
    pub ty: Option<ColumnType>,
    pub value: ColValue,
}

impl Typed {
    pub fn label(&self) -> &'static str {
        match self.ty {
            Some(t) => t.as_str(),
            None => "Null",
        }
    }
}
