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
/// Commutative and associative, so a column's type does not depend on arrival order -
/// the suite checks this by feeding one file's lines in several orders and diffing the
/// tree. A rule that read differently backwards would not merely fail that check, it
/// would make it unmeasurable.
///
/// Monotonic: a column never narrows, because narrowing is what would reintroduce order
/// dependence - forbidden even where it looks obviously right. Price: one `"N/A"` among
/// ten million clean timestamps pins that column to String permanently and sends every
/// time-range query on it to a full scan. Payoff: it is how rung 2's known
/// misclassification self-corrects - a real timestamp field stays `Datetime`, while a
/// ten-digit order id meets its first `AX-99213` and widens back to `String` on its own,
/// with no heuristic, second pass or sampling.
///
/// `Int64 ⊔ Float64 = Float64` loses precision above 2^53. Accepted: String is worse for
/// every query anyone would actually run.
pub fn join(a: Option<ColumnType>, b: Option<ColumnType>) -> Option<ColumnType> {
    match (a, b) {
        // `None`'s second job: the identity element of ⊔. Both sides `None` land here
        // too, so a column that has only ever held nulls keeps type `None` - a different
        // statement from `String`.
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
}

/// Which rung of the ladder read a `Datetime`, so a later widening can be undone.
///
/// A `Datetime` column stores nanoseconds and throws the text away. Promoting one to
/// `String` therefore has to *reconstruct* the text, and rendering a ten-digit order id
/// that rung 2 mistook for a timestamp as `2112-08-16T...` is not the string that was
/// ingested. Recording the rung makes the promotion invert the rung instead.
///
/// Leading zeros are lost in every case - `0012345678` parses to the same nanoseconds as
/// `12345678` and no tag brings it back. That loss happens at parse time, which is why
/// this has to travel from the ladder rather than be worked out at write time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DtForm {
    /// Rows in this partition came from more than one rung. Promotion falls back to
    /// canonical RFC 3339.
    Mixed,
    Rfc3339,
    EpochSecs,
    EpochMillis,
}

impl DtForm {
    /// The `.col` header byte, in the order the storage format lists them.
    pub fn byte(self) -> u8 {
        match self {
            DtForm::Mixed => 0,
            DtForm::Rfc3339 => 1,
            DtForm::EpochSecs => 2,
            DtForm::EpochMillis => 3,
        }
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(DtForm::Mixed),
            1 => Some(DtForm::Rfc3339),
            2 => Some(DtForm::EpochSecs),
            3 => Some(DtForm::EpochMillis),
            _ => None,
        }
    }

    /// The form of a whole partition, folded one row at a time.
    ///
    /// Set only where every row came from the same rung: two rungs in one partition is
    /// `Mixed`, and `Mixed` absorbs everything after it. `None` is the identity, as it is
    /// in the type lattice - a column with no datetimes yet claims nothing.
    pub fn merge(a: Option<DtForm>, b: Option<DtForm>) -> Option<DtForm> {
        match (a, b) {
            (None, other) | (other, None) => other,
            (Some(x), Some(y)) if x == y => Some(x),
            _ => Some(DtForm::Mixed),
        }
    }

    /// The text a promotion to `String` should write, inverting the rung.
    ///
    /// The RFC 3339 case is *canonical*, not original: the offset the sender wrote and any
    /// sub-second digits beyond what chrono renders are already gone. Accepted - the
    /// alternative is keeping the raw text beside every timestamp, which is the column
    /// store this one exists instead of.
    pub fn render(self, ns: i64) -> String {
        match self {
            DtForm::EpochSecs => (ns / 1_000_000_000).to_string(),
            DtForm::EpochMillis => (ns / 1_000_000).to_string(),
            DtForm::Rfc3339 | DtForm::Mixed => chrono::DateTime::from_timestamp_nanos(ns)
                .to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        }
    }
}

/// One value on one line: the type the type ladder chose, plus the decoded value.
///
/// `ty` is `None` only for an explicit JSON `null`. A null carries no evidence about
/// the column's type, so the ladder is not run on it and nothing is claimed.
///
/// `dt_form` is `Some` exactly when `ty` is `Some(Datetime)`. It rides here rather than
/// inside `ColValue::Datetime` because `engine::value_matches` compares a stored
/// `ColValue` against `infer(text).value`: folding the rung into the variant would stop
/// `ts=1705314600` matching a row that was ingested as RFC 3339, which is a silent change
/// to what a query means.
#[derive(Debug, Clone, PartialEq)]
pub struct Typed {
    pub ty: Option<ColumnType>,
    pub value: ColValue,
    pub dt_form: Option<DtForm>,
}

impl Typed {
    pub fn label(&self) -> &'static str {
        match self.ty {
            Some(t) => t.as_str(),
            None => "Null",
        }
    }
}
