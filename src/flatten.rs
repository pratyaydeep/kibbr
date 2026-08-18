//! Flattening, fingerprinting, and the parsed row.

use std::collections::HashMap;

use serde_json::Value;

use crate::error::DetectorError;
use crate::infer::infer;
use crate::types::{ColValue, ColumnType, Typed};

/// A guard against pathological or hostile input, not a real-world constraint.
pub const MAX_DEPTH: usize = 8;

/// Keys whose values are low-cardinality enough to be promoted into the fingerprint
/// itself as `key=value`, which is what makes the trie branch usefully.
///
/// Which keys belong here is still an open question, so it holds at the single key
/// there is a worked example for and nothing else. Widening it changes every
/// template id in the system, so it wants evidence, not a guess.
const PROMOTED_KEYS: &[&str] = &["event"];

/// The longest promoted value; beyond this it is not a dimension, it is a payload.
const MAX_PROMOTED_LEN: usize = 40;

/// One line, flattened, fingerprinted and typed.
#[derive(Debug, Clone)]
pub struct ParsedLog {
    pub template_id: u64,
    /// Sorted, flattened. Includes promoted keys.
    pub keys: Vec<String>,
    /// Raw text of every value, promoted keys included.
    pub values: HashMap<String, String>,
    /// Inferred, for the keys that stayed values.
    pub typed_values: HashMap<String, Typed>,
    /// Keys promoted into the fingerprint, in fingerprint order.
    pub promoted: Vec<String>,
    pub fingerprint: String,
    pub is_new_template: bool,
    pub raw_line: Vec<u8>,
}

impl ParsedLog {
    /// The keys that stayed values, in sorted order.
    pub fn value_keys(&self) -> impl Iterator<Item = &String> {
        self.keys.iter().filter(|k| !self.promoted.contains(k))
    }

    /// `typed_values` in the JSON form the parsed row carries.
    pub fn typed_values_json(&self) -> serde_json::Map<String, Value> {
        self.typed_values
            .iter()
            .map(|(k, t)| (k.clone(), t.value.to_json()))
            .collect()
    }
}

/// Flatten one JSON object into sorted dotted keys paired with their leaf values.
/// Arrays are never flattened - they are stored as their raw JSON string, because
/// flattening them would make the fingerprint depend on array length.
pub fn flatten(root: &Value) -> Result<Vec<(String, Value)>, DetectorError> {
    let obj = match root {
        Value::Object(o) => o,
        other => {
            return Err(DetectorError::NotAnObject {
                found: kind_of(other),
            })
        }
    };
    let mut out = Vec::new();
    walk(obj, &mut String::new(), 1, &mut out)?;
    // Sorting here is what makes the fingerprint order-independent: two lines from
    // the same call site agree even if the serializer shuffled field order.
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn walk(
    obj: &serde_json::Map<String, Value>,
    prefix: &mut String,
    depth: usize,
    out: &mut Vec<(String, Value)>,
) -> Result<(), DetectorError> {
    for (key, value) in obj {
        let restore = prefix.len();
        if !prefix.is_empty() {
            prefix.push('.');
        }
        prefix.push_str(key);

        match value {
            Value::Object(inner) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(DetectorError::NestingTooDeep {
                        key: prefix.clone(),
                        limit: MAX_DEPTH,
                    });
                }
                walk(inner, prefix, depth + 1, out)?;
            }
            other => out.push((prefix.clone(), other.clone())),
        }

        prefix.truncate(restore);
    }
    Ok(())
}

/// The raw text of a leaf value. Strings are their own contents (not re-quoted);
/// everything else is its JSON encoding, which is how arrays survive intact.
pub fn raw_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Flatten, type, promote, fingerprint. `raw_line` is kept verbatim.
pub fn parse_line(raw_line: &[u8], line_no: usize) -> Result<ParsedLog, DetectorError> {
    let root: Value =
        serde_json::from_slice(raw_line).map_err(|source| DetectorError::InvalidJson {
            line: line_no,
            source,
        })?;
    let pairs = flatten(&root)?;

    let mut keys = Vec::with_capacity(pairs.len());
    let mut values = HashMap::with_capacity(pairs.len());
    let mut typed_values = HashMap::new();
    let mut promoted = Vec::new();
    let mut tokens = Vec::with_capacity(pairs.len());

    for (key, leaf) in pairs {
        let raw = raw_text(&leaf);
        // An explicit JSON null carries no evidence about the column's type, so the
        // type ladder is not run on it - otherwise it would come back String and
        // pin the column to the four-character text "null".
        let typed = if leaf.is_null() {
            Typed {
                ty: None,
                value: ColValue::Null,
            }
        } else {
            infer(&raw)
        };

        if is_promotable(&key, &typed, &raw) {
            tokens.push(format!("{key}={raw}"));
            promoted.push(key.clone());
        } else {
            tokens.push(key.clone());
            typed_values.insert(key.clone(), typed);
        }

        values.insert(key.clone(), raw);
        keys.push(key);
    }

    let fingerprint = tokens.join(" | ");
    Ok(ParsedLog {
        template_id: template_id(&fingerprint),
        keys,
        values,
        typed_values,
        promoted,
        fingerprint,
        // Filled in by the caller, which is the only thing that knows what it has
        // already seen.
        is_new_template: false,
        raw_line: raw_line.to_vec(),
    })
}

fn is_promotable(key: &str, typed: &Typed, raw: &str) -> bool {
    let leaf = key.rsplit('.').next().unwrap_or(key);
    PROMOTED_KEYS.contains(&leaf)
        && typed.ty == Some(ColumnType::String)
        && !raw.is_empty()
        && raw.len() <= MAX_PROMOTED_LEN
        // A value containing the delimiter or whitespace would make the fingerprint
        // ambiguous to read and to split.
        && !raw.contains('|')
        && !raw.chars().any(char::is_whitespace)
}

/// FNV-1a over the fingerprint. The trie hands out sequential ids from an
/// `AtomicU64`, but there is no trie until M1; a content hash is
/// deterministic and order-independent, which is all M0 needs.
fn template_id(fingerprint: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in fingerprint.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
