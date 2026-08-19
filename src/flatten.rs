//! Flattening, fingerprinting, and the parsed row.

use std::collections::HashMap;

use serde_json::Value;

use crate::error::{DetectorError, DetectorErrorKind};
use crate::infer::infer;
use crate::types::{ColValue, ColumnType, Typed};

/// A guard against pathological or hostile input, not a real-world constraint.
/// Counted over objects only, and only outside arrays - see `MAX_ARRAY_DEPTH`.
pub const MAX_DEPTH: usize = 8;

/// The ceiling on nesting inside an array, counting containers of either kind from
/// the outermost `[`.
///
/// Arrays are not flattened, so `MAX_DEPTH` does not apply to what is inside one - but
/// "no limit" is not a thing that can be built. An array is stored as its raw JSON
/// text, which means `serde_json` has to materialise it first, and materialising is
/// recursive descent: with `serde_json`'s own limit removed, a 100 000-deep array
/// overflows the stack and aborts the process. So the choice is not whether there is a
/// ceiling, only whether it is ours and documented or `serde_json`'s and accidental.
/// This is ours. 8 + 64 leaves `serde_json`'s 128 unreachable.
pub const MAX_ARRAY_DEPTH: usize = 64;

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
pub fn flatten(root: &Value) -> Result<Vec<(String, Value)>, DetectorErrorKind> {
    let obj = match root {
        Value::Object(o) => o,
        other => {
            return Err(DetectorErrorKind::NotAnObject {
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
) -> Result<(), DetectorErrorKind> {
    for (key, value) in obj {
        let restore = prefix.len();
        if !prefix.is_empty() {
            prefix.push('.');
        }
        prefix.push_str(key);

        match value {
            Value::Object(inner) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(DetectorErrorKind::NestingTooDeep {
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

/// One open container, while the depth scan is inside it.
struct Frame {
    is_array: bool,
    /// Byte range of the member key whose value is being read at this level, quotes
    /// included. `None` at the root, inside arrays, and between `{` and the first key.
    cur_key: Option<std::ops::Range<usize>>,
    /// Whether the next string at this level is a key rather than a value.
    expect_key: bool,
}

/// Reject over-deep input before `serde_json` builds anything.
///
/// The depth rule has to be enforced on the raw bytes, because the tree `walk` descends
/// does not exist until `serde_json` has already recursed to build it. `serde_json`
/// stops at 128 nested containers and calls it a syntax error, so without this pass
/// anything deeper comes back as `InvalidJson` - valid JSON reported as malformed, and
/// the diagnostic getting less accurate exactly as the input gets more hostile. Scanning
/// first keeps both ceilings ours and reachable at any depth, including 100 000.
///
/// This is a depth scan, not a validator. Every other way a line can be wrong - bad
/// syntax, an unterminated string, trailing bytes - is left for `serde_json` to
/// describe, so those messages are unchanged.
fn check_nesting(bytes: &[u8]) -> Result<(), DetectorErrorKind> {
    let mut stack: Vec<Frame> = Vec::new();
    // Objects outside any array, which is what `MAX_DEPTH` bounds, and containers of
    // either kind from the outermost `[`, which is the ceiling on a value stored whole.
    let mut obj_depth = 0usize;
    let mut arr_depth = 0usize;
    let mut open_arrays = 0usize;

    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            // Skipped as one unit, so a brace inside a string is text, not structure.
            b'"' => {
                let start = i;
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                // Past the end only if the string was unterminated, which is
                // `serde_json`'s to report; clamp and carry on.
                let end = i.min(bytes.len());
                if let Some(frame) = stack.last_mut() {
                    if frame.expect_key {
                        frame.cur_key = Some(start..end);
                        frame.expect_key = false;
                    }
                }
                continue;
            }
            b'{' | b'[' => {
                let is_array = bytes[i] == b'[';
                // Inside an array everything below is stored as text and never walked,
                // so object depth stops counting there and the array ceiling takes over.
                if open_arrays > 0 || is_array {
                    arr_depth += 1;
                    if arr_depth > MAX_ARRAY_DEPTH {
                        return Err(DetectorErrorKind::ArrayNestingTooDeep {
                            key: key_path(&stack, bytes),
                            limit: MAX_ARRAY_DEPTH,
                        });
                    }
                } else {
                    obj_depth += 1;
                    if obj_depth > MAX_DEPTH {
                        return Err(DetectorErrorKind::NestingTooDeep {
                            key: key_path(&stack, bytes),
                            limit: MAX_DEPTH,
                        });
                    }
                }
                if is_array {
                    open_arrays += 1;
                }
                stack.push(Frame {
                    is_array,
                    cur_key: None,
                    expect_key: !is_array,
                });
            }
            b'}' | b']' => {
                if let Some(frame) = stack.pop() {
                    if frame.is_array {
                        open_arrays -= 1;
                    }
                    // `open_arrays` is now what it was when this container opened, so
                    // this picks the same counter that was incremented then.
                    if open_arrays > 0 || frame.is_array {
                        arr_depth = arr_depth.saturating_sub(1);
                    } else {
                        obj_depth = obj_depth.saturating_sub(1);
                    }
                }
            }
            b',' => {
                if let Some(frame) = stack.last_mut() {
                    if !frame.is_array {
                        frame.expect_key = true;
                        frame.cur_key = None;
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    Ok(())
}

/// The dotted path to the container the scan is about to enter, in the form `walk`
/// builds - so a rejection reads the same whichever pass caught it.
fn key_path(stack: &[Frame], bytes: &[u8]) -> String {
    let path = stack
        .iter()
        .filter_map(|f| f.cur_key.clone())
        .map(|range| key_text(&bytes[range]))
        .collect::<Vec<_>>()
        .join(".");
    if path.is_empty() {
        // No enclosing key: the offending container is the top-level value itself.
        "<top level>".to_string()
    } else {
        path
    }
}

/// A key as it reads, escapes resolved. Quotes are included in `raw`, so it is a whole
/// JSON string; only a key the scan clipped at end of input fails to decode.
fn key_text(raw: &[u8]) -> String {
    serde_json::from_slice::<String>(raw).unwrap_or_else(|_| {
        let inner = raw.strip_prefix(b"\"").unwrap_or(raw);
        String::from_utf8_lossy(inner.strip_suffix(b"\"").unwrap_or(inner)).into_owned()
    })
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
///
/// Every way this fails comes back tagged with `line_no`, so the caller can say where
/// without having to know what went wrong.
pub fn parse_line(raw_line: &[u8], line_no: usize) -> Result<ParsedLog, DetectorError> {
    parse_bytes(raw_line).map_err(|kind| DetectorError::new(line_no, kind))
}

/// `raw_line` is bytes, not `&str`: bytes that are not UTF-8 are one bad line for
/// `serde_json` to reject, not a decoding failure that takes the whole stream with it.
fn parse_bytes(raw_line: &[u8]) -> Result<ParsedLog, DetectorErrorKind> {
    // Before `from_slice`, not after: past 128 levels `serde_json` refuses to build the
    // value at all, and its refusal is a syntax error. The depth rules only stay
    // reachable if they are checked on the bytes first.
    check_nesting(raw_line)?;
    let root: Value = serde_json::from_slice(raw_line)
        .map_err(|source| DetectorErrorKind::InvalidJson { source })?;
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
/// `AtomicU64`, but there is no trie yet; a content hash is deterministic and
/// order-independent, which is all the parse surface needs.
fn template_id(fingerprint: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in fingerprint.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
