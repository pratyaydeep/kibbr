// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Flattening, fingerprinting, and the parsed row.

use std::collections::HashMap;
use std::fmt;

use serde_json::error::Category;
use serde_json::Value;

use crate::engine::error::{BadByte, DetectorError, DetectorErrorKind};
use crate::engine::model::infer::{infer, time_form_nanos};
use crate::engine::model::tokens::{key_token, promoted_token};
use crate::engine::model::row::Row;
use crate::engine::parse::key::KeyPath;
use crate::engine::model::types::{ColValue, ColumnType, Typed};

/// A guard against hostile input, counted over objects outside arrays.
pub const MAX_DEPTH: usize = 8;

/// The ceiling inside an array, which is stored whole and so parsed recursively: ours
/// and documented, below `serde_json`'s own.
pub const MAX_ARRAY_DEPTH: usize = 64;

/// Keys low-cardinality enough to be promoted into the fingerprint as `key=value`.
/// Widening it moves every template id, so it wants evidence, not a guess.
const PROMOTED_KEYS: &[&str] = &["event"];

/// The longest promoted value; beyond this it is not a dimension, it is a payload.
const MAX_PROMOTED_LEN: usize = 40;

/// One element of the fingerprint; `template_id` tells the two kinds apart by a tag,
/// the text by `key_token`.
enum Token {
    /// A key that stayed a value. Its value varies line to line and is not in here.
    Key(String),
    /// A key promoted into the fingerprint, carrying the value that branches the trie.
    Promoted { key: String, value: String },
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Key(key) => f.write_str(&key_token(key)),
            Token::Promoted { key, value } => f.write_str(&promoted_token(key, value)),
        }
    }
}

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
    /// The JSON value the line wrote, for each key whose stored value would not print it
    /// back.
    pub written: HashMap<String, Value>,
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

    /// The trie path: promoted tokens first, then the rest sorted, so a query's pattern is
    /// a prefix of it.
    pub fn walk_tokens(&self) -> Vec<String> {
        let mut tokens = Vec::with_capacity(self.keys.len());
        // `promoted` is already in key order: it is filled while walking `keys`, and
        // `flatten` sorted those.
        for key in &self.promoted {
            let value = self.values.get(key).map_or("", String::as_str);
            tokens.push(promoted_token(key, value));
        }
        for key in self.value_keys() {
            tokens.push(key_token(key).into_owned());
        }
        tokens
    }

    /// What this line's value for `key` contributes to the column's type join: `None` for
    /// a null, `String` for a promoted key.
    pub fn column_type(&self, key: &str) -> Option<ColumnType> {
        match self.typed_values.get(key) {
            Some(typed) => typed.ty,
            None if self.promoted.iter().any(|p| p == key) => Some(ColumnType::String),
            None => None,
        }
    }

    /// Whether this line's value for `key` is digits rule 2 read as an instant.
    pub fn is_epoch_digits(&self, key: &str) -> bool {
        self.typed_values
            .get(key)
            .is_some_and(Typed::is_epoch_digits)
    }

    /// Whether a time column reads this line's value for `key` as a time: a `Datetime`,
    /// or one of the forms only a time column reads. Asked of the configured names alone,
    /// where a template's time source is chosen; the column's type is not changed by it.
    pub fn reads_as_time(&self, key: &str) -> bool {
        match self.typed_values.get(key) {
            Some(typed) if typed.ty == Some(ColumnType::Datetime) => true,
            Some(typed) if typed.ty.is_some() => self
                .values
                .get(key)
                .is_some_and(|raw| time_form_nanos(raw).is_some()),
            _ => false,
        }
    }

    /// `typed_values` in the JSON form the parsed row carries.
    pub fn typed_values_json(&self) -> serde_json::Map<String, Value> {
        self.typed_values
            .iter()
            .map(|(k, t)| (k.clone(), t.value.to_json()))
            .collect()
    }
}

/// Flatten one JSON object into sorted key paths and leaf values; arrays stay whole, or
/// the fingerprint would depend on their length, and an object with no members is a leaf,
/// or it would be no key at all and could not be given back.
pub fn flatten(root: &Value) -> Result<Vec<(KeyPath, Value)>, DetectorErrorKind> {
    let obj = match root {
        Value::Object(o) => o,
        other => {
            return Err(DetectorErrorKind::NotAnObject {
                found: kind_of(other),
            })
        }
    };
    let mut out = Vec::new();
    walk(obj, &mut KeyPath::new(), 1, &mut out)?;
    // Sorted so the fingerprint is order-independent, and on the rendered key because
    // that is the order `keys` is emitted in.
    out.sort_by_cached_key(|(path, _)| path.to_string());
    Ok(out)
}

fn walk(
    obj: &serde_json::Map<String, Value>,
    path: &mut KeyPath,
    depth: usize,
    out: &mut Vec<(KeyPath, Value)>,
) -> Result<(), DetectorErrorKind> {
    for (key, value) in obj {
        path.push(key);

        match value {
            Value::Object(inner) => {
                if depth + 1 > MAX_DEPTH {
                    return Err(DetectorErrorKind::NestingTooDeep {
                        key: path.to_string(),
                        limit: MAX_DEPTH,
                    });
                }
                // An object with no members has no key to walk to: it is a leaf, and
                // its value is itself.
                if inner.is_empty() {
                    out.push((path.clone(), value.clone()));
                } else {
                    walk(inner, path, depth + 1, out)?;
                }
            }
            other => out.push((path.clone(), other.clone())),
        }

        path.pop();
    }
    Ok(())
}

/// One open container, while the depth scan is inside it.
struct Frame {
    is_array: bool,
    /// Byte range of the member key being read at this level, quotes included.
    cur_key: Option<std::ops::Range<usize>>,
    /// Whether the next string at this level is a key rather than a value.
    expect_key: bool,
}

/// Reject over-deep input on the raw bytes, before `serde_json` recurses to build it
/// and reports valid JSON as a syntax error.
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

/// The path to the container the scan is entering, rendered by `KeyPath` so a rejection
/// reads the same whichever pass caught it.
fn key_path(stack: &[Frame], bytes: &[u8]) -> String {
    let path: KeyPath = stack
        .iter()
        .filter_map(|f| f.cur_key.clone())
        .map(|range| key_text(&bytes[range]))
        .collect();
    if path.is_empty() {
        // No enclosing key: the offending container is the top-level value itself.
        "<top level>".to_string()
    } else {
        path.to_string()
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

/// The raw text of a leaf value: a string's contents, anything else as its JSON encoding.
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

/// Flatten, type, promote and fingerprint one line, every failure tagged with `line_no`.
pub fn parse_line(raw_line: &[u8], line_no: usize) -> Result<ParsedLog, DetectorError> {
    parse_bytes(raw_line).map_err(|kind| DetectorError::new(line_no, kind))
}

/// Decide what a failed parse was about: bad encoding, bad syntax, or both. A syntax
/// verdict before `valid_up_to` is sound; one at or past it is a symptom of the bad byte.
fn classify_failure(raw_line: &[u8], source: serde_json::Error) -> DetectorErrorKind {
    let Err(utf8) = std::str::from_utf8(raw_line) else {
        return invalid_json(&source, None);
    };
    let bad = BadByte {
        // `valid_up_to` is always in bounds here: the whole line would have decoded
        // otherwise, and then there would be no error to classify.
        byte: raw_line[utf8.valid_up_to()],
        offset: utf8.valid_up_to(),
    };

    // A line cut mid-character is a truncated line, not an encoding fault:
    // `error_len() == None` with `Eof` says the write ended early.
    if utf8.error_len().is_none() && source.classify() == Category::Eof {
        return invalid_json(&source, None);
    }

    match split_position(&source).0 {
        // A syntax error inside the decoded region is a separate fault: report both.
        Some(offset) if offset < bad.offset => invalid_json(&source, Some(bad)),
        // At or past the boundary `serde_json` tripped over the corruption itself.
        _ => DetectorErrorKind::InvalidUtf8 { bad },
    }
}

fn invalid_json(source: &serde_json::Error, also_bad_bytes: Option<BadByte>) -> DetectorErrorKind {
    let (offset, message) = split_position(source);
    DetectorErrorKind::InvalidJson {
        offset,
        message,
        also_bad_bytes,
    }
}

/// `serde_json`'s message and its position, separated. Its line is always 1, so only
/// the column is kept, as the 0-based byte offset `BadByte` reports.
fn split_position(e: &serde_json::Error) -> (Option<usize>, String) {
    let text = e.to_string();
    if e.line() == 0 {
        return (None, text);
    }
    match text.rsplit_once(" at line ") {
        Some((message, _)) => (Some(e.column().saturating_sub(1)), message.to_string()),
        // The format changed under us: hand back the message whole. No assertion - a rejected
        // line must not end the run; `a_bad_line_still_reports_its_position` catches it.
        None => (None, text),
    }
}

/// `raw_line` is bytes, not `&str`: bytes that are not UTF-8 are one bad line for
/// `serde_json` to reject, not a decoding failure that takes the whole stream with it.
fn parse_bytes(raw_line: &[u8]) -> Result<ParsedLog, DetectorErrorKind> {
    // Before `from_slice`, which past its own depth limit calls nesting a syntax error.
    check_nesting(raw_line)?;
    let root: Value =
        serde_json::from_slice(raw_line).map_err(|source| classify_failure(raw_line, source))?;
    let pairs = flatten(&root)?;

    let mut keys = Vec::with_capacity(pairs.len());
    let mut values = HashMap::with_capacity(pairs.len());
    let mut typed_values = HashMap::new();
    let mut written = HashMap::new();
    let mut promoted = Vec::new();
    let mut tokens = Vec::with_capacity(pairs.len());

    for (path, leaf) in pairs {
        let key = path.to_string();
        let raw = raw_text(&leaf);
        // A JSON null is no evidence about the column's type; run through the ladder it
        // would pin the column to String.
        let typed = match &leaf {
            Value::Null => Typed {
                ty: None,
                value: ColValue::Null,
                dt_form: None,
            },
            // An array, or an object with no members: held as its JSON text and marked as
            // one, in a column of text. Never run through the ladder, which reads strings.
            Value::Array(_) | Value::Object(_) => Typed {
                ty: Some(ColumnType::String),
                value: ColValue::Json(raw.clone()),
                dt_form: None,
            },
            _ => infer(&raw),
        };

        // What the line said, beside the value the row will hold.
        let said = leaf;
        let promote = is_promotable(path.last().unwrap_or_default(), &typed, &raw);
        let held = if promote {
            ColValue::String(raw.clone())
        } else {
            typed.value.clone()
        };
        if held.printed() != said {
            written.insert(key.clone(), said);
        }

        if promote {
            tokens.push(Token::Promoted {
                key: key.clone(),
                value: raw.clone(),
            });
            promoted.push(key.clone());
        } else {
            tokens.push(Token::Key(key.clone()));
            typed_values.insert(key.clone(), typed);
        }

        values.insert(key.clone(), raw);
        keys.push(key);
    }

    // Distinct paths cannot render alike, so a duplicate means key escaping stopped being
    // reversible: refuse the line, in every build, rather than file it under a wrong shape.
    if let Some(key) = duplicate_key(&keys) {
        return Err(DetectorErrorKind::DuplicateKey { key: key.clone() });
    }

    let template_id = template_id(&tokens);
    let fingerprint = tokens
        .iter()
        .map(Token::to_string)
        .collect::<Vec<_>>()
        .join(" | ");
    Ok(ParsedLog {
        template_id,
        keys,
        values,
        typed_values,
        written,
        promoted,
        fingerprint,
        // Filled in by the caller, which is the only thing that knows what it has
        // already seen.
        is_new_template: false,
        raw_line: raw_line.to_vec(),
    })
}

/// Takes the leaf segment, not the rendered key: splitting on `.` would promote the
/// literal key `a.event` as a nested one.
fn is_promotable(leaf: &str, typed: &Typed, raw: &str) -> bool {
    PROMOTED_KEYS.contains(&leaf)
        && typed.ty == Some(ColumnType::String)
        // A string, and not an array or an empty object held as its text: a dimension
        // is a word.
        && matches!(typed.value, ColValue::String(_))
        && !raw.is_empty()
        && raw.len() <= MAX_PROMOTED_LEN
        // A value containing the delimiter or whitespace would make the fingerprint
        // ambiguous to read and to split.
        && !raw.contains('|')
        && !raw.chars().any(char::is_whitespace)
}

/// FNV-1a over the tokens, each length-prefixed and tagged by kind, not over the joined
/// fingerprint: `{"a | b":1}` and `{"a":1,"b":1}` join alike and must not share an id.
fn template_id(tokens: &[Token]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for token in tokens {
        match token {
            Token::Key(key) => {
                hash = fnv1a(hash, &[0]);
                hash = feed(hash, key);
            }
            Token::Promoted { key, value } => {
                hash = fnv1a(hash, &[1]);
                hash = feed(hash, key);
                hash = feed(hash, value);
            }
        }
    }
    hash
}

/// One length-prefixed string. The length goes first, so `["ab"]` and `["a","b"]`
/// diverge before their bytes rather than never.
fn feed(hash: u64, text: &str) -> u64 {
    let hash = fnv1a(hash, &(text.len() as u64).to_le_bytes());
    fnv1a(hash, text.as_bytes())
}

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The first key that appears twice, if any; `flatten` sorts, so equal keys are adjacent.
/// A function so the check can be tested: `parse_bytes` cannot produce a duplicate.
fn duplicate_key(keys: &[String]) -> Option<&String> {
    keys.windows(2)
        .find(|pair| pair[0] == pair[1])
        .map(|pair| &pair[0])
}

/// One parsed line as a `Row`, with an entry for every key on the path so a row's key
/// set is the leaf's key set.
pub fn row_of(parsed: &ParsedLog) -> Row {
    let mut row = Row::with_capacity(parsed.keys.len());
    for key in &parsed.keys {
        let value = match parsed.typed_values.get(key) {
            Some(typed) => typed.value.clone(),
            None => ColValue::String(parsed.values.get(key).cloned().unwrap_or_default()),
        };
        row.insert_written(key.clone(), value, parsed.written.get(key).cloned());
    }
    row
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "flatten_tests.rs"]
mod tests;
