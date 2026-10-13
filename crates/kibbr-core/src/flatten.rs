// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Flattening, fingerprinting, and the parsed row.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use serde_json::error::Category;
use serde_json::Value;

use crate::error::{BadByte, DetectorError, DetectorErrorKind};
use crate::infer::infer;
use crate::key::KeyPath;
use crate::types::{ColValue, ColumnType, Typed};

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

/// A key as a token, `=` written `\=` so a literal key cannot spell a promoted
/// `key=value`.
pub fn key_token(key: &str) -> Cow<'_, str> {
    if key.contains('=') {
        Cow::Owned(key.replace('=', "\\="))
    } else {
        Cow::Borrowed(key)
    }
}

/// A promoted key and its value as a token, joined by the first bare `=`.
pub fn promoted_token(key: &str, value: &str) -> String {
    format!("{}={value}", key_token(key))
}

/// A promoted token back into its key and value, split at the first unescaped `=`;
/// `None` for a key's token.
pub fn split_token(token: &str) -> Option<(Cow<'_, str>, &str)> {
    let mut chars = token.char_indices();
    while let Some((at, ch)) = chars.next() {
        match ch {
            '\\' => {
                chars.next();
            }
            '=' => {
                let key = &token[..at];
                let key = if key.contains("\\=") {
                    Cow::Owned(key.replace("\\=", "="))
                } else {
                    Cow::Borrowed(key)
                };
                return Some((key, &token[at + 1..]));
            }
            _ => {}
        }
    }
    None
}

/// The `key=value` tokens on this path, split back into unescaped pairs.
pub fn promoted_of<'a>(
    path: &'a [String],
    columns: &BTreeSet<&str>,
) -> BTreeMap<Cow<'a, str>, &'a str> {
    let mut out = BTreeMap::new();
    for label in path {
        // A leaf from before `=` was escaped may hold rows of both a literal and a promoted
        // `event=login`; its label does not speak for every row, so the scan decides.
        if columns.contains(label.as_str()) {
            continue;
        }
        if let Some((key, value)) = split_token(label) {
            if columns.contains(key.as_ref()) {
                out.insert(key, value);
            }
        }
    }
    out
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

    /// `typed_values` in the JSON form the parsed row carries.
    pub fn typed_values_json(&self) -> serde_json::Map<String, Value> {
        self.typed_values
            .iter()
            .map(|(k, t)| (k.clone(), t.value.to_json()))
            .collect()
    }
}

/// Flatten one JSON object into sorted key paths and leaf values; arrays stay whole, or
/// the fingerprint would depend on their length.
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
                walk(inner, path, depth + 1, out)?;
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
        let typed = if leaf.is_null() {
            Typed {
                ty: None,
                value: ColValue::Null,
                dt_form: None,
            }
        } else {
            infer(&raw)
        };

        // What the line said, beside the value the row will hold; an array is stored as
        // its JSON text.
        let said = match leaf {
            Value::Array(_) | Value::Object(_) => Value::String(raw.clone()),
            other => other,
        };
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

// ── Tests ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// `split_position` can only cut `serde_json`'s " at line N column M" suffix while it
    /// is spelled that way; this fails, in every build, when the format moves.
    #[test]
    fn a_serde_json_message_still_carries_its_position_the_way_we_cut_it() {
        let e = serde_json::from_slice::<Value>(br#"{"a": }"#).expect_err("this is not JSON");
        assert!(
            e.to_string().contains(" at line "),
            "serde_json no longer spells the position ` at line N column M`, so \
             `split_position` is cutting nothing: {e}"
        );

        let (offset, message) = split_position(&e);
        assert_eq!(
            offset,
            Some(e.column() - 1),
            "the offset is not the 0-based column"
        );
        assert!(
            !message.contains(" at line "),
            "the position was left in the message and will be printed twice: {message}"
        );
    }

    /// A rejected line, whole, through the front door: the message has a position and
    /// does not repeat it.
    #[test]
    fn a_bad_line_still_reports_its_position() {
        let e = parse_line(br#"{"a": }"#, 7).expect_err("this is not JSON");
        assert_eq!(e.line, 7);
        let text = e.kind.to_string();
        assert!(
            !text.contains(" at line "),
            "the rejection repeats serde_json's position: {text}"
        );
    }

    /// `{"a.b": 1, "a": {"b": 2}}` would collide if key rendering were not reversible.
    #[test]
    fn a_dotted_key_and_a_nested_one_do_not_collide() {
        let parsed = parse_line(br#"{"a.b": 1, "a": {"b": 2}}"#, 1).expect("this is JSON");
        assert_eq!(parsed.keys, vec!["a.b".to_string(), "a\\.b".to_string()]);
        assert_eq!(duplicate_key(&parsed.keys), None);
    }

    /// Through `duplicate_key`, because `parse_bytes` cannot be made to produce a duplicate.
    #[test]
    fn a_repeated_key_is_found_in_every_build() {
        let keys = ["a".to_string(), "b".to_string(), "b".to_string()];
        assert_eq!(duplicate_key(&keys), Some(&"b".to_string()));

        let kind = DetectorErrorKind::DuplicateKey {
            key: "b".to_string(),
        };
        assert!(
            kind.to_string().contains("`b`"),
            "the rejection does not name the key: {kind}"
        );
    }

    /// `{"event=login":1}` and `{"event":"login"}` are two shapes that must not share a
    /// trie edge; the `=` inside a key is escaped.
    #[test]
    fn a_key_holding_an_equals_sign_does_not_spell_a_promoted_token() {
        let literal = parse_line(br#"{"event=login":1}"#, 1).expect("this is JSON");
        let promoted = parse_line(br#"{"event":"login"}"#, 2).expect("this is JSON");

        assert_eq!(literal.walk_tokens(), vec!["event\\=login".to_string()]);
        assert_eq!(promoted.walk_tokens(), vec!["event=login".to_string()]);
        assert_eq!(literal.fingerprint, "event\\=login");
        assert_eq!(promoted.fingerprint, "event=login");
        assert_ne!(literal.template_id, promoted.template_id);
    }

    /// The escape is in the token and nowhere else: the key is still the name the line
    /// gave it, which is the column a query names and the member a row prints back.
    #[test]
    fn the_escape_is_in_the_token_and_not_in_the_key() {
        let parsed = parse_line(br#"{"a=b":1}"#, 1).expect("this is JSON");
        assert_eq!(parsed.keys, vec!["a=b".to_string()]);
        assert!(parsed.values.contains_key("a=b"));
        assert!(parsed.typed_values.contains_key("a=b"));
        assert_eq!(parsed.walk_tokens(), vec!["a\\=b".to_string()]);
    }

    /// The first bare `=` is the one promotion wrote, whatever the parent key or the
    /// value holds.
    #[test]
    fn the_first_bare_equals_sign_ends_a_promoted_key() {
        let nested = parse_line(br#"{"a=b":{"event":"login"}}"#, 1).expect("this is JSON");
        assert_eq!(nested.promoted, vec!["a=b.event".to_string()]);
        assert_eq!(nested.walk_tokens(), vec!["a\\=b.event=login".to_string()]);
        assert_eq!(nested.fingerprint, "a\\=b.event=login");

        let literal = parse_line(br#"{"a=b.event=login":1}"#, 2).expect("this is JSON");
        assert_eq!(
            literal.walk_tokens(),
            vec!["a\\=b\\.event\\=login".to_string()]
        );

        let in_value = parse_line(br#"{"event":"x.event=login"}"#, 4).expect("this is JSON");
        let in_key = parse_line(br#"{"event=x":{"event":"login"}}"#, 5).expect("this is JSON");
        assert_eq!(in_value.walk_tokens(), vec!["event=x.event=login"]);
        assert_eq!(in_key.walk_tokens(), vec!["event\\=x.event=login"]);

        // A backslash before the `=` is the key's own, already doubled by the key's
        // rendering, so it cannot pass for the escape.
        let slashed = parse_line(br#"{"event\\=login":1}"#, 3).expect("this is JSON");
        assert_eq!(slashed.walk_tokens(), vec!["event\\\\\\=login".to_string()]);
    }

    /// A token reads back as the kind, key and value it was written from.
    #[test]
    fn a_token_reads_back_as_what_it_was_written_from() {
        for key in [
            "a",
            "a=b",
            "=",
            "a\\\\",
            "a\\\\=b",
            "a\\.b=c",
            "a=b.event",
            "",
        ] {
            let token = key_token(key);
            assert_eq!(split_token(&token), None, "`{key}` read as promoted");
            for value in ["login", "a=b", "=", "\\", "x\\=y"] {
                let token = promoted_token(key, value);
                let (k, v) = split_token(&token).expect("a promoted token");
                assert_eq!((k.as_ref(), v), (key, value), "{token}");
            }
        }
    }

    #[test]
    fn a_key_with_no_equals_sign_is_its_own_token() {
        let parsed = parse_line(br#"{"a.b":1,"c":{"d":"x"},"event":"hit","e\\f":2}"#, 1)
            .expect("this is JSON");
        assert_eq!(
            parsed.walk_tokens(),
            vec!["event=hit", "a\\.b", "c.d", "e\\\\f"]
        );
        assert_eq!(parsed.fingerprint, "a\\.b | c.d | e\\\\f | event=hit");
    }
}
