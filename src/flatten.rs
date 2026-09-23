//! Flattening, fingerprinting, and the parsed row.

use std::collections::HashMap;
use std::fmt;

use serde_json::error::Category;
use serde_json::Value;

use crate::error::{BadByte, DetectorError, DetectorErrorKind};
use crate::infer::infer;
use crate::key::KeyPath;
use crate::types::{ColValue, ColumnType, Typed};

/// A guard against pathological or hostile input, not a real-world constraint.
/// Counted over objects only, and only outside arrays - see `MAX_ARRAY_DEPTH`.
pub const MAX_DEPTH: usize = 8;

/// The ceiling on nesting inside an array, counting containers of either kind from the
/// outermost `[`.
///
/// Arrays are not flattened, so `MAX_DEPTH` does not apply inside one - but "no limit"
/// cannot be built. An array is stored as raw JSON text, so `serde_json` has to
/// materialise it first by recursive descent, and a 100 000-deep array overflows the
/// stack. The choice is not whether there is a ceiling but whose: this one is ours and
/// documented rather than `serde_json`'s and accidental. 8 + 64 leaves its 128
/// unreachable.
pub const MAX_ARRAY_DEPTH: usize = 64;

/// Keys low-cardinality enough to be promoted into the fingerprint as `key=value`,
/// which is what makes the trie branch usefully.
///
/// Which keys belong here is still open, so it holds the single key there is a worked
/// example for. Widening it moves every template id in the system, so it wants
/// evidence, not a guess.
const PROMOTED_KEYS: &[&str] = &["event"];

/// The longest promoted value; beyond this it is not a dimension, it is a payload.
const MAX_PROMOTED_LEN: usize = 40;

/// One element of the fingerprint.
///
/// The two kinds render alike on purpose - `event=login` reads the same whether the
/// sender nested it or named a field that - but they are different shapes, and the
/// identity has to tell them apart where the text cannot.
enum Token {
    /// A key that stayed a value. Its value varies line to line and is not in here.
    Key(String),
    /// A key promoted into the fingerprint, carrying the value that branches the trie.
    Promoted { key: String, value: String },
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Key(key) => f.write_str(key),
            Token::Promoted { key, value } => write!(f, "{key}={value}"),
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
    /// back: `"0045000012"` beside the `Int64`, `1705312200` beside the `Datetime`. Every
    /// other key's value prints as written, and is not repeated here.
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

    /// The trie path: promoted tokens first in key order, then the rest sorted.
    ///
    /// Not identity order, and it leaves identity order alone: `fingerprint` and
    /// `template_id` keep the order this file builds - keys sorted, promoted keys
    /// rendered in place. Same multiset either way, so the two can never disagree about
    /// which leaf a line belongs to.
    ///
    /// The hoist is what querying needs. A query turns its pattern into tokens and walks
    /// them, omitted keys acting as wildcards over a subtree via `walk_prefix` - which
    /// works only if the pattern's tokens are a *prefix* of the path. Patterns are written
    /// over the promoted dimensions (`{"event":"processed"}`), so those tokens have to sit
    /// at the top of the trie. Under identity order they sort wherever the alphabet puts
    /// them: on `--seed 42`, `event=` lands six levels down under `amount → currency →
    /// customer.email → …`, and counting one event means searching nearly every subtree
    /// instead of taking one edge. Costs 1.7% of the prefix sharing - 4,026 nodes against
    /// 3,957 - for a walk that is O(depth) rather than a search.
    pub fn walk_tokens(&self) -> Vec<String> {
        let mut tokens = Vec::with_capacity(self.keys.len());
        // `promoted` is already in key order: it is filled while walking `keys`, and
        // `flatten` sorted those.
        for key in &self.promoted {
            let value = self.values.get(key).map_or("", String::as_str);
            tokens.push(format!("{key}={value}"));
        }
        for key in self.value_keys() {
            tokens.push(key.clone());
        }
        tokens
    }

    /// What this line's value for `key` contributes to that column's type join.
    ///
    /// `None` where the value was JSON null: the ladder is not run on a null, which
    /// claims nothing about the column. A promoted key carries its value in the path
    /// rather than in `typed_values`, and promotion requires the ladder to have said
    /// `String`, so that is what it contributes - the key is still on the path, and the
    /// path is the key set.
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

/// Flatten one JSON object into sorted key paths paired with their leaf values. Arrays
/// are stored as their raw JSON string, never flattened - flattening them would make the
/// fingerprint depend on array length.
///
/// Yields `KeyPath`s, not joined strings: the join is where a dotted literal key and a
/// nested path stop being distinguishable, and the caller still needs the leaf segment
/// for promotion. See `KeyPath`.
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
    // Sorting is what makes the fingerprint order-independent: two lines from the same
    // call site agree even if the serializer shuffled field order.
    //
    // On the rendered key, not the segments - the two are not the same relation, and
    // rendered is the order `keys` is emitted in, so a consumer re-sorting that array
    // lands back on the same one. `_cached_` renders each key once, not once per compare.
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
    /// Byte range of the member key whose value is being read at this level, quotes
    /// included. `None` at the root, inside arrays, and between `{` and the first key.
    cur_key: Option<std::ops::Range<usize>>,
    /// Whether the next string at this level is a key rather than a value.
    expect_key: bool,
}

/// Reject over-deep input before `serde_json` builds anything.
///
/// Enforced on the raw bytes, because the tree `walk` descends does not exist until
/// `serde_json` has already recursed to build it. `serde_json` stops at 128 nested
/// containers and calls it a syntax error, so without this pass anything deeper comes
/// back as `InvalidJson` - valid JSON reported as malformed, the diagnostic degrading
/// exactly as the input gets more hostile. Scanning first keeps both ceilings ours and
/// reachable at any depth, including 100 000.
///
/// A depth scan, not a validator: bad syntax, unterminated strings and trailing bytes
/// are left for `serde_json` to describe, so those messages are unchanged.
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
///
/// Built as a `KeyPath` and rendered by it, never joined here. Joining by hand is how
/// the two passes drift: the scan would report `a.b` for a key that `walk` calls `a\.b`,
/// sending an operator after a nesting level that does not exist.
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

/// Decide what a failed parse was actually about: bad encoding, bad syntax, or both.
///
/// Runs only after `from_slice` has failed, so a good line pays nothing - and has had
/// its UTF-8 validated by `serde_json` on the way through anyway. Non-UTF-8 bytes always
/// fail `from_slice`, inside a string or as a byte that cannot begin a token, so nothing
/// reaches here unnoticed.
///
/// `valid_up_to` bounds what `serde_json` was entitled to an opinion about: everything
/// before it decoded cleanly. A syntax verdict inside that region is sound and wins; one
/// at or past it was tripped by the corruption itself and describes a symptom.
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

    // A line cut mid-character is a truncated line, not an encoding fault: both checks
    // describe the one event - the write ended early - and `error_len() == None` paired
    // with `Eof` is what says so. Without this, the same cut one byte later leaves valid
    // UTF-8 and reports something else entirely.
    if utf8.error_len().is_none() && source.classify() == Category::Eof {
        return invalid_json(&source, None);
    }

    match split_position(&source).0 {
        // A syntax error inside the decoded region is a real and separate fault: report
        // it, and say the line does not decode either rather than making the operator
        // find that out on the next run.
        Some(offset) if offset < bad.offset => invalid_json(&source, Some(bad)),
        // At or past the boundary, `serde_json` tripped over the corruption itself and
        // describes a symptom. No position at all is no claim about where the fault was,
        // so there is nothing to weigh against the encoding fault either.
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

/// `serde_json`'s message and its position, separated.
///
/// Its `Display` is `{message} at line {line} column {column}`, but only when it has a
/// position; with none it prints the bare message and says so by reporting `line() == 0`.
/// Branching on that same condition is what makes cutting the suffix safe - it is only
/// cut where upstream is known to have added it.
///
/// The line number it reports is always 1, since the parser is handed one line at a time,
/// so printing it beside the real line number is worse than not printing it. The column
/// is 1-based and counted in bytes, making `column - 1` the same 0-based offset into the
/// line that `BadByte` reports.
fn split_position(e: &serde_json::Error) -> (Option<usize>, String) {
    let text = e.to_string();
    if e.line() == 0 {
        return (None, text);
    }
    match text.rsplit_once(" at line ") {
        Some((message, _)) => (Some(e.column().saturating_sub(1)), message.to_string()),
        // The format changed under us. Hand back what `serde_json` said, whole and
        // unedited, rather than a half-cut message or a position printed twice.
        //
        // No assertion here, in either build. A rejected line must not end the run over a
        // library's formatting, and an assertion that only fires in debug is not a check -
        // it is a check in the build nobody ships. `a_bad_line_still_reports_its_position`
        // below is what fails when the format moves, in every build and in CI.
        None => (None, text),
    }
}

/// `raw_line` is bytes, not `&str`: bytes that are not UTF-8 are one bad line for
/// `serde_json` to reject, not a decoding failure that takes the whole stream with it.
fn parse_bytes(raw_line: &[u8]) -> Result<ParsedLog, DetectorErrorKind> {
    // Before `from_slice`: past 128 levels `serde_json` refuses to build the value at
    // all, and its refusal is a syntax error. The depth rules only stay reachable if
    // they are checked on the bytes first.
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
        // An explicit JSON null carries no evidence about the column's type, so the
        // ladder is not run on it - it would come back String and pin the column to the
        // four-character text "null".
        let typed = if leaf.is_null() {
            Typed {
                ty: None,
                value: ColValue::Null,
                dt_form: None,
            }
        } else {
            infer(&raw)
        };

        // What the line said, beside the value the row will hold for this key - a
        // promoted key holds its text. An array is text: it is stored as its JSON and comes
        // back as that string.
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

    // `keys` is a set, and distinct paths cannot render alike - so a duplicate here does
    // not mean the input was odd, it means the rendering stopped being reversible. The
    // fingerprint, the merge index's set arithmetic and the trie's edge labels are all
    // downstream of that.
    //
    // **Checked in every build and the line is refused.** It was a debug assertion, which
    // in the build that ships is no check at all: the duplicate would go on to give this
    // line a fingerprint that is not its shape, and the rows under it would answer a
    // query about a different template. One refused line naming the key it collided on is
    // the cheaper outcome, and it is the one an operator can report.
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

/// Takes the leaf segment, not the rendered key. Splitting the rendered key on `.` would
/// read the literal key `{"a.event":"login"}` as the nested path `a` -> `event` and
/// promote it as one - a value the sender never nested.
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

/// FNV-1a over the token list. The trie hands out sequential ids, but there is no trie
/// yet; a content hash is deterministic and order-independent, which is all the parse
/// surface needs.
///
/// Over the tokens, not the fingerprint they join into: `" | "` is not a separator a key
/// is obliged to stay out of, and `join` does not make it one. Hashing the joined string
/// gives one id to every pair whose tokens happen to concatenate alike:
///
/// ```text
/// {"a | b":1}         vs  {"a":1,"b":1}       one key or two, both `a | b`
/// {"event=login":1}   vs  {"event":"login"}   a literal key, or a promoted one
/// {}                  vs  {"":""}             no key, or one key with no name
/// ```
///
/// Each pair is two different shapes - different key sets, and in the second pair
/// different types - sharing one template, which merges unrelated rows into one leaf and
/// asks the column store to hold two incompatible schemas in one place.
///
/// A length prefix closes the first two pairs: a separator inside a token cannot forge a
/// boundary that is counted rather than searched for, and the empty token list is no
/// longer the one-element list holding the empty string. A kind tag closes the third,
/// where the text is the same token and only what produced it differs.
///
/// `fingerprint` stays the joined text - what a human reads, not the identity. Two shapes
/// that read alike are entitled to, so long as they do not merge.
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

/// The first key that appears twice, if any.
///
/// A function so the check has something to be tested against: `parse_bytes` cannot be
/// made to produce a duplicate through its front door, because key escaping is reversible
/// and that is exactly why. `flatten` sorts, so equal keys are adjacent.
fn duplicate_key(keys: &[String]) -> Option<&String> {
    keys.windows(2)
        .find(|pair| pair[0] == pair[1])
        .map(|pair| &pair[0])
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// The two checks that used to be debug assertions, which is to say the two checks that
/// were not in the build that ships.
#[cfg(test)]
mod tests {
    use super::*;

    /// `split_position` cuts `serde_json`'s " at line N column M" suffix off a message
    /// and reports the offset separately, and it can only do that while the suffix is
    /// spelled that way.
    ///
    /// **This is the check, and the debug assertion was not.** A format change used to
    /// fail the suite in debug and silently double-print the position in release; now it
    /// fails here, in every build and in CI. The fallback it guarded is still there and
    /// still hands back the message whole - a rejected line must not end the run over a
    /// library's formatting.
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

    /// Distinct paths cannot render alike, which is what makes the key set a set.
    ///
    /// `{"a.b": 1, "a": {"b": 2}}` is the pair that would collide if the rendering were
    /// not reversible: one is a key holding a dot, the other is a nested key. They come
    /// back as `a.b` and `a\.b` - sorted, and `.` sorts before `\` - so the line has two
    /// keys rather than one repeated.
    #[test]
    fn a_dotted_key_and_a_nested_one_do_not_collide() {
        let parsed = parse_line(br#"{"a.b": 1, "a": {"b": 2}}"#, 1).expect("this is JSON");
        assert_eq!(parsed.keys, vec!["a.b".to_string(), "a\\.b".to_string()]);
        assert_eq!(duplicate_key(&parsed.keys), None);
    }

    /// And if it ever did collide, the line is refused rather than filed under a
    /// fingerprint that is not its shape.
    ///
    /// Driven through `duplicate_key` because `parse_bytes` cannot be made to produce a
    /// duplicate - that is the property above. What is being checked is that the guard
    /// finds one, in a release build, where the debug assertion found nothing.
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
}
