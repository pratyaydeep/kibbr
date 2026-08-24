//! The identity of a flattened key.

use std::fmt;

/// The sequence of member names walked to reach one leaf value.
///
/// Segments, not a joined string, because the join is the lossy step. `{"a.b":1}` and
/// `{"a":{"b":2}}` are different documents that both flatten to the three characters
/// `a.b`, and once a key *is* that string nothing downstream can tell them apart: `keys`
/// gains a duplicate, the fingerprint repeats a segment, one value is dropped without a
/// word, and the trie grows two edges wearing one label. The flattened key is the
/// identity for all of that, so it has to carry the distinction.
///
/// `Display` escapes: inside a segment `.` is written `\.` and `\` is written `\\`, so an
/// unescaped `.` occurs only at a segment boundary. That makes the rendering reversible,
/// and a reversible rendering cannot collide - the cases above stop being reachable
/// rather than being detected after the fact.
///
/// The cost lands only where the ambiguity is: a segment containing neither character
/// renders unchanged, so ordinary keys - nearly all of them - read as they did before,
/// template ids unmoved.
///
/// Reversible over paths of at least one segment, which is the domain it is used over -
/// the empty path is the root, `walk` reaches a leaf only after pushing a segment, and
/// the depth scan gives the root its own text. The empty path and the one-segment path
/// `[""]` do both render empty; that pair is the separate question of what a nameless key
/// means, answered in `template_id`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct KeyPath(Vec<String>);

impl KeyPath {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn push(&mut self, segment: &str) {
        self.0.push(segment.to_string());
    }

    pub fn pop(&mut self) {
        self.0.pop();
    }

    /// The leaf member name, with no separator to split on - which is the point.
    /// Splitting a rendered key on `.` reads `{"a.event":1}` as the nested path
    /// `a` -> `event` and promotes it as one.
    pub fn last(&self) -> Option<&str> {
        self.0.last().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for KeyPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            for ch in segment.chars() {
                if ch == '.' || ch == '\\' {
                    f.write_str("\\")?;
                }
                write!(f, "{ch}")?;
            }
        }
        Ok(())
    }
}

impl FromIterator<String> for KeyPath {
    fn from_iter<I: IntoIterator<Item = String>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// The inverse of `Display`, which is what makes "reversible" a claim the suite checks
/// rather than one the doc comment asserts.
///
/// Beside the rendering deliberately: an encoding and its inverse drift apart the moment
/// they are two files apart. Gated to tests because the real caller is the query side's
/// key parser, which does not exist yet - a query naming `a\.b` has to arrive at the same
/// path the flattener built, and this is the function it will call.
///
/// Lenient on escapes it cannot have produced: rendering only emits `\.` and `\\`, so a
/// backslash before anything else is taken literally rather than rejected. Round-tripping
/// is unaffected - those inputs are not in the image of `Display`.
#[cfg(test)]
pub fn parse(text: &str) -> KeyPath {
    let mut segments = vec![String::new()];
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        let current = segments.last_mut().expect("never emptied");
        match ch {
            '.' => segments.push(String::new()),
            '\\' => match chars.next() {
                Some(next @ ('.' | '\\')) => current.push(next),
                // A backslash this rendering never emits, and one at the very end.
                Some(next) => {
                    current.push('\\');
                    current.push(next);
                }
                None => current.push('\\'),
            },
            other => current.push(other),
        }
    }
    KeyPath(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(segments: &[&str]) -> KeyPath {
        segments.iter().map(|s| (*s).to_string()).collect()
    }

    /// The case the whole type exists for: one dotted literal key, one nested path,
    /// and no way for them to render alike.
    #[test]
    fn a_dotted_literal_and_a_nested_path_render_differently() {
        assert_eq!(path(&["a.b"]).to_string(), "a\\.b");
        assert_eq!(path(&["a", "b"]).to_string(), "a.b");
    }

    /// A segment holding neither `.` nor `\` is untouched, so the escaping costs
    /// ordinary keys nothing - including their template ids.
    #[test]
    fn ordinary_keys_render_unchanged() {
        for segments in [
            &["ts"][..],
            &["user", "id"],
            &["cache", "origin", "host"],
            &["user_id"],
            &[""],
        ] {
            assert_eq!(path(segments).to_string(), segments.join("."));
        }
    }

    /// Escaping `.` alone would not be enough: a literal backslash next to a separator
    /// would forge one. `\` has to escape itself for the rendering to stay reversible.
    #[test]
    fn a_backslash_cannot_forge_a_separator() {
        let literal = path(&["a\\", "b"]);
        let forged = path(&["a\\.b"]);
        assert_ne!(literal.to_string(), forged.to_string());
        assert_eq!(literal.to_string(), "a\\\\.b");
        assert_eq!(forged.to_string(), "a\\\\\\.b");
    }

    /// Reversible, over every shape that is awkward for the encoding: separators and
    /// backslashes at the edges of segments, empty segments, and runs of both.
    ///
    /// Paths of at least one segment. The root is not a key and never renders as one -
    /// see `the_root_path_is_not_a_key`.
    #[test]
    fn rendering_round_trips() {
        let cases: &[&[&str]] = &[
            &[""],
            &["a"],
            &["a", "b"],
            &["a.b"],
            &["a.b", "c"],
            &["a", "b.c"],
            &["."],
            &["..", "."],
            &["\\"],
            &["\\\\"],
            &["a\\"],
            &["\\.b"],
            &["a\\.b", "\\", ".", ""],
            &["", "", ""],
            &["ключ.путь", "日本.語"],
        ];
        for segments in cases {
            let original = path(segments);
            let rendered = original.to_string();
            assert_eq!(
                parse(&rendered),
                original,
                "{segments:?} rendered {rendered:?} and did not survive the trip"
            );
        }
    }

    /// The property the trie needs, stated directly: distinct paths render distinctly.
    /// Round-tripping implies it, but this is the claim callers rely on.
    #[test]
    fn distinct_paths_render_distinctly() {
        let cases: &[&[&str]] = &[
            &[""],
            &["", ""],
            &["a"],
            &["a", "b"],
            &["a.b"],
            &["a", "", "b"],
            &["a\\", "b"],
            &["a\\.b"],
            &["a.b", "c"],
            &["a", "b.c"],
            &["a.b.c"],
            &["a", "b", "c"],
        ];
        let mut seen = std::collections::HashMap::new();
        for segments in cases {
            let rendered = path(segments).to_string();
            if let Some(other) = seen.insert(rendered.clone(), *segments) {
                panic!("{segments:?} and {other:?} both render as {rendered:?}");
            }
        }
    }

    /// The one pair the rendering cannot separate, and the reason it does not have to.
    ///
    /// The root and the one-segment path `[""]` both render empty. The root is not a key:
    /// `walk` pushes a segment before reaching any leaf, so nothing it emits is empty,
    /// and the depth scan gives the root the text `<top level>`. What remains is the empty
    /// key on its own, which collides with something else again - answered in
    /// `template_id`.
    #[test]
    fn the_root_path_is_not_a_key() {
        assert!(KeyPath::new().is_empty());
        assert_eq!(KeyPath::new().to_string(), path(&[""]).to_string());
    }

    /// The leaf comes off the path, never off the rendered string.
    #[test]
    fn the_leaf_segment_is_the_last_one_not_the_text_after_the_last_dot() {
        assert_eq!(path(&["a", "event"]).last(), Some("event"));
        assert_eq!(path(&["a.event"]).last(), Some("a.event"));
        assert_eq!(KeyPath::new().last(), None);
    }

    #[test]
    fn push_and_pop_are_inverses() {
        let mut p = KeyPath::new();
        p.push("a");
        p.push("b.c");
        assert_eq!(p.to_string(), "a.b\\.c");
        p.pop();
        assert_eq!(p, path(&["a"]));
        p.pop();
        assert!(p.is_empty());
    }
}
