// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The identity of a flattened key.

use std::fmt;

/// The member names walked to reach one leaf, kept as segments because joining is lossy:
/// `Display` escapes `.` and `\` so `{"a.b":1}` and `{"a":{"b":2}}` render apart.
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

    /// The leaf member name, taken from the path because splitting a rendered key on `.`
    /// misreads `{"a.event":1}`.
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

/// The inverse of `Display`, beside it so the two cannot drift; lenient on a backslash
/// the rendering never emits.
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

    /// Every shape awkward for the encoding; the root is not a key - see
    /// `the_root_path_is_not_a_key`.
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

    /// The root and the one-segment path `[""]` both render empty, the one pair the rendering
    /// cannot separate; the root is never a key, and the empty key is `template_id`'s question.
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
