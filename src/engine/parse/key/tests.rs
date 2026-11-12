// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

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
