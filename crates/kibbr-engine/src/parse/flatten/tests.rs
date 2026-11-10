// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::model::tokens::split_token;

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
