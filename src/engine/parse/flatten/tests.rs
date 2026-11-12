// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::engine::model::tokens::split_token;

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

// ── Arrays and empty objects ───────────────────────────────────────────────────────

/// The JSON a row gives back for `key` of one line.
fn back(line: &str, key: &str) -> Option<Value> {
    let parsed = parse_line(line.as_bytes(), 1).expect("this is JSON");
    row_of(&parsed).json(key)
}

/// **An array is a leaf held as its JSON text and marked as an array**, whatever is in
/// it: its column is text, its key is in the fingerprint once, and the row gives the
/// array back.
#[test]
fn an_array_is_a_leaf_that_comes_back_as_the_array() {
    for array in [
        r#"["en"]"#,
        "[124]",
        "[]",
        r#"[{"a":1.5},null,true,"x"]"#,
        "[[1,[2,[]]],[]]",
        "[null,null]",
        r#"[{"b":1,"a":2},{}]"#,
    ] {
        let line = format!(r#"{{"k":{array}}}"#);
        let parsed = parse_line(line.as_bytes(), 1).expect("this is JSON");
        assert_eq!(parsed.keys, ["k"], "{array}");
        let typed = &parsed.typed_values["k"];
        assert_eq!(typed.ty, Some(ColumnType::String), "{array}");
        assert_eq!(typed.value, ColValue::Json(array.to_string()), "{array}");
        let want: Value = serde_json::from_str(array).expect("an array");
        assert!(want.is_array());
        assert_eq!(back(&line, "k"), Some(want), "{array}");
    }
}

/// **An object with no members is a leaf at its path**, at the top or nested, and comes
/// back as `{}` there. An object with members is walked, as it always was.
#[test]
fn an_empty_object_is_a_leaf_at_its_path() {
    let parsed = parse_line(br#"{"a":{"b":{}},"c":{},"d":{"e":1}}"#, 1).expect("this is JSON");
    assert_eq!(parsed.keys, ["a.b", "c", "d.e"]);
    assert_eq!(parsed.fingerprint, "a.b | c | d.e");
    for key in ["a.b", "c"] {
        let typed = &parsed.typed_values[key];
        assert_eq!(typed.ty, Some(ColumnType::String));
        assert_eq!(typed.value, ColValue::Json("{}".to_string()));
    }
    let row = row_of(&parsed);
    assert_eq!(row.json("a.b"), Some(serde_json::json!({})));
    assert_eq!(row.json("c"), Some(serde_json::json!({})));
    assert_eq!(row.json("d.e"), Some(serde_json::json!(1)));
}

/// **`{}`, `[]`, `null`, a missing key, `""`, `"{}"` and `"[]"` are seven things**, and a
/// row tells every one from the others.
#[test]
fn an_empty_object_an_empty_array_null_absent_and_their_strings_are_told_apart() {
    use serde_json::json;
    let cases = [
        (r#"{"k":{}}"#, Some(json!({}))),
        (r#"{"k":[]}"#, Some(json!([]))),
        (r#"{"k":null}"#, Some(Value::Null)),
        (r#"{"other":1}"#, None),
        (r#"{"k":""}"#, Some(json!(""))),
        (r#"{"k":"{}"}"#, Some(json!("{}"))),
        (r#"{"k":"[]"}"#, Some(json!("[]"))),
    ];
    for (line, want) in &cases {
        assert_eq!(&back(line, "k"), want, "{line}");
    }
    for (i, (a, _)) in cases.iter().enumerate() {
        for (b, _) in &cases[i + 1..] {
            assert_ne!(back(a, "k"), back(b, "k"), "{a} and {b}");
        }
    }
}

/// **A string whose text is an array's stays a string**, beside an array of the same
/// text: the two hold the same characters and are not the same value.
#[test]
fn a_string_that_looks_like_an_array_is_a_string() {
    let string = parse_line(br#"{"k":"[124]"}"#, 1).expect("this is JSON");
    let array = parse_line(br#"{"k":[124]}"#, 1).expect("this is JSON");
    assert_eq!(
        string.template_id, array.template_id,
        "one key, one template"
    );
    assert_eq!(
        string.typed_values["k"].value,
        ColValue::String("[124]".into())
    );
    assert_eq!(
        array.typed_values["k"].value,
        ColValue::Json("[124]".into())
    );
    assert_eq!(row_of(&string).json("k"), Some(Value::from("[124]")));
    assert_eq!(row_of(&array).json("k"), Some(serde_json::json!([124])));
}

/// **A number inside an array is the number a line's own numbers are**: read as the JSON
/// reader reads it, so `1.50` is 1.5 there as it is outside one, and an object inside an
/// array keeps its members in the order they were written.
#[test]
fn what_is_inside_an_array_is_read_as_the_reader_reads_it() {
    let parsed = parse_line(br#"{"k":[1.50, {"b":1,"a":2}, 1e3]}"#, 1).expect("this is JSON");
    assert_eq!(
        parsed.values["k"], r#"[1.5,{"b":1,"a":2},1000.0]"#,
        "the text a query compares it as"
    );
    assert_eq!(
        row_of(&parsed).json("k").map(|v| v.to_string()).as_deref(),
        Some(r#"[1.5,{"b":1,"a":2},1000.0]"#)
    );
}

/// **An array or an empty object under a promoted key is not promoted**: a dimension is
/// a word, and the value is given back as what it was.
#[test]
fn an_array_under_a_promoted_key_stays_a_value() {
    for (line, want) in [
        (r#"{"event":["a"],"n":1}"#, serde_json::json!(["a"])),
        (r#"{"event":{},"n":1}"#, serde_json::json!({})),
    ] {
        let parsed = parse_line(line.as_bytes(), 1).expect("this is JSON");
        assert!(parsed.promoted.is_empty(), "{line}");
        assert_eq!(parsed.fingerprint, "event | n", "{line}");
        assert_eq!(row_of(&parsed).json("event"), Some(want), "{line}");
    }
    let word = parse_line(br#"{"event":"[a]","n":1}"#, 1).expect("this is JSON");
    assert_eq!(word.promoted, ["event"], "a string is still promoted");
}

/// **An empty object counts against the nesting limit like any other**: one at depth 8
/// is a leaf, and one a level deeper is refused by the depth rule.
#[test]
fn an_empty_object_past_the_nesting_limit_is_refused() {
    let nested = |depth: usize| {
        let mut line = String::new();
        for _ in 0..depth - 1 {
            line.push_str(r#"{"a":"#);
        }
        line.push_str("{}");
        line.push_str(&"}".repeat(depth - 1));
        line
    };
    let deepest = parse_line(nested(MAX_DEPTH).as_bytes(), 1).expect("depth 8 is allowed");
    assert_eq!(deepest.keys, ["a.a.a.a.a.a.a"]);
    let e = parse_line(nested(MAX_DEPTH + 1).as_bytes(), 1).expect_err("depth 9 is not");
    assert!(
        matches!(e.kind, DetectorErrorKind::NestingTooDeep { .. }),
        "{}",
        e.kind
    );
}
