// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::engine::query::Query;
use crate::engine::rng::Rng;

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).expect("a representable instant")
}

/// Not "returns an error" - `parse` returning at all is the whole assertion.
fn survives(src: &str) {
    let _ = parse(src, now());
}

#[test]
fn nul_bytes_are_just_characters() {
    survives("\0");
    survives("COUNT\0LOG {}");
    survives("COUNT LOG {\"a\\u0000b\": \"c\"}");
    assert!(parse("COUNT LOG {\"a\0b\": *}", now()).is_ok());
}

#[test]
fn megabyte_inputs_return() {
    survives(&"{".repeat(1_000_000));
    survives(&format!(
        r#"COUNT LOG {{"a": "{}"}}"#,
        "x".repeat(1_000_000)
    ));
    survives(&format!(
        "COUNT LOG {{{}}}",
        r#""a": "b", "#.repeat(100_000)
    ));
    survives(&format!("COUNT LOG {{}} SINCE {}h", "9".repeat(1_000_000)));
    survives(&"é".repeat(500_000));
    survives(&r#"\u"#.repeat(500_000));
}

/// A deterministic sweep over the characters the grammar cares about: the failure this is
/// looking for is a panic, so every input is its own assertion.
#[test]
fn a_hundred_thousand_random_queries_return() {
    const ALPHABET: [&str; 26] = [
        "COUNT",
        "MATCH",
        "LOG",
        "SINCE",
        "BETWEEN",
        "AND",
        "GROUP",
        "BY",
        "{",
        "}",
        ":",
        ",",
        "*",
        "\"",
        "\\",
        "1h",
        "é",
        "🙂",
        "2024-01-01T00:00:00Z",
        " ",
        "\u{0}",
        "\\u",
        // `time`, its parentheses, and a width of zero - the group key that is parsed
        // rather than read, and the one operand the grammar rejects outright.
        "time",
        "(",
        ")",
        "0s",
    ];
    let mut rng = Rng::new(9);
    for _ in 0..100_000 {
        let mut src = String::new();
        for _ in 0..rng.below(12) {
            src.push_str(ALPHABET[rng.below(ALPHABET.len() as u64) as usize]);
        }
        survives(&src);
    }
}

/// The two the grammar prints, parsed rather than merely accepted.
#[test]
fn grammar_examples() {
    let a = parse(r#"COUNT LOG {"event": "login_failed"} SINCE 1h"#, now())
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(a.mode, QueryMode::Count);
    assert_eq!(a.matches.len(), 1);
    assert_eq!(a.time.until, None);
    assert!(a.time.since.is_some());

    let b = parse(
        r#"MATCH LOG {"event": "processed"} SINCE 24h GROUP BY level"#,
        now(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(
        b.mode,
        QueryMode::GroupBy(vec![GroupKey::Column("level".to_string())])
    );
}

/// The flag surface cannot tell these apart: `--match k=*` is always the wildcard, so
/// `Exact("*")` is a value only TrieQL can ask for.
#[test]
fn a_quoted_star_is_a_value_not_the_wildcard() {
    let any = parse(r#"COUNT LOG {"a": *}"#, now()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(any.matches[0].value, MatchValue::Any);

    let literal = parse(r#"COUNT LOG {"a": "*"}"#, now()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(literal.matches[0].value, MatchValue::Exact("*".to_string()));
}

/// JSON's escapes, because the pattern is written as JSON. A key holding a quote is
/// otherwise unaskable.
#[test]
fn strings_take_json_escapes() {
    let q = parse(
        r#"COUNT LOG {"a\"b": "c\\d", "tab": "x\ty", "astral": "\ud83d\ude42"}"#,
        now(),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(q.matches[0].key, "a\"b");
    assert_eq!(q.matches[0].value, MatchValue::Exact("c\\d".to_string()));
    assert_eq!(q.matches[1].value, MatchValue::Exact("x\ty".to_string()));
    assert_eq!(q.matches[2].value, MatchValue::Exact("🙂".to_string()));
}

/// Byte offsets, checked where they can be read as numbers rather than out of a
/// rendered message.
#[test]
fn positions_are_byte_offsets_within_the_input() {
    for src in [
        "",
        "COUNT LOG",
        r#"COUNT LOG {"é": }"#,
        r#"COUNT LOG {"🙂": "x"} SINCE 🙂"#,
        r#"COUNT LOG {"a": "b"} LIMIT ten"#,
        r#"COUNT LOG {"a": "b"} LIMIT -1"#,
    ] {
        let e = parse(src, now()).expect_err("expected a parse error");
        assert!(e.pos <= src.len(), "{src:?} → byte {}", e.pos);
        assert!(src.is_char_boundary(e.pos), "{src:?} → byte {}", e.pos);
    }
    // The exact one, so "inside the input" cannot pass by always answering 0. The
    // `é` is two bytes, so a parser counting characters would say 21.
    let src = r#"COUNT LOG {"é": "b"} SORT BY ts"#;
    let e = parse(src, now()).unwrap_err();
    assert_eq!(e.pos, 22);
    assert_eq!(&src[e.pos..], "SORT BY ts");
}

/// `order_clause`: time is the one thing to order by, ascending is oldest first, and a
/// query without the clause, or with `DESC`, is newest first.
#[test]
fn order_by_time_names_the_end_rows_start_at() {
    let order = |src: &str| parse(src, now()).unwrap_or_else(|e| panic!("{e}")).order;
    assert_eq!(order(r#"MATCH LOG {}"#), Order::NewestFirst);
    assert_eq!(order(r#"MATCH LOG {} ORDER BY time"#), Order::NewestFirst);
    assert_eq!(order(r#"MATCH LOG {} ORDER BY time DESC"#), Order::NewestFirst);
    assert_eq!(order(r#"MATCH LOG {} order by TIME asc"#), Order::OldestFirst);
    let q = parse(
        r#"MATCH LOG {"a": *} SINCE 1h ORDER BY time ASC LIMIT 5 AFTER "v3.00""#,
        now(),
    );
    assert!(q.is_err(), "the cursor is still shape-checked after ORDER BY");
    let q = parse(r#"MATCH LOG {"a": *} SINCE 1h ORDER BY time ASC LIMIT 5"#, now())
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!((q.order, q.limit), (Order::OldestFirst, Some(5)));

    // A column is not something rows are ordered by, and the clause comes before LIMIT.
    let src = r#"MATCH LOG {} ORDER BY ts"#;
    let e = parse(src, now()).unwrap_err();
    assert_eq!(&src[e.pos..], "ts");
    assert!(e.to_string().contains("`time`"), "{e}");
    for src in [
        r#"MATCH LOG {} ORDER time"#,
        r#"MATCH LOG {} ORDER BY"#,
        r#"MATCH LOG {} LIMIT 5 ORDER BY time"#,
        r#"MATCH LOG {} ORDER BY time UP"#,
    ] {
        assert!(parse(src, now()).is_err(), "{src}");
    }
}

/// `limit_clause`. `None` and `Some(0)` are different answers.
#[test]
fn limit_is_parsed_and_absence_is_not_zero() {
    let q = |src: &str| parse(src, now()).unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(q(r#"MATCH LOG {} LIMIT 10"#).limit, Some(10));
    assert_eq!(q(r#"MATCH LOG {}"#).limit, None);
    assert_eq!(q(r#"MATCH LOG {} LIMIT 0"#).limit, Some(0));
    assert_eq!(
        q(r#"MATCH LOG {"a": *} SINCE 1h GROUP BY b LIMIT 3"#).limit,
        Some(3)
    );
    // Keywords ignore case, as every other one does.
    assert_eq!(q(r#"match log {} limit 7"#).limit, Some(7));

    for bad in [
        r#"MATCH LOG {} LIMIT"#,
        r#"MATCH LOG {} LIMIT -1"#,
        r#"MATCH LOG {} LIMIT 1.5"#,
        r#"MATCH LOG {} LIMIT "10""#,
        r#"MATCH LOG {} LIMIT 99999999999999999999999999"#,
    ] {
        parse(bad, now()).expect_err(bad);
    }
}

/// `after_clause`, and the shape check the parser is answerable for. Whether a
/// well-formed cursor belongs to *this* query is `query::build`'s, not the parser's.
#[test]
fn after_takes_a_cursor_and_nothing_else() {
    let mint = |ts: Option<i64>| {
        let q = Query::new(
            Vec::new(),
            QueryMode::Fetch,
            TimeRange::default(),
            None,
            None,
            std::time::SystemTime::UNIX_EPOCH,
            crate::engine::query::limits::Limits::default(),
        );
        q.cursor_at((std::cmp::Reverse(ts), 3, 17)).encode()
    };
    for ts in [Some(1_700_000_000_000_000_000), None, Some(i64::MIN)] {
        let text = mint(ts);
        let q = parse(&format!(r#"MATCH LOG {{}} LIMIT 5 AFTER "{text}""#), now())
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(q.after.as_deref(), Some(text.as_str()));
        // Round-trips, which is what makes handing `next_cursor` back safe.
        assert_eq!(Cursor::decode(&text).expect("a cursor").encode(), text);
    }
    assert_eq!(parse("MATCH LOG {}", now()).unwrap().after, None);

    let empty = r#"MATCH LOG {} AFTER """#;
    for bad in [
        r#"MATCH LOG {} AFTER"#,
        empty,
        r#"MATCH LOG {} AFTER "v1""#,
        r#"MATCH LOG {} AFTER "v1.zz""#,
        r#"MATCH LOG {} AFTER "v9.0000""#,
        // Right length, wrong alphabet - the hex check is not a length check.
        &format!(r#"MATCH LOG {{}} AFTER "v1.{}""#, "g".repeat(58)),
    ] {
        parse(bad, now()).expect_err(bad);
    }
}

/// `group_key`'s two alternatives. `time` is a column name until a `(` follows it,
/// which is the same rule `pair` applies to `*` and `"*"`.
#[test]
fn a_time_bucket_is_a_parenthesis_away_from_a_column_named_time() {
    let q = |src: &str| parse(src, now()).unwrap_or_else(|e| panic!("{src}: {e}"));
    let keys = |src: &str| match q(src).mode {
        QueryMode::GroupBy(keys) => keys,
        other => panic!("{src}: expected a group by, got {other:?}"),
    };

    assert_eq!(
        keys("COUNT LOG {} SINCE 6h GROUP BY time(5m)"),
        vec![GroupKey::Time(Duration::from_secs(300))]
    );
    // Every unit the grammar spells, and the keyword's case ignored as every other
    // keyword's is. The width's unit is not a keyword and keeps its own case.
    assert_eq!(
        keys("count log {} since 6h group by TIME(2d)"),
        vec![GroupKey::Time(Duration::from_secs(172_800))]
    );
    // A bare `time` with nothing after it and a quoted one before a parenthesis both name
    // the column: the second is the escape for a column that really is called `time`.
    assert_eq!(
        keys("COUNT LOG {} GROUP BY time"),
        vec![GroupKey::Column("time".to_string())]
    );
    assert_eq!(
        keys(r#"COUNT LOG {} GROUP BY "time""#),
        vec![GroupKey::Column("time".to_string())]
    );
    // Mixed, in the order written - which is the order every surface prints.
    assert_eq!(
        keys("COUNT LOG {} SINCE 6h GROUP BY time(5m), level"),
        vec![
            GroupKey::Time(Duration::from_secs(300)),
            GroupKey::Column("level".to_string()),
        ]
    );
    assert_eq!(
        keys("COUNT LOG {} SINCE 6h GROUP BY level, time(1h)"),
        vec![
            GroupKey::Column("level".to_string()),
            GroupKey::Time(Duration::from_secs(3_600)),
        ]
    );

    for bad in [
        "COUNT LOG {} GROUP BY time(",
        "COUNT LOG {} GROUP BY time()",
        "COUNT LOG {} GROUP BY time(5m",
        "COUNT LOG {} GROUP BY time(5)",
        "COUNT LOG {} GROUP BY time(\"5m\")",
        "COUNT LOG {} GROUP BY time(-5m)",
        // A zero width has no boundaries to floor against.
        "COUNT LOG {} GROUP BY time(0s)",
        // One time axis. Two is well-formed above and means nothing below.
        "COUNT LOG {} GROUP BY time(5m), time(1h)",
        "COUNT LOG {} GROUP BY time(5m), level, time(5m)",
    ] {
        parse(bad, now()).expect_err(bad);
    }
}

/// `{}` is legal syntax and means every template: the pair list is optional.
#[test]
fn the_empty_pattern_parses_to_no_terms() {
    for src in [
        "MATCH LOG {}",
        "COUNT LOG {}",
        "MATCH LOG {} SINCE 1h LIMIT 10",
        "MATCH LOG {   }",
    ] {
        let q = parse(src, now()).unwrap_or_else(|e| panic!("{src}: {e}"));
        assert!(q.matches.is_empty(), "{src}");
    }
}
