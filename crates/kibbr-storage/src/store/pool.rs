// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The texts and values the filter's tests are run over.
//!
//! One list, shared, because the tests that use it hold different pieces of code to the
//! same contract, and a value one of them never tried is a value nothing says they agree on.

use crate::store::Row;

/// Term texts, and the JSON strings a line carries; no text is here twice.
pub const TEXTS: &[&str] = &[
    // Text built to sit on every rung's edge: the list the decoder's shortcut for plain
    // text is held to the ladder over, in `v3::read`'s tests.
    "",
    "INFO",
    "GET",
    "error",
    "debug",
    "abc",
    "deadbeef",
    "fe80::1",
    "::1",
    "false",
    "False",
    "true",
    "True",
    "TRUE",
    "inf",
    "NaN",
    "infinity",
    "-inf",
    "1e3",
    ".5",
    "+5",
    "-0",
    "0",
    "2024-01-15",
    "2024-01-15T10:30:00Z",
    "1705312200",
    "550e8400-e29b-41d4-a716-446655440000",
    "10.0.0.1",
    "u42@auth.example.com",
    "Tuesday",
    "a:b",
    "ab.cd",
    "c0ffee",
    // One number in several spellings, and numbers that are equal only as numbers.
    "+0",
    "00",
    "45",
    "0045",
    "+45",
    "200",
    "200.0",
    "2e2",
    "1000.0",
    "0.010",
    "0.01",
    "149.00",
    // One instant in the forms the ladder takes, and two it refuses: a space for the
    // `T`, and a second of 60.
    "2024-01-15T10:30:00.000Z",
    "2024-01-15t10:30:00z",
    "2024-01-15 10:30:00Z",
    "2024-01-15T16:00:00+05:30",
    "2024-01-15T10:30:60Z",
    // The same instant as epoch seconds and milliseconds, then ten digits that are no epoch,
    // and the seconds signed and zero-led, which are integers and the digits of no date.
    "1705314600",
    "1705314600000",
    "0946684800",
    "4500000101",
    "+1705314600",
    "01705314600",
    // The UUID above in upper and in mixed case.
    "550E8400-E29B-41D4-A716-446655440000",
    "550e8400-E29B-41d4-A716-446655440000",
    // One address in three spellings, and a mapped one.
    "2001:db8::1",
    "2001:DB8::1",
    "::ffff:192.0.2.1",
    "2001:0db8:0000:0000:0000:0000:0000:0001",
    // Two past `i64::MAX`: an integer no `Int64` holds.
    "9223372036854775809",
];

/// The JSON a line can carry that is not a string, each also a text above so the same
/// characters are ingested quoted and bare; and `null`.
pub const LITERALS: &[&str] = &[
    "0",
    "45",
    "200",
    "200.0",
    "1e3",
    "true",
    "false",
    "1705314600",
    "1705314600000",
    "9223372036854775809",
    "null",
];

/// Every value of the pool as the JSON a line writes it: each text as a string, then
/// each literal bare.
pub fn written() -> Vec<String> {
    TEXTS
        .iter()
        .map(|text| serde_json::Value::String((*text).to_string()).to_string())
        .chain(LITERALS.iter().map(|literal| (*literal).to_string()))
        .collect()
}

/// The row ingest stores for a line whose one key, `v`, holds `json`, through the parser
/// and `db::row_of`.
pub fn row_of(json: &str) -> Row {
    let line = format!("{{\"v\":{json}}}");
    let parsed = crate::core::flatten::parse_line(line.as_bytes(), 1).expect("a line of one key");
    crate::db::row_of(&parsed)
}

/// One row for every value of the pool, in `written`'s order.
pub fn rows() -> Vec<Row> {
    written().iter().map(|json| row_of(json)).collect()
}

/// Values as a line writes them, one type in one spelling per list, so a few draws reach
/// every way a writer lays a column out.
pub const KINDS: &[&[&str]] = &[
    // Words.
    &[
        r#""INFO""#,
        r#""GET""#,
        r#""error""#,
        r#""debug""#,
        r#""abc""#,
        r#""Tuesday""#,
        r#""""#,
        r#""u42@auth.example.com""#,
        r#""deadbeef""#,
        r#""c0ffee""#,
        r#""a:b""#,
        r#""ab.cd""#,
    ],
    // Text the ladder types, or nearly does, and that no form prints back.
    &[
        r#""-0""#,
        r#"".5""#,
        r#""1e3""#,
        r#""2e2""#,
        r#""TRUE""#,
        r#""inf""#,
        r#""NaN""#,
        r#""-inf""#,
        r#""infinity""#,
        r#""2024-01-15""#,
        r#""2024-01-15 10:30:00Z""#,
        r#""2024-01-15T10:30:60Z""#,
        r#""9223372036854775809""#,
    ],
    // Integers: as strings, padded, signed, and bare.
    &[
        r#""0""#,
        r#""45""#,
        r#""200""#,
        r#""-17""#,
        r#""4500000101""#,
    ],
    &[r#""0045""#, r#""00""#, r#""0200""#, r#""0946684800""#],
    &[r#""+45""#, r#""+0""#, r#""+5""#, r#""+200""#],
    &["0", "45", "200", "-17", "9223372036854775807"],
    // Floats: one digit count, several, and bare.
    &[r#""200.0""#, r#""1000.0""#, r#""12.5""#, r#""0.5""#],
    &[
        r#""0.010""#,
        r#""0.01""#,
        r#""149.00""#,
        r#""12.500""#,
        r#""200.0""#,
    ],
    &["200.0", "1e3", "0.5", "1.5", "-0.0", "9223372036854775809"],
    // Booleans.
    &[r#""true""#, r#""false""#],
    &[r#""True""#, r#""False""#],
    &["true", "false"],
    // Dates: whole seconds, mixed fraction digits, small letters, an offset.
    &[
        r#""2024-01-15T10:30:00Z""#,
        r#""2024-01-15T10:30:01Z""#,
        r#""2024-01-15T10:29:59Z""#,
    ],
    &[
        r#""2024-01-15T10:30:00.000Z""#,
        r#""2024-01-15T10:30:00.5Z""#,
        r#""2024-01-15T10:30:00.120Z""#,
        r#""2024-01-15T10:30:00Z""#,
    ],
    &[r#""2024-01-15t10:30:00z""#, r#""2024-01-15t10:30:01z""#],
    &[
        r#""2024-01-15T16:00:00+05:30""#,
        r#""2024-01-15T16:00:01+05:30""#,
    ],
    // Epoch seconds and milliseconds, quoted and bare.
    &[r#""1705314600""#, r#""1705312200""#],
    &["1705314600", "1705312200"],
    &[r#""1705314600000""#, r#""1705312200123""#],
    &["1705314600000", "1705312200123"],
    // Ids: small letters, capitals, and both, which is text.
    &[
        r#""550e8400-e29b-41d4-a716-446655440000""#,
        r#""6ba7b810-9dad-11d1-80b4-00c04fd430c8""#,
    ],
    &[
        r#""550E8400-E29B-41D4-A716-446655440000""#,
        r#""6BA7B810-9DAD-11D1-80B4-00C04FD430C8""#,
    ],
    &[r#""550e8400-E29B-41d4-A716-446655440000""#],
    // Addresses, in each spelling one has.
    &[r#""10.0.0.1""#, r#""10.0.0.2""#, r#""192.0.2.1""#],
    &[
        r#""2001:db8::1""#,
        r#""::1""#,
        r#""fe80::1""#,
        r#""::ffff:192.0.2.1""#,
    ],
    &[r#""2001:DB8::1""#, r#""FE80::1""#],
    &[
        r#""2001:0db8:0000:0000:0000:0000:0000:0001""#,
        r#""2001:0db8:0000:0000:0000:0000:0000:0002""#,
    ],
    &[r#""2001:0DB8:0000:0000:0000:0000:0000:0001""#],
];

/// Every text of the pool and every value of `KINDS`, as the text a term asks for: a
/// string's characters, a bare number or bool as it is written.
pub fn terms() -> Vec<String> {
    let mut texts: std::collections::BTreeSet<String> =
        TEXTS.iter().map(|text| (*text).to_string()).collect();
    for json in KINDS.iter().flat_map(|kind| kind.iter()) {
        texts.insert(
            match serde_json::from_str(json).expect("a value a line can carry") {
                serde_json::Value::String(text) => text,
                _ => (*json).to_string(),
            },
        );
    }
    texts.into_iter().collect()
}
