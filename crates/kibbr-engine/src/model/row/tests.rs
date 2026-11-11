// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::parse::flatten::{parse_line, row_of};

fn row(line: &str) -> Row {
    row_of(&parse_line(line.as_bytes(), 1).expect("a line that parses"))
}

/// **A row's time is its time column in the form the line used**, read from what the
/// line said: the three forms only a time column reads, as a string and as a number.
#[test]
fn a_row_s_time_is_its_time_column_in_a_time_column_form() {
    let cases = [
        (r#"{"t":"1679711330.570420890"}"#, 1_679_711_330_570_420_890),
        (r#"{"t":"1679711330.5"}"#, 1_679_711_330_500_000_000),
        (r#"{"t":1679711330.5}"#, 1_679_711_330_500_000_000),
        (r#"{"t":1679711330.570420}"#, 1_679_711_330_570_420_000),
        (r#"{"t":1679711330.0}"#, 1_679_711_330_000_000_000),
        (r#"{"t":1732206349000167}"#, 1_732_206_349_000_167_000),
        (r#"{"t":"1732206349000167"}"#, 1_732_206_349_000_167_000),
        (r#"{"t":1679711330570420890}"#, 1_679_711_330_570_420_890),
        (
            r#"{"t":"2023-03-27 00:26:35.719 EDT"}"#,
            1_679_891_195_719_000_000,
        ),
    ];
    for (line, ns) in cases {
        let row = row(line);
        // The ladder did not call it a date, and the row holds what the ladder made of it.
        assert!(!matches!(row["t"], ColValue::Datetime(_)), "{line}");
        assert_eq!(ts_of(&row, "t"), None, "{line}");
        assert_eq!(said_ts(&row, "t"), Some(ns), "{line}");
        assert_eq!(own_ts(&row, "t"), Some(ns), "{line}");
        assert_eq!(effective_ts(&row, "t"), Some(ns), "{line}");
        // Under a column that is not the time column it is no time: nothing reads it.
        assert_eq!(effective_ts(&row, "other"), None, "{line}");
    }
}

/// **What the ladder reads as a date is the row's time as before**, and `said_ts` reads
/// the same instant from it.
#[test]
fn a_date_the_ladder_read_is_still_the_row_s_time() {
    for line in [
        r#"{"t":"2024-01-15T10:30:00Z"}"#,
        r#"{"t":"2024-01-15T16:00:00.123+05:30"}"#,
        r#"{"t":1705314600}"#,
        r#"{"t":"1705314600"}"#,
        r#"{"t":1705314600123}"#,
    ] {
        let row = row(line);
        let ColValue::Datetime(ns) = row["t"] else {
            panic!("{line} is not a date");
        };
        assert_eq!(ts_of(&row, "t"), Some(ns), "{line}");
        assert_eq!(said_ts(&row, "t"), Some(ns), "{line}");
        assert_eq!(effective_ts(&row, "t"), Some(ns), "{line}");
    }
}

/// **A time column reads the text the line wrote, not the value it parsed to**, and
/// nothing that is not one of its forms: such a row has no time of its own.
#[test]
fn a_value_in_no_time_column_form_is_not_a_row_s_time() {
    for line in [
        // The integer 1679711330, written in sixteen characters: not a count of seconds.
        r#"{"t":"0000001679711330"}"#,
        r#"{"t":"+1732206349000167"}"#,
        r#"{"t":"+1705314600"}"#,
        r#"{"t":"1679711330.5704208901"}"#,
        r#"{"t":"2023-03-27 00:26:35.719 IST"}"#,
        r#"{"t":4102444800000000}"#,
        r#"{"t":4102444800.5}"#,
        r#"{"t":999999999}"#,
        r#"{"t":12.5}"#,
        r#"{"t":"N/A"}"#,
        r#"{"t":true}"#,
        r#"{"t":null}"#,
        r#"{"t":[1732206349000167]}"#,
        r#"{"u":1732206349000167}"#,
    ] {
        let row = row(line);
        assert_eq!(own_ts(&row, "t"), None, "{line}");
        assert_eq!(effective_ts(&row, "t"), None, "{line}");
    }
}

/// **A row stored with an arrival time is filed under it, whatever its time column now
/// reads as**: the arrival time is there only because the build that stored the row read
/// no time of its own, and the row stays in the day it was put.
#[test]
fn a_row_with_an_arrival_time_keeps_it_over_a_time_column_form() {
    const ARRIVED: i64 = 1_800_000_000_000_000_000;
    for line in [
        r#"{"t":"1679711330.570420890"}"#,
        r#"{"t":1732206349000167}"#,
        r#"{"t":"2023-03-27 00:26:35.719 EDT"}"#,
        r#"{"t":"N/A"}"#,
    ] {
        let mut row = row(line);
        row.insert(RECEIVED_AT_COLUMN.to_string(), ColValue::Datetime(ARRIVED));
        assert_eq!(effective_ts(&row, "t"), Some(ARRIVED), "{line}");
    }
    // A date the ladder read comes first, as it always did.
    let mut dated = row(r#"{"t":"2024-01-15T10:30:00Z"}"#);
    dated.insert(RECEIVED_AT_COLUMN.to_string(), ColValue::Datetime(ARRIVED));
    assert_eq!(effective_ts(&dated, "t"), Some(1_705_314_600_000_000_000));
}

/// **A JSON number's time is its digits as the JSON parser holds them**: exact to the
/// microsecond, which is as much as a double of seconds holds this century; past it the
/// number was rounded before the ladder saw it, as every such number is, and a string
/// is how a line keeps nine digits.
#[test]
fn a_json_number_of_seconds_is_exact_to_the_microsecond() {
    for micros in [0, 1, 7, 570_420, 999_999, 500_000, 123_456] {
        for secs in [
            1_000_000_000_i64,
            1_679_711_330,
            2_147_483_647,
            4_102_444_799,
        ] {
            let line = format!(r#"{{"t":{secs}.{micros:06}}}"#);
            let row = row(&line);
            assert_eq!(
                said_ts(&row, "t"),
                Some(secs * 1_000_000_000 + micros * 1_000),
                "{line}"
            );
        }
    }
    let text = row(r#"{"t":"1679711330.570420891"}"#);
    assert_eq!(said_ts(&text, "t"), Some(1_679_711_330_570_420_891));
    // The same digits as a number are the nearest double's, a few hundred nanoseconds off.
    let number = row(r#"{"t":1679711330.570420891}"#);
    let got = said_ts(&number, "t").expect("still a time");
    assert!((got - 1_679_711_330_570_420_891).abs() < 500, "{got}");
}
