// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

const TYPES: [Option<ColumnType>; 8] = [
    None,
    Some(ColumnType::String),
    Some(ColumnType::Int64),
    Some(ColumnType::Float64),
    Some(ColumnType::Datetime),
    Some(ColumnType::Bool),
    Some(ColumnType::Uuid),
    Some(ColumnType::Ip),
];

/// **A `Datetime` column that meets an integer is `Int64`, and an `Int64` column that
/// meets rule 2's digits stays `Int64`**: neither is `String` any more.
#[test]
fn a_datetime_and_an_integer_widen_to_int64() {
    let (int, date) = (Some(ColumnType::Int64), Some(ColumnType::Datetime));
    // MongoDB's `attr.Available`: epoch digits, then 999999999.
    assert_eq!(widen(date, int, false), int);
    // CockroachDB's `event.GoAllocBytes`: integers, then 1220305536.
    assert_eq!(widen(int, date, true), int);
    // And from there the column is one of integers, whichever comes next.
    assert_eq!(widen(int, int, false), int);
    assert_eq!(widen(widen(date, int, false), date, true), int);
}

/// **An RFC 3339 date is no integer**: arriving in an `Int64` column it makes the column
/// `String`, as it always did.
#[test]
fn an_rfc3339_date_in_an_integer_column_is_still_string() {
    let (int, date) = (Some(ColumnType::Int64), Some(ColumnType::Datetime));
    assert_eq!(widen(int, date, false), Some(ColumnType::String));
    // Also after the column widened from a date: the next RFC 3339 value says it was
    // never a column of counts.
    assert_eq!(
        widen(widen(date, int, false), date, false),
        Some(ColumnType::String)
    );
}

/// **Every other widening is the join it was**, for every pair of types and whichever
/// rule read the incoming value.
#[test]
fn every_other_widening_is_the_join() {
    let mut same = 0;
    for column in TYPES {
        for incoming in TYPES {
            for epoch_digits in [false, true] {
                let int_and_digits = column == Some(ColumnType::Int64)
                    && incoming == Some(ColumnType::Datetime)
                    && epoch_digits;
                let date_and_int =
                    column == Some(ColumnType::Datetime) && incoming == Some(ColumnType::Int64);
                if int_and_digits || date_and_int {
                    continue;
                }
                assert_eq!(
                    widen(column, incoming, epoch_digits),
                    join(column, incoming),
                    "{column:?} and {incoming:?}, epoch digits: {epoch_digits}"
                );
                same += 1;
            }
        }
    }
    assert_eq!(same, 8 * 8 * 2 - 3);
    // Named, because they are the ones a reader would guess had changed with it.
    let date = Some(ColumnType::Datetime);
    let text = Some(ColumnType::String);
    assert_eq!(widen(date, Some(ColumnType::Float64), false), text);
    assert_eq!(widen(Some(ColumnType::Float64), date, true), text);
    assert_eq!(widen(date, text, false), text);
    assert_eq!(widen(text, date, true), text);
    assert_eq!(widen(text, Some(ColumnType::Int64), false), text);
    assert_eq!(widen(date, Some(ColumnType::Bool), false), text);
    assert_eq!(widen(date, date, true), date);
    assert_eq!(widen(date, date, false), date);
    assert_eq!(widen(None, date, true), date);
    assert_eq!(widen(date, None, false), date);
    assert_eq!(
        widen(Some(ColumnType::Int64), Some(ColumnType::Float64), false),
        Some(ColumnType::Float64)
    );
}

/// **The first configured name that reads as a time is the time source**, in the order
/// configured and not the line's; none is arrival.
#[test]
fn the_time_source_is_the_first_configured_name_that_is_a_time() {
    let names = ["ts".to_string(), "timestamp".into(), "time_us".into()];
    let pick = |times: &[&str]| TimeSource::choose_by(&names, |name| times.contains(&name));
    assert_eq!(pick(&["time_us"]), TimeSource::Column("time_us".into()));
    assert_eq!(
        pick(&["time_us", "timestamp"]),
        TimeSource::Column("timestamp".into())
    );
    assert_eq!(pick(&["other"]), TimeSource::Arrival);
    assert_eq!(pick(&[]), TimeSource::Arrival);
    // By types alone, as a catalog with no `time_source` record is read.
    let typed = TimeSource::choose(&names, |name| match name {
        "ts" => Some(ColumnType::Int64),
        "timestamp" => Some(ColumnType::Datetime),
        _ => None,
    });
    assert_eq!(typed, TimeSource::Column("timestamp".into()));
}
