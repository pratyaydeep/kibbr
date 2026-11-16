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

const INT: Option<ColumnType> = Some(ColumnType::Int64);
const DATE: Option<ColumnType> = Some(ColumnType::Datetime);
const TEXT: Option<ColumnType> = Some(ColumnType::String);

/// One value as `widen` is told of it: its type, and whether rule 2 read it.
type Value = (Option<ColumnType>, bool);
const DIGITS: Value = (DATE, true);
const RFC: Value = (DATE, false);
const WHOLE: Value = (INT, false);

/// A column over these values in this order, from no evidence at all.
fn folded(values: &[Value]) -> (Option<ColumnType>, Option<bool>) {
    values.iter().fold((None, None), |(column, digits), value| {
        widen(column, digits, value.0, value.1)
    })
}

/// **A `Datetime` column of epoch digits that meets an integer is `Int64`, and an `Int64`
/// column that meets rule 2's digits stays `Int64`**: neither is `String` any more.
#[test]
fn a_datetime_of_digits_and_an_integer_widen_to_int64() {
    // MongoDB's `attr.Available`: epoch digits, then 999999999.
    assert_eq!(folded(&[DIGITS]), (DATE, Some(true)));
    assert_eq!(folded(&[DIGITS, WHOLE]), (INT, None));
    // CockroachDB's `event.GoAllocBytes`: integers, then 1220305536.
    assert_eq!(folded(&[WHOLE, DIGITS]), (INT, None));
    // And from there the column is one of integers, whichever comes next.
    assert_eq!(folded(&[DIGITS, DIGITS, WHOLE, DIGITS, WHOLE]), (INT, None));
    assert_eq!(widen(INT, None, INT, false), (INT, None));
}

/// **An RFC 3339 date is no integer**: a column that has held one is `String` when an
/// integer arrives, and an `Int64` column is `String` when one arrives, as both always
/// were. A column says it has held one from then on, whatever digits follow.
#[test]
fn an_rfc3339_date_and_an_integer_are_still_string() {
    assert_eq!(folded(&[RFC]), (DATE, Some(false)));
    assert_eq!(folded(&[RFC, WHOLE]), (TEXT, None));
    assert_eq!(folded(&[WHOLE, RFC]), (TEXT, None));
    assert_eq!(folded(&[DIGITS, RFC]), (DATE, Some(false)));
    assert_eq!(folded(&[RFC, DIGITS]), (DATE, Some(false)));
    assert_eq!(folded(&[RFC, DIGITS, DIGITS]), (DATE, Some(false)));
    assert_eq!(folded(&[DIGITS, RFC, DIGITS, WHOLE]), (TEXT, None));
    assert_eq!(folded(&[DIGITS, WHOLE, RFC]), (TEXT, None));
}

/// **The column's type does not depend on the order its values arrived in**, for every
/// arrangement of epoch digits, RFC 3339 dates, integers and nulls up to six values long:
/// which is what the fact a `Datetime` column keeps about its values is for.
#[test]
fn dates_and_integers_widen_alike_in_every_order() {
    const NULL: Value = (None, false);
    let kinds = [DIGITS, RFC, WHOLE, NULL];
    let (mut sequences, mut ints, mut texts, mut dates) = (0, 0, 0, 0);
    for len in 1..=6u32 {
        for code in 0..4usize.pow(len) {
            let values: Vec<Value> = (0..len)
                .map(|at| kinds[code / 4usize.pow(at) % 4])
                .collect();
            let has = |kind: Value| values.contains(&kind);
            // By what the values are, and not by where they stand.
            let want = match (has(DIGITS), has(RFC), has(WHOLE)) {
                (false, false, false) => None,
                (_, _, false) => DATE,
                (_, false, true) => INT,
                (_, true, true) => TEXT,
            };
            let (got, digits) = folded(&values);
            assert_eq!(got, want, "{values:?}");
            // And a `Datetime` column says truly whether every date in it was digits.
            assert_eq!(digits, (got == DATE).then_some(!has(RFC)), "{values:?}");
            sequences += 1;
            ints += usize::from(got == INT);
            texts += usize::from(got == TEXT);
            dates += usize::from(got == DATE);
        }
    }
    assert!(
        sequences > 5_000 && ints > 500 && texts > 500 && dates > 500,
        "{sequences} {ints} {texts} {dates}"
    );
}

/// **A `Datetime` column that does not say what its values were** - one from a catalog
/// written before the fact was kept - **takes an integer as the sign that they are
/// counts**, and learns that they are not from the first RFC 3339 date it meets.
#[test]
fn a_datetime_column_of_unrecorded_values_is_taken_for_counts() {
    assert_eq!(widen(DATE, None, INT, false), (INT, None));
    // Digits tell it nothing new: one value says nothing of the ones before it.
    assert_eq!(widen(DATE, None, DATE, true), (DATE, None));
    // An RFC 3339 date does, and from then on an integer makes the column text.
    assert_eq!(widen(DATE, None, DATE, false), (DATE, Some(false)));
    assert_eq!(widen(DATE, Some(false), INT, false), (TEXT, None));
    assert_eq!(widen(DATE, Some(true), INT, false), (INT, None));
    // A null changes neither the type nor what the column says.
    for digits in [None, Some(true), Some(false)] {
        assert_eq!(widen(DATE, digits, None, false), (DATE, digits));
    }
}

/// **Every other widening is the join it was**, for every pair of types, whichever rule
/// read the incoming value and whatever the column says of itself.
#[test]
fn every_other_widening_is_the_join() {
    let mut same = 0;
    for column in TYPES {
        for incoming in TYPES {
            for epoch_digits in [false, true] {
                for digits in [None, Some(true), Some(false)] {
                    let int_and_digits = column == INT && incoming == DATE && epoch_digits;
                    let date_and_int = column == DATE && incoming == INT && digits != Some(false);
                    if int_and_digits || date_and_int {
                        continue;
                    }
                    assert_eq!(
                        widen(column, digits, incoming, epoch_digits).0,
                        join(column, incoming),
                        "{column:?} ({digits:?}) and {incoming:?}, epoch digits: {epoch_digits}"
                    );
                    same += 1;
                }
            }
        }
    }
    assert_eq!(same, 8 * 8 * 2 * 3 - 3 - 4);
    // Named, because they are the ones a reader would guess had changed with it.
    let float = Some(ColumnType::Float64);
    assert_eq!(widen(DATE, Some(true), float, false).0, TEXT);
    assert_eq!(widen(float, None, DATE, true).0, TEXT);
    assert_eq!(widen(DATE, Some(true), TEXT, false).0, TEXT);
    assert_eq!(widen(TEXT, None, DATE, true).0, TEXT);
    assert_eq!(widen(TEXT, None, INT, false).0, TEXT);
    assert_eq!(
        widen(DATE, Some(true), Some(ColumnType::Bool), false).0,
        TEXT
    );
    assert_eq!(widen(INT, None, float, false), (float, None));
    // Only a `Datetime` column says anything of its values.
    for column in TYPES {
        for incoming in TYPES {
            let (ty, digits) = widen(column, None, incoming, true);
            assert!(
                digits.is_none() || ty == DATE,
                "{column:?} and {incoming:?}"
            );
        }
    }
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
