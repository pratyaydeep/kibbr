// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a time column reads beyond the ladder, and that the ladder reads none of it.
//! Every test runs under the default epoch window, 2000 up to 2100: nothing in this
//! crate's tests sets another.

use super::*;

/// 2100-01-01T00:00:00Z in seconds: the first moment outside the window.
const END: i64 = 4_102_444_800;

fn ty(raw: &str) -> Option<ColumnType> {
    infer(raw).ty
}

/// The instant an RFC 3339 text names, through rule 1: what a form is held to.
fn rfc(text: &str) -> i64 {
    match infer(text).value {
        ColValue::Datetime(ns) => ns,
        other => panic!("{text} is not a date: {other:?}"),
    }
}

/// **Decimal epoch seconds are read to the nanosecond that was written**, with one to
/// nine fraction digits - CockroachDB's `1679711330.570420890` among them.
#[test]
fn decimal_seconds_are_read_exactly_with_one_to_nine_fraction_digits() {
    assert_eq!(
        time_form_nanos("1679711330.570420890"),
        Some(1_679_711_330_570_420_890)
    );
    // Read through a float it would be another nanosecond: a double of seconds this
    // century is spaced 238 nanoseconds apart.
    let through_a_float = "1679711330.570420890".parse::<f64>().expect("a float") * 1e9;
    assert_ne!(through_a_float as i64, 1_679_711_330_570_420_890);
    let cases = [
        ("1679711330.5", 500_000_000),
        ("1679711330.57", 570_000_000),
        ("1679711330.570", 570_000_000),
        ("1679711330.5704", 570_400_000),
        ("1679711330.57042", 570_420_000),
        ("1679711330.570420", 570_420_000),
        ("1679711330.5704208", 570_420_800),
        ("1679711330.57042089", 570_420_890),
        ("1679711330.000000001", 1),
        ("1679711330.999999999", 999_999_999),
        ("1679711330.0", 0),
    ];
    for (text, sub) in cases {
        assert_eq!(
            time_form_nanos(text),
            Some(1_679_711_330 * 1_000_000_000 + sub),
            "{text}"
        );
        assert_eq!(time_nanos(text), time_form_nanos(text), "{text}");
    }
}

/// **A tenth fraction digit is more than a nanosecond holds, and is not a time**; nor is
/// any decimal that is not ten digits of seconds, a point, and digits.
#[test]
fn a_decimal_with_ten_fraction_digits_or_another_shape_is_not_a_time() {
    for text in [
        "1679711330.5704208901",
        "1679711330.0000000000",
        "1679711330.",
        ".570420890",
        "167971133.570420890",
        "16797113300.570420890",
        "0679711330.570420890",
        "-1679711330.570420890",
        "+1679711330.570420890",
        "1679711330.5704e2",
        "1679711330.57 ",
        " 1679711330.57",
        "1679711330,57",
        "1679711330.57.1",
        "1.67971133057042089e9",
    ] {
        assert_eq!(time_form_nanos(text), None, "{text}");
        assert_eq!(time_nanos(text), None, "{text}");
    }
}

/// **Sixteen digits are epoch microseconds and nineteen are nanoseconds** - Bluesky's
/// `time_us` - and no other count of digits is read by these forms.
#[test]
fn sixteen_digits_are_microseconds_and_nineteen_are_nanoseconds() {
    assert_eq!(
        time_form_nanos("1732206349000167"),
        Some(1_732_206_349_000_167_000)
    );
    assert_eq!(
        time_form_nanos("1679711330570420"),
        Some(1_679_711_330_570_420_000)
    );
    assert_eq!(
        time_form_nanos("1679711330570420890"),
        Some(1_679_711_330_570_420_890)
    );
    for text in [
        "167971133057042",
        "16797113305704208",
        "167971133057042089",
        "16797113305704208901",
        "0679711330570420",
        "0679711330570420890",
        "-679711330570420",
        "+679711330570420",
        "167971133057042x",
    ] {
        assert_eq!(time_form_nanos(text), None, "{text}");
    }
}

/// **The epoch forms are held to the epoch window, as rule 2 is**: the last moment of
/// 2099 reads, the first of 2100 does not. The window opens at 2000-01-01, which is nine
/// digits of seconds and fifteen of microseconds, so - as for rule 2 - the first value a
/// form can spell, 2001-09-09T01:46:40Z, is already inside it.
#[test]
fn values_just_outside_the_epoch_window_are_not_times() {
    // Seconds with a fraction.
    assert_eq!(
        time_form_nanos("4102444799.999999999"),
        Some((END - 1) * 1_000_000_000 + 999_999_999)
    );
    assert_eq!(time_form_nanos("4102444800.0"), None);
    assert_eq!(time_form_nanos("4102444800.000000001"), None);
    assert_eq!(time_form_nanos("9999999999.5"), None);
    assert_eq!(
        time_form_nanos("1000000000.0"),
        Some(1_000_000_000_000_000_000)
    );
    // Below the window there are only nine digits of seconds, which is not the form.
    assert_eq!(time_form_nanos("946684799.5"), None);
    assert_eq!(time_form_nanos("999999999.999999999"), None);

    // Microseconds.
    assert_eq!(
        time_form_nanos("4102444799999999"),
        Some((END * 1_000_000 - 1) * 1_000)
    );
    assert_eq!(time_form_nanos("4102444800000000"), None);
    assert_eq!(time_form_nanos("9999999999999999"), None);
    assert_eq!(
        time_form_nanos("1000000000000000"),
        Some(1_000_000_000_000_000_000)
    );
    assert_eq!(time_form_nanos("946684799999999"), None);

    // Nanoseconds; nineteen nines are past what an i64 holds at all.
    assert_eq!(
        time_form_nanos("4102444799999999999"),
        Some(END * 1_000_000_000 - 1)
    );
    assert_eq!(time_form_nanos("4102444800000000000"), None);
    assert_eq!(time_form_nanos("9223372036854775807"), None);
    assert_eq!(time_form_nanos("9999999999999999999"), None);
    assert_eq!(
        time_form_nanos("1000000000000000000"),
        Some(1_000_000_000_000_000_000)
    );
    assert_eq!(time_form_nanos("946684799999999999"), None);
}

/// **A date and time with a named zone is the instant that zone's offset makes it** -
/// PostgreSQL's `2023-03-27 00:26:35.719 EDT` - for each of the eleven names, and each
/// name is held to the RFC 3339 spelling of its offset.
#[test]
fn every_listed_zone_reads_at_its_offset() {
    assert_eq!(
        time_form_nanos("2023-03-27 00:26:35.719 EDT"),
        Some(rfc("2023-03-27T04:26:35.719Z"))
    );
    assert_eq!(
        time_form_nanos("2023-03-27 00:26:35.719 EDT"),
        Some(1_679_891_195_719_000_000)
    );
    let offsets = [
        ("UTC", "+00:00"),
        ("GMT", "+00:00"),
        ("Z", "+00:00"),
        ("EST", "-05:00"),
        ("EDT", "-04:00"),
        ("CST", "-06:00"),
        ("CDT", "-05:00"),
        ("MST", "-07:00"),
        ("MDT", "-06:00"),
        ("PST", "-08:00"),
        ("PDT", "-07:00"),
    ];
    // Every name the reader knows is in the table above, and nothing else is.
    let known: Vec<&str> = ZONES.iter().map(|(name, _)| *name).collect();
    let listed: Vec<&str> = offsets.iter().map(|(name, _)| *name).collect();
    assert_eq!(known, listed);
    for (zone, offset) in offsets {
        for (clock, fraction) in [
            ("2023-03-27 00:26:35", ""),
            ("2023-03-27 00:26:35", ".7"),
            ("2023-03-27 00:26:35", ".719"),
            ("2023-03-27 00:26:35", ".719000001"),
            ("2024-02-29 23:59:59", ".999999999"),
            ("2023-12-31 20:00:00", ""),
        ] {
            let text = format!("{clock}{fraction} {zone}");
            let same = format!("{}{fraction}{offset}", clock.replace(' ', "T"));
            assert_eq!(time_form_nanos(&text), Some(rfc(&same)), "{text}");
            assert_eq!(time_nanos(&text), Some(rfc(&same)), "{text}");
        }
    }
}

/// **`IST` is India, Israel and Ireland, and is not read**; nor is any name outside the
/// list, any spelling of a listed one but its own, or any date that is not that shape.
#[test]
fn ist_and_every_other_zone_or_shape_is_not_a_time() {
    for text in [
        "2023-03-27 00:26:35.719 IST",
        "2023-03-27 00:26:35 IST",
        "2023-03-27 00:26:35.719 CET",
        "2023-03-27 00:26:35.719 CEST",
        "2023-03-27 00:26:35.719 BST",
        "2023-03-27 00:26:35.719 JST",
        "2023-03-27 00:26:35.719 AEST",
        "2023-03-27 00:26:35.719 AKST",
        "2023-03-27 00:26:35.719 HST",
        "2023-03-27 00:26:35.719 edt",
        "2023-03-27 00:26:35.719 Edt",
        "2023-03-27 00:26:35.719 z",
        "2023-03-27 00:26:35.719 +0000",
        "2023-03-27 00:26:35.719 -04:00",
        "2023-03-27 00:26:35.719 America/New_York",
        "2023-03-27 00:26:35.719",
        "2023-03-27 00:26:35.719 ",
        "2023-03-27 00:26:35.719  EDT",
        "2023-03-27 00:26:35.719EDT",
        "2023-03-27T00:26:35.719 EDT",
        "2023-03-27  00:26:35.719 EDT",
        "2023-03-27 00:26:35. EDT",
        "2023-03-27 00:26:35.7190000001 EDT",
        "2023-03-27 00:26:35,719 EDT",
        "2023-03-27 00:26 EDT",
        "2023-03-27 0:26:35 EDT",
        "2023-3-27 00:26:35 EDT",
        "23-03-27 00:26:35 EDT",
        "2023/03/27 00:26:35 EDT",
        "2023-03-27 00:26:60 EDT",
        "2023-03-27 24:00:00 EDT",
        "2023-03-27 00:60:00 EDT",
        "2023-02-29 00:26:35 EDT",
        "2023-13-01 00:26:35 EDT",
        "2023-00-10 00:26:35 EDT",
        "2023-03-27 00:26:35 EDT ",
        " 2023-03-27 00:26:35 EDT",
        "EDT",
        " EDT",
    ] {
        assert_eq!(time_form_nanos(text), None, "{text}");
        assert_eq!(time_nanos(text), None, "{text}");
    }
    // A zoned date the type cannot hold exactly is not one either: 2262 is where
    // nanoseconds run out.
    assert_eq!(
        time_form_nanos("2262-04-11 23:47:16 UTC"),
        Some(i64::MAX / 1_000_000_000 * 1_000_000_000)
    );
    assert_eq!(time_form_nanos("2262-04-11 23:47:17 UTC"), None);
    assert_eq!(time_form_nanos("9999-12-31 23:59:59 UTC"), None);
}

/// **The ladder reads none of these forms**: under a key that is not a time column each
/// is the number or the string it always was - a sixteen-digit id stays `Int64`.
#[test]
fn the_ladder_types_every_time_column_form_as_it_did() {
    let id = infer("1732206349000167");
    assert_eq!(id.ty, Some(ColumnType::Int64));
    assert_eq!(id.value, ColValue::Int64(1_732_206_349_000_167));
    assert_eq!(id.dt_form, None);
    assert_eq!(
        infer("1679711330570420890").value,
        ColValue::Int64(1_679_711_330_570_420_890)
    );
    assert_eq!(ty("1679711330.570420890"), Some(ColumnType::Float64));
    assert_eq!(ty("1679711330.5"), Some(ColumnType::Float64));
    assert_eq!(ty("2023-03-27 00:26:35.719 EDT"), Some(ColumnType::String));
    assert_eq!(ty("2023-03-27 00:26:35 UTC"), Some(ColumnType::String));
    // And what the ladder did read, it reads as before.
    assert_eq!(ty("1679711330"), Some(ColumnType::Datetime));
    assert_eq!(infer("1679711330").dt_form, Some(DtForm::EpochSecs));
    assert_eq!(infer("1679711330570").dt_form, Some(DtForm::EpochMillis));
    assert_eq!(ty("999999999"), Some(ColumnType::Int64));
    assert_eq!(ty("1220305536"), Some(ColumnType::Datetime));
    assert_eq!(ty("4102444800"), Some(ColumnType::Int64));
    assert_eq!(ty("2023-03-27 00:26:35Z"), Some(ColumnType::Datetime));
}

/// **A time column reads what the ladder reads as a time too**, to the same instant, so
/// a row's time is one reading whichever form the line used.
#[test]
fn time_nanos_reads_the_ladders_dates_as_the_ladder_does() {
    for text in [
        "2024-01-15T10:30:00Z",
        "2024-01-15T16:00:00.123456789+05:30",
        "2024-01-15 10:30:00Z",
        "1705314600",
        "1705314600123",
        "1000000000",
        "4102444799",
        "4102444799999",
    ] {
        assert_eq!(time_nanos(text), Some(rfc_or_epoch(text)), "{text}");
        assert_eq!(time_form_nanos(text), None, "{text} is the ladder's");
    }
    for text in [
        "4102444800",
        "999999999",
        "0705314600",
        "N/A",
        "",
        "true",
        "12.5",
    ] {
        assert_eq!(time_nanos(text), None, "{text}");
    }
}

fn rfc_or_epoch(text: &str) -> i64 {
    match infer(text).value {
        ColValue::Datetime(ns) => ns,
        other => panic!("{text}: {other:?}"),
    }
}

/// **An integer is read as its plain decimal digits are**, with no text made: the scan's
/// reading of a column of integers is the row's reading of the line.
#[test]
fn an_integers_time_is_the_time_of_its_digits() {
    let mut values = vec![0, 1, -1, 42, i64::MAX, i64::MIN, -1_732_206_349_000_167];
    for digits in 1..=19u32 {
        let low = 10i64.pow(digits - 1);
        values.extend([low, low + 1, low * 2, low.saturating_mul(9), low - 1]);
    }
    for base in [946_684_800, 1_000_000_000, 1_679_711_330, END - 1, END] {
        for unit in [1, 1_000, 1_000_000, 1_000_000_000i64] {
            if let Some(scaled) = base.checked_mul(unit) {
                values.extend([scaled - 1, scaled, scaled + 1]);
            }
        }
    }
    let mut read = 0;
    for value in values {
        assert_eq!(
            time_nanos_of_int(value),
            time_nanos(&value.to_string()),
            "{value}"
        );
        read += usize::from(time_nanos_of_int(value).is_some());
    }
    assert!(read >= 20, "only {read} of the integers were times");
}

/// **Rule 2's instant divides back to the digits it was read from, exactly**: the
/// instant is the count times a whole number of nanoseconds, across the whole window, so
/// a column of integers holds the digits and loses nothing.
#[test]
fn epoch_digits_are_the_count_the_instant_divides_back_to() {
    let cases = [
        ("1220305536", 1_000_000_000),
        ("1000000000", 1_000_000_000),
        ("1679711330", 1_000_000_000),
        ("4102444799", 1_000_000_000),
        ("1000000000000", 1_000_000),
        ("1679711330570", 1_000_000),
        ("4102444799999", 1_000_000),
    ];
    for (text, per_unit) in cases {
        let count: i64 = text.parse().expect("digits");
        let typed = infer(text);
        let ColValue::Datetime(ns) = typed.value else {
            panic!("{text} is not a date");
        };
        assert!(typed.is_epoch_digits(), "{text}");
        assert_eq!(epoch_digits(text), Some(count), "{text}");
        assert_eq!((ns / per_unit, ns % per_unit), (count, 0), "{text}");
        assert_eq!(ns, count * per_unit, "{text}");
    }
    // Not rule 2's: outside the window, a leading zero, another length, RFC 3339.
    for text in [
        "999999999",
        "4102444800",
        "4102444800000",
        "0220305536",
        "12203055360",
        "2024-01-15T10:30:00Z",
        "1679711330.5",
    ] {
        assert_eq!(epoch_digits(text), None, "{text}");
        assert!(!infer(text).is_epoch_digits(), "{text}");
    }
}
