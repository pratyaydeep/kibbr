// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::engine::format::column::Column;
use crate::engine::testkit::row_groups::{self, Sample, Stored};
use crate::engine::testkit::values as pool;
use crate::engine::store::{effective_ts, said_ts, ts_of, RECEIVED_AT_COLUMN};
use crate::engine::rng::Rng;

fn decoded(sample: &Sample, chunk: &Stored) -> Column {
    Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
        .unwrap_or_else(|bad| panic!("{}: {}", sample.name, bad.what))
}

/// Column `v` of the layout sample of this name.
fn layout(name: &str) -> (Sample, usize) {
    let sample = row_groups::layouts()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no sample named `{name}`"));
    let at = sample.place("v").expect("a column v");
    (sample, at)
}

// ── The bitmap ─────────────────────────────────────────────────────────────────

/// A selection made from a list of answers, held to the list every way it is read.
fn held(list: &[bool]) -> Sel {
    let rows = list.len();
    let mut sel = Sel::none(rows);
    for (row, hit) in list.iter().enumerate() {
        if *hit {
            sel.set(row);
        }
    }
    // Three ways to make it, one selection.
    assert_eq!(Sel::of(list, |hit| *hit), sel);
    assert_eq!(Sel::of_each(rows, list.iter().copied()), sel);

    let rows_in: Vec<usize> = (0..rows).filter(|row| list[*row]).collect();
    assert_eq!(sel.rows(), rows);
    assert_eq!(sel.count(), rows_in.len() as u64);
    assert_eq!(sel.is_empty(), rows_in.is_empty());
    assert_eq!(sel.iter().collect::<Vec<_>>(), rows_in);
    for (row, hit) in list.iter().enumerate() {
        assert_eq!(sel.get(row), *hit, "row {row} of {rows}");
    }
    for (n, row) in rows_in.iter().enumerate() {
        assert_eq!(sel.nth(n as u64), Some(*row), "match {n} of {rows} rows");
    }
    assert_eq!(sel.nth(rows_in.len() as u64), None);
    assert_eq!(sel.nth(u64::MAX), None);

    // A row the selection is not over is never in, asked or told.
    let before = sel.clone();
    for past in [rows, rows + 1, rows + 64, 8_192, 100_000] {
        assert!(!sel.get(past));
        sel.set(past);
    }
    assert_eq!(sel, before);
    for bit in rows..WORDS * 64 {
        assert_eq!(
            sel.bits[bit / 64] & (1 << (bit % 64)),
            0,
            "bit {bit} of {rows} rows"
        );
    }
    sel
}

#[test]
fn a_selection_holds_the_rows_it_was_given_and_no_others() {
    for rows in [0usize, 1, 63, 64, 65, 8_191, 8_192] {
        let mut rng = Rng::new(rows as u64 + 1);
        let lists: Vec<Vec<bool>> = vec![
            vec![false; rows],
            vec![true; rows],
            (0..rows).map(|_| rng.chance(50)).collect(),
            (0..rows).map(|_| rng.chance(2)).collect(),
            (0..rows).map(|row| row + 1 == rows).collect(),
            (0..rows)
                .map(|row| row % 64 == 63 || row % 64 == 0)
                .collect(),
        ];
        let sels: Vec<Sel> = lists.iter().map(|list| held(list)).collect();
        assert_eq!(Sel::none(rows), sels[0]);
        assert_eq!(Sel::all(rows), sels[1]);
        assert_eq!(Sel::all(rows).count(), rows as u64);
        assert!(Sel::none(rows).is_empty());
        // ANDing two selections is ANDing their lists, row by row.
        for (a, list_a) in sels.iter().zip(&lists) {
            for (b, list_b) in sels.iter().zip(&lists) {
                let both: Vec<bool> =
                    list_a.iter().zip(list_b).map(|(a, b)| *a && *b).collect();
                let mut and = a.clone();
                and.and(b);
                assert_eq!(and, held(&both), "{rows} rows");
            }
        }
    }
    // A row group holds 8,192 rows, and a selection asked for more is over that many.
    let over = Sel::all(9_000);
    assert_eq!((over.rows(), over.count()), (8_192, 8_192));
    assert_eq!(Sel::of(&vec![true; 9_000], |hit| *hit), over);
    // What a failed comparison prints: the rows, not a hundred and twenty-eight words.
    let mut few = Sel::none(8);
    few.set(1);
    few.set(5);
    assert_eq!(format!("{few:?}"), "[1, 5] of 8 rows");
}

// ── Time ───────────────────────────────────────────────────────────────────────

/// Windows that cut a list of instants every way: open, open at each end, around the
/// middle, holding one instant only, and empty.
fn windows(instants: &[Option<i64>]) -> Vec<(Option<i64>, Option<i64>)> {
    let mut known: Vec<i64> = instants.iter().flatten().copied().collect();
    known.sort_unstable();
    let mut out = vec![
        (None, None),
        (Some(0), Some(0)),
        (Some(i64::MIN), Some(i64::MAX)),
    ];
    if let (Some(first), Some(last)) = (known.first(), known.last()) {
        let middle = known[known.len() / 2];
        out.extend([
            (Some(middle), None),
            (None, Some(middle)),
            (Some(*first), Some(*last)),
            (Some(*first), Some(last.saturating_add(1))),
            (Some(middle), Some(middle.saturating_add(1))),
            (Some(*last), Some(*first)),
            (Some(last.saturating_add(1)), None),
            (None, Some(*first)),
        ]);
    }
    out
}

/// A column's instants, the window over them and a row's effective time are each what a
/// row a query would build says, over every column of every sample.
#[test]
fn a_column_s_instants_are_what_each_row_s_cell_reads_as() {
    let (mut with, mut without, mut borrowed, mut fell_back) = (0, 0, 0, 0);
    for sample in row_groups::all() {
        let rows = sample.rows;
        let built = sample.built();
        let columns: Vec<Column> = sample
            .chunks
            .iter()
            .map(|chunk| decoded(&sample, chunk))
            .collect();
        let cols: Vec<Col<'_>> = columns
            .iter()
            .zip(&sample.chunks)
            .map(|(column, chunk)| column.over(&chunk.bytes).expect("over its own bytes"))
            .collect();
        for (col, name) in cols.iter().zip(&sample.columns) {
            let got = instants(*col).expect("instants");
            let want: Vec<Option<i64>> = (0..rows)
                .map(|row| instant_of(&col.cell(row).expect("a cell").0))
                .collect();
            for (row, want) in want.iter().enumerate() {
                assert_eq!(got.get(row), *want, "{}, `{name}`, row {row}", sample.name);
                assert_eq!(ts_of(&built[row], name), *want);
            }
            assert_eq!(got.get(rows), None);
            assert_eq!(got.missing(), want.iter().any(Option::is_none) || rows == 0);
            with += want.iter().flatten().count();
            without += want.iter().filter(|ns| ns.is_none()).count();
            borrowed += usize::from(matches!(got.ns, Cow::Borrowed(_)) && got.has.is_none());

            for (since, until) in windows(&want) {
                let sel = got.window(since, until);
                assert_eq!(sel.rows(), rows);
                for (row, ns) in want.iter().enumerate() {
                    assert_eq!(
                        sel.get(row),
                        in_window(*ns, since, until),
                        "{}, `{name}`, row {row}, window {since:?}..{until:?}",
                        sample.name
                    );
                }
            }

            // The row's effective time under this column as its time column: the
            // column's instant, or the arrival time where the row has one stored.
            let arrived = match sample.place(RECEIVED_AT_COLUMN) {
                Some(at) => instants(cols[at]).expect("instants"),
                None => Instants::absent(rows),
            };
            let effective = instants(*col).expect("instants").or(&arrived);
            for (row, built) in built.iter().enumerate() {
                assert_eq!(effective.get(row), effective_ts(built, name));
                fell_back += usize::from(want[row].is_none() && effective.get(row).is_some());
            }
            // And with any column behind it, which is the same rule.
            for (behind, behind_name) in cols.iter().zip(&sample.columns) {
                let both = instants(*col)
                    .expect("instants")
                    .or(&instants(*behind).expect("instants"));
                for (row, built) in built.iter().enumerate() {
                    let want = ts_of(built, name).or_else(|| ts_of(built, behind_name));
                    assert_eq!(
                        both.get(row),
                        want,
                        "{}, `{name}` then `{behind_name}`",
                        sample.name
                    );
                }
                assert_eq!(both.missing(), (0..rows).any(|row| both.get(row).is_none()));
            }
        }
    }
    // Not agreement on nothing: rows with an instant and rows without, time columns
    // read with nothing copied, and rows whose time came from their arrival.
    assert!(
        with > 300 && without > 300 && borrowed > 30 && fell_back >= 3,
        "{with} {without} {borrowed} {fell_back}"
    );
}

/// A column the schema does not have gives no row an instant and puts no row in any
/// window.
#[test]
fn an_absent_column_has_no_instants() {
    let none = Instants::absent(5);
    assert!(none.missing());
    assert_eq!((0..6).filter_map(|row| none.get(row)).count(), 0);
    assert_eq!(none.window(None, None), Sel::none(5));
    let still = Instants::absent(5).or(&Instants::absent(5));
    assert!(still.missing() && still.window(None, None).is_empty());
}

// ── The inverse a compare of stored bytes leans on ─────────────────────────────

/// A term is put to an id or an address column as bytes, which is only right if the text
/// a row would print names one set of bytes and no other.
#[test]
fn an_id_and_an_address_read_back_as_the_bytes_that_printed_them() {
    let both = |bytes: [u8; 16]| {
        for upper in [false, true] {
            let text = print_uuid(&bytes, upper);
            assert_eq!(uuid_bytes(&text), Some(bytes), "{text}");
        }
        for spelling in 6..=9 {
            let text = print_ip(&bytes, spelling).expect("an address prints");
            assert_eq!(ip_bytes(&text).as_deref(), Some(&bytes[..]), "{text}");
        }
        let text = print_ip(&bytes[..4], 4).expect("an address prints");
        assert_eq!(ip_bytes(&text).as_deref(), Some(&bytes[..4]), "{text}");
    };
    let mut rng = Rng::new(7);
    for _ in 0..10_000 {
        let mut bytes = [0u8; 16];
        // Runs of zero groups are what the short spelling folds, so make many.
        for group in bytes.chunks_mut(2) {
            if !rng.chance(40) {
                group[0] = rng.below(256) as u8;
                group[1] = rng.below(256) as u8;
            }
        }
        both(bytes);
    }
    // By hand: nothing but zeros, an IPv4-mapped address, one zero group alone, a run
    // at each end, and two runs of one length.
    let groups = |groups: [u16; 8]| {
        let mut bytes = [0u8; 16];
        for (at, group) in groups.iter().enumerate() {
            bytes[2 * at..2 * at + 2].copy_from_slice(&group.to_be_bytes());
        }
        bytes
    };
    for bytes in [
        groups([0; 8]),
        groups([0, 0, 0, 0, 0, 0xffff, 0xc000, 0x0201]),
        groups([0, 0, 0, 0, 0, 0xffff, 0, 0]),
        groups([1, 0, 2, 3, 4, 5, 6, 7]),
        groups([0, 0, 1, 2, 3, 4, 5, 6]),
        groups([1, 2, 3, 4, 5, 6, 0, 0]),
        groups([1, 0, 0, 2, 3, 0, 0, 4]),
        groups([0, 0, 0, 0, 0, 0, 0, 1]),
        groups([0xffff; 8]),
    ] {
        both(bytes);
    }
}

// ── A term ─────────────────────────────────────────────────────────────────────

/// The form of the piece that holds a row, by its type's name; `None` for a null.
fn kind_of(col: Col<'_>, row: usize) -> Option<&'static str> {
    let (in_b, _) = match col.slots() {
        None => (false, row),
        Some(slots) => slots[row].get()?,
    };
    let pieces = col.pieces().expect("pieces");
    let piece = pieces[usize::from(in_b)].as_ref()?;
    Some(piece.form.column_type().as_str())
}

/// Texts a query might ask a value by: what its line wrote, what it prints as, and,
/// for an instant, each form the ladder reads one in.
fn spellings(value: &ColValue, written: Option<&serde_json::Value>) -> Vec<String> {
    let text = |json: &serde_json::Value| match json {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    let mut out = Vec::new();
    if matches!(value, ColValue::Null) {
        return out;
    }
    out.extend(written.map(text));
    out.push(text(&value.printed()));
    if let ColValue::Datetime(ns) = value {
        out.push((ns / 1_000_000_000).to_string());
        out.push((ns / 1_000_000).to_string());
    }
    out
}

fn flipped(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_ascii_lowercase() {
                c.to_ascii_uppercase()
            } else {
                c.to_ascii_lowercase()
            }
        })
        .collect()
}

/// The typed compare finds the rows the printed cells do, for every column of every
/// sample, as its own type and as text.
#[test]
fn a_typed_compare_finds_the_rows_the_printed_cells_do() {
    let mut matched: BTreeMap<&'static str, [u64; 3]> = BTreeMap::new();
    let (mut present, mut compares) = (0u64, 0u64);
    for sample in row_groups::all() {
        for (chunk, name) in sample.chunks.iter().zip(&sample.columns) {
            let column = decoded(&sample, chunk);
            let col = column.over(&chunk.bytes).expect("over its own bytes");
            let mut texts: BTreeSet<String> =
                pool::TEXTS.iter().map(|text| text.to_string()).collect();
            for row in 0..sample.rows {
                let (value, written) = col.cell(row).expect("a cell");
                for text in spellings(&value, written.as_ref()) {
                    texts.insert(flipped(&text));
                    texts.insert(text);
                }
            }
            let mut terms = vec![Term::any("v")];
            terms.extend(texts.iter().map(|text| Term::exact("v", text)));
            for term in &terms {
                for (at, shown) in [Shown::Stored, Shown::Text, Shown::Count]
                    .into_iter()
                    .enumerate()
                {
                    let slow = hits_by_row(col, term, shown).expect("by row");
                    let fast = hits(col, term, shown).expect("typed");
                    assert_eq!(
                        fast, slow,
                        "{}, `{name}`: {term:?}, shown as {shown:?}",
                        sample.name
                    );
                    compares += 1;
                    if matches!(term.want(), Want::Any) {
                        present += fast.count();
                        continue;
                    }
                    for row in fast.iter() {
                        let kind = kind_of(col, row).expect("a null matched a term");
                        matched.entry(kind).or_default()[at] += 1;
                    }
                }
            }
        }
    }
    // Not agreement on nothing: a piece of every type matched a term, as its own
    // type, in a column since widened to text and in one since widened to integers.
    let kinds: Vec<&str> = matched.keys().copied().collect();
    assert_eq!(
        kinds,
        ["Bool", "Datetime", "Float64", "Int64", "Ip", "String", "Uuid"],
        "{matched:?}"
    );
    assert!(
        matched
            .values()
            .all(|[own, text, count]| *own >= 10 && *text >= 10 && *count >= 10),
        "{matched:?}"
    );
    assert!(present > 1_000 && compares > 5_000, "{present} {compares}");
}

/// The rows of a sample's column `v` that a term finds where the column is now `Int64`,
/// by typed compare - held to the printed cells on the way.
fn counted(sample: &Sample, text: &str) -> Vec<usize> {
    let at = sample.place("v").expect("a column v");
    let chunk = &sample.chunks[at];
    let column = decoded(sample, chunk);
    let col = column.over(&chunk.bytes).expect("over its own bytes");
    let term = Term::exact("v", text);
    let fast = hits(col, &term, Shown::Count).expect("typed");
    assert_eq!(
        fast,
        hits_by_row(col, &term, Shown::Count).expect("by row"),
        "{text}"
    );
    fast.iter().collect()
}

/// **In a column that is now `Int64`, a date stored from epoch digits is the integer it
/// arrived as**: found by those digits and not by another spelling of the instant, while
/// an RFC 3339 date beside it is still the date it was.
#[test]
fn a_date_in_an_integer_column_is_found_as_the_integer_it_arrived_as() {
    // MongoDB's `attr.Available` and CockroachDB's `event.GoAllocBytes`: epochs in
    // seconds as numbers and as strings, one in milliseconds, plain integers, a date.
    let sample = row_groups::of_values(
        "epochs beside integers",
        &[
            "1679711330",
            "\"1679711330\"",
            "999999999",
            "1220305536",
            "1679711330000",
            "\"2023-03-25T02:28:50Z\"",
            "5",
            "\"+1679711330\"",
            "null",
            "1679711331",
        ],
    );
    // Rows 0 and 1 are that integer. Row 5 is an RFC 3339 date, no integer, and so still
    // the date it was: any spelling of its instant finds it, these digits among them.
    assert_eq!(counted(&sample, "1679711330"), [0, 1, 5]);
    assert_eq!(counted(&sample, "999999999"), [2]);
    assert_eq!(counted(&sample, "1220305536"), [3]);
    // The same moment in milliseconds is another integer, though it is one instant.
    assert_eq!(counted(&sample, "1679711330000"), [4, 5]);
    // The date's own spelling finds the date, and no longer the counts of its instant.
    assert_eq!(counted(&sample, "2023-03-25T02:28:50Z"), [5]);
    assert_eq!(counted(&sample, "5"), [6]);
    assert_eq!(counted(&sample, "05"), [6]);
    // A term the ladder reads as an integer is that integer however it is spelled, and
    // finds every row that is: the two that arrived as its digits, and the one the line
    // signed. The digits alone are a date to the ladder, and do not find the signed row -
    // row 7 is not among the rows of `1679711330` above - as they never did.
    assert_eq!(counted(&sample, "+1679711330"), [0, 1, 7]);
    assert_eq!(counted(&sample, "1679711331"), [9]);
    assert_eq!(counted(&sample, "1679711332"), [] as [usize; 0]);

    // While the column is still a date, every spelling of the instant finds the row.
    let at = sample.place("v").expect("a column v");
    let column = decoded(&sample, &sample.chunks[at]);
    let col = column.over(&sample.chunks[at].bytes).expect("over");
    let stored = |text: &str| -> Vec<usize> {
        hits(col, &Term::exact("v", text), Shown::Stored)
            .expect("typed")
            .iter()
            .collect()
    };
    assert_eq!(stored("1679711330"), [0, 1, 4, 5]);
    assert_eq!(stored("2023-03-25T02:28:50Z"), [0, 1, 4, 5]);
}

/// **A time column's instants are what each row's cell says**, through `said_instant`,
/// over every column of every sample and over one of the forms only a time column reads;
/// and behind the ladder's instants and the arrival time they are the row's effective time.
#[test]
fn a_time_column_s_instants_are_what_each_row_says() {
    let mut all = row_groups::all();
    all.push(row_groups::of_values(
        "time column forms",
        &[
            "\"1679711330.570420890\"",
            "1732206349000167",
            "\"1732206349000167\"",
            "\"2023-03-27 00:26:35.719 EDT\"",
            "1679711330.57042",
            "1679711330570420890",
            "\"0000001679711330\"",
            "\"+1732206349000167\"",
            "\"2023-03-27 00:26:35.719 IST\"",
            "\"1679711330.5704208901\"",
            "1679711330",
            "\"2023-03-25T02:28:50Z\"",
            "\"N/A\"",
            "12",
            "true",
            "null",
            "4102444800000000",
        ],
    ));
    all.push(row_groups::of_values(
        "microseconds",
        &["1732206349000167", "1732206349000999", "1732206350000001"],
    ));
    let (mut said_only, mut plain) = (0, 0);
    for sample in all {
        let rows = sample.rows;
        let built = sample.built();
        let columns: Vec<Column> = sample
            .chunks
            .iter()
            .map(|chunk| decoded(&sample, chunk))
            .collect();
        let cols: Vec<Col<'_>> = columns
            .iter()
            .zip(&sample.chunks)
            .map(|(column, chunk)| column.over(&chunk.bytes).expect("over its own bytes"))
            .collect();
        let arrived = match sample.place(RECEIVED_AT_COLUMN) {
            Some(at) => instants(cols[at]).expect("instants"),
            None => Instants::absent(rows),
        };
        for (col, name) in cols.iter().zip(&sample.columns) {
            let got = said_instants(*col).expect("said instants");
            for (row, built) in built.iter().enumerate() {
                let (value, written) = col.cell(row).expect("a cell");
                let want = said_instant(&value, written.as_ref());
                assert_eq!(got.get(row), want, "{}, `{name}`, row {row}", sample.name);
                assert_eq!(said_ts(built, name), want, "{}, `{name}`", sample.name);
                // Whatever the ladder reads as an instant, a time column reads as the same.
                if let Some(ns) = instant_of(&value) {
                    assert_eq!(want, Some(ns), "{}, `{name}`, row {row}", sample.name);
                } else if want.is_some() {
                    said_only += 1;
                }
            }
            plain += usize::from(matches!(
                col.pieces().expect("pieces")[0].as_ref().map(|p| p.form),
                Some(Form::Int64 {
                    plus: false,
                    width: 0,
                    ..
                })
            ));
            let effective = instants(*col).expect("instants").or(&arrived).or(&got);
            for (row, built) in built.iter().enumerate() {
                assert_eq!(
                    effective.get(row),
                    effective_ts(built, name),
                    "{}, `{name}`, row {row}",
                    sample.name
                );
            }
        }
    }
    // Not agreement on nothing: rows only a time column reads, and integers read with
    // no text made.
    assert!(said_only >= 9 && plain >= 5, "{said_only} {plain}");

    // By hand, the column of forms: what each row's time is, and that the rest have none.
    let sample = row_groups::of_values(
        "forms",
        &[
            "\"1679711330.570420890\"",
            "1732206349000167",
            "\"2023-03-27 00:26:35.719 EDT\"",
            "\"2023-03-27 00:26:35.719 IST\"",
            "\"0000001679711330\"",
        ],
    );
    let at = sample.place("v").expect("v");
    let column = decoded(&sample, &sample.chunks[at]);
    let got = said_instants(column.over(&sample.chunks[at].bytes).expect("over")).expect("said");
    assert_eq!(
        (0..5).map(|row| got.get(row)).collect::<Vec<_>>(),
        [
            Some(1_679_711_330_570_420_890),
            Some(1_732_206_349_000_167_000),
            Some(1_679_891_195_719_000_000),
            None,
            None,
        ]
    );
}

/// The rows of a sample's column `v` that a term finds, by typed compare - held to
/// the printed cells on the way.
fn found(name: &str, text: &str, text_column: bool) -> Vec<usize> {
    let (sample, at) = layout(name);
    let chunk = &sample.chunks[at];
    let column = decoded(&sample, chunk);
    let col = column.over(&chunk.bytes).expect("over its own bytes");
    let term = match text {
        "*" => Term::any("v"),
        text => Term::exact("v", text),
    };
    let fast = hits(col, &term, text_column).expect("typed");
    assert_eq!(fast, hits_by_row(col, &term, text_column).expect("by row"));
    fast.iter().collect()
}

/// What a term means, case by case, on rows small enough to read: the cases a typed
/// compare could most easily get wrong.
#[test]
fn a_term_finds_the_rows_it_always_found() {
    // One number however it was spelled, and a number kept as text because no form
    // printed it back: `-0` and `0` are one integer, found by either.
    let words = "text the ladder types";
    assert_eq!(found(words, "0", false), [0, 1, 13]);
    assert_eq!(found(words, "-0", false), [0, 1, 13]);
    assert_eq!(found(words, "45", false), [9, 10, 11]);
    assert_eq!(found(words, "0045", false), [9, 10, 11]);
    assert_eq!(found(words, "1000.0", false), [3]);
    assert_eq!(found(words, "0.5", false), [2]);
    // `TRUE` is text, and `true` a bool that `True` spells too.
    assert_eq!(found(words, "TRUE", false), [4]);
    assert_eq!(found(words, "true", false), [5]);
    assert_eq!(found(words, "True", false), [5]);
    assert_eq!(found(words, "inf", false), [6]);
    // A date kept as text is found by any spelling of its instant while the column
    // is not text, and only by its own once it is.
    assert_eq!(found(words, "2024-01-15T10:30:00.000Z", false), [7, 8]);
    assert_eq!(found(words, "1705314600", false), [7, 8]);
    assert_eq!(
        found(words, "2024-01-15T10:30:00.000Z", true),
        [] as [usize; 0]
    );
    assert_eq!(found(words, "2024-01-15T10:30:00Z", true), [7]);
    assert_eq!(found(words, "1705314600", true), [8]);
    assert_eq!(found(words, "INFO", false), [12]);
    assert_eq!(found(words, "info", false), [] as [usize; 0]);
    assert_eq!(found(words, "*", false).len(), 16);

    // Integers: bare, quoted, padded and signed are the number.
    assert_eq!(found("integers, bare and quoted", "200", false), [0, 3]);
    assert_eq!(found("integers, bare and quoted", "0200", false), [0, 3]);
    assert_eq!(
        found("integers, bare and quoted", "200.0", false),
        [] as [usize; 0]
    );
    assert_eq!(found("integers padded with zeros", "45", true), [2]);
    assert_eq!(found("integers with a plus", "0", false), [3]);
    assert_eq!(
        found("one integer spelled two ways", "200", false),
        [0, 1, 2, 3]
    );
    assert_eq!(
        found("one integer spelled two ways", "201", false),
        [] as [usize; 0]
    );

    // Floats compare as floats: a zero of either sign is zero, and `200` is an
    // integer that no float equals.
    assert_eq!(found("a zero of each sign", "0.0", false), [0, 1, 2]);
    assert_eq!(found("a zero of each sign", "-0.0", false), [0, 1, 2]);
    assert_eq!(found("a zero of each sign", "0", false), [] as [usize; 0]);
    assert_eq!(found("floats of mixed digit counts", "0.01", false), [0, 7]);
    assert_eq!(found("floats of mixed digit counts", "1e3", false), [4]);
    assert_eq!(
        found("floats of mixed digit counts", "200", false),
        [] as [usize; 0]
    );
    assert_eq!(found("floats of mixed digit counts", "2e2", false), [8]);

    // A null matches nothing, `*` included; a word beside numbers is found beside them.
    let mixed = "integers beside text and nulls";
    assert_eq!(found(mixed, "*", false), [0, 1, 2, 3, 4, 6]);
    assert_eq!(found(mixed, "N/A", false), [3, 4]);
    assert_eq!(found(mixed, "45", false), [6]);
    assert_eq!(found("nothing but nulls", "*", false), [] as [usize; 0]);
    let level = "a level that is a number, a word and a bool";
    assert_eq!(found(level, "30", false), [1, 2, 6]);
    assert_eq!(found(level, "true", false), [3]);
    assert_eq!(found(level, "WARN", true), [5]);

    // Dates: every spelling of the instant in a `Datetime` column, the spelling that
    // arrived in a column that is now text.
    let dates = "dates of mixed fraction digits";
    assert_eq!(found(dates, "2024-01-15T10:30:00Z", false), [0, 3]);
    assert_eq!(found(dates, "1705314600", false), [0, 3]);
    assert_eq!(found(dates, "2024-01-15T16:00:00+05:30", false), [0, 3]);
    assert_eq!(found(dates, "2024-01-15T10:30:00Z", true), [0]);
    assert_eq!(found(dates, "2024-01-15T10:30:00.000Z", true), [3]);
    assert_eq!(found(dates, "1705314600", true), [] as [usize; 0]);
    // Epoch digits arrived as digits, bare or quoted, and are found by them alone.
    let epoch = "epoch seconds, bare and quoted";
    assert_eq!(found(epoch, "1705314600", false), [0, 3]);
    assert_eq!(found(epoch, "2024-01-15T10:30:00Z", false), [0, 3]);
    assert_eq!(found(epoch, "1705314600", true), [0, 3]);
    assert_eq!(found(epoch, "+1705314600", true), [] as [usize; 0]);
    assert_eq!(found(epoch, "1705314600000", true), [] as [usize; 0]);
    assert_eq!(found(epoch, "2024-01-15T10:30:00Z", true), [] as [usize; 0]);
    assert_eq!(found("epoch milliseconds", "1705314600000", true), [0]);
    assert_eq!(
        found("epoch milliseconds", "1705314600", true),
        [] as [usize; 0]
    );
    assert_eq!(found("one epoch second", "1705314600", true), [0, 1, 2]);

    assert_eq!(found("booleans, bare and quoted", "true", false), [0, 2, 5]);
    assert_eq!(
        found("booleans, bare and quoted", "False", false),
        [1, 3, 4]
    );
    assert_eq!(
        found("booleans, bare and quoted", "TRUE", false),
        [] as [usize; 0]
    );

    // An id is found in the case it was written in, and in no other.
    let lower = "550e8400-e29b-41d4-a716-446655440000";
    let upper = "550E8400-E29B-41D4-A716-446655440000";
    let mixed = "550e8400-E29B-41d4-A716-446655440000";
    assert_eq!(found("ids", lower, false), [0, 2]);
    assert_eq!(found("ids", upper, false), [] as [usize; 0]);
    assert_eq!(found("ids in both cases", lower, false), [0, 4]);
    assert_eq!(found("ids in both cases", upper, false), [1, 3]);
    assert_eq!(found("ids in both cases", mixed, false), [] as [usize; 0]);
    assert_eq!(found("one id", lower, true), [0, 1, 2]);
    assert_eq!(found("one id", upper, true), [] as [usize; 0]);

    // An address is found in the spelling it was written in.
    assert_eq!(found("addresses, v4", "10.0.0.1", false), [0, 2]);
    assert_eq!(found("addresses, v4", "010.0.0.1", false), [] as [usize; 0]);
    assert_eq!(found("addresses, v6", "2001:db8::1", false), [0]);
    assert_eq!(
        found("addresses, v6", "2001:DB8::1", false),
        [] as [usize; 0]
    );
    assert_eq!(found("addresses, v6", "::ffff:192.0.2.1", false), [2]);
    assert_eq!(
        found("addresses, v6", "::ffff:c000:201", false),
        [] as [usize; 0]
    );
    assert_eq!(
        found("addresses, v6 in capitals", "2001:DB8::1", false),
        [0]
    );
    assert_eq!(
        found("addresses, v6 in capitals", "2001:db8::1", false),
        [] as [usize; 0]
    );
    let whole = "2001:0db8:0000:0000:0000:0000:0000:0001";
    assert_eq!(found("addresses, v6 with every group", whole, false), [0]);
    assert_eq!(
        found("addresses, v6 with every group", "2001:db8::1", false),
        [] as [usize; 0]
    );
    assert_eq!(
        found("addresses of both families", "10.0.0.1", false),
        [0, 2, 4]
    );
    assert_eq!(
        found("addresses of both families", "2001:db8::2", false),
        [3]
    );
}
