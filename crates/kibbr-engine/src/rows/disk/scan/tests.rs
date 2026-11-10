// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use super::super::tests::{counters, slow_visit, store_with, thresholds, Scratch, T0};
use super::super::{DiskLeaf, DiskStore, Frozen};
use super::*;
use crate::format::read::TWO_PIECES;
use crate::format::{day_of, samples, Form, DAY_NS};
use crate::rows::{project, DiskOptions, FlushKind, LeafStore, Store};
use crate::model::types::ColValue;

const SECOND: i64 = 1_000_000_000;

/// The rows of a row group, and of the older day: three row groups, five rows moved
/// aside and five buffered.
const UNIT: usize = 16;
const OLDER: usize = 3 * UNIT + 10;
/// The newer day: one row group and two rows buffered.
const NEWER: usize = UNIT + 2;

/// One row of the leaf `mixed` builds, in one of three shapes that between them hold every
/// layout the tests below name. `at` is the row's time, `n` its number in its day.
fn shaped(shape: usize, at: &str, n: usize, arrived: i64) -> Row {
    let line = match shape {
        0 => format!(
            r#"{{"ts":"{at}","n":{n},"level":"{}","note":null,"when":"2024-01-15T16:00:{n:02}+05:30"}}"#,
            if n.is_multiple_of(3) { "WARN" } else { "INFO" }
        ),
        1 => format!(
            r#"{{"ts":"{at}","n":{n},"level":{},"note":{},"when":{},"extra":"e{}"}}"#,
            [r#""INFO""#, "null", "30", "30"][n % 4],
            if n.is_multiple_of(2) {
                r#""x""#
            } else {
                "null"
            },
            if n.is_multiple_of(5) {
                r#""N/A""#.to_string()
            } else {
                format!(r#""2024-01-15T10:30:{n:02}Z""#)
            },
            n % 2
        ),
        _ => format!(
            r#"{{"ts":"{}","n":{n},"level":{},"note":"y","when":{},"extra":"e2"}}"#,
            if n % 4 == 1 { "N/A" } else { at },
            if n.is_multiple_of(8) {
                r#""0030""#
            } else {
                "30"
            },
            1_705_314_600 + n
        ),
    };
    let mut row = samples::row(&line);
    if shape == 2 && n % 4 == 1 {
        row.insert(RECEIVED_AT_COLUMN.into(), ColValue::Datetime(arrived));
    }
    row
}

/// Row `n` of the older day, at `T0 + n` seconds: a row group of each shape, and then
/// the rows in memory, of the three shapes by turns.
fn older(n: usize) -> Row {
    let shape = if n < 3 * UNIT { n / UNIT } else { n % 3 };
    shaped(
        shape,
        &format!("2024-01-15T10:30:{n:02}Z"),
        n,
        T0 + n as i64 * SECOND,
    )
}

/// Row `n` of the newer day, a day later to the second.
fn newer(n: usize) -> Row {
    shaped(
        1,
        &format!("2024-01-16T10:30:{n:02}Z"),
        n,
        T0 + DAY_NS + n as i64 * SECOND,
    )
}

/// One template over two days, with every kind of column a scan has to put a term to:
/// `the_mixed_leaf_holds_the_layouts_it_is_for` says which.
fn mixed(scratch: &Scratch, in_memory: bool) -> (DiskStore, Arc<DiskLeaf>) {
    mixed_on(scratch, in_memory, 0)
}

/// Row groups of sixteen rows, in a store whose scans may choose rows on `threads`
/// threads and share out every run of two row groups or more.
fn shared(threads: usize) -> DiskOptions {
    DiskOptions {
        scan_threads: threads,
        fan_rows: 0,
        ..thresholds(UNIT, usize::MAX)
    }
}

/// The runs of row groups a store's scans have shared out so far.
fn fanned(s: &DiskStore) -> u64 {
    s.fan.as_ref().map_or(0, |fan| fan.fanned())
}

/// `mixed`, in a store opened with `shared(threads)`.
fn mixed_on(scratch: &Scratch, in_memory: bool, threads: usize) -> (DiskStore, Arc<DiskLeaf>) {
    let s = store_with(scratch, shared(threads));
    let opened = s.open(7, "ts").expect("leaf");
    for n in 0..3 * UNIT {
        opened.append(older(n));
    }
    for n in 0..UNIT {
        opened.append(newer(n));
    }
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!((done.rows, done.row_groups), (4 * UNIT as u64, 4));

    let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&7]);
    if in_memory {
        for n in 3 * UNIT..3 * UNIT + 5 {
            leaf.append(older(n));
        }
        // The first hold of a flush and not its second: the rows are moved aside and
        // stay there, as they are while the flush is encoding and writing them.
        assert!(
            leaf.move_day(day_of(T0)).is_some(),
            "nothing was moved aside"
        );
        for n in 3 * UNIT + 5..OLDER {
            leaf.append(older(n));
        }
        for n in UNIT..NEWER {
            leaf.append(newer(n));
        }
    }
    (s, leaf)
}

/// A stored row group of a frozen leaf with its header, for a test that looks inside.
fn headers(frozen: &Frozen) -> Vec<(&Stored, Header)> {
    let mut out = Vec::new();
    for view in &frozen.views {
        for s in view.stored.iter() {
            let mut reader = Reader::open(&s.file.path).expect("open");
            let header = reader.header(&s.entry, s.schema.len()).expect("header");
            out.push((s, header));
        }
    }
    out
}

/// The leaf holds the layouts it was built to hold. Without this a change to what the
/// builder makes of these lines would leave the tests below passing over less.
#[test]
fn the_mixed_leaf_holds_the_layouts_it_is_for() {
    let scratch = Scratch::new("scan-layouts");
    let (_s, leaf) = mixed(&scratch, true);
    let frozen = leaf.freeze(None, None);
    let mut seen = BTreeSet::new();
    let mut schemas = BTreeSet::new();
    for (s, header) in headers(&frozen) {
        schemas.insert(s.schema.len());
        for (name, dir) in s.schema.iter().zip(&header.chunks) {
            let form = dir.a.map(|piece| piece.form);
            let what = match (name.as_str(), dir.layout, form) {
                ("note", ALL_NULL, _) => "a column null in every row",
                ("level", layout, _) if layout == TWO_PIECES | SOME_NULL => {
                    "two pieces beside nulls"
                }
                ("when", _, Some(Form::Datetime { .. })) if dir.b.is_none() => {
                    "a date stored as a date"
                }
                ("when", layout, _) if layout & TWO_PIECES != 0 => "a date beside text",
                ("ts", layout, _) if layout & TWO_PIECES != 0 => "a time column in two pieces",
                (RECEIVED_AT_COLUMN, layout, _) if layout & SOME_NULL != 0 => {
                    "an arrival time some rows have"
                }
                _ => continue,
            };
            seen.insert(what);
        }
    }
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        [
            "a column null in every row",
            "a date beside text",
            "a date stored as a date",
            "a time column in two pieces",
            "an arrival time some rows have",
            "two pieces beside nulls",
        ]
    );
    // One row group has no `extra`, and one has an arrival time: three schemas.
    assert_eq!(schemas.len(), 3, "{schemas:?}");
    let state = leaf.state();
    let part = &state.days[&day_of(T0)];
    assert_eq!(
        (
            part.stored.len(),
            part.closing.as_ref().map_or(0, |batch| batch.rows.len()),
            part.buffer.len(),
        ),
        (3, 5, 5)
    );
}

/// Lists of terms to ask the mixed leaf, each with whether some row of it passes them
/// when nothing else is asked.
fn term_lists() -> Vec<(Vec<Term>, bool)> {
    vec![
        (Vec::new(), true),
        (vec![Term::exact("level", "INFO")], true),
        // A number in a column that also holds words, bare and padded.
        (vec![Term::exact("level", "30")], true),
        (vec![Term::exact("level", "0030")], true),
        (vec![Term::exact("level", "info")], false),
        (vec![Term::any("level")], true),
        // Null in every row of one row group, in some rows of another.
        (vec![Term::any("note")], true),
        (vec![Term::exact("note", "x")], true),
        // A key one row group's schema does not have, and one no schema has.
        (vec![Term::any("extra")], true),
        (vec![Term::exact("extra", "e1")], true),
        (vec![Term::any("absent")], false),
        // One instant of `when` as each row group spells it, and as none does. Which
        // of them finds a row depends on whether the column is text.
        (vec![Term::exact("when", "2024-01-15T16:00:07+05:30")], true),
        (vec![Term::exact("when", "2024-01-15T10:30:07Z")], true),
        (vec![Term::exact("when", "2024-01-15T10:30:21Z")], true),
        (vec![Term::exact("when", "1705314640")], true),
        // A row in memory by another spelling of its instant: found while `when` is a
        // date, and not once it is text.
        (vec![Term::exact("when", "2024-01-15T10:30:51Z")], true),
        (vec![Term::exact("when", "N/A")], true),
        (vec![Term::any("when")], true),
        // The time column, by an instant and by the text of a row that has none.
        (vec![Term::exact("ts", "2024-01-15T10:30:05Z")], true),
        (vec![Term::exact("ts", "N/A")], true),
        (vec![Term::exact("n", "7")], true),
        // Two terms: the second is asked only of what the first left.
        (vec![Term::any("note"), Term::exact("level", "30")], true),
        (
            vec![Term::exact("level", "INFO"), Term::exact("n", "3")],
            false,
        ),
        (
            vec![Term::any("absent"), Term::exact("level", "INFO")],
            false,
        ),
    ]
}

/// Windows over the mixed leaf, in nanoseconds: none, whole days, ones that cut and prune
/// row groups, one that holds only rows in memory, and one with no beginning.
fn windows() -> Vec<(Option<i64>, Option<i64>)> {
    let at = |second: i64| Some(T0 + second * SECOND);
    vec![
        (None, None),
        (at(0), Some(T0 + 2 * DAY_NS)),
        (at(20), Some(T0 + 2 * DAY_NS)),
        (at(20), at(40)),
        (at(50), at(56)),
        (Some(T0 + DAY_NS), None),
        (None, at(10)),
    ]
}

fn instant(ns: i64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_nanos(ns as u64)
}

/// Every way the tests below ask the mixed leaf: each list of terms, under each window,
/// with `when` and the time column text and not, reading every column or the named few.
fn asks() -> Vec<(ScanOptions, Vec<Term>)> {
    let mut out = Vec::new();
    for (terms, _) in term_lists() {
        for (since, until) in windows() {
            for text_columns in [
                vec![],
                vec!["when".to_string()],
                vec!["ts".into(), "when".into()],
            ] {
                let mut named: Vec<String> =
                    terms.iter().map(|t| t.key().to_string()).collect();
                named.extend(["level".to_string(), "ts".into(), RECEIVED_AT_COLUMN.into()]);
                named.sort();
                named.dedup();
                for columns in [Vec::new(), named] {
                    out.push((
                        ScanOptions {
                            columns,
                            since: since.map(instant),
                            until: until.map(instant),
                            ts_column: Some("ts".into()),
                            text_columns: text_columns.clone(),
                            ..ScanOptions::default()
                        },
                        terms.clone(),
                    ));
                }
            }
        }
    }
    out
}

/// A visitor that keeps what it is handed and what it was asked, takes every day or
/// declines every day, and has no use for the rows `refuses` names.
struct Keeps {
    wants: Wants,
    days: bool,
    rows: Vec<(Row, u64)>,
    /// Each row it was asked about: its time, its position, and whether the store
    /// said that time is in the day the row is stored under.
    asked: Vec<(Option<i64>, u64, bool)>,
    refuses: fn(u64) -> bool,
}

impl Keeps {
    fn new(wants: Wants, days: bool) -> Self {
        Self {
            wants,
            days,
            rows: Vec::new(),
            asked: Vec::new(),
            refuses: |_| false,
        }
    }

    /// One that keeps the newest rows, and so is asked first, and refuses some.
    fn refusing(days: bool, refuses: fn(u64) -> bool) -> Self {
        Self {
            refuses,
            ..Self::new(Wants::Newest, days)
        }
    }
}

/// Every third position: the rows a visitor that refuses some has no use for.
fn third(seq: u64) -> bool {
    seq % 3 == 1
}

impl Visitor for Keeps {
    fn row(&mut self, row: &Row, seq: u64) {
        assert!(
            self.wants != Wants::Nothing,
            "a visitor that reads nothing was handed a row"
        );
        // A visitor that is asked first is handed a row straight after its ask, and the row
        // shows the time it was asked with. Every test here names the time column `ts`.
        if let Some((ts, asked, _)) = self.asked.last() {
            assert_eq!(
                (effective_ts(row, "ts"), seq),
                (*ts, *asked),
                "handed {row:?}"
            );
        }
        self.rows.push((row.clone(), seq));
    }

    fn wants_partition(&mut self, _: Option<i64>) -> bool {
        self.days
    }

    fn wants(&self) -> Wants {
        self.wants.clone()
    }

    fn wants_row(&mut self, ts: Option<i64>, seq: u64, placed: bool) -> bool {
        assert_eq!(
            self.wants,
            Wants::Newest,
            "a visitor that did not ask to be asked first was asked about a row"
        );
        self.asked.push((ts, seq, placed));
        !(self.refuses)(seq)
    }
}

/// What a visitor that keeps the newest rows was asked and handed, against the plain scan's
/// rows: asked about every row that passed where `asked_first`, and about none otherwise.
fn held_to_plain(v: &Keeps, plain: &[(Row, u64)], asked_first: bool, said: &str) {
    let asked: Vec<(Option<i64>, u64, bool)> = plain
        .iter()
        .filter(|_| asked_first)
        .map(|(row, seq)| (effective_ts(row, "ts"), *seq, true))
        .collect();
    assert_eq!(v.asked, asked, "{said}");
    let handed: Vec<&(Row, u64)> = plain
        .iter()
        .filter(|(_, seq)| !(asked_first && (v.refuses)(*seq)))
        .collect();
    assert_eq!(v.rows.iter().collect::<Vec<_>>(), handed, "{said}");
}

/// What a grouping would name: a column every row group has, the two time columns, one
/// some schemas lack and one no schema has.
fn named() -> Vec<String> {
    ["absent", "extra", "level", "ts", RECEIVED_AT_COLUMN]
        .map(String::from)
        .to_vec()
}

/// The scan answers what the plain scan answers whatever the visitor reads: counters, rows,
/// positions and order, and every ask of a visitor that keeps the newest.
#[test]
fn the_scan_answers_what_the_plain_scan_answers_whatever_is_read() {
    held_to_the_plain_scan_whatever_is_read(0);
}

/// And it answers the same with its row groups shared between threads, which the count of
/// runs shared out shows is not the scan in turn passing twice.
#[test]
fn the_scan_shared_between_two_threads_answers_what_the_plain_scan_answers() {
    held_to_the_plain_scan_whatever_is_read(2);
}

#[test]
fn the_scan_shared_between_four_threads_answers_what_the_plain_scan_answers() {
    held_to_the_plain_scan_whatever_is_read(4);
}

fn held_to_the_plain_scan_whatever_is_read(threads: usize) {
    let scratch = Scratch::new(&format!("scan-plain-{threads}"));
    let (s, leaf) = mixed_on(&scratch, true, threads);
    let mut found = BTreeSet::new();
    let (mut scans, mut asked, mut refused) = (0usize, 0usize, 0usize);
    for (at, (asking, terms)) in asks().into_iter().enumerate() {
        let frozen = leaf.freeze(asking.since.map(nanos_of), asking.until.map(nanos_of));
        // No limit; the first row; a few rows; and one more row than the newer day has
        // stored, which stops the scan on a row in memory.
        for limit in [0, 1, 5, UNIT + 1] {
            let opts = ScanOptions {
                limit,
                ..asking.clone()
            };
            // The visitors, whether each takes the days it is offered, and the rows each
            // refuses when it is asked first. One that declines a day still has it counted.
            let none: fn(u64) -> bool = |_| false;
            let visitors = [
                (Wants::Nothing, true, none),
                (Wants::Columns(named()), true, none),
                (Wants::Rows, true, none),
                (Wants::Newest, true, none),
                (Wants::Newest, true, third),
                (Wants::Nothing, false, none),
                (Wants::Rows, false, none),
                (Wants::Newest, false, third),
            ];
            for (wants, days, refuses) in visitors {
                let said = format!("{terms:?} with {opts:?}, read as {wants:?}, days {days}");
                let mut plain = Keeps::new(Wants::Rows, days);
                let slow = slow_visit(&frozen, &opts, &terms, &mut plain).expect("plain scan");
                let mut v = Keeps {
                    refuses,
                    ..Keeps::new(wants.clone(), days)
                };
                let scan = frozen.visit(&opts, &terms, &mut v).expect("scan");
                assert_eq!(counters(&scan), counters(&slow), "{said}");
                match &wants {
                    Wants::Nothing => assert!(v.rows.is_empty(), "{said}"),
                    Wants::Rows => assert_eq!(v.rows, plain.rows, "{said}"),
                    Wants::Newest => {
                        held_to_plain(&v, &plain.rows, limit == 0, &said);
                        asked += v.asked.len();
                        refused += v.asked.len() - v.rows.len().min(v.asked.len());
                    }
                    Wants::Columns(names) => {
                        let narrow = |rows: &[(Row, u64)]| -> Vec<(Row, u64)> {
                            rows.iter()
                                .map(|(row, seq)| (project(row, names), *seq))
                                .collect()
                        };
                        assert_eq!(narrow(&v.rows), narrow(&plain.rows), "{said}");
                    }
                }
                scans += 1;
                if limit == 0 && days && scan.matched > 0 && asking.since.is_none() {
                    found.insert(at / (windows().len() * 3 * 2));
                }
            }
        }
    }
    // Not vacuous: the lists of terms meant to find a row found one, and no other did.
    let meant: BTreeSet<usize> = term_lists()
        .iter()
        .enumerate()
        .filter_map(|(at, (_, some))| some.then_some(at))
        .collect();
    assert_eq!(found, meant);
    assert!(scans > 30_000, "{scans}");
    // And the visitor that is asked first was asked, and refused, by the thousand.
    assert!(asked > 20_000 && refused > 5_000, "{asked} {refused}");
    // In turn where the store has no scan threads, and shared out where it has.
    let shared_out = fanned(&s);
    assert_eq!(
        shared_out > 1_000,
        threads >= 2,
        "{shared_out} runs shared out"
    );
    assert_eq!(shared_out == 0, threads < 2, "{shared_out} runs shared out");
}

/// One template and one day whose time column a query may be told is text, written every
/// way a line writes a time, and by its arrival time where it has none.
fn spelled(scratch: &Scratch, threads: usize) -> (DiskStore, Arc<DiskLeaf>) {
    let line = |n: usize| {
        let secs = 1_705_314_600 + n;
        let ts = match n % 7 {
            1 | 4 => format!(r#""2024-01-15T16:00:{n:02}+05:30""#),
            2 => secs.to_string(),
            5 => format!(r#""{secs}""#),
            6 => r#""N/A""#.to_string(),
            _ => format!(r#""2024-01-15T10:30:{n:02}Z""#),
        };
        let mut row = samples::row(&format!(r#"{{"ts":{ts},"n":{n}}}"#));
        if n % 7 == 6 {
            row.insert(
                RECEIVED_AT_COLUMN.into(),
                ColValue::Datetime(T0 + n as i64 * SECOND),
            );
        }
        row
    };
    let s = store_with(scratch, shared(threads));
    let opened = s.open(9, "ts").expect("leaf");
    for n in 0..2 * UNIT {
        opened.append(line(n));
    }
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!((done.rows, done.row_groups), (2 * UNIT as u64, 2));
    let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&9]);
    for n in 2 * UNIT..2 * UNIT + 5 {
        leaf.append(line(n));
    }
    assert!(
        leaf.move_day(day_of(T0)).is_some(),
        "nothing was moved aside"
    );
    for n in 2 * UNIT + 5..2 * UNIT + 10 {
        leaf.append(line(n));
    }
    (s, leaf)
}

/// A visitor ranks a row by the time it reads off it: asked with another, a row that belongs
/// in an answer would go unbuilt, and only under a limit, once the heap is full.
#[test]
fn a_row_is_asked_about_with_the_time_it_shows_and_in_the_day_it_is_stored_under() {
    asked_about_with_the_time_it_shows(0);
}

/// A thread that chose a row group's rows may have decoded its time column first, and the
/// time a row is asked about with is still the one it shows.
#[test]
fn a_row_of_a_scan_shared_between_two_threads_is_asked_about_with_the_time_it_shows() {
    asked_about_with_the_time_it_shows(2);
}

#[test]
fn a_row_of_a_scan_shared_between_four_threads_is_asked_about_with_the_time_it_shows() {
    asked_about_with_the_time_it_shows(4);
}

fn asked_about_with_the_time_it_shows(threads: usize) {
    let scratch = Scratch::new(&format!("scan-asked-{threads}"));
    let (m, mixed) = mixed_on(&scratch, true, threads);
    let other = Scratch::new(&format!("scan-asked-spelled-{threads}"));
    let (s, spelled) = spelled(&other, threads);

    // The second leaf's questions: nothing, a term, a window that cuts both row groups and
    // the rows in memory, and both; each with the time column a date and with it text.
    let mut spelled_asks = Vec::new();
    for terms in [vec![], vec![Term::any("n")], vec![Term::exact("n", "0012")]] {
        for (since, until) in [
            (None, None),
            (Some(T0 + 9 * SECOND), Some(T0 + 39 * SECOND)),
        ] {
            for text_columns in [vec![], vec!["ts".to_string()]] {
                spelled_asks.push((
                    ScanOptions {
                        since: since.map(instant),
                        until: until.map(instant),
                        ts_column: Some("ts".into()),
                        text_columns,
                        ..ScanOptions::default()
                    },
                    terms.clone(),
                ));
            }
        }
    }

    // What the rows handed over showed as their time, where the time column is text:
    // each way of writing one, by how its text ends.
    let mut shown: BTreeSet<&str> = BTreeSet::new();
    let (mut asked, mut text, mut in_memory, mut by_arrival) = (0usize, 0usize, 0usize, 0);
    for (leaf, stored, asks) in [
        (&mixed, 3 * UNIT as u64, asks()),
        (&spelled, 2 * UNIT as u64, spelled_asks),
    ] {
        for (opts, terms) in asks {
            let frozen = leaf.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of));
            let said = format!("{terms:?} with {opts:?}");
            let mut plain = Keeps::new(Wants::Rows, true);
            let slow = slow_visit(&frozen, &opts, &terms, &mut plain).expect("plain scan");
            for refuses in [|_| false, third] {
                let mut v = Keeps::refusing(true, refuses);
                let scan = frozen.visit(&opts, &terms, &mut v).expect("scan");
                assert_eq!(counters(&scan), counters(&slow), "{said}");
                assert_eq!(v.asked.len() as u64, scan.matched, "{said}");
                assert!(v.asked.iter().all(|(_, _, placed)| *placed), "{said}");
                held_to_plain(&v, &plain.rows, true, &said);
                asked += v.asked.len();
            }
            let ts_is_text = opts.text_columns.iter().any(|c| c == "ts");
            for (row, seq) in &plain.rows {
                // The older day of the mixed leaf, and the one day of the other, hold
                // rows in memory after their stored ones; a position says which.
                let older = effective_ts(row, "ts").is_some_and(|ns| ns < T0 + DAY_NS);
                in_memory += usize::from(older && *seq >= stored);
                match row.get("ts") {
                    Some(ColValue::String(at)) if ts_is_text => {
                        text += 1;
                        shown.insert(match at.as_bytes().last() {
                            Some(b'Z') => "a date ending in Z",
                            Some(b'0') if at.ends_with("+05:30") => "a date with an offset",
                            Some(b'A') => {
                                by_arrival += 1;
                                "no time"
                            }
                            _ => "digits",
                        });
                    }
                    _ => {}
                }
            }
        }
    }
    assert_eq!(
        shown.into_iter().collect::<Vec<_>>(),
        [
            "a date ending in Z",
            "a date with an offset",
            "digits",
            "no time"
        ]
    );
    assert!(
        asked > 20_000 && text > 2_000 && in_memory > 1_000 && by_arrival > 200,
        "{asked} {text} {in_memory} {by_arrival}"
    );
    let shared_out = fanned(&m) + fanned(&s);
    assert_eq!(
        shared_out > 1_000,
        threads >= 2,
        "{shared_out} runs shared out"
    );
    assert_eq!(shared_out == 0, threads < 2, "{shared_out} runs shared out");
}

/// A term's column is read because the term is asked of it: naming columns says what a row
/// should hold, and cannot make a term match nothing.
#[test]
fn a_term_reads_its_column_whether_or_not_it_was_named() {
    let scratch = Scratch::new("scan-unnamed");
    let (_s, leaf) = mixed(&scratch, true);
    let terms = [Term::exact("level", "INFO")];
    let every = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let only_ts = ScanOptions {
        columns: vec!["ts".into()],
        ..every.clone()
    };
    let mut whole = Keeps::new(Wants::Rows, true);
    let all = leaf.visit(&every, &terms, &mut whole).expect("scan");
    let mut narrow = Keeps::new(Wants::Rows, true);
    let some = leaf.visit(&only_ts, &terms, &mut narrow).expect("scan");
    assert!(all.matched > 0);
    assert_eq!((some.matched, some.scanned), (all.matched, all.scanned));
    let seqs = |rows: &[(Row, u64)]| rows.iter().map(|(_, seq)| *seq).collect::<Vec<_>>();
    assert_eq!(seqs(&narrow.rows), seqs(&whole.rows));
    assert!(some.bytes_read < all.bytes_read);
}

/// A visitor that names columns is handed exactly the named columns its row group has; and
/// a row group with no column for a term's key gives no row, its rows still counted.
#[test]
fn a_visitor_is_handed_the_columns_it_named_and_no_others() {
    let scratch = Scratch::new("scan-named");
    let (_s, leaf) = mixed(&scratch, false);
    let opts = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let frozen = leaf.freeze(None, None);
    let mut v = Keeps::new(Wants::Columns(named()), true);
    let scan = frozen
        .visit(&opts, &[Term::any("n")], &mut v)
        .expect("scan");
    let stored = (4 * UNIT) as u64;
    assert_eq!((scan.matched, scan.scanned), (stored, stored));
    let mut kinds = BTreeSet::new();
    for (row, _) in &v.rows {
        let mut keys: Vec<&str> = row.keys().map(String::as_str).collect();
        keys.sort_unstable();
        kinds.insert(keys);
    }
    assert_eq!(
        kinds.into_iter().collect::<Vec<_>>(),
        [
            // The row group of shape 2, some of whose rows carry an arrival time.
            vec!["\\_received_at", "extra", "level", "ts"],
            vec!["extra", "level", "ts"],
            // The row group of shape 0, which has no `extra`.
            vec!["level", "ts"],
        ]
    );

    // The first row group of the older day has no `extra`: none of its sixteen rows
    // is handed over, and all sixteen were looked at.
    let mut v = Keeps::new(Wants::Rows, true);
    let scan = frozen
        .visit(&opts, &[Term::any("extra")], &mut v)
        .expect("scan");
    assert_eq!((scan.matched, scan.scanned), (stored - UNIT as u64, stored));
    assert!(v.rows.iter().all(|(row, _)| row.contains_key("extra")));
    let older: Vec<u64> = v.rows[UNIT..].iter().map(|(_, seq)| *seq).collect();
    assert_eq!(older, (UNIT as u64..3 * UNIT as u64).collect::<Vec<_>>());
}

/// The units of a frozen leaf's row groups for one question, each handed to `f` after
/// its bytes are read and before anything of it is decoded.
fn each_unit(frozen: &Frozen, ask: &Ask<'_>, mut f: impl FnMut(&Unit<'_>)) {
    let mut out = Scan::default();
    for view in &frozen.views {
        for s in view.stored.iter() {
            let shape = ask.shape(&s.schema);
            let mut reader = Reader::open(&s.file.path).expect("open");
            let unit = Unit::read(s, &shape, view.day, 0, &mut reader, &mut out).expect("read");
            f(&unit);
        }
    }
}

/// A debug build makes this comparison inside `pick`; here the test makes it itself, so
/// that it is made in a release build too, and says how much it compared.
#[test]
fn the_rows_chosen_from_columns_are_the_rows_chosen_cell_by_cell() {
    let scratch = Scratch::new("scan-pick");
    let (_s, leaf) = mixed(&scratch, false);
    let frozen = leaf.freeze(None, None);
    let (mut some, mut every, mut none) = (0, 0, 0);
    for (opts, terms) in asks() {
        let ask = Ask::new(&opts, &terms, Wants::Rows);
        each_unit(&frozen, &ask, |unit| {
            let fast = pick(unit, &ask).expect("pick");
            let before = decoded(unit);
            let slow = slow_pick(unit, &ask).expect("slow_pick");
            assert_eq!(fast, slow, "{terms:?} with {opts:?}");
            // And the plain way decoded for itself: the unit is as `pick` left it.
            assert_eq!(decoded(unit), before);
            match fast.count() as usize {
                0 => none += 1,
                n if n == unit.rows => every += 1,
                _ => some += 1,
            }
        });
    }
    assert!(
        some > 500 && every > 200 && none > 500,
        "{some} {every} {none}"
    );
}

/// The names of the columns a unit has decoded so far.
fn decoded(unit: &Unit<'_>) -> Vec<String> {
    (0..unit.columns.len())
        .filter(|place| unit.columns[*place].get().is_some())
        .map(|place| unit.at.schema[unit.shape.wanted[place]].clone())
        .collect()
}

/// A column is decoded when something asks for it, and not before: the answer would be the
/// same with every column decoded, and several times slower.
#[test]
fn a_row_group_decodes_only_the_columns_it_is_asked_about() {
    let scratch = Scratch::new("scan-lazy");
    let (_s, leaf) = mixed(&scratch, false);
    let frozen = leaf.freeze(None, None);
    let base = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let windowed = ScanOptions {
        since: Some(instant(T0)),
        ..base.clone()
    };
    // What is asked, and the columns each row group may decode to choose its rows.
    let cases: [(&ScanOptions, Vec<Term>, &[&str]); 7] = [
        (&base, vec![], &[]),
        (&base, vec![Term::exact("level", "INFO")], &["level"]),
        // Every row of `n` is there, and its chunk says so.
        (&base, vec![Term::any("n")], &[]),
        // `note` is all null in one row group, has a presence bitmap in another, and
        // is whole in the third: decoded only where the bitmap has to be read.
        (&base, vec![Term::any("note")], &["note"]),
        // The first term leaves no row, so the second's column is left alone.
        (
            &base,
            vec![Term::exact("level", "info"), Term::exact("n", "3")],
            &["level"],
        ),
        // The window reads the time column, and the arrival time only where a row
        // has no time of its own.
        (&windowed, vec![], &["\\_received_at", "ts"]),
        (
            &windowed,
            vec![Term::exact("n", "3")],
            &["\\_received_at", "n", "ts"],
        ),
    ];
    for (opts, terms, may) in cases {
        let ask = Ask::new(opts, &terms, Wants::Nothing);
        let mut all = BTreeSet::new();
        each_unit(&frozen, &ask, |unit| {
            assert!(decoded(unit).is_empty(), "decoded by being read");
            let sel = pick(unit, &ask).expect("pick");
            let chosen = decoded(unit);
            assert!(
                chosen.iter().all(|name| may.contains(&name.as_str())),
                "{terms:?}: decoded {chosen:?}"
            );
            all.extend(chosen.clone());
            // A count takes the rows chosen and decodes nothing to do it.
            let mut out = Scan::default();
            hand(
                unit,
                &ask,
                &sel,
                &mut Keeps::new(Wants::Nothing, true),
                &mut out,
            )
            .expect("hand");
            assert_eq!(out.matched, sel.count());
            assert_eq!(decoded(unit), chosen, "{terms:?}: decoded by a count");
        });
        // And each of them was decoded by some row group: the list is not slack.
        assert_eq!(
            all.iter().map(String::as_str).collect::<Vec<_>>(),
            may,
            "{terms:?}"
        );
    }

    // Rows for a visitor that named one column decode that column beside the term's,
    // and only in a row group some row of which passed.
    let terms = [Term::exact("level", "WARN")];
    let ask = Ask::new(&base, &terms, Wants::Columns(vec!["n".into()]));
    let mut built = 0;
    each_unit(&frozen, &ask, |unit| {
        let sel = pick(unit, &ask).expect("pick");
        let mut v = Keeps::new(Wants::Columns(vec!["n".into()]), true);
        hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
        let want: &[&str] = if sel.is_empty() {
            &["level"]
        } else {
            &["level", "n"]
        };
        assert_eq!(decoded(unit), want);
        built += v.rows.len();
    });
    assert!(built > 0);

    // A visitor that is asked first and takes no row: only the time it is asked with is
    // decoded. Then it takes one row, and the whole row group is decoded to build it.
    let ask = Ask::new(&base, &[], Wants::Newest);
    let mut fell_back = 0;
    each_unit(&frozen, &ask, |unit| {
        let sel = pick(unit, &ask).expect("pick");
        assert!(decoded(unit).is_empty());
        let mut v = Keeps::refusing(true, |_| true);
        hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
        assert_eq!((v.asked.len(), v.rows.len()), (unit.rows, 0));
        let times = decoded(unit);
        assert!(
            times == ["ts"] || times == ["\\_received_at", "ts"],
            "decoded {times:?} to ask"
        );
        fell_back += usize::from(times.len() == 2);

        let mut v = Keeps::refusing(true, |seq| seq != 3);
        hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
        assert_eq!(v.rows.len(), 1);
        assert_eq!(decoded(unit).len(), unit.columns.len());
    });
    assert_eq!(fell_back, 1);

    // And where the terms leave no row of a row group there is nothing to ask about:
    // not even the time column is decoded.
    let ask = Ask::new(&base, &terms, Wants::Newest);
    let (mut asked, mut unasked) = (0, 0);
    each_unit(&frozen, &ask, |unit| {
        let sel = pick(unit, &ask).expect("pick");
        let mut v = Keeps::refusing(true, |_| true);
        hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
        assert_eq!(v.asked.len() as u64, sel.count());
        let want: &[&str] = if sel.is_empty() {
            unasked += 1;
            &["level"]
        } else {
            asked += 1;
            &["level", "ts"]
        };
        assert_eq!(decoded(unit), want);
    });
    assert!(asked > 0 && unasked > 0, "{asked} {unasked}");
}

/// The places, among a unit's wanted columns, of the ones decoded so far.
fn places(unit: &Unit<'_>) -> Vec<usize> {
    (0..unit.columns.len())
        .filter(|place| unit.columns[*place].get().is_some())
        .collect()
}

/// A thread that chooses a row group's rows decodes only what `hand` would decode next, so
/// no thread decodes a column the scan in turn would not have.
#[test]
fn a_task_decodes_the_column_a_fetch_asks_with_and_leaves_it_in_the_unit() {
    let scratch = Scratch::new("scan-warm");
    let (_m, timed) = mixed(&scratch, false);
    let other = Scratch::new("scan-warm-untimed");
    let s = store_with(&other, thresholds(UNIT, usize::MAX));
    // A template with no time column, and one whose rows hold no time at all.
    let by_arrival = s.open(11, "ts").expect("leaf");
    let timeless = s.open(12, "ts").expect("leaf");
    for n in 0..UNIT {
        let mut row = samples::row(&format!(r#"{{"n":{n},"level":"INFO"}}"#));
        row.insert(
            RECEIVED_AT_COLUMN.into(),
            ColValue::Datetime(T0 + n as i64 * SECOND),
        );
        by_arrival.append(row);
        timeless.append(samples::row(&format!(r#"{{"n":{n},"level":"INFO"}}"#)));
    }
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!(done.row_groups, 2);
    let leaves = s.leaves.read().expect("leaves");
    let frozen = [
        timed.freeze(None, None),
        leaves[&11].freeze(None, None),
        leaves[&12].freeze(None, None),
    ];

    let base = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let text = ScanOptions {
        text_columns: vec!["ts".into()],
        ..base.clone()
    };
    let windowed = ScanOptions {
        since: Some(instant(T0)),
        ..base.clone()
    };
    let stopped = ScanOptions {
        limit: 5,
        ..base.clone()
    };
    let info = vec![Term::exact("level", "INFO")];
    let warn = vec![Term::exact("level", "WARN")];
    let cases: [(&ScanOptions, &[Term], Wants); 9] = [
        (&base, &[], Wants::Newest),
        (&base, &info, Wants::Newest),
        // Leaves no row of most row groups: there is then nothing to ask about.
        (&base, &warn, Wants::Newest),
        // The window has `pick` decode the times itself.
        (&windowed, &[], Wants::Newest),
        (&text, &[], Wants::Newest),
        (&text, &info, Wants::Newest),
        // Visitors that are not asked first: handed every row, handed none, and one
        // that would be asked but for the number of rows the scan stops at.
        (&base, &info, Wants::Rows),
        (&base, &info, Wants::Nothing),
        (&stopped, &[], Wants::Newest),
    ];
    let mut kinds = BTreeSet::new();
    for (opts, terms, wants) in cases {
        let ask = Ask::new(opts, terms, wants.clone());
        let said = format!("{terms:?} with {opts:?}, read as {wants:?}");
        for frozen in &frozen {
            each_unit(frozen, &ask, |unit| {
                let sel = pick(unit, &ask).expect("pick");
                let chosen = places(unit);
                warm(unit, &ask, &sel);
                let first = unit.shape.ts.or(unit.shape.received);
                let warmed = ask.warms() && !sel.is_empty();
                let mut want = chosen.clone();
                want.extend(first.filter(|place| warmed && !chosen.contains(place)));
                want.sort_unstable();
                assert_eq!(places(unit), want, "{said}: decoded by a task");
                if !ask.warms() {
                    // Nothing is decoded ahead for a visitor that is not asked first,
                    // or is asked with a time read a row at a time.
                    assert_eq!(want, chosen, "{said}");
                    if ask.gated() {
                        kinds.insert("a time column now text");
                    }
                    return;
                }
                if sel.is_empty() {
                    kinds.insert("no row chosen");
                }

                let mut v = Keeps::refusing(true, |_| true);
                hand(unit, &ask, &sel, &mut v, &mut Scan::default()).expect("hand");
                assert_eq!((v.asked.len() as u64, v.rows.len()), (sel.count(), 0));
                // Read after `hand`, so that looking does not decode for it.
                let gaps = match (unit.shape.ts, unit.shape.received) {
                    (Some(ts), Some(received)) if ts != received && !sel.is_empty() => {
                        let times = instants(unit.column(ts).expect("the time column"));
                        times.expect("instants").missing().then_some(received)
                    }
                    _ => None,
                };
                let mut after = want.clone();
                after.extend(gaps.filter(|place| !want.contains(place)));
                after.sort_unstable();
                assert_eq!(places(unit), after, "{said}: decoded by `hand`");
                if sel.is_empty() {
                    return;
                }
                kinds.insert(match (unit.shape.ts, unit.shape.received) {
                    (None, None) => "no time at all",
                    (None, Some(_)) => "no time column",
                    (Some(_), _) if gaps.is_some() => "rows with no instant",
                    (Some(_), _) => "a time column every row has an instant in",
                });
                if let Some(ts) = unit.shape.ts {
                    let layout = unit.header.chunks[unit.shape.wanted[ts]].layout;
                    kinds.insert(if layout & TWO_PIECES != 0 {
                        "a time column in two pieces"
                    } else {
                        "a time column in one piece"
                    });
                }
            });
        }
    }
    assert_eq!(
        kinds.into_iter().collect::<Vec<_>>(),
        [
            "a time column every row has an instant in",
            "a time column in one piece",
            "a time column in two pieces",
            "a time column now text",
            "no row chosen",
            "no time at all",
            "no time column",
            "rows with no instant",
        ]
    );
}

/// One scan of a leaf with the scan threads `fan` in place of the store's own: what it
/// counted, and the runs of row groups it shared out.
fn scan_with(
    leaf: &DiskLeaf,
    fan: &Arc<Fan>,
    opts: &ScanOptions,
    terms: &[Term],
    wants: Wants,
) -> (Scan, u64) {
    let frozen = Frozen {
        fan: Some(Arc::clone(fan)),
        ..leaf.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of))
    };
    let before = fan.fanned();
    let mut v = Keeps::new(wants, true);
    let scan = frozen.visit(opts, terms, &mut v).expect("scan");
    (scan, fan.fanned() - before)
}

/// When a run of row groups is shared out and when it is not (`Ask::fans`, `Fan::turn`):
/// only with two row groups or more, enough rows, something to decode and the threads free.
#[test]
fn a_run_of_row_groups_is_shared_out_when_a_second_thread_has_something_to_do() {
    let scratch = Scratch::new("scan-fans");
    let (_s, leaf) = mixed(&scratch, true);
    let base = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let term = [Term::exact("level", "INFO")];
    let every = Fan::new(4, 0).expect("a fan");
    let out = |opts: &ScanOptions, terms: &[Term], wants: Wants| {
        scan_with(&leaf, &every, opts, terms, wants).1
    };

    // Something to decode: once, for the older day's three row groups.
    assert_eq!(out(&base, &term, Wants::Nothing), 1, "a count with a term");
    assert_eq!(out(&base, &term, Wants::Rows), 1, "a fetch with a term");
    assert_eq!(out(&base, &term, Wants::Columns(named())), 1, "a grouping");
    assert_eq!(
        out(&base, &[], Wants::Newest),
        1,
        "an open fetch asked first"
    );
    // Nothing to decode before the visitor is asked.
    assert_eq!(out(&base, &[], Wants::Nothing), 0, "an open count");
    assert_eq!(
        out(&base, &[], Wants::Rows),
        0,
        "an open fetch handed every row"
    );
    let text = ScanOptions {
        text_columns: vec!["ts".into()],
        ..base.clone()
    };
    assert_eq!(
        out(&text, &[], Wants::Newest),
        0,
        "asked first, by a time that is text"
    );
    assert_eq!(out(&text, &term, Wants::Newest), 1, "and with a term");
    // A number of rows to stop at.
    for limit in [1, 5, 1_000] {
        let stops = ScanOptions {
            limit,
            ..base.clone()
        };
        assert_eq!(out(&stops, &term, Wants::Rows), 0, "a limit of {limit}");
        assert_eq!(out(&stops, &[], Wants::Newest), 0, "a limit of {limit}");
    }
    // A window that leaves two of the older day's row groups, and one that leaves
    // one: a row group alone is not a run.
    let from = |second: i64| ScanOptions {
        since: Some(instant(T0 + second * SECOND)),
        until: Some(instant(T0 + 50 * SECOND)),
        ..base.clone()
    };
    let (scan, shared_out) = scan_with(&leaf, &every, &from(20), &[], Wants::Nothing);
    assert_eq!((scan.row_groups_read, scan.row_groups_pruned), (2, 2));
    assert_eq!(shared_out, 1, "a window over two row groups");
    let (scan, shared_out) = scan_with(&leaf, &every, &from(40), &[], Wants::Nothing);
    assert_eq!((scan.row_groups_read, scan.row_groups_pruned), (1, 3));
    assert_eq!(shared_out, 0, "a window over one row group");

    // Rows by the index: the older day's three row groups hold forty-eight.
    let stored = 3 * UNIT as u64;
    let exactly = Fan::new(4, stored).expect("a fan");
    let one_more = Fan::new(4, stored + 1).expect("a fan");
    let shared_out = |fan: &Arc<Fan>| scan_with(&leaf, fan, &base, &term, Wants::Nothing).1;
    assert_eq!(shared_out(&exactly), 1);
    assert_eq!(shared_out(&one_more), 0, "a run under the least rows");
    assert!(
        !one_more.started(),
        "threads were started for a run not shared out"
    );

    // An ingest body in its task, and another scan with the threads.
    let body = every.body();
    assert_eq!(
        out(&base, &term, Wants::Nothing),
        0,
        "beside an ingest body"
    );
    drop(body);
    assert_eq!(out(&base, &term, Wants::Nothing), 1, "after it");
    let turn = every.turn().expect("a turn");
    assert_eq!(
        out(&base, &term, Wants::Nothing),
        0,
        "beside another scan's turn"
    );
    drop(turn);
    assert_eq!(out(&base, &term, Wants::Nothing), 1, "after it");
}

/// A day of more row groups than one run holds is shared out a run at a time, and counted
/// as the plain scan counts them.
#[test]
fn a_long_day_is_shared_out_a_run_of_row_groups_at_a_time() {
    const DAY_ROW_GROUPS: usize = 600;
    let scratch = Scratch::new("scan-runs");
    let s = store_with(
        &scratch,
        DiskOptions {
            scan_threads: 4,
            fan_rows: 0,
            ..thresholds(1, usize::MAX)
        },
    );
    let opened = s.open(7, "ts").expect("leaf");
    for n in 0..DAY_ROW_GROUPS {
        let line = format!(
            r#"{{"ts":"2024-01-15T10:{:02}:{:02}Z","n":{n}}}"#,
            30 + n / 60,
            n % 60
        );
        opened.append(samples::row(&line));
    }
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!(done.row_groups, DAY_ROW_GROUPS);
    let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&7]);

    let opts = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    // By a term every row passes, by one a single row passes, and by none at all:
    // a fetch that is asked first has its time column to decode either way.
    for (terms, matched) in [
        (vec![Term::any("n")], DAY_ROW_GROUPS),
        (vec![Term::exact("n", "7")], 1),
        (vec![], DAY_ROW_GROUPS),
    ] {
        let frozen = leaf.freeze(None, None);
        let mut plain = Keeps::new(Wants::Rows, true);
        let slow = slow_visit(&frozen, &opts, &terms, &mut plain).expect("plain scan");
        let before = fanned(&s);
        let mut v = Keeps::new(Wants::Newest, true);
        let scan = frozen.visit(&opts, &terms, &mut v).expect("scan");
        assert_eq!(fanned(&s) - before, 3, "{terms:?}");
        assert_eq!(counters(&scan), counters(&slow), "{terms:?}");
        assert_eq!(
            (scan.matched, scan.row_groups_read),
            (matched as u64, DAY_ROW_GROUPS as u64)
        );
        held_to_plain(&v, &plain.rows, true, &format!("{terms:?}"));
    }
    assert_eq!(WINDOW_ROW_GROUPS, 256);
}

/// One day of eight row groups of sixteen rows in two files: five in the first, and
/// three in a second.
fn two_files(scratch: &Scratch) -> (DiskStore, Arc<DiskLeaf>) {
    let s = store_with(scratch, thresholds(UNIT, usize::MAX));
    let opened = s.open(7, "ts").expect("leaf");
    for (from, to) in [(0, 5 * UNIT), (5 * UNIT, 8 * UNIT)] {
        for n in from..to {
            let line = format!(
                r#"{{"ts":"2024-01-15T10:{:02}:{:02}Z","n":{n}}}"#,
                30 + n / 60,
                n % 60
            );
            opened.append(samples::row(&line));
        }
        // Closes the row groups and seals their file: the next rows open the next.
        let done = s.flush(FlushKind::Final);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!(done.row_groups, (to - from) / UNIT);
    }
    let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&7]);
    (s, leaf)
}

/// A scan told to stop reads no row group more: the one the stop was said in is handed
/// over whole, and the scan ends with `Stopped` and not with a count.
#[test]
fn a_scan_told_to_stop_reads_no_row_group_more() {
    use crate::rows::Stop;

    struct Until<'s> {
        stop: &'s Stop,
        rows: usize,
    }
    impl Visitor for Until<'_> {
        fn row(&mut self, _: &Row, _: u64) {
            self.rows += 1;
            self.stop.set();
        }
    }

    let scratch = Scratch::new("scan-stops");
    let (_s, leaf) = two_files(&scratch);
    let asked = |stop: &Stop| ScanOptions {
        ts_column: Some("ts".into()),
        stop: stop.clone(),
        ..ScanOptions::default()
    };

    let never = Stop::new();
    let whole = leaf.scan(&asked(&never), &[]).expect("a scan");
    assert_eq!((whole.matched, whole.row_groups_read), (8 * UNIT as u64, 8));

    let stop = Stop::new();
    let mut v = Until {
        stop: &stop,
        rows: 0,
    };
    let told = leaf.visit(&asked(&stop), &[], &mut v);
    assert!(matches!(told, Err(StoreError::Stopped)), "{told:?}");
    assert_eq!(v.rows, UNIT, "the row group the stop was said in, and no other");

    // Told already: not a row.
    v.rows = 0;
    let told = leaf.visit(&asked(&stop), &[], &mut v);
    assert!(matches!(told, Err(StoreError::Stopped)), "{told:?}");
    assert_eq!(v.rows, 0);

}

/// A scan that fails ends on the first error in storage order, shared out or not. Run a
/// hundred times each, since what a thread finishes first must not show.
#[test]
fn a_scan_fails_with_the_first_error_in_storage_order_shared_out_or_not() {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    let scratch = Scratch::new("scan-errors");
    let (_s, leaf) = two_files(&scratch);
    let frozen = leaf.freeze(None, None);
    assert_eq!(frozen.views.len(), 1);
    let stored = &frozen.views[0].stored;
    assert_eq!(stored.len(), 8);
    let (first, second) = (&stored[0].file.path, &stored[5].file.path);
    assert_eq!(first, &stored[4].file.path);
    assert_ne!(first, second, "the day is in one file");

    // The first byte of the third row group's chunk area, turned over and back.
    let third = &stored[2].entry;
    let at = third.offset + u64::from(third.frame_len);
    let flip = || {
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(first)
            .expect("open");
        let mut byte = [0u8];
        file.seek(SeekFrom::Start(at)).expect("seek");
        file.read_exact(&mut byte).expect("read");
        file.seek(SeekFrom::Start(at)).expect("seek");
        file.write_all(&[!byte[0]]).expect("write");
    };
    flip();
    std::fs::remove_file(second).expect("remove");

    let opts = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let terms = [Term::any("n")];
    let fan = Fan::new(4, 0).expect("a fan");
    // The scan's error as it is said, and the rows the visitor had been handed.
    let failed = |fan: Option<&Arc<Fan>>, wants: Wants| -> (String, usize, usize) {
        let frozen = Frozen {
            fan: fan.map(Arc::clone),
            ..leaf.freeze(None, None)
        };
        let mut v = Keeps::new(wants, true);
        let error = match frozen.visit(&opts, &terms, &mut v) {
            Ok(scan) => panic!("the scan answered: {:?}", counters(&scan)),
            Err(error) => error.to_string(),
        };
        (error, v.rows.len(), v.asked.len())
    };

    let crc = failed(None, Wants::Rows);
    assert!(crc.0.contains("a chunk's CRC does not hold"), "{}", crc.0);
    assert!(crc.0.contains(&first.display().to_string()), "{}", crc.0);
    assert_eq!((crc.1, crc.2), (2 * UNIT, 0));
    let asked = failed(None, Wants::Newest);
    assert_eq!((&asked.0, asked.1, asked.2), (&crc.0, 2 * UNIT, 2 * UNIT));
    for _ in 0..100 {
        assert_eq!(failed(None, Wants::Rows), crc);
        assert_eq!(failed(Some(&fan), Wants::Rows), crc);
        assert_eq!(failed(Some(&fan), Wants::Newest), asked);
    }
    assert_eq!(fan.fanned(), 200);

    flip();
    let gone = failed(None, Wants::Rows);
    assert!(gone.0.contains(&second.display().to_string()), "{}", gone.0);
    assert!(!gone.0.contains("CRC"), "{}", gone.0);
    assert_eq!((gone.1, gone.2), (5 * UNIT, 0));
    let asked = failed(None, Wants::Newest);
    assert_eq!((&asked.0, asked.1, asked.2), (&gone.0, 5 * UNIT, 5 * UNIT));
    for _ in 0..100 {
        assert_eq!(failed(None, Wants::Rows), gone);
        assert_eq!(failed(Some(&fan), Wants::Rows), gone);
        assert_eq!(failed(Some(&fan), Wants::Newest), asked);
    }
    assert_eq!(fan.fanned(), 400);
}

/// `Frozen::visit` for a visitor that takes every day, with a batch's budget said by the
/// test and the scan threads `fan` in place of the store's own.
fn visit_by(
    frozen: &Frozen,
    opts: &ScanOptions,
    terms: &[Term],
    v: &mut Keeps,
    fan: &Fan,
    batch: u64,
) -> Result<Scan, StoreError> {
    let mut out = Scan {
        row_groups_pruned: frozen.pruned,
        ..Scan::default()
    };
    let ask = Ask::new(opts, terms, v.wants());
    let mut readers = HashMap::new();
    for view in &frozen.views {
        assert!(v.wants_partition(view.span.map(|(_, hi)| hi)));
        if !day_by(view, &ask, v, &mut out, &mut readers, Some(fan), batch)? {
            break;
        }
    }
    Ok(out)
}

/// What each stored row group of a day is counted for against a batch's bytes, for
/// one question: read off its header, with nothing decoded.
fn inflated(view: &View, ask: &Ask<'_>) -> Vec<u64> {
    let mut sizes = Vec::new();
    for s in view.stored.iter() {
        let shape = ask.shape(&s.schema);
        let mut reader = Reader::open(&s.file.path).expect("open");
        let unit = Unit::read(s, &shape, view.day, 0, &mut reader, &mut Scan::default())
            .expect("read");
        sizes.push(unit.inflates(ask));
    }
    sizes
}

/// How a scan that may share out every run cuts each day of a frozen leaf into batches of
/// `batch` bytes, said here from what a batch is, apart from the scan.
fn batches_of(frozen: &Frozen, ask: &Ask<'_>, batch: u64) -> Vec<Vec<usize>> {
    let mut days = Vec::new();
    for view in &frozen.views {
        let sizes: Vec<u64> = view
            .stored
            .iter()
            .zip(inflated(view, ask))
            .filter(|(s, _)| {
                ask.since.is_none_or(|from| s.entry.max_time >= from)
                    && ask.until.is_none_or(|to| s.entry.min_time < to)
            })
            .map(|(_, size)| size)
            .collect();
        if sizes.len() < 2 || !ask.fans() {
            continue;
        }
        let (mut cut, mut held) = (vec![0usize], 0u64);
        for (at, size) in sizes.iter().enumerate() {
            held += size;
            *cut.last_mut().expect("a batch") += 1;
            if held >= batch && at + 1 < sizes.len() {
                cut.push(0);
                held = 0;
            }
        }
        days.push(cut);
    }
    days
}

/// A run that is shared out is chosen a batch at a time, whatever the budget, and answers
/// what the scan in turn answers; a batch of one is not a run shared out.
#[test]
fn a_shared_out_run_is_chosen_a_batch_at_a_time_and_answers_what_the_scan_in_turn_answers() {
    let scratch = Scratch::new("scan-batches");
    let (s, leaf) = mixed(&scratch, true);
    assert!(s.fan.is_none(), "the reference store has scan threads");
    let fan = Fan::new(4, 0).expect("a fan");
    let none: fn(u64) -> bool = |_| false;
    let visitors = [
        (Wants::Nothing, none),
        (Wants::Columns(named()), none),
        (Wants::Rows, none),
        (Wants::Newest, none),
        (Wants::Newest, third),
    ];
    // How each shared-out day was cut, over every scan: the row groups in each batch.
    let mut cuts: BTreeSet<Vec<usize>> = BTreeSet::new();
    let (mut scans, mut shared_out) = (0usize, 0u64);
    for (opts, terms) in asks() {
        let frozen = leaf.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of));
        for (wants, refuses) in &visitors {
            let said = format!("{terms:?} with {opts:?}, read as {wants:?}");
            let mut plain = Keeps {
                refuses: *refuses,
                ..Keeps::new(wants.clone(), true)
            };
            let in_turn = frozen.visit(&opts, &terms, &mut plain).expect("scan");

            let ask = Ask::new(&opts, &terms, wants.clone());
            // The budgets, from what the oldest day's row groups are counted for: one every row
            // group reaches, one the first just reaches, one it just misses, the sum, and none.
            let oldest = frozen.views.last().expect("a day");
            let sizes = inflated(oldest, &ask);
            let budgets = [
                1,
                sizes[0],
                sizes[0] + 1,
                sizes.iter().sum::<u64>(),
                u64::MAX,
            ];
            for batch in budgets {
                let said = format!("{said}, a batch of {batch} bytes");
                let before = fan.fanned();
                let mut v = Keeps {
                    refuses: *refuses,
                    ..Keeps::new(wants.clone(), true)
                };
                let scan = visit_by(&frozen, &opts, &terms, &mut v, &fan, batch).expect("scan");
                assert_eq!(counters(&scan), counters(&in_turn), "{said}");
                assert_eq!(v.rows, plain.rows, "{said}");
                assert_eq!(v.asked, plain.asked, "{said}");

                let days = batches_of(&frozen, &ask, batch);
                let runs = days.iter().flatten().filter(|held| **held >= 2).count();
                assert_eq!(fan.fanned() - before, runs as u64, "{said}: {days:?}");
                shared_out += runs as u64;
                cuts.extend(days);
                scans += 1;
            }
        }
    }
    assert!(scans > 20_000, "{scans}");
    assert!(shared_out > 5_000, "{shared_out} runs shared out");
    // Not vacuous: a day was cut every way three row groups can be, and two both ways.
    assert_eq!(
        cuts.into_iter().collect::<Vec<_>>(),
        [
            vec![1, 1],
            vec![1, 1, 1],
            vec![1, 2],
            vec![2],
            vec![2, 1],
            vec![3],
        ]
    );
    // And the budget every scan runs with leaves a run of such row groups whole.
    assert_eq!(BATCH_BODY_BYTES, 32 * 1024 * 1024);
}

/// What a batch may hold decoded is counted off the header, and is never less than what
/// choosing its rows decodes: without this a batch could be closed late, over its budget.
#[test]
fn a_row_group_is_counted_for_at_least_what_choosing_its_rows_decodes() {
    let scratch = Scratch::new("scan-inflates");
    let (_s, leaf) = mixed(&scratch, false);
    let frozen = leaf.freeze(None, None);
    let (mut some, mut exact, mut spared) = (0usize, 0usize, 0usize);
    for (opts, terms) in asks() {
        for wants in [Wants::Nothing, Wants::Rows, Wants::Newest] {
            let ask = Ask::new(&opts, &terms, wants);
            each_unit(&frozen, &ask, |unit| {
                let counted = unit.inflates(&ask);
                let sel = pick(unit, &ask).expect("pick");
                warm(unit, &ask, &sel);
                let decoded: u64 = places(unit)
                    .into_iter()
                    .map(|place| {
                        u64::from(unit.header.chunks[unit.shape.wanted[place]].body_len)
                    })
                    .sum();
                assert!(
                    decoded <= counted,
                    "{terms:?} with {opts:?}: {decoded} bytes decoded, {counted} counted"
                );
                some += usize::from(decoded > 0);
                exact += usize::from(decoded > 0 && decoded == counted);
                spared += usize::from(decoded < counted);
            });
        }
    }
    assert!(
        some > 2_000 && exact > 1_000 && spared > 1_000,
        "{some} {exact} {spared}"
    );
}

/// A row group nothing will be built from holds no decoded column once its rows are
/// chosen, and `hand` then counts and hands over as before: letting them go lost nothing.
#[test]
fn a_row_group_nothing_is_built_from_lets_its_columns_go_once_its_rows_are_chosen() {
    let scratch = Scratch::new("scan-choose");
    let (_s, leaf) = mixed(&scratch, false);
    let frozen = leaf.freeze(None, None);
    let (mut counted, mut empty, mut kept) = (0usize, 0usize, 0usize);
    for (opts, terms) in asks() {
        for wants in [Wants::Nothing, Wants::Columns(named()), Wants::Newest] {
            let ask = Ask::new(&opts, &terms, wants.clone());
            let said = format!("{terms:?} with {opts:?}, read as {wants:?}");
            for view in &frozen.views {
                for s in view.stored.iter() {
                    let shape = ask.shape(&s.schema);
                    let mut reader = Reader::open(&s.file.path).expect("open");
                    let mut read = || {
                        Unit::read(s, &shape, view.day, 0, &mut reader, &mut Scan::default())
                            .expect("read")
                    };
                    // The row group as a scan in turn has it when `hand` is reached,
                    // but for the column a thread decodes ahead.
                    let plain = read();
                    let sel = pick(&plain, &ask).expect("pick");
                    let chosen = places(&plain);
                    warm(&plain, &ask, &sel);

                    let mut slot = Slot {
                        unit: read(),
                        sel: None,
                    };
                    choose(&mut slot, &ask);
                    let Slot { unit, sel: got } = slot;
                    let got = got.expect("chosen").expect("pick");
                    assert_eq!(got, sel, "{said}");
                    if wants == Wants::Nothing || sel.is_empty() {
                        assert!(places(&unit).is_empty(), "{said}: {:?}", places(&unit));
                        counted += usize::from(wants == Wants::Nothing && !chosen.is_empty());
                        empty += usize::from(sel.is_empty() && !chosen.is_empty());
                    } else {
                        assert_eq!(places(&unit), places(&plain), "{said}");
                        kept += usize::from(!places(&unit).is_empty());
                    }

                    let (mut a, mut b) = (Scan::default(), Scan::default());
                    let mut mine = Keeps::refusing(true, third);
                    let mut theirs = Keeps::refusing(true, third);
                    mine.wants = wants.clone();
                    theirs.wants = wants.clone();
                    let on = hand(&unit, &ask, &got, &mut mine, &mut a).expect("hand");
                    assert!(on && hand(&plain, &ask, &sel, &mut theirs, &mut b).expect("hand"));
                    assert_eq!(counters(&a), counters(&b), "{said}");
                    assert_eq!((&mine.rows, &mine.asked), (&theirs.rows, &theirs.asked));
                }
            }
        }
    }
    // Not vacuous: columns were let go for a count and for an empty selection, and
    // kept for a row group that has rows to build.
    assert!(
        counted > 1_000 && empty > 1_000 && kept > 1_000,
        "{counted} {empty} {kept}"
    );
}

/// A scan that fails ends on the same error, with the same rows handed over, however its
/// run is cut into batches: the error met first, last or mid-batch.
#[test]
fn a_scan_cut_into_batches_fails_with_the_error_and_the_rows_of_the_scan_in_turn() {
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    let scratch = Scratch::new("scan-batch-errors");
    let (_s, leaf) = two_files(&scratch);
    let opts = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let terms = [Term::exact("n", "7")];
    let frozen = leaf.freeze(None, None);
    let stored = &frozen.views[0].stored;
    let (first, second) = (stored[0].file.path.clone(), stored[5].file.path.clone());
    let third = &stored[2].entry;
    let at = third.offset + u64::from(third.frame_len);
    // The budgets for each visitor, taken before anything is broken: one every row group
    // reaches, ones that two and three of the first four reach, and one nothing reaches.
    let budgets = |wants: &Wants| -> [u64; 4] {
        let sizes = inflated(&frozen.views[0], &Ask::new(&opts, &terms, wants.clone()));
        let one = sizes[0];
        assert!(
            one > 0 && sizes[..4] == [one; 4] && (one..=2 * one).contains(&sizes[4]),
            "{sizes:?}"
        );
        [1, one + 1, 2 * one + 1, u64::MAX]
    };
    let cut = [Wants::Rows, Wants::Newest].map(|wants| (budgets(&wants), wants));

    let flip = || {
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .open(&first)
            .expect("open");
        let mut byte = [0u8];
        file.seek(SeekFrom::Start(at)).expect("seek");
        file.read_exact(&mut byte).expect("read");
        file.seek(SeekFrom::Start(at)).expect("seek");
        file.write_all(&[!byte[0]]).expect("write");
    };
    let fan = Fan::new(4, 0).expect("a fan");
    // The scan's error as it is said, and the rows the visitor was handed and asked
    // about: in turn through the store's own scan, and cut into batches.
    let failed = |batch: Option<u64>, wants: Wants| -> (String, Vec<(Row, u64)>, usize) {
        let frozen = leaf.freeze(None, None);
        let mut v = Keeps::new(wants, true);
        let scan = match batch {
            None => frozen.visit(&opts, &terms, &mut v),
            Some(batch) => visit_by(&frozen, &opts, &terms, &mut v, &fan, batch),
        };
        let error = match scan {
            Ok(scan) => panic!("the scan answered: {:?}", counters(&scan)),
            Err(error) => error.to_string(),
        };
        (error, v.rows, v.asked.len())
    };
    let held = |file: &std::path::Path, crc: bool, row_groups: usize| {
        for (budgets, wants) in &cut {
            let in_turn = failed(None, wants.clone());
            assert!(
                in_turn.0.contains(&file.display().to_string()),
                "{}",
                in_turn.0
            );
            assert_eq!(in_turn.0.contains("CRC"), crc, "{}", in_turn.0);
            // One row in the first row group passes, and it was handed over.
            assert_eq!(in_turn.1.len(), 1);
            assert_eq!(in_turn.2, usize::from(*wants == Wants::Newest));
            for batch in *budgets {
                for _ in 0..20 {
                    assert_eq!(
                        failed(Some(batch), wants.clone()),
                        in_turn,
                        "a batch of {batch} bytes, {row_groups} row groups before the error"
                    );
                }
            }
        }
    };

    flip();
    std::fs::remove_file(&second).expect("remove");
    held(&first, true, 2);
    // Two row groups stand before the error: shared out as one batch of two where a
    // batch holds two, three or all, and never where it holds one.
    assert_eq!(fan.fanned(), 2 * 20 * 3);
    flip();
    held(&second, false, 5);
    // Five before it: two batches of two and the fifth alone; a batch of three and
    // one of two; and one of five.
    assert_eq!(fan.fanned(), 2 * 20 * 3 + 2 * 20 * (2 + 2 + 1));
}
