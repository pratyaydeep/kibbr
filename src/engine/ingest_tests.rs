// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::engine::store::{effective_ts, ScanOptions, Visitor, Wants};
use crate::engine::templates::trie::Schema;

const ARRIVED: i64 = 1_800_000_000_000_000_000;

struct Rows(Vec<Row>);

impl Visitor for Rows {
    fn row(&mut self, row: &Row, _: u64) {
        self.0.push(row.clone());
    }

    fn wants(&self) -> Wants {
        Wants::Rows
    }
}

/// **A row whose time column holds a time-column form is not given an arrival time**:
/// it is stored under its own, and one whose time column holds no time still is.
#[test]
fn a_line_with_a_time_column_form_is_stored_under_its_own_time() {
    let lines = [
        r#"{"timestamp":"1679711330.570420890","channel":"OPS"}"#,
        r#"{"timestamp":"1679711331.5","channel":"DEV"}"#,
        r#"{"timestamp":"not a time","channel":"OPS"}"#,
        r#"{"time_us":1732206349000167,"kind":"commit"}"#,
        r#"{"timestamp":"2023-03-27 00:26:35.719 EDT","pid":27}"#,
        r#"{"timestamp":"2023-03-27 00:26:35.719 IST","pid":28}"#,
    ]
    .join("\n");
    let schema = Schema {
        time_columns: vec!["timestamp".into(), "time_us".into()],
    };
    let now = chrono::DateTime::from_timestamp_nanos(ARRIVED);
    let db = into_memory(lines.as_bytes(), true, now, &schema).expect("load");
    assert_eq!(db.stats.ingested, 6);

    let mut seen: Vec<(String, Option<i64>, bool)> = Vec::new();
    for leaf in db.trie.all_leaves() {
        let (id, column) = {
            let slot = leaf.leaf_meta.read().expect("leaf");
            let meta = slot.as_ref().expect("a template");
            (meta.template_id, meta.time_source.column().to_string())
        };
        let mut rows = Rows(Vec::new());
        db.stores
            .get(id)
            .expect("the leaf")
            .visit(&ScanOptions::default(), &[], &mut rows)
            .expect("scan");
        for row in rows.0 {
            let said = row.json("timestamp").or_else(|| row.json("time_us"));
            seen.push((
                said.expect("a time key").to_string(),
                effective_ts(&row, &column),
                row.contains_key(RECEIVED_AT_COLUMN),
            ));
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        [
            (
                r#""1679711330.570420890""#.to_string(),
                Some(1_679_711_330_570_420_890),
                false
            ),
            (
                r#""1679711331.5""#.into(),
                Some(1_679_711_331_500_000_000),
                false
            ),
            // The EDT line made the template, on `timestamp`; the IST line is one of its
            // rows and has no time of its own.
            (
                r#""2023-03-27 00:26:35.719 EDT""#.into(),
                Some(1_679_891_195_719_000_000),
                false
            ),
            (
                r#""2023-03-27 00:26:35.719 IST""#.into(),
                Some(ARRIVED),
                true
            ),
            (r#""not a time""#.into(), Some(ARRIVED), true),
            (
                "1732206349000167".into(),
                Some(1_732_206_349_000_167_000),
                false
            ),
        ]
    );
}

/// **A line replays as the build that logged it stored it.** The record carries the time
/// the row was filed under: where that is the time-column form's, this build logged it
/// and the row has its own time; where it is the arrival time, a build that did not read
/// the form logged it, and the row takes the arrival time again.
#[test]
fn a_logged_line_takes_the_time_its_record_says_it_was_filed_under() {
    let line = br#"{"timestamp":"1679711330.570420890","channel":"OPS"}"#;
    let own = 1_679_711_330_570_420_890;

    let by_this_build = row_from_log(line, "timestamp", ARRIVED, own).expect("a row");
    assert!(!by_this_build.contains_key(RECEIVED_AT_COLUMN));
    assert_eq!(effective_ts(&by_this_build, "timestamp"), Some(own));

    let by_an_older_build = row_from_log(line, "timestamp", ARRIVED, ARRIVED).expect("a row");
    assert_eq!(
        by_an_older_build.get(RECEIVED_AT_COLUMN),
        Some(&ColValue::Datetime(ARRIVED))
    );
    assert_eq!(effective_ts(&by_an_older_build, "timestamp"), Some(ARRIVED));

    // A date the ladder reads was filed under itself by every build; the record's
    // arrival time is not put on the row, as it never was.
    let dated = br#"{"timestamp":"2024-01-15T10:30:00Z","channel":"OPS"}"#;
    let at = 1_705_314_600_000_000_000;
    for time in [at, ARRIVED] {
        let row = row_from_log(dated, "timestamp", ARRIVED, time).expect("a row");
        assert!(!row.contains_key(RECEIVED_AT_COLUMN));
        assert_eq!(effective_ts(&row, "timestamp"), Some(at));
    }

    // No time in any form: the arrival time, as before.
    let none = br#"{"timestamp":"N/A","channel":"OPS"}"#;
    let row = row_from_log(none, "timestamp", ARRIVED, ARRIVED).expect("a row");
    assert_eq!(effective_ts(&row, "timestamp"), Some(ARRIVED));
    // A template on arrival time names the system column as its time column.
    let row = row_from_log(line, RECEIVED_AT_COLUMN, ARRIVED, ARRIVED).expect("a row");
    assert_eq!(effective_ts(&row, RECEIVED_AT_COLUMN), Some(ARRIVED));
}
