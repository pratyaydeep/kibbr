// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// A backend whose leaves fail to open until `shut` is cleared: what a `chmod 000`
/// directory looks like from the manager, and then the `chmod 755`.
struct Shut(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Store for Shut {
    fn open(&self, template_id: u64, t: &str) -> Result<Arc<dyn LeafStore>, StoreError> {
        if self.0.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(StoreError::Io {
                template_id,
                source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
            });
        }
        MemoryStore::new().open(template_id, t)
    }

    fn kind(&self) -> &'static str {
        "shut"
    }

    fn base_path(&self) -> Option<&Path> {
        None
    }
}

/// The store stays degraded through any number of ticks until the leaf opens: a tick that
/// cleared it sooner would let a retrying shipper's next body in, to store lines twice.
#[test]
fn a_row_whose_leaf_will_not_open_is_refused_and_the_store_stays_degraded() {
    let shut = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let stores = StoreManager::new(Box::new(Shut(std::sync::Arc::clone(&shut))));
    for n in 0..3 {
        let refused = stores.append(7, DEFAULT_TS_COLUMN, Row::new());
        assert!(
            matches!(refused, Err(StoreError::Io { template_id: 7, .. })),
            "row {n} was not refused: {refused:?}"
        );
    }
    assert_eq!(stores.rows_dropped(), 3);
    assert_eq!(stores.rows(), 0);
    assert!(stores.is_degraded());

    for _ in 0..3 {
        stores.flush_all(FlushKind::Tick);
        assert!(
            stores.is_degraded(),
            "a tick cleared degraded with the leaf still shut"
        );
    }

    shut.store(false, std::sync::atomic::Ordering::Relaxed);
    stores.flush_all(FlushKind::Tick);
    assert!(
        !stores.is_degraded(),
        "the leaf opened and the store is still degraded"
    );
    assert!(
        stores.lookup(7).is_some(),
        "the tick did not keep the leaf it opened"
    );
    // The count is never cleared: those rows are still gone.
    assert_eq!(stores.rows_dropped(), 3);
    stores
        .append(7, DEFAULT_TS_COLUMN, Row::new())
        .expect("the leaf is open now");
    assert_eq!(stores.rows(), 1);
}

#[test]
fn a_row_the_store_takes_is_not_counted_as_dropped() {
    let stores = StoreManager::memory();
    stores
        .append(7, DEFAULT_TS_COLUMN, Row::new())
        .expect("the memory store always opens");
    assert_eq!(stores.rows_dropped(), 0);
    assert_eq!(stores.rows(), 1);
}

/// Past twice the buffer limit the buffers are full - what `/ingest` refuses bodies on -
/// and a flush, closing the largest early, empties them again.
#[test]
fn the_buffers_are_full_past_twice_their_limit_until_a_flush() {
    let dir = std::env::temp_dir().join(format!("kibbr-full-{}", std::process::id()));
    let row = |n: i64| {
        let mut row = Row::new();
        row.insert(
            "ts".into(),
            ColValue::Datetime(1_705_314_600_000_000_000 + n),
        );
        row.insert("n".into(), ColValue::Int64(n));
        row
    };
    // What one such row holds, by the store's own count.
    let _ = std::fs::remove_dir_all(&dir);
    let probe = StoreManager::disk(dir.clone(), DiskOptions::default()).expect("open");
    probe.append(7, "ts", row(0)).expect("append");
    let each = probe.buffer_bytes().0;
    drop(probe);

    let _ = std::fs::remove_dir_all(&dir);
    let options = DiskOptions {
        max_buffer_bytes: 5 * each,
        ..DiskOptions::default()
    };
    let stores = StoreManager::disk(dir.clone(), options).expect("open");
    for n in 0..6 {
        stores.append(7, "ts", row(n)).expect("append");
    }
    assert!(stores.over_buffer_limit() && !stores.buffers_full());
    for n in 6..11 {
        stores.append(7, "ts", row(n)).expect("append");
    }
    assert!(stores.buffers_full(), "{:?}", stores.buffer_bytes());
    let done = stores.flush_all(FlushKind::Tick);
    assert_eq!((done.forced, done.rows), (1, 11));
    assert!(!stores.over_buffer_limit() && !stores.buffers_full());
    drop(stores);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The invariant at `disk::DiskLeaf::state`, under load: every snapshot holds every row
/// appended before it began, once and in place. `KIBBR_SNAPSHOT_SECONDS` sets how long (10).
#[test]
fn every_snapshot_sees_every_row_exactly_once_while_row_groups_close() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

    const ID: u64 = 7;
    const T0: i64 = 1_705_314_600_000_000_000;
    const WINDOW: u64 = 2_048;
    let seconds: u64 = std::env::var("KIBBR_SNAPSHOT_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let dir = std::env::temp_dir().join(format!("kibbr-snapshot-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    // Every flush closes whatever is buffered, in row groups of 64: a close is always
    // under way, and a snapshot meets rows in row groups, moved aside and buffered.
    let options = DiskOptions {
        row_groups: RowGroupLimits {
            rows: 64,
            bytes: 1 << 20,
            age: Duration::ZERO,
        },
        ..DiskOptions::default()
    };
    let stores = StoreManager::disk(dir.clone(), options).expect("open");

    let appended = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let closes = AtomicU64::new(0);
    let row_groups = AtomicU64::new(0);
    let flushes = AtomicU64::new(0);
    let snapshots = AtomicU64::new(0);
    let twice = AtomicU64::new(0);
    let missing = AtomicU64::new(0);
    let misplaced = AtomicU64::new(0);

    /// One snapshot's rows, `(n, position)`, against every row from `lo` up to at
    /// least `floor`: rows seen twice, rows missing, rows out of place.
    fn check(mut seen: Vec<(i64, u64)>, lo: u64, floor: u64) -> (u64, u64, u64) {
        seen.sort_unstable();
        let (mut twice, mut missing, mut misplaced) = (0, 0, 0);
        let mut next = lo;
        for (n, seq) in seen {
            let n = n as u64;
            if n < next {
                twice += 1;
                continue;
            }
            missing += n - next;
            // A single day, so a row's position is its place among all of them.
            if seq != n {
                misplaced += 1;
            }
            next = n + 1;
        }
        missing += floor.saturating_sub(next);
        (twice, missing, misplaced)
    }

    let row = |n: i64| {
        let mut row = Row::new();
        row.insert("ts".into(), ColValue::Datetime(T0 + n));
        row.insert("n".into(), ColValue::Int64(n));
        row
    };
    let snapshot = |since: Option<u64>| {
        let leaf = stores.lookup(ID)?;
        let opts = ScanOptions {
            ts_column: Some("ts".into()),
            since: since.map(|n| UNIX_EPOCH + Duration::from_nanos((T0 + n as i64) as u64)),
            ..ScanOptions::default()
        };
        let mut seen = Vec::new();
        leaf.visit(
            &opts,
            &[],
            &mut Rows(|row: &Row, seq: u64| {
                if let Some(ColValue::Int64(n)) = row.get("n") {
                    seen.push((*n, seq));
                }
            }),
        )
        .expect("visit");
        Some(seen)
    };

    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut n = 0i64;
            while !stop.load(Relaxed) {
                for _ in 0..32 {
                    stores.append(ID, "ts", row(n)).expect("append");
                    n += 1;
                    appended.store(n as u64, Release);
                }
                std::thread::sleep(Duration::from_micros(250));
            }
        });
        scope.spawn(|| {
            while !stop.load(Relaxed) {
                let done = stores.flush_all(FlushKind::Tick);
                assert_eq!(done.failed, 0, "a flush failed");
                closes.fetch_add(done.partitions as u64, Relaxed);
                row_groups.fetch_add(done.row_groups as u64, Relaxed);
                flushes.fetch_add(done.syncs as u64, Relaxed);
            }
        });
        for _ in 0..4 {
            scope.spawn(|| {
                while !stop.load(Relaxed) {
                    let floor = appended.load(Acquire);
                    let lo = floor.saturating_sub(WINDOW);
                    let Some(seen) = snapshot(Some(lo)) else {
                        continue;
                    };
                    let (t, m, p) = check(seen, lo, floor);
                    twice.fetch_add(t, Relaxed);
                    missing.fetch_add(m, Relaxed);
                    misplaced.fetch_add(p, Relaxed);
                    snapshots.fetch_add(1, Relaxed);
                }
            });
        }
        std::thread::sleep(Duration::from_secs(seconds));
        stop.store(true, Relaxed);
    });

    // The whole leaf, once nothing is moving; then written and sealed, and read back.
    let rows = appended.load(Acquire);
    let whole = check(snapshot(None).expect("the leaf"), 0, rows);
    let done = stores.flush_all(FlushKind::Final);
    assert_eq!(done.failed, 0, "the final flush failed");
    drop(stores);
    let reopened = StoreManager::disk(dir.clone(), options).expect("reopen");
    reopened.open_existing(ID, "ts").expect("leaf");
    let after = reopened.lookup(ID).map_or(0, |leaf| leaf.len());
    drop(reopened);
    let _ = std::fs::remove_dir_all(&dir);

    let (twice, missing, misplaced) = (
        twice.load(Relaxed),
        missing.load(Relaxed),
        misplaced.load(Relaxed),
    );
    eprintln!(
        "snapshots under closes: {seconds}s, {rows} rows appended, {} closes into {} row \
             groups under {} flushes, {} snapshots by 4 threads; {twice} rows seen twice, \
             {missing} missing, {misplaced} out of place; the whole leaf at the end {:?}; \
             {after} rows after a restart",
        closes.load(Relaxed),
        row_groups.load(Relaxed),
        flushes.load(Relaxed),
        snapshots.load(Relaxed),
        whole,
    );
    assert!(
        snapshots.load(Relaxed) > 0 && closes.load(Relaxed) > 0,
        "nothing ran"
    );
    assert_eq!(
        (twice, missing, misplaced),
        (0, 0, 0),
        "snapshots saw rows twice, missed them, or out of place"
    );
    assert_eq!(whole, (0, 0, 0), "the whole leaf is not every row once");
    assert_eq!(after, rows, "a restart does not hold every row");
}

// ── A term, and a row that was not rendered ────────────────────────────────────

/// 2024-01-15T10:30:00Z: the instant the pool spells in every form the ladder takes.
const INSTANT: i64 = 1_705_314_600_000_000_000;

fn spelling(text: &str) -> Option<serde_json::Value> {
    Some(serde_json::Value::String(text.to_string()))
}

/// A row whose one key, `v`, holds `value` beside what a line wrote for it.
fn holding(value: ColValue, written: Option<serde_json::Value>) -> Row {
    let mut row = Row::new();
    row.insert_written("v".into(), value, written);
    row
}

/// A `Datetime` under each kind of spelling `render_text` tells apart, and a value of every
/// other type with a spelling kept and with none.
fn spelled() -> Vec<Row> {
    let seconds = serde_json::Value::from(1_705_314_600_i64);
    let millis = serde_json::Value::from(1_705_314_600_000_i64);
    vec![
        holding(ColValue::Datetime(INSTANT), None),
        holding(
            ColValue::Datetime(INSTANT),
            spelling("2024-01-15T16:00:00+05:30"),
        ),
        holding(
            ColValue::Datetime(INSTANT),
            spelling(&crate::engine::fmt::rfc3339(INSTANT)),
        ),
        holding(ColValue::Datetime(INSTANT), Some(seconds)),
        holding(ColValue::Datetime(INSTANT), Some(millis)),
        holding(ColValue::Datetime(INSTANT), spelling("1705314600")),
        holding(ColValue::String("INFO".into()), None),
        holding(
            ColValue::String("[1,2]".into()),
            Some(serde_json::json!([1, 2])),
        ),
        holding(ColValue::Int64(45), None),
        holding(ColValue::Int64(45), spelling("0045")),
        holding(ColValue::Float64(200.0), None),
        holding(ColValue::Float64(200.0), spelling("2e2")),
        holding(ColValue::Bool(true), None),
        holding(ColValue::Bool(true), spelling("True")),
        holding(ColValue::Null, None),
    ]
}

/// Rendering clones the row to change one value; this is what lets a scan use the two
/// unrendered readings in its place.
#[test]
fn a_term_reads_a_stored_row_as_it_reads_the_rendered_one() {
    let mut rows = spelled();
    rows.extend(samples::rows());
    let mut terms = vec![Term::any("v"), Term::any("w"), Term::exact("w", "INFO")];
    terms.extend(samples::TEXTS.iter().map(|text| Term::exact("v", text)));

    let mut matched = [0u32; 2];
    for row in &rows {
        for text_columns in [Vec::new(), vec!["v".to_string()]] {
            let rendered = render_row(row, &text_columns);
            let seen = rendered.as_ref().unwrap_or(row);
            for term in &terms {
                let is_text = text_columns.iter().any(|column| column == term.key());
                let want = term.matches_row(seen);
                assert_eq!(
                    term.matches_in(row, is_text),
                    want,
                    "{term:?} put to {row:?}, text column: {is_text}"
                );
                let by_value = row.get(term.key()).is_some_and(|value| {
                    term.matches_cell(value, row.written(term.key()), is_text)
                });
                assert_eq!(
                    by_value, want,
                    "{term:?} put to the value of {row:?}, text column: {is_text}"
                );
                matched[usize::from(is_text)] += u32::from(want);
            }
        }
    }
    // Not agreement on nothing: both readings said yes, and often.
    assert!(matched[0] > 100 && matched[1] > 100, "{matched:?}");

    // The case the two readings exist for, by hand: in a `Datetime` column every spelling of
    // the instant finds the row; once the column is text, only the one it arrived in does.
    let zulu = Term::exact("v", "2024-01-15T10:30:00Z");
    let arrived = Term::exact("v", "2024-01-15T16:00:00+05:30");
    let digits = Term::exact("v", "1705314600");
    let offset = holding(
        ColValue::Datetime(INSTANT),
        spelling("2024-01-15T16:00:00+05:30"),
    );
    assert!(zulu.matches_in(&offset, false) && arrived.matches_in(&offset, false));
    assert!(!zulu.matches_in(&offset, true) && arrived.matches_in(&offset, true));
    let epoch = holding(
        ColValue::Datetime(INSTANT),
        Some(serde_json::Value::from(1_705_314_600_i64)),
    );
    assert!(zulu.matches_in(&epoch, false) && digits.matches_in(&epoch, false));
    assert!(!zulu.matches_in(&epoch, true) && digits.matches_in(&epoch, true));
    // A row without the key matches nothing, `*` included.
    assert!(!Term::any("w").matches_in(&epoch, false) && !Term::any("w").matches_row(&epoch));
}

/// **A term reads a stored row of an integer column as it reads the rendered one**: the
/// two unrendered readings a scan uses, held to `shown_row` and `matches_row`, over every
/// spelling of a value and every term.
#[test]
fn a_term_reads_a_stored_row_of_an_integer_column_as_it_reads_the_rendered_one() {
    let mut rows = spelled();
    rows.extend(samples::rows());
    // Epoch digits as a number and as a string, in seconds and in milliseconds, and
    // integers the ladder never read as a date though they spell one.
    for (ns, written) in [
        (INSTANT, Some(serde_json::Value::from(1_705_314_600_i64))),
        (INSTANT, spelling("1705314600")),
        (
            INSTANT,
            Some(serde_json::Value::from(1_705_314_600_000_i64)),
        ),
        (INSTANT, spelling("1705314600000")),
    ] {
        rows.push(holding(ColValue::Datetime(ns), written));
    }
    rows.push(holding(
        ColValue::Int64(1_705_314_600),
        spelling("+1705314600"),
    ));
    rows.push(holding(
        ColValue::Int64(1_705_314_600),
        spelling("01705314600"),
    ));
    rows.push(holding(ColValue::Int64(999_999_999), None));

    let mut terms = vec![Term::any("v"), Term::any("w")];
    terms.extend(samples::TEXTS.iter().map(|text| Term::exact("v", text)));
    for text in [
        "1705314600",
        "+1705314600",
        "1705314600000",
        "999999999",
        "01705314600",
    ] {
        terms.push(Term::exact("v", text));
    }

    let opts = ScanOptions {
        int_columns: vec!["v".to_string()],
        ..ScanOptions::default()
    };
    let (mut matched, mut rendered_rows) = (0u32, 0u32);
    for row in &rows {
        let rendered = shown_row(row, &opts);
        rendered_rows += u32::from(rendered.is_some());
        let seen = rendered.as_ref().unwrap_or(row);
        // What the line said is still what an answer prints.
        assert_eq!(seen.json("v"), row.json("v"), "{row:?}");
        for term in &terms {
            let want = term.matches_row(seen);
            assert_eq!(
                term.matches_in(row, Shown::Count),
                want,
                "{term:?} put to {row:?}, shown as {seen:?}"
            );
            let by_value = row.get(term.key()).is_some_and(|value| {
                term.matches_cell(value, row.written(term.key()), Shown::Count)
            });
            assert_eq!(by_value, want, "{term:?} put to the value of {row:?}");
            matched += u32::from(want);
        }
    }
    assert!(
        matched > 100 && rendered_rows >= 5,
        "{matched} {rendered_rows}"
    );

    // By hand. A date that arrived as ten digits is that integer: exactly, as a number.
    let digits = Term::exact("v", "1705314600");
    let millis = Term::exact("v", "1705314600000");
    let zulu = Term::exact("v", "2024-01-15T10:30:00Z");
    let epoch = holding(
        ColValue::Datetime(INSTANT),
        Some(serde_json::Value::from(1_705_314_600_i64)),
    );
    let shown = shown_row(&epoch, &opts).expect("rendered");
    assert_eq!(shown.get("v"), Some(&ColValue::Int64(1_705_314_600)));
    assert_eq!(shown.json("v"), Some(serde_json::json!(1_705_314_600_i64)));
    assert!(digits.matches_in(&epoch, Shown::Count) && digits.matches_row(&shown));
    // Neither another spelling of the instant nor another unit of it is that integer...
    assert!(!zulu.matches_in(&epoch, Shown::Count) && !zulu.matches_row(&shown));
    assert!(!millis.matches_in(&epoch, Shown::Count) && !millis.matches_row(&shown));
    // ...though each finds the row while the column is still a date.
    assert!(zulu.matches_in(&epoch, Shown::Stored) && millis.matches_in(&epoch, Shown::Stored));
    // The time such a row shows is still the instant it was stored as.
    assert_eq!(crate::engine::rows::effective_ts(&shown, "v"), Some(INSTANT));
    assert_eq!(crate::engine::rows::effective_ts(&epoch, "v"), Some(INSTANT));

    // A date that arrived as RFC 3339 is no integer: it is not rendered, and stays a date.
    let dated = holding(ColValue::Datetime(INSTANT), None);
    assert!(shown_row(&dated, &opts).is_none());
    assert!(zulu.matches_in(&dated, Shown::Count) && digits.matches_in(&dated, Shown::Count));

    // An integer the line signed was never a date, and the digits alone - a date to the
    // ladder - do not find it: as before the column could be one of counts.
    let signed = holding(ColValue::Int64(1_705_314_600), spelling("+1705314600"));
    assert!(shown_row(&signed, &opts).is_none());
    assert!(!digits.matches_in(&signed, Shown::Count) && !digits.matches_row(&signed));
    assert!(Term::exact("v", "+1705314600").matches_row(&signed));
}

/// **The count a date is shown as is the digits the line wrote, exactly**: read from what
/// the line said, never worked back from the instant, for every edge of the epoch window.
#[test]
fn a_date_shown_as_a_count_is_the_digits_it_arrived_as() {
    for text in [
        "1000000000",
        "1220305536",
        "1679711330",
        "4102444799",
        "1000000000000",
        "1679711330570",
        "4102444799999",
    ] {
        let count: i64 = text.parse().expect("digits");
        for quoted in [false, true] {
            let line = if quoted {
                format!(r#"{{"v":"{text}"}}"#)
            } else {
                format!(r#"{{"v":{text}}}"#)
            };
            let row = crate::engine::format::samples::row(&line);
            assert!(
                matches!(row.get("v"), Some(ColValue::Datetime(_))),
                "{line}"
            );
            assert_eq!(
                render_count(&row["v"], row.written("v")),
                Some(count),
                "{line}"
            );
            let shown = shown_row(
                &row,
                &ScanOptions {
                    int_columns: vec!["v".into()],
                    ..ScanOptions::default()
                },
            )
            .expect("rendered");
            assert_eq!(shown.get("v"), Some(&ColValue::Int64(count)));
            // And the answer prints what the line held, a number or a string.
            let said: serde_json::Value = serde_json::from_str(&line).expect("json");
            assert_eq!(shown.json("v").as_ref(), Some(&said["v"]), "{line}");
            assert_eq!(shown_count(&shown["v"], shown.written("v")), Some(count));
        }
    }
    // Not a count: RFC 3339 text, and anything that is not a `Datetime`.
    let dated = crate::engine::format::samples::row(r#"{"v":"2024-01-15T16:00:00+05:30"}"#);
    assert_eq!(render_count(&dated["v"], dated.written("v")), None);
    let plain = crate::engine::format::samples::row(r#"{"v":42}"#);
    assert_eq!(render_count(&plain["v"], plain.written("v")), None);
    assert_eq!(shown_count(&plain["v"], plain.written("v")), None);
}
