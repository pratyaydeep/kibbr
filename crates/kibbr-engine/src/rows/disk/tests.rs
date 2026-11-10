// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::format::Source;
use crate::rows::{in_window, render_row, Counted, Rows, DEFAULT_TS_COLUMN};
use crate::model::types::ColValue;
use std::time::Duration;

pub(super) struct Scratch(pub(super) PathBuf);

impl Scratch {
    pub(super) fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "kibbr-disk-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(super) const T0: i64 = 1_705_314_600_000_000_000;

/// A row group closes on every flush: nothing waits for a threshold.
pub(super) fn eager() -> DiskOptions {
    DiskOptions {
        row_groups: RowGroupLimits {
            age: Duration::ZERO,
            ..RowGroupLimits::default()
        },
        max_open_files: 8,
        ..DiskOptions::default()
    }
}

pub(super) fn store(scratch: &Scratch) -> DiskStore {
    store_with(scratch, eager())
}

pub(super) fn store_with(scratch: &Scratch, options: DiskOptions) -> DiskStore {
    DiskStore::open(scratch.0.clone(), options).expect("open")
}

pub(super) fn row(ts: i64, n: i64) -> Row {
    let mut row = Row::new();
    row.insert("ts".into(), ColValue::Datetime(ts));
    row.insert_written(
        "n".into(),
        ColValue::Int64(n),
        Some(serde_json::Value::String(format!("{n:04}"))),
    );
    row
}

fn all(leaf: &dyn LeafStore) -> Vec<Row> {
    leaf.scan(
        &ScanOptions {
            ts_column: Some("ts".into()),
            ..ScanOptions::default()
        },
        &[],
    )
    .expect("scan")
    .rows
}

/// Each row's `n`, sorted: what a leaf holds, however it is split between the disk and
/// memory.
pub(super) fn ns(leaf: &dyn LeafStore) -> Vec<i64> {
    let mut out: Vec<i64> = all(leaf)
        .iter()
        .map(|r| match r.get("n") {
            Some(ColValue::Int64(n)) => *n,
            other => panic!("n is {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

#[test]
fn rows_come_back_as_written_before_and_after_a_restart() {
    let scratch = Scratch::new("restart");
    let want: Vec<Row> = (0..5).map(|n| row(T0 + n * 1_000_000_000, n)).collect();
    {
        let s = store_with(&scratch, DiskOptions::default());
        let leaf = s.open(7, "ts").expect("leaf");
        for r in &want {
            leaf.append(r.clone());
        }
        assert_eq!(all(leaf.as_ref()).len(), 5, "buffered rows are visible");
        let done = s.flush(FlushKind::Final);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        assert_eq!((done.rows, done.sealed), (5, 1));
        assert_eq!(all(leaf.as_ref()), want);
    }
    let s = store(&scratch);
    assert_eq!(s.claimed(), vec![7]);
    let leaf = s.open(7, "ts").expect("leaf");
    assert_eq!(all(leaf.as_ref()), want);
    assert_eq!(data_files(&scratch.0).len(), 1, "one file for one day");
}

#[test]
fn a_file_the_manifest_does_not_name_is_deleted_and_so_is_staging() {
    let scratch = Scratch::new("unnamed");
    let day = scratch.0.join(DATA_DIR).join("2024-01-15");
    fs::create_dir_all(&day).expect("mkdir");
    fs::write(day.join("seg_0_000001.col"), b"KBBR\x03\0\0\0").expect("write");
    fs::create_dir_all(scratch.0.join(STAGING_DIR).join("abc")).expect("mkdir");
    let _ = store(&scratch);
    assert!(!day.exists(), "an unnamed file survived startup");
    assert!(
        !scratch.0.join(STAGING_DIR).exists(),
        "staging survived startup"
    );
}

#[test]
fn a_leaf_directory_is_refused_by_path() {
    let scratch = Scratch::new("legacy");
    let leaf = scratch.0.join("leaf_101");
    fs::create_dir_all(&leaf).expect("mkdir");
    match DiskStore::open(scratch.0.clone(), DiskOptions::default()) {
        Err(StoreError::LegacyLayout { path }) => assert_eq!(path, leaf),
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("opened a directory holding the per-template layout"),
    }
}

/// A named file whose number is the last there is leaves none for the day's next file:
/// the start is refused, naming the file.
#[test]
fn a_file_number_with_none_after_it_is_refused_by_name() {
    let scratch = Scratch::new("last-number");
    let rel = format!("2024-01-15/{}", file_name(u32::MAX));
    {
        let (mut manifest, _) =
            Manifest::open(&manifest::path_in(&scratch.0), &mut |_| {}).expect("manifest");
        manifest
            .append(&[Record::Add {
                files: vec![FileRecord::open(rel.clone(), &[7; 16])],
                source: None,
            }])
            .expect("append");
    }
    match DiskStore::open(scratch.0.clone(), DiskOptions::default()) {
        Err(e) => {
            let why = e.to_string();
            assert!(why.contains(&rel) && why.contains("whose number"), "{why}");
        }
        Ok(_) => panic!("opened a store whose manifest names `{rel}`"),
    }
}

/// A crash after row groups were flushed and before the seal: the next start recovers
/// the file, records the seal, and every flushed row is there.
#[test]
fn an_unsealed_file_is_recovered_and_its_rows_kept() {
    let scratch = Scratch::new("crash");
    {
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..3 {
            leaf.append(row(T0 + n, n));
        }
        s.flush(FlushKind::Tick);
        // No seal: the process dies here.
    }
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    assert_eq!(all(leaf.as_ref()).len(), 3);
    drop(leaf);
    drop(s);
    // And that seal is durable: the next start opens it through its footer.
    let (_, records) =
        Manifest::open(&manifest::path_in(&scratch.0), &mut |_| {}).expect("manifest");
    let live = manifest::live(&records);
    assert!(live.values().all(|f| f.sealed), "{live:?}");
}

/// The file being written has no footer, and a query still reads its row groups - from
/// disk, through the entries its writer holds, and not from the buffer they left.
#[test]
fn the_open_file_is_read_through_its_writers_index() {
    let scratch = Scratch::new("open-index");
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..10 {
        leaf.append(row(T0 + n, n));
    }
    s.flush(FlushKind::Tick);
    assert_eq!(leaf.buffered(), 0, "the rows are still buffered");
    let path = data_files(&scratch.0)
        .into_iter()
        .next()
        .expect("a data file");
    assert!(
        file::open_sealed(&path).expect("readable").is_none(),
        "the open file has a footer"
    );
    let scan = leaf
        .visit(
            &ScanOptions::default(),
            &[],
            &mut Rows(|_: &Row, _: u64| {}),
        )
        .expect("scan");
    assert_eq!((scan.matched, scan.row_groups_read), (10, 1));
    assert_eq!(ns(leaf.as_ref()), (0..10).collect::<Vec<_>>());
}

/// Two templates over two days: four row groups, two files, and one flush for each
/// file - not one per row group.
#[test]
fn every_row_group_that_is_due_goes_out_before_one_flush_per_file() {
    let scratch = Scratch::new("one-flush");
    let s = store(&scratch);
    let a = s.open(7, "ts").expect("leaf");
    let b = s.open(8, "ts").expect("leaf");
    for n in 0..4 {
        a.append(row(T0 + n, n));
        b.append(row(T0 + DAY_NS + n, n));
        a.append(row(T0 + DAY_NS + n, 10 + n));
        b.append(row(T0 + n, 10 + n));
    }
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!(
        (done.rows, done.row_groups, done.syncs, done.sealed),
        (16, 4, 2, 0)
    );
    assert_eq!(data_files(&scratch.0).len(), 2);
    let done = s.flush(FlushKind::Final);
    assert_eq!((done.rows, done.syncs, done.sealed), (0, 0, 2));
}

/// The UTC day each data file's directory names, in path order.
fn file_days(scratch: &Scratch) -> Vec<String> {
    data_files(&scratch.0)
        .iter()
        .map(|p| {
            let day = p.parent().and_then(|d| d.file_name()).unwrap_or_default();
            day.to_string_lossy().into_owned()
        })
        .collect()
}

/// Every row group of every sealed data file lies in the day its directory names.
fn every_row_group_in_its_own_day(scratch: &Scratch) {
    for path in data_files(&scratch.0) {
        let dir = path
            .parent()
            .and_then(|d| d.file_name())
            .unwrap_or_default();
        let day = day_of_name(&dir.to_string_lossy()).expect("a day directory");
        let index = file::open_sealed(&path).expect("readable").expect("sealed");
        for e in &index.entries {
            assert_eq!(
                (day_of(e.min_time), day_of(e.max_time)),
                (day, day),
                "{} holds a row group of another day",
                path.display()
            );
        }
    }
}

/// Past the cap on open days the least recently written file is sealed and a late row
/// opens its own day's: no row group is ever in another day's file.
#[test]
fn past_the_open_day_cap_a_late_row_opens_its_own_days_file() {
    let scratch = Scratch::new("late");
    let options = DiskOptions {
        max_open_files: 2,
        ..eager()
    };
    let first = day_of(T0);
    {
        let s = store_with(&scratch, options);
        let leaf = s.open(7, "ts").expect("leaf");
        for d in 0..3 {
            leaf.append(row(T0 + d * DAY_NS, d));
        }
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        // The first day's file is flushed and sealed to make room for the third's.
        assert_eq!((done.syncs, done.sealed), (3, 1));
        assert_eq!(s.open_files(), (2, 2), "the cap was passed");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
        s.flush(FlushKind::Final);
    }
    assert_eq!(
        file_days(&scratch),
        vec![day_name(first), day_name(first + 1), day_name(first + 2)],
        "a row went into another day's file"
    );
    every_row_group_in_its_own_day(&scratch);
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
}

/// Rows of one template and one timestamp keep their order across a restart when their
/// day's file was sealed for the cap in between, or a cursor would repeat one and skip one.
#[test]
fn tied_rows_keep_their_order_across_a_restart_past_the_open_day_cap() {
    let scratch = Scratch::new("tied");
    let options = DiskOptions {
        max_open_files: 1,
        segment_max_bytes: 700,
        ..eager()
    };
    let tied = T0 + DAY_NS;
    let positions = |leaf: &dyn LeafStore| {
        let mut out = Vec::new();
        leaf.visit(
            &ScanOptions::default(),
            &[],
            &mut Rows(|r: &Row, seq: u64| out.push((r.get("n").cloned(), seq))),
        )
        .expect("scan");
        out.sort_by_key(|(_, seq)| *seq);
        out.into_iter().map(|(n, _)| n).collect::<Vec<_>>()
    };
    let before = {
        let s = store_with(&scratch, options);
        let a = s.open(7, "ts").expect("leaf");
        let b = s.open(8, "ts").expect("leaf");
        let c = s.open(9, "ts").expect("leaf");
        a.append(row(T0, 0));
        s.flush(FlushKind::Tick);
        b.append(row(tied, 1));
        s.flush(FlushKind::Tick);
        // Wide enough to take its file past the size cap.
        let mut wide = row(T0 + 2 * DAY_NS, 0);
        wide.insert("pad".into(), ColValue::String("f".repeat(600)));
        c.append(wide);
        s.flush(FlushKind::Tick);
        b.append(row(tied, 2));
        let done = s.flush(FlushKind::Tick);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
        let before = positions(b.as_ref());
        s.flush(FlushKind::Final);
        before
    };
    assert_eq!(
        before,
        vec![Some(ColValue::Int64(1)), Some(ColValue::Int64(2))]
    );
    let s = store_with(&scratch, options);
    let b = s.open(8, "ts").expect("leaf");
    assert_eq!(
        positions(b.as_ref()),
        before,
        "a restart reordered tied rows"
    );
    every_row_group_in_its_own_day(&scratch);
}

/// A day's file past the size cap is sealed after the flush that took it there, and the
/// next row for the day opens the next number.
#[test]
fn a_file_past_the_size_cap_is_sealed_and_the_next_number_opened() {
    let scratch = Scratch::new("size-cap");
    let options = DiskOptions {
        segment_max_bytes: 1,
        ..eager()
    };
    let s = store_with(&scratch, options);
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..3 {
        leaf.append(row(T0 + n, n));
        let done = s.flush(FlushKind::Tick);
        assert_eq!((done.syncs, done.sealed), (1, 1));
    }
    let files: Vec<PathBuf> = data_files(&scratch.0).into_iter().collect();
    assert_eq!(files.len(), 3);
    for path in &files {
        assert!(file::open_sealed(path).expect("readable").is_some());
    }
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
}

/// Past the memory limit the largest buffer closes first, though no threshold has
/// fired, and the small one waits for its own.
#[test]
fn past_the_buffer_limit_the_largest_buffer_closes_first() {
    let scratch = Scratch::new("limit");
    // What one row holds, by the store's own count.
    let each = {
        let probe = store_with(&scratch, DiskOptions::default());
        probe.open(9, "ts").expect("leaf").append(row(T0, 0));
        probe.buffer_bytes().0
    };
    let options = DiskOptions {
        max_buffer_bytes: 20 * each,
        ..DiskOptions::default()
    };
    let s = store_with(&scratch, options);
    let big = s.open(7, "ts").expect("leaf");
    let small = s.open(8, "ts").expect("leaf");
    for n in 0..40 {
        big.append(row(T0 + n, n));
    }
    small.append(row(T0, 0));
    assert_eq!(s.buffer_bytes().0, 41 * each);
    let done = s.flush(FlushKind::Tick);
    assert_eq!((done.forced, done.rows), (1, 40));
    assert_eq!((big.buffered(), small.buffered()), (0, 1));
    assert_eq!(s.buffer_bytes().0, each);
}

/// A flush that cannot write puts the rows back where they were: still buffered, still
/// answered once each, and written by the first flush that can.
#[test]
#[cfg(unix)]
fn rows_a_flush_cannot_write_go_back_to_their_buffer() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("restore");
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..5 {
        leaf.append(row(T0 + n, n));
    }
    let data = scratch.0.join(DATA_DIR);
    fs::set_permissions(&data, fs::Permissions::from_mode(0o555)).expect("chmod");
    let done = s.flush(FlushKind::Tick);
    fs::set_permissions(&data, fs::Permissions::from_mode(0o755)).expect("chmod");
    assert_eq!(done.failed.len(), 1, "the flush did not fail");
    assert_eq!(leaf.buffered(), 5);
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4]);

    leaf.append(row(T0 + 5, 5));
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!((done.rows, leaf.buffered()), (6, 0));
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 5]);
    // In their order: the restored rows went back ahead of the one that came after.
    let mut seqs = Vec::new();
    leaf.visit(
        &ScanOptions::default(),
        &[],
        &mut Rows(|r: &Row, seq: u64| seqs.push((r.get("n").cloned(), seq))),
    )
    .expect("scan");
    for (n, seq) in seqs {
        assert_eq!(n, Some(ColValue::Int64(seq as i64)));
    }
}

/// Row groups of `rows` rows and `bytes` payload, and a timeout that never comes.
pub(super) fn thresholds(rows: usize, bytes: usize) -> DiskOptions {
    DiskOptions {
        row_groups: RowGroupLimits {
            rows,
            bytes,
            age: Duration::from_secs(3600),
        },
        max_open_files: 8,
        ..DiskOptions::default()
    }
}

/// A tick closes the full row groups and leaves the short rest buffered, by either
/// threshold; the final flush closes the rest.
#[test]
fn a_tick_closes_the_full_row_groups_and_leaves_the_rest() {
    let scratch = Scratch::new("tick-rows");
    let s = store_with(&scratch, thresholds(4, usize::MAX));
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..10 {
        leaf.append(row(T0 + n, n));
    }
    let done = s.flush(FlushKind::Tick);
    assert_eq!((done.rows, done.row_groups, leaf.buffered()), (8, 2, 2));
    assert_eq!(s.flush(FlushKind::Tick).rows, 0, "the short rest closed");
    for n in 10..12 {
        leaf.append(row(T0 + n, n));
    }
    let done = s.flush(FlushKind::Tick);
    assert_eq!((done.rows, done.row_groups, leaf.buffered()), (4, 1, 0));
    leaf.append(row(T0 + 12, 12));
    let done = s.flush(FlushKind::Final);
    assert_eq!((done.rows, done.row_groups, leaf.buffered()), (1, 1, 0));
    assert_eq!(ns(leaf.as_ref()), (0..13).collect::<Vec<_>>());

    // By payload: three rows' worth fills a row group.
    let scratch = Scratch::new("tick-bytes");
    let each = {
        let probe = store_with(&scratch, DiskOptions::default());
        let leaf = probe.open(9, "ts").expect("leaf");
        leaf.append(row(T0, 0));
        leaf.approx_bytes() as usize
    };
    let s = store_with(&scratch, thresholds(usize::MAX, 3 * each));
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..8 {
        leaf.append(row(T0 + n, n));
    }
    let done = s.flush(FlushKind::Tick);
    assert_eq!((done.rows, done.row_groups, leaf.buffered()), (6, 2, 2));
    assert_eq!(ns(leaf.as_ref()), (0..8).collect::<Vec<_>>());
}

/// Rows a flush could not write go back ahead of the rest and are cut into row groups
/// again from the head: the next tick closes the full ones and no more.
#[test]
#[cfg(unix)]
fn rows_put_back_are_cut_into_row_groups_again() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("tick-restore");
    let s = store_with(&scratch, thresholds(4, usize::MAX));
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..10 {
        leaf.append(row(T0 + n, n));
    }
    let data = scratch.0.join(DATA_DIR);
    fs::set_permissions(&data, fs::Permissions::from_mode(0o555)).expect("chmod");
    let done = s.flush(FlushKind::Tick);
    fs::set_permissions(&data, fs::Permissions::from_mode(0o755)).expect("chmod");
    assert_eq!(done.failed.len(), 1, "the flush did not fail");
    assert_eq!(leaf.buffered(), 10);

    leaf.append(row(T0 + 10, 10));
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!((done.rows, done.row_groups, leaf.buffered()), (8, 2, 3));
    leaf.append(row(T0 + 11, 11));
    let done = s.flush(FlushKind::Tick);
    assert_eq!((done.rows, done.row_groups, leaf.buffered()), (4, 1, 0));
    assert_eq!(ns(leaf.as_ref()), (0..12).collect::<Vec<_>>());
}

/// A load's file for template `id`, rows `ns` on T0's day: staged under the load's own
/// name, sealed and flagged finished, as the loader leaves one.
pub(super) fn staged(dir: &Path, name: &str, id: u64, ns: std::ops::Range<i64>) -> StagedFile {
    staged_at(dir, name, id, T0, ns)
}

/// `staged`, with rows at `base + n` and on `base`'s day.
fn staged_at(
    dir: &Path,
    name: &str,
    id: u64,
    base: i64,
    ns: std::ops::Range<i64>,
) -> StagedFile {
    let path = dir.join(name);
    let mut writer = Writer::create(&path).expect("create");
    writer.finish();
    let rows: Vec<Row> = ns.clone().map(|n| row(base + n, n)).collect();
    let refs: Vec<&Row> = rows.iter().collect();
    let times: Vec<i64> = ns.clone().map(|n| base + n).collect();
    let columns = schema_of(refs.iter().copied());
    let encoded = build::encode(
        id,
        &columns,
        &refs,
        &times,
        Source::Load {
            stream: 9,
            first: ns.start as u64 + 1,
            last: ns.end as u64,
        },
        Level::Finished,
    )
    .expect("encode");
    let schema_id = writer.schema_id(&columns).expect("schema");
    writer
        .append(id, &encoded.frame(schema_id))
        .expect("append");
    StagedFile {
        day: day_of(base),
        path,
        index: writer.seal().expect("seal"),
    }
}

/// A commit lands in an open leaf at once, behind its live rows; a second commit of the
/// same load is refused by its id; and it is all there after a restart.
#[test]
fn a_committed_load_is_read_at_once_and_after_a_restart() {
    let scratch = Scratch::new("commit");
    {
        let s = store(&scratch);
        let leaf = s.open(7, "ts").expect("leaf");
        leaf.append(row(T0 + 100, 100));
        let dir = s.stage(9).expect("stage");
        let file = staged(&dir, "a.col", 7, 0..5);
        assert!(!s.was_loaded(9));
        let done = s
            .commit_load(LoadCommit {
                load_id: 9,
                files: vec![file],
                replace_day: None,
            })
            .expect("commit");
        assert_eq!(
            done.added,
            vec![format!("{}/seg_0_000001.col", day_name(day_of(T0)))]
        );
        assert!(!dir.exists(), "staging outlived its commit");
        assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 100]);
        assert!(s.was_loaded(9));

        let again = s.stage(9).expect("stage");
        let refused = s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![staged(&again, "a.col", 7, 0..5)],
            replace_day: None,
        });
        assert!(matches!(
            refused,
            Err(StoreError::LoadRepeated { load_id: 9 })
        ));
        s.unstage(9);
        s.flush(FlushKind::Final);
    }
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 100]);
    let (_, records) =
        Manifest::open(&manifest::path_in(&scratch.0), &mut |_| {}).expect("manifest");
    let finished = manifest::live(&records)
        .into_values()
        .filter(|f| f.finished)
        .count();
    assert_eq!(finished, 1, "the load's file is not flagged finished");
}

/// A template no leaf is open for takes the load's row groups when it opens.
#[test]
fn a_load_for_a_template_with_no_leaf_waits_for_the_leaf() {
    let scratch = Scratch::new("no-leaf");
    let s = store(&scratch);
    let dir = s.stage(9).expect("stage");
    s.commit_load(LoadCommit {
        load_id: 9,
        files: vec![staged(&dir, "a.col", 8, 0..3)],
        replace_day: None,
    })
    .expect("commit");
    let leaf = s.open(8, "ts").expect("leaf");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2]);
}

/// The record is the commit: a stop after it and before the move leaves the files in
/// staging, and the next start moves them rather than deleting them.
#[test]
fn a_load_stopped_between_its_record_and_its_move_is_moved_at_the_next_start() {
    let scratch = Scratch::new("finish-move");
    let rel = format!("{}/seg_0_000001.col", day_name(day_of(T0)));
    {
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        let file = staged(&dir, "a.col", 7, 0..4);
        // `commit_load`'s record, and no move: the process stops here.
        let mut files = lock(&s.files);
        files
            .record(vec![Record::Add {
                files: vec![FileRecord::sealed(rel.clone(), &file.index)],
                source: Some(manifest::load_source(9)),
            }])
            .expect("record");
    }
    assert!(!scratch.0.join(DATA_DIR).join(&rel).exists());
    let s = store(&scratch);
    assert!(scratch.0.join(DATA_DIR).join(&rel).exists(), "not moved");
    assert!(!scratch.0.join(STAGING_DIR).exists(), "staging survived");
    let leaf = s.open(7, "ts").expect("leaf");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3]);
    assert!(s.was_loaded(9));
}

/// A committed file found neither in `data/` nor in staging stops the start with
/// staging untouched - never after the only copy of a load's rows is deleted.
#[test]
fn a_committed_load_missing_from_staging_stops_the_start_and_keeps_staging() {
    let scratch = Scratch::new("fail-closed");
    let rel = format!("{}/seg_0_000001.col", day_name(day_of(T0)));
    {
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        let file = staged(&dir, "a.col", 7, 0..4);
        let other = staged(&dir, "b.col", 7, 4..6);
        let mut files = lock(&s.files);
        files
            .record(vec![Record::Add {
                files: vec![
                    FileRecord::sealed(rel.clone(), &file.index),
                    FileRecord::sealed(
                        format!("{}/seg_0_000002.col", day_name(day_of(T0))),
                        &other.index,
                    ),
                ],
                source: Some(manifest::load_source(9)),
            }])
            .expect("record");
        // One of the two is lost - a bad sector, a hand that tidied up.
        fs::remove_file(&other.path).expect("remove");
    }
    assert!(DiskStore::open(scratch.0.clone(), eager()).is_err());
    assert!(
        files_under(&scratch.0.join(STAGING_DIR))
            .expect("staging")
            .iter()
            .any(|p| p.ends_with("a.col")),
        "the start deleted the staged copy it could still have moved"
    );
}

/// A committed load whose files are all in place is not held up by what else sits in
/// its staging directory: a later run of the same inputs cut short at a file's header.
#[test]
fn a_stray_file_in_a_moved_loads_staging_does_not_stop_the_start() {
    let scratch = Scratch::new("stray");
    {
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![staged(&dir, "a.col", 7, 0..3)],
            replace_day: None,
        })
        .expect("commit");
        // The same inputs again, with --replace-day, stopped inside a file's header.
        let again = scratch.0.join(STAGING_DIR).join(format!("{:016x}", 9));
        fs::create_dir_all(again.join("2024-01-15")).expect("mkdir");
        fs::write(again.join("2024-01-15").join("000001.col"), b"").expect("write");
    }
    let s = store(&scratch);
    assert_eq!(ns(s.open(7, "ts").expect("leaf").as_ref()), vec![0, 1, 2]);
    assert!(!scratch.0.join(STAGING_DIR).exists(), "staging survived");
}

/// While a committed load's files are still in staging, no load stages - the same load
/// again would stage into the same directory, over the only copy.
#[test]
fn nothing_stages_over_a_committed_load_left_in_staging() {
    let scratch = Scratch::new("move-pending");
    let s = store(&scratch);
    let dir = s.stage(9).expect("stage");
    let file = staged(&dir, "a.col", 7, 0..4);
    // The move fails: something that is not a directory sits where the day's is.
    let day_dir = scratch.0.join(DATA_DIR).join(day_name(day_of(T0)));
    fs::write(&day_dir, b"in the way").expect("block");
    let failed = s.commit_load(LoadCommit {
        load_id: 9,
        files: vec![file],
        replace_day: None,
    });
    assert!(matches!(
        failed,
        Err(StoreError::NotMoved { load_id: 9, .. })
    ));
    fs::remove_file(&day_dir).expect("unblock");
    assert!(matches!(
        s.stage(9),
        Err(StoreError::MovePending { load_id: 9 })
    ));
    assert!(matches!(
        s.stage(10),
        Err(StoreError::MovePending { load_id: 9 })
    ));
    assert!(dir.join("a.col").exists(), "the only copy went");
    drop(s);
    // The next start finishes the move.
    let s = store(&scratch);
    assert_eq!(
        ns(s.open(7, "ts").expect("leaf").as_ref()),
        vec![0, 1, 2, 3]
    );
}

/// Before the record, a load is only its staging directory, which the next start
/// deletes: no row of it, and no file.
#[test]
fn a_load_stopped_before_its_record_leaves_nothing() {
    let scratch = Scratch::new("undo");
    {
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        staged(&dir, "a.col", 7, 0..4);
    }
    let s = store(&scratch);
    assert!(!scratch.0.join(STAGING_DIR).exists(), "staging survived");
    assert!(data_files(&scratch.0).is_empty());
    assert!(!s.was_loaded(9));
    assert!(ns(s.open(7, "ts").expect("leaf").as_ref()).is_empty());
}

/// Replace-a-day writes the day's rows still in memory, retires every file of the day in
/// the load's record, and deletes one only once the last query holding it lets go.
#[test]
fn replace_a_day_retires_the_day_and_deletes_its_files_after_the_last_reader() {
    let scratch = Scratch::new("replace");
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..3 {
        leaf.append(row(T0 + n, 50 + n));
    }
    s.flush(FlushKind::Tick);
    leaf.append(row(T0 + 9, 59));
    let old: Vec<PathBuf> = data_files(&scratch.0).into_iter().collect();
    assert_eq!(old.len(), 1);
    // A query's snapshot of the day, taken before the replace.
    let held = {
        let leaves = s.leaves.read().expect("leaves");
        let state = leaves[&7].state();
        Arc::clone(&state.days[&day_of(T0)].stored)
    };
    let dir = s.stage(9).expect("stage");
    let done = s
        .commit_load(LoadCommit {
            load_id: 9,
            files: vec![staged(&dir, "a.col", 7, 0..5)],
            replace_day: Some(day_of(T0)),
        })
        .expect("replace");
    assert_eq!(done.replaced.len(), 1, "{done:?}");
    assert_eq!(
        ns(leaf.as_ref()),
        vec![0, 1, 2, 3, 4],
        "the day holds only the load"
    );
    assert!(old[0].exists(), "deleted under a reader");
    drop(held);
    assert!(!old[0].exists(), "a retired file outlived its last reader");
    drop(leaf);
    drop(s);
    let s = store(&scratch);
    assert_eq!(
        ns(s.open(7, "ts").expect("leaf").as_ref()),
        vec![0, 1, 2, 3, 4]
    );
}

/// A day whose file was sealed for the cap and opened again is replaced like any other:
/// every file of it is in its own directory, so retiring that retires the day alone.
#[test]
fn replace_a_day_after_the_open_day_cap_retires_that_day_and_no_other() {
    let scratch = Scratch::new("replace-capped");
    let options = DiskOptions {
        max_open_files: 1,
        ..eager()
    };
    let s = store_with(&scratch, options);
    let leaf = s.open(7, "ts").expect("leaf");
    let late = day_of(T0 + DAY_NS);
    leaf.append(row(T0, 0));
    leaf.append(row(T0 + DAY_NS, 1));
    s.flush(FlushKind::Tick);
    leaf.append(row(T0 + 1, 2));
    s.flush(FlushKind::Tick);
    leaf.append(row(T0 + DAY_NS + 1, 3));
    s.flush(FlushKind::Tick);
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3]);
    let dir = s.stage(9).expect("stage");
    let file = staged_at(&dir, "a.col", 7, T0 + DAY_NS, 10..12);
    let done = s
        .commit_load(LoadCommit {
            load_id: 9,
            files: vec![file],
            replace_day: Some(late),
        })
        .expect("a day that passed through the cap is replaced");
    assert!(
        done.replaced
            .iter()
            .all(|rel| rel.starts_with(&format!("{}/", day_name(late)))),
        "{done:?}"
    );
    assert_eq!(
        ns(leaf.as_ref()),
        vec![0, 2, 10, 11],
        "the other day lost rows, or the replaced day kept some"
    );
    s.flush(FlushKind::Final);
    every_row_group_in_its_own_day(&scratch);
}

/// Each row's `n` from one template of a snapshot, sorted.
pub(super) fn snapshot_ns(snap: &mut dyn Snapshot, id: u64) -> Vec<i64> {
    let mut out = Vec::new();
    let opts = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    snap.visit(
        id,
        &opts,
        &[],
        &mut Rows(|r: &Row, _: u64| match r.get("n") {
            Some(ColValue::Int64(n)) => out.push(*n),
            other => panic!("n is {other:?}"),
        }),
    )
    .expect("a leaf")
    .expect("scan");
    out.sort_unstable();
    out
}

/// A load publishes every template in one step and a snapshot is taken in one step, so
/// no query sees one template's share of a load and not another's.
#[test]
fn a_snapshot_sees_a_load_in_every_template_or_in_none() {
    let scratch = Scratch::new("snapshot");
    let s = store(&scratch);
    let a = s.open(7, "ts").expect("leaf");
    let b = s.open(8, "ts").expect("leaf");
    a.append(row(T0 + 100, 100));
    b.append(row(T0 + 200, 200));
    let leaves =
        || -> Vec<(u64, Arc<dyn LeafStore>)> { vec![(7, Arc::clone(&a)), (8, Arc::clone(&b))] };
    let opts = ScanOptions::default();
    let mut before = s.snapshot(leaves(), &opts);
    let generation = s.generation();
    assert_eq!(before.generation(), generation);

    let dir = s.stage(9).expect("stage");
    s.commit_load(LoadCommit {
        load_id: 9,
        files: vec![
            staged(&dir, "a.col", 7, 0..3),
            staged(&dir, "b.col", 8, 10..12),
        ],
        replace_day: None,
    })
    .expect("commit");
    assert_eq!(s.generation(), generation + 1, "a publish is not counted");

    assert_eq!(snapshot_ns(before.as_mut(), 7), vec![100]);
    assert_eq!(snapshot_ns(before.as_mut(), 8), vec![200]);
    let mut after = s.snapshot(leaves(), &opts);
    assert_eq!(snapshot_ns(after.as_mut(), 7), vec![0, 1, 2, 100]);
    assert_eq!(snapshot_ns(after.as_mut(), 8), vec![10, 11, 200]);
    assert!(before
        .visit(9, &opts, &[], &mut Rows(|_: &Row, _| {}))
        .is_none());
}

/// A day a replace retires after a snapshot was taken is still read, whole, through that
/// snapshot, and its file goes when the snapshot does.
#[test]
fn a_snapshot_reads_a_replaced_day_until_it_is_dropped() {
    let scratch = Scratch::new("snapshot-replace");
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    for n in 0..3 {
        leaf.append(row(T0 + n, 50 + n));
    }
    s.flush(FlushKind::Final);
    let old: Vec<PathBuf> = data_files(&scratch.0).into_iter().collect();
    let opts = ScanOptions::default();
    let mut held = s.snapshot(vec![(7, Arc::clone(&leaf))], &opts);
    let dir = s.stage(9).expect("stage");
    s.commit_load(LoadCommit {
        load_id: 9,
        files: vec![staged(&dir, "a.col", 7, 0..5)],
        replace_day: Some(day_of(T0)),
    })
    .expect("replace");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4]);
    assert!(old[0].exists(), "deleted under a snapshot");
    assert_eq!(
        snapshot_ns(held.as_mut(), 7),
        vec![50, 51, 52],
        "the snapshot lost the day it was taken with"
    );
    drop(held);
    assert!(
        !old[0].exists(),
        "a retired file outlived its last snapshot"
    );
}

/// A day a file found at startup shares with another is not replaced: retiring its
/// directory would take the other day's rows, or leave some of its own.
#[test]
fn replace_a_day_refuses_a_day_a_file_from_before_shares_with_another() {
    let scratch = Scratch::new("replace-mixed");
    let first = day_of(T0);
    let late = first + 1;
    {
        let s = store(&scratch);
        // What the spill left: a file under the first day's directory holding a row
        // group of the next day's.
        let dir = s.stage(9).expect("stage");
        let file = StagedFile {
            day: first,
            ..staged_at(&dir, "a.col", 7, T0 + DAY_NS, 0..2)
        };
        s.commit_load(LoadCommit {
            load_id: 9,
            files: vec![file],
            replace_day: None,
        })
        .expect("commit");
        let dir = s.stage(10).expect("stage");
        s.commit_load(LoadCommit {
            load_id: 10,
            files: vec![staged_at(&dir, "b.col", 7, T0 + 2 * DAY_NS, 5..6)],
            replace_day: None,
        })
        .expect("commit");
    }
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    for (load_id, day) in [(11, first), (12, late)] {
        let dir = s.stage(load_id).expect("stage");
        let refused = s.commit_load(LoadCommit {
            load_id,
            files: vec![staged_at(&dir, "c.col", 7, day * DAY_NS, 20..21)],
            replace_day: Some(day),
        });
        match refused {
            Err(StoreError::ReplaceRefused { path, .. }) => {
                assert!(path.starts_with(&day_name(first)), "{path}")
            }
            other => panic!("replaced a day a mixed file shares: {other:?}"),
        }
        s.unstage(load_id);
    }
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 5], "a refused replace changed rows");
    let dir = s.stage(13).expect("stage");
    s.commit_load(LoadCommit {
        load_id: 13,
        files: vec![staged_at(&dir, "d.col", 7, T0 + 2 * DAY_NS, 30..31)],
        replace_day: Some(first + 2),
    })
    .expect("a day no mixed file touches is replaced");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 30]);
}

/// At the cap, the file sealed to make room is one no later batch of the flush wants, so
/// no day is sealed and opened again as its next number.
#[test]
fn at_the_cap_a_day_the_flush_still_wants_is_not_the_one_sealed() {
    let scratch = Scratch::new("victim");
    let options = DiskOptions {
        max_open_files: 2,
        ..eager()
    };
    let s = store_with(&scratch, options);
    let leaf = s.open(7, "ts").expect("leaf");
    leaf.append(row(T0 + 9 * DAY_NS, 9));
    s.flush(FlushKind::Tick);
    leaf.append(row(T0 + 3 * DAY_NS, 3));
    s.flush(FlushKind::Tick);
    // The ninth day, open since the first flush, is the least recently written - and
    // this flush wants it again. The third day is the one to seal.
    leaf.append(row(T0 + DAY_NS, 1));
    leaf.append(row(T0 + 9 * DAY_NS + 1, 10));
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!(done.sealed, 1, "a day was sealed and opened again");
    s.flush(FlushKind::Final);
    let names: Vec<String> = data_files(&scratch.0)
        .iter()
        .map(|p| p.file_name().unwrap_or_default().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.iter().all(|n| n == "seg_0_000001.col"),
        "a day opened a second file: {names:?}"
    );
    assert_eq!(ns(leaf.as_ref()), vec![1, 3, 9, 10]);
    every_row_group_in_its_own_day(&scratch);
}

/// The final flush seals one file per open day, however many templates wrote to it.
#[test]
fn the_final_flush_seals_one_file_per_open_day() {
    let scratch = Scratch::new("final");
    let s = store_with(&scratch, DiskOptions::default());
    for id in 0..50u64 {
        let leaf = s.open(id, "ts").expect("leaf");
        for d in 0..3 {
            leaf.append(row(T0 + d * DAY_NS, id as i64));
        }
    }
    let done = s.flush(FlushKind::Final);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!(
        (done.rows, done.row_groups, done.syncs, done.sealed),
        (150, 150, 3, 3)
    );
    assert_eq!(s.open_files().0, 0);
}

// ── The write-ahead log's positions ────────────────────────────────────────────

/// The source of every row group a leaf holds of one day, in storage order.
fn sources(s: &DiskStore, template_id: u64, day: i64) -> Vec<Source> {
    let stored = {
        let leaves = s.leaves.read().expect("leaves");
        let state = leaves[&template_id].state();
        Arc::clone(&state.days[&day].stored)
    };
    stored
        .iter()
        .map(|g| {
            Reader::open(&g.file.path)
                .expect("open")
                .header(&g.entry, g.schema.len())
                .expect("header")
                .source
        })
        .collect()
}

fn wal(first: u64, last: u64) -> Source {
    Source::Wal {
        stream: 0xabc,
        first,
        last,
    }
}

/// A row group records the log positions it holds, by day, so a position is held against
/// its own day's range; the ranges are read back from the files, not kept.
#[test]
fn a_row_group_records_the_log_positions_it_holds_and_they_are_read_back() {
    let scratch = Scratch::new("positions");
    let s = store(&scratch);
    s.attach_wal(0xabc, 0);
    let leaf = s.open(7, "ts").expect("leaf");
    let today = day_of(T0);
    for (pos, ts) in [(3, T0), (4, T0 + DAY_NS), (6, T0 + 1), (9, T0 + DAY_NS + 1)] {
        leaf.append_at(row(ts, pos as i64), pos);
    }
    assert_eq!(s.oldest_position(), Some(3));
    s.flush(FlushKind::Tick);
    assert_eq!(s.oldest_position(), None);
    leaf.append_at(row(T0 + 2, 11), 11);
    assert_eq!(s.oldest_position(), Some(11));
    s.flush(FlushKind::Tick);

    assert_eq!(sources(&s, 7, today), [wal(3, 6), wal(11, 11)]);
    assert_eq!(sources(&s, 7, today + 1), [wal(4, 9)]);
    let ranges =
        |s: &DiskStore, id, day, stream| s.wal_ranges(id, day, stream).expect("ranges");
    assert_eq!(ranges(&s, 7, today, 0xabc), [(3, 6), (11, 11)]);
    assert_eq!(ranges(&s, 7, today + 1, 0xabc), [(4, 9)]);
    // Another log's positions, another template's, another day's: nothing of this
    // stream's to skip.
    assert!(ranges(&s, 7, today, 0xdef).is_empty());
    assert!(ranges(&s, 8, today, 0xabc).is_empty());
    assert!(ranges(&s, 7, today + 2, 0xabc).is_empty());

    drop(leaf);
    drop(s);
    let s = store(&scratch);
    s.open(7, "ts").expect("leaf");
    assert_eq!(ranges(&s, 7, today, 0xabc), [(3, 6), (11, 11)]);
    assert_eq!(ranges(&s, 7, today + 1, 0xabc), [(4, 9)]);
}

/// Ranges that touch or overlap are one run: what replay searches is in order, with
/// none inside another.
#[test]
fn the_ranges_replay_reads_are_merged_where_they_touch() {
    let scratch = Scratch::new("ranges");
    let s = store(&scratch);
    s.attach_wal(0xabc, 0);
    let leaf = s.open(7, "ts").expect("leaf");
    for group in [1..4u64, 4..6, 9..10] {
        for pos in group {
            leaf.append_at(row(T0 + pos as i64, pos as i64), pos);
        }
        s.flush(FlushKind::Tick);
    }
    assert_eq!(
        sources(&s, 7, day_of(T0)),
        [wal(1, 3), wal(4, 5), wal(9, 9)]
    );
    assert_eq!(
        s.wal_ranges(7, day_of(T0), 0xabc).expect("ranges"),
        [(1, 5), (9, 9)]
    );
}

/// A row that came by no log has no position to record, with a log attached or not:
/// a range over it would promise lines it never held.
#[test]
fn rows_that_came_by_no_log_record_no_range() {
    let scratch = Scratch::new("no-position");
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    leaf.append_at(row(T0, 1), 5);
    s.flush(FlushKind::Tick);
    s.attach_wal(0xabc, 0);
    leaf.append(row(T0 + 1, 2));
    assert_eq!(s.oldest_position(), None);
    s.flush(FlushKind::Tick);
    leaf.append_at(row(T0 + 2, 3), 6);
    leaf.append(row(T0 + 3, 4));
    s.flush(FlushKind::Tick);
    assert_eq!(
        sources(&s, 7, day_of(T0)),
        [
            Source::NotRecorded,
            Source::NotRecorded,
            Source::NotRecorded
        ]
    );
    assert!(s
        .wal_ranges(7, day_of(T0), 0xabc)
        .expect("ranges")
        .is_empty());
}

/// Rows a flush could not write go back with their positions: the oldest position in
/// memory is still theirs, and the row group that takes them records the same range.
#[test]
fn rows_put_back_keep_their_positions() {
    let scratch = Scratch::new("positions-restore");
    let s = store(&scratch);
    s.attach_wal(0xabc, 0);
    let leaf = s.open(7, "ts").expect("leaf");
    leaf.append_at(row(T0, 1), 21);
    leaf.append_at(row(T0 + 1, 2), 22);
    let data = scratch.0.join(DATA_DIR);
    fs::remove_dir_all(&data).expect("remove");
    fs::write(&data, b"not a directory").expect("write");
    let done = s.flush(FlushKind::Tick);
    assert_eq!(done.failed.len(), 1, "the flush wrote into a file");
    leaf.append_at(row(T0 + 2, 3), 23);
    assert_eq!(s.oldest_position(), Some(21));

    fs::remove_file(&data).expect("remove");
    fs::create_dir_all(&data).expect("mkdir");
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!(s.oldest_position(), None);
    assert_eq!(sources(&s, 7, day_of(T0)), [wal(21, 23)]);
}

/// A replace-a-day under a log says to it what it retired: the last position applied
/// when the day was written out, which the next start reads back.
#[test]
fn replace_a_day_tells_the_log_what_it_retired() {
    let scratch = Scratch::new("replace-wal");
    let s = store(&scratch);
    s.attach_wal(0xabc, 0);
    let leaf = s.open(7, "ts").expect("leaf");
    let today = day_of(T0);
    leaf.append_at(row(T0, 50), 4);
    leaf.append_at(row(T0 + DAY_NS, 51), 5);
    s.flush(FlushKind::Tick);
    leaf.append_at(row(T0 + 1, 52), 8);
    assert!(s.replaced_days(0xabc).is_empty());

    let dir = s.stage(9).expect("stage");
    s.commit_load(LoadCommit {
        load_id: 9,
        files: vec![staged(&dir, "a.col", 7, 0..5)],
        replace_day: Some(today),
    })
    .expect("replace");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4, 51]);
    assert_eq!(s.replaced_days(0xabc), BTreeMap::from([(today, 8)]));
    assert!(s.replaced_days(0xdef).is_empty());
    // Nothing covers positions 4 and 8 any more - the mark is what stops a replay
    // storing them - and the other day's row group still covers its own.
    assert!(s.wal_ranges(7, today, 0xabc).expect("ranges").is_empty());
    assert_eq!(s.wal_ranges(7, today + 1, 0xabc).expect("ranges"), [(5, 5)]);

    drop(leaf);
    drop(s);
    let s = store(&scratch);
    assert_eq!(s.replaced_days(0xabc), BTreeMap::from([(today, 8)]));

    // A mark is read back as it is written: the same day spelled any other way a date
    // parser takes names no day, and drops no line of a replay.
    let name = day_name(today);
    let (year, rest) = name.split_once('-').expect("a day's name");
    let spelled = [
        format!("+{name}"),
        format!(" {name}"),
        format!("{year}-{}", rest.trim_start_matches('0')),
    ];
    for day in &spelled {
        assert!(day_of_name(day).is_some(), "`{day}` is not a date at all");
    }
    lock(&s.files)
        .replaced
        .extend(spelled.map(|day| manifest::WalMark::new(0xabc, day, 99)));
    assert_eq!(s.replaced_days(0xabc), BTreeMap::from([(today, 8)]));
}

/// With no log attached a replace says nothing to one.
#[test]
fn replace_a_day_with_no_log_carries_no_mark() {
    let scratch = Scratch::new("replace-no-wal");
    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    leaf.append(row(T0, 50));
    let dir = s.stage(9).expect("stage");
    s.commit_load(LoadCommit {
        load_id: 9,
        files: vec![staged(&dir, "a.col", 7, 0..5)],
        replace_day: Some(day_of(T0)),
    })
    .expect("replace");
    assert!(lock(&s.files).replaced.is_empty());
}

/// Rows a start put back from the log have waited already: once the buffers are
/// aged, the next tick closes them, where it would have left them their whole wait.
#[test]
fn aged_buffers_close_at_the_next_tick() {
    let scratch = Scratch::new("aged");
    let options = DiskOptions {
        row_groups: RowGroupLimits {
            age: Duration::from_secs(30),
            ..RowGroupLimits::default()
        },
        ..DiskOptions::default()
    };
    let s = store_with(&scratch, options);
    s.attach_wal(0xabc, 0);
    let leaf = s.open(7, "ts").expect("leaf");
    leaf.append_at(row(T0, 1), 1);
    leaf.append_at(row(T0 + DAY_NS, 2), 2);
    assert_eq!(s.flush(FlushKind::Tick).rows, 0, "closed before its wait");
    assert_eq!(s.oldest_position(), Some(1));
    s.age_buffers();
    assert_eq!(s.flush(FlushKind::Tick).rows, 2);
    assert_eq!(s.oldest_position(), None);
}

/// Row groups no leaf has taken yet cover what they cover: "no leaf" read as "nothing
/// stored" would have every one of their lines put back a second time.
#[test]
fn the_ranges_of_a_template_with_no_open_leaf_are_read_from_its_row_groups() {
    let scratch = Scratch::new("ranges-unopened");
    let day = day_of(T0);
    {
        let s = store(&scratch);
        s.attach_wal(0xabc, 0);
        let leaf = s.open(7, "ts").expect("leaf");
        leaf.append_at(row(T0, 1), 4);
        leaf.append_at(row(T0, 2), 5);
        s.flush(FlushKind::Final);
    }
    let s = store(&scratch);
    assert_eq!(s.wal_ranges(7, day, 0xabc).expect("ranges"), [(4, 5)]);
    assert!(s.wal_ranges(7, day + 1, 0xabc).expect("ranges").is_empty());
    assert!(s.wal_ranges(8, day, 0xabc).expect("ranges").is_empty());
    s.open(7, "ts").expect("leaf");
    assert_eq!(s.wal_ranges(7, day, 0xabc).expect("ranges"), [(4, 5)]);
}

/// Five rows of one day, flushed and sealed, and the store dropped: the file they are
/// in, and its day directory.
fn one_sealed_day(scratch: &Scratch) -> (PathBuf, PathBuf) {
    {
        let s = store_with(scratch, DiskOptions::default());
        let leaf = s.open(7, "ts").expect("leaf");
        for n in 0..5 {
            leaf.append(row(T0 + n * 1_000_000_000, n));
        }
        let done = s.flush(FlushKind::Final);
        assert!(done.failed.is_empty(), "{:?}", done.failed);
    }
    let file = data_files(&scratch.0).into_iter().next().expect("a file");
    let day = file.parent().expect("a day directory").to_path_buf();
    (file, day)
}

/// What a start prints for a store that will not open: the error and every cause below
/// it, on one line, so a cause said twice shows as it would to an operator.
fn printed(e: &StoreError) -> String {
    crate::error::chain(e)
}

/// A named file in a directory that will not be searched is not "not on disk": the start
/// is refused with the directory and the system's error, said once.
#[test]
#[cfg(unix)]
fn a_named_file_in_a_directory_that_cannot_be_searched_is_unread_not_gone() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("unsearchable");
    let (file, day) = one_sealed_day(&scratch);

    fs::set_permissions(&day, fs::Permissions::from_mode(0o000)).expect("chmod 000");
    let readable = fs::read_dir(&day).is_ok();
    let refused = DiskStore::open(scratch.0.clone(), eager()).err();
    fs::set_permissions(&day, fs::Permissions::from_mode(0o755)).expect("chmod 755");
    if readable {
        // Root reads through mode 000: there is nothing to refuse, and nothing tested.
        return;
    }

    let e = refused.expect("a start on a day directory at mode 000 was not refused");
    let said = printed(&e);
    assert_eq!(
        said,
        format!(
            "{}: Permission denied (os error 13): manifest.log names {}, and that file \
                 was not read - whether it is on disk is not known",
            day.display(),
            file.display()
        )
    );
    assert_eq!(said.matches("Permission denied").count(), 1, "{said}");
    assert!(
        !said.contains("not on disk") && !said.contains("rows are gone"),
        "{said}"
    );

    let s = store(&scratch);
    let leaf = s.open(7, "ts").expect("leaf");
    assert_eq!(ns(leaf.as_ref()), vec![0, 1, 2, 3, 4]);
}

/// The same, one directory further up: it is `data/` that is named, not the day.
#[test]
#[cfg(unix)]
fn the_directory_named_is_the_one_that_refused_the_search() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("unsearchable-data");
    let (file, day) = one_sealed_day(&scratch);
    let data = scratch.0.join(DATA_DIR);

    fs::set_permissions(&data, fs::Permissions::from_mode(0o000)).expect("chmod 000");
    let readable = fs::read_dir(&data).is_ok();
    let refused = DiskStore::open(scratch.0.clone(), eager()).err();
    fs::set_permissions(&data, fs::Permissions::from_mode(0o755)).expect("chmod 755");
    if readable {
        return;
    }

    let said = printed(&refused.expect("a start on data/ at mode 000 was not refused"));
    assert!(
        said.starts_with(&format!("{}: Permission denied (os error 13): ", data.display())),
        "{said}"
    );
    assert!(!said.starts_with(&day.display().to_string()), "{said}");
    assert_eq!(said.matches(&file.display().to_string()).count(), 1, "{said}");
    assert_eq!(said.matches("Permission denied").count(), 1, "{said}");
    assert!(said.contains("was not read"), "{said}");
    assert!(
        !said.contains("not on disk") && !said.contains("rows are gone"),
        "{said}"
    );
}

/// A named file that is absent from a directory that was read is what "not on disk"
/// is for - said once.
#[test]
fn a_named_file_that_is_absent_is_called_not_on_disk_once() {
    let scratch = Scratch::new("absent");
    let (file, _) = one_sealed_day(&scratch);
    fs::remove_file(&file).expect("remove");

    let e = DiskStore::open(scratch.0.clone(), eager())
        .err()
        .expect("a start with a named file gone was not refused");
    assert_eq!(
        printed(&e),
        format!(
            "{}: named by manifest.log and not on disk; its rows are gone",
            file.display()
        )
    );
}

/// The same question asked earlier in a start, of a committed load's file while its
/// staging directory is still there: unread, not "neither here ... nor in staging".
#[test]
#[cfg(unix)]
fn a_loads_file_in_a_directory_that_cannot_be_searched_is_unread_not_missing() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("unsearchable-load");
    let rel = format!("{}/seg_0_000001.col", day_name(day_of(T0)));
    {
        let s = store(&scratch);
        let dir = s.stage(9).expect("stage");
        let file = staged(&dir, "a.col", 7, 0..4);
        let mut files = lock(&s.files);
        files
            .record(vec![Record::Add {
                files: vec![FileRecord::sealed(rel.clone(), &file.index)],
                source: Some(manifest::load_source(9)),
            }])
            .expect("record");
    }
    // A start moves the file into place and removes staging.
    drop(store(&scratch));
    let at = scratch.0.join(DATA_DIR).join(&rel);
    assert!(at.exists(), "not moved");
    // The load's staging directory is there again, as a stop part way leaves it.
    let staging = scratch.0.join(STAGING_DIR).join(format!("{:016x}", 9));
    fs::create_dir_all(&staging).expect("mkdir");
    let day = at.parent().expect("a day directory").to_path_buf();

    fs::set_permissions(&day, fs::Permissions::from_mode(0o000)).expect("chmod 000");
    let readable = fs::read_dir(&day).is_ok();
    let refused = DiskStore::open(scratch.0.clone(), eager()).err();
    fs::set_permissions(&day, fs::Permissions::from_mode(0o755)).expect("chmod 755");
    if readable {
        return;
    }

    let e = refused.expect("a start on a day directory at mode 000 was not refused");
    let said = printed(&e);
    assert_eq!(
        said,
        format!(
            "{}: Permission denied (os error 13): manifest.log names {}, and that file \
                 was not read - whether it is on disk is not known",
            day.display(),
            at.display()
        )
    );
    assert!(!said.contains("neither here"), "{said}");
    assert!(staging.exists(), "staging was removed by a start that was refused");
}

// ── What a scan reads ──────────────────────────────────────────────────────────

/// One day's rows read the plain way, a `Row` built for every stored row: what the scan
/// is compared with, row for row and counter for counter. Not to be made faster.
pub(super) fn slow_day(
    view: &View,
    opts: &ScanOptions,
    terms: &[Term],
    v: &mut dyn Visitor,
    out: &mut Scan,
    readers: &mut HashMap<PathBuf, Reader>,
) -> Result<bool, StoreError> {
    let ts_name = opts.ts_column.as_deref().unwrap_or(DEFAULT_TS_COLUMN);
    let since = opts.since.map(nanos_of);
    let until = opts.until.map(nanos_of);
    let windowed = since.is_some() || until.is_some();

    let mut seq = 0u64;
    for s in view.stored.iter() {
        let rows = u64::from(s.entry.row_count);
        if windowed
            && !(since.is_none_or(|from| s.entry.max_time >= from)
                && until.is_none_or(|to| s.entry.min_time < to))
        {
            out.row_groups_pruned += 1;
            seq += rows;
            continue;
        }
        out.row_groups_read += 1;
        let wanted: Vec<usize> = (0..s.schema.len())
            .filter(|at| {
                let name = &s.schema[*at];
                opts.columns.is_empty()
                    || opts.columns.iter().any(|w| w == name)
                    || (windowed
                        && (name == ts_name || name == crate::rows::RECEIVED_AT_COLUMN))
            })
            .collect();
        let reader = match readers.entry(s.file.path.clone()) {
            std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
            std::collections::hash_map::Entry::Vacant(slot) => {
                out.files_read += 1;
                slot.insert(Reader::open(&s.file.path)?)
            }
        };
        let header = reader.header(&s.entry, s.schema.len())?;
        let (columns, read) = reader.chunks(&s.entry, &header, &wanted)?;
        out.bytes_read += read;

        let mut row = Row::with_capacity(wanted.len());
        for r in 0..header.row_count as usize {
            out.scanned += 1;
            row.clear();
            for (at, cells) in wanted.iter().zip(&columns) {
                let (value, written) = &cells[r];
                row.insert_written(s.schema[*at].clone(), value.clone(), written.clone());
            }
            let position = seq;
            seq += 1;
            if windowed && !in_window(effective_ts(&row, ts_name), since, until) {
                continue;
            }
            let rendered = render_row(&row, &opts.text_columns);
            let seen = rendered.as_ref().unwrap_or(&row);
            if !terms.iter().all(|term| term.matches_row(seen)) {
                continue;
            }
            out.matched += 1;
            v.row(seen, position);
            if opts.limit != 0 && out.matched as usize >= opts.limit {
                return Ok(false);
            }
        }
    }

    let in_memory = view
        .closing
        .iter()
        .flat_map(|batch| batch.rows.iter())
        .chain(view.buffer.iter());
    for b in in_memory {
        out.scanned += 1;
        let position = seq;
        seq += 1;
        if windowed && !in_window(effective_ts(&b.row, ts_name), since, until) {
            continue;
        }
        let rendered = render_row(&b.row, &opts.text_columns);
        let seen = rendered.as_ref().unwrap_or(&b.row);
        if !terms.iter().all(|term| term.matches_row(seen)) {
            continue;
        }
        out.matched += 1;
        v.row(seen, position);
        if opts.limit != 0 && out.matched as usize >= opts.limit {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `Frozen::visit` over `slow_day`: the same days, asked for and counted the same way.
pub(super) fn slow_visit(
    frozen: &Frozen,
    opts: &ScanOptions,
    terms: &[Term],
    v: &mut dyn Visitor,
) -> Result<Scan, StoreError> {
    let since = opts.since.map(nanos_of);
    let until = opts.until.map(nanos_of);
    let mut out = Scan {
        row_groups_pruned: frozen.pruned,
        ..Scan::default()
    };
    let mut readers = HashMap::new();
    for view in &frozen.views {
        if !v.wants_partition(view.span.map(|(_, hi)| hi)) {
            if let Some(matched) = DiskLeaf::countable(view, terms, since, until) {
                out.matched += matched;
                continue;
            }
        }
        if !slow_day(view, opts, terms, v, &mut out, &mut readers)? {
            break;
        }
    }
    Ok(out)
}

/// Everything a scan counts, in the order `Scan` names it.
pub(super) fn counters(scan: &Scan) -> [u64; 6] {
    [
        scan.matched,
        scan.scanned,
        scan.row_groups_read,
        scan.row_groups_pruned,
        scan.files_read,
        scan.bytes_read,
    ]
}

const SECOND: i64 = 1_000_000_000;

/// The rows of `four_units` in each of its units, and all of them.
const UNIT: i64 = 16;
const TAIL: i64 = 10;
const ROWS: i64 = 3 * UNIT + TAIL;

/// Row 7 of the first row group, of the second, and of the rows in memory: each alone in
/// its second, so a window one second long has it as its first row and its only one.
const HITS: [(i64, i64); 3] = [(7, 1_000), (UNIT + 7, 2_000), (3 * UNIT + 7, 3_000)];

/// A second no row is in, inside the first row group's range and after every other's.
const QUIET: i64 = 4_000;

/// One template and one day in four units: three row groups, rows moved aside, rows
/// buffered. The first row group's range holds every window asked, so it is always read.
fn four_units(scratch: &Scratch) -> (DiskStore, Arc<DiskLeaf>) {
    let at = |n: i64| {
        let second = match HITS.iter().find(|(row, _)| *row == n) {
            Some((_, second)) => *second,
            None if n == UNIT - 1 => 5_000,
            None => n,
        };
        row(T0 + second * SECOND, n)
    };
    let s = store_with(scratch, thresholds(UNIT as usize, usize::MAX));
    let opened = s.open(7, "ts").expect("leaf");
    for n in 0..3 * UNIT {
        opened.append(at(n));
    }
    let done = s.flush(FlushKind::Tick);
    assert!(done.failed.is_empty(), "{:?}", done.failed);
    assert_eq!((done.rows, done.row_groups), (3 * UNIT as u64, 3));

    let leaf = Arc::clone(&s.leaves.read().expect("leaves")[&7]);
    for n in 3 * UNIT..3 * UNIT + TAIL / 2 {
        leaf.append(at(n));
    }
    // The first hold of a flush and not its second: the rows are moved aside and stay
    // there, as they are while the flush is encoding and writing them.
    assert!(
        leaf.move_day(day_of(T0)).is_some(),
        "nothing was moved aside"
    );
    for n in 3 * UNIT + TAIL / 2..ROWS {
        leaf.append(at(n));
    }
    {
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
    (s, leaf)
}

/// The second `second` of the day, as a window, and what `Exists` asks of a store: one
/// row, and no column but the ones the window reads.
fn exists_in(second: i64) -> ScanOptions {
    let from = T0 + second * SECOND;
    ScanOptions {
        columns: vec![crate::rows::RECEIVED_AT_COLUMN.into(), "ts".into()],
        limit: 1,
        since: Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(from as u64)),
        until: Some(SystemTime::UNIX_EPOCH + Duration::from_nanos((from + SECOND) as u64)),
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    }
}

/// "Is anything in this window?" stops on the first row inside it. `scanned` is rows
/// looked at, not rows built: an answer reports it, and it must not move.
#[test]
fn exists_on_disk_stops_on_the_first_row_inside_the_window() {
    let scratch = Scratch::new("exists-window");
    let (_s, leaf) = four_units(&scratch);
    // Each window, then: rows matched, rows scanned, row groups read and pruned.
    let cases = [
        // Row 7 of the first row group: the seven before it, and it.
        (HITS[0].1, (1, 8, 1, 0)),
        // Row 7 of the second: the first row group whole, then eight.
        (HITS[1].1, (1, UNIT as u64 + 8, 2, 0)),
        // Row 7 of the rows in memory: the first row group whole, the other two
        // pruned, then eight.
        (HITS[2].1, (1, UNIT as u64 + 8, 1, 2)),
        // Nothing: the first row group whole, and every row in memory.
        (QUIET, (0, (UNIT + TAIL) as u64, 1, 2)),
    ];
    for (second, want) in cases {
        let scan = leaf
            .visit(&exists_in(second), &[], &mut Counted)
            .expect("scan");
        assert_eq!(
            (
                scan.matched,
                scan.scanned,
                scan.row_groups_read,
                scan.row_groups_pruned
            ),
            want,
            "the window at second {second}"
        );
    }
}

/// The same question settled by a term: the scan stops on the first row it matches, and
/// no row group is pruned, since a term rules none out.
#[test]
fn exists_on_disk_stops_on_the_first_row_a_term_matches() {
    let scratch = Scratch::new("exists-term");
    let (_s, leaf) = four_units(&scratch);
    let open = ScanOptions {
        columns: vec![
            crate::rows::RECEIVED_AT_COLUMN.into(),
            "n".into(),
            "ts".into(),
        ],
        limit: 1,
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let all_day = ScanOptions {
        since: Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(T0 as u64)),
        until: Some(
            SystemTime::UNIX_EPOCH + Duration::from_nanos((T0 + 6_000 * SECOND) as u64),
        ),
        ..open.clone()
    };
    // Each `n`, then: rows matched, rows scanned, row groups read.
    let cases = [
        (HITS[0].0, (1, 8, 1)),
        (HITS[1].0, (1, UNIT as u64 + 8, 2)),
        (HITS[2].0, (1, 3 * UNIT as u64 + 8, 3)),
        // An `n` no row holds: every row was looked at.
        (ROWS, (0, ROWS as u64, 3)),
    ];
    for (n, want) in cases {
        // As the row's value prints, and as its line wrote it: one `Int64` to a term.
        for text in [n.to_string(), format!("{n:04}")] {
            for opts in [&open, &all_day] {
                let scan = leaf
                    .visit(opts, &[Term::exact("n", &text)], &mut Counted)
                    .expect("scan");
                assert_eq!(
                    (
                        scan.matched,
                        scan.scanned,
                        scan.row_groups_read,
                        scan.row_groups_pruned
                    ),
                    (want.0, want.1, want.2, 0),
                    "n = {text}, windowed: {}",
                    opts.since.is_some()
                );
            }
        }
    }
}

/// The scan and `slow_visit` over one frozen leaf: the rows, their positions, their order
/// and every counter must be the same. What the scan answered is returned.
fn scan_both(leaf: &DiskLeaf, opts: &ScanOptions, terms: &[Term]) -> (Scan, Vec<(Row, u64)>) {
    let frozen = leaf.freeze(opts.since.map(nanos_of), opts.until.map(nanos_of));
    let mut rows = Vec::new();
    let scan = frozen
        .visit(
            opts,
            terms,
            &mut Rows(|row: &Row, seq: u64| rows.push((row.clone(), seq))),
        )
        .expect("scan");
    let mut slow_rows = Vec::new();
    let slow = slow_visit(
        &frozen,
        opts,
        terms,
        &mut Rows(|row: &Row, seq: u64| slow_rows.push((row.clone(), seq))),
    )
    .expect("the plain scan");
    assert_eq!(counters(&scan), counters(&slow), "counters, for {opts:?}");
    assert_eq!(rows, slow_rows, "rows, for {opts:?}");
    (scan, rows)
}

/// The plain scan is the scan, over every unit of storage, windowed or not, limited or
/// not; and a row's position is the rows of its day before it, read or pruned.
#[test]
fn the_plain_scan_hands_over_what_the_scan_does() {
    let scratch = Scratch::new("plain-scan");
    let (_s, leaf) = four_units(&scratch);
    let whole = ScanOptions {
        ts_column: Some("ts".into()),
        ..ScanOptions::default()
    };
    let (scan, rows) = scan_both(&leaf, &whole, &[]);
    assert_eq!((scan.matched, scan.scanned), (ROWS as u64, ROWS as u64));
    for (at, (row, seq)) in rows.iter().enumerate() {
        assert_eq!(
            (row.get("n"), *seq),
            (Some(&ColValue::Int64(at as i64)), at as u64)
        );
    }

    for (row, second) in HITS {
        for limit in [0, 1] {
            for columns in [Vec::new(), vec!["ts".to_string()]] {
                let opts = ScanOptions {
                    columns,
                    limit,
                    ..exists_in(second)
                };
                let (scan, rows) = scan_both(&leaf, &opts, &[]);
                assert_eq!(scan.matched, 1, "{opts:?}");
                let seqs: Vec<u64> = rows.iter().map(|(_, seq)| *seq).collect();
                assert_eq!(seqs, [row as u64], "{opts:?}");
            }
        }
    }
    let (scan, rows) = scan_both(&leaf, &exists_in(QUIET), &[]);
    assert_eq!((scan.matched, rows.len()), (0, 0));
    // A window the whole day is outside of: the day is ruled out before it is read.
    let (scan, _) = scan_both(&leaf, &exists_in(2 * 86_400), &[]);
    assert_eq!(counters(&scan), [0, 0, 0, 3, 0, 0]);

    // Terms: one a single row matches, on disk and in memory; `*` on a key every row has and
    // on one none has; two together; and a term beside a window with and without its row.
    let second = HITS[1].0.to_string();
    let buffered = HITS[2].0.to_string();
    let lists: [(Vec<Term>, Vec<u64>); 5] = [
        (vec![Term::exact("n", &second)], vec![HITS[1].0 as u64]),
        (vec![Term::exact("n", &buffered)], vec![HITS[2].0 as u64]),
        (vec![Term::any("n")], (0..ROWS as u64).collect()),
        (vec![Term::any("absent")], Vec::new()),
        (
            vec![Term::any("ts"), Term::exact("n", &second)],
            vec![HITS[1].0 as u64],
        ),
    ];
    for (terms, want) in &lists {
        for limit in [0, 1] {
            let opts = ScanOptions {
                limit,
                ..whole.clone()
            };
            let (scan, rows) = scan_both(&leaf, &opts, terms);
            let seqs: Vec<u64> = rows.iter().map(|(_, seq)| *seq).collect();
            let kept = if limit == 0 {
                want.len()
            } else {
                want.len().min(1)
            };
            assert_eq!(seqs, want[..kept], "{terms:?}, limit {limit}");
            assert_eq!(scan.matched, kept as u64, "{terms:?}, limit {limit}");
        }
    }
    let term = [Term::exact("n", &second)];
    for (window, want) in [(HITS[1].1, 1), (HITS[0].1, 0)] {
        // Every column, so the term's own is among the ones read.
        let opts = ScanOptions {
            columns: Vec::new(),
            ..exists_in(window)
        };
        let (scan, _) = scan_both(&leaf, &opts, &term);
        assert_eq!(scan.matched, want, "the window at second {window}");
    }
}
