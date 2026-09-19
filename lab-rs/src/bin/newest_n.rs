//! B6 - the newest 500 rows across everything.
//!
//! 500 templates x 40 row groups; every row group knows its min and max time and holds
//! 256 rows as a zstd-compressed byte blob, so "opening" one is a decode.
//!   read_all_then_sort   open every row group, sort all rows by time, take 500
//!   newest_first_stop    open row groups newest-first (by max time) and stop when 500
//!                        rows newer than every unopened row group's max are in hand
//! Then the same with rows inside each row group sorted by another column and NOT by
//! time (the compaction idea): rows from each opened row group are sorted by time
//! before use.
//!
//! Simplifies: rows are (time, id) pairs, one column, no filter. Decides whether
//! newest-first with early stop is worth having and what time-unsorted row groups
//! cost it.

use rand::prelude::*;
use rand::rngs::StdRng;
use std::collections::BinaryHeap;
use std::time::Instant;
use triedb_lab_rs::*;

const TEMPLATES: usize = 500;
const GROUPS: usize = 40;
const ROWS_PER_GROUP: usize = 256;
const N: usize = 500;

struct RowGroup {
    min_ts: i64,
    max_ts: i64,
    blob: Vec<u8>, // zstd of [(ts: i64, id: u64)] little-endian
}

fn decode(rg: &RowGroup) -> Vec<(i64, u64)> {
    let raw = zstd::bulk::decompress(&rg.blob, ROWS_PER_GROUP * 16).unwrap();
    raw.chunks_exact(16).map(|c| (i64::from_le_bytes(c[..8].try_into().unwrap()), u64::from_le_bytes(c[8..].try_into().unwrap()))).collect()
}

fn build(sorted_by_time: bool) -> Vec<RowGroup> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut out = Vec::with_capacity(TEMPLATES * GROUPS);
    let width: i64 = 1_000_000;
    let mut id = 0u64;
    for t in 0..TEMPLATES {
        let offset = rng.gen_range(0..width);
        for g in 0..GROUPS {
            let lo = g as i64 * width + offset;
            let mut rows: Vec<(i64, u64)> = (0..ROWS_PER_GROUP)
                .map(|_| {
                    id += 1;
                    (lo + rng.gen_range(0..width), id)
                })
                .collect();
            if sorted_by_time {
                rows.sort_unstable();
            } else {
                // "compacted": sorted by another column - here by id hashed, i.e. not time.
                rows.sort_unstable_by_key(|r| r.1.wrapping_mul(0x9E3779B97F4A7C15));
            }
            let min_ts = rows.iter().map(|r| r.0).min().unwrap();
            let max_ts = rows.iter().map(|r| r.0).max().unwrap();
            let mut raw = Vec::with_capacity(ROWS_PER_GROUP * 16);
            for (ts, id) in &rows {
                raw.extend_from_slice(&ts.to_le_bytes());
                raw.extend_from_slice(&id.to_le_bytes());
            }
            out.push(RowGroup { min_ts, max_ts, blob: zstd::bulk::compress(&raw, 3).unwrap() });
            let _ = t;
        }
    }
    out
}

fn read_all(rgs: &[RowGroup], sort_inside: bool) -> (Vec<(i64, u64)>, usize) {
    let mut all = Vec::with_capacity(rgs.len() * ROWS_PER_GROUP);
    for rg in rgs {
        let mut rows = decode(rg);
        if sort_inside {
            rows.sort_unstable();
        }
        all.extend(rows);
    }
    all.sort_unstable_by(|a, b| b.cmp(a));
    all.truncate(N);
    (all, rgs.len())
}

fn newest_first(rgs: &[RowGroup], sort_inside: bool) -> (Vec<(i64, u64)>, usize) {
    let mut order: Vec<usize> = (0..rgs.len()).collect();
    order.sort_unstable_by_key(|&i| std::cmp::Reverse(rgs[i].max_ts));
    // Min-heap of the N newest seen so far: the top is the oldest of them.
    let mut heap: BinaryHeap<std::cmp::Reverse<(i64, u64)>> = BinaryHeap::with_capacity(N + 1);
    let mut opened = 0;
    for (k, &i) in order.iter().enumerate() {
        let next_unopened_max = order.get(k).map(|&j| rgs[j].max_ts);
        if heap.len() >= N {
            if let Some(m) = next_unopened_max {
                if heap.peek().unwrap().0 .0 >= m {
                    break;
                }
            }
        }
        let mut rows = decode(&rgs[i]);
        opened += 1;
        if sort_inside {
            rows.sort_unstable_by(|a, b| b.cmp(a));
        }
        for r in rows {
            if heap.len() < N {
                heap.push(std::cmp::Reverse(r));
            } else if r > heap.peek().unwrap().0 {
                heap.pop();
                heap.push(std::cmp::Reverse(r));
            }
        }
    }
    let mut v: Vec<(i64, u64)> = heap.into_iter().map(|r| r.0).collect();
    v.sort_unstable_by(|a, b| b.cmp(a));
    let _ = rgs[0].min_ts;
    (v, opened)
}

fn main() {
    println!("B6 newest_n: {TEMPLATES} templates x {GROUPS} row groups x {ROWS_PER_GROUP} rows (zstd 3 blobs), newest {N}; 3 runs");
    let mut rec = Recorder::new("B6_newest_n");
    let mut out = Vec::new();
    for (layout, sorted) in [("rows_sorted_by_time_inside", true), ("rows_sorted_by_other_column_inside", false)] {
        let rgs = build(sorted);
        let sort_inside = !sorted;
        let mut reference = None;
        for (name, f) in [("read_all_then_sort", read_all as fn(&[RowGroup], bool) -> (Vec<(i64, u64)>, usize)), ("newest_first_stop_early", newest_first)] {
            let mut ms = vec![];
            let mut opened = 0;
            for _ in 0..3 {
                let t = Instant::now();
                let (rows, n) = f(&rgs, sort_inside);
                ms.push(t.elapsed().as_secs_f64() * 1000.0);
                opened = n;
                match &reference {
                    None => reference = Some(rows),
                    Some(r) => assert_eq!(r, &rows, "the two strategies disagree on the newest {N}"),
                }
            }
            let s = Stat::of(&ms);
            let v = format!("{layout}/{name}");
            record(&mut rec, &v, "time_ms", s, "ms", &format!("{} of {} row groups opened", opened, rgs.len()));
            rec.add(&v, "row_groups_opened", opened as f64, "count", "");
            out.push(vec![layout.into(), name.into(), format!("{:.1} ±{:.0}%", s.median, s.spread_pct()), opened.to_string()]);
        }
    }
    table(&["row group layout", "strategy", "ms", "opened"], &out);
    rec.flush();
}
