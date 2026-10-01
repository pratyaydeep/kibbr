//! B3 - what a row-group close does to the searchers.
//!
//! One template buffer behind a RwLock. An ingest thread appends rows in batches of
//! 64 and waits when the buffer is more than 4 row groups ahead of the closer. A closer keeps closing row groups of 8,192 rows: encode with zstd 3, write to a
//! file, `sync_all`. Four search threads keep taking a snapshot (a clone of the
//! buffer's `ts` column plus the list of visible row-id ranges).
//!
//! X: hold the write lock from the start to the end of the close.
//! Y: lock, move the rows aside as "being written", unlock; encode, write, flush with
//!    no lock; lock, publish the row group and drop the moved rows in one step, unlock.
//!
//! CORRECTNESS: every row carries a sequence id. A snapshot must contain every id from
//! 0 to its own maximum exactly once, across buffer + being-written + published. Any
//! violation is counted and reported.
//!
//! Simplifies: one template, a synthetic 12-column row, searches that only snapshot.
//! Decides whether the close must hold the lock end to end or can publish in one step.
//! `sync_all` on Darwin is `F_FULLFSYNC` (~3 ms), so a close costs at least that.

use parking_lot::RwLock;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use kibbr_lab_rs::workload::{Columns, Gen, Row};
use kibbr_lab_rs::*;

const ROW_GROUP: usize = 8192;

#[derive(Default)]
struct State {
    buf: Columns,
    buf_first_id: u64,
    moved: Option<Columns>,
    moved_first_id: u64,
    published: Vec<(u64, u64)>, // (first_id, count)
}

fn encode_write(cols: &Columns, file: &mut std::fs::File) -> usize {
    let bytes = cols.to_bytes();
    let c = zstd::bulk::compress(&bytes, 3).unwrap();
    file.write_all(&c).unwrap();
    file.sync_all().unwrap();
    c.len()
}

fn run(strategy: char, rows: &Arc<Vec<Row>>, secs: f64, path: &PathBuf) -> (f64, Lat, u64, u64, u64) {
    let _ = std::fs::remove_file(path);
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    let st = Arc::new(RwLock::new(State::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let ingested = Arc::new(AtomicU64::new(0));
    let closes = Arc::new(AtomicU64::new(0));
    let violations = Arc::new(AtomicU64::new(0));
    let snapshots = Arc::new(AtomicU64::new(0));
    // Ingest.
    let ingest = {
        let (st, stop, rows, ingested) = (st.clone(), stop.clone(), rows.clone(), ingested.clone());
        std::thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                // Backpressure: a real ingest is told to wait when the buffer is far
                // ahead of the closer; without it Y grows the buffer without bound.
                if st.read().buf.len() > 4 * ROW_GROUP {
                    std::thread::sleep(Duration::from_micros(100));
                    continue;
                }
                let mut s = st.write();
                for _ in 0..64 {
                    s.buf.push(&rows[i % rows.len()]);
                    i += 1;
                }
                drop(s);
                ingested.store(i as u64, Ordering::Relaxed);
                // The real ingest parses JSON between batches; a short pause stands in
                // for it so the closer and searchers get a turn.
                std::thread::sleep(Duration::from_micros(20));
            }
        })
    };
    // Closer.
    let closer = {
        let (st, stop, closes) = (st.clone(), stop.clone(), closes.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let ready = st.read().buf.len() >= ROW_GROUP;
                if !ready {
                    std::thread::sleep(Duration::from_micros(200));
                    continue;
                }
                if strategy == 'X' {
                    let mut s = st.write();
                    let first = s.buf_first_id;
                    let cols = s.buf.drain_first(ROW_GROUP);
                    let n = cols.len() as u64;
                    encode_write(&cols, &mut file);
                    s.published.push((first, n));
                    s.buf_first_id = first + n;
                } else {
                    let (cols, first) = {
                        let mut s = st.write();
                        let first = s.buf_first_id;
                        let cols = s.buf.drain_first(ROW_GROUP);
                        let n = cols.len() as u64;
                        // The moved rows stay visible to searchers through `moved`
                        // (only the ts column is kept there: that is what a snapshot
                        // reads) while the encode happens with no lock held.
                        let mut moved = Columns::default();
                        moved.ts = cols.ts.clone();
                        s.moved = Some(moved);
                        s.moved_first_id = first;
                        s.buf_first_id = first + n;
                        (cols, first)
                    };
                    encode_write(&cols, &mut file);
                    let mut s = st.write();
                    s.published.push((first, cols.len() as u64));
                    s.moved = None;
                }
                closes.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    // Searchers.
    let hs: Vec<_> = (0..4)
        .map(|_| {
            let (st, stop, violations, snapshots) = (st.clone(), stop.clone(), violations.clone(), snapshots.clone());
            std::thread::spawn(move || {
                let mut lat = Lat::new();
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    let s = st.read();
                    let _ts_copy: Vec<i64> = s.buf.ts.clone();
                    let mut ranges: Vec<(u64, u64)> = s.published.clone();
                    if let Some(m) = &s.moved {
                        ranges.push((s.moved_first_id, m.ts.len() as u64));
                    }
                    ranges.push((s.buf_first_id, s.buf.len() as u64));
                    drop(s);
                    lat.record(t.elapsed());
                    // Verify: every id 0..=max exactly once.
                    ranges.sort_unstable();
                    let mut expect = 0u64;
                    let mut bad = false;
                    for (first, n) in &ranges {
                        if *first != expect {
                            bad = true;
                            break;
                        }
                        expect += n;
                    }
                    if bad {
                        violations.fetch_add(1, Ordering::Relaxed);
                    }
                    snapshots.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_micros(50));
                }
                lat
            })
        })
        .collect();
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs_f64(secs));
    stop.store(true, Ordering::Relaxed);
    ingest.join().unwrap();
    closer.join().unwrap();
    let mut lat = Lat::new();
    for h in hs {
        lat.merge(&h.join().unwrap());
    }
    let el = t0.elapsed().as_secs_f64();
    let _ = std::fs::remove_file(path);
    (ingested.load(Ordering::Relaxed) as f64 / el, lat, closes.load(Ordering::Relaxed), violations.load(Ordering::Relaxed), snapshots.load(Ordering::Relaxed))
}

fn main() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out/b3");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("rg.bin");
    let mut g = Gen::new(1);
    let rows = Arc::new(g.rows(ROW_GROUP * 4));
    println!("B3 buffer_lock: 1 ingest thread (batches of 64), 1 closer (8,192-row groups, zstd 3 + sync_all), 4 searchers; 3 s per run, 3 runs");
    let mut rec = Recorder::new("B3_buffer_lock");
    let mut out = Vec::new();
    for strategy in ['X', 'Y'] {
        let (mut rate, mut p50, mut p99, mut max, mut cl, mut viol, mut snaps) = (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
        for _ in 0..3 {
            let (r, lat, c, v, s) = run(strategy, &rows, 3.0, &path);
            rate.push(r);
            p50.push(lat.p50_us());
            p99.push(lat.p99_us());
            max.push(lat.max_us());
            cl.push(c as f64);
            viol.push(v as f64);
            snaps.push(s as f64);
        }
        let name = if strategy == 'X' { "X_hold_lock_for_whole_close" } else { "Y_move_aside_publish_in_one_step" };
        let (r, a, b, m, c, v, s) = (Stat::of(&rate), Stat::of(&p50), Stat::of(&p99), Stat::of(&max), Stat::of(&cl), Stat::of(&viol), Stat::of(&snaps));
        record(&mut rec, name, "ingest_rows_per_sec", r, "rows_per_sec", "");
        record(&mut rec, name, "search_wait_p50", a, "us", "read lock + snapshot");
        record(&mut rec, name, "search_wait_p99", b, "us", "worst 1 percent");
        record(&mut rec, name, "search_wait_max", m, "us", "");
        record(&mut rec, name, "closes", c, "count", "row groups closed in 3 s");
        record(&mut rec, name, "snapshots", s, "count", "snapshots taken in 3 s by 4 searchers");
        rec.add(name, "violations", v.max, "count", "snapshots that did not see every row exactly once (max over 3 runs)");
        out.push(vec![name.into(), format!("{} ±{:.0}%", fmt_rate(r.median), r.spread_pct()), fmt_us(a.median), fmt_us(b.median), fmt_us(m.median), format!("{:.0}", c.median), format!("{:.0}", s.median), format!("{:.0}", v.max)]);
    }
    table(&["strategy", "ingest rows/s", "search p50", "search p99", "search max", "closes", "snapshots", "violations"], &out);
    rec.flush();
}
