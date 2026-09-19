//! B9 - what per-template buffers cost in memory.
//!
//! 1,000 and 10,000 templates, rows from the skewed workload, a buffer closes at 8,192
//! rows (closing = zstd 3 encode of the buffer's bytes, then the rows are dropped):
//!   presized_8192   every template's column vectors reserve 8,192 rows up front
//!   grow_on_demand  vectors grow as rows arrive and keep their capacity after a close
//!   grow_and_shrink vectors grow, and are shrunk to fit (empty) after each close
//! Resident memory (RSS) at the end and rows per second.
//!
//! BURST: 10,000 templates, 30 s at ten times a normal 30,000 rows/s (the A1 HTTP
//! rate), with no memory limit versus a total buffer-bytes limit (256 MB) where the
//! largest buffers are closed first when it is reached. Peak RSS and rows per second.
//!
//! Simplifies: one thread, no disk, rows already parsed, "memory" is the process RSS
//! with every variant in a fresh process so no allocator reuse leaks between them.
//! Decides the buffer growth policy and whether a total memory limit is needed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use triedb_lab_rs::workload::{Columns, Gen, Row};
use triedb_lab_rs::*;

const ROW_GROUP: usize = 8192;
const STEADY_ROWS: usize = 1_000_000;

fn est_bytes(c: &Columns) -> usize {
    // What the buffer holds: vector reservations plus the string heap (avg ~60 B/row).
    c.reserved_bytes() + c.len() * 60
}

fn close(c: &mut Columns, shrink: bool) -> usize {
    let n = zstd::bulk::compress(&c.to_bytes(), 3).unwrap().len();
    c.clear();
    if shrink {
        c.shrink();
    }
    n
}

struct Sampler {
    stop: Arc<AtomicBool>,
    peak: Arc<AtomicU64>,
    h: Option<std::thread::JoinHandle<()>>,
}
impl Sampler {
    fn start() -> Sampler {
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(0));
        let (s, p) = (stop.clone(), peak.clone());
        let h = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                p.fetch_max(rss_bytes(), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        Sampler { stop, peak, h: Some(h) }
    }
    fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        self.h.take().unwrap().join().unwrap();
        self.peak.load(Ordering::Relaxed).max(rss_bytes())
    }
}

fn steady(t: usize, policy: &str, rows: &[Row]) -> (f64, u64, u64) {
    let mut bufs: HashMap<u32, Columns> = HashMap::with_capacity(t);
    if policy == "presized_8192" {
        for i in 0..t {
            bufs.insert(i as u32, Columns::with_capacity(ROW_GROUP));
        }
    }
    let sampler = Sampler::start();
    let t0 = Instant::now();
    let mut closes = 0;
    for r in rows {
        let b = bufs.entry(r.template).or_default();
        b.push(r);
        if b.len() >= ROW_GROUP {
            close(b, policy == "grow_and_shrink");
            closes += 1;
        }
    }
    let el = t0.elapsed().as_secs_f64();
    let end = rss_bytes();
    let peak = sampler.finish();
    let _ = closes;
    (rows.len() as f64 / el, end, peak)
}

fn burst(limit: Option<usize>, secs: f64, rate: f64) -> (f64, u64, usize, usize) {
    let t = 10_000;
    let mut g = Gen::new(t);
    let mut bufs: HashMap<u32, Columns> = HashMap::with_capacity(t);
    let mut total_est = 0usize;
    let sampler = Sampler::start();
    let t0 = Instant::now();
    let mut n = 0usize;
    let mut closes = 0;
    let mut forced = 0;
    let batch = 1000usize;
    while t0.elapsed().as_secs_f64() < secs {
        // Pace: `batch` rows every batch/rate seconds; fall behind if we cannot keep up.
        let due = t0 + Duration::from_secs_f64(n as f64 / rate);
        let now = Instant::now();
        if now < due {
            std::thread::sleep(due - now);
        }
        for _ in 0..batch {
            let r = g.next_row();
            let b = bufs.entry(r.template).or_default();
            let before = est_bytes(b);
            b.push(&r);
            total_est += est_bytes(b) - before;
            if b.len() >= ROW_GROUP {
                let before = est_bytes(b);
                close(b, false);
                total_est -= before - est_bytes(b);
                closes += 1;
            }
        }
        n += batch;
        if let Some(lim) = limit {
            while total_est > lim {
                // Largest buffers first.
                let (k, _) = bufs.iter().max_by_key(|(_, c)| est_bytes(c)).map(|(k, c)| (*k, c.len())).unwrap();
                let b = bufs.get_mut(&k).unwrap();
                let before = est_bytes(b);
                close(b, true);
                total_est -= before - est_bytes(b);
                forced += 1;
            }
        }
    }
    let el = t0.elapsed().as_secs_f64();
    let peak = sampler.finish();
    (n as f64 / el, peak, closes, forced)
}

/// Runs one variant in a fresh process, so its RSS is its own and not what an earlier
/// variant left in the allocator. Prints "rate end_or_peak peak closes forced".
fn child(args: &[String]) {
    match args[0].as_str() {
        "--steady" => {
            let t: usize = args[1].parse().unwrap();
            let mut g = Gen::new(t);
            let rows = g.rows(STEADY_ROWS);
            let before = rss_bytes(); // the generated rows themselves, not the buffers
            let (r, e, p) = steady(t, &args[2], &rows);
            println!("{r} {} {} 0 0", e.saturating_sub(before), p.saturating_sub(before));
        }
        _ => {
            let limit: usize = args[1].parse().unwrap();
            let (r, p, c, f) = burst(if limit == 0 { None } else { Some(limit) }, 30.0, 300_000.0);
            println!("{r} {p} {p} {c} {f}");
        }
    }
}

fn run_child(args: &[&str]) -> (f64, f64, f64, usize, usize) {
    let out = std::process::Command::new(std::env::current_exe().unwrap()).args(args).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let s = String::from_utf8(out.stdout).unwrap();
    let v: Vec<&str> = s.split_whitespace().collect();
    (v[0].parse().unwrap(), v[1].parse().unwrap(), v[2].parse().unwrap(), v[3].parse().unwrap(), v[4].parse().unwrap())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if !args.is_empty() {
        child(&args);
        return;
    }
    println!("B9 buffer_memory: {STEADY_ROWS} rows per steady run, row group {ROW_GROUP}; 3 runs, each in a fresh process");
    let mut rec = Recorder::new("B9_buffer_memory");
    let mut out = Vec::new();
    for t in [1_000usize, 10_000] {
        for policy in ["presized_8192", "grow_on_demand", "grow_and_shrink"] {
            let (mut rates, mut ends, mut peaks) = (vec![], vec![], vec![]);
            for _ in 0..3 {
                let (r, e, p, _, _) = run_child(&["--steady", &t.to_string(), policy]);
                rates.push(r);
                ends.push(e);
                peaks.push(p);
            }
            let (r, e, p) = (Stat::of(&rates), Stat::of(&ends), Stat::of(&peaks));
            let v = format!("{t}_templates/{policy}");
            record(&mut rec, &v, "rows_per_sec", r, "rows_per_sec", "");
            record(&mut rec, &v, "rss_end_bytes", e, "bytes", "RSS growth over the run (the 1M generated rows are excluded); fresh process per run");
            record(&mut rec, &v, "rss_peak_bytes", p, "bytes", "sampled every 100 ms");
            out.push(vec![t.to_string(), policy.into(), format!("{} ±{:.0}%", fmt_rate(r.median), r.spread_pct()), fmt_bytes(e.median), fmt_bytes(p.median)]);
        }
    }
    table(&["templates", "policy", "rows/s", "RSS end", "RSS peak"], &out);

    println!("\nburst: 10,000 templates, 30 s at 300,000 rows/s (10x the A1 HTTP rate)");
    let mut out = Vec::new();
    for (name, limit) in [("no_memory_limit", None), ("limit_256MB_close_largest_first", Some(256usize << 20))] {
        let (mut rates, mut peaks, mut cl, mut fo) = (vec![], vec![], 0, 0);
        for _ in 0..3 {
            let (r, _, p, c, f) = run_child(&["--burst", &limit.unwrap_or(0).to_string()]);
            rates.push(r);
            peaks.push(p);
            cl = c;
            fo = f;
        }
        let (r, p) = (Stat::of(&rates), Stat::of(&peaks));
        let v = format!("burst/{name}");
        record(&mut rec, &v, "rows_per_sec", r, "rows_per_sec", "target 300,000; lower means it fell behind");
        record(&mut rec, &v, "rss_peak_bytes", p, "bytes", &format!("whole-process RSS, fresh process; {cl} closes at 8,192 rows, {fo} forced by the limit"));
        out.push(vec![name.into(), format!("{} ±{:.0}%", fmt_rate(r.median), r.spread_pct()), fmt_bytes(p.median), cl.to_string(), fo.to_string()]);
    }
    table(&["variant", "rows/s", "RSS peak", "closes", "forced closes"], &out);
    rec.flush();
}
