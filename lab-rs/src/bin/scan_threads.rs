//! B7 - how many threads a scan should decode with.
//!
//! 2,000 zstd 3 column chunks (8,192 `path` strings each, ~190 KB raw) decoded with
//! rayon at 1, 2, 4, 8 threads. Chunks per second and the speed-up over 1 thread.
//!
//! Simplifies: decode only, nothing is done with the bytes, all chunks in memory.
//! Decides the scan's thread count and where the speed-up stops on this machine.

use rayon::prelude::*;
use std::time::Instant;
use triedb_lab_rs::workload::Gen;
use triedb_lab_rs::*;

const CHUNKS: usize = 2000;

fn main() {
    let mut g = Gen::new(500);
    let mut chunks = Vec::with_capacity(CHUNKS);
    let mut raw_total = 0usize;
    // 8 distinct chunks repeated: 2,000 fresh ones would take longer to build than to
    // decode and say nothing more.
    let base: Vec<(usize, Vec<u8>)> = (0..8)
        .map(|_| {
            let rows = g.rows(8192);
            let mut raw = Vec::new();
            for r in &rows {
                raw.extend_from_slice(r.path.as_bytes());
                raw.push(b'\n');
            }
            (raw.len(), zstd::bulk::compress(&raw, 3).unwrap())
        })
        .collect();
    for i in 0..CHUNKS {
        let (n, c) = &base[i % base.len()];
        raw_total += n;
        chunks.push(c.clone());
    }
    println!("B7 scan_threads: {CHUNKS} chunks, {} raw, {} compressed; 3 runs", fmt_bytes(raw_total as f64), fmt_bytes(chunks.iter().map(|c| c.len()).sum::<usize>() as f64));
    let mut rec = Recorder::new("B7_scan_threads");
    let mut out = Vec::new();
    let mut one = 0.0;
    for threads in [1usize, 2, 4, 8] {
        let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
        let mut rates = vec![];
        for _ in 0..3 {
            let t = Instant::now();
            let total: usize = pool.install(|| chunks.par_iter().map(|c| zstd::bulk::decompress(c, 1 << 20).unwrap().len()).sum());
            assert_eq!(total, raw_total);
            rates.push(CHUNKS as f64 / t.elapsed().as_secs_f64());
        }
        let s = Stat::of(&rates);
        if threads == 1 {
            one = s.median;
        }
        let name = format!("{threads}_threads");
        record(&mut rec, &name, "chunks_per_sec", s, "chunks_per_sec", "zstd decode of 8,192-string column chunks");
        rec.add(&name, "speedup_over_1_thread", s.median / one, "x", "");
        out.push(vec![threads.to_string(), format!("{:.0} ±{:.0}%", s.median, s.spread_pct()), format!("{:.2}x", s.median / one), fmt_bytes(raw_total as f64 * s.median / CHUNKS as f64) + "/s"]);
    }
    table(&["threads", "chunks/s", "speed-up", "decoded"], &out);
    rec.flush();
}
