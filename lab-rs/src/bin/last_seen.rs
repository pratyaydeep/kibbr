//! B4 - keeping a template's "last seen" up to date from 8 threads.
//!
//!   write_lock_per_line   parking_lot RwLock<u64>, write-locked for every line
//!   atomic_per_line       AtomicU64::fetch_max for every line
//!   once_per_500_lines    a thread-local max, written under the lock every 500 lines
//!
//! Simplifies: the threads do nothing but update, so contention is the whole cost.
//! Decides how the per-template last-seen stamp should be kept.

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use triedb_lab_rs::*;

const THREADS: usize = 8;
const PER_THREAD: u64 = 2_000_000;

fn main() {
    println!("B4 last_seen: {THREADS} threads x {PER_THREAD} updates on one template; 3 runs");
    let mut rec = Recorder::new("B4_last_seen");
    let mut out = Vec::new();
    for variant in ["write_lock_per_line", "atomic_per_line", "once_per_500_lines"] {
        let mut rates = vec![];
        let mut final_v = 0;
        for _ in 0..3 {
            let lock = Arc::new(RwLock::new(0u64));
            let atom = Arc::new(AtomicU64::new(0));
            let t = Instant::now();
            let hs: Vec<_> = (0..THREADS)
                .map(|tid| {
                    let (lock, atom) = (lock.clone(), atom.clone());
                    std::thread::spawn(move || {
                        let base = tid as u64 * PER_THREAD;
                        match variant {
                            "write_lock_per_line" => {
                                for i in 0..PER_THREAD {
                                    let v = base + i;
                                    let mut g = lock.write();
                                    if v > *g {
                                        *g = v;
                                    }
                                }
                            }
                            "atomic_per_line" => {
                                for i in 0..PER_THREAD {
                                    atom.fetch_max(base + i, Ordering::Relaxed);
                                }
                            }
                            _ => {
                                let mut local = 0u64;
                                for i in 0..PER_THREAD {
                                    let v = base + i;
                                    local = local.max(v);
                                    if i % 500 == 499 {
                                        let mut g = lock.write();
                                        if local > *g {
                                            *g = local;
                                        }
                                    }
                                }
                                let mut g = lock.write();
                                if local > *g {
                                    *g = local;
                                }
                            }
                        }
                    })
                })
                .collect();
            for h in hs {
                h.join().unwrap();
            }
            let el = t.elapsed().as_secs_f64();
            rates.push((THREADS as u64 * PER_THREAD) as f64 / el);
            final_v = if variant == "atomic_per_line" { atom.load(Ordering::Relaxed) } else { *lock.read() };
            assert_eq!(final_v, THREADS as u64 * PER_THREAD - 1);
        }
        let s = Stat::of(&rates);
        record(&mut rec, variant, "updates_per_sec", s, "updates_per_sec", "8 threads, one template");
        out.push(vec![variant.into(), format!("{} ±{:.0}%", fmt_rate(s.median), s.spread_pct()), final_v.to_string()]);
    }
    table(&["variant", "updates/s", "final value"], &out);
    rec.flush();
}
