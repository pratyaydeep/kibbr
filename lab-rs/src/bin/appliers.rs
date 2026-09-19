//! B2 - one applier or K appliers keyed by template.
//!
//! Rows arrive in order from one channel in batches of 64. ONE applier appends every
//! row to its template's column buffers, versus K appliers (2, 4, 8) fed by a router
//! that sends a template always to the same applier, so per-template order is kept.
//!
//! Simplifies: rows are pre-parsed, an applier only appends (no trie walk, no type
//! inference, no close). Decides whether one applier thread is enough or K appliers
//! pay for their skew (the top 10 templates hold 80% of rows).

use crossbeam_channel::{bounded, Receiver, Sender};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use triedb_lab_rs::workload::{Columns, Gen, Row};
use triedb_lab_rs::*;

const ROWS: usize = 1_000_000;
const BATCH: usize = 64;

fn apply(rx: Receiver<Arc<Vec<Row>>>) -> u64 {
    let mut bufs: HashMap<u32, Columns> = HashMap::new();
    let mut n = 0u64;
    for batch in rx {
        for r in batch.iter() {
            bufs.entry(r.template).or_default().push(r);
            n += 1;
        }
    }
    n
}

fn run(rows: &Arc<Vec<Row>>, k: usize) -> (f64, f64) {
    let (tx, rx): (Sender<Arc<Vec<Row>>>, Receiver<Arc<Vec<Row>>>) = bounded(1024);
    let t = Instant::now();
    let producer = {
        let rows = rows.clone();
        std::thread::spawn(move || {
            for chunk in rows.chunks(BATCH) {
                tx.send(Arc::new(chunk.to_vec())).unwrap();
            }
        })
    };
    let counts: Vec<u64> = if k == 1 {
        let h = std::thread::spawn(move || {
            let mut bufs: HashMap<u32, Columns> = HashMap::new();
            let mut n = 0u64;
            for batch in rx {
                for r in batch.iter() {
                    bufs.entry(r.template).or_default().push(r);
                    n += 1;
                }
            }
            n
        });
        vec![h.join().unwrap()]
    } else {
        // Router: splits each batch by template % k into k sub-batches, in order.
        let mut txs = Vec::new();
        let mut hs = Vec::new();
        for _ in 0..k {
            let (atx, arx) = bounded::<Arc<Vec<Row>>>(1024);
            txs.push(atx);
            hs.push(std::thread::spawn(move || apply(arx)));
        }
        let router = std::thread::spawn(move || {
            for batch in rx {
                let mut parts: Vec<Vec<Row>> = (0..k).map(|_| Vec::new()).collect();
                for r in batch.iter() {
                    parts[r.template as usize % k].push(r.clone());
                }
                for (i, p) in parts.into_iter().enumerate() {
                    if !p.is_empty() {
                        txs[i].send(Arc::new(p)).unwrap();
                    }
                }
            }
        });
        router.join().unwrap();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    };
    producer.join().unwrap();
    let el = t.elapsed().as_secs_f64();
    let total: u64 = counts.iter().sum();
    assert_eq!(total as usize, ROWS);
    let mean = total as f64 / counts.len() as f64;
    let uneven = *counts.iter().max().unwrap() as f64 / mean;
    (ROWS as f64 / el, uneven)
}

fn main() {
    let mut g = Gen::new(500);
    let rows = Arc::new(g.rows(ROWS));
    println!("B2 appliers: {ROWS} rows, 500 templates (top 10 hold 80%), batches of {BATCH} on one channel, 3 runs");
    let mut rec = Recorder::new("B2_appliers");
    let mut out = Vec::new();
    for k in [1usize, 2, 4, 8] {
        let mut rates = vec![];
        let mut un = 0.0;
        for _ in 0..3 {
            let (r, u) = run(&rows, k);
            rates.push(r);
            un = u;
        }
        let s = Stat::of(&rates);
        let name = if k == 1 { "one_applier".to_string() } else { format!("{k}_appliers_by_template_mod_k") };
        record(&mut rec, &name, "rows_per_sec", s, "rows_per_sec", "append to per-template column buffers");
        rec.add(&name, "busiest_applier_over_mean", un, "ratio", "rows on the busiest applier / mean (1 = even)");
        out.push(vec![name, format!("{} ±{:.0}%", fmt_rate(s.median), s.spread_pct()), format!("{un:.2}")]);
    }
    table(&["variant", "rows/s", "busiest/mean"], &out);
    rec.flush();
}
