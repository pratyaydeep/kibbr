//! B12 - the CPU side of choosing an encoding.
//!
//! 8,192-row chunks of six columns (ts, level, status, user_id, duration_ms, trace_id)
//! encoded with: plain; same value stored once (run lengths); word list (dictionary);
//! timestamp differences (delta); variable-length integers (LEB128); and zstd 3 on top
//! of each. Then "try all and keep the smallest" against one fixed choice per column
//! type. Rows per second, and the size next to it.
//!
//! Simplifies: hand-written encoders over synthetic columns, no nulls, sizes are
//! indicative only (the Python lab's script 05 has the real ones). Decides whether
//! trying every encoding per chunk is affordable.

use std::time::Instant;
use kibbr_lab_rs::workload::{Columns, Gen};
use kibbr_lab_rs::*;

const ROWS: usize = 8192;
const CHUNKS: usize = 50;

#[derive(Clone)]
enum Col<'a> {
    I(&'a [i64]),
    F(&'a [f64]),
    S(&'a [String]),
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn plain(c: &Col) -> Vec<u8> {
    let mut o = Vec::with_capacity(ROWS * 16);
    match c {
        Col::I(v) => v.iter().for_each(|x| o.extend_from_slice(&x.to_le_bytes())),
        Col::F(v) => v.iter().for_each(|x| o.extend_from_slice(&x.to_le_bytes())),
        Col::S(v) => v.iter().for_each(|s| {
            o.extend_from_slice(&(s.len() as u32).to_le_bytes());
            o.extend_from_slice(s.as_bytes());
        }),
    }
    o
}
fn rle(c: &Col) -> Vec<u8> {
    let mut o = Vec::with_capacity(ROWS * 4);
    fn runs<T: PartialEq>(v: &[T], mut emit: impl FnMut(&T, u32)) {
        let mut i = 0;
        while i < v.len() {
            let mut j = i + 1;
            while j < v.len() && v[j] == v[i] {
                j += 1;
            }
            emit(&v[i], (j - i) as u32);
            i = j;
        }
    }
    match c {
        Col::I(v) => runs(v, |x, n| {
            o.extend_from_slice(&x.to_le_bytes());
            varint(n as u64, &mut o);
        }),
        Col::F(v) => runs(v, |x, n| {
            o.extend_from_slice(&x.to_le_bytes());
            varint(n as u64, &mut o);
        }),
        Col::S(v) => runs(v, |s, n| {
            o.extend_from_slice(&(s.len() as u32).to_le_bytes());
            o.extend_from_slice(s.as_bytes());
            varint(n as u64, &mut o);
        }),
    }
    o
}
fn dict(c: &Col) -> Option<Vec<u8>> {
    let Col::S(v) = c else { return None };
    let mut words: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
    let mut table: Vec<&str> = Vec::new();
    let mut idx = Vec::with_capacity(v.len());
    for s in v.iter() {
        let n = words.len() as u32;
        let i = *words.entry(s.as_str()).or_insert_with(|| {
            table.push(s.as_str());
            n
        });
        idx.push(i);
    }
    let mut o = Vec::with_capacity(v.len() * 2);
    varint(table.len() as u64, &mut o);
    for w in table {
        varint(w.len() as u64, &mut o);
        o.extend_from_slice(w.as_bytes());
    }
    for i in idx {
        varint(i as u64, &mut o);
    }
    Some(o)
}
fn delta(c: &Col) -> Option<Vec<u8>> {
    let Col::I(v) = c else { return None };
    let mut o = Vec::with_capacity(v.len() * 8);
    let mut prev = 0i64;
    for x in v.iter() {
        o.extend_from_slice(&(x - prev).to_le_bytes());
        prev = *x;
    }
    Some(o)
}
fn varints(c: &Col) -> Option<Vec<u8>> {
    let Col::I(v) = c else { return None };
    let mut o = Vec::with_capacity(v.len() * 4);
    for x in v.iter() {
        varint(zigzag(*x), &mut o);
    }
    Some(o)
}
fn delta_varint(c: &Col) -> Option<Vec<u8>> {
    let Col::I(v) = c else { return None };
    let mut o = Vec::with_capacity(v.len() * 2);
    let mut prev = 0i64;
    for x in v.iter() {
        varint(zigzag(x - prev), &mut o);
        prev = *x;
    }
    Some(o)
}

fn cols(c: &Columns) -> Vec<(&'static str, Col<'_>)> {
    vec![("ts", Col::I(&c.ts)), ("level", Col::S(&c.level)), ("status", Col::I(&c.status)), ("user_id", Col::I(&c.user_id)), ("duration_ms", Col::F(&c.duration_ms)), ("trace_id", Col::S(&c.trace_id))]
}

const ENCODINGS: [&str; 6] = ["plain", "rle_same_value_once", "dict_word_list", "delta_timestamp_diff", "varint", "delta_varint"];

fn encode(name: &str, c: &Col) -> Option<Vec<u8>> {
    match name {
        "plain" => Some(plain(c)),
        "rle_same_value_once" => Some(rle(c)),
        "dict_word_list" => dict(c),
        "delta_timestamp_diff" => delta(c),
        "varint" => varints(c),
        "delta_varint" => delta_varint(c),
        _ => None,
    }
}

fn fixed_choice(col: &str) -> &'static str {
    match col {
        "ts" => "delta_varint",
        "status" | "user_id" => "varint",
        "level" | "trace_id" => "dict_word_list",
        _ => "plain",
    }
}

fn main() {
    let mut g = Gen::new(500);
    let chunks: Vec<Columns> = (0..CHUNKS)
        .map(|_| {
            let mut c = Columns::with_capacity(ROWS);
            let mut rows = g.rows(ROWS);
            rows.sort_by_key(|r| r.ts);
            for r in &rows {
                c.push(r);
            }
            c
        })
        .collect();
    println!("B12 encoding_cost: {CHUNKS} chunks x {ROWS} rows x 6 columns; 3 runs");
    let mut rec = Recorder::new("B12_encoding_cost");
    let mut out = Vec::new();
    // Per encoding, with and without zstd 3 on top, across every column it applies to.
    for enc in ENCODINGS {
        for z in [false, true] {
            let mut rates = vec![];
            let mut bytes = 0usize;
            let mut applied = 0;
            for _ in 0..3 {
                let t = Instant::now();
                bytes = 0;
                applied = 0;
                for c in &chunks {
                    for (_, col) in cols(c) {
                        if let Some(e) = encode(enc, &col) {
                            applied += 1;
                            bytes += if z { zstd::bulk::compress(&e, 3).unwrap().len() } else { e.len() };
                        }
                    }
                }
                rates.push((CHUNKS * ROWS) as f64 / t.elapsed().as_secs_f64());
            }
            let s = Stat::of(&rates);
            let name = format!("{enc}{}", if z { "+zstd3" } else { "" });
            record(&mut rec, &name, "rows_per_sec", s, "rows_per_sec", &format!("{} column-chunks applicable of {}", applied, CHUNKS * 6));
            rec.add(&name, "bytes_per_row_applicable_columns", bytes as f64 / (applied as f64 / 6.0 * ROWS as f64).max(1.0), "bytes", "size over the columns the encoding applies to");
            out.push(vec![name, format!("{} ±{:.0}%", fmt_rate(s.median), s.spread_pct()), (applied / CHUNKS).to_string(), fmt_bytes(bytes as f64 / CHUNKS as f64)]);
        }
    }
    table(&["encoding", "rows/s (chunk-rows)", "cols applicable", "bytes/chunk"], &out);

    // Try all and keep the smallest, versus the fixed choice per column type. Both with
    // zstd 3 on top, since that is what would be written.
    println!("\nchoice strategies (zstd 3 on top of the chosen encoding):");
    let mut out = Vec::new();
    let mut fixed_bytes = 0.0;
    let mut fixed_rate = 0.0;
    for strategy in ["fixed_choice_per_column_type", "try_all_keep_smallest"] {
        let mut rates = vec![];
        let mut bytes = 0usize;
        for _ in 0..3 {
            let t = Instant::now();
            bytes = 0;
            for c in &chunks {
                for (name, col) in cols(c) {
                    if strategy == "fixed_choice_per_column_type" {
                        let e = encode(fixed_choice(name), &col).unwrap();
                        bytes += zstd::bulk::compress(&e, 3).unwrap().len();
                    } else {
                        let mut best = usize::MAX;
                        for enc in ENCODINGS {
                            if let Some(e) = encode(enc, &col) {
                                best = best.min(zstd::bulk::compress(&e, 3).unwrap().len());
                            }
                        }
                        bytes += best;
                    }
                }
            }
            rates.push((CHUNKS * ROWS) as f64 / t.elapsed().as_secs_f64());
        }
        let s = Stat::of(&rates);
        if strategy == "fixed_choice_per_column_type" {
            fixed_bytes = bytes as f64;
            fixed_rate = s.median;
        }
        record(&mut rec, strategy, "rows_per_sec", s, "rows_per_sec", "");
        rec.add(strategy, "bytes_total", bytes as f64, "bytes", "");
        rec.add(strategy, "size_vs_fixed", bytes as f64 / fixed_bytes, "ratio", "");
        rec.add(strategy, "cpu_vs_fixed", fixed_rate / s.median, "x", "encode time relative to the fixed choice");
        out.push(vec![strategy.into(), format!("{} ±{:.0}%", fmt_rate(s.median), s.spread_pct()), fmt_bytes(bytes as f64), format!("{:.3}", bytes as f64 / fixed_bytes), format!("{:.2}x", fixed_rate / s.median)]);
    }
    table(&["strategy", "rows/s", "bytes", "size vs fixed", "cpu vs fixed"], &out);
    rec.flush();
}
