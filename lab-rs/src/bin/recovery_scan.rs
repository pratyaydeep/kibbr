//! B13 - the startup cost of an unfinished file.
//!
//! An unfinished file of 128 MB, 512 MB and 1 GB made of row groups, each
//! `[len u32][min_ts i64][max_ts i64][crc32c u32][payload]` with the payload a zstd 3 blob
//! of ~64 KB of synthetic column bytes. Startup walks the file, verifies every checksum
//! and rebuilds the index (offset, len, min, max per row group). CRC with the aarch64
//! instruction and with a software table, both reported.
//!
//! Simplifies: the file was just written, so it is in the page cache (no root here to
//! purge it); the index is a Vec. Decides how the file size limit sets the startup cost
//! after a crash.

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::time::Instant;
use kibbr_lab_rs::crc32c;
use kibbr_lab_rs::workload::{Columns, Gen};
use kibbr_lab_rs::*;

const HEADER: usize = 4 + 8 + 8 + 4;

fn build(path: &PathBuf, target_bytes: u64) -> usize {
    let mut g = Gen::new(500);
    let mut f = std::io::BufWriter::new(File::create(path).unwrap());
    let mut written = 0u64;
    let mut groups = 0;
    // A few distinct payloads: encoding 1 GB of fresh rows would dominate the build.
    let payloads: Vec<(i64, i64, Vec<u8>)> = (0..16)
        .map(|_| {
            let mut c = Columns::with_capacity(512);
            let rows = g.rows(512);
            for r in &rows {
                c.push(r);
            }
            (rows[0].ts, rows[rows.len() - 1].ts, zstd::bulk::compress(&c.to_bytes(), 3).unwrap())
        })
        .collect();
    while written < target_bytes {
        let (lo, hi, p) = &payloads[groups % payloads.len()];
        f.write_all(&(p.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&lo.to_le_bytes()).unwrap();
        f.write_all(&hi.to_le_bytes()).unwrap();
        f.write_all(&crc32c::hardware(0, p).to_le_bytes()).unwrap();
        f.write_all(p).unwrap();
        written += (HEADER + p.len()) as u64;
        groups += 1;
    }
    // Unfinished: a torn last record, as a crash leaves it.
    f.write_all(&[0u8; 37]).unwrap();
    f.flush().unwrap();
    groups
}

fn scan(path: &PathBuf, hw: bool) -> (usize, usize, u64) {
    let mut r = BufReader::with_capacity(1 << 20, File::open(path).unwrap());
    let mut index: Vec<(u64, u32, i64, i64)> = Vec::new();
    let mut off = 0u64;
    let mut hdr = [0u8; HEADER];
    let mut buf = Vec::new();
    let mut bad = 0;
    loop {
        if r.read_exact(&mut hdr).is_err() {
            break;
        }
        let len = u32::from_le_bytes(hdr[..4].try_into().unwrap());
        let lo = i64::from_le_bytes(hdr[4..12].try_into().unwrap());
        let hi = i64::from_le_bytes(hdr[12..20].try_into().unwrap());
        let crc = u32::from_le_bytes(hdr[20..24].try_into().unwrap());
        if len == 0 || len > 16 << 20 {
            break;
        }
        buf.resize(len as usize, 0);
        if r.read_exact(&mut buf).is_err() {
            break;
        }
        let got = if hw { crc32c::hardware(0, &buf) } else { crc32c::software(0, &buf) };
        if got != crc {
            bad += 1;
            break;
        }
        index.push((off, len, lo, hi));
        off += (HEADER + len as usize) as u64;
    }
    (index.len(), bad, off)
}

fn main() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out/b13");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("unfinished.seg");
    println!("B13 recovery_scan: hardware CRC-32C available: {}; 3 runs each (file just written, page cache warm)", crc32c::hardware_available());
    let mut rec = Recorder::new("B13_recovery_scan");
    let mut out = Vec::new();
    for mb in [128u64, 512, 1024] {
        let groups = build(&path, mb << 20);
        let size = std::fs::metadata(&path).unwrap().len();
        for (name, hw) in [("crc_hardware", true), ("crc_software", false)] {
            let mut ms = vec![];
            let mut found = 0;
            for _ in 0..3 {
                let t = Instant::now();
                let (n, bad, end) = scan(&path, hw);
                ms.push(t.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(n, groups);
                assert_eq!(bad, 0);
                assert!(end < size);
                found = n;
            }
            let s = Stat::of(&ms);
            let v = format!("{mb}MB/{name}");
            record(&mut rec, &v, "scan_ms", s, "ms", &format!("{found} row groups, torn tail detected"));
            rec.add(&v, "mb_per_sec", size as f64 / 1e6 / (s.median / 1000.0), "MB_per_sec", "");
            out.push(vec![format!("{mb}MB"), name.into(), format!("{:.0} ±{:.0}%", s.median, s.spread_pct()), format!("{:.0}", size as f64 / 1e6 / (s.median / 1000.0)), found.to_string()]);
        }
    }
    table(&["file", "crc", "ms", "MB/s", "row groups"], &out);
    let _ = std::fs::remove_file(&path);
    rec.flush();
}
