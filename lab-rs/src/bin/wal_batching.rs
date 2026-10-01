//! B1 - WAL group commit.
//!
//! One writer thread fed by a channel, a real file, a real `sync_all`. Before each
//! flush the writer waits 0, 1, 5 or 20 ms to gather more lines. Senders (1, 2, 4, 8)
//! run closed-loop: send a line, wait for "saved", send the next. Two extra modes: reply
//! before the flush, and compress each batch with zstd 3 before writing it.
//!
//! Simplifies: a WAL record is the raw line with no length prefix or checksum, and a
//! sender does nothing but send. Decides the group-commit wait and the acknowledgement
//! mode, and whether batch compression is worth its CPU. Rust's `sync_all` on Darwin is
//! `fcntl(F_FULLFSYNC)`, ~3-4 ms per call here: that floor is the whole shape of this
//! table, and it is the honest one (plain `fsync(2)` on a Mac stops at the drive cache).

use crossbeam_channel::{bounded, Receiver, Sender};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use kibbr_lab_rs::workload::Gen;
use kibbr_lab_rs::*;

struct Msg {
    line: Arc<Vec<u8>>,
    ack: Sender<()>,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    AckAfterFlush,
    AckBeforeFlush,
    Zstd3,
}

fn run(lines: &Arc<Vec<Arc<Vec<u8>>>>, wait: Duration, senders: usize, mode: Mode, secs: f64, path: &PathBuf) -> (f64, Lat, f64) {
    let _ = std::fs::remove_file(path);
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    let (tx, rx): (Sender<Msg>, Receiver<Msg>) = bounded(65536);
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let writer = {
        let (stop, written, bytes) = (stop.clone(), written.clone(), bytes.clone());
        std::thread::spawn(move || {
            let mut batch: Vec<Msg> = Vec::with_capacity(4096);
            let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
            loop {
                let first = match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(m) => m,
                    Err(_) => {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        continue;
                    }
                };
                batch.push(first);
                let deadline = Instant::now() + wait;
                if wait.is_zero() {
                    while let Ok(m) = rx.try_recv() {
                        batch.push(m);
                        if batch.len() >= 4096 {
                            break;
                        }
                    }
                } else {
                    loop {
                        let now = Instant::now();
                        if now >= deadline || batch.len() >= 4096 {
                            break;
                        }
                        match rx.recv_timeout(deadline - now) {
                            Ok(m) => batch.push(m),
                            Err(_) => break,
                        }
                    }
                }
                if mode == Mode::AckBeforeFlush {
                    for m in &batch {
                        let _ = m.ack.send(());
                    }
                }
                buf.clear();
                for m in &batch {
                    buf.extend_from_slice(&m.line);
                }
                if mode == Mode::Zstd3 {
                    let c = zstd::bulk::compress(&buf, 3).unwrap();
                    file.write_all(&c).unwrap();
                    bytes.fetch_add(c.len() as u64, Ordering::Relaxed);
                } else {
                    file.write_all(&buf).unwrap();
                    bytes.fetch_add(buf.len() as u64, Ordering::Relaxed);
                }
                file.sync_all().unwrap();
                written.fetch_add(batch.len() as u64, Ordering::Relaxed);
                if mode != Mode::AckBeforeFlush {
                    for m in &batch {
                        let _ = m.ack.send(());
                    }
                }
                batch.clear();
            }
        })
    };
    let t0 = Instant::now();
    let hs: Vec<_> = (0..senders)
        .map(|s| {
            let (tx, lines, stop) = (tx.clone(), lines.clone(), stop.clone());
            std::thread::spawn(move || {
                let (atx, arx) = bounded::<()>(1);
                let mut lat = Lat::new();
                let mut i = s;
                while !stop.load(Ordering::Relaxed) {
                    let line = lines[i % lines.len()].clone();
                    i += senders;
                    let t = Instant::now();
                    tx.send(Msg { line, ack: atx.clone() }).unwrap();
                    arx.recv().unwrap();
                    lat.record(t.elapsed());
                }
                lat
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs_f64(secs));
    stop.store(true, Ordering::Relaxed);
    let mut lat = Lat::new();
    for h in hs {
        lat.merge(&h.join().unwrap());
    }
    drop(tx);
    writer.join().unwrap();
    let el = t0.elapsed().as_secs_f64();
    let n = written.load(Ordering::Relaxed);
    let bpl = bytes.load(Ordering::Relaxed) as f64 / n.max(1) as f64;
    let _ = std::fs::remove_file(path);
    (n as f64 / el, lat, bpl)
}

fn main() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out/b1");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("wal.log");
    let mut g = Gen::new(500);
    let lines: Arc<Vec<Arc<Vec<u8>>>> = Arc::new((0..20000).map(|_| Arc::new(format!("{}\n", g.next_row().to_json()).into_bytes())).collect());
    let raw_len: f64 = lines.iter().map(|l| l.len() as f64).sum::<f64>() / lines.len() as f64;
    println!("B1 wal_batching: {} pre-built lines, {raw_len:.0} bytes each, 1.5 s per config, 3 runs", lines.len());
    let mut rec = Recorder::new("B1_wal_batching");
    let mut rows = Vec::new();
    let configs: Vec<(Mode, u64)> = [0u64, 1, 5, 20].iter().map(|w| (Mode::AckAfterFlush, *w)).chain([(Mode::AckBeforeFlush, 5), (Mode::Zstd3, 5), (Mode::Zstd3, 20)]).collect();
    for (mode, wait_ms) in configs {
        for senders in [1usize, 2, 4, 8] {
            let mut rates = vec![];
            let mut p50 = vec![];
            let mut p99 = vec![];
            let mut bpl = vec![];
            for _ in 0..3 {
                let (r, lat, b) = run(&lines, Duration::from_millis(wait_ms), senders, mode, 1.5, &path);
                rates.push(r);
                p50.push(lat.p50_us());
                p99.push(lat.p99_us());
                bpl.push(b);
            }
            let (r, a, b, c) = (Stat::of(&rates), Stat::of(&p50), Stat::of(&p99), Stat::of(&bpl));
            let m = match mode {
                Mode::AckAfterFlush => "ack_after_flush",
                Mode::AckBeforeFlush => "ack_before_flush",
                Mode::Zstd3 => "zstd3_ack_after_flush",
            };
            let name = format!("{m}/wait_{wait_ms}ms/{senders}_senders");
            record(&mut rec, &name, "lines_per_sec", r, "lines_per_sec", "closed-loop senders");
            record(&mut rec, &name, "saved_wait_p50", a, "us", "sender's wait for the ack");
            record(&mut rec, &name, "saved_wait_p99", b, "us", "worst 1 percent");
            record(&mut rec, &name, "wal_bytes_per_line", c, "bytes", "raw line avg 200 B");
            rows.push(vec![m.into(), wait_ms.to_string(), senders.to_string(), format!("{} ±{:.0}%", fmt_rate(r.median), r.spread_pct()), fmt_us(a.median), fmt_us(b.median), format!("{:.0}", c.median)]);
        }
    }
    table(&["mode", "wait ms", "senders", "lines/s", "saved p50", "saved p99", "B/line"], &rows);
    rec.flush();
}
