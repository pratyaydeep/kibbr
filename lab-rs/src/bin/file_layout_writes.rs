//! B10 - where do row groups go on disk?
//!
//! Pre-built row group blobs of 64 KB to 4 MB, appended by 1/2/4/8 producer threads
//! with one `sync_all` per row group, into: one shared file; N = 4 and 8 files chosen by
//! `template mod N`; N files chosen by least bytes so far; one file per template.
//!
//! Simplifies: the blob is random bytes, nothing is encoded or indexed, and a producer
//! does nothing between row groups. Decides whether one shared file per day is ever the
//! write limit. Note that Rust's `sync_all` on Darwin is `fcntl(F_FULLFSYNC)` - real
//! durability, ~3 ms per call on this SSD - not the 20 us `fsync(2)` that stops at the
//! drive cache (the Python lab measured that pair at 30,664/s versus 369/s).

use parking_lot::Mutex;
use rand::prelude::*;
use rand::rngs::StdRng;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use kibbr_lab_rs::workload::{pick_template, DEFAULT_TEMPLATES};
use kibbr_lab_rs::*;

const SIZES: [usize; 4] = [64 << 10, 256 << 10, 1 << 20, 4 << 20];
const ROW_GROUPS: usize = 128;

#[derive(Clone, Copy, Debug)]
enum Layout {
    Shared,
    Mod(usize),
    LeastBytes(usize),
    PerTemplate,
}

impl Layout {
    fn name(&self) -> String {
        match self {
            Layout::Shared => "one_shared_file".into(),
            Layout::Mod(n) => format!("template_mod_{n}"),
            Layout::LeastBytes(n) => format!("least_bytes_{n}"),
            Layout::PerTemplate => format!("one_file_per_template_{DEFAULT_TEMPLATES}"),
        }
    }
    fn files(&self) -> usize {
        match self {
            Layout::Shared => 1,
            Layout::Mod(n) | Layout::LeastBytes(n) => *n,
            Layout::PerTemplate => DEFAULT_TEMPLATES,
        }
    }
}

struct Sink {
    files: Vec<Mutex<File>>,
    bytes: Mutex<Vec<u64>>,
    layout: Layout,
}

impl Sink {
    fn open(dir: &PathBuf, layout: Layout) -> Sink {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let files = (0..layout.files())
            .map(|i| Mutex::new(OpenOptions::new().create(true).append(true).open(dir.join(format!("f{i}"))).unwrap()))
            .collect();
        Sink { files, bytes: Mutex::new(vec![0; layout.files()]), layout }
    }
    fn write(&self, template: u32, blob: &[u8]) {
        let idx = match self.layout {
            Layout::Shared => 0,
            Layout::Mod(n) => template as usize % n,
            Layout::PerTemplate => template as usize,
            Layout::LeastBytes(_) => {
                let mut b = self.bytes.lock();
                let (i, _) = b.iter().enumerate().min_by_key(|(_, v)| **v).unwrap();
                b[i] += blob.len() as u64;
                i
            }
        };
        if !matches!(self.layout, Layout::LeastBytes(_)) {
            self.bytes.lock()[idx] += blob.len() as u64;
        }
        let mut f = self.files[idx].lock();
        f.write_all(blob).unwrap();
        f.sync_all().unwrap();
    }
}

fn main() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("out/b10");
    let mut rng = StdRng::seed_from_u64(7);
    let blobs: Vec<Vec<u8>> = SIZES.iter().map(|&n| (0..n).map(|_| rng.gen::<u8>()).collect()).collect();
    // The job list: 128 row groups, sizes cycling, templates from the skewed workload.
    let jobs: Vec<(u32, usize)> = (0..ROW_GROUPS).map(|i| (pick_template(&mut rng, DEFAULT_TEMPLATES), i % SIZES.len())).collect();
    let total_bytes: usize = jobs.iter().map(|(_, s)| SIZES[*s]).sum();
    let blobs = Arc::new(blobs);
    let jobs = Arc::new(jobs);

    let layouts = [Layout::Shared, Layout::Mod(4), Layout::Mod(8), Layout::LeastBytes(4), Layout::LeastBytes(8), Layout::PerTemplate];
    let mut rec = Recorder::new("B10_file_layout_writes");
    let mut rows = Vec::new();
    println!("B10 file_layout_writes: {ROW_GROUPS} row groups, {} total, sizes {:?}", fmt_bytes(total_bytes as f64), SIZES);
    for layout in layouts {
        for producers in [1usize, 2, 4, 8] {
            let mut mbps = Vec::new();
            let mut rgps = Vec::new();
            let mut uneven = 0.0;
            for _ in 0..3 {
                let sink = Arc::new(Sink::open(&dir, layout));
                let next = Arc::new(AtomicUsize::new(0));
                let t = Instant::now();
                let hs: Vec<_> = (0..producers)
                    .map(|_| {
                        let (sink, next, blobs, jobs) = (sink.clone(), next.clone(), blobs.clone(), jobs.clone());
                        std::thread::spawn(move || loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= jobs.len() {
                                break;
                            }
                            let (tpl, s) = jobs[i];
                            sink.write(tpl, &blobs[s]);
                        })
                    })
                    .collect();
                for h in hs {
                    h.join().unwrap();
                }
                let el = t.elapsed().as_secs_f64();
                mbps.push(total_bytes as f64 / 1e6 / el);
                rgps.push(ROW_GROUPS as f64 / el);
                let b = sink.bytes.lock().clone();
                let nonempty = b.iter().filter(|v| **v > 0).count().max(1) as f64;
                let mean = b.iter().sum::<u64>() as f64 / nonempty;
                uneven = *b.iter().max().unwrap() as f64 / mean;
            }
            let m = Stat::of(&mbps);
            let r = Stat::of(&rgps);
            let name = format!("{}/{producers}_producers", layout.name());
            record(&mut rec, &name, "mb_per_sec", m, "MB_per_sec", "sync_all per row group");
            record(&mut rec, &name, "row_groups_per_sec", r, "rg_per_sec", "");
            rec.add(&name, "max_file_over_mean", uneven, "ratio", "largest file / mean of non-empty files (1 = even)");
            rows.push(vec![layout.name(), producers.to_string(), format!("{:.0} ±{:.0}%", m.median, m.spread_pct()), format!("{:.0}", r.median), format!("{uneven:.2}")]);
        }
    }
    table(&["layout", "producers", "MB/s", "rg/s", "max/mean"], &rows);
    rec.flush();
    let _ = std::fs::remove_dir_all(&dir);
}
