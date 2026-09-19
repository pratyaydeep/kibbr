//! Shared helpers for the design benchmarks: the synthetic workload, the results CSV,
//! timing and the statistics every table reports. Nothing here imports TrieDB.
pub mod crc32c;
pub mod workload;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

/// `lab-rs/results/results.csv`, resolved from the crate root so a bench can be run from
/// anywhere.
pub fn results_csv() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("results/results.csv")
}

fn git_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn now_iso() -> String {
    let out = Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"]).output();
    out.ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn csv_quote(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Appends rows to the results CSV. Every row shares `when` and `git_sha`.
pub struct Recorder {
    bench: String,
    when: String,
    sha: String,
    rows: Vec<String>,
}

impl Recorder {
    pub fn new(bench: &str) -> Self {
        Recorder { bench: bench.into(), when: now_iso(), sha: git_sha(), rows: Vec::new() }
    }

    pub fn add(&mut self, variant: &str, metric: &str, value: f64, unit: &str, note: &str) {
        self.rows.push(format!(
            "{},{},{},{},{},{},{},{}",
            self.when,
            self.sha,
            self.bench,
            csv_quote(variant),
            csv_quote(metric),
            fmt_num(value),
            unit,
            csv_quote(note)
        ));
    }

    pub fn flush(&self) {
        let path = results_csv();
        let fresh = !path.exists() || std::fs::metadata(&path).map(|m| m.len() == 0).unwrap_or(true);
        let mut f = OpenOptions::new().create(true).append(true).open(&path).expect("results.csv");
        if fresh {
            writeln!(f, "when,git_sha,bench,variant,metric,value,unit,note").unwrap();
        }
        for r in &self.rows {
            writeln!(f, "{r}").unwrap();
        }
        eprintln!("[{}] {} rows appended to {}", self.bench, self.rows.len(), path.display());
    }
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.4}")
    }
}

/// Median and spread (max - min, as a percentage of the median) over `runs`.
#[derive(Clone, Copy, Debug)]
pub struct Stat {
    pub median: f64,
    pub min: f64,
    pub max: f64,
}

impl Stat {
    pub fn of(vals: &[f64]) -> Stat {
        let mut v = vals.to_vec();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = v.len();
        let median = if n % 2 == 1 { v[n / 2] } else { (v[n / 2 - 1] + v[n / 2]) / 2.0 };
        Stat { median, min: v[0], max: v[n - 1] }
    }
    pub fn spread_pct(&self) -> f64 {
        if self.median == 0.0 { 0.0 } else { (self.max - self.min) / self.median * 100.0 }
    }
}

/// Runs `f` three times, returns the median and spread of what it measures.
pub fn three<F: FnMut() -> f64>(mut f: F) -> Stat {
    let v: Vec<f64> = (0..3).map(|_| f()).collect();
    Stat::of(&v)
}

/// Records both the median and the spread of a Stat under `metric` / `metric_spread`.
pub fn record(rec: &mut Recorder, variant: &str, metric: &str, s: Stat, unit: &str, note: &str) {
    rec.add(variant, metric, s.median, unit, note);
    rec.add(variant, &format!("{metric}_spread"), s.spread_pct(), "pct_of_median", "max-min over 3 runs");
}

pub fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

pub fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let t = Instant::now();
    let r = f();
    (r, t.elapsed())
}

/// Latency histogram in microseconds with p50 / p99 / max.
pub struct Lat {
    h: hdrhistogram::Histogram<u64>,
}

impl Default for Lat {
    fn default() -> Self {
        Lat { h: hdrhistogram::Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap() }
    }
}

impl Lat {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn record(&mut self, d: Duration) {
        let us = (d.as_nanos() / 1000).max(1) as u64;
        self.h.record(us.min(60_000_000_000)).ok();
    }
    pub fn merge(&mut self, other: &Lat) {
        self.h.add(&other.h).ok();
    }
    pub fn p50_us(&self) -> f64 {
        self.h.value_at_quantile(0.5) as f64
    }
    pub fn p99_us(&self) -> f64 {
        self.h.value_at_quantile(0.99) as f64
    }
    pub fn max_us(&self) -> f64 {
        self.h.max() as f64
    }
    pub fn count(&self) -> u64 {
        self.h.len()
    }
}

/// Resident set size of this process in bytes, from `ps`, which is what the memory
/// benchmarks want: what the OS charges, not what the allocator thinks it holds.
pub fn rss_bytes() -> u64 {
    let pid = std::process::id().to_string();
    let out = Command::new("ps").args(["-o", "rss=", "-p", &pid]).output().ok();
    out.and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

/// A plain text table: header row then rows, columns padded.
pub fn table(header: &[&str], rows: &[Vec<String>]) {
    let ncol = header.len();
    let mut w = vec![0; ncol];
    for (i, h) in header.iter().enumerate() {
        w[i] = w[i].max(h.len());
    }
    for r in rows {
        for (i, c) in r.iter().enumerate() {
            w[i] = w[i].max(c.len());
        }
    }
    let line = |cells: Vec<String>| {
        cells.iter().enumerate().map(|(i, c)| format!("{:<width$}", c, width = w[i])).collect::<Vec<_>>().join("  ")
    };
    println!("{}", line(header.iter().map(|s| s.to_string()).collect()));
    println!("{}", w.iter().map(|n| "-".repeat(*n)).collect::<Vec<_>>().join("  "));
    for r in rows {
        println!("{}", line(r.clone()));
    }
}

pub fn fmt_rate(v: f64) -> String {
    if v >= 1e6 { format!("{:.2}M", v / 1e6) } else if v >= 1e3 { format!("{:.1}k", v / 1e3) } else { format!("{v:.0}") }
}

pub fn fmt_us(us: f64) -> String {
    if us >= 1e6 { format!("{:.2}s", us / 1e6) } else if us >= 1e3 { format!("{:.2}ms", us / 1e3) } else { format!("{us:.0}us") }
}

pub fn fmt_bytes(b: f64) -> String {
    if b >= 1e9 { format!("{:.2}GB", b / 1e9) } else if b >= 1e6 { format!("{:.1}MB", b / 1e6) } else if b >= 1e3 { format!("{:.1}KB", b / 1e3) } else { format!("{b:.0}B") }
}
