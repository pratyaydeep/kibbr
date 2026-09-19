//! The synthetic workload every design benchmark shares.
//!
//! Log lines of about 200 bytes with 12 columns, spread over `T` templates so that the
//! top 10 templates hold about 80 percent of the lines. Fixed seed, so two benchmarks
//! that both ask for 1,000,000 rows get the same rows.
//!
//! Simplification: a "template" here is a shape id plus a per-template constant key set;
//! the real system derives the template from the line's keys. The values follow the
//! real generator's spirit (a few low-cardinality strings, ids that are nearly unique,
//! a timestamp that advances) without copying its code.


use rand::prelude::*;
use rand::rngs::StdRng;

pub const SEED: u64 = 42;
pub const DEFAULT_TEMPLATES: usize = 500;
pub const COLUMNS: usize = 12;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// One row. `ts` is milliseconds since an epoch and advances with the row index.
#[derive(Clone, Debug)]
pub struct Row {
    pub template: u32,
    pub ts: i64,
    pub level: &'static str,
    pub status: i64,
    pub service: &'static str,
    pub event: &'static str,
    pub user_id: i64,
    pub duration_ms: f64,
    pub trace_id: String,
    pub method: &'static str,
    pub path: String,
    pub ok: bool,
    pub msg: &'static str,
}

const LEVELS: &[&str] = &["INFO", "INFO", "INFO", "INFO", "INFO", "WARN", "WARN", "DEBUG", "ERROR"];
const SERVICES: &[&str] = &["auth", "api", "payments", "search", "cart", "gateway"];
const EVENTS: &[&str] = &["login_ok", "login_failed", "checkout", "payment_declined", "search", "view", "add_to_cart"];
const METHODS: &[&str] = &["GET", "GET", "GET", "POST", "PUT", "DELETE"];
const MSGS: &[&str] = &["request handled", "retry", "upstream timeout", "cache miss", "user not found", "ok"];

/// Picks a template index so the top 10 hold about 80% of the mass: a Zipf-ish split
/// where 80% of the draws go uniformly to the top 10 and the rest uniformly to the
/// remaining `t - 10`.
pub fn pick_template(rng: &mut StdRng, t: usize) -> u32 {
    if t <= 10 {
        return rng.gen_range(0..t) as u32;
    }
    if rng.gen::<f64>() < 0.8 {
        rng.gen_range(0..10) as u32
    } else {
        rng.gen_range(10..t) as u32
    }
}

pub struct Gen {
    rng: StdRng,
    t: usize,
    i: i64,
}

impl Gen {
    pub fn new(t: usize) -> Gen {
        Gen { rng: StdRng::seed_from_u64(SEED), t, i: 0 }
    }
    pub fn with_seed(t: usize, seed: u64) -> Gen {
        Gen { rng: StdRng::seed_from_u64(seed), t, i: 0 }
    }
    pub fn next_row(&mut self) -> Row {
        let rng = &mut self.rng;
        let template = pick_template(rng, self.t);
        self.i += 1;
        let status = match rng.gen_range(0..100) {
            0..=84 => 200,
            85..=89 => 404,
            90..=93 => 301,
            94..=96 => 500,
            _ => 503,
        };
        let level = if status >= 500 && rng.gen::<f64>() < 0.7 { "ERROR" } else { LEVELS[rng.gen_range(0..LEVELS.len())] };
        Row {
            template,
            ts: 1_707_000_000_000 + self.i * 7 + rng.gen_range(0..5),
            level,
            status,
            service: SERVICES[rng.gen_range(0..SERVICES.len())],
            event: EVENTS[rng.gen_range(0..EVENTS.len())],
            user_id: rng.gen_range(1..5_000_000),
            duration_ms: rng.gen::<f64>() * 4000.0,
            trace_id: format!("{:08x}-{:04x}-{:04x}-{:012x}", rng.gen::<u32>(), rng.gen::<u16>(), rng.gen::<u16>(), rng.gen::<u64>() & 0xffff_ffff_ffff),
            method: METHODS[rng.gen_range(0..METHODS.len())],
            path: format!("/api/v{}/items/{}", rng.gen_range(1..4), rng.gen_range(0..100_000)),
            ok: status < 400,
            msg: MSGS[rng.gen_range(0..MSGS.len())],
        }
    }
    pub fn rows(&mut self, n: usize) -> Vec<Row> {
        (0..n).map(|_| self.next_row()).collect()
    }
}

impl Row {
    /// The NDJSON line this row would arrive as. About 200 bytes.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"ts\":{},\"level\":\"{}\",\"status\":{},\"service\":\"{}\",\"event\":\"{}\",\"user_id\":{},\"duration_ms\":{:.2},\"trace_id\":\"{}\",\"method\":\"{}\",\"path\":\"{}\",\"ok\":{},\"msg\":\"{}\",\"t\":{}}}",
            self.ts, self.level, self.status, self.service, self.event, self.user_id, self.duration_ms, self.trace_id, self.method, self.path, self.ok, self.msg, self.template
        )
    }
    /// The row as 12 generic values, in column order.
    pub fn values(&self) -> Vec<Value> {
        vec![
            Value::Int(self.ts),
            Value::Str(self.level.to_string()),
            Value::Int(self.status),
            Value::Str(self.service.to_string()),
            Value::Str(self.event.to_string()),
            Value::Int(self.user_id),
            Value::Float(self.duration_ms),
            Value::Str(self.trace_id.clone()),
            Value::Str(self.method.to_string()),
            Value::Str(self.path.clone()),
            Value::Bool(self.ok),
            Value::Str(self.msg.to_string()),
        ]
    }
}

pub const COLUMN_NAMES: [&str; COLUMNS] = [
    "ts", "level", "status", "service", "event", "user_id", "duration_ms", "trace_id", "method", "path", "ok", "msg",
];

/// Column-oriented storage of a batch of rows: one Vec per column.
#[derive(Default, Clone)]
pub struct Columns {
    pub ts: Vec<i64>,
    pub level: Vec<String>,
    pub status: Vec<i64>,
    pub service: Vec<String>,
    pub event: Vec<String>,
    pub user_id: Vec<i64>,
    pub duration_ms: Vec<f64>,
    pub trace_id: Vec<String>,
    pub method: Vec<String>,
    pub path: Vec<String>,
    pub ok: Vec<bool>,
    pub msg: Vec<String>,
}

impl Columns {
    pub fn with_capacity(n: usize) -> Columns {
        Columns {
            ts: Vec::with_capacity(n),
            level: Vec::with_capacity(n),
            status: Vec::with_capacity(n),
            service: Vec::with_capacity(n),
            event: Vec::with_capacity(n),
            user_id: Vec::with_capacity(n),
            duration_ms: Vec::with_capacity(n),
            trace_id: Vec::with_capacity(n),
            method: Vec::with_capacity(n),
            path: Vec::with_capacity(n),
            ok: Vec::with_capacity(n),
            msg: Vec::with_capacity(n),
        }
    }
    pub fn push(&mut self, r: &Row) {
        self.ts.push(r.ts);
        self.level.push(r.level.to_string());
        self.status.push(r.status);
        self.service.push(r.service.to_string());
        self.event.push(r.event.to_string());
        self.user_id.push(r.user_id);
        self.duration_ms.push(r.duration_ms);
        self.trace_id.push(r.trace_id.clone());
        self.method.push(r.method.to_string());
        self.path.push(r.path.clone());
        self.ok.push(r.ok);
        self.msg.push(r.msg.to_string());
    }
    pub fn len(&self) -> usize {
        self.ts.len()
    }
    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }
    pub fn clear(&mut self) {
        self.ts.clear(); self.level.clear(); self.status.clear(); self.service.clear();
        self.event.clear(); self.user_id.clear(); self.duration_ms.clear(); self.trace_id.clear();
        self.method.clear(); self.path.clear(); self.ok.clear(); self.msg.clear();
    }
    /// Removes and returns the first `n` rows, keeping the rest in place.
    pub fn drain_first(&mut self, n: usize) -> Columns {
        let n = n.min(self.len());
        Columns {
            ts: self.ts.drain(..n).collect(),
            level: self.level.drain(..n).collect(),
            status: self.status.drain(..n).collect(),
            service: self.service.drain(..n).collect(),
            event: self.event.drain(..n).collect(),
            user_id: self.user_id.drain(..n).collect(),
            duration_ms: self.duration_ms.drain(..n).collect(),
            trace_id: self.trace_id.drain(..n).collect(),
            method: self.method.drain(..n).collect(),
            path: self.path.drain(..n).collect(),
            ok: self.ok.drain(..n).collect(),
            msg: self.msg.drain(..n).collect(),
        }
    }
    pub fn shrink(&mut self) {
        self.ts.shrink_to_fit(); self.level.shrink_to_fit(); self.status.shrink_to_fit(); self.service.shrink_to_fit();
        self.event.shrink_to_fit(); self.user_id.shrink_to_fit(); self.duration_ms.shrink_to_fit(); self.trace_id.shrink_to_fit();
        self.method.shrink_to_fit(); self.path.shrink_to_fit(); self.ok.shrink_to_fit(); self.msg.shrink_to_fit();
    }
    /// Heap bytes reserved by the vectors themselves (not the strings they hold).
    pub fn reserved_bytes(&self) -> usize {
        self.ts.capacity() * 8 + self.level.capacity() * 24 + self.status.capacity() * 8 + self.service.capacity() * 24
            + self.event.capacity() * 24 + self.user_id.capacity() * 8 + self.duration_ms.capacity() * 8 + self.trace_id.capacity() * 24
            + self.method.capacity() * 24 + self.path.capacity() * 24 + self.ok.capacity() + self.msg.capacity() * 24
    }
    /// Inverse of `to_bytes`.
    pub fn from_bytes(bytes: &[u8]) -> Columns {
        let text = std::str::from_utf8(bytes).unwrap();
        let mut out = Columns::default();
        let mut it = text.split('\n');
        loop {
            let Some(ts) = it.next() else { break };
            if ts.is_empty() { break; }
            out.ts.push(ts.parse().unwrap());
            out.level.push(it.next().unwrap().to_string());
            out.status.push(it.next().unwrap().parse().unwrap());
            out.service.push(it.next().unwrap().to_string());
            out.event.push(it.next().unwrap().to_string());
            out.user_id.push(it.next().unwrap().parse().unwrap());
            out.duration_ms.push(it.next().unwrap().parse().unwrap());
            out.trace_id.push(it.next().unwrap().to_string());
            out.method.push(it.next().unwrap().to_string());
            out.path.push(it.next().unwrap().to_string());
            out.ok.push(it.next().unwrap() == "true");
            out.msg.push(it.next().unwrap().to_string());
        }
        out
    }
    pub fn append(&mut self, o: &Columns) {
        self.ts.extend_from_slice(&o.ts); self.level.extend_from_slice(&o.level); self.status.extend_from_slice(&o.status);
        self.service.extend_from_slice(&o.service); self.event.extend_from_slice(&o.event); self.user_id.extend_from_slice(&o.user_id);
        self.duration_ms.extend_from_slice(&o.duration_ms); self.trace_id.extend_from_slice(&o.trace_id); self.method.extend_from_slice(&o.method);
        self.path.extend_from_slice(&o.path); self.ok.extend_from_slice(&o.ok); self.msg.extend_from_slice(&o.msg);
    }
    /// A crude serialisation of the whole batch, for the benchmarks that need bytes to
    /// compress or write: every column concatenated, values separated by a newline.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len() * 200);
        use std::io::Write;
        for i in 0..self.len() {
            let _ = write!(out, "{}\n{}\n{}\n{}\n{}\n{}\n{:.2}\n{}\n{}\n{}\n{}\n{}\n",
                self.ts[i], self.level[i], self.status[i], self.service[i], self.event[i], self.user_id[i],
                self.duration_ms[i], self.trace_id[i], self.method[i], self.path[i], self.ok[i], self.msg[i]);
        }
        out
    }
}
