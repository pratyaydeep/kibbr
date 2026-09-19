//! B5 - how a filter should be evaluated.
//!
//! 1,000,000 rows, filter `level == "ERROR" and status >= 500`. Three shapes:
//!   rows_hashmap_closure   rows as HashMap<String, Value>, a boxed closure per row
//!   columns_closure        column vectors, the same boxed-closure shape, called per row
//!   columns_plain_data     column vectors, the filter as an enum evaluated column by
//!                          column into a bitmap
//!
//! Simplifies: everything is in memory and decoded, one filter, no nulls. Decides the
//! evaluation model for the scan.

use std::collections::HashMap;
use std::time::Instant;
use triedb_lab_rs::workload::{Columns, Gen, Value, COLUMN_NAMES};
use triedb_lab_rs::*;

const ROWS: usize = 1_000_000;

enum Pred {
    StrEq(&'static str, &'static str),
    IntGe(&'static str, i64),
}
enum Filter {
    And(Vec<Pred>),
}

fn eval_column(cols: &Columns, p: &Pred, out: &mut [bool]) {
    match p {
        Pred::StrEq(c, v) => {
            let col: &Vec<String> = match *c {
                "level" => &cols.level,
                "service" => &cols.service,
                "event" => &cols.event,
                _ => unreachable!(),
            };
            for (o, x) in out.iter_mut().zip(col) {
                *o = *o && x == v;
            }
        }
        Pred::IntGe(c, v) => {
            let col: &Vec<i64> = match *c {
                "status" => &cols.status,
                "user_id" => &cols.user_id,
                _ => unreachable!(),
            };
            for (o, x) in out.iter_mut().zip(col) {
                *o = *o && *x >= *v;
            }
        }
    }
}

fn main() {
    let mut g = Gen::new(500);
    let rows = g.rows(ROWS);
    let maps: Vec<HashMap<String, Value>> = rows
        .iter()
        .map(|r| COLUMN_NAMES.iter().map(|s| s.to_string()).zip(r.values()).collect())
        .collect();
    let mut cols = Columns::with_capacity(ROWS);
    for r in &rows {
        cols.push(r);
    }
    drop(rows);
    println!("B5 filters: {ROWS} rows, level == \"ERROR\" and status >= 500; 3 runs of 40 passes each");
    let mut rec = Recorder::new("B5_filters");
    let mut out = Vec::new();

    let by_map: Box<dyn Fn(&HashMap<String, Value>) -> bool> = Box::new(|m| {
        matches!(m.get("level"), Some(Value::Str(s)) if s == "ERROR") && matches!(m.get("status"), Some(Value::Int(x)) if *x >= 500)
    });
    let by_col: Box<dyn Fn(&Columns, usize) -> bool> = Box::new(|c, i| c.level[i] == "ERROR" && c.status[i] >= 500);
    let plain = Filter::And(vec![Pred::StrEq("level", "ERROR"), Pred::IntGe("status", 500)]);

    let mut expected = None;
    let mut bench = |name: &str, f: &mut dyn FnMut() -> usize, rec: &mut Recorder, out: &mut Vec<Vec<String>>| {
        let mut rates = vec![];
        f(); // warm-up: the first pass over 1M rows pays the page faults
        for _ in 0..3 {
            let t = Instant::now();
            let mut n = 0;
            for _ in 0..40 {
                n = f();
            }
            rates.push(40.0 * ROWS as f64 / t.elapsed().as_secs_f64());
            match expected {
                None => expected = Some(n),
                Some(e) => assert_eq!(e, n, "variants disagree on the match count"),
            }
        }
        let s = Stat::of(&rates);
        record(rec, name, "rows_per_sec", s, "rows_per_sec", &format!("{} matches", expected.unwrap()));
        out.push(vec![name.into(), format!("{} ±{:.0}%", fmt_rate(s.median), s.spread_pct()), expected.unwrap().to_string()]);
    };
    bench("rows_hashmap_closure", &mut || maps.iter().filter(|m| by_map(m)).count(), &mut rec, &mut out);
    bench("columns_closure_per_row", &mut || (0..cols.len()).filter(|&i| by_col(&cols, i)).count(), &mut rec, &mut out);
    bench(
        "columns_plain_data_column_at_a_time",
        &mut || {
            let mut bits = vec![true; cols.len()];
            let Filter::And(ps) = &plain;
            for p in ps {
                eval_column(&cols, p, &mut bits);
            }
            bits.iter().filter(|b| **b).count()
        },
        &mut rec,
        &mut out,
    );
    table(&["variant", "rows/s", "matches"], &out);
    rec.flush();
}
