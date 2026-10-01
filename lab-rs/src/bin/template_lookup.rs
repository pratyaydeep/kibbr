//! B8 - finding the templates that contain a key.
//!
//! At 1,000 / 10,000 / 100,000 templates, each holding 8 to 16 keys drawn (skewed)
//! from a pool of 2,000 key ids: check every template's sorted key list, versus a map
//! from key to the templates that contain it. Lookups per second over 10,000 lookups
//! with the same skew as the templates.
//!
//! Simplifies: keys are integers, templates are static, and no query does anything with
//! the answer. Decides whether an inverted key->templates map is needed and from what
//! template count.

use rand::prelude::*;
use rand::rngs::StdRng;
use rand_distr::Zipf;
use std::collections::HashMap;
use std::time::Instant;
use kibbr_lab_rs::*;

const POOL: u64 = 2000;
const LOOKUPS: usize = 10_000;

fn main() {
    println!("B8 template_lookup: key pool {POOL}, 8-16 keys per template, {LOOKUPS} lookups; 3 runs");
    let mut rec = Recorder::new("B8_template_lookup");
    let mut out = Vec::new();
    for t in [1_000usize, 10_000, 100_000] {
        let mut rng = StdRng::seed_from_u64(42);
        let z = Zipf::new(POOL, 1.1).unwrap();
        let templates: Vec<Vec<u32>> = (0..t)
            .map(|_| {
                let n = rng.gen_range(8..=16);
                let mut keys: Vec<u32> = (0..n).map(|_| z.sample(&mut rng) as u32 - 1).collect();
                keys.sort_unstable();
                keys.dedup();
                keys
            })
            .collect();
        let mut index: HashMap<u32, Vec<u32>> = HashMap::new();
        for (i, keys) in templates.iter().enumerate() {
            for k in keys {
                index.entry(*k).or_default().push(i as u32);
            }
        }
        let queries: Vec<u32> = (0..LOOKUPS).map(|_| z.sample(&mut rng) as u32 - 1).collect();
        let mut sizes = vec![];
        for (name, f) in [
            ("check_every_template", Box::new(|k: u32| templates.iter().enumerate().filter(|(_, keys)| keys.binary_search(&k).is_ok()).map(|(i, _)| i as u32).collect::<Vec<u32>>()) as Box<dyn Fn(u32) -> Vec<u32>>),
            ("key_to_templates_map", Box::new(|k: u32| index.get(&k).cloned().unwrap_or_default())),
        ] {
            let mut rates = vec![];
            let mut found = 0usize;
            for _ in 0..3 {
                let t0 = Instant::now();
                found = 0;
                for q in &queries {
                    found += f(*q).len();
                }
                rates.push(LOOKUPS as f64 / t0.elapsed().as_secs_f64());
            }
            sizes.push(found);
            let s = Stat::of(&rates);
            let v = format!("{t}_templates/{name}");
            record(&mut rec, &v, "lookups_per_sec", s, "lookups_per_sec", &format!("avg {} templates per answer", found / LOOKUPS));
            out.push(vec![t.to_string(), name.into(), format!("{} ±{:.0}%", fmt_rate(s.median), s.spread_pct()), (found / LOOKUPS).to_string()]);
        }
        assert_eq!(sizes[0], sizes[1]);
    }
    table(&["templates", "variant", "lookups/s", "avg answer size"], &out);
    rec.flush();
}
