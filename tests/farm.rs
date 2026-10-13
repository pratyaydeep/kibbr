// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `gen-logs`: the hostile profile's bytes are frozen, and the farm has the properties
//! the storage measurements are going to lean on. Everything runs the real binary.

use std::collections::HashSet;
use std::process::{Command, Stdio};

fn kibbr() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kibbr"))
}

fn run(args: &[&str]) -> String {
    let out = kibbr().args(args).output().expect("failed to run kibbr");
    assert!(
        out.status.success(),
        "kibbr {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

fn refused(args: &[&str]) -> String {
    let out = kibbr().args(args).output().expect("failed to run kibbr");
    assert!(!out.status.success(), "kibbr {args:?} was accepted");
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// `parse --json` over `input`; stderr must be empty, so a rejected line fails the test.
fn parse_json(input: &str) -> Vec<serde_json::Value> {
    let mut child = kibbr()
        .args(["parse", "-", "--json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run kibbr parse");
    let mut stdin = child.stdin.take().expect("stdin was piped");
    let input = input.as_bytes().to_vec();
    let writer = std::thread::spawn(move || {
        use std::io::Write;
        stdin.write_all(&input).expect("write to parse");
    });
    let out = child.wait_with_output().expect("parse did not finish");
    writer.join().expect("writer thread");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success() && stderr.is_empty(), "parse: {stderr}");
    String::from_utf8(out.stdout)
        .expect("utf-8")
        .lines()
        .map(|l| serde_json::from_str(l).expect("parse --json wrote a record"))
        .collect()
}

fn farm(lines: usize, extra: &[&str]) -> String {
    let n = lines.to_string();
    let mut args = vec!["gen-logs", "--profile", "farm", "--lines", &n];
    args.extend_from_slice(extra);
    run(&args)
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The instant a farm line was stamped with, in milliseconds, whichever service wrote it.
/// The access log's `time` is to the second, so its `msec` is the one read.
fn stamp_ms(line: &serde_json::Value) -> i64 {
    if let Some(ms) = line.get("ts_ms").and_then(|v| v.as_i64()) {
        return ms;
    }
    if let Some(msec) = line.get("msec").and_then(|v| v.as_str()) {
        let (s, frac) = msec.split_once('.').expect("msec has a fraction");
        return s.parse::<i64>().unwrap() * 1000 + frac.parse::<i64>().unwrap();
    }
    let text = ["@timestamp", "time", "ts"]
        .iter()
        .find_map(|k| line.get(*k).and_then(|v| v.as_str()))
        .unwrap_or_else(|| panic!("no time field: {line}"));
    let t = chrono::DateTime::parse_from_rfc3339(text).expect("an RFC 3339 time");
    t.timestamp_millis()
}

/// Fixtures and snapshots were made from these bytes. Any change to the generator that
/// moves them is a change to every test built on them, and has to be made on purpose.
#[test]
fn the_hostile_profile_is_frozen() {
    let default = run(&["gen-logs", "--lines", "2000", "--seed", "42"]);
    assert_eq!(default.len(), 470_135);
    assert_eq!(fnv1a(default.as_bytes()), 0x523d_e2ba_5e26_2072);
    let named = run(&[
        "gen-logs",
        "--profile",
        "hostile",
        "--lines",
        "2000",
        "--seed",
        "42",
    ]);
    assert_eq!(
        default, named,
        "--profile hostile is not the default's bytes"
    );
    let twelve = run(&[
        "gen-logs",
        "--lines",
        "2000",
        "--seed",
        "42",
        "--templates",
        "12",
    ]);
    assert_eq!(default, twelve, "--templates 12 is not the default's bytes");
}

#[test]
fn farm_is_reproducible() {
    let a = farm(3000, &["--seed", "42"]);
    let b = farm(3000, &["--seed", "42"]);
    assert_eq!(a, b, "same seed, different bytes");
    let c = farm(3000, &["--seed", "43"]);
    assert_ne!(a, c, "different seeds, same bytes");
}

#[test]
fn farm_writes_exactly_the_lines_asked_for() {
    for n in [0usize, 1, 7, 1000, 12_345] {
        let out = farm(n, &[]);
        assert_eq!(out.lines().count(), n, "--lines {n}");
        assert!(n == 0 || out.ends_with('\n'));
    }
}

/// The crash test takes a line as its own identity, so this is a requirement, not a
/// statistic: every line names its host and no host is given one millisecond twice.
#[test]
fn no_two_farm_lines_are_equal() {
    for seed in ["1", "42"] {
        let out = farm(60_000, &["--seed", seed]);
        let distinct: HashSet<&str> = out.lines().collect();
        assert_eq!(distinct.len(), 60_000, "--seed {seed} repeated a line");
    }
}

#[test]
fn farm_is_one_day_in_time_order() {
    let out = farm(20_000, &["--day", "2023-03-26"]);
    let day_start = 1_679_788_800_000i64; // 2023-03-26T00:00:00Z
    let mut last = i64::MIN;
    for (i, text) in out.lines().enumerate() {
        let line: serde_json::Value = serde_json::from_str(text).expect("farm wrote JSON");
        let t = stamp_ms(&line);
        assert!(
            (day_start..day_start + 86_400_000).contains(&t),
            "line {} is not on 2023-03-26: {text}",
            i + 1
        );
        assert!(
            t >= last,
            "line {} is earlier than the one before it: {text}",
            i + 1
        );
        last = t;
    }
}

/// Nothing the generator draws depends on the date, so another day is the same lines on
/// another date: every template in the same place.
#[test]
fn the_day_moves_the_dates_and_nothing_else() {
    let a = parse_json(&farm(3000, &[]));
    let b = parse_json(&farm(3000, &["--day", "2025-11-02"]));
    let ids = |v: &[serde_json::Value]| {
        v.iter()
            .map(|r| r["fingerprint"].clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&a), ids(&b));
}

/// Where the dirty values go, and that `--dirty-rate` moves them and nothing else.
#[test]
fn dirty_values_land_on_the_named_keys_only() {
    let clean = farm(40_000, &["--dirty-rate", "0"]);
    let dirty = farm(40_000, &["--dirty-rate", "20"]);
    let named = ["user_id", "amount", "duration_ms", "rows", "template"];
    let mut hits = 0;
    for (c, d) in clean.lines().zip(dirty.lines()) {
        let c: serde_json::Value = serde_json::from_str(c).unwrap();
        let d: serde_json::Value = serde_json::from_str(d).unwrap();
        let (c, d) = (c.as_object().unwrap(), d.as_object().unwrap());
        assert_eq!(
            c.keys().collect::<Vec<_>>(),
            d.keys().collect::<Vec<_>>(),
            "the key set moved"
        );
        for (k, cv) in c {
            let dv = &d[k];
            if cv != dv {
                assert!(named.contains(&k.as_str()), "{k} changed: {cv} -> {dv}");
                let placeholder = dv == "" || dv == "N/A";
                let number_for_text = cv.is_string() && dv.is_number();
                assert!(placeholder || number_for_text, "{k}: {cv} -> {dv}");
                hits += 1;
            }
            if named.contains(&k.as_str()) && k != "template" {
                assert!(cv != "" && cv != "N/A", "--dirty-rate 0 wrote {k}={cv}");
            }
        }
    }
    assert!(hits > 100, "only {hits} dirty values at 20%");
}

#[test]
fn services_limit_the_estate() {
    let one = farm(2000, &["--services", "1"]);
    assert!(
        one.lines().all(|l| l.contains("\"ecs.version\"")),
        "--services 1 is the gateway only"
    );
    let three = farm(5000, &["--services", "3"]);
    for l in three.lines() {
        let gateway = l.contains("\"ecs.version\"");
        let auth = l.contains("\"logger\":\"auth.");
        let payments = l.contains("\"@version\"");
        assert!(gateway || auth || payments, "--services 3 wrote {l}");
    }
    assert!(
        three.contains("\"@version\""),
        "--services 3 has no payments line"
    );
}

#[test]
fn farm_flags_belong_to_farm() {
    for (args, says) in [
        (
            vec!["gen-logs", "--day", "2024-01-01"],
            "--day applies only to --profile farm",
        ),
        (
            vec!["gen-logs", "--services", "3"],
            "--services applies only to --profile farm",
        ),
        (
            vec!["gen-logs", "--dirty-rate", "1"],
            "--dirty-rate applies only to --profile farm",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--templates", "5"],
            "--templates applies only to --profile hostile",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--services", "0"],
            "--services 0",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--services", "11"],
            "--services 11",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--dirty-rate", "101"],
            "--dirty-rate 101",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--day", "2024-13-01"],
            "--day `2024-13-01`",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--day", "2024-1-5"],
            "--day `2024-1-5`",
        ),
        // Before 1970 the epoch fields go negative; after 9999 RFC 3339 has no year.
        (
            vec!["gen-logs", "--profile", "farm", "--day", "1969-12-31"],
            "--day `1969-12-31`",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--day", "+10000-01-01"],
            "--day `+10000-01-01`",
        ),
        (
            vec!["gen-logs", "--profile", "farm", "--lines", "1000000001"],
            "at most 1000000000 lines",
        ),
    ] {
        let stderr = refused(&args);
        assert!(stderr.contains(says), "{args:?}: {stderr}");
    }
}

/// Rows pile onto a few templates, some columns carry two types, every template has a time
/// of its own, and its ten-digit order ids fall outside the epoch window.
#[test]
fn farm_has_what_the_hostile_profile_lacks() {
    let records = parse_json(&farm(20_000, &[]));
    let time_keys = ["@timestamp", "time", "ts", "ts_ms"];
    let mut per_template: std::collections::HashMap<String, usize> = Default::default();
    let mut types: std::collections::HashMap<(String, String), HashSet<String>> =
        Default::default();
    for r in &records {
        let t = r["types"].as_object().unwrap();
        assert!(
            time_keys
                .iter()
                .any(|k| t.get(*k).is_some_and(|v| v == "Datetime")),
            "no time source: {}",
            r["fingerprint"]
        );
        let tid = r["template_id"].to_string();
        *per_template.entry(tid.clone()).or_default() += 1;
        for (k, ty) in t {
            if ty != "Null" {
                types
                    .entry((tid.clone(), k.clone()))
                    .or_default()
                    .insert(ty.as_str().unwrap().to_string());
            }
        }
    }
    let mut counts: Vec<usize> = per_template.values().copied().collect();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    let top10: usize = counts.iter().take(10).sum();
    assert!(counts.len() >= 60, "only {} templates", counts.len());
    assert!(
        top10 * 2 > records.len(),
        "the top ten hold only {top10} of {} rows",
        records.len()
    );
    let widened = types.values().filter(|t| t.len() > 1).count();
    assert!(widened > 0, "no column carries two types");
    // A ten-digit id that is not a time, and is not read as one.
    let order_ids: HashSet<&str> = records
        .iter()
        .filter_map(|r| r["types"]["order_id"].as_str())
        .collect();
    assert_eq!(order_ids, HashSet::from(["Int64"]));
}

/// A request's id goes wherever the request goes: a checkout's is in the gateway line, the
/// auth line that checked its token, and the payments lines it caused.
#[test]
fn a_request_id_crosses_services() {
    let out = farm(20_000, &[]);
    let mut seen: std::collections::HashMap<String, HashSet<&str>> = Default::default();
    for text in out.lines() {
        let line: serde_json::Value = serde_json::from_str(text).unwrap();
        let mut note = |id: Option<&str>, svc: &'static str| {
            if let Some(id) = id {
                seen.entry(id.to_string()).or_default().insert(svc);
            }
        };
        note(
            line.pointer("/http/request/id").and_then(|v| v.as_str()),
            "gateway",
        );
        if line["logger"]
            .as_str()
            .is_some_and(|l| l.starts_with("auth."))
        {
            note(line["request_id"].as_str(), "auth");
        }
        note(line["requestId"].as_str(), "payments");
    }
    assert!(
        seen.values().any(|s| s.len() == 3),
        "no request id is in a gateway, an auth and a payments line"
    );
}

/// The scheduler keeps its crontab: every job runs, the daily ones included. When the
/// budget is short it is the busiest job's shards that go, not the rarest jobs.
#[test]
fn every_scheduled_job_runs() {
    let out = farm(60_000, &[]);
    for job in [
        "cleanup-sessions",
        "expire-carts",
        "sync-inventory",
        "rotate-signing-keys",
        "nightly-report",
        "reindex-catalog",
        "rollup-metrics",
    ] {
        let triggered = format!("\"event\":\"triggered\",\"job\":\"{job}\"");
        assert!(out.contains(&triggered), "{job} never ran");
    }
    assert!(
        out.contains("\"event\":\"keys_rotated\""),
        "no key rotation"
    );
}
