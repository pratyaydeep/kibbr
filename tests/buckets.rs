//! `GROUP BY time(...)` as an operator sees it: what the engine cannot see about itself.
//! An empty hour reaches the terminal as zeros, a refusal is a refusal and not a short
//! chart, and the two surfaces and the three formats agree about the same buckets.

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

/// Three hours from 09:00, at four minutes past every minute-multiple of four, with
/// **10:00-11:00 deliberately silent**. Two levels, so a second group key has two series.
fn fixture() -> String {
    let mut out = String::new();
    for minute in (0..180).step_by(4) {
        if (60..120).contains(&minute) {
            continue;
        }
        let level = if minute % 8 == 0 { "ERROR" } else { "WARN" };
        out.push_str(&format!(
            "{{\"ts\":\"2024-01-15T{:02}:{:02}:00Z\",\"level\":\"{level}\",\"event\":\"tick\"}}\n",
            9 + minute / 60,
            minute % 60
        ));
    }
    // A second template with no `ts` column at all - the other of the two empty answers
    // ("found, and nothing matched"), and the only source of a `bucket: null` group.
    out.push_str("{\"event\":\"beat\",\"node\":\"a\"}\n");
    out
}

const WINDOW: [&str; 2] = ["2024-01-15T09:00:00Z", "2024-01-15T12:00:00Z"];

fn run(args: &[&str]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kibbr"))
        .arg("query")
        .arg("-")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    let body = fixture();
    std::thread::spawn(move || stdin.write_all(body.as_bytes()));
    child.wait_with_output().expect("failed to run kibbr")
}

fn query(args: &[&str]) -> String {
    let out = run(args);
    assert!(
        out.status.success(),
        "kibbr query {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

fn json(args: &[&str]) -> Value {
    let mut with_format = args.to_vec();
    with_format.extend(["--format", "json"]);
    serde_json::from_str(&query(&with_format)).expect("query did not emit JSON")
}

/// `(bucket, count)` for every group in the answer, in the order it came back.
fn buckets(result: &Value) -> Vec<(String, u64)> {
    result["groups"]
        .as_array()
        .expect("groups is an array")
        .iter()
        .map(|group| {
            (
                match &group["bucket"] {
                    Value::Null => "(null)".to_string(),
                    at => at.as_str().expect("a bucket is a string").to_string(),
                },
                group["count"].as_u64().expect("a count"),
            )
        })
        .collect()
}

/// An hour with no rows in it reads as zeros, not as an hour that is not there: a gap
/// and a zero are different statements.
#[test]
fn an_empty_hour_reads_as_zeros() {
    let result = json(&[&format!(
        "COUNT LOG {{}} BETWEEN {} AND {} GROUP BY time(5m)",
        WINDOW[0], WINDOW[1]
    )]);
    let got = buckets(&result);

    // Three hours at 5m, with nothing missing from the middle.
    assert_eq!(got.len(), 36, "{got:?}");
    for (n, (bucket, _)) in got.iter().enumerate() {
        let minute = n * 5;
        assert_eq!(
            bucket,
            &format!("2024-01-15T{:02}:{:02}:00Z", 9 + minute / 60, minute % 60),
            "bucket {n} is misplaced"
        );
    }

    let (quiet, loud): (Vec<_>, Vec<_>) = got
        .iter()
        .partition(|(bucket, _)| bucket.starts_with("2024-01-15T10:"));
    assert_eq!(quiet.len(), 12);
    assert!(quiet.iter().all(|(_, n)| *n == 0), "{quiet:?}");
    assert!(loud.iter().all(|(_, n)| *n > 0), "{loud:?}");

    // And they are zeros in the count, not rows quietly dropped from it.
    let summed: u64 = got.iter().map(|(_, n)| n).sum();
    assert_eq!(summed, result["count"].as_u64().expect("a count"));
}

/// The table an operator reads, since that is where the zeros have to appear.
#[test]
fn the_table_prints_the_empty_buckets() {
    let text = query(&[&format!(
        "COUNT LOG {{}} BETWEEN {} AND {} GROUP BY time(5m)",
        WINDOW[0], WINDOW[1]
    )]);
    let rows: Vec<&str> = text.lines().collect();
    assert_eq!(
        rows[0].split_whitespace().collect::<Vec<_>>(),
        ["bucket", "count"]
    );
    for minute in (0..60).step_by(5) {
        let want = format!("2024-01-15T10:{minute:02}:00Z  0");
        assert!(text.contains(&want), "missing `{want}` in:\n{text}");
    }
    assert!(text.contains("36 groups"), "{text}");
}

/// Boundaries are floored against the epoch, so a chart does not shift when the range is
/// nudged.
#[test]
fn boundaries_do_not_move_when_the_range_is_nudged() {
    let counts = |from: &str, to: &str| -> Vec<(String, u64)> {
        buckets(&json(&[&format!(
            "COUNT LOG {{}} BETWEEN {from} AND {to} GROUP BY time(15m)"
        )]))
    };

    let wide = counts(WINDOW[0], WINDOW[1]);
    // Ranges nudged by less than a bucket, in both directions, and one starting mid-bucket.
    for (from, to) in [
        ("2024-01-15T09:01:00Z", "2024-01-15T11:59:00Z"),
        ("2024-01-15T09:07:30Z", "2024-01-15T11:52:30Z"),
        ("2024-01-15T09:14:59Z", "2024-01-15T11:45:01Z"),
    ] {
        let nudged = counts(from, to);
        // Every bucket the nudged range reports is one the wide range also reports, at the
        // same instant. Only the two partial buckets at the edges may hold fewer rows.
        for (bucket, count) in &nudged {
            let (_, same) = wide
                .iter()
                .find(|(at, _)| at == bucket)
                .unwrap_or_else(|| panic!("{from}..{to}: {bucket} is off the grid"));
            let edge = bucket == &nudged[0].0 || bucket == &nudged[nudged.len() - 1].0;
            if !edge {
                assert_eq!(count, same, "{from}..{to}: {bucket} disagrees");
            }
        }
        assert!(nudged.iter().all(|(at, _)| {
            let minute: u32 = at[14..16].parse().expect("a minute");
            minute.is_multiple_of(15)
        }));
    }
}

/// Every series gets a point in every bucket. A chart with two lines has two lines in the
/// silent hour too - each of them flat on zero, which is a thing that happened.
#[test]
fn a_second_key_is_zero_filled_for_every_series() {
    let result = json(&[&format!(
        "COUNT LOG {{}} BETWEEN {} AND {} GROUP BY time(30m), level",
        WINDOW[0], WINDOW[1]
    )]);
    let groups = result["groups"].as_array().expect("groups is an array");

    // 6 half-hours x 2 levels, and the pairs are unique.
    assert_eq!(groups.len(), 12, "{groups:?}");
    let quiet: Vec<&Value> = groups
        .iter()
        .filter(|g| g["bucket"].as_str().is_some_and(|b| b.contains("T10:")))
        .collect();
    assert_eq!(quiet.len(), 4, "two half-hours, two levels");
    assert!(
        quiet.iter().all(|g| g["count"] == 0),
        "the silent hour is not flat on zero for both levels: {quiet:?}"
    );
    for level in ["ERROR", "WARN"] {
        assert!(
            quiet.iter().any(|g| g["keys"]["level"] == level),
            "{level} has no point in the silent hour"
        );
    }
}

/// A template with no time column is on arrival time, which is outside a window over 2024:
/// found, counts zero, and the window's buckets zero-filled rather than none.
#[test]
fn a_template_with_no_ts_column_is_on_arrival_time() {
    let result = json(&[&format!(
        "COUNT LOG {{\"event\": \"beat\"}} BETWEEN {} AND {} GROUP BY time(5m)",
        WINDOW[0], WINDOW[1]
    )]);
    assert_eq!(result["exists"], Value::Bool(true));
    assert_eq!(result["count"], 0);
    let got = buckets(&result);
    assert_eq!(got.len(), 36, "three hours at five minutes: {got:?}");
    assert!(got.iter().all(|(b, n)| b != "(null)" && *n == 0), "{got:?}");
}

/// With no window, rows on arrival time land in the bucket their arrival falls in, not in
/// a `null` bucket of their own; days, because the axis runs from 2024 to today.
#[test]
fn rows_on_arrival_time_land_in_the_newest_bucket() {
    let result = json(&["COUNT LOG {} GROUP BY time(1d)"]);
    let got = buckets(&result);

    assert!(got.iter().all(|(b, _)| b != "(null)"), "{got:?}");
    assert_eq!(
        got.last().map(|(_, n)| *n),
        Some(1),
        "the one `beat` row, today"
    );
    assert_eq!(
        got.iter().map(|(_, n)| n).sum::<u64>(),
        result["count"].as_u64().expect("a count"),
        "the groups no longer sum to the count: {got:?}"
    );
    // NDJSON: every record carries a bucket.
    let stream = query(&["COUNT LOG {} GROUP BY time(1d)", "--format", "ndjson"]);
    let first: Value = serde_json::from_str(stream.lines().next().expect("a line")).expect("json");
    assert_ne!(first["bucket"], Value::Null);
}

/// Refused, not truncated. A chart missing its tail still looks like a chart, and nothing
/// on it says which half is gone - so the error names both counts and draws nothing.
#[test]
fn too_many_buckets_is_refused_with_both_counts() {
    let out = run(&["COUNT LOG {} SINCE 30d GROUP BY time(1s)"]);
    assert!(!out.status.success(), "an over-cap query must not succeed");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("2,592,0"), "{stderr}");
    assert!(stderr.contains("10,000"), "{stderr}");
    assert!(stderr.contains("time(1s)"), "{stderr}");
    assert!(out.stdout.is_empty(), "it drew something anyway");

    // The cap is a setting, not a constant: raised, the same query is answered.
    let widened = json(&[
        "COUNT LOG {} SINCE 30d GROUP BY time(1d)",
        "--max-buckets",
        "31",
    ]);
    assert!(widened["groups"].as_array().expect("groups").len() <= 31);

    // And the group cross product has its own cap, since a second key multiplies past the
    // bucket one. 36 buckets x 2 levels is 72, which 50 does not fit.
    let dense = run(&[
        &format!(
            "COUNT LOG {{}} BETWEEN {} AND {} GROUP BY time(5m), level",
            WINDOW[0], WINDOW[1]
        ),
        "--max-groups",
        "50",
    ]);
    assert!(!dense.status.success());
    let stderr = String::from_utf8_lossy(&dense.stderr);
    assert!(stderr.contains("50"), "{stderr}");
}

/// The flag surface and the grammar say the same thing, as they must for every other
/// clause: one `Query` is built either way.
#[test]
fn the_flag_and_the_grammar_agree() {
    let window = format!("BETWEEN {} AND {}", WINDOW[0], WINDOW[1]);
    let from_grammar = query(&[
        &format!("COUNT LOG {{}} {window} GROUP BY time(5m), level"),
        "--format",
        "json",
    ]);
    let from_flags = query(&[
        "--group-by",
        "time(5m),level",
        "--between",
        WINDOW[0],
        WINDOW[1],
        "--format",
        "json",
    ]);
    let strip = |text: &str| {
        let value: Value = serde_json::from_str(text).expect("json");
        value["groups"].clone()
    };
    assert_eq!(strip(&from_grammar), strip(&from_flags));
}

/// The plan says where the boundaries fall and how many there will be, before the query
/// runs - which is the one thing about a bucketed query that cannot be read off its text.
#[test]
fn the_plan_names_the_bucket_count() {
    let text = query(&[
        &format!(
            "COUNT LOG {{}} BETWEEN {} AND {} GROUP BY time(5m)",
            WINDOW[0], WINDOW[1]
        ),
        "--explain",
    ]);
    assert!(text.contains("36 buckets of 5m"), "{text}");
    assert!(text.contains("from the epoch"), "{text}");
    assert!(text.contains("mode group by time(5m)"), "{text}");
    // `--explain` executes nothing, so there is a plan and no answer under it.
    assert!(!text.contains("groups,"), "{text}");
    assert!(!text.contains("rows scanned"), "{text}");
}
