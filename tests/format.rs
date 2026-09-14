//! `--format ndjson`: one JSON object per line, no envelope, nothing else on stdout.
//!
//! The shape is the contract - something downstream parses this line by line, so a line
//! that is not a record, or a record that is not a line, is the bug this file is watching
//! for. The other two formats are checked where their own concerns live: `table` in
//! `tests/query.rs`, `json` wherever a differential compares two runs.

use std::process::{Command, Stdio};

use serde_json::Value;

const FIXTURE: &str = include_str!("../testdata/sample_logs.ndjson");

fn triedb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_triedb"))
}

/// stdout and stderr apart, because for this format the difference between them is the
/// point: "no such template" lives on stderr precisely so it stays out of stdout.
fn query(args: &[&str]) -> (String, String) {
    use std::io::Write;

    let mut child = triedb()
        .arg("query")
        .arg("-")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    std::thread::spawn(move || stdin.write_all(FIXTURE.as_bytes()));
    let out = child.wait_with_output().expect("failed to run triedb");
    assert!(
        out.status.success(),
        "triedb query {args:?} exited {:?}",
        out.status.code()
    );
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Every line, parsed. A line that is not an object fails here rather than downstream.
fn objects(stdout: &str) -> Vec<Value> {
    stdout
        .lines()
        .map(|line| {
            let value: Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("not JSON: {line:?}\n{e}"));
            assert!(value.is_object(), "not an object: {line}");
            value
        })
        .collect()
}

/// `MATCH` prints the rows and only the rows - no envelope, no count line, no `-` padding
/// for a column some other template happens to have.
#[test]
fn match_prints_one_row_per_line() {
    let (stdout, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "ndjson"]);
    let rows = objects(&stdout);
    assert_eq!(rows.len(), 2, "{stdout}");

    for row in &rows {
        let obj = row.as_object().expect("an object");
        assert_eq!(obj["service"], Value::from("auth"));
        // The row carries its own template's columns and no others. `pay_id` belongs to
        // the payment template, and the table format would have shown it here as `-`.
        assert!(!obj.contains_key("pay_id"), "{row}");
        assert!(!obj.contains_key("exists"), "the envelope leaked: {row}");
        assert!(!obj.contains_key("meta"), "the envelope leaked: {row}");
    }
}

/// The other three modes print their own records rather than nothing at all.
#[test]
fn every_mode_prints_records() {
    let (count, _) = query(&[r#"COUNT LOG {"service": "auth"}"#, "--format", "ndjson"]);
    assert_eq!(objects(&count), vec![serde_json::json!({"count": 2})]);

    let (exists, _) = query(&["--match", "service=auth", "--exists", "--format", "ndjson"]);
    assert_eq!(objects(&exists), vec![serde_json::json!({"exists": true})]);

    let (groups, _) = query(&[
        r#"COUNT LOG {"service": "auth"} GROUP BY level"#,
        "--format",
        "ndjson",
    ]);
    assert_eq!(
        objects(&groups),
        vec![
            serde_json::json!({"level": "ERROR", "count": 1}),
            serde_json::json!({"level": "INFO", "count": 1}),
        ]
    );
}

/// A group label is the values joined by a separator; each one has to come back out under
/// the key that asked for it, in order, rather than as one joined string.
#[test]
fn a_multi_key_group_splits_back_into_its_keys() {
    let (stdout, _) = query(&[
        r#"COUNT LOG {"service": "auth"} GROUP BY service, level"#,
        "--format",
        "ndjson",
    ]);
    assert_eq!(
        objects(&stdout),
        vec![
            serde_json::json!({"service": "auth", "level": "ERROR", "count": 1}),
            serde_json::json!({"service": "auth", "level": "INFO", "count": 1}),
        ]
    );
}

/// The two empty answers, kept apart without putting an odd-shaped object in the stream.
///
/// "No such template" and "that template exists and nothing matched" both print no rows.
/// They stay distinguishable because the first one says so on stderr and the second says
/// nothing - so a reader parsing stdout sees an empty stream either way, and an operator
/// watching the terminal still sees two different answers.
#[test]
fn unknown_and_empty_stay_different_answers() {
    let (unknown_out, unknown_err) = query(&[
        r#"MATCH LOG {"event": "no_such_event"}"#,
        "--format",
        "ndjson",
    ]);
    assert_eq!(unknown_out, "", "an unknown template wrote to stdout");
    assert!(unknown_err.contains("no template matches"), "{unknown_err}");

    // The template is real; the window is not one its rows fall in.
    let (empty_out, empty_err) = query(&[
        r#"MATCH LOG {"service": "auth"} BETWEEN 2020-01-01T00:00:00Z AND 2020-01-02T00:00:00Z"#,
        "--format",
        "ndjson",
    ]);
    assert_eq!(empty_out, "");
    assert!(
        !empty_err.contains("no template matches"),
        "an existing template was reported as unknown: {empty_err}"
    );
}

/// A `Datetime` is RFC 3339 in both JSON formats and in the table, all three through the
/// one renderer in `fmt`. Sub-second precision survives, so the rendering never implies
/// the ladder threw any away.
///
/// `parse --json`'s `typed_values` is the deliberate exception and still carries the
/// epoch nanoseconds: `tests/fixture.rs` pins the saturation boundaries there as exact
/// instants.
#[test]
fn renders_datetimes_as_rfc3339() {
    let (rows, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "ndjson"]);
    for row in objects(&rows) {
        let ts = row["ts"]
            .as_str()
            .unwrap_or_else(|| panic!("ts not a string: {row}"));
        assert!(ts.starts_with("2024-01-15T10:3"), "{ts}");
        assert!(ts.ends_with('Z'), "{ts}");
    }

    // The same value through the other JSON format.
    let (envelope, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "json"]);
    let body: Value = serde_json::from_str(envelope.trim()).expect("json");
    assert_eq!(body["rows"][0]["ts"], objects(&rows)[0]["ts"]);

    // Nanoseconds in, nanoseconds out.
    let mut child = triedb()
        .args([
            "query",
            "-",
            r#"MATCH LOG {"event": "x"}"#,
            "--format",
            "ndjson",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    std::thread::spawn(move || {
        use std::io::Write;
        stdin.write_all(br#"{"ts":"2024-01-15T10:30:00.123456789Z","event":"x"}"#)
    });
    let out = child.wait_with_output().expect("failed to run triedb");
    assert_eq!(
        objects(&String::from_utf8_lossy(&out.stdout)),
        vec![serde_json::json!({"event": "x", "ts": "2024-01-15T10:30:00.123456789Z"})]
    );
}

/// `jsonl` is the same format under the name the file extension uses.
#[test]
fn jsonl_is_an_alias() {
    let (a, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "ndjson"]);
    let (b, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "jsonl"]);
    assert_eq!(a, b);
    assert!(!a.is_empty());
}

/// What comes out goes back in, types included: `ts` leaves as RFC 3339 and the type
/// ladder reads it back as a `Datetime`, which is why rows do not carry the epoch
/// nanoseconds `ColValue::to_json` holds. The rendering is pinned in
/// `renders_datetimes_as_rfc3339`; this one checks the stream re-ingests and re-queries.
#[test]
fn the_output_is_ingestable() {
    use std::io::Write;

    let (stdout, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "ndjson"]);

    let mut child = triedb()
        .args([
            "query",
            "-",
            r#"COUNT LOG {"service": "auth"}"#,
            "--format",
            "ndjson",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    std::thread::spawn(move || stdin.write_all(stdout.as_bytes()));
    let out = child.wait_with_output().expect("failed to run triedb");

    assert!(out.status.success());
    assert_eq!(
        objects(&String::from_utf8_lossy(&out.stdout)),
        vec![serde_json::json!({"count": 2})]
    );
}
