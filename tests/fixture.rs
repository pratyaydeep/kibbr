//! Tests target the outer surface. Everything here runs the real binary and
//! reads its stdout; nothing reaches inside the crate.

use std::process::{Command, Stdio};

fn triedb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_triedb"))
}

fn run(args: &[&str]) -> String {
    let out = triedb().args(args).output().expect("failed to run triedb");
    assert!(
        out.status.success(),
        "triedb {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

/// Feeds stdin from a separate thread. Writing inline deadlocks as soon as the input
/// outgrows a pipe buffer: the child blocks writing stdout while we block writing stdin.
fn feed(child: &mut std::process::Child, input: &[u8]) -> std::thread::JoinHandle<()> {
    use std::io::Write;

    let mut stdin = child.stdin.take().expect("stdin was not piped");
    let input = input.to_vec();
    std::thread::spawn(move || {
        // A short `--limit` makes the reader exit early; that closed pipe is the
        // expected end of the write, not a failure.
        let _ = stdin.write_all(&input);
    })
}

fn run_stdin(args: &[&str], input: &str) -> String {
    let mut child = triedb()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let writer = feed(&mut child, input.as_bytes());
    let out = child.wait_with_output().expect("failed to wait on triedb");
    writer.join().expect("stdin writer thread panicked");
    assert!(
        out.status.success(),
        "triedb {args:?} exited {:?}",
        out.status.code()
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

/// `parse` over stdin, returning stdout and stderr whatever the exit status.
///
/// `run_stdin` cannot stand in: it asserts the run succeeded, and a run that rejected
/// a line exits non-zero by contract, so it would panic before the test asserted
/// anything. It also leaves stderr uncaptured, which is where a rejection is reported.
///
/// Takes bytes rather than `&str` so a test can feed input that is not UTF-8, which is
/// the whole point of some of them. `&str` and `&String` still pass unchanged.
fn run_stdin_rejecting(input: impl AsRef<[u8]>) -> (String, String) {
    let mut child = triedb()
        .args(["parse", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let writer = feed(&mut child, input.as_ref());
    let out = child.wait_with_output().expect("failed to wait on triedb");
    writer.join().expect("stdin writer thread panicked");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Split `parse` output into one block of lines per `line N` record.
fn blocks(stdout: &str) -> Vec<Vec<&str>> {
    let mut out: Vec<Vec<&str>> = Vec::new();
    for line in stdout.lines() {
        if line.starts_with("line ") {
            out.push(Vec::new());
        } else if let Some(current) = out.last_mut() {
            if line.starts_with("  ") {
                current.push(line);
            }
        }
    }
    out
}

/// The value column of the row for `key`, as whitespace-separated fields.
fn row<'a>(block: &[&'a str], key: &str) -> Vec<&'a str> {
    let prefix = format!("  {key} ");
    let line = block
        .iter()
        .find(|l| l.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no row for key `{key}` in block {block:?}"));
    line.split_whitespace().collect()
}

/// The done-when for type inference: every value in the hand-labelled fixture gets the
/// labelled type, ambiguous ones included.
#[test]
fn every_fixture_value_gets_its_labelled_type() {
    let fixture = include_str!("../testdata/fixture.ndjson");
    let expected: Vec<&str> = fixture.lines().filter(|l| !l.trim().is_empty()).collect();

    let stdout = run(&["parse", "testdata/fixture.ndjson"]);
    let blocks = blocks(&stdout);
    assert_eq!(
        blocks.len(),
        expected.len(),
        "parsed {} lines, fixture has {}",
        blocks.len(),
        expected.len()
    );

    let mut failures = Vec::new();
    for (block, source) in blocks.iter().zip(expected) {
        // The label lives in the fixture next to the data: `expect` is a String
        // whose text is the type name `v` must be given.
        let label = row(block, "expect");
        let actual = row(block, "v");
        let want = label[2];
        let got = actual[1];
        if want != got {
            failures.push(format!("{source}\n    want {want}, got {got}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} fixture values got the wrong type:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// The worked example, byte for byte.
#[test]
fn flattening_matches_the_worked_example() {
    let line =
        r#"{"error":{"code":401,"reason":"unauth"},"tags":["auth","login"],"user":{"id":"u42"}}"#;
    let stdout = run_stdin(&["parse", "-"], &format!("{line}\n"));
    let blocks = blocks(&stdout);
    assert_eq!(blocks.len(), 1);
    let block = &blocks[0];

    assert!(
        block[0].ends_with("error.code | error.reason | tags | user.id"),
        "fingerprint line was {:?}",
        block[0]
    );
    assert_eq!(row(block, "error.code")[1], "Int64");
    assert_eq!(row(block, "error.reason")[1], "String");
    // Arrays are never flattened: the raw JSON text is the value.
    assert_eq!(
        row(block, "tags"),
        ["tags", "String", r#"["auth","login"]"#]
    );
    assert_eq!(row(block, "user.id")[1], "String");
}

/// Field order is the serializer's business, not the fingerprint's.
#[test]
fn fingerprint_is_order_independent() {
    let a = r#"{"error":{"reason":"unauth","code":401},"user":{"id":"u42"},"tags":[]}"#;
    let b = r#"{"tags":[],"user":{"id":"u42"},"error":{"code":401,"reason":"unauth"}}"#;
    let stdout = run_stdin(&["parse", "-"], &format!("{a}\n{b}\n"));
    let blocks = blocks(&stdout);
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0][0], blocks[1][0]);
    assert!(stdout.contains("2 lines, 1 templates"), "{stdout}");
}

/// A low-cardinality key is promoted into the fingerprint as `key=value`,
/// which is the shape `parse` prints.
#[test]
fn low_cardinality_key_is_promoted_into_the_fingerprint() {
    let line = r#"{"event":"login_failed","level":"info","reason":"invalid_password","ts":"2024-01-15T10:30:00Z","user_id":"u42"}"#;
    let stdout = run_stdin(&["parse", "-"], &format!("{line}\n"));
    let blocks = blocks(&stdout);
    assert!(
        blocks[0][0].ends_with("event=login_failed | level | reason | ts | user_id"),
        "fingerprint line was {:?}",
        blocks[0][0]
    );
    // Promoted keys are part of the fingerprint, so they are no longer values.
    assert!(
        !blocks[0].iter().any(|l| l.starts_with("  event ")),
        "promoted key was also printed as a value"
    );
    assert_eq!(row(&blocks[0], "ts")[1], "Datetime");
}

/// One `--json` record, as a `Value`.
fn json_record(line: &str) -> serde_json::Value {
    serde_json::from_str(line).unwrap_or_else(|e| panic!("record was not JSON: {e}\n{line}"))
}

/// A datetime that falls off the low end of the range lands at the low end.
///
/// `Datetime` is i64 unix nanoseconds, so it spans 1677 to 2262 while RFC 3339 spans
/// year 0 to 9999. Both ends overflow. Clamping both to `i64::MAX` put every pre-1677
/// timestamp in 2262: a year-1600 line and a year-9999 line compared equal, and both
/// sorted after every real one. A time-range query over historical data then returns
/// the wrong rows rather than none, which is the version of the bug that looks fine.
///
/// The type label cannot catch this - every value here is `Datetime` either way - so
/// the assertion has to be on the instant.
#[test]
fn a_datetime_past_either_end_of_the_range_saturates_towards_that_end() {
    let cases = [
        // Inside the range: exact values, no clamping.
        ("1677-09-21T00:12:44Z", -9_223_372_036_000_000_000_i64),
        ("1970-01-01T00:00:00Z", 0),
        ("2262-04-11T23:47:16Z", 9_223_372_036_000_000_000),
        // One second past each end, then the far extremes of RFC 3339.
        ("1677-09-21T00:12:43Z", i64::MIN),
        ("2262-04-11T23:47:17Z", i64::MAX),
        ("0001-01-01T00:00:00Z", i64::MIN),
        ("9999-12-31T23:59:59Z", i64::MAX),
    ];

    let input: String = cases
        .iter()
        .map(|(v, _)| format!("{{\"v\":\"{v}\"}}\n"))
        .collect();
    let stdout = run_stdin(&["parse", "-", "--json"], &input);

    let mut failures = Vec::new();
    for (line, (raw, want)) in stdout.lines().zip(cases) {
        let record = json_record(line);
        assert_eq!(record["types"]["v"], "Datetime", "{raw} was not a Datetime");
        let got = record["typed_values"]["v"]
            .as_i64()
            .unwrap_or_else(|| panic!("{raw} had no i64 instant"));
        if got != want {
            failures.push(format!("{raw}\n    want {want}, got {got}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} datetimes saturated the wrong way:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// The `keys` array of a `--json` record.
fn keys(record: &serde_json::Value) -> Vec<String> {
    record["keys"]
        .as_array()
        .unwrap_or_else(|| panic!("no keys array in {record}"))
        .iter()
        .map(|k| k.as_str().expect("key was not a string").to_string())
        .collect()
}

/// A dotted literal key and the nested path that spells the same thing are two keys,
/// and both values survive.
///
/// Flattening joins segments with `.`, so `{"a.b":1}` and `{"a":{"b":2}}` both used to
/// produce the three characters `a.b`. Together on one line that is one key twice: the
/// `keys` array held a duplicate, the fingerprint repeated a segment, and one of the two
/// values was dropped with no diagnostic. Which one it was depended on field order,
/// since last-wins into the map is the only thing that decided it.
#[test]
fn a_dotted_literal_key_and_a_nested_path_stay_distinct() {
    // Both orders: the two keys must not depend on which arrived first.
    for line in [r#"{"a.b":1,"a":{"b":2}}"#, r#"{"a":{"b":2},"a.b":1}"#] {
        let record = json_record(run_stdin(&["parse", "-", "--json"], &format!("{line}\n")).trim());
        assert_eq!(keys(&record), ["a.b", "a\\.b"], "for {line}");
        // The nested path keeps the spelling it always had, so its template id is
        // unmoved; only the literal key, which was the ambiguous one, gains an escape.
        assert_eq!(record["values"]["a.b"], "2", "nested value lost: {record}");
        assert_eq!(
            record["values"]["a\\.b"], "1",
            "literal value lost: {record}"
        );
        assert_eq!(record["fingerprint"], "a.b | a\\.b");
    }
}

/// The two spellings are different shapes, so they are different templates.
///
/// This is the same fault seen from the trie's side: sharing a template id means sharing
/// a leaf, and one leaf cannot describe both a nested object and a flat dotted field.
#[test]
fn a_dotted_literal_key_and_a_nested_path_are_different_templates() {
    let parsed = run_stdin(
        &["parse", "-", "--json"],
        "{\"a.b\":1}\n{\"a\":{\"b\":1}}\n",
    );
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(keys(&records[0]), ["a\\.b"]);
    assert_eq!(keys(&records[1]), ["a.b"]);
    assert_ne!(
        records[0]["template_id"], records[1]["template_id"],
        "two shapes shared one template: {parsed}"
    );
}

/// Promotion reads the leaf segment, not the text after the last dot.
///
/// `{"a.event":"login"}` is a field called `a.event`, not a field `event` nested under
/// `a`. Splitting the rendered key to find the leaf promoted it into the fingerprint as
/// though the sender had nested it.
#[test]
fn a_dotted_literal_key_is_not_promoted_as_though_it_were_nested() {
    let parsed = run_stdin(
        &["parse", "-", "--json"],
        "{\"a.event\":\"login\"}\n{\"a\":{\"event\":\"login\"}}\n",
    );
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(records[0]["fingerprint"], "a\\.event");
    // The nested one is promoted, as it was before, and reads exactly as it did.
    assert_eq!(records[1]["fingerprint"], "a.event=login");
}

/// A backslash in a key is data, and must not be able to forge a separator.
///
/// Escaping `.` alone leaves `{"a\\":{"b":1}}` and `{"a\\.b":1}` rendering alike, which
/// is the original collision reintroduced one character over.
#[test]
fn a_backslash_in_a_key_cannot_forge_a_separator() {
    let line = r#"{"a\\":{"b":1},"a\\.b":2}"#;
    let record = json_record(run_stdin(&["parse", "-", "--json"], &format!("{line}\n")).trim());
    let keys = keys(&record);
    assert_eq!(keys.len(), 2, "keys collided: {record}");
    assert_ne!(keys[0], keys[1]);
    assert_eq!(record["values"][&keys[0]], "1");
    assert_eq!(record["values"][&keys[1]], "2");
}

/// Insert `value` at `segments`, creating intermediate objects. `false` when the path
/// cannot exist alongside what is already there - one path being a prefix of another.
fn insert_path(
    root: &mut serde_json::Map<String, serde_json::Value>,
    segments: &[&str],
    value: serde_json::Value,
) -> bool {
    let (last, parents) = segments.split_last().expect("no empty paths");
    let mut node = root;
    for segment in parents {
        let next = node
            .entry(*segment)
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        match next.as_object_mut() {
            Some(obj) => node = obj,
            None => return false,
        }
    }
    if node.contains_key(*last) {
        return false;
    }
    node.insert((*last).to_string(), value);
    true
}

/// The invariant `keys` is required to hold, over every pair of awkward paths that can
/// share a line: it is a set, and no value goes missing.
///
/// Exhaustive rather than sampled. The space that matters here is small and entirely
/// about where separators and backslashes fall, so enumerating it covers more than
/// random documents would - and the same pairs run on every future change to the
/// encoding, which is the part worth keeping.
#[test]
fn no_two_key_paths_on_one_line_ever_collide() {
    let paths: &[&[&str]] = &[
        &["a"],
        &["b"],
        &["a", "b"],
        &["a", "c"],
        &["a.b"],
        &["a.b", "c"],
        &["a", "b.c"],
        &["a.b.c"],
        &["a", "b", "c"],
        &["a\\"],
        &["a\\", "b"],
        &["a\\.b"],
        &["a\\\\", "b"],
        &["."],
        &[".", "."],
        &[""],
        &["a", ""],
        &["", "b"],
    ];

    // One line per pair, so the whole matrix costs a single run of the binary.
    let mut lines = Vec::new();
    let mut expected = Vec::new();
    for (i, first) in paths.iter().enumerate() {
        for second in &paths[i + 1..] {
            let mut doc = serde_json::Map::new();
            if !insert_path(&mut doc, first, serde_json::json!(1))
                || !insert_path(&mut doc, second, serde_json::json!(2))
            {
                // One path is a prefix of the other; they cannot both exist in one
                // document, and JSON's own last-wins would decide it rather than us.
                continue;
            }
            lines.push(serde_json::Value::Object(doc).to_string());
            expected.push((first, second));
        }
    }
    assert!(lines.len() > 100, "only {} pairs were built", lines.len());

    let parsed = run_stdin(
        &["parse", "-", "--json"],
        &format!("{}\n", lines.join("\n")),
    );
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(records.len(), lines.len(), "lines went missing");

    let mut failures = Vec::new();
    for ((record, line), (first, second)) in records.iter().zip(&lines).zip(&expected) {
        let keys = keys(record);
        let unique: std::collections::HashSet<_> = keys.iter().collect();
        if keys.len() != 2 || unique.len() != 2 {
            failures.push(format!("{first:?} + {second:?} -> keys {keys:?}  ({line})"));
            continue;
        }
        // Neither value was dropped on the way into the map.
        let values = record["values"].as_object().expect("no values object");
        let mut got: Vec<_> = values.values().filter_map(|v| v.as_str()).collect();
        got.sort_unstable();
        if got != ["1", "2"] {
            failures.push(format!(
                "{first:?} + {second:?} -> values {got:?}  ({line})"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} pairs collided:\n  {}",
        failures.len(),
        records.len(),
        failures.join("\n  ")
    );
}

/// A fingerprint is what a human reads. The template id is the identity, and two
/// different shapes must not share one however alike they read.
///
/// `tokens.join(" | ")` was the identity, and `" | "` is not a separator a key is
/// obliged to stay out of - nor is `=`, nor is "having a name at all". Each pair below
/// is two genuinely different shapes that the joined text could not tell apart. Sharing
/// a template id means sharing a leaf once the trie exists, and the column store is then
/// handed two schemas for one place.
#[test]
fn two_shapes_that_read_alike_are_not_one_template() {
    for (first, second, what) in [
        // The separator, forged inside a single key: one key, or two.
        (r#"{"a | b":1}"#, r#"{"a":1,"b":1}"#, "the separator"),
        // A promoted `key=value` token, forged by a key literally named that. The
        // literal is an Int64 under a long name; the promoted one is a String.
        (
            r#"{"event=login":1}"#,
            r#"{"event":"login"}"#,
            "a promoted token",
        ),
        // No keys at all, or one key whose name is empty.
        (r#"{}"#, r#"{"":""}"#, "the empty key"),
        // And the empty key beside a real one, where the join leaves a leading gap.
        (r#"{"":1,"a":2}"#, r#"{" | a":3}"#, "a leading empty key"),
    ] {
        let parsed = run_stdin(&["parse", "-", "--json"], &format!("{first}\n{second}\n"));
        let records: Vec<_> = parsed.lines().map(json_record).collect();
        assert_ne!(
            records[0]["template_id"], records[1]["template_id"],
            "{what} was forged: {first} and {second} share a template\n{parsed}"
        );
        assert_ne!(
            keys(&records[0]),
            keys(&records[1]),
            "the test pair is wrong - these have the same key set"
        );
    }
}

/// The other half of the same claim: separating those shapes must not have split the
/// shapes that really are one. A template id still depends on nothing but the key set
/// and the promoted values.
#[test]
fn the_same_shape_still_gets_one_template() {
    let parsed = run_stdin(
        &["parse", "-", "--json"],
        concat!(
            r#"{"event":"login","user":{"id":"u1"},"n":1}"#,
            "\n",
            // Reordered, and every value different: neither moves the shape.
            r#"{"n":9999,"user":{"id":"u2"},"event":"login"}"#,
            "\n",
        ),
    );
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(
        records[0]["template_id"], records[1]["template_id"],
        "{parsed}"
    );

    // A promoted value is part of the shape, so changing that one does split it.
    let split = run_stdin(
        &["parse", "-", "--json"],
        "{\"event\":\"login\"}\n{\"event\":\"logout\"}\n",
    );
    let records: Vec<_> = split.lines().map(json_record).collect();
    assert_ne!(
        records[0]["template_id"], records[1]["template_id"],
        "{split}"
    );
}

/// Two empty objects genuinely do flatten to no keys, and stay one template. The empty
/// key was never the same claim as the empty object, which is why only one of them moved.
#[test]
fn an_object_that_flattens_to_no_keys_is_one_template() {
    let parsed = run_stdin(&["parse", "-", "--json"], "{}\n{\"a\":{\"b\":{}}}\n");
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(keys(&records[0]), Vec::<String>::new());
    assert_eq!(keys(&records[1]), Vec::<String>::new());
    assert_eq!(
        records[0]["template_id"], records[1]["template_id"],
        "{parsed}"
    );
}

/// Same seed, byte-identical output. Different seed, different output.
#[test]
fn generation_is_reproducible() {
    let a = run(&["gen-logs", "--lines", "200", "--seed", "42"]);
    let b = run(&["gen-logs", "--lines", "200", "--seed", "42"]);
    assert_eq!(a, b, "same seed produced different bytes");

    let c = run(&["gen-logs", "--lines", "200", "--seed", "43"]);
    assert_ne!(a, c, "different seeds produced identical bytes");
}

/// The generator has to produce the cases hand-written fixtures do not.
#[test]
fn generated_data_is_hostile_enough() {
    let logs = run(&["gen-logs", "--lines", "2000", "--seed", "7"]);
    let parsed = run_stdin(&["parse", "-"], &logs);

    for (needle, what) in [
        ("Datetime", "timestamps"),
        ("Int64", "integers"),
        ("Float64", "floats"),
        ("Bool", "bools"),
        ("Uuid", "uuids"),
        ("Ip", "ip addresses"),
        ("Null", "explicit nulls"),
        ("\"\"", "empty strings"),
        ("(= ", "ten-digit ints read as timestamps"),
    ] {
        assert!(parsed.contains(needle), "no {what} in generated data");
    }
    // Nested objects, and unicode that is not decoration.
    assert!(logs.contains("\"user\":{"), "no nested objects");
    assert!(
        logs.chars().any(|c| c as u32 > 0x3000),
        "no unicode string values"
    );
    // An array survived as raw text rather than being flattened.
    assert!(logs.contains("\"tags\":["), "no arrays");

    // Optional fields: at least one template must have keys that come and go.
    let templates = parsed
        .lines()
        .filter(|l| l.contains("fingerprint"))
        .collect::<std::collections::HashSet<_>>();
    assert!(
        templates.len() > 12,
        "expected optional fields to split 12 templates into more fingerprints, got {}",
        templates.len()
    );
}

/// The depth limit is a guard against hostile input, and it fires.
#[test]
fn nesting_beyond_the_limit_is_rejected() {
    let mut line = String::from("\"leaf\"");
    for i in 0..9 {
        line = format!("{{\"k{i}\":{line}}}");
    }
    let (stdout, stderr) = run_stdin_rejecting(format!("{line}\n"));
    assert!(blocks(&stdout).is_empty(), "deep nesting was accepted");
    assert!(stderr.contains("nesting deeper than 8"), "stderr: {stderr}");
}

/// `{"d":{"a":{"a":...1...}}}`, nested `depth` levels below `d`.
fn nested_object(depth: usize) -> String {
    format!(
        "{{\"d\":{}1{}}}",
        "{\"a\":".repeat(depth),
        "}".repeat(depth)
    )
}

/// `{"d":[[[...1...]]]}`, nested `depth` levels.
fn nested_array(depth: usize) -> String {
    format!("{{\"d\":{}1{}}}", "[".repeat(depth), "]".repeat(depth))
}

/// The depth rules are enforced on the raw bytes, before `serde_json` builds anything,
/// so they stay reachable at any depth. Checked after parsing instead, `serde_json`'s
/// own 128-container limit fires first and reports valid-but-deep JSON as malformed -
/// the diagnostic getting less accurate exactly as the input gets more hostile.
#[test]
fn depth_past_the_serde_limit_still_reports_the_depth_rule() {
    for depth in [9, 500, 100_000] {
        let (_, stderr) = run_stdin_rejecting(format!("{}\n", nested_object(depth)));
        assert!(
            stderr.contains("nesting deeper than 8 levels at key `d.a.a.a.a.a.a.a`"),
            "object depth {depth} did not report the depth rule\nstderr: {stderr}"
        );
        assert!(
            !stderr.contains("invalid JSON"),
            "object depth {depth} was reported as malformed JSON\nstderr: {stderr}"
        );
    }
}

/// Arrays are stored whole, so the object limit does not apply inside them - but a
/// ceiling still does, because the array is materialised before it is stored and
/// materialising it is recursive.
#[test]
fn array_nesting_has_its_own_ceiling() {
    let (stdout, _) = run_stdin_rejecting(format!("{}\n", nested_array(64)));
    assert_eq!(blocks(&stdout).len(), 1, "array depth 64 was rejected");

    for depth in [65, 500, 100_000] {
        let (_, stderr) = run_stdin_rejecting(format!("{}\n", nested_array(depth)));
        assert!(
            stderr.contains("nesting deeper than 64 levels inside the array at `d`"),
            "array depth {depth} did not report the array ceiling\nstderr: {stderr}"
        );
        assert!(
            !stderr.contains("invalid JSON"),
            "array depth {depth} was reported as malformed JSON\nstderr: {stderr}"
        );
    }
}

/// An array is never flattened, so what is inside one is never walked and its object
/// nesting is not counted against the limit of 8.
#[test]
fn objects_inside_an_array_do_not_count_against_the_object_limit() {
    let deep = format!("{{\"d\":[{}1{}]}}", "{\"a\":".repeat(20), "}".repeat(20));
    let (stdout, _) = run_stdin_rejecting(format!("{deep}\n"));
    assert_eq!(
        blocks(&stdout).len(),
        1,
        "objects nested inside an array were counted against the object limit"
    );
}

/// One rejected line does not cost the lines around it, whatever the rejection was.
#[test]
fn a_depth_rejection_does_not_stop_the_file() {
    let input = format!(
        "{{\"first\":1}}\n{}\n{}\n{{\"last\":2}}\n",
        nested_object(500),
        nested_array(500)
    );
    let (stdout, stderr) = run_stdin_rejecting(&input);
    assert_eq!(blocks(&stdout).len(), 2, "stdout: {stdout}");
    assert!(
        stdout.contains("first") && stdout.contains("last"),
        "{stdout}"
    );
    assert!(
        stderr.contains("line 2:") && stderr.contains("line 3:"),
        "{stderr}"
    );
}

/// One line of good input, so a rejection test can show the run carried on around it.
const FIRST: &[u8] = b"{\"first\":1}\n";

/// Bytes that are not UTF-8 are an encoding fault, named as one and located to the byte.
///
/// `serde_json` calls these `invalid unicode code point`, which describes the symptom and
/// sends the operator to look at their JSON. The fault is in whatever wrote the line, and
/// the byte value is most of the diagnosis: `0xff` here says UTF-16 or latin-1.
#[test]
fn invalid_utf8_is_reported_as_an_encoding_fault_with_its_byte() {
    // The bad bytes sit at different offsets in a value and in a key.
    for (input, offset) in [
        ([FIRST, b"{\"a\":\"\xff\xfe\"}\n"].concat(), 6),
        ([FIRST, b"{\"\xff\xfe\":1}\n"].concat(), 2),
    ] {
        let (stdout, stderr) = run_stdin_rejecting(&input);
        assert!(
            stderr.contains(&format!(
                "line 2: invalid UTF-8: byte 0xff at offset {offset}"
            )),
            "{stderr}"
        );
        // The offset is relative to the line, not the file - line 1 is 12 bytes wide
        // and must not shift it, because `/ingest` indexes it into the echoed line.
        assert!(
            !stderr.contains("offset 1"),
            "offset looks file-relative: {stderr}"
        );
        assert_eq!(blocks(&stdout).len(), 1, "the good line was lost: {stdout}");
    }
}

/// A bad byte outside a string is the case `serde_json` cannot see at all: it reports
/// `expected value`, because a byte that cannot begin a token is all it knows. Asking the
/// bytes directly is what turns that back into the encoding fault it is.
#[test]
fn a_bad_byte_outside_a_string_is_still_an_encoding_fault() {
    let (_, stderr) = run_stdin_rejecting([FIRST, b"{\"a\":\xff}\n"].concat());
    assert!(
        stderr.contains("line 2: invalid UTF-8: byte 0xff at offset 5"),
        "{stderr}"
    );
    assert!(!stderr.contains("expected value"), "{stderr}");
}

/// A line can be malformed *and* fail to decode, independently. Both are true, so both
/// are reported - dropping either costs the operator a second run to find it.
///
/// The syntax error is at column 8 and the bad byte at offset 28. `valid_up_to` is the
/// boundary of what `serde_json` was entitled to judge: column 8 is inside the region
/// that decoded cleanly, so its verdict is sound and leads.
#[test]
fn a_line_that_is_both_malformed_and_undecodable_reports_both() {
    let (_, stderr) =
        run_stdin_rejecting([FIRST, b"{\"a\":1,,\"b\":\"padpadpadpadpad\xff\"}\n"].concat());
    assert!(
        stderr.contains(
            "line 2: invalid JSON at offset 7: key must be a string \
             (line also contains invalid UTF-8: byte 0xff at offset 28)"
        ),
        "{stderr}"
    );
}

/// The mirror of the case above: when the bad byte comes first, `serde_json` only ever
/// tripped over the corruption, so there is no second finding to report and no note.
#[test]
fn a_bad_byte_before_a_syntax_error_reports_only_the_encoding_fault() {
    let (_, stderr) =
        run_stdin_rejecting([FIRST, b"{\"a\":\"\xff padpadpadpadpad\",,\"b\":1}\n"].concat());
    assert!(
        stderr.contains("line 2: invalid UTF-8: byte 0xff at offset 6"),
        "{stderr}"
    );
    assert!(!stderr.contains("also contains"), "{stderr}");
}

/// A write cut mid-character is a truncated line, not an encoding fault.
///
/// The done-when is that both cuts report the same thing: one lands inside a two-byte
/// character and one just after it, but the cause is identical - the line ended early -
/// and the diagnosis must not turn on where the knife fell.
#[test]
fn a_line_cut_mid_character_reports_truncation_not_encoding() {
    for cut in [&b"{\"a\":\"caf\xc3"[..], &b"{\"a\":\"caf\xc3\xa9"[..]] {
        let (_, stderr) = run_stdin_rejecting([FIRST, cut].concat());
        assert!(stderr.contains("line 2: invalid JSON"), "{stderr}");
        assert!(stderr.contains("EOF while parsing a string"), "{stderr}");
        assert!(
            !stderr.contains("UTF-8"),
            "truncation read as encoding: {stderr}"
        );
    }
}

/// The note is for the case that earns it. An ordinary syntax error on a line that
/// decodes fine says only what is wrong with it.
#[test]
fn an_ordinary_syntax_error_carries_no_encoding_note() {
    let (_, stderr) = run_stdin_rejecting([FIRST, b"{\"a\":1,}\n"].concat());
    assert!(
        stderr.contains("line 2: invalid JSON at offset 7: trailing comma\n"),
        "{stderr}"
    );
    assert!(!stderr.contains("UTF-8"), "{stderr}");
}

/// A rejection is one line, and the only line number on it is the real one.
///
/// `serde_json` appends `at line {line} column {column}` to its own messages, and that
/// line is always 1 - it is handed one line at a time. Printed under a rejection against
/// line 6, it reads as a contradiction. The position it does know is worth keeping, so
/// the suffix is cut and the column reported as an offset into the line.
#[test]
fn a_syntax_rejection_reports_one_position_and_it_is_ours() {
    let input = [
        FIRST,
        b"{\"lat\":NaN}\n",
        b"{\"n\":1e400}\n",
        b"{\"msg\":\"a\x07b\"}\n",
    ]
    .concat();
    let (_, stderr) = run_stdin_rejecting(input);

    assert!(
        stderr.contains("line 2: invalid JSON at offset 7: expected value\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("line 3: invalid JSON at offset 9: number out of range\n"),
        "{stderr}"
    );
    assert!(
        stderr.contains("line 4: invalid JSON at offset 9: control character"),
        "{stderr}"
    );
    // The tell for a suffix that slipped through, whatever the message around it.
    assert!(!stderr.contains(" at line "), "{stderr}");
    // One line per rejection, so nothing is left dangling under it.
    assert_eq!(
        stderr
            .lines()
            .filter(|l| l.contains("invalid JSON"))
            .count(),
        3,
        "{stderr}"
    );
}

/// A byte order mark is a marker, not content, and it is dropped wherever it appears.
///
/// The mark is invisible in an editor, so a line rejected for carrying one looks like a
/// line rejected for nothing. `run_stdin` asserts the run succeeded, which is the whole
/// of the done-when: not one line lost, and the exit code says so.
#[test]
fn a_byte_order_mark_is_stripped_wherever_it_appears() {
    // At the head of the file, on a payload line, and on one that ends CRLF - the mark
    // and the line terminator must not depend on each other.
    let input = "\u{feff}{\"a\":1}\n{\"b\":2}\n\u{feff}{\"c\":3}\r\n";
    let parsed = run_stdin(&["parse", "-"], input);
    assert_eq!(blocks(&parsed).len(), 3, "{parsed}");
    assert!(parsed.contains("3 lines, 3 templates\n"), "{parsed}");
}

/// The mark is stripped before the line is parsed, so it neither hides a fault further
/// along nor shifts the offset reported for one.
///
/// Without the strip this reads `offset 0: expected value`, which points at the marker
/// instead of the trailing comma and sends the operator to the wrong end of the line.
/// The offset here is the one the same line gets with no mark on it at all.
#[test]
fn a_byte_order_mark_does_not_hide_the_rest_of_the_line() {
    let (_, stderr) = run_stdin_rejecting([FIRST, "\u{feff}{\"a\":1,}\n".as_bytes()].concat());
    assert!(
        stderr.contains("line 2: invalid JSON at offset 7: trailing comma\n"),
        "{stderr}"
    );
}

/// A line holding nothing but a mark is a blank line, counted with the blank ones.
#[test]
fn a_line_of_only_a_byte_order_mark_counts_as_blank() {
    let parsed = run_stdin(&["parse", "-"], "{\"a\":1}\n\u{feff}\n{\"b\":2}\n");
    assert_eq!(blocks(&parsed).len(), 2, "{parsed}");
    assert!(parsed.contains("1 blank lines skipped"), "{parsed}");
}

/// Only a leading mark, and only the UTF-8 encoding of one.
///
/// U+FEFF inside a string is a character the line is entitled to carry, and a UTF-16
/// mark is a line written in an encoding this does not read - which is worth saying as
/// an encoding fault, because `0xff` is most of that diagnosis.
#[test]
fn only_a_leading_utf8_mark_is_stripped() {
    let parsed = run_stdin(&["parse", "-", "--json"], "{\"a\":\"\u{feff}x\"}\n");
    assert!(
        parsed.contains("\u{feff}x"),
        "mark stripped from a value: {parsed}"
    );

    let (_, stderr) = run_stdin_rejecting([FIRST, b"\xff\xfe{\"a\":1}\n"].concat());
    assert!(
        stderr.contains("line 2: invalid UTF-8: byte 0xff at offset 0"),
        "{stderr}"
    );
}

/// `--json` is a machine surface, so it must be stable run to run even though `ParsedLog`
/// stores its maps as `HashMap`.
#[test]
fn json_output_is_stable_across_runs() {
    let logs = run(&["gen-logs", "--lines", "100", "--seed", "5"]);
    let a = run_stdin(&["parse", "-", "--json"], &logs);
    let b = run_stdin(&["parse", "-", "--json"], &logs);
    assert_eq!(a, b, "--json output was not stable between runs");
    assert_eq!(a.lines().count(), 100);
}

/// `--limit` stops after N lines.
#[test]
fn limit_stops_early() {
    let logs = run(&["gen-logs", "--lines", "50", "--seed", "1"]);
    let parsed = run_stdin(&["parse", "-", "--limit", "3"], &logs);
    assert_eq!(blocks(&parsed).len(), 3);
    assert!(parsed.contains("3 lines,"), "{parsed}");
}
