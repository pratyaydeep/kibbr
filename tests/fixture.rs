//! Tests target the outer surface. Everything here runs the real binary and
//! reads its stdout; nothing reaches inside the crate.

use std::process::{Command, Stdio};

fn kibbr() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kibbr"));
    // The fixture's labels are the *default* window's, so an exported one would turn the
    // whole file red; the two tests that want a window set it back.
    cmd.env_remove("KIBBR__SCHEMA__EPOCH_WINDOW");
    cmd
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
    let mut child = kibbr()
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let writer = feed(&mut child, input.as_bytes());
    let out = child.wait_with_output().expect("failed to wait on kibbr");
    writer.join().expect("stdin writer thread panicked");
    assert!(
        out.status.success(),
        "kibbr {args:?} exited {:?}",
        out.status.code()
    );
    String::from_utf8(out.stdout).expect("stdout was not utf-8")
}

/// `parse` over stdin, returning stdout and stderr whatever the exit status: a rejected
/// line exits non-zero and is reported on stderr. Bytes, so a test can feed non-UTF-8.
fn run_stdin_rejecting(input: impl AsRef<[u8]>) -> (String, String) {
    let mut child = kibbr()
        .args(["parse", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let writer = feed(&mut child, input.as_ref());
    let out = child.wait_with_output().expect("failed to wait on kibbr");
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

/// A fixture line that carries a `template` label gets that fingerprint, and no two
/// labelled lines share one or an id: `{"event=login":1}` is not `{"event":"login"}`.
#[test]
fn every_labelled_fixture_line_gets_its_template() {
    let stdout = run(&["parse", "testdata/fixture.ndjson", "--json"]);
    let mut seen = std::collections::HashMap::new();
    for line in stdout.lines() {
        let record = json_record(line);
        let Some(label) = record["values"]["template"].as_str() else {
            continue;
        };
        assert_eq!(record["fingerprint"], label, "{line}");
        let id = record["template_id"].as_u64().expect("an id");
        if let Some(other) = seen.insert(id, label.to_string()) {
            panic!("`{other}` and `{label}` share template {id}");
        }
    }
    assert_eq!(seen.len(), 2, "the two labelled lines are in the fixture");
    let texts: std::collections::HashSet<&String> = seen.values().collect();
    assert_eq!(
        texts.len(),
        2,
        "the two labelled lines read alike: {seen:?}"
    );
}

/// Escaping `=` moves no template that has none: the ids and fingerprints of `--seed 42`
/// lines keep their digest (FNV-1a over `id fingerprint\n`).
#[test]
fn seed_42_template_ids_are_the_ones_before_the_equals_escape() {
    let lines = run(&["gen-logs", "--lines", "20000", "--seed", "42"]);
    let stdout = run_stdin(&["parse", "-", "--json"], &lines);
    let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
    let mut count = 0;
    for line in stdout.lines() {
        let record = json_record(line);
        let id = record["template_id"].as_u64().expect("an id");
        let fingerprint = record["fingerprint"].as_str().expect("a fingerprint");
        for byte in format!("{id} {fingerprint}\n").bytes() {
            digest ^= u64::from(byte);
            digest = digest.wrapping_mul(0x0000_0100_0000_01b3);
        }
        count += 1;
    }
    assert_eq!(count, 20_000);
    assert_eq!(digest, 0x438b_4bfd_4b1f_10b7);
}

/// `parse --json` over stdin with the epoch window set, returning the exit status, the
/// type of `v` on each line, and stderr.
fn parse_under_window(window: &str, input: &str) -> (bool, Vec<String>, String) {
    let mut child = kibbr()
        .args(["parse", "-", "--json"])
        .env("KIBBR__SCHEMA__EPOCH_WINDOW", window)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn kibbr");
    let writer = feed(&mut child, input.as_bytes());
    let out = child.wait_with_output().expect("failed to wait on kibbr");
    writer.join().expect("stdin writer thread panicked");
    let types = String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| {
            json_record(l)["types"]["v"]
                .as_str()
                .unwrap_or("Null")
                .to_string()
        })
        .collect();
    (
        out.status.success(),
        types,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The window is a setting, and the fixture's labels are its default; a leading zero is
/// not an epoch under any window.
#[test]
fn the_epoch_window_is_a_setting() {
    let input = concat!(
        r#"{"v":4102444799}"#,
        "\n",
        r#"{"v":4102444800}"#,
        "\n",
        r#"{"v":7258118399}"#,
        "\n",
        r#"{"v":7258118400}"#,
        "\n",
        r#"{"v":1700000000}"#,
        "\n",
        r#"{"v":4102444800000}"#,
        "\n",
        r#"{"v":"2024-01-15T10:30:00Z"}"#,
        "\n",
    );
    let (ok, types, stderr) =
        parse_under_window("2100-01-01T00:00:00Z, 2200-01-01T00:00:00Z", input);
    assert!(ok, "stderr: {stderr}");
    assert_eq!(
        types,
        ["Int64", "Datetime", "Datetime", "Int64", "Int64", "Datetime", "Datetime"]
    );

    let input = concat!(
        r#"{"v":"0000000000"}"#,
        "\n",
        r#"{"v":"0946684799"}"#,
        "\n",
        r#"{"v":"0946684799999"}"#,
        "\n",
        r#"{"v":1700000000}"#,
        "\n",
    );
    let (ok, types, stderr) =
        parse_under_window("1970-01-01T00:00:00Z, 2000-01-01T00:00:00Z", input);
    assert!(ok, "stderr: {stderr}");
    assert_eq!(types, ["Int64", "Int64", "Int64", "Int64"]);
}

/// A window reaching past 2262 admits digits an `i64` of nanoseconds cannot hold: they
/// fall through to rule 6 rather than clamp to the last instant.
#[test]
fn digits_past_the_last_nanosecond_are_not_a_datetime_under_any_window() {
    let input = concat!(
        r#"{"v":9223372036}"#,
        "\n",
        r#"{"v":9223372037}"#,
        "\n",
        r#"{"v":9999999999}"#,
        "\n",
        r#"{"v":9223372036854}"#,
        "\n",
        r#"{"v":9223372036855}"#,
        "\n",
    );
    let (ok, types, stderr) =
        parse_under_window("2000-01-01T00:00:00Z, 2300-01-01T00:00:00Z", input);
    assert!(ok, "stderr: {stderr}");
    assert_eq!(types, ["Datetime", "Int64", "Int64", "Datetime", "Int64"]);
}

/// A window that cannot be read stops the process before a line is typed, rather than
/// typing every line under the default the operator thought they had replaced.
#[test]
fn an_unreadable_epoch_window_is_refused() {
    for (window, why) in [
        ("2000-01-01T00:00:00Z", "1 comma-separated values"),
        (
            "2000-01-01T00:00:00Z,2050-01-01T00:00:00Z,2100-01-01T00:00:00Z",
            "3 comma-separated values",
        ),
        ("", "1 comma-separated values"),
        ("2000-01-01,2100-01-01", "not an RFC 3339 instant"),
        (
            "2100-01-01T00:00:00Z,2000-01-01T00:00:00Z",
            "window is empty",
        ),
        (
            "2000-01-01T00:00:00Z,2000-01-01T00:00:00Z",
            "window is empty",
        ),
    ] {
        let (ok, types, stderr) = parse_under_window(window, "{\"v\":1700000000}\n");
        assert!(!ok, "`{window}` was accepted");
        assert!(types.is_empty(), "`{window}` typed lines anyway: {types:?}");
        assert!(
            stderr.contains("KIBBR__SCHEMA__EPOCH_WINDOW") && stderr.contains(why),
            "`{window}`: stderr did not say `{why}`: {stderr}"
        );
    }
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

/// An instant a `Datetime` cannot hold is text, not the nearest instant the type does
/// hold; the two edges are checked to the nanosecond, so they are held, not clamped.
#[test]
fn an_instant_past_either_end_of_the_range_is_text() {
    let cases = [
        // Inside the range: exact values, to the nanosecond at both ends.
        ("1677-09-21T00:12:44Z", Some(-9_223_372_036_000_000_000_i64)),
        ("1970-01-01T00:00:00Z", Some(0)),
        ("2262-04-11T23:47:16Z", Some(9_223_372_036_000_000_000)),
        ("1677-09-21T00:12:43.145224192Z", Some(i64::MIN)),
        ("2262-04-11T23:47:16.854775807Z", Some(i64::MAX)),
        // The same last instant through an offset: the edge is the instant, not the text.
        ("2262-04-12T05:17:16.854775807+05:30", Some(i64::MAX)),
        // One nanosecond past each end, the same through an offset, one second past each
        // end, then the far extremes of RFC 3339.
        ("1677-09-21T00:12:43.145224191Z", None),
        ("2262-04-11T23:47:16.854775808Z", None),
        ("2262-04-12T05:17:16.854775808+05:30", None),
        ("1677-09-21T00:12:43Z", None),
        ("2262-04-11T23:47:17Z", None),
        ("0001-01-01T00:00:00Z", None),
        ("9999-12-31T23:59:59Z", None),
    ];

    let input: String = cases
        .iter()
        .map(|(v, _)| format!("{{\"v\":\"{v}\"}}\n"))
        .collect();
    let stdout = run_stdin(&["parse", "-", "--json"], &input);

    let mut failures = Vec::new();
    for (line, (raw, want)) in stdout.lines().zip(cases) {
        let record = json_record(line);
        let (ty, value) = (&record["types"]["v"], &record["typed_values"]["v"]);
        let ok = match want {
            Some(ns) => ty == "Datetime" && value.as_i64() == Some(ns),
            None => ty == "String" && value == raw,
        };
        if !ok {
            let want = want.map_or_else(|| format!("String {raw}"), |ns| format!("Datetime {ns}"));
            failures.push(format!("{raw}\n    want {want}, got {ty} {value}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} instants were not held exactly or refused:\n  {}",
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

/// The two spellings are different shapes, so they are different templates: one leaf
/// cannot describe both a nested object and a flat dotted field.
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

/// Promotion reads the leaf segment, not the text after the last dot: `{"a.event":"login"}`
/// is a field called `a.event`, not `event` nested under `a`.
#[test]
fn a_dotted_literal_key_is_not_promoted_as_though_it_were_nested() {
    let parsed = run_stdin(
        &["parse", "-", "--json"],
        "{\"a.event\":\"login\"}\n{\"a\":{\"event\":\"login\"}}\n",
    );
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(records[0]["fingerprint"], "a\\.event");
    // The nested one is promoted.
    assert_eq!(records[1]["fingerprint"], "a.event=login");
}

/// A backslash in a key is data, and must not be able to forge a separator: escaping `.`
/// alone leaves `{"a\\":{"b":1}}` and `{"a\\.b":1}` rendering alike.
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

/// The invariant `keys` holds over every pair of awkward paths that can share a line: it
/// is a set, and no value goes missing. Exhaustive, because the space is small.
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

/// Separating those shapes must not have split the shapes that really are one: a
/// template id depends on nothing but the key set and the promoted values.
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

/// Two empty objects genuinely do flatten to no keys, and stay one template; the empty
/// key is not the same claim as the empty object.
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

/// The depth rules are enforced on the raw bytes, before `serde_json` builds anything:
/// its own 128-container limit would report valid-but-deep JSON as malformed.
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

/// Arrays are stored whole, so the object limit does not apply inside them, but
/// materialising one is recursive, so a ceiling still does.
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

/// Bytes that are not UTF-8 are an encoding fault, named as one and located to the byte:
/// the fault is in whatever wrote the line, and the byte value is most of the diagnosis.
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
/// `expected value`, so the bytes are asked directly.
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

/// A write cut mid-character is a truncated line, not an encoding fault: a cut inside a
/// two-byte character and one just after it must report the same thing.
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

/// A rejection is one line, and the only line number on it is the real one: `serde_json`'s
/// `at line 1 column N` suffix is cut and the column reported as an offset into the line.
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

/// A byte order mark is a marker, not content, and it is dropped wherever it appears;
/// invisible in an editor, a line rejected for one looks rejected for nothing.
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

/// Only a leading mark, and only the UTF-8 encoding of one: U+FEFF inside a string is a
/// character, and a UTF-16 mark is an encoding fault.
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

#[test]
fn limit_stops_early() {
    let logs = run(&["gen-logs", "--lines", "50", "--seed", "1"]);
    let parsed = run_stdin(&["parse", "-", "--limit", "3"], &logs);
    assert_eq!(blocks(&parsed).len(), 3);
    assert!(parsed.contains("3 lines,"), "{parsed}");
}
