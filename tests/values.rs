// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a value is, from the binary's outside: the type each one is given, the template
//! each line is, and that a value comes back as itself. One module a subject: the type
//! ladder here, then `shapes`, then `time_types`.
//!
//! The type ladder: every labelled line of `tests/data/type_ladder.ndjson` must come out
//! of `kibbr parse` with the type its label says. Everything here runs the real binary
//! and reads its stdout; nothing reaches inside the crate.

use crate::harness::*;

/// `parse` over stdin, returning stdout and stderr whatever the exit status: a rejected
/// line exits non-zero and is reported on stderr. Bytes, so a test can feed non-UTF-8.
fn run_stdin_rejecting(input: impl AsRef<[u8]>) -> (String, String) {
    let out = run_piped(&["parse", "-"], input.as_ref(), &[]);
    (text(&out.stdout), text(&out.stderr))
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
    let fixture = include_str!("../tests/data/type_ladder.ndjson");
    let expected: Vec<&str> = fixture.lines().filter(|l| !l.trim().is_empty()).collect();

    let stdout = run(&["parse", "tests/data/type_ladder.ndjson"]);
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
    let stdout = run(&["parse", "tests/data/type_ladder.ndjson", "--json"]);
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
    let out = run_piped(
        &["parse", "-", "--json"],
        input.as_bytes(),
        &[("KIBBR__SCHEMA__EPOCH_WINDOW", window)],
    );
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

/// The top-level object with no members has no keys, and is one template however often it
/// comes. An empty object under a key is a value at that key - a leaf, like `null` or an
/// array - so the line that holds one has that key and is another template.
#[test]
fn an_empty_object_is_a_key_only_under_one() {
    let parsed = run_stdin(
        &["parse", "-", "--json"],
        "{}\n{}\n{\"a\":{\"b\":{}}}\n{\"a\":{\"b\":{}}}\n",
    );
    let records: Vec<_> = parsed.lines().map(json_record).collect();
    assert_eq!(keys(&records[0]), Vec::<String>::new());
    assert_eq!(
        records[0]["template_id"], records[1]["template_id"],
        "{parsed}"
    );
    assert_eq!(keys(&records[2]), ["a.b"]);
    assert_eq!(records[2]["values"]["a.b"], "{}", "{parsed}");
    assert_eq!(records[2]["types"]["a.b"], "String", "{parsed}");
    assert_eq!(
        records[2]["template_id"], records[3]["template_id"],
        "{parsed}"
    );
    assert_ne!(
        records[0]["template_id"], records[2]["template_id"],
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

// ── The system column, and ids that widen to text ──────────────────────────────────

/// **What goes in comes back out**: five ten-digit ids, `AX-99213`, five more, each line
/// in a row group of its own, and `MATCH` returns the eleven strings exactly as ingested.
/// Twice, because the epoch window puts the two sets on different rungs: `45000001NN`
/// names 2112, outside it, so it is an Int64 that widens to text; `17053121NN` names 2024,
/// inside it, so it is a Datetime that widens to text - and before `dt_form` was read
/// back, it came back as `2024-01-15T09:50:0NZ`. (Was `scripts/check/order_ids.sh`.)
#[test]
fn ten_digit_ids_that_widen_to_text_come_back_as_they_went_in() {
    let db = Db::new();
    // The row group timeout is floored at the flush interval, so 50ms is the shortest
    // both can be; the sleep between lines is what puts each one in its own row group.
    let server = db.serve(&["--flush-interval-ms", "50", "--row-group-timeout-ms", "50"]);
    let undated = [
        "4500000101",
        "4500000102",
        "4500000103",
        "4500000104",
        "4500000105",
        "AX-99213",
        "4500000107",
        "4500000108",
        "4500000109",
        "4500000110",
        "4500000111",
    ];
    let dated = [
        "1705312101",
        "1705312102",
        "1705312103",
        "1705312104",
        "1705312105",
        "AX-99213",
        "1705312107",
        "1705312108",
        "1705312109",
        "1705312110",
        "1705312111",
    ];
    for (event, ids) in [("undated", undated), ("dated", dated)] {
        for id in ids {
            let line = serde_json::json!({ "event": event, "order_id": id });
            server.ingest(format!("{line}\n").as_bytes());
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let answer = server.query(&format!(r#"MATCH LOG {{"event":"{event}"}}"#));
        let mut got: Vec<String> = answer["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|row| match &row["order_id"] {
                serde_json::Value::String(id) => id.clone(),
                other => other.to_string(),
            })
            .collect();
        got.sort();
        let mut want: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
        want.sort();
        assert_eq!(
            got, want,
            "{event}: MATCH returned strings that were never ingested"
        );
    }
}

/// **The system column is not a key** (was `scripts/check/received_at.sh`): the template
/// count of 20,000 `--seed 42` lines is the 823 it was before `_received_at` existed, so
/// it neither makes a template nor splits one; a line that carries its own
/// `_received_at` comes back unchanged, with the system value left out of that row; and
/// a line with no time column at all comes back with the `_received_at` the store gave it.
#[test]
fn the_system_column_is_not_a_key() {
    let server = Server::spawn(&[], None, &["--storage", "memory"], &[]);
    let report = server.ingest(&hostile(20_000, 42));
    assert_eq!(report["ingested"], 20_000, "{report}");
    let templates = server.call("GET", "/templates", b"");
    assert_eq!(templates["count"], 823, "the template count moved");

    server.ingest(b"{\"event\":\"own_key\",\"_received_at\":\"2019-05-05T05:05:05Z\",\"n\":1}\n");
    server.ingest(b"{\"event\":\"no_time_key\",\"n\":2}\n");

    let own = server.query(r#"MATCH LOG {"event":"own_key"}"#)["rows"][0].clone();
    assert_eq!(own["_received_at"], "2019-05-05T05:05:05Z", "{own}");
    let mut keys: Vec<&String> = own.as_object().expect("a row").keys().collect();
    keys.sort();
    assert_eq!(keys, ["_received_at", "event", "n"], "{own}");

    let none = server.query(r#"MATCH LOG {"event":"no_time_key"}"#)["rows"][0].clone();
    let stamped = none["_received_at"].as_str().unwrap_or_default();
    assert!(
        chrono::DateTime::parse_from_rfc3339(stamped).is_ok(),
        "a row with no time column has no _received_at: {none}"
    );
}

mod shapes {
    //! The JSON values a line held are the JSON values a query gives back: an array as that
    //! array and an empty object as `{}`, beside the strings that spell them, through both
    //! doors - `kibbr load` and a server's `/ingest` - in memory, on disk, after a restart and
    //! after `kibbr compact`.
    //!
    //! Every test runs the real binary. A line's rows are compared with the line itself,
    //! flattened as the store flattens it: nested objects joined with `.`, and an array or an
    //! object with no members left whole at its key.

    use std::path::Path;

    use serde_json::{json, Map, Value};

    use crate::harness::*;

    const DAY: &str = "2024-01-15";

    /// Every row there is, oldest first: each line below has a second of its own.
    const ALL: &str = "MATCH LOG {} ORDER BY time ASC LIMIT 1000";

    /// The values of `k`, one line each: every kind of value one key can hold, and each of
    /// `{}`, `[]`, `null`, `""`, `"{}"` and `"[]"`. A missing key is the line with `other`.
    const KINDS: &[&str] = &[
        r#"["en"]"#,
        r#""[\"en\"]""#,
        "[]",
        r#""[]""#,
        "{}",
        r#""{}""#,
        "null",
        r#""""#,
        "7",
        "1.5",
        "true",
        r#""plain""#,
        r#""2024-01-15T09:00:00Z""#,
        r#"[124]"#,
        r#""[124]""#,
        r#"[{"a":1.5,"b":[null,{}]},null,true,"x"]"#,
        "[[1,[2,[]]],[]]",
        "[null,null]",
        r#"[{"b":1,"a":2},{}]"#,
        r#"["en"]"#,
    ];

    /// The lines of the tests: `KINDS` under one key, then the same kinds in other shapes.
    fn lines() -> Vec<String> {
        let mut bodies: Vec<String> = KINDS.iter().map(|v| format!(r#""k":{v}"#)).collect();
        bodies.extend(
            [
                // No `k` at all.
                r#""other":1"#,
                // An empty object nested, alone and beside a key that is not empty.
                r#""k":{"b":{}}"#,
                r#""k":{"b":{"c":{}},"d":1}"#,
                r#""k":{"b":{"c":1},"d":{}}"#,
                // One template, where `tags` is an array, a string, empty, null, and `a.b`
                // is an empty object, a string and a number.
                r#""a":{"b":{}},"tags":["x","y"],"n":1"#,
                r#""a":{"b":"s"},"tags":"x,y","n":2"#,
                r#""a":{"b":3},"tags":[],"n":3"#,
                r#""a":{"b":{}},"tags":null,"n":4"#,
                r#""a":{"b":[]},"tags":"[\"x\",\"y\"]","n":5"#,
                // Under a key that is promoted when it holds a word.
                r#""event":["a"],"n":1"#,
                r#""event":"a","n":1"#,
                r#""event":{},"n":1"#,
            ]
            .map(str::to_string),
        );
        bodies
            .iter()
            .enumerate()
            .map(|(n, body)| format!(r#"{{"ts":"{DAY}T10:{:02}:{:02}Z",{body}}}"#, n / 60, n % 60))
            .collect()
    }

    /// A line as its row: nested objects joined with `.`; an array, and an object with no
    /// members, whole.
    fn flattened(line: &str) -> Value {
        fn walk(prefix: &str, object: &Map<String, Value>, out: &mut Map<String, Value>) {
            for (key, value) in object {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                match value {
                    Value::Object(inner) if !inner.is_empty() => walk(&path, inner, out),
                    other => {
                        out.insert(path, other.clone());
                    }
                }
            }
        }
        let Value::Object(object) = serde_json::from_str(line).expect("a line is JSON") else {
            panic!("a line is an object");
        };
        let mut out = Map::new();
        walk("", &object, &mut out);
        Value::Object(out)
    }

    fn expected() -> Vec<Value> {
        lines().iter().map(|line| flattened(line)).collect()
    }

    // ── A scratch directory, the binary, and a server ──────────────────────────────────

    impl Scratch {
        fn data(&self) -> String {
            self.path("data")
        }

        fn lines_file(&self, name: &str, lines: &[String]) -> String {
            let body: String = lines.iter().map(|line| format!("{line}\n")).collect();
            self.file(name, &body)
        }
    }

    /// `kibbr <args>`, which must exit 0; its stdout as JSON.
    fn kibbr_json(args: &[&str]) -> Value {
        let out = kibbr().args(args).output().expect("failed to run kibbr");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "kibbr {args:?}:\n{stdout}\n{stderr}");
        serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("not JSON ({e}): {stdout}"))
    }

    /// `kibbr serve <args>` on a port of its own, compacting only when asked.
    fn serve(args: &[&str]) -> Server {
        let mut flags = vec!["--compact-interval-ms", "0"];
        flags.extend_from_slice(args);
        Server::spawn(&[], None, &flags, &[])
    }

    impl Server {
        /// The rows of a query, the system column left out: no line here has that key.
        fn rows(&self, q: &str) -> Vec<Value> {
            let answer = self.query(q);
            answer["rows"]
                .as_array()
                .unwrap_or_else(|| panic!("no rows in {answer}"))
                .iter()
                .map(|row| {
                    let mut row = row.as_object().expect("a row is an object").clone();
                    row.remove("_received_at");
                    Value::Object(row)
                })
                .collect()
        }
    }

    /// Every line, row for row, as itself.
    fn assert_rows_are_the_lines(server: &Server, when: &str) {
        let got = server.rows(ALL);
        let want = expected();
        assert_eq!(got.len(), want.len(), "{when}: {} rows", got.len());
        for (n, (g, w)) in got.iter().zip(&want).enumerate() {
            assert_eq!(g, w, "{when}: line {} came back otherwise", n + 1);
        }
    }

    /// `kibbr compact` of the tests' day; `rebuilt` is whether it had row groups to rebuild.
    fn compact(dir: &Path, rebuilt: bool) {
        let data = dir.display().to_string();
        let report = kibbr_json(&["compact", "--data-dir", &data, "--day", DAY, "--json"]);
        assert_eq!(report["ok"], true, "{report}");
        assert_eq!(report["days"][0]["compacted"], rebuilt, "{report}");
    }

    // ── The tests ──────────────────────────────────────────────────────────────────────

    /// **The store flattens a line as these tests say it does**: the expectation is checked
    /// against what it must be for three lines written out by hand.
    #[test]
    fn the_expectation_is_the_line_flattened_with_arrays_and_empty_objects_whole() {
        assert_eq!(
            flattened(r#"{"ts":"t","k":{"b":{"c":{}},"d":1},"e":[],"f":{},"g":"[]"}"#),
            json!({"ts": "t", "k.b.c": {}, "k.d": 1, "e": [], "f": {}, "g": "[]"})
        );
        assert_eq!(flattened(r#"{"a":{"b":{"c":1}}}"#), json!({"a.b.c": 1}));
        assert_eq!(flattened(r#"{"k":[{"a":{}}]}"#), json!({"k": [{"a": {}}]}));
    }

    /// **Through `/ingest`, held in memory**: every line comes back as itself.
    #[test]
    fn every_kind_comes_back_as_itself_from_memory() {
        let server = serve(&["--storage", "memory"]);
        server.ingest_lines(&lines());
        assert_rows_are_the_lines(&server, "in memory");
    }

    /// **Through `/ingest`, on disk**: while the rows are in buffers and row groups of the
    /// live door, after a restart reads them from files, and after `kibbr compact` has
    /// rebuilt the day - which a second restart reads.
    #[test]
    fn every_kind_comes_back_as_itself_through_ingest_before_and_after_compaction() {
        let scratch = Scratch::new("shapes");
        let live = [
            "--data-dir",
            &scratch.data(),
            "--flush-interval-ms",
            "50",
            "--row-group-timeout-ms",
            "50",
        ];
        let all = lines();
        let server = serve(&live);
        // Three bodies with a wait between, so each template's rows lie in several row
        // groups and a compaction has something to rebuild.
        for part in all.chunks(all.len().div_ceil(3)) {
            server.ingest_lines(part);
            std::thread::sleep(std::time::Duration::from_millis(400));
        }
        assert_rows_are_the_lines(&server, "on disk, warm");
        server.shutdown();

        let server = serve(&live);
        assert_rows_are_the_lines(&server, "on disk, restarted");
        server.shutdown();

        compact(Path::new(&scratch.data()), true);
        let server = serve(&live);
        assert_rows_are_the_lines(&server, "on disk, compacted");
    }

    /// **Through `kibbr load`**: as loaded, in two loads so the day is two files; after
    /// `kibbr compact`, which leaves a loaded day's finished files as they are; and after the
    /// live door has added the same lines to the day and a compaction has rebuilt those.
    #[test]
    fn every_kind_comes_back_as_itself_through_load_before_and_after_compaction() {
        let scratch = Scratch::new("shapes");
        let all = lines();
        let (first, second) = all.split_at(all.len() / 2);
        for (name, part) in [("first.ndjson", first), ("second.ndjson", second)] {
            let file = scratch.lines_file(name, part);
            let report = kibbr_json(&["load", "--data-dir", &scratch.data(), "--json", &file]);
            assert_eq!(report["rows"], part.len() as u64, "{report}");
            assert_eq!(report["rejected"], 0, "{report}");
        }
        let server = serve(&["--data-dir", &scratch.data()]);
        assert_rows_are_the_lines(&server, "loaded");
        server.shutdown();

        compact(Path::new(&scratch.data()), false);
        let server = serve(&["--data-dir", &scratch.data()]);
        assert_rows_are_the_lines(
            &server,
            "loaded, after a compaction with nothing to rebuild",
        );

        // The same lines again through the live door, in three bodies: the day then holds
        // every line twice, the second copy in row groups a compaction rebuilds.
        for part in all.chunks(all.len().div_ceil(3)) {
            server.ingest_lines(part);
        }
        server.shutdown();
        compact(Path::new(&scratch.data()), true);
        let server = serve(&["--data-dir", &scratch.data()]);
        let got = server.rows(ALL);
        let want = expected();
        assert_eq!(got.len(), want.len() * 2);
        // Two rows a line, at one time: both are the line, whichever store each came from.
        for (n, pair) in got.chunks(2).enumerate() {
            assert_eq!(pair, [want[n].clone(), want[n].clone()], "line {}", n + 1);
        }
    }

    /// **A query compares an array as its JSON text, as it always did**: the text finds the
    /// array and the string that spells it, each given back as what it is; `*` finds an array
    /// and an empty object, which are present, and not a null; and a group is labelled by the
    /// text. In memory and from compacted files alike.
    #[test]
    fn an_array_is_matched_and_grouped_by_its_text() {
        let scratch = Scratch::new("shapes");
        let file = scratch.lines_file("all.ndjson", &lines());
        kibbr_json(&["load", "--data-dir", &scratch.data(), "--json", &file]);
        let disk = serve(&["--data-dir", &scratch.data()]);
        let memory = serve(&["--storage", "memory"]);
        memory.ingest_lines(&lines());

        for (server, store) in [(&memory, "memory"), (&disk, "disk")] {
            let found = server.rows(r#"MATCH LOG { "k": "[\"en\"]" } ORDER BY time ASC LIMIT 100"#);
            let values: Vec<&Value> = found.iter().map(|row| &row["k"]).collect();
            assert_eq!(
                values,
                [&json!(["en"]), &json!("[\"en\"]"), &json!(["en"])],
                "{store}"
            );

            let empty = server.rows(r#"MATCH LOG { "k": "{}" } ORDER BY time ASC LIMIT 100"#);
            let values: Vec<&Value> = empty.iter().map(|row| &row["k"]).collect();
            assert_eq!(values, [&json!({}), &json!("{}")], "{store}");

            let present = server.query(r#"COUNT LOG { "k": * }"#);
            let nulls = KINDS.iter().filter(|v| **v == "null").count();
            assert_eq!(present["count"], (KINDS.len() - nulls) as u64, "{store}");

            let nested = server.rows(r#"MATCH LOG { "k.b": * } LIMIT 100"#);
            assert_eq!(nested.len(), 1, "{store}: {nested:?}");
            assert_eq!(nested[0]["k.b"], json!({}), "{store}");

            let groups = server.query(r#"COUNT LOG { "n": * } GROUP BY "tags""#);
            let mut labels: Vec<(String, u64)> = groups["groups"]
                .as_array()
                .expect("groups")
                .iter()
                .map(|g| {
                    let label = g["keys"]["tags"].as_str().expect("a label").to_string();
                    (label, g["count"].as_u64().expect("a count"))
                })
                .collect();
            labels.sort();
            assert_eq!(
                labels,
                [
                    // Three lines of another template have `n` and no `tags`, and one a null.
                    ("(null)".to_string(), 4),
                    ("[\"x\",\"y\"]".to_string(), 2),
                    ("[]".to_string(), 1),
                    ("x,y".to_string(), 1),
                ],
                "{store}"
            );
        }
    }
}

mod time_types {
    //! Timestamps and epoch-like integers from end to end, on the values a preflight over
    //! real datasets found mistyped: MongoDB's `attr.Available`, CockroachDB's
    //! `event.GoAllocBytes` and decimal `timestamp`, Bluesky's `time_us`, PostgreSQL's zoned
    //! `timestamp`. Through the server - in memory, in row groups, and replayed from the
    //! write-ahead log - and through `kibbr query`, where the engine is held to the oracle.

    use serde_json::Value;

    use crate::harness::*;

    const TIME_COLUMNS: &str = "ts,timestamp,time_us";

    /// The lines, in the order they arrive: each column's first value, and then the value
    /// that used to turn it to `String`.
    const LINES: &[&str] = &[
        // MongoDB: a column of epoch seconds, then an integer below the window.
        r#"{"ts":"2023-03-25T02:28:50Z","c":"STORAGE","attr":{"Available":1679711330}}"#,
        r#"{"ts":"2023-03-25T02:28:51Z","c":"STORAGE","attr":{"Available":999999999}}"#,
        r#"{"ts":"2023-03-25T02:28:52Z","c":"STORAGE","attr":{"Available":"1679711332"}}"#,
        // CockroachDB: decimal seconds as the time, and integers that then take ten digits
        // inside the epoch window.
        r#"{"timestamp":"1679711330.570420890","channel":"OPS","event":{"GoAllocBytes":999999999}}"#,
        r#"{"timestamp":"1679711331.000000001","channel":"OPS","event":{"GoAllocBytes":1220305536}}"#,
        r#"{"timestamp":1679711332.5,"channel":"OPS","event":{"GoAllocBytes":7}}"#,
        // Bluesky: sixteen digits of microseconds.
        r#"{"time_us":1732206349000167,"did":"did:plc:a","kind":"commit"}"#,
        r#"{"time_us":1732206349000999,"did":"did:plc:b","kind":"identity"}"#,
        // PostgreSQL jsonlog: a date and time with a named zone.
        r#"{"timestamp":"2023-03-27 00:26:35.719 EDT","pid":27,"message":"starting"}"#,
        r#"{"timestamp":"2023-03-27 00:26:36 EST","pid":28,"message":"ready"}"#,
        // A sixteen-digit id under a key that is no time column, beside a real time.
        r#"{"ts":"2023-03-25T02:28:53Z","order_id":1732206349000167}"#,
    ];

    /// More of the same templates, for the lines a test leaves in the write-ahead log.
    const LATER: &[&str] = &[
        r#"{"ts":"2023-03-25T02:29:00Z","c":"NETWORK","attr":{"Available":5}}"#,
        r#"{"timestamp":"1679711340.25","channel":"DEV","event":{"GoAllocBytes":1220305537}}"#,
        r#"{"time_us":1732206350000001,"did":"did:plc:c","kind":"commit"}"#,
        r#"{"timestamp":"2023-03-27 00:27:00.5 EDT","pid":29,"message":"listening"}"#,
    ];

    /// A query with what it counts over `LINES`, and over `LINES` and `LATER`.
    const COUNTS: &[(&str, u64, u64)] = &[
        ("COUNT LOG {}", 11, 15),
        // The nanosecond CockroachDB wrote, and no other.
        (
            r#"COUNT LOG {} BETWEEN "2023-03-25T02:28:50.570420890Z" AND "2023-03-25T02:28:50.570420891Z""#,
            1,
            1,
        ),
        (
            r#"COUNT LOG {} BETWEEN "2023-03-25T02:28:50.570420891Z" AND "2023-03-25T02:28:51Z""#,
            0,
            0,
        ),
        (
            r#"COUNT LOG { "channel": * } BETWEEN "2023-03-25T02:28:50Z" AND "2023-03-25T02:28:53Z""#,
            3,
            3,
        ),
        // A JSON number of seconds.
        (
            r#"COUNT LOG {} BETWEEN "2023-03-25T02:28:52.5Z" AND "2023-03-25T02:28:52.500000001Z""#,
            1,
            1,
        ),
        // Bluesky's microsecond.
        (
            r#"COUNT LOG {} BETWEEN "2024-11-21T16:25:49.000167Z" AND "2024-11-21T16:25:49.000168Z""#,
            1,
            1,
        ),
        (
            r#"COUNT LOG { "kind": * } BETWEEN "2024-11-21T00:00:00Z" AND "2024-11-22T00:00:00Z""#,
            2,
            3,
        ),
        // PostgreSQL's EDT is four hours behind UTC, EST five.
        (
            r#"COUNT LOG {} BETWEEN "2023-03-27T04:26:35.719Z" AND "2023-03-27T04:26:35.720Z""#,
            1,
            1,
        ),
        (
            r#"COUNT LOG {} BETWEEN "2023-03-27T05:26:36Z" AND "2023-03-27T05:26:37Z""#,
            1,
            1,
        ),
        (
            r#"COUNT LOG { "pid": * } BETWEEN "2023-03-27T00:00:00Z" AND "2023-03-28T00:00:00Z""#,
            2,
            3,
        ),
        // The columns that are integers now, found by the digits they arrived as.
        (r#"COUNT LOG { "attr.Available": "1679711330" }"#, 1, 1),
        (r#"COUNT LOG { "attr.Available": "999999999" }"#, 1, 1),
        (r#"COUNT LOG { "attr.Available": "1679711332" }"#, 1, 1),
        (
            r#"COUNT LOG { "attr.Available": "2023-03-25T02:28:50Z" }"#,
            0,
            0,
        ),
        (r#"COUNT LOG { "event.GoAllocBytes": "1220305536" }"#, 1, 1),
        (r#"COUNT LOG { "event.GoAllocBytes": "999999999" }"#, 1, 1),
        (r#"COUNT LOG { "event.GoAllocBytes": "7" }"#, 1, 1),
        (r#"COUNT LOG { "order_id": "1732206349000167" }"#, 1, 1),
        (r#"COUNT LOG { "time_us": "1732206349000167" }"#, 1, 1),
        (r#"COUNT LOG { "timestamp": "1679711330.570420890" }"#, 1, 1),
        (
            r#"COUNT LOG { "timestamp": "2023-03-27 00:26:35.719 EDT" }"#,
            1,
            1,
        ),
    ];

    /// Queries whose whole answer the engine and the oracle, and a server before and after
    /// a restart, must give alike.
    const BATTERY: &[&str] = &[
        "MATCH LOG {} LIMIT 1000",
        "MATCH LOG {} ORDER BY time ASC LIMIT 1000",
        r#"MATCH LOG { "channel": "OPS" } ORDER BY time ASC LIMIT 1000"#,
        r#"MATCH LOG { "attr.Available": * } LIMIT 1000"#,
        r#"COUNT LOG {} GROUP BY "attr.Available""#,
        r#"COUNT LOG {} GROUP BY "event.GoAllocBytes""#,
        r#"COUNT LOG {} GROUP BY "time_us""#,
        r#"COUNT LOG {} GROUP BY "timestamp""#,
        r#"COUNT LOG { "channel": * } BETWEEN "2023-03-25T02:00:00Z" AND "2023-03-25T04:00:00Z" GROUP BY time(1h)"#,
        r#"COUNT LOG { "kind": * } BETWEEN "2024-11-21T16:25:49Z" AND "2024-11-21T16:25:51Z" GROUP BY time(1s)"#,
        r#"COUNT LOG { "pid": * } BETWEEN "2023-03-27T04:00:00Z" AND "2023-03-27T06:00:00Z" GROUP BY time(30m)"#,
    ];

    fn body(lines: &[&str]) -> Vec<u8> {
        lines
            .iter()
            .flat_map(|l| format!("{l}\n").into_bytes())
            .collect()
    }

    /// The flags every server here is started with.
    const TIMED: &[&str] = &["--time-columns", TIME_COLUMNS];

    // ── A server ───────────────────────────────────────────────────────────────────────

    impl Server {
        /// `(time source, {column: type})` of the one template that has every one of `keys`.
        fn template(&self, keys: &[&str]) -> (String, serde_json::Map<String, Value>) {
            let all = self.call("GET", "/templates", b"");
            let mut found = Vec::new();
            for t in all["templates"].as_array().expect("templates") {
                let columns: serde_json::Map<String, Value> = t["columns"]
                    .as_array()
                    .expect("columns")
                    .iter()
                    .map(|c| {
                        (
                            c["name"].as_str().expect("a name").to_string(),
                            c["type"].clone(),
                        )
                    })
                    .collect();
                if keys.iter().all(|key| columns.contains_key(*key)) {
                    let source = t["time_source"].as_str().expect("a time source");
                    found.push((source.to_string(), columns));
                }
            }
            assert_eq!(found.len(), 1, "templates with {keys:?}: {found:?}");
            found.remove(0)
        }
    }

    fn assert_counts(server: &Server, later: bool, when: &str) {
        for (q, first, both) in COUNTS {
            let want = if later { *both } else { *first };
            assert_eq!(server.query(q)["count"], want, "{when}: `{q}`");
        }
    }

    /// What every template of `LINES` must be: the time source chosen, and the types the
    /// ladder and the widening gave - never `String` for an epoch beside an integer.
    fn assert_templates(server: &Server, when: &str) {
        let (source, columns) = server.template(&["attr.Available"]);
        assert_eq!(source, "column:ts", "{when}");
        assert_eq!(
            columns["attr.Available"], "Int64",
            "{when}: MongoDB's attr.Available"
        );
        assert_eq!(columns["ts"], "Datetime", "{when}");

        let (source, columns) = server.template(&["event.GoAllocBytes"]);
        assert_eq!(
            source, "column:timestamp",
            "{when}: CockroachDB's timestamp"
        );
        assert_eq!(columns["event.GoAllocBytes"], "Int64", "{when}");
        assert_eq!(columns["timestamp"], "Float64", "{when}");

        let (source, columns) = server.template(&["time_us"]);
        assert_eq!(source, "column:time_us", "{when}: Bluesky's time_us");
        assert_eq!(columns["time_us"], "Int64", "{when}");

        let (source, columns) = server.template(&["pid"]);
        assert_eq!(source, "column:timestamp", "{when}: PostgreSQL's timestamp");
        assert_eq!(columns["timestamp"], "String", "{when}");

        let (source, columns) = server.template(&["order_id"]);
        assert_eq!(source, "column:ts", "{when}");
        assert_eq!(columns["order_id"], "Int64", "{when}: a sixteen-digit id");
    }

    /// No row of `LINES` took its arrival time, and each shows the value its line held.
    fn assert_rows(server: &Server, when: &str) {
        let all = server.query("MATCH LOG {} ORDER BY time ASC LIMIT 1000");
        let rows = all["rows"].as_array().expect("rows");
        assert!(rows.len() >= LINES.len(), "{when}: {all}");
        for row in rows {
            assert!(
                row.get("_received_at").is_none(),
                "{when}: a row took its arrival time: {row}"
            );
        }
        // In time order: March 25th's lines, then the 27th's, then November 2024's.
        let first = &rows[0];
        assert_eq!(first["ts"], "2023-03-25T02:28:50Z", "{when}");
        assert_eq!(first["attr.Available"], 1679711330u64, "{when}");
        assert_eq!(rows[1]["timestamp"], "1679711330.570420890", "{when}");
        assert_eq!(rows[1]["event.GoAllocBytes"], 999999999u64, "{when}");
        let last = &rows[rows.len() - 1];
        assert!(
            last["time_us"]
                .as_u64()
                .is_some_and(|t| t >= 1732206349000999),
            "{when}: the newest row is Bluesky's: {last}"
        );

        // The integers a group is labelled by are the digits that arrived.
        let groups = server.query(r#"COUNT LOG { "attr.Available": * } GROUP BY "attr.Available""#);
        let mut labels: Vec<String> = groups["groups"]
            .as_array()
            .expect("groups")
            .iter()
            .map(|g| {
                g["keys"]["attr.Available"]
                    .as_str()
                    .expect("a label")
                    .to_string()
            })
            .collect();
        labels.sort();
        assert_eq!(&labels[..2], ["1679711330", "1679711332"], "{when}");
        assert!(
            labels.contains(&"999999999".to_string()),
            "{when}: {labels:?}"
        );
    }

    // ── Through the server ─────────────────────────────────────────────────────────────

    /// **The five mistyped columns of the preflight are typed, and timed, as they should
    /// be** - in memory, then in row groups after a stop, with every answer unchanged.
    #[test]
    fn the_preflight_values_are_typed_and_timed_in_memory_and_in_row_groups() {
        let db = Db::new();
        let first = db.serve(TIMED);
        first.ingest_lines(LINES);
        assert_templates(&first, "in memory");
        assert_counts(&first, false, "in memory");
        assert_rows(&first, "in memory");
        let before: Vec<Value> = BATTERY.iter().map(|q| first.answer(q)).collect();
        first.shutdown();

        let second = db.serve(TIMED);
        assert_templates(&second, "in row groups");
        assert_counts(&second, false, "in row groups");
        assert_rows(&second, "in row groups");
        for (q, before) in BATTERY.iter().zip(&before) {
            assert_eq!(&second.answer(q), before, "`{q}` across a restart");
        }
    }

    /// **Lines this build logged replay under the times it filed them at**: a server killed
    /// with rows only in its log puts every one back, holds none, and answers as before.
    #[test]
    fn a_time_column_form_replays_from_the_log_at_the_time_it_was_filed_under() {
        let db = Db::new();
        let first = db.serve(TIMED);
        first.ingest_lines(LINES);
        first.shutdown();

        let second = db.serve(TIMED);
        second.ingest_lines(LATER);
        assert_counts(&second, true, "before the kill");
        let before: Vec<Value> = BATTERY.iter().map(|q| second.answer(q)).collect();
        drop(second);

        let third = db.serve(TIMED);
        let replay = third.call("GET", "/stats", b"")["storage"]["wal"]["replay"].clone();
        assert_eq!(
            replay["applied"],
            LATER.len() as u64,
            "{replay}\n{}",
            third.banner
        );
        assert_eq!(replay["held"], 0, "{replay}\n{}", third.banner);
        assert_templates(&third, "after a replay");
        assert_counts(&third, true, "after a replay");
        assert_rows(&third, "after a replay");
        for (q, before) in BATTERY.iter().zip(&before) {
            assert_eq!(&third.answer(q), before, "`{q}` across a kill");
        }
    }

    // ── Through `kibbr query`, against the oracle ──────────────────────────────────────

    fn cli(q: &str, reference: bool) -> Value {
        let lines: Vec<&str> = LINES.iter().chain(LATER).copied().collect();
        cli_over(&lines, q, reference)
    }

    /// `kibbr query` over these lines on stdin: the engine, or with `--reference` the oracle.
    fn cli_over(lines: &[&str], q: &str, reference: bool) -> Value {
        let mut args = vec!["query", "-", q, "--format", "json"];
        args.extend(["--time-columns", TIME_COLUMNS]);
        if reference {
            args.push("--reference");
        }
        let out = run_piped(&args, &body(lines), &[]);
        assert!(
            out.status.success(),
            "kibbr query `{q}` exited {:?}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        let full: Value = serde_json::from_slice(&out.stdout).expect("query did not emit JSON");
        answer_of(&full)
    }

    /// **The oracle, which works the forms and the widening out for itself, answers as the
    /// engine does**: every count above, and the whole answer of every battery query.
    #[test]
    fn the_engine_and_the_oracle_agree_on_the_preflight_values() {
        for (q, _, both) in COUNTS {
            let engine = cli(q, false);
            assert_eq!(engine["count"], *both, "the engine: `{q}`");
            assert_eq!(cli(q, true), engine, "the oracle and the engine: `{q}`");
        }
        // The oracle has one order, newest first: `ORDER BY time ASC` is the engine's alone.
        for q in BATTERY.iter().filter(|q| !q.contains("ORDER BY")) {
            let engine = cli(q, false);
            assert!(
                engine["count"].as_u64().is_some_and(|n| n > 0),
                "`{q}`: {engine}"
            );
            assert_eq!(cli(q, true), engine, "the oracle and the engine: `{q}`");
        }
    }

    // ── What a Datetime column remembers of its values ─────────────────────────────────

    /// Two columns that are `Datetime` after two lines: `counts` has held only epoch digits,
    /// `dated` an RFC 3339 date as well. Then both take an integer.
    const DIGITS_AND_DATES: &[&str] = &[
        r#"{"ts":"2023-03-25T02:28:50Z","counts":1679711330,"dated":1679711330}"#,
        r#"{"ts":"2023-03-25T02:28:51Z","counts":"1679711331000","dated":"2023-03-25T02:28:51Z"}"#,
    ];
    const THEN_AN_INTEGER: &str = r#"{"ts":"2023-03-25T02:28:52Z","counts":5,"dated":5}"#;

    /// **Whether a `Datetime` column held only epoch digits is in the catalog, and decides
    /// what an integer makes of it after a restart**: digits and an integer are `Int64`; a
    /// column that has held an RFC 3339 date is `String`, as it always was. The same either
    /// side of the restart, and the same to the oracle.
    #[test]
    fn a_datetime_column_knows_after_a_restart_whether_it_held_only_digits() {
        let db = Db::new();
        let first = db.serve(TIMED);
        first.ingest_lines(DIGITS_AND_DATES);
        let (_, columns) = first.template(&["counts"]);
        assert_eq!(columns["counts"], "Datetime");
        assert_eq!(columns["dated"], "Datetime");
        first.shutdown();

        let second = db.serve(TIMED);
        second.ingest_lines(&[THEN_AN_INTEGER]);
        let (_, columns) = second.template(&["counts"]);
        assert_eq!(columns["counts"], "Int64", "epoch digits and an integer");
        assert_eq!(
            columns["dated"], "String",
            "an RFC 3339 date and an integer"
        );

        // Every value comes back as its line held it, and is found by what it was.
        let rows = second.query("MATCH LOG {} ORDER BY time ASC LIMIT 10");
        assert_eq!(rows["rows"][0]["counts"], 1679711330u64);
        assert_eq!(rows["rows"][1]["counts"], "1679711331000");
        assert_eq!(rows["rows"][2]["counts"], 5);
        assert_eq!(rows["rows"][0]["dated"], 1679711330u64);
        assert_eq!(rows["rows"][1]["dated"], "2023-03-25T02:28:51Z");
        for (q, count) in [
            (r#"COUNT LOG { "counts": "1679711330" }"#, 1),
            (r#"COUNT LOG { "counts": "1679711331000" }"#, 1),
            (r#"COUNT LOG { "counts": "5" }"#, 1),
            // One instant as a date is not that integer; in the text column it is its text.
            (r#"COUNT LOG { "counts": "2023-03-25T02:28:50Z" }"#, 0),
            (r#"COUNT LOG { "dated": "1679711330" }"#, 1),
            (r#"COUNT LOG { "dated": "2023-03-25T02:28:51Z" }"#, 1),
            (r#"COUNT LOG { "dated": "5" }"#, 1),
        ] {
            assert_eq!(second.query(q)["count"], count, "`{q}`");
            // In one process, with no restart between the lines, and by the oracle.
            let mut lines = DIGITS_AND_DATES.to_vec();
            lines.push(THEN_AN_INTEGER);
            let engine = cli_over(&lines, q, false);
            assert_eq!(engine["count"], count, "the engine, in one run: `{q}`");
            assert_eq!(cli_over(&lines, q, true), engine, "the oracle: `{q}`");
        }
        for q in [
            r#"COUNT LOG {} GROUP BY "counts""#,
            r#"COUNT LOG {} GROUP BY "dated""#,
            "MATCH LOG {} LIMIT 10",
        ] {
            let mut lines = DIGITS_AND_DATES.to_vec();
            lines.push(THEN_AN_INTEGER);
            let engine = cli_over(&lines, q, false);
            assert_eq!(cli_over(&lines, q, true), engine, "the oracle: `{q}`");
            assert_eq!(
                second.answer(q),
                engine,
                "the server after its restart: `{q}`"
            );
        }
    }
}
