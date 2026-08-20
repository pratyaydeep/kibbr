//! Tests target the outer surface. Everything here runs the real binary and
//! reads its stdout; nothing reaches inside the crate.

use std::process::{Command, Stdio};

fn triedb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_triedb"))
}

fn run(args: &[&str]) -> String {
    let out = triedb()
        .args(args)
        .output()
        .expect("failed to run triedb");
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
    let expected: Vec<&str> = fixture
        .lines()
        .filter(|l| !l.trim().is_empty())
        .collect();

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
    let line = r#"{"error":{"code":401,"reason":"unauth"},"tags":["auth","login"],"user":{"id":"u42"}}"#;
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
    assert_eq!(row(block, "tags"), ["tags", "String", r#"["auth","login"]"#]);
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
