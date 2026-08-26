//! The TrieQL parser, from outside the binary: the grammar's own examples parse, a
//! malformed query reports a position inside the query, and nothing makes it panic.
//!
//! That a TrieQL query and the flags it translates return the same `QueryResult` is
//! `scripts/parser_equivalence.sh`, over a hundred generated combinations rather than the
//! handful anyone would write here. What is pinned below is the translation itself, one
//! case per grammar clause.

use std::process::{Command, Output, Stdio};

const FIXTURE: &str = include_str!("../testdata/sample_logs.ndjson");

fn triedb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_triedb"))
}

/// `triedb explain`, which parses and executes nothing - the parser on its own.
fn explain(query: &str) -> Output {
    triedb()
        .args(["explain", query])
        .output()
        .expect("failed to run triedb")
}

/// Runs a query over the fixture. Fed from a thread for the reason `tests/query.rs`
/// gives: writing inline deadlocks once the input outgrows a pipe buffer.
fn query(args: &[&str]) -> String {
    use std::io::Write;

    let mut child = triedb()
        .arg("query")
        .arg("-")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    std::thread::spawn(move || stdin.write_all(FIXTURE.as_bytes()));
    let out = child.wait_with_output().expect("failed to run triedb");
    assert!(
        out.status.success(),
        "triedb query {args:?} exited {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The `--format json` body, minus the one field two runs of the same engine never share.
fn answer(args: &[&str]) -> String {
    let text = query(args);
    match text.find(",\"duration_ms\"") {
        Some(at) => format!("{}}}}}\n", &text[..at]),
        None => text,
    }
}

// ── The grammar ────────────────────────────────────────────────────────────────────

/// The two queries the grammar prints as its own examples. If these do not parse,
/// nothing else in this file is worth reading.
#[test]
fn grammar_examples_parse() {
    for q in [
        r#"COUNT LOG {"event": "login_failed"} SINCE 1h"#,
        r#"MATCH LOG {"event": "processed"} SINCE 24h GROUP BY level"#,
    ] {
        let out = explain(q);
        assert!(
            out.status.success(),
            "grammar example failed to parse: {q}\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// Every clause of the grammar, and the shapes the productions allow but the examples do
/// not show: the empty pattern, the bare `*`, `BETWEEN`, a multi-key `GROUP BY`.
#[test]
fn every_production_parses() {
    for q in [
        r#"COUNT LOG {}"#,
        r#"MATCH LOG {}"#,
        r#"COUNT LOG {"event": "login_ok"}"#,
        r#"COUNT LOG {"event": "login_ok", "level": "info"}"#,
        r#"COUNT LOG {"user_id": *}"#,
        r#"COUNT LOG {} SINCE 90s"#,
        r#"COUNT LOG {} SINCE 5m"#,
        r#"COUNT LOG {} SINCE 7d"#,
        r#"COUNT LOG {} BETWEEN 2024-01-01T00:00:00Z AND 2024-02-01T00:00:00Z"#,
        r#"MATCH LOG {} GROUP BY level"#,
        r#"MATCH LOG {} GROUP BY level, service"#,
        r#"MATCH LOG {"event": "login_ok"} SINCE 1h GROUP BY level, service"#,
    ] {
        let out = explain(q);
        assert!(
            out.status.success(),
            "failed to parse: {q}\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// The grammar is a closed list. Everything here is something a query language obviously
/// ought to have and the grammar does not list. Each has to be a parse error rather than
/// a quietly accepted extension.
#[test]
fn nothing_outside_the_grammar_parses() {
    for q in [
        r#"COUNT LOG {"a": "b"} ORDER BY ts"#,
        r#"COUNT LOG {"a": "b"} AND {"c": "d"}"#,
        r#"COUNT LOG {"a": "b"} OR {"c": "d"}"#,
        r#"COUNT LOG {"a": > 5}"#,
        r#"COUNT LOG {"a": "b"} WHERE c = d"#,
        r#"SELECT * FROM LOG"#,
        r#"DELETE LOG {"a": "b"}"#,
        r#"EXISTS LOG {"a": "b"}"#,
        r#"COUNT LOG {"a": {"b": "c"}}"#,
        r#"COUNT LOG {"a": 5}"#,
        r#"COUNT LOG {"a": "b"} SINCE 1h SINCE 2h"#,
        r#"COUNT LOG {"a": "b"} GROUP BY a GROUP BY b"#,
        r#"COUNT LOG {"a": "b"} GROUP BY a SINCE 1h"#, // the grammar orders the two clauses
        r#"COUNT LOG {"a": "b",}"#,                    // no trailing comma in the pattern
        r#"COUNT LOG {} SINCE "1h""#,                  // `duration` is spelled out, unquoted
        r#"COUNT LOG {} LIMIT 5 LIMIT 6"#,
        r#"COUNT LOG {} LIMIT "5""#, // `number` is spelled out, unquoted
        r#"COUNT LOG {} LIMIT 5 SINCE 1h"#, // the grammar orders the clauses
        r#"COUNT LOG {} LIMIT 5 GROUP BY a"#,
        r#"MATCH LOG {} AFTER v1.00"#,          // a cursor is quoted
        r#"MATCH LOG {} AFTER "not-a-cursor""#, // and has to be one
        r#"MATCH LOG {} AFTER "v2.00""#,        // of a version this build issues
        r#"MATCH LOG {} OFFSET 10"#,            // paging is `AFTER`, not an offset
    ] {
        let out = explain(q);
        assert!(
            !out.status.success(),
            "accepted something the grammar does not list: {q}\n{}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
}

// ── Positions ──────────────────────────────────────────────────────────────────────

/// The byte position out of the error message, which is the only place it is observable.
fn reported_position(stderr: &str) -> usize {
    let at = stderr
        .find("at byte ")
        .unwrap_or_else(|| panic!("no position in the error:\n{stderr}"));
    stderr[at + "at byte ".len()..]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unparseable position in the error:\n{stderr}"))
}

/// Every malformed query names a byte in `0..=len`, where the one-past-the-end value is
/// "the query stopped early". A position outside that is a position pointing at someone
/// else's text.
#[test]
fn errors_report_a_position_inside_the_query() {
    // Multibyte on purpose in several of these: a parser counting characters where it
    // promised bytes lands short, and a parser slicing bytes where it should count
    // characters panics.
    for q in [
        "",
        " ",
        "{}",
        "COUNT",
        "COUNT LOG",
        "COUNT LOG {",
        r#"COUNT LOG {"a""#,
        r#"COUNT LOG {"a":"#,
        r#"COUNT LOG {"a": "b""#,
        r#"COUNT LOG {"a" "b"}"#,
        r#"COUNT LOG {"": "b"}"#,
        r#"COUNT LOG {"é": }"#,
        r#"COUNT LOG {"日本語": "ü"} SINCE"#,
        r#"COUNT LOG {} SINCE 5x"#,
        r#"COUNT LOG {} SINCE ü"#,
        r#"COUNT LOG {} SINCE 99999999999999999999d"#,
        r#"COUNT LOG {} BETWEEN"#,
        r#"COUNT LOG {} BETWEEN nonsense AND 2024-01-01T00:00:00Z"#,
        r#"COUNT LOG {} BETWEEN 2024-02-01T00:00:00Z AND 2024-01-01T00:00:00Z"#,
        r#"COUNT LOG {} GROUP"#,
        r#"COUNT LOG {} GROUP BY"#,
        r#"COUNT LOG {} GROUP BY a,"#,
        r#"COUNT LOG {"a": "\q"}"#,
        r#"COUNT LOG {"a": "\ud800"}"#,
        r#"COUNT LOG {"a": "\u00"}"#,
        "COUNT LOG {\"a\": \"b\"}\n\nORDER BY ts",
        r#"COUNT LOG {} LIMIT"#,
        r#"COUNT LOG {} LIMIT -1"#,
        r#"COUNT LOG {"ü": "ö"} LIMIT 🙂"#,
        "🙂 LOG {}",
        r#"COUNT LOG {"🙂": "🙂"} SINCE 🙂"#,
    ] {
        let out = explain(q);
        assert!(!out.status.success(), "expected a parse error for {q:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        let pos = reported_position(&stderr);
        assert!(
            pos <= q.len(),
            "{q:?} is {} bytes but the error names byte {pos}\n{stderr}",
            q.len()
        );
        assert!(
            q.is_char_boundary(pos.min(q.len())),
            "{q:?}: byte {pos} is inside a character\n{stderr}"
        );
    }
}

// ── Never panics ───────────────────────────────────────────────────────────────────

/// Whatever goes in, what comes out is a parse error or a query - never a panic. 101 is
/// the exit code of a Rust panic, and it is the only outcome this rejects.
///
/// A NUL byte cannot be passed through `argv` at all, so the one input this surface
/// cannot carry is checked against the parser directly, in `src/trieql.rs`.
#[test]
fn no_input_panics() {
    let mut hostile: Vec<String> = vec![
        String::new(),
        "\u{feff}COUNT LOG {}".to_string(),          // BOM
        "COUNT\u{a0}LOG {}".to_string(),             // non-breaking space
        "COUNT LOG {\"a\": \"b\"}\r\n".to_string(),  // CRLF
        "\"".to_string(),
        "\\".to_string(),
        r#"COUNT LOG {"a": "\"#.to_string(),
        r#"COUNT LOG {"a": "\u"#.to_string(),
        r#"COUNT LOG {"a": "\ud800\ud800"}"#.to_string(),
        r#"COUNT LOG {"a": "\udfff"}"#.to_string(),
        "COUNT LOG {\"a\": \"\u{10ffff}\"}".to_string(),
        ":::::".to_string(),
        "{{{{{".to_string(),
        ",,,,,".to_string(),
        "COUNT LOG {} SINCE d".to_string(),
        "COUNT LOG {} SINCE -1h".to_string(),
        "COUNT LOG {} SINCE 1000000000000d".to_string(),
        "COUNT LOG {} SINCE 18446744073709551615d".to_string(),
        "COUNT LOG {} BETWEEN 9999-12-31T23:59:59Z AND +262143-12-31T23:59:59Z".to_string(),
    ];

    // Deep, long, and wide. The grammar has no recursive production, so none of these can
    // reach the stack - which is the claim, and it is worth an assertion rather than a
    // reading of the grammar.
    hostile.push("{".repeat(100_000));
    hostile.push(format!("COUNT LOG {}{}", "{".repeat(50_000), "}".repeat(50_000)));
    hostile.push(format!(r#"COUNT LOG {{"a": "{}"}}"#, "x".repeat(100_000)));
    hostile.push(format!("COUNT LOG {{{}}}", r#""a": "b", "#.repeat(5_000)));
    hostile.push(format!("COUNT LOG {{}} GROUP BY {}", vec!["k"; 20_000].join(",")));
    hostile.push(format!("COUNT LOG {{}} SINCE {}h", "9".repeat(50_000)));
    hostile.push("é".repeat(100_000));

    for q in &hostile {
        let out = explain(q);
        let code = out.status.code();
        assert_ne!(
            code,
            Some(101),
            "panicked on a {}-byte input starting {:?}\n{}",
            q.len(),
            q.chars().take(40).collect::<String>(),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            matches!(code, Some(0) | Some(1)),
            "exited {code:?} on a {}-byte input",
            q.len()
        );
    }
}

// ── The translation ────────────────────────────────────────────────────────────────

/// One case per grammar clause, against the flags that already meant it. The generated
/// hundred are in `scripts/parser_equivalence.sh`; these are the ones a reader can check
/// by eye.
///
/// Every value here is one the fixture actually holds. A pattern that matches no template
/// answers `exists: false, count: 0, rows: [], group_counts: {}` whatever the mode asked
/// for, so a pair built on a value that is not there agrees with itself and checks
/// nothing - which is what `answers_rows` is asserting against.
#[test]
fn trieql_answers_what_the_flags_answer() {
    let pairs: [(&str, &[&str]); 8] = [
        (r#"COUNT LOG {}"#, &["--count"]),
        (
            r#"COUNT LOG {"event": "login_success"}"#,
            &["--match", "event=login_success", "--count"],
        ),
        (
            r#"MATCH LOG {"event": "login_success"}"#,
            &["--match", "event=login_success", "--fetch"],
        ),
        (
            r#"COUNT LOG {"user_id": *}"#,
            &["--match", "user_id=*", "--count"],
        ),
        (
            r#"COUNT LOG {"service": "auth", "level": "INFO"}"#,
            &["--match", "service=auth", "--match", "level=INFO", "--count"],
        ),
        (
            r#"MATCH LOG {} GROUP BY event, level"#,
            &["--group-by", "event,level"],
        ),
        (
            r#"COUNT LOG {} BETWEEN 2024-01-15T10:31:00Z AND 2024-06-01T00:00:00Z"#,
            &["--between", "2024-01-15T10:31:00Z", "2024-06-01T00:00:00Z", "--count"],
        ),
        // The other answer, on purpose: "no such template" has to translate too.
        (
            r#"COUNT LOG {"event": "no_such_event"}"#,
            &["--match", "event=no_such_event", "--count"],
        ),
    ];

    let mut answers_rows = 0;
    for (trieql, flags) in pairs {
        let mut with_flags = flags.to_vec();
        with_flags.extend(["--format", "json"]);
        let from_text = answer(&[trieql, "--format", "json"]);
        assert_eq!(
            from_text,
            answer(&with_flags),
            "`{trieql}` and {flags:?} disagree"
        );
        if from_text.contains(r#""exists":true"#) && !from_text.contains(r#""count":0,"#) {
            answers_rows += 1;
        }
    }
    assert!(
        answers_rows >= 6,
        "only {answers_rows} of the pairs matched anything - the rest agreed on nothing"
    );
}

/// `--group-by` beats `--fetch` on the flag surface, so `GROUP BY` beats `MATCH` here.
/// A `Query` has one mode field, and the same rule fills it on both surfaces.
#[test]
fn group_by_settles_the_mode_under_either_verb() {
    let flags = answer(&[
        "--match",
        "service=auth",
        "--group-by",
        "level",
        "--format",
        "json",
    ]);
    // Two groups, so the answer distinguishes `GroupBy` from every other mode: `Fetch`
    // would return rows and an empty `group_counts`, `Count` a bare count.
    assert!(flags.contains(r#""ERROR":1"#), "{flags}");
    assert!(flags.contains(r#""INFO":1"#), "{flags}");

    for verb in ["MATCH", "COUNT"] {
        let q = format!(r#"{verb} LOG {{"service": "auth"}} GROUP BY level"#);
        assert_eq!(answer(&[&q, "--format", "json"]), flags, "{q}");
    }
}

/// `SINCE` narrows.
///
/// The fixture is from 2024 and the window is relative to now, so an hour-wide one is
/// empty on both surfaces - which is evidence the clause was applied only alongside the
/// same query without it, which is not. Without the first assertion a parser that read
/// `SINCE` and threw it away would pass this, as `scripts/parser_equivalence.sh` found
/// when that mutation was tried against it.
#[test]
fn since_narrows_the_window() {
    let all = answer(&[r#"COUNT LOG {}"#, "--format", "json"]);
    let hour = answer(&[r#"COUNT LOG {} SINCE 1h"#, "--format", "json"]);
    assert!(all.contains(r#""count":3"#), "{all}");
    assert!(hour.contains(r#""exists":true,"count":0"#), "{hour}");
    assert_eq!(
        hour,
        answer(&["--since", "1h", "--count", "--format", "json"])
    );
}

/// Keywords are case-insensitive; the duration's unit is not, because the grammar spells
/// it out and `--since` reads it the same way. The pair that matters is the second one:
/// the two surfaces have to refuse the same input, not merely accept the same input.
#[test]
fn keywords_ignore_case_and_duration_units_do_not() {
    assert!(explain(r#"count log {"a": "b"} since 1h group by level"#)
        .status
        .success());

    let trieql = explain(r#"COUNT LOG {} SINCE 1H"#);
    let flags = triedb()
        .args(["query", "-", "--since", "1H", "--count"])
        .output()
        .expect("failed to run triedb");
    assert!(!trieql.status.success(), "TrieQL took an uppercase unit");
    assert!(!flags.status.success(), "--since took an uppercase unit");
}

/// A character with no glyph is named rather than printed. Quoting a BOM back invisibly
/// would report `found \`COUNT\`` against a query whose verb reads as `COUNT`.
#[test]
fn invisible_characters_are_named_in_the_error() {
    let out = explain("\u{feff}COUNT LOG {}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success());
    assert!(stderr.contains("\\u{feff}"), "{stderr}");
}

/// The two surfaces are exclusive: a query half in a string and half in flags is a query
/// nobody can read, and clap rejects it before either parser runs.
#[test]
fn the_two_surfaces_do_not_mix() {
    let out = triedb()
        .args(["query", "-", r#"COUNT LOG {}"#, "--match", "a=b"])
        .output()
        .expect("failed to run triedb");
    assert!(!out.status.success());
}

/// `triedb explain` reads no data - it takes no file to read.
#[test]
fn explain_reports_the_parse_and_reads_nothing() {
    let out = explain(r#"MATCH LOG {"event": "login_ok"} SINCE 1h GROUP BY level"#);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(text.contains("group by level"), "{text}");
    assert!(text.contains("event=login_ok"), "{text}");
    assert!(text.contains("ts in ["), "{text}");
}
