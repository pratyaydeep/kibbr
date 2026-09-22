//! "No such template" and "that template exists but zero rows matched" are two answers,
//! not one.
//!
//! Both have `count: 0`, so a result that carries only a count cannot tell them apart and
//! nothing downstream can either. Pinned this early because retrofitting it means
//! touching the Elasticsearch translation layer too - by then an unknown field and an
//! empty result have already been the same response for several releases.
//!
//! Pinned here rather than left to the differential in `scripts/agree.sh`, because that
//! script judges the engine against the oracle: if the distinction were dropped from
//! both, they would keep agreeing. These assertions are against the observable answer.

use std::process::{Command, Stdio};

use serde_json::Value;

/// The worked example: three templates, one promoted dimension (`event`), and
/// `user_id` on all three - which is what gives both a promoted and a non-promoted route
/// to each of the two answers.
const FIXTURE: &str = include_str!("../testdata/sample_logs.ndjson");

fn triedb() -> Command {
    Command::new(env!("CARGO_BIN_EXE_triedb"))
}

/// Runs a query with the fixture on stdin. Feeds from a separate thread for the same
/// reason `tests/trie.rs` does: writing inline deadlocks once the input outgrows a pipe
/// buffer.
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
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(FIXTURE.as_bytes());
    });
    let out = child.wait_with_output().expect("failed to wait on triedb");
    writer.join().expect("stdin writer thread panicked");
    assert!(
        out.status.success(),
        "triedb query {args:?} exited {:?}\nstderr: {}",
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

/// Patterns that name a shape the trie has never seen. Every one of these is `exists:
/// false`, whatever the rows would have been.
const UNKNOWN: &[&[&str]] = &[
    // A value the promoted dimension has never held.
    &["--match", "event=login_timeout"],
    // A key no template carries at all.
    &["--match", "nonexistent=*"],
    // Two keys that exist, but never together: `reason` is only on login_failed and
    // `pay_id` only on processed. No single template holds both.
    &["--match", "reason=*", "--match", "pay_id=*"],
];

/// Patterns naming a template that does exist and holds no matching row. Both routes are
/// here because they are settled in different places: the window and the value both in
/// the scan, neither by the walk.
const KNOWN_BUT_EMPTY: &[&[&str]] = &[
    // The template's one row sits outside the window.
    &[
        "--match",
        "event=login_success",
        "--between",
        "2024-01-15T10:31:00Z",
        "2024-01-15T10:32:00Z",
    ],
    // `user_id` is a real column on that template; no row holds this value.
    &[
        "--match",
        "event=login_success",
        "--match",
        "user_id=nobody",
    ],
];

/// Every verb, because the distinction is a property of the result shape rather than of
/// any one of them.
const VERBS: &[&[&str]] = &[
    &["--count"],
    &["--fetch"],
    &["--exists"],
    &["--group-by", "level"],
];

fn with(pattern: &[&str], extra: &[&str]) -> Vec<String> {
    pattern
        .iter()
        .chain(extra.iter())
        .map(|s| (*s).to_string())
        .collect()
}

fn borrow(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

#[test]
fn an_unknown_template_reports_exists_false() {
    for pattern in UNKNOWN {
        for verb in VERBS {
            let args = with(pattern, verb);
            let result = json(&borrow(&args));
            assert_eq!(
                result["exists"],
                Value::Bool(false),
                "{args:?} should be an unknown template"
            );
            assert_eq!(result["count"], Value::from(0), "{args:?}");
            assert_eq!(
                result["meta"]["templates_matched"],
                Value::from(0),
                "{args:?}"
            );
        }
    }
}

#[test]
fn a_known_template_with_no_matching_row_reports_exists_true() {
    for pattern in KNOWN_BUT_EMPTY {
        for verb in VERBS {
            let args = with(pattern, verb);
            let result = json(&borrow(&args));
            assert_eq!(
                result["exists"],
                Value::Bool(true),
                "{args:?} names a template that exists"
            );
            assert_eq!(result["count"], Value::from(0), "{args:?}");
            assert!(
                result["meta"]["templates_matched"].as_u64().unwrap() > 0,
                "{args:?} matched no template"
            );
        }
    }
}

/// The pair, side by side. This is the assertion the other two exist to support: the two
/// answers agree on every field a caller is likely to branch on, and differ on `exists`.
#[test]
fn the_two_answers_differ_only_in_exists() {
    let unknown = json(&["--match", "event=login_timeout", "--count"]);
    let empty = json(&[
        "--match",
        "event=login_success",
        "--match",
        "user_id=nobody",
        "--count",
    ]);

    assert_eq!(unknown["count"], empty["count"]);
    assert_eq!(unknown["rows"], empty["rows"]);
    assert_eq!(unknown["groups"], empty["groups"]);
    // Present and empty on both, rather than absent on both - which the comparison above
    // would pass just as happily.
    assert_eq!(unknown["groups"], Value::Array(Vec::new()));
    assert_ne!(
        unknown["exists"], empty["exists"],
        "a caller reading count alone cannot tell an unknown field from an empty result"
    );
}

/// The CLI output has to carry the distinction too, not only the JSON. An operator
/// reading a table is the first consumer of it.
#[test]
fn the_table_names_an_unknown_template() {
    for pattern in UNKNOWN {
        for verb in VERBS {
            let args = with(pattern, verb);
            let out = query(&borrow(&args));
            assert!(out.contains("unknown template"), "{args:?} printed:\n{out}");
        }
    }
    for pattern in KNOWN_BUT_EMPTY {
        for verb in VERBS {
            let args = with(pattern, verb);
            let out = query(&borrow(&args));
            assert!(
                !out.contains("unknown template"),
                "{args:?} named a template that exists as unknown:\n{out}"
            );
        }
    }
}

/// The oracle draws the same line. It has no trie, so it reaches the verdict by
/// scanning lines for the pattern's key set - a wholly separate route to one answer.
#[test]
fn the_oracle_agrees_about_which_answer_it_is() {
    for pattern in UNKNOWN.iter().chain(KNOWN_BUT_EMPTY.iter()) {
        for verb in VERBS {
            let args = with(pattern, verb);
            let engine = json(&borrow(&args));
            let reference = json(&borrow(&with(&borrow(&args), &["--reference"])));
            assert_eq!(
                engine["exists"], reference["exists"],
                "{args:?}: engine and oracle disagree about whether the template exists"
            );
            assert_eq!(engine["count"], reference["count"], "{args:?}");
            assert_eq!(
                engine["meta"]["templates_matched"], reference["meta"]["templates_matched"],
                "{args:?}"
            );
        }
    }
}
