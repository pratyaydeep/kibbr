//! The things the query language left open, pinned against the observable answer.
//!
//! Each of these is a choice rather than a discovery, so nothing in the source can be read
//! to check it and no differential can find it: the engine and the oracle would agree just
//! as happily on the opposite choice. `scripts/parser_equivalence.sh` pins the ones that
//! need a corpus; these are the ones that need a shaped fixture, and `--exists`, which the
//! grammar cannot spell and that script therefore does not cover.
//!
//! - **LIMIT and paging.** A limit bounds rows returned, not rows counted; a `MATCH` that
//!   asks for no limit gets every row; and `AFTER` walks the rest without gaps or repeats.
//! - **Row order.** `ts` descending; rows that cannot be placed in time last.
//! - **A template with no `ts` column, under a time filter.** `exists: true, count: 0`.
//! - **`--exists` with a time clause.** A liveness check that reads one row, not a
//!   contradiction and not a walk that ignored the clause.

use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

/// Two templates and no overlap between them: `beat` has no `ts` at all, `hit` has one on
/// every row. Five `hit` rows, so a limit has something to truncate and an order to get
/// wrong.
const FIXTURE: &str = concat!(
    r#"{"event":"beat","node":"a"}"#,
    "\n",
    r#"{"event":"beat","node":"b"}"#,
    "\n",
    r#"{"event":"hit","n":1,"ts":"2024-01-15T10:00:00Z"}"#,
    "\n",
    r#"{"event":"hit","n":2,"ts":"2024-01-15T12:00:00Z"}"#,
    "\n",
    r#"{"event":"hit","n":3,"ts":"2024-01-15T11:00:00Z"}"#,
    "\n",
    r#"{"event":"hit","n":4,"ts":"2024-01-15T14:00:00Z"}"#,
    "\n",
    r#"{"event":"hit","n":5,"ts":"2024-01-15T13:00:00Z"}"#,
    "\n",
);

/// A window that covers the whole fixture, written absolutely so the test does not depend
/// on when it runs.
const ALL: [&str; 3] = ["--between", "2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z"];

fn query(args: &[&str]) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .arg("query")
        .arg("-")
        .args(args)
        .args(["--format", "json"])
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
    serde_json::from_slice(&out.stdout).expect("query did not emit JSON")
}

/// The whole process, for the cases where failing is the assertion.
fn raw(args: &[&str]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .arg("query")
        .arg("-")
        .args(args)
        .args(["--format", "json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn triedb");
    let mut stdin = child.stdin.take().expect("stdin was not piped");
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(FIXTURE.as_bytes());
    });
    let out = child.wait_with_output().expect("failed to wait on triedb");
    writer.join().expect("stdin writer thread panicked");
    out
}

/// Every row rendered, so rows with no `n` column are still comparable.
fn rows_as_text(result: &Value) -> Vec<String> {
    result["rows"]
        .as_array()
        .expect("rows is an array")
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// The `n` column of every returned row, in the order they came back.
fn ns(result: &Value) -> Vec<i64> {
    result["rows"]
        .as_array()
        .expect("rows is an array")
        .iter()
        .map(|row| row["n"].as_i64().expect("n is a number"))
        .collect()
}

// ── Row order ──────────────────────────────────────────────────────────────────────

/// Newest first. Written as the `n` sequence rather than as timestamps, so an ascending
/// implementation fails loudly instead of returning a plausible-looking list.
#[test]
fn rows_come_back_newest_first() {
    let all = query(&[r#"MATCH LOG {"event":"hit"}"#]);
    assert_eq!(ns(&all), [4, 5, 2, 3, 1]);
}

/// The whole point of the order: a limit takes the newest, not the first encountered. The
/// rows were ingested 1,2,3,4,5, so an insertion-order limit would answer `[1, 2]`.
#[test]
fn a_limit_takes_the_newest_rows_not_the_first() {
    assert_eq!(ns(&query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#])), [4, 5]);
    assert_eq!(ns(&query(&[r#"MATCH LOG {"event":"hit"} LIMIT 1"#])), [4]);
    // The flag surface resolves to the same rows.
    assert_eq!(
        ns(&query(&["--fetch", "--match", "event=hit", "--limit", "2"])),
        [4, 5]
    );
}

/// A template with no `ts` column has no timestamp to order by, so its rows fall to the
/// end - after every row that can be placed in time - and keep the order they were
/// ingested in.
#[test]
fn rows_with_no_timestamp_sort_last_in_insertion_order() {
    let all = query(&["MATCH LOG {}"]);
    let rows = all["rows"].as_array().expect("rows is an array");
    let events: Vec<&str> = rows
        .iter()
        .map(|row| row["event"].as_str().expect("event is a string"))
        .collect();
    assert_eq!(events, ["hit", "hit", "hit", "hit", "hit", "beat", "beat"]);
    // Insertion order among the undated ones, which is the documented fallback.
    let nodes: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get("node").and_then(Value::as_str))
        .collect();
    assert_eq!(nodes, ["a", "b"]);
}

// ── LIMIT ──────────────────────────────────────────────────────────────────────────

/// `count` is every row that matched; `rows_returned` is how many survived the limit. A
/// limit that also capped the count would report five errors as two.
#[test]
fn a_limit_bounds_the_rows_and_not_the_count() {
    let limited = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#]);
    assert_eq!(limited["count"], 5);
    assert_eq!(limited["meta"]["rows_returned"], 2);
    assert_eq!(limited["meta"]["truncated"], true);

    let whole = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 5"#]);
    assert_eq!(whole["count"], 5);
    assert_eq!(whole["meta"]["rows_returned"], 5);
    // Exactly filled is not truncated: nothing was dropped.
    assert_eq!(whole["meta"]["truncated"], false);
}

/// `COUNT` returns no rows, so a limit has nothing to bound there and does not silently
/// halve an answer whose whole point is to be a total.
#[test]
fn a_limit_does_not_change_a_count() {
    assert_eq!(query(&[r#"COUNT LOG {"event":"hit"}"#])["count"], 5);
    assert_eq!(query(&[r#"COUNT LOG {"event":"hit"} LIMIT 2"#])["count"], 5);
    assert_eq!(
        query(&[r#"COUNT LOG {"event":"hit"} LIMIT 2"#])["meta"]["truncated"],
        false
    );
    // And the same through GROUP BY, which also returns no rows.
    let grouped = query(&[r#"COUNT LOG {"event":"hit"} GROUP BY event LIMIT 1"#]);
    assert_eq!(grouped["count"], 5);
    assert_eq!(grouped["group_counts"]["hit"], 5);
}

/// `LIMIT 0` is a limit of zero rows, not an absent limit. `Parsed::limit` is an `Option`
/// for exactly this - a 0-for-absent sentinel would make the two indistinguishable.
#[test]
fn limit_zero_is_zero_rows_and_not_the_default() {
    let none = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 0"#]);
    assert_eq!(none["rows"].as_array().expect("rows is an array").len(), 0);
    assert_eq!(none["count"], 5);
    assert_eq!(none["meta"]["truncated"], true);
}

/// A `MATCH` that asks for no limit gets every matching row and no cursor. There is no
/// cap: an answer that silently held rows back would be a different answer to the one
/// asked for, and "show me the errors" is the request that can least afford it.
#[test]
fn a_match_with_no_limit_returns_everything() {
    let all = query(&["MATCH LOG {}"]);
    assert_eq!(all["meta"]["rows_returned"], 7);
    assert_eq!(all["count"], 7);
    assert_eq!(all["meta"]["truncated"], false);
    // Nothing was withheld, so there is no next page to offer.
    assert_eq!(all["meta"]["next_cursor"], Value::Null);
}

// ── Paging ─────────────────────────────────────────────────────────────────────────

/// A cursor is issued only when rows were actually withheld. `rows_returned == limit` is
/// not that fact: the fixture has five `hit` rows, so `LIMIT 5` fills the page exactly and
/// still has nothing after it. A cursor there would page into an empty answer.
#[test]
fn a_cursor_is_issued_only_when_there_is_a_next_page() {
    let short = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#]);
    assert!(short["meta"]["next_cursor"].is_string());

    let exact = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 5"#]);
    assert_eq!(exact["meta"]["rows_returned"], 5);
    assert_eq!(exact["meta"]["next_cursor"], Value::Null);
}

/// The whole point: paging tiles the unpaged answer exactly. No row twice, none missing,
/// same order. Walked at `LIMIT 2` over five rows so the boundary lands mid-answer twice.
#[test]
fn paging_tiles_the_whole_answer_with_no_gaps_or_repeats() {
    let whole = ns(&query(&[r#"MATCH LOG {"event":"hit"}"#]));

    let mut paged = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let q = match &cursor {
            Some(c) => format!(r#"MATCH LOG {{"event":"hit"}} LIMIT 2 AFTER "{c}""#),
            None => r#"MATCH LOG {"event":"hit"} LIMIT 2"#.to_string(),
        };
        let page = query(&[&q]);
        // Every page reports the total, not the page - that is what makes a limit a
        // statement about the response rather than about the answer.
        assert_eq!(page["count"], 5);
        paged.extend(ns(&page));
        match page["meta"]["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert_eq!(paged, whole);
    assert_eq!(paged, [4, 5, 2, 3, 1]);
}

/// Rows a timestamp cannot separate page like any other. `beat` has no `ts` at all, so
/// every one of its rows ranks the same on time and the tiebreak carries the boundary.
#[test]
fn undated_rows_page_without_gaps() {
    let whole = query(&["MATCH LOG {}"]);
    let all: Vec<String> = rows_as_text(&whole);

    let mut paged = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..20 {
        let q = match &cursor {
            Some(c) => format!(r#"MATCH LOG {{}} LIMIT 3 AFTER "{c}""#),
            None => "MATCH LOG {} LIMIT 3".to_string(),
        };
        let page = query(&[&q]);
        paged.extend(rows_as_text(&page));
        match page["meta"]["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    assert_eq!(paged, all);
}

/// A cursor names a leaf by its ordinal among the leaves *this* pattern matched, so the
/// same text against a different pattern names a different leaf. Refused, not answered.
#[test]
fn a_cursor_from_another_query_is_refused() {
    let page = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#]);
    let c = page["meta"]["next_cursor"]
        .as_str()
        .expect("a cursor for a truncated page");

    // Same cursor, different pattern.
    let out = raw(&[&format!(r#"MATCH LOG {{}} LIMIT 2 AFTER "{c}""#)]);
    assert!(!out.status.success(), "a foreign cursor was accepted");

    // And the query that issued it still takes it.
    let ok = query(&[&format!(r#"MATCH LOG {{"event":"hit"}} LIMIT 2 AFTER "{c}""#)]);
    assert_eq!(ok["meta"]["rows_returned"], 2);
}

/// The flag surface pages identically - `--after` is `AFTER`, as `--limit` is `LIMIT`.
#[test]
fn the_flag_surface_pages_the_same_way() {
    let page = query(&["--fetch", "--match", "event=hit", "--limit", "2"]);
    let c = page["meta"]["next_cursor"].as_str().expect("a cursor");
    let next = query(&["--fetch", "--match", "event=hit", "--limit", "2", "--after", c]);
    assert_eq!(ns(&next), [2, 3]);
}

// ── A template with no `ts` column, under a time filter ─────────────────────────────

/// Found, and nothing in it could satisfy the predicate. Not an error, and not a query
/// that quietly dropped its own time clause.
#[test]
fn a_template_with_no_ts_under_a_time_filter_exists_and_counts_zero() {
    let windowed = query(&[&format!(
        r#"COUNT LOG {{"event":"beat"}} BETWEEN {} AND {}"#,
        ALL[1], ALL[2]
    )]);
    assert_eq!(windowed["exists"], true);
    assert_eq!(windowed["count"], 0);
    // The rows were read and rejected, not skipped - the window is what excluded them.
    assert_eq!(windowed["meta"]["rows_scanned"], 2);

    // Without the clause the same template answers with its rows, which is what makes the
    // zero above the time filter's doing rather than the pattern's.
    let open = query(&[r#"COUNT LOG {"event":"beat"}"#]);
    assert_eq!(open["exists"], true);
    assert_eq!(open["count"], 2);

    // And the other of the two empty answers still reads differently.
    let unknown = query(&[r#"COUNT LOG {"event":"no_such_event"}"#]);
    assert_eq!(unknown["exists"], false);
    assert_eq!(unknown["count"], 0);
}

// ── `--exists` and a time clause ────────────────────────────────────────────────────

/// No time clause: the walk, and nothing read. `rows_scanned: 0` is the claim.
#[test]
fn exists_without_a_window_reads_nothing() {
    let out = query(&["--exists", "--match", "event=hit"]);
    assert_eq!(out["exists"], true);
    assert_eq!(out["meta"]["rows_scanned"], 0);
    assert_eq!(out["meta"]["files_opened"], 0);
}

/// With one: "is this firing?", answered by the first row inside the window and stopping
/// there. Five rows are in range and exactly one is read.
#[test]
fn exists_with_a_window_stops_on_the_first_matching_row() {
    let mut args = vec!["--exists", "--match", "event=hit"];
    args.extend(ALL);
    let firing = query(&args);
    assert_eq!(firing["exists"], true);
    assert_eq!(firing["count"], 1);
    assert_eq!(firing["meta"]["rows_scanned"], 1);

    // A window with nothing in it: the template still exists, and nothing is firing.
    let quiet = query(&[
        "--exists",
        "--match",
        "event=hit",
        "--between",
        "2023-01-01T00:00:00Z",
        "2023-02-01T00:00:00Z",
    ]);
    assert_eq!(quiet["exists"], true);
    assert_eq!(quiet["count"], 0);
    // It had to look to find that out, and says so.
    assert_eq!(quiet["meta"]["rows_scanned"], 5);
}
