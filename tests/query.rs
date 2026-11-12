// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr query` from outside the binary: the choices the query language left open,
//! pinned against the observable answer, since engine and oracle would agree on the
//! opposite. One module a subject: `exists` here, then `decisions`, `buckets`, `format`.
//!
//! "No such template" and "that template exists but zero rows matched" are two answers,
//! not one. Both have `count: 0`, so a result that carries only a count cannot tell
//! them apart. Pinned here because the differential judges the engine against the oracle:
//! dropped from both, they would keep agreeing.

use serde_json::Value;

use crate::harness::*;

/// Runs a query over the worked example, `SAMPLE_LOGS`.
fn query(args: &[&str]) -> String {
    query_over(SAMPLE_LOGS, args)
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

/// Patterns naming a template that does exist and holds no matching row: the window and
/// the value are both settled in the scan, neither by the walk.
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

mod decisions {
    //! The things the query language left open, pinned against the observable answer: each is
    //! a choice, so no differential can find it - engine and oracle would agree on the opposite.
    //! Covered: LIMIT and paging, row order, a template with no `ts` under a time filter, and
    //! `--exists` with a time clause.

    use serde_json::Value;

    use crate::harness::*;

    /// Two templates: `beat` has no `ts` at all, `hit` has one on every row. Five `hit` rows,
    /// so a limit has something to truncate and an order to get wrong.
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

    /// Environment variable for the backstop configuration.
    const MAX_ROWS_ENV: &str = "KIBBR__QUERY__MAX_ROWS";

    fn query(args: &[&str]) -> Value {
        let out = raw(args);
        assert!(
            out.status.success(),
            "kibbr query {args:?} exited {:?}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        json_of(&out)
    }

    /// The whole process, for the cases where failing is the assertion.
    fn raw(args: &[&str]) -> std::process::Output {
        raw_env(args, &[])
    }

    /// Test with environment variables: flag > environment > default.
    fn raw_env(args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
        raw_over(FIXTURE, args, env)
    }

    /// The same, over lines of the test's own where `FIXTURE` has no such rows.
    fn raw_over(lines: &'static str, args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
        let mut all = vec!["query", "-"];
        all.extend_from_slice(args);
        all.extend(["--format", "json"]);
        run_piped(&all, lines.as_bytes(), env)
    }

    fn json_of(out: &std::process::Output) -> Value {
        serde_json::from_slice(&out.stdout).expect("query did not emit JSON")
    }

    /// Every row as text, less `_received_at`: the CLI rebuilds its database per invocation,
    /// so a row on arrival time is stamped afresh by every page.
    fn rows_as_text(result: &Value) -> Vec<String> {
        result["rows"]
            .as_array()
            .expect("rows is an array")
            .iter()
            .map(|row| {
                let mut row = row.clone();
                if let Some(obj) = row.as_object_mut() {
                    obj.remove("_received_at");
                }
                row.to_string()
            })
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
        assert_eq!(
            ns(&query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#])),
            [4, 5]
        );
        assert_eq!(ns(&query(&[r#"MATCH LOG {"event":"hit"} LIMIT 1"#])), [4]);
        // The flag surface resolves to the same rows.
        assert_eq!(
            ns(&query(&["--fetch", "--match", "event=hit", "--limit", "2"])),
            [4, 5]
        );
    }

    /// A template with no `ts` column is on arrival time, so its rows are the newest and come
    /// first; they share one instant, and the tiebreak is insertion order.
    #[test]
    fn rows_with_no_timestamp_sort_first_in_insertion_order() {
        let all = query(&["MATCH LOG {}"]);
        let rows = all["rows"].as_array().expect("rows is an array");
        let events: Vec<&str> = rows
            .iter()
            .map(|row| row["event"].as_str().expect("event is a string"))
            .collect();
        assert_eq!(events, ["beat", "beat", "hit", "hit", "hit", "hit", "hit"]);
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

    /// The count in the group whose keys are exactly `want`, or a panic naming what was there.
    fn group(result: &Value, want: &[(&str, &str)]) -> u64 {
        let groups = result["groups"].as_array().expect("groups is an array");
        groups
            .iter()
            .find(|group| want.iter().all(|(key, value)| group["keys"][key] == *value))
            .unwrap_or_else(|| panic!("no group for {want:?} in {groups:?}"))["count"]
            .as_u64()
            .expect("a count")
    }

    /// `COUNT` returns no rows, so a limit has nothing to bound there and does not halve a total.
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
        assert_eq!(group(&grouped, &[("event", "hit")]), 5);
    }

    /// `LIMIT 0` is a limit of zero rows, not an absent limit. `Parsed::limit` is an `Option`
    /// for exactly this - a 0-for-absent sentinel would make the two indistinguishable.
    #[test]
    fn limit_zero_is_zero_rows_and_not_the_default() {
        let none = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 0"#]);
        assert_eq!(none["rows"].as_array().expect("rows is an array").len(), 0);
        assert_eq!(none["count"], 5);
        assert_eq!(none["meta"]["truncated"], true);
        // No row was handed out, so there is none to resume after - and the oracle, which
        // finds a page's last row by counting back from the limit, gives the same answer.
        assert_eq!(none["meta"]["next_cursor"], Value::Null);
        let oracle = query(&["--reference", r#"MATCH LOG {"event":"hit"} LIMIT 0"#]);
        assert_eq!(oracle["rows"], none["rows"]);
        assert_eq!(oracle["count"], 5);
        assert_eq!(oracle["meta"]["next_cursor"], Value::Null);
    }

    /// The largest `LIMIT` there is asks for every row, and gets them: holding one row more
    /// than the limit, to know whether a next page exists, is not a sum that may be done on it.
    #[test]
    fn the_largest_limit_is_every_row() {
        let all = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 18446744073709551615"#]);
        assert_eq!(ns(&all), [4, 5, 2, 3, 1]);
        assert_eq!(all["count"], 5);
        assert_eq!(all["meta"]["truncated"], false);
        assert_eq!(all["meta"]["limit_source"], "caller");
        assert_eq!(all["meta"]["next_cursor"], Value::Null);
    }

    /// A `MATCH` with no limit under the backstop gets every row and no cursor. `limit_source`
    /// names who set the page size, `truncated` whether it fired - both readings are needed.
    #[test]
    fn a_match_with_no_limit_returns_everything_under_the_backstop() {
        for (args, source) in [
            (&["MATCH LOG {}"][..], "backstop"),
            (&["--max-rows", "0", "MATCH LOG {}"][..], "unlimited"),
        ] {
            let all = query(args);
            assert_eq!(all["meta"]["rows_returned"], 7);
            assert_eq!(all["count"], 7);
            assert_eq!(all["meta"]["truncated"], false);
            assert_eq!(all["meta"]["limit_source"], source);
            // Nothing was withheld, so there is no next page to offer.
            assert_eq!(all["meta"]["next_cursor"], Value::Null);
        }
    }

    // ── The max_rows backstop ──────────────────────────────────────────────────────────

    /// A `MATCH` naming no `LIMIT` is cut at the backstop, and the answer says so twice:
    /// `truncated` that the page is short, `limit_source` that the server decided it.
    #[test]
    fn the_backstop_cuts_an_unlimited_match_and_says_who_cut_it() {
        let capped = query(&["--max-rows", "3", "MATCH LOG {}"]);
        assert_eq!(capped["meta"]["rows_returned"], 3);
        // The total is the answer's, not the page's.
        assert_eq!(capped["count"], 7);
        assert_eq!(capped["meta"]["truncated"], true);
        assert_eq!(capped["meta"]["limit_source"], "backstop");
        assert!(
            capped["meta"]["next_cursor"].is_string(),
            "a page cut by the server has to be continuable"
        );
    }

    /// The distinction the backstop rests on: a page size, not a truncation. Cutting an answer
    /// and handing back no way to continue it would be the silent shrink this exists to avoid.
    #[test]
    fn a_backstopped_answer_pages_to_completion() {
        let whole = rows_as_text(&query(&["MATCH LOG {} LIMIT 100"]));

        let mut paged = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..20 {
            // No `LIMIT` on any page: the backstop supplies it every time, which is the shape
            // a client that never learned to page would produce.
            let q = match &cursor {
                Some(c) => format!(r#"MATCH LOG {{}} AFTER "{c}""#),
                None => "MATCH LOG {}".to_string(),
            };
            let page = query(&["--max-rows", "2", &q]);
            paged.extend(rows_as_text(&page));
            match page["meta"]["next_cursor"].as_str() {
                Some(c) => cursor = Some(c.to_string()),
                None => break,
            }
        }
        assert_eq!(paged, whole);
    }

    /// A caller's own `LIMIT` is honoured above the backstop rather than clamped to it: the
    /// backstop is for the query that named nothing.
    #[test]
    fn a_callers_limit_is_honoured_above_the_backstop() {
        let over = query(&["--max-rows", "2", r#"MATCH LOG {"event":"hit"} LIMIT 4"#]);
        assert_eq!(over["meta"]["rows_returned"], 4);
        assert_eq!(over["meta"]["limit_source"], "caller");

        let under = query(&["--max-rows", "1000", r#"MATCH LOG {"event":"hit"} LIMIT 2"#]);
        assert_eq!(under["meta"]["rows_returned"], 2);
        assert_eq!(under["meta"]["limit_source"], "caller");
    }

    /// `COUNT` and `GROUP BY` return no rows, so the backstop has nothing to bound there. A cap
    /// that reached them would report seven rows as two - a wrong answer wearing a right shape.
    #[test]
    fn the_backstop_does_not_reach_a_count_or_a_group_by() {
        let counted = query(&["--max-rows", "2", "COUNT LOG {}"]);
        assert_eq!(counted["count"], 7);
        assert_eq!(counted["meta"]["truncated"], false);
        assert_eq!(counted["meta"]["limit_source"], "unlimited");

        let grouped = query(&["--max-rows", "1", "COUNT LOG {} GROUP BY event"]);
        assert_eq!(grouped["count"], 7);
        assert_eq!(group(&grouped, &[("event", "beat")]), 2);
        assert_eq!(group(&grouped, &[("event", "hit")]), 5);
    }

    /// Precedence: flag > environment > default. An unreadable env value is an error,
    /// not a silent fallback - an operator who set a value must get confirmation or error.
    #[test]
    fn the_environment_sets_the_backstop_and_the_flag_beats_it() {
        let from_env = raw_env(&["MATCH LOG {}"], &[(MAX_ROWS_ENV, "2")]);
        assert!(from_env.status.success());
        let from_env = json_of(&from_env);
        assert_eq!(from_env["meta"]["rows_returned"], 2);
        assert_eq!(from_env["meta"]["limit_source"], "backstop");

        let flagged = raw_env(&["--max-rows", "4", "MATCH LOG {}"], &[(MAX_ROWS_ENV, "2")]);
        assert!(flagged.status.success());
        assert_eq!(json_of(&flagged)["meta"]["rows_returned"], 4);

        let bad = raw_env(&["MATCH LOG {}"], &[(MAX_ROWS_ENV, "lots")]);
        assert!(!bad.status.success(), "an unreadable backstop was ignored");
        assert!(String::from_utf8_lossy(&bad.stderr).contains(MAX_ROWS_ENV));
    }

    // ── Paging ─────────────────────────────────────────────────────────────────────────

    /// A cursor is issued only when rows were withheld, which `rows_returned == limit` does not
    /// say: `LIMIT 5` over five rows fills the page and has nothing after it.
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
    /// its rows share one arrival instant and the tiebreak carries the boundary.
    #[test]
    fn rows_sharing_an_instant_page_without_gaps() {
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

    /// Two templates whose key sets read alike with a NUL after each key - `a\0b`, and `a` with
    /// `b` - are two leaves, and with one instant shared only the leaf's key separates their rows.
    #[test]
    fn rows_of_two_templates_that_spell_alike_page_without_gaps() {
        const LINES: &str = concat!(
            r#"{"a\u0000b":1,"n":1,"ts":"2024-01-15T10:00:00Z"}"#,
            "\n",
            r#"{"a":1,"b":1,"n":2,"ts":"2024-01-15T10:00:00Z"}"#,
            "\n",
        );
        let over = |q: &str| {
            let out = raw_over(LINES, &[q], &[]);
            assert!(out.status.success(), "`{q}` failed");
            json_of(&out)
        };
        let mut whole = ns(&over("MATCH LOG {}"));
        let first = over("MATCH LOG {} LIMIT 1");
        let c = first["meta"]["next_cursor"]
            .as_str()
            .expect("a cursor for a truncated page");
        let second = over(&format!(r#"MATCH LOG {{}} LIMIT 1 AFTER "{c}""#));
        assert_eq!(second["count"], 2);
        assert_eq!(second["meta"]["next_cursor"], Value::Null);

        let paged: Vec<i64> = ns(&first).into_iter().chain(ns(&second)).collect();
        assert_eq!(paged, whole);
        whole.sort_unstable();
        assert_eq!(whole, [1, 2]);
    }

    /// A key that spells a promoted token, beside the promoted token it spells: two shapes,
    /// and both read `event=login`.
    const SPELLED_ALIKE: &str =
        concat!(r#"{"event=login":1}"#, "\n", r#"{"event":"login"}"#, "\n",);

    fn over_spelled_alike(args: &[&str]) -> Value {
        let out = raw_over(SPELLED_ALIKE, args, &[]);
        assert!(
            out.status.success(),
            "{args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        json_of(&out)
    }

    /// They are two templates to the engine, as they are to the oracle: the two tokens are
    /// not one trie edge.
    #[test]
    fn a_key_spelling_a_promoted_token_is_its_own_template() {
        let engine = over_spelled_alike(&["MATCH LOG {}"]);
        let oracle = over_spelled_alike(&["MATCH LOG {}", "--reference"]);
        assert_eq!(oracle["meta"]["templates_matched"], 2);
        assert_eq!(engine["meta"]["templates_matched"], 2);
        assert_eq!(engine["count"], 2);
    }

    /// A pattern naming the promoted value counts the line that has it and not the other,
    /// and finds it in one template.
    #[test]
    fn a_promoted_value_does_not_count_the_key_that_spells_it() {
        for extra in [&[][..], &["--reference"]] {
            let mut args = vec![r#"COUNT LOG {"event":"login"}"#];
            args.extend_from_slice(extra);
            let result = over_spelled_alike(&args);
            assert_eq!(result["count"], 1, "{args:?}");
            assert_eq!(result["meta"]["templates_matched"], 1, "{args:?}");
        }
        let rows = over_spelled_alike(&[r#"MATCH LOG {"event":"login"}"#]);
        assert_eq!(rows_as_text(&rows), [r#"{"event":"login"}"#]);

        // A pattern naming both keys names a template no line has: the two do not share a leaf.
        for extra in [&[][..], &["--reference"]] {
            let mut args = vec![r#"COUNT LOG {"event":"login","event=login":*}"#];
            args.extend_from_slice(extra);
            let result = over_spelled_alike(&args);
            assert_eq!(result["count"], 0, "{args:?}");
            assert_eq!(result["exists"], false, "{args:?}");
            assert_eq!(result["meta"]["templates_matched"], 0, "{args:?}");
        }
    }

    /// Two rows of one instant and two templates must not carry one rank, in the oracle or the
    /// engine. The lines carry a `ts` because arrival time is stamped afresh per invocation.
    #[test]
    fn rows_of_a_key_and_the_promoted_token_it_spells_page_without_gaps() {
        const LINES: &str = concat!(
            r#"{"event=login":1,"ts":"2024-01-15T10:00:00Z"}"#,
            "\n",
            r#"{"event":"login","ts":"2024-01-15T10:00:00Z"}"#,
            "\n",
        );
        for extra in [&["--reference"][..], &[]] {
            let ask = |q: &str| {
                let mut args = vec![q];
                args.extend_from_slice(extra);
                let out = raw_over(LINES, &args, &[]);
                assert!(out.status.success(), "{args:?} failed");
                json_of(&out)
            };
            let whole = rows_as_text(&ask("MATCH LOG {}"));
            let first = ask("MATCH LOG {} LIMIT 1");
            let c = first["meta"]["next_cursor"]
                .as_str()
                .unwrap_or_else(|| panic!("{extra:?}: no cursor for a truncated page"));
            let second = ask(&format!(r#"MATCH LOG {{}} LIMIT 1 AFTER "{c}""#));
            assert_eq!(second["count"], 2, "{extra:?}");
            assert_eq!(second["meta"]["next_cursor"], Value::Null, "{extra:?}");

            let mut paged = rows_as_text(&first);
            paged.extend(rows_as_text(&second));
            assert_eq!(paged, whole, "{extra:?}");
            assert_eq!(whole.len(), 2, "{extra:?}");
            assert_ne!(whole[0], whole[1], "{extra:?}");
        }
    }

    /// What goes in comes back out: the escape is in the trie's label, and the row a `MATCH`
    /// returns names the key as the line did, to the engine and to the oracle.
    #[test]
    fn a_key_holding_an_equals_sign_comes_back_as_it_went_in() {
        for extra in [&[][..], &["--reference"]] {
            for pattern in [
                r#"MATCH LOG {"event=login":"1"}"#,
                r#"MATCH LOG {"event=login":*}"#,
            ] {
                let mut args = vec![pattern];
                args.extend_from_slice(extra);
                let result = over_spelled_alike(&args);
                assert_eq!(result["count"], 1, "{args:?}");
                assert_eq!(result["meta"]["templates_matched"], 1, "{args:?}");
                assert_eq!(rows_as_text(&result), [r#"{"event=login":1}"#], "{args:?}");
            }
        }
    }

    /// A promoted key under a parent whose name holds `=` is still settled by the path: its
    /// token `a\=b.event=login` is split at the bare `=`, not the first.
    #[test]
    fn a_promoted_key_under_a_parent_holding_an_equals_sign_is_settled_by_the_path() {
        const LINES: &str = concat!(
            r#"{"a=b":{"event":"login"},"n":1}"#,
            "\n",
            r#"{"a=b":{"event":"logout"},"n":2}"#,
            "\n",
            r#"{"a":"x","a=b":{"event":"login"},"n":3}"#,
            "\n",
        );
        for (pattern, count, templates) in [
            (r#"{"a=b.event":"login"}"#, 2, 2),
            (r#"{"a=b.event":"logout"}"#, 1, 1),
            (r#"{"a=b.event":"nope"}"#, 0, 0),
            (r#"{"a":"x"}"#, 1, 1),
            (r#"{"a":"b.event=login"}"#, 0, 1),
        ] {
            for extra in [&[][..], &["--reference"]] {
                let q = format!("COUNT LOG {pattern}");
                let mut args = vec![q.as_str()];
                args.extend_from_slice(extra);
                let out = raw_over(LINES, &args, &[]);
                assert!(out.status.success(), "{args:?} failed");
                let result = json_of(&out);
                assert_eq!(result["count"], count, "{args:?}");
                assert_eq!(result["exists"], templates > 0, "{args:?}");
                assert_eq!(result["meta"]["templates_matched"], templates, "{args:?}");
            }
        }
    }

    /// A promoted value that begins the way a pattern's key goes on does not hide the leaf:
    /// `event=login=z` is not another value of the literal key `event=login`.
    #[test]
    fn a_promoted_value_that_spells_the_rest_of_a_key_does_not_hide_its_leaf() {
        const LINES: &str = concat!(r#"{"event":"login=z","event=login":"q"}"#, "\n");
        for extra in [&[][..], &["--reference"]] {
            let mut args = vec![r#"MATCH LOG {"event=login":"q"}"#];
            args.extend_from_slice(extra);
            let out = raw_over(LINES, &args, &[]);
            assert!(out.status.success(), "{args:?} failed");
            let result = json_of(&out);
            assert_eq!(result["count"], 1, "{args:?}");
            assert_eq!(result["rows"][0]["event=login"], "q", "{args:?}");
            assert_eq!(result["rows"][0]["event"], "login=z", "{args:?}");
        }
    }

    /// A cursor names a leaf by its ordinal among the leaves *this* pattern matched, so the
    /// same text against a different pattern names a different leaf. Refused, not answered.
    #[test]
    fn a_cursor_from_another_query_is_refused() {
        let page = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#]);
        let c = page["meta"]["next_cursor"]
            .as_str()
            .expect("a cursor for a truncated page");

        let out = raw(&[&format!(r#"MATCH LOG {{}} LIMIT 2 AFTER "{c}""#)]);
        assert!(!out.status.success(), "a foreign cursor was accepted");

        // And the query that issued it still takes it.
        let ok = query(&[&format!(
            r#"MATCH LOG {{"event":"hit"}} LIMIT 2 AFTER "{c}""#
        )]);
        assert_eq!(ok["meta"]["rows_returned"], 2);
    }

    /// "Another query" is another pattern, however alike the two print: the guard is over the
    /// terms themselves, so each of these refuses the other's cursor.
    #[test]
    fn a_cursor_from_a_pattern_that_prints_the_same_is_refused() {
        for (issued_by, sent_to) in [
            (r#"{"event": *}"#, r#"{"event": "*"}"#),
            (
                r#"{"event": "hit", "n": *}"#,
                r#"{"event": "hit\u0000n=*"}"#,
            ),
            (r#"{"event": "hit"}"#, r#"{"event=hit": ""}"#),
        ] {
            let page = query(&[&format!("MATCH LOG {issued_by} LIMIT 2")]);
            let c = page["meta"]["next_cursor"]
                .as_str()
                .expect("a cursor for a truncated page");
            let out = raw(&[&format!(r#"MATCH LOG {sent_to} LIMIT 2 AFTER "{c}""#)]);
            assert!(
                !out.status.success(),
                "a cursor of {issued_by} was accepted by {sent_to}"
            );
            let said = String::from_utf8_lossy(&out.stderr);
            assert!(said.contains("different query"), "{sent_to}: {said}");
        }
    }

    /// A cursor carrying any other version prefix is refused before its rank is read: the
    /// components mean different things across a version.
    #[test]
    fn a_cursor_from_another_version_is_refused() {
        let page = query(&[r#"MATCH LOG {"event":"hit"} LIMIT 2"#]);
        let c = page["meta"]["next_cursor"].as_str().expect("a cursor");
        let body = c.split_once('.').expect("a versioned cursor").1;

        for stale in [format!("v1.{body}"), format!("v99.{body}")] {
            let out = raw(&[&format!(
                r#"MATCH LOG {{"event":"hit"}} LIMIT 2 AFTER "{stale}""#
            )]);
            assert!(!out.status.success(), "{stale} was accepted");
            let said = String::from_utf8_lossy(&out.stderr);
            assert!(said.contains("version"), "unhelpful refusal: {said}");
        }
    }

    /// A cursor nobody minted. Refused as malformed rather than decoded into some rank that
    /// happens to parse - the bytes are opaque precisely so that no one can write one.
    #[test]
    fn a_forged_cursor_is_refused() {
        for forged in ["v2.", "v2.zz", "v2.0011", "not-a-cursor", ""] {
            let out = raw(&[&format!(
                r#"MATCH LOG {{"event":"hit"}} LIMIT 2 AFTER "{forged}""#
            )]);
            assert!(!out.status.success(), "`{forged}` was accepted");
        }
    }

    /// The flag surface pages identically - `--after` is `AFTER`, as `--limit` is `LIMIT`.
    #[test]
    fn the_flag_surface_pages_the_same_way() {
        let page = query(&["--fetch", "--match", "event=hit", "--limit", "2"]);
        let c = page["meta"]["next_cursor"].as_str().expect("a cursor");
        let next = query(&[
            "--fetch",
            "--match",
            "event=hit",
            "--limit",
            "2",
            "--after",
            c,
        ]);
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
        // `leaves_opened` beside `files_opened`: the CLI's store is in memory and reads no file
        // whatever the query, so the second alone could not fail.
        assert_eq!(out["meta"]["leaves_opened"], 0);
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
}

mod buckets {
    //! `GROUP BY time(...)` as an operator sees it: what the engine cannot see about itself.
    //! An empty hour reaches the terminal as zeros, a refusal is a refusal and not a short
    //! chart, and the two surfaces and the three formats agree about the same buckets.

    use serde_json::Value;

    use crate::harness::*;

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
        let mut all = vec!["query", "-"];
        all.extend_from_slice(args);
        run_piped(&all, fixture().as_bytes(), &[])
    }

    fn query(args: &[&str]) -> String {
        query_over(&fixture(), args)
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
        let first: Value =
            serde_json::from_str(stream.lines().next().expect("a line")).expect("json");
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
}

mod format {
    //! `--format ndjson`: one JSON object per line, no envelope, nothing else on stdout.
    //! Something downstream parses this line by line, so a line that is not a record, or a
    //! record that is not a line, is the bug this file is watching for.

    use std::process::Stdio;

    use serde_json::Value;

    use crate::harness::*;

    /// stdout and stderr apart, because for this format the difference between them is the
    /// point: "no such template" lives on stderr precisely so it stays out of stdout.
    fn query(args: &[&str]) -> (String, String) {
        let mut all = vec!["query", "-"];
        all.extend_from_slice(args);
        let out = run_piped(&all, SAMPLE_LOGS.as_bytes(), &[]);
        assert!(
            out.status.success(),
            "kibbr query {args:?} exited {:?}",
            out.status.code()
        );
        (text(&out.stdout), text(&out.stderr))
    }

    /// Every line, parsed. A line that is not an object fails here rather than downstream.
    fn objects(stdout: &str) -> Vec<Value> {
        stdout
            .lines()
            .map(|line| {
                let value: Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("not JSON: {line:?}\n{e}"));
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

    /// "No such template" and "exists and nothing matched" both print no rows: the first says so
    /// on stderr, so stdout is an empty stream either way and an operator sees two answers.
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

    /// A `Datetime` is RFC 3339 in every format, with sub-second precision kept; the
    /// `typed_values` of `parse --json` are the deliberate exception (`tests/values.rs`).
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
        let mut child = kibbr()
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
            .expect("failed to spawn kibbr");
        let mut stdin = child.stdin.take().expect("stdin was not piped");
        std::thread::spawn(move || {
            use std::io::Write;
            stdin.write_all(br#"{"ts":"2024-01-15T10:30:00.123456789Z","event":"x"}"#)
        });
        let out = child.wait_with_output().expect("failed to run kibbr");
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
    /// ladder reads it back as a `Datetime`.
    #[test]
    fn the_output_is_ingestable() {
        use std::io::Write;

        let (stdout, _) = query(&[r#"MATCH LOG {"service": "auth"}"#, "--format", "ndjson"]);

        let mut child = kibbr()
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
            .expect("failed to spawn kibbr");
        let mut stdin = child.stdin.take().expect("stdin was not piped");
        std::thread::spawn(move || stdin.write_all(stdout.as_bytes()));
        let out = child.wait_with_output().expect("failed to run kibbr");

        assert!(out.status.success());
        assert_eq!(
            objects(&String::from_utf8_lossy(&out.stdout)),
            vec![serde_json::json!({"count": 2})]
        );
    }
}
