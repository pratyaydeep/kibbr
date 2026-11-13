// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[test]
fn the_four_shapes_are_the_texts_a_saved_result_is_compared_on() {
    let shapes = queries();
    let texts: Vec<&str> = shapes.each().iter().map(|q| q.text.as_str()).collect();
    assert_eq!(
        texts,
        [
            r#"COUNT LOG {"event":"login_ok"}"#,
            r#"COUNT LOG {"level":"error"}"#,
            r#"MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}"#,
            "MATCH LOG {} LIMIT 500",
        ]
    );
    assert_eq!(
        TRIE_SOURCE,
        format!(
            "gen::generate(lines {TRIE_LINES}, templates {TRIE_FAMILIES}, seed \
                 {TRIE_SEED}) through Trie::ingest"
        )
    );
}

#[test]
fn the_trie_holds_exactly_ten_thousand_templates_and_each_shape_plans_to_its_leaves() {
    let run = measure(2).expect("the plan step");
    assert_eq!(
        run.templates, 10_000,
        "the count the plan step is named for"
    );
    assert_eq!((run.nodes, run.tokens), (57_642, 98_495));
    assert_eq!(
        run.lines_skipped, 0,
        "the generator writes no line the parser refuses"
    );
    // What each shape plans to today. A later build may plan faster and must still
    // plan to these: a saved result holds them, and a table fails on any other.
    let leaves: Vec<u64> = run.shapes.each().iter().map(|shape| shape.leaves).collect();
    assert_eq!(leaves, [1_360, 4_460, 1_005, 10_000]);
    for shape in run.shapes.each() {
        assert_eq!(shape.ms.len(), 2);
        assert!(shape.ms.iter().all(|ms| *ms > 0.0), "{:?}", shape.ms);
        assert_eq!(
            shape.p50_ms,
            percentile(&shape.ms, 0.50).expect("two timings")
        );
        assert_eq!(
            shape.p99_ms,
            percentile(&shape.ms, 0.99).expect("two timings")
        );
        assert!(shape.min_ms <= shape.p50_ms && shape.p50_ms <= shape.p99_ms);
    }
}

/// A line as the child prints it: what `measure` returns, through serde, and a line
/// feed.
fn printed(run: &PlanRun) -> Vec<u8> {
    format!(
        "{}\n",
        serde_json::to_string(run).expect("a plan run is JSON")
    )
    .into_bytes()
}

fn a_run(samples: usize) -> PlanRun {
    let shape = |ms: f64, leaves: u64| PlanShape {
        ms: vec![ms; samples],
        p50_ms: ms,
        p99_ms: ms,
        min_ms: ms,
        leaves,
    };
    PlanRun {
        templates: TEMPLATES,
        nodes: 57_642,
        tokens: 98_495,
        lines_skipped: 0,
        shapes: Shapes {
            p1: shape(4.58, 1_360),
            p2: shape(16.2, 4_460),
            p3: shape(14.4, 1_005),
            p4: shape(17.9, 10_000),
        },
    }
}

#[test]
fn the_childs_one_line_reads_back_as_what_it_measured() {
    let run = a_run(3);
    let line = printed(&run);
    assert_eq!(line.iter().filter(|byte| **byte == b'\n').count(), 1);
    assert_eq!(read_line(&line, 3), Ok(run));
}

#[test]
fn anything_but_that_one_line_is_said_in_a_sentence() {
    let wrong = |stdout: &[u8], samples| read_line(stdout, samples).expect_err("refused");

    assert_eq!(
        wrong(b"", 3),
        "it printed 0 lines where one line of JSON is read"
    );
    let mut twice = printed(&a_run(3));
    twice.extend(printed(&a_run(3)));
    assert_eq!(
        wrong(&twice, 3),
        "it printed 2 lines where one line of JSON is read"
    );
    assert!(wrong(b"bench --plan-only\n", 3).starts_with("its line is not the plan step's JSON ("));
    assert!(wrong(&[0xff, b'\n'], 3).starts_with("its standard output is not UTF-8 ("));

    // Timings of another trie, or fewer of them than were asked for, are not this
    // run's plan step.
    let mut other = a_run(3);
    other.templates = 9_999;
    assert_eq!(
        wrong(&printed(&other), 3),
        "it planned on a trie of 9,999 templates where the plan step's has 10,000"
    );
    assert_eq!(
        wrong(&printed(&a_run(3)), 100),
        "it timed P1 3 times where 100 were asked for"
    );
}

#[test]
fn a_shape_is_its_timings_and_the_one_leaf_count_every_plan_returned() {
    let kept = shape("P2", vec![3.0, 1.0, 2.0], &[7, 7, 7]).expect("a shape");
    assert_eq!(kept.ms, [3.0, 1.0, 2.0], "in the order taken");
    assert_eq!((kept.p50_ms, kept.p99_ms, kept.min_ms), (2.0, 3.0, 1.0));
    assert_eq!(kept.leaves, 7);

    let moved = shape("P2", vec![1.0, 1.0], &[7, 8]).expect_err("two counts");
    assert_eq!(
        moved.to_string(),
        "engine::plan did not answer the way kibbr-bench reads it: P2 planned to 7 \
             leaves and then to 8 on the same trie"
    );
    let none = shape("P2", Vec::new(), &[]).expect_err("no timing");
    assert!(
        none.to_string().contains("P2 was planned no times"),
        "{none}"
    );
}

#[test]
fn the_arrival_time_is_noon_of_the_farms_day() {
    assert_eq!(arrival().to_rfc3339(), "2024-01-15T12:00:00+00:00");
}

/// A directory for one test's child, removed when the test ends.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("kibbr-speed-plan-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("a scratch directory");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(unix)]
#[test]
fn a_child_that_fails_or_prints_something_else_is_named_with_where_its_words_are() {
    let scratch = Scratch::new("child");

    // `echo` exits 0 and prints its arguments: one line, and not the plan step's.
    let echoed = child(Path::new("/bin/echo"), 3, &scratch.0).expect_err("not JSON");
    let SpeedError::Child { what, code, stderr } = &echoed else {
        panic!("{echoed:?}");
    };
    assert_eq!((*what, *code), ("kibbr-bench --plan-only", Some(0)));
    assert!(
        stderr.starts_with("its line is not the plan step's JSON ("),
        "{stderr}"
    );
    assert!(
        stderr.contains("; its standard error is kept in "),
        "{stderr}"
    );

    // A child that exits 1 having written to standard error is quoted from the file.
    let failed = child(Path::new("/bin/ls"), 3, &scratch.0).expect_err("ls refuses the flags");
    let SpeedError::Child { code, stderr, .. } = &failed else {
        panic!("{failed:?}");
    };
    assert_ne!(*code, Some(0));
    assert!(
        stderr.contains("plan-") && stderr.contains(".log and ends:\n"),
        "{stderr}"
    );
    assert!(
        stderr.lines().count() >= 2,
        "ls said why, and it is quoted: {stderr}"
    );

    // Each child's standard error is a file of its own.
    let logs = fs::read_dir(&scratch.0).expect("the scratch").count();
    assert_eq!(logs, 2);
}
