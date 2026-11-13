// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::bench::{flags, plan, queries, Args};

#[test]
fn a_result_is_written_under_the_names_a_reader_is_told_and_reads_back_the_same() {
    let mut file = ResultFile {
        schema: SCHEMA.to_string(),
        runs: vec![Run::default()],
        ..ResultFile::default()
    };
    file.runs[0].idle.queries.s2.client_ms = vec![45.8];
    file.runs[0].loaded.queries.s4.count_over_loaded = vec![140_000];
    file.runs[0].plan.shapes.p2.leaves = 4_460;
    file.runs[0].ingest.refused.s503 = 2;

    let json = serde_json::to_value(&file).expect("a result is JSON");
    let run = &json["runs"][0];
    assert_eq!(run["idle"]["S2"]["client_ms"][0], 45.8);
    assert_eq!(run["idle"]["SCAN"]["asks"], 0);
    assert_eq!(run["loaded"]["S4"]["count_over_loaded"][0], 140_000);
    assert_eq!(run["plan"]["P2"]["leaves"], 4_460);
    assert_eq!(run["ingest"]["refused"]["503"], 2);
    assert_eq!(run["scan"]["beside"]["S2"]["key_rows"], 0);
    assert!(json["expect"]["SCANKEY"]["rows_returned"].is_null());
    assert!(json["baseline"].is_null());
    // The queries of an idle server stand between its scan block and its end, in order.
    let keys: Vec<&str> = run["idle"]
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "server",
            "key_rows",
            "scan_key_rows",
            "SCAN",
            "S1",
            "S2",
            "S3",
            "S4",
            "health_at_end",
            "stats_at_end"
        ]
    );

    let text = serde_json::to_string(&file).expect("a result is JSON");
    let back: ResultFile = serde_json::from_str(&text).expect("and reads back");
    assert_eq!(back, file);
}

#[test]
fn a_missing_optional_field_reads_as_null_and_an_unknown_one_is_stepped_over() {
    let expect: Expect =
        serde_json::from_str(r#"{"count": 7, "added_later": [1, 2]}"#).expect("lenient");
    assert_eq!(
        expect,
        Expect {
            count: 7,
            ..Expect::default()
        }
    );
    // A field that is not optional is still asked for.
    assert!(serde_json::from_str::<Expect>(r#"{"rows_returned": 0}"#).is_err());
    // Beside a group that is written flat, too.
    let plan: PlanRun = serde_json::from_str(
        r#"{"templates": 10000, "nodes": 1, "tokens": 2, "lines_skipped": 0, "later": 1,
                "P1": {"ms": [1.5], "p50_ms": 1.5, "p99_ms": 1.5, "min_ms": 1.5, "leaves": 3},
                "P2": {"ms": [], "p50_ms": 0, "p99_ms": 0, "min_ms": 0, "leaves": 0},
                "P3": {"ms": [], "p50_ms": 0, "p99_ms": 0, "min_ms": 0, "leaves": 0},
                "P4": {"ms": [], "p50_ms": 0, "p99_ms": 0, "min_ms": 0, "leaves": 0}}"#,
    )
    .expect("lenient");
    assert_eq!((plan.templates, plan.shapes.p1.leaves), (10_000, 3));
    assert_eq!(plan.shapes.p1.ms, [1.5]);
}

#[test]
fn laps_saved_before_the_store_s_writes_were_kept_read_with_none() {
    let laps: Laps = serde_json::from_str(
        r#"{"lines": [640000], "seconds": [3.71], "refused_bodies": [0], "wraps": [0],
                "start_s": [0.2], "forced_closes": [1], "buffered_bytes": [561204220],
                "requests_shed": [0]}"#,
    )
    .expect("lenient");
    assert_eq!(laps.forced_closes, [Some(1)]);
    assert!(laps.write_failures.is_empty() && laps.degraded.is_empty());
    // And nothing is made of what was not said.
    assert_eq!(laps_that_could_not_write(&laps), 0);

    let said: Laps = serde_json::from_str(
        r#"{"lines": [1, 2, 3], "seconds": [1.0, 1.0, 1.0], "refused_bodies": [0, 0, 0],
                "wraps": [0, 0, 0], "start_s": [0.2, 0.2, 0.2], "forced_closes": [0, 0, 0],
                "buffered_bytes": [1, 1, 1], "requests_shed": [0, 0, 0],
                "write_failures": [0, 2, null], "degraded": [false, false, true]}"#,
    )
    .expect("as it is written");
    assert_eq!(said.write_failures, [Some(0), Some(2), None]);
    assert_eq!(laps_that_could_not_write(&said), 2);
}

#[test]
fn the_log_keeps_every_line_it_prints() {
    let mut printed = Vec::new();
    let mut log = Log::new(&mut printed);
    log.line("kibbr-bench  farm").expect("a line");
    log.line("commit       1bfe096e45ef").expect("a line");
    let kept = log.text().to_string();
    assert_eq!(kept, "kibbr-bench  farm\ncommit       1bfe096e45ef\n");
    assert_eq!(printed, kept.as_bytes());
}

// ── Three results by hand: a baseline and two columns after it ───────────────────

/// One value a run for every number a table or a summary prints.
struct Numbers {
    ingest: [f64; 3],
    /// For each of the four queries, its p50 a run and its p99 a run.
    idle: [([f64; 3], [f64; 3]); 4],
    loaded: [([f64; 3], [f64; 3]); 4],
    scan_cpu: [f64; 3],
    scan_wall: [f64; 3],
    cores: f64,
    plan: [[f64; 3]; 4],
    at_limit: [u64; 3],
}

/// Three runs a hundredth apart, the first of them the middle one.
fn around(middle: f64) -> [f64; 3] {
    [middle, middle * 0.99, middle * 1.01]
}

fn baseline_numbers() -> Numbers {
    Numbers {
        ingest: [91_166.0, 89_802.0, 93_410.0],
        idle: [
            ([1.52, 1.55, 1.49], [2.10, 2.31, 2.04]),
            ([45.8, 46.1, 45.6], [49.2, 52.0, 48.8]),
            ([131.0, 133.0, 130.0], [139.0, 146.0, 138.0]),
            ([1_842.0, 1_861.0, 1_838.0], [1_903.0, 1_950.0, 1_899.0]),
        ],
        loaded: [
            ([4.10, 4.30, 4.00], [9.80, 12.1, 9.20]),
            ([96.0, 98.2, 95.1], [131.0, 142.0, 129.0]),
            ([180.0, 184.0, 178.0], [236.0, 251.0, 231.0]),
            ([2_650.0, 2_702.0, 2_633.0], [3_120.0, 3_300.0, 3_090.0]),
        ],
        scan_cpu: [1_690_268.0, 1_671_000.0, 1_702_300.0],
        scan_wall: [1_686_061.0, 1_668_200.0, 1_698_900.0],
        cores: 1.00,
        plan: [
            [4.58, 4.61, 4.55],
            [16.2, 16.4, 16.1],
            [14.4, 14.5, 14.4],
            [17.9, 18.0, 17.8],
        ],
        at_limit: [97, 98, 96],
    }
}

/// A column whose scan is 15 times the baseline's on one core: three of its ratios
/// are inside the runs' spread, and the rest are not.
fn columns_numbers() -> Numbers {
    Numbers {
        ingest: [92_040.0, 90_310.0, 93_120.0],
        idle: [
            (around(0.61), around(0.92)),
            (around(3.10), around(3.60)),
            (around(8.40), around(9.90)),
            (around(410.0), around(436.0)),
        ],
        loaded: [
            (around(3.00), [6.10, 5.90, 9.60]),
            (around(10.0), around(14.2)),
            (around(15.0), around(21.0)),
            (around(600.0), around(690.0)),
        ],
        scan_cpu: around(25_400_000.0),
        scan_wall: around(25_300_000.0),
        cores: 1.00,
        plan: [around(4.57), [16.1, 16.0, 16.3], around(14.4), around(17.9)],
        at_limit: [0, 0, 0],
    }
}

/// The same scan spread over four threads: the rate per CPU-second does not move, and
/// the wall clock's does.
fn parallel_numbers() -> Numbers {
    Numbers {
        ingest: [90_870.0, 88_940.0, 92_300.0],
        idle: [
            (around(0.58), around(0.90)),
            (around(1.05), around(1.40)),
            (around(2.90), around(3.80)),
            (around(121.0), around(140.0)),
        ],
        loaded: [
            (around(3.00), [5.80, 5.60, 9.30]),
            (around(6.00), around(9.10)),
            (around(8.00), around(12.4)),
            (around(250.0), around(310.0)),
        ],
        scan_cpu: around(23_100_000.0),
        scan_wall: around(87_700_000.0),
        cores: 3.80,
        plan: [around(4.57), [16.2, 16.1, 16.3], around(14.4), around(17.9)],
        at_limit: [0, 0, 0],
    }
}

fn the_args() -> Args {
    Args {
        kibbr: None,
        lines: 1_000_000,
        out: None,
        baseline: None,
        label: None,
        dir: PathBuf::from("target/scratch/bench"),
        runs: 3,
        samples: 100,
        body_lines: 2_000,
        threads: None,
        unchecked: false,
    }
}

fn run_of(run: usize, numbers: &Numbers) -> Run {
    let at = run - 1;
    let mut made = Run {
        run,
        ..Run::default()
    };
    made.ingest.lines_per_s = numbers.ingest[at];
    // As every server of a real result said when its phase ended.
    let ok = Health {
        status: "ok".to_string(),
        detail: None,
    };
    (made.ingest.health_at_end, made.idle.health_at_end) = (ok.clone(), ok);
    for (block, (p50, p99)) in made.idle.queries.each_mut().into_iter().zip(numbers.idle) {
        (block.p50_ms, block.p99_ms) = (p50[at], p99[at]);
    }
    let over = [
        [240, 238, 240],
        [905, 911, 905],
        [0, 0, 0],
        [140_000, 142_000, 140_000],
    ];
    for ((block, (p50, p99)), over) in made
        .loaded
        .queries
        .each_mut()
        .into_iter()
        .zip(numbers.loaded)
        .zip(over)
    {
        (block.p50_ms, block.p99_ms) = (p50[at], p99[at]);
        block.count_over_loaded_median = over[at];
        block.client_ms = vec![p50[at]; 100];
    }
    made.loaded.laps = 100;
    made.loaded.laps_at_buffer_limit = Some(numbers.at_limit[at]);
    made.scan.rows_per_cpu_s = Some(numbers.scan_cpu[at]);
    made.scan.rows_per_wall_s = Some(numbers.scan_wall[at]);
    made.scan.cores_busy = Some(numbers.cores);
    made.scan.cpu_tail_s = Some(0.0);
    let [p1, p2, p3, p4] = numbers.plan;
    made.plan.shapes.p1.p50_ms = p1[at];
    made.plan.shapes.p2.p50_ms = p2[at];
    made.plan.shapes.p3.p50_ms = p3[at];
    made.plan.shapes.p4.p50_ms = p4[at];
    made.checks.spin_ms_before = Some([101.4, 101.9, 101.6][at]);
    made.checks.flush_ms_before = Some([53.6, 54.9, 53.2][at]);
    made
}

/// A whole result around `numbers`, its `per_run` and `medians` worked out from its
/// runs by the functions a real one goes through.
fn result(label: &str, sha: &str, numbers: &Numbers) -> ResultFile {
    let runs: Vec<Run> = (1..=3).map(|run| run_of(run, numbers)).collect();
    let per_run = per_run(&runs);
    let medians = medians(&per_run);
    ResultFile {
        schema: SCHEMA.to_string(),
        label: label.to_string(),
        commit: Commit {
            sha: Some(sha.to_string()),
            src_dirty: Some(false),
            committed_at: Some("2026-10-07T02:40:29+05:30".to_string()),
            how: "git rev-parse HEAD in the working directory when the bench ran".to_string(),
        },
        binary: Binary {
            path: "/Users/pratyaydeep/repo/kibbr/target/release/kibbr".to_string(),
            bytes: 6_735_968,
            crc32c: "9a3f01c2".to_string(),
            modified_at: Some("2026-10-07T02:44:11+05:30".to_string()),
            build: "release".to_string(),
            stale: Some(false),
        },
        bench: BenchSource {
            source_crc32c: "5d11e0aa".to_string(),
            source_files: 8,
        },
        flags: flags(&the_args()),
        queries: queries("20834abd-1e3d-462e-b89b-fb1d4643e6bb"),
        plan_queries: plan::queries(),
        plan_trie: PlanTrie {
            source: plan::TRIE_SOURCE.to_string(),
            templates: 10_000,
            nodes: 57_642,
            tokens: 98_495,
        },
        runs,
        per_run,
        medians,
        ..ResultFile::default()
    }
}

const BASELINE_SHA: &str = "1bfe096e45ef729cb24d60eb80f6d2267c76d082";

fn baseline() -> Saved {
    Saved {
        path: PathBuf::from("target/scratch/bench/baseline.json"),
        file: result("baseline", BASELINE_SHA, &baseline_numbers()),
    }
}

fn columns() -> Saved {
    Saved {
        path: PathBuf::from("target/scratch/bench/after-1.json"),
        file: result(
            "after 1 (columns)",
            "77aa3e12ab77000000000000000000000000beef",
            &columns_numbers(),
        ),
    }
}

fn parallel() -> Saved {
    Saved {
        path: PathBuf::from("target/scratch/bench/after-2.json"),
        file: result(
            "after 2 (parallel)",
            "9d0c3e12ab77000000000000000000000000beef",
            &parallel_numbers(),
        ),
    }
}

/// The lines of a table that is expected to carry no `FAIL`.
fn printed(saved: &[Saved]) -> String {
    let table = table(saved).expect("the same work");
    assert_eq!(table.fails, 0, "{:#?}", table.lines);
    table.lines.join("\n")
}

// ── How a number is printed ──────────────────────────────────────────────────────

#[test]
fn milliseconds_keep_three_figures_and_never_round_into_a_fourth() {
    assert_eq!(ms(0.61), "0.61");
    assert_eq!(ms(9.80), "9.80");
    assert_eq!(ms(45.8), "45.8");
    assert_eq!(ms(131.0), "131.0");
    assert_eq!(ms(1_842.0), "1,842");
    assert_eq!(ms(0.0), "0.00");
    // Under a tenth, two decimals would leave one figure, or none: three plans of
    // 26, 30 and 34 microseconds are three numbers, and one of 4 is not `0.00`.
    assert_eq!(ms(0.026), "0.0260");
    assert_eq!(ms(0.03), "0.0300");
    assert_eq!(ms(0.034), "0.0340");
    assert_eq!(ms(0.004), "0.0040");
    assert_eq!(ms(0.0994), "0.0994");
    // Where four decimals would print `0.1000`.
    assert_eq!(ms(0.0996), "0.10");
    assert_eq!(ms(0.1), "0.10");
    // Where two decimals would print `10.00`, and one `1000.0`.
    assert_eq!(ms(9.994), "9.99");
    assert_eq!(ms(9.996), "10.0");
    assert_eq!(ms(999.94), "999.9");
    assert_eq!(ms(999.96), "1,000");
    assert_eq!(ms(1_234_567.4), "1,234,567");
}

#[test]
fn a_ratio_keeps_its_second_decimal_only_near_one() {
    assert_eq!(times(1.0), "1.00x");
    assert_eq!(times(1.0096), "1.01x");
    assert_eq!(times(0.87), "0.87x");
    assert_eq!(times(1.094), "1.09x");
    assert_eq!(times(1.096), "1.1x");
    assert_eq!(times(1.607), "1.6x");
    assert_eq!(times(15.03), "15.0x");
    assert_eq!(times_or_na(None), "n/a");
    assert_eq!(after(None), "");
    assert_eq!(after(Some(Some(2.49))), " (2.5x)");
    assert_eq!(after(Some(None)), " (n/a)");
}

#[test]
fn a_speed_up_is_above_one_when_faster_for_a_rate_and_for_a_time() {
    // A rate that doubled and a time that halved are both twice as fast.
    assert_eq!(speedup_of_rate(200.0, 100.0), Some(2.0));
    assert_eq!(speedup_of_time(50.0, 100.0), Some(2.0));
    // And both slower read under 1.
    assert_eq!(speedup_of_rate(50.0, 100.0), Some(0.5));
    assert_eq!(speedup_of_time(200.0, 100.0), Some(0.5));
    // Nothing is divided by 0.
    assert_eq!(speedup_of_rate(100.0, 0.0), None);
    assert_eq!(speedup_of_time(0.0, 100.0), None);
    assert_eq!(speedup_of_rate(0.0, 100.0), Some(0.0));
}

#[test]
fn two_sets_of_runs_overlap_when_their_ranges_share_a_value() {
    assert!(overlap(&[1.0, 2.0, 3.0], &[3.0, 4.0, 5.0]), "ends count");
    assert!(!overlap(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]));
    // Either way round, in any order, and one inside the other.
    assert!(overlap(&[5.0, 3.0, 4.0], &[2.0, 3.0, 1.0]));
    assert!(!overlap(&[6.0, 4.0, 5.0], &[3.0, 1.0, 2.0]));
    assert!(overlap(&[1.0, 9.0], &[4.0, 5.0]));
    // No runs overlap nothing.
    assert!(!overlap(&[], &[1.0]));
    assert!(!overlap(&[1.0], &[]));

    // A rate and a time take the same rule; only the unit in the line differs.
    let rate = overlap_line(
        "ingest lines/s",
        &[90_310.0, 93_120.0],
        &[89_802.0, 93_410.0],
        false,
    );
    assert_eq!(
        rate.as_deref(),
        Some(
            "ingest lines/s: the runs overlap (90,310 to 93,120 against the baseline's \
                 89,802 to 93,410) - not a difference"
        )
    );
    let time = overlap_line("plan step", &[16.0, 16.3], &[16.4, 16.1], true);
    assert_eq!(
        time.as_deref(),
        Some(
            "plan step: the runs overlap (16.0 to 16.3 ms against the baseline's 16.1 to \
                 16.4) - not a difference"
        )
    );
    assert_eq!(overlap_line("S1 p50", &[0.6, 0.7], &[1.4, 1.5], true), None);
}

// ── Over the runs ────────────────────────────────────────────────────────────────

#[test]
fn each_table_number_is_the_middle_of_the_runs_own_numbers() {
    let file = baseline().file;
    let (each, middle) = (&file.per_run, &file.medians);
    assert_eq!(each.ingest_lines_per_s, [91_166.0, 89_802.0, 93_410.0]);
    assert_eq!(middle.ingest_lines_per_s, 91_166.0);
    assert_eq!(each.idle.s2.p99_ms, [49.2, 52.0, 48.8]);
    assert_eq!(
        middle.idle.s2,
        Pair {
            p50_ms: 45.8,
            p99_ms: 49.2
        }
    );
    assert_eq!(each.loaded.s4.p50_ms, [2_650.0, 2_702.0, 2_633.0]);
    assert_eq!(
        middle.loaded.s4,
        Pair {
            p50_ms: 2_650.0,
            p99_ms: 3_120.0
        }
    );
    assert_eq!(middle.scan_rows_per_cpu_s, Some(1_690_268.0));
    assert_eq!(middle.scan_rows_per_wall_s, Some(1_686_061.0));
    assert_eq!(middle.scan_cores_busy, Some(1.0));
    assert_eq!(each.plan_ms.p2, [16.2, 16.4, 16.1]);
    assert_eq!(
        middle.plan_ms,
        Shapes {
            p1: 4.58,
            p2: 16.2,
            p3: 14.4,
            p4: 17.9
        }
    );
    // The plan step is the second shape's, and the gate is a millisecond.
    assert_eq!(
        (middle.plan_step_ms, middle.plan_step_over_1_ms),
        (16.2, true)
    );
    assert_eq!(
        each.loaded_count_over_loaded_median.s4,
        [140_000, 142_000, 140_000]
    );
    assert_eq!(middle.loaded_count_over_loaded_median.s4, 140_000);
    // Laps past the buffer limit are counted over all the laps, not taken a run.
    assert_eq!(each.laps_at_buffer_limit, [Some(97), Some(98), Some(96)]);
    assert_eq!(middle.laps_at_buffer_limit, Some(291));
    assert_eq!((middle.spin_ms, middle.flush_ms), (Some(101.6), Some(53.6)));

    // A median over one run is that run's.
    let one = per_run(&file.runs[1..2]);
    assert_eq!(medians(&one).ingest_lines_per_s, 89_802.0);
}

#[test]
fn a_number_one_run_could_not_give_has_no_median() {
    let mut runs = baseline().file.runs;
    runs[1].scan.rows_per_cpu_s = None;
    runs[2].loaded.laps_at_buffer_limit = None;
    runs[0].checks.spin_ms_before = None;
    let middle = medians(&per_run(&runs));
    assert_eq!(
        middle.scan_rows_per_cpu_s, None,
        "not the middle of the other two"
    );
    assert_eq!(middle.laps_at_buffer_limit, None);
    assert_eq!(middle.spin_ms, None);
    assert_eq!(middle.scan_rows_per_wall_s, Some(1_686_061.0));

    // A step of exactly a millisecond is not over it; with no run there is no number.
    let mut at_the_gate = baseline().file.runs;
    for run in &mut at_the_gate {
        run.plan.shapes.p2.p50_ms = 1.0;
    }
    assert!(!medians(&per_run(&at_the_gate)).plan_step_over_1_ms);
    let none = medians(&per_run(&[]));
    assert_eq!(
        (none.ingest_lines_per_s, none.laps_at_buffer_limit),
        (0.0, None)
    );
}

// ── The digest of the bench's own code ───────────────────────────────────────────

#[test]
fn the_source_digest_does_not_move_with_white_space_and_moves_with_an_edit() {
    let source = [("a.rs", "fn a() { 1 }\n"), ("b.rs", "fn b() {}\n")];
    let digest = digest_of(&source);
    // What a formatter does: lines broken, indented, a tab, a carriage return.
    let formatted = [
        ("a.rs", "fn a() {\r\n\t1\n}\n\n"),
        ("b.rs", "fn  b ( )  { }"),
    ];
    assert_eq!(digest_of(&formatted), digest);
    // One character of code is another digest, and so is another order of the files.
    let edited = [("a.rs", "fn a() { 2 }\n"), ("b.rs", "fn b() {}\n")];
    assert_ne!(digest_of(&edited), digest);
    let swapped = [("b.rs", "fn b() {}\n"), ("a.rs", "fn a() { 1 }\n")];
    assert_ne!(digest_of(&swapped), digest);

    // The real one: eight files, each of them the bench's, in the order recorded.
    let names: Vec<&str> = SOURCES.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        names,
        [
            "bench.rs",
            "corpus.rs",
            "child.rs",
            "wire.rs",
            "host.rs",
            "phases.rs",
            "plan.rs",
            "report.rs"
        ]
    );
    let bench = source_digest();
    assert_eq!(bench.source_files, 8);
    assert_eq!(bench.source_crc32c, crc_hex(digest_of(&SOURCES)));
    assert_eq!(bench.source_crc32c.len(), 8);
    let spaced: Vec<(&str, String)> = SOURCES
        .iter()
        .map(|(name, text)| (*name, text.replace('\n', "\n\n  ")))
        .collect();
    let spaced: Vec<(&str, &str)> = spaced
        .iter()
        .map(|(name, text)| (*name, text.as_str()))
        .collect();
    assert_eq!(crc_hex(digest_of(&spaced)), bench.source_crc32c);
}

// ── Writing and reading ──────────────────────────────────────────────────────────

/// A directory for one test's files, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("kibbr-speed-report-{}-{name}", std::process::id()));
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

#[test]
fn a_result_is_saved_beside_its_log_and_reads_back_the_same() {
    let scratch = Scratch::new("write");
    // The directory is made when it is not there.
    let out = scratch.0.join("results").join("baseline.json");
    let file = baseline().file;
    let log = format!("run 1  end\n{}\n", saved_line(&out));
    write(&out, &file, &log).expect("saved");

    let log_at = scratch.0.join("results").join("baseline.log");
    assert_eq!(log_path(&out), log_at);
    assert_eq!(fs::read_to_string(&log_at).expect("the log"), log);
    assert!(log.ends_with(&format!(
        "saved {} and {}\n",
        out.display(),
        log_at.display()
    )));
    let names: Vec<String> = fs::read_dir(scratch.0.join("results"))
        .expect("the directory")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    assert_eq!(names.len(), 2, "no .tmp is left: {names:?}");

    let back = read(&out).expect("a saved result");
    assert_eq!(back.path, out);
    assert_eq!(back.file, file);

    // One field a line, one run a line: the numbers of a table can be found by eye.
    let text = fs::read_to_string(&out).expect("the result");
    assert!(text.starts_with("{\n  \"schema\": \"kibbr-bench/1\",\n  \"label\": \"baseline\",\n"));
    assert!(
        text.ends_with("  \"baseline\": null\n}\n"),
        "{}",
        &text[text.len() - 40..]
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("    {\"run\": "))
            .count(),
        0
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("    {\"run\":"))
            .count(),
        3
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("  \"medians\": {"))
            .count(),
        1
    );
}

#[test]
fn a_place_a_result_cannot_be_written_to_is_found_before_a_run_is_spent() {
    let scratch = Scratch::new("writable");
    // The directory is made, and nothing is left in it.
    let out = scratch.0.join("results").join("deep").join("baseline.json");
    writable(&out).expect("a place that can be written to");
    let dir = scratch.0.join("results").join("deep");
    assert!(dir.is_dir());
    assert_eq!(fs::read_dir(&dir).expect("the directory").count(), 0);
    // A name with no directory before it is in the directory the bench was run in.
    assert_eq!(
        beside(Path::new("a.json"), ".probe"),
        Path::new("a.json.probe")
    );

    // A `.tmp` beside the place may be the samples of a result that did not read
    // back: the proof is not written through it.
    fs::write(beside(&out, ".tmp"), "an earlier result's samples").expect("a file");
    writable(&out).expect("still writable");
    assert_eq!(
        fs::read_to_string(beside(&out, ".tmp")).expect("kept"),
        "an earlier result's samples"
    );

    // A file of the proof's own name is someone's: named, and left as it is.
    let probe = beside(&out, ".probe");
    fs::write(&probe, "someone's").expect("a file");
    let refused = writable(&out).expect_err("it is there");
    assert!(matches!(refused, SpeedError::Host(_)), "{refused:?}");
    assert!(
        refused.to_string().starts_with(&format!(
            "{} is already there, and the bench makes",
            probe.display()
        )),
        "{refused}"
    );
    assert_eq!(fs::read_to_string(&probe).expect("kept"), "someone's");

    // A directory that cannot be made: its parent is a file.
    let under_a_file = probe.join("results").join("baseline.json");
    let refused = writable(&under_a_file).expect_err("no directory can be made there");
    assert!(
        matches!(&refused, SpeedError::Io { path, .. } if *path == probe.join("results")),
        "{refused:?}"
    );
}

#[cfg(unix)]
#[test]
fn a_directory_no_file_can_be_made_in_is_refused_by_the_result_s_own_name() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("shut");
    let dir = scratch.0.join("results");
    fs::create_dir(&dir).expect("a directory");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("shut it");
    let out = dir.join("baseline.json");
    let outcome = writable(&out);
    // Opened again before anything is asserted, so that the scratch can be removed.
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("open it");
    // Whoever may write anywhere is not refused, and has nothing to be told.
    if let Err(refused) = outcome {
        assert!(
            matches!(&refused, SpeedError::Io { path, .. } if *path == out),
            "{refused:?}"
        );
        assert!(refused
            .to_string()
            .starts_with(&format!("{}: ", out.display())));
    }
    assert_eq!(fs::read_dir(&dir).expect("the directory").count(), 0);
}

#[test]
fn a_saved_result_is_never_written_over() {
    let scratch = Scratch::new("refuse");
    let out = scratch.0.join("baseline.json");
    let file = baseline().file;
    write(&out, &file, "the first\n").expect("saved");

    let mut second = file.clone();
    second.label = "another".to_string();
    let refused = write(&out, &second, "the second\n").expect_err("it is there");
    assert_eq!(
        refused.to_string(),
        format!(
            "{} exists; a saved result is not written over",
            out.display()
        )
    );
    assert_eq!(read(&out).expect("unchanged").file.label, "baseline");
    assert_eq!(
        fs::read_to_string(log_path(&out)).expect("the log"),
        "the first\n"
    );

    // A log without its result is someone's file too.
    let lone = scratch.0.join("after-1.json");
    fs::write(log_path(&lone), "kept").expect("a file");
    let refused = write(&lone, &file, "the log\n").expect_err("its log is there");
    assert!(matches!(&refused, SpeedError::OutExists(path) if *path == log_path(&lone)));
    assert!(!lone.exists());
    assert_eq!(fs::read_to_string(log_path(&lone)).expect("kept"), "kept");

    // A `.tmp` an interrupted bench left is not: it does not stop a result.
    let third = scratch.0.join("after-2.json");
    fs::write(beside(&third, ".tmp"), "half a result").expect("a file");
    write(&third, &file, "the log\n").expect("saved over the .tmp");
    assert_eq!(read(&third).expect("whole").file, file);
    assert!(!beside(&third, ".tmp").exists());
}

#[test]
fn a_result_and_its_log_are_never_one_file() {
    assert_eq!(
        log_path(Path::new("a/baseline.json")),
        Path::new("a/baseline.log")
    );
    assert_eq!(log_path(Path::new("baseline")), Path::new("baseline.log"));
    assert_eq!(
        log_path(Path::new("a/result.log")),
        Path::new("a/result.log.log")
    );
    assert_eq!(
        beside(Path::new("a/baseline.json"), ".tmp"),
        Path::new("a/baseline.json.tmp")
    );
}

#[test]
fn a_result_that_would_not_read_back_keeps_its_tmp_name() {
    let scratch = Scratch::new("nan");
    let out = scratch.0.join("baseline.json");
    let mut file = baseline().file;
    // A rate over a window of no length: not a number, and JSON has no word for it.
    file.runs[0].ingest.lines_per_s = f64::NAN;
    let refused = write(&out, &file, "the log\n").expect_err("not a result");
    let SpeedError::Baseline { path, detail } = &refused else {
        panic!("{refused:?}");
    };
    assert_eq!(*path, beside(&out, ".tmp"));
    assert!(
        detail.contains("does not read back as a result"),
        "{detail}"
    );
    assert!(!out.exists() && !log_path(&out).exists());
    assert!(path.exists(), "every sample is still on the disk");
}

#[test]
fn a_file_that_is_not_a_result_of_this_schema_is_refused_by_path() {
    let scratch = Scratch::new("read");
    let refused = |name: &str, text: Option<&str>| {
        let path = scratch.0.join(name);
        if let Some(text) = text {
            fs::write(&path, text).expect("a file");
        }
        let error = read(&path).expect_err("refused");
        let said = error.to_string();
        let prefix = format!("{} cannot be the baseline of this run: ", path.display());
        assert!(said.starts_with(&prefix), "{said}");
        said[prefix.len()..].to_string()
    };
    assert!(refused("absent.json", None).starts_with("it cannot be read ("));
    assert!(refused("text.json", Some("not json")).starts_with("it is not JSON ("));
    assert_eq!(
        refused("later.json", Some(r#"{"schema": "kibbr-bench/2"}"#)),
        "its schema is `kibbr-bench/2`, and this build reads `kibbr-bench/1`"
    );
    assert_eq!(
        refused("other.json", Some(r#"{"lines": 3}"#)),
        "it has no `schema`, so it is not a result kibbr-bench saved"
    );
    assert!(refused("half.json", Some(r#"{"schema": "kibbr-bench/1"}"#))
        .starts_with("it is not a whole result of `kibbr-bench/1` ("));
}

// ── The same work ────────────────────────────────────────────────────────────────

#[test]
fn a_baseline_of_other_work_is_refused_with_each_key_and_both_values() {
    let baseline = baseline();
    let file = &baseline.file;
    let before = |flags: &Flags, machine: &Machine, server: &ServerFacts| {
        comparable_before(&baseline, flags, machine, server, &file.plan_queries)
    };
    before(&file.flags, &file.machine, &file.server).expect("the same work");

    let mut flags = file.flags.clone();
    flags.samples = 2;
    flags.lines = 2_000;
    let mut machine = file.machine.clone();
    machine.cpu = Some("Apple M3".to_string());
    machine.memory_bytes = Some(17_179_869_184);
    let mut server = file.server.clone();
    server.argv = vec![
        "serve".to_string(),
        "--scan-threads".to_string(),
        "4".to_string(),
    ];
    let refused = before(&flags, &machine, &server).expect_err("other work");
    assert_eq!(
        refused.to_string(),
        "target/scratch/bench/baseline.json cannot be the baseline of this run: flags.lines: \
             1000000 in the baseline, 2000 in this run; flags.samples: 100 in the baseline, \
             2 in this run; server.argv: \"\" in the baseline, \"serve --scan-threads 4\" \
             in this run; machine.cpu: null in the baseline, \"Apple M3\" in this run; \
             machine.memory_bytes: null in the baseline, 17179869184 in this run"
    );

    // Every key of the list is looked at, and a plan shape's text is one of them.
    let mut every_flag = file.flags.clone();
    every_flag.profile = "hostile".to_string();
    every_flag.runs = 5;
    every_flag.body_lines = 100;
    every_flag.senders = 8;
    every_flag.load_at = [0.1, 0.2, 0.3, 0.4];
    every_flag.warmup = 1;
    every_flag.plan_warmup = 1;
    every_flag.scan_asks = 1;
    every_flag.received_at = "2025-01-01T00:00:00Z".to_string();
    let mut shapes = file.plan_queries.clone();
    shapes.p2.text = r#"COUNT LOG {"level":"warn"}"#.to_string();
    let mut machine = file.machine.clone();
    machine.cores_logical = 10;
    let refused = comparable_before(&baseline, &every_flag, &machine, &file.server, &shapes)
        .expect_err("other work")
        .to_string();
    for key in [
            "flags.profile: \"farm\" in the baseline, \"hostile\" in this run",
            "flags.runs: 3 in the baseline, 5 in this run",
            "flags.body_lines: 2000 in the baseline, 100 in this run",
            "flags.senders: 4 in the baseline, 8 in this run",
            "flags.load_at: [0.02,0.03,0.07,0.14] in the baseline, [0.1,0.2,0.3,0.4] in this run",
            "flags.warmup: 3 in the baseline, 1 in this run",
            "flags.plan_warmup: 5 in the baseline, 1 in this run",
            "flags.scan_asks: 25 in the baseline, 1 in this run",
            "flags.received_at: \"2024-01-15T12:00:00Z\" in the baseline, \"2025-01-01T00:00:00Z\" in this run",
            "plan_queries.P2.text: \"COUNT LOG {\\\"level\\\":\\\"error\\\"}\" in the baseline, \"COUNT LOG {\\\"level\\\":\\\"warn\\\"}\" in this run",
            "machine.cores_logical: 0 in the baseline, 10 in this run",
        ] {
            assert!(refused.contains(key), "{key}\n{refused}");
        }
    // What does not make other work is not a key: a label, a pause, the checks.
    let mut same = file.flags.clone();
    same.checked = false;
    same.settle_s = 0;
    before(&same, &file.machine, &file.server).expect("still the same work");
}

fn a_file(path: &str, bytes: u64, crc32c: &str) -> FileDigest {
    FileDigest {
        path: path.to_string(),
        bytes,
        crc32c: crc32c.to_string(),
    }
}

#[test]
fn other_bytes_or_another_text_are_refused_before_an_answer_is_looked_at() {
    let mut baseline = baseline();
    baseline.file.data.files = vec![
        a_file("catalog.log", 211_004, "17c0a2e1"),
        a_file("data/2024-01-15/seg_0_000001.col", 36_212_480, "b40d91f3"),
        a_file("manifest.log", 251, "0e5a7c22"),
    ];
    baseline.file.data.live_stream.crc32c = "41d2f09e".to_string();
    let file = &baseline.file;
    comparable_data(&baseline, &file.data, &file.queries, &file.expect).expect("the same");

    let mut data = file.data.clone();
    data.files[1] = a_file("data/2024-01-15/seg_0_000001.col", 36_212_481, "00000000");
    data.files[2] = a_file("manifest.log.bak", 251, "0e5a7c22");
    data.live_stream.crc32c = "ffffffff".to_string();
    let mut queries = file.queries.clone();
    queries.s3.text = r#"MATCH LOG {"job.id":"another"}"#.to_string();
    // The answers differ too, and are not what is said: the bytes are not the same.
    let mut expect = file.expect.clone();
    expect.s2.count = 1;
    let refused = comparable_data(&baseline, &data, &queries, &expect).expect_err("other bytes");
    assert_eq!(
        refused.to_string(),
        "target/scratch/bench/baseline.json cannot be the baseline of this run: data.files: \
             data/2024-01-15/seg_0_000001.col is 36,212,480 bytes, crc32c b40d91f3 in the \
             baseline and 36,212,481 bytes, crc32c 00000000 in this run, manifest.log is in \
             the baseline and not in this run, manifest.log.bak is in this run and not in \
             the baseline; data.live_stream.crc32c: \"41d2f09e\" in the baseline, \
             \"ffffffff\" in this run; queries.S3.text: \"MATCH LOG \
             {\\\"job.id\\\":\\\"20834abd-1e3d-462e-b89b-fb1d4643e6bb\\\"}\" in the \
             baseline, \"MATCH LOG {\\\"job.id\\\":\\\"another\\\"}\" in this run"
    );

    // Many differing files are counted past the third.
    let many: Vec<FileDigest> = (0..5)
        .map(|n| a_file(&format!("seg_{n}"), n, "00000000"))
        .collect();
    assert_eq!(
        file_differences(&many, &[], "this run").expect("they differ"),
        "data.files: seg_0 is in the baseline and not in this run, seg_1 is in the \
             baseline and not in this run, seg_2 is in the baseline and not in this run, \
             and 2 more"
    );
    let turned: Vec<FileDigest> = many.iter().rev().cloned().collect();
    assert_eq!(
        file_differences(&many, &turned, "this run").expect("they differ"),
        "data.files: the same files, listed in another order in this run"
    );
}

#[test]
fn another_answer_over_the_same_bytes_fails_with_both() {
    let mut baseline = baseline();
    let answer = |count, rows, templates, truncated, limit: &str| Expect {
        count,
        rows_returned: Some(rows),
        templates_matched: Some(templates),
        truncated: Some(truncated),
        limit_source: Some(limit.to_string()),
    };
    baseline.file.expect.s2 = answer(28_907, 0, 61, false, "unlimited");
    baseline.file.expect.s4 = answer(1_000_000, 500, 145, true, "caller");
    let file = &baseline.file;

    let mut expect = file.expect.clone();
    expect.s2.count = 28_115;
    let refused =
        comparable_data(&baseline, &file.data, &file.queries, &expect).expect_err("a wrong answer");
    assert_eq!(
        refused.to_string(),
        "target/scratch/bench/baseline.json cannot be the baseline of this run: FAIL S2 counts \
             28,115 where the baseline counted 28,907: the same query over the same bytes \
             has another answer"
    );

    // The same count through other templates, or fewer rows, is another answer too.
    let mut expect = file.expect.clone();
    expect.s4.rows_returned = Some(499);
    expect.s4.templates_matched = None;
    let refused = comparable_data(&baseline, &file.data, &file.queries, &expect)
        .expect_err("a wrong answer")
        .to_string();
    assert!(
        refused.ends_with(
            "FAIL S4 answers rows_returned 499 where the baseline's was 500, \
                 templates_matched null where the baseline's was 145: the same query over \
                 the same bytes has another answer"
        ),
        "{refused}"
    );
}

// ── The table ────────────────────────────────────────────────────────────────────

#[test]
fn the_table_of_one_result_is_the_eight_rows_and_what_the_scan_row_was_divided_by() {
    // A result with nothing to say of itself: no lap of it reached the buffer limit.
    let mut quiet = baseline();
    for run in &mut quiet.file.runs {
        run.loaded.laps_at_buffer_limit = Some(0);
    }
    quiet.file.per_run = per_run(&quiet.file.runs);
    quiet.file.medians = medians(&quiet.file.per_run);
    let rows = r##"kibbr-bench   commit 1bfe096e45ef   farm 1,000,000 lines, median of 3 runs of 100 samples
                                               baseline
ingest lines/s                                 91,166
S1 promoted value   p50 / p99                  1.52 / 2.10 ms
S2 unpromoted key   p50 / p99                  45.8 / 49.2 ms
S3 one id           p50 / p99                  131.0 / 139.0 ms
S4 newest 500       p50 / p99                  1,842 / 1,903 ms
scan rows/s/core                               1,690,268
plan step at 10,000 templates:                 16.2 ms   over 1 ms: yes
same four queries under 4-sender ingest, p99:  S1 9.80  S2 131.0  S3 236.0  S4 3,120 ms
scan rows/s/core is rows holding `status` x 25 asks of COUNT LOG {"status":"200"} / CPU-seconds of the server process. By the wall clock: 1,686,061 rows/s on 1.00 cores."##;
    assert_eq!(printed(&[quiet]), rows);
    // The baseline's own laps did, and with no column to set them against it is said
    // as it is.
    assert_eq!(
        printed(&[baseline()]),
        format!("{rows}\nS4 under ingest: the buffer limit was reached in 291 of 300 laps")
    );
}

/// A baseline and one column after it, as they print.
const TWO: &str = r##"kibbr-bench   commit 77aa3e12ab77   baseline on 1bfe096e45ef   farm 1,000,000 lines, median of 3 runs of 100 samples
                                               baseline              after 1 (columns)
ingest lines/s                                 91,166                92,040 (1.01x)
S1 promoted value   p50 / p99                  1.52 / 2.10 ms        0.61 / 0.92 ms (2.5x / 2.3x)
S2 unpromoted key   p50 / p99                  45.8 / 49.2 ms        3.10 / 3.60 ms (14.8x / 13.7x)
S3 one id           p50 / p99                  131.0 / 139.0 ms      8.40 / 9.90 ms (15.6x / 14.0x)
S4 newest 500       p50 / p99                  1,842 / 1,903 ms      410.0 / 436.0 ms (4.5x / 4.4x)
scan rows/s/core                               1,690,268             25,400,000 (15.0x)
plan step at 10,000 templates:                 16.2 ms   over 1 ms: yes      16.1 ms (1.01x)   over 1 ms: yes
same four queries under 4-sender ingest, p99:  S1 9.80  S2 131.0  S3 236.0  S4 3,120 ms
                                               S1 6.10 (1.6x)  S2 14.2 (9.2x)  S3 21.0 (11.2x)  S4 690.0 (4.5x) ms    after 1 (columns)
(Nx) is the speed-up on the baseline: this / baseline for the two rates, baseline / this for every time; above 1 is faster.
scan rows/s/core is rows holding `status` x 25 asks of COUNT LOG {"status":"200"} / CPU-seconds of the server process. By the wall clock: baseline 1,686,061 rows/s on 1.00 cores; after 1 (columns) 25,300,000 on 1.00 (15.0x).
ingest lines/s: the runs overlap (90,310 to 93,120 against the baseline's 89,802 to 93,410) - not a difference
plan step: the runs overlap (16.0 to 16.3 ms against the baseline's 16.1 to 16.4) - not a difference
S1 under ingest p99: the runs overlap (5.90 to 9.60 ms against the baseline's 9.20 to 12.1) - not a difference
S4 under ingest: the buffer limit was reached in 291 of 300 baseline laps and 0 of 300 here"##;

#[test]
fn the_table_of_two_results_puts_each_speed_up_on_its_number() {
    assert_eq!(printed(&[baseline(), columns()]), TWO);
}

#[test]
fn two_saved_files_print_the_table_their_results_did() {
    // What `kibbr-bench --table a.json b.json` does: the numbers go through JSON and
    // come back, and the table is the same to the character.
    let scratch = Scratch::new("table");
    let paths = [
        scratch.0.join("baseline.json"),
        scratch.0.join("after-1.json"),
    ];
    write(&paths[0], &baseline().file, "").expect("saved");
    write(&paths[1], &columns().file, "").expect("saved");
    let mut out = Vec::new();
    crate::bench::table(&paths, &mut out).expect("no FAIL line");
    assert_eq!(String::from_utf8(out).expect("text"), format!("{TWO}\n"));
}

#[test]
fn the_table_of_three_results_has_a_column_a_file_and_heads_each_line_under_it() {
    assert_eq!(
        printed(&[baseline(), columns(), parallel()]),
        r##"kibbr-bench   commit 9d0c3e12ab77   baseline on 1bfe096e45ef   farm 1,000,000 lines, median of 3 runs of 100 samples
                                               baseline              after 1 (columns)                after 2 (parallel)
ingest lines/s                                 91,166                92,040 (1.01x)                   90,870 (1.00x)
S1 promoted value   p50 / p99                  1.52 / 2.10 ms        0.61 / 0.92 ms (2.5x / 2.3x)     0.58 / 0.90 ms (2.6x / 2.3x)
S2 unpromoted key   p50 / p99                  45.8 / 49.2 ms        3.10 / 3.60 ms (14.8x / 13.7x)   1.05 / 1.40 ms (43.6x / 35.1x)
S3 one id           p50 / p99                  131.0 / 139.0 ms      8.40 / 9.90 ms (15.6x / 14.0x)   2.90 / 3.80 ms (45.2x / 36.6x)
S4 newest 500       p50 / p99                  1,842 / 1,903 ms      410.0 / 436.0 ms (4.5x / 4.4x)   121.0 / 140.0 ms (15.2x / 13.6x)
scan rows/s/core                               1,690,268             25,400,000 (15.0x)               23,100,000 (13.7x)
plan step at 10,000 templates:                 16.2 ms   over 1 ms: yes      16.1 ms (1.01x)   over 1 ms: yes      16.2 ms (1.00x)   over 1 ms: yes
same four queries under 4-sender ingest, p99:  S1 9.80  S2 131.0  S3 236.0  S4 3,120 ms
                                               S1 6.10 (1.6x)  S2 14.2 (9.2x)  S3 21.0 (11.2x)  S4 690.0 (4.5x) ms      after 1 (columns)
                                               S1 5.80 (1.7x)  S2 9.10 (14.4x)  S3 12.4 (19.0x)  S4 310.0 (10.1x) ms    after 2 (parallel)
(Nx) is the speed-up on the baseline: this / baseline for the two rates, baseline / this for every time; above 1 is faster.
scan rows/s/core is rows holding `status` x 25 asks of COUNT LOG {"status":"200"} / CPU-seconds of the server process. By the wall clock: baseline 1,686,061 rows/s on 1.00 cores; after 1 (columns) 25,300,000 on 1.00 (15.0x); after 2 (parallel) 87,700,000 on 3.80 (52.0x).
after 1 (columns)    ingest lines/s: the runs overlap (90,310 to 93,120 against the baseline's 89,802 to 93,410) - not a difference
after 1 (columns)    plan step: the runs overlap (16.0 to 16.3 ms against the baseline's 16.1 to 16.4) - not a difference
after 1 (columns)    S1 under ingest p99: the runs overlap (5.90 to 9.60 ms against the baseline's 9.20 to 12.1) - not a difference
after 1 (columns)    S4 under ingest: the buffer limit was reached in 291 of 300 baseline laps and 0 of 300 here
after 2 (parallel)   ingest lines/s: the runs overlap (88,940 to 92,300 against the baseline's 89,802 to 93,410) - not a difference
after 2 (parallel)   plan step: the runs overlap (16.1 to 16.3 ms against the baseline's 16.1 to 16.4) - not a difference
after 2 (parallel)   S1 under ingest p99: the runs overlap (5.60 to 9.30 ms against the baseline's 9.20 to 12.1) - not a difference
after 2 (parallel)   S4 under ingest: the buffer limit was reached in 291 of 300 baseline laps and 0 of 300 here"##
    );
}

#[test]
fn a_result_against_itself_is_one_everywhere_and_inside_the_noise_everywhere() {
    let lines = table(&[baseline(), baseline()])
        .expect("the same work")
        .lines;
    let rows = &lines[2..11];
    for row in &rows[..6] {
        assert!(row.contains("(1.00x") && row.ends_with("1.00x)"), "{row}");
    }
    assert!(
        rows[6].ends_with("16.2 ms (1.00x)   over 1 ms: yes"),
        "{}",
        rows[6]
    );
    assert_eq!(
        rows[8].trim_start(),
        "S1 9.80 (1.00x)  S2 131.0 (1.00x)  S3 236.0 (1.00x)  S4 3,120 (1.00x) ms    baseline"
    );
    // Every ratio printed has its line: one for ingest, two for each idle query, the
    // scan row, the plan step, one for each query under ingest, the wall clock's.
    let inside: Vec<&str> = lines
        .iter()
        .filter(|line| line.ends_with(" - not a difference"))
        .map(|line| line.split(": the runs overlap (").next().expect("a name"))
        .collect();
    assert_eq!(
        inside,
        [
            "ingest lines/s",
            "S1 p50",
            "S1 p99",
            "S2 p50",
            "S2 p99",
            "S3 p50",
            "S3 p99",
            "S4 p50",
            "S4 p99",
            "scan rows/s/core",
            "plan step",
            "S1 under ingest p99",
            "S2 under ingest p99",
            "S3 under ingest p99",
            "S4 under ingest p99",
            "scan by the wall clock",
        ]
    );
    // And nothing else is said: the two are the same in every other respect.
    assert_eq!(lines.len(), 13 + inside.len(), "{lines:#?}");
}

#[test]
fn a_scan_block_with_no_cpu_time_prints_no_rate_and_says_why() {
    let mut saved = baseline();
    saved.file.runs[1].scan.cpu_s = Some(0.0);
    saved.file.runs[1].scan.rows_per_cpu_s = None;
    saved.file.per_run = per_run(&saved.file.runs);
    saved.file.medians = medians(&saved.file.per_run);
    let lines = table(&[saved.clone()]).expect("a table").lines;
    assert_eq!(
        lines[7],
        "scan rows/s/core                               n/a"
    );
    assert_eq!(
        lines[11..],
        [
            "scan rows/s/core is n/a: the system reported no CPU time over the scan block \
                 in run 2",
            "S4 under ingest: the buffer limit was reached in 291 of 300 laps"
        ]
    );
    // Beside a baseline that has one, no ratio is made up and no line claims overlap.
    let lines = table(&[baseline(), saved.clone()]).expect("a table").lines;
    assert!(
        lines[7].ends_with("1,690,268             n/a"),
        "{}",
        lines[7]
    );
    assert!(!lines
        .iter()
        .any(|line| line.starts_with("scan rows/s/core: the runs")));
    // And the other way round, the rate is printed without one.
    let lines = table(&[saved, baseline()]).expect("a table").lines;
    assert!(
        lines[7].ends_with("n/a                   1,690,268 (n/a)"),
        "{}",
        lines[7]
    );

    // A system with no source of a child's CPU time says that instead.
    let mut blind = baseline();
    for run in &mut blind.file.runs {
        run.scan.cpu_s = None;
        run.scan.rows_per_cpu_s = None;
    }
    blind.file.per_run = per_run(&blind.file.runs);
    blind.file.medians = medians(&blind.file.per_run);
    assert_eq!(
        notes(&blind.file),
        [
            "scan rows/s/core is n/a: the system gave no CPU time for the server process \
                 in run 1",
            "S4 under ingest: the buffer limit was reached in 291 of 300 laps"
        ]
    );
}

#[test]
fn a_line_under_the_table_is_printed_only_when_it_is_true() {
    // Nothing is true of the baseline but its laps at the buffer limit.
    assert_eq!(
        notes(&baseline().file),
        ["S4 under ingest: the buffer limit was reached in 291 of 300 laps"]
    );

    let mut file = baseline().file;
    file.flags.checked = false;
    file.flags.samples = 1;
    file.binary.build = "debug".to_string();
    file.binary.stale = Some(true);
    file.commit.src_dirty = Some(true);
    file.runs[0].loaded.queries.s4.late = 2;
    file.runs[2].loaded.queries.s4.late = 1;
    file.runs[1].loaded.wrapped_laps = 7;
    file.runs[0].ingest.refused.s503 = 10;
    file.runs[0].ingest.refused.s429 = 2;
    file.runs[1].loaded.refused_bodies = 4;
    file.runs[0].idle.scan.refused = 1;
    file.runs[0].idle.queries.s2.refused = 3;
    file.runs[0].loaded.queries.s1.refused = 5;
    // Stores that failed to write: mid-window and recovered in run 1, still failing at run
    // 2's end, and laps known by the count alone, the flag alone, and both.
    file.runs[0].ingest.at_end.write_failures = Some(3);
    file.runs[1].ingest.at_end.write_failures = Some(1);
    file.runs[1].ingest.health_at_end = Health {
        status: "degraded".to_string(),
        detail: Some("the store cannot write".to_string()),
    };
    file.runs[0].loaded.lap.write_failures = vec![Some(0), Some(2), None];
    file.runs[0].loaded.lap.degraded = vec![Some(false), Some(false), None];
    file.runs[2].loaded.lap.write_failures = vec![None, Some(4)];
    file.runs[2].loaded.lap.degraded = vec![Some(true), Some(true)];
    file.runs[2].idle.health_at_end.status = "degraded".to_string();
    file.runs[2].voided = vec![Voided::default()];
    for run in &mut file.runs {
        run.scan.cpu_tail_s = Some(0.412);
        run.idle.scan.tail_ms = 100;
        run.loaded.laps_at_buffer_limit = Some(0);
    }
    file.per_run = per_run(&file.runs);
    file.medians = medians(&file.per_run);
    assert_eq!(
        notes(&file),
        [
            "p99 of 1 samples",
            "UNCHECKED - not a measurement",
            "debug build - not a measurement",
            "the commit is the checkout's; the binary was built before it was committed",
            "the commit is the checkout's; its source differed from the commit when this \
                 ran, so the commit does not name the code measured",
            "S4 under ingest: 3 of 300 samples started late",
            "7 of 300 laps ran past the end of the stream",
            "bodies were refused 12 times in the ingest phase, each followed by a 50 ms \
                 pause inside the window",
            "bodies were refused 4 times in the laps under ingest",
            "ingest: 3 writes to the server's store failed in run 1: it acknowledged \
                 lines while no row reached a file",
            "ingest: the server was degraded at the end of run 2 (the store cannot write)",
            "ingest: 1 write to the server's store failed in run 2: it acknowledged lines \
                 while no row reached a file",
            "3 of 300 laps ran on a store that could not write",
            "idle: the server was degraded at the end of run 3",
            "scan: asks of the block were refused 1 time and sent again; the wait is in \
                 its wall clock",
            "S1 under ingest: asks were refused 5 times and sent again; the wait is in \
                 their samples",
            "S2: asks of the idle server were refused 3 times and sent again; the wait is \
                 in their samples",
            "1 unit was disturbed, thrown away and done again",
            "scan: the server used 0.412 s of CPU in the 100 ms after the block - workers \
                 that do not sleep are inside the scan row",
        ]
    );

    // In a table of its own they stand under the scan note, as they are.
    let alone = table(&[Saved {
        path: PathBuf::from("a.json"),
        file: file.clone(),
    }])
    .expect("a table")
    .lines;
    assert_eq!(alone[11], "p99 of 1 samples");
    assert_eq!(alone[12], "UNCHECKED - not a measurement");
    assert_eq!(alone.len(), 11 + 19);
    assert!(alone[0].ends_with("median of 3 runs of 1 samples"));
}

#[test]
fn what_was_not_the_same_as_the_baseline_is_said_with_both_numbers() {
    let base = baseline();
    let mut other = columns();
    let file = &mut other.file;
    file.bench.source_crc32c = "77c1f0b2".to_string();
    file.machine.os = "macos".to_string();
    file.machine.os_version = Some("14.4".to_string());
    for run in &mut file.runs {
        run.loaded.queries.s4.count_over_loaded_median = 148_001;
        // One round of bodies is 8,000 lines: this far is still the same store.
        run.loaded.queries.s2.count_over_loaded_median = 905 + 8_000;
        run.checks.spin_ms_before = Some(118.0);
        run.checks.flush_ms_before = Some(71.0);
        run.loaded.laps_at_buffer_limit = Some(1);
    }
    file.runs[0].idle.queries.s2.rows_scanned = Some(40_402);
    file.runs[0].idle.queries.s2.row_groups_read = Some(12);
    file.runs[0].idle.queries.s2.bytes_read = Some(58_112);
    file.runs[0].idle.queries.s2.files_opened = Some(1);
    file.runs[0].idle.scan.rows_scanned = Some(400_000);
    file.per_run = per_run(&file.runs);
    file.medians = medians(&file.per_run);
    let mut base = base;
    let was = &mut base.file.runs[0].idle;
    was.queries.s2.rows_scanned = Some(121_207);
    was.queries.s2.row_groups_read = Some(74);
    was.queries.s2.bytes_read = Some(1_204_224);
    was.queries.s2.files_opened = Some(61);
    was.scan.rows_scanned = Some(433_520);
    // What both read the same, and what one of them did not say, is not a line.
    was.queries.s1.rows_scanned = Some(11_915);
    other.file.runs[0].idle.queries.s1.rows_scanned = Some(11_915);
    was.queries.s3.rows_scanned = Some(76_100);

    assert_eq!(
        against_lines(&base.file, &other.file),
        [
            "NOT THE SAME BENCH: the bench's own source differs from the baseline's \
                 (crc32c 5d11e0aa, 77c1f0b2) - these ratios compare two instruments",
            "the system is macos 14.4 where the baseline's was n/a",
            "S4 under ingest: the buffer limit was reached in 291 of 300 baseline laps \
                 and 3 of 300 here",
            "S4 under ingest: its answers held a median of 148,001 live rows where the \
                 baseline's held 140,000 - not the same store",
            "spin 118.0 ms against the baseline's 101.6 ms: not the same machine state",
            "flush 71.0 ms against the baseline's 53.6 ms: not the same disk state - \
                 ingest lines/s leans on it",
            "the server now counts 40,402 rows scanned for S2 where the baseline's \
                 counted 121,207",
            "the server now reads 12 row groups for S2 where the baseline's read 74",
            "the server now reads 58,112 bytes for S2 where the baseline's read 1,204,224",
            "the server now opens 1 files for S2 where the baseline's opened 61",
            "the server now counts 400,000 rows scanned for the scan block where the \
                 baseline's counted 433,520",
        ]
    );
    // None of it stops the table or fails it.
    let table = table(&[base.clone(), other.clone()]).expect("still the same work");
    assert_eq!(table.fails, 0);
    assert!(table
        .lines
        .iter()
        .any(|line| line.starts_with("NOT THE SAME BENCH: ")));

    // A reference inside its share of the baseline's is the same state.
    assert!(!apart(106.0, 101.6, SPIN_APART) && apart(107.0, 101.6, SPIN_APART));
    assert!(!apart(96.6, 101.6, SPIN_APART) && apart(96.0, 101.6, SPIN_APART));
    assert!(!apart(61.0, 53.6, FLUSH_APART) && apart(62.0, 53.6, FLUSH_APART));
}

#[test]
fn a_server_environment_that_is_not_the_baseline_s_is_said_and_stops_nothing() {
    let base = baseline();
    let mut other = baseline();
    other.path = PathBuf::from("target/scratch/bench/after-1.json");
    other.file.label = "four threads".to_string();
    // Two results that differ in nothing: no such line, and no other.
    assert_eq!(against_lines(&base.file, &other.file), Vec::<String>::new());

    let env = &mut other.file.server.env_noted;
    env.insert("RAYON_NUM_THREADS".to_string(), "4".to_string());
    env.insert("MIMALLOC_ARENA_EAGER_COMMIT".to_string(), "1".to_string());
    assert_eq!(
        against_lines(&base.file, &other.file),
        [
            "the server's environment held MIMALLOC_ARENA_EAGER_COMMIT=1, \
                 RAYON_NUM_THREADS=4 where the baseline's held none - not the same server \
                 settings"
        ]
    );
    // The other way round, and a value that moved.
    assert_eq!(
        against_lines(&other.file, &base.file),
        [
            "the server's environment held none where the baseline's held \
                 MIMALLOC_ARENA_EAGER_COMMIT=1, RAYON_NUM_THREADS=4 - not the same server \
                 settings"
        ]
    );
    let mut eight = other.clone();
    eight
        .file
        .server
        .env_noted
        .insert("RAYON_NUM_THREADS".to_string(), "8".to_string());
    assert!(against_lines(&other.file, &eight.file)[0].starts_with(
        "the server's environment held MIMALLOC_ARENA_EAGER_COMMIT=1, RAYON_NUM_THREADS=8 \
             where the baseline's held MIMALLOC_ARENA_EAGER_COMMIT=1, RAYON_NUM_THREADS=4 - "
    ));

    // It is a line under the table, from saved files as from a run, and not a refusal.
    let table = table(&[base, other]).expect("still the same work");
    assert_eq!(table.fails, 0);
    assert!(
        table
            .lines
            .iter()
            .any(|line| line.starts_with("the server's environment held ")),
        "{:#?}",
        table.lines
    );
}

// ── How the machine was powered ──────────────────────────────────────────────────

fn power(source: &str, low_power_mode: bool) -> Power {
    Power {
        source: Some(source.to_string()),
        low_power_mode: Some(low_power_mode),
    }
}

/// A result whose every run began and ended in one power state.
fn powered(mut saved: Saved, source: &str, low_power_mode: bool) -> Saved {
    for run in &mut saved.file.runs {
        run.checks.power_before = Some(power(source, low_power_mode));
        run.checks.power_after = Some(power(source, low_power_mode));
    }
    saved
}

#[test]
fn a_result_saved_before_the_power_state_was_kept_reads_with_none() {
    // A run's checks as an earlier build of the bench wrote them.
    let earlier = r#"{"waited_s": 0, "waited_for": [], "swapouts_before": 7983780,
            "swapins_before": 7390167, "load_average": 1.21,
            "memory_available_bytes": 4102033408, "spin_ms_before": 458.8,
            "spin_ms_after": 267.0, "flush_ms_before": 53.6, "flush_ms_after": 54.1,
            "after_unit_checks": true}"#;
    let checks: Checks = serde_json::from_str(earlier).expect("lenient");
    assert_eq!((&checks.power_before, &checks.power_after), (&None, &None));
    assert_eq!(checks.spin_ms_after, Some(267.0));

    // A whole result with the two keys taken out of every run reads, and is the
    // result with no reading.
    let with = powered(baseline(), "Battery Power", true).file;
    let mut json = serde_json::to_value(&with).expect("a result is JSON");
    for run in json["runs"].as_array_mut().expect("runs") {
        let checks = run["checks"].as_object_mut().expect("checks");
        assert!(checks.remove("power_before").is_some());
        assert!(checks.remove("power_after").is_some());
    }
    let without: ResultFile = serde_json::from_value(json).expect("lenient");
    assert_eq!(without, baseline().file);
    // And no line is made of what was not recorded.
    assert_eq!(notes(&without), notes(&baseline().file));
    assert_eq!(power_of(&without.runs), None);

    // As it is written: the system's own name for the source, and a half the system
    // did not give as `null`.
    let half = Checks {
        power_before: Some(power("Battery Power", true)),
        power_after: Some(Power {
            source: Some("AC Power".to_string()),
            low_power_mode: None,
        }),
        ..Checks::default()
    };
    let json = serde_json::to_value(&half).expect("JSON");
    assert_eq!(
        json["power_before"],
        serde_json::json!({"source": "Battery Power", "low_power_mode": true})
    );
    assert_eq!(
        json["power_after"],
        serde_json::json!({"source": "AC Power", "low_power_mode": null})
    );
    let back: Checks = serde_json::from_value(json).expect("and reads back");
    assert_eq!(back, half);
}

#[test]
fn a_run_whose_two_spins_are_apart_says_the_machine_changed_state() {
    let mut file = baseline().file;
    let quiet = notes(&file);
    // Run 1 began in Low Power Mode and ended out of it. Run 2 is a hair inside the share
    // of its first reading; run 3 a hair over it, the other way.
    (
        file.runs[0].checks.spin_ms_before,
        file.runs[0].checks.spin_ms_after,
    ) = (Some(458.8), Some(267.0));
    (
        file.runs[1].checks.spin_ms_before,
        file.runs[1].checks.spin_ms_after,
    ) = (Some(260.0), Some(272.9));
    (
        file.runs[2].checks.spin_ms_before,
        file.runs[2].checks.spin_ms_after,
    ) = (Some(260.0), Some(246.9));
    let said: Vec<String> = file.runs.iter().filter_map(spin_moved).collect();
    assert_eq!(
        said,
        [
            "spin 458.8 ms before run 1 and 267.0 ms after it: the machine changed state \
                 during the run",
            "spin 260.0 ms before run 3 and 246.9 ms after it: the machine changed state \
                 during the run",
        ]
    );
    // They are lines of the result alone, under its table and in its notes.
    let mut with = said.clone();
    with.extend(quiet.clone());
    assert_eq!(notes(&file), with);
    let lines = table(&[Saved {
        path: PathBuf::from("a.json"),
        file: file.clone(),
    }])
    .expect("a table")
    .lines;
    assert_eq!(lines[11..13], said[..]);

    // A run with one reading, or none, has nothing to set against itself.
    file.runs[0].checks.spin_ms_after = None;
    file.runs[2].checks.spin_ms_before = None;
    assert_eq!(notes(&file), quiet);
    assert_eq!(spin_moved(&Run::default()), None);
}

#[test]
fn runs_on_a_battery_or_in_low_power_mode_are_named_as_not_the_machine_s_on_mains() {
    const WHY: &str = "the numbers are not the machine's on mains with Low Power Mode off";
    // On mains with the mode off, whatever else is true, there is no line; nor where
    // nothing was read.
    let quiet = notes(&baseline().file);
    assert_eq!(notes(&powered(baseline(), "AC Power", false).file), quiet);
    assert_eq!(off_mains_lines(&baseline().file.runs), Vec::<String>::new());

    // Every run the same way: one line names them all.
    let file = powered(baseline(), "Battery Power", true).file;
    let all =
        format!("runs 1, 2 and 3 began and ended on Battery Power with Low Power Mode on: {WHY}");
    assert_eq!(off_mains_lines(&file.runs), std::slice::from_ref(&all));
    let mut with = vec![all.clone()];
    with.extend(quiet.clone());
    assert_eq!(notes(&file), with);

    // Either half is enough: a battery with the mode off, and mains with it on.
    let file = powered(baseline(), "Battery Power", false).file;
    assert_eq!(
        off_mains_lines(&file.runs[..1]),
        [format!(
            "run 1 began and ended on Battery Power with Low Power Mode off: {WHY}"
        )]
    );
    let file = powered(baseline(), "AC Power", true).file;
    assert_eq!(
        off_mains_lines(&file.runs[1..]),
        [format!(
            "runs 2 and 3 began and ended on AC Power with Low Power Mode on: {WHY}"
        )]
    );

    // A run that left Low Power Mode part way, one on mains throughout, one unplugged before
    // it ended. Runs that did the same are named together, in the order first met.
    let mut file = powered(baseline(), "AC Power", false).file;
    file.runs[0].checks.power_before = Some(power("Battery Power", true));
    file.runs[0].checks.power_after = Some(power("Battery Power", false));
    file.runs[2].checks.power_after = Some(power("Battery Power", false));
    assert_eq!(
        off_mains_lines(&file.runs),
        [
            format!(
                "run 1 began on Battery Power with Low Power Mode on and ended on Battery \
                     Power with Low Power Mode off: {WHY}"
            ),
            format!("run 3 ended on Battery Power with Low Power Mode off: {WHY}"),
        ]
    );
    file.runs[0].checks.power_after = None;
    file.runs[1].checks.power_before = Some(power("Battery Power", true));
    assert_eq!(
        off_mains_lines(&file.runs)[0],
        format!("runs 1 and 2 began on Battery Power with Low Power Mode on: {WHY}")
    );

    // A half the system did not give is not held against the machine.
    let unknown = Power {
        source: None,
        low_power_mode: Some(false),
    };
    assert!(!unknown.off_mains() && !Power::default().off_mains());
    assert!(!power("AC Power", false).off_mains());
    assert!(power("UPS Power", false).off_mains());
    assert_eq!(
        (
            runs_named(&[2]),
            runs_named(&[1, 3]),
            runs_named(&[1, 2, 3])
        ),
        (
            "run 2".to_string(),
            "runs 1 and 3".to_string(),
            "runs 1, 2 and 3".to_string()
        )
    );

    // In a table of two results the line is headed by the result it is true of.
    let lines = table(&[
        powered(baseline(), "AC Power", false),
        powered(columns(), "Battery Power", true),
    ])
    .expect("still the same work")
    .lines;
    assert!(
        lines.contains(&format!("after 1 (columns)   {all}")),
        "{lines:#?}"
    );
}

#[test]
fn a_power_state_that_is_not_the_baseline_s_is_said_with_both() {
    let mains = powered(baseline(), "AC Power", false);
    let mut other = powered(baseline(), "AC Power", false);
    other.path = PathBuf::from("target/scratch/bench/after-1.json");
    // The same state in both: no line.
    assert_eq!(
        against_lines(&mains.file, &other.file),
        Vec::<String>::new()
    );

    let battery = powered(other.clone(), "Battery Power", true);
    assert_eq!(
        against_lines(&mains.file, &battery.file),
        [
            "the machine was on Battery Power with Low Power Mode on where the baseline's \
                 was on AC Power with Low Power Mode off: not the same machine state"
        ]
    );
    // The other way round, and a result whose machine changed state as it ran: each
    // state once, in the order it was read.
    let mut changed = battery.clone();
    changed.file.runs[0].checks.power_after = Some(power("AC Power", false));
    for run in &mut changed.file.runs[1..] {
        run.checks.power_before = Some(power("AC Power", false));
        run.checks.power_after = Some(power("AC Power", false));
    }
    assert_eq!(
        against_lines(&battery.file, &changed.file),
        [
            "the machine was on Battery Power with Low Power Mode on, then on AC Power \
                 with Low Power Mode off where the baseline's was on Battery Power with Low \
                 Power Mode on: not the same machine state"
        ]
    );

    // A result with no reading - an earlier one, an unchecked one, another system's -
    // is not said to differ, on either side.
    let unread = baseline();
    assert_eq!(power_of(&unread.file.runs), None);
    assert_eq!(
        against_lines(&unread.file, &battery.file),
        Vec::<String>::new()
    );
    assert_eq!(
        against_lines(&battery.file, &unread.file),
        Vec::<String>::new()
    );

    // A half the system did not give is not a state the machine was in. One `pmset`
    // that did not answer in the middle of a result, on either side: no line.
    let mut half_read = other.clone();
    half_read.file.runs[1].checks.power_before = Some(Power {
        source: Some("AC Power".to_string()),
        low_power_mode: None,
    });
    half_read.file.runs[2].checks.power_after = Some(Power {
        source: None,
        low_power_mode: Some(false),
    });
    assert_eq!(
        power_of(&half_read.file.runs).as_deref(),
        Some(
            "on AC Power with Low Power Mode off, then on AC Power with Low Power Mode \
                 n/a, then on AC Power with Low Power Mode off, then on n/a with Low Power \
                 Mode off"
        )
    );
    assert!(!power_differs(&mains.file.runs, &half_read.file.runs));
    assert_eq!(
        against_lines(&mains.file, &half_read.file),
        Vec::<String>::new()
    );
    assert_eq!(
        against_lines(&half_read.file, &mains.file),
        Vec::<String>::new()
    );
    // What it did give still counts: the same result against one on its battery is
    // said, with every reading as it was.
    assert_eq!(
        against_lines(&half_read.file, &battery.file).len(),
        1,
        "a known half differs"
    );
    // A machine that never names Low Power Mode is told from itself on its battery by
    // the half it has, and is the same machine state as itself.
    let unnamed = |source: &str| {
        let mut saved = other.clone();
        for run in &mut saved.file.runs {
            let power = Some(Power {
                source: Some(source.to_string()),
                low_power_mode: None,
            });
            (run.checks.power_before, run.checks.power_after) = (power.clone(), power);
        }
        saved
    };
    assert_eq!(
        against_lines(&unnamed("AC Power").file, &unnamed("Battery Power").file),
        [
            "the machine was on Battery Power with Low Power Mode n/a where the \
                 baseline's was on AC Power with Low Power Mode n/a: not the same machine \
                 state"
        ]
    );
    assert_eq!(
        against_lines(&unnamed("AC Power").file, &unnamed("AC Power").file),
        Vec::<String>::new()
    );
    // And beside a machine that does name it, only the half both have is compared.
    assert_eq!(
        against_lines(&mains.file, &unnamed("AC Power").file),
        Vec::<String>::new()
    );
    assert_eq!(
        against_lines(&mains.file, &unnamed("Battery Power").file).len(),
        1
    );

    // It is a line under the table, from saved files as from a run, and stops nothing.
    let table = table(&[mains, battery]).expect("still the same work");
    assert_eq!(table.fails, 0);
    assert!(
        table
            .lines
            .iter()
            .any(|line| line.starts_with("the machine was on Battery Power ")),
        "{:#?}",
        table.lines
    );
}

#[test]
fn a_plan_that_returns_other_leaves_or_another_trie_fails_the_table() {
    let mut base = baseline();
    for run in &mut base.file.runs {
        run.plan.shapes.p2.leaves = 4_460;
        run.plan.shapes.p4.leaves = 10_000;
    }
    let mut other = columns();
    for run in &mut other.file.runs {
        run.plan.shapes.p2.leaves = 4_460;
        run.plan.shapes.p4.leaves = 10_000;
    }
    assert_eq!(
        table(&[base.clone(), other.clone()])
            .expect("a table")
            .fails,
        0
    );

    // One run of the three planned to fewer leaves, on a trie of more nodes.
    other.file.runs[2].plan.shapes.p2.leaves = 4_455;
    other.file.plan_trie.nodes = 57_650;
    let two = table(&[base.clone(), other.clone()]).expect("a table");
    assert_eq!(two.fails, 2);
    let fails: Vec<&String> = two
        .lines
        .iter()
        .filter(|line| line.starts_with("FAIL"))
        .collect();
    assert_eq!(
        fails,
        [
            "FAIL the plan step's trie has 57,650 nodes where the baseline's had 57,642: \
                 the plans were timed on another trie",
            "FAIL P2 plans to 4,455 leaves where the baseline's planned to 4,460: a plan \
                 that returns other leaves is not a faster one",
        ]
    );
    assert_eq!(
        two.lines.last(),
        fails.last().copied(),
        "the last thing a column says"
    );

    // With more than one column after the baseline, the line says whose it is.
    let mut third = parallel();
    for run in &mut third.file.runs {
        run.plan.shapes.p2.leaves = 4_460;
        run.plan.shapes.p4.leaves = 9_999;
    }
    let three = table(&[base, other, third]).expect("a table");
    assert_eq!(three.fails, 3);
    assert!(three.lines.contains(
        &"FAIL after 2 (parallel): P4 plans to 9,999 leaves where the baseline's planned \
              to 10,000: a plan that returns other leaves is not a faster one"
            .to_string()
    ));
    assert!(three
        .lines
        .iter()
        .any(|line| line.starts_with("FAIL after 1 (columns): the plan step's trie has ")));
}

#[test]
fn results_of_other_work_do_not_make_a_table() {
    let mut other = columns();
    other.file.flags.samples = 2;
    other.file.machine.cores_logical = 10;
    let refused = table(&[baseline(), other]).expect_err("other work");
    assert_eq!(
        refused.to_string(),
        "target/scratch/bench/baseline.json cannot be the baseline of this run: \
             flags.samples: 100 in the baseline, 2 in target/scratch/bench/after-1.json; \
             machine.cores_logical: 0 in the baseline, 10 in target/scratch/bench/after-1.json"
    );
    let mut wrong = columns();
    wrong.file.expect.s1.count = 7;
    let refused = table(&[baseline(), wrong])
        .expect_err("another answer")
        .to_string();
    assert!(
        refused.contains(": FAIL S1 counts 7 where the baseline counted 0: "),
        "{refused}"
    );
    // No file is no table, and not an error.
    assert_eq!(table(&[]).expect("nothing"), Table::default());
}

// ── The raw log ──────────────────────────────────────────────────────────────────

/// The first run of the baseline, with everything a log line reads filled in.
fn the_first_run() -> Run {
    let mut run = baseline().file.runs.remove(0);
    run.seconds = 662.0;
    run.checks = Checks {
        waited_s: 0,
        waited_for: Vec::new(),
        swapouts_before: Some(7_983_780),
        swapins_before: Some(7_390_167),
        load_average: Some(1.21),
        memory_available_bytes: Some(4_102_033_408),
        spin_ms_before: Some(101.4),
        spin_ms_after: Some(102.0),
        flush_ms_before: Some(53.6),
        flush_ms_after: Some(54.1),
        power_before: Some(power("AC Power", false)),
        power_after: Some(power("AC Power", false)),
        after_unit_checks: true,
    };
    let shape = |p50_ms, p99_ms, leaves| PlanShape {
        ms: Vec::new(),
        p50_ms,
        p99_ms,
        min_ms: p50_ms,
        leaves,
    };
    run.plan = PlanRun {
        templates: 10_000,
        nodes: 57_642,
        tokens: 98_495,
        lines_skipped: 0,
        shapes: Shapes {
            p1: shape(4.58, 4.90, 1_360),
            p2: shape(16.2, 17.0, 4_460),
            p3: shape(14.4, 15.1, 1_430),
            p4: shape(17.9, 18.8, 10_000),
        },
    };
    let server = |pid| ServerUnit {
        pid,
        start_s: if pid == 40_390 { 0.20 } else { 0.21 },
        copy_ms: 4.0,
        copy_bytes: 36_423_735,
    };
    let ok = Health {
        status: "ok".to_string(),
        detail: None,
    };
    run.ingest = IngestRun {
        server: server(40_390),
        lines_ingested_before: 0,
        lines_ingested_after: 1_000_000,
        lines: 1_000_000,
        uptime_before_s: 0.412,
        uptime_after_s: 11.381,
        window_s: 10.969,
        lines_per_s: 91_166.0,
        window_s_bench: 10.971,
        stream_lines: 1_000_000,
        bodies: 500,
        acked_bodies: 500,
        acked_lines: 1_000_000,
        counters_agree: true,
        acked_lines_by_second: vec![
            96_000, 190_000, 286_000, 382_000, 470_000, 548_000, 630_000, 712_000, 800_000,
            890_000, 978_000,
        ],
        cpu_s: Some(51.2),
        cores_busy: Some(4.67),
        resident_bytes_at_end: Some(1_470_103_552),
        pageins: Some(0),
        at_end: AtEnd {
            buffered_rows: Some(290_412),
            buffered_bytes: Some(735_032_772),
            max_buffer_bytes: Some(536_870_912),
            forced_closes: Some(3),
            reached_buffer_limit: Some(true),
            requests_shed: Some(0),
            lines_rejected: Some(0),
            write_failures: Some(0),
            wal_behind: Some(0),
            templates_before: Some(145),
            templates_after: Some(151),
            rows: Some(2_000_000),
        },
        health_at_end: ok.clone(),
        ..IngestRun::default()
    };

    let answer = |count, rows, templates, truncated, limit: &str| Expect {
        count,
        rows_returned: Some(rows),
        templates_matched: Some(templates),
        truncated: Some(truncated),
        limit_source: Some(limit.to_string()),
    };
    run.idle.server = server(40_131);
    run.idle.key_rows = 121_207;
    run.idle.scan_key_rows = 433_520;
    run.idle.health_at_end = ok;
    run.idle.scan = ScanBlock {
        asks: 25,
        files_opened: Some(2),
        cpu_user_s: Some(6.29),
        cpu_system_s: Some(0.12),
        tail_ms: 100,
        ..ScanBlock::default()
    };
    let idle = [
        (1.52, 2.10, 1.44, 3.90, 1.31, 1.80, 11_915, 2, 0.151, 0.153),
        (
            45.8, 49.2, 44.9, 50.3, 45.5, 48.9, 121_207, 74, 4.580, 4.612,
        ),
        (
            131.0, 139.0, 128.0, 141.0, 130.0, 138.0, 76_100, 46, 13.02, 13.14,
        ),
        (
            1_842.0, 1_903.0, 1_811.0, 1_960.0, 1_838.0, 1_899.0, 829_670, 199, 183.9, 184.6,
        ),
    ];
    let answers = [
        answer(11_915, 0, 1, false, "unlimited"),
        answer(28_907, 0, 61, false, "unlimited"),
        answer(2, 2, 37, false, "backstop"),
        answer(1_000_000, 500, 145, true, "caller"),
    ];
    for ((block, numbers), answer) in run
        .idle
        .queries
        .each_mut()
        .into_iter()
        .zip(idle)
        .zip(answers)
    {
        let (p50, p99, min, max, server_p50, server_p99, scanned, groups, cpu, wall) = numbers;
        *block = IdleBlock {
            p50_ms: p50,
            p99_ms: p99,
            min_ms: min,
            max_ms: max,
            server_p50_ms: Some(server_p50),
            server_p99_ms: Some(server_p99),
            answer,
            answers_identical: true,
            rows_scanned: Some(scanned),
            row_groups_read: Some(groups),
            cpu_s: Some(cpu),
            wall_s: wall,
            ..IdleBlock::default()
        };
    }
    run.scan = ScanRun {
        query: "SCAN".to_string(),
        key_rows: 433_520,
        asks: 25,
        rows: 10_838_000,
        cpu_s: Some(6.412),
        rows_per_cpu_s: Some(1_690_268.0),
        wall_s: 6.428,
        cores_busy: Some(1.00),
        rows_per_wall_s: Some(1_686_061.0),
        cpu_tail_s: Some(0.0),
        server_rows_scanned_sum: Some(10_838_000),
        beside: Beside {
            s2: BesideKeyed {
                key_rows: 121_207,
                asks: 100,
                rows: 12_120_700,
                cpu_s: Some(4.580),
                rows_per_cpu_s: Some(2_646_441.0),
                leaves_opened: Some(61),
            },
            s1: BesideRate {
                rows_per_cpu_s: Some(7_890_728.0),
            },
            s3: BesideRate {
                rows_per_cpu_s: Some(584_485.0),
            },
            s4: BesideRate {
                rows_per_cpu_s: Some(451_153.0),
            },
        },
    };

    let loaded = [
        (
            4.10,
            9.80,
            2.20,
            11.4,
            3.60,
            9.10,
            [20_000, 26_000],
            [232, 301],
        ),
        (
            96.0,
            131.0,
            88.1,
            140.0,
            95.2,
            130.0,
            [30_000, 36_000],
            [861, 1_040],
        ),
        (
            180.0,
            236.0,
            166.0,
            244.0,
            179.0,
            235.0,
            [70_000, 76_000],
            [0, 0],
        ),
        (
            2_650.0,
            3_120.0,
            2_480.0,
            3_190.0,
            2_644.0,
            3_113.0,
            [140_000, 148_000],
            [140_000, 148_000],
        ),
    ];
    for (block, numbers) in run.loaded.queries.each_mut().into_iter().zip(loaded) {
        let (p50, p99, min, max, server_p50, server_p99, at, over) = numbers;
        (block.p50_ms, block.p99_ms, block.min_ms, block.max_ms) = (p50, p99, min, max);
        (block.server_p50_ms, block.server_p99_ms) = (Some(server_p50), Some(server_p99));
        // Not in order, so that the least and the greatest are seen to be looked for.
        block.at_lines = vec![at[1], at[0], at[0]];
        block.count_over_loaded = vec![over[0], over[1], over[0]];
    }
    run.loaded.lines_per_s_median = Some(176_400.0);
    run
}

/// The baseline as the log's first lines read it: before any run.
fn at_the_start() -> (ResultFile, Corpus) {
    let mut file = baseline().file;
    file.runs.clear();
    file.machine = Machine {
        os: "macos".to_string(),
        os_version: Some("14.3.1".to_string()),
        arch: "aarch64".to_string(),
        cpu: Some("Apple M2".to_string()),
        cores_logical: 8,
        cores_performance: Some(4),
        cores_efficiency: Some(4),
        memory_bytes: Some(8_589_934_592),
        disk_free_bytes: 14_222_929_920,
        cpu_clock: Some("proc_pid_rusage, mach timebase 125/3".to_string()),
        cpu_clock_check: Some(1.0),
        swap_counter: Some("vm_stat".to_string()),
    };
    file.server.argv = "serve --host 127.0.0.1 --port 0 --data-dir <copy> --storage disk \
                            --max-rows 10000 --compact-interval-ms 0"
        .split(' ')
        .map(String::from)
        .collect();
    file.data = Data {
        dir: "target/scratch/bench/farm-1000000-seed42".to_string(),
        lines: 1_000_000,
        seed: 42,
        input: InputDigest {
            bytes: 489_036_605,
            crc32c: "0c77ab10".to_string(),
        },
        load: LoadFacts {
            load_id: Some("8eea6021fce6b677".to_string()),
            rows: 1_000_000,
            rejected: Some(0),
            row_groups: Some(231),
            days: 1,
            bytes: Some(36_212_480),
            seconds: Some(16.8),
        },
        files: vec![
            a_file("catalog.log", 211_004, "17c0a2e1"),
            a_file("data/2024-01-15/seg_0_000001.col", 36_212_480, "b40d91f3"),
            a_file("manifest.log", 251, "0e5a7c22"),
        ],
        s3_id: "20834abd-1e3d-462e-b89b-fb1d4643e6bb".to_string(),
        built_at: "2026-10-07T15:58:40Z".to_string(),
        built_by: BuiltBy::default(),
        live_stream: LiveStreamFacts {
            lines: 1_000_000,
            seed: 43,
            bytes: 489_486_504,
            crc32c: "41d2f09e".to_string(),
            bodies: 500,
        },
    };
    let answer = |count, rows, limit: &str| Expect {
        count,
        rows_returned: Some(rows),
        templates_matched: Some(1),
        truncated: Some(false),
        limit_source: Some(limit.to_string()),
    };
    file.expect = Seven {
        s1: answer(11_915, 0, "unlimited"),
        s2: answer(28_907, 0, "unlimited"),
        s3: answer(2, 2, "backstop"),
        s4: answer(1_000_000, 500, "caller"),
        key: answer(121_207, 0, "unlimited"),
        scan: answer(363_140, 0, "unlimited"),
        scan_key: answer(433_520, 0, "unlimited"),
    };
    let loaded = Corpus {
        dir: PathBuf::from("target/scratch/bench/farm-1000000-seed42"),
        marker_path: PathBuf::from("target/scratch/bench/farm-1000000-seed42.json"),
        reused: true,
        ..Corpus::default()
    };
    (file, loaded)
}

#[test]
fn the_log_opens_with_what_is_measured_on_what_over_which_bytes() {
    let (file, loaded) = at_the_start();
    assert_eq!(
        start_lines(&file, &loaded).join("\n"),
        r##"kibbr-bench  farm, 1,000,000 lines; 3 runs of 100 samples; 4 senders, 2,000-line bodies; label: baseline
commit       1bfe096e45ef, src clean (git rev-parse HEAD in the working directory when the bench ran; committed 2026-10-07T02:40:29+05:30)
binary       /Users/pratyaydeep/repo/kibbr/target/release/kibbr  6,735,968 bytes  crc32c 9a3f01c2  built 2026-10-07T02:44:11+05:30  release
bench code   crc32c 5d11e0aa over 8 source files, white space left out
machine      Apple M2; 8 cores (4 performance, 4 efficiency); 8,589,934,592 bytes of memory; macOS 14.3.1; 13.2 GiB free on the volume
cpu clock    proc_pid_rusage, mach timebase 125/3: this process over a spin by that source, over the same spin by getrusage, 1.000
swap         swap-outs are read from vm_stat
server       kibbr serve --host 127.0.0.1 --port 0 --data-dir <copy> --storage disk --max-rows 10000 --compact-interval-ms 0
             no KIBBR__ variable was set; noted: none. A new process on a new copy for every unit below, killed when the unit ends
loaded       target/scratch/bench/farm-1000000-seed42  reused: 3 files, 36,423,735 bytes, each file's crc32c as recorded when it was loaded
             loaded once, 2026-10-07T15:58:40Z, by `kibbr load` from gen-logs --profile farm --lines 1000000 --seed 42 (489,036,605 bytes, crc32c 0c77ab10): 1,000,000 rows, 231 row groups
live stream  gen-logs --profile farm --lines 1000000 --seed 43: 489,486,504 bytes, crc32c 41d2f09e; 500 bodies of 2,000 lines, built before any server starts
queries      S1 promoted value   COUNT LOG {"event":"login_ok"}                                 count 11,915
             S2 unpromoted key   COUNT LOG {"level":"info"}                                     count 28,907
             S3 one id           MATCH LOG {"job.id":"20834abd-1e3d-462e-b89b-fb1d4643e6bb"}    count 2, 2 rows; no LIMIT: the 10,000-row backstop applies and is not reached
             S4 newest 500       MATCH LOG {} LIMIT 500                                         count 1,000,000, 500 rows
             rows holding level  COUNT LOG {"level": *}                                         count 121,207
             scan block          COUNT LOG {"status":"200"}                                     count 363,140; not one of the four: asked 25 times for the scan rate
             rows holding status COUNT LOG {"status": *}                                        count 433,520
preflight    a server on a copy answered /health, /stats, the seven queries and one /ingest body the way this bench reads them
what is ms   a query's ms: this process's clock from the request's first byte written to the answer's last byte read, one open connection, one query at a time
             p50, p99: the 50th and 99th smallest of 100 (nearest rank)"##
    );
}

#[test]
fn the_log_says_what_a_first_line_cannot_take_for_granted() {
    let (mut file, mut loaded) = at_the_start();
    file.flags.checked = false;
    file.flags.samples = 1;
    file.commit.sha = None;
    file.binary.stale = Some(true);
    file.machine.cores_performance = None;
    file.machine.cpu_clock = None;
    file.machine.swap_counter = None;
    file.server.env_removed = vec!["KIBBR__LIMITS__MAX_ROWS".to_string()];
    file.server
        .env_noted
        .insert("RAYON_NUM_THREADS".to_string(), "4".to_string());
    file.expect.s3.truncated = Some(true);
    file.baseline = Some(BaselineRef {
        file: "target/scratch/bench/baseline.json".to_string(),
        sha: Some(BASELINE_SHA.to_string()),
        label: "baseline".to_string(),
        same_bench: false,
    });
    loaded.reused = false;
    loaded.left_out = vec![PathBuf::from("LOCK"), PathBuf::from("data/.DS_Store")];
    let lines = start_lines(&file, &loaded);
    let has = |text: &str| assert!(lines.iter().any(|line| line == text), "{text}\n{lines:#?}");
    has("kibbr-bench  farm, 1,000,000 lines; 3 runs of 1 samples; 4 senders, 2,000-line bodies; label: baseline; UNCHECKED - not a measurement");
    has("commit       unknown (git rev-parse HEAD in the working directory when the bench ran)");
    has("binary       /Users/pratyaydeep/repo/kibbr/target/release/kibbr  6,735,968 bytes  crc32c 9a3f01c2  built 2026-10-07T02:44:11+05:30  release  - built before the commit above was made");
    has("machine      Apple M2; 8 cores; 8,589,934,592 bytes of memory; macOS 14.3.1; 13.2 GiB free on the volume");
    has("cpu clock    n/a: this system has no source of a child's CPU time that this build reads, so scan rows/s/core is n/a");
    has("swap         this system has no swap counter that this build reads: swap is not a gate");
    has("             taken out of every child's environment: KIBBR__LIMITS__MAX_ROWS; noted: RAYON_NUM_THREADS=4. A new process on a new copy for every unit below, killed when the unit ends");
    has("loaded       target/scratch/bench/farm-1000000-seed42  built by this invocation: 3 files, 36,423,735 bytes, each file's crc32c recorded in target/scratch/bench/farm-1000000-seed42.json");
    has("             not part of the loaded bytes, and not copied: LOCK");
    has("             not part of the loaded bytes, and not copied: data/.DS_Store");
    has("baseline     target/scratch/bench/baseline.json  label: baseline; commit 1bfe096e45ef; NOT THE SAME BENCH: its bench code has another digest. The same flags, bytes, texts, answers, server arguments and machine");
    has("             p99 of 1 samples: p50, p99 are the 1st and 1st smallest of 1 (nearest rank)");
    assert!(lines
        .iter()
        .any(|line| line.ends_with("the 10,000-row backstop applies and is reached")));
}

#[test]
fn a_rank_is_named_as_the_percentile_rule_takes_it() {
    assert_eq!((rank(100, 0.50), rank(100, 0.99)), (50, 99));
    assert_eq!((rank(101, 0.50), rank(101, 0.99)), (51, 100));
    assert_eq!((rank(2, 0.50), rank(2, 0.99)), (1, 2));
    let spelled: Vec<String> = [1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 50, 99, 101, 111]
        .map(ordinal)
        .to_vec();
    assert_eq!(
        spelled,
        [
            "1st", "2nd", "3rd", "4th", "11th", "12th", "13th", "21st", "22nd", "23rd", "50th",
            "99th", "101st", "111th"
        ]
    );
}

#[test]
fn a_runs_lines_say_what_was_divided_by_what() {
    let run = the_first_run();
    let file = baseline().file;
    let flags = &file.flags;
    let mut lines = checks_lines(1, &run.checks, flags);
    lines.extend(plan_lines(1, &run.plan, flags));
    lines.extend(ingest_lines(1, &run.ingest));
    lines.extend(idle_lines(1, &run.idle, &run.scan, &file.queries, flags));
    lines.extend(laps_lines(1, flags));
    lines.extend(loaded_lines(1, &run.loaded, flags));
    lines.push(end_line(&run));
    assert_eq!(
        lines.join("\n"),
        r##"
run 1  checks    paused 20 s; no other kibbr process; swapouts 7,983,780 did not move in 2.0 s (swapins 7,390,167); load average 1.21 and 3.8 GiB available (printed, not gates); spin 101.4 ms; flush 53.6 ms; on AC Power with Low Power Mode off
run 1  checks    spin: 2^27 rounds of arithmetic on one thread, least of 3. flush: 20 writes of 136,000 bytes to a file in the work directory, each followed by sync_all, least of 3. Neither touches kibbr
run 1  plan      engine::plan on a trie of 10,000 templates, 57,642 nodes, 98,495 tokens, in a process of its own, one thread; p50 of 100 calls after 5 unmeasured (p99; leaves)
run 1  plan      P1 promoted value 4.58 (4.90; 1,360)   P2 unpromoted key 16.2 (17.0; 4,460)   P3 one id 14.4 (15.1; 1,430)   P4 newest 500 17.9 (18.8; 10,000)
run 1  ingest    server pid 40390 on a verified copy (36,423,735 bytes, 4.00 ms), ready 0.20 s after spawn; no query is sent to this server
run 1  ingest    the server's ingest.lines_ingested went 0 -> 1,000,000 while its uptime_seconds went 0.412 -> 11.381: 1,000,000 lines / 10.969 s = 91,166 lines/s
run 1  ingest    senders: 500 bodies answered 200, their `ingested` fields sum to 1,000,000 (the two counts agree); refused 503 x 0, 429 x 0 (0 of them before the body was all written); attempts with no answer 0; reconnects 0; short bodies 0; unknown outcomes 0; the same window on this process's clock 10.971 s
run 1  ingest    server at the end: 290,412 rows buffered in 735,032,772 bytes of a 536,870,912-byte limit; forced closes 3 (the limit was reached); requests shed 0; lines rejected 0; store writes failed 0; log 0 lines ahead of the store; templates 145 -> 151; health ok; cpu 51.2 s (4.67 cores busy); resident 1,402 MiB
run 1  ingest    lines acknowledged by the second, this process's clock: 96,000 190,000 286,000 382,000 470,000 548,000 630,000 712,000 800,000 890,000 978,000
run 1  idle      server pid 40131 on a verified copy (36,423,735 bytes, 4.00 ms), ready 0.21 s after spawn; no line is sent to this server
run 1  scan      433,520 rows hold status x 25 asks of COUNT LOG {"status":"200"}, back to back after 3 unmeasured = 10,838,000 rows / 6.412 s of CPU the server process used over them (user 6.29 + system 0.12, read from the OS for pid 40131; the whole process: HTTP, the plan and 2 file opens an ask are in it) = 1,690,268 rows per CPU-second
run 1  scan      the same block by the wall clock: 6.428 s, 1.00 cores busy, 1,686,061 rows per wall-second; the server says it scanned 10,838,000; in the 100 ms after the block, nothing asked, it used 0.000 s of CPU
run 1  idle      100 asks of each query after 3 unmeasured; client p50 p99 (min max) | the server's own duration_ms p50 p99 | the answer | CPU the server used over the 100
run 1  idle S1   1.52  2.10  (1.44  3.90) | 1.31  1.80 | count 11,915  scanned 11,915  1 template  2 row groups  unlimited | 0.151 CPU-s in 0.153 s
run 1  idle S2   45.8  49.2  (44.9  50.3) | 45.5  48.9 | count 28,907  scanned 121,207  61 templates  74 row groups  unlimited | 4.580 CPU-s in 4.612 s
run 1  idle S3   131.0  139.0  (128.0  141.0) | 130.0  138.0 | count 2, 2 rows  scanned 76,100  37 templates  46 row groups  backstop, not truncated | 13.02 CPU-s in 13.14 s
run 1  idle S4   1,842  1,903  (1,811  1,960) | 1,838  1,899 | count 1,000,000, 500 rows  scanned 829,670  145 templates  199 row groups  caller | 183.9 CPU-s in 184.6 s
run 1  idle      beside the scan rate, not in the table: S2's own block, 121,207 rows hold level x 100 asks / 4.580 s of CPU = 2,646,441 rows per CPU-second, held down by 61 file opens an ask; S1 7,890,728; S3 584,485; S4 451,153 (rows the server says it scanned / CPU-seconds)
run 1  loaded    100 laps. A lap: a new copy, a new server, S1 S2 S3 once unmeasured, 4 senders flat out from their first body; each query asked once when 20,000 / 30,000 / 70,000 / 140,000 live lines are acknowledged; kill
run 1  loaded    a query reads one snapshot taken before its scan: `over the loaded answer` is what its count held of the live stream; lines acknowledged while it ran are load beside it, not rows it read
run 1  loaded S1 4.10  9.80  (2.20  11.4) | 3.60  9.10 | asked at 20,000 to 26,000 lines; counted 232 to 301 over the loaded answer; 0 late
run 1  loaded S2 96.0  131.0  (88.1  140.0) | 95.2  130.0 | asked at 30,000 to 36,000 lines; counted 861 to 1,040 over the loaded answer; 0 late
run 1  loaded S3 180.0  236.0  (166.0  244.0) | 179.0  235.0 | asked at 70,000 to 76,000 lines; counted 0 to 0 over the loaded answer; 0 late
run 1  loaded S4 2,650  3,120  (2,480  3,190) | 2,644  3,113 | asked at 140,000 to 148,000 lines; counted 140,000 to 148,000 over the loaded answer; 0 late
run 1  loaded    0 laps ran past the end of the stream; bodies refused 0 times; the buffer limit was reached in 97 of 100 laps (forced closes above 0 when the lap ended); ingest with the queries running 176,400 lines/s (median over laps of the server's lines over its uptime between two /stats reads; not in the table)
run 1  end       0 units thrown away; spin 102.0 ms; flush 54.1 ms; on AC Power with Low Power Mode off; the binary is the one that started; 11 min 02 s"##
    );
}

#[test]
fn a_run_that_waited_was_disturbed_or_was_not_checked_says_so() {
    let mut run = the_first_run();
    let file = baseline().file;
    run.checks.waited_s = 20;
    run.checks.waited_for = vec!["another kibbr process".to_string()];
    run.checks.swapouts_before = None;
    run.checks.load_average = None;
    // A machine on its battery, holding its cores back.
    run.checks.power_before = Some(power("Battery Power", true));
    let lines = checks_lines(2, &run.checks, &file.flags);
    assert_eq!(
        lines,
        [
            "",
            "run 2  checks    waited 20 s for another kibbr process; paused 20 s; no other \
                 kibbr process; this system has no swap counter (not a gate); load average \
                 n/a and 3.8 GiB available (printed, not gates); spin 101.4 ms; flush 53.6 \
                 ms; on Battery Power with Low Power Mode on",
        ],
        "and what the references are is said in the first run only"
    );
    let mut unchecked = file.flags.clone();
    unchecked.checked = false;
    assert_eq!(
        checks_lines(1, &Checks::default(), &unchecked),
        [
            "",
            "run 1  checks    none (--unchecked): no pause, no wait for a quiet machine, no \
                 spin or flush reference, and nothing is looked at after a unit",
        ]
    );

    let lap = Voided {
        unit: "lap 41".to_string(),
        reason: "1,204 pages were swapped out during it".to_string(),
    };
    assert_eq!(
            voided_line(2, &lap, 10),
            "run 2  loaded    lap 41 thrown away: 1,204 pages were swapped out during it; waited 10 s; done again"
        );
    let ingest = Voided {
        unit: "ingest".to_string(),
        reason: "another kibbr process was alive (pid 36231)".to_string(),
    };
    assert_eq!(
        voided_line(3, &ingest, 0),
        "run 3  ingest    this unit thrown away: another kibbr process was alive (pid \
             36231); waited 0 s; done again"
    );

    run.voided = vec![lap];
    run.checks.spin_ms_after = None;
    run.checks.flush_ms_after = None;
    run.checks.power_after = None;
    run.seconds = 48.4;
    assert_eq!(
        end_line(&run),
        "run 1  end       1 unit thrown away; spin n/a ms; flush n/a ms; power n/a; the \
             binary is the one that started; 48 s"
    );
    // A system that gave one half of the reading: the other is not made up.
    run.checks.power_before = Some(Power {
        source: None,
        low_power_mode: Some(false),
    });
    assert!(checks_lines(2, &run.checks, &file.flags)[1]
        .ends_with("; flush 53.6 ms; on n/a with Low Power Mode off"));
    run.checks.power_before = Some(Power {
        source: Some("AC Power".to_string()),
        low_power_mode: None,
    });
    assert!(checks_lines(2, &run.checks, &file.flags)[1]
        .ends_with("; flush 53.6 ms; on AC Power with Low Power Mode n/a"));
    assert_eq!(spent(59.6), "1 min 00 s");
    assert_eq!(spent(3_725.0), "62 min 05 s");
}

#[test]
fn a_number_the_server_or_the_system_did_not_give_is_printed_as_not_available() {
    let mut run = the_first_run();
    let file = baseline().file;

    run.ingest.at_end = AtEnd::default();
    run.ingest.cpu_s = None;
    run.ingest.cores_busy = None;
    run.ingest.resident_bytes_at_end = None;
    run.ingest.counters_agree = false;
    run.ingest.acked_lines = 998_000;
    run.ingest.refused.s503 = 12;
    run.ingest.refused.before_body_end = 3;
    run.ingest.retry_after_max_s = Some(1);
    run.ingest.short_bodies = 1;
    run.ingest.short_lines = 2_000;
    run.ingest.window_s_bench = 11.3;
    run.ingest.health_at_end = Health {
        status: "degraded".to_string(),
        detail: Some("the disk is full".to_string()),
    };
    let lines = ingest_lines(1, &run.ingest);
    assert_eq!(
        lines[2],
        "run 1  ingest    senders: 500 bodies answered 200, their `ingested` fields sum to \
             998,000 (THE TWO COUNTS DIFFER: the server counted 1,000,000); refused 503 x 12, \
             429 x 0 (3 of them before the body was all written); attempts with no answer 0; \
             reconnects 0; short bodies 1 (2,000 lines not taken); unknown outcomes 0; the \
             same window on this process's clock 11.300 s, 3.0% from the server's; the \
             longest Retry-After was 1 s, recorded and not obeyed"
    );
    assert_eq!(
        lines[3],
        "run 1  ingest    server at the end: n/a rows buffered in n/a bytes of a n/a-byte \
             limit; forced closes n/a; requests shed n/a; lines rejected n/a; store writes \
             failed n/a; log n/a lines ahead of the store; templates n/a -> n/a; health \
             degraded (the disk is full); cpu n/a (n/a cores busy); resident n/a"
    );
    assert_eq!(
        lines[4],
        "run 1  ingest    n/a above: /stats did not give storage.buffered_rows, \
             storage.buffered_bytes, storage.max_buffer_bytes, storage.forced_closes, \
             ingest.requests_shed, ingest.lines_rejected, storage.write_failures, \
             storage.wal.last_seq or storage.wal.applied, trie.templates"
    );

    // A scan block the system gave no CPU time for, and one it gave none over.
    run.scan.cpu_s = None;
    run.scan.rows_per_cpu_s = None;
    run.scan.cores_busy = None;
    let lines = idle_lines(1, &run.idle, &run.scan, &file.queries, &file.flags);
    assert!(
        lines[1].ends_with(
            "= 10,838,000 rows / n/a: the system gave no CPU time for pid 40131 = n/a \
                 rows per CPU-second"
        ),
        "{}",
        lines[1]
    );
    assert!(lines[2].contains("6.428 s, n/a cores busy, 1,686,061 rows per wall-second"));
    run.scan.cpu_s = Some(0.0);
    let lines = idle_lines(1, &run.idle, &run.scan, &file.queries, &file.flags);
    assert!(
        lines[1].ends_with("are in it) = n/a (the OS reported no CPU time over the block)"),
        "{}",
        lines[1]
    );

    run.loaded.laps_at_buffer_limit = None;
    run.loaded.lines_per_s_median = None;
    run.loaded.wrapped_laps = 1;
    run.loaded.queries.s4.late = 3;
    run.loaded.queries.s4.refused = 1;
    // One body that was shed for a second: twenty refusals of it.
    run.loaded.refused_bodies = 20;
    let lines = loaded_lines(1, &run.loaded, &file.flags);
    assert!(
        lines[3].ends_with("; 3 late | refused 1 time and sent again"),
        "{}",
        lines[3]
    );
    assert!(
        lines[4].starts_with(
            "run 1  loaded    1 lap ran past the end of the stream; bodies refused 20 \
                 times; the buffer limit: n/a (/stats did not give storage.forced_closes in \
                 every lap); ingest with the queries running n/a lines/s ("
        ),
        "{}",
        lines[4]
    );
    assert_eq!(
        lines[5],
        "run 1  loaded    late: asked more than one round of bodies (8,000 lines) past its \
             place, because the query before it was still running"
    );
    assert_eq!(
        lines.len(),
        6,
        "and no word of a store that wrote: {lines:#?}"
    );

    // Laps whose server said its store had failed to write, by the count or the flag.
    run.loaded.laps = 4;
    run.loaded.lap.write_failures = vec![Some(0), Some(3), None, Some(0)];
    run.loaded.lap.degraded = vec![Some(false), Some(false), None, Some(true)];
    let lines = loaded_lines(1, &run.loaded, &file.flags);
    assert_eq!(
        lines[5],
        "run 1  loaded    2 of 4 laps ran on a store that could not write (in the lap's \
             last /stats, storage.write_failures above 0 or storage.degraded): their servers \
             acknowledged lines while no row reached a file"
    );
}

#[test]
fn a_work_directory_left_behind_is_named_and_nothing_more() {
    let mut printed = Vec::new();
    let mut log = Log::new(&mut printed);
    log_stale(&mut log, &[]).expect("nothing");
    assert_eq!(log.text(), "");
    log_stale(
        &mut log,
        &[PathBuf::from("target/scratch/bench/work-501.noindex")],
    )
    .expect("a line");
    assert_eq!(
        log.text(),
        "left behind  target/scratch/bench/work-501.noindex: the work directory of a bench that \
             did not finish; not removed, and nothing in it is used\n"
    );
}

#[test]
fn a_lap_is_marked_as_each_quarter_of_them_ends() {
    let marks = |laps| {
        (0..=laps + 1)
            .filter(|lap| marked(*lap, laps))
            .collect::<Vec<_>>()
    };
    assert_eq!(marks(100), [25, 50, 75, 100]);
    assert_eq!(marks(10), [3, 5, 8, 10]);
    assert_eq!(marks(2), [1, 2]);
    assert_eq!(marks(1), [1]);
    assert_eq!(marks(0), [0usize; 0]);

    let mut printed = Vec::new();
    let mut log = Log::new(&mut printed);
    for lap in 1..=100 {
        log_lap(&mut log, 1, lap, 100).expect("a line");
    }
    assert_eq!(
        log.text(),
        "run 1  loaded    lap 25 of 100\nrun 1  loaded    lap 50 of 100\nrun 1  loaded    \
             lap 75 of 100\nrun 1  loaded    lap 100 of 100\n"
    );
}

#[test]
fn the_summary_is_every_run_and_then_the_median() {
    let mut printed = Vec::new();
    let mut log = Log::new(&mut printed);
    log_summary(&mut log, &baseline().file).expect("printed");
    assert_eq!(
        log.text(),
        r##"
every run, then the median                     run 1        run 2        run 3       median
ingest lines/s                                91,166       89,802       93,410       91,166
idle S1 p50 ms                                  1.52         1.55         1.49         1.52
idle S1 p99 ms                                  2.10         2.31         2.04         2.10
idle S2 p50 ms                                  45.8         46.1         45.6         45.8
idle S2 p99 ms                                  49.2         52.0         48.8         49.2
idle S3 p50 ms                                 131.0        133.0        130.0        131.0
idle S3 p99 ms                                 139.0        146.0        138.0        139.0
idle S4 p50 ms                                 1,842        1,861        1,838        1,842
idle S4 p99 ms                                 1,903        1,950        1,899        1,903
loaded S1 p50 ms                                4.10         4.30         4.00         4.10
loaded S1 p99 ms                                9.80         12.1         9.20         9.80
loaded S2 p50 ms                                96.0         98.2         95.1         96.0
loaded S2 p99 ms                               131.0        142.0        129.0        131.0
loaded S3 p50 ms                               180.0        184.0        178.0        180.0
loaded S3 p99 ms                               236.0        251.0        231.0        236.0
loaded S4 p50 ms                               2,650        2,702        2,633        2,650
loaded S4 p99 ms                               3,120        3,300        3,090        3,120
scan rows per CPU-second                   1,690,268    1,671,000    1,702,300    1,690,268
scan rows per wall-second (not a row)      1,686,061    1,668,200    1,698,900    1,686,061
scan cores busy (not a row)                     1.00         1.00         1.00         1.00
plan P1 p50 ms                                  4.58         4.61         4.55         4.58
plan P2 p50 ms                                  16.2         16.4         16.1         16.2
plan P3 p50 ms                                  14.4         14.5         14.4         14.4
plan P4 p50 ms                                  17.9         18.0         17.8         17.9
spin ms, before the run (not a row)            101.4        101.9        101.6        101.6
flush ms, before the run (not a row)            53.6         54.9         53.2         53.6

"##
    );

    // A number one run could not give is not available in its place or as a median.
    let mut file = baseline().file;
    file.runs[1].scan.rows_per_cpu_s = None;
    file.per_run = per_run(&file.runs);
    file.medians = medians(&file.per_run);
    assert!(summary_lines(&file).contains(
            &"scan rows per CPU-second                   1,690,268          n/a    1,702,300          n/a"
                .to_string()
        ));
}

#[test]
fn the_baseline_a_result_records_says_whether_the_bench_was_the_same() {
    let baseline = baseline();
    let same = baseline_ref(&baseline, &baseline.file.bench);
    assert_eq!(
        same,
        BaselineRef {
            file: "target/scratch/bench/baseline.json".to_string(),
            sha: Some(BASELINE_SHA.to_string()),
            label: "baseline".to_string(),
            same_bench: true,
        }
    );
    let other = BenchSource {
        source_crc32c: "77c1f0b2".to_string(),
        source_files: 8,
    };
    assert!(!baseline_ref(&baseline, &other).same_bench);
}
