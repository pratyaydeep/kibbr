// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[test]
fn a_place_is_a_whole_line_count_and_the_same_one_floating_point_would_miss() {
    assert_eq!(
        place_lines(1_000_000),
        [20_000, 30_000, 70_000, 140_000],
        "the measurement's four places"
    );
    assert_eq!(place_lines(2_000), [40, 60, 140, 280]);
    // A share that is not a whole line waits for the next one.
    assert_eq!(place_lines(33), [1, 1, 3, 5]);
    for (place, share) in LOAD_AT_PERCENT.iter().zip(LOAD_AT) {
        assert_eq!(*place as f64 / 100.0, share, "the two spellings of a place");
    }
}

#[test]
fn a_small_run_has_fewer_unmeasured_asks_and_one_scan_ask_at_least() {
    assert_eq!((warmup(100), plan_warmup(100), scan_asks(100)), (3, 5, 25));
    assert_eq!((warmup(1), plan_warmup(1), scan_asks(1)), (1, 1, 1));
    assert_eq!((warmup(4), plan_warmup(4), scan_asks(7)), (3, 4, 1));
}

#[test]
fn the_seven_texts_are_the_ones_a_saved_result_is_compared_on() {
    let queries = queries("20834abd-1e3d-462e-b89b-fb1d4643e6bb");
    let texts: Vec<&str> = queries.each().iter().map(|q| q.text.as_str()).collect();
    assert_eq!(
        texts,
        [
            r#"COUNT LOG {"event":"login_ok"}"#,
            r#"COUNT LOG {"level":"info"}"#,
            r#"MATCH LOG {"job.id":"20834abd-1e3d-462e-b89b-fb1d4643e6bb"}"#,
            "MATCH LOG {} LIMIT 500",
            r#"COUNT LOG {"level": *}"#,
            r#"COUNT LOG {"status":"200"}"#,
            r#"COUNT LOG {"status": *}"#,
        ]
    );
}

#[test]
fn an_error_quotes_the_end_of_what_a_child_wrote() {
    assert_eq!(tail("a\nb\nc\n", 2), "b\nc");
    assert_eq!(tail("a\nb", 20), "a\nb");
    assert_eq!(tail("", 20), "");
}

#[test]
fn an_error_says_what_was_met_in_one_sentence() {
    assert_eq!(
        SpeedError::Profile.to_string(),
        "kibbr-bench measures the farm profile only"
    );
    assert_eq!(
        SpeedError::OutExists(PathBuf::from("target/scratch/bench/baseline.json")).to_string(),
        "target/scratch/bench/baseline.json exists; a saved result is not written over"
    );
    // True of a run, which saved its result, and of `--table`, which saves none.
    assert_eq!(
        SpeedError::Failed { lines: 2 }.to_string(),
        "the table above carries 2 FAIL line(s)"
    );
    // Either gate can be the reason, and the advice fits both.
    let disturbed = |reason: &str| {
        SpeedError::Disturbed {
            run: 2,
            units: 6,
            reason: reason.to_string(),
        }
        .to_string()
    };
    assert_eq!(
        disturbed("another kibbr process was alive when it ended (pid 36231)"),
        "run 2: 6 of its units were disturbed and thrown away, the last because another \
             kibbr process was alive when it ended (pid 36231). Nothing was saved: stop what \
             runs beside the bench, or make room if the machine swapped, and start again"
    );
    assert!(disturbed("1,204 pages were swapped out during it")
        .ends_with("or make room if the machine swapped, and start again"));

    let child = |code| {
        SpeedError::Child {
            what: "kibbr gen-logs",
            code,
            stderr: "999 lines where 1,000 were asked for".to_string(),
        }
        .to_string()
    };
    assert_eq!(
        child(Some(0)),
        "`kibbr gen-logs`, which the bench runs as a child, exited 0 without doing what \
             was asked of it: 999 lines where 1,000 were asked for"
    );
    assert!(child(Some(1)).contains("a child, exited 1: "));
    assert!(child(None).contains("a child, was ended by a signal: "));

    let sender = |sender| {
        SpeedError::Sender {
            sender,
            detail: "five attempts in a row read no answer".to_string(),
        }
        .to_string()
    };
    assert_eq!(
        sender(0),
        "ingest sender 1 of 4: five attempts in a row read no answer"
    );
    assert!(sender(SENDERS).starts_with("the 4 ingest senders: "));

    let start = SpeedError::ServerStart {
        log: PathBuf::from("work/server-1.log"),
        tail: "kibbr: cannot listen".to_string(),
    }
    .to_string();
    assert!(start.contains("within 120 s"), "{start}");
    assert!(start.ends_with("kept in work/server-1.log and ends:\nkibbr: cannot listen"));
}

/// A directory for one test's files, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("kibbr-speed-mod-{}-{name}", std::process::id()));
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
fn a_work_directory_that_is_already_there_is_an_earlier_bench_s_and_is_refused() {
    let scratch = Scratch::new("work");
    let work = corpus::work_dir(&scratch.0);
    make_work(&work).expect("made");
    assert!(work.is_dir());

    // What a bench that did not end well left in it: the only copy of its result.
    fs::write(work.join("unsaved.json"), "an earlier result").expect("a file");
    let refused = make_work(&work).expect_err("it is someone's");
    assert!(matches!(refused, SpeedError::Host(_)), "{refused:?}");
    let said = refused.to_string();
    assert!(
        said.starts_with(&format!(
            "{} is already there: the work directory of an earlier bench",
            work.display()
        )) && said
            .ends_with("Move it away and run again; the bench removes nothing it did not make"),
        "{said}"
    );
    assert_eq!(
        fs::read_to_string(work.join("unsaved.json")).expect("still there"),
        "an earlier result"
    );

    // A place it cannot be made in is that place's error, by path.
    let nowhere = scratch.0.join("not-there").join("work-1.noindex");
    let error = make_work(&nowhere).expect_err("no parent");
    assert!(
        matches!(&error, SpeedError::Io { path, .. } if *path == nowhere),
        "{error:?}"
    );
}

fn a_result() -> ResultFile {
    ResultFile {
        schema: report::SCHEMA.to_string(),
        label: "baseline".to_string(),
        ..ResultFile::default()
    }
}

#[test]
fn a_result_that_cannot_be_saved_where_it_was_told_is_kept_in_the_work_directory() {
    let scratch = Scratch::new("unsaved");
    let work = scratch.0.join("work-1.noindex");
    fs::create_dir_all(&work).expect("the work directory");
    let out = scratch.0.join("results").join("baseline.json");
    let first = SpeedError::OutExists(out.clone());
    let file = a_result();

    let mut said = Vec::new();
    keep_unsaved(&work, &out, &file, "the log\n", &first, &mut said);
    let kept = work.join("unsaved.json");
    assert_eq!(
        String::from_utf8(said).expect("text"),
        format!(
            "kibbr: the result could not be saved to {}; it is in {}, with its log beside \
                 it\n",
            out.display(),
            kept.display()
        )
    );
    assert_eq!(report::read(&kept).expect("a whole result").file, file);
    assert_eq!(
        fs::read_to_string(work.join("unsaved.log")).expect("its log"),
        "the log\n"
    );
}

#[test]
fn a_result_that_can_be_kept_nowhere_is_said_with_both_reasons_and_printed_whole() {
    let scratch = Scratch::new("nowhere");
    let out = scratch.0.join("baseline.json");
    let first = SpeedError::OutExists(out.clone());
    let file = a_result();

    // A work directory that is not there: nothing can be kept in it.
    let work = scratch.0.join("gone").join("work-1.noindex");
    fs::write(scratch.0.join("gone"), "a file where a directory is wanted").expect("a file");
    let mut said = Vec::new();
    keep_unsaved(&work, &out, &file, "the log\n", &first, &mut said);
    let said = String::from_utf8(said).expect("text");
    let kept = work.join("unsaved.json");
    assert!(
        said.starts_with(&format!(
            "kibbr: the result could not be saved to {}, nor kept in {}: ",
            out.display(),
            kept.display()
        )),
        "{said}"
    );
    // The second write's own reason is on that first line, and then the result, as
    // it would have been saved.
    let (reason, rest) = said.split_once('\n').expect("more than one line");
    assert!(
        reason.contains(&scratch.0.join("gone").display().to_string()),
        "{reason}"
    );
    let text = report::result_text(&file).expect("a result is JSON");
    assert!(
        rest.starts_with("kibbr: so that what was measured is not lost, the result follows")
            && rest.ends_with(&format!("\n----------\n{text}----------\n")),
        "{rest}"
    );
    let between: ResultFile = serde_json::from_str(&text).expect("it reads as a result");
    assert_eq!(between, file);

    // A result that would not read back is on the disk under its `.tmp` name, by
    // whichever write met it: it is named, and not printed.
    let work = scratch.0.join("work-2.noindex");
    fs::create_dir_all(&work).expect("the work directory");
    let mut unreadable = a_result();
    unreadable.medians.ingest_lines_per_s = f64::NAN;
    let mut said = Vec::new();
    keep_unsaved(&work, &out, &unreadable, "the log\n", &first, &mut said);
    let said = String::from_utf8(said).expect("text");
    assert_eq!(said.lines().count(), 1, "{said}");
    assert!(
        said.contains("unsaved.json.tmp")
            && said.contains("what was measured is kept in this file"),
        "{said}"
    );
    assert!(work.join("unsaved.json.tmp").exists());
}

#[test]
fn a_label_is_the_one_given_or_says_which_side_of_a_comparison_this_is() {
    let mut args = Args {
        kibbr: None,
        lines: 1,
        out: None,
        baseline: None,
        label: None,
        dir: PathBuf::from(DEFAULT_DIR),
        runs: DEFAULT_RUNS,
        samples: DEFAULT_SAMPLES,
        body_lines: DEFAULT_BODY_LINES,
        threads: None,
        unchecked: false,
    };
    assert_eq!(label(&args), "baseline");
    args.baseline = Some(PathBuf::from("baseline.json"));
    assert_eq!(label(&args), "this run");
    args.label = Some("after 1 (columns)".to_string());
    assert_eq!(label(&args), "after 1 (columns)");
}
