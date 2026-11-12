// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The two benches, from outside the process: `kibbr bench-storage` (`storage`) and
//! `kibbr-bench` (`speed`). Their numbers are a machine's, so what is held is what must be
//! true of them on any machine. `speed` is built with `--features bench` only, as
//! `kibbr-bench` is: it is also the slowest module in the suite.

mod storage {
    //! `kibbr bench-storage`, from outside the process: its numbers are a disk's, so what is
    //! held here is what must be true of them on any disk.

    use std::fs;

    use crate::harness::*;

    /// Farm lines, one day: more than a day has flushes, and few enough that either door
    /// stores each template as one row group.
    const LINES: u64 = 1_500;

    /// The flushes in a day that is flushed once a minute.
    const FLUSHES: u64 = 1_440;

    /// The names down the left of a `kibbr total` block, after its first.
    const PARTS: &[&str] = &[
        "data files",
        "column data",
        "row group headers",
        "schema records",
        "footers",
        "file headers",
        "catalog.log",
        "manifest.log",
    ];

    /// A line with every count in it replaced by `N`, and the counts in the order they stood.
    /// `1,234` is one count; `2.5s` is two, around the point.
    fn shape(line: &str) -> (String, Vec<u64>) {
        let mut out = String::new();
        let mut counts = Vec::new();
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            if !c.is_ascii_digit() {
                out.push(c);
                continue;
            }
            let mut digits = String::from(c);
            while let Some(&next) = chars.peek() {
                let grouped = next == ',' && {
                    let mut ahead = chars.clone();
                    ahead.next();
                    ahead.peek().is_some_and(char::is_ascii_digit)
                };
                if next.is_ascii_digit() {
                    digits.push(next);
                } else if !grouped {
                    break;
                }
                chars.next();
            }
            counts.push(digits.parse().expect("a count"));
            out.push('N');
        }
        (out, counts)
    }

    /// The table rows from the one named `first` up to the next blank line: each row's name,
    /// and its bytes.
    fn block(lines: &[&str], first: &str) -> Vec<(String, u64)> {
        let at = lines
            .iter()
            .position(|line| line.trim_start().starts_with(first))
            .unwrap_or_else(|| panic!("no `{first}` row in:\n{}", lines.join("\n")));
        lines[at..]
            .iter()
            .take(1 + PARTS.len())
            .map(|line| {
                let name = line[2..26].trim().to_string();
                let bytes = line[26..40].trim().replace(',', "");
                (name, bytes.parse().expect("a row's bytes"))
            })
            .collect()
    }

    #[test]
    fn a_live_day_is_measured_before_and_after_compaction_and_is_smaller_after() {
        let scratch = Scratch::new("live-day");
        let input = scratch.0.join("farm.ndjson");
        let farm = kibbr()
            .args(["gen-logs", "--profile", "farm", "--seed", "42"])
            .args(["--lines", &LINES.to_string()])
            .output()
            .expect("failed to run gen-logs");
        assert!(farm.status.success(), "gen-logs failed");
        fs::write(&input, farm.stdout).expect("write the input");

        let ran = kibbr()
            .arg("bench-storage")
            .arg(&input)
            .arg("--scratch")
            .arg(scratch.0.join("scratch"))
            // One file a day at either door, whatever the caller's environment caps a file at.
            .env_remove("KIBBR__STORAGE__SEGMENT_MAX_BYTES")
            .output()
            .expect("failed to run bench-storage");
        let stderr = String::from_utf8_lossy(&ran.stderr);
        assert!(ran.status.success(), "bench-storage failed:\n{stderr}");
        let stdout = String::from_utf8(ran.stdout).expect("stdout was not utf-8");
        let lines: Vec<&str> = stdout.lines().collect();

        // ── The bulk door's table, as it was before there was a second ──
        let (loaded, bulk) = shape(lines[1]);
        assert!(
            loaded.starts_with("  N lines, N rows as load ")
                && loaded.contains(": N row groups in N file(s) over N day(s), loaded in N.Ns"),
            "the load's line is {:?}",
            lines[1]
        );
        assert_eq!(&bulk[..2], [LINES, LINES], "{:?}", lines[1]);
        // The load's id is hexadecimal, so its digits are counts here too: the row groups are
        // counted back from the end - files, days and the two halves of the seconds after it.
        let bulk_row_groups = bulk[bulk.len() - 5];
        let first = block(&lines, "kibbr total ");
        assert_eq!(
            first
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            [&["kibbr total"], PARTS].concat()
        );

        // ── The live day's two lines ──
        let title = lines
            .iter()
            .position(|line| line.starts_with("the same lines as a live day: "))
            .unwrap_or_else(|| panic!("no live day in:\n{stdout}"));
        assert_eq!(lines[title - 1], "", "no blank line before the live day");
        assert_eq!(
            lines[title],
            "the same lines as a live day: flushed 1,440 time(s) over the file, as a day that \
         arrives evenly is flushed once a minute, then compacted"
        );
        let (before, was) = shape(lines[title + 1]);
        assert_eq!(
            before,
            "  before compaction: N lines, N rows, N row groups in N file(s) over N day(s), \
         ingested in N.Ns"
        );
        let (after, is) = shape(lines[title + 2]);
        assert_eq!(
        after,
        "  after compaction: N rows, N row groups in N file(s) over N day(s), compacted in N.Ns"
    );
        let (was_rows, was_row_groups, was_files, was_days) = (was[1], was[2], was[3], was[4]);
        let (is_rows, is_row_groups, is_files, is_days) = (is[0], is[1], is[2], is[3]);
        assert_eq!(was[0], LINES);
        assert_eq!((was_rows, is_rows), (LINES, LINES), "a row was lost");
        assert_eq!((was_days, is_days), (1, 1));
        assert_eq!((was_files, is_files), (1, 1));
        // A flush that took a row leaves a row group at least, and no row is in two: flushed
        // a line or two at a time, a row group is a row or two; compacted, it is a template.
        assert!(
            (FLUSHES..=LINES).contains(&was_row_groups),
            "{was_row_groups} row groups before compaction, from {FLUSHES} flushes"
        );
        assert_eq!(
            is_row_groups, bulk_row_groups,
            "compaction did not leave the day in the row groups the bulk door cuts it into"
        );

        // ── Its two blocks, and the line that compares them ──
        let before = block(&lines, "kibbr total, before");
        let after = block(&lines, "kibbr total, after");
        for (block, total) in [
            (&before, "kibbr total, before"),
            (&after, "kibbr total, after"),
        ] {
            assert_eq!(
                block
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                [&[total], PARTS].concat()
            );
            let bytes = |name: &str| block.iter().find(|(n, _)| n == name).map_or(0, |(_, b)| *b);
            assert_eq!(
                bytes("data files"),
                PARTS[1..6].iter().map(|part| bytes(part)).sum::<u64>(),
                "{total}: the data files' parts do not add up to them"
            );
            assert_eq!(
                bytes(total),
                bytes("data files") + bytes("catalog.log") + bytes("manifest.log"),
                "{total}: the total is not its three parts"
            );
        }
        let (total_before, total_after) = (before[0].1, after[0].1);
        assert!(
            total_after < total_before,
            "{total_after} bytes after compaction, {total_before} before"
        );
        // Row group headers are what compaction is for: one a template, where there was one a
        // flush.
        assert!(
            after[3].1 * 4 < before[3].1,
            "row group headers barely shrank"
        );
        // And it leaves the day as the bulk door stores it: the same row groups, encoded at
        // the level a finished file is, so the same column data to the byte.
        assert_eq!(
            after[2].1, first[2].1,
            "compaction's column data is not the bulk door's: not written at the finished level"
        );
        assert_eq!(
            lines.last().copied(),
            Some(
                format!(
                "  after compaction is {:.3} of before in bytes, 1.000 in files and {:.3} in row \
                 groups",
                total_after as f64 / total_before as f64,
                is_row_groups as f64 / was_row_groups as f64
            )
                .as_str()
            )
        );

        assert!(!scratch.0.join("scratch").exists());
    }

    #[test]
    fn no_zstd_leaves_zstd_out_and_measures_the_rest() {
        let scratch = Scratch::new("no-zstd");
        let input = scratch.0.join("farm.ndjson");
        let farm = kibbr()
            .args([
                "gen-logs",
                "--profile",
                "farm",
                "--seed",
                "42",
                "--lines",
                "200",
            ])
            .output()
            .expect("failed to run gen-logs");
        assert!(farm.status.success(), "gen-logs failed");
        fs::write(&input, farm.stdout).expect("write the input");

        let ran = kibbr()
            .arg("bench-storage")
            .arg(&input)
            .arg("--no-zstd")
            .arg("--scratch")
            .arg(scratch.0.join("scratch"))
            .output()
            .expect("failed to run bench-storage");
        let stderr = String::from_utf8_lossy(&ran.stderr);
        assert!(ran.status.success(), "bench-storage failed:\n{stderr}");
        let stdout = String::from_utf8(ran.stdout).expect("stdout was not utf-8");

        assert!(
            !stdout.lines().any(|l| l.starts_with("  zstd -19 ")),
            "a zstd row under --no-zstd:\n{stdout}"
        );
        assert!(
            stdout.lines().any(|l| l.starts_with("  gzip -9 ")),
            "{stdout}"
        );
        assert!(
            stdout.contains("of gzip -9; zstd -19 was not run (--no-zstd)"),
            "{stdout}"
        );
        assert!(stdout.contains("kibbr total, after"), "{stdout}");
        assert!(!scratch.0.join("scratch").exists());
    }

    #[test]
    fn zstd_threads_change_zstd_alone_and_say_how_long_each_took() {
        let scratch = Scratch::new("zstd-threads");
        let input = scratch.0.join("farm.ndjson");
        let farm = kibbr()
            .args([
                "gen-logs",
                "--profile",
                "farm",
                "--seed",
                "42",
                "--lines",
                "200",
            ])
            .output()
            .expect("failed to run gen-logs");
        assert!(farm.status.success(), "gen-logs failed");
        fs::write(&input, farm.stdout).expect("write the input");

        let run = |threads: u64| {
            let ran = kibbr()
                .arg("bench-storage")
                .arg(&input)
                .args(["--zstd-threads", &threads.to_string()])
                .arg("--scratch")
                .arg(scratch.0.join("scratch"))
                .env_remove("KIBBR__STORAGE__SEGMENT_MAX_BYTES")
                .output()
                .expect("failed to run bench-storage");
            let stderr = String::from_utf8_lossy(&ran.stderr);
            assert!(
                ran.status.success(),
                "bench-storage --zstd-threads {threads} failed:\n{stderr}"
            );
            let stdout = String::from_utf8(ran.stdout).expect("stdout was not utf-8");
            let lines: Vec<&str> = stdout.lines().collect();

            // The line scripts/bench/compression.py's TIMES reads: gzip's seconds, zstd's
            // threads and seconds. Not a table row: it does not start with a compressor's name.
            let times = lines
                .iter()
                .find(|line| line.starts_with("  gzip -9 ran on "))
                .unwrap_or_else(|| panic!("no timing line in:\n{stdout}"));
            let (shaped, counts) = shape(times);
            assert_eq!(
                shaped, "  gzip -N ran on N thread in N.Ns; zstd -N on N thread(s) in N.Ns",
                "{times:?}"
            );
            assert_eq!(counts[5], threads, "{times:?}");

            let kibbr = ["kibbr total ", "kibbr total, before", "kibbr total, after"]
                .map(|first| block(&lines, first));
            assert!(!scratch.0.join("scratch").exists());
            kibbr
        };

        // zstd -19's own bytes may differ with its threads; kibbr's may not.
        assert_eq!(run(1), run(4), "kibbr's bytes moved with zstd's threads");
    }
}

#[cfg(feature = "bench")]
mod speed {
    //! `kibbr-bench`, from outside the process. Its numbers are a machine's, so nothing here
    //! holds a timing to more than "positive": what is held is what must be true on any machine.
    //! Every run is `--unchecked`: the test binary is a debug build, and other tests' servers
    //! are alive beside it, which a checked run would wait for and then be disturbed by.

    use std::fs;
    use std::path::Path;
    use std::process::{Command, Output};

    use serde_json::Value;

    use crate::harness::*;

    /// The bench binary, with no `KIBBR__` variable of the caller's: the bench takes them out
    /// of its children itself, and the test does not lean on that. It shadows the harness's
    /// `kibbr()`: every command in this module is the bench's.
    fn kibbr() -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr-bench"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("KIBBR__") {
                command.env_remove(name);
            }
        }
        command
    }

    /// Lines in the loaded farm day and in the live stream: few, and still enough that each of
    /// the seven queries matches a row.
    const LINES: u64 = 2_000;

    /// Lines in one `/ingest` body: twenty bodies, five a sender.
    const BODY_LINES: u64 = 100;

    /// The four standard queries, and the four plan shapes, as a result names them.
    const QUERIES: [&str; 4] = ["S1", "S2", "S3", "S4"];
    const SHAPES: [&str; 4] = ["P1", "P2", "P3", "P4"];

    /// `kibbr-bench --lines 2000 --profile farm ... --unchecked --dir <dir>` with `more` after
    /// it.
    fn bench(dir: &Path, runs: usize, samples: usize, more: &[&str]) -> Output {
        kibbr()
            .args(["--profile", "farm", "--unchecked"])
            .args(["--lines", &LINES.to_string()])
            .args(["--body-lines", &BODY_LINES.to_string()])
            .args(["--runs", &runs.to_string()])
            .args(["--samples", &samples.to_string()])
            .arg("--dir")
            .arg(dir)
            .args(more)
            .output()
            .expect("failed to run kibbr-bench")
    }

    /// Strict, as `harness::text` is not: the bench prints only UTF-8.
    fn text(bytes: &[u8]) -> String {
        String::from_utf8(bytes.to_vec()).expect("the output was not utf-8")
    }

    /// A line with every count replaced by `N` and every run of spaces one space: what a line
    /// says, whatever was measured and however wide its columns then are.
    fn shape(line: &str) -> String {
        let mut out = String::new();
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            if c == ' ' {
                if !out.ends_with(' ') {
                    out.push(' ');
                }
                continue;
            }
            if !c.is_ascii_digit() {
                out.push(c);
                continue;
            }
            while let Some(&next) = chars.peek() {
                let joins = (next == ',' || next == '.') && {
                    let mut ahead = chars.clone();
                    ahead.next();
                    ahead.peek().is_some_and(char::is_ascii_digit)
                };
                if !next.is_ascii_digit() && !joins {
                    break;
                }
                chars.next();
            }
            out.push('N');
        }
        out
    }

    /// The table: the last lines of what was printed, from the line that heads it.
    fn table_of(stdout: &str) -> Vec<&str> {
        let lines: Vec<&str> = stdout.lines().collect();
        let at = lines
            .iter()
            .rposition(|line| line.starts_with("kibbr bench   commit "))
            .unwrap_or_else(|| panic!("no table in:\n{stdout}"));
        lines[at..].to_vec()
    }

    fn read_json(path: &Path) -> Value {
        let bytes = fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|e| panic!("{} is not JSON: {e}", path.display()))
    }

    fn number(value: &Value) -> f64 {
        value
            .as_f64()
            .unwrap_or_else(|| panic!("{value} is not a number"))
    }

    fn count(value: &Value) -> u64 {
        value
            .as_u64()
            .unwrap_or_else(|| panic!("{value} is not a count"))
    }

    fn numbers(value: &Value) -> Vec<f64> {
        value
            .as_array()
            .unwrap_or_else(|| panic!("{value} is not a list"))
            .iter()
            .map(number)
            .collect()
    }

    fn counts(value: &Value) -> Vec<u64> {
        value
            .as_array()
            .unwrap_or_else(|| panic!("{value} is not a list"))
            .iter()
            .map(count)
            .collect()
    }

    /// The value at nearest rank `p`: the `ceil(p x n)`-th smallest. Worked out here and not
    /// taken from the crate, so that the rule the result is held to is written twice.
    fn nearest_rank(values: &[f64], p: f64) -> f64 {
        assert!(!values.is_empty(), "no values to rank");
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let rank = ((p * sorted.len() as f64 - 1e-9).ceil() as usize).clamp(1, sorted.len());
        sorted[rank - 1]
    }

    /// The middle of an odd count of values.
    fn middle(values: &[f64]) -> f64 {
        assert!(values.len() % 2 == 1, "{values:?} has no middle");
        nearest_rank(values, 0.50)
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-9 * a.abs().max(b.abs())
    }

    /// Every file under `dir` as the bench lists it - its path from `dir`, its length and its
    /// CRC-32C - less `LOCK` at the root and every name that begins with a dot.
    fn listing(dir: &Path) -> Vec<(String, u64, String)> {
        fn walk(root: &Path, below: &str, found: &mut Vec<(String, u64, String)>) {
            for entry in fs::read_dir(root.join(below)).expect("read a directory") {
                let entry = entry.expect("a directory entry");
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') || (below.is_empty() && name == "LOCK") {
                    continue;
                }
                let from_root = if below.is_empty() {
                    name
                } else {
                    format!("{below}/{name}")
                };
                if entry.file_type().expect("a file type").is_dir() {
                    walk(root, &from_root, found);
                } else {
                    let bytes = fs::read(entry.path()).expect("read a file");
                    found.push((
                        from_root,
                        bytes.len() as u64,
                        format!("{:08x}", crc32c::crc32c(&bytes)),
                    ));
                }
            }
        }
        let mut found = Vec::new();
        walk(dir, "", &mut found);
        found.sort();
        found
    }

    /// A list of files as a marker or a result records it.
    fn recorded(files: &Value) -> Vec<(String, u64, String)> {
        files
            .as_array()
            .expect("a list of files")
            .iter()
            .map(|file| {
                (
                    file["path"].as_str().expect("a path").to_string(),
                    count(&file["bytes"]),
                    file["crc32c"].as_str().expect("a digest").to_string(),
                )
            })
            .collect()
    }

    /// The work directories under `dir`: an invocation's copies, removed when it ends well
    /// and never made by one that is refused before it starts.
    fn work_left(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .expect("read the scratch")
            .map(|entry| {
                entry
                    .expect("an entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| name.starts_with("work-"))
            .collect()
    }

    #[test]
    fn a_bench_run_prints_the_eight_rows_and_saves_every_sample_behind_them() {
        let scratch = Scratch::new("run");
        let dir = scratch.0.as_path();
        let a = dir.join("a.json");
        let b = dir.join("b.json");

        let ran = bench(dir, 3, 1, &["--out", a.to_str().expect("a path")]);
        let stderr = text(&ran.stderr);
        assert!(ran.status.success(), "kibbr-bench failed:\n{stderr}");
        let stdout = text(&ran.stdout);

        // ── The table is the last thing printed: the eight rows, then what is under them ──
        let table = table_of(&stdout);
        let shapes: Vec<String> = table.iter().map(|line| shape(line)).collect();
        assert!(
            shapes[0].starts_with("kibbr bench commit ")
                && shapes[0].ends_with(" farm N lines, median of N runs of N samples"),
            "{:?}",
            table[0]
        );
        assert_eq!(shapes[1], " baseline", "{:?}", table[1]);
        let scan_row = shapes[7].clone();
        assert!(
            scan_row == "scan rows/s/core N" || scan_row == "scan rows/s/core n/a",
            "{:?}",
            table[7]
        );
        // Whether a plan takes over a millisecond is a number's, and what a later build is
        // meant to change: either word is the row.
        let plan_row = shapes[8].clone();
        assert!(
            plan_row == "plan step at N templates: N ms over N ms: yes"
                || plan_row == "plan step at N templates: N ms over N ms: no",
            "{:?}",
            table[8]
        );
        assert_eq!(
            [&shapes[2..7], &shapes[9..11]].concat(),
            [
                "ingest lines/s N",
                "SN promoted value pN / pN N / N ms",
                "SN unpromoted key pN / pN N / N ms",
                "SN one id pN / pN N / N ms",
                "SN newest N pN / pN N / N ms",
                "same four queries under N-sender ingest, pN: SN N SN N SN N SN N ms",
                "scan rows/s/core is rows holding `status` x N asks of COUNT LOG \
             {\"status\":\"N\"} / CPU-seconds of the server process. By the wall clock: N \
             rows/s on N cores.",
            ],
            "the eight rows, less the scan and plan rows, and the note under them, in:\n{}",
            table.join("\n")
        );
        for (line, name) in table[2..10].iter().zip([
            "ingest lines/s  ",
            "S1 promoted value   p50 / p99  ",
            "S2 unpromoted key   p50 / p99  ",
            "S3 one id           p50 / p99  ",
            "S4 newest 500       p50 / p99  ",
            "scan rows/s/core  ",
            "plan step at 10,000 templates:  ",
            "same four queries under 4-sender ingest, p99:  S1 ",
        ]) {
            assert!(line.starts_with(name), "{line:?} is not the row {name:?}");
        }
        let under = &table[11..];
        assert!(
            under.contains(&"UNCHECKED - not a measurement"),
            "under the table:\n{}",
            under.join("\n")
        );
        assert!(under.contains(&"p99 of 1 samples"), "{}", under.join("\n"));

        // ── The result ──
        let result = read_json(&a);
        assert_eq!(result["schema"], "kibbr-bench/1");
        assert_eq!(result["flags"]["checked"], false);
        let runs = result["runs"].as_array().expect("runs");
        assert_eq!(runs.len(), 3);
        for run in runs {
            // Nothing is read after a unit under --unchecked, so the servers of the tests
            // beside this one cannot throw a unit of it away.
            assert_eq!(run["checks"]["after_unit_checks"], false);
            assert_eq!(run["voided"], serde_json::json!([]));
            for name in QUERIES {
                for phase in ["idle", "loaded"] {
                    let ms = numbers(&run[phase][name]["client_ms"]);
                    assert_eq!(ms.len(), 1, "{phase} {name}");
                    assert!(ms[0] > 0.0, "{phase} {name} took {}", ms[0]);
                }
            }
            for name in SHAPES {
                let ms = numbers(&run["plan"][name]["ms"]);
                assert_eq!(ms.len(), 1, "plan {name}");
                assert!(ms[0] > 0.0, "plan {name} took {}", ms[0]);
            }
        }

        // ── The medians are the medians, of numbers that are the samples' ──
        let (per_run, medians) = (&result["per_run"], &result["medians"]);
        let of_runs = |of: &dyn Fn(&Value) -> f64| -> Vec<f64> { runs.iter().map(of).collect() };
        let held = |what: &str, each: &Value, median: &Value, from_runs: Vec<f64>| {
            assert_eq!(
                numbers(each),
                from_runs,
                "per_run {what} is not the runs' own"
            );
            assert_eq!(number(median), middle(&from_runs), "medians {what}");
        };
        held(
            "ingest_lines_per_s",
            &per_run["ingest_lines_per_s"],
            &medians["ingest_lines_per_s"],
            of_runs(&|run| number(&run["ingest"]["lines_per_s"])),
        );
        for name in QUERIES {
            for phase in ["idle", "loaded"] {
                for (key, p) in [("p50_ms", 0.50), ("p99_ms", 0.99)] {
                    held(
                        &format!("{phase} {name} {key}"),
                        &per_run[phase][name][key],
                        &medians[phase][name][key],
                        of_runs(&|run| nearest_rank(&numbers(&run[phase][name]["client_ms"]), p)),
                    );
                    // What a run records as its own percentile is that number too.
                    for run in runs {
                        assert_eq!(
                            number(&run[phase][name][key]),
                            nearest_rank(&numbers(&run[phase][name]["client_ms"]), p)
                        );
                    }
                }
            }
        }
        for name in SHAPES {
            held(
                &format!("plan {name}"),
                &per_run["plan_ms"][name],
                &medians["plan_ms"][name],
                of_runs(&|run| nearest_rank(&numbers(&run["plan"][name]["ms"]), 0.50)),
            );
        }
        assert_eq!(medians["plan_step_ms"], medians["plan_ms"]["P2"]);
        assert_eq!(
            medians["plan_step_over_1_ms"],
            number(&medians["plan_step_ms"]) > 1.0
        );
        // And the word the row ends on is that flag, whichever it is.
        let word = if medians["plan_step_over_1_ms"] == true {
            "yes"
        } else {
            "no"
        };
        assert!(
            table[8].ends_with(&format!("over 1 ms: {word}")),
            "{:?} with plan_step_ms {}",
            table[8],
            medians["plan_step_ms"]
        );
        for name in QUERIES {
            let each: Vec<f64> = runs
                .iter()
                .map(|run| count(&run["loaded"][name]["count_over_loaded_median"]) as f64)
                .collect();
            held(
                &format!("loaded_count_over_loaded_median {name}"),
                &per_run["loaded_count_over_loaded_median"][name],
                &medians["loaded_count_over_loaded_median"][name],
                each,
            );
        }

        // ── Ingest is counted by the server, on the server's clock ──
        for run in runs {
            let ingest = &run["ingest"];
            let before = count(&ingest["lines_ingested_before"]);
            let after = count(&ingest["lines_ingested_after"]);
            assert_eq!(count(&ingest["lines"]), after - before);
            assert_eq!(count(&ingest["lines"]), LINES);
            assert_eq!(count(&ingest["acked_lines"]), LINES);
            assert_eq!(ingest["counters_agree"], true);
            let seconds = number(&ingest["uptime_after_s"]) - number(&ingest["uptime_before_s"]);
            assert!(seconds > 0.0);
            assert!(
                close(number(&ingest["lines_per_s"]), LINES as f64 / seconds),
                "{} lines/s is not {LINES} lines over {seconds} s",
                ingest["lines_per_s"]
            );
        }

        // ── The answers are the data's ──
        let expect = &result["expect"];
        for name in ["S1", "S2", "S3", "SCAN"] {
            assert!(count(&expect[name]["count"]) >= 1, "{name} matches nothing");
        }
        assert!(count(&expect["SCANKEY"]["count"]) >= count(&expect["SCAN"]["count"]));
        assert_eq!(count(&expect["S4"]["count"]), LINES);
        assert_eq!(count(&expect["S4"]["rows_returned"]), 500);
        assert_eq!(count(&result["plan_trie"]["templates"]), 10_000);
        for run in runs {
            for name in ["S1", "S2", "S3", "S4", "SCAN"] {
                assert_eq!(run["idle"][name]["answers_identical"], true, "{name}");
            }
            assert_eq!(count(&run["plan"]["P4"]["leaves"]), 10_000);
        }

        // ── The scan rate is the data's rows over the server's CPU ──
        let mut every_run_has_a_rate = true;
        for run in runs {
            let scan = &run["scan"];
            assert_eq!(scan["query"], "SCAN");
            assert_eq!(count(&scan["asks"]), 1);
            assert_eq!(count(&scan["rows"]), count(&run["idle"]["scan_key_rows"]));
            assert_eq!(scan["key_rows"], expect["SCANKEY"]["count"]);
            let cpu_s = number(&scan["cpu_s"]);
            assert!(cpu_s >= 0.0);
            // Where the system counts a process's CPU in steps of nanoseconds, one ask is
            // never no CPU at all; where it counts in ticks of 10 ms, it can be.
            if cfg!(target_os = "macos") {
                assert!(cpu_s > 0.0, "the server used no CPU over the scan block");
            }
            if cpu_s > 0.0 {
                assert!(close(
                    number(&scan["rows_per_cpu_s"]),
                    count(&scan["rows"]) as f64 / cpu_s
                ));
            } else {
                assert!(scan["rows_per_cpu_s"].is_null(), "nothing is divided by 0");
                every_run_has_a_rate = false;
            }
        }
        if every_run_has_a_rate {
            assert_eq!(scan_row, "scan rows/s/core N");
            held(
                "scan_rows_per_cpu_s",
                &per_run["scan_rows_per_cpu_s"],
                &medians["scan_rows_per_cpu_s"],
                of_runs(&|run| number(&run["scan"]["rows_per_cpu_s"])),
            );
        } else {
            assert_eq!(scan_row, "scan rows/s/core n/a");
            assert!(medians["scan_rows_per_cpu_s"].is_null());
        }

        // ── Under load means under load ──
        let mut lap_lines = 0;
        for run in runs {
            let loaded = &run["loaded"];
            for name in QUERIES {
                let block = &loaded[name];
                let place = count(&block["place_lines"]);
                assert!(place > 0);
                for at in counts(&block["at_lines"]) {
                    assert!(
                        at >= place,
                        "{name} was asked at {at}, before its place {place}"
                    );
                }
                let over = counts(&block["count_over_loaded"]);
                let counted = counts(&block["count"]);
                for (over, counted) in over.iter().zip(&counted) {
                    assert_eq!(*over, counted - count(&expect[name]["count"]), "{name}");
                }
                if name == "S4" {
                    // It matches every row, so what it counted over the loaded rows is the
                    // live rows its snapshot held.
                    assert!(
                        over.iter().all(|over| *over > 0),
                        "S4 saw no live row: {over:?}"
                    );
                }
            }
            lap_lines += counts(&loaded["lap"]["lines"]).iter().sum::<u64>();
        }
        assert!(lap_lines > 0, "no lap's server took a line");

        // ── The loaded directory was read, and not written ──
        let loaded_dir = dir.join(format!("farm-{LINES}-seed42"));
        let marker_path = dir.join(format!("farm-{LINES}-seed42.json"));
        let marker_bytes = fs::read(&marker_path).expect("the marker");
        let marker = read_json(&marker_path);
        assert_eq!(listing(&loaded_dir), recorded(&marker["files"]));
        assert_eq!(
            recorded(&result["data"]["files"]),
            recorded(&marker["files"])
        );
        assert!(
            !loaded_dir.join("wal").exists(),
            "a server opened the loaded directory"
        );
        assert!(!loaded_dir.join("trie.json").exists());
        assert_eq!(work_left(dir), Vec::<String>::new());
        let log = text(&fs::read(dir.join("a.log")).expect("the raw log beside the result"));
        assert!(
            log.ends_with(&format!("{}\n", table.join("\n"))),
            "the saved log does not end with the table"
        );
        assert_eq!(log, stdout, "the saved log is what was printed");

        // ── A file against itself: every ratio is 1, and every one is said to be no difference ──
        let printed = kibbr()
            .args(["--table"])
            .arg(&a)
            .arg(&a)
            .output()
            .expect("failed to run kibbr-bench --table");
        assert!(printed.status.success(), "{}", text(&printed.stderr));
        let printed = text(&printed.stdout);
        let rows: Vec<&str> = printed.lines().collect();
        assert_eq!(rows[2].matches("(1.00x)").count(), 1, "{:?}", rows[2]);
        for row in &rows[3..7] {
            assert_eq!(row.matches("(1.00x / 1.00x)").count(), 1, "{row:?}");
        }
        assert_eq!(rows[8].matches("(1.00x)").count(), 1, "{:?}", rows[8]);
        assert_eq!(rows[10].matches("(1.00x)").count(), 4, "{:?}", rows[10]);
        let mut ratios = vec!["ingest lines/s".to_string()];
        for name in QUERIES {
            ratios.push(format!("{name} p50"));
            ratios.push(format!("{name} p99"));
        }
        ratios.push("plan step".to_string());
        for name in QUERIES {
            ratios.push(format!("{name} under ingest p99"));
        }
        if every_run_has_a_rate {
            assert_eq!(rows[7].matches("(1.00x)").count(), 1, "{:?}", rows[7]);
            ratios.push("scan rows/s/core".to_string());
        }
        // The wall clock's rate needs no CPU time, so its line is there with or without one.
        ratios.push("scan by the wall clock".to_string());
        for ratio in &ratios {
            assert!(
                rows.iter().any(
                    |row| row.starts_with(&format!("{ratio}: the runs overlap ("))
                        && row.ends_with(" - not a difference")
                ),
                "no line says that `{ratio}` is not a difference, in:\n{printed}"
            );
        }
        assert_eq!(
            rows.iter()
                .filter(|row| row.contains("the runs overlap"))
                .count(),
            ratios.len()
        );

        // ── A second run reuses the loaded directory, and a dot file in it stops nothing ──
        fs::write(loaded_dir.join(".DS_Store"), b"a file manager was here").expect("plant");
        fs::write(loaded_dir.join("data").join(".DS_Store"), b"and here").expect("plant");
        let again = bench(dir, 1, 1, &["--out", b.to_str().expect("a path")]);
        assert!(again.status.success(), "{}", text(&again.stderr));
        let again_log = text(&again.stdout);
        for planted in [".DS_Store", "data/.DS_Store"] {
            assert!(
                again_log.lines().any(|line| line.ends_with(&format!(
                    "not part of the loaded bytes, and not copied: {planted}"
                ))),
                "the log does not name {planted}:\n{again_log}"
            );
        }
        assert_eq!(
            fs::read(&marker_path).expect("the marker"),
            marker_bytes,
            "the marker was written again: the day was loaded twice"
        );
        let second = read_json(&b);
        assert_eq!(second["data"]["files"], result["data"]["files"]);
        assert!(recorded(&second["data"]["files"])
            .iter()
            .all(|(path, _, _)| !path.contains(".DS_Store")));
        assert_eq!(second["runs"].as_array().expect("runs").len(), 1);
        assert_eq!(work_left(dir), Vec::<String>::new());

        // ── A saved result is not written over ──
        let saved = fs::read(&a).expect("the result");
        let over = bench(dir, 3, 1, &["--out", a.to_str().expect("a path")]);
        assert_eq!(over.status.code(), Some(1));
        let said = text(&over.stderr);
        assert!(
            said.contains("a.json exists; a saved result is not written over"),
            "{said}"
        );
        assert_eq!(fs::read(&a).expect("the result"), saved);

        // ── A baseline of other work is refused before any server starts ──
        let c = dir.join("c.json");
        let other = bench(
            dir,
            3,
            2,
            &[
                "--baseline",
                a.to_str().expect("a path"),
                "--out",
                c.to_str().expect("a path"),
            ],
        );
        assert_eq!(other.status.code(), Some(1));
        let said = text(&other.stderr);
        assert!(
            said.contains("flags.samples: 1 in the baseline, 2 in this run"),
            "{said}"
        );
        assert!(text(&other.stdout).is_empty(), "something was measured");
        assert_eq!(work_left(dir), Vec::<String>::new());
        assert!(!c.exists());

        // ── So is a baseline taken without `--threads`, by a run with it ──
        let threaded = bench(
            dir,
            3,
            1,
            &[
                "--baseline",
                a.to_str().expect("a path"),
                "--out",
                c.to_str().expect("a path"),
                "--threads",
                "1",
            ],
        );
        assert_eq!(threaded.status.code(), Some(1));
        let said = text(&threaded.stderr);
        assert!(
            said.contains("server.argv: ") && said.contains("--scan-threads 1\" in this run"),
            "{said}"
        );
        assert!(text(&threaded.stdout).is_empty(), "something was measured");
        assert_eq!(work_left(dir), Vec::<String>::new());
        assert!(!c.exists());

        // ── A place the result could not be written to is refused before any server starts ──
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let shut = dir.join("shut");
            fs::create_dir(&shut).expect("a directory");
            fs::set_permissions(&shut, fs::Permissions::from_mode(0o555)).expect("shut it");
            // Whoever may write anywhere is not stopped by a mode, and has no such refusal.
            if fs::write(shut.join("tried"), b"").is_err() {
                let d = shut.join("d.json");
                let nowhere = bench(dir, 1, 1, &["--out", d.to_str().expect("a path")]);
                assert_eq!(nowhere.status.code(), Some(1));
                let said = text(&nowhere.stderr);
                assert!(
                    said.contains(&format!("{}: ", d.display())),
                    "the refusal does not name the result: {said}"
                );
                assert!(text(&nowhere.stdout).is_empty(), "something was measured");
                assert_eq!(work_left(dir), Vec::<String>::new());
            }
            fs::set_permissions(&shut, fs::Permissions::from_mode(0o755)).expect("open it");
            assert_eq!(
                fs::read_dir(&shut).expect("the directory").count(),
                usize::from(shut.join("tried").exists()),
                "the refusal left something beside the result's place"
            );
        }
    }

    #[test]
    fn a_profile_the_bench_does_not_measure_is_refused_by_name() {
        let scratch = Scratch::new("profile");
        let dir = scratch.0.join("bench");
        let ran = kibbr()
            .args(["--profile", "hostile", "--unchecked"])
            .args(["--lines", &LINES.to_string()])
            .arg("--dir")
            .arg(&dir)
            .output()
            .expect("failed to run kibbr-bench");
        assert_eq!(ran.status.code(), Some(1));
        let said = text(&ran.stderr);
        assert!(
            said.contains("kibbr bench measures the farm profile only"),
            "{said}"
        );
        assert!(ran.stdout.is_empty());
        assert!(!dir.exists(), "something was made under --dir");
    }

    /// **`--threads T` is `--scan-threads T` on every server the bench starts, and the result
    /// says so.** A count that is not the default, so a flag that reached no server shows.
    #[test]
    fn the_threads_asked_for_reach_every_server_and_are_in_the_result() {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let default = (cores / 2).clamp(1, 4);
        let asked = if default == 1 { 2.min(cores) } else { 1 };

        let scratch = Scratch::new("threads");
        let dir = scratch.0.as_path();
        let out = dir.join("threads.json");
        let ran = bench(
            dir,
            1,
            1,
            &[
                "--out",
                out.to_str().expect("a path"),
                "--threads",
                &asked.to_string(),
            ],
        );
        assert!(ran.status.success(), "{}", text(&ran.stderr));

        let result = read_json(&out);
        let argv: Vec<&str> = result["server"]["argv"]
            .as_array()
            .expect("server.argv")
            .iter()
            .map(|argument| argument.as_str().expect("an argument"))
            .collect();
        let asked_text = asked.to_string();
        assert_eq!(
            argv[argv.len() - 2..],
            ["--scan-threads", asked_text.as_str()]
        );
        assert!(
            text(&ran.stdout)
                .lines()
                .any(|line| line.starts_with("server ")
                    && line.ends_with(&format!(" --scan-threads {asked}"))),
            "the log's server line does not end with the flag"
        );
        // Each server's own word for it, in the `/stats` a unit ends on.
        let run = &result["runs"][0];
        for unit in ["ingest", "idle"] {
            assert_eq!(
                count(&run[unit]["stats_at_end"]["queries"]["scan_threads"]),
                asked as u64,
                "{unit}"
            );
        }
        assert_eq!(work_left(dir), Vec::<String>::new());
    }

    /// More threads than cores are refused by the bench, in its own words, before a day is
    /// generated: `serve` would refuse them too, as a server that did not come up.
    #[test]
    fn more_threads_than_cores_are_refused_before_anything_is_made() {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        let scratch = Scratch::new("threads-above");
        let dir = scratch.0.join("bench");
        let ran = bench(&dir, 1, 1, &["--threads", &(cores + 1).to_string()]);
        assert_eq!(ran.status.code(), Some(1));
        let said = text(&ran.stderr);
        assert!(
            said.contains(&format!(
                "--threads {} is above the {cores} core(s) this machine has",
                cores + 1
            )),
            "{said}"
        );
        assert!(ran.stdout.is_empty());
        assert!(!dir.exists(), "something was made under --dir");

        let none = bench(&dir, 1, 1, &["--threads", "0"]);
        assert_eq!(none.status.code(), Some(2), "{}", text(&none.stderr));
    }
}
