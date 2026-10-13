// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr bench`, from outside the process. Its numbers are a machine's, so nothing here
//! holds a timing to more than "positive": what is held is what must be true on any machine.
//! Every run is `--unchecked`: the test binary is a debug build, and other tests' servers
//! are alive beside it, which a checked run would wait for and then be disturbed by.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

/// Lines in the loaded farm day and in the live stream: few, and still enough that each of
/// the seven queries matches a row.
const LINES: u64 = 2_000;

/// Lines in one `/ingest` body: twenty bodies, five a sender.
const BODY_LINES: u64 = 100;

/// The four standard queries, and the four plan shapes, as a result names them.
const QUERIES: [&str; 4] = ["S1", "S2", "S3", "S4"];
const SHAPES: [&str; 4] = ["P1", "P2", "P3", "P4"];

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("kibbr-speed-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create the scratch root");
        Self(root)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The binary, with no `KIBBR__` variable of the caller's: the bench takes them out of
/// its children itself, and the test does not lean on that.
fn kibbr() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kibbr"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("KIBBR__") {
            command.env_remove(name);
        }
    }
    command
}

/// `kibbr bench --lines 2000 --profile farm ... --unchecked --dir <dir>` with `more` after
/// it.
fn bench(dir: &Path, runs: usize, samples: usize, more: &[&str]) -> Output {
    kibbr()
        .args(["bench", "--profile", "farm", "--unchecked"])
        .args(["--lines", &LINES.to_string()])
        .args(["--body-lines", &BODY_LINES.to_string()])
        .args(["--runs", &runs.to_string()])
        .args(["--samples", &samples.to_string()])
        .arg("--dir")
        .arg(dir)
        .args(more)
        .output()
        .expect("failed to run kibbr bench")
}

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
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{} is not JSON: {e}", path.display()))
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
    assert!(ran.status.success(), "kibbr bench failed:\n{stderr}");
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
        .args(["bench", "--table"])
        .arg(&a)
        .arg(&a)
        .output()
        .expect("failed to run kibbr bench --table");
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
        ratios.push("scan by the wall clock".to_string());
    }
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
        .args(["bench", "--profile", "hostile", "--unchecked"])
        .args(["--lines", &LINES.to_string()])
        .arg("--dir")
        .arg(&dir)
        .output()
        .expect("failed to run kibbr bench");
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
