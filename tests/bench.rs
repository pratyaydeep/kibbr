// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr bench-storage`, from outside the process: its numbers are a disk's, so what is
//! held here is what must be true of them on any disk.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

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

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("kibbr-bench-test-{}", std::process::id()));
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

fn kibbr() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kibbr"))
}

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
    let scratch = Scratch::new();
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
