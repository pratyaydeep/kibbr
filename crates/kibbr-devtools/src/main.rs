// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr-bench`: how fast a build of kibbr is on one farm day. Not part of the product
//! and never published. It measures the `kibbr` binary in the directory it is in itself
//! (`bench::kibbr_beside`), and building one does not build the other.

mod bench;

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

/// The allocator `kibbr` runs under (its `src/main.rs` says why), so that the plan step,
/// which is timed in this process, is timed under the server's allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The profiles `kibbr gen-logs` has. Only `farm` is measured.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Profile {
    Hostile,
    Farm,
}

/// Measure how fast the kibbr beside this binary is on one farm day: ingest lines a second,
/// the four standard queries idle and under ingest, scan rows per CPU-second, and the plan
/// step.
#[derive(Parser)]
#[command(name = "kibbr-bench", version, long_about = None)]
struct Cli {
    /// Lines in the farm day that is loaded (seed 42), and in the live stream sent to
    /// the servers (seed 43). The measurement is `--lines 1000000`.
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        required_unless_present_any = ["table", "plan_only"]
    )]
    lines: Option<u64>,
    /// Only `farm` is measured: the queries and their expected answers are the
    /// farm's. `hostile` is refused.
    #[arg(long, value_enum, required_unless_present_any = ["table", "plan_only"])]
    profile: Option<Profile>,
    /// Where the result is saved, the raw log beside it as `.log`; a file that exists is
    /// refused. [default: <dir>/results/<UTC time>-<commit>.json]
    #[arg(long, value_name = "FILE")]
    out: Option<PathBuf>,
    /// A saved result to compare with, held to be the same work before anything is measured;
    /// each number is printed with its speed-up on it.
    #[arg(long, value_name = "FILE")]
    baseline: Option<PathBuf>,
    /// The heading of this result's column, saved in the file.
    /// [default: baseline, or `this run` with --baseline]
    #[arg(long, value_name = "TEXT")]
    label: Option<String>,
    /// Holds the loaded directory, each server's copy of it, and the default results.
    #[arg(long, value_name = "PATH", default_value = bench::DEFAULT_DIR)]
    dir: PathBuf,
    /// How many times the whole sequence is run; each printed number is the middle
    /// one. Odd. Two results compare only at the same value.
    #[arg(long, value_name = "R", default_value_t = bench::DEFAULT_RUNS, value_parser = odd)]
    runs: usize,
    /// Timed asks of each query, and timed plans of each shape. Two results compare only at
    /// the same value.
    #[arg(
        long,
        value_name = "K",
        default_value_t = bench::DEFAULT_SAMPLES,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    samples: usize,
    /// Lines in one `/ingest` body. Two results compare only at the same value.
    #[arg(
        long,
        value_name = "L",
        default_value_t = bench::DEFAULT_BODY_LINES,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    body_lines: usize,
    /// Scan threads of every server measured: each is started with `--scan-threads T`.
    /// Two results compare only at the same value.
    #[arg(
        long,
        value_name = "T",
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    threads: Option<usize>,
    /// Skip the checks that make a run a measurement, and allow a debug build; the result is
    /// marked `UNCHECKED - not a measurement`. For tests.
    #[arg(long)]
    unchecked: bool,
    /// Measure nothing: print the table from saved results, the first as the baseline
    /// and one column a file, headed by its label.
    #[arg(
        long,
        value_name = "FILE",
        num_args = 2..,
        conflicts_with_all = [
            "lines", "profile", "out", "baseline", "label", "dir", "runs", "samples",
            "body_lines", "unchecked", "plan_only"
        ]
    )]
    table: Vec<PathBuf>,
    // What a run starts as a child, once a run. Hidden, because its one JSON line is read
    // by the bench and not by a person.
    #[arg(
        long,
        hide = true,
        conflicts_with_all = [
            "lines", "profile", "out", "baseline", "label", "dir", "runs", "body_lines",
            "unchecked"
        ]
    )]
    plan_only: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let result = bench::cli(
        &mut out,
        cli.lines,
        cli.profile,
        cli.out,
        cli.baseline,
        cli.label,
        cli.dir,
        cli.runs,
        cli.samples,
        cli.body_lines,
        cli.threads,
        cli.unchecked,
        cli.table,
        cli.plan_only,
    );
    // A closed downstream pipe (`| head`) is how the command ends, not a failure.
    let result = result.and_then(|()| match out.flush() {
        Err(e) if e.kind() != io::ErrorKind::BrokenPipe => Err(bench::on_stdout(e)),
        _ => Ok(()),
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // The lines `kibbr` ends a failed command with: the error, then each cause.
            kibbr_engine::error::report(&e);
            ExitCode::FAILURE
        }
    }
}

/// `--runs`: an odd count, so that the median of the runs is one of them and not a
/// number between two.
fn odd(text: &str) -> Result<usize, String> {
    match text.parse::<usize>() {
        Ok(runs) if runs % 2 == 1 => Ok(runs),
        Ok(runs) => Err(format!(
            "{runs} is not odd; each printed number is the middle one of the runs"
        )),
        Err(e) => Err(e.to_string()),
    }
}
