// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The command line of kibbr: the top-level clap type, the enum of the subcommands, and the
//! dispatch to each. A subcommand's own flags are in its own file, as a `Flags` struct.
//!
//! The commands someone runs in production are one module each: `serve`, `load`,
//! `compact`, `query` and `status`. What tries, measures or tests kibbr is in `tools`,
//! which no production command names. `config`, `logging`, `http` and `error` are what
//! the commands share.

mod compact;
mod config;
mod error;
mod http;
mod load;
mod logging;
mod query;
mod serve;
mod status;
mod tools;

use kibbr::model::infer;
use tools::gen_logs;
use tools::{bench_storage, demo, parse};

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use error::CliError;

#[derive(Parser)]
#[command(name = "kibbr", version, about = "kibbr - log store", long_about = None)]
struct Cli {
    /// A TOML file of settings (see kibbr.toml.example); a key is named as its variable is.
    /// Flag > variable > file > default; an unknown key is refused. No --config, no file.
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    /// How much of kibbr's own log is written, to stderr: trace, debug, info, warn or
    /// error. Overrides `KIBBR__LOG__LEVEL` and `[log] level`. [default: info]
    #[arg(long, global = true, value_enum, value_name = "LEVEL")]
    log_level: Option<config::LogLevel>,
    /// How a log line is written, to stderr: `text`, or `json`, one object a line.
    /// Overrides `KIBBR__LOG__FORMAT` and `[log] format`. [default: text]
    #[arg(long, global = true, value_enum, value_name = "FORMAT")]
    log_format: Option<config::LogFormat>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate reproducible NDJSON logs on stdout.
    GenLogs(gen_logs::Flags),
    /// Print the fingerprint and inferred types of each line.
    Parse(parse::Flags),
    /// Build the trie over a file and print it as an ASCII tree.
    Demo(demo::Flags),
    /// Serve the HTTP API and the log viewer. Rows are written to `--data-dir` and survive a
    /// restart; `--storage memory` keeps nothing past the process.
    Serve(serve::Flags),
    /// Load closed days of logs into a data directory: NDJSON, gzip or zstd, from files,
    /// directories or stdin. Every row or none, and the same inputs are never loaded twice.
    Load(load::Flags),
    /// Compact a finished day: its small row groups rebuilt into full ones, published in one
    /// step. Nothing a query can see moves. A server holding the directory is asked to run it.
    Compact(compact::Flags),
    /// Measure what a file costs to store: raw, gzip -9, zstd -19 and as kibbr holds it, in
    /// bytes and disk blocks; then the same lines as a live day, before compaction and after.
    BenchStorage(bench_storage::Flags),
    /// List every template a running server holds: its id, its rows, where its time
    /// comes from, and its path. What `GET /templates` answers, asked for you.
    Templates(status::TemplatesFlags),
    /// Print what a running server says of itself: uptime, storage, the trie, ingest,
    /// the write-ahead log. What `GET /stats` answers, asked for you - a line a number.
    Stats(status::StatsFlags),
    /// Ask a running server whether it is well: `GET /health`. Prints `ok`, or `degraded` and
    /// why, and exits 0; exits 1 when no server answers. What the image's HEALTHCHECK runs.
    Health(status::HealthFlags),
    /// Parse a TrieQL query and print what it means. Opens no file and reads no data;
    /// `query <file> '<trieql>' --explain` is the half that prints the execution plan.
    Explain(query::ExplainFlags),
    /// Query a file, with TrieQL or with the flags.
    Query(query::Flags),
}

/// What `main` is: parse the command line, run the command, and say what ended it.
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => code,
        Err(e) => {
            logging::failed(&e);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode, CliError> {
    // The file first, and for every command alike: a bad one stops the process before
    // anything is done under settings its writer did not ask for.
    let file = config::File::read(cli.config.as_deref())?;
    let file = &file;
    // Then the log, so that everything after this line is said the way it was asked for.
    let log = config::log(file, cli.log_level, cli.log_format)?;
    logging::install(log);
    // Before any command types a value, and for every command alike: a bad window stops
    // the process here rather than after it has typed lines under the default.
    infer::set_epoch_window(config::epoch_window(file)?);

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    // What every command but `serve` ends on once it has not failed.
    let mut code = ExitCode::SUCCESS;

    let result = match cli.command {
        Command::GenLogs(flags) => gen_logs::gen_logs(
            &mut out,
            flags.profile,
            flags.lines,
            flags.templates,
            flags.seed,
            flags.day,
            flags.services,
            flags.dirty_rate,
        )
        .map_err(CliError::from),
        Command::Parse(flags) => parse::cli(&flags.input, &mut out, flags.limit, flags.json),
        Command::Demo(flags) => demo::cli(&flags.input, &mut out, flags.full),
        // The server is the `server` module, and this arm is its command line. Writes
        // nothing to `out`: the banner and the lifecycle log go to stderr through `logging`.
        Command::Serve(flags) => {
            code = serve::cli(file, flags, log)?;
            Ok(())
        }
        Command::Load(flags) => load::cli(file, &mut out, flags),
        Command::Compact(flags) => compact::cli(file, &mut out, flags),
        Command::BenchStorage(flags) => bench_storage::run(
            &bench_storage::Args {
                input: flags.input,
                es: flags.es,
                scratch: flags.scratch,
                keep: flags.keep,
                no_zstd: flags.no_zstd,
                zstd_threads: flags
                    .zstd_threads
                    .unwrap_or_else(bench_storage::all_threads),
                settings: file.clone(),
            },
            config::schema(file, flags.time_columns),
            &mut out,
        ),
        Command::Explain(flags) => query::describe(
            &mut out,
            &flags.query,
            config::limits(file, flags.max_rows, flags.max_buckets, flags.max_groups)?,
        ),
        Command::Templates(flags) => status::templates(
            &mut out,
            &status::server(file, flags.addr, flags.data_dir)?,
            flags.json,
        ),
        Command::Stats(flags) => status::stats(
            &mut out,
            &status::server(file, flags.addr, flags.data_dir)?,
            flags.json,
        ),
        Command::Health(flags) => status::health(
            &mut out,
            &status::server(file, flags.addr, flags.data_dir)?,
            flags.json,
        ),
        Command::Query(flags) => query::cli(file, &mut out, flags),
    };

    // A closed downstream pipe (`| head`) is how the command ends, not a failure.
    match result {
        Err(e) if is_broken_pipe(&e) => return Ok(code),
        other => other?,
    }
    match out.flush() {
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(code),
        Err(e) => Err(CliError::Write(e)),
        Ok(()) => Ok(code),
    }
}

/// The input a command reads: a file by its path, or standard input for `-`.
pub(crate) fn open(input: &str) -> Result<Box<dyn BufRead>, CliError> {
    if input == "-" {
        return Ok(Box::new(BufReader::new(io::stdin())));
    }
    let file = File::open(input).map_err(|source| CliError::Read {
        path: input.to_string(),
        source,
    })?;
    Ok(Box::new(BufReader::new(file)))
}

fn is_broken_pipe(e: &CliError) -> bool {
    let io_error = match e {
        CliError::Write(inner) => Some(inner),
        CliError::Read { source, .. } => Some(source),
        CliError::Input(kibbr::ReadError::Read { source, .. }) => Some(source),
        CliError::Gen(error::GenError::Write(inner)) => Some(inner),
        _ => None,
    };
    io_error.is_some_and(|inner| inner.kind() == io::ErrorKind::BrokenPipe)
}
