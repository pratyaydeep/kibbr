// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The command line: the flags of every subcommand, and the dispatch to the module that
//! runs it. One module a subcommand, plus the settings, the process's own log, the errors
//! a command ends on, and the small HTTP client the commands that ask a server share.

pub mod bench_storage;
pub mod compact;
pub mod config;
pub mod demo;
pub mod error;
pub mod gen_logs;
pub mod http;
pub mod load;
pub mod logging;
pub mod parse;
pub mod query;
pub mod serve;
pub mod status;

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::engine::model::infer;
use error::CliError;
use gen_logs as gen;
use logging as logs;

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
    GenLogs(gen_logs::Args),
    /// Print the fingerprint and inferred types of each line.
    Parse(parse::Args),
    /// Build the trie over a file and print it as an ASCII tree.
    Demo(demo::Args),
    /// Serve the HTTP API and the log viewer. Rows are written to `--data-dir` and survive a
    /// restart; `--storage memory` keeps nothing past the process.
    Serve(serve::Args),
    /// Load closed days of logs into a data directory: NDJSON, gzip or zstd, from files,
    /// directories or stdin. Every row or none, and the same inputs are never loaded twice.
    Load(load::Args),
    /// Compact a finished day: its small row groups rebuilt into full ones, published in one
    /// step. Nothing a query can see moves. A server holding the directory is asked to run it.
    Compact(compact::Args),
    /// Measure what a file costs to store: raw, gzip -9, zstd -19 and as kibbr holds it, in
    /// bytes and disk blocks; then the same lines as a live day, before compaction and after.
    BenchStorage(bench_storage::Args),
    /// List every template a running server holds: its id, its rows, where its time
    /// comes from, and its path. What `GET /templates` answers, asked for you.
    Templates(status::TemplatesArgs),
    /// Print what a running server says of itself: uptime, storage, the trie, ingest,
    /// the write-ahead log. What `GET /stats` answers, asked for you - a line a number.
    Stats(status::StatsArgs),
    /// Ask a running server whether it is well: `GET /health`. Prints `ok`, or `degraded` and
    /// why, and exits 0; exits 1 when no server answers. What the image's HEALTHCHECK runs.
    Health(status::HealthArgs),
    /// Parse a TrieQL query and print what it means. Opens no file and reads no data;
    /// `query <file> '<trieql>' --explain` is the half that prints the execution plan.
    Explain(query::ExplainArgs),
    /// Query a file, with TrieQL or with the flags.
    Query(query::Args),
}

/// The binary: parse the command line, run the command, and say why if it failed.
pub fn run() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli) {
        Ok(code) => code,
        Err(e) => {
            logs::failed(&e);
            ExitCode::FAILURE
        }
    }
}

/// What every command does first, `kibbr-bench` too: the settings file read, the log
/// installed, and the epoch window set; the file and the log are handed back.
pub(crate) fn settle(
    config: Option<&Path>,
    log_level: Option<config::LogLevel>,
    log_format: Option<config::LogFormat>,
) -> Result<(config::File, config::Log), CliError> {
    // The file first, and for every command alike: a bad one stops the process before
    // anything is done under settings its writer did not ask for.
    let file = config::File::read(config)?;
    // Then the log, so that everything after this line is said the way it was asked for.
    let log = config::log(&file, log_level, log_format)?;
    logs::install(log);
    // Before any command types a value, and for every command alike: a bad window stops
    // the process here rather than after it has typed lines under the default.
    infer::set_epoch_window(config::epoch_window(&file)?);
    Ok((file, log))
}

fn dispatch(cli: Cli) -> Result<ExitCode, CliError> {
    let (file, log) = settle(cli.config.as_deref(), cli.log_level, cli.log_format)?;
    let file = &file;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    // What every command but `serve` ends on once it has not failed.
    let mut code = ExitCode::SUCCESS;

    let result = match cli.command {
        Command::GenLogs(args) => gen::gen_logs(&mut out, args).map_err(CliError::from),
        Command::Parse(args) => parse::cli(&mut out, args),
        Command::Demo(args) => demo::cli(&mut out, args),
        // The server is a module of its own (`crate::server`), and this arm is its command
        // line. Writes nothing to `out`: the banner and the lifecycle log go to stderr
        // through `logging`.
        Command::Serve(args) => {
            code = serve::cli(file, args, log)?;
            Ok(())
        }
        Command::Load(args) => load::cli(file, &mut out, args),
        Command::Compact(args) => compact::cli(file, &mut out, args),
        Command::BenchStorage(args) => bench_storage::cli(file, &mut out, args),
        Command::Explain(args) => query::describe(
            &mut out,
            &args.query,
            config::limits(file, args.max_rows, args.max_buckets, args.max_groups)?,
        ),
        Command::Templates(args) => status::templates(
            &mut out,
            &status::server(file, args.addr, args.data_dir)?,
            args.json,
        ),
        Command::Stats(args) => status::stats(
            &mut out,
            &status::server(file, args.addr, args.data_dir)?,
            args.json,
        ),
        Command::Health(args) => status::health(
            &mut out,
            &status::server(file, args.addr, args.data_dir)?,
            args.json,
        ),
        Command::Query(args) => query::cli(file, &mut out, args),
    };

    finish(result, out).map(|()| code)
}

/// A command's result once its output is flushed: a closed downstream pipe (`| head`) is
/// how the command ends, not a failure.
pub(crate) fn finish(result: Result<(), CliError>, mut out: impl Write) -> Result<(), CliError> {
    match result {
        Err(e) if is_broken_pipe(&e) => return Ok(()),
        other => other?,
    }
    match out.flush() {
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => Err(CliError::Write(e)),
        Ok(()) => Ok(()),
    }
}

fn is_broken_pipe(e: &CliError) -> bool {
    let io_error = match e {
        CliError::Write(inner) => Some(inner),
        CliError::Read { source, .. } => Some(source),
        CliError::Input(crate::engine::ReadError::Read { source, .. }) => Some(source),
        CliError::Gen(error::GenError::Write(inner)) => Some(inner),
        _ => None,
    };
    io_error.is_some_and(|inner| inner.kind() == io::ErrorKind::BrokenPipe)
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
