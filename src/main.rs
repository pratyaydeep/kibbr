//! TrieDB - M0. Two subcommands: generate hostile logs, and parse them.

mod error;
mod flatten;
mod gen;
mod infer;
mod parse;
mod rng;
mod types;

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use error::CliError;

#[derive(Parser)]
#[command(name = "triedb", version, about = "TrieDB - log store", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate reproducible NDJSON logs on stdout.
    GenLogs {
        /// How many NDJSON lines to write.
        #[arg(long, default_value_t = 1000)]
        lines: u64,
        /// How many distinct log shapes to draw from. Optional fields split each
        /// shape into several fingerprints, so `triedb parse` reports more
        /// templates than this.
        #[arg(long, default_value_t = 12)]
        templates: usize,
        /// Same seed, byte-identical output. Nothing here reads the wall clock.
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// Print the fingerprint and inferred types of each line.
    Parse {
        /// Path to an NDJSON file, or `-` for stdin.
        input: String,
        /// Stop after this many lines.
        #[arg(long)]
        limit: Option<u64>,
        /// Emit the `ParsedLog` as NDJSON instead of the human table.
        #[arg(long)]
        json: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            report(&e);
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), CliError> {
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let result = match cli.command {
        Command::GenLogs {
            lines,
            templates,
            seed,
        } => gen::generate(&mut out, lines, templates, seed).map_err(CliError::from),
        Command::Parse { input, limit, json } => {
            let reader = open(&input)?;
            parse::run(reader, &mut out, limit, json)
        }
    };

    // A closed downstream pipe (`| head`) is how the command ends, not a failure.
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

fn open(input: &str) -> Result<Box<dyn BufRead>, CliError> {
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
        CliError::Gen(error::GenError::Write(inner)) => Some(inner),
        _ => None,
    };
    io_error.is_some_and(|inner| inner.kind() == io::ErrorKind::BrokenPipe)
}

/// Print the error and every cause below it, so a `#[source]` chain is not lost.
fn report(e: &CliError) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "triedb: {e}");
    let mut source = std::error::Error::source(e);
    while let Some(cause) = source {
        let _ = writeln!(stderr, "  caused by: {cause}");
        source = cause.source();
    }
}
