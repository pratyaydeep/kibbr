// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr serve`: the command line of the server, which is a module of its own
//! (`crate::server`). Writes nothing to standard output: the banner and the lifecycle log go to stderr through `logging`.

use std::process::ExitCode;

use tokio_util::sync::CancellationToken;

use crate::cli::config::{self, File, Log, ServeFlags};
use crate::cli::error::CliError;

/// A flag rather than inferred from `--data-dir`, so that `--storage memory --data-dir /x`
/// reads as an instruction and not as a contradiction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum StorageKind {
    Disk,
    Memory,
}

/// Run a server on the settings `flags`, `file` and the defaults make, until a signal stops
/// it, and say what the process exits with.
pub fn cli(file: &File, flags: ServeFlags, log: Log) -> Result<ExitCode, CliError> {
    let settings = config::settings(file, flags, log)?;
    // Nothing here cancels the token: a server started from the command line is
    // stopped by SIGTERM or SIGINT, which it hears for itself.
    let report = crate::server::run(settings, CancellationToken::new())?;
    Ok(exit_code(&report))
}

/// What `kibbr serve` exits with: 0 for every run that ended in a report, the deadline's
/// cut included, because every acknowledged line is in the write-ahead log.
fn exit_code(report: &crate::server::Report) -> ExitCode {
    // Every field named, so that one added to the report is looked at here.
    let crate::server::Report { addr: _, cut: _ } = report;
    ExitCode::SUCCESS
}
