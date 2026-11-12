// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `kibbr compact` - the command: who runs it and what it prints. The rebuild is the
//! store's (`store::disk::rebuild`) and the run the engine's
//! (`crate::engine::database::compaction`). Who holds `LOCK` decides where it runs, as for `kibbr load`:
//! one process writes the manifest, so a server holding the directory is asked
//! (`POST /compact`), and with none the command takes the lock and compacts alone.

use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::cli::config::LoadStorage as _;

use crate::engine::database::compaction::{line, Report, NO_DISK};
use crate::engine::StoreError;

#[derive(Debug, Error)]
pub enum CompactError {
    #[error("{0}")]
    Input(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The count of due days that failed: a run carries on past a bad day, and each failure
    /// is said as it happens.
    #[error(
        "{failed} of the {of} day(s) that were due could not be compacted; each is reported above"
    )]
    Days { failed: usize, of: usize },
    #[error("{}", NO_DISK)]
    NoDisk,
    #[error(transparent)]
    Cli(Box<crate::cli::error::CliError>),
    /// The server that holds the data directory ran the compaction, and it failed there.
    #[error("{0}")]
    Remote(String),
    /// The server that holds the data directory could not be asked.
    #[error("the server at {address}, which holds the data directory: {detail}")]
    Server { address: String, detail: String },
}

impl From<crate::cli::error::CliError> for CompactError {
    fn from(e: crate::cli::error::CliError) -> Self {
        CompactError::Cli(Box::new(e))
    }
}

impl From<crate::engine::LockError> for CompactError {
    fn from(e: crate::engine::LockError) -> Self {
        CompactError::Input(e.to_string())
    }
}

// ── The command ────────────────────────────────────────────────────────────────────

/// `kibbr compact`, as the command line gave it.
#[derive(Debug)]
pub struct Args {
    pub day: Option<String>,
    pub threads: Option<usize>,
    /// Whether `--segment-max-bytes` was given, only to say so when a server's own size will
    /// be used instead.
    pub segment_max_bytes: Option<usize>,
    pub json: bool,
}

/// What `kibbr compact` refuses before the storage settings are loaded, which would create
/// the directory: a `--day` that is not a day, and a path with no `manifest.log`.
pub fn precheck(day: Option<&str>, base: &Path) -> Result<Option<i64>, CompactError> {
    // Refused here as well as by a server, so a client's mistake fails before it is sent.
    let day =
        match day {
            None => None,
            Some(text) => Some(crate::engine::day_of_name(text).ok_or_else(|| {
                CompactError::Input(format!("--day {text}: not a YYYY-MM-DD day"))
            })?),
        };
    crate::engine::refuse_other_versions(base)?;
    if !crate::engine::manifest_path(base).is_file() {
        return Err(CompactError::Input(format!(
            "{}: not a kibbr data directory (no manifest.log); nothing was compacted",
            base.display()
        )));
    }
    Ok(day)
}

/// Run `kibbr compact` alone if no process holds the data directory, otherwise as a client
/// of the server that does, by the same rule as `load::command`.
pub fn command(
    args: Args,
    day: Option<i64>,
    storage: crate::cli::config::Storage,
    schema: crate::cli::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), CompactError> {
    let base = storage
        .base_path
        .clone()
        .ok_or_else(|| CompactError::Input("no data directory".to_string()))?;
    let waited = Instant::now();
    loop {
        let opened = crate::engine::Database::open_offline(
            crate::cli::config::options(&storage, &schema),
            crate::engine::Opener::Compact,
        );
        match opened {
            Ok(db) => return alone(db, &args, day, out),
            Err(crate::engine::Error::Lock(crate::engine::LockError::Held { path, holder })) => {
                match crate::engine::holder(&base) {
                    Some(crate::engine::Holder {
                        address: Some(address),
                        token,
                        ..
                    }) => {
                        let token = token.unwrap_or_default();
                        return client(&address, &token, &base, &args, out);
                    }
                    // A load or another compaction running alone: no server to ask.
                    Some(crate::engine::Holder { load: true, .. }) => {
                        return Err(crate::engine::LockError::Held { path, holder }.into())
                    }
                    // A server still starting: it writes its address once it can answer.
                    _ if waited.elapsed() < crate::cli::load::SERVER_STARTING_WAIT => {
                        std::thread::sleep(crate::cli::load::POLL);
                    }
                    _ => return Err(crate::engine::LockError::Held { path, holder }.into()),
                }
            }
            Err(e) => return Err(opened_alone(e)),
        }
    }
}

/// What a run leaves on standard output: a line a day, or the one JSON object. A run that
/// failed still prints the days it finished, which the exit code alone would not say.
fn print_report(report: &Report, json: bool, out: &mut dyn io::Write) -> Result<(), CompactError> {
    if !report.ok && report.days.is_empty() {
        return Ok(());
    }
    let written = if json {
        writeln!(out, "{}", serde_json::to_string(report).unwrap_or_default())
    } else {
        report
            .days
            .iter()
            .try_for_each(|day| writeln!(out, "{}", line(day)))
    };
    written.map_err(|e| crate::cli::error::CliError::Write(e).into())
}

/// What opening the directory alone failed with, as the command has always said it: the
/// lock's and the version check's own errors, and the start's as the server's.
fn opened_alone(e: crate::engine::Error) -> CompactError {
    match e {
        crate::engine::Error::Lock(e) => e.into(),
        crate::engine::Error::Store(e) => e.into(),
        e => crate::cli::error::CliError::from(e).into(),
    }
}

/// The compaction in this process, holding the directory's lock.
fn alone(
    db: crate::engine::Database,
    args: &Args,
    day: Option<i64>,
    out: &mut dyn io::Write,
) -> Result<(), CompactError> {
    let threads = args
        .threads
        .unwrap_or_else(crate::cli::load::alone_threads)
        .max(1);
    let (days, ran) = db
        .compact_now(day, threads, &mut |_, result| {
            // One day asked for is one error, and the command ends on it. Of the days that
            // were due, each failure is said where it happened and the command ends on a count.
            if let (None, Err(e)) = (day, result) {
                crate::cli::error::report(e);
            }
        })
        .ok_or(CompactError::NoDisk)?;
    print_report(&ran.report, args.json, out)?;
    let failed = ran.failed.len();
    match (day, ran.failed.into_iter().next()) {
        (_, None) => Ok(()),
        (Some(_), Some(e)) => Err(e.into()),
        (None, Some(_)) => Err(CompactError::Days {
            failed,
            of: days.len(),
        }),
    }
}

/// The compaction run by the server that holds the directory: asked over HTTP, then polled.
fn client(
    address: &str,
    token: &str,
    base: &Path,
    args: &Args,
    out: &mut dyn io::Write,
) -> Result<(), CompactError> {
    if args.segment_max_bytes.is_some() {
        tracing::warn!(
            "--segment-max-bytes is ignored: the server at {address} holds this \
             data directory, and a file is sealed at the server's own size"
        );
    }
    let server = |detail: String| CompactError::Server {
        address: address.to_string(),
        detail,
    };
    let data_dir = std::fs::canonicalize(base)
        .map_err(|e| CompactError::Input(format!("{}: {e}", base.display())))?;
    let data_dir = data_dir.to_str().ok_or_else(|| {
        CompactError::Input(format!(
            "{}: not UTF-8, so it cannot be named to the server holding it; stop the server \
             and compact alone",
            data_dir.display()
        ))
    })?;
    let body = serde_json::json!({
        "day": args.day,
        "threads": args.threads,
        "data_dir": data_dir,
    });
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let shown = [("x-kibbr-load-token", token)];
    let (status, answer) = crate::cli::http::request(
        address,
        "POST",
        "/compact",
        &shown,
        Some(("application/json", &bytes)),
        Duration::from_secs(60),
    )
    .map_err(|e| server(e.to_string()))?;
    let answer = crate::cli::load::json_of(status, &answer).map_err(server)?;
    if status != 202 {
        return Err(CompactError::Remote(
            answer["error"]
                .as_str()
                .map_or_else(|| format!("answered {status}"), str::to_string),
        ));
    }
    let job = answer["job"]
        .as_u64()
        .ok_or_else(|| server("no job number in its answer".to_string()))?;
    let report = |answer: &serde_json::Value| {
        serde_json::from_value::<Report>(answer["report"].clone())
            .map_err(|e| server(format!("a report this build cannot read ({e})")))
    };
    loop {
        std::thread::sleep(crate::cli::load::POLL);
        let (status, answer) = crate::cli::http::request(
            address,
            "GET",
            &format!("/compact/{job}"),
            &shown,
            None,
            Duration::from_secs(60),
        )
        .map_err(|e| {
            server(format!(
                "{e} while compaction job {job} was running; if the server stopped, each day \
                 is whole either way - rebuilt if its record was written, as it was if not"
            ))
        })?;
        if status != 200 {
            return Err(server(format!(
                "answered {status} about compaction job {job}"
            )));
        }
        let answer = crate::cli::load::json_of(status, &answer).map_err(server)?;
        match answer["state"].as_str() {
            Some("running") => continue,
            Some("done") => return print_report(&report(&answer)?, args.json, out),
            Some("failed") => {
                // The days it finished before the one that failed, if it says so.
                if let Ok(report) = report(&answer) {
                    print_report(&report, args.json, out)?;
                }
                return Err(CompactError::Remote(
                    answer["error"].as_str().unwrap_or("failed").to_string(),
                ));
            }
            other => return Err(server(format!("compaction job {job} in state {other:?}"))),
        }
    }
}

/// The `compact` arm of `cli.rs`: the day checked, the settings loaded, then `command`.
pub fn cli(
    file: &crate::cli::config::File,
    out: &mut dyn io::Write,
    day: Option<String>,
    data_dir: Option<std::path::PathBuf>,
    threads: Option<usize>,
    segment_max_bytes: Option<usize>,
    json: bool,
) -> Result<(), crate::cli::error::CliError> {
    // Before the settings are loaded, which creates the directory they name: a
    // compaction of a path that is not a data directory makes nothing there.
    let base = crate::cli::config::Storage::base_named(file, data_dir.clone());
    let day_number = precheck(day.as_deref(), &base)?;
    let storage = crate::cli::config::Storage::load(
        file,
        true,
        data_dir,
        None,
        None,
        None,
        None,
        segment_max_bytes,
        None,
    )?;
    let args = Args {
        day,
        threads,
        segment_max_bytes,
        json,
    };
    let schema = crate::cli::config::schema(file, None);
    command(args, day_number, storage, schema, out).map_err(crate::cli::error::CliError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_with_no_day_due_prints_nothing_as_lines_and_no_days_as_json() {
        let mut out = Vec::new();
        print_report(
            &Report {
                ok: true,
                days: Vec::new(),
            },
            false,
            &mut out,
        )
        .expect("print");
        assert!(out.is_empty(), "no day due printed {out:?}");
        print_report(
            &Report {
                ok: true,
                days: Vec::new(),
            },
            true,
            &mut out,
        )
        .expect("print");
        assert_eq!(out, b"{\"ok\":true,\"days\":[]}\n");
    }

    /// `LOCK` says `load` too, so a second command gives up at once rather than waiting for
    /// a server still starting.
    #[test]
    fn a_compaction_alone_says_so_in_lock_and_a_second_command_is_refused_at_once() {
        let dir = std::env::temp_dir().join(format!("kibbr-compact-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let storage = crate::cli::config::Storage {
            base_path: Some(dir.clone()),
            trie_file: Some(dir.join("trie.json")),
            ..crate::cli::config::Storage::default()
        };
        let schema = crate::cli::config::Schema::default();
        let args = || Args {
            day: None,
            threads: Some(1),
            segment_max_bytes: None,
            json: false,
        };

        // A directory with no day in it: the run is over at once, and what it wrote to
        // `LOCK` while it ran is still there to read.
        let db = crate::engine::Database::open_offline(
            crate::cli::config::options(&storage, &schema),
            crate::engine::Opener::Compact,
        )
        .expect("the directory, alone");
        let mut out = Vec::new();
        alone(db, &args(), None, &mut out).expect("compact alone");
        assert!(out.is_empty(), "no day was due, and it printed {out:?}");
        let said = crate::engine::holder(&dir).expect("LOCK does not say who held it");
        assert!(
            said.compact && said.load && said.address.is_none(),
            "{said:?}"
        );

        // Those words under a lock that is held are a compaction still running.
        let _held = crate::engine::inspect::DataLock::acquire(&dir).expect("the lock");
        let asked = Instant::now();
        match command(args(), None, storage, schema, &mut out) {
            Err(CompactError::Input(why)) => {
                assert!(why.contains("a compaction running alone"), "{why}")
            }
            other => panic!("a second command beside a compaction alone: {other:?}"),
        }
        assert!(
            asked.elapsed() < Duration::from_secs(5),
            "it waited {:?} for a server that was never starting",
            asked.elapsed()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
