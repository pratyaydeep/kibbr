//! `kibbr compact` - the command: who runs it and what it prints. The rebuild is the
//! store's (`store::disk::compact`) and the run the server crate's
//! (`kibbr_server::compact`). Who holds `LOCK` decides where it runs, as for `kibbr load`:
//! one process writes the manifest, so a server holding the directory is asked
//! (`POST /compact`), and with none the command takes the lock and compacts alone.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::config::LoadStorage as _;

use kibbr_server::compact::{line, plan, run, Report, NO_DISK};
use kibbr_storage::store::disk::{self, CompactOptions};
use kibbr_storage::store::StoreError;

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
    Cli(Box<crate::error::CliError>),
    /// The server that holds the data directory ran the compaction, and it failed there.
    #[error("{0}")]
    Remote(String),
    /// The server that holds the data directory could not be asked.
    #[error("the server at {address}, which holds the data directory: {detail}")]
    Server { address: String, detail: String },
}

impl From<crate::error::CliError> for CompactError {
    fn from(e: crate::error::CliError) -> Self {
        CompactError::Cli(Box::new(e))
    }
}

impl From<kibbr_storage::lock::LockError> for CompactError {
    fn from(e: kibbr_storage::lock::LockError) -> Self {
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
            Some(text) => Some(disk::day_of_name(text).ok_or_else(|| {
                CompactError::Input(format!("--day {text}: not a YYYY-MM-DD day"))
            })?),
        };
    disk::refuse_other_versions(base)?;
    if !kibbr_storage::store::manifest::path_in(base).is_file() {
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
    storage: crate::config::Storage,
    schema: crate::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), CompactError> {
    let base = storage
        .base_path
        .clone()
        .ok_or_else(|| CompactError::Input("no data directory".to_string()))?;
    let waited = Instant::now();
    loop {
        match kibbr_storage::lock::DataLock::acquire(&base) {
            Ok(lock) => return alone(lock, &args, day, &storage, &schema, out),
            Err(kibbr_storage::lock::LockError::Held { path, holder }) => {
                match kibbr_storage::lock::holder(&base) {
                    Some(kibbr_storage::lock::Holder {
                        address: Some(address),
                        token,
                        ..
                    }) => {
                        let token = token.unwrap_or_default();
                        return client(&address, &token, &base, &args, out);
                    }
                    // A load or another compaction running alone: no server to ask.
                    Some(kibbr_storage::lock::Holder { load: true, .. }) => {
                        return Err(kibbr_storage::lock::LockError::Held { path, holder }.into())
                    }
                    // A server still starting: it writes its address once it can answer.
                    _ if waited.elapsed() < crate::cmd::load::SERVER_STARTING_WAIT => {
                        std::thread::sleep(crate::cmd::load::POLL);
                    }
                    _ => return Err(kibbr_storage::lock::LockError::Held { path, holder }.into()),
                }
            }
            Err(e) => return Err(e.into()),
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
    written.map_err(|e| crate::error::CliError::Write(e).into())
}

/// The compaction in this process, holding the directory's lock.
fn alone(
    mut lock: kibbr_storage::lock::DataLock,
    args: &Args,
    day: Option<i64>,
    storage: &crate::config::Storage,
    schema: &crate::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), CompactError> {
    let base = storage.base_path.clone().unwrap_or_default();
    let _ = lock.announce(&kibbr_storage::lock::Holder::compact());
    disk::refuse_other_versions(&base)?;
    let (_trie, stores) =
        kibbr_server::open_durable(storage, schema).map_err(crate::error::CliError::from)?;
    let store = stores.disk_store().ok_or(CompactError::NoDisk)?;
    let opts = CompactOptions {
        threads: args
            .threads
            .unwrap_or_else(crate::cmd::load::alone_threads)
            .max(1),
        cancel: Arc::default(),
    };
    let days = plan(store, day);
    let ran = run(store, &days, &opts, &mut |_, result| {
        // One day asked for is one error, and the command ends on it. Of the days that
        // were due, each failure is said where it happened and the command ends on a count.
        if let (None, Err(e)) = (day, result) {
            crate::error::report(e);
        }
    });
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
    let (status, answer) = crate::client::request(
        address,
        "POST",
        "/compact",
        &shown,
        Some(("application/json", &bytes)),
        Duration::from_secs(60),
    )
    .map_err(|e| server(e.to_string()))?;
    let answer = crate::cmd::load::json_of(status, &answer).map_err(server)?;
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
        std::thread::sleep(crate::cmd::load::POLL);
        let (status, answer) = crate::client::request(
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
        let answer = crate::cmd::load::json_of(status, &answer).map_err(server)?;
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

/// The `compact` arm of `main.rs`: the day checked, the settings loaded, then `command`.
pub fn cli(
    file: &crate::config::File,
    out: &mut dyn io::Write,
    day: Option<String>,
    data_dir: Option<std::path::PathBuf>,
    threads: Option<usize>,
    segment_max_bytes: Option<usize>,
    json: bool,
) -> Result<(), crate::error::CliError> {
    // Before the settings are loaded, which creates the directory they name: a
    // compaction of a path that is not a data directory makes nothing there.
    let base = crate::config::Storage::base_named(file, data_dir.clone());
    let day_number = precheck(day.as_deref(), &base)?;
    let storage = crate::config::Storage::load(
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
    let schema = crate::config::schema(file, None);
    command(args, day_number, storage, schema, out).map_err(crate::error::CliError::from)
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
        let storage = crate::config::Storage {
            base_path: Some(dir.clone()),
            trie_file: Some(dir.join("trie.json")),
            ..crate::config::Storage::default()
        };
        let schema = crate::config::Schema::default();
        let args = || Args {
            day: None,
            threads: Some(1),
            segment_max_bytes: None,
            json: false,
        };

        // A directory with no day in it: the run is over at once, and what it wrote to
        // `LOCK` while it ran is still there to read.
        let lock = kibbr_storage::lock::DataLock::acquire(&dir).expect("the lock");
        let mut out = Vec::new();
        alone(lock, &args(), None, &storage, &schema, &mut out).expect("compact alone");
        assert!(out.is_empty(), "no day was due, and it printed {out:?}");
        let said = kibbr_storage::lock::holder(&dir).expect("LOCK does not say who held it");
        assert!(
            said.compact && said.load && said.address.is_none(),
            "{said:?}"
        );

        // Those words under a lock that is held are a compaction still running.
        let _held = kibbr_storage::lock::DataLock::acquire(&dir).expect("the lock");
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
