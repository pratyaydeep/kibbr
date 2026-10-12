//! `kibbr load` - the command.
//!
//! The load itself is the server crate's (`kibbr_server::load`): the inputs, the arrival
//! check, the staging and the one manifest record. What is here is who runs it and what
//! it prints.
//!
//! **Who holds `LOCK` decides where the load runs**, because one process writes the
//! catalog and the manifest: a server holding the directory is asked to run it
//! (`POST /load`) and polled; with none, the command takes the lock and runs it alone.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use thiserror::Error;

pub use kibbr_server::load::*;

use crate::config::LoadStorage as _;

/// What `kibbr load` can fail with that the load itself cannot: the server that holds
/// the data directory, asked to run it. Carried in the load's own error as its `Caller`,
/// so a command has one error to return and each still prints its own sentence.
#[derive(Debug, Error)]
pub enum Asked {
    /// The server that holds the data directory ran the load, and it failed there.
    #[error("{0}")]
    Remote(String),
    /// The server that holds the data directory could not be asked.
    #[error("the server at {address}, which holds the data directory: {detail}")]
    Server { address: String, detail: String },
}

impl From<Asked> for LoadError {
    fn from(e: Asked) -> Self {
        LoadError::Caller(Box::new(e))
    }
}

impl From<crate::error::CliError> for LoadError {
    fn from(e: crate::error::CliError) -> Self {
        LoadError::Caller(Box::new(e))
    }
}


// ── The command ────────────────────────────────────────────────────────────────────

/// `kibbr load`, as the command line gave it.
#[derive(Debug)]
pub struct Args {
    pub inputs: Vec<PathBuf>,
    pub replace_day: Option<String>,
    pub max_mb_per_sec: Option<f64>,
    pub threads: Option<usize>,
    pub load_buffer_bytes: Option<usize>,
    pub received_at: Option<String>,
    pub proceed: bool,
    pub max_arrival_share: Option<f64>,
    pub time_columns: Option<Vec<String>>,
    pub json: bool,
}

/// How long `kibbr load` waits for a server that holds the directory's lock to say where
/// it listens: a server writes its address once it can answer, a moment after it starts.
pub(crate) const SERVER_STARTING_WAIT: Duration = Duration::from_secs(30);

/// How often a client asks a server how its load is going.
pub(crate) const POLL: Duration = Duration::from_millis(100);

/// Run `kibbr load`: alone, if no process holds the data directory; otherwise as a
/// client of the server that does.
///
/// **Who holds `LOCK` decides where the load runs**, because one process writes the
/// catalog and the manifest. A server is asked to run the load - its trie already has the
/// catalog's every template, and its leaves take the load's rows the moment it commits.
/// With no server, the command takes the lock and opens the directory exactly as a
/// server's start would, recovery included, so an interrupted load is finished or undone
/// here too.
pub fn command(
    args: Args,
    storage: crate::config::Storage,
    schema: crate::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), LoadError> {
    let base = storage
        .base_path
        .clone()
        .ok_or_else(|| LoadError::Input("no data directory".to_string()))?;
    let replace_day = match args.replace_day.as_deref() {
        None => None,
        Some(text) => Some(
            kibbr_storage::store::disk::day_of_name(text).ok_or_else(|| {
                LoadError::Input(format!("--replace-day {text}: not a YYYY-MM-DD day"))
            })?,
        ),
    };
    let received_at = match args.received_at.as_deref() {
        None => Utc::now(),
        Some(text) => DateTime::parse_from_rfc3339(text)
            .map_err(|e| LoadError::Input(format!("--received-at {text}: {e}")))?
            .with_timezone(&Utc),
    };
    let share = args.max_arrival_share.unwrap_or(DEFAULT_MAX_ARRIVAL_SHARE);
    if !(share.is_finite() && share >= 0.0) {
        return Err(LoadError::Input(format!(
            "--max-arrival-share {share}: not a percentage"
        )));
    }
    // Refused here as well as by a server, so a client load fails before it is sent.
    let load_buffer_bytes = args.load_buffer_bytes.unwrap_or(DEFAULT_LOAD_BUFFER_BYTES);
    if load_buffer_bytes == 0 {
        return Err(LoadError::Input(
            "--load-buffer-bytes 0: a load has to hold at least one row".to_string(),
        ));
    }
    let waited = Instant::now();
    loop {
        match kibbr_storage::lock::DataLock::acquire(&base) {
            Ok(lock) => {
                let opts = Options {
                    inputs: Vec::new(),
                    replace_day,
                    max_bytes_per_sec: args.max_mb_per_sec.map(|mb| mb * 1_000_000.0),
                    threads: args.threads.unwrap_or_else(alone_threads).max(1),
                    load_buffer_bytes,
                    received_at,
                    proceed: args.proceed,
                    max_arrival_share: share,
                };
                return alone(lock, &args, opts, &storage, &schema, out);
            }
            Err(kibbr_storage::lock::LockError::Held { path, holder }) => {
                match kibbr_storage::lock::holder(&base) {
                    Some(kibbr_storage::lock::Holder {
                        address: Some(address),
                        token,
                        ..
                    }) => {
                        let token = token.unwrap_or_default();
                        return client(&address, &token, &base, &args, share, out);
                    }
                    Some(kibbr_storage::lock::Holder { load: true, .. }) => {
                        return Err(kibbr_storage::lock::LockError::Held { path, holder }.into())
                    }
                    // A server still starting: it writes its address once it can answer.
                    _ if waited.elapsed() < SERVER_STARTING_WAIT => {
                        std::thread::sleep(POLL);
                    }
                    _ => return Err(kibbr_storage::lock::LockError::Held { path, holder }.into()),
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
}


fn print_report(report: &Report, json: bool, out: &mut dyn io::Write) -> Result<(), LoadError> {
    let text = if json {
        serde_json::to_string(report).unwrap_or_default()
    } else {
        report.summary()
    };
    writeln!(out, "{text}").map_err(|e| crate::error::CliError::Write(e).into())
}

/// The load in this process, holding the directory's lock.
fn alone(
    mut lock: kibbr_storage::lock::DataLock,
    args: &Args,
    mut opts: Options,
    storage: &crate::config::Storage,
    schema: &crate::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), LoadError> {
    let base = storage.base_path.clone().unwrap_or_default();
    let _ = lock.announce(&kibbr_storage::lock::Holder::load());
    kibbr_storage::store::disk::refuse_other_versions(&base)?;
    let (trie, stores) =
        kibbr_server::open_durable(storage, schema).map_err(crate::error::CliError::from)?;
    // Standard input is spooled inside `staging/`, which the next start deletes: a load
    // that dies leaves nothing behind that anything names.
    let staging = base.join(kibbr_storage::store::disk::STAGING_DIR);
    let spool = staging.join(format!("stdin-{}.spool", std::process::id()));
    if args.inputs.iter().any(|a| a.as_os_str() == "-") {
        fs::create_dir_all(&staging).map_err(|source| LoadError::Read {
            path: staging.clone(),
            source,
        })?;
    }
    let result = resolve(&args.inputs, Some(&spool)).and_then(|inputs| {
        opts.inputs = inputs;
        run(&trie, &stores, &opts, &Control::default(), &mut |e| {
            crate::error::report(e)
        })
    });
    let _ = fs::remove_file(&spool);
    print_report(&result?, args.json, out)
}

/// The load run by the server that holds the directory: asked over HTTP, then polled.
///
/// The inputs are resolved here - the command line's directories, and its standard input,
/// spooled to a temporary file - and sent as absolute paths: the server reads them itself,
/// on the same machine, since it holds a lock on a directory here.
fn client(
    address: &str,
    token: &str,
    base: &Path,
    args: &Args,
    share: f64,
    out: &mut dyn io::Write,
) -> Result<(), LoadError> {
    if args.time_columns.is_some() {
        tracing::warn!(
            "--time-columns is ignored: the server at {address} holds this data \
             directory, and a template's time source is chosen by the server's own list"
        );
    }
    // Spooled inside the data directory's `staging/`, which the server can read because it
    // owns it - a temporary directory of the client's may be one the server cannot see - and
    // which the server's next start clears if this client is killed before it removes it.
    let staging = base.join(kibbr_storage::store::disk::STAGING_DIR);
    let spool = staging.join(format!(
        "stdin-{}-{}.spool",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    if args.inputs.iter().any(|a| a.as_os_str() == "-") {
        fs::create_dir_all(&staging).map_err(|source| LoadError::Read {
            path: staging.clone(),
            source,
        })?;
    }
    let result = client_run(address, token, base, args, share, &spool);
    let _ = fs::remove_file(&spool);
    let report = result?;
    for e in &report.errors {
        eprintln!("kibbr: line {}: {}", e.line, e.error);
    }
    if report.errors_omitted > 0 {
        eprintln!(
            "kibbr: {} more rejected line(s) not listed",
            report.errors_omitted
        );
    }
    print_report(&report, args.json, out)
}

/// A server's answer as JSON, or what it said instead, with its status - a proxy's error
/// page or a body-size refusal is not JSON, and "not JSON" alone would hide which it was.
pub(crate) fn json_of(status: u16, answer: &[u8]) -> Result<serde_json::Value, String> {
    serde_json::from_slice(answer).map_err(|_| {
        let text = String::from_utf8_lossy(&answer[..answer.len().min(300)]);
        format!("answered {status}: {}", text.trim())
    })
}

fn client_run(
    address: &str,
    token: &str,
    base: &Path,
    args: &Args,
    share: f64,
    spool: &Path,
) -> Result<Report, LoadError> {
    let server = |detail: String| LoadError::from(Asked::Server {
        address: address.to_string(),
        detail,
    });
    let inputs = resolve(&args.inputs, Some(spool))?;
    // JSON carries text: a path that is not UTF-8 cannot be sent, and is named rather than
    // mangled into another file's name.
    let text = |path: &Path| {
        path.to_str().map(str::to_string).ok_or_else(|| {
            LoadError::Input(format!(
                "{}: not UTF-8, so it cannot be sent to the server holding the data \
                 directory; rename it, or stop the server and load alone",
                path.display()
            ))
        })
    };
    let mut sent = Vec::with_capacity(inputs.len());
    for input in &inputs {
        sent.push(serde_json::json!({"path": text(&input.path)?, "name": input.name}));
    }
    let data_dir = fs::canonicalize(base).map_err(|source| LoadError::Read {
        path: base.to_path_buf(),
        source,
    })?;
    let body = serde_json::json!({
        "inputs": sent,
        "replace_day": args.replace_day,
        "max_mb_per_sec": args.max_mb_per_sec,
        "threads": args.threads,
        "load_buffer_bytes": args.load_buffer_bytes,
        "received_at": args.received_at,
        "proceed": args.proceed,
        "max_arrival_share": share,
        "data_dir": text(&data_dir)?,
    });
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let shown = [("x-kibbr-load-token", token)];
    let (status, answer) = crate::client::request(
        address,
        "POST",
        "/load",
        &shown,
        Some(("application/json", &bytes)),
        Duration::from_secs(60),
    )
    .map_err(|e| server(e.to_string()))?;
    let answer = json_of(status, &answer).map_err(server)?;
    if status != 202 {
        return Err(LoadError::from(Asked::Remote(
            answer["error"]
                .as_str()
                .map_or_else(|| format!("answered {status}"), str::to_string),
        )));
    }
    let job = answer["job"]
        .as_u64()
        .ok_or_else(|| server("no job number in its answer".to_string()))?;
    loop {
        std::thread::sleep(POLL);
        let (status, answer) = crate::client::request(
            address,
            "GET",
            &format!("/load/{job}"),
            &shown,
            None,
            Duration::from_secs(60),
        )
        .map_err(|e| {
            server(format!(
                "{e} while load job {job} was running; if the server stopped, the load either \
                 committed before it did or left nothing behind"
            ))
        })?;
        if status != 200 {
            return Err(server(format!("answered {status} about load job {job}")));
        }
        let answer = json_of(status, &answer).map_err(server)?;
        match answer["state"].as_str() {
            Some("running") => continue,
            Some("done") => {
                return serde_json::from_value(answer["report"].clone())
                    .map_err(|e| server(format!("a report this build cannot read ({e})")))
            }
            Some("failed") => {
                return Err(LoadError::from(Asked::Remote(
                    answer["error"].as_str().unwrap_or("failed").to_string(),
                )))
            }
            other => return Err(server(format!("load job {job} in state {other:?}"))),
        }
    }
}

/// The `load` arm of `main.rs`: the settings loaded, then `command`.
#[allow(clippy::too_many_arguments)]
pub fn cli(
    file: &crate::config::File,
    out: &mut dyn io::Write,
    inputs: Vec<PathBuf>,
    data_dir: Option<PathBuf>,
    replace_day: Option<String>,
    max_mb_per_sec: Option<f64>,
    threads: Option<usize>,
    load_buffer_bytes: Option<usize>,
    received_at: Option<String>,
    proceed: bool,
    max_arrival_share: Option<f64>,
    time_columns: Option<Vec<String>>,
    segment_max_bytes: Option<usize>,
    json: bool,
) -> Result<(), crate::error::CliError> {
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
    let schema = crate::config::schema(file, time_columns.clone());
    let args = Args {
        inputs,
        replace_day,
        max_mb_per_sec,
        threads,
        load_buffer_bytes,
        received_at,
        proceed,
        max_arrival_share,
        time_columns,
        json,
    };
    command(args, storage, schema, out).map_err(crate::error::CliError::from)
}
