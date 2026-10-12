//! `kibbr templates`, `kibbr stats` and `kibbr health`: what a running server answers
//! `GET /templates`, `GET /stats` and `GET /health` with, asked for and printed.
//!
//! `health` is the one a machine runs: the image's `HEALTHCHECK`, which has no curl to
//! ask with. It prints a word and exits 0 while the server answers `"ok":true`.
//!
//! Both were HTTP only. They stay the server's to answer - the row counts and the
//! counters are in its memory, and a second process opening the directory would be
//! refused its `LOCK` - so these are two requests and what to do with the answers: a
//! table or a line a number for a person, or with `--json` the body as the server sent
//! it, for whatever reads it next.

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::Value;
use thiserror::Error;

use crate::config::{self, File};
use crate::error::CliError;
use kibbr_core::fmt::commas;

/// How long a server is given to answer. Both answers are built from memory.
const TIMEOUT: Duration = Duration::from_secs(10);

/// How long `health` gives it: under the five seconds the image's `HEALTHCHECK` waits
/// before it calls the check itself failed, so that a server that does not answer is
/// said by this command, with its reason, and not by a kill.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug, Error)]
pub enum AskError {
    /// Nothing answered. Says where the address came from, since the remedy differs: a
    /// stale `LOCK` is a server that is gone, and the default is one never started here.
    #[error(
        "no server answered at {address} ({found}): {source}. `templates`, `stats` and \
         `health` ask a running `kibbr serve`; --addr names one, or --data-dir the directory \
         it holds"
    )]
    Unreachable {
        address: String,
        found: String,
        #[source]
        source: std::io::Error,
    },

    #[error("the server at {address} answered {path} with {status}: {body}")]
    Refused {
        address: String,
        path: &'static str,
        status: u16,
        body: String,
    },

    #[error("the server at {address} answered {path} with something that is not JSON: {detail}")]
    NotJson {
        address: String,
        path: &'static str,
        detail: String,
    },

    /// `/health` answered 200 and did not say `"ok":true`: not this server, or not one
    /// that is well.
    #[error("the server at {address} answered /health without `\"ok\":true`: {body}")]
    NotOk { address: String, body: String },
}

/// The server to ask, and how it was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Server {
    pub address: String,
    pub found: String,
}

/// Which server: `--addr`; or the one holding the data directory, whose `LOCK` says
/// where it listens - a server on `--port 0` is found no other way; or, with neither,
/// the address a server here would listen on by its settings.
///
/// The `LOCK` is read and not taken: this writes nothing, and taking it would refuse a
/// server that was starting.
pub fn server(
    file: &File,
    addr: Option<String>,
    data_dir: Option<PathBuf>,
) -> Result<Server, CliError> {
    if let Some(address) = addr {
        return Ok(Server {
            address,
            found: "--addr".to_string(),
        });
    }
    let base = <config::Storage as config::LoadStorage>::base_named(file, data_dir);
    if let Some(address) = kibbr_storage::lock::holder(&base).and_then(|holder| holder.address) {
        return Ok(Server {
            address,
            found: format!("from {}", base.join(kibbr_storage::lock::LOCK_FILE).display()),
        });
    }
    let (host, port) = config::listen(file, None, None)?;
    // An address of every interface is reached on loopback from the machine it is on.
    let address = match format!("{host}:{port}").parse::<SocketAddr>() {
        Ok(listen) if listen.ip().is_unspecified() => format!("127.0.0.1:{port}"),
        _ => format!("{host}:{port}"),
    };
    Ok(Server {
        address,
        found: format!(
            "no server's LOCK in {}, so the address in the settings",
            base.display()
        ),
    })
}

/// `GET path`, answered 200 with JSON: the body as it was sent, and as it reads.
fn get(server: &Server, path: &'static str) -> Result<(String, Value), AskError> {
    get_within(server, path, TIMEOUT)
}

/// `get`, with how long the server is given.
fn get_within(
    server: &Server,
    path: &'static str,
    timeout: Duration,
) -> Result<(String, Value), AskError> {
    let (status, body) = crate::client::request(&server.address, "GET", path, &[], None, timeout)
        .map_err(|source| AskError::Unreachable {
            address: server.address.clone(),
            found: server.found.clone(),
            source,
        })?;
    let text = String::from_utf8_lossy(&body).trim().to_string();
    if status != 200 {
        return Err(AskError::Refused {
            address: server.address.clone(),
            path,
            status,
            body: text.chars().take(300).collect(),
        });
    }
    let read = serde_json::from_str(&text).map_err(|e| AskError::NotJson {
        address: server.address.clone(),
        path,
        detail: e.to_string(),
    })?;
    Ok((text, read))
}

/// `kibbr health`: `ok`, or `degraded` and why, while the server answers `/health` with
/// `"ok":true`; an error, and so a non-zero exit, when it does not answer, answers
/// another status, or answers something else.
///
/// **A degraded server is a healthy one here, as it is to `/health`**: it cannot write,
/// it still answers queries, and restarting it does not empty a disk. What this command
/// exits on is what an orchestrator should restart for - a process that does not answer.
pub fn health<W: Write>(out: &mut W, server: &Server, json: bool) -> Result<(), CliError> {
    let (text, answer) = get_within(server, "/health", HEALTH_TIMEOUT)?;
    if answer["ok"] != Value::Bool(true) {
        return Err(AskError::NotOk {
            address: server.address.clone(),
            body: text.chars().take(300).collect(),
        }
        .into());
    }
    if json {
        return writeln!(out, "{text}").map_err(CliError::Write);
    }
    writeln!(out, "{}", health_line(&answer)).map_err(CliError::Write)
}

/// The answer as a line: its status, and its detail after a colon where it has one.
fn health_line(answer: &Value) -> String {
    let status = answer["status"].as_str().unwrap_or("ok");
    match answer["detail"].as_str() {
        Some(detail) => format!("{status}: {detail}"),
        None => status.to_string(),
    }
}

/// `kibbr templates`: every template's id, rows, time source and path, in the order the
/// server lists them; or the server's JSON.
pub fn templates<W: Write>(out: &mut W, server: &Server, json: bool) -> Result<(), CliError> {
    let (text, answer) = get(server, "/templates")?;
    if json {
        return writeln!(out, "{text}").map_err(CliError::Write);
    }
    write!(out, "{}", templates_table(&answer)).map_err(CliError::Write)
}

/// The table. A value the answer does not hold is a dash, and never a guess: this is
/// printed from whatever a server of any build sent.
fn templates_table(answer: &Value) -> String {
    let empty = Vec::new();
    let listed = answer["templates"].as_array().unwrap_or(&empty);
    let rows: Vec<[String; 4]> = listed
        .iter()
        .map(|template| {
            let number = |key: &str| {
                template[key]
                    .as_u64()
                    .map_or_else(|| "-".to_string(), |n| n.to_string())
            };
            let path: Vec<&str> = template["path"]
                .as_array()
                .map(|tokens| tokens.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            [
                number("template_id"),
                template["rows"]
                    .as_u64()
                    .map_or_else(|| "-".to_string(), commas),
                template["time_source"].as_str().unwrap_or("-").to_string(),
                path.join(", "),
            ]
        })
        .collect();
    let heads = ["ID", "ROWS", "TIME", "PATH"];
    let width = |column: usize| {
        rows.iter()
            .map(|row| row[column].chars().count())
            .chain([heads[column].len()])
            .max()
            .unwrap_or(0)
    };
    let (id, count, time) = (width(0), width(1), width(2));
    let mut table = String::new();
    let mut line = |cells: [&str; 4]| {
        let text = format!(
            "{:>id$}  {:>count$}  {:<time$}  {}",
            cells[0], cells[1], cells[2], cells[3]
        );
        table.push_str(text.trim_end());
        table.push('\n');
    };
    line(heads);
    for row in &rows {
        line([&row[0], &row[1], &row[2], &row[3]]);
    }
    let total = rows.len() as u64;
    table.push_str(&format!(
        "{} template{}\n",
        commas(total),
        if total == 1 { "" } else { "s" }
    ));
    table
}

/// `kibbr stats`: every number the server reports, a line each under its dotted name;
/// or the server's JSON.
pub fn stats<W: Write>(out: &mut W, server: &Server, json: bool) -> Result<(), CliError> {
    let (text, answer) = get(server, "/stats")?;
    if json {
        return writeln!(out, "{text}").map_err(CliError::Write);
    }
    write!(out, "{}", stats_lines(&answer)).map_err(CliError::Write)
}

/// The answer flattened: `ingest.lines_read  1204`, in the order the server wrote it.
/// Nothing is renamed, rounded or left out, so a name here is the path to the same
/// value in `--json` and in `GET /stats`.
fn stats_lines(answer: &Value) -> String {
    fn walk(name: String, value: &Value, lines: &mut Vec<(String, String)>) {
        match value {
            Value::Object(members) if !members.is_empty() => {
                for (key, member) in members {
                    let under = if name.is_empty() {
                        key.clone()
                    } else {
                        format!("{name}.{key}")
                    };
                    walk(under, member, lines);
                }
            }
            Value::Array(items) if !items.is_empty() => {
                for (at, item) in items.iter().enumerate() {
                    walk(format!("{name}.{at}"), item, lines);
                }
            }
            Value::String(text) => lines.push((name, text.clone())),
            other => lines.push((name, other.to_string())),
        }
    }
    let mut lines = Vec::new();
    walk(String::new(), answer, &mut lines);
    let width = lines.iter().map(|(name, _)| name.len()).max().unwrap_or(0);
    lines
        .iter()
        .map(|(name, value)| format!("{name:<width$}  {value}\n"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// **The table is the answer's templates, a line each, and what the answer does not
    /// hold is a dash.**
    #[test]
    fn the_table_lists_each_template_and_dashes_what_is_missing() {
        let answer = json!({"count": 2, "templates": [
            {"template_id": 101, "rows": 1204, "time_source": "column:ts",
             "path": ["event=login", "ts", "user"]},
            {"template_id": 102, "path": []},
        ]});
        assert_eq!(
            templates_table(&answer),
            " ID   ROWS  TIME       PATH\n\
             101  1,204  column:ts  event=login, ts, user\n\
             102      -  -\n\
             2 templates\n"
        );
        assert_eq!(
            templates_table(&json!({"templates": []})),
            "ID  ROWS  TIME  PATH\n0 templates\n"
        );
    }

    /// **Every value is a line under its path, in the server's order, as it was sent.**
    #[test]
    fn stats_are_a_line_a_value_under_its_dotted_name() {
        let answer = json!({
            "uptime_seconds": 1.5,
            "storage": {"mode": "disk", "last_write_error": null, "days": []},
            "shed": {"by": [3, 0]},
        });
        assert_eq!(
            stats_lines(&answer),
            "uptime_seconds            1.5\n\
             storage.mode              disk\n\
             storage.last_write_error  null\n\
             storage.days              []\n\
             shed.by.0                 3\n\
             shed.by.1                 0\n"
        );
    }

    /// **Health is the server's status as a word, with what is wrong after it when
    /// something is.**
    #[test]
    fn health_is_a_word_and_then_what_is_wrong() {
        assert_eq!(health_line(&json!({"ok": true, "status": "ok"})), "ok");
        assert_eq!(
            health_line(&json!({"ok": true, "status": "degraded", "detail": "the disk is full"})),
            "degraded: the disk is full"
        );
        assert_eq!(health_line(&json!({"ok": true})), "ok");
    }

    /// **`--addr` is the server asked, whatever the directory says; and with no `--addr`
    /// it is the one whose `LOCK` the directory holds.** The address of the settings,
    /// under both, is `tests/operations.rs`'s: it reads a variable, which is the
    /// process's and not one test's.
    #[test]
    fn the_server_is_addr_and_then_the_one_the_directory_names() {
        let none = std::env::temp_dir().join(format!("kibbr-ask-none-{}", std::process::id()));
        let file = File::parse("[api]\nnative_addr = \"0.0.0.0:7811\"\n").expect("a file");

        let named = server(&file, Some("10.0.0.9:1".to_string()), Some(none));
        assert_eq!(named.expect("named").address, "10.0.0.9:1");

        let held = std::env::temp_dir().join(format!("kibbr-ask-held-{}", std::process::id()));
        std::fs::create_dir_all(&held).expect("mkdir");
        std::fs::write(
            held.join(kibbr_storage::lock::LOCK_FILE),
            r#"{"pid":1,"address":"127.0.0.1:40123"}"#,
        )
        .expect("a LOCK");
        let by_lock = server(&file, None, Some(held.clone())).expect("by its LOCK");
        assert_eq!(by_lock.address, "127.0.0.1:40123");
        let _ = std::fs::remove_dir_all(&held);
    }
}
