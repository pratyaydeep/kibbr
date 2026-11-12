// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! kibbr's own log: every lifecycle line, warning and error is a `tracing` event, and the
//! one subscriber here writes each to stderr as a line of text or of JSON. As text a line
//! stays byte for byte what scripts, tests and `kibbr bench` read off a server's stderr.
//! A command's data on stdout is not a log line. A subscriber of our own rather than
//! `tracing-subscriber`: a write that fails has to be dropped and not panic - see `write`.

use std::fmt;
use std::io::Write;
use std::sync::OnceLock;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{span, Event, Level, Metadata};

use crate::cli::config::{Log, LogFormat, LogLevel};

/// What `install` was given. Unset until then - in the tests of this crate, which
/// install nothing, and in a process that failed before it had read its settings.
static INSTALLED: OnceLock<Log> = OnceLock::new();

/// Start writing the process's log as `log` says, once, since a process has one stderr
/// and one answer to how it is written.
pub fn install(log: Log) {
    if INSTALLED.set(log).is_ok() {
        // An error only where a global subscriber is already set, and nothing here sets
        // another.
        let _ = tracing::subscriber::set_global_default(Lines { log });
    }
}

/// Whether log lines are JSON, which `failed` asks so an error is one event like any other.
pub fn json() -> bool {
    INSTALLED
        .get()
        .is_some_and(|log| log.format == LogFormat::Json)
}

/// Report what ended the command, the error and every cause under it: `error::report`'s
/// lines as text, one `error` event as JSON.
pub fn failed(e: &(dyn std::error::Error + 'static)) {
    if json() {
        tracing::error!("{}", crate::cli::error::chain(e));
    } else {
        crate::cli::error::report(e);
    }
}

/// One line to stderr, dropped if it cannot be written: `eprintln!` panics when stderr is
/// gone, and that would end the work for want of somewhere to say it was being done.
fn write(line: &str) {
    let _ = writeln!(std::io::stderr().lock(), "{line}");
}

fn level_of(level: LogLevel) -> Level {
    match level {
        LogLevel::Trace => Level::TRACE,
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Info => Level::INFO,
        LogLevel::Warn => Level::WARN,
        LogLevel::Error => Level::ERROR,
    }
}

/// An event as the line of text it always was: the sentence, with its level before it
/// unless it is `info`'s.
fn text_line(level: Level, message: &str) -> String {
    match level {
        Level::INFO => format!("kibbr: {message}"),
        level => format!("kibbr: {level} {message}"),
    }
}

/// An event as one JSON object: when, how loud, from which module, the sentence, and
/// then the event's own fields under their own names.
fn json_line(
    ts: &str,
    level: Level,
    target: &str,
    message: &str,
    fields: Vec<(&'static str, Value)>,
) -> String {
    let mut line = Map::new();
    line.insert("ts".into(), ts.into());
    line.insert("level".into(), level.as_str().to_lowercase().into());
    line.insert("target".into(), target.into());
    line.insert("msg".into(), message.into());
    for (name, value) in fields {
        // The four above are the line's own, and an event's field does not replace one.
        line.entry(name).or_insert(value);
    }
    Value::Object(line).to_string()
}

/// The subscriber: every event at or above the level, one line each.
struct Lines {
    log: Log,
}

impl Lines {
    /// Whether an event is the server's banner (`crate::server::BANNER`), written bare as
    /// text; as JSON it is an event like any other.
    fn banner(&self, metadata: &Metadata<'_>) -> bool {
        self.log.format == LogFormat::Text && metadata.target() == crate::server::BANNER
    }
}

/// An event's message, and its other fields as JSON values.
#[derive(Default)]
struct Read {
    message: String,
    fields: Vec<(&'static str, Value)>,
}

impl Visit for Read {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields
                .push((field.name(), format!("{value:?}").into()));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.push((field.name(), value.into()));
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.push((field.name(), value.into()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.push((field.name(), value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields.push((field.name(), value.into()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.push((field.name(), value.into()));
    }
}

impl tracing::Subscriber for Lines {
    /// At or above the level - and the server's banner at any level: it is the one place a
    /// person is told what survives a stop.
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.banner(metadata) || *metadata.level() <= level_of(self.log.level)
    }

    /// Never under `info`, which is the banner's: a hint under it would drop that event
    /// before `enabled` was asked. What is written is still what `enabled` passes.
    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(level_of(self.log.level).max(Level::INFO).into())
    }

    fn event(&self, event: &Event<'_>) {
        let mut read = Read::default();
        event.record(&mut read);
        let metadata = event.metadata();
        match self.log.format {
            // Already the paragraph: no `kibbr: ` before it, and nothing after.
            LogFormat::Text if self.banner(metadata) => write(&read.message),
            LogFormat::Text => write(&text_line(*metadata.level(), &read.message)),
            LogFormat::Json => {
                let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                write(&json_line(
                    &ts,
                    *metadata.level(),
                    metadata.target(),
                    &read.message,
                    read.fields,
                ));
            }
        }
    }

    // No spans: nothing here opens one. One id for any a dependency might, and nothing
    // kept of it.
    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

#[cfg(test)]
mod tests;
