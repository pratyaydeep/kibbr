//! kibbr's own log: every lifecycle line, warning and error of the process is a
//! `tracing` event, and the one subscriber here writes each to stderr as a line - of
//! text, as the lines always were, or of JSON.
//!
//! **As text a line is what it was before there was a subscriber:** `kibbr: ` and the
//! sentence, with `WARN` or `ERROR` before a warning's or an error's. Scripts and tests
//! read those lines off a server's stderr, and `kibbr bench` reads the banner. As JSON a
//! line is one object - `ts`, `level`, `target`, `msg`, then any fields the event
//! carries - for whatever collects a container's stderr.
//!
//! **A command's data is not a log line.** What `query`, `parse`, `explain`, `gen-logs`
//! and the rest print on stdout is theirs, and so is what a command says to the person
//! who ran it about its input - a rejected line by its number, `demo`'s timing, `bench`'s
//! progress. Those are stderr as they were, in either format.
//!
//! A subscriber of our own rather than `tracing-subscriber`: there are no spans here, the
//! text line has to be byte for byte the one it replaces, and a write that fails has to
//! be dropped and not panic - see `write`.

use std::fmt;
use std::io::Write;
use std::sync::OnceLock;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{span, Event, Level, Metadata};

use crate::config::{Log, LogFormat, LogLevel};

/// What `install` was given. Unset until then - in the tests of this crate, which
/// install nothing, and in a process that failed before it had read its settings.
static INSTALLED: OnceLock<Log> = OnceLock::new();

/// Start writing the process's log as `log` says. Once: a second call changes nothing,
/// since a process has one stderr and one answer to how it is written.
pub fn install(log: Log) {
    if INSTALLED.set(log).is_ok() {
        // An error only where a global subscriber is already set, and nothing here sets
        // another.
        let _ = tracing::subscriber::set_global_default(Lines { log });
    }
}

/// Whether log lines are JSON. What the banner asks: as text it is a paragraph for a
/// person, and as JSON it has to be one event like every other line.
pub fn json() -> bool {
    INSTALLED
        .get()
        .is_some_and(|log| log.format == LogFormat::Json)
}

/// What ended the command: the error and every cause under it. As text, the lines
/// `error::report` always printed; as JSON, one `error` event with the chain as its
/// message, so that a collector reading objects is not handed a sentence.
pub fn failed(e: &(dyn std::error::Error + 'static)) {
    if json() {
        tracing::error!("{}", crate::error::chain(e));
    } else {
        crate::error::report(e);
    }
}

/// One line to stderr. **A line that cannot be written is dropped**: `eprintln!` panics
/// when stderr is gone - the collector at the other end of the pipe went away - and an
/// event is written from the flush task, from the write-ahead log's thread and from the
/// threads a load and a compaction run on. The panic would end the work for want of
/// somewhere to say it was being done.
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
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= level_of(self.log.level)
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(level_of(self.log.level).into())
    }

    fn event(&self, event: &Event<'_>) {
        let mut read = Read::default();
        event.record(&mut read);
        let metadata = event.metadata();
        match self.log.format {
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
mod tests {
    use super::*;

    /// **As text, a line is the line that was written before there was a subscriber**:
    /// the sentence after `kibbr: `, and `WARN` or `ERROR` before a warning's or an
    /// error's. What the scripts and the tests read off a server's stderr.
    #[test]
    fn a_text_line_is_the_line_it_always_was() {
        assert_eq!(text_line(Level::INFO, "exit 0"), "kibbr: exit 0");
        assert_eq!(
            text_line(Level::WARN, "trie.json: no such file"),
            "kibbr: WARN trie.json: no such file"
        );
        assert_eq!(
            text_line(Level::ERROR, "write-ahead log - full"),
            "kibbr: ERROR write-ahead log - full"
        );
        assert_eq!(
            text_line(Level::DEBUG, "GET /health"),
            "kibbr: DEBUG GET /health"
        );
    }

    /// **As JSON, a line is one object on one line**, whatever the sentence holds - a
    /// quote, a newline, a path with a backslash - with the line's own four keys first
    /// and never replaced by a field of the same name.
    #[test]
    fn a_json_line_is_one_object_on_one_line() {
        let line = json_line(
            "2026-10-10T14:18:26.123Z",
            Level::WARN,
            "kibbr::serve",
            "leaf 7 - \"data\\7\":\nPermission denied",
            vec![("requests", 3.into()), ("msg", "not this one".into())],
        );
        assert!(!line.contains('\n'), "{line}");
        let read: Value = serde_json::from_str(&line).expect("an object");
        assert_eq!(read["ts"], "2026-10-10T14:18:26.123Z");
        assert_eq!(read["level"], "warn");
        assert_eq!(read["target"], "kibbr::serve");
        assert_eq!(read["msg"], "leaf 7 - \"data\\7\":\nPermission denied");
        assert_eq!(read["requests"], 3);
        let keys: Vec<&String> = read.as_object().expect("an object").keys().collect();
        assert_eq!(keys, ["ts", "level", "target", "msg", "requests"]);
    }

    /// **A level lets through itself and everything louder.**
    #[test]
    fn a_level_lets_through_itself_and_what_is_louder() {
        let passes = |set: LogLevel, event: Level| event <= level_of(set);
        assert!(passes(LogLevel::Info, Level::INFO));
        assert!(passes(LogLevel::Info, Level::WARN));
        assert!(passes(LogLevel::Info, Level::ERROR));
        assert!(!passes(LogLevel::Info, Level::DEBUG));
        assert!(!passes(LogLevel::Error, Level::WARN));
        assert!(passes(LogLevel::Trace, Level::TRACE));
    }
}
