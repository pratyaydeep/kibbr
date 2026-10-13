// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What is wrong with one line, and how any error in the workspace is printed.
//!
//! Down here, under every other crate, because the flattener makes these errors and the
//! store and the trie print theirs the same way; the errors that end a command or refuse
//! a request are the binary's (`kibbr::error`).

use std::io::{self, Write};

use thiserror::Error;

/// What was wrong with one line, apart from where: `/ingest` reports the message and the
/// line as two fields.
#[derive(Debug, Error)]
pub enum DetectorErrorKind {
    #[error("nesting deeper than {limit} levels at key `{key}`")]
    NestingTooDeep { key: String, limit: usize },

    /// The template or widening the line needed could not be made durable; per line, so the
    /// next line that needs no new record still goes in.
    #[error("not ingested: {detail}")]
    NotDurable { detail: String },

    /// The store could not take the row and there is no write-ahead log holding it: the CLI,
    /// the bench, a bulk load.
    #[error("not ingested: store: {detail}")]
    NotStored { detail: String },

    /// Two flattened keys rendered to the same text: key escaping has stopped being
    /// reversible, so the line is refused.
    #[error(
        "flattened key `{key}` appears twice; distinct paths cannot render alike, so key \
         escaping has stopped being reversible"
    )]
    DuplicateKey { key: String },

    /// The ceiling inside an array, which is materialised recursively before it is stored
    /// as text.
    #[error("nesting deeper than {limit} levels inside the array at `{key}`")]
    ArrayNestingTooDeep { key: String, limit: usize },

    #[error("top-level JSON value is {found}, expected an object")]
    NotAnObject { found: &'static str },

    /// The line ran past the byte cap before any newline; no byte count, because it is
    /// reported before the end is found, and the end may never come.
    #[error("line longer than the {limit} byte limit; skipped to the next line")]
    LineTooLong { limit: usize },

    /// The line is not UTF-8: an encoding fault in the producer, so `serde_json`'s complaint
    /// about the bytes is not carried.
    #[error("invalid UTF-8: {bad}")]
    InvalidUtf8 { bad: BadByte },

    /// `serde_json`'s complaint as text, not a `#[source]`, which the CLI's chain walk would
    /// print twice.
    #[error("invalid JSON{}: {message}{}", at_offset(.offset), utf8_note(.also_bad_bytes))]
    InvalidJson {
        /// The byte offset into the line, or `None` when `serde_json` had no position
        /// to give - then the message stands alone rather than claiming a false 0.
        offset: Option<usize>,
        message: String,
        /// Set when the line holds a syntax error and also fails to decode further along; both
        /// are reported.
        also_bad_bytes: Option<BadByte>,
    },
}

/// Where a line stopped being UTF-8: the 0-based offset of the first bad byte, and the
/// byte, which is most of the diagnosis.
#[derive(Debug, Clone, Copy)]
pub struct BadByte {
    pub offset: usize,
    pub byte: u8,
}

impl std::fmt::Display for BadByte {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "byte {:#04x} at offset {}", self.byte, self.offset)
    }
}

/// The trailing clause on an `InvalidJson` whose line also failed to decode. Empty in
/// the ordinary case, so the common message reads as one uninterrupted sentence.
fn utf8_note(bad: &Option<BadByte>) -> String {
    bad.map_or(String::new(), |bad| {
        format!(" (line also contains invalid UTF-8: {bad})")
    })
}

/// The position clause, omitted entirely when there is no position rather than printed
/// as a 0 that would read like the first byte.
fn at_offset(offset: &Option<usize>) -> String {
    offset.map_or(String::new(), |offset| format!(" at offset {offset}"))
}

/// A rejected line: what was wrong, and which physical line it was.
#[derive(Debug)]
pub struct DetectorError {
    pub line: usize,
    pub kind: DetectorErrorKind,
}

impl DetectorError {
    pub fn new(line: usize, kind: DetectorErrorKind) -> Self {
        Self { line, kind }
    }
}

impl std::fmt::Display for DetectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.kind)
    }
}

// Hand-written: `source` skips past `kind`, which a derive would put in the chain and
// `report` would print twice.
impl std::error::Error for DetectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.kind)
    }
}

/// The error and every cause below it on one line, for an HTTP body that has one
/// `error` field.
pub fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// Print the error and every cause below it, the same for a rejected line as for a
/// failed run.
pub fn report(e: &(dyn std::error::Error + 'static)) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "kibbr: {e}");
    let mut source = e.source();
    while let Some(cause) = source {
        let _ = writeln!(stderr, "  caused by: {cause}");
        source = cause.source();
    }
}
