//! What is wrong with one line, and how any error in the workspace is printed.
//!
//! Down here, under every other crate, because the flattener makes these errors and the
//! store and the trie print theirs the same way; the errors that end a command or refuse
//! a request are the binary's (`kibbr::error`).

use std::io::{self, Write};

use thiserror::Error;

/// What was wrong with one line, with no claim about where it was.
///
/// Split from the line number because the ingest endpoint reports failures as
/// `{"line":400,"msg":"..."}` - the message and the position are two fields there,
/// so they are two things here.
#[derive(Debug, Error)]
pub enum DetectorErrorKind {
    #[error("nesting deeper than {limit} levels at key `{key}`")]
    NestingTooDeep { key: String, limit: usize },

    /// The line parsed, and the template or widening it needed could not be made durable
    /// before it counts. Not the line's fault, but it is the line that did not land, and
    /// `{"line":N,"msg":"..."}` is the one place a client hears that. The next line that
    /// needs no new record still goes in, which is why this is per line and not the body.
    /// A body of nothing but these has no next line: where each was a write that failed,
    /// `/ingest` answers it `503`, with the same errors by line, so its sender keeps it
    /// and sends it again. A record that would not encode, or a template whose lock is
    /// poisoned, is not such a write, and a body of those is still a `200`.
    #[error("not ingested: {detail}")]
    NotDurable { detail: String },

    /// The line parsed and its template exists, and the store could not take the row - a
    /// leaf whose directory would not open. **Only where there is no write-ahead log**:
    /// the CLI, the bench, a bulk load. There the row is nowhere, so the line is refused
    /// rather than counted. `/ingest` never builds one: its line is durable in the log
    /// before the store is asked, and a store that fails then delays the row and is not
    /// an error (`IngestError`).
    #[error("not ingested: store: {detail}")]
    NotStored { detail: String },

    /// Two flattened keys rendered to the same text.
    ///
    /// Not a fact about the line: distinct JSON paths cannot render alike, because key
    /// escaping is reversible. Reaching this means it has stopped being reversible, and
    /// the fingerprint, the merge index's set arithmetic and the trie's edge labels are
    /// all downstream of that - so this line is refused rather than filed under a
    /// template that is not its shape. It was a debug assertion, which is to say it was
    /// nothing in the build that ships.
    #[error(
        "flattened key `{key}` appears twice; distinct paths cannot render alike, so key \
         escaping has stopped being reversible"
    )]
    DuplicateKey { key: String },

    /// Arrays are stored whole, so the object depth limit does not apply inside one - but
    /// a ceiling still has to, since the array is materialised recursively before it can
    /// be stored as text. This is the one that fires there, counting containers of either
    /// kind from the outermost `[`.
    #[error("nesting deeper than {limit} levels inside the array at `{key}`")]
    ArrayNestingTooDeep { key: String, limit: usize },

    #[error("top-level JSON value is {found}, expected an object")]
    NotAnObject { found: &'static str },

    /// The line ran past the byte cap before any newline arrived.
    ///
    /// A rejection like the others rather than an `io::Error` that would end the run: the
    /// reader steps to the next newline and carries on, so an over-long line costs its own
    /// line and nothing after it. Distinct from every other kind here in that nothing
    /// looked at the content - the fault is in the framing, not the JSON.
    ///
    /// No byte count, deliberately. The length is only knowable once the line ends, and
    /// this is reported before the reader goes looking for that end - on the input that
    /// most needs the diagnostic, the end never comes. A message that always prints beats
    /// a better one that sometimes does not.
    #[error("line longer than the {limit} byte limit; skipped to the next line")]
    LineTooLong { limit: usize },

    /// The line is not UTF-8, so it is not text and cannot be a JSON document.
    ///
    /// Split from `InvalidJson` because the remedy differs: an encoding fault in whatever
    /// produced the line, not a syntax fault in the document. No `#[source]` -
    /// `serde_json`'s complaint about these bytes names whatever they happened to break
    /// (`invalid unicode code point` inside a string, `expected value` outside one),
    /// which is the symptom and sometimes a misleading one.
    #[error("invalid UTF-8: {bad}")]
    InvalidUtf8 { bad: BadByte },

    /// `serde_json`'s complaint, carried as text rather than as a `#[source]`.
    ///
    /// Interpolating it and keeping it as a source would print it twice, since the CLI
    /// walks the chain. Text wins because the position is rewritten on the way through:
    /// `serde_json` reports a line number of its own, and it is always 1.
    #[error("invalid JSON{}: {message}{}", at_offset(.offset), utf8_note(.also_bad_bytes))]
    InvalidJson {
        /// The byte offset into the line, or `None` when `serde_json` had no position
        /// to give - then the message stands alone rather than claiming a false 0.
        offset: Option<usize>,
        message: String,
        /// Set when the line holds a syntax error *and* fails to decode further along.
        /// Both are true and independent, so both are reported - dropping either costs
        /// the operator a second run to discover it.
        also_bad_bytes: Option<BadByte>,
    },
}

/// Where a line stopped being UTF-8: the offset of the first bad byte, and the byte.
///
/// The byte value is most of the diagnosis: `0xff`/`0xfe` is UTF-16, `0x80`-`0x9f` is
/// cp1252, a lone lead byte is a write cut mid-character. The offset is 0-based and
/// relative to the line, so it indexes straight into the `raw_line` that `/ingest`
/// echoes back.
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
///
/// Every rejection carries its line, because the parse loop reports and continues
/// rather than aborting - without the number the operator cannot find the line.
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

// Hand-written rather than derived: `source` skips past `kind` to whatever caused it.
// Deriving would put `kind` in the chain and `report` would print it twice - once
// interpolated above, once as a `caused by` line.
impl std::error::Error for DetectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.kind)
    }
}

/// The error and every cause below it on one line.
///
/// `report` prints the same chain on stderr as indented `caused by:` lines. An HTTP
/// response body has one `error` field and no second line to put them on, and dropping
/// the chain there would lose the part that says *where* in a TrieQL string the parse
/// failed - which is the only part the client can act on.
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

/// Print the error and every cause below it, so a `#[source]` chain is not lost.
///
/// Takes `dyn Error` so the fatal path and the per-line path render identically: a
/// rejected line should not look different from a failed run.
pub fn report(e: &(dyn std::error::Error + 'static)) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "kibbr: {e}");
    let mut source = e.source();
    while let Some(cause) = source {
        let _ = writeln!(stderr, "  caused by: {cause}");
        source = cause.source();
    }
}
