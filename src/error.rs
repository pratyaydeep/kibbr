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

    /// Arrays are stored whole, so the object depth limit does not apply inside one.
    /// A ceiling still has to: the array is materialised before it can be stored as
    /// text, and materialising it is recursive. This is the one that fires there,
    /// counting containers of either kind from the outermost `[`.
    #[error("nesting deeper than {limit} levels inside the array at `{key}`")]
    ArrayNestingTooDeep { key: String, limit: usize },

    #[error("top-level JSON value is {found}, expected an object")]
    NotAnObject { found: &'static str },

    /// The line is not UTF-8, so it is not text and cannot be a JSON document.
    ///
    /// Split from `InvalidJson` because the remedy is different: an encoding fault in
    /// whatever produced the line, not a syntax fault in the document. It carries no
    /// `#[source]` - `serde_json`'s complaint about these bytes names whatever they
    /// happened to break (`invalid unicode code point` inside a string, `expected
    /// value` outside one), which is the symptom and sometimes a misleading one.
    #[error("invalid UTF-8: {bad}")]
    InvalidUtf8 { bad: BadByte },

    /// `serde_json`'s complaint, carried as text rather than as a `#[source]`.
    ///
    /// Interpolating it and keeping it as a source would print it twice, since the CLI
    /// walks the chain. Text wins because the position has to be rewritten on the way
    /// through: `serde_json` reports a line number of its own, and it is always 1.
    #[error("invalid JSON{}: {message}{}", at_offset(.offset), utf8_note(.also_bad_bytes))]
    InvalidJson {
        /// The byte offset into the line, or `None` when `serde_json` had no position
        /// to give - then the message stands alone rather than claiming a false 0.
        offset: Option<usize>,
        message: String,
        /// Set when the line holds a syntax error *and* fails to decode further along.
        /// Both are true and independent, so both are reported: dropping either costs
        /// the operator a second run to discover it.
        also_bad_bytes: Option<BadByte>,
    },
}

/// Where a line stopped being UTF-8: the offset of the first bad byte, and the byte.
///
/// The byte value is most of the diagnosis. `0xff`/`0xfe` is UTF-16, `0x80`-`0x9f` is
/// cp1252, and a lone lead byte is a write cut mid-character. The offset is 0-based and
/// relative to the line, because it indexes straight into the `raw_line` that `/ingest`
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

// Hand-written rather than derived: `source` skips past `kind`, straight to whatever
// `kind` was caused by. Deriving would put `kind` in the chain and `report` would
// print it twice - once interpolated above, once as a `caused by` line.
impl std::error::Error for DetectorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.kind)
    }
}

/// Errors from the log generator.
#[derive(Debug, Error)]
pub enum GenError {
    #[error("--templates must be at least 1")]
    NoTemplates,

    #[error("failed to serialize generated line: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("writing generated output: {0}")]
    Write(#[source] std::io::Error),
}

/// Errors surfaced by the CLI itself.
///
/// A malformed line is not one of these - it is a `DetectorError`, reported against
/// its line and stepped over. What reaches here ends the run: the input could not be
/// read, stdout could not be written, or the run finished having rejected something.
#[derive(Debug, Error)]
pub enum CliError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("writing to stdout: {0}")]
    Write(#[source] std::io::Error),

    #[error("{count} of {total} lines rejected")]
    RejectedLines { count: u64, total: u64 },

    #[error(transparent)]
    Gen(#[from] GenError),

    #[error(transparent)]
    Trie(#[from] crate::trie::TrieError),
}

/// Print the error and every cause below it, so a `#[source]` chain is not lost.
///
/// Takes `dyn Error` so the fatal path and the per-line path render identically -
/// a rejected line should not look different from a failed run.
pub fn report(e: &(dyn std::error::Error + 'static)) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "triedb: {e}");
    let mut source = e.source();
    while let Some(cause) = source {
        let _ = writeln!(stderr, "  caused by: {cause}");
        source = cause.source();
    }
}
