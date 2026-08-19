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

    #[error("top-level JSON value is {found}, expected an object")]
    NotAnObject { found: &'static str },

    // The cause is not interpolated here: it is a `#[source]`, and the CLI walks the
    // chain. Doing both prints it twice.
    #[error("invalid JSON")]
    InvalidJson {
        #[source]
        source: serde_json::Error,
    },
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
