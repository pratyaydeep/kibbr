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
    #[error("not ingested: {detail}")]
    NotDurable { detail: String },

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

/// A `triedb query` flag that could not be turned into a `Query`.
///
/// Flag-level only: nothing here carries a byte position, because a flag value is its own
/// unit and quoting it whole says where it was. TrieQL's failures are
/// `trieql::ParseError`, which does carry one.
#[derive(Debug, Error)]
pub enum QueryError {
    #[error("--match `{arg}`: expected KEY=VALUE")]
    BadMatch { arg: String },

    #[error("--match `{arg}`: the key is empty")]
    EmptyKey { arg: String },

    #[error("--group-by `{arg}`: a key is empty")]
    EmptyGroupKey { arg: String },

    #[error("--group-by `{arg}`: expected `time(<duration>)` with a non-zero number followed by s, m, h or d")]
    BadBucket { arg: String },

    #[error("--group-by `{arg}`: names two time buckets; it can have one")]
    TwoTimeBuckets { arg: String },

    /// `bucket` is the column a `time(...)` reports under on every surface, so a group key
    /// of that name beside one would head two table columns the same and collide in the
    /// NDJSON object. Refused rather than letting one of them quietly win.
    #[error("`bucket` is the column a `time(...)` group reports under; group by it under another name, or drop the time bucket")]
    BucketKeyClash,

    #[error("--since `{arg}`: expected a number followed by s, m, h or d")]
    BadDuration { arg: String },

    #[error("--between `{arg}`: expected an RFC 3339 datetime")]
    BadDatetime { arg: String },

    #[error("--between: `{from}` is after `{to}`")]
    BackwardsRange { from: String, to: String },
}

/// What stops `triedb serve` from starting, or from finishing cleanly.
///
/// Nothing a *request* gets wrong is in here. A bad query is a 400 with the parse error
/// in the body and the server carries on; a malformed ingest line is one entry in that
/// response's `errors` array. These are the faults that leave no server running.
#[derive(Debug, Error)]
pub enum ServeError {
    /// Almost always "address already in use" - a second `triedb serve` on 7700, or
    /// something else holding it. Names the address, because the remedy is `--port`.
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        addr: String,
        #[source]
        source: std::io::Error,
    },

    #[error("starting the async runtime: {0}")]
    Runtime(#[source] std::io::Error),

    #[error("serving: {0}")]
    Serve(#[source] std::io::Error),

    /// Registering the SIGTERM handler failed, which is checked at startup rather than
    /// at shutdown: a server that cannot hear SIGTERM will be killed by the SIGKILL that
    /// follows it, and finding that out during a deploy is finding it out too late.
    #[error("installing the {signal} handler: {source}")]
    Signal {
        signal: &'static str,
        #[source]
        source: std::io::Error,
    },

    /// The data directory could not be opened. At startup rather than at the first flush:
    /// an operator watching a server come up should be told the disk is unusable before a
    /// shipper has been told its lines were accepted.
    #[error("opening storage: {detail}")]
    Storage { detail: String },

    /// The data directory was written by the per-column layout. There is no migration -
    /// see `store::StoreError::LegacyLayout` - so this is fatal at startup rather than a
    /// warning, and it names the path so an operator can act on it without reading source.
    #[error(
        "{path}: this data directory was written by the per-column layout (one file per \
         template, day and column), which this build cannot read. There is no migration: \
         point --data-dir at a new directory, or remove this one and re-ingest."
    )]
    LegacyLayout { path: std::path::PathBuf },

    /// The final flush could not write every buffered row, so they went with the process.
    ///
    /// **An error and not a log line**, because the exit code is the only part of a
    /// shutdown an orchestrator reads. It used to exit 0 whatever happened - 20,000 lines
    /// accepted, 10,625 back after a restart, exit 0. Everything else in the shutdown
    /// sequence still runs and still prints; this is what the process ends with.
    #[error(
        "the final flush failed for {leaves} leaf/leaves and {rows} buffered row(s) went \
         with the process; with no write-ahead log a buffered row was only ever in memory"
    )]
    RowsLost { leaves: usize, rows: u64 },
}

/// Errors surfaced by the CLI itself.
///
/// A malformed line is not one of these - it is a `DetectorError`, reported against its
/// line and stepped over. What reaches here ends the run: the input could not be read,
/// stdout could not be written, or the run finished having rejected something.
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

    /// No newline within `MAX_SKIP_BYTES` of an over-long line, so the input is not
    /// newline-delimited and there is no next line to carry on to.
    ///
    /// Fatal, where `LineTooLong` is not, and the difference is whether anything is left
    /// to isolate. A long line among short ones is one bad line, and one bad line must
    /// not cost the others; a hundred megabytes with no `\n` in it is not lines at all -
    /// most likely a binary file handed to `parse`, or a producer writing no terminators.
    /// Reading on would consume the whole input to reach an answer that cannot arrive.
    #[error(
        "line {line}: no newline in the {scanned} bytes after the {limit} byte line limit \
         - the input does not look like newline-delimited JSON"
    )]
    NotNewlineDelimited {
        line: u64,
        scanned: u64,
        limit: usize,
    },

    #[error(transparent)]
    Gen(#[from] GenError),

    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),

    #[error(transparent)]
    Trie(#[from] crate::trie::TrieError),

    #[error(transparent)]
    Query(#[from] QueryError),

    // Transparent, not wrapped: `ParseError` says where it was and `report` walks the
    // source chain, so a wrapper would print the same sentence twice.
    #[error(transparent)]
    Trieql(#[from] crate::trieql::ParseError),

    // Transparent for the reason `Trieql` is: `report` walks the source chain, so a
    // wrapper prints the same sentence twice. No `--after:` prefix either - the flag
    // surface and `AFTER` both land here, and naming one of them would be wrong half the
    // time. `CursorError`'s own messages say what is wrong with the cursor.
    #[error(transparent)]
    Cursor(#[from] crate::engine::CursorError),

    #[error(transparent)]
    Store(#[from] crate::store::StoreError),

    // Transparent for the reason `Trieql` is - `BucketError`'s own sentence names the
    // requested and permitted counts, and a wrapper would print a vaguer one above it.
    #[error(transparent)]
    Exec(#[from] crate::engine::ExecError),

    #[error(transparent)]
    Serve(#[from] ServeError),
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
    let _ = writeln!(stderr, "triedb: {e}");
    let mut source = e.source();
    while let Some(cause) = source {
        let _ = writeln!(stderr, "  caused by: {cause}");
        source = cause.source();
    }
}
