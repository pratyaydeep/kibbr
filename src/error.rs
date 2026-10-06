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

/// Errors from the log generator.
#[derive(Debug, Error)]
pub enum GenError {
    #[error("--templates must be at least 1")]
    NoTemplates,

    #[error("{flag} applies only to --profile {profile}")]
    WrongProfile {
        flag: &'static str,
        profile: &'static str,
    },

    #[error("--day `{0}`: expected YYYY-MM-DD, from 1970-01-01 to 9999-12-31")]
    BadDay(String),

    #[error("--lines: the farm writes at most {0} lines")]
    TooManyLines(u64),

    #[error("--services {0}: expected 1 to 10")]
    BadServices(usize),

    #[error("--dirty-rate {0}: expected a percent from 0 to 100")]
    BadDirtyRate(f64),

    #[error("failed to serialize generated line: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("writing generated output: {0}")]
    Write(#[source] std::io::Error),
}

/// A `kibbr query` flag that could not be turned into a `Query`.
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

/// What stops the live door from taking a body at all. A line it refuses is not one of
/// these: that is a `DetectorErrorKind` against the line's number, and the body goes on.
///
/// **No variant holds a `StoreError`, and none can be made from one.** A line is durable
/// in the write-ahead log before the store is asked for anything, so a store that cannot
/// take the row - a leaf that will not open, a full disk under `data/` - is not a reason
/// to tell the sender its line was refused: a `500` would be a lie, and a shipper would
/// send again a line that is already safe. The store's failure is logged, the row comes
/// back at the next start's replay, and the sender is told `Ok`.
///
/// Two things hold that, and the compiler holds both. The store is asked by the log's
/// applier, whose function returns nothing (`wal::Apply`) - or, with storage in memory
/// and no log, through the same call the applier makes, which answers only whether the
/// row was taken - so there is no store error for the ingest path to look at. And the
/// assertions below stop building
/// the day this type can be converted from a store's error or from either error that
/// wraps one - which is how one would be carried in, by a `?`. What they cannot see is a
/// variant built by hand from a store error's text; there is no store call here to take
/// one from.
#[derive(Debug, Error)]
pub enum IngestError {
    /// The body could not be split into lines.
    #[error("{0}")]
    Body(String),

    /// The trie's lock poisoned mid-body, which fails every line after it too: the body
    /// is abandoned rather than answered with 500 copies of one message. A catalog append
    /// that fails is not this - it costs its line, reported by number.
    #[error(transparent)]
    Trie(#[from] crate::trie::TrieError),

    /// The write-ahead log could not take the body. **The refusal that is about
    /// durability**, and the only error here by which a body whose lines parsed is
    /// refused: none of it is in the log - a body goes in whole or not at all - so nothing
    /// has it, the sender is told `503`, and sending it again stores nothing twice. The
    /// text is the log's own error; the log knows nothing of the store. The other such
    /// body is one the catalog could write nothing for, and that is no error of this
    /// type: every line of it is refused by number (`DetectorErrorKind::NotDurable`), and
    /// `/ingest` answers `503` from the count of them.
    #[error("the write-ahead log could not take the body: {0}")]
    Log(String),

    /// More is waiting on the write-ahead log than it may hold: the disk is behind what
    /// arrives. A shed, as the door's are - nothing of the body is in the log, and the
    /// sender is told `503` and when to come back.
    #[error("the write-ahead log has more waiting to be written than it may hold")]
    Busy,

    /// The body's lines, as log records, are more than one write of the log holds - a
    /// body is written whole. The sender's to fix, by sending less at once: a `413`.
    #[error(
        "the body's lines come to {bytes} bytes as records of the write-ahead log, past \
         the {limit} one body may; send it as smaller bodies"
    )]
    TooLarge { bytes: usize, limit: usize },
}

/// Fails to build if `$type: From<$from>`. With that conversion in scope the two impls
/// below both apply and the compiler cannot choose between them.
macro_rules! never_from {
    ($type:ty, $from:ty) => {
        const _: fn() = || {
            trait Ambiguous<A> {
                fn check() {}
            }
            impl<T: ?Sized> Ambiguous<()> for T {}
            #[allow(dead_code)]
            struct Converts;
            impl<T: ?Sized + From<$from>> Ambiguous<Converts> for T {}
            let _ = <$type as Ambiguous<_>>::check;
        };
    };
}

// A `#[from] StoreError` on a variant, or `?` on a store call inside the ingest path.
never_from!(IngestError, crate::store::StoreError);
// And the two errors that can hold one, which `?` would carry it in by.
never_from!(IngestError, CliError);
never_from!(IngestError, crate::db::SinkError);

/// What stops `kibbr serve` from starting, or from finishing cleanly.
///
/// Nothing a *request* gets wrong is in here. A bad query is a 400 with the parse error
/// in the body and the server carries on; a malformed ingest line is one entry in that
/// response's `errors` array. These are the faults that leave no server running.
#[derive(Debug, Error)]
pub enum ServeError {
    /// Almost always "address already in use" - a second `kibbr serve` on 7700, or
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
        "{path}: this data directory holds a storage layout from before format v3 (a \
         directory per template), which this build cannot read. There is no migration: \
         point --data-dir at a new directory, or remove this one and re-ingest."
    )]
    LegacyLayout { path: std::path::PathBuf },

    /// A segment whose header version is not the one this build reads. Fatal at startup
    /// and not a leaf answering empty: a count of 0 for a leaf whose rows are sitting on
    /// disk is a wrong answer given as a right one, and guessing at the bytes would be
    /// worse. Names the file, because the operator's remedy is about that file.
    #[error(
        "{path}: segment format version {found}; this build reads version {} only and \
         does not guess at the rest. Nothing was served. Start the build that wrote it, or \
         move that file out of the data directory.",
        crate::store::v3::VERSION
    )]
    SegmentVersion { path: std::path::PathBuf, found: u8 },

    /// A data file written by a pre-rename build, whose magic this build does not write.
    /// Refused as another version is, by path and before anything is served.
    #[error(
        "{path}: written by a pre-rename build; this build does not read it and does not \
         guess at the rest. Nothing was served. There is nothing to migrate: start the build \
         that wrote it, or move that file out of the data directory."
    )]
    PreRename { path: std::path::PathBuf },

    /// The write-ahead log could not be opened, read back or started. At startup and
    /// fatal: a server that cannot make a line durable has nothing true to answer `200`
    /// with, and one that guessed at a log it could not read would store lines twice or
    /// not at all.
    #[error(transparent)]
    Wal(#[from] crate::wal::WalError),

    /// Another process owns the data directory. Taken before anything is written and
    /// before the port - only the read-only version check comes first: a second writer of
    /// the catalog or the manifest would write over the first one's records.
    #[error(transparent)]
    Lock(#[from] crate::lock::LockError),
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
    #[error(transparent)]
    Load(#[from] crate::load::LoadError),
    #[error(transparent)]
    Compact(#[from] crate::compact::CompactError),
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
