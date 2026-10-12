use thiserror::Error;

// What is wrong with one line is the flattener's to say, and it is two crates down; here
// under the names every command already uses.
pub use kibbr_core::error::{chain, report, BadByte, DetectorError, DetectorErrorKind};

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

    #[error("--since `{arg}`: expected a number followed by s, m, h or d")]
    BadDuration { arg: String },

    #[error("--between `{arg}`: expected an RFC 3339 datetime")]
    BadDatetime { arg: String },

    #[error("--between: `{from}` is after `{to}`")]
    BackwardsRange { from: String, to: String },
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

    /// What ended a read of lines below any command: the read loop's own error
    /// (`db::ReadError`), printed as the sentence it wrote.
    #[error(transparent)]
    Input(#[from] kibbr_storage::db::ReadError),

    #[error(transparent)]
    Gen(#[from] GenError),

    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),

    #[error(transparent)]
    Trie(#[from] kibbr_storage::trie::TrieError),

    #[error(transparent)]
    Query(#[from] QueryError),

    // Transparent, not wrapped: `ParseError` says where it was and `report` walks the
    // source chain, so a wrapper would print the same sentence twice.
    #[error(transparent)]
    Trieql(#[from] kibbr_engine::trieql::ParseError),

    // Transparent for the reason `Trieql` is: `report` walks the source chain, so a
    // wrapper prints the same sentence twice. No `--after:` prefix either - the flag
    // surface and `AFTER` both land here, and naming one of them would be wrong half the
    // time. `CursorError`'s own messages say what is wrong with the cursor.
    #[error(transparent)]
    Cursor(#[from] kibbr_engine::engine::CursorError),

    #[error(transparent)]
    Store(#[from] kibbr_storage::store::StoreError),

    // Transparent for the reason `Trieql` is - `BucketError`'s own sentence names the
    // requested and permitted counts, and a wrapper would print a vaguer one above it.
    #[error(transparent)]
    Exec(#[from] kibbr_engine::engine::ExecError),

    /// What a server could not start or finish with, and what a TrieQL string is refused
    /// with: the server crate's own error, printed as the sentence it wrote.
    #[error(transparent)]
    Serve(kibbr_server::Error),
    #[error(transparent)]
    Load(#[from] crate::cmd::load::LoadError),
    #[error(transparent)]
    Compact(#[from] crate::cmd::compact::CompactError),
    #[error(transparent)]
    Speed(#[from] crate::cmd::bench::SpeedError),
    #[error(transparent)]
    Ask(#[from] crate::cmd::ask::AskError),
}

impl From<kibbr_server::Error> for CliError {
    /// The engine's own errors, which the server crate carries through as themselves, are
    /// the same errors here: a TrieQL string refused by `kibbr query` and by `POST /query`
    /// is one failure, and it is the variant it always was. Everything else is the
    /// server's.
    fn from(e: kibbr_server::Error) -> Self {
        use kibbr_server::Error;
        match e {
            Error::Trieql(e) => CliError::Trieql(e),
            Error::Cursor(e) => CliError::Cursor(e),
            Error::Exec(e) => CliError::Exec(e),
            e => CliError::Serve(e),
        }
    }
}
