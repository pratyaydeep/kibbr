// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use thiserror::Error;

// What is wrong with one line is the flattener's to say, and it is two crates down; here
// under the names every command already uses.
pub use crate::core::error::{chain, report, BadByte, DetectorError, DetectorErrorKind};

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

/// A `kibbr query` flag that could not be turned into a `Query`, with no byte position
/// because the flag value is quoted whole.
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


/// Errors that end the run; a malformed line is a `DetectorError`, reported and stepped over.
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

    /// No newline within `MAX_SKIP_BYTES` of an over-long line: fatal where `LineTooLong`
    /// is not, because the input is not lines and there is no next one to carry on to.
    #[error(
        "line {line}: no newline in the {scanned} bytes after the {limit} byte line limit \
         - the input does not look like newline-delimited JSON"
    )]
    NotNewlineDelimited {
        line: u64,
        scanned: u64,
        limit: usize,
    },

    /// What ended a read of lines below any command: the read loop's own error (`db::ReadError`).
    #[error(transparent)]
    Input(#[from] crate::storage::db::ReadError),

    #[error(transparent)]
    Gen(#[from] GenError),

    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),

    #[error(transparent)]
    Trie(#[from] crate::storage::trie::TrieError),

    #[error(transparent)]
    Query(#[from] QueryError),

    // Transparent, not wrapped: `ParseError` says where it was and `report` walks the
    // source chain, so a wrapper would print the same sentence twice.
    #[error(transparent)]
    Trieql(#[from] crate::engine::trieql::ParseError),

    // Transparent for the reason `Trieql` is. No `--after:` prefix either - the flag and
    // `AFTER` both land here, and naming one of them would be wrong half the time.
    #[error(transparent)]
    Cursor(#[from] crate::engine::engine::CursorError),

    #[error(transparent)]
    Store(#[from] crate::storage::store::StoreError),

    // Transparent for the reason `Trieql` is - `BucketError`'s own sentence names the
    // requested and permitted counts, and a wrapper would print a vaguer one above it.
    #[error(transparent)]
    Exec(#[from] crate::engine::engine::ExecError),

    /// What a server could not start or finish with, or refused a TrieQL string with: the
    /// server crate's own error.
    #[error(transparent)]
    Serve(crate::server::Error),
    #[error(transparent)]
    Load(#[from] crate::cmd::load::LoadError),
    #[error(transparent)]
    Compact(#[from] crate::cmd::compact::CompactError),
    #[error(transparent)]
    Speed(#[from] crate::cmd::bench::SpeedError),
    #[error(transparent)]
    Ask(#[from] crate::cmd::ask::AskError),
}

impl From<crate::server::Error> for CliError {
    /// The engine's errors the server crate carries through stay the variants they always
    /// were, so a TrieQL string refused by `kibbr query` and by `POST /query` is one failure.
    fn from(e: crate::server::Error) -> Self {
        use crate::server::Error;
        match e {
            Error::Trieql(e) => CliError::Trieql(e),
            Error::Cursor(e) => CliError::Cursor(e),
            Error::Exec(e) => CliError::Exec(e),
            e => CliError::Serve(e),
        }
    }
}
