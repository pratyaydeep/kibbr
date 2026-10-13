// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a server can fail with: `Error`, which stops one from starting or from finishing
//! cleanly, and `IngestError`, which stops the live door from taking a body.

use thiserror::Error;

// What is wrong with one line is the flattener's to say, and it is two crates down; here
// under the names the door already uses.
pub use kibbr_core::error::{chain, DetectorError, DetectorErrorKind};

/// What stops the live door from taking a body at all; no variant holds a `StoreError`,
/// since a line is durable in the log before the store is asked.
#[derive(Debug, Error)]
pub enum IngestError {
    /// The body could not be split into lines.
    #[error("{0}")]
    Body(String),

    /// The trie's lock poisoned mid-body, which fails every later line too, so the body is
    /// abandoned rather than answered line by line.
    #[error(transparent)]
    Trie(#[from] kibbr_storage::trie::TrieError),

    /// The write-ahead log could not take the body, which goes in whole or not at all: a
    /// `503`, and sending it again stores nothing twice.
    #[error("the write-ahead log could not take the body: {0}")]
    Log(String),

    /// More is waiting on the write-ahead log than it may hold: a shed, a `503` that says
    /// when to come back.
    #[error("the write-ahead log has more waiting to be written or stored than it may hold")]
    Busy,

    /// The body's sender hung up while it waited for room in the log; not a shed, as nobody
    /// was told to come back.
    #[error("the body's sender left while it waited for room in the write-ahead log")]
    Left,

    /// The body's lines, as log records, are more than one write of the log holds: a `413`.
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
never_from!(IngestError, kibbr_storage::store::StoreError);
// And the two errors that can hold one, which `?` would carry it in by: the server's own,
// as its `Storage`, and the read loop's.
never_from!(IngestError, Error);
never_from!(IngestError, kibbr_storage::db::SinkError);

/// What stops a server from starting or from finishing cleanly, and, last, the engine's
/// errors a request is refused with.
#[derive(Debug, Error)]
pub enum Error {
    /// Almost always "address already in use"; names the address, because the remedy is
    /// `--port`.
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

    /// Registering the SIGTERM handler failed, checked at startup: a server that cannot hear
    /// SIGTERM is killed by the SIGKILL that follows it.
    #[error("installing the {signal} handler: {source}")]
    Signal {
        signal: &'static str,
        #[source]
        source: std::io::Error,
    },

    /// The data directory could not be opened, said at startup, before a shipper is told its
    /// lines were accepted.
    #[error("opening storage: {detail}")]
    Storage { detail: String },

    /// The data directory was written by the per-column layout, which has no migration: fatal
    /// at startup, naming the path.
    #[error(
        "{path}: this data directory holds a storage layout from before format v3 (a \
         directory per template), which this build cannot read. There is no migration: \
         point --data-dir at a new directory, or remove this one and re-ingest."
    )]
    LegacyLayout { path: std::path::PathBuf },

    /// A segment whose header version this build does not read: fatal at startup, since a
    /// count of 0 for a leaf whose rows are on disk is a wrong answer given as a right one.
    #[error(
        "{path}: segment format version {found}; this build reads version {} only and \
         does not guess at the rest. Nothing was served. Start the build that wrote it, or \
         move that file out of the data directory.",
        kibbr_storage::store::v3::VERSION
    )]
    SegmentVersion { path: std::path::PathBuf, found: u8 },

    /// A data file written by a pre-rename build, refused by path before anything is served.
    #[error(
        "{path}: written by a pre-rename build; this build does not read it and does not \
         guess at the rest. Nothing was served. There is nothing to migrate: start the build \
         that wrote it, or move that file out of the data directory."
    )]
    PreRename { path: std::path::PathBuf },

    /// The write-ahead log could not be opened, read back or started: fatal, since a server
    /// that cannot make a line durable has nothing true to answer `200` with.
    #[error(transparent)]
    Wal(#[from] kibbr_storage::wal::WalError),

    /// Another process owns the data directory, and a second writer of the catalog or the
    /// manifest would write over the first one's records.
    #[error(transparent)]
    Lock(#[from] kibbr_storage::lock::LockError),

    // Transparent, not wrapped: `ParseError` says where it was and `report` walks the
    // source chain, so a wrapper would print the same sentence twice.
    #[error(transparent)]
    Trieql(#[from] kibbr_engine::trieql::ParseError),

    // Transparent for the reason `Trieql` is. No `--after:` prefix: the flag surface and
    // `AFTER` both land here.
    #[error(transparent)]
    Cursor(#[from] kibbr_engine::engine::CursorError),

    // Transparent for the reason `Trieql` is - `BucketError`'s own sentence names the
    // requested and permitted counts, and a wrapper would print a vaguer one above it.
    #[error(transparent)]
    Exec(#[from] kibbr_engine::engine::ExecError),

    /// A group key named `bucket` beside a `time(...)`, which reports under that name on
    /// every surface: refused rather than letting one quietly win.
    #[error("`bucket` is the column a `time(...)` group reports under; group by it under another name, or drop the time bucket")]
    BucketKeyClash,
}

/// What this crate's functions return when they fail.
pub type Result<T, E = Error> = std::result::Result<T, E>;
