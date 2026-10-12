//! What a server can fail with: `Error`, which stops one from starting or from finishing
//! cleanly, and `IngestError`, which stops the live door from taking a body.
//!
//! The storage crate's and the engine's own errors are carried through as themselves -
//! each prints the sentence it wrote. How an error is said to whoever ran the process is
//! the binary's (`kibbr_core::error::report`, and its own log).

use thiserror::Error;

// What is wrong with one line is the flattener's to say, and it is two crates down; here
// under the names the door already uses.
pub use kibbr_core::error::{chain, DetectorError, DetectorErrorKind};

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
    Trie(#[from] kibbr_storage::trie::TrieError),

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

    /// More is waiting on the write-ahead log - to be written, or written and not yet
    /// stored - than it may hold: the log's disk or the store is behind what arrives. A
    /// shed, as the door's are - nothing of the body is in the log, and the sender is
    /// told `503` and when to come back.
    #[error("the write-ahead log has more waiting to be written or stored than it may hold")]
    Busy,

    /// The body's sender hung up while the body waited for room in the write-ahead log,
    /// and the body was let go: nothing of it is in the log, and there is nobody left to
    /// answer. Not a shed - no sender was told to come back.
    #[error("the body's sender left while it waited for room in the write-ahead log")]
    Left,

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
never_from!(IngestError, kibbr_storage::store::StoreError);
// And the two errors that can hold one, which `?` would carry it in by: the server's own,
// as its `Storage`, and the read loop's.
never_from!(IngestError, Error);
never_from!(IngestError, kibbr_storage::db::SinkError);

/// What stops a server from starting, or from finishing cleanly - and, last, what a
/// TrieQL string is refused with.
///
/// Nothing else a *request* gets wrong is in here: a malformed ingest line is one entry
/// in that response's `errors` array, and a body the door will not take is an
/// `IngestError`. The faults above the last four leave no server running; those four are
/// the engine's own errors carried through, a 400 with the sentence in its body, and the
/// server carries on.
#[derive(Debug, Error)]
pub enum Error {
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
        kibbr_storage::store::v3::VERSION
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
    Wal(#[from] kibbr_storage::wal::WalError),

    /// Another process owns the data directory. Taken before anything is written and
    /// before the port - only the read-only version check comes first: a second writer of
    /// the catalog or the manifest would write over the first one's records.
    #[error(transparent)]
    Lock(#[from] kibbr_storage::lock::LockError),

    // Transparent, not wrapped: `ParseError` says where it was and `report` walks the
    // source chain, so a wrapper would print the same sentence twice.
    #[error(transparent)]
    Trieql(#[from] kibbr_engine::trieql::ParseError),

    // Transparent for the reason `Trieql` is. No `--after:` prefix either - the flag
    // surface and `AFTER` both land here, and naming one of them would be wrong half the
    // time. `CursorError`'s own messages say what is wrong with the cursor.
    #[error(transparent)]
    Cursor(#[from] kibbr_engine::engine::CursorError),

    // Transparent for the reason `Trieql` is - `BucketError`'s own sentence names the
    // requested and permitted counts, and a wrapper would print a vaguer one above it.
    #[error(transparent)]
    Exec(#[from] kibbr_engine::engine::ExecError),

    /// `bucket` is the column a `time(...)` reports under on every surface, so a group key
    /// of that name beside one would head two table columns the same and collide in the
    /// NDJSON object. Refused rather than letting one of them quietly win.
    #[error("`bucket` is the column a `time(...)` group reports under; group by it under another name, or drop the time bucket")]
    BucketKeyClash,
}

/// What this crate's functions return when they fail.
pub type Result<T, E = Error> = std::result::Result<T, E>;
