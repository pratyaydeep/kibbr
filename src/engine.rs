// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The whole database of [kibbr](https://github.com/pratyaydeep/kibbr), a database for
//! structured JSON logs, behind one handle, `Database`: no HTTP, no flags, no async.
//!
//! Everything needed to store and answer: a line parsed and its template found, its row
//! logged and placed, the day files, loads and compactions, and TrieQL answered over them.
//! The server and the command line each hold one `Database`.
//!
//! What the layers above may name, all of it here: the items re-exported below (they say
//! `engine::Database`, `engine::commas`, ...), and four namespaces, `model`, `query`,
//! `load` and `compaction`. Every other module is private. Two doors reach further, each
//! for named callers: `inspect` (the diagnostic commands, a few tests, and the tests under
//! tests/ through `kibbr::testing`) and, with the fuzz harnesses, `fuzzing`.

// Private: what the layers above use is re-exported below, or is in one of the four
// namespaces after them.
mod database;
mod display;
mod error;
mod format;
mod ingest;
mod lock;
mod parse;
mod rng;
mod stop;
mod store;
mod templates;
#[cfg(test)]
mod testkit;
mod wal;

// The namespaces: a value's types and how it is inferred (`model`), a query and its
// answer (`query`), the bulk door (`load`), and a compaction run (`compaction`).
pub(crate) mod load;
pub(crate) mod model;
pub(crate) mod query;
pub(crate) use database::compaction;

// The handle, how it is opened, and what it hands back.
pub use database::{CompactRefused, Database, LineError, Opener, Options, Schema, Stats, Storage};
pub use store::{FlushSummary, StoreError};
pub use wal::Stats as WalStats;

// A server's tests open a database without its lock, and fill a report to its limit.
#[cfg(test)]
pub use database::{Claim, MAX_REPORTED_ERRORS};

// The defaults of `Storage` and of the server's clocks, for the settings to start from.
pub use database::{
    DEFAULT_COMPACT_INTERVAL_MS, DEFAULT_FAN_ROWS, DEFAULT_FLUSH_INTERVAL_MS,
    DEFAULT_MAX_BUFFER_BYTES, DEFAULT_MAX_OPEN_SEGMENTS, DEFAULT_ROW_GROUP_TIMEOUT_MS,
    DEFAULT_SEGMENT_MAX_BYTES, DEFAULT_WAL_BATCH_LINES, DEFAULT_WAL_QUEUE_BYTES,
    DEFAULT_WAL_SEGMENT_BYTES,
};

// What the engine fails with, and how an error and its causes are said.
pub use error::{chain, report, DetectorError, DetectorErrorKind, Error, IngestError};
pub use templates::trie::TrieError;

// A line read, parsed and made a row without a database: `parse`, `demo` and the oracle.
pub use ingest::{load_lines, row_of, LoadStats, ReadError};
pub use model::row::Row;
pub use parse::flatten::{parse_line, ParsedLog};
pub use parse::lines::{
    read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES,
};

// How a value, a number, a time and a duration are printed, the same everywhere.
pub use display::{col_text, commas, duration_text, millis, rfc3339, rfc3339_at, NULL_TEXT};

// A data directory from outside: its lock and who holds it, its days, its manifest.
pub use lock::{holder, Holder, LockError, LOCK_FILE};
pub use store::disk::{day_name, day_of_name, refuse_other_versions, STAGING_DIR};
pub use store::manifest::path_in as manifest_path;
pub use store::{present, system_time};

// A query's stop, the scan threads a server lets go last, and the generator's numbers.
pub use rng::Rng;
pub use stop::Stop;
pub use store::scan_threads::Fan;

/// The fuzz harnesses of the engine's own formats, which `src/fuzzing.rs` hands to
/// `fuzz/`; they see the private modules, as tests do.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) mod fuzzing;

/// What the command line's diagnostic commands reach past the handle for: `demo` draws the
/// trie, `bench-storage` writes a live day without a log and walks the frames of the files
/// it measures, and the `cursor` fuzz harness builds a database from its lines in memory;
/// the tests under tests/ read a data file with it (`kibbr::testing`); and, in a test,
/// what the server's and the commands' tests reach into a database with. The server
/// itself never names it (`tests/source_rules.rs`).
pub(crate) mod inspect {
    pub use crate::engine::format::file::{open_sealed, Reader};
    pub use crate::engine::format::frame::{read as read_frame, Next};
    pub use crate::engine::format::FormatError;
    pub use crate::engine::format::{
        HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, TRAILER_LEN,
    };
    pub use crate::engine::ingest::{ingest_line, read_lines, SinkError};
    pub use crate::engine::store::disk::{data_files, CompactOptions};
    pub use crate::engine::store::{FlushKind, StoreManager};
    pub use crate::engine::templates::catalog::path_in as catalog_path;
    pub use crate::engine::templates::trie::{Node, Trie, ROOT_LABEL};

    /// A database's trie and stores, as they are.
    pub(crate) fn parts(db: &crate::engine::Database) -> (&Trie, &StoreManager) {
        db.parts()
    }

    /// A database built from its lines in memory, for the `cursor` fuzz harness.
    #[cfg(any(test, feature = "fuzzing"))]
    pub use crate::engine::ingest::{into_memory, MemoryDb};

    /// What the server's and the commands' tests reach a database's parts with.
    #[cfg(test)]
    pub(crate) use crate::engine::{
        lock::DataLock, store::disk::DATA_DIR, templates::catalog::Catalog, wal::Wal,
    };

    /// The log's numbers, and the size a body's lines come to in it.
    #[cfg(test)]
    pub(crate) mod wal {
        pub(crate) use crate::engine::wal::{counted, Stats};
    }
}
