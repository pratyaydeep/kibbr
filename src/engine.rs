// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The whole database of [kibbr](https://github.com/pratyaydeep/kibbr), a database for
//! structured JSON logs, behind one handle, `Database`: no HTTP, no flags, no async.
//!
//! Everything needed to store and answer: a line parsed and its template found, its row
//! logged and placed, the day files, loads and compactions, and TrieQL answered over them.
//! The server and the command line each hold one `Database`; the machinery is private to
//! this module, and `inspect` holds what the command line's diagnostics read past it.

pub mod database;
pub mod display;
pub mod error;
mod format;
#[cfg(any(test, feature = "fuzzing"))]
pub mod fuzzing;
mod ingest;
pub mod load;
mod lock;
pub mod model;
pub mod parse;
pub mod query;
pub mod rng;
pub mod stop;
mod store;
mod templates;
#[cfg(test)]
mod testkit;
mod wal;

pub use database::compaction;
pub use database::{CompactRefused, Database, LineError, Opener, Options, Stats, Storage};
pub use error::Error;

// The values the handle takes and hands back that are not its own: a query's stop and
// what it fails with, a flush's summary, the log's numbers, the scan threads a server lets
// go last, and how a UTC day and a line's limit are said.
pub use ingest::{load_lines, row_of, LoadStats, ReadError};
pub use lock::{holder, Holder, LockError, LOCK_FILE};
pub use model::row::Row;
pub use parse::lines::MAX_LINE_BYTES;
pub use stop::Stop;
pub use store::disk::{day_name, day_of_name, refuse_other_versions, STAGING_DIR};
pub use store::fan::Fan;
pub use store::manifest::path_in as manifest_path;
pub use store::{present, system_time};
pub use store::{FlushSummary, StoreError};
pub use templates::trie::TrieError;
pub use wal::Stats as WalStats;

/// What the command line's diagnostic commands reach past the handle for: `demo` draws the
/// trie, `bench-storage` writes a live day without a log and walks the frames of the files
/// it measures, and the `cursor` fuzz harness builds a database from its lines in memory;
/// and, in a test, what the server's tests reach into a database with. The server itself
/// never names it (`tests/layers.rs`).
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
        lock::DataLock, store::disk::DATA_DIR, store::DEFAULT_FAN_ROWS,
        templates::catalog::Catalog, wal::Wal,
    };

    /// The log's numbers, and the size a body's lines come to in it.
    #[cfg(test)]
    pub(crate) mod wal {
        pub(crate) use crate::engine::wal::{counted, Stats};
    }
}
