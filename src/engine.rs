// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The whole database of [kibbr](https://github.com/pratyaydeep/kibbr), a database for
//! structured JSON logs, behind one handle, `Database`: no HTTP, no flags, no async.
//!
//! Everything needed to store and answer: a line parsed and its template found, its row
//! logged and placed, the day files, loads and compactions, and TrieQL answered over them.
//! The server and the command line each hold one `Database`; the machinery is private to
//! this crate, and `internals` holds what the command line's diagnostics read past it.

pub mod model;
pub mod parse;
pub mod display;
pub mod rng;
pub mod error;
pub(crate) mod lock;
pub(crate) mod format;
pub(crate) mod templates;
pub(crate) mod store;
pub(crate) mod ingest;
pub(crate) mod wal;
pub mod load;
pub mod query;
pub mod database;
#[cfg(feature = "fuzzing")]
pub mod fuzzing;
#[cfg(test)]
mod testkit;
pub mod stop;

pub use database::{
    Claim, CompactRefused, Database, Ingested, LineError, Opener, Options, Refused, Schema,
    Stats, Storage, Template, TemplateColumn,
};
pub use error::{Error, IngestError, Result};
pub use database::compaction;
pub use database::lifecycle::Replayed;

// The values the handle takes and hands back that are not its own: a query's stop and
// what it fails with, a flush's summary, the log's numbers, the scan threads a server lets
// go last, and how a UTC day and a line's limit are said.
pub use parse::lines::MAX_LINE_BYTES;
pub use ingest::{load_lines, row_of, LoadStats, ReadError};
pub use lock::{holder, Holder, LockError, LOCK_FILE};
pub use model::row::Row;
pub use store::{present, system_time};
pub use templates::trie::TrieError;
pub use store::disk::{day_name, day_of_name, refuse_other_versions, STAGING_DIR};
pub use store::manifest::path_in as manifest_path;
pub use store::fan::Fan;
pub use stop::Stop;
pub use store::{FlushSummary, StoreError};
pub use wal::{Advanced, Stats as WalStats};

/// What the command line's diagnostic commands reach past the handle for: `demo` draws the
/// trie, `bench-storage` writes a live day without a log and walks the frames of the files
/// it measures, and the `cursor` fuzz harness builds a database from its lines in memory. No stability promise, and `kibbr-server` never names it
/// (`scripts/check/no_internals.sh`).
#[doc(hidden)]
pub mod internals {
    pub use crate::engine::templates::catalog::path_in as catalog_path;
    pub use crate::engine::ingest::{ingest_line, into_memory, read_lines, MemoryDb, SinkError};
    pub use crate::engine::format::frame::{read as read_frame, Next};
    pub use crate::engine::lock::DataLock;
    pub use crate::engine::store::disk::{data_files, CompactOptions};
    pub use crate::engine::format::file::{open_sealed, Reader};
    pub use crate::engine::format::FormatError;
    pub use crate::engine::format::{
        HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, TRAILER_LEN,
    };
    pub use crate::engine::store::{FlushKind, StoreManager};
    pub use crate::engine::templates::trie::{Node, Trie, ROOT_LABEL};

    /// A database's trie and stores, as they are.
    pub fn parts(db: &crate::engine::Database) -> (&Trie, &StoreManager) {
        db.parts()
    }
}

/// What the tests of the crates above reach the database's parts with: not for anything
/// but a test, and no promise of any kind.
#[cfg(test)]
#[doc(hidden)]
pub mod testing {
    pub use crate::engine::templates::catalog::Catalog;
    pub use crate::engine::ingest::{ingest_line, read_lines};
    pub use crate::engine::store::disk::DATA_DIR;
    pub use crate::engine::store::{FlushKind, StoreManager, DEFAULT_FAN_ROWS};
    pub use crate::engine::templates::trie::Trie;
    pub use crate::engine::wal::Wal;

    /// The log's numbers, and the size a body's lines come to in it.
    pub mod wal {
        pub use crate::engine::wal::{counted, Stats};
    }
}

