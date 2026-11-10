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
pub mod fmt;
pub mod rng;
pub mod error;
pub(crate) mod lock;
pub(crate) mod format;
pub(crate) mod templates;
pub(crate) mod rows;
pub(crate) mod ingest;
pub(crate) mod wal;
pub mod load;
pub mod compact;
pub mod query;
mod lifecycle;
pub mod database;
#[cfg(feature = "fuzzing")]
pub mod fuzzing;

pub use database::{
    Claim, CompactRefused, Database, Ingested, LineError, Opener, Options, Refused, Schema,
    Stats, Storage, Template, TemplateColumn,
};
pub use error::{Error, IngestError, Result};
pub use lifecycle::Replayed;

// The values the handle takes and hands back that are not its own: a query's stop and
// what it fails with, a flush's summary, the log's numbers, the scan threads a server lets
// go last, and how a UTC day and a line's limit are said.
pub use parse::lines::MAX_LINE_BYTES;
pub use ingest::{load_lines, row_of, LoadStats, ReadError};
pub use lock::{holder, Holder, LockError, LOCK_FILE};
pub use model::row::Row;
pub use rows::{present, system_time};
pub use templates::trie::TrieError;
pub use rows::disk::{day_name, day_of_name, refuse_other_versions, STAGING_DIR};
pub use rows::manifest::path_in as manifest_path;
pub use rows::scan_threads::Fan;
pub use rows::{FlushSummary, Stop, StoreError};
pub use wal::{Advanced, Stats as WalStats};

/// What the command line's diagnostic commands reach past the handle for: `demo` draws the
/// trie, `bench-storage` writes a live day without a log and walks the frames of the files
/// it measures, and the `cursor` fuzz harness builds a database from its lines in memory. No stability promise, and `kibbr-server` never names it
/// (`scripts/check/no_internals.sh`).
#[doc(hidden)]
pub mod internals {
    pub use crate::templates::catalog::path_in as catalog_path;
    pub use crate::ingest::{ingest_line, load, read_lines, Db, SinkError};
    pub use crate::format::frame::{read as read_frame, Next};
    pub use crate::lock::DataLock;
    pub use crate::rows::disk::{data_files, CompactOptions};
    pub use crate::format::file::{open_sealed, Reader};
    pub use crate::format::FormatError;
    pub use crate::format::{
        HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, TRAILER_LEN,
    };
    pub use crate::rows::{FlushKind, StoreManager};
    pub use crate::templates::trie::{Node, Trie, ROOT_LABEL};

    /// A database's trie and stores, as they are.
    pub fn parts(db: &crate::Database) -> (&Trie, &StoreManager) {
        db.parts()
    }
}

/// What the tests of the crates above reach the database's parts with: not for anything
/// but a test, and no promise of any kind.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod testing {
    pub use crate::templates::catalog::Catalog;
    pub use crate::ingest::{ingest_line, read_lines};
    pub use crate::rows::disk::DATA_DIR;
    pub use crate::rows::{FlushKind, StoreManager, DEFAULT_FAN_ROWS};
    pub use crate::templates::trie::Trie;
    pub use crate::wal::Wal;

    /// The log's numbers, and the size a body's lines come to in it.
    pub mod wal {
        pub use crate::wal::{counted, Stats};
    }
}

