// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The query planner, executor and TrieQL of [kibbr](https://github.com/pratyaydeep/kibbr),
//! a database for structured JSON logs. Internal to kibbr; no API stability promised.
//!
//! Answering a query: the plan over the trie's key index, the execution over the stores,
//! and TrieQL, the text a query is written in.
//!
//! Above `kibbr-storage`, whose trie and stores it is handed, and under the binary: it
//! opens no socket and reads no flag.

use kibbr_core as core;
use kibbr_storage as storage;

pub mod answer;
pub mod compact;
pub mod database;
pub mod engine;
pub mod error;
mod lifecycle;
pub mod limits;
pub mod load;
pub mod trieql;

pub use database::{
    Claim, CompactRefused, Database, Ingested, LineError, Opener, Options, Refused, Schema,
    Stats, Storage, Template, TemplateColumn,
};
pub use error::{Error, IngestError, Result};
pub use lifecycle::Replayed;

// The values the handle takes and hands back that are not its own: a query's stop and
// what it fails with, a flush's summary, the log's numbers, the scan threads a server lets
// go last, and how a UTC day and a line's limit are said.
pub use core::fmt;
pub use core::lines::MAX_LINE_BYTES;
pub use storage::db::{load_lines, row_of, LoadStats, ReadError};
pub use storage::lock::{holder, Holder, LockError, LOCK_FILE};
pub use storage::store::{present, system_time, Row};
pub use storage::trie::TrieError;
pub use storage::store::disk::{day_name, day_of_name, refuse_other_versions, STAGING_DIR};
pub use storage::store::manifest::path_in as manifest_path;
pub use storage::store::fan::Fan;
pub use storage::store::{FlushSummary, Stop, StoreError};
pub use storage::wal::{Advanced, Stats as WalStats};

/// What the command line's diagnostic commands reach past the handle for: `demo` draws the
/// trie, and `bench-storage` writes a live day without a log and walks the frames of the
/// files it measures. No stability promise, and `kibbr-server` never names it
/// (`scripts/check/no_internals.sh`).
#[doc(hidden)]
pub mod internals {
    pub use crate::storage::catalog::path_in as catalog_path;
    pub use crate::storage::db::{ingest_line, read_lines, SinkError};
    pub use crate::storage::frame::{read as read_frame, Next};
    pub use crate::storage::lock::DataLock;
    pub use crate::storage::store::disk::{data_files, CompactOptions};
    pub use crate::storage::store::v3::file::open_sealed;
    pub use crate::storage::store::v3::{
        HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, TRAILER_LEN,
    };
    pub use crate::storage::store::{FlushKind, StoreManager};
    pub use crate::storage::trie::{Node, Trie, ROOT_LABEL};

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
    pub use crate::storage::catalog::Catalog;
    pub use crate::storage::db::{ingest_line, read_lines};
    pub use crate::storage::store::disk::DATA_DIR;
    pub use crate::storage::store::{FlushKind, StoreManager, DEFAULT_FAN_ROWS};
    pub use crate::storage::trie::Trie;
    pub use crate::storage::wal::{self, Wal};
}

/// The trie and the stores of a data directory, restored with its log left as it is; the
/// caller holds the directory's `LOCK`. What the commands open a directory with until they
/// open a `Database`.
pub use lifecycle::open_durable;
