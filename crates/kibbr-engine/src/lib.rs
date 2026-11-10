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
pub use storage::lock::LockError;
pub use storage::store::disk::{day_name, day_of_name};
pub use storage::store::fan::Fan;
pub use storage::store::{FlushSummary, Stop, StoreError};
pub use storage::wal::{Advanced, Stats as WalStats};

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
