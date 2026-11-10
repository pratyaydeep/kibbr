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

/// The trie and the stores of a data directory, restored with its log left as it is; the
/// caller holds the directory's `LOCK`. What the commands open a directory with until they
/// open a `Database`.
pub use lifecycle::open_durable;
