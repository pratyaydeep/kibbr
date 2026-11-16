// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! [kibbr](https://github.com/pratyaydeep/kibbr), a log store for structured JSON logs that
//! keeps a fraction of the bytes and answers in milliseconds. **The product is the `kibbr`
//! binary and its HTTP API.** This library is the engine that binary is built on: the whole
//! database behind one handle, [`Database`], with no HTTP, no flags and no async.
//!
//! # No stable interface
//!
//! The library is how the binary is built, and it promises no stable interface. Its items
//! are public so that the binary, the bench and the fuzz targets can name them, and any of
//! them may change in any release. Use the binary and its HTTP API, which are versioned in
//! the [changelog](https://github.com/pratyaydeep/kibbr/blob/main/CHANGELOG.md).
//!
//! The features `test-support` and `fuzzing` are for code that is not shipped: the hooks
//! the binary's tests reach into a database with, and the fuzz targets' harnesses. **Never
//! enable either in a build that runs in production.** `test-support` only adds items and
//! changes no behaviour, but with it on a long-running server keeps a list that grows
//! without bound.
//!
//! # Where the code is
//!
//! - This library's root is `src/engine/lib.rs`, so there is no `src/engine.rs` beside
//!   `src/server.rs`.
//! - The binary, `src/main.rs`, holds the server and the command line. Inside the binary,
//!   `kibbr::` means this library and `crate::` means the binary.
//! - `ARCHITECTURE.md` in the repository is the map of both.
//!
//! # The one idea
//!
//! kibbr takes NDJSON log lines and splits each into its **shape** (the set of keys) and
//! its **values**. It stores every shape once, in an in-memory trie, and the values as
//! typed columns on disk:
//!
//! ```text
//! {"ts":"2024-01-15T09:12:03Z","level":"info","event":"login_ok","user":"u_812","ms":41}
//!
//! shape   {ts, level, event, user, ms}     stored once, as a path in the trie
//! values  2024-01-15T09:12:03Z info ...     appended to typed columns at that path's leaf
//! ```
//!
//! An application emits thousands of lines but only dozens of shapes. kibbr calls a shape
//! a *template*: the keys, each value's type, and an id. A query walks the trie first, so
//! templates that cannot match are skipped without touching disk. On application logs the
//! result is about 7.6% of the raw size, two thirds of `gzip -9`, and still queryable.
//!
//! # The engine
//!
//! Everything needed to store and answer is behind [`Database`]: a line parsed and its
//! template found, its row logged and placed, the day files, loads and compactions, and
//! TrieQL answered over them. The server and the command line each hold one `Database`,
//! opened to serve, offline, or in memory.
//!
//! The modules, in the order a log line travels:
//!
//! | Group | Module | What it holds |
//! |---|---|---|
//! | A line before storage | [`model`] | the column types and typed values, the type ladder, a template's tokens, a row |
//! | | [`parse`] | a line read under a byte limit, flattened to dotted keys, and its fingerprint |
//! | Its shape | `templates` | the trie and its key index, `catalog.log`, `trie.json` |
//! | Writing | `ingest` | the live door: one parsed line into the trie and its store |
//! | | [`load`] | the bulk door: a closed day staged and committed in one record |
//! | | `wal` | the write-ahead log: group commit, replay, the checkpoint |
//! | Stored, and compressed | `rows` | the stores (memory, and the day files), the scan, the manifest, compaction |
//! | | `format` | format version 3: frames, row groups, the column encodings, the file |
//! | Asking | [`query`] | TrieQL, the plan, the scan, buckets, the cursor, the limits, the answer |
//! | The handle and the rest | [`database`] | `Database` and `Options`: three ways to open, and everything asked of it |
//! | | `lifecycle` | open (restore and replay), the checkpoint, the close order |
//! | | [`compact`] | which days a compaction run takes, its report, one run at a time |
//! | | `lock` | `LOCK`: one process owns a data directory |
//! | | [`error`], [`fmt`], [`rng`] | the errors, how values are printed, a seeded random generator |
//!
//! kibbr's own words (template, leaf, live door, bulk door, checkpoint) are in
//! `docs/glossary.md` in the repository.
//!
//! [`model`] and [`parse`] are what a line is before anything stores it: the parser that
//! flattens nested JSON into dotted keys, the *type ladder* that decides whether a value
//! is a `Datetime`, `Int64`, `Uuid`, `Ip`, `String` and so on, and the fingerprint a
//! template is found by. They have no I/O of their own.
//!
//! Under the handle, and private to this crate, `templates` keeps the trie and the
//! catalog log that rebuilds it, `ingest` files one parsed line into the trie and its
//! store, `wal` is the write-ahead log, and `rows` keeps the day files of row groups and
//! compressed column chunks, the manifest that says which files count, and compaction.
//! `format` is how a day file is laid out. Each column chunk picks its own encoding,
//! then zstd on top: same-value-once, deltas for timestamps, varints for integers, a
//! dictionary for strings. `internals` holds what the command line's diagnostics read
//! past the handle.
//!
//! [`query`] parses [TrieQL](query::trieql), plans a query over the trie's key index and
//! runs it over the stores, reading only the columns the query names:
//!
//! ```text
//! COUNT LOG {"event": "login_failed"} SINCE 1h
//! MATCH LOG {"level": "error", "service": *} SINCE 24h LIMIT 100
//! COUNT LOG {"level": "error"} SINCE 6h GROUP BY time(5m)
//! ```
//!
//! `MATCH` returns lines and `COUNT` a number; `{...}` is a pattern over keys and values;
//! `SINCE`, `BETWEEN`, `GROUP BY` and `LIMIT` do what they say.
//!
//! # Running it
//!
//! ```text
//! cargo install --locked kibbr
//! kibbr gen-logs --profile farm --lines 10000 --seed 42 > day.ndjson
//! kibbr serve --data-dir kibbr-data
//! curl -s -X POST --data-binary @day.ndjson http://127.0.0.1:7700/ingest
//! curl -s -X POST -d '{"q": "COUNT LOG {\"event\": \"login_ok\"}"}' http://127.0.0.1:7700/query
//! ```
//!
//! A `200` from `/ingest` means every line is in the write-ahead log and flushed. Open
//! `http://127.0.0.1:7700/` for the console. `kibbr compact --day 2024-01-15` shrinks a
//! finished day without changing any answer. The guide, benchmarks and operating docs are
//! on the [website](https://pratyaydeep.github.io/kibbr/).
//!
//! # What it is, and is not
//!
//! - Structured JSON only. A line with no stable key set gains nothing.
//! - Not a full-text search engine: queries filter columns; there is no inverted index.
//! - Keys made up by users mint shapes without end. The main benefit is space, not speed.
//! - On prose-heavy data such as GitHub events, gzip is smaller. You cannot query gzip.
//! - Acknowledged means stored: the write-ahead log is replayed after a crash without
//!   loss or duplicates, every record and chunk carries a checksum, and the trie is a
//!   cache the catalog log can rebuild.
//!
//! Licensed AGPL-3.0-only; contributions under MIT.

// A line before storage.
pub mod model;
pub mod parse;

// Its shape.
pub(crate) mod templates;

// Writing.
pub(crate) mod ingest;
pub mod load;
pub(crate) mod wal;

// Stored, and compressed.
pub(crate) mod format;
pub(crate) mod rows;

// Asking.
pub mod query;

// The handle and the rest.
pub mod compact;
pub mod database;
pub mod error;
pub mod fmt;
mod lifecycle;
pub(crate) mod lock;
pub mod rng;

// Not shipped: the fuzz targets' harnesses.
#[cfg(any(feature = "fuzzing", feature = "test-support"))]
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
/// trie, and `bench-storage` writes a live day without a log and walks the frames of the files
/// it measures. No stability promise, and the binary's server module never names it
/// (`tests/source_rules.rs`).
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

