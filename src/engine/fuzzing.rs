// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What each fuzz target runs: one module for each target, each with one `run(data)`.
//!
//! | File | What it holds |
//! |---|---|
//! | `fuzzing/trieql.rs` | the TrieQL parser: it never panics, and an error points inside the input |
//! | `fuzzing/cursor.rs` | the `AFTER` cursor: a forged token, or one of another query, is refused |
//! | `fuzzing/wal_record.rs` | the write-ahead log's reader, against a second reader written from the format |
//! | `fuzzing/segment.rs` | a data file's bytes through every way the store reads one |
//! | `fuzzing/record_log.rs` | the record reader under `catalog.log` and `manifest.log` |
//! | `tests/fuzzing.rs` | every saved input replayed through its harness |
//!
//! What the fuzz targets hold their subject to, one module per target, each with one
//! `run(data)`: a TrieQL string, an `AFTER` cursor, the log's records, a data file's
//! segments, and the catalog's and the manifest's records. They live beside what they
//! test, with its private parts in reach, and `tests/fuzzing.rs` replays every committed
//! crasher on the stable toolchain. Compiled only with `fuzzing`, but for `cursor`, which
//! `test-support` compiles too: the command line's test of its flags reads an input
//! through it.
//! A `run` returns for every input its subject handles correctly and panics when an
//! invariant breaks.

pub mod cursor;
#[cfg(feature = "fuzzing")]
pub mod record_log;
#[cfg(feature = "fuzzing")]
pub mod segment;
#[cfg(feature = "fuzzing")]
pub mod trieql;
#[cfg(feature = "fuzzing")]
pub mod wal_record;

#[cfg(all(test, feature = "fuzzing"))]
#[path = "tests/fuzzing.rs"]
mod tests;
