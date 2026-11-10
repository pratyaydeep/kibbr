// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What the fuzz targets over the database's own readers hold their subject to, one module
//! per target, each with one `run(data)`: the log's records, a data file's segments, and
//! the catalog's and the manifest's records. They live beside what they test, with its
//! private parts in reach, and `tests/fuzz_regressions.rs` at the repository root replays
//! every committed crasher on the stable toolchain. Compiled only with `fuzzing`.
//! A `run` returns for every input its subject handles correctly and panics when an
//! invariant breaks.

pub mod record_log;
pub mod segment;
pub mod wal_record;
