// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The fuzz targets' harnesses, one module per target, each with one `run(data)`: the two
//! over the command line's query building are here, and the three over the engine's
//! readers are the engine's own (`engine::fuzzing`, which sees its private modules) and
//! named here, so `fuzz/` calls all five as `kibbr::fuzzing::<target>::run`. They live in
//! the library so `tests` here replays every committed crasher on the stable toolchain,
//! with no libFuzzer in the build. Compiled with `fuzzing`, and for this crate's tests.
//! A `run` returns for every input its subject handles correctly and panics when an
//! invariant breaks.

pub mod cursor;
pub mod trieql;

pub use crate::engine::fuzzing::{record_log, segment, wal_record};

#[cfg(test)]
mod tests;
