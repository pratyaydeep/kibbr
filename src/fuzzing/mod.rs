// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What the two fuzz targets over the command line's query building hold their subject to,
//! one module per target, each with one `run(data)`; the other three are the engine's
//! (`kibbr_engine::fuzzing`). They live here so `tests/fuzz_regressions.rs` replays every
//! committed crasher on the stable toolchain, with no libFuzzer in the build. Compiled
//! only with `fuzzing`.
//! A `run` returns for every input its subject handles correctly and panics when an
//! invariant breaks.

pub mod cursor;
pub mod trieql;
