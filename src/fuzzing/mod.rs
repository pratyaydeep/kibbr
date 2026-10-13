// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What each fuzz target under `fuzz/` holds its subject to, one module per target, each
//! with one `run(data)`. They live here so `tests/fuzz_regressions.rs` replays every
//! committed crasher on the stable toolchain, with no libFuzzer in the build.
//! A `run` returns for every input its subject handles correctly and panics when an
//! invariant breaks.

pub mod cursor;
pub mod record_log;
pub mod trieql;
pub mod v3_segment;
pub mod wal_record;
