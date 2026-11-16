// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The commands that try, measure or test kibbr. No production command names them.
//!
//! | File | What it holds |
//! |---|---|
//! | `tools/gen_logs.rs` | `kibbr gen-logs`: reproducible logs, the `hostile` profile here and `farm` in `gen_logs/farm.rs` |
//! | `tools/parse.rs` | `kibbr parse`: each line's fingerprint and types |
//! | `tools/demo.rs` | `kibbr demo`: the trie over a file, as a tree |
//! | `tools/bench_storage.rs` | `kibbr bench-storage`: what a file costs to store |
//!
//! What someone runs to try, measure or test kibbr: `bench-storage`, the generators,
//! `parse` and `demo`. No production command names any of it.

pub mod bench_storage;
pub mod demo;
pub mod gen_logs;
pub mod parse;
