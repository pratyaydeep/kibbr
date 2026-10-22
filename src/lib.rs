// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! kibbr is a database for structured JSON logs, shipped as one binary.
//!
//! It splits every log line into its shape (the set of keys) and its values.
//! Each shape is stored once, in an in-memory trie; the values go to disk as
//! typed columns. Application logs are stored at a fraction of their raw size
//! and stay queryable in milliseconds.
//!
//! **This crate is the `kibbr` command.** Install it with `cargo install kibbr`.
//! The quickstart, benchmarks and docs are in the
//! [README](https://github.com/pratyaydeep/kibbr).
//!
//! ## As a library
//!
//! The modules below are what the binary is built from. They are public so
//! the fuzz targets under `fuzz/` can call the harnesses in [`fuzzing`].
//! Nothing here is a stable interface.

pub use kibbr_core as core;
pub use kibbr_storage as storage;
pub use kibbr_engine as engine;
pub use kibbr_server as server;

pub mod client;
pub mod cmd;
pub mod config;
pub mod error;
pub mod fuzzing;
pub mod gen;
pub mod logs;

/// The oracle, compiled in only so `--reference` and a differential test can judge the
/// real engine.
pub mod reference {
    pub mod naive;
}
