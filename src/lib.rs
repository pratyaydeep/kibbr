// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Kibbr as a library: the modules the binary in `main.rs` is built from, public so
//! that the fuzz targets under `fuzz/` can call the harnesses in `fuzzing`. Nothing else
//! depends on this crate, and nothing here is a stable interface.

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
