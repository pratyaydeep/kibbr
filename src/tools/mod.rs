// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What someone runs to try, measure or test kibbr: the bench, the generators, `parse`,
//! `demo` and the fuzz harnesses. A command in `cmd` never names any of it.

pub mod bench;
pub mod bench_storage;
pub mod demo;
#[cfg(feature = "fuzzing")]
pub mod fuzzing;
pub mod gen;
pub mod parse;
