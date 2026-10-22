// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The parser and type ladder of [kibbr](https://github.com/pratyaydeep/kibbr), a database
//! for structured JSON logs. Internal to kibbr; no API stability promised.
//!
//! What a log line is before anything stores it: its keys flattened, each value typed by
//! the ladder, and the fingerprint its template is found by - with the errors a line is
//! refused with and the few things every crate above prints and draws with.
//!
//! The bottom of the workspace: this crate names no other kibbr crate.

pub mod error;
pub mod flatten;
pub mod fmt;
pub mod infer;
pub mod key;
pub mod lines;
pub mod rng;
pub mod types;
