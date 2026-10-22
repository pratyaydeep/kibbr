// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The trie, catalog, day files and write-ahead log of
//! [kibbr](https://github.com/pratyaydeep/kibbr), a database for structured JSON logs.
//! Internal to kibbr; no API stability promised.
//!
//! Where templates and rows are kept: the trie and its catalog, the stores under it, the
//! write-ahead log, `db` - the write path for one parsed line - and `lock`.
//!
//! Above `kibbr-core` and under the engine: nothing here plans or answers a query.

use kibbr_core as core;

pub mod catalog;
pub mod db;
pub mod frame;
pub mod lock;
pub mod store;
pub mod trie;
pub mod triefile;
pub mod wal;
