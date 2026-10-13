// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Where templates and rows are kept: the trie and its catalog, the stores under it, the
//! write-ahead log, `db` - the write path for one parsed line - and `lock`.
//!
//! Above `kibbr-core` and under the engine: nothing here plans or answers a query.

pub mod catalog;
pub mod db;
pub mod frame;
pub mod lock;
pub mod store;
pub mod trie;
pub mod triefile;
pub mod wal;
