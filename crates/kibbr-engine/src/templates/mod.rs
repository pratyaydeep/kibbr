// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The templates: the trie and its key index, `catalog.log` that makes it durable, and
//! `trie.json`, the readable snapshot. Nothing here knows of rows.

pub mod catalog;
pub mod snapshot;
pub mod trie;
