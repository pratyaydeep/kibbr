// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Answering a query: the plan over the trie's key index, the execution over the stores,
//! and TrieQL, the text a query is written in.
//!
//! Above `kibbr-storage`, whose trie and stores it is handed, and under the binary: it
//! opens no socket and reads no flag.

pub mod engine;
pub mod limits;
pub mod trieql;
