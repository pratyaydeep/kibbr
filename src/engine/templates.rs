// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The templates: every shape that kibbr has stored.
//!
//! | File | What it holds |
//! |---|---|
//! | `templates/trie.rs` | the trie: one token an edge, a template at each leaf |
//! | `templates/trie/keys.rs` | the key index a plan reads: for each key, the templates that have it |
//! | `templates/catalog.rs` | `catalog.log`: the durable record of every template |
//! | `templates/snapshot.rs` | `trie.json`: a readable snapshot, written at a clean stop |
//!
//! kibbr's own words are in `docs/glossary.md`.
//!
//! The templates: the trie and its key index, `catalog.log` that makes it durable, and
//! `trie.json`, the readable snapshot. Nothing here knows of rows.

pub(crate) mod catalog;
pub(crate) mod snapshot;
pub(crate) mod trie;
