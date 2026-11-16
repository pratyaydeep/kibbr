// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! A line of text into a parsed line.
//!
//! | File | What it holds |
//! |---|---|
//! | `parse/lines.rs` | the next line of an input, read under a byte limit |
//! | `parse/flatten.rs` | a JSON object flattened to dotted keys, its fingerprint, and the parsed row |
//! | `parse/key.rs` | the identity of a flattened key |
//!
//! kibbr's own words are in `docs/glossary.md`.
//!
//! A line of text into a parsed line: reading lines under a byte cap, flattening a JSON
//! object into dotted keys, and the key paths themselves. No I/O beyond the reader handed in.

pub mod flatten;
pub mod key;
pub mod lines;
