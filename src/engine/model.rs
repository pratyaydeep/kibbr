// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a log line is made of once it is parsed.
//!
//! | File | What it holds |
//! |---|---|
//! | `model/types.rs` | the column types, and a typed value |
//! | `model/infer.rs` | the type ladder: a value's text read as a type |
//! | `model/tokens.rs` | how a template's tokens are spelled: a key, and a promoted `key=value` |
//! | `model/row.rs` | one row of values, and the instant it is filed under |
//!
//! kibbr's own words are in `docs/glossary.md`.
//!
//! What a log line is made of, once parsed: the column types and typed values, the type
//! ladder that reads a value's text into one, and the rules a template's tokens are spelled
//! by. No I/O.

pub mod infer;
pub mod row;
pub mod tokens;
pub mod types;
