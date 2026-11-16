// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What a log line is made of, once parsed: the column types and typed values, the type
//! ladder that reads a value's text into one, and the rules a template's tokens are spelled
//! by. No I/O.

pub mod infer;
pub mod row;
pub mod tokens;
pub mod types;
