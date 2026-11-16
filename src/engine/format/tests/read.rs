// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::model::types::ColumnType;

/// The shortcut agrees with the ladder over text built to sit on every rung's edge.
#[test]
fn surely_text_never_claims_what_the_ladder_would_type() {
    let samples = [
        "",
        "INFO",
        "GET",
        "error",
        "debug",
        "abc",
        "deadbeef",
        "fe80::1",
        "::1",
        "false",
        "False",
        "true",
        "True",
        "TRUE",
        "inf",
        "NaN",
        "infinity",
        "-inf",
        "1e3",
        ".5",
        "+5",
        "-0",
        "0",
        "2024-01-15",
        "2024-01-15T10:30:00Z",
        "1705312200",
        "550e8400-e29b-41d4-a716-446655440000",
        "10.0.0.1",
        "u42@auth.example.com",
        "Tuesday",
        "a:b",
        "ab.cd",
        "c0ffee",
    ];
    for text in samples {
        if surely_text(text) {
            assert_eq!(
                infer(text).ty,
                Some(ColumnType::String),
                "`{text}` was called text"
            );
        }
    }
}
