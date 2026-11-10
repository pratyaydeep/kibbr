// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! How a template's tokens are spelled: a key, and a promoted `key=value`, and how a
//! promoted token reads back. The parser writes them and a query reads them, so they are
//! neither's.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

/// A key as a token, `=` written `\=` so a literal key cannot spell a promoted
/// `key=value`.
pub fn key_token(key: &str) -> Cow<'_, str> {
    if key.contains('=') {
        Cow::Owned(key.replace('=', "\\="))
    } else {
        Cow::Borrowed(key)
    }
}

/// A promoted key and its value as a token, joined by the first bare `=`.
pub fn promoted_token(key: &str, value: &str) -> String {
    format!("{}={value}", key_token(key))
}

/// A promoted token back into its key and value, split at the first unescaped `=`;
/// `None` for a key's token.
pub fn split_token(token: &str) -> Option<(Cow<'_, str>, &str)> {
    let mut chars = token.char_indices();
    while let Some((at, ch)) = chars.next() {
        match ch {
            '\\' => {
                chars.next();
            }
            '=' => {
                let key = &token[..at];
                let key = if key.contains("\\=") {
                    Cow::Owned(key.replace("\\=", "="))
                } else {
                    Cow::Borrowed(key)
                };
                return Some((key, &token[at + 1..]));
            }
            _ => {}
        }
    }
    None
}

/// The `key=value` tokens on this path, split back into unescaped pairs.
pub fn promoted_of<'a>(
    path: &'a [String],
    columns: &BTreeSet<&str>,
) -> BTreeMap<Cow<'a, str>, &'a str> {
    let mut out = BTreeMap::new();
    for label in path {
        // A leaf from before `=` was escaped may hold rows of both a literal and a promoted
        // `event=login`; its label does not speak for every row, so the scan decides.
        if columns.contains(label.as_str()) {
            continue;
        }
        if let Some((key, value)) = split_token(label) {
            if columns.contains(key.as_ref()) {
                out.insert(key, value);
            }
        }
    }
    out
}
