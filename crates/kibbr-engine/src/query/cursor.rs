// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The paging cursor: a row's place in its template's day, and the guard that holds it to
//! the query it came from.

use super::*;

// ── Cursors ────────────────────────────────────────────────────────────────────────

/// Where a page stopped: the `Rank` of its last row, which names a row and so is keyset
/// and stateless, plus a guard that refuses it against another pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    pub(super) rank: Rank,
    query: u32,
}

/// Bumped when `Rank`'s components change meaning, and read first in `decode`: a cursor
/// from an older layout would answer confidently with the wrong rows.
const CURSOR_VERSION: &str = "v3";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CursorError {
    #[error("not a cursor: expected `{CURSOR_VERSION}.` followed by {HEX_LEN} hex digits")]
    Malformed,
    #[error("cursor version `{found}` is not `{CURSOR_VERSION}` - it was issued before the page order changed; re-run the query without `AFTER`")]
    Version { found: String },
    #[error("this cursor came from a different query - a cursor is only valid for the pattern and order that produced it")]
    WrongQuery,
}

/// `[1 present][8 ts][8 leaf_key][8 seq][4 guard]`, big-endian, as hex.
const CURSOR_BYTES: usize = 1 + 8 + 8 + 8 + 4;
const HEX_LEN: usize = CURSOR_BYTES * 2;

impl Cursor {
    /// The fingerprint a cursor is checked against - the pattern and the order, not the time
    /// window, because `SINCE 1h` resolves against `now` and would refuse its own cursors.
    pub fn guard(query: &Query) -> u32 {
        let h = fnv(FNV_OFFSET, &Self::spelling(query));
        // Folded to 32 bits: this is a guard against a mistake, not against an attacker,
        // and the cursor is already long.
        ((h >> 32) ^ h) as u32
    }

    /// The bytes `guard` hashes, length-prefixed so that no two patterns spell the same bytes;
    /// `Query::tokens` is not this spelling and must not become it.
    pub fn spelling(query: &Query) -> Vec<u8> {
        fn text(out: &mut Vec<u8>, text: &str) {
            out.extend_from_slice(&(text.len() as u64).to_be_bytes());
            out.extend_from_slice(text.as_bytes());
        }
        let mut out = Vec::new();
        out.extend_from_slice(&(query.matches.len() as u64).to_be_bytes());
        for term in &query.matches {
            text(&mut out, &term.key);
            match &term.value {
                MatchValue::Any => out.push(0),
                MatchValue::Exact(value) => {
                    out.push(1);
                    text(&mut out, value);
                }
            }
        }
        // The order is by the effective time, whatever a template calls its column, so a cursor
        // survives a template being created with another name between two pages.
        out.extend_from_slice(b"effective_time");
        // Only oldest first adds bytes, so a newest-first cursor issued before `ORDER BY`
        // existed still reads; one order's cursor is refused by the other.
        if query.order == Order::OldestFirst {
            out.extend_from_slice(b" ascending");
        }
        out
    }

    /// The `Rank` a page stopped at, read by the scan and by the oracle.
    pub fn rank(&self) -> Rank {
        self.rank
    }

    pub(super) fn new(rank: Rank, query: &Query) -> Self {
        Self {
            rank,
            query: Self::guard(query),
        }
    }

    /// The cursor as opaque text: anyone who can write one by hand will depend on the layout
    /// `CURSOR_VERSION` exists to change.
    pub fn encode(&self) -> String {
        let (Reverse(ts), leaf, seq) = self.rank;
        let mut out = String::with_capacity(CURSOR_VERSION.len() + 1 + HEX_LEN);
        out.push_str(CURSOR_VERSION);
        out.push('.');
        let mut hex = |bytes: &[u8]| {
            for b in bytes {
                out.push_str(&format!("{b:02x}"));
            }
        };
        // `ts` is an `Option`, and the absent case is a real one: the flag carries it, and the
        // eight bytes beside it are then zero and mean nothing.
        match ts {
            Some(ns) => {
                hex(&[1]);
                hex(&ns.to_be_bytes());
            }
            None => {
                hex(&[0]);
                hex(&0i64.to_be_bytes());
            }
        }
        hex(&leaf.to_be_bytes());
        hex(&seq.to_be_bytes());
        hex(&self.query.to_be_bytes());
        out
    }

    /// The text back into a cursor; the guard is *not* checked here, because the query it
    /// belongs to is not built yet at parse time - see `check`.
    pub fn decode(text: &str) -> Result<Self, CursorError> {
        let (version, body) = text.split_once('.').ok_or(CursorError::Malformed)?;
        if version != CURSOR_VERSION {
            return Err(CursorError::Version {
                found: version.to_string(),
            });
        }
        if body.len() != HEX_LEN || !body.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(CursorError::Malformed);
        }
        let mut bytes = [0u8; CURSOR_BYTES];
        for (n, slot) in bytes.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&body[n * 2..n * 2 + 2], 16).map_err(|_| {
                // Unreachable - every byte was checked above. Not an `unwrap`, because
                // `decode` is fed by the CLI and "never panics" is the parser's rule too.
                CursorError::Malformed
            })?;
        }
        let word = |at: usize| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[at..at + 8]);
            buf
        };
        let ts = match bytes[0] {
            0 => None,
            _ => Some(i64::from_be_bytes(word(1))),
        };
        let mut guard = [0u8; 4];
        guard.copy_from_slice(&bytes[25..29]);
        Ok(Self {
            rank: (
                Reverse(ts),
                u64::from_be_bytes(word(9)),
                u64::from_be_bytes(word(17)),
            ),
            query: u32::from_be_bytes(guard),
        })
    }

    /// Whether this cursor belongs to this query, checked once before the scan.
    pub fn check(&self, query: &Query) -> Result<(), CursorError> {
        if self.query == Self::guard(query) {
            return Ok(());
        }
        Err(CursorError::WrongQuery)
    }
}
