// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The flags and TrieQL build the same query, cursor included: `--after` takes a token as
//! `AFTER` does, and refuses what it refuses.
//!
//! Held here and not by a fuzz target: the flags are an operator's input, and a fuzz
//! target reaches the engine only. The engine's `cursor` harness reads each input and
//! hands every query it builds from TrieQL to `parity`, so the inputs replayed are its
//! own fixtures and every file in `fuzz/regressions/cursor/`, read as the fuzzer reads them.

use std::fs;
use std::path::Path;

use super::*;
use crate::engine::fuzzing::cursor::{self as harness, Flagged};
use crate::engine::query::CursorError;
use crate::engine::Error;

/// The query as the flags say it: a term is `KEY=VALUE` with `*` for any value, and
/// `--after` takes the token as it stands.
fn flags(asked: &Flagged) -> Args {
    let fetching = asked.mode == QueryMode::Fetch;
    Args {
        trieql: None,
        matches: asked
            .terms
            .iter()
            .map(|(key, want)| format!("{key}={}", want.unwrap_or("*")))
            .collect(),
        count: !fetching,
        fetch: fetching,
        exists: false,
        group_by: None,
        since: asked.since.map(str::to_string),
        between: asked
            .between
            .map(|pair| pair.iter().map(|at| at.to_string()).collect()),
        format: Format::Table,
        explain: false,
        reference: false,
        limit: asked.limit,
        after: Some(asked.after.to_string()),
        limits: asked.limits,
        schema: Schema::default(),
    }
}

/// One query, built both ways.
fn parity(asked: &Flagged) {
    let (src, text) = (asked.src, asked.after);
    let by_flags = build(&flags(asked), asked.now);
    let decoded = Cursor::decode(text);

    // `--count --after`: the flags hand the token to the decoder with no parser in front
    // of it, so the refusal is the decoder's own, and a cursor a count cannot page by is
    // dropped unread.
    if asked.mode != QueryMode::Fetch {
        match (by_flags, decoded) {
            (Ok(query), Ok(_)) => assert_eq!(query.cursor, None, "--count kept `{text}`"),
            (Err(CliError::Cursor(got)), Err(want)) => {
                assert_eq!(got, want, "--count --after `{text}`");
            }
            (other, _) => panic!("--count --after `{text}`: {other:?}"),
        }
        return;
    }

    match (decoded, asked.built) {
        (Err(want), _) => match by_flags {
            Err(CliError::Cursor(got)) => assert_eq!(got, want, "--after `{text}`"),
            other => panic!("--after took text that is not a cursor: {other:?}"),
        },
        // The flags build the query the text does, cursor included, so what the harness
        // shows of one by paging it, it shows of both.
        (Ok(_), Ok(query)) => {
            let flagged = by_flags.unwrap_or_else(|e| panic!("--after refused `{text}`: {e:?}"));
            assert_eq!(
                flagged.matches, query.matches,
                "`{src}`: the flags' pattern"
            );
            assert_eq!(flagged.mode, query.mode, "`{src}`: the flags' mode");
            assert_eq!(
                (flagged.time.since, flagged.time.until),
                (query.time.since, query.time.until),
                "`{src}`: the flags' window"
            );
            assert_eq!(
                (flagged.limit, flagged.limit_source),
                (query.limit, query.limit_source),
                "`{src}`: the flags' limit"
            );
            assert_eq!(flagged.cursor, query.cursor, "`{src}`: the flags' cursor");
        }
        (Ok(_), Err(Error::Cursor(CursorError::WrongQuery))) => match by_flags {
            Err(CliError::Cursor(CursorError::WrongQuery)) => {}
            other => panic!("`{src}`: --after did not refuse another query's cursor: {other:?}"),
        },
        (Ok(_), Err(other)) => panic!("`{src}` refused a cursor for something else: {other:?}"),
    }
}

#[test]
fn the_flags_build_the_query_trieql_builds_for_every_cursor_of_the_fixture() {
    let inputs = harness::fixtures();
    assert!(!inputs.is_empty(), "the harness has no fixture");
    for input in inputs {
        harness::run_with(&input, &parity);
    }
}

#[test]
fn the_flags_build_the_query_trieql_builds_for_every_cursor_regression() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/regressions/cursor");
    let mut paths: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no input under {}", dir.display());
    for path in paths {
        let data = fs::read(&path).unwrap();
        let caught = std::panic::catch_unwind(|| harness::run_with(&data, &parity));
        assert!(caught.is_ok(), "{} breaks the flags", path.display());
    }
}
