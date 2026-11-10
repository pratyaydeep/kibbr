// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Every input a fuzz target ever crashed on, and the committed corpus, replayed through
//! the code the fuzzer runs (`kibbr::tools::fuzzing` and `kibbr_engine::fuzzing`) on the
//! stable toolchain, so a change that breaks an input the fuzzer already reached fails here.
//! Compiled with the `fuzzing` feature: `cargo test --all-features`.

#![cfg(feature = "fuzzing")]

use std::fs;
use std::path::Path;

fn replay(target: &str, run: fn(&[u8])) {
    let fuzz = Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz");
    for dir in ["regressions", "corpus"] {
        let dir = fuzz.join(dir).join(target);
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        let mut paths: Vec<_> = entries.map(|e| e.unwrap().path()).collect();
        paths.sort();
        for path in paths {
            if !path.is_file() {
                continue;
            }
            let data = fs::read(&path).unwrap();
            let caught = std::panic::catch_unwind(|| run(&data));
            assert!(caught.is_ok(), "{} breaks the {target} target", path.display());
        }
    }
}

#[test]
fn trieql() {
    replay("trieql", kibbr::tools::fuzzing::trieql::run);
}

#[test]
fn wal_record() {
    replay("wal_record", kibbr_engine::fuzzing::wal_record::run);
}

#[test]
fn cursor() {
    replay("cursor", kibbr::tools::fuzzing::cursor::run);
}

#[test]
fn segment() {
    replay("segment", kibbr_engine::fuzzing::segment::run);
}

#[test]
fn record_log() {
    replay("record_log", kibbr_engine::fuzzing::record_log::run);
}
