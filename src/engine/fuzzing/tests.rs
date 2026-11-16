// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! Every input a fuzz target ever crashed on, and the committed corpus, replayed through
//! the code the fuzzer runs on the stable toolchain, so a change that breaks an input the
//! fuzzer already reached fails here. Compiled with the `fuzzing` feature:
//! `cargo test --workspace --all-features`.

use std::fs;
use std::path::Path;

fn replay(target: &str, run: fn(&[u8])) {
    // Two directories up: the repository root, where `fuzz/` is.
    let fuzz = Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz");
    let mut replayed = 0;
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
            replayed += 1;
        }
    }
    // A target with nothing to replay is a moved folder or a renamed target, not a pass.
    assert!(
        replayed > 0,
        "no input for the {target} target under {}",
        fuzz.display()
    );
}

#[test]
fn trieql() {
    replay("trieql", super::trieql::run);
}

#[test]
fn wal_record() {
    replay("wal_record", super::wal_record::run);
}

#[test]
fn cursor() {
    replay("cursor", super::cursor::run);
}

#[test]
fn segment() {
    replay("segment", super::segment::run);
}

#[test]
fn record_log() {
    replay("record_log", super::record_log::run);
}
