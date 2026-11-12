// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The fuzz targets' harnesses, one module per target, each with one `run(data)`: the two
//! over the command line's query building are here, and the three over the engine's
//! readers are the engine's own (`engine::fuzzing`, which sees its private modules) and
//! named here, so `fuzz/` calls all five as `kibbr::fuzzing::<target>::run`. They live in
//! the library so `tests` here replays every committed crasher on the stable toolchain,
//! with no libFuzzer in the build. Compiled with `fuzzing`, and for this crate's tests.
//! A `run` returns for every input its subject handles correctly and panics when an
//! invariant breaks.

pub mod cursor;
pub mod trieql;

pub use crate::engine::fuzzing::{record_log, segment, wal_record};

#[cfg(test)]
mod tests {
    //! Every input a fuzz target ever crashed on (`fuzz/regressions/`, committed), and the
    //! corpus where one is on disk (`fuzz/corpus/`, not in git), replayed through the code
    //! the fuzzer runs (`kibbr::fuzzing`) on the stable toolchain, so a change that breaks
    //! an input the fuzzer already reached fails here. A plain `cargo test` runs them.

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
                assert!(
                    caught.is_ok(),
                    "{} breaks the {target} target",
                    path.display()
                );
            }
        }
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
}
