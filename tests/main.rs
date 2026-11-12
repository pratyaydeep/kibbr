// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The integration tests: one binary, `e2e`, that runs the real `kibbr` binary from
//! outside, one module a topic. `harness` is what they share: the binary as a command, a
//! server as a process, a scratch data directory, one HTTP request, and the inputs - and
//! the one list of library items any test reaches past the binary for.
//!
//! `cargo test --test e2e -- <module>::` runs one topic. `bench::speed` runs `kibbr-bench`,
//! which is built only with `--features bench`; `--all-features` runs everything. The fuzz
//! regressions are unit tests of the crate (`src/fuzzing.rs`), replayed by a plain
//! `cargo test`. `layers` reads the source rather than running the binary.

mod harness;

/// `kibbr bench-storage`, and `kibbr-bench` with the `bench` feature.
mod bench;
/// `kibbr compact` and a server's compaction.
mod compact;
/// `kibbr gen-logs`.
mod gen;
/// Which layer of the source may name which, and the licence header on every file.
mod layers;
/// `kibbr load`, the bulk door.
mod load;
/// Settings, the commands that ask a running server, the log, and a stop's deadline.
mod operations;
/// `kibbr query`: exists, limits and paging, time buckets, output formats.
mod query;
/// `kibbr serve` and its HTTP surface.
mod serve;
/// Row groups, restarts, and data directories other builds wrote.
mod storage;
/// The trie: one tree whatever order the lines came in.
mod templates;
/// The TrieQL parser and its agreement with the flags.
mod trieql;
/// The type each value is given, the template each line is, and a value back as itself.
mod values;
