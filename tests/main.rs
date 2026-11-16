// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The end-to-end tests of kibbr, as one binary: `cargo test --test e2e`. Most of them
//! run the real `kibbr` binary and read what it prints or answers. One module a topic;
//! `cargo test --test e2e serve::` runs one topic.
//!
//! One binary and not one a file: it links once, and every test of the suite runs in one
//! process, in parallel. So no test here may change what the process is: no environment
//! variable set, no working directory changed, no fixed port, and each scratch directory
//! is named for its topic.

mod harness;

// What a line becomes: the generators, the parser's types, shapes and the trie.
mod farm;
mod fixture;
mod shapes;
mod time_types;
mod trie;

// Asking: the flags, TrieQL, buckets, and what the answer looks like.
mod buckets;
mod decisions;
mod format;
mod query;
mod trieql;

// Storing: the two doors, the files, compaction, and what a start finds.
mod compact;
mod compat;
mod load;
mod restart;
mod segments;

// The server and the process: the HTTP contract, settings, signals and the log.
mod operations;
mod serve;

// `bench-storage`.
mod bench;

// Not end to end: the rules of the layers, read off the source.
mod source_rules;
