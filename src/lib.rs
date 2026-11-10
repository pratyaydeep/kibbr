// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! A log store for structured JSON logs that keeps a quarter of the bytes and answers in
//! milliseconds.
//!
//! kibbr takes NDJSON log lines, splits each into its **shape** (the set of keys) and its
//! **values**, stores every shape once in an in-memory trie and the values as typed
//! columns on disk. On application logs that is about 7.6% of the raw size, two thirds of
//! `gzip -9`, and still queryable. It ships as one static binary, `kibbr`, and this crate
//! is that binary.
//!
//! - **Store**: `POST /ingest` or `kibbr load`, both into the same day files, every line
//!   in a write-ahead log before it is acknowledged.
//! - **Query**: [TrieQL](engine::trieql), a small language over keys and values, through
//!   `POST /query`, `kibbr query` or the built-in web console.
//! - **Run**: `cargo install kibbr`, then `kibbr serve`. No cluster, no JVM, no sidecar.
//!
//! The guide, benchmarks and operating docs are on the
//! [website](https://pratyaydeep.github.io/kibbr/) and in the
//! [repository](https://github.com/pratyaydeep/kibbr). This page is a tour of the code.
//!
//! # A tour of kibbr
//!
//! The binary is built from four library crates, stacked so that each names only the
//! ones under it, plus the modules in this crate that make them a command.
//!
//! ## The one idea
//!
//! Every log line is two things:
//!
//! ```text
//! {"ts":"2024-01-15T09:12:03Z","level":"info","event":"login_ok","user":"u_812","ms":41}
//!
//! shape   {ts, level, event, user, ms}     stored once, as a path in the trie
//! values  2024-01-15T09:12:03Z info ...     appended to typed columns at that path's leaf
//! ```
//!
//! An application emits thousands of lines but only dozens of shapes. kibbr calls a shape
//! a *template*: the keys, each value's type, and an id. A query walks the trie first, so
//! templates that cannot match are skipped without touching disk.
//!
//! ## From a line to a template
//!
//! [`core`] (the `kibbr-core` crate) is what a line is before anything stores it: the
//! parser that flattens nested JSON into dotted keys, the *type ladder* that decides
//! whether a value is a `Datetime`, `Int64`, `Uuid`, `Ip`, `String` and so on, and the
//! fingerprint a template is found by. It has no I/O.
//!
//! ## Where rows live
//!
//! [`storage`] (`kibbr-storage`) keeps templates and rows: the trie and the catalog log
//! that rebuilds it, the write-ahead log, the day files of row groups and compressed
//! column chunks, the manifest that says which files count, and compaction. Each column
//! chunk picks its own encoding, then zstd on top: same-value-once, deltas for
//! timestamps, varints for integers, a dictionary for strings.
//!
//! ## Answering a question
//!
//! [`engine`] (`kibbr-engine`) parses TrieQL, plans a query over the trie's key index and
//! runs it over the stores, reading only the columns the query names:
//!
//! ```text
//! COUNT LOG {"event": "login_failed"} SINCE 1h
//! MATCH LOG {"level": "error", "service": *} SINCE 24h LIMIT 100
//! COUNT LOG {"level": "error"} SINCE 6h GROUP BY time(5m)
//! ```
//!
//! `MATCH` returns lines and `COUNT` a number; `{...}` is a pattern over keys and values;
//! `SINCE`, `BETWEEN`, `GROUP BY` and `LIMIT` do what they say.
//!
//! ## The server and the commands
//!
//! [`server`] (`kibbr-server`) is the HTTP side: `/ingest`, `/query`, `/templates`,
//! `/stats`, `/health`, the web console, and the loads and compactions it runs. [`cmd`]
//! holds the subcommands of the binary itself: `serve`, `load`, `compact`, `query`,
//! `explain`, `templates`, `stats`, `health`, `parse`, `demo`, `gen-logs`, `bench` and
//! `bench-storage`. [`client`] is the small HTTP client `load`, `compact` and
//! `bench-storage` use to talk to a server.
//!
//! ## Generated logs and the oracle
//!
//! [`gen`] writes reproducible fake logs (`kibbr gen-logs --profile farm --seed 42`), the
//! same lines on every machine, so a benchmark or a fault test means the same thing
//! everywhere. [`reference::naive`] is the oracle: every line, a closure, a counter.
//! Obviously correct and never shipped, it judges the real engine in `--reference` runs
//! and differential tests.
//!
//! ## Settings and logging
//!
//! [`config`] resolves every setting the same way: the flag, or else the `KIBBR__`
//! variable, or else the file `--config` names, or else the default. [`logs`] is kibbr's
//! own log, one `tracing` subscriber writing text or JSON lines to stderr. [`error`] is
//! the errors every command shares.
//!
//! # Running it
//!
//! ```text
//! cargo install kibbr
//! kibbr gen-logs --profile farm --lines 10000 --seed 42 > day.ndjson
//! kibbr serve --data-dir kibbr-data
//! curl -s -X POST --data-binary @day.ndjson http://127.0.0.1:7700/ingest
//! curl -s -X POST -d '{"q": "COUNT LOG {\"event\": \"login_ok\"}"}' http://127.0.0.1:7700/query
//! ```
//!
//! A `200` from `/ingest` means every line is in the write-ahead log and flushed. Open
//! `http://127.0.0.1:7700/` for the console. `kibbr compact --day 2024-01-15` shrinks a
//! finished day without changing any answer.
//!
//! # What it is, and is not
//!
//! - Structured JSON only. A line with no stable key set gains nothing.
//! - Not a full-text search engine: queries filter columns; there is no inverted index.
//! - Keys made up by users mint shapes without end. The main benefit is space, not speed.
//! - On prose-heavy data such as GitHub events, gzip is smaller. You cannot query gzip.
//! - Acknowledged means stored: the write-ahead log is replayed after a crash without
//!   loss or duplicates, every record and chunk carries a checksum, and the trie is a
//!   cache the catalog log can rebuild.
//!
//! # Stability
//!
//! The modules here are public so the fuzz targets under `fuzz/` can call the harnesses
//! in [`fuzzing`], and so the four crates can be read as one. None of it is a stable
//! interface: use the binary and its HTTP API, which are versioned in the
//! [changelog](https://github.com/pratyaydeep/kibbr/blob/main/CHANGELOG.md).
//!
//! Licensed AGPL-3.0-only; contributions under MIT.

pub use kibbr_engine as engine;
pub use kibbr_server as server;

pub mod client;
pub mod cmd;
pub mod config;
pub mod error;
pub mod fuzzing;
pub mod gen;
pub mod logs;

/// The oracle, compiled in only so `--reference` and a differential test can judge the
/// real engine.
pub mod reference {
    pub mod naive;
}
