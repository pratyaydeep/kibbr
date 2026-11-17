# Changelog

One entry per tag, newest first.

## 0.5.1 — 2026-11-18

- **The README's logo and console screenshot show on crates.io.** The page there turns a
  path into a link for `<img src>` but not for `<source srcset>`, so the four `srcset`
  values are full links now.
- No change to the code: the binary is 0.5.0's, with the new version number.

## 0.5.0 — 2026-11-18

- **One crate, from three.** The engine and the server are part of the `kibbr` crate now: its
  library is the engine, and its binary is the server and the command line.
  `cargo install --locked kibbr` is as before. `kibbr-engine` and `kibbr-server` get no more
  releases; both end at 0.4.1, which is a notice only. The library is how the binary is built
  and promises no stable interface: use the binary and its HTTP API.
- **`kibbr bench` is `kibbr-bench`,** a binary of its own that is not published
  (`crates/kibbr-devtools/`). It measures the `kibbr` in its own directory, so build both:
  `cargo build --release --workspace`, then `target/release/kibbr-bench --lines 1000000
  --profile farm`. It takes no `--config`, `--log-level` or `--log-format`. With it go the
  Dockerfile's `bench` target and `bench/run.sh`: the bench runs natively
  (`scripts/bench/README.md`).
- **A bench result saved before this release reads `NOT THE SAME BENCH`** beside a new one.
  The bench's digest of its own source changed with the move; take a new baseline.
- **kibbr's own log targets changed,** because the module paths did. In the JSON log
  (`--log-format json`) the `target` of a line is now:

  | before | now |
  |---|---|
  | `kibbr_server::lifecycle` | `kibbr::server::run` |
  | `kibbr_server::routes` | `kibbr::server::routes` |
  | `kibbr_engine::lifecycle` | `kibbr::lifecycle` |
  | `kibbr_engine::wal` | `kibbr::wal` |
  | `kibbr_engine::rows::disk` | `kibbr::rows::disk` |
  | `kibbr_engine::rows::scan_threads` | `kibbr::rows::scan_threads` |
  | `kibbr_engine::templates::trie` | `kibbr::templates::trie` |
  | `kibbr_engine::templates::snapshot` | `kibbr::templates::snapshot` |
  | `kibbr::cmd::load` | `kibbr::cli::load` |
  | `kibbr::cmd::compact` | `kibbr::cli::compact` |
  | `kibbr::tools::bench_storage` | `kibbr::cli::tools::bench_storage` |
  | `kibbr::logs` | `kibbr::cli::logging` |

  The text log, the banner's target and every message are as they were.
- No change to the on-disk format, to compression, to the HTTP API or to what any other
  command prints.
- **Four docs pages moved.** `docs/ask.md` is now the last section of `docs/serve.md`;
  `docs/things-to-try.md` is the last section of `docs/generate-and-parse.md`;
  `docs/container.md` is the first section of `deploy/README.md`; and `docs/layout.md` is
  replaced by `ARCHITECTURE.md`. New: `docs/README.md`, the index, and `docs/glossary.md`,
  kibbr's own words. The README's install line is `cargo install --locked kibbr`.
- **Where things are:** test data is in `tests/data/` (was `testdata/` and
  `tests/fixtures/`), and the bench scripts and results in `scripts/bench/` (was `bench/`).
- For contributors: `tests/source_rules.rs` holds the rules of the layers and replaces
  `scripts/check/spdx.sh`, `no_internals.sh` and `cmd_no_tools.sh`; `scripts/bump.sh` turns
  `## Unreleased` into the version's heading; `scripts/publish.sh` publishes one crate.

## 0.4.0 — 2026-11-12

- **JSON comes back as it went in.** An array comes back as that array, and an empty object
  `{}` as `{}` at its path; `{}`, `[]`, `null`, a missing key, `""`, `"{}"` and `"[]"` stay
  distinct. A query matches, counts and groups an array by its JSON text, as before. Rows
  written by an older build answer as they did (arrays as strings, no key for `{}`); an older
  build that compacts a day written by 0.4.0 turns its arrays back into strings.
- **Time columns read more forms:** decimal epoch seconds ("1679711330.570420890", exact to 9
  fraction digits as a string, to about 6 as a JSON number), 16- and 19-digit epochs
  (microseconds, nanoseconds), and "YYYY-MM-DD HH:MM:SS[.fraction] ZONE" for UTC, GMT, Z, EST,
  EDT, CST, CDT, MST, MDT, PST and PDT. Only in time columns; a column's stored type is
  unchanged. A template made before 0.4.0 keeps its time source until its data is loaded again.
- **Epoch-like digits beside integers widen to Int64,** not String. The catalog records
  whether a Datetime column's values were all epoch digits, so RFC 3339 dates beside integers
  become String in any order.
- **On-disk format still version 3, additions only:** an optional `epoch_digits` on
  catalog.log's column record, and an optional row-group section (kind 0x82) marking JSON
  values. 0.3.0 and 0.2.3 open, query and keep writing a directory 0.4.0 wrote
  (scripts/check/downgrade.sh, new).
- `scripts/check/roundtrip.sh` now holds arrays and empty objects to themselves.
- Bench: `bench/compression.py` reads corpora published as many files (`urls`), with
  `bench/sizes.py` for sizes before downloading; Bluesky 100M and HDFS logs sources.

## 0.3.0 — 2026-11-11

- **Three crates, from five.** `kibbr-engine` is now the whole database behind one handle,
  `Database`: what was `kibbr-core` is its `model` and `parse` modules, and what was
  `kibbr-storage` is its `format`, `templates`, `rows`, `ingest`, `wal` and `lock`
  modules. The bulk load, compaction runs and the answer JSON move into it from
  `kibbr-server`, which keeps HTTP and depends on `kibbr-engine` alone. `kibbr` is the
  command line, and opens a data directory only through `Database`.
- `kibbr-core` and `kibbr-storage` are not published from 0.3.0 on; their last version is
  0.2.3. A library crate that named either names `kibbr-engine` instead. Below 1.0 this is
  the breaking middle number, and the internal dependencies are pinned exactly (`=0.3.0`).
- The fuzz harnesses are behind a `fuzzing` feature, off by default, in `kibbr-engine`
  (`wal_record`, `segment`, `record_log`) and in `kibbr` (`cursor`, `trieql`). The
  `v3_segment` target is now `segment`. `cargo test --workspace --all-features` replays
  every fuzz regression; CI runs it so.
- Each module's unit tests are in a `tests.rs` beside it. The format test vectors are
  `testdata/format/`.
- No change for anyone using the binary or the HTTP API: the commands and flags, the
  settings, TrieQL, every route and answer, and the on-disk format (version 3) are those
  of 0.2.3.

## 0.2.3 — 2026-11-11

- **`bench-storage --zstd-threads N`**: zstd -19 runs on N threads, every CPU the process
  may use when not given; on one thread it is about 3 MB/s, hours on a corpus of tens of
  gigabytes. 1 is the plain encoder and the smallest. kibbr's own bytes are the same at
  any N. A line under the first table gives gzip -9's and zstd -19's seconds.
- **Bench tooling**: `bench/compression.py` passes `--zstd-threads`, keeps the timings,
  records the machine's physical cores, and lists CockroachDB, Elasticsearch and Spark
  from YScope's CLP datasets. `bench/cloud/` runs a job on an EC2 box that uploads its
  results and terminates itself; see bench/cloud/README.md.
- **The oracle answers with `times`**, as the engine has since 0.2.2, so
  `scripts/check/agree.sh` passes again. `scripts/check/parser_equivalence.sh` reads an
  epoch given as a JSON number.
- `kibbr-core` and `kibbr-storage` become part of `kibbr-engine` from 0.3.0; this is their
  last version.
- No change to the binary's commands other than the new flag, nor to the HTTP API or the
  on-disk format.

## 0.2.2 — 2026-11-09

- **Website**: https://pratyaydeep.github.io/kibbr/, one scroll-driven page that shows how
  a log line is stored: split into shape and values, walked through the trie, laid into
  typed columns, squeezed column by column, then queried. Built from `web/site/` by
  `npm run site` (web/scripts/site.mjs) and published by `.github/workflows/pages.yml`.
- **docs.rs**: the `kibbr` crate's front page is a tour of the code: the one idea, a
  section per crate and module, TrieQL samples, the quickstart, limits and stability.
- Cargo metadata: `homepage` is the website and `documentation` is docs.rs. The README
  names kibbr in its title and links the website; every doc under docs/ names kibbr in
  its title. CITATION.cff gains the website.
- No code changes: the binary, the HTTP API and the on-disk format are those of 0.2.1.

## 0.2.1 — 2026-10-29

- No code changes. The first release published by CI: publishing a GitHub release runs
  `.github/workflows/release.yml`, which checks the version, the CHANGELOG and the built
  web console, and publishes the five crates. See CONTRIBUTING.md.

## 0.2.0 — 2026-10-28

- **Web console**: `GET /` is a console of four pages in place of the one-page viewer. Logs:
  a search box, suggestions from the template tree, and the queries run from this browser.
  Results: matches over time, the rows as ingested or as columns, Explain, and the templates
  matched, paged as the list scrolls. Templates: the tree, and one template's columns and
  latest lines. Status: every value `/stats` and `/health` report. Light and dark themes, a
  banner when the server is degraded or storing in memory, and four `localStorage` keys.
- **Built from `web/`**: ReScript and React, bundled by Vite into one self-contained
  `viewer.html` - scripts, styles, fonts and icons inlined - that is committed and compiled
  in, so `cargo build`, the Docker image and the crates need no Node. CI fails when the
  committed page differs from what `web/` builds. See docs/viewer.md.
- No API or server behaviour changed. The page is ~458 KB against ~48 KB before, and the
  binary ~410 KB larger. NOTICE lists what the console bundles.

Known issues:

- The console has no control to save a query yet; Saved lists what `kibbr.saved` holds.
- The chart stacks every level other than warn and error as info.

## 0.1.1 — 2026-10-23

- Crate metadata and docs only; no code changes. Descriptions, links and the first
  paragraph of each crate's docs, for crates.io and docs.rs.

## 0.1.0 — 2026-10-21, first public release

- **Licence**: AGPL-3.0-only in place of the Elastic License 2.0; contributions are taken
  under MIT with a DCO sign-off. `/health` and the viewer's footer name the source.
- **Compaction**: `kibbr compact` and a server's own timer rebuild a finished day's
  small row groups into full ones, published in one step.
- **Crash safety**: a write-ahead log in front of the live door - a `200` from
  `/ingest` is durable, and `kill -9` loses and doubles nothing; cargo-fuzz over the five
  readers of untrusted bytes.
- **Scale**: `kibbr bench`; a disk scan filters on columns and is shared between
  threads; the plan reads a key index; `/ingest` pushes back with `503` instead of growing.
- **Crates and operations**: a workspace of five crates; a settings file (`--config`);
  `shutdown_deadline` and `request_timeout`; the log as text or JSON; `kibbr templates`,
  `stats` and `health`; a Dockerfile and Kubernetes manifests.
- **Refactor**: the server is a library and the binary the command line over it; `src/cmd`
  and `src/gen`; scripts filed under `check/`, `fault/` and `measure/`; comments and docs
  shortened. No behaviour changed.

Known issues:

- Ids in the epoch range are typed `Datetime`, then widened when a later value is not one
  (seen on GH Archive).
- The bulk door writes small row groups, which stay small until the day is compacted.
- `kibbr templates` fails on an answer over ~25 MB.
- Per-template overhead grows large at 1,500+ templates.

## v0.1.0-alpha.2 - 2026-10-03

- Renamed from TrieDB to kibbr. `load`, the bulk door, and `bench-storage`; one snapshot
  per query; mimalloc as the allocator; one fixed encoding per column type and zstd on
  chunks.

## v0.1.0-alpha.1 - 2026-09-24

- The first tag: the parser and type ladder, the trie, TrieQL and the query engine, the
  HTTP server and its viewer, day-partitioned disk storage with a template catalog, and
  storage format v3 - one shared file per UTC day - behind per-leaf locks.
