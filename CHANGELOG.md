# Changelog

One entry per tag, newest first.

## 0.4.0 — 2026-11-12

- TODO: what changed

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
