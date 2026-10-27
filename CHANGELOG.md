# Changelog

One entry per tag, newest first.

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
