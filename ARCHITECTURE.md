# Architecture

This file is a map of kibbr for someone who wants to change the code. It names files,
modules and types. It does not link to them: use your editor's file and symbol search.
It changes rarely. Details live in each module's `//!` comment. Words in *italics* are in
the [glossary](docs/glossary.md).

## The one idea

Every JSON log line has a **shape** (its set of keys) and **values**.
An application writes millions of lines but only dozens of shapes.
kibbr stores each shape once, as a path in an in-memory *trie*. That shape is a *template*.
It stores the values as typed, compressed columns in one *day file* per UTC day.
A query walks the trie first, so it skips templates that cannot match without any disk I/O.

## Bird's-eye view

One Cargo package, `kibbr`, in three layers, each naming only the ones under it:

- `src/engine.rs` and `src/engine/`: the whole database behind one handle, `Database`.
  Synchronous: no async runtime, no HTTP, no command-line flags. The layers above name
  only what `engine.rs` offers: the items it re-exports (`engine::Database`,
  `engine::commas`, ...) and four namespaces, `engine::model`, `engine::query`,
  `engine::load` and `engine::compaction`. Every other engine module is private. Two
  doors reach further, each for named callers: `engine::inspect` (the diagnostic
  commands, some tests, and `kibbr::testing`) and `engine::fuzzing` (the fuzz harnesses).
- `src/server.rs` and `src/server/`: HTTP over one `Database`.
- `src/cli.rs` and `src/cli/`: the command line, one file a subcommand. `src/main.rs` sets
  the allocator (mimalloc; why is at `#[global_allocator]`) and calls `kibbr::run()`.

Beside them, behind features: `src/bench.rs` and `src/bench/` (the `kibbr-bench` binary,
`src/bin/kibbr-bench.rs`, feature `bench`), and `src/fuzzing.rs` and `src/fuzzing/` (the
fuzz harnesses, feature `fuzzing`, and always in the crate's own tests). `src/lib.rs` keeps
all of it private; its few `#[doc(hidden)]` exports are for the two binaries, `fuzz/` and
`tests/` (`kibbr::testing`).
Every command holds one `Database`, and opens a data directory no other way.

Write path (engine paths are under `src/engine/`):

```
 POST /ingest  (live door)                   kibbr load / POST /load  (bulk door)
 src/server/handlers.rs  admit, post_ingest  src/cli/load.rs, src/server/handlers.rs post_load
 src/server/body.rs      body -> lines        │
        │ Database::ingest                    │ Database::load        (database.rs)
        ▼                                     ▼
 parse/flatten.rs parse_line ── model/infer.rs infer (the type ladder)
        │ ParsedLog: keys, typed values, fingerprint
        ▼
 ingest.rs place / place_in ── templates/trie.rs Trie: find or make the template
        │                   └─ templates/catalog.rs: catalog.log record, flushed first
        │ Placed: template id + row
        ├──────────────────────────────────┐
        ▼ live                             ▼ bulk
 wal.rs Wal: group commit, fsync      load.rs: stage full row groups,
        │ (now the line is durable)     publish them in ONE manifest record
        │ the applier thread
        ▼
 database/lifecycle.rs Held::apply ─► store.rs StoreManager ─► LeafStore (store/memory.rs | store/disk.rs)
        │ rows applied: the HTTP 200 goes back
        │ flush, on the clock in src/server/runtime.rs
        ▼
 format/build.rs RowGroup ─► format/file.rs Writer ─► data/YYYY-MM-DD/seg_0_NNNNNN.col
 store/manifest.rs: manifest.log names every live file;  wal.rs: the checkpoint moves up
 later: store/disk/rebuild.rs rebuilds a finished day; database/compaction.rs keeps the job table
```

Query path:

```
 POST /query  src/server/handlers.rs post_query       kibbr query  src/cli/query.rs
        │ Database::answer_until                        │ TrieQL text or flags -> Query
        ▼                                               │ --reference -> src/cli/query/oracle.rs
 query/answer.rs ─► query/trieql.rs parse -> Query      ▼ Database::execute
        ▼
 query/plan.rs     walk the trie's key index (templates/trie/keys.rs) -> Plan. No row read.
        ▼
 query/execute.rs  scan only the planned leaves: store/disk/scan.rs, store/filter.rs,
        │          store/fan.rs; query/buckets.rs (GROUP BY time), query/cursor.rs
        │          (AFTER), query/limits.rs (the backstop)
        ▼
 QueryResult (query.rs) -> JSON (query/answer.rs): one shape for HTTP and --format json
```

## Where things live

| Path | What it is |
|---|---|
| `src/engine/database.rs`, `database/` | `Database`, `Options`: the three ways to open, every request; `lifecycle.rs` (open, replay, checkpoint, close order) and `compaction.rs` (which days, the job table) |
| `src/engine/model/`, `parse/` | a line before storage: types, the ladder, flattening, the fingerprint. No I/O |
| `src/engine/templates/` | the trie and its key index, `catalog.log`, `trie.json` |
| `src/engine/ingest.rs`, `load.rs` | the live door's write path, and the bulk door |
| `src/engine/wal.rs` | the write-ahead log |
| `src/engine/store.rs`, `store/` | the leaf stores (memory, disk), scan, filter, fan-out, manifest, a day's rebuild |
| `src/engine/format.rs`, `format/` | storage format v3: frames, row groups, column encodings; its tests are `format_tests.rs`, with the encodings in `format/encodings_tests.rs` and the vectors in `format/vectors_tests.rs` |
| `src/engine/query.rs`, `query/` | TrieQL, plan, scan, buckets, cursor, limits, result JSON |
| `src/engine/display.rs` | how values are *printed* (not the storage format) |
| `src/server.rs`, `src/server/` | router, handlers, the body of a request, flush clock and shutdown (`runtime.rs`), `/compact` (`compactor.rs`), the console `viewer.html` |
| `src/cli.rs` | the clap definitions and the dispatch |
| `src/cli/` | one file a subcommand (`serve.rs`, `load.rs`, `query.rs` with `query/oracle.rs`, `gen_logs.rs`, ...), and `config.rs` (settings), `logging.rs` (kibbr's own log), `http.rs` (the small client) |
| `src/bench.rs`, `src/bench/` | `kibbr-bench` (`src/bin/kibbr-bench.rs`), built only with `--features bench`; it measures the `kibbr` beside it |
| `src/fuzzing.rs`, `src/fuzzing/` | the fuzz harnesses `fuzz/` calls as `kibbr::fuzzing::<target>::run`, and the replay of `fuzz/regressions/` (a unit test) |
| `tests/main.rs` | the one integration-test binary, `e2e`: a module a topic, `harness.rs` shared, `layers.rs` the layer rules |
| `tests/data/` | `type_ladder.ndjson` (labelled types), `format/` (format vectors), `hostile/`, `compat/<sha>/` (data directories older builds wrote); its `README.md` says why each is there |
| `scripts/check/`, `scripts/fault/`, `scripts/measure/`, `scripts/bench/` | differential checks; kill and fault tests; measurements; benchmarks (all on the release binary) |
| `fuzz/` | its own Cargo workspace; the harnesses live in `src/fuzzing/` and `src/engine/fuzzing/` |
| `web/` | the console's ReScript source; it builds `src/server/viewer.html` |
| `docs/` | user docs, indexed in `docs/README.md`; `docs/serve.md` is the HTTP API and TrieQL |
| `deploy/` | Kubernetes manifests, IAM policies, Vector sinks, the container image |

## To change X, touch Y

Engine paths are under `src/engine/`.

| To change | Touch |
|---|---|
| TrieQL syntax or query meaning | `query/trieql.rs` (grammar), `query.rs` (`Query`), `query/plan.rs`, `query/execute.rs`; the flags in `src/cli/query.rs` build the same `Query`; `src/cli/query/oracle.rs` must give the same answer. Tests: `query/trieql_tests.rs`, `query_tests.rs`, `tests/trieql.rs`, `tests/query.rs`. Checks: `scripts/check/parser_equivalence.sh`, `scripts/check/agree.sh`. Docs: the `/query` section of `docs/serve.md` |
| `GROUP BY time(...)` bucketing | `query/buckets.rs` (`bucket_of`, `check_buckets`) **and** `floor_to` in `src/cli/query/oracle.rs`. The oracle re-implements the rule on purpose, so do not make it call the engine. Tests: `tests/query.rs` (`buckets`); check: `scripts/check/agree.sh` |
| A new value type | `model/types.rs` (`ColumnType`, `ColValue`, the join), `model/infer.rs` (a new rung; order matters), `format.rs` (`type_byte`, `type_of_byte`, `encodes`), `format/build.rs`; `src/cli/gen_logs.rs` (`Gen`) to generate it; `tests/data/type_ladder.ndjson` and `tests/data/README.md` (run by `tests/values.rs`). A new type byte is also a storage format change: see below |
| A new HTTP endpoint | `src/server/routes.rs` (`router`), `src/server/handlers.rs`, `src/server_tests.rs`, `tests/serve.rs` (the HTTP contract), `docs/serve.md`; `src/cli/http.rs` if a command calls it |
| The storage format | `format/` (`VERSION`, the kinds, the `OPTIONAL` bit), `format/vectors_tests.rs` with `tests/data/format/*.col` and their `.col.txt` dumps, `tests/storage.rs` with `tests/data/compat/<sha>/` (each has a `make.py`), `scripts/check/downgrade.sh`, `fuzz/fuzz_targets/segment.rs`. Prefer an optional kind, so that older builds step over it |
| A new CLI subcommand | a variant and its dispatch in `src/cli.rs`, the command in `src/cli/<name>.rs`, a test module in `tests/`, a page in `docs/` |
| A setting | `src/cli/config.rs`, `kibbr.toml.example`, `docs/settings.md`; `src/server/settings.rs` if the server reads it |

## Invariants and boundaries

| Rule | Enforced by |
|---|---|
| The engine has no async runtime, no HTTP and no flags, and names neither the server nor the command line. The server names neither the command line nor `engine::inspect`. | `tests/layers.rs`, which reads the source. |
| Above the engine, code names only what `engine.rs` re-exports, its four namespaces (`model`, `query`, `load`, `compaction`), and the two doors `inspect` and `fuzzing`; every other engine module is private. | The compiler (a private module is E0603). |
| A `200` from `/ingest` means every line of the body is in the WAL, flushed. It is stored exactly once, even after `kill -9`. | `scripts/fault/crash_test.sh` (CI), `tests/storage.rs`, the `wal_record` fuzz target. |
| A template's `catalog.log` record is flushed before its first row is logged. | `ingest::place_in`; `scripts/fault/crash_templates.sh`. |
| A data file that `manifest.log` does not name is not part of the database. | `store/manifest.rs`; `scripts/fault/compact_kill.sh`, `scripts/fault/load_kill.sh`. |
| Both doors route a line with `ingest::place`, so they store it the same way. | `scripts/check/two_doors.sh`. |
| The memory store and the disk store give the same answers. | `scripts/check/store_agree.sh`. |
| The oracle shares no mechanism with the engine. | Review; `scripts/check/agree.sh` (CI) compares them. |
| Traits only where there are two implementations (the row stores in `store.rs`). | Review. |
| Every `.rs` file starts with the copyright and SPDX lines. | `tests/layers.rs`. |

## Cross-cutting concerns

- **Back-pressure.** The engine returns refusals as values. The server decides how to
  wait or *shed* (a `503` with `Retry-After`). The counters are `Door` and `Shed` in
  `src/server.rs`.
- **Errors.** `src/engine/error.rs`, `src/server/error.rs`, `src/cli/error.rs`.
- **Determinism.** `src/engine/rng.rs` and `src/cli/gen_logs.rs`: the same seed gives the
  same bytes on every machine.

## Tests

A module's unit tests are its `tests` module. Up to 100 lines it stays inline, as
`#[cfg(test)] mod tests { ... }` at the end of the module; longer, it is a sibling file
named for the module, `wal.rs` -> `wal_tests.rs`, declared as
`#[cfg(test)] #[path = "wal_tests.rs"] mod tests;`. A folder holds child modules only,
never just tests. A test of the binary or of HTTP goes in a topic module of `tests/main.rs`
(`tests/<topic>.rs`), with what topics share in `tests/harness.rs`; a test that needs a
killed process, two builds or a differential run is a script in `scripts/fault/` or
`scripts/check/` ([scripts/README.md](scripts/README.md)).

```
cargo test                  # unit tests (the fuzz regressions among them) and e2e
cargo test --all-features   # also bench::speed, which runs kibbr-bench
```
