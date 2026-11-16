# kibbr's layout, tests and lints

One published package, `kibbr`, with two crates in it. The library is the engine
(`src/engine/`). The binary is the server and the command line (`src/main.rs`). Each layer
names only the ones under it: the command line, then the server, then the engine.
[ARCHITECTURE.md](../ARCHITECTURE.md) is the map. `fuzz/` is a workspace of its own that
depends on the library with `fuzzing` on, and `crates/kibbr-devtools/` holds the bench.

## The command line

The command line over a `Database`. It opens a data directory no other way.

```
Cargo.toml                       the workspace, and the `kibbr` package: the library and the binary
src/main.rs                      the binary's root: the allocator, and the two modules `server` and `cli`
src/cli.rs                       the command line: the enum of the subcommands and the dispatch; each one's flags are in its own file
src/cli/config.rs                where a setting comes from: a flag, a `KIBBR__` variable, the `--config` file, a default
src/cli/logging.rs               the process's own log: `tracing` events as lines of text or of JSON
src/cli/error.rs                 thiserror enums of the commands, and what ends one
src/cli/http.rs                  the small HTTP client `load`, `compact`, the status commands and `bench-storage --es` speak
src/cli/serve.rs                 `kibbr serve` - the server's settings made, the server run, the exit code
src/cli/load.rs                  `kibbr load` - the command: alone, or asked of the server holding the directory
src/cli/compact.rs               `kibbr compact` - the command, the same two ways
src/cli/query.rs                 the `query` command: the flags, the table, the plan
src/cli/query/oracle.rs          the brute-force oracle behind `query --reference`
src/cli/status.rs                `kibbr templates`, `kibbr stats` and `kibbr health`
src/cli/tools/                   what someone runs to try, measure or test kibbr; no production command names it
src/cli/tools/parse.rs           the `parse` output
src/cli/tools/demo.rs            the `demo` tree
src/cli/tools/gen_logs.rs        the generators - `hostile` (this file) and the log farm (gen_logs/farm.rs)
src/cli/tools/bench_storage.rs   `kibbr bench-storage`
tests/                           one file an area (load, query, compact, restart, compat, ...); fixtures/ holds old-format data dirs
tests/fixture.rs                 outer-surface tests
tests/serve.rs                   the HTTP contract of the binary, over a bare TcpStream
crates/kibbr-devtools/           `kibbr-bench`, the speed bench: its own binary, never published; it measures the `kibbr` beside it
Dockerfile                       the image: a static binary on distroless
bench/                           the compression scripts, and bench/results/, what was measured
deploy/                          the Kubernetes manifests, the IAM policies and the Vector sinks
scripts/                         the check, fault and measure scripts, and bump.sh and publish.sh
web/                             the console's ReScript source, built into viewer.html (docs/viewer.md)
```

- **Traits only where there are two.** The engine's row stores are the one place with a
  second implementation, and four traits in `rows.rs`: `Store`, `LeafStore` and
  `Snapshot`, each implemented in memory (`rows/memory.rs`, whose snapshot is `Live`
  beside the traits) and on disk (`rows/disk.rs`), and `Visitor`, where a
  scan hands its rows - a count, a fetch, a group-by. Everything else has one
  implementation, and an interface with a single caller is a guess. That includes the
  ingest path: it is a plain function until something gives it a second caller.
- **The allocator is [mimalloc](https://github.com/microsoft/mimalloc).** On macOS arm64 the
  system one aborted the server with "pointer being freed was not allocated" at unrelated
  free sites that neither the code (no `unsafe`) nor AddressSanitizer could account for.

## The server

HTTP over one `Database`: a module of the binary, `server`. It names the engine's public
items only, and never the command line.

```
src/server.rs            what the handlers share, and the load job table
src/server/settings.rs   `Settings`: every value a server runs on, plain data with a default
src/server/run.rs        `run` - the runtime, the flush clock, signals, shutdown, the banner
src/server/routes.rs     the router, the console at `/`, `/templates`, `/stats`, the request timeout
src/server/handlers.rs   `/health`, `/ingest`'s door, `/query`, `/load`
src/server/receive.rs    the door's half: a body's lines through `Database::ingest`, and the counts
src/server/schedule.rs   `/compact`, and the look for a day that is due
src/server/error.rs      what stops a server, and what stops the live door taking a body
src/server/tests.rs      the unit tests
src/server/viewer.html   the log viewer served at `/` - one file, compiled in
```

## The engine

The whole database behind one handle, `Database`: the library, in `src/engine/`. No HTTP,
no flags, no async runtime. The paths below are from `src/engine/`. Modules lowest first;
each one's unit tests are in a `tests.rs` beside it (`wal/tests.rs`), and
`format/encodings.rs` and `format/vectors.rs` are modules of tests of their own.

```
model/       the types (`ColumnType`, `ColValue`, `TimeSource`), the type ladder, the tokens, a row and its instant
parse/       a line read under a byte cap, flattened into a row, and `KeyPath`
fmt.rs       how a value, a count and an instant are printed
rng.rs       a deterministic PRNG
error/       `Error`, `IngestError`, and the line errors
lock.rs      `DataLock`: one process owns a data directory
format/      storage format v3: frame, build, the file `Writer` and `Reader`, read, column
templates/   the trie and its key index, `catalog.log`, and `trie.json`
rows/        `StoreManager` and the store traits, the disk store (scan, compaction), memory, filter, manifest, scan threads
ingest.rs    the live door: one parsed line into the trie and its store
wal.rs       the write-ahead log
load.rs      the bulk door
compact.rs   which days a run takes, its report, the one-at-a-time job table
query/       TrieQL, the plan, the scan, buckets, the cursor, limits, the answer
lifecycle.rs open (restore and replay), the checkpoint moved behind the flush, the close order
database.rs  `Database` and `Options`: the three ways to open, and everything asked of it
fuzzing/     the five fuzz harnesses and the replay of their inputs, behind the `fuzzing` feature
lib.rs       the public names, `internals`, and `testing` behind `test-support`
```

- **Three ways to open.** To serve, `Database::claim` then `Claim::open`; offline,
  `Database::open_offline`; in memory, `Database::in_memory`.
- **`internals`** is doc-hidden and has no stability promise. It holds what the command
  line's diagnostics (`demo`, `bench-storage`) read past the handle. The server never
  names it (`tests/source_rules.rs`).
- **Features.** `test-support` gives the binary's tests their fixtures; `fuzzing` is
  off by default. Neither is for a build that runs in production.
- The format's test vectors are in `testdata/format/`.
- **Column encodings.** Each piece of a column chunk gets one encoding, by its type, and
  never several tried with the smallest kept (`format.rs`, `format/build.rs`):
  - a value stored once when every row of the piece holds it
  - a dictionary for strings
  - varints for integers
  - delta encoding for timestamps
  - a bitmap for booleans
  - plain for floats, Uuids and IPs

  Then one zstd frame goes over the whole chunk, unless every piece is a constant, a bitmap
  or a Uuid's raw bytes.

## Tests and lints

```
cargo test                                                           # the `kibbr` package: engine, server, command line, tests/
cargo test --workspace --all-features                                # the bench too, and the fuzz replay
cargo test --lib                                                     # the engine's own tests
cargo test --bin kibbr                                               # the server's and the command line's own tests
cargo test --lib rows::                                              # one area: the module path is the filter
cargo clippy --workspace --all-targets --all-features -- -D warnings # lints have to be clean
```

- Without `--all-features`, the engine's `fuzzing/tests.rs` is not compiled and the
  committed crashers are not replayed.
- Most tests under `tests/` run the real binary and read its stdout. `compact.rs` also
  reads day files through `internals`. There are no mocks.
- The one that matters most in `tests/fixture.rs` is
  `every_fixture_value_gets_its_labelled_type`: every hand-labelled value in
  `testdata/fixture.ndjson` must come out with the type its label says.
