# kibbr's layout, tests and lints

A workspace of three crates, each naming only the ones under it - `kibbr`, then
`kibbr-server`, `kibbr-engine` - and `fuzz/` beside them, a workspace of its own that depends
on `kibbr-engine` and `kibbr` with `fuzzing` on. The two library crates each have a README
with their files: [engine](../crates/kibbr-engine/README.md),
[server](../crates/kibbr-server/README.md). What were `kibbr-core` and `kibbr-storage` are
modules of `kibbr-engine` now (`model` and `parse`; `format`, `templates`, `rows`, `ingest`,
`wal` and `lock`); on crates.io their names stop at 0.2.3.

## The binary, `kibbr`

The command line over a `Database`. It opens a data directory no other way.

```
Cargo.toml                  the workspace, and the `kibbr` crate: the binary - the command line
src/main.rs                 the clap definitions and the dispatch
src/config.rs               where a setting comes from: a flag, a `KIBBR__` variable, the `--config` file, a default
src/logs.rs                 the process's own log: `tracing` events as lines of text or of JSON
src/error.rs                thiserror enums of the commands, and what ends one
src/client.rs               the small HTTP client `load`, `compact` and `bench-storage --es` speak
src/cmd/                    what someone runs in production; it never names src/tools/
src/cmd/serve.rs            `kibbr serve` - the server's settings made, the server run, the exit code
src/cmd/load.rs             `kibbr load` - the command: alone, or asked of the server holding the directory
src/cmd/compact.rs          `kibbr compact` - the command, the same two ways
src/cmd/query.rs            the `query` command: the flags, the table, the plan
src/cmd/oracle.rs           the brute-force oracle behind `query --reference`
src/cmd/status.rs           `kibbr templates`, `kibbr stats` and `kibbr health`
src/tools/                  what someone runs to try, measure or test kibbr
src/tools/parse.rs          the `parse` output
src/tools/demo.rs           the `demo` tree
src/tools/gen/              the generators - `hostile` (mod.rs) and the log farm (farm.rs)
src/tools/bench_storage.rs  `kibbr bench-storage`
src/tools/bench/            `kibbr bench`
src/tools/fuzzing/          the `cursor` and `trieql` harnesses, behind the `fuzzing` feature
tests/fixture.rs            outer-surface tests
tests/serve.rs              the HTTP contract, over a bare TcpStream - in this process, or of the binary
Dockerfile                  the image: a static binary on distroless, and the `bench` target
bench/run.sh                `kibbr bench` in a container; bench/results/ holds what it measured
deploy/                     the Kubernetes manifests and the Vector sinks
scripts/                    the check, fault and measure scripts
```

- **Traits only where there are two.** The engine's row stores are the one place with a
  second implementation, and four traits in `rows/mod.rs`: `Store`, `LeafStore` and
  `Snapshot`, each implemented in memory (`rows/memory.rs`, whose snapshot is `Live`
  beside the traits) and on disk (`rows/disk.rs`), and `Visitor`, where a
  scan hands its rows - a count, a fetch, a group-by. Everything else has one
  implementation, and an interface with a single caller is a guess. That includes the
  ingest path: it is a plain function until something gives it a second caller.
- **The allocator is [mimalloc](https://github.com/microsoft/mimalloc).** On macOS arm64 the
  system one aborted the server with "pointer being freed was not allocated" at unrelated
  free sites that neither the code (no `unsafe`) nor AddressSanitizer could account for.

## The server, `kibbr-server`

HTTP over one `Database`. It depends on `kibbr-engine` only, and reaches no further than the
engine's public API.

```
src/lib.rs         what the handlers share, and the load job table
src/settings.rs    `Settings`: every value a server runs on, plain data with a default
src/lifecycle.rs   `run` - the runtime, the flush clock, signals, shutdown, the banner
src/routes.rs      the router, `/templates`, `/stats`, `/health`, the request timeout
src/handlers.rs    `/ingest`'s door, `/query`, `/load`
src/ingest.rs      the door's half: a body's lines through `Database::ingest`, and the counts
src/compaction.rs  `/compact`, and the look for a day that is due
src/error.rs       what stops a server, and what stops the live door taking a body
src/tests.rs       the unit tests
src/viewer.html    the log viewer served at `/` - one file, compiled in
```

## The engine, `kibbr-engine`

The whole database behind one handle, `Database`. No HTTP, no flags, no async runtime.
Modules lowest first; each one's unit tests are in a `tests.rs` beside it (`src/wal/tests.rs`),
and `format/encodings.rs` and `format/vectors.rs` are modules of tests of their own. Each
module's files are in [its README](../crates/kibbr-engine/README.md).

```
src/model/       the types (`ColumnType`, `ColValue`, `TimeSource`), the type ladder, the tokens, a row and its instant
src/parse/       a line read under a byte cap, flattened into a row, and `KeyPath`
src/fmt.rs       how a value, a count and an instant are printed
src/rng.rs       a deterministic PRNG
src/error/       `Error`, `IngestError`, and the line errors
src/lock.rs      `DataLock`: one process owns a data directory
src/format/      storage format v3: frame, build, the file `Writer` and `Reader`, read, column
src/templates/   the trie and its key index, `catalog.log`, and `trie.json`
src/rows/        `StoreManager` and the store traits, the disk store (scan, compaction), memory, filter, manifest, scan threads
src/ingest.rs    the live door: one parsed line into the trie and its store
src/wal.rs       the write-ahead log
src/load.rs      the bulk door
src/compact.rs   which days a run takes, its report, the one-at-a-time job table
src/query/       TrieQL, the plan, the scan, buckets, the cursor, limits, the answer
src/lifecycle.rs open (restore and replay), the checkpoint moved behind the flush, the close order
src/database.rs  `Database` and `Options`: the three ways to open, and everything asked of it
src/fuzzing/     the `wal_record`, `segment` and `record_log` harnesses, behind the `fuzzing` feature
src/lib.rs       the public names, and `internals`
```

- **Three ways to open.** To serve, `Database::claim` then `Claim::open`; offline,
  `Database::open_offline`; in memory, `Database::in_memory`.
- **`internals`** is doc-hidden and has no stability promise. It holds what the command
  line's diagnostics (`demo`, `bench-storage`) and the `cursor` fuzz harness read past the
  handle. `kibbr-server` never names it (`scripts/check/no_internals.sh`).
- **Features.** `test-support` gives the tests of the crates above their fixtures;
  `fuzzing` is off by default.
- The format's test vectors are in `testdata/format/`.

## Tests and lints

```
cargo test --workspace --all-features                                # every crate, tests/ at the root, the fuzz regressions
cargo test -p kibbr-engine --all-features                            # one crate's own
cargo test -p kibbr-engine rows::                                    # one area: the module path is the filter
cargo clippy --workspace --all-targets --all-features -- -D warnings # lints have to be clean
```

- Without `--all-features`, `tests/fuzz_regressions.rs` is not compiled and the committed
  crashers are not replayed.
- The tests under `tests/` run the real binary and read its stdout: nothing reaches inside
  the crate and there are no mocks.
- The one that matters most in `tests/fixture.rs` is
  `every_fixture_value_gets_its_labelled_type`: every hand-labelled value in
  `testdata/fixture.ndjson` must come out with the type its label says.
