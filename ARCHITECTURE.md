# kibbr: the map of the code

kibbr is one published package. Inside it are two crates: a library and a binary.

- **The library is the engine**: the database, in `src/engine/`. It has no HTTP, no flags
  and no async runtime.
- **The binary is the server and the command line**: `src/main.rs`, which declares the
  two modules `server` and `cli`. It is built on the library.

A second package, `crates/kibbr-devtools`, holds the speed bench (`kibbr-bench`). It is
never published. kibbr's own words (template, leaf, door, shed, backstop) are in
[docs/glossary.md](docs/glossary.md).

## The layers

Each layer names only the layers under it: engine, then server, then command line.

```
   +- binary crate (src/main.rs) --+
   |   command line --> server     |
   +------+--------------+---------+
          v              v
   +- library crate (src/engine/) -+      kibbr-devtools, fuzz/, tests/
   |           engine              | <--  (the library only: nothing can
   +-------------------------------+       import a binary)
```

- The engine never names the server, the command line, an async runtime or HTTP.
- The server never names the command line.
- The engine does a flush or a compaction when it is called. The server decides when.

## Three things to know first

- **The library's root is `src/engine/lib.rs`.** So there is no `src/engine.rs` beside
  `src/server.rs` and `src/cli.rs`.
- **Inside the binary, `kibbr::` means the library and `crate::` means the binary.** The
  server and the command line write `kibbr::Database`.
- **`pub(crate)` in the engine means "engine only".** `src/engine/lib.rs` is the list of
  what the binary, the bench and the fuzz targets may name.

## The folders

```
Cargo.toml              the workspace, and the `kibbr` package: the library and the binary
src/engine/             the library: the database
src/main.rs             the binary's root: the allocator, `mod server;`, `mod cli;`
src/server.rs  server/  HTTP over one `Database`; owns the clocks
src/cli.rs  cli/        the commands, the settings, kibbr's own log
src/cli/tools/          what tries, measures or tests kibbr; no production command names it
tests/                  one test binary, `e2e`, that runs the real `kibbr`; data/ is its data
crates/kibbr-devtools/  `kibbr-bench`, the speed bench; never published
fuzz/                   the fuzz targets, a workspace of its own, on nightly
scripts/                check, fault and measure scripts; bump.sh, publish.sh; bench/
web/                    the console's source (ReScript), built into src/server/viewer.html
deploy/                 the container image's notes, and the Kubernetes manifests
docs/                   how to use kibbr; glossary.md
```

## The engine, `src/engine/`

The whole database behind one handle, `Database`. `src/engine/lib.rs` has the tour: the
one idea (a line is a shape and its values), the modules, and TrieQL. It is the front
page on docs.rs. The modules, in the order a log line travels:

| Group | Module | What it holds |
|---|---|---|
| A line before storage | `model/` | the column types and typed values, the type ladder, a template's tokens, a row |
| | `parse/` | a line read under a byte limit, flattened to dotted keys, and its fingerprint |
| Its shape | `templates/` | the trie and its key index, `catalog.log`, `trie.json` |
| Writing | `ingest.rs` | the live door: one parsed line into the trie and its store |
| | `load.rs` | the bulk door: a closed day staged and committed in one record |
| | `wal.rs` | the write-ahead log: group commit, replay, the checkpoint |
| Stored, and compressed | `rows/` | the stores (memory, and the day files), the scan, the manifest, compaction |
| | `format/` | format version 3: frames, row groups, the column encodings, the file |
| Asking | `query/` | TrieQL, the plan, the scan, buckets, the cursor, the limits, the answer |
| The handle and the rest | `database.rs` | `Database` and `Options`: three ways to open, and everything asked of it |
| | `lifecycle.rs` | open (restore and replay), the checkpoint, the close order |
| | `compact.rs` | which days a compaction run takes, its report, one run at a time |
| | `lock.rs` | `LOCK`: one process owns a data directory |
| | `error.rs`, `fmt.rs`, `rng.rs` | the errors, how values are printed, a seeded random generator |
| Not shipped | `fuzzing/` | the five fuzz harnesses, behind the `fuzzing` feature |

Facts about the engine that are not obvious from the files:

- **Three ways to open.** To serve, `Database::claim` then `Claim::open`; offline,
  `Database::open_offline`; in memory, `Database::in_memory`.
- **`internals`** is a hidden module with no stability promise. It holds what the command
  line's diagnostics (`demo`, `bench-storage`) read past the handle. The server never
  names it.
- **Traits only where there are two implementations.** The row stores are the one such
  place: `Store`, `LeafStore` and `Snapshot` in `rows.rs`, each implemented in memory
  (`rows/memory.rs`) and on disk (`rows/disk.rs`), and `Visitor`, where a scan hands its
  rows. Everything else has one implementation and no interface.
- **The scan threads** are a small pool that the disk store owns, never a global pool. One
  file names the thread-pool crate, and a test in `rows/tests/scan_threads.rs` holds that.
- **Unit tests** of `dir/x.rs` are `dir/tests/x.rs`. `format/encodings.rs` and
  `format/vectors.rs` are modules of tests of their own.

### Compression

"Performance" in kibbr means how small the stored data is. This is where that code is.

| What | Where |
|---|---|
| The encodings' names: constant, delta, dictionary, varint, zstd | `format.rs` |
| Choosing an encoding, writing it, building a row group | `format/build.rs` |
| Reading the encodings back | `format/column.rs`, `format/read.rs` |
| The encodings' tests | `format/encodings.rs` |
| The rebuild of a day into full row groups | `rows/disk/compact.rs` |
| The write-ahead log's own compression | `wal.rs` |

Each piece of a column chunk gets one encoding, chosen by its type, and never several
tried with the smallest kept:

- a value stored once when every row of the piece holds it
- a dictionary for strings
- varints for integers
- delta encoding for timestamps
- a bitmap for booleans
- plain for floats, Uuids and IPs

Then one zstd frame goes over the whole chunk, unless every piece is a constant, a bitmap
or a Uuid's raw bytes. Compression is measured by `kibbr bench-storage` and by
`scripts/bench/compression.py`, with the results under `scripts/bench/results/`.

## The server, `src/server.rs` and `src/server/`

HTTP over one `Database`. Nothing there reads a flag or writes to stderr, and the flush
clock is there, not in the store.

| File | What it holds |
|---|---|
| `server.rs` | what the handlers share, and the table of load jobs |
| `server/settings.rs` | `Settings`: every value a server runs on, plain data with a default |
| `server/run.rs` | `run`: the runtime, the flush clock, signals, shutdown, the banner |
| `server/routes.rs` | the router, the console at `/`, `/templates`, `/stats`, the request timeout |
| `server/handlers.rs` | `/health`, the door of `/ingest`, `/query`, `/load` |
| `server/receive.rs` | an `/ingest` body taken in: its lines through `Database::ingest`, and the counts |
| `server/schedule.rs` | `/compact`, and the look for a day that is due |
| `server/error.rs` | what stops a server, and what stops the live door taking a body |
| `server/viewer.html` | the console served at `/`: one file, built from `web/`, compiled in |

## The command line, `src/cli.rs` and `src/cli/`

`src/cli.rs` is the top-level clap type, the enum of the subcommands, and the dispatch. A
subcommand's flags are in its own file, as a struct named `Flags`.

| File | What it holds |
|---|---|
| `cli/serve.rs` | `kibbr serve`: the settings made, the server run, the exit code |
| `cli/load.rs` | `kibbr load`: alone, or asked of the server that holds the directory |
| `cli/compact.rs` | `kibbr compact`, the same two ways |
| `cli/query.rs` | `kibbr query` and `kibbr explain`: the flags, the table, the plan |
| `cli/query/oracle.rs` | the brute-force oracle behind `query --reference` |
| `cli/status.rs` | `kibbr templates`, `kibbr stats`, `kibbr health` |
| `cli/config.rs` | where a setting comes from: the flag, or else the `KIBBR__` variable, or else the file `--config` names, or else the default |
| `cli/logging.rs` | kibbr's own log: one `tracing` subscriber that writes text or JSON lines to stderr |
| `cli/http.rs` | the small HTTP client that `load`, `compact` and the status commands use |
| `cli/error.rs` | the errors every command shares |
| `cli/tools/` | what tries, measures or tests kibbr: `gen-logs`, `parse`, `demo`, `bench-storage` |

- **The generator** (`cli/tools/gen_logs.rs`) writes the same lines on every machine for
  a seed, so a benchmark or a fault test means the same thing everywhere.
- **The oracle** answers a query with every line, one closure and one counter. It is
  plainly correct and never shipped as the answer. `scripts/check/agree.sh` holds the
  engine to it.
- **The allocator** is [mimalloc](https://github.com/microsoft/mimalloc), set in
  `src/main.rs`. On macOS arm64 the system allocator aborted the server at unrelated
  places that neither the code (no `unsafe`) nor AddressSanitizer could account for.

## To change X, touch Y

| To change | Touch | And test it in |
|---|---|---|
| How a value's type is decided | `src/engine/model/infer.rs` | `tests/data/fixture.ndjson`, which `tests/fixture.rs` checks line by line |
| What a line's shape is, or a promoted key | `src/engine/parse/flatten.rs`, `model/tokens.rs` | `src/engine/parse/tests/`, `tests/shapes.rs`, `tests/trie.rs` |
| How small the data is | the files of "Compression" above | `format/encodings.rs`, `format/vectors.rs`; then `kibbr bench-storage` |
| The on-disk format | `src/engine/format/` | `tests/data/format/` (the vectors), `tests/compat.rs`, `scripts/check/downgrade.sh` |
| What a crash may lose | `src/engine/wal.rs`, `lifecycle.rs` | `tests/restart.rs`, `scripts/fault/crash_test.sh` |
| The query language | `src/engine/query/trieql.rs` | `src/engine/query/tests/trieql.rs`, `tests/trieql.rs`, the `trieql` fuzz target |
| What a query answers | `src/engine/query/plan.rs`, `execute.rs` | `tests/query.rs`, `tests/decisions.rs`, `scripts/check/agree.sh` |
| An HTTP endpoint | `src/server/handlers.rs`, `routes.rs` | `src/tests/server.rs`, `tests/serve.rs` |
| When a flush or a compaction happens | `src/server/run.rs`, `schedule.rs` | `tests/operations.rs`, `tests/compact.rs` |
| A setting or a flag | `src/cli/config.rs`, the command's `Flags` | `src/cli/tests/config.rs`, `tests/operations.rs`; `docs/settings.md` |
| A command's output | the command's file under `src/cli/` | the topic of that name under `tests/` |
| The console | `web/src/`, then `npm run build` in `web/` | CI's `web` job holds `viewer.html` to the build |
| A dependency | `Cargo.toml`, and a layer's list in `tests/source_rules.rs` | the layer test fails until a layer lists it |

## The rules, and what holds each

| Rule | Held by |
|---|---|
| The engine cannot name the server or the command line | the compiler: a library cannot see the binary that uses it |
| Nothing above can reach the engine's private parts | the compiler: `pub(crate)` and private modules |
| The engine uses only its own external crates | `tests/source_rules.rs`, rule 2 |
| The server uses only its own external crates, and never names the command line or `internals` | `tests/source_rules.rs`, rules 2 and 3 |
| The engine opens no socket, and writes `crate::`, not `kibbr::` | `tests/source_rules.rs`, rule 4 |
| `test-support` only adds whole items | `tests/source_rules.rs`, rule 5 |
| Every `.rs` file starts with the licence lines | `tests/source_rules.rs`, rule 6 |
| Unit tests sit in `dir/tests/x.rs` | `tests/source_rules.rs`, rule 7 |
| No production command names the tools | `tests/source_rules.rs`, rule 8 |
| No `mod.rs` | the clippy lint `mod_module_files` |
| An acknowledged line is stored exactly once, whatever kills the process | `scripts/fault/crash_test.sh`, `tests/restart.rs` |
| The engine and the oracle give the same answers | `scripts/check/agree.sh` |
| Flags and TrieQL build the same query | `src/cli/tests/query.rs`, `scripts/check/parser_equivalence.sh` |
| Every stored value comes back as it went in | `scripts/check/roundtrip.sh` |
| An older build opens what this build wrote | `scripts/check/downgrade.sh`, `tests/compat.rs` |

## The two features

`test-support` and `fuzzing` are for code that is not shipped. Never enable either in a
build that runs in production.

- **`test-support` only adds items; it never changes behaviour.** It gates the hooks the
  binary's tests reach into a database with. The package depends on itself for tests,
  with this feature on, so under `cargo test` the `kibbr` binary has it on, and under
  `cargo build` it has it off. Because the feature only adds items, the two binaries
  behave alike.
- **`fuzzing`** gates the fuzz targets' harnesses, which are in `src/engine/fuzzing/`.
  The saved fuzz inputs are replayed only with `--all-features`.

## What is fuzzed, and what is not

A fuzz target can reach only the library. So the rule is: **code that decodes untrusted
bytes lives in the engine.** That is query text, cursors and every file format.

- The server's and the command line's own code is not fuzzed.
- One check left the fuzz targets when the harnesses moved into the engine: "the flags of
  `kibbr query` and TrieQL build the same query". It is about operator input, so it is a
  test of the binary now (`src/cli/tests/query.rs`). It runs on the harness's own cursors
  and on every file in `fuzz/regressions/cursor/`. It is no longer fuzzed with coverage
  guidance.

## Reading the code, from the bottom up

Each file names only what is above it in this list, so each can be read with what came
before it.

**The engine**

1. `model/types.rs`, `model/infer.rs`: what a value is, and how its text becomes a type.
2. `parse/lines.rs`, `parse/flatten.rs`: a line of bytes into keys, values and a
   fingerprint.
3. `templates/trie.rs`, then `templates/catalog.rs`: where a shape is kept, and how it
   outlasts the process.
4. `format/frame.rs`, `format/build.rs`, `format/file.rs`: how rows become bytes on disk.
5. `rows.rs`, `rows/memory.rs`, `rows/disk.rs`: where a template's rows are kept, and
   `rows/manifest.rs` for which files count.
6. `wal.rs`, then `ingest.rs`: how a line is made durable, then placed.
7. `load.rs`: the other way in, for a whole day.
8. `query/trieql.rs`, `query/plan.rs`, `query/execute.rs`: text to a query, to the
   templates that can match, to an answer.
9. `rows/disk/compact.rs`, `compact.rs`: a finished day rebuilt.
10. `lifecycle.rs`, then `database.rs`: open, close, and the one handle over all of it.
11. `lib.rs`: what of all this the binary may name.

**The server**

12. `server/settings.rs`, `server.rs`: what a server runs on, and what its handlers share.
13. `server/receive.rs`, `server/handlers.rs`, `server/routes.rs`: a request to an engine
    call to an answer.
14. `server/schedule.rs`, `server/run.rs`: the clocks, signals and shutdown.

**The command line**

15. `cli/config.rs`, `cli/logging.rs`, `cli/error.rs`: settings, the log, errors.
16. `cli/serve.rs`, `cli/load.rs`, `cli/compact.rs`, `cli/query.rs`, `cli/status.rs`: one
    command a file.
17. `cli.rs`, then `main.rs`: the dispatch, and the 19 lines that start it all.

## Building and testing

```
cargo build                                    # the kibbr binary
cargo test                                     # engine, server, command line, and tests/
cargo test --workspace --all-features          # the bench too, and the fuzz replay
cargo test --lib                               # the engine's own tests
cargo test --bin kibbr                         # the server's and the command line's own tests
cargo test --lib rows::                        # one area: the module path is the filter
cargo test --test e2e serve::                  # one topic of the end-to-end tests
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

- Without `--all-features` the saved fuzz inputs are not replayed.
- Most tests under `tests/` run the real binary and read what it prints. There are no
  mocks.
- The test that matters most in `tests/fixture.rs` is
  `every_fixture_value_gets_its_labelled_type`: every hand-labelled value in
  `tests/data/fixture.ndjson` must get the type its label says.
