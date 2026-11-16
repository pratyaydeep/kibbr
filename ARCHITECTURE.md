# kibbr: the map of the code

kibbr is one published package. Inside it are two crates: a library and a binary.

- **The library is the engine**: the database, in `src/engine/`. It has no HTTP, no flags
  and no async runtime.
- **The binary is the server and the command line**, in `src/main.rs` and the modules it
  declares. It is built on the library.

A second package, `crates/kibbr-devtools`, holds the speed bench (`kibbr-bench`). It is
never published.

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

- The engine never names the server, the command line, an async runtime or HTTP. The
  compiler holds this: a library cannot see the binary that uses it.
- The server never names the command line.
- The engine does a flush or a compaction when it is called. The server decides when.

## Three things to know first

- **The library's root is `src/engine/lib.rs`.** So there is no `src/engine.rs` beside
  `src/server.rs`.
- **Inside the binary, `kibbr::` means the library and `crate::` means the binary.** The
  server and the command line write `kibbr::Database`.
- **`pub(crate)` in the engine means "engine only".** `src/engine/lib.rs` is the list of
  what the binary, the bench and the fuzz targets may name.

## The engine

`src/engine/lib.rs` has the tour of the engine: the one idea (a line is a shape and its
values), the modules, and TrieQL. It is the front page on docs.rs.

## The server and the commands

`src/server.rs` and `src/server/` are the HTTP side over one `Database`: `/ingest`,
`/query`, `/templates`, `/stats`, `/health`, the web console, and the loads and
compactions it runs. Nothing there reads a flag or writes to stderr, and the flush clock
is there, not in the store.

The subcommands of the binary are in two halves.

- `src/cmd/` is what someone runs in production: `serve`, `load`, `compact`, `query`,
  `explain`, `templates`, `stats` and `health`.
- `src/tools/` is what someone runs to try, measure or test kibbr: `parse`, `demo`,
  `gen-logs` and `bench-storage`. Nothing in `src/cmd/` names it.

`src/client.rs` is the small HTTP client that `load`, `compact`, the status commands and
`bench-storage` use to talk to a server.

## Generated logs and the oracle

`src/tools/gen/` writes reproducible fake logs (`kibbr gen-logs --profile farm --seed
42`), the same lines on every machine, so a benchmark or a fault test means the same
thing everywhere. `src/cmd/oracle.rs` is the oracle: every line, a closure, a counter.
Obviously correct and never shipped, it judges the real engine in `--reference` runs and
differential tests.

## Settings and logging

`src/config.rs` resolves every setting the same way: the flag, or else the `KIBBR__`
variable, or else the file `--config` names, or else the default. `src/logs.rs` is
kibbr's own log, one `tracing` subscriber that writes text or JSON lines to stderr.
`src/error.rs` is the errors every command shares.

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
  test of the binary now (`src/cmd/query/tests.rs`). It runs on the harness's own cursors
  and on every file in `fuzz/regressions/cursor/`. It is no longer fuzzed with coverage
  guidance.
