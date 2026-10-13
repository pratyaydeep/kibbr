# Layout, tests, lints

A workspace of five crates, each naming only the ones under it - `kibbr`, then
`kibbr-server`, `kibbr-engine`, `kibbr-storage`, `kibbr-core` - and `fuzz/` beside them, a
workspace of its own. The four library crates each have a README with their files:
[core](../crates/kibbr-core/README.md), [storage](../crates/kibbr-storage/README.md),
[engine](../crates/kibbr-engine/README.md), [server](../crates/kibbr-server/README.md).

## The binary, `kibbr`

```
Cargo.toml                the workspace, and the `kibbr` crate: the binary - the command line
src/main.rs               the clap definitions and the dispatch
src/config.rs             where a setting comes from: a flag, a `KIBBR__` variable, the `--config` file, a default
src/logs.rs               the process's own log: `tracing` events as lines of text or of JSON
src/error.rs              thiserror enums of the commands, and what ends one
src/client.rs             the small HTTP client `load`, `compact` and `bench-storage --es` speak
src/cmd/parse.rs          the `parse` output
src/cmd/demo.rs           the `demo` tree
src/cmd/query.rs          the `query` command: the flags, the table, the plan
src/cmd/load.rs           `kibbr load` - the command: alone, or asked of the server holding the directory
src/cmd/compact.rs        `kibbr compact` - the command, the same two ways
src/cmd/ask.rs            `kibbr templates`, `kibbr stats` and `kibbr health`
src/cmd/bench_storage.rs  `kibbr bench-storage`
src/cmd/bench/            `kibbr bench`
src/gen/                  the generators - `hostile` (mod.rs) and the log farm (farm.rs)
src/reference/            the brute-force oracle behind `query --reference`
src/fuzzing/              what each fuzz target holds its subject to
tests/fixture.rs          outer-surface tests
tests/serve.rs            the HTTP contract, over a bare TcpStream - in this process, or of the binary
Dockerfile                the image: a static binary on distroless, and the `bench` target
bench/run.sh              `kibbr bench` in a container; bench/results/ holds what it measured
deploy/                   the Kubernetes manifests and the Vector sinks
scripts/                  the check, fault and measure scripts
```

- **No traits.** There is one implementation of everything so far, and an interface with a
  single caller is a guess. That includes the ingest path: a future `Ingestor` would sit
  between the API and the trie, and it is a plain function until something gives it a
  second caller to answer to.
- **The allocator is [mimalloc](https://github.com/microsoft/mimalloc).** On macOS arm64 the
  system one aborted the server with "pointer being freed was not allocated" at unrelated
  free sites that neither the code (no `unsafe`) nor AddressSanitizer could account for.

## Tests and lints

```
cargo test --workspace                                   # every crate, and tests/ at the root
cargo test -p kibbr-core                                 # one crate's own
cargo clippy --workspace --all-targets -- -D warnings    # lints have to be clean
```

- The tests under `tests/` run the real binary and read its stdout: nothing reaches inside
  the crate and there are no mocks.
- The one that matters most in `tests/fixture.rs` is
  `every_fixture_value_gets_its_labelled_type`: every hand-labelled value in
  `testdata/fixture.ndjson` must come out with the type its label says.
