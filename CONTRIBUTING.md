# Contributing

Thank you for helping. This page starts with what you need on day one.
The legal part and the release steps are at the end.

## Build and test

You need Rust. The version is pinned in `rust-toolchain.toml`, so `rustup` picks it.
From the repository root:

```
cargo build --release
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets -- -D warnings
```

Use `--all-features`: without it `kibbr-bench` (the `bench` feature) is not built and its
tests, `bench::speed`, do not run; the fuzz regressions are replayed either way. The
integration tests are one binary, `e2e`, a module a topic; to run one topic give its
module path, `cargo test --all-features --test e2e -- load::`, and to run one area's unit
tests its path in the crate, `cargo test --lib store::`.

Run the tests as a normal user. As root, the tests that make a directory read-only to
see a refusal fail, because root writes anyway; if you must, run
`cargo test --all-features --no-fail-fast` and read the failures as those.

Where a new test goes (inline, a `<module>_tests.rs` file, `tests/`, or a script) is in
"Tests" in [ARCHITECTURE.md](ARCHITECTURE.md#tests).

Try the binary in one minute: see "Try it in a minute" in [README.md](README.md).

## Find your way

Read [ARCHITECTURE.md](ARCHITECTURE.md) first. It is short. It shows the write path and
the query path, where each part lives, and which files to change for common tasks.
Some words in the code are kibbr's own (template, leaf, door, shed, backstop):
[docs/glossary.md](docs/glossary.md) explains each one.

Ask in an issue before a large change.

## Sign your commits

Every commit needs a `Signed-off-by:` line: use `git commit -s`.
"Licence and sign-off" at the end says what it means.

## Before a pull request

From the repository root, the four commands of "Build and test", then:

```
scripts/check/agree.sh
ROUNDS=3 scripts/fault/crash_test.sh
```

CI (`.github/workflows/ci.yml`) runs the same on Linux.

## Rules a check enforces

- A new `.rs` file starts with the two lines every other one starts with: the copyright
  line and `// SPDX-License-Identifier: AGPL-3.0-only`. `tests/source_rules.rs` checks it.
- The engine names no async runtime, no HTTP and no command line, and the server names no
  command line: `tests/source_rules.rs`, which reads the source.
- clippy with `-D warnings`, on all targets, with every feature and with none.

## The web console

The console is `src/server/viewer.html`, built from the ReScript source in `web/`. Edit
`web/`, run `npm run build` there, and commit the built `viewer.html` with it: CI fails if
they differ ([docs/viewer.md](docs/viewer.md)).

## Fuzzing and benchmarks

The fuzz targets are in `fuzz/`, a Cargo workspace of its own; their harnesses are
`src/fuzzing/` and `src/engine/fuzzing/`. Fuzzing needs a nightly toolchain and
`cargo install cargo-fuzz`. `fuzz/run.sh [secs] [target...]` runs every target (or the
ones named) for `secs` seconds each, 1800 by default. A crash is fixed, then its input is
committed under `fuzz/regressions/<target>/`, which a plain `cargo test` replays on stable.

`kibbr-bench` measures this build's speed on one farm day:
`cargo run --release --features bench --bin kibbr-bench -- --help`. `scripts/bench/run.sh`
runs it inside the container image and writes the table;
[scripts/bench/README.md](scripts/bench/README.md) says how to read a result and compare it
with a baseline.

## Licence and sign-off

### The licence of a contribution

Every contribution to kibbr is licensed by its author under the MIT terms in
[LICENSE-CONTRIBUTOR](LICENSE-CONTRIBUTOR). kibbr as a whole is distributed under
AGPL-3.0-only ([LICENSE](LICENSE)), which MIT-licensed contributions can be part of.


### Sign-off

Every commit carries a `Signed-off-by:` line with your real name and an address you can be
reached at - `git commit -s` adds it. It says that you agree to the Developer Certificate
of Origin, in full:

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

## Releases

A GitHub release publishes the crate, `kibbr`, to crates.io:
`.github/workflows/release.yml` runs when a release is published (not a draft).

Once per repository, add the token: on crates.io, Account Settings > API Tokens, make one
with the `publish-update` scope for the kibbr crate; on GitHub, Settings > Secrets and
variables > Actions, add it as the repository secret `CARGO_REGISTRY_TOKEN`.

For each version, for example 0.3.0:

1. `scripts/bump.sh 0.3.0` sets the version in Cargo.toml, Cargo.lock and the docs, and
   adds a `## 0.3.0 — DATE` section to CHANGELOG.md. Write that section in place of its
   placeholder, which the release refuses, and commit.
2. Check it: `scripts/publish.sh --dry-run 0.3.0`. The dry run builds the crate as packaged,
   under `target/package/`.
3. Push the commit. Tag it `v0.3.0` and push the tag, or let the release form make the tag.
4. On GitHub, Releases > Draft a new release: tag `v0.3.0`, the notes from
   `scripts/publish.sh --notes 0.3.0`. Publish it.

The workflow fails before publishing anything if the tag is not the version in Cargo.toml,
CHANGELOG.md has no section for it, `web/` does not build the committed `viewer.html`, the
token is missing, or the dry run fails. Then it publishes `kibbr`.
[scripts/README.md](scripts/README.md) describes the scripts.
