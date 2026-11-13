# Contributing

Thank you for helping. This page starts with what you need on day one.
The legal part is at the end.

## Build and test

You need Rust. The version is pinned in `rust-toolchain.toml`, so `rustup` picks it.
From the repository root:

```
cargo build --release
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
```

Use `--all-features`: without it `kibbr-bench` (the `bench` feature) is not built and its
tests, `bench::speed`, do not run; the fuzz regressions are replayed either way. The
integration tests are one binary, `e2e`, a module a topic; to run one topic give its
module path, `cargo test --all-features --test e2e -- load::`, and to run one area's unit
tests its path in the crate, `cargo test --lib store::`.

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

From the repository root:

```
cargo build --release
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets -- -D warnings
scripts/check/agree.sh
ROUNDS=3 scripts/fault/crash_test.sh
```

CI (`.github/workflows/ci.yml`) runs the same on Linux.

## Where a test goes

A unit test goes in its module's `tests` module: inline up to 100 lines, and past that a
sibling file, `wal.rs` -> `wal_tests.rs`, declared as
`#[cfg(test)] #[path = "wal_tests.rs"] mod tests;`. A test of the binary or of HTTP goes in a topic module of the `e2e`
binary (`tests/<topic>.rs`), with what topics share in `tests/harness.rs`. A test that needs
a killed process, two builds or a differential run is a script in `scripts/fault/` or
`scripts/check/`. The scripts run the release binary and need `bash`, `curl` and `python3`;
[scripts/README.md](scripts/README.md) lists them and what each prints when it passes.

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

## Releases

How a version is released is in [RELEASING.md](RELEASING.md).

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
