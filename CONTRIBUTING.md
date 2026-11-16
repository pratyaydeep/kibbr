# Contributing

## The licence of a contribution

Every contribution to kibbr is licensed by its author under the MIT terms in
[LICENSE-CONTRIBUTOR](LICENSE-CONTRIBUTOR). kibbr as a whole is distributed under
AGPL-3.0-only ([LICENSE](LICENSE)), which MIT-licensed contributions can be part of.

## Sign-off

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

## Build, test and checks

What you need:

- **Rust**, the version in `rust-toolchain.toml`; `rustup` installs it for you.
- **`bash`, `curl` and `python3`** for the scripts under `scripts/`.
- **Node 20.19 or newer** only for the console under `web/`: `npm ci`, then
  `npm run build`, which writes `src/server/viewer.html`. The Rust build needs no Node.

Before a pull request, from the repository root:

```
cargo build --release
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/check/agree.sh
ROUNDS=3 scripts/fault/crash_test.sh
```

CI (`.github/workflows/ci.yml`) runs the same on Linux, and also plain `cargo test`, clippy
on the build that ships, `cargo publish --dry-run --locked` and a check that the fuzz targets
build. Without `--all-features` the fuzzing feature is off and the fuzz regressions are not
replayed.

The scripts run the release binary. [scripts/README.md](scripts/README.md) lists them and
what each prints when it passes. A new `.rs` file starts with the two lines every other one
starts with: the copyright line and `// SPDX-License-Identifier: AGPL-3.0-only`.

## Where changes go

[ARCHITECTURE.md](ARCHITECTURE.md) has the whole map and a longer table. The short one:

| Area | Path | Tests to add |
|---|---|---|
| How a value's type is decided | `src/engine/model/infer.rs` | a line in `tests/data/fixture.ndjson` |
| Compression: the encodings and row groups | `src/engine/format/build.rs`, `format/column.rs`, `format/read.rs`; `src/engine/rows/disk/compact.rs` | `src/engine/format/encodings.rs`; measure with `kibbr bench-storage` |
| The on-disk format | `src/engine/format/` | a vector in `tests/data/format/`; `tests/compat.rs` |
| Durability: the log, a start, a stop | `src/engine/wal.rs`, `src/engine/lifecycle.rs` | `tests/restart.rs`; `scripts/fault/crash_test.sh` |
| The query language and the answers | `src/engine/query/` | `src/engine/query/tests/`, `tests/query.rs`, `tests/trieql.rs` |
| An HTTP endpoint | `src/server/handlers.rs`, `src/server/routes.rs` | `src/tests/server.rs`, `tests/serve.rs` |
| A command, a flag or a setting | `src/cli/` | the topic of that name under `tests/`; `docs/settings.md` |
| The console | `web/src/` | CI's `web` job |

Three rules that a change must keep, each held by `tests/source_rules.rs`:

- The engine names no HTTP, no async runtime, and nothing of the server or the command
  line.
- A new dependency goes into a layer's list in that test, or the test fails.
- The unit tests of `dir/x.rs` are `dir/tests/x.rs`.

## Releases

A GitHub release publishes the `kibbr` crate to crates.io:
`.github/workflows/release.yml` runs when a release is published (not a draft).

Once per repository, add the token: on crates.io, Account Settings > API Tokens, make one
with the `publish-update` scope for the `kibbr` crate; on GitHub, Settings > Secrets and
variables > Actions, add it as the repository secret `CARGO_REGISTRY_TOKEN`.

For each version, for example 0.3.0:

1. `scripts/bump.sh 0.3.0` sets the version in Cargo.toml, Cargo.lock and the docs, and
   turns the `## Unreleased` section of CHANGELOG.md into `## 0.3.0 — DATE`. Read that
   section over, and commit. With no `## Unreleased` it adds a section with a placeholder,
   which the release refuses: write the section in its place.
2. Check it: `scripts/publish.sh --dry-run 0.3.0`. The dry run builds the package from
   what would be uploaded, in `target/package/`.
3. Push the commit. Tag it `v0.3.0` and push the tag, or let the release form make the tag.
4. On GitHub, Releases > Draft a new release: tag `v0.3.0`, the notes from
   `scripts/publish.sh --notes 0.3.0`. Publish it.

The workflow fails before publishing anything if the tag is not the workspace version,
CHANGELOG.md has no section for it, `web/` does not build the committed `viewer.html`, the
token is missing, or the dry run fails. Then it publishes the one crate, `kibbr`. The
bench (`kibbr-bench`) is never published. [scripts/README.md](scripts/README.md) describes the scripts.
