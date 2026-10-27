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

## Tests and checks

The Rust version is the one in `rust-toolchain.toml`. Before a pull request, from the
repository root:

```
cargo build --release
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
scripts/check/spdx.sh
scripts/check/agree.sh
ROUNDS=3 scripts/fault/crash_test.sh
```

CI (`.github/workflows/ci.yml`) runs the same on Linux.

The scripts run the release binary and need `bash`, `curl` and `python3`.
[scripts/README.md](scripts/README.md) lists the rest of them and what each prints when it
passes. A new `.rs` file starts with the two lines every other one starts with: the
copyright line and `// SPDX-License-Identifier: AGPL-3.0-only`.

## Releases

A GitHub release publishes the five crates to crates.io:
`.github/workflows/release.yml` runs when a release is published (not a draft).

Once per repository, add the token: on crates.io, Account Settings > API Tokens, make one
with the `publish-update` scope for the five kibbr crates; on GitHub, Settings > Secrets and
variables > Actions, add it as the repository secret `CARGO_REGISTRY_TOKEN`.

For each version, for example 0.3.0:

1. Set `version = "0.3.0"` under `[workspace.package]` in Cargo.toml, add a
   `## 0.3.0 — DATE` section to CHANGELOG.md, and commit.
2. Check it: `scripts/publish.sh --dry-run 0.3.0`. The dry run builds the five crates,
   about 2 GB in `target/`.
3. Push the commit. Tag it `v0.3.0` and push the tag, or let the release form make the tag.
4. On GitHub, Releases > Draft a new release: tag `v0.3.0`, the notes from
   `scripts/publish.sh --notes 0.3.0`. Publish it.

The workflow fails before publishing anything if the tag is not the workspace version,
CHANGELOG.md has no section for it, `web/` does not build the committed `viewer.html`, the
token is missing, or the dry run fails. Then it publishes in dependency order, 30 seconds
apart. [scripts/README.md](scripts/README.md) describes `scripts/release.sh`, which does the
same from a laptop, without CI.
