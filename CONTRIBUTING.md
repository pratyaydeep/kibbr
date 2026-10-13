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
