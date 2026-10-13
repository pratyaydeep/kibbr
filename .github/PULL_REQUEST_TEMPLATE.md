**What this changes, and why**

**How it was checked**

- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `scripts/check/agree.sh`, and any other script under `scripts/` the change touches

**Sign-off**

- [ ] Every commit is signed off (`git commit -s`): the `Signed-off-by:` line is the
      Developer Certificate of Origin in [CONTRIBUTING.md](../CONTRIBUTING.md), and the
      contribution is licensed under MIT ([LICENSE-CONTRIBUTOR](../LICENSE-CONTRIBUTOR)).
