# Releasing kibbr

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
