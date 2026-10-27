#!/usr/bin/env bash
# Publish the five crates to crates.io from here and tag the release: a release without
# CI. The usual way is a GitHub release, which runs .github/workflows/release.yml; see
# CONTRIBUTING.md. Do not do both for one version: the second finds it already published.
#
# Before anything is published:
#
#   1. The checks of scripts/publish.sh: VERSION is the workspace version in Cargo.toml,
#      and CHANGELOG.md has a section for it.
#   2. The tree is clean: nothing modified, nothing untracked.
#   3. The tag vVERSION exists neither here nor on origin.
#
# Then scripts/publish.sh: the five dry-runs, and the five publishes 30 seconds apart. Last,
# the tag, annotated with the version's CHANGELOG.md section. Pushing the tag is left to
# the person running this.
#
#   scripts/release.sh 0.1.0

set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=${1:?usage: scripts/release.sh VERSION}
TAG="v$VERSION"

fail() { echo "release: $*" >&2; exit 1; }

NOTES=$(scripts/publish.sh --notes "$VERSION")

[ -z "$(git status --porcelain)" ] || fail "the tree is not clean"

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null && fail "tag $TAG exists"
git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null && fail "tag $TAG exists on origin"

scripts/publish.sh "$VERSION"

# `verbatim`: by default git drops lines that start with `#` as comments, and the notes
# open with their `## VERSION` heading.
git tag -a --cleanup=verbatim "$TAG" -m "$NOTES"
echo "== tagged $TAG; push it with: git push origin $TAG"
