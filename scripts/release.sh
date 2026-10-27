#!/usr/bin/env bash
# Publish the five crates to crates.io and tag the release.
#
# In this order, each after the ones it depends on: kibbr-core, kibbr-storage,
# kibbr-engine, kibbr-server, kibbr. Before anything is published:
#
#   1. VERSION is the workspace version in Cargo.toml.
#   2. The tree is clean: nothing modified, nothing untracked.
#   3. The tag vVERSION exists neither here nor on origin.
#   4. The five dry-runs pass. They run as one `cargo publish --dry-run`, so a crate is
#      checked against the ones under it as they are here, not as crates.io has them.
#
# Then the five real publishes, 30 seconds apart so crates.io has indexed one before the
# next asks for it, and the tag, annotated with the version's CHANGELOG.md section.
# Pushing the tag is left to the person running this.
#
#   scripts/release.sh 0.1.0

set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=${1:?usage: scripts/release.sh VERSION}
TAG="v$VERSION"
CRATES=(kibbr-core kibbr-storage kibbr-engine kibbr-server kibbr)
WAIT_SECONDS=30

fail() { echo "release: $*" >&2; exit 1; }

WORKSPACE=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
[ "$WORKSPACE" = "$VERSION" ] || fail "Cargo.toml says $WORKSPACE, not $VERSION"

[ -z "$(git status --porcelain)" ] || fail "the tree is not clean"

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null && fail "tag $TAG exists"
git ls-remote --exit-code --tags origin "refs/tags/$TAG" >/dev/null && fail "tag $TAG exists on origin"

# The section under `## VERSION ...`, up to the next `## `.
NOTES=$(awk -v v="$VERSION" '
    /^## / { if (on) exit; on = (index($0, "## " v " ") == 1) }
    on' CHANGELOG.md)
[ -n "$NOTES" ] || fail "no '## $VERSION' section in CHANGELOG.md"

PACKAGES=()
for crate in "${CRATES[@]}"; do PACKAGES+=(-p "$crate"); done
echo "== dry run: ${CRATES[*]}"
cargo publish --locked --dry-run "${PACKAGES[@]}"

for i in "${!CRATES[@]}"; do
    crate=${CRATES[$i]}
    if [ "$i" -gt 0 ]; then
        echo "== waiting ${WAIT_SECONDS}s for crates.io to index ${CRATES[$((i - 1))]}"
        sleep "$WAIT_SECONDS"
    fi
    echo "== publish $crate $VERSION"
    cargo publish --locked -p "$crate"
done

# `verbatim`: by default git drops lines that start with `#` as comments, and the notes
# open with their `## VERSION` heading.
git tag -a --cleanup=verbatim "$TAG" -m "$NOTES"
echo "== tagged $TAG; push it with: git push origin $TAG"
