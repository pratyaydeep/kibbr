#!/usr/bin/env bash
# Set the version of kibbr, the first step of a release (CONTRIBUTING.md).
#
#   1. Cargo.toml: `version` under [workspace.package], which the `kibbr` package and the
#      bench's package inherit.
#   2. Cargo.lock: the two packages' entries, by `cargo update --workspace`; CI publishes
#      with --locked, so a lockfile left behind fails the release.
#   3. docs/serve.md and docs/container.md: the version in the /health example.
#   4. CHANGELOG.md: the `## Unreleased` section becomes `## VERSION — today`, with the
#      notes that are under it. With no such section, and none for VERSION, a new one goes
#      on top, holding a placeholder that scripts/publish.sh refuses to release.
#
# Everything is checked before anything is written. It commits nothing.
#
#   scripts/bump.sh 0.3.0

set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=${1:?usage: scripts/bump.sh VERSION}
DOCS=(docs/serve.md docs/container.md)
PLACEHOLDER="- TODO: what changed"

fail() { echo "bump: $*" >&2; exit 1; }

# The same form the release workflow takes from a tag, without the v.
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || fail "'$VERSION' is not X.Y.Z"

OLD=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
[ -n "$OLD" ] || fail "no version under [workspace.package] in Cargo.toml"
[ "$OLD" != "$VERSION" ] || fail "Cargo.toml is already $VERSION"

# Dots in OLD are literal, not "any character".
OLD_RE=${OLD//./\\.}
for doc in "${DOCS[@]}"; do
    grep -q "\"version\":\"$OLD_RE\"" "$doc" || fail "$doc has no \"version\":\"$OLD\""
done

# Through a temporary file: `sed -i` takes different arguments on macOS and Linux.
edit() {
    local file=$1; shift
    sed "$@" "$file" > "$file.bump" && mv "$file.bump" "$file"
}

echo "== $OLD -> $VERSION"
edit Cargo.toml \
    -e "/^\[workspace.package\]/,/^\[/s/^version = \"$OLD_RE\"/version = \"$VERSION\"/"
cargo update --workspace
for doc in "${DOCS[@]}"; do
    edit "$doc" -e "s/\"version\":\"$OLD_RE\"/\"version\":\"$VERSION\"/"
done

HEAD="## $VERSION — $(date +%Y-%m-%d)"
if grep -q "^## $VERSION " CHANGELOG.md; then
    echo "== CHANGELOG.md has a $VERSION section already"
elif grep -q "^## Unreleased$" CHANGELOG.md; then
    # The notes written since the last release are this version's: the heading over them
    # is renamed, so that none is left under a heading no release has.
    edit CHANGELOG.md -e "s/^## Unreleased\$/$HEAD/"
    echo "== CHANGELOG.md: '## Unreleased' is now '$HEAD'; read it over"
else
    # Above the first `## ` line: the newest entry.
    awk -v head="$HEAD" -v body="$PLACEHOLDER" '
        !done && /^## / { print head; print ""; print body; print ""; done = 1 }
        { print }' CHANGELOG.md > CHANGELOG.md.bump && mv CHANGELOG.md.bump CHANGELOG.md
    echo "== CHANGELOG.md: write the $VERSION section in place of '$PLACEHOLDER'"
fi

git diff --stat
echo "== commit, push, then make the GitHub release v$VERSION (CONTRIBUTING.md)"
