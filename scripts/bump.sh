#!/usr/bin/env bash
# Set the version of the crate, the first step of a release (RELEASING.md).
#
#   1. Cargo.toml: `version` under [package].
#   2. Cargo.lock: the crate's entry, by `cargo update --workspace`; CI publishes with
#      --locked, so a lockfile left behind fails the release.
#   3. docs/serve.md and deploy/README.md: the version in the /health example.
#   4. CHANGELOG.md: a `## VERSION — today` section on top, unless it has one, holding a
#      placeholder that scripts/publish.sh refuses to release.
#
# Everything is checked before anything is written. It commits nothing.
#
#   scripts/bump.sh 0.3.0

set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=${1:?usage: scripts/bump.sh VERSION}
DOCS=(docs/serve.md deploy/README.md)
PLACEHOLDER="- TODO: what changed"

fail() { echo "bump: $*" >&2; exit 1; }

# The same form the release workflow takes from a tag, without the v.
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || fail "'$VERSION' is not X.Y.Z"

OLD=$(sed -n '/^\[package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
[ -n "$OLD" ] || fail "no version under [package] in Cargo.toml"
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
    -e "/^\[package\]/,/^\[/s/^version = \"$OLD_RE\"/version = \"$VERSION\"/"
cargo update --workspace
for doc in "${DOCS[@]}"; do
    edit "$doc" -e "s/\"version\":\"$OLD_RE\"/\"version\":\"$VERSION\"/"
done

if grep -q "^## $VERSION " CHANGELOG.md; then
    echo "== CHANGELOG.md has a $VERSION section already"
else
    # Above the first `## ` line: the newest entry.
    awk -v head="## $VERSION — $(date +%Y-%m-%d)" -v body="$PLACEHOLDER" '
        !done && /^## / { print head; print ""; print body; print ""; done = 1 }
        { print }' CHANGELOG.md > CHANGELOG.md.bump && mv CHANGELOG.md.bump CHANGELOG.md
    echo "== CHANGELOG.md: write the $VERSION section in place of '$PLACEHOLDER'"
fi

git diff --stat
echo "== commit, push, then make the GitHub release v$VERSION (RELEASING.md)"
