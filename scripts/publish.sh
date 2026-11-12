#!/usr/bin/env bash
# Check a version and publish the crate to crates.io: what a GitHub release runs
# (.github/workflows/release.yml). Run here, it checks a version before the release.
#
# Before anything is published:
#
#   1. VERSION is the package version in Cargo.toml.
#   2. CHANGELOG.md has a `## VERSION ...` section.
#   3. The dry run passes.
#
# Then the real publish. cargo reads the token from `cargo login` or CARGO_REGISTRY_TOKEN.
#
#   scripts/publish.sh 0.1.0             checks, dry run, publish
#   scripts/publish.sh --dry-run 0.1.0   checks and dry run; publishes nothing
#   scripts/publish.sh --notes 0.1.0     checks 1 and 2, then prints the CHANGELOG section

set -euo pipefail
cd "$(dirname "$0")/.."

MODE=publish
case ${1:-} in
    --dry-run) MODE=dry-run; shift ;;
    --notes) MODE=notes; shift ;;
esac
VERSION=${1:?usage: scripts/publish.sh [--dry-run | --notes] VERSION}

fail() { echo "publish: $*" >&2; exit 1; }

PACKAGE=$(sed -n '/^\[package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
[ "$PACKAGE" = "$VERSION" ] || fail "Cargo.toml says $PACKAGE, not $VERSION"

# The section under `## VERSION ...`, up to the next `## `.
NOTES=$(awk -v v="$VERSION" '
    /^## / { if (on) exit; on = (index($0, "## " v " ") == 1) }
    on' CHANGELOG.md)
[ -n "$NOTES" ] || fail "no '## $VERSION' section in CHANGELOG.md"
grep -qF -- "- TODO: what changed" <<<"$NOTES" \
    && fail "the '## $VERSION' section in CHANGELOG.md still holds scripts/bump.sh's placeholder"

if [ "$MODE" = notes ]; then
    printf '%s\n' "$NOTES"
    exit 0
fi

echo "== dry run: kibbr"
cargo publish --locked --dry-run

[ "$MODE" = dry-run ] && exit 0

echo "== publish kibbr $VERSION"
cargo publish --locked
