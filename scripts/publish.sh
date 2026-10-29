#!/usr/bin/env bash
# Check a version and publish the five crates to crates.io: the part of a release that is
# the same here (scripts/release.sh) and in CI (.github/workflows/release.yml).
#
# In this order, each after the ones it depends on: kibbr-core, kibbr-storage,
# kibbr-engine, kibbr-server, kibbr. Before anything is published:
#
#   1. VERSION is the workspace version in Cargo.toml.
#   2. CHANGELOG.md has a `## VERSION ...` section.
#   3. The five dry-runs pass. They run as one `cargo publish --dry-run`, so a crate is
#      checked against the ones under it as they are here, not as crates.io has them.
#
# Then the five real publishes, 30 seconds apart so crates.io has indexed one before the
# next asks for it. cargo reads the token from `cargo login` or CARGO_REGISTRY_TOKEN.
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
CRATES=(kibbr-core kibbr-storage kibbr-engine kibbr-server kibbr)
WAIT_SECONDS=30

fail() { echo "publish: $*" >&2; exit 1; }

WORKSPACE=$(sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml)
[ "$WORKSPACE" = "$VERSION" ] || fail "Cargo.toml says $WORKSPACE, not $VERSION"

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

PACKAGES=()
for crate in "${CRATES[@]}"; do PACKAGES+=(-p "$crate"); done
echo "== dry run: ${CRATES[*]}"
cargo publish --locked --dry-run "${PACKAGES[@]}"

[ "$MODE" = dry-run ] && exit 0

for i in "${!CRATES[@]}"; do
    crate=${CRATES[$i]}
    if [ "$i" -gt 0 ]; then
        echo "== waiting ${WAIT_SECONDS}s for crates.io to index ${CRATES[$((i - 1))]}"
        sleep "$WAIT_SECONDS"
    fi
    echo "== publish $crate $VERSION"
    cargo publish --locked -p "$crate"
done
