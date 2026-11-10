#!/usr/bin/env bash
# kibbr-server is HTTP over one Database and never reaches past the engine's public API:
# no file under crates/kibbr-server/ names `internals`, the engine's doc-hidden module for
# the command line's diagnostics. Exits 1, naming each line that does.
#
#   scripts/check/no_internals.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

FOUND=$(grep -rn --include='*.rs' --include='*.toml' 'internals' crates/kibbr-server/ || true)
if [ -n "$FOUND" ]; then
    echo "$FOUND"
    echo "kibbr-server names kibbr_engine::internals; it may use the engine's public API only"
    exit 1
fi
echo "kibbr-server never names kibbr_engine::internals"
