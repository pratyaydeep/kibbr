#!/usr/bin/env bash
# The server is HTTP over one Database and never reaches past the engine's public API:
# neither src/server.rs nor a file under src/server/ names `internals`, the engine's
# doc-hidden module for the command line's diagnostics. Exits 1, naming each line that does.
#
#   scripts/check/no_internals.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

FOUND=$(grep -rn --include='*.rs' 'internals' src/server.rs src/server/ || true)
if [ -n "$FOUND" ]; then
    echo "$FOUND"
    echo "the server names kibbr::internals; it may use the engine's public API only"
    exit 1
fi
echo "the server never names kibbr::internals"
