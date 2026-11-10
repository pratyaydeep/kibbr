#!/usr/bin/env bash
# What runs in production never leans on what tries, measures or tests it: no file under
# src/cmd/ names `tools` - `crate::tools`, `kibbr::tools`, `super::super::tools`, a `tools`
# in a `use` list or a `#[path]` into src/tools/ - the module of the bench, the generators,
# `parse`, `demo` and the fuzz harnesses. Comment lines are not read. Exits 1, naming each
# line that does.
#
#   scripts/check/cmd_no_tools.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

FOUND=$(grep -rnE --include='*.rs' '(^|[^A-Za-z0-9_])tools([[:space:]]*(::|[;},])|/)' src/cmd \
    | grep -vE '^[^:]+:[0-9]+:[[:space:]]*//' || true)
if [ -n "$FOUND" ]; then
    echo "$FOUND"
    echo "src/cmd/ names src/tools/; a command may use the engine, the server and the rest of src/"
    exit 1
fi
echo "src/cmd/ never names src/tools/"
