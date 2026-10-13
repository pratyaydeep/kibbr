#!/usr/bin/env bash
# Every .rs file under src/, crates/, tests/ and fuzz/ starts with the same two lines:
#
#   // Copyright (C) 2026 Pratyaydeep Ghanta
#   // SPDX-License-Identifier: AGPL-3.0-only
#
# With no argument this lists the files that do not, and exits 1 if there is one. With
# --fix it puts the two lines, and a blank one, on top of each of them. The files are the
# ones git tracks or would track, so a build directory under fuzz/ is not walked.
#
#   scripts/check/spdx.sh
#   scripts/check/spdx.sh --fix

set -uo pipefail
cd "$(dirname "$0")/../.."

FIRST='// Copyright (C) 2026 Pratyaydeep Ghanta'
SECOND='// SPDX-License-Identifier: AGPL-3.0-only'

FIX=0
case "${1:-}" in
    "") ;;
    --fix) FIX=1 ;;
    *) echo "usage: $0 [--fix]" >&2; exit 2 ;;
esac

FILES=0
MISSING=0
while IFS= read -r file; do
    [ -f "$file" ] || continue
    FILES=$((FILES + 1))
    if [ "$(sed -n 1p "$file")" = "$FIRST" ] && [ "$(sed -n 2p "$file")" = "$SECOND" ]; then
        continue
    fi
    MISSING=$((MISSING + 1))
    if [ "$FIX" = 1 ]; then
        tmp=$(mktemp)
        { printf '%s\n%s\n\n' "$FIRST" "$SECOND"; cat "$file"; } > "$tmp"
        cat "$tmp" > "$file"
        rm -f "$tmp"
    else
        echo "no header: $file"
    fi
done < <(git ls-files -co --exclude-standard -- 'src/*.rs' 'crates/*.rs' 'tests/*.rs' 'fuzz/*.rs')

if [ "$FIX" = 1 ]; then
    echo "$FILES files, $MISSING given the header"
    exit 0
fi
echo "$FILES files, $MISSING without the header"
[ "$MISSING" = 0 ]
