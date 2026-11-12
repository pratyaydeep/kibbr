#!/usr/bin/env bash
# A data directory this build wrote, handed to an older build: it must open, answer and
# keep writing.
#
# Whatever this build adds to what is on disk has to be something an older build steps
# over - an optional section in a row group, an optional field in a record - and never
# something it refuses or misreads. That cannot be seen from inside one build, so this
# takes two: it builds COMMIT in a git worktree of its own, has the current build write a
# data directory, and starts the older binary on it.
#
# What this asserts:
#
#   1. The current build takes every line: some into row groups by a graceful stop, some
#      rebuilt by `kibbr compact`, and the last left in the write-ahead log only, the
#      server killed before it stored them.
#   2. The older build starts on that directory, is healthy, counts every line, and
#      returns every row - compared with the lines as the older build understands them
#      (see "The comparison").
#   3. The older build keeps writing: it takes more lines, compacts every day that holds
#      row groups to rebuild, stops with exit 0, starts again, and still returns every row.
#   4. The current build then opens what the older one left, and returns every row too.
#
# THE COMPARISON. A build at or before ca6d867 returns an array as the string of its JSON
# text, and a line that held an empty object without that key; this build returns both as
# themselves, and stores `{}` under a key. So a row is compared with its line after both
# are brought to what every build agrees on: an array and the string of its JSON text are
# taken as one value - the elements, read as JSON - and a key whose value is `{}` or the
# string "{}" is left out. What is left must match line for line, both ways. roundtrip.sh is what holds the current build to the
# whole of the model; this holds two builds to each other.
#
#   scripts/check/downgrade.sh                 # against ca6d867, the build before the JSON section
#   scripts/check/downgrade.sh v0.3.0          # against any commit or tag with today's command line
#   OLD_BIN=/path/to/an/older/kibbr scripts/check/downgrade.sh   # a binary already built
#
# The older build's worktree and its target directory are kept under target/downgrade/, so
# a second run against the same commit builds nothing. `git worktree remove
# target/downgrade/<sha>` takes one away.

set -uo pipefail
cd "$(dirname "$0")/../.."

COMMIT=${1:-ca6d867}
BIN=${BIN:-./target/release/kibbr}
OLD_BIN=${OLD_BIN:-}
HOSTILE=${HOSTILE:-2000}
FARM=${FARM:-2000}
CASES=tests/data/roundtrip.ndjson

[ "$BIN" = ./target/release/kibbr ] && { cargo build --release --quiet || exit 1; }

if [ -z "$OLD_BIN" ]; then
  SHA=$(git rev-parse --short=12 --verify "$COMMIT^{commit}") \
    || { echo "downgrade.sh: no such commit: $COMMIT" >&2; exit 1; }
  TREE="target/downgrade/$SHA"
  if [ ! -e "$TREE/Cargo.toml" ]; then
    mkdir -p target/downgrade
    git worktree add --quiet --detach "$TREE" "$SHA" || exit 1
  fi
  # One target directory for every older build, apart from this build's own: sharing
  # ./target would have each build undo the other's.
  ( cd "$TREE" && CARGO_TARGET_DIR="$PWD/../target" cargo build --release --locked --quiet ) \
    || { echo "downgrade.sh: $COMMIT did not build" >&2; exit 1; }
  OLD_BIN="target/downgrade/kibbr-$SHA"
  # A copy by commit, taken at once: the next older build writes over the shared one.
  cp "target/downgrade/target/release/kibbr" "$OLD_BIN" || exit 1
fi
echo "current: $("$BIN" --version)   older: $("$OLD_BIN" --version) ($COMMIT)"

WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

{
  "$BIN" gen-logs --lines "$HOSTILE" --seed 11
  "$BIN" gen-logs --profile farm --lines "$FARM" --seed 11
  cat "$CASES"
} > "$WORK/all.ndjson"
"$BIN" gen-logs --profile farm --lines 500 --seed 12 > "$WORK/more.ndjson"
# Three parts of the first input: stored and compacted, stored, and logged only.
python3 - "$WORK" <<'PY'
import sys
work = sys.argv[1]
lines = open(work + "/all.ndjson", "rb").read().split(b"\n")
if lines and lines[-1] == b"":
    lines.pop()
third = len(lines) // 3
for name, part in (("a", lines[:third]), ("b", lines[third:2 * third]), ("c", lines[2 * third:])):
    open("%s/%s.ndjson" % (work, name), "wb").write(b"".join(l + b"\n" for l in part))
PY

PORT=""
start() {  # start <binary> <logfile>
  "$1" serve --port 0 --data-dir "$WORK/data" --compact-interval-ms 0 \
       --flush-interval-ms 100 --row-group-timeout-ms 100 2>"$2" &
  PID=$!
  PORT=""
  for _ in $(seq 1 100); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$2" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  [ -n "$PORT" ] || { echo "FAIL: $1 did not start on the directory:" >&2; cat "$2" >&2; PID=""; return 1; }
}

stop() {  # stop: SIGTERM, and the exit code
  kill -TERM "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  local code=$?
  PID=""
  return $code
}

ingest() {  # ingest <file> <label>
  local code
  code=$(curl -s -o "$WORK/ingest.$2.json" -w '%{http_code}' \
         -X POST "http://127.0.0.1:$PORT/ingest" --data-binary "@$1")
  [ "$code" = "200" ] || { echo "FAIL: /ingest of $2 answered $code" >&2
                           head -c 2000 "$WORK/ingest.$2.json" >&2; return 1; }
}

# Every day the directory holds, compacted by name: without --day only a day that ended
# before today is taken, and these rows were written now.
compact_all() {  # compact_all <binary> <label>
  local day rebuilt=0
  for day in $(ls "$WORK/data/data"); do
    "$1" compact --data-dir "$WORK/data" --day "$day" --json \
         > "$WORK/compact.json" 2>"$WORK/compact.err" \
      || { echo "FAIL: $2: compact --day $day failed:" >&2; cat "$WORK/compact.err" >&2; return 1; }
    grep -q '"compacted":true' "$WORK/compact.json" && rebuilt=$((rebuilt + 1))
  done
  echo "$2: compacted $rebuilt day(s)"
  [ "$rebuilt" -gt 0 ] || { echo "FAIL: $2: no day had row groups to rebuild" >&2; return 1; }
}

healthy() {  # healthy <label>
  curl -s "http://127.0.0.1:$PORT/health" | grep -q '"ok":true' \
    || { echo "FAIL: $1: /health does not say ok" >&2; return 1; }
}

# Every page of `MATCH LOG {}`, held to the lines of the files named, each with the
# /ingest report that says which of its lines were refused.
rows_are() {  # rows_are <label> <file:report>...
  python3 - "$PORT" "$@" <<'PY'
import json, sys, urllib.request
from collections import Counter
from fractions import Fraction

port, label, inputs = sys.argv[1], sys.argv[2], sys.argv[3:]

def escape(segment):
    return segment.replace("\\", "\\\\").replace(".", "\\.")

def flatten(obj, prefix, out):
    for key, value in obj.items():
        path = prefix + [escape(key)]
        if isinstance(value, dict) and value:
            flatten(value, path, out)
        else:
            out[".".join(path)] = value
    return out

def shaped(value):
    """An array or an object, or the string of one's JSON text, as the value it spells;
    `None` for anything else. One build's array is another's string, and the text is not
    compared as text: each JSON writer spells a number its own way."""
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str) and value[:1] in ("[", "{"):
        try:
            inner = json.loads(value)
        except ValueError:
            return None
        if isinstance(inner, (list, dict)):
            return inner
    return None

def canon(value):
    if value is None or isinstance(value, (bool, str)):
        return (type(value).__name__, value)
    if isinstance(value, list):
        return ("array", tuple(canon(v) for v in value))
    if isinstance(value, dict):
        return ("object", tuple(sorted((k, canon(v)) for k, v in value.items())))
    if isinstance(value, int) and not (-2**63 <= value < 2**64):
        value = float(value)
    return ("number", Fraction(value))

def row_key(row):
    """A row as every build gives it, hashable: see "The comparison"."""
    out = []
    for key, value in row.items():
        inner = shaped(value)
        if inner == {}:
            continue
        out.append((key, canon(value if inner is None else inner)))
    return tuple(sorted(out))

want = Counter()
for pair in inputs:
    path, report_path = pair.split(":")
    report = json.load(open(report_path))
    refused = {e["line"] for e in report.get("errors", [])}
    raw = open(path, encoding="utf-8", errors="surrogateescape").read().split("\n")
    taken = 0
    for n, line in enumerate(raw, 1):
        if n in refused or not line.strip():
            continue
        want[row_key(flatten(json.loads(line), [], {}))] += 1
        taken += 1
    if taken != report["ingested"]:
        sys.exit("FAIL: /ingest took %d lines of %s and %d are accounted for"
                 % (report["ingested"], path, taken))

rows, cursor = [], None
while True:
    q = "MATCH LOG {}" + (' AFTER "%s"' % cursor if cursor else "")
    req = urllib.request.Request("http://127.0.0.1:%s/query" % port,
                                 json.dumps({"q": q}).encode(),
                                 {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        answer = json.loads(r.read())
    rows.extend(answer.get("rows") or [])
    cursor = answer.get("meta", {}).get("next_cursor")
    if not cursor:
        break

got = Counter()
for row in rows:
    if "_received_at" in row and row_key(row) not in want:
        row = {k: v for k, v in row.items() if k != "_received_at"}
    got[row_key(row)] += 1
missing, extra = want - got, got - want
differences = sum(missing.values()) + sum(extra.values())
shaped = sum(1 for row in rows if any(isinstance(v, (list, dict)) for v in row.values()))
print("%-34s %d lines, %d rows, %d differences; %d rows hold an array or an object"
      % (label + ":", sum(want.values()), len(rows), differences, shaped))
for key in list(missing.elements())[:10]:
    print("  not returned: %s" % dict(key))
for key in list(extra.elements())[:10]:
    print("  returned and never ingested: %s" % dict(key))
sys.exit(1 if differences else 0)
PY
}

A="$WORK/a.ndjson:$WORK/ingest.a.json"
B="$WORK/b.ndjson:$WORK/ingest.b.json"
C="$WORK/c.ndjson:$WORK/ingest.c.json"
MORE="$WORK/more.ndjson:$WORK/ingest.more.json"
status=0

# 1. The current build writes: a part stored and compacted, a part stored, a part logged.
start "$BIN" "$WORK/new1.log" || exit 1
ingest "$WORK/a.ndjson" a || exit 1
sleep 0.5
stop || { echo "FAIL: the current build's first stop did not exit 0" >&2; exit 1; }
compact_all "$BIN" "current build" || exit 1
start "$BIN" "$WORK/new2.log" || exit 1
ingest "$WORK/b.ndjson" b || exit 1
sleep 0.5
rows_are "current build, stored" "$A" "$B" || status=1
stop || { echo "FAIL: the current build's second stop did not exit 0" >&2; exit 1; }
start "$BIN" "$WORK/new3.log" || exit 1
ingest "$WORK/c.ndjson" c || exit 1
rows_are "current build, before the kill" "$A" "$B" "$C" || status=1
kill -KILL "$PID"; wait "$PID" 2>/dev/null; PID=""

# 2. The older build opens it.
start "$OLD_BIN" "$WORK/old1.log" || exit 1
healthy "the older build" || status=1
rows_are "older build, opens" "$A" "$B" "$C" || status=1

# 3. The older build keeps writing: more lines, a compaction, a stop, a start.
ingest "$WORK/more.ndjson" more || exit 1
sleep 0.5
rows_are "older build, after writing" "$A" "$B" "$C" "$MORE" || status=1
stop || { echo "FAIL: the older build's stop did not exit 0:" >&2; tail -20 "$WORK/old1.log" >&2; status=1; }
compact_all "$OLD_BIN" "older build" || status=1
start "$OLD_BIN" "$WORK/old2.log" || exit 1
healthy "the older build, restarted" || status=1
rows_are "older build, compacted, restarted" "$A" "$B" "$C" "$MORE" || status=1
stop || { echo "FAIL: the older build's second stop did not exit 0" >&2; status=1; }

# 4. The current build opens what the older one left.
start "$BIN" "$WORK/new4.log" || exit 1
healthy "the current build, after the older one" || status=1
rows_are "current build, after the older one" "$A" "$B" "$C" "$MORE" || status=1
stop || { echo "FAIL: the current build's last stop did not exit 0" >&2; status=1; }

# A file either build could not read is named with the offset it stopped at.
for log in "$WORK"/old*.log "$WORK"/new4.log; do
  if grep -qE 'offset [0-9]+: ' "$log"; then
    echo "FAIL: $(basename "$log") names bytes it could not read:" >&2
    grep -E 'offset [0-9]+: ' "$log" | head -5 >&2
    status=1
  fi
done

[ $status -eq 0 ] && echo "downgrade to $COMMIT: ok"
exit $status
