#!/usr/bin/env bash
# What goes in comes back out: every row a `MATCH` returns, compared value by value with
# the line that was ingested.
#
# The rule is the data model's, not any one format's: a value returned by a query is the
# JSON value that was ingested - a string character for character, a number as the same
# number, true, false and null as themselves. Typed storage is allowed only where it can
# print the original back. So this ingests a file, pages `MATCH LOG {}` to the end, and
# holds the answer to the input as JSON values, both ways: every ingested line returned
# exactly once, and nothing returned that was not ingested.
#
# The input is generated lines - the hostile profile, every shape the parser must survive,
# and the farm, shaped like real traffic - and testdata/roundtrip.ndjson, written by hand
# to hold every spelling that must come back as it went in: leading zeros, a `+`, "-0", a
# string and a number that parse alike, RFC 3339 at an offset and at "-00:00", a lower-case
# `t` and `z` and a space, an epoch as a number and as a string, "True" and "TRUE", upper-
# and mixed-case UUIDs, IPv6 in each spelling, an integer past i64, 1.50 and 1e3, the edges
# of the nanosecond range, leap seconds, a tenth fraction digit - and the same values mixed
# in one column, where the column widens to text and each row must still come back as
# itself.
#
# Three answers, each compared: the in-memory store, the disk store while its rows are
# still warm, and the disk store after a restart has it read everything back from files
# written by a process that is gone. Any difference fails.
#
# What "the line that was ingested" means, precisely:
# - flattened as the store flattens it: nested objects joined with `.`, a `.` or `\`
#   inside a member name escaped, an empty object contributing no key;
# - an array is stored as its JSON text and returned as that string;
# - numbers compared as the JSON reader kibbr uses reads them: an integer inside i64 or
#   u64 exactly, anything else as the nearest double - so 123456789012345678901234567890
#   is the double it was always read as, the same number to every reader that reads JSON
#   numbers as doubles;
# - `_received_at` is a system column, not ingested, and is ignored - unless the line has
#   a key of that name, which is then compared like any other.
#
#   scripts/roundtrip.sh
#   HOSTILE=5000 FARM=5000 scripts/roundtrip.sh
#   BIN=/path/to/an/older/kibbr scripts/roundtrip.sh

set -uo pipefail
cd "$(dirname "$0")/.."

# BIN names another build to hold to the rule - an older one, to see what it breaks.
BIN=${BIN:-./target/release/kibbr}
HOSTILE=${HOSTILE:-3000}
FARM=${FARM:-3000}
CASES=testdata/roundtrip.ndjson

[ "$BIN" = ./target/release/kibbr ] && { cargo build --release --quiet || exit 1; }
WORK=$(mktemp -d)
MEM_PID=""
DISK_PID=""
cleanup() {
  [ -n "$MEM_PID" ] && kill -TERM "$MEM_PID" 2>/dev/null
  [ -n "$DISK_PID" ] && kill -TERM "$DISK_PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
{
  "$BIN" gen-logs --lines "$HOSTILE" --seed 7
  "$BIN" gen-logs --profile farm --lines "$FARM" --seed 7
  cat "$CASES"
} > "$INPUT"

# Sets PID and PORT in this shell; see store_agree.sh for why not a subshell.
PID=""
PORT=""
start() {  # start <logfile> <flags...>
  local log=$1; shift
  "$BIN" serve --port 0 --flush-interval-ms 100 --row-group-timeout-ms 100 "$@" 2>"$log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 80); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  [ -n "$PORT" ] || { echo "server did not start:" >&2; cat "$log" >&2; return 1; }
}

stop() {  # stop <pid>
  kill -TERM "$1" 2>/dev/null
  wait "$1" 2>/dev/null
}

start "$WORK/mem.log" --storage memory || exit 1
MEM_PID=$PID; MEM_PORT=$PORT
start "$WORK/disk.log" --data-dir "$WORK/data" || exit 1
DISK_PID=$PID; DISK_PORT=$PORT

for port in "$MEM_PORT" "$DISK_PORT"; do
  code=$(curl -s -o "$WORK/ingest.$port.json" -w '%{http_code}' \
         -X POST "http://127.0.0.1:$port/ingest" --data-binary "@$INPUT")
  [ "$code" = "200" ] || { echo "ingest into :$port answered $code" >&2
                           head -c 2000 "$WORK/ingest.$port.json" >&2; exit 1; }
done
# Let the disk server close a row group or two, so the warm answer reads files as well as
# buffers.
sleep 0.5

# Every page of `MATCH LOG {}`, the cursor followed to the end.
page_all() {  # page_all <port> <outfile>
  python3 - "$1" "$2" <<'PY'
import json, sys, urllib.request
port, out = sys.argv[1], sys.argv[2]
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
json.dump(rows, open(out, "w"))
PY
}

page_all "$MEM_PORT" "$WORK/memory.json" || exit 1
page_all "$DISK_PORT" "$WORK/warm.json" || exit 1
stop "$DISK_PID"
start "$WORK/disk2.log" --data-dir "$WORK/data" || exit 1
DISK_PID=$PID; DISK_PORT=$PORT
page_all "$DISK_PORT" "$WORK/restarted.json" || exit 1

python3 - "$INPUT" "$WORK/ingest.$MEM_PORT.json" "$WORK/memory.json" "$WORK/warm.json" \
          "$WORK/restarted.json" <<'PY'
import json, sys
from collections import Counter
from fractions import Fraction

input_path, report_path = sys.argv[1], sys.argv[2]
answers = [("memory", sys.argv[3]), ("disk, warm", sys.argv[4]),
           ("disk, restarted", sys.argv[5])]

def escape(segment):
    return segment.replace("\\", "\\\\").replace(".", "\\.")

def flatten(obj, prefix, out):
    for key, value in obj.items():
        path = prefix + [escape(key)]
        if isinstance(value, dict):
            flatten(value, path, out)
        else:
            out[".".join(path)] = value
    return out

def text_of_array(value):
    # serde_json's compact form: no spaces, non-ASCII as itself.
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

def as_ingested(value):
    if isinstance(value, list):
        return text_of_array(value)
    return value

def canon(value):
    """One comparable, hashable form per JSON value: a string, a bool and null are
    themselves; a number is its exact value as the reader holds it."""
    if value is None or isinstance(value, (bool, str)):
        return (type(value).__name__, value)
    if isinstance(value, int) and not (-2**63 <= value < 2**64):
        value = float(value)
    return ("number", Fraction(value))

def row_key(row):
    return tuple(sorted((k, canon(v)) for k, v in row.items()))

# The lines that went in: every non-blank line, less the ones /ingest refused by number.
report = json.load(open(report_path))
refused = {e["line"] for e in report.get("errors", [])}
raw = open(input_path, encoding="utf-8", errors="surrogateescape").read().split("\n")
if raw and raw[-1] == "":
    raw.pop()
ingested = []
for n, line in enumerate(raw, 1):
    if n in refused or not line.strip():
        continue
    ingested.append(flatten(json.loads(line), [], {}))
if len(ingested) != report["ingested"]:
    sys.exit("/ingest took %d lines and %d are accounted for; the refusals were not all "
             "listed" % (report["ingested"], len(ingested)))
want = Counter(row_key({k: as_ingested(v) for k, v in r.items()}) for r in ingested)

status = 0
for label, path in answers:
    rows = json.load(open(path))
    got = Counter()
    for row in rows:
        # `_received_at` is the system column unless the line had a key of that name, in
        # which case that key is shown instead and is compared like any other.
        if "_received_at" in row and row_key(row) not in want:
            row = {k: v for k, v in row.items() if k != "_received_at"}
        got[row_key(row)] += 1
    missing = want - got
    extra = got - want
    differences = sum(missing.values()) + sum(extra.values())
    print("%-16s %d lines ingested, %d rows returned, %d differences"
          % (label + ":", sum(want.values()), len(rows), differences))
    # Pair each missing line with a returned row of the same keys, and say which values
    # differ - the useful half of a failure.
    spare = list(extra.elements())
    for key in list(missing.elements())[:20]:
        names = [k for k, _ in key]
        twin = next((r for r in spare if [k for k, _ in r] == names), None)
        if twin is None:
            print("  not returned: %s" % dict(key))
            continue
        spare.remove(twin)
        for (k, a), (_, b) in zip(key, twin):
            if a != b:
                print("  %s: ingested %r, returned %r" % (k, a[1], b[1]))
    for key in spare[:20]:
        print("  returned and never ingested: %s" % dict(key))
    if differences:
        status = 1
sys.exit(status)
PY
