#!/usr/bin/env bash
# The second differential pair: the in-memory store against the disk store.
#
# Both sides are the same binary, given the same corpus and the same queries over the same
# HTTP surface - `--storage` is the only difference - so a disagreement is a bug in one of
# the two implementations, with no hand-written expected answer anywhere. This is what the
# in-memory store is kept for: it is the reference the disk store is measured against, not
# dead code.
#
# TWO ROUNDS, and the second is the one that matters. Round 1 asks both servers while the
# disk store still holds its rows in the write buffer it has not flushed; round 2 restarts
# the disk server and asks again, so every answer comes from files reopened by a process
# that did not write them. A store that agrees only while warm has persisted nothing, and
# a single-round version of this script would call that a pass.
#
# The queries are TrieQL rather than flags because storage only exists behind `serve`.
# They are built in python and compared there too: `meta` has to be stripped from each
# answer before the comparison, and splitting a JSON object in shell is how a test harness
# starts lying about what it tested.
#
#   COMBOS=200 SEED=7 NLINES=4000 scripts/store_agree.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=./target/release/triedb
COMBOS=${COMBOS:-120}
SEED=${SEED:-7}
NLINES=${NLINES:-2000}
GEN_SEED=${GEN_SEED:-1}

cargo build --release --quiet || exit 1
WORK=$(mktemp -d)

MEM_PID=""
DISK_PID=""
# TERM rather than KILL: the disk server's graceful exit is what writes trie.json and runs
# the final flush, and round 2 is a lie if the restart it does is really a crash recovery.
cleanup() {
  [ -n "$MEM_PID" ] && kill -TERM "$MEM_PID" 2>/dev/null
  [ -n "$DISK_PID" ] && kill -TERM "$DISK_PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

INPUT="$WORK/input.ndjson"
"$BIN" gen-logs --lines "$NLINES" --templates 12 --seed "$GEN_SEED" > "$INPUT"

# Port 0 and the bound port read back off the banner, as the tests do: a fixed pair of
# ports would collide with a developer's own `triedb serve`.
#
# Sets PID and PORT rather than echoing them, and so must be called in *this* shell. A
# `read < <(start ...)` would put the server in a process-substitution subshell, where it
# is nobody's child here - `wait` then returns immediately and the restart below races the
# shutdown that writes trie.json, which is exactly the thing this script exists to check.
PID=""
PORT=""
start() {  # start <logfile> <flags...>
  local log=$1; shift
  "$BIN" serve --port 0 "$@" 2>"$log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 80); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; cat "$log" >&2; return 1; fi
}

# SIGTERM and then actually wait for the exit. `wait` alone is not enough to conclude the
# snapshot is on disk, so the caller checks for the file.
stop() {  # stop <pid>
  kill -TERM "$1" 2>/dev/null
  wait "$1" 2>/dev/null
  for _ in $(seq 1 100); do
    kill -0 "$1" 2>/dev/null || return 0
    sleep 0.1
  done
  echo "server $1 did not exit on SIGTERM" >&2
  return 1
}

start "$WORK/mem.log" --storage memory || exit 1
MEM_PID=$PID; MEM_PORT=$PORT
start "$WORK/disk.log" --data-dir "$WORK/data" || exit 1
DISK_PID=$PID; DISK_PORT=$PORT

# One arrival time for both servers: a template with no time column stamps its
# rows with the instant the ingest call was given, and two calls a second apart would
# put a different `_received_at` on the same row on each side - a disagreement about the
# clock, not about the store.
RECEIVED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
for port in "$MEM_PORT" "$DISK_PORT"; do
  code=$(curl -s -o "$WORK/ingest.$port" -w '%{http_code}' \
         -X POST "http://127.0.0.1:$port/ingest?received_at=$RECEIVED_AT" --data-binary "@$INPUT")
  if [ "$code" != "200" ]; then
    echo "ingest into :$port answered $code" >&2; cat "$WORK/ingest.$port" >&2; exit 1
  fi
done
echo "input: $NLINES lines, ingested into memory (:$MEM_PORT) and disk (:$DISK_PORT)"

# The queries, written once and used by both rounds. Built from values that are actually
# in the file, so most of them hit something rather than all missing.
cat > "$WORK/build_queries.py" <<'PY'
import json, random, sys

seed, combos = int(sys.argv[1]), int(sys.argv[2])
rng = random.Random(seed)

# Grouped by line rather than pooled, for the reason agree.sh gives: terms drawn from a
# pool almost never co-occur on one template, so a hundred such queries would agree on
# "unknown template" a hundred times and check nothing.
keys, rows = set(), []
for line in sys.stdin:
    vals = json.loads(line)["values"]
    keys.update(vals)
    terms = [(k, v) for k, v in vals.items() if v and len(v) <= 40]
    if terms:
        rows.append(terms)
if not rows:
    sys.exit("no candidate values harvested")
keys = sorted(keys)

WIDTHS = ["5m", "1h", "6h", "1d"]
out = []
for i in range(combos):
    terms = dict(rng.sample(rows[rng.randrange(len(rows))], 1))
    # One value in six is replaced with one nothing holds, and one in six widened to the
    # `*` wildcard: the first reaches the empty answer, the second the present-and-not-null
    # path. Both are answers the two stores could disagree about.
    pattern = {}
    for k, v in terms.items():
        roll = rng.randrange(6)
        pattern[k] = "no_such_value_%d" % i if roll == 0 else ("*" if roll == 1 else v)
    pat = ", ".join('%s: %s' % (json.dumps(k), json.dumps(v)) for k, v in pattern.items())

    verb = rng.choice(["MATCH", "COUNT"])
    q = "%s LOG {%s}" % (verb, pat)

    # Every generated timestamp is from 2024, so a row with its own time is outside a
    # `SINCE`; a row on arrival time is inside it, at the instant both servers were given.
    when = rng.randrange(4)
    if when == 0:
        q += " SINCE %dh" % (1 + rng.randrange(48))
    elif when == 1:
        q += ' BETWEEN "2024-01-01T00:00:00Z" AND "2024-03-01T00:00:00Z"'

    if rng.randrange(3) == 0:
        g = json.dumps(rng.choice(keys))
        # A time bucket in one grouped query in three. Where the boundaries fall is the
        # whole of what the two sides could disagree about here, so the widths span three
        # orders of magnitude and the zero-filled buckets are compared with the rest.
        if rng.randrange(3) == 0:
            g = "time(%s), %s" % (rng.choice(WIDTHS), g)
        q += " GROUP BY %s" % g
    elif verb == "MATCH":
        # Ordering only shows up where a limit cuts the answer, and newest-first across
        # day partitions is the disk store's most reorderable moment.
        q += " LIMIT %d" % rng.choice([1, 5, 50, 100000])

    out.append(q)
print("\n".join(out))
PY
# Written to a file and then piped: the generator reads `parse --json` on stdin, so it
# cannot also be handed a heredoc there.
"$BIN" parse "$INPUT" --json 2>/dev/null \
  | python3 "$WORK/build_queries.py" "$SEED" "$COMBOS" > "$WORK/queries.txt" || exit 1
[ -s "$WORK/queries.txt" ] || { echo "no queries built" >&2; exit 1; }

# One round: every query to both servers, answers compared with `meta` stripped.
round() {  # round <label> <mem_port> <disk_port>
  python3 - "$1" "$2" "$3" "$WORK/queries.txt" <<'PY'
import json, sys, urllib.request

label, mem_port, disk_port, path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

def ask(port, q):
    body = json.dumps({"q": q}).encode()
    req = urllib.request.Request("http://127.0.0.1:%s/query" % port, body,
                                 {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"http_error": e.code, "body": e.read().decode("utf-8", "replace")}

def comparable(a):
    """Everything the two stores must agree on.

    `meta` is dropped except for `templates_matched`: it also carries the duration, the
    files opened and the rows scanned, and those differ between a store that reads a Vec
    and one that opens day partitions - by design, not as a regression."""
    out = {k: a.get(k) for k in ("exists", "count", "rows", "groups", "http_error")}
    out["templates_matched"] = a.get("meta", {}).get("templates_matched")
    return json.dumps(out, sort_keys=True)

queries = [q for q in open(path).read().splitlines() if q.strip()]
bad = known = nonempty = 0
for q in queries:
    m, d = ask(mem_port, q), ask(disk_port, q)
    if m.get("exists"):
        known += 1
    if m.get("count"):
        nonempty += 1
    if comparable(m) == comparable(d):
        continue
    bad += 1
    print("DISAGREE  %s" % q)
    print("  memory  %s" % comparable(m)[:400])
    print("  disk    %s" % comparable(d)[:400])

print("%s: %d queries, %d known template, %d returned rows, %d disagreements"
      % (label, len(queries), known, nonempty, bad))
sys.exit(1 if bad else 0)
PY
}

status=0
round "round 1 (disk warm)" "$MEM_PORT" "$DISK_PORT" || status=1

# The restart. The memory server is left alone - it is the reference, and restarting it
# would empty it.
stop "$DISK_PID" || exit 1
[ -f "$WORK/data/trie.json" ] || { echo "the graceful exit wrote no trie.json:" >&2
                                   cat "$WORK/disk.log" >&2; exit 1; }
start "$WORK/disk2.log" --data-dir "$WORK/data" || exit 1
DISK_PID=$PID; DISK_PORT=$PORT
grep -q "restored" "$WORK/disk2.log" || { echo "the restart restored no templates:" >&2
                                          cat "$WORK/disk2.log" >&2; status=1; }

round "round 2 (disk restarted)" "$MEM_PORT" "$DISK_PORT" || status=1

echo
raw=$(du -sh "$INPUT" | cut -f1)
stored=$(du -sh "$WORK/data" | cut -f1)
echo "raw ndjson $raw, on disk $stored"
[ "$status" -eq 0 ] && echo "stores agree" || echo "STORES DISAGREE"
exit $status
