#!/usr/bin/env bash
# A differential pair: the live door against the bulk door.
#
# `POST /ingest` and `kibbr load` are two ways into a data directory that share almost no
# write path. The live door buffers rows per template and closes a row group at 8,192 rows
# or on a timeout, one flush tick at a time; the loader parses on its own pool, writes full
# row groups into staging and publishes them in one manifest record. A query must not be
# able to tell which door a row came through. So the same lines go through each door into
# its own data directory, and agree.sh's query set asks both servers: a disagreement is a
# bug in one of the doors, with no hand-written expected answer anywhere.
#
# What this asserts:
#
#   1. Every /ingest body answers 200, and the rows ingested are the lines less the ones
#      rejected and the blank ones.
#   2. `kibbr load`, a client of the server running on its directory, exits 0 with as
#      many rows and as many rejections as the live door had.
#   3. Every query answers the same on both: exists, count, rows, groups, templates_matched,
#      truncated and next_cursor. The cursor is the last row's rank - timestamp, key set,
#      ordinal in its store - so equal cursors mean both pages stopped at the same row,
#      which equal counts cannot say. Few answers pass max_rows and carry one, so each
#      MATCH that returned two rows or more is asked again with a LIMIT of half of them,
#      and that cursor is compared too. A refusal - a grouping past the row budget - is an answer
#      as well, compared text and all. /templates agrees on every template's id, column
#      names and types, time source and row count.
#   4. Both servers stop on SIGTERM with exit 0, and 3 holds again after both restart.
#   5. At least a quarter of the queries returned rows: agreeing on a hundred empty answers
#      checks nothing.
#
# TWO ROUNDS. Round 1 asks while the live door still holds most of its rows in buffers -
# few templates reach 8,192 rows, and the 60-second timeout has not fired - and the loaded
# rows sit in finished files. The load has to be visible to /query without a restart,
# which is also the evidence that it went through the server rather than behind its back.
# Round 2 restarts both, so every answer on both sides comes from files.
#
# Not compared: files_opened, row_groups_read and _pruned, bytes_read, rows_scanned and
# duration. The loader writes full row groups where the live door writes whatever the
# timeout closed, so those differ by design.
#
# ONE SENDER, in file order. A row's position among rows of equal timestamp is the order
# it arrived in, and the loader reads the file in order. Two senders would interleave
# bodies and give the live door an order the file does not have, and rows and cursors would
# then disagree about the harness rather than about either door.
#
# ONE ARRIVAL INSTANT for both doors, to the second. It is every row's `_received_at` and
# the time of a row with none of its own, so two instants would be a disagreement about the
# clock.
#
# The input is agree.sh's generator, flag for flag, cached under testdata/two-doors/, so
# agree.sh pointed at it with INPUT draws from the same lines. The TrieQL is built in python
# and the answers compared there: splitting JSON in shell is how a harness starts lying
# about what it tested.
#
#   NLINES=200000 COMBOS=100 SEED=7 GEN_SEED=1 scripts/two_doors.sh

set -uo pipefail
cd "$(dirname "$0")/.."

BIN=${BIN:-./target/release/kibbr}
NLINES=${NLINES:-200000}
GEN_SEED=${GEN_SEED:-1}
COMBOS=${COMBOS:-100}
SEED=${SEED:-7}
BATCH=2000

# Another build can be checked by naming its binary; the default is built first.
[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1
# Absolute from here on, which agree.sh reads as another build and so does not build again:
# one build per run, and none replacing the binary under servers already running from it.
case "$BIN" in /*) ;; *) BIN=$PWD/${BIN#./} ;; esac

# Written aside and renamed: an interrupted generation left in place would be cached as the
# corpus and every later run would ask about a truncated file.
mkdir -p testdata/two-doors
INPUT=testdata/two-doors/${NLINES}-seed${GEN_SEED}.ndjson
if [ ! -s "$INPUT" ]; then
  "$BIN" gen-logs --lines "$NLINES" --templates 12 --seed "$GEN_SEED" > "$INPUT.tmp" &&
    mv "$INPUT.tmp" "$INPUT" || exit 1
fi

WORK=$(mktemp -d)
LIVE_PID=""
BULK_PID=""
HARVEST_PID=""
# KILL for the servers: nothing they hold is needed once the script is done. TERM for the
# harvest, so agree.sh's own trap removes its scratch directory.
cleanup() {
  [ -n "$HARVEST_PID" ] && kill -TERM "$HARVEST_PID" 2>/dev/null
  for p in $LIVE_PID $BULK_PID; do kill -KILL "$p" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# The query set, drawn while the doors fill: it reads only the file. Printed once and asked
# of both servers in both rounds - agree.sh's term draws are reseeded from the clock, so a
# second harvest would not be the same list.
INPUT=$INPUT PRINT_QUERIES=1 COMBOS=$COMBOS SEED=$SEED GEN_SEED=$GEN_SEED NLINES=$NLINES BIN=$BIN \
  scripts/agree.sh > "$WORK/combos.tsv" 2>"$WORK/harvest.log" &
HARVEST_PID=$!

# Sets PID and PORT in this shell; see store_agree.sh for why not a subshell.
PID=""
PORT=""
start() {  # start <logfile> <data-dir>
  "$BIN" serve --port 0 --data-dir "$2" 2>"$1" &
  PID=$!
  PORT=""
  # Up to 60s: a restart reopens everything the round before wrote.
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$1" >&2; return 1; fi
}

# SIGTERM, then the exit code. The live door's clean stop is what writes its buffers, and
# a restart after a stop that failed would be a crash recovery wearing round 2's name.
CODE=""
stop() {  # stop <pid>; sets CODE
  kill -TERM "$1" 2>/dev/null
  for _ in $(seq 1 3000); do
    kill -0 "$1" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$1" 2>/dev/null; then
    CODE="still running 300s after SIGTERM"
    kill -KILL "$1" 2>/dev/null; wait "$1" 2>/dev/null
    return
  fi
  wait "$1"
  CODE=$?
}

field() {  # field <json file> <key>
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

buffered() {  # buffered <port>
  curl -s "http://127.0.0.1:$1/stats" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["storage"]["buffered_rows"])' 2>/dev/null
}

# The live door's one sender: every body in file order down one keep-alive connection,
# stopping at the first that is not a 200.
cat > "$WORK/send.py" <<'PY'
import http.client, json, sys

port, path, at, batch = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
lines = open(path, "rb").read().split(b"\n")
if lines and lines[-1] == b"":
    lines.pop()

def blank(line):
    # As the server reads one: line ending and BOM stripped, then only whitespace left.
    line = line.rstrip(b"\r")
    if line.startswith(b"\xef\xbb\xbf"):
        line = line[3:]
    try:
        return line.decode("utf-8").strip() == ""
    except UnicodeDecodeError:
        return False

conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=900)
ingested = rejected = 0
for first in range(0, len(lines), batch):
    body = b"\n".join(lines[first:first + batch]) + b"\n"
    conn.request("POST", "/ingest?received_at=" + at, body,
                 {"Content-Type": "application/x-ndjson"})
    response = conn.getresponse()
    text = response.read()
    if response.status != 200:
        print("the body of lines %d-%d answered %d: %r"
              % (first + 1, first + batch, response.status, text[:300]), file=sys.stderr)
        sys.exit(1)
    report = json.loads(text)
    ingested += report["ingested"]
    rejected += len(report["errors"]) + report.get("errors_omitted", 0)
print(json.dumps({"lines": len(lines), "blank": sum(map(blank, lines)),
                  "ingested": ingested, "rejected": rejected}))
PY

# agree.sh's flags, one combination per line, into TrieQL. Only the translation is new:
# the draws are agree.sh's, so a case added there reaches both doors here too.
cat > "$WORK/translate.py" <<'PY'
import json, re, sys

# Bare, `time(5m)` is the computed bucket; quoted, it would be a column of that name.
BUCKET = re.compile(r"time\([0-9]+[smhd]\)$")

def key(text):
    return text if BUCKET.match(text) else json.dumps(text)

def trieql(fields):
    pattern, verb, window, group = [], "COUNT", "", ""
    it = iter(fields)
    for flag in it:
        if flag == "--match":
            # On the first `=`, as the flag parser splits. `k=*` is the wildcard, which
            # TrieQL spells as a bare star; a quoted one is the literal value.
            k, v = next(it).split("=", 1)
            pattern.append("%s: %s" % (json.dumps(k), "*" if v == "*" else json.dumps(v)))
        elif flag in ("--count", "--exists"):
            # The grammar has no verb for `--exists`; COUNT's answer carries `exists` too.
            verb = "COUNT"
        elif flag == "--fetch":
            # No LIMIT, as the flag has none: past max_rows the backstop cuts the answer
            # and hands back a next_cursor.
            verb = "MATCH"
        elif flag == "--group-by":
            group = " GROUP BY " + ", ".join(key(k.strip()) for k in next(it).split(","))
        elif flag == "--between":
            window = " BETWEEN %s AND %s" % (json.dumps(next(it)), json.dumps(next(it)))
        elif flag == "--since":
            window = " SINCE " + next(it)
        else:
            sys.exit("agree.sh printed a flag this translation does not know: %r" % flag)
    return "%s LOG {%s}%s%s" % (verb, ", ".join(pattern), window, group)

for line in sys.stdin:
    line = line.rstrip("\n")
    if line:
        print(trieql(line.split("\t")))
PY

# One round: every query to both servers, then /templates from both.
cat > "$WORK/round.py" <<'PY'
import json, sys, urllib.error, urllib.request

label, live, bulk, path, tally = sys.argv[1:6]
queries = [q for q in open(path).read().splitlines() if q]

def ask(port, method, route, body=None):
    """The answer twice: as python reads it, for the tallies and the printout, and with
    every number kept as the text the server wrote, for the comparison - 1.50 against 1.5,
    or -0 against 0, is a value that did not come back as it went in on one side."""
    req = urllib.request.Request("http://127.0.0.1:%s%s" % (port, route), body,
                                 {"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            text = r.read()
    except urllib.error.HTTPError as e:
        err = {"http_error": e.code, "body": e.read().decode("utf-8", "replace")}
        return err, err
    num = lambda s: {"#number": s}
    return json.loads(text), json.loads(text, parse_int=num, parse_float=num)

FIELDS = ("exists", "count", "rows", "groups")
META = ("templates_matched", "truncated", "next_cursor")

def comparable(a):
    if "http_error" in a:
        return a
    out = {k: a.get(k) for k in FIELDS}
    meta = a.get("meta") or {}
    out.update({k: meta.get(k) for k in META})
    return out

def clip(v):
    s = json.dumps(v, sort_keys=True, ensure_ascii=False)
    return s if len(s) <= 300 else s[:300] + "..."

def explain(a, b):
    """Which fields differ, and for rows and groups the first element that does."""
    pa, pb = comparable(a[0]), comparable(b[0])
    ma, mb = comparable(a[1]), comparable(b[1])
    # The union: one side refusing and the other answering have no field in common.
    for k in sorted(set(ma) | set(mb)):
        x, y = ma.get(k), mb.get(k)
        if x == y:
            continue
        if isinstance(x, list) and isinstance(y, list):
            at = next((i for i, (u, v) in enumerate(zip(x, y)) if u != v), min(len(x), len(y)))
            print("  %-17s live %d, bulk %d; the first difference is [%d]"
                  % (k, len(x), len(y), at))
            print("    live  %s" % (clip(pa[k][at]) if at < len(x) else "(none)"))
            print("    bulk  %s" % (clip(pb[k][at]) if at < len(y) else "(none)"))
        else:
            print("  %-17s live %s" % (k, clip(pa.get(k))))
            print("  %-17s bulk %s" % ("", clip(pb.get(k))))

def differs(q, a, b):
    if comparable(a[1]) == comparable(b[1]):
        return 0
    print("DISAGREE  %s" % q)
    explain(a, b)
    return 1

def both(q):
    body = json.dumps({"q": q}).encode()
    return ask(live, "POST", "/query", body), ask(bulk, "POST", "/query", body)

known = nonempty = refused = cut = halved = bad = 0
for q in queries:
    a, b = both(q)
    # Every query parsed before this round, so an error here is the server's refusal - a
    # grouping past its row budget - and is compared, text and all, like any answer: both
    # doors hold the same rows, so both must refuse with the same numbers.
    if "http_error" in a[0]:
        refused += 1
    elif a[0].get("exists"):
        known += 1
    if a[0].get("count"):
        nonempty += 1
    if (a[0].get("meta") or {}).get("next_cursor"):
        cut += 1
    bad += differs(q, a, b)
    # Few answers pass max_rows, so few carry a cursor, and a cursor is the only place a
    # row's ordinal in its store shows. So every MATCH that returned two rows or more is
    # asked again cut in half, and the middle row's rank is compared as well.
    got = len(a[0].get("rows") or [])
    if q.startswith("MATCH") and got >= 2:
        half = "%s LIMIT %d" % (q, got // 2)
        halved += 1
        bad += differs(half, *both(half))

def templates(port):
    answer = ask(port, "GET", "/templates")[0]
    out = {}
    for t in answer.get("templates", []):
        # Names and types, not their order: no answer depends on the order the columns
        # were first seen in, and a loader that met them in another order is not wrong.
        cols = sorted(([c["name"], c["type"]] for c in t["columns"]), key=lambda c: c[0])
        out[json.dumps(t["path"])] = {"template_id": t["template_id"], "columns": cols,
                                     "time_source": t["time_source"], "rows": t["rows"]}
    return out

tl, tb = templates(live), templates(bulk)
tbad = 0
for p in sorted(set(tl) | set(tb)):
    x, y = tl.get(p), tb.get(p)
    if x == y:
        continue
    tbad += 1
    if tbad > 10:
        continue
    print("TEMPLATE  %s" % p)
    for k in sorted(set(x or {}) | set(y or {})):
        u = (x or {}).get(k, "(no such template)")
        v = (y or {}).get(k, "(no such template)")
        if u != v:
            print("  %-12s live %s" % (k, clip(u)))
            print("  %-12s bulk %s" % ("", clip(v)))
if tbad > 10:
    print("TEMPLATE  ... and %d more" % (tbad - 10))

n = len(queries)
print("%s: %d queries, %d known template, %d unknown, %d refused, %d returned rows, %d cut "
      "at max_rows, %d asked again halved, %d disagreements; %d templates live, %d bulk, "
      "%d differ"
      % (label, n, known, n - known - refused, refused, nonempty, cut, halved, bad,
         len(tl), len(tb), tbad))
failed = False
if bad:
    print("FAIL %s: %d of %d queries disagree, want 0" % (label, bad, n + halved)); failed = True
if tbad:
    print("FAIL %s: %d templates differ between the doors, want 0" % (label, tbad)); failed = True
if not tl:
    print("FAIL %s: /templates listed no template" % label); failed = True
if nonempty * 4 < n:
    print("FAIL %s: %d of %d queries returned rows, want at least a quarter"
          % (label, nonempty, n)); failed = True
json.dump({"known": known, "unknown": n - known - refused, "refused": refused,
           "nonempty": nonempty, "cut": cut, "halved": halved, "templates": len(tl)},
          open(tally, "w"))
# 3, not 1: a traceback also exits 1, and that is a harness that checked nothing.
sys.exit(3 if failed else 0)
PY

# Its FAIL lines are round.py's own; any other exit is the comparison falling over.
round() {  # round <label> <tally file>
  python3 "$WORK/round.py" "$1" "$LIVE_PORT" "$BULK_PORT" "$WORK/queries.txt" "$2"
  case $? in
    0) ;;
    3) FAIL=1 ;;
    *) echo "FAIL $1: the comparison did not finish"; FAIL=1 ;;
  esac
}

FAIL=0
AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

start "$WORK/live.log" "$WORK/live" || exit 1
LIVE_PID=$PID; LIVE_PORT=$PORT
start "$WORK/bulk.log" "$WORK/bulk" || exit 1
BULK_PID=$PID; BULK_PORT=$PORT

# The two doors fill at once; each has its own server and directory. The load finds its
# server through the directory's LOCK.
"$BIN" load --data-dir "$WORK/bulk" --received-at "$AT" --json "$INPUT" \
  > "$WORK/load.json" 2>"$WORK/load.err" &
LOAD_PID=$!
python3 "$WORK/send.py" "$LIVE_PORT" "$INPUT" "$AT" "$BATCH" > "$WORK/sent.json"
SEND_CODE=$?
wait "$LOAD_PID"
LOAD_CODE=$?

if [ "$SEND_CODE" != 0 ]; then echo "FAIL the live door refused a body"; exit 1; fi
LINES=$(field "$WORK/sent.json" lines)
BLANK=$(field "$WORK/sent.json" blank)
INGESTED=$(field "$WORK/sent.json" ingested)
REJECTED=$(field "$WORK/sent.json" rejected)
echo "live door  $INGESTED rows from $LINES lines ($REJECTED rejected, $BLANK blank) at $AT"
[ "$INGESTED" = $((LINES - REJECTED - BLANK)) ] ||
  { echo "FAIL the live door ingested $INGESTED rows, want $((LINES - REJECTED - BLANK))"; FAIL=1; }

if [ "$LOAD_CODE" != 0 ]; then
  echo "FAIL kibbr load exited $LOAD_CODE, want 0"
  tail -20 "$WORK/load.err"
  exit 1
fi
LOADED=$(field "$WORK/load.json" rows)
LOAD_REJECTED=$(field "$WORK/load.json" rejected)
echo "bulk door  $LOADED rows ($LOAD_REJECTED rejected) as load $(field "$WORK/load.json" load_id)"
[ "$LOADED" = "$INGESTED" ] ||
  { echo "FAIL the load wrote $LOADED rows, want the live door's $INGESTED"; FAIL=1; }
[ "$LOAD_REJECTED" = "$REJECTED" ] ||
  { echo "FAIL the load rejected $LOAD_REJECTED lines, want the live door's $REJECTED"; FAIL=1; }

wait "$HARVEST_PID"
HARVEST_CODE=$?
HARVEST_PID=""
if [ "$HARVEST_CODE" != 0 ]; then echo "FAIL agree.sh printed no query set:"; cat "$WORK/harvest.log"; exit 1; fi
echo "queries    $(tail -1 "$WORK/harvest.log")"
python3 "$WORK/translate.py" < "$WORK/combos.tsv" > "$WORK/queries.txt" || exit 1
NQ=$(wc -l < "$WORK/queries.txt" | tr -d ' ')
[ "$NQ" = "$COMBOS" ] || { echo "FAIL agree.sh printed $NQ combinations, want $COMBOS"; exit 1; }
# Parsed before either server is asked: a translation bug would draw the same 400 from
# both, and two identical refusals agree while checking nothing.
while IFS= read -r q; do
  "$BIN" explain "$q" > /dev/null 2> "$WORK/explain.err" ||
    { echo "FAIL the translation wrote a query TrieQL does not parse: $q"
      cat "$WORK/explain.err"; exit 1; }
done < "$WORK/queries.txt"

echo "round 1    the live door holds $(buffered "$LIVE_PORT") of its rows in buffers"
round "round 1 (live door buffered)" "$WORK/tally.1"

stop "$LIVE_PID"; LIVE_PID=""
[ "$CODE" = 0 ] || { echo "FAIL the live server's SIGTERM exit was $CODE, want 0"; FAIL=1; }
stop "$BULK_PID"; BULK_PID=""
[ "$CODE" = 0 ] || { echo "FAIL the bulk server's SIGTERM exit was $CODE, want 0"; FAIL=1; }
start "$WORK/live2.log" "$WORK/live" || exit 1
LIVE_PID=$PID; LIVE_PORT=$PORT
start "$WORK/bulk2.log" "$WORK/bulk" || exit 1
BULK_PID=$PID; BULK_PORT=$PORT

round "round 2 (both restarted)" "$WORK/tally.2"
stop "$LIVE_PID"; LIVE_PID=""
[ "$CODE" = 0 ] || { echo "FAIL the live server's second SIGTERM exit was $CODE, want 0"; FAIL=1; }
stop "$BULK_PID"; BULK_PID=""
[ "$CODE" = 0 ] || { echo "FAIL the bulk server's second SIGTERM exit was $CODE, want 0"; FAIL=1; }

echo
if [ "$FAIL" = 0 ]; then
  echo "PASS two doors agree on $NQ queries x 2 rounds ($(field "$WORK/tally.1" known) known" \
       "template, $(field "$WORK/tally.1" unknown) unknown, $(field "$WORK/tally.1" refused)" \
       "refused, $(field "$WORK/tally.1" nonempty) returned rows, $(field "$WORK/tally.1" cut)" \
       "cut at max_rows, $(field "$WORK/tally.1" halved) asked again halved;" \
       "$(field "$WORK/tally.1" templates) templates; $INGESTED rows through each door)"
  exit 0
fi
exit 1
