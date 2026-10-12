#!/usr/bin/env bash
# A differential pair: a live-ingested day before compaction, and the same day after.
#
# The live door writes a day as it arrives: whatever each template's buffer held when the
# row group timeout fired, one small row group after another, over as many files as the
# size cap cut. Compaction reads those files, rebuilds each template's rows into
# full row groups at zstd 9, writes one finished file - or several, at the size cap - and
# publishes it with one `replace` record. Nothing a query can see may move: not a row, not
# a count, not a group, and not a row's position among its template's rows of the day,
# which is what a cursor is made of. So a farm day is ingested live into many small row
# groups in several files, agree.sh's query set is asked, the day is compacted, and the set
# is asked again. Identical, with no hand-written expected answer anywhere.
#
# What this asserts:
#
#   1. Every query answers after compaction what it answered before: exists, count, rows -
#      in their order - groups, templates_matched, truncated and next_cursor. Each MATCH
#      that returned two rows or more is asked again with a LIMIT of half of them, and the
#      cursor that page ends on is followed for the page after it; asked after compaction,
#      that second page is asked with the cursor the day gave out BEFORE it was compacted.
#      A refusal is an answer too, compared text and all. /templates agrees on every
#      template's id, column names and types, time source and row count. At least a
#      quarter of the queries return rows: a hundred empty answers that agree check nothing.
#   2. Three ways of compacting, each held to the one set of answers taken from the live
#      directory before anything was compacted:
#        (a) through the running server - `kibbr compact --day D` while `serve` holds the
#            directory is a client of that server, and exits 0 - asked again on the same
#            server with no restart, and again after a stop and a restart, which orders the
#            day's files by path rather than by when they were published;
#        (b) alone - a copy of the directory no server holds, `kibbr compact --day D` with
#            the small size cap so the day comes out as several files - one file fails the
#            run - then a server on it;
#        (c) by the server's own scheduler - a copy whose data files are dated the day
#            after the day itself, served with `--compact-interval-ms 200`, until /stats
#            storage.compaction.days_compacted says 1. A control copy, not backdated and
#            served the same way, must still be uncompacted three seconds on: its last
#            live row was written today, and a day is due the day AFTER its last live row.
#   3. Queries in flight across the replace: while (a) compacts, a few threads keep asking
#      row-returning queries from the set and COUNT LOG {}. Every answer equals the one
#      from before, and no request fails. A query that started before the replace finishes
#      on the old files and one that starts after reads the new ones. Every COUNT LOG {} is
#      timed and so is the command: at least one must have been in flight while the command
#      ran, and row_groups_read says which side of the replace each of those was on.
#   4. A cursor survives. MATCH LOG {} is paged to the end before compaction, and the
#      cursor half way kept; after compaction the paging resumes from that cursor. The
#      pages before it and the pages read after compaction are the full paging, row for
#      row, and each page ends on the cursor it ended on before.
#   5. Compaction did what it is for, read off manifest.log and the directory, for each of
#      (a) (b) (c): exactly one record was appended, a `replace`; it removes exactly the
#      files that were live and unfinished, and adds files that are sealed and finished,
#      under paths never used before, holding as many rows as the ones removed; every live
#      file of the day is finished; every file but the day's last reached the size cap,
#      and in (b), given a cap the day does not fit under, there are at least two;
#      data/D/*.col is exactly the manifest's live set; nothing is left in staging/; and the day
#      has fewer row groups than before. The command's own report - `--json` in (a), the
#      line in (b), /stats storage.compaction.last in (c) - is held to the same numbers.
#   6. A second `kibbr compact --day D` is a no-op: exit 0, "nothing to compact", and
#      manifest.log byte for byte as it was. So is `kibbr compact` with no `--day` where no
#      day is due - the day just compacted, and the control whose day has not ended.
#   7. Every server stops on SIGTERM with exit 0, and a restart appends nothing to the
#      manifest.
#
# MANY SMALL ROW GROUPS IN SEVERAL FILES, or compaction has nothing to prove. The day goes
# in through `serve --flush-interval-ms 100 --row-group-timeout-ms 100 --segment-max-bytes
# 1048576`: a tenth of a second is all a row group may wait, so 200,000 lines sent over
# five seconds close some 2,200 row groups where the loader would write 219, and the cap
# cuts them into eight files. The script waits for storage.buffered_rows to reach 0 before
# it stops the server, so every row group was closed by the timeout and none by the final
# flush. The counts are printed; fewer than four files, or fewer than two row groups a
# template, fails the run before anything is compared.
#
# Not compared: files_opened, row_groups_read and _pruned, bytes_read, rows_scanned and
# duration. Changing those is what compaction is for; row_groups_read is used only as
# evidence that it did.
#
# ONE CARVE-OUT. `SINCE 13h GROUP BY time(5m)` zero-fills its buckets up to now, so its
# groups say what time it was when it was asked, and the rounds here are a minute apart.
# For a query with both, the empty buckets are left out of the comparison - every farm
# line carries its own 2024 time, so that is all of them - and its exists, count and
# templates_matched are still held.
#
# ONE SENDER, in file order, as in two_doors.sh: a row's position among rows of one
# timestamp is the order it arrived in, and the position is what the cursors are checked on.
#
# The corpus is two_doors.sh's farm day, cached under testdata/two-doors/, and the query set
# is agree.sh's, printed once and asked of every round. Do not run this across midnight UTC:
# "today" is what makes the live directory and the control not due.
#
#   scripts/check/compact_agree.sh
#   NLINES=200000 COMBOS=100 SEED=7 GEN_SEED=1 scripts/check/compact_agree.sh
#   SEGMENT_MAX_BYTES=2097152 PAGES=4 HAMMER=8 scripts/check/compact_agree.sh
#   BIN=$PWD/other/kibbr scripts/check/compact_agree.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=${BIN:-./target/release/kibbr}
NLINES=${NLINES:-200000}
GEN_SEED=${GEN_SEED:-1}
COMBOS=${COMBOS:-100}
SEED=${SEED:-7}
# The size cap the live day is cut at, and the one (b) compacts under.
SEGMENT_MAX_BYTES=${SEGMENT_MAX_BYTES:-1048576}
# How many pages MATCH LOG {} is read in; the cursor kept is the one half way.
PAGES=${PAGES:-10}
# Threads asking the queries across the replace; one more asks COUNT LOG {} alone.
HAMMER=${HAMMER:-4}
BATCH=2000
# The farm's one day (gen-logs --profile farm), and the clock agree.sh draws windows from.
DAY=2024-01-15
CLOCK=(BASE=1705276800 SPAN=86400)

# Another build can be checked by naming its binary; the default is built first.
[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1
# Absolute from here on, which agree.sh reads as another build and so does not build again.
case "$BIN" in /*) ;; *) BIN=$PWD/${BIN#./} ;; esac

# two_doors.sh's file, so a run of either leaves the corpus for the other. Written aside
# and renamed: a generation cut short must not be cached as the corpus.
mkdir -p testdata/two-doors
INPUT=testdata/two-doors/farm-${NLINES}-seed${GEN_SEED}.ndjson
if [ ! -s "$INPUT" ]; then
  "$BIN" gen-logs --profile farm --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT.tmp.$$" \
    && mv "$INPUT.tmp.$$" "$INPUT" || { rm -f "$INPUT.tmp.$$"; exit 1; }
fi

WORK=$(mktemp -d)
SERVERS=""
HARVEST_PID=""
HAMMER_PID=""
# KILL for the servers and the hammer: nothing they hold is needed once the script is done.
# TERM for the harvest, so agree.sh's own trap removes its scratch directory.
cleanup() {
  [ -n "$HARVEST_PID" ] && kill -TERM "$HARVEST_PID" 2>/dev/null
  [ -n "$HAMMER_PID" ] && kill -KILL "$HAMMER_PID" 2>/dev/null
  for p in $SERVERS; do kill -KILL "$p" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# The query set, drawn while the day is ingested: it reads only the file. Printed once and
# asked of every round; it is the set its SEED draws, on every run.
env "${CLOCK[@]}" INPUT=$INPUT PRINT_QUERIES=1 COMBOS=$COMBOS SEED=$SEED GEN_SEED=$GEN_SEED \
  NLINES=$NLINES BIN=$BIN scripts/check/agree.sh > "$WORK/combos.tsv" 2>"$WORK/harvest.log" &
HARVEST_PID=$!

# Sets PID and PORT rather than echoing them, so it must be called in this shell: in a
# subshell the server is not this shell's child, and `wait` could not collect its exit.
PID=""
PORT=""
start() {  # start <logfile> <serve flags...>
  local log=$1; shift
  "$BIN" serve --port 0 "$@" 2>"$log" &
  PID=$!
  SERVERS="$SERVERS $PID"
  PORT=""
  # Up to 60s: a start reopens everything the server before it wrote.
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then echo "server did not start:" >&2; tail -40 "$log" >&2; return 1; fi
}

# SIGTERM, then the exit code: assertion 7. A stop that failed is a server that may not
# have written what it held, and whatever is asked of its directory next is asked of a
# crash recovery.
FAIL=0
CODE=""
STOPS=0
STOPPED=0
stop() {  # stop <pid> <which server, for the FAIL line>; sets CODE
  local left="" p
  STOPS=$((STOPS + 1))
  kill -TERM "$1" 2>/dev/null
  for _ in $(seq 1 3000); do
    kill -0 "$1" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$1" 2>/dev/null; then
    CODE="still running 300s after SIGTERM"
    kill -KILL "$1" 2>/dev/null; wait "$1" 2>/dev/null
  else
    wait "$1"
    CODE=$?
  fi
  for p in $SERVERS; do [ "$p" = "$1" ] || left="$left $p"; done
  SERVERS=$left
  if [ "$CODE" = 0 ]; then
    STOPPED=$((STOPPED + 1))
  else
    echo "FAIL the $2 server's SIGTERM exit was $CODE, want 0"; FAIL=1
  fi
}

field() {  # field <json file> <key>
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

# One value of /stats, as JSON, by its dotted path; nothing when it is not there.
stat_of() {  # stat_of <port> <dotted path>
  curl -s "http://127.0.0.1:$1/stats" \
    | python3 -c 'import json,sys
a = json.load(sys.stdin)
for k in sys.argv[1].split("."): a = a[k]
print(json.dumps(a))' "$2" 2>/dev/null
}

# A python check prints its own FAIL lines and exits 3; any other exit is the check falling
# over, which is a harness that checked nothing and must not read as agreement.
check() {  # check <label> <python file> <arguments...>
  local label=$1; shift
  python3 "$@"
  case $? in
    0) ;;
    3) FAIL=1 ;;
    *) echo "FAIL $label: the check did not finish"; FAIL=1 ;;
  esac
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

# agree.sh's flags, one combination per line, into TrieQL: two_doors.sh's translation, so
# a case added to agree.sh reaches this pair as it reaches that one.
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

# What every asker shares: how a question is put, and what of an answer is compared.
cat > "$WORK/answer.py" <<'PY'
import json, re, urllib.error, urllib.request

def ask(port, method, route, body=None):
    """The answer twice: as python reads it, for the tallies and the printout, and with
    every number kept as the text the server wrote, for the comparison - 1.50 against 1.5,
    or -0 against 0, is a value that did not come back as it was stored."""
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

def query(port, q):
    return ask(port, "POST", "/query", json.dumps({"q": q}).encode())

FIELDS = ("exists", "count", "rows", "groups")
META = ("templates_matched", "truncated", "next_cursor")
ZERO = {"#number": "0"}

QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"')

def clocked(q):
    """A SINCE window bucketed by time. Its buckets are zero-filled up to now, so two
    askings a minute apart differ in the empty ones whatever is stored. With every string
    taken out, what is left of the query is keywords: a key named "SINCE" cannot match."""
    bare = QUOTED.sub('""', q)
    return " SINCE " in bare and "time(" in bare

def comparable(q, a):
    """`a` is the number-as-text reading of an answer."""
    if "http_error" in a:
        return a
    out = {k: a.get(k) for k in FIELDS}
    meta = a.get("meta") or {}
    out.update({k: meta.get(k) for k in META})
    if clocked(q):
        out["groups"] = [g for g in out["groups"] or [] if g.get("count") != ZERO]
    return out

def clip(v):
    s = v if isinstance(v, str) else json.dumps(v, sort_keys=True, ensure_ascii=False)
    return s if len(s) <= 300 else s[:300] + "..."
PY

# One round: the questions, then /templates. With no answers to hold it to, the round is
# the "before": it asks the query set and builds the list every later round is asked. With
# them, it asks exactly that list again - text for text, so a second page is asked with
# the cursor the day gave out before it was compacted - and holds each answer to the old one.
cat > "$WORK/round.py" <<'PY'
import json, sys
from answer import ZERO, ask, clip, comparable, query

label, port, path, answers, tally = sys.argv[1:6]
before = json.load(open(sys.argv[6])) if len(sys.argv) > 6 else None

asked = []
plain = {}

def put(q, kind):
    a = query(port, q)
    asked.append({"q": q, "kind": kind, "a": comparable(q, a[1])})
    plain[q] = a[0]
    return a[0]

if before is None:
    for q in [q for q in open(path).read().splitlines() if q]:
        a = put(q, "query")
        # Few answers pass max_rows, so few carry a cursor, and a cursor is the only place
        # a row's position in its store shows. So every MATCH that returned two rows or
        # more is asked again cut in half, and the page after that cut is asked for too.
        got = len(a.get("rows") or [])
        if q.startswith("MATCH") and got >= 2:
            half = "%s LIMIT %d" % (q, got // 2)
            cursor = (put(half, "halved").get("meta") or {}).get("next_cursor")
            if cursor:
                put("%s AFTER %s" % (half, json.dumps(cursor)), "followed")
    # Not one of the set: the row count, and - from its meta, which is never compared -
    # how many row groups the store holds.
    put("COUNT LOG {}", "all")
else:
    for item in before["answers"]:
        put(item["q"], item["kind"])

def templates():
    out = {}
    for t in ask(port, "GET", "/templates")[0].get("templates", []):
        # Names and types, not their order: no answer depends on the order the columns
        # were first seen in.
        cols = sorted(([c["name"], c["type"]] for c in t["columns"]), key=lambda c: c[0])
        out[json.dumps(t["path"])] = {"template_id": t["template_id"], "columns": cols,
                                     "time_source": t["time_source"], "rows": t["rows"]}
    return out

listed = templates()

def of(kind):
    return [x for x in asked if x["kind"] == kind]

first = of("query")
n = len(first)
refused = sum(1 for x in first if "http_error" in x["a"])
known = sum(1 for x in first if x["a"].get("exists"))
nonempty = sum(1 for x in first if x["a"].get("count") not in (None, ZERO))
cut = sum(1 for x in first if x["a"].get("next_cursor"))
everything = plain["COUNT LOG {}"]
meta = everything.get("meta") or {}
json.dump({"queries": n, "answers": len(asked), "known": known,
           "unknown": n - known - refused, "refused": refused, "nonempty": nonempty,
           "cut": cut, "halved": len(of("halved")), "followed": len(of("followed")),
           "templates": len(listed), "rows": everything.get("count"),
           "row_groups": meta.get("row_groups_read", 0) + meta.get("row_groups_pruned", 0)},
          open(tally, "w"))
json.dump({"answers": asked, "templates": listed}, open(answers, "w"))

failed = False
if before is None:
    print("%-11s  %d queries: %d known template, %d unknown, %d refused, %d returned rows, "
          "%d cut at max_rows, %d asked again halved, %d cursors followed; %d templates"
          % (label, n, known, n - known - refused, refused, nonempty, cut, len(of("halved")),
             len(of("followed")), len(listed)))
    if "http_error" in everything:
        print("FAIL %s: COUNT LOG {} answered %s" % (label, clip(everything))); failed = True
    if not listed:
        print("FAIL %s: /templates listed no template" % label); failed = True
    if nonempty * 4 < n:
        print("FAIL %s: %d of %d queries returned rows, want at least a quarter"
              % (label, nonempty, n)); failed = True
    sys.exit(3 if failed else 0)

def explain(old, new):
    """Which fields differ, and for rows and groups the first element that does."""
    for k in sorted(set(old) | set(new)):
        x, y = old.get(k), new.get(k)
        if x == y:
            continue
        if isinstance(x, list) and isinstance(y, list):
            at = next((i for i, (u, v) in enumerate(zip(x, y)) if u != v), min(len(x), len(y)))
            print("  %-17s before %d, after %d; the first difference is [%d]"
                  % (k, len(x), len(y), at))
            print("    before  %s" % (clip(x[at]) if at < len(x) else "(none)"))
            print("    after   %s" % (clip(y[at]) if at < len(y) else "(none)"))
        else:
            print("  %-17s before %s" % (k, clip(x)))
            print("  %-17s after  %s" % ("", clip(y)))

bad = 0
for old, new in zip(before["answers"], asked):
    if old["a"] == new["a"]:
        continue
    bad += 1
    if bad <= 10:
        print("CHANGED   %s" % new["q"])
        explain(old["a"], new["a"])
if bad > 10:
    print("CHANGED   ... and %d more" % (bad - 10))

was = before["templates"]
tbad = 0
for p in sorted(set(was) | set(listed)):
    x, y = was.get(p), listed.get(p)
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
            print("  %-12s before %s" % (k, clip(u)))
            print("  %-12s after  %s" % ("", clip(v)))
if tbad > 10:
    print("TEMPLATE  ... and %d more" % (tbad - 10))

print("%-11s  %d answers and %d templates asked again: %d answers changed, %d templates"
      % (label, len(asked), len(listed), bad, tbad))
if bad:
    print("FAIL %s: %d of %d answers are not what they were before compaction, want 0"
          % (label, bad, len(asked))); failed = True
if tbad:
    print("FAIL %s: %d templates are not as /templates listed them before compaction, want 0"
          % (label, tbad)); failed = True
# 3, not 1: a traceback also exits 1, and that is a harness that checked nothing.
sys.exit(3 if failed else 0)
PY

# Assertion 4. `full` pages a MATCH to its end and keeps every row, as one line of JSON
# with its keys sorted and its numbers as the server wrote them, and every cursor.
# `resume` starts again from the cursor half way and holds what comes back to those lines.
cat > "$WORK/page.py" <<'PY'
import json, sys
from answer import clip, query

def page(port, q, k, cursor):
    text = "%s LIMIT %d" % (q, k) + (" AFTER %s" % json.dumps(cursor) if cursor else "")
    plain, kept = query(port, text)
    if "http_error" in plain:
        sys.exit("%s answered %s: %s" % (clip(text), plain["http_error"], clip(plain["body"])))
    rows = [json.dumps(r, sort_keys=True) for r in kept.get("rows") or []]
    return rows, (plain.get("meta") or {}).get("next_cursor")

cmd, args = sys.argv[1], sys.argv[2:]

if cmd == "full":  # full <port> <query> <rows a page> <prefix>: prints rows, pages
    port, q, k, prefix = args[0], args[1], int(args[2]), args[3]
    sizes, cursors, cursor = [], [], None
    with open(prefix + ".rows", "w") as out:
        while True:
            rows, cursor = page(port, q, k, cursor)
            out.writelines(r + "\n" for r in rows)
            sizes.append(len(rows))
            cursors.append(cursor)
            if not cursor:
                break
            if len(sizes) > 100000:
                sys.exit("%s LIMIT %d: 100,000 pages and still a next_cursor" % (q, k))
    json.dump({"q": q, "k": k, "sizes": sizes, "cursors": cursors}, open(prefix + ".json", "w"))
    print(sum(sizes), len(sizes))

elif cmd == "resume":  # resume <label> <port> <prefix>
    label, port, prefix = args
    kept = json.load(open(prefix + ".json"))
    q, k, sizes, cursors = kept["q"], kept["k"], kept["sizes"], kept["cursors"]
    half = len(sizes) // 2
    head = sum(sizes[:half])
    with open(prefix + ".rows") as f:
        full = f.read().splitlines()
    want, ends = full[head:], cursors[half:]
    got, pages, wrong_ends, cursor = [], 0, [], cursors[half - 1]
    while True:
        rows, cursor = page(port, q, k, cursor)
        if pages >= len(ends) or cursor != ends[pages]:
            wrong_ends.append(half + pages + 1)
        got.extend(rows)
        pages += 1
        # Two pages past where the paging ended before is a paging that lost its place.
        if not cursor or pages >= len(ends) + 2:
            break
    differ = [i for i, (a, b) in enumerate(zip(want, got)) if a != b]
    print("%-11s  cursor: the %d rows before the cursor kept and %d read from it after "
          "compaction, in %d pages; %d of them are not the row the full paging had there"
          % (label, head, len(got), pages, len(differ) + abs(len(want) - len(got))))
    failed = False
    if differ:
        at = differ[0]
        print("FAIL %s: %d rows after the cursor differ from the paging before compaction, "
              "want 0; the first is row %d of the paging" % (label, len(differ), head + at + 1))
        print("    before  %s" % clip(want[at]))
        print("    after   %s" % clip(got[at]))
        failed = True
    if len(got) != len(want):
        print("FAIL %s: %d rows came after the cursor, want the %d that came after it before "
              "compaction" % (label, len(got), len(want)))
        failed = True
    if wrong_ends:
        print("FAIL %s: page %s ended on another cursor than before compaction, want the same "
              "one on each of %d pages" % (label, ", ".join(map(str, wrong_ends[:10])), len(ends)))
        failed = True
    sys.exit(3 if failed else 0)
PY

# Assertion 3. Each thread asks every query in turn until the stop file appears, and then
# once more through all of them: those last requests started after the compaction command
# had returned, so they are the ones that must read the new files.
#
# A first pass before the command and a last one after it see the old row groups and the
# new whatever happened in between, so neither says a request met the replace. What does is
# the clock: each COUNT LOG {} is kept with the time it was sent and the time its answer
# was read, and the ones counted are those whose interval overlaps the command's.
#
# A pass through the queries can take as long as the compaction does - the answers with rows
# are large, and reading them is most of a pass - and then a thread's one COUNT LOG {} a
# pass may fall either side of the command. So one more thread asks nothing else, back to back.
cat > "$WORK/hammer.py" <<'PY'
import json, os, sys, threading, time
from answer import ZERO, clip, comparable, query

label, port, before, threads, row_groups, ready, stop, window, out = sys.argv[1:10]
threads, row_groups = int(threads), int(row_groups)
asked = json.load(open(before))["answers"]

first = [x for x in asked if x["kind"] == "query" and "http_error" not in x["a"]]
rows = [x for x in first if x["q"].startswith("MATCH") and x["a"].get("rows")][:3]
counts = [x for x in first if not x["q"].startswith("MATCH")
          and x["a"].get("count") not in (None, ZERO)][:3]
everything = [x for x in asked if x["kind"] == "all"]
qs = rows + counts + everything

lock = threading.Lock()
tally = {"requests": 0, "failed": 0, "differ": 0}
said = []
passed = [0]
# Every COUNT LOG {} that answered: when it was sent, when its answer was read, and the row
# groups it saw.
spans = []

def one(x):
    sent = time.time()
    try:
        plain, kept = query(port, x["q"])
    except Exception as e:  # a refused or reset connection is a request that failed
        return "failed", "%s: %s" % (type(e).__name__, e), None
    read = time.time()
    if "http_error" in plain:
        return "failed", "answered %s: %s" % (plain["http_error"], clip(plain["body"])), None
    if comparable(x["q"], kept) != x["a"]:
        got = comparable(x["q"], kept)
        return "differ", "before %s\n      during %s" % (clip(x["a"]), clip(got)), None
    meta = plain.get("meta") or {}
    return "ok", "", (sent, read,
                      meta.get("row_groups_read", 0) + meta.get("row_groups_pruned", 0))

def put(x):
    how, why, seen = one(x)
    with lock:
        tally["requests"] += 1
        if how != "ok":
            tally[how] += 1
            if len(said) < 5:
                said.append("%s\n      %s" % (clip(x["q"]), why))
        elif x["kind"] == "all":
            spans.append(seen)

def worker(i):
    turn = 0
    while True:
        last = os.path.exists(stop)
        for j in range(len(qs)):
            put(qs[(i + j) % len(qs)])
        turn += 1
        if turn == 1:
            with lock:
                passed[0] += 1
                if passed[0] == threads:
                    open(ready, "w").close()
        if last:
            break

def counter():
    while True:
        last = os.path.exists(stop)
        put(everything[0])
        if last:
            break

pool = [threading.Thread(target=worker, args=(i,)) for i in range(threads)]
pool.append(threading.Thread(target=counter))
for t in pool:
    t.start()
for t in pool:
    t.join()

# The command's own interval, written by the script before the stop file appeared.
began, ended = map(float, open(window).read().split())
across = [groups for sent, read, groups in spans if sent <= ended and read >= began]
old = sum(1 for groups in across if groups == row_groups)
tally.update({"queries": len(qs), "threads": threads, "counts": len(spans),
              "across": len(across), "old": old, "new": len(across) - old,
              "seconds": ended - began})
json.dump(tally, open(out, "w"))
print("%-11s  hammer: %d requests, of %d queries on %d threads and of COUNT LOG {} on one "
      "more, %d failed, %d differ; of %d COUNT LOG {}, %d were in flight in the %.2fs the "
      "command ran: %d read the old row groups and %d the new"
      % (label, tally["requests"], len(qs), threads, tally["failed"], tally["differ"],
         len(spans), len(across), ended - began, old, len(across) - old))
for s in said:
    print("    %s" % s)
failed = False
if tally["failed"]:
    print("FAIL %s: %d requests in flight across the replace failed, want 0"
          % (label, tally["failed"])); failed = True
if tally["differ"]:
    print("FAIL %s: %d answers in flight across the replace are not the answer from before, "
          "want 0" % (label, tally["differ"])); failed = True
if not across:
    print("FAIL %s: of %d COUNT LOG {}, none was in flight in the %.2fs the command ran, want "
          "at least 1: nothing was asked across the replace"
          % (label, len(spans), ended - began)); failed = True
sys.exit(3 if failed else 0)
PY

# The command the hammer is held to, with the time it was started and the time it returned,
# both off the clock the hammer reads. Its exit code is the command's.
cat > "$WORK/timed.py" <<'PY'
import subprocess, sys, time

window, command = sys.argv[1], sys.argv[2:]
began = time.time()
code = subprocess.call(command)
ended = time.time()
with open(window, "w") as f:
    f.write("%r %r\n" % (began, ended))
# A command a signal ended has no exit code of its own; 1 is not 0.
sys.exit(code if code >= 0 else 1)
PY

# Assertions 5 and 6: manifest.log, the day's directory, and what the command reported.
cat > "$WORK/files.py" <<'PY'
import hashlib, json, os, re, struct, sys, time

def records(base):
    """Every record in manifest.log, in order, and how many bytes they took.

    Frames are [len u32 LE][crc u32 LE][JSON] from offset 0. The CRC is not rechecked: a
    payload that is not whole JSON ends the walk, which is what a torn tail looks like, and
    the caller holds the bytes walked to the file's length."""
    try:
        data = open(os.path.join(base, "manifest.log"), "rb").read()
    except FileNotFoundError:
        data = b""
    out, at = [], 0
    while at + 8 <= len(data):
        size = struct.unpack_from("<I", data, at)[0]
        payload = data[at + 8:at + 8 + size]
        if size == 0 or len(payload) < size:
            break
        try:
            record = json.loads(payload)
        except ValueError:
            break
        out.append(record)
        at += 8 + size
    return out, at, data

def live(recs):
    """The live files by path, after every record is applied: the last word on a path
    wins, a removed path is gone, and a replace removes before it adds."""
    files = {}
    for r in recs:
        kind = r.get("kind")
        if kind in ("add", "seal"):
            added, removed = r.get("files") or [], []
        elif kind == "remove":
            added, removed = [], r.get("files") or []
        elif kind == "replace":
            added, removed = r.get("add") or [], r.get("remove") or []
        else:
            continue
        for path in removed:
            files.pop(path, None)
        for f in added:
            files[f["path"]] = f
    return files

def named(recs):
    """Every path a record has ever named, live now or not."""
    out = set()
    for r in recs:
        for f in (r.get("files") or []) + (r.get("add") or []) + (r.get("remove") or []):
            out.add(f if isinstance(f, str) else f.get("path"))
    return out

def size_of(base, path):
    try:
        return os.path.getsize(os.path.join(base, "data", path))
    except OSError:
        return 0

def on_disk(base, day):
    """The .col files under data/<day>/, spelled the way the manifest spells them."""
    try:
        names = os.listdir(os.path.join(base, "data", day))
    except OSError:
        names = []
    return set("%s/%s" % (day, n) for n in names if n.endswith(".col"))

def snapshot(base, day):
    recs, walked, data = records(base)
    mine = {p: f for p, f in live(recs).items() if p.startswith(day + "/")}
    return {"records": recs, "walked": walked, "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "live": mine,
            "unfinished": sorted(p for p in mine if not mine[p].get("finished")),
            "sizes": {p: size_of(base, p) for p in mine}}

def commas(n):
    return "{:,}".format(n)

class Held:
    """FAIL lines for one label; what failed is printed as it is found."""
    def __init__(self, label):
        self.label, self.failed = label, False
    def fail(self, text):
        print("FAIL %s: %s" % (self.label, text))
        self.failed = True
    def same(self, what, got, want):
        if got != want or type(got) is not type(want):
            self.fail("%s is %s, want %s" % (what, clip(got), clip(want)))
    def done(self, said=None):
        if said and not self.failed:
            print("%-11s  %s" % (self.label, said))
        sys.exit(3 if self.failed else 0)

def clip(v):
    s = json.dumps(v, sort_keys=True)
    return s if len(s) <= 300 else s[:300] + "..."

def the_replace(before, after):
    appended = after["records"][len(before["records"]):]
    return appended[0] if len(appended) == 1 and appended[0].get("kind") == "replace" else None

def day_report(held, what, d, day, before, after, groups_before, groups_after):
    """One day of a report - `--json`, or /stats storage.compaction.last - against what the
    manifest, the directory and COUNT LOG {} say happened."""
    replace = the_replace(before, after)
    if not isinstance(d, dict) or replace is None:
        if not isinstance(d, dict):
            held.fail("%s is %s, want one day's report" % (what, clip(d)))
        return
    removed = before["unfinished"]
    added = sorted(f["path"] for f in replace.get("add") or [])
    want = [("day", day), ("compacted", True),
            ("rows", sum(before["live"][p].get("rows") or 0 for p in removed)),
            ("files_in", len(removed)), ("files_out", len(added)),
            ("row_groups_in", groups_before), ("row_groups_out", groups_after),
            ("bytes_in", sum(before["sizes"][p] for p in removed)),
            ("bytes_out", sum(after["sizes"].get(p, 0) for p in added))]
    for k, v in want:
        if k not in d:
            held.fail("%s has no `%s`" % (what, k))
        else:
            held.same("`%s` in %s" % (k, what), d[k], v)
    for k, v in (("added", added), ("removed", removed)):
        if not isinstance(d.get(k), list):
            held.fail("`%s` in %s is %s, want a list of paths" % (k, what, clip(d.get(k))))
        else:
            held.same("`%s` in %s, sorted," % (k, what), sorted(d[k]), v)
    s = d.get("seconds")
    if isinstance(s, bool) or not isinstance(s, (int, float)) or s < 0:
        held.fail("`seconds` in %s is %s, want a number of seconds" % (what, clip(s)))

LINE = re.compile(r"compacted (\S+): ([0-9,]+) rows, ([0-9,]+) row groups in ([0-9,]+) "
                  r"file\(s\) \(([0-9,]+) bytes\) -> ([0-9,]+) row groups in ([0-9,]+) "
                  r"file\(s\) \(([0-9,]+) bytes\), in [0-9.]+s")
NOTHING = ("rows", "files_in", "files_out", "row_groups_in", "row_groups_out", "bytes_in",
           "bytes_out")

cmd, args = sys.argv[1], sys.argv[2:]

if cmd == "snap":  # snap <data-dir> <day> <out>
    json.dump(snapshot(args[0], args[1]), open(args[2], "w"))

elif cmd == "count":  # count <snap>: live files of the day, their bytes, rows, unfinished
    s = json.load(open(args[0]))
    print(len(s["live"]), sum(s["sizes"].values()),
          sum(f.get("rows") or 0 for f in s["live"].values()), len(s["unfinished"]))

elif cmd == "evidence":
    # evidence <label> <data-dir> <day> <before snap> <out> <size cap> [several]
    label, base, day, out, cap = args[0], args[1], args[2], args[4], int(args[5])
    several = args[6:] == ["several"]
    before = json.load(open(args[3]))
    held = Held(label)
    # Retired files are unlinked when their last reader lets go, so the directory is given
    # a moment; the manifest is not, since the command had returned.
    for _ in range(100):
        after = snapshot(base, day)
        if on_disk(base, day) == set(after["live"]):
            break
        time.sleep(0.1)
    json.dump(after, open(out, "w"))
    disk, alive = on_disk(base, day), set(after["live"])
    if disk != alive:
        held.fail("data/%s holds %s that the manifest does not name and lacks %s that it does, "
                  "10s on; want exactly its %d live files"
                  % (day, clip(sorted(disk - alive)), clip(sorted(alive - disk)), len(alive)))
    try:
        staged = sorted(os.listdir(os.path.join(base, "staging")))
    except OSError:
        staged = []
    if staged:
        held.fail("staging/ holds %s, want nothing left there" % clip(staged))

    if after["walked"] != after["size"]:
        held.fail("manifest.log is %d bytes and its whole records end at %d, want no torn tail"
                  % (after["size"], after["walked"]))
    data = open(os.path.join(base, "manifest.log"), "rb").read()[:before["size"]]
    if hashlib.sha256(data).hexdigest() != before["sha256"]:
        held.fail("the first %d bytes of manifest.log are not what they were before "
                  "compaction, want records only appended" % before["size"])
    appended = after["records"][len(before["records"]):]
    kinds = [r.get("kind") for r in appended]
    if kinds != ["replace"]:
        held.fail("compaction appended %d records to manifest.log, of kind %s; want exactly "
                  "one, a replace" % (len(appended), kinds))
    replace = the_replace(before, after)
    if replace is not None:
        removed, added = replace.get("remove") or [], replace.get("add") or []
        if sorted(removed) != before["unfinished"]:
            held.fail("the replace removes %s, want exactly the %d files that were live and "
                      "unfinished: %s" % (clip(sorted(removed)), len(before["unfinished"]),
                                          clip(before["unfinished"])))
        if not added:
            held.fail("the replace adds no file")
        for f in added:
            path = f.get("path", "")
            if not path.startswith(day + "/"):
                held.fail("the replace adds %s, want a file of %s" % (path, day))
            if f.get("sealed") is not True or f.get("finished") is not True:
                held.fail("the replace adds %s with sealed %s and finished %s, want both true"
                          % (path, clip(f.get("sealed")), clip(f.get("finished"))))
            if path in named(before["records"]):
                held.fail("the replace adds %s, a path the manifest had named before; want "
                          "one never used" % path)
            if not re.fullmatch(r"[0-9a-f]{32}", f.get("file_id", "")):
                held.fail("the replace adds %s with file_id %s, want 32 hex digits"
                          % (path, clip(f.get("file_id"))))
        rows_in = sum(before["live"][p].get("rows") or 0 for p in before["unfinished"])
        rows_out = sum(f.get("rows") or 0 for f in added)
        if rows_out != rows_in:
            held.fail("the replace adds files of %d rows for files of %d, want as many"
                      % (rows_out, rows_in))
        if str(replace.get("source") or "").startswith("load:"):
            held.fail("the replace's source is %s, want none a load could be mistaken for"
                      % clip(replace.get("source")))
        # One file, or several at the size cap: a file is closed for having reached the cap,
        # so every one but the last is at least that long.
        paths = sorted(f.get("path", "") for f in added)
        short = [p for p in paths[:-1] if after["sizes"].get(p, 0) < cap]
        if short:
            held.fail("%s closed short of the %d-byte size cap with another file after it; "
                      "want one file, or each but the last at the cap" % (clip(short), cap))
        # With one file there is no file but the last, and the line above holds nothing: a
        # cap that was never applied would pass it. So where the cap is one the day cannot
        # fit under, one file fails.
        if several and len(paths) < 2:
            held.fail("the day came out as %d file of %s bytes under a %d-byte size cap, want "
                      "at least 2 files, each but the last at the cap"
                      % (len(paths), commas(sum(after["sizes"].get(p, 0) for p in paths)), cap))
    unfinished = after["unfinished"]
    if unfinished or not after["live"]:
        held.fail("%d of the day's %d live files are not finished, want every one finished: %s"
                  % (len(unfinished), len(after["live"]), clip(unfinished)))
    print("%-11s  files: %d, %d of them unfinished, %s bytes -> %d, %d unfinished, %s bytes; "
          "records appended to manifest.log: %d; .col files in data/%s: %d"
          % (label, len(before["live"]), len(before["unfinished"]),
             commas(sum(before["sizes"].values())), len(after["live"]), len(unfinished),
             commas(sum(after["sizes"].values())), len(appended), day, len(disk)))
    held.done()

elif cmd == "report":
    # report <label> <json|line|stats> <file> <day> <before snap> <after snap>
    #        <row groups before> <row groups after>
    label, form, path, day = args[:4]
    before, after = json.load(open(args[4])), json.load(open(args[5]))
    groups = int(args[6]), int(args[7])
    held = Held(label)
    text = open(path).read()
    if form == "line":
        m = LINE.fullmatch(text.rstrip("\n"))
        if not m or text.count("\n") != 1:
            held.fail("kibbr compact printed %s; want the one line `compacted %s: N rows, N row "
                      "groups in N file(s) (N bytes) -> N row groups in N file(s) (N bytes), in "
                      "N.Ns`" % (clip(text), day))
            held.done()
        n = [int(g.replace(",", "")) for g in m.groups()[1:]]
        d = {"day": m.group(1), "compacted": True, "rows": n[0], "row_groups_in": n[1],
             "files_in": n[2], "bytes_in": n[3], "row_groups_out": n[4], "files_out": n[5],
             "bytes_out": n[6], "seconds": 0.0}
        replace = the_replace(before, after) or {}
        d["added"] = [f["path"] for f in replace.get("add") or []]
        d["removed"] = replace.get("remove") or []
        day_report(held, "the line", d, day, before, after, *groups)
        held.done("report: " + text.rstrip("\n"))
    try:
        got = json.loads(text)
    except ValueError:
        held.fail("%s read as %s, want one JSON object"
                  % ("`kibbr compact --json`" if form == "json" else "/stats storage.compaction",
                     clip(text)))
        held.done()
    if form == "json":
        held.same("the report's `ok`", got.get("ok"), True)
        days = got.get("days")
        if not isinstance(days, list) or len(days) != 1:
            held.fail("the report's `days` is %s, want the one day asked for" % clip(days))
        else:
            day_report(held, "the report", days[0], day, before, after, *groups)
    else:  # /stats storage.compaction, once the scheduler has compacted the day
        held.same("storage.compaction.running", got.get("running"), False)
        held.same("storage.compaction.days_compacted", got.get("days_compacted"), 1)
        held.same("storage.compaction.last_error", got.get("last_error", "(absent)"), None)
        runs = got.get("runs")
        if isinstance(runs, bool) or not isinstance(runs, int) or runs < 1:
            held.fail("storage.compaction.runs is %s, want at least 1" % clip(runs))
        day_report(held, "storage.compaction.last", got.get("last"), day, before, after, *groups)
    held.done("report: %s agrees with the manifest, the directory and COUNT LOG {}"
              % ("--json" if form == "json" else "/stats storage.compaction, days_compacted 1,"))

elif cmd == "nothing":  # nothing <label> <line|json|none|none-json|stats> <file> <day>
    label, form, path, day = args
    held = Held(label)
    text = open(path).read()
    if form == "line":
        if text != "%s: nothing to compact\n" % day:
            held.fail("kibbr compact printed %s, want the one line `%s: nothing to compact`"
                      % (clip(text), day))
    elif form == "none":
        if text != "":
            held.fail("kibbr compact with no day due printed %s, want nothing" % clip(text))
    else:
        try:
            got = json.loads(text)
        except ValueError:
            said = "/stats storage.compaction" if form == "stats" else "`kibbr compact --json`"
            held.fail("%s read as %s, want one JSON object" % (said, clip(text)))
            held.done()
        if form == "stats":  # the control: a day that is not due
            held.same("storage.compaction.days_compacted", got.get("days_compacted"), 0)
            held.same("storage.compaction.last", got.get("last", "(absent)"), None)
            held.same("storage.compaction.last_error", got.get("last_error", "(absent)"), None)
            held.done()
        held.same("the report's `ok`", got.get("ok"), True)
        days = got.get("days")
        if form == "none-json":
            held.same("the report's `days`, with no day due,", days, [])
        elif not isinstance(days, list) or len(days) != 1 or not isinstance(days[0], dict):
            held.fail("the report's `days` is %s, want the one day asked for" % clip(days))
        else:
            d = days[0]
            held.same("the report's `day`", d.get("day"), day)
            held.same("the report's `compacted`", d.get("compacted"), False)
            for k in NOTHING:
                held.same("the report's `%s`" % k, d.get(k), 0)
            for k in ("added", "removed"):
                held.same("the report's `%s`" % k, d.get(k), [])
    held.done()

else:
    sys.exit("files.py: no command %r" % cmd)
PY

# One round of questions; the answers land in answers.<name> and the tallies in tally.<name>.
ROUNDS=0
round() {  # round <label> <port> <name> [name of the round to hold it to]
  [ -z "${4:-}" ] || ROUNDS=$((ROUNDS + 1))
  check "$1" "$WORK/round.py" "$1" "$2" "$WORK/queries.txt" "$WORK/answers.$3" "$WORK/tally.$3" \
    ${4:+"$WORK/answers.$4"}
}

# Assertion 5's last line: the row groups, from COUNT LOG {} as the round just asked it.
fewer() {  # fewer <label> <name>
  local rg
  rg=$(field "$WORK/tally.$2" row_groups 2>/dev/null)
  if [ -z "$rg" ] || [ "$rg" -ge "$RG_BEFORE" ]; then
    echo "FAIL $1: ${rg:-no count of} row groups after compaction, want fewer than the" \
         "$RG_BEFORE before"
    FAIL=1
  fi
}

NOTHINGS=0
# Assertion 6: `kibbr compact` where there is nothing to compact. Exit 0, the output that
# says so, and manifest.log byte for byte as it was.
nothing() {  # nothing <label> <form> <data-dir> <compact flags...>
  local label=$1 form=$2 dir=$3 code
  shift 3
  cp "$dir/manifest.log" "$WORK/manifest.held"
  "$BIN" compact --data-dir "$dir" "$@" > "$WORK/nothing.out" 2> "$WORK/nothing.err"
  code=$?
  if [ "$code" != 0 ]; then
    echo "FAIL $label: \`kibbr compact $*\` with nothing to compact exited $code, want 0"
    head -5 "$WORK/nothing.err" | sed 's/^/    /'
    FAIL=1
  else
    check "$label" "$WORK/files.py" nothing "$label" "$form" "$WORK/nothing.out" "$DAY"
  fi
  cmp -s "$dir/manifest.log" "$WORK/manifest.held" ||
    { echo "FAIL $label: \`kibbr compact $*\` with nothing to compact changed manifest.log," \
           "want it byte for byte as it was"; FAIL=1; }
  NOTHINGS=$((NOTHINGS + 1))
}

# ── The live day ─────────────────────────────────────────────────────────────────────
LIVE="$WORK/live"
AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

start "$WORK/ingest.log" --data-dir "$LIVE" --flush-interval-ms 100 --row-group-timeout-ms 100 \
  --segment-max-bytes "$SEGMENT_MAX_BYTES" || exit 1
python3 "$WORK/send.py" "$PORT" "$INPUT" "$AT" "$BATCH" > "$WORK/sent.json" ||
  { echo "FAIL the live door refused a body"; exit 1; }
LINES=$(field "$WORK/sent.json" lines)
BLANK=$(field "$WORK/sent.json" blank)
INGESTED=$(field "$WORK/sent.json" ingested)
REJECTED=$(field "$WORK/sent.json" rejected)
echo "live day     $INGESTED rows from $LINES lines ($REJECTED rejected, $BLANK blank) at $AT"
[ "$INGESTED" = $((LINES - REJECTED - BLANK)) ] ||
  { echo "FAIL the live door ingested $INGESTED rows, want $((LINES - REJECTED - BLANK))"; FAIL=1; }

# Every row group closed by the timeout, none by the final flush: the day a server that ran
# all day would have left.
LEFT=""
for _ in $(seq 1 600); do
  LEFT=$(stat_of "$PORT" storage.buffered_rows)
  [ "$LEFT" = 0 ] && break
  sleep 0.1
done
[ "$LEFT" = 0 ] ||
  { echo "FAIL ${LEFT:-an unknown number of} rows were still buffered 60s after the last body," \
         "want 0"; exit 1; }
stop "$PID" ingest
[ "$CODE" = 0 ] || exit 1

# The day as the live door left it, three more times, while no server holds it.
python3 "$WORK/files.py" snap "$LIVE" "$DAY" "$WORK/snap.before" || exit 1
cp "$LIVE/manifest.log" "$WORK/manifest.live"
for copy in alone sched control; do
  cp -R "$LIVE" "$WORK/$copy" || exit 1
done
COUNTED=$(python3 "$WORK/files.py" count "$WORK/snap.before") || exit 1
read -r F_BEFORE B_BEFORE R_BEFORE U_BEFORE <<< "$COUNTED"

wait "$HARVEST_PID"
HARVEST_CODE=$?
HARVEST_PID=""
if [ "$HARVEST_CODE" != 0 ]; then
  echo "FAIL agree.sh printed no query set:"; cat "$WORK/harvest.log"; exit 1
fi
python3 "$WORK/translate.py" < "$WORK/combos.tsv" > "$WORK/queries.txt" || exit 1
NQ=$(wc -l < "$WORK/queries.txt" | tr -d ' ')
[ "$NQ" = "$COMBOS" ] || { echo "FAIL agree.sh printed $NQ combinations, want $COMBOS"; exit 1; }
# Parsed before any server is asked: a translation bug would draw the same 400 before and
# after, and two identical refusals agree while checking nothing.
while IFS= read -r q; do
  "$BIN" explain "$q" > /dev/null 2> "$WORK/explain.err" ||
    { echo "FAIL the translation wrote a query TrieQL does not parse: $q"
      cat "$WORK/explain.err"; exit 1; }
done < "$WORK/queries.txt"

# ── Before ───────────────────────────────────────────────────────────────────────────
# The server (a) compacts through. No flag of its own: the day's files were written today,
# so its scheduler does not find the day due, and what compacts it is the command.
start "$WORK/a.log" --data-dir "$LIVE" || exit 1
A_PID=$PID; A_PORT=$PORT
CAP=$(stat_of "$A_PORT" storage.segment_max_bytes)

round "before" "$A_PORT" before > "$WORK/before.out"
[ -s "$WORK/tally.before" ] || { cat "$WORK/before.out"; exit 1; }
RG_BEFORE=$(field "$WORK/tally.before" row_groups)
ROWS=$(field "$WORK/tally.before" rows)
TEMPLATES=$(field "$WORK/tally.before" templates)
echo "             $F_BEFORE files, $RG_BEFORE row groups, $B_BEFORE bytes, cut at" \
     "--segment-max-bytes $SEGMENT_MAX_BYTES"
echo "queries      $(tail -1 "$WORK/harvest.log")"
cat "$WORK/before.out"
[ "$ROWS" = "$INGESTED" ] ||
  { echo "FAIL COUNT LOG {} on the live day is $ROWS, want the $INGESTED rows ingested"; FAIL=1; }
[ "$R_BEFORE" = "$INGESTED" ] ||
  { echo "FAIL the manifest's live files of $DAY hold $R_BEFORE rows, want the $INGESTED" \
         "ingested"; FAIL=1; }
[ "$U_BEFORE" = "$F_BEFORE" ] ||
  { echo "FAIL $U_BEFORE of the live day's $F_BEFORE files are unfinished, want all of them"
    FAIL=1; }
[ "$F_BEFORE" -ge 4 ] ||
  { echo "FAIL the live day is $F_BEFORE files, want at least 4: lower SEGMENT_MAX_BYTES"; FAIL=1; }
[ "$RG_BEFORE" -ge $((2 * TEMPLATES)) ] ||
  { echo "FAIL the live day is $RG_BEFORE row groups over $TEMPLATES templates, want at least" \
         "two a template: raise NLINES"; FAIL=1; }
# A day that is not many small row groups in several files, or a query set that mostly
# misses, leaves nothing for compaction to be held to.
[ "$FAIL" = 0 ] || exit 1

# The paging, before: about PAGES pages of every row there is.
PAGE_Q='MATCH LOG {}'
K=$(((ROWS + PAGES - 1) / PAGES))
PAGED=$(python3 "$WORK/page.py" full "$A_PORT" "$PAGE_Q" "$K" "$WORK/paging") ||
  { echo "FAIL the paging of $PAGE_Q LIMIT $K before compaction did not finish"; exit 1; }
read -r PAGED_ROWS PAGED_PAGES <<< "$PAGED"
echo "cursor       $PAGE_Q LIMIT $K: $PAGED_ROWS rows in $PAGED_PAGES pages; kept, the cursor" \
     "page $((PAGED_PAGES / 2)) ended on"
[ "$PAGED_ROWS" = "$ROWS" ] ||
  { echo "FAIL the paging returned $PAGED_ROWS rows, want COUNT LOG {}'s $ROWS"; exit 1; }
[ "$PAGED_PAGES" -ge 2 ] ||
  { echo "FAIL the paging was $PAGED_PAGES page, want at least 2 for a cursor to resume from:" \
         "raise PAGES"; exit 1; }

# ── (a) Through the running server ───────────────────────────────────────────────────
L="(a) server"
python3 "$WORK/hammer.py" "$L" "$A_PORT" "$WORK/answers.before" "$HAMMER" "$RG_BEFORE" \
  "$WORK/hammer.ready" "$WORK/hammer.stop" "$WORK/hammer.window" "$WORK/hammer.json" \
  > "$WORK/hammer.out" 2>&1 &
HAMMER_PID=$!
# Every thread has been through every query once before the compaction starts.
for _ in $(seq 1 1200); do
  [ -e "$WORK/hammer.ready" ] && break
  kill -0 "$HAMMER_PID" 2>/dev/null || break
  sleep 0.1
done
[ -e "$WORK/hammer.ready" ] ||
  { echo "FAIL the hammer had not asked each of its queries once in 120s:"
    cat "$WORK/hammer.out"; exit 1; }

python3 "$WORK/timed.py" "$WORK/hammer.window" \
  "$BIN" compact --day "$DAY" --data-dir "$LIVE" --json > "$WORK/a.report" 2> "$WORK/a.err"
COMPACT_CODE=$?
if [ "$COMPACT_CODE" != 0 ]; then
  echo "FAIL $L: \`kibbr compact --day $DAY --json\`, a client of the server holding the" \
       "directory, exited $COMPACT_CODE, want 0"
  head -5 "$WORK/a.err" | sed 's/^/    /'
  exit 1
fi
touch "$WORK/hammer.stop"
wait "$HAMMER_PID"
HAMMER_CODE=$?
HAMMER_PID=""
cat "$WORK/hammer.out"
case "$HAMMER_CODE" in
  0) ;;
  3) FAIL=1 ;;
  *) echo "FAIL $L: the hammer did not finish"; FAIL=1 ;;
esac

check "$L" "$WORK/files.py" evidence "$L" "$LIVE" "$DAY" "$WORK/snap.before" "$WORK/snap.a" "$CAP"
round "$L" "$A_PORT" a before
fewer "$L" a
check "$L" "$WORK/page.py" resume "$L" "$A_PORT" "$WORK/paging"
check "$L" "$WORK/files.py" report "$L" json "$WORK/a.report" "$DAY" "$WORK/snap.before" \
  "$WORK/snap.a" "$RG_BEFORE" "$(field "$WORK/tally.a" row_groups)"
nothing "$L" line "$LIVE" --day "$DAY"
nothing "$L" none "$LIVE"

# Stopped and started: the day's files are now found in path order, not publish order, and
# every position - so every cursor - has to come out the same.
cp "$LIVE/manifest.log" "$WORK/manifest.a"
stop "$A_PID" "(a)"
L="(a) restart"
start "$WORK/a2.log" --data-dir "$LIVE" || exit 1
A_PID=$PID; A_PORT=$PORT
round "$L" "$A_PORT" a2 before
fewer "$L" a2
check "$L" "$WORK/page.py" resume "$L" "$A_PORT" "$WORK/paging"
[ "$(field "$WORK/tally.a2" row_groups)" = "$(field "$WORK/tally.a" row_groups)" ] ||
  { echo "FAIL $L: $(field "$WORK/tally.a2" row_groups) row groups after the restart, want the" \
         "$(field "$WORK/tally.a" row_groups) before it"; FAIL=1; }
stop "$A_PID" "restarted (a)"
cmp -s "$LIVE/manifest.log" "$WORK/manifest.a" ||
  { echo "FAIL $L: a stop and a restart of the compacted directory changed manifest.log, want" \
         "it byte for byte as it was"; FAIL=1; }

# ── (b) Alone ────────────────────────────────────────────────────────────────────────
# No server holds the copy, so the command takes the directory and compacts it itself -
# under the cap the live day was cut at, so the day is several finished files and the
# restart has their order to get right as well.
L="(b) alone"
"$BIN" compact --day "$DAY" --data-dir "$WORK/alone" --segment-max-bytes "$SEGMENT_MAX_BYTES" \
  > "$WORK/b.report" 2> "$WORK/b.err"
COMPACT_CODE=$?
if [ "$COMPACT_CODE" != 0 ]; then
  echo "FAIL $L: \`kibbr compact --day $DAY\` with no server on the directory exited" \
       "$COMPACT_CODE, want 0"
  head -5 "$WORK/b.err" | sed 's/^/    /'
  exit 1
fi
# Read before any server starts: a start deletes what the manifest does not name, and the
# command has to have left the directory right by itself.
check "$L" "$WORK/files.py" evidence "$L" "$WORK/alone" "$DAY" "$WORK/snap.before" \
  "$WORK/snap.b" "$SEGMENT_MAX_BYTES" several
nothing "$L" json "$WORK/alone" --day "$DAY" --json
start "$WORK/b.log" --data-dir "$WORK/alone" || exit 1
round "$L" "$PORT" b before
fewer "$L" b
check "$L" "$WORK/page.py" resume "$L" "$PORT" "$WORK/paging"
check "$L" "$WORK/files.py" report "$L" line "$WORK/b.report" "$DAY" "$WORK/snap.before" \
  "$WORK/snap.b" "$RG_BEFORE" "$(field "$WORK/tally.b" row_groups)"
stop "$PID" "(b)"

# ── (c) By the scheduler ─────────────────────────────────────────────────────────────
# A day is due the day after its last live row, and a start takes that from the newest of
# the day's unfinished files. So one copy's files are dated the midnight the day ended, and
# the other's are left as written - today.
L="(c) sched"
STAMP=$(python3 -c 'import datetime, sys
after = datetime.date.fromisoformat(sys.argv[1]) + datetime.timedelta(days=1)
print(after.strftime("%Y%m%d0000"))' "$DAY")
TZ=UTC touch -t "$STAMP" "$WORK/sched/data/$DAY"/*.col || exit 1

TODAY=$(date -u +%Y-%m-%d)
start "$WORK/control.log" --data-dir "$WORK/control" --compact-interval-ms 200 || exit 1
CONTROL_PID=$PID; CONTROL_PORT=$PORT; CONTROL_AT=$SECONDS
start "$WORK/c.log" --data-dir "$WORK/sched" --compact-interval-ms 200 || exit 1
SCHED_PID=$PID; SCHED_PORT=$PORT
[ -n "$(stat_of "$SCHED_PORT" storage.compaction.days_compacted)" ] ||
  { echo "FAIL $L: /stats has no storage.compaction.days_compacted"; exit 1; }

DONE=""
for _ in $(seq 1 1200); do
  DONE=$(stat_of "$SCHED_PORT" storage.compaction.days_compacted)
  case "$DONE" in ''|0) ;; *) break ;; esac
  kill -0 "$SCHED_PID" 2>/dev/null || break
  sleep 0.1
done
case "$DONE" in
  ''|0) echo "FAIL $L: storage.compaction.days_compacted is ${DONE:-unreadable} 120s after a" \
             "start with --compact-interval-ms 200 on a day that ended, want 1"
        grep -hE 'ERROR|panic|compact' "$WORK/c.log" 2>/dev/null | tail -5 | sed 's/^/    /'
        exit 1 ;;
esac
for _ in $(seq 1 100); do
  [ "$(stat_of "$SCHED_PORT" storage.compaction.running)" = false ] && break
  sleep 0.1
done

check "$L" "$WORK/files.py" evidence "$L" "$WORK/sched" "$DAY" "$WORK/snap.before" \
  "$WORK/snap.c" "$(stat_of "$SCHED_PORT" storage.segment_max_bytes)"
round "$L" "$SCHED_PORT" c before
fewer "$L" c
check "$L" "$WORK/page.py" resume "$L" "$SCHED_PORT" "$WORK/paging"
# Read last: the scheduler has come round a few hundred times since, and a finished day
# must not have been compacted twice.
stat_of "$SCHED_PORT" storage.compaction > "$WORK/c.report"
check "$L" "$WORK/files.py" report "$L" stats "$WORK/c.report" "$DAY" "$WORK/snap.before" \
  "$WORK/snap.c" "$RG_BEFORE" "$(field "$WORK/tally.c" row_groups)"
nothing "$L" none "$WORK/sched"

# The control: the same server, flags and scheduler, on a day whose last live row was today.
L="control"
while [ $((SECONDS - CONTROL_AT)) -lt 4 ]; do sleep 0.2; done
CONTROL="not due, and not compacted"
if [ "$(date -u +%Y-%m-%d)" != "$TODAY" ]; then
  # Midnight UTC passed while it ran, and the day became due. Nothing to hold it to.
  CONTROL="NOT CHECKED: midnight UTC passed during the run"
  echo "$L      $CONTROL"
else
  stat_of "$CONTROL_PORT" storage.compaction > "$WORK/control.report"
  check "$L" "$WORK/files.py" nothing "$L" stats "$WORK/control.report" "$DAY"
  nothing "$L" none-json "$WORK/control" --json
  cmp -s "$WORK/control/manifest.log" "$WORK/manifest.live" ||
    { echo "FAIL $L: manifest.log changed under --compact-interval-ms 200 on a day whose last" \
           "live row was today, want it byte for byte as the live door left it"; FAIL=1; }
  round "$L" "$CONTROL_PORT" control before
  [ "$(field "$WORK/tally.control" row_groups)" = "$RG_BEFORE" ] ||
    { echo "FAIL $L: $(field "$WORK/tally.control" row_groups) row groups, want the $RG_BEFORE" \
           "the live door wrote"; FAIL=1; }
fi
python3 "$WORK/files.py" snap "$WORK/control" "$DAY" "$WORK/snap.control" || FAIL=1
stop "$SCHED_PID" "(c)"
stop "$CONTROL_PID" "control"

# ── The summary ──────────────────────────────────────────────────────────────────────
echo
printf '%-11s  %5s  %10s  %12s\n' "" files "row groups" bytes
printf '%-11s  %5s  %10s  %12s\n' before "$F_BEFORE" "$RG_BEFORE" "$B_BEFORE"
for row in "(a) server:a" "(b) alone:b" "(c) sched:c" "control:control"; do
  label=${row%%:*}; name=${row#*:}
  read -r f b _ _ <<< "$(python3 "$WORK/files.py" count "$WORK/snap.$name" 2>/dev/null)"
  printf '%-11s  %5s  %10s  %12s\n' "$label" "${f:--}" \
    "$(field "$WORK/tally.$name" row_groups 2>/dev/null || echo -)" "${b:--}"
done
ANSWERS=$(field "$WORK/tally.before" answers)
echo "answers      $ANSWERS a round - $NQ queries, $(field "$WORK/tally.before" halved) halved," \
     "$(field "$WORK/tally.before" followed) cursors followed, COUNT LOG {} - asked before, and" \
     "again in $ROUNDS rounds: (a), (a) restarted, (b), (c), the control"
echo "hammer       $(field "$WORK/hammer.json" requests) requests," \
     "$(field "$WORK/hammer.json" across) COUNT LOG {} in flight while (a)'s command ran:" \
     "$(field "$WORK/hammer.json" old) on the old files," \
     "$(field "$WORK/hammer.json" new) on the new"
echo "pages        $PAGED_ROWS rows in $PAGED_PAGES pages of $K before; resumed from the cursor" \
     "after page $((PAGED_PAGES / 2)) on (a), (a) restarted, (b) and (c)"
echo "no-ops       $NOTHINGS more \`kibbr compact\` with nothing to compact: with --day on (a)" \
     "and (b), with none on (a), (c) and the control"
echo "control      $CONTROL"
echo "stops        $STOPPED of $STOPS servers exited 0 on SIGTERM"

echo
if [ "$FAIL" = 0 ]; then
  echo "PASS a live day of $INGESTED rows answers the same compacted three ways as it did before:" \
       "$ANSWERS answers x $ROUNDS rounds ($(field "$WORK/tally.before" nonempty) of $NQ queries" \
       "returned rows, $(field "$WORK/tally.before" cut) cut at max_rows), $TEMPLATES templates," \
       "$RG_BEFORE row groups in $F_BEFORE files -> $(field "$WORK/tally.a" row_groups) in" \
       "$(python3 "$WORK/files.py" count "$WORK/snap.a" | cut -d' ' -f1);" \
       "a cursor from before resumed on each"
  exit 0
fi
exit 1
