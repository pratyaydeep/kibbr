#!/usr/bin/env bash
# A compaction killed with kill -9 at any moment leaves every row stored exactly once.
#
# `kibbr compact` reads a day's small row groups out of the files the live door left,
# rebuilds them into full ones in new files, and swaps the two sets with one `replace`
# record in manifest.log. Until that record is durable the new files are files nobody
# named, and the next start deletes them; from then on the old ones are. A kill can land
# while the new files are half written, between the last of them and the record, or after
# the record and before the old files are gone - and at each of those both sets, or parts
# of both, are on disk. A start that kept both would answer every row twice; one that
# kept neither, or the new set half named, would lose a day. COUNT cannot tell a day
# stored once from one that is short here and doubled there, so this compares the rows.
#
# The day is built once: a farm day sent through /ingest to a server that closes a row
# group every 100 ms and seals a file every LIVE_SEGMENT_BYTES, stopped cleanly once
# every row is on disk. Every round works on a copy of that directory. Its answers and
# its DUMP - every row `MATCH LOG {}` returns, as a multiset - are the reference, and the
# wall time T of one uninterrupted `kibbr compact --day` on a copy sets the kill window.
#
# Then ROUNDS times, in two modes that alternate:
#
#   alone    no server holds the copy: `kibbr compact` takes the directory itself, and
#            the kill is of that process.
#   server   a server holds the copy and `kibbr compact` is its client: the kill is of
#            the SERVER, and the client must then exit non-zero unless it had its answer.
#
# and, within each mode, two ways of timing the kill that alternate:
#
#   uniform  after a delay drawn uniform in [0, 1.1 T].
#   window   a watcher polling in a tight loop kills on what the commit looks like from
#            outside: the moment manifest.log holds a `replace` record ("record"), or a
#            drawn 0-100% of T after a .col file the day did not have appears in its
#            directory ("file"). Each mode's window rounds are split between the two,
#            in a seeded order. A watcher that sees neither kills at 1.5 T ("timeout").
#
# What this asserts:
#
#   1. The live day is what compaction is for: NLINES rows in at least four files and at
#      least two row groups a template, none of it finished - and its dump is the input,
#      line for line, as roundtrip.sh compares them.
#   2. One uninterrupted `kibbr compact --day`, alone, exits 0; its report names the files
#      it read and wrote and counts the rows; the day is then "new" (see 5), in at least
#      two files, with the reference's battery and dump. Asked again - alone, and through
#      a server - it says "nothing to compact", exits 0, and leaves manifest.log byte for
#      byte as it was.
#   3. After every kill the server starts on the directory.
#   4. That start leaves no file under staging/, and data/<day>/*.col is exactly the set
#      of files the manifest names: nothing unnamed kept, nothing named missing.
#   5. The day is WHOLE, read off the manifest: "old" - no `replace` record, the live
#      files are the day's own, by path and file id, none finished - or "new" - exactly
#      one `replace` record, none of the day's own files live, every live file sealed and
#      finished, their row counts adding up. Anything else is partial.
#   6. The battery - COUNT, by the hour, by one key's values - equals the reference.
#   7. The dump equals the reference multiset: every row once, none twice, none missing,
#      each value as the server wrote it.
#   8. `kibbr compact --day` again, through that server, exits 0. After "old" it compacts,
#      its report names the day's own files as removed and the live ones as added, and
#      /stats storage.compaction.days_compacted is 1; after "new" it is the no-op,
#      days_compacted is 0 and manifest.log does not change. Either way the day is then
#      "new" with every file it names on disk, 6 and 7 hold again, and the server stops
#      on SIGTERM with exit 0.
#
# A `kibbr compact` that exits 0 has the whole compaction behind it - it finished before
# the kill, or its server was killed before it had asked and it then took the directory
# and compacted alone. That round is "finished" and the day must be "new".
#
# Old or new after the restart does not say the start had anything to do: a kill before
# the first new file is written leaves the day old, and one after the last old file is
# deleted leaves it new, and each leaves one set on disk. So what is in data/<day>/ is
# counted after every kill, once the killed process is collected and before the restart:
# "in" the day's own files still there, "out" the .col files the day did not have. The
# run fails if every round was finished, and - from 8 rounds up - unless at least one
# kill left the day old with an out file on disk, which no record names, and at least one
# left it new with an in file on disk, which the record replaced: kills that give the
# start nothing to delete, or reach only one side of the record, have not tested the
# commit.
#
# The schedule comes from a seeded generator and each kill's time is printed, so a failing
# round replays with the same SEED and T_MS pinned to the printed T. The scheduler is
# switched off (KIBBR__STORAGE__COMPACT_INTERVAL_MS=0): every compaction here is one this
# script asked for.
#
# NLINES is 100,000, half of load_kill.sh's: a compaction of it still runs for seconds,
# and each round reads the whole day back twice, which at 200,000 rows is most of the run.
# The dump is one unlimited answer (serve --max-rows 0); MAX_ROWS=50000 pages it instead,
# following the cursor. An answer that arrives cut short - 50MB over loopback on a busy
# machine sometimes does - is asked again, and the run says how many were.
# COMPACT_SEGMENT_BYTES is the size cap compaction writes under, small enough that the
# day comes out as several files and the one record has to name them all.
#
#   NLINES=100000 ROUNDS=20 SEED=1 scripts/fault/compact_kill.sh
#   SEED=1 T_MS=2000 ROUNDS=20 scripts/fault/compact_kill.sh
#   NLINES=200000 MAX_ROWS=50000 scripts/fault/compact_kill.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=${BIN:-./target/release/kibbr}
NLINES=${NLINES:-100000}
GEN_SEED=${GEN_SEED:-42}
ROUNDS=${ROUNDS:-20}
SEED=${SEED:-1}
T_MS=${T_MS:-}
LIVE_SEGMENT_BYTES=${LIVE_SEGMENT_BYTES:-524288}
COMPACT_SEGMENT_BYTES=${COMPACT_SEGMENT_BYTES:-1048576}
MAX_ROWS=${MAX_ROWS:-0}
BATCH=2000
# gen-logs --profile farm writes this one UTC day.
DAY=2024-01-15

export KIBBR__STORAGE__COMPACT_INTERVAL_MS=0

# Another build can be tested by naming its binary; the default is built first.
[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1
case "$BIN" in /*) ;; *) BIN=$PWD/${BIN#./} ;; esac

# Written under a temporary name and renamed: a generation cut short must not leave a
# truncated file that later runs trust.
mkdir -p testdata/compact-kill
INPUT=testdata/compact-kill/farm-${NLINES}-seed${GEN_SEED}.ndjson
if [ ! -s "$INPUT" ]; then
  "$BIN" gen-logs --profile farm --lines "$NLINES" --seed "$GEN_SEED" > "$INPUT.tmp.$$" \
    && mv "$INPUT.tmp.$$" "$INPUT" || { rm -f "$INPUT.tmp.$$"; exit 1; }
fi

WORK=$(mktemp -d)
PID=""
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

# The live door's one sender, as two_doors.sh has it: every body in file order down one
# keep-alive connection, stopping at the first that is not a 200.
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

# Everything that reads JSON, the manifest or a clock. A command that compared and found
# a difference prints it and exits 3; any other non-zero exit is a traceback, which is a
# harness that checked nothing and is reported as that.
cat > "$WORK/help.py" <<'PY'
import hashlib, http.client, json, os, random, signal, struct, subprocess, sys, time
import urllib.error, urllib.request
from collections import Counter

def post(port, q, timeout):
    """The answer's bytes. An answer cut short in transit - the connection closed before
    the length it announced - is asked again, and noted: a 50MB body over loopback on a
    busy machine does not always arrive, and that says nothing about what is stored. A
    refusal, an error status or a server that is not there is not asked again."""
    req = urllib.request.Request("http://127.0.0.1:%s/query" % port,
                                 json.dumps({"q": q}).encode(),
                                 {"Content-Type": "application/json"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (http.client.IncompleteRead, ConnectionResetError) as e:
            if attempt == 3:
                raise
            with open(os.environ["CUTS"], "a") as f:
                f.write("%s: %s\n" % (q[:40], str(e)[:80]))

def ask(port, q):
    try:
        return json.loads(post(port, q, 120))
    except urllib.error.HTTPError as e:
        return {"http_error": e.code}
    except Exception as e:
        return {"http_error": str(e)}

def comparable(a):
    """What a compaction, or a kill in the middle of one, must not move: the count, every
    group, and which templates answered. How many files and row groups were read is what
    compaction is there to change, and is left out."""
    return {"exists": a.get("exists"), "count": a.get("count"), "groups": a.get("groups"),
            "templates_matched": (a.get("meta") or {}).get("templates_matched"),
            "http_error": a.get("http_error")}

def queries(path):
    return [q for q in open(path).read().splitlines() if q.strip()]

def clip(text, n=240):
    return text if len(text) <= n else text[:n] + "..."

# ── manifest.log ─────────────────────────────────────────────────────────────────────

def walk(d):
    """Every whole record of manifest.log, in order, and the bytes they take.

    Frames are [len u32 LE][crc u32 LE][JSON] from offset 0, as tests/load.rs reads them.
    The CRC is not rechecked: a payload that is short or is not JSON ends the walk, which
    is what a record still being written looks like."""
    try:
        data = open(os.path.join(d, "manifest.log"), "rb").read()
    except OSError:
        return [], 0
    out, at = [], 0
    while at + 8 <= len(data):
        n = struct.unpack_from("<I", data, at)[0]
        payload = data[at + 8:at + 8 + n]
        if n == 0 or len(payload) < n:
            break
        try:
            record = json.loads(payload)
        except ValueError:
            break
        out.append(record)
        at += 8 + n
    return out, at

def live(records):
    """The live files by path: the last word on a path wins, a removed path is gone, and
    a replace removes before it adds."""
    files = {}
    for record in records:
        kind = record.get("kind")
        if kind in ("add", "seal"):
            added, removed = record.get("files") or [], []
        elif kind == "remove":
            added, removed = [], record.get("files") or []
        elif kind == "replace":
            added, removed = record.get("add") or [], record.get("remove") or []
        else:
            continue
        for path in removed:
            files.pop(path, None)
        for f in added:
            files[f["path"]] = f
    return files

def on_disk(d, day):
    """The .col files of data/<day>/, spelled the way the manifest spells them."""
    try:
        names = os.listdir(os.path.join(d, "data", day))
    except OSError:
        names = []
    return set(day + "/" + n for n in names if n.endswith(".col"))

def some(paths, n=3):
    paths = sorted(paths)
    return ", ".join(paths[:n]) + (" and %d more" % (len(paths) - n) if len(paths) > n else "")

def look(d, day, base, disk):
    """(state, live files of the day, problems)."""
    problems = []
    records, _ = walk(d)
    files = live(records)
    of_day = dict((p, f) for p, f in files.items() if p.startswith(day + "/"))
    others = set(files) - set(of_day)
    if others:
        problems.append("the manifest names files outside %s: %s" % (day, some(others)))

    staged = []
    for root, _, names in os.walk(os.path.join(d, "staging")):
        staged.extend(os.path.relpath(os.path.join(root, n), d) for n in names)
    if staged:
        problems.append("staging/ holds %d file(s) after the start: %s"
                        % (len(staged), some(staged)))

    there = on_disk(d, day)
    absent = set(of_day) - there
    unnamed = there - set(of_day)
    if absent:
        problems.append("named by the manifest and not on disk: %s" % some(absent))
    if unnamed and disk == "exact":
        problems.append("in data/%s and not named by the manifest: %s" % (day, some(unnamed)))

    replaces = [r for r in records if r.get("kind") == "replace"]
    finished = [p for p, f in of_day.items() if f.get("finished")]
    unsealed = [p for p, f in of_day.items() if not f.get("sealed")]
    ids = dict((p, f.get("file_id")) for p, f in of_day.items())
    kept = [p for p in of_day if p in base["files"]]
    kept_ids = set(ids.values()) & set(base["files"].values())
    rows = sum(f.get("rows") or 0 for f in of_day.values())

    if not replaces and ids == base["files"] and not finished and not unsealed:
        state = "old"
    elif (len(replaces) == 1 and of_day and len(finished) == len(of_day) and not unsealed
          and not kept and not kept_ids):
        state = "new"
    else:
        state = "partial"
        problems.append(
            "the day is neither old nor new: %d replace record(s), %d live file(s) of which "
            "%d finished and %d unsealed, %d of the day's own %d still live"
            % (len(replaces), len(of_day), len(finished), len(unsealed), len(kept),
               len(base["files"])))
    if rows != base["rows"]:
        problems.append("the manifest's live files hold %d rows, want %d" % (rows, base["rows"]))
    return state, of_day, problems

# ── the dump ─────────────────────────────────────────────────────────────────────────

NUM = "#number"

def number(text):
    # Kept as the text the server wrote: 1.50 against 1.5 is a value that changed.
    return {NUM: text}

def canonical(row):
    return json.dumps(row, sort_keys=True)

def digest(text):
    return hashlib.blake2b(text.encode("ascii"), digest_size=16).digest()

def pages(port):
    """Every page of `MATCH LOG {}`, the cursor followed to the end. With no row cap on
    the server that is one page."""
    cursor = None
    while True:
        q = "MATCH LOG {}" + (' AFTER "%s"' % cursor if cursor else "")
        answer = json.loads(post(port, q, 600), parse_int=number, parse_float=number)
        yield answer.get("rows") or []
        cursor = (answer.get("meta") or {}).get("next_cursor")
        if not cursor:
            break

def digests(path):
    data = open(path, "rb").read()
    return Counter(data[i:i + 16] for i in range(0, len(data), 16))

def plain(o):
    if len(o) == 1 and isinstance(o.get(NUM), str):
        s = o[NUM]
        return float(s) if any(c in s for c in ".eE") else int(s)
    return o

# roundtrip.sh's comparison of what came back with what went in.

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

def as_ingested(value):
    if isinstance(value, list):
        # serde_json's compact form: no spaces, non-ASCII as itself.
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return value

def canon(value):
    """One comparable, hashable form per JSON value: a string, a bool and null are
    themselves; a number is its exact value as the reader holds it. roundtrip.sh makes
    that a Fraction; python already compares and hashes an int and a float by their exact
    values, so 5 and 5.0 are one key here too, at a fraction of the time."""
    if value is None or isinstance(value, (bool, str)):
        return (type(value).__name__, value)
    if isinstance(value, int) and not (-2**63 <= value < 2**64):
        value = float(value)
    return ("number", value)

def row_key(row):
    return tuple(sorted((k, canon(v)) for k, v in row.items()))

cmd, args = sys.argv[1], sys.argv[2:]

if cmd == "now_ms":
    print(int(time.time() * 1000))

elif cmd == "key":  # key <input> <head lines>
    # `level` when the input has it anywhere in its head; a group per value partitions every
    # row, the ones without it included, so it need not be on most lines. Otherwise the
    # most widespread key with few enough values that each group is a real slice.
    path, head = args[0], int(args[1])
    seen, values = {}, {}
    with open(path) as f:
        for i, line in enumerate(f):
            if i >= head:
                break
            try:
                o = json.loads(line)
            except ValueError:
                continue
            if not isinstance(o, dict):
                continue
            for k, v in o.items():
                seen[k] = seen.get(k, 0) + 1
                s = values.setdefault(k, set())
                if len(s) <= 32:
                    s.add(json.dumps(v, sort_keys=True))
    if "level" in seen:
        print("level")
    else:
        few = [k for k in seen if len(values[k]) <= 32]
        if not few:
            sys.exit("no key in the first %d lines has 32 or fewer values" % head)
        print(sorted(few, key=lambda k: (-seen[k], k))[0])

elif cmd == "field":  # field <json file> <key>
    print(json.load(open(args[0]))[args[1]])

elif cmd == "buffered":  # buffered <port>
    with urllib.request.urlopen("http://127.0.0.1:%s/stats" % args[0], timeout=30) as r:
        print(json.loads(r.read())["storage"]["buffered_rows"])

elif cmd == "shape":  # shape <port>: rows, row groups and templates of the whole store
    a = ask(args[0], "COUNT LOG {}")
    meta = a.get("meta") or {}
    print(a.get("count"), meta.get("row_groups_read"), meta.get("templates_matched"))

elif cmd == "plan":  # plan <seed> <t_ms> <rounds>: a line per round, mode timing ms
    rng = random.Random(int(args[0]))
    t, rounds = float(args[1]), int(args[2])
    plan = []
    for r in range(rounds):
        mode = "alone" if r % 2 == 0 else "server"
        timing = "uniform" if (r // 2) % 2 == 0 else "window"
        plan.append([mode, timing, rng.random()])
    # Half the window rounds of each mode wait for the record and half for a new file,
    # in a drawn order.
    for mode in ("alone", "server"):
        mine = [p for p in plan if p[0] == mode and p[1] == "window"]
        kinds = ["record", "file"] * (len(mine) // 2 + 1)
        kinds = kinds[:len(mine)]
        rng.shuffle(kinds)
        for p, kind in zip(mine, kinds):
            p[1] = kind
    for mode, timing, u in plan:
        ms = {"uniform": 1.1 * u * t, "file": u * t, "record": 0}[timing]
        print(mode, timing, int(round(ms)))

elif cmd == "battery":  # battery <port> <queries file>
    print(json.dumps([comparable(ask(args[0], q)) for q in queries(args[1])], sort_keys=True))

elif cmd == "clean":  # clean <battery file>: exit 1 if any answer was an error
    sys.exit(1 if any(a["http_error"] for a in json.load(open(args[0]))) else 0)

elif cmd == "same":  # same <got> <reference> <queries>: a line per answer that differs
    got, want = (json.load(open(p)) for p in args[:2])
    bad = 0
    for q, g, w in zip(queries(args[2]), got, want):
        if g == w:
            continue
        bad += 1
        if g["http_error"]:
            print("%s answered %s" % (q, g["http_error"]))
        elif g["count"] != w["count"]:
            print("%s: count %s, want %s" % (q, g["count"], w["count"]))
        else:
            fields = [k for k in sorted(w) if g.get(k) != w[k]]
            print("%s: %s differ from the reference" % (q, ", ".join(fields)))
    sys.exit(3 if bad else 0)

elif cmd == "base":  # base <dir> <day>: the day as the live door left it, as JSON
    d, day = args
    records, _ = walk(d)
    files = dict((p, f) for p, f in live(records).items() if p.startswith(day + "/"))
    problems = []
    if any(r.get("kind") == "replace" for r in records):
        problems.append("the live day's manifest already holds a replace record")
    if len(files) != len(live(records)):
        problems.append("the live day's manifest names files outside %s" % day)
    if [p for p, f in files.items() if not f.get("sealed")]:
        problems.append("a clean stop left a file of the day unsealed")
    if [p for p, f in files.items() if f.get("finished")]:
        problems.append("the live door wrote a file flagged finished")
    if on_disk(d, day) != set(files):
        problems.append("data/%s is not exactly the manifest's live files" % day)
    print(json.dumps({
        "files": dict((p, f.get("file_id")) for p, f in files.items()),
        "rows": sum(f.get("rows") or 0 for f in files.values()),
        "bytes": sum(os.path.getsize(os.path.join(d, "data", p)) for p in files
                     if os.path.exists(os.path.join(d, "data", p))),
        "problems": problems}))

elif cmd == "state":  # state <dir> <day> <base.json> <exact|named>
    # Line 1 the state, line 2 the day's live files, then a line per problem.
    d, day, base_path, disk = args
    state, of_day, problems = look(d, day, json.load(open(base_path)), disk)
    print(state)
    print(len(of_day))
    for p in problems:
        print(p)
    sys.exit(3 if problems else 0)

elif cmd == "dump":  # dump <port> <out> [<reference>]
    # Line 1 the rows returned, then a line per problem. <out> is every row's digest;
    # the reference also keeps the rows, to show the ones a later dump is missing.
    port, out = args[0], args[1]
    want = digests(args[2]) if len(args) > 2 else None
    got, strays, rows = Counter(), [], 0
    keep = None if want is not None else open(out + ".rows", "w")
    try:
        for page in pages(port):
            for row in page:
                text = canonical(row)
                key = digest(text)
                got[key] += 1
                rows += 1
                if keep is not None:
                    keep.write(text + "\n")
                elif got[key] > want.get(key, 0) and len(strays) < 2:
                    strays.append((got[key], text))
    except urllib.error.HTTPError as e:
        print(rows)
        print("MATCH LOG {} answered %s: %s"
              % (e.code, clip(e.read().decode("utf-8", "replace"))))
        sys.exit(3)
    if keep is not None:
        keep.close()
    with open(out, "wb") as f:
        f.write(b"".join(sorted(got.elements())))
    print(rows)
    if want is None or got == want:
        sys.exit(0)
    missing, extra = want - got, got - want
    twice = sum(1 for k in extra if want.get(k, 0))
    print("the dump is not the reference: %d row(s) missing, %d returned that should not "
          "be - %d of them a stored row returned again"
          % (sum(missing.values()), sum(extra.values()), twice))
    for n, text in strays:
        print("returned %s: %s" % ("%d times" % n if n > 1 else "and never stored", clip(text)))
    shown = 0
    with open(args[2] + ".rows") as f:
        for line in f:
            if shown == 2:
                break
            if digest(line.rstrip("\n")) in missing:
                shown += 1
                print("missing: %s" % clip(line.rstrip("\n")))
    sys.exit(3)

elif cmd == "input":  # input <input file> <reference rows>: the dump against the lines
    raw = open(args[0], encoding="utf-8", errors="surrogateescape").read().split("\n")
    want = Counter()
    for line in raw:
        if line.strip():
            flat = flatten(json.loads(line), [], {})
            want[row_key(dict((k, as_ingested(v)) for k, v in flat.items()))] += 1
    got, rows = Counter(), 0
    with open(args[1]) as f:
        for line in f:
            row = json.loads(line, object_hook=plain)
            # `_received_at` is the system column unless the line had a key of that name.
            if "_received_at" in row and row_key(row) not in want:
                row = dict((k, v) for k, v in row.items() if k != "_received_at")
            got[row_key(row)] += 1
            rows += 1
    missing, extra = want - got, got - want
    differences = sum(missing.values()) + sum(extra.values())
    print("%d lines, %d rows returned, %d differences" % (sum(want.values()), rows, differences))
    for key in list(missing.elements())[:3]:
        print("  not returned: %s" % clip(repr(dict(key))))
    for key in list(extra.elements())[:3]:
        print("  returned and never ingested: %s" % clip(repr(dict(key))))
    sys.exit(3 if differences else 0)

elif cmd == "kill":
    # kill <mode> <timing> <ms> <t_ms> <server pid> <dir> <day> <base.json> <stdout> <stderr>
    #      <command...>
    # Runs the command, kills at the chosen moment - the command itself when it is alone,
    # the server it is a client of otherwise - and prints: the milliseconds from launch to
    # the kill, what set it off, the command's exit code, and whether the kill landed on a
    # compaction still in flight (yes), after it had exited 0 (no), after it had failed by
    # itself (own), or left a client that never gave up (hung).
    mode, timing, ms, t_ms, server, d, day, base_path, out_path, err_path = args[:10]
    ms, t_ms, server = int(ms), float(t_ms), int(server)
    own = set(json.load(open(base_path))["files"])
    manifest = os.path.join(d, "manifest.log")
    with open(out_path, "wb") as so, open(err_path, "wb") as se:
        child = subprocess.Popen(args[10:], stdout=so, stderr=se)
    t0 = time.monotonic()
    deadline = t0 + 1.5 * t_ms / 1000

    def exits_within(seconds):
        try:
            child.wait(timeout=max(seconds, 0))
            return True
        except subprocess.TimeoutExpired:
            return False

    trigger = None
    try:
        if timing == "uniform":
            trigger = "exited" if exits_within(ms / 1000) else "delay"
        seen = -1
        while trigger is None:
            if child.poll() is not None:
                trigger = "exited"
            elif time.monotonic() >= deadline:
                trigger = "timeout"
            elif timing == "record":
                try:
                    size = os.stat(manifest).st_size
                except OSError:
                    size = seen
                if size != seen:
                    records, used = walk(d)
                    if any(r.get("kind") == "replace" for r in records):
                        trigger = "record"
                    elif used == size:
                        # Whole records and none of them the commit; a record caught half
                        # written is read again on the next turn.
                        seen = size
            elif on_disk(d, day) - own:
                trigger = "exited" if exits_within(ms / 1000) else "file"
            else:
                # The kill comes a drawn while after the file, so a millisecond's notice is
                # early enough; the record's watcher above does not sleep at all.
                time.sleep(0.001)

        before = child.poll()
        at = int(round((time.monotonic() - t0) * 1000))
        if mode == "alone":
            if before is None:
                child.kill()
            code = child.wait()
        else:
            try:
                os.kill(server, signal.SIGKILL)
            except ProcessLookupError:
                pass  # already gone: its exit code, read by the shell, will not be 137
            code = before
            if before is None:
                try:
                    code = child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    code = None
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
    if code is None:
        landed, code = "hung", "-"
    elif code == 0:
        landed = "no"
    elif mode == "alone":
        landed = "yes" if code == -signal.SIGKILL else "own"
    else:
        landed = "yes" if before is None else "own"
    if isinstance(code, int) and code < 0:
        code = 128 - code
    print(at, trigger, code, landed)

elif cmd == "left":  # left <dir> <day> <base.json>: what a kill left in data/<day>/
    # The day's own files still there, then the .col files the day did not have. Which of
    # the two the next start has to delete is the manifest's to say, and is read after it.
    d, day, base_path = args
    own = set(json.load(open(base_path))["files"])
    disk = on_disk(d, day)
    print(len(disk & own), len(disk - own))

elif cmd == "report":
    # report <json file> <compacted|nothing> <dir> <day> <base.json> <row groups before>
    path, expect, d, day, base_path, groups = args
    base = json.load(open(base_path))
    text = open(path).read()
    try:
        report = json.loads(text)
        entry = report["days"][0] if len(report["days"]) == 1 else None
    except (ValueError, KeyError, TypeError, IndexError):
        print("the report is not one JSON object with a `days` list: %s" % clip(text.strip()))
        sys.exit(3)
    problems = []
    if report.get("ok") is not True:
        problems.append("`ok` is %r, want true" % report.get("ok"))
    if entry is None:
        print("the report names %d days, want the one asked for" % len(report["days"]))
        sys.exit(3)
    of_day = dict((p, f) for p, f in live(walk(d)[0]).items() if p.startswith(day + "/"))
    if expect == "compacted":
        added = entry.get("added") or []
        want = {
            "day": day, "compacted": True, "rows": base["rows"],
            "files_in": len(base["files"]), "files_out": len(of_day),
            "row_groups_in": int(groups), "bytes_in": base["bytes"],
            "bytes_out": sum(os.path.getsize(os.path.join(d, "data", p)) for p in of_day
                             if os.path.exists(os.path.join(d, "data", p))),
            "added": sorted(of_day), "removed": sorted(base["files"]),
        }
        out = entry.get("row_groups_out")
        if not isinstance(out, int) or isinstance(out, bool) or not 0 < out < int(groups):
            problems.append("`row_groups_out` is %r, want fewer than the %s read and more "
                            "than none" % (out, groups))
    else:
        want = {"day": day, "compacted": False, "rows": 0, "files_in": 0, "files_out": 0,
                "row_groups_in": 0, "row_groups_out": 0, "bytes_in": 0, "bytes_out": 0,
                "added": [], "removed": []}
    for k in sorted(want):
        got = entry.get(k, "absent")
        if isinstance(want[k], list) and isinstance(got, list):
            got = sorted(got)
        if got != want[k] or isinstance(got, bool) != isinstance(want[k], bool):
            problems.append("`%s` is %s, want %s"
                            % (k, clip(json.dumps(got), 120), clip(json.dumps(want[k]), 120)))
    seconds = entry.get("seconds", "absent")
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or seconds < 0:
        problems.append("`seconds` is %s, want a number" % clip(json.dumps(seconds), 120))
    for p in problems:
        print(p)
    sys.exit(3 if problems else 0)

elif cmd == "compaction":  # compaction <port> <days compacted> <day> <rows>
    port, days, day, rows = args[0], int(args[1]), args[2], int(args[3])
    with urllib.request.urlopen("http://127.0.0.1:%s/stats" % port, timeout=30) as r:
        c = json.loads(r.read()).get("storage", {}).get("compaction")
    if not isinstance(c, dict):
        print("/stats has no storage.compaction object")
        sys.exit(3)
    problems = []
    if c.get("running") is not False:
        problems.append("`running` is %s with no compaction running, want false"
                        % json.dumps(c.get("running", "absent")))
    if c.get("days_compacted") != days:
        problems.append("`days_compacted` is %s, want %d"
                        % (json.dumps(c.get("days_compacted", "absent")), days))
    runs = c.get("runs", "absent")
    if isinstance(runs, bool) or not isinstance(runs, int) or runs < days:
        problems.append("`runs` is %s, want a count of at least %d" % (json.dumps(runs), days))
    if c.get("last_error", "absent") is not None:
        problems.append("`last_error` is %s, want null"
                        % clip(json.dumps(c.get("last_error", "absent"))))
    last = c.get("last", "absent")
    if days == 0:
        if last is not None:
            problems.append("`last` is %s, want null: this server compacted no day"
                            % clip(json.dumps(last), 120))
    elif not isinstance(last, dict) or last.get("day") != day or last.get("rows") != rows:
        problems.append("`last` is %s, want the report of %s with %d rows"
                        % (clip(json.dumps(last), 120), day, rows))
    for p in problems:
        print("/stats storage.compaction: " + p)
    sys.exit(3 if problems else 0)

else:
    sys.exit("help.py: no command %r" % cmd)
PY
H="$WORK/help.py"
# Where help.py notes an answer it had to ask for twice.
export CUTS="$WORK/cuts"
: > "$CUTS"

# Sets PID and PORT rather than echoing them, so it must be called in this shell: in a
# subshell the server is not this shell's child, and `wait` could not collect its exit.
PORT=""
start() {  # start <logfile> <serve flags...>
  local log=$1; shift
  "$BIN" serve --port 0 "$@" 2>"$log" &
  PID=$!
  PORT=""
  for _ in $(seq 1 600); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$log" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then
    kill -KILL "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; PID=""
    return 1
  fi
}

# SIGTERM, then the exit code. Capped: a compaction thread that shutdown forgot to join
# would otherwise hold this script for ever.
STOP_CODE=""
stop() {
  kill -TERM "$PID" 2>/dev/null
  for _ in $(seq 1 600); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$PID" 2>/dev/null; then
    STOP_CODE="none, still running 60s after SIGTERM"
    kill -KILL "$PID" 2>/dev/null; wait "$PID" 2>/dev/null
  else
    wait "$PID" 2>/dev/null
    STOP_CODE=$?
  fi
  PID=""
}

# What a server said about itself, less the banner and the lines every start and stop
# print: what is left is what recovery did - the files it deleted as unnamed - and what
# went wrong.
said() {  # said <logfile> <lines>
  grep -hE '^kibbr: |panic' "$1" 2>/dev/null \
    | grep -vE 'widened|catalog\.log - |SIGTERM received|stop accepting - 0 |in-flight requests complete' \
    | grep -vE 'final flush - |WAL|trie\.json - |close stores - |kibbr: exit 0' \
    | tail -"$2" | sed 's/^/         /'
}

# Every server after the live one: the dump's row cap, and the size cap compaction writes
# under - a client's own flag cannot set a server's.
SERVE=(--max-rows "$MAX_ROWS" --segment-max-bytes "$COMPACT_SEGMENT_BYTES")
COMPACT=(compact --day "$DAY")
ALONE=(--segment-max-bytes "$COMPACT_SEGMENT_BYTES")

# 4 to 7 on the server at $PORT over <dir>. Sets STATE, FILES and ROWS, and adds what is
# wrong to why[].
STATE=""; FILES=""; ROWS=""
why=()
look() {  # look <dir> <tag> <exact|named> <when>
  local d=$1 tag=$2 when=$4 line code
  python3 "$H" state "$d" "$DAY" "$WORK/base.json" "$3" > "$WORK/state.$tag"
  code=$?
  STATE=$(sed -n 1p "$WORK/state.$tag"); FILES=$(sed -n 2p "$WORK/state.$tag")
  case "$code" in
    0) ;;
    3) while IFS= read -r line; do why[${#why[@]}]="$when $line"; done \
         < <(sed 1,2d "$WORK/state.$tag") ;;
    *) STATE=unread; why[${#why[@]}]="$when the manifest was not read: the walk did not finish" ;;
  esac

  python3 "$H" battery "$PORT" "$Q" > "$WORK/battery.$tag"
  python3 "$H" same "$WORK/battery.$tag" "$WORK/whole.json" "$Q" > "$WORK/same.$tag"
  case $? in
    0) ;;
    3) while IFS= read -r line; do why[${#why[@]}]="$when $line"; done < "$WORK/same.$tag" ;;
    *) why[${#why[@]}]="$when the battery was not compared: the comparison did not finish" ;;
  esac

  python3 "$H" dump "$PORT" "$WORK/dump.$tag" "$WORK/ref.dump" > "$WORK/dumped.$tag"
  code=$?
  ROWS=$(sed -n 1p "$WORK/dumped.$tag")
  case "$code" in
    0) ;;
    3) while IFS= read -r line; do why[${#why[@]}]="$when $line"; done \
         < <(sed 1d "$WORK/dumped.$tag") ;;
    *) ROWS=${ROWS:--}; why[${#why[@]}]="$when the dump did not finish" ;;
  esac
  rm -f "$WORK/dump.$tag"
}

# One `compact --day` through the server at $PORT over <dir>, and what it must report.
again() {  # again <dir> <tag> <compacted|nothing> <when>
  local d=$1 tag=$2 expect=$3 when=$4 line code days=0
  cp "$d/manifest.log" "$WORK/manifest.$tag"
  "$BIN" "${COMPACT[@]}" --data-dir "$d" --json > "$WORK/again.$tag.out" 2> "$WORK/again.$tag.err"
  code=$?
  [ "$code" = 0 ] ||
    why[${#why[@]}]="$when kibbr compact through the server exited $code, want 0: $(tail -1 "$WORK/again.$tag.err")"
  python3 "$H" report "$WORK/again.$tag.out" "$expect" "$d" "$DAY" "$WORK/base.json" "$BASE_GROUPS" \
    > "$WORK/report.$tag"
  case $? in
    0) ;;
    3) while IFS= read -r line; do why[${#why[@]}]="$when the report's $line"; done < "$WORK/report.$tag" ;;
    *) why[${#why[@]}]="$when the report was not checked: the check did not finish" ;;
  esac
  if [ "$expect" = compacted ]; then
    days=1
  else
    cmp -s "$d/manifest.log" "$WORK/manifest.$tag" ||
      why[${#why[@]}]="$when a compaction with nothing to compact wrote to manifest.log"
  fi
  python3 "$H" compaction "$PORT" "$days" "$DAY" "$N" > "$WORK/stats.$tag"
  case $? in
    0) ;;
    3) while IFS= read -r line; do why[${#why[@]}]="$when $line"; done < "$WORK/stats.$tag" ;;
    *) why[${#why[@]}]="$when /stats was not checked: the check did not finish" ;;
  esac
}

# Prints why[] as FAIL lines and says whether there were any.
failed() {  # failed <label>
  local w
  [ ${#why[@]} -gt 0 ] || return 1
  for w in "${why[@]}"; do echo "FAIL $1: $w"; done
  return 0
}

FAIL=0

# ── The battery ──────────────────────────────────────────────────────────────────────
# COUNT is the headline; by the hour and by one key's values catch a day that is short in
# one place and doubled in another, which leaves the total right. The dump catches the
# rest.
KEY=$(python3 "$H" key "$INPUT" 10000) || exit 1
Q="$WORK/queries.txt"
{
  echo 'COUNT LOG {}'
  echo 'COUNT LOG {} GROUP BY time(1h)'
  echo "COUNT LOG {} GROUP BY $(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$KEY")"
} > "$Q"

# ── The live day ─────────────────────────────────────────────────────────────────────
# A tick every 100 ms closes whatever each template's buffer holds, so the day lands as
# short row groups, and the small size cap spreads them over several files. Compaction
# has nothing to prove on one file of full row groups.
AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
start "$WORK/live.log" --data-dir "$WORK/base" --flush-interval-ms 100 \
  --row-group-timeout-ms 100 --segment-max-bytes "$LIVE_SEGMENT_BYTES" ||
  { echo "FAIL no server started on an empty directory"; tail -20 "$WORK/live.log"; exit 1; }
T0=$(python3 "$H" now_ms)
python3 "$WORK/send.py" "$PORT" "$INPUT" "$AT" "$BATCH" > "$WORK/sent.json" ||
  { echo "FAIL the live door refused a body"; exit 1; }
# Every row in a closed row group before the stop: the final flush would write them too,
# but then the last of the day would be one long row group per template.
LEFT=""
for _ in $(seq 1 600); do
  LEFT=$(python3 "$H" buffered "$PORT" 2>/dev/null)
  [ "$LEFT" = 0 ] && break
  sleep 0.1
done
T1=$(python3 "$H" now_ms)
[ "$LEFT" = 0 ] || { echo "FAIL ${LEFT:-an unknown number of} rows were still buffered 60s after the last body, want 0"; exit 1; }
stop
[ "$STOP_CODE" = 0 ] || { echo "FAIL the live server stopped with exit $STOP_CODE, want 0"; tail -5 "$WORK/live.log"; exit 1; }
INGESTED=$(python3 "$H" field "$WORK/sent.json" ingested)

# ── The reference: the day's answers and its dump, from files alone ──────────────────
start "$WORK/ref.log" --data-dir "$WORK/base" "${SERVE[@]}" ||
  { echo "FAIL no server started on the live day"; tail -20 "$WORK/ref.log"; exit 1; }
python3 "$H" battery "$PORT" "$Q" > "$WORK/whole.json"
read -r N BASE_GROUPS TEMPLATES < <(python3 "$H" shape "$PORT")
D0=$(python3 "$H" now_ms)
DUMPED=$(python3 "$H" dump "$PORT" "$WORK/ref.dump") ||
  { echo "FAIL the reference dump did not finish"; exit 1; }
D1=$(python3 "$H" now_ms)
stop
[ "$STOP_CODE" = 0 ] || { echo "FAIL the reference server stopped with exit $STOP_CODE, want 0"; FAIL=1; }

# Read after the stop: this is the day every round starts from, to the byte.
python3 "$H" base "$WORK/base" "$DAY" > "$WORK/base.json" ||
  { echo "FAIL the live day's manifest was not read"; exit 1; }
BASE_FILES=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["files"]))' "$WORK/base.json")
BASE_BYTES=$(python3 "$H" field "$WORK/base.json" bytes)

echo "input      $INPUT, $NLINES lines"
echo "live day   $DAY: $N rows in $BASE_GROUPS row groups in $BASE_FILES files ($BASE_BYTES bytes), $TEMPLATES templates; on disk $((T1 - T0))ms after the first body"
echo "battery    $(tr '\n' '|' < "$Q" | sed 's/|$//; s/|/, /g')"

# The reference has to be worth comparing against. Every farm line is valid and carries
# its own time, so a row short of NLINES is a reference that is not the input; an error in
# the battery would let an erroring restart pass as equal; and a day that is one file, or
# full row groups, is one a compaction has nothing to do to.
[ "$INGESTED" = "$NLINES" ] || { echo "FAIL the live door ingested $INGESTED rows, want $NLINES"; FAIL=1; }
[ "$N" = "$NLINES" ] || { echo "FAIL COUNT LOG {} on the live day is $N, want $NLINES"; FAIL=1; }
python3 "$H" clean "$WORK/whole.json" || { echo "FAIL the reference battery answered an error"; FAIL=1; }
[ "$DUMPED" = "$NLINES" ] || { echo "FAIL the reference dump returned $DUMPED rows, want $NLINES"; FAIL=1; }
python3 - "$WORK/base.json" <<'PY' || FAIL=1
import json, sys
problems = json.load(open(sys.argv[1]))["problems"]
for p in problems:
    print("FAIL " + p)
sys.exit(1 if problems else 0)
PY
[ "$BASE_FILES" -ge 4 ] 2>/dev/null ||
  { echo "FAIL the live day is $BASE_FILES file(s), want at least 4: lower LIVE_SEGMENT_BYTES"; FAIL=1; }
[ "$BASE_GROUPS" -ge $((2 * TEMPLATES)) ] 2>/dev/null ||
  { echo "FAIL the live day is $BASE_GROUPS row groups over $TEMPLATES templates, want at least two a template"; FAIL=1; }
[ "$FAIL" = 0 ] || exit 1

SAME=$(python3 "$H" input "$INPUT" "$WORK/ref.dump.rows")
case $? in
  0) ;;
  3) echo "FAIL the reference dump is not the input: $SAME"; exit 1 ;;
  *) echo "FAIL the reference dump was not compared with the input: the comparison did not finish"; exit 1 ;;
esac
echo "reference  the dump is the input: $SAME; one dump takes $((D1 - D0))ms"

# ── T: one uninterrupted compaction, alone ───────────────────────────────────────────
cp -R "$WORK/base" "$WORK/whole"
T0=$(python3 "$H" now_ms)
"$BIN" "${COMPACT[@]}" --data-dir "$WORK/whole" "${ALONE[@]}" --json > "$WORK/whole.out" 2> "$WORK/whole.err"
REF_CODE=$?
T1=$(python3 "$H" now_ms)
MEASURED=$((T1 - T0))
if [ "$REF_CODE" != 0 ]; then
  echo "FAIL the reference compaction, kibbr ${COMPACT[*]} alone, exited $REF_CODE, want 0"
  tail -5 "$WORK/whole.err" | sed 's/^/    /'
  exit 1
fi
T=${T_MS:-$MEASURED}

why=()
python3 "$H" report "$WORK/whole.out" compacted "$WORK/whole" "$DAY" "$WORK/base.json" "$BASE_GROUPS" \
  > "$WORK/report.whole"
case $? in
  0) ;;
  3) while IFS= read -r line; do why[${#why[@]}]="the report's $line"; done < "$WORK/report.whole" ;;
  *) why[${#why[@]}]="the report was not checked: the check did not finish" ;;
esac
# Alone again, before any server has started on it: nothing to do, and nothing written.
cp "$WORK/whole/manifest.log" "$WORK/manifest.whole"
"$BIN" "${COMPACT[@]}" --data-dir "$WORK/whole" "${ALONE[@]}" > "$WORK/twice.out" 2> "$WORK/twice.err"
CODE=$?
[ "$CODE" = 0 ] || why[${#why[@]}]="a second compaction alone exited $CODE, want 0: $(tail -1 "$WORK/twice.err")"
[ "$(cat "$WORK/twice.out")" = "$DAY: nothing to compact" ] ||
  why[${#why[@]}]="a second compaction alone printed '$(head -c 200 "$WORK/twice.out")', want '$DAY: nothing to compact'"
cmp -s "$WORK/whole/manifest.log" "$WORK/manifest.whole" ||
  why[${#why[@]}]="a second compaction alone wrote to manifest.log"

if ! start "$WORK/whole.log" --data-dir "$WORK/whole" "${SERVE[@]}"; then
  why[${#why[@]}]="no server started on the compacted day"
  failed "the reference compaction"
  said "$WORK/whole.log" 6
  exit 1
fi
look "$WORK/whole" whole exact "after it"
[ "$STATE" = new ] || why[${#why[@]}]="after it the day is $STATE, want new"
OUT_FILES=$FILES
# One file is a record with one name to add and a restart with no order to get right: a
# size cap that compaction ignored, or one too large for this day, would pass every round.
[ "$OUT_FILES" -ge 2 ] 2>/dev/null ||
  why[${#why[@]}]="after it the day is ${OUT_FILES:-an unknown number of} file(s) under a $COMPACT_SEGMENT_BYTES-byte cap, want at least 2: lower COMPACT_SEGMENT_BYTES"
read -r _ OUT_GROUPS _ < <(python3 "$H" shape "$PORT")
again "$WORK/whole" whole nothing "asked again,"
stop
[ "$STOP_CODE" = 0 ] || why[${#why[@]}]="the server on the compacted day stopped with exit $STOP_CODE, want 0"
echo "compacted  $BASE_GROUPS row groups in $BASE_FILES files -> $OUT_GROUPS in $OUT_FILES, in ${MEASURED}ms alone; asked again, nothing to compact"
if failed "the reference compaction"; then
  said "$WORK/whole.log" 6
  exit 1
fi
rm -rf "$WORK/whole"
echo "kills      uniform in [0, 1.1 x ${T}ms] or on the commit window, SEED=$SEED (replay: SEED=$SEED T_MS=$T ROUNDS=$ROUNDS scripts/fault/compact_kill.sh)"

MODES=(); TIMINGS=(); DELAYS=()
while read -r m t d; do
  MODES[${#MODES[@]}]=$m; TIMINGS[${#TIMINGS[@]}]=$t; DELAYS[${#DELAYS[@]}]=$d
done < <(python3 "$H" plan "$SEED" "$T" "$ROUNDS")
[ "${#MODES[@]}" = "$ROUNDS" ] || { echo "FAIL the schedule has ${#MODES[@]} rounds, want $ROUNDS"; exit 1; }

# ── The rounds ───────────────────────────────────────────────────────────────────────
echo
printf '%5s  %-6s  %-7s  %-7s  %7s  %4s  %3s  %3s  %-8s  %7s  %-9s  %s\n' \
  round mode timing "kill on" "kill ms" exit in out state rows retry verdict
OLD=0; NEW=0; OLD_MID=0; NEW_MID=0; FINISHED=0; FAILED=0
for r in $(seq 1 "$ROUNDS"); do
  D="$WORK/r$r"
  mode=${MODES[$((r - 1))]}
  timing=${TIMINGS[$((r - 1))]}
  delay=${DELAYS[$((r - 1))]}
  why=()
  outcome="-"; retry="-"; ROWS="-"
  kat="-"; kon="-"; kexit="-"; klanded=""; kin="-"; kout="-"
  cp -R "$WORK/base" "$D"

  # The kill. Alone, the command gets the size cap itself; under a server, the server has
  # it. The server is this shell's child and the command the killer's: each exit code is
  # read by the process that can.
  victim=0
  flags=("${ALONE[@]}")
  if [ "$mode" = server ]; then
    if start "$WORK/victim.$r.log" --data-dir "$D" "${SERVE[@]}"; then
      victim=$PID
      flags=(--json)
    else
      why[${#why[@]}]="no server started on a copy of the live day"
    fi
  fi
  if [ ${#why[@]} = 0 ]; then
    # Inside one redirection: once the killer returns, bash says on its own stderr that a
    # child of its was killed, and that line is not this script's to print.
    SCODE=137
    {
      python3 "$H" kill "$mode" "$timing" "$delay" "$T" "$victim" "$D" "$DAY" "$WORK/base.json" \
        "$WORK/kill.$r.out" "$WORK/kill.$r.err" "$BIN" "${COMPACT[@]}" --data-dir "$D" "${flags[@]}" \
        > "$WORK/kill.$r" 2> "$WORK/kill.$r.trace"
      KCODE=$?
      if [ "$mode" = server ]; then
        wait "$PID"
        SCODE=$?
        PID=""
      fi
    } 2>/dev/null
    read -r kat kon kexit klanded < "$WORK/kill.$r"
    [ "$KCODE" = 0 ] && [ -n "$klanded" ] ||
      why[${#why[@]}]="the killer did not finish: $(tail -1 "$WORK/kill.$r.trace")"
    [ "$SCODE" = 137 ] || why[${#why[@]}]="the server exited $SCODE by itself before the kill, want it killed"
    # The kill's time as drawn where it was drawn, as measured where it was watched for.
    [ "$timing" != uniform ] || [ "$kon" != delay ] || kat=$delay
    case "$klanded" in
      own)  why[${#why[@]}]="kibbr compact exited $kexit by itself before the kill, want 0 or killed: $(tail -1 "$WORK/kill.$r.err")" ;;
      hung) why[${#why[@]}]="kibbr compact was still running 30s after its server was killed, want it to give up" ;;
    esac
    # What the kill left for the start to choose between. Read here and not by the killer:
    # a killed server is this shell's child, and until it is collected it may still be
    # deleting.
    read -r kin kout < <(python3 "$H" left "$D" "$DAY" "$WORK/base.json" 2>/dev/null)
    if [ -z "$kout" ]; then
      kin="-"; kout="-"
      why[${#why[@]}]="data/$DAY was not read after the kill: the listing did not finish"
    fi
  fi

  # The restart: startup keeps one set of the day's files and deletes the other.
  if [ ${#why[@]} = 0 ]; then
    if ! start "$WORK/serve.$r.log" --data-dir "$D" "${SERVE[@]}"; then
      outcome="no start"
      why[${#why[@]}]="the server did not start after the kill"
    else
      look "$D" "$r.kill" exact "after the restart"
      outcome=$STATE
      if [ "$klanded" = no ]; then
        outcome=finished
        [ "$STATE" = new ] ||
          why[${#why[@]}]="kibbr compact exited 0 and after the restart the day is $STATE, want new"
      fi

      # The same compaction again, through the running server.
      if [ "$STATE" = old ] || [ "$STATE" = new ]; then
        retry=nothing
        [ "$STATE" = new ] || retry=compacted
        again "$D" "$r" "$retry" "the retry:"
        look "$D" "$r.retry" named "after the retry"
        [ "$STATE" = new ] || why[${#why[@]}]="after the retry the day is $STATE, want new"
      fi

      stop
      [ "$STOP_CODE" = 0 ] || why[${#why[@]}]="the server stopped with exit $STOP_CODE on SIGTERM, want 0"
    fi
  fi

  verdict=ok
  if [ ${#why[@]} -gt 0 ]; then
    verdict=FAIL
    FAILED=$((FAILED + 1))
  else
    case "$outcome" in
      old) OLD=$((OLD + 1)); [ "$kout" = 0 ] || OLD_MID=$((OLD_MID + 1)) ;;
      new) NEW=$((NEW + 1)); [ "$kin" = 0 ] || NEW_MID=$((NEW_MID + 1)) ;;
      finished) FINISHED=$((FINISHED + 1)) ;;
    esac
  fi
  printf '%5s  %-6s  %-7s  %-7s  %7s  %4s  %3s  %3s  %-8s  %7s  %-9s  %s\n' \
    "$r" "$mode" "$timing" "$kon" "$kat" "$kexit" "$kin" "$kout" "$outcome" "$ROWS" "$retry" "$verdict"
  if [ "$verdict" = FAIL ]; then
    for w in "${why[@]}"; do echo "       FAIL round $r: $w"; done
    tail -2 "$WORK/kill.$r.err" 2>/dev/null | sed 's/^/         /'
    said "$WORK/serve.$r.log" 6
  fi
  rm -rf "$D"
done

echo
echo "$ROUNDS rounds: $OLD old, $OLD_MID of them with an out file on disk at the kill; $NEW new, $NEW_MID of them with an in file; $FINISHED finished; $FAILED failed"
echo "runtime    ${SECONDS}s"
[ ! -s "$CUTS" ] ||
  echo "note       $(wc -l < "$CUTS" | tr -d ' ') answer(s) arrived cut short and were asked again"
[ "$FAILED" = 0 ] || FAIL=1
if [ "$FINISHED" = "$ROUNDS" ]; then
  echo "FAIL every kill landed after the compaction had exited, so none tested one in flight: raise NLINES"
  FAIL=1
elif [ "$ROUNDS" -ge 8 ] && [ "$FAILED" = 0 ]; then
  if [ "$OLD_MID" = 0 ]; then
    echo "FAIL no kill left the day old with an out file on disk ($OLD left it old), want at least one: none reached a compaction between its first new file and its record, so no start had a file no record names to delete"
    FAIL=1
  fi
  if [ "$NEW_MID" = 0 ]; then
    echo "FAIL no kill left the day new with an in file on disk ($NEW left it new), want at least one: none reached a compaction between its record and the last file it deletes, so no start had a replaced file to delete"
    FAIL=1
  fi
fi
if [ "$FAIL" = 0 ]; then
  echo "PASS $ROUNDS/$ROUNDS rounds: every row exactly once ($N rows; $((OLD + NEW)) kills landed mid-compaction, $OLD before the record - $OLD_MID with new files on disk for the start to delete - and $NEW after it - $NEW_MID with the day's own still there; $FINISHED after it had finished; ${MEASURED}ms alone)"
  exit 0
fi
exit 1
