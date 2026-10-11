#!/usr/bin/env bash
# kill -9 at any moment: every acknowledged line is stored, and none is stored twice.
#
# This is the specification of the write-ahead log, written before it. A 200 from /ingest
# is the durability boundary: once a line is acknowledged nothing may lose it, and nothing
# may store it a second time either. The second half is the one a replay gets wrong. A
# replay that skips by the checkpoint alone stores again every row whose row group reached
# the disk before the checkpoint moved, and a test that only asks "is every acknowledged
# line still there" passes it. So this compares the lines, not a count: after every kill
# the whole store is read back with `MATCH LOG {}` and held, row by row, to what the
# senders were told.
#
# The stream is NLINES generated lines, no two of them equal, so a line is its own
# identity: nine in ten from the farm profile, one in ten from the hostile one - thirty
# days, hundreds of templates, columns that widen - and, made here, one line in fifty with
# no time of its own, which is stored under the arrival time its body was sent with, and
# one in 997 that is not JSON. Each round sends the next NLINES/ROUNDS of them, BATCH to a
# body, down SENDERS connections at once, every body stamped with its own `received_at`.
#
# ROUNDS times, on ONE data directory, without a clean stop between them:
#
#   1. the senders send, and every answer is recorded: a 200 acknowledges each line of the
#      body it does not list in `errors`;
#   2. the server is killed with kill -9 at a random offset - once a drawn number of bodies
#      has been acknowledged - in one of four ways, more often the first:
#        flush       the moment a data file under data/ grows: a row group has been
#                    written and the checkpoint has not moved past it yet. This is the gap
#                    a replay by checkpoint doubles rows in.
#        offset      at once.
#        checkpoint  the moment wal/checkpoint changes: the segments behind it are being
#                    deleted.
#        roll        the moment a new segment appears in wal/.
#   3. in some rounds the newest WAL segment then has a torn record appended to it, as a
#      write the kill cut short leaves one: "cut" is a frame whose length says more than
#      is there, "crc" a whole frame whose checksum fails, "short" less than a frame's
#      header. A start must warn about it and carry on, never refuse. (The log's frames
#      are the catalog's: a u32 length, a u32 CRC-32C, the payload.)
#   4. in some rounds the server is started and killed again a few milliseconds in, while
#      it is still recovering, and started once more;
#   5. the server starts - it must - and the store is read back.
#
# What this asserts, after every kill:
#
#   a. every acknowledged line is stored;
#   b. no line is stored twice;
#   c. nothing is stored that was never sent, and nothing that is not a line of the stream;
#   d. a line stored and not acknowledged belongs to a body that was in flight when the
#      kill landed. No test can ask for less than that: a body made durable a moment
#      before its 200 was written is stored and was never acknowledged. But such a body
#      is stored whole or not at all - a body is one write of the log, however many
#      lines it holds - and what one restart found of it every later restart finds. A
#      body that was answered and refused is stored not at all;
#   e. a line the server acknowledged is one that parses, and a line that does not parse
#      is named in `errors` by its number;
#   f. a row stored under its arrival time still carries the arrival time its body was
#      sent with, not the time of the replay - and a line with no time of its own is
#      stored under one;
#   g. `COUNT LOG {}` is the number of rows read back.
#
# The server of round N's check is the server round N+1 kills, so every kill but the
# first is of a server that began by reading the log back. The second kill of step 4 is
# the one that lands while it still is: rows it has put back in memory, and not yet in a
# row group, go with it and must come back again.
#
# A line per round: how the kill was timed ("flush?" is a kill that waited three seconds
# for a row group and saw none), the torn record appended, the second kill's delay, the
# lines acknowledged so far, the rows read back, how many of those are lines of bodies in
# flight - and how many such bodies there have been - what the start put back from the
# log and what it skipped as already in a row group, and the log's first and last segment
# as the kill left them.
#
# After the last round the server is stopped with SIGTERM and must exit 0; the next start
# must find nothing to replay, and the same rows.
#
# The run also fails unless the kills tested what they are for (from 8 rounds up): at
# least one start had to skip lines a durable row group already held past the checkpoint,
# at least one had to put lines back from the log, and at least one kill found the log's
# first segment gone - the checkpoint moved while the server ran and deleted what was
# behind it. And a start that was handed a torn record has to say it saw one.
#
# The schedule is drawn from SEED and printed, so a failing round replays. KEEP=1 keeps
# the work directory of a failing run.
#
#   scripts/fault/crash_test.sh
#   SEED=7 ROUNDS=20 scripts/fault/crash_test.sh
#   KEEP=1 SEED=3 scripts/fault/crash_test.sh
#   COMPACT=1 scripts/fault/crash_test.sh

set -uo pipefail
cd "$(dirname "$0")/../.."

BIN=${BIN:-./target/release/kibbr}
ROUNDS=${ROUNDS:-20}
NLINES=${NLINES:-200000}
GEN_SEED=${GEN_SEED:-42}
SEED=${SEED:-1}
SENDERS=${SENDERS:-4}
BATCH=${BATCH:-100}
# Between one sender's bodies, so a round lasts long enough for row groups to close and
# the checkpoint to move while it is being sent - whatever an fsync costs on the machine.
PACE_MS=${PACE_MS:-20}
# Small, so segments roll and are deleted many times a round. The default is 64MB.
WAL_SEGMENT_BYTES=${WAL_SEGMENT_BYTES:-131072}
# Where one flush of the log stops taking more bodies: two and a half bodies' worth of
# lines, so a flush takes up to three of the senders' bodies together and its bound falls
# in the middle of one - which must go in whole all the same. The default is 500.
WAL_BATCH_LINES=${WAL_BATCH_LINES:-250}
FLUSH_MS=${FLUSH_MS:-20}
ROW_GROUP_MS=${ROW_GROUP_MS:-60}
# The read back is paged: one answer of every row is tens of megabytes over loopback, and
# on a busy machine that does not always arrive whole.
MAX_ROWS=${MAX_ROWS:-50000}
KEEP=${KEEP:-}

# COMPACT=1 compacts while the kills land. For as long as a round is being sent, one day
# of the stream after another is asked for by name - `kibbr compact --day`, which the
# server holding the directory runs, and which takes the directory itself when it finds
# no server, as it does between the kill and the restart. A day asked for by name is
# rebuilt whatever the log still holds of it, so the kill finds row groups a compaction
# merged and files one was half way through, and the replay has to hold its lines against
# the merged ranges. "flush" is then also the moment a compaction's file grows.
COMPACT=${COMPACT:-}

# And no day is rebuilt except as asked: every other row group is one this script's own
# rows closed. The scheduler would find none due here anyway - it leaves a day the live
# door wrote today.
export KIBBR__STORAGE__COMPACT_INTERVAL_MS=0

# Another build can be tested by naming its binary; the default is built first.
[ "$BIN" != ./target/release/kibbr ] || cargo build --release --quiet || exit 1
case "$BIN" in /*) ;; *) BIN=$PWD/${BIN#./} ;; esac

PER_ROUND=$((NLINES / ROUNDS))
[ "$PER_ROUND" -ge "$BATCH" ] || { echo "NLINES/ROUNDS is under one body of $BATCH lines" >&2; exit 2; }

WORK=$(mktemp -d)
PID=""
COMPACTOR=""
failed=0
cleanup() {
  [ -n "$PID" ] && kill -KILL "$PID" 2>/dev/null
  stop_compactor
  wait 2>/dev/null
  if [ -n "$KEEP" ] && [ "$failed" != 0 ]; then
    echo "kept: $WORK" >&2
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

cat > "$WORK/crash.py" <<'PY'
import hashlib, http.client, json, os, random, signal, struct, sys, threading, time
import urllib.error, urllib.request
from collections import Counter, deque

# ── the stream ───────────────────────────────────────────────────────────────────────

# 2024-03-01T00:00:00Z. A body is sent with the arrival time of its first line's number,
# two seconds a line from here: the stream's arrival times cover days, and each body's is
# its own.
EPOCH = 1709251200

def stamp(first):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(EPOCH + 2 * first))

def body_of(n, per_round, batch):
    """The number of the first line of the body line `n` is sent in."""
    start = n - n % per_round
    return start + (n - start) // batch * batch

# roundtrip.sh's reading of "the line that was ingested": flattened as the store flattens
# it, an array as its JSON text, a number as its value.

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

def canon(value):
    if isinstance(value, list):
        # serde_json's compact form: no spaces, non-ASCII as itself.
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int) and not (-2**63 <= value < 2**64):
        value = float(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value

def digest(row):
    text = json.dumps({k: canon(v) for k, v in row.items()}, sort_keys=True)
    return hashlib.blake2b(text.encode("utf-8", "surrogatepass"), digest_size=16).digest()

NOT_A_LINE = bytes(16)

def read_lines(path):
    lines = open(path, "rb").read().split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    return lines

def parsed(line):
    """The line as a JSON object, or None for one /ingest must refuse."""
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None

TIME_KEYS = ("@timestamp", "timestamp", "time", "ts", "ts_ms", "ts_unix", "datetime",
             "event_time", "created_at")

# ── the answers ──────────────────────────────────────────────────────────────────────

def post(port, q, timeout):
    """The answer's bytes. An answer cut short in transit is asked again, as
    compact_kill.sh asks: a large body over loopback on a busy machine does not always
    arrive, and that says nothing about what is stored."""
    req = urllib.request.Request("http://127.0.0.1:%s/query" % port,
                                 json.dumps({"q": q}).encode(),
                                 {"Content-Type": "application/json"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (http.client.IncompleteRead, ConnectionResetError):
            if attempt == 3:
                raise

def pages(port):
    """Every page of `MATCH LOG {}`, the cursor followed to the end."""
    cursor = None
    while True:
        q = "MATCH LOG {}" + (' AFTER "%s"' % cursor if cursor else "")
        answer = json.loads(post(port, q, 900))
        yield answer.get("rows") or []
        cursor = (answer.get("meta") or {}).get("next_cursor")
        if not cursor:
            break

def stats(port):
    with urllib.request.urlopen("http://127.0.0.1:%s/stats" % port, timeout=30) as r:
        return json.loads(r.read())

def segments(data):
    """The numbers of the log's segments on disk, in order."""
    try:
        names = os.listdir(os.path.join(data, "wal"))
    except OSError:
        return []
    out = []
    for name in names:
        if name.startswith("seg_") and name.endswith(".wal") and name[4:-4].isdigit():
            out.append(int(name[4:-4]))
    return sorted(out)

def clip(text, n=200):
    return text if len(text) <= n else text[:n] + "..."

# ── what a kill waits for ────────────────────────────────────────────────────────────

def data_bytes(data):
    total = 0
    try:
        days = list(os.scandir(os.path.join(data, "data")))
    except OSError:
        return 0
    for day in days:
        try:
            for f in os.scandir(day.path):
                if f.name.endswith(".col"):
                    total += f.stat().st_size
        except OSError:
            pass
    return total

def checkpoint_of(data):
    try:
        return open(os.path.join(data, "wal", "checkpoint"), "rb").read()
    except OSError:
        return None

WATCH = {
    "flush": data_bytes,
    "checkpoint": checkpoint_of,
    "roll": lambda data: (segments(data) or [0])[-1],
}

cmd, args = sys.argv[1], sys.argv[2:]

if cmd == "corpus":  # corpus <farm> <hostile> <lines> <out>
    farm, hostile = iter(read_lines(args[0])), iter(read_lines(args[1]))
    n = int(args[2])
    with open(args[3], "wb") as out:
        for i in range(n):
            if i % 997 == 996:
                # Not JSON: refused by number, never acknowledged, never stored.
                line = b'{"msg":"cut off","n":%d' % i
            elif i % 50 == 49:
                # No time of its own: stored under the arrival time of its body.
                line = json.dumps({"msg": "heartbeat", "node": "edge-%d" % (i % 7),
                                   "beat": i, "lag_ms": i % 113},
                                  separators=(",", ":")).encode()
            elif i % 10 == 9:
                line = next(hostile)
            else:
                line = next(farm)
            out.write(line + b"\n")

elif cmd == "keys":  # keys <input> <out>: 16 bytes a line, and the stream held to its rules
    lines = read_lines(args[0])
    seen, bad, untimed = {}, 0, []
    with open(args[1], "wb") as out:
        for n, line in enumerate(lines):
            obj = parsed(line)
            if obj is None:
                bad += 1
                out.write(NOT_A_LINE)
                continue
            row = flatten(obj, [], {})
            if "_received_at" in row:
                sys.exit("line %d has a key `_received_at`, which this reads as the system "
                         "column" % (n + 1))
            if not any(k in row for k in TIME_KEYS):
                untimed.append(n)
            key = digest(row)
            if key in seen or key == NOT_A_LINE:
                sys.exit("lines %d and %d are equal as rows: a line is not its own identity "
                         "in this stream" % (seen.get(key, 0) + 1, n + 1))
            seen[key] = n
            out.write(key)
    # The lines with no time of their own: each must come back under an arrival time.
    with open(args[1] + ".untimed", "w") as out:
        out.write("".join("%d\n" % n for n in untimed))
    print(len(lines), bad, len(untimed))

elif cmd == "plan":  # plan <seed> <rounds>: a line per round - mode delay_ms tear again_ms u
    rng = random.Random(int(args[0]))
    rounds = int(args[1])

    def drawn(cycle):
        """`rounds` of the cycle's values, each run of the cycle in a drawn order: a short
        run still gets the cycle's mix, and not its first few values."""
        out = []
        while len(out) < rounds:
            run = list(cycle)
            rng.shuffle(run)
            out.extend(run)
        return out[:rounds]

    modes = drawn(["flush"] * 8 + ["offset"] * 5 + ["checkpoint"] * 4 + ["roll"] * 3)
    tears = drawn(["cut", "-", "-", "crc", "-", "short", "-"])
    agains = drawn([0, 0, 0, 1])
    for mode, tear, again in zip(modes, tears, agains):
        delay = rng.choice([0, 0, 0, 1, 2, 5]) if mode == "flush" else 0
        print(mode, delay, tear, rng.randrange(5, 150) if again else "-", "%.6f" % rng.random())

elif cmd == "drive":
    # drive <port> <pid> <input> <first> <count> <batch> <senders> <pace_ms> <mode> <u>
    #       <delay_ms> <data> <state> <round>
    port, pid = int(args[0]), int(args[1])
    first, count, batch = int(args[3]), int(args[4]), int(args[5])
    senders, pace = int(args[6]), float(args[7]) / 1000
    mode, u, delay = args[8], float(args[9]), float(args[10]) / 1000
    data, state, round_ = args[11], args[12], args[13]
    lines = read_lines(args[2])[first:first + count]
    queue = deque((first + at, lines[at:at + batch]) for at in range(0, len(lines), batch))
    bodies = len(queue)
    # The offset: the kill is armed once this many bodies are acknowledged. A kill that
    # waits for something to happen is armed in the first seven tenths of the round, so
    # there is traffic left for it to happen in.
    span = bodies if mode == "offset" else max(1, bodies * 7 // 10)
    arm_at = min(bodies, 1 + int(u * span))

    lock = threading.Condition()
    acked, maybe, problems = [], [], []
    done = {"acks": 0, "senders": 0, "sent": 0}
    # Set before the kill is sent: a body that fails after it was in flight when the
    # kill landed, and one that fails before it is a server that dropped a connection.
    killing = threading.Event()

    def send(_):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=60)
        try:
            while True:
                with lock:
                    if not queue:
                        return
                    start, body = queue.popleft()
                    done["sent"] += len(body)
                try:
                    conn.request("POST", "/ingest?received_at=" + stamp(start),
                                 b"\n".join(body) + b"\n",
                                 {"Content-Type": "application/x-ndjson"})
                    response = conn.getresponse()
                    text = response.read()
                except (OSError, http.client.HTTPException) as e:
                    # Sent, or partly, and never answered: in flight when the kill landed.
                    with lock:
                        maybe.append({"first": start, "count": len(body), "why": "unanswered"})
                        if not killing.is_set():
                            problems.append("the body at line %d got no answer before the "
                                            "kill was sent: %s" % (start + 1, e))
                    return
                want = set(at + 1 for at, line in enumerate(body) if parsed(line) is None)
                with lock:
                    if response.status == 200:
                        report = json.loads(text)
                        named = set(e["line"] for e in report.get("errors") or [])
                        if report.get("errors_omitted") or named != want:
                            problems.append(
                                "the body at line %d: errors names lines %s, and the lines "
                                "that do not parse are %s"
                                % (start + 1, sorted(named)[:5], sorted(want)[:5]))
                        if report.get("ingested") != len(body) - len(named):
                            problems.append(
                                "the body at line %d: ingested is %s of %d lines with %d refused"
                                % (start + 1, report.get("ingested"), len(body), len(named)))
                        acked.extend(start + at for at in range(len(body))
                                     if at + 1 not in named)
                    else:
                        # Refused: nothing of it is acknowledged, and what it stored is
                        # held to the rule for a body in flight.
                        maybe.append({"first": start, "count": len(body),
                                      "why": str(response.status)})
                    done["acks"] += 1
                    lock.notify_all()
                time.sleep(pace)
        finally:
            with lock:
                done["senders"] += 1
                lock.notify_all()

    threads = [threading.Thread(target=send, args=(i,)) for i in range(senders)]
    for t in threads:
        t.start()
    with lock:
        while done["acks"] < arm_at and done["senders"] < senders:
            lock.wait(0.05)
        armed_at = done["acks"]
    how = mode
    if mode in WATCH:
        look = WATCH[mode]
        before, deadline = look(data), time.time() + 3.0
        while look(data) == before:
            if time.time() > deadline:
                how = mode + "?"   # nothing to wait for happened: killed anyway
                break
            time.sleep(0.0002)
        else:
            time.sleep(delay)
    killing.set()
    os.kill(pid, signal.SIGKILL)
    for t in threads:
        t.join()

    with open(os.path.join(state, "acked.%s" % round_), "w") as f:
        f.write("".join("%d\n" % n for n in sorted(acked)))
    with open(os.path.join(state, "maybe.%s" % round_), "w") as f:
        json.dump(maybe, f)
    print(json.dumps({
        "how": how, "armed_at": armed_at, "bodies": bodies, "sent": done["sent"],
        "acked": len(acked), "unanswered": sum(1 for m in maybe if m["why"] == "unanswered"),
        "refused": sum(1 for m in maybe if m["why"] != "unanswered"),
        "problems": problems}))

elif cmd == "tear":  # tear <data> <kind> <seed>: a torn record at the end of the newest segment
    data, kind, rng = args[0], args[1], random.Random(int(args[2]))
    numbers = segments(data)
    # A record follows a header that was written and flushed: a segment the kill caught
    # being created has less than one, or one that is still zeros, and no write could
    # have left a record torn after it.
    newest = os.path.join(data, "wal", "seg_%06d.wal" % numbers[-1]) if numbers else None
    if newest is None or os.path.getsize(newest) < 28 or open(newest, "rb").read(4) != b"KWAL":
        print("none")
        sys.exit(0)
    if kind == "cut":
        junk = struct.pack("<II", 4096, rng.getrandbits(32)) + rng.randbytes(rng.randrange(1, 2000))
    elif kind == "crc":
        n = rng.randrange(1, 512)
        junk = struct.pack("<II", n, rng.getrandbits(32)) + rng.randbytes(n)
    else:
        junk = rng.randbytes(rng.randrange(1, 8))
    with open(newest, "ab") as f:
        f.write(junk)
    print(kind)

elif cmd == "wal":  # wal <port>: what the start read back - applied covered records torn
    wal = (stats(args[0]).get("storage") or {}).get("wal") or {}
    replay = wal.get("replay") or {}
    print(replay.get("applied", "-"), replay.get("covered", "-"), replay.get("records", "-"),
          replay.get("torn", "-"))

elif cmd == "segments":  # segments <data>: the log's first and last segment on disk
    numbers = segments(args[0])
    print(numbers[0] if numbers else "-", numbers[-1] if numbers else "-")

elif cmd == "verify":  # verify <port> <keys> <state> <per_round> <batch> <input>
    # Line 1 is JSON: what is stored against what was acknowledged. Then a line per
    # problem, and exit 3 if there is one.
    port, state = args[0], args[2]
    per_round, batch = int(args[3]), int(args[4])
    keys = open(args[1], "rb").read()
    untimed = set(int(n) for n in open(args[1] + ".untimed").read().split())
    line_of = {}
    for n in range(len(keys) // 16):
        key = keys[16 * n:16 * n + 16]
        if key != NOT_A_LINE:
            line_of[key] = n
    acked, maybe = set(), []
    for name in os.listdir(state):
        path = os.path.join(state, name)
        if name.startswith("acked."):
            acked.update(int(n) for n in open(path).read().split())
        elif name.startswith("maybe."):
            maybe.extend(json.load(open(path)))
    in_flight = {}
    for m in maybe:
        for n in range(m["first"], m["first"] + m["count"]):
            in_flight[n] = m["first"]

    problems = []
    try:
        rows = [row for page in pages(port) for row in page]
        counted = json.loads(post(port, "COUNT LOG {}", 300)).get("count")
    except urllib.error.HTTPError as e:
        print(json.dumps({"stored": None}))
        print("the read back answered %s: %s"
              % (e.code, clip(e.read().decode("utf-8", "replace"))))
        sys.exit(3)
    got, strays, stamps = Counter(), [], []
    for row in rows:
        at = row.pop("_received_at", None)
        n = line_of.get(digest(row))
        if n is None:
            if len(strays) < 3:
                strays.append(clip(json.dumps(row, sort_keys=True)))
            got[None] += 1
            continue
        got[n] += 1
        # Under the arrival time its body was sent with, if it has one - and a line with
        # no time of its own has to have one.
        wrong = at != stamp(body_of(n, per_round, batch)) if at is not None else n in untimed
        if wrong and len(stamps) < 3:
            stamps.append((n, at))
    stray_rows = got.pop(None, 0)

    lines = None
    def show(n):
        global lines
        if lines is None:
            lines = read_lines(args[5])
        return clip(lines[n].decode("utf-8", "replace"))

    missing = sorted(n for n in acked if n not in got)
    twice = sorted(n for n, c in got.items() if c > 1)
    unsent = sorted(n for n in got if n not in acked and n not in in_flight)
    if missing:
        problems.append("%d acknowledged line(s) are not stored, the first line %d: %s"
                        % (len(missing), missing[0] + 1, show(missing[0])))
    if twice:
        problems.append("%d line(s) are stored more than once, the first line %d, %d times: %s"
                        % (len(twice), twice[0] + 1, got[twice[0]], show(twice[0])))
    if unsent:
        problems.append("%d line(s) are stored that were neither acknowledged nor in flight, "
                        "the first line %d: %s" % (len(unsent), unsent[0] + 1, show(unsent[0])))
    if stray_rows:
        problems.append("%d row(s) read back are no line of the stream: %s"
                        % (stray_rows, " | ".join(strays)))
    for n, at in stamps:
        problems.append("line %d came back with %s, and its body was sent with %s"
                        % (n + 1, "_received_at %s" % at if at else "no _received_at",
                           stamp(body_of(n, per_round, batch))))
    if counted != len(rows):
        problems.append("COUNT LOG {} is %s and MATCH LOG {} returned %d rows"
                        % (counted, len(rows)))

    # Of a body that was never acknowledged: all of it or none - and the same at every
    # start. Of one that was refused: none.
    kept_path = os.path.join(state, "kept.json")
    try:
        kept_before = json.load(open(kept_path))
    except OSError:
        kept_before = {}
    extra = 0
    for m in maybe:
        # A line the server would have refused holds no place in the log.
        took = [n for n in range(m["first"], m["first"] + m["count"])
                if keys[16 * n:16 * n + 16] != NOT_A_LINE]
        kept = sum(1 for n in took if n in got)
        extra += kept
        if m["why"] != "unanswered" and kept:
            problems.append("the body at line %d was answered %s, and %d of its lines are "
                            "stored" % (m["first"] + 1, m["why"], kept))
        elif kept not in (0, len(took)):
            problems.append("the body at line %d was never acknowledged, and %d of its %d "
                            "lines are stored: a body is in the log whole or not at all"
                            % (m["first"] + 1, kept, len(took)))
        was = kept_before.setdefault(str(m["first"]), kept)
        if was != kept:
            problems.append("the body at line %d was never acknowledged; the start after its "
                            "kill found %d of its lines stored, and this one finds %d"
                            % (m["first"] + 1, was, kept))
    with open(kept_path, "w") as f:
        json.dump(kept_before, f)

    print(json.dumps({"stored": len(rows), "acked": len(acked), "extra": extra,
                      "in_flight": len(maybe)}))
    for p in problems:
        print(p)
    sys.exit(3 if problems else 0)

else:
    sys.exit("unknown command " + cmd)
PY
py() { python3 "$WORK/crash.py" "$@"; }
field() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"; }

# Written under a temporary name and renamed: a generation cut short must not leave a
# truncated file that later runs trust.
mkdir -p testdata/crash-test
INPUT=testdata/crash-test/stream-${NLINES}-seed${GEN_SEED}.ndjson
if [ ! -s "$INPUT" ]; then
  "$BIN" gen-logs --profile farm --lines "$NLINES" --seed "$GEN_SEED" > "$WORK/farm.ndjson" \
    && "$BIN" gen-logs --lines "$((NLINES / 10 + 1))" --seed "$GEN_SEED" > "$WORK/hostile.ndjson" \
    && py corpus "$WORK/farm.ndjson" "$WORK/hostile.ndjson" "$NLINES" "$INPUT.tmp.$$" \
    && mv "$INPUT.tmp.$$" "$INPUT" || { rm -f "$INPUT.tmp.$$"; exit 2; }
  rm -f "$WORK/farm.ndjson" "$WORK/hostile.ndjson"
fi
KEYS="$WORK/keys"
shape=$(py keys "$INPUT" "$KEYS") || { echo "$shape" >&2; exit 2; }
set -- $shape
echo "stream: $1 lines, no two equal; $2 that do not parse, $3 with no time of their own"
echo "        $ROUNDS rounds of $PER_ROUND lines, $BATCH to a body, $SENDERS senders; seed $SEED"
echo

DATA="$WORK/data"
STATE="$WORK/state"
mkdir -p "$STATE"

# Sets PID in this shell, not a subshell's: `wait` must be able to collect it.
spawn() {  # spawn <logfile>
  KIBBR__STORAGE__WAL_SEGMENT_BYTES=$WAL_SEGMENT_BYTES \
  KIBBR__INGESTOR__BATCH_SIZE=$WAL_BATCH_LINES \
    "$BIN" serve --port 0 --data-dir "$DATA" --max-rows "$MAX_ROWS" \
      --flush-interval-ms "$FLUSH_MS" --row-group-timeout-ms "$ROW_GROUP_MS" 2>"$1" &
  PID=$!
}

PORT=""
start() {  # start <logfile>
  spawn "$1"
  PORT=""
  # Up to 120s: a start after a kill recovers the open files and replays the log first.
  for _ in $(seq 1 1200); do
    PORT=$(grep -o 'http://127\.0\.0\.1:[0-9]*' "$1" 2>/dev/null | head -1 | sed 's/.*://')
    [ -n "$PORT" ] && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if [ -z "$PORT" ]; then
    echo "the server did not start:" >&2
    tail -20 "$1" >&2
    # One that is still running and has said nothing is a start that hangs: killed, or
    # the `wait` below would hang with it.
    kill -KILL "$PID" 2>/dev/null
    wait "$PID" 2>/dev/null
    PID=""
    return 1
  fi
}

# SIGTERM, and up to 60s to exit: a stop that hangs is a failure too. Sets `code` to the
# exit code, or to `hung`.
stop() {
  kill -TERM "$PID"
  for _ in $(seq 1 600); do
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$PID" 2>/dev/null; then
    kill -KILL "$PID"
    wait "$PID" 2>/dev/null
    code=hung
  else
    wait "$PID"
    code=$?
  fi
  PID=""
}

# With COMPACT: one of the days on disk after another, by name, until it is stopped. An
# answer that says the day was rebuilt is a line in $WORK/compacted; one that could not be
# had - the server was killed under it - is nothing.
compactor() {
  while :; do
    day=$(ls "$DATA/data" 2>/dev/null | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' \
      | awk -v seed="$RANDOM" 'BEGIN { srand(seed) } { day[NR] = $0 } END { if (NR) print day[int(rand() * NR) + 1] }')
    if [ -n "$day" ]; then
      "$BIN" compact --day "$day" --data-dir "$DATA" --json 2>/dev/null \
        | grep -o '"compacted":true' >> "$WORK/compacted"
    fi
    sleep 0.05
  done
}

# Stop the compactor, and the compaction it was in the middle of: with the server gone
# that one has the directory to itself, and it is killed as the server was.
stop_compactor() {
  [ -n "$COMPACTOR" ] || return 0
  kill -KILL "$COMPACTOR" 2>/dev/null
  wait "$COMPACTOR" 2>/dev/null
  COMPACTOR=""
  pkill -KILL -f "compact --day .* --data-dir $DATA" 2>/dev/null
  for _ in $(seq 1 200); do
    pgrep -f "compact --day .* --data-dir $DATA" >/dev/null 2>&1 || break
    sleep 0.05
  done
}

# One check of the store against everything acknowledged so far. Sets `stored`, `acked`,
# `extra` and `in_flight`; prints the problems, indented, and returns 1 if there were any.
check() {  # check <label>
  py verify "$PORT" "$KEYS" "$STATE" "$PER_ROUND" "$BATCH" "$INPUT" > "$WORK/verify.out"
  local code=$?
  if [ "$code" != 0 ] && [ "$code" != 3 ]; then
    echo "$1: the check itself failed (exit $code)" >&2
    cat "$WORK/verify.out" >&2
    exit 2
  fi
  head -1 "$WORK/verify.out" > "$WORK/verify.json"
  stored=$(field "$WORK/verify.json" stored)
  acked=$(field "$WORK/verify.json" acked)
  extra=$(field "$WORK/verify.json" extra)
  in_flight=$(field "$WORK/verify.json" in_flight)
  [ "$code" = 0 ] && return 0
  tail -n +2 "$WORK/verify.out" | sed 's/^/    /'
  return 1
}

py plan "$SEED" "$ROUNDS" > "$WORK/plan"
start "$WORK/start.log" || exit 1

covered_rounds=0
applied_rounds=0
deleted_rounds=0
for round in $(seq 1 "$ROUNDS"); do
  set -- $(sed -n "${round}p" "$WORK/plan")
  mode=$1; delay=$2; tear=$3; again=$4; u=$5
  verdict=ok
  notes=""

  # Inside one redirection: once the sender has killed the server, bash says on its own
  # stderr that a child of its was killed, and that line is not this script's to print.
  {
    if [ -n "$COMPACT" ]; then
      compactor &
      COMPACTOR=$!
    fi
    py drive "$PORT" "$PID" "$INPUT" "$(((round - 1) * PER_ROUND))" "$PER_ROUND" "$BATCH" \
       "$SENDERS" "$PACE_MS" "$mode" "$u" "$delay" "$DATA" "$STATE" "$round" \
       > "$WORK/drive.json" 2> "$WORK/drive.trace"
    drove=$?
    wait "$PID"
    code=$?
    PID=""
    stop_compactor
  } 2>/dev/null
  [ "$drove" = 0 ] || { echo "round $round: the sender failed" >&2; cat "$WORK/drive.trace" >&2; exit 2; }
  # 137 is the kill. Anything else is a server that stopped before it was killed.
  [ "$code" = 137 ] || { verdict=FAIL; notes="$notes
    the server exited $code before the kill: $(grep -v '^$' "$WORK"/*.log | tail -1)"; }
  problems=$(python3 -c 'import json,sys; print("\n".join("    " + p for p in json.load(open(sys.argv[1]))["problems"]))' "$WORK/drive.json")
  [ -z "$problems" ] || { verdict=FAIL; notes="$notes
$problems"; }
  how=$(field "$WORK/drive.json" how)

  # The log as the kill left it: the segments the checkpoint had not got behind.
  set -- $(py segments "$DATA")
  first_seg=$1; last_seg=$2

  torn=-
  [ "$tear" = - ] || torn=$(py tear "$DATA" "$tear" "$SEED$round")
  [ "$torn" != none ] || torn=-

  # Killed again while it recovers, before it is asked anything.
  if [ "$again" != - ]; then
    {
      spawn "$WORK/again.$round.log"
      python3 -c 'import sys,time; time.sleep(int(sys.argv[1]) / 1000)' "$again"
      kill -KILL "$PID"
      wait "$PID"
      PID=""
    } 2>/dev/null
    again="${again}ms"
  fi

  if ! start "$WORK/restart.$round.log"; then
    # A start that refuses turns a crash into an outage, and nothing after it can be read.
    echo "round $round: the server did not start after the kill ($how, torn $torn)"
    failed=$((failed + 1))
    break
  fi
  check "round $round" > "$WORK/problems.out" || verdict=FAIL
  set -- $(py wal "$PORT")
  applied=$1; covered=$2; saw_torn=$4
  [ "$covered" = - ] || [ "$covered" = 0 ] || covered_rounds=$((covered_rounds + 1))
  [ "$applied" = - ] || [ "$applied" = 0 ] || applied_rounds=$((applied_rounds + 1))
  # The segments behind the checkpoint were deleted while the killed server ran: a start
  # opens a segment and deletes none.
  [ "$first_seg" = - ] || [ "$first_seg" -le 1 ] || deleted_rounds=$((deleted_rounds + 1))
  # A start handed a torn record must have met one: a helper that tore nothing, or a
  # reader that never looked, would pass every other check here. The start that met it
  # may be the one killed again: it warned, flushed what it put back, and deleted the
  # segment behind its checkpoint - so the start read here finds none, and is right.
  if [ "$torn" != - ] && { [ "$saw_torn" = - ] || [ "$saw_torn" = 0 ]; } \
     && ! grep -q 'torn record' "$WORK/again.$round.log" 2>/dev/null; then
    verdict=FAIL; notes="$notes
    a torn record ($torn) was appended to the log, and no start said it read one"
  fi
  if grep -q "panicked" "$WORK/restart.$round.log"; then
    verdict=FAIL; notes="$notes
    the restart panicked: $(grep panicked "$WORK/restart.$round.log" | head -1)"
  fi

  [ "$verdict" = ok ] || failed=$((failed + 1))
  printf 'round %2d  kill %-11s torn %-5s again %-5s  acked %7s  stored %7s  +%s of %s in flight  replayed %6s  skipped %6s  wal %s..%s  %s\n' \
    "$round" "$how" "$torn" "$again" "$acked" "$stored" "$extra" "$in_flight" \
    "$applied" "$covered" "$first_seg" "$last_seg" "$verdict"
  cat "$WORK/problems.out"
  [ -z "$notes" ] || echo "$notes" | sed '/^$/d'
done

# The clean stop, and the start after it: nothing to replay, and the same rows.
if [ -n "$PID" ]; then
  stop
  if [ "$code" != 0 ]; then
    echo "SIGTERM: the server exited $code, want 0"
    tail -5 "$WORK/restart.$ROUNDS.log" | sed 's/^/    /'
    failed=$((failed + 1))
  fi
  if start "$WORK/clean.log"; then
    verdict=ok
    check "after the clean stop" > "$WORK/problems.out" || verdict=FAIL
    set -- $(py segments "$DATA")
    clean_first=$1; clean_last=$2
    set -- $(py wal "$PORT")
    if [ "$3" != - ] && [ "$3" != 0 ]; then
      verdict=FAIL
      echo "    the start after a clean stop read $3 line(s) back from the log, want 0" >> "$WORK/problems.out"
    fi
    [ "$verdict" = ok ] || failed=$((failed + 1))
    printf 'clean stop, restart                              acked %7s  stored %7s  +%s of %s in flight  replayed %6s  skipped %6s  wal %s..%s  %s\n' \
      "$acked" "$stored" "$extra" "$in_flight" "$1" "$2" "$clean_first" "$clean_last" "$verdict"
    cat "$WORK/problems.out"
    stop
    if [ "$code" != 0 ]; then
      echo "SIGTERM: the last server exited $code, want 0"
      failed=$((failed + 1))
    fi
  else
    failed=$((failed + 1))
  fi
fi

echo
if [ "$failed" = 0 ] && [ "$ROUNDS" -ge 8 ]; then
  # Kills that left a start nothing to skip, or nothing to put back, have not tested it.
  if [ "$covered_rounds" = 0 ]; then
    echo "no start had to skip a line a row group already held: no kill landed between a row group's flush and the checkpoint, or this build has no write-ahead log"
    failed=$((failed + 1))
  fi
  if [ "$applied_rounds" = 0 ]; then
    echo "no start had to put a line back from the log"
    failed=$((failed + 1))
  fi
  if [ "$deleted_rounds" = 0 ]; then
    echo "no kill found the log's first segment gone: the checkpoint never deleted a segment while the server ran"
    failed=$((failed + 1))
  fi
  if [ -n "$COMPACT" ] && [ ! -s "$WORK/compacted" ]; then
    echo "COMPACT was asked for and no day was rebuilt"
    failed=$((failed + 1))
  fi
fi
if [ "$failed" = 0 ]; then
  echo "$ROUNDS rounds: every acknowledged line stored exactly once after kill -9"
  echo "  $covered_rounds start(s) skipped lines a row group already held past the checkpoint, $applied_rounds put lines back from the log, $deleted_rounds kill(s) found segments already deleted behind the checkpoint"
  [ -z "$COMPACT" ] || echo "  $(wc -l < "$WORK/compacted" | tr -d ' ') time(s) a day was rebuilt by name while the rounds were sent"
  exit 0
fi
echo "FAILED: $failed problem(s) in $ROUNDS rounds"
exit 1
