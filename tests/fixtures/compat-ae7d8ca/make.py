#!/usr/bin/env python3
"""Write the compatibility fixture with a kibbr built from ae7d8ca.

    python3 tests/fixtures/compat-ae7d8ca/make.py /path/to/kibbr-built-from-ae7d8ca

Writes, beside this file:

  data/           the data directory that build left: row groups from a graceful stop
                  (phase A), then lines that are only in the write-ahead log, the server
                  having been killed before it stored them (phase B)
  expected.json   what that same build answers once it has started on a copy of data/
                  and replayed the log: every query of QUERIES, and /templates

tests/compat.rs starts the current build on a copy of data/ and holds it to
expected.json. Nothing here is run by the tests; it is how the fixture was made.
"""
import http.client, json, os, shutil, signal, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
TIME_COLUMNS = "ts,timestamp,time_us"

# Phase A: stored in row groups by a graceful stop.
PHASE_A = [
    # A column of ten-digit epochs (Datetime) that takes 999999999: String at ae7d8ca.
    # A column of integers that takes 1220305536, ten digits inside the window: String too.
    {"ts": "2024-01-15T10:30:00Z", "level": "info", "msg": "a", "avail": 1705312200, "bytes": 512},
    {"ts": "2024-01-15T10:30:01Z", "level": "info", "msg": "b", "avail": 1705312201, "bytes": 1024},
    {"ts": "2024-01-15T10:30:02Z", "level": "warn", "msg": "c", "avail": 999999999, "bytes": 2048},
    {"ts": "2024-01-15T10:30:03Z", "level": "warn", "msg": "d", "avail": 1705312203, "bytes": 1220305536},
    # A time column holding decimal seconds on a later line: filed under its arrival time.
    {"ts": "1705314600.250000000", "level": "error", "msg": "e", "avail": 1705312204, "bytes": 4096},
    # Time columns in forms ae7d8ca does not read: these templates are on arrival time.
    {"timestamp": "1679711330.570420890", "channel": "OPS", "goalloc": 1220305536},
    {"timestamp": "1679711331.000000001", "channel": "OPS", "goalloc": 7},
    {"time_us": 1732206349000167, "did": "did:plc:a", "kind": "commit"},
    {"time_us": 1732206349000999, "did": "did:plc:b", "kind": "identity"},
    {"timestamp": "2023-03-27 00:26:35.719 EDT", "pid": 27, "message": "starting"},
    # Columns that never widened: a Datetime of epoch digits and an Int64.
    {"ts": "2024-01-15T11:00:00Z", "seen": 1705316400, "count": 42},
    {"ts": "2024-01-15T11:00:01Z", "seen": 1705316401, "count": 43},
]

# Phase B: in the write-ahead log only.
PHASE_B = [
    {"ts": "2024-01-15T10:31:00Z", "level": "info", "msg": "f", "avail": 1705312260, "bytes": 8192},
    {"ts": "2024-01-15T10:31:01Z", "level": "info", "msg": "g", "avail": 5, "bytes": 1705312261},
    {"ts": "1705314601.5", "level": "error", "msg": "h", "avail": 1705312262, "bytes": 16384},
    {"timestamp": "1679711332.25", "channel": "DEV", "goalloc": 9},
    {"time_us": 1732206350000001, "did": "did:plc:c", "kind": "commit"},
    {"timestamp": "2023-03-27 00:26:36.001 EDT", "pid": 28, "message": "ready"},
    {"ts": "2024-01-15T11:00:02Z", "seen": 1705316402, "count": 44},
]

QUERIES = [
    "COUNT LOG {}",
    "MATCH LOG {} LIMIT 1000",
    "MATCH LOG {} ORDER BY time ASC LIMIT 1000",
    'MATCH LOG { "level": "error" } LIMIT 1000',
    'MATCH LOG { "avail": "1705312200" } LIMIT 1000',
    'MATCH LOG { "avail": "999999999" } LIMIT 1000',
    'MATCH LOG { "avail": "5" } LIMIT 1000',
    'MATCH LOG { "avail": "2024-01-15T09:50:00Z" } LIMIT 1000',
    'MATCH LOG { "bytes": "512" } LIMIT 1000',
    'MATCH LOG { "bytes": "1220305536" } LIMIT 1000',
    'MATCH LOG { "bytes": "1705312261" } LIMIT 1000',
    'MATCH LOG { "seen": "1705316400" } LIMIT 1000',
    'MATCH LOG { "seen": "2024-01-15T11:00:02Z" } LIMIT 1000',
    'MATCH LOG { "count": "42" } LIMIT 1000',
    'MATCH LOG { "goalloc": "1220305536" } LIMIT 1000',
    'MATCH LOG { "timestamp": "1679711330.570420890" } LIMIT 1000',
    'MATCH LOG { "time_us": "1732206349000167" } LIMIT 1000',
    'MATCH LOG { "kind": "commit" } LIMIT 1000',
    'MATCH LOG { "channel": "OPS" } ORDER BY time ASC LIMIT 1000',
    'COUNT LOG {} GROUP BY "level"',
    'COUNT LOG {} GROUP BY "avail"',
    'COUNT LOG {} GROUP BY "bytes"',
    'COUNT LOG {} GROUP BY "seen"',
    'COUNT LOG {} GROUP BY "count"',
    'COUNT LOG {} GROUP BY "goalloc"',
    'COUNT LOG { "level": * } BETWEEN "2024-01-15T00:00:00Z" AND "2024-01-16T00:00:00Z" GROUP BY time(1h)',
    'COUNT LOG {} BETWEEN "2024-01-15T00:00:00Z" AND "2024-01-16T00:00:00Z"',
    'MATCH LOG {} BETWEEN "2024-01-15T10:30:00Z" AND "2024-01-15T10:31:00Z" LIMIT 1000',
    # The times those lines name, which ae7d8ca did not read: no row is filed there.
    'COUNT LOG {} BETWEEN "2023-03-01T00:00:00Z" AND "2023-04-01T00:00:00Z"',
    'COUNT LOG {} BETWEEN "2024-11-21T00:00:00Z" AND "2024-11-22T00:00:00Z"',
    'COUNT LOG { "msg": * } BETWEEN "2024-01-15T11:10:00Z" AND "2024-01-15T11:11:00Z"',
]


class Server:
    def __init__(self, binary, data):
        self.child = subprocess.Popen(
            [binary, "serve", "--port", "0", "--data-dir", data, "--time-columns", TIME_COLUMNS],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        self.port = None
        for _ in range(60):
            line = self.child.stderr.readline()
            if not line:
                break
            if "http://127.0.0.1:" in line:
                self.port = int(line.split("http://127.0.0.1:")[1].split()[0].strip("/"))
                break
        if self.port is None:
            raise SystemExit("the server did not say where it listens")

    def call(self, method, path, body=b""):
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        c.request(method, path, body, {"Content-Type": "application/json"})
        r = c.getresponse()
        text = r.read().decode()
        if r.status != 200:
            raise SystemExit(f"{method} {path}: {r.status} {text}")
        return json.loads(text)

    def ingest(self, lines):
        body = "".join(json.dumps(l, separators=(",", ":")) + "\n" for l in lines).encode()
        report = self.call("POST", "/ingest", body)
        assert report["ingested"] == len(lines), report

    def query(self, q):
        full = self.call("POST", "/query", json.dumps({"q": q}).encode())
        return {k: full.get(k) for k in ("exists", "count", "rows", "groups")}

    def templates(self):
        out = []
        for t in self.call("GET", "/templates")["templates"]:
            out.append({
                "template_id": t["template_id"],
                "time_source": t["time_source"],
                "rows": t["rows"],
                "columns": {c["name"]: c["type"] for c in t["columns"]},
            })
        return sorted(out, key=lambda t: t["template_id"])

    def stop(self, sig):
        self.child.send_signal(sig)
        self.child.wait()


def main():
    binary = os.path.abspath(sys.argv[1])
    # Written in a temporary directory and moved here: trie.json records the path each
    # template was first given, and the fixture should not carry a home directory.
    work = tempfile.mkdtemp(prefix="kibbr-compat-")
    data = os.path.join(work, "data")

    first = Server(binary, data)
    first.ingest(PHASE_A)
    first.stop(signal.SIGTERM)

    second = Server(binary, data)
    second.ingest(PHASE_B)
    time.sleep(0.5)
    second.stop(signal.SIGKILL)
    # The killed server's lock: its pid, its port and its token, and nothing a start needs.
    os.remove(os.path.join(data, "LOCK"))

    kept = os.path.join(HERE, "data")
    shutil.rmtree(kept, ignore_errors=True)
    shutil.move(data, kept)
    shutil.rmtree(work)
    data = kept

    with tempfile.TemporaryDirectory() as scratch:
        copy = os.path.join(scratch, "data")
        shutil.copytree(data, copy)
        third = Server(binary, copy)
        expected = {
            "time_columns": TIME_COLUMNS,
            "templates": third.templates(),
            "queries": {q: third.query(q) for q in QUERIES},
        }
        third.stop(signal.SIGKILL)

    with open(os.path.join(HERE, "expected.json"), "w") as f:
        json.dump(expected, f, indent=1, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    main()
