#!/usr/bin/env python3
"""Write the JSON-shapes compatibility fixture with a kibbr built from ca6d867.

    python3 tests/data/compat/ca6d867/make.py /path/to/kibbr-built-from-ca6d867

ca6d867 is the last build that stored an array as the string of its JSON text and left an
empty object out of a line's keys. Writes, beside this file:

  data/           the data directory that build left: row groups from a graceful stop
                  (phase A), then lines that are only in the write-ahead log, the server
                  having been killed before it stored them (phase B)
  expected.json   what that same build answers once it has started on a copy of data/
                  and replayed the log: every query of QUERIES, and /templates

tests/storage.rs starts the current build on a copy of data/ and holds it to
expected.json. Nothing here is run by the tests; it is how the fixture was made.
"""
import http.client, json, os, shutil, signal, subprocess, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
TIME_COLUMNS = "ts"

# Phase A: stored in row groups by a graceful stop. Written as text, so an empty object
# and a number's spelling reach the server as they are here.
PHASE_A = [
    # An array, an empty array and a string in one column; an empty object that ca6d867
    # gave no key, beside one with a member, which is another template.
    '{"ts":"2024-01-15T10:30:00Z","level":"info","tags":["auth","login"],"attr":{"storage":{}}}',
    '{"ts":"2024-01-15T10:30:01Z","level":"info","tags":[],"attr":{"storage":{}}}',
    '{"ts":"2024-01-15T10:30:02Z","level":"warn","tags":"auth,login","attr":{"storage":{"bytes":1}}}',
    # A string that spells an array, in the column that holds arrays.
    '{"ts":"2024-01-15T10:30:03Z","level":"warn","tags":"[\\"auth\\",\\"login\\"]"}',
    # An empty object alone under its key: at ca6d867 a line of `ts` only.
    '{"ts":"2024-01-15T10:30:04Z","k":{}}',
    '{"ts":"2024-01-15T10:30:05Z","k":"{}"}',
    '{"ts":"2024-01-15T10:30:06Z","k":[{"a":1.50},null,true,"x"]}',
    '{"ts":"2024-01-15T10:30:07Z","k":7}',
    '{"ts":"2024-01-15T10:30:08Z","k":null}',
    # An array under a promoted key: ca6d867 promoted its text.
    '{"ts":"2024-01-15T10:30:09Z","event":["a"],"n":1}',
    '{"ts":"2024-01-15T10:30:10Z","event":"login","n":2}',
]

# Phase B: in the write-ahead log only.
PHASE_B = [
    '{"ts":"2024-01-15T10:31:00Z","level":"info","tags":["late"],"attr":{"storage":{}}}',
    '{"ts":"2024-01-15T10:31:01Z","level":"info","tags":"late"}',
    '{"ts":"2024-01-15T10:31:02Z","k":{}}',
    '{"ts":"2024-01-15T10:31:03Z","k":[1,2]}',
    '{"ts":"2024-01-15T10:31:04Z","k":"[1,2]"}',
    '{"ts":"2024-01-15T10:31:05Z","event":["a"],"n":3}',
    '{"ts":"2024-01-15T10:31:06Z","event":[],"n":4}',
]

STORED = 'BETWEEN "2024-01-15T10:30:00Z" AND "2024-01-15T10:31:00Z"'
LOGGED = 'BETWEEN "2024-01-15T10:31:00Z" AND "2024-01-15T10:32:00Z"'

QUERIES = [
    "COUNT LOG {}",
    "MATCH LOG {} LIMIT 1000",
    "MATCH LOG {} ORDER BY time ASC LIMIT 1000",
    # The rows ca6d867 stored, and the rows it only logged.
    "MATCH LOG {} " + STORED + " ORDER BY time ASC LIMIT 1000",
    "MATCH LOG {} " + LOGGED + " ORDER BY time ASC LIMIT 1000",
    'MATCH LOG { "tags": "[\\"auth\\",\\"login\\"]" } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "tags": "[]" } LIMIT 1000',
    'MATCH LOG { "tags": "[\\"late\\"]" } LIMIT 1000',
    'MATCH LOG { "tags": * } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "k": "{}" } LIMIT 1000',
    'MATCH LOG { "k": "[1,2]" } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "k": * } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "k": "7" } LIMIT 1000',
    'MATCH LOG { "event": "[\\"a\\"]" } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "event": "login" } LIMIT 1000',
    'MATCH LOG { "event": * } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "n": * } ORDER BY time ASC LIMIT 1000',
    'MATCH LOG { "attr.storage": * } LIMIT 1000',
    'MATCH LOG { "attr.storage.bytes": * } LIMIT 1000',
    'MATCH LOG { "level": "info" } ORDER BY time ASC LIMIT 1000',
    'COUNT LOG { "tags": * }',
    'COUNT LOG { "k": * }',
    'COUNT LOG {} GROUP BY "tags"',
    'COUNT LOG {} GROUP BY "k"',
    'COUNT LOG {} GROUP BY "event"',
    'COUNT LOG {} GROUP BY "level"',
    "COUNT LOG {} " + STORED,
    "COUNT LOG {} " + LOGGED,
    'COUNT LOG { "level": * } ' + STORED + " GROUP BY time(1m)",
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
        body = "".join(l + "\n" for l in lines).encode()
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
