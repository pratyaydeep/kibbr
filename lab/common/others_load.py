"""Loading the `uneven` dataset into Elasticsearch, ClickHouse and clp-s, and querying it.

Called by `07_vs_others.sh`, which owns the containers. Everything here is HTTP and
subprocess, so a system that fails to come up records a failure row rather than taking
the script down with it - a comparison missing one row is still worth having.

Elasticsearch needs `@timestamp`: logsdb mode sorts on it and refuses an index without
one. The field is added on the way in, derived from whichever of `ts` / `ts_unix` /
`ts_ms` the line carries, and every ratio below says so - it is 30 bytes per document
that the raw NDJSON does not contain.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from . import datasets

ES = "http://127.0.0.1:9201"
CH = "http://127.0.0.1:8124"
# Matches docker-compose.yml. ClickHouse 26.x answers 401 without them.
CH_USER = "lab"
CH_PASSWORD = "lab"
BULK_DOCS = 5_000


def _req(url: str, data=None, method="GET", ctype="application/json", timeout=600):
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", ctype)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def ch_ping() -> str:
    """ClickHouse's /ping needs no credentials, so readiness and auth stay separate."""
    return f"{CH}/ping"


def wait_for(url: str, tries: int = 120) -> bool:
    for _ in range(tries):
        try:
            _req(url, timeout=5)
            return True
        except Exception:
            time.sleep(2)
    return False


def _timestamp(obj) -> str | None:
    for key in ("ts", "ts_unix", "ts_ms"):
        v = _find(obj, key)
        if v is None:
            continue
        if isinstance(v, str):
            return v
        s = str(int(v))
        if len(s) == 10:
            secs = float(v)
        elif len(s) == 13:
            secs = v / 1000.0
        else:
            continue
        return datetime.fromtimestamp(secs, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return None


def _find(obj, key: str):
    if key in obj:
        return obj[key]
    for v in obj.values():
        if isinstance(v, dict):
            got = _find(v, key)
            if got is not None:
                return got
    return None


# --- Elasticsearch ----------------------------------------------------------------


def es_create(index: str, logsdb: bool) -> None:
    try:
        _req(f"{ES}/{index}", method="DELETE")
    except urllib.error.HTTPError:
        pass
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "-1",
        }
    }
    if logsdb:
        # logsdb defaults to sorting on host.name and @timestamp; the dataset has no
        # host.name, so the sort is set explicitly to the field it does have.
        settings["settings"]["index"] = {
            "mode": "logsdb",
            "sort.field": ["@timestamp"],
            "sort.order": ["desc"],
        }
        settings["mappings"] = {
            "properties": {"@timestamp": {"type": "date"}}
        }
    _req(f"{ES}/{index}", data=json.dumps(settings).encode(), method="PUT")


def es_load(index: str, lines: list) -> float:
    t0 = time.perf_counter()
    action = json.dumps({"index": {}}).encode()
    for s in range(0, len(lines), BULK_DOCS):
        body = bytearray()
        for obj in lines[s : s + BULK_DOCS]:
            body += action + b"\n" + json.dumps(obj).encode() + b"\n"
        _req(
            f"{ES}/{index}/_bulk",
            data=bytes(body),
            method="POST",
            ctype="application/x-ndjson",
        )
    _req(f"{ES}/{index}/_refresh", method="POST")
    _req(f"{ES}/{index}/_forcemerge?max_num_segments=1", method="POST", timeout=3600)
    _req(f"{ES}/{index}/_refresh", method="POST")
    _req(f"{ES}/{index}/_flush", method="POST")
    return time.perf_counter() - t0


def es_size(index: str) -> int:
    d = json.loads(_req(f"{ES}/{index}/_stats/store"))
    return d["indices"][index]["primaries"]["store"]["size_in_bytes"]


def es_searches(index: str, event: str, ident_field: str, ident: str) -> dict:
    queries = {
        "event value": {"query": {"match": {"event": event}}, "size": 0},
        "level = ERROR": {"query": {"match": {"level": "error"}}, "size": 0},
        "one id": {"query": {"match": {ident_field: ident}}, "size": 0},
        "newest 500": {"sort": [{"@timestamp": "desc"}], "size": 500},
    }
    out = {}
    for name, q in queries.items():
        t0 = time.perf_counter()
        try:
            _req(f"{ES}/{index}/_search", data=json.dumps(q).encode(), method="POST")
            out[name] = time.perf_counter() - t0
        except Exception:
            out[name] = None
    return out


# --- ClickHouse --------------------------------------------------------------------


def ch_sql(sql: str, body: bytes | None = None, timeout=3600) -> bytes:
    url = (
        f"{CH}/?user={CH_USER}&password={CH_PASSWORD}"
        f"&query={urllib.parse.quote(sql)}"
    )
    return _req(url, data=body if body is not None else b"", method="POST", timeout=timeout)


def ch_load(lines: list) -> float:
    """A `ts` column beside a `JSON` payload, sorted on `ts`.

    ClickHouse is given the same help Elasticsearch's logsdb gets and TrieDB's day
    partitions get: the table is ordered by time. Leaving the sort key empty would make
    "newest 500" a full scan and the comparison a straw man.
    """
    ch_sql("DROP TABLE IF EXISTS logs")
    ch_sql(
        "CREATE TABLE logs (ts DateTime64(3), payload JSON) "
        "ENGINE = MergeTree ORDER BY ts"
    )
    t0 = time.perf_counter()
    for s in range(0, len(lines), BULK_DOCS):
        rows = []
        for o in lines[s : s + BULK_DOCS]:
            rows.append(
                json.dumps({"ts": o.get("@timestamp", "1970-01-01T00:00:00Z"),
                            "payload": o}).encode()
            )
        ch_sql("INSERT INTO logs FORMAT JSONEachRow", body=b"\n".join(rows))
    ch_sql("OPTIMIZE TABLE logs FINAL", timeout=7200)
    return time.perf_counter() - t0


def ch_size() -> int:
    out = ch_sql(
        "SELECT sum(bytes_on_disk) FROM system.parts WHERE table='logs' AND active"
    )
    return int(out.strip() or 0)


def ch_searches(event: str, ident_field: str, ident: str) -> dict:
    qs = {
        "event value": f"SELECT count() FROM logs WHERE payload.event = '{event}'",
        "level = ERROR": "SELECT count() FROM logs WHERE payload.level = 'error'",
        "one id": f"SELECT count() FROM logs WHERE payload.{ident_field} = '{ident}'",
        "newest 500": "SELECT * FROM logs ORDER BY ts DESC LIMIT 500 FORMAT Null",
    }
    out = {}
    for name, q in qs.items():
        t0 = time.perf_counter()
        try:
            ch_sql(q, timeout=600)
            out[name] = time.perf_counter() - t0
        except Exception:
            out[name] = None
    return out


# --- clp-s --------------------------------------------------------------------------


def clps_run(args: list, timeout=7200) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", "lab_clps"] + args,
        capture_output=True,
        timeout=timeout,
    )


def clps_load(host_input: Path) -> float:
    subprocess.run(["docker", "exec", "lab_clps", "rm", "-rf", "/work/archive"], check=False)
    subprocess.run(
        ["docker", "cp", str(host_input), "lab_clps:/work/input.ndjson"], check=True
    )
    t0 = time.perf_counter()
    r = clps_run(["clp-s", "c", "/work/archive", "/work/input.ndjson"])
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode()[:400])
    return time.perf_counter() - t0


def clps_size() -> int:
    r = clps_run(["du", "-sb", "/work/archive"])
    return int(r.stdout.split()[0]) if r.returncode == 0 else 0


def clps_searches(event: str, ident: str) -> dict:
    qs = {
        "event value": f'event: "{event}"',
        "level = ERROR": 'level: "error"',
        "one id": f'*: "{ident}"',
        "newest 500": "*",
    }
    out = {}
    for name, q in qs.items():
        t0 = time.perf_counter()
        r = clps_run(["clp-s", "s", "/work/archive", q], timeout=900)
        out[name] = time.perf_counter() - t0 if r.returncode == 0 else None
    return out


# --- the dataset --------------------------------------------------------------------


def load_dataset(name: str, size: str):
    """Payload objects with `@timestamp` added, plus the probes the searches use."""
    ds = datasets.ensure(name, size)
    objs = []
    raw_bytes = 0
    for _, payload in datasets.iter_payloads(ds.path):
        raw_bytes += len(payload)
        o = json.loads(payload)
        ts = _timestamp(o)
        if ts:
            o["@timestamp"] = ts
        objs.append(o)

    event = None
    ident_field = None
    ident = None
    for o in objs:
        if event is None and isinstance(o.get("event"), str):
            event = o["event"]
        for f in ("request_id", "trace_id", "order_id", "user_id"):
            v = _find(o, f)
            if ident is None and isinstance(v, (str, int)):
                ident_field, ident = f, str(v)
        if event and ident:
            break
    return ds, objs, raw_bytes, event or "login_ok", ident_field or "request_id", ident or "x"
