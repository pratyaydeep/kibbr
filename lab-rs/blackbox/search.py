#!/usr/bin/env python3
"""Run one TrieQL query against /query N times; report client and server timings.

Usage: search.py URL 'QUERY' N [--duration SECONDS]
Prints one JSON line with p50/p99/max of client wall time (ms), the server's own
duration_ms, and the last answer's rows_scanned / files_opened / row_groups_read / count.
"""
import json, sys, time, urllib.request

def pct(v, p):
    v = sorted(v)
    if not v: return 0
    return v[min(len(v) - 1, int(round(p * (len(v) - 1))))]

def main():
    url, q, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
    duration = float(sys.argv[sys.argv.index("--duration") + 1]) if "--duration" in sys.argv else None
    body = json.dumps({"q": q}).encode()
    client, server, meta = [], [], {}
    deadline = time.monotonic() + duration if duration else None
    i = 0
    while (deadline is None and i < n) or (deadline is not None and time.monotonic() < deadline):
        i += 1
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=300) as r:
            d = json.loads(r.read())
        client.append((time.perf_counter() - t0) * 1000)
        m = d.get("meta", {})
        server.append(m.get("duration_ms", 0))
        meta = {"rows_scanned": m.get("rows_scanned"), "files_opened": m.get("files_opened"),
                "row_groups_read": m.get("row_groups_read"), "count": d.get("count"),
                "rows_returned": m.get("rows_returned"), "templates_matched": m.get("templates_matched")}
    print(json.dumps({"n": len(client),
                      "client_p50_ms": round(pct(client, 0.5), 3), "client_p99_ms": round(pct(client, 0.99), 3),
                      "client_max_ms": round(max(client), 3),
                      "server_p50_ms": round(pct(server, 0.5), 3), "server_p99_ms": round(pct(server, 0.99), 3),
                      **meta}))

main()
