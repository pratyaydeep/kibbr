#!/usr/bin/env python3
"""POST an NDJSON file to /ingest with N concurrent senders.

Usage: http_ingest.py FILE URL SENDERS [BATCH_LINES] [--duration SECONDS]
Prints one JSON line: {"lines":..,"seconds":..,"lines_per_sec":..,"errors":..,"requests":..}
With --duration, loops over the file until the time is up (for the "under load" runs).
"""
import json, sys, threading, time, urllib.request, urllib.error

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    path, url, senders = args[0], args[1], int(args[2])
    batch = int(args[3]) if len(args) > 3 else 2000
    duration = None
    if "--duration" in sys.argv:
        duration = float(sys.argv[sys.argv.index("--duration") + 1])
    with open(path, "rb") as f:
        lines = f.read().split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    batches = [b"\n".join(lines[i:i + batch]) + b"\n" for i in range(0, len(lines), batch)]
    lock = threading.Lock()
    state = {"next": 0, "ingested": 0, "errors": 0, "requests": 0, "shed": 0}
    deadline = time.monotonic() + duration if duration else None

    def worker():
        while True:
            with lock:
                if deadline is None:
                    if state["next"] >= len(batches):
                        return
                    i = state["next"]; state["next"] += 1
                else:
                    if time.monotonic() >= deadline:
                        return
                    i = state["next"] % len(batches); state["next"] += 1
            body = batches[i]
            for attempt in range(50):
                req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/x-ndjson"})
                try:
                    with urllib.request.urlopen(req, timeout=120) as r:
                        d = json.loads(r.read())
                    with lock:
                        state["ingested"] += d.get("ingested", 0)
                        state["errors"] += len(d.get("errors", [])) + d.get("errors_omitted", 0)
                        state["requests"] += 1
                    break
                except urllib.error.HTTPError as e:
                    if e.code == 503:
                        with lock:
                            state["shed"] += 1
                        time.sleep(0.05)
                        continue
                    with lock:
                        state["errors"] += 1; state["requests"] += 1
                    break
                except Exception:
                    with lock:
                        state["errors"] += 1; state["requests"] += 1
                    break

    t0 = time.monotonic()
    ts = [threading.Thread(target=worker) for _ in range(senders)]
    for t in ts: t.start()
    for t in ts: t.join()
    secs = time.monotonic() - t0
    print(json.dumps({"lines": state["ingested"], "seconds": round(secs, 3),
                      "lines_per_sec": round(state["ingested"] / secs, 1) if secs else 0,
                      "errors": state["errors"], "requests": state["requests"], "shed_503": state["shed"],
                      "senders": senders, "batch_lines": batch}))

main()
