#!/usr/bin/env bash
# A5 - ingest into a small filesystem until it is full. macOS can make a RAM disk without
# root (hdiutil attach ram:// + diskutil erasevolume); if that is refused, ABSENT.
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
BENCH=A5_disk_full
machine_info; ensure_data
MB=${A5_MB:-48}
dev=$(hdiutil attach -nomount ram://$((MB * 2048)) 2>/dev/null | tr -d '[:space:]')
if [[ -z "$dev" ]]; then
  echo "A5: ABSENT - hdiutil attach ram:// refused (no small filesystem without root)"
  csv_row $BENCH ramdisk status 0 absent "hdiutil attach ram:// refused; no loopback or tmpfs without root on macOS"
  exit 0
fi
if ! diskutil erasevolume APFS labrs "$dev" >/dev/null 2>&1; then
  hdiutil detach "$dev" >/dev/null 2>&1
  echo "A5: ABSENT - diskutil erasevolume refused on $dev"
  csv_row $BENCH ramdisk status 0 absent "diskutil erasevolume refused; no small filesystem without root"
  exit 0
fi
MNT=/Volumes/labrs
SERVER_LOG_DIR="$OUT/a5"; mkdir -p "$SERVER_LOG_DIR"
trap 'stop_server; sleep 1; hdiutil detach "$dev" -force >/dev/null 2>&1' EXIT
echo "A5: ${MB}MB APFS RAM disk at $MNT ($dev)"
csv_row $BENCH ramdisk size_mb $MB MB "APFS on hdiutil ram://, no root needed"
for run in 1 2 3; do
  rm -rf "$MNT/data"
  start_server 7750 "$MNT/data" --row-group-timeout-ms 1000 || exit 1
  # Send until the sender sees a failure or the disk is full. One sender, 2000-line
  # batches, looping over the 300k file; stop when /stats shows write failures.
  $PY - "$DATA_300K" 7750 > "$OUT/a5_run$run.json" <<'PYEOF'
import json, sys, time, urllib.request, urllib.error
path, port = sys.argv[1], sys.argv[2]
lines = open(path, "rb").read().split(b"\n"); lines = [l for l in lines if l]
batches = [b"\n".join(lines[i:i+2000]) + b"\n" for i in range(0, len(lines), 2000)]
sent = 0; codes = {}; first_fail = None; t0 = time.time(); i = 0
def stats():
    return json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/stats", timeout=10))
while time.time() - t0 < 240:
    body = batches[i % len(batches)]; i += 1
    req = urllib.request.Request(f"http://127.0.0.1:{port}/ingest", data=body)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            d = json.loads(r.read()); codes[200] = codes.get(200, 0) + 1; sent += d["ingested"]
    except urllib.error.HTTPError as e:
        codes[e.code] = codes.get(e.code, 0) + 1
        if first_fail is None: first_fail = {"code": e.code, "body": e.read()[:300].decode(errors="replace"), "after_lines": sent}
    except Exception as e:
        codes["conn"] = codes.get("conn", 0) + 1
        if first_fail is None: first_fail = {"code": "conn", "body": str(e)[:300], "after_lines": sent}
        break
    if i % 5 == 0:
        try:
            s = stats()["storage"]
            if s["write_failures"] > 0:
                # keep sending a few more batches to see what the sender is told after the disk is full
                for _ in range(10):
                    body = batches[i % len(batches)]; i += 1
                    req = urllib.request.Request(f"http://127.0.0.1:{port}/ingest", data=body)
                    try:
                        with urllib.request.urlopen(req, timeout=60) as r:
                            d = json.loads(r.read()); codes[200] = codes.get(200, 0) + 1; sent += d["ingested"]
                    except urllib.error.HTTPError as e:
                        codes[e.code] = codes.get(e.code, 0) + 1
                        if first_fail is None: first_fail = {"code": e.code, "body": e.read()[:300].decode(errors="replace"), "after_lines": sent}
                break
        except Exception as e:
            first_fail = first_fail or {"code": "stats", "body": str(e)[:200], "after_lines": sent}; break
time.sleep(3)
try:
    st = stats(); health = json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5))
    q = json.load(urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/query", data=json.dumps({"q": "COUNT LOG {}"}).encode(), headers={"Content-Type": "application/json"}), timeout=60))
    server = {"up": True, "health": health, "rows": st["storage"]["rows"], "buffered": st["storage"]["buffered_rows"],
              "write_failures": st["storage"]["write_failures"], "last_write_error": st["storage"]["last_write_error"], "count_query": q["count"]}
except Exception as e:
    server = {"up": False, "err": str(e)[:200]}
print(json.dumps({"lines_accepted": sent, "response_codes": codes, "first_failure": first_fail, "server_after": server, "seconds": round(time.time()-t0, 1)}))
PYEOF
  cat "$OUT/a5_run$run.json"
  df -h "$MNT" | tail -1
  stop_server
  tail -3 "$SERVER_LOG" 2>/dev/null | cut -c1-200
  grep -c 'SIGTERM received' "$SERVER_LOG" | xargs echo "  shutdown log lines seen (SIGTERM received):"
  # Restart on the full disk: does it come up, and how many rows read back?
  if start_server 7750 "$MNT/data" --row-group-timeout-ms 1000; then
    after=$(curl -s -XPOST -H 'content-type: application/json' -d '{"q":"COUNT LOG {}"}' http://127.0.0.1:7750/query | $PY -c 'import json,sys; d=json.load(sys.stdin); print(d.get("count", "ERR"))')
    echo "  after restart: COUNT LOG {} = $after"
  else
    after=-1; echo "  after restart: server did not come up"
  fi
  stop_server
  $PY - "$OUT/a5_run$run.json" "$after" "$run" <<'PYEOF' >> "$OUT/a5_rows.txt"
import json, sys
d = json.load(open(sys.argv[1])); after = sys.argv[2]; run = sys.argv[3]
s = d["server_after"]
print(f'{run} {d["lines_accepted"]} {s.get("rows", -1)} {s.get("buffered", -1)} {s.get("write_failures", -1)} {int(s.get("up", False))} {after} {json.dumps(d["response_codes"])} {json.dumps(d["first_failure"])} {json.dumps(s.get("last_write_error"))}')
PYEOF
done
while read -r run acc rows buf wf up after codes ff lwe; do
  csv_row $BENCH "run_$run" lines_accepted_200 "$acc" lines "lines the sender got a 200 for before/after the disk filled"
  csv_row $BENCH "run_$run" rows_reported "$rows" rows "/stats rows after the disk filled"
  csv_row $BENCH "run_$run" rows_buffered "$buf" rows "/stats buffered_rows (never reached disk)"
  csv_row $BENCH "run_$run" write_failures "$wf" count "/stats write_failures"
  csv_row $BENCH "run_$run" server_up "$up" bool "/health after the disk filled"
  csv_row $BENCH "run_$run" rows_after_restart "$after" rows "COUNT LOG {} on the same data dir after SIGTERM + restart"
  csv_row $BENCH "run_$run" sender_saw 0 text "response codes $codes; first failure $ff; last_write_error $lwe"
done < "$OUT/a5_rows.txt"
rm -f "$OUT/a5_rows.txt"
