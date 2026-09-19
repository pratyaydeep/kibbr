#!/usr/bin/env bash
# Rebuilds RESULTS.md from results/results.csv. Only the latest run of each benchmark is
# shown; every earlier run stays in the CSV.
cd "$(dirname "${BASH_SOURCE[0]}")"
python3 - <<'PYEOF'
import csv, collections, datetime
rows = list(csv.DictReader(open("results/results.csv")))
by = collections.defaultdict(list)
for r in rows: by[r["bench"]].append(r)

# What each benchmark simplifies, and what decision it informs. Two sentences each.
DESC = {
 "A4_baseline_searches": ("Measures today's binary from outside over the first 300,000 lines of the seed-42 corpus (1M lines cannot drain on this machine, see the ABSENT row) after a clean shutdown and a cold start, so the buffers are empty and every row is read from a segment file. Informs which of the four standard search shapes is slow today and where (files opened versus rows scanned), which is the baseline every design change is judged against.",),
 "A1_where_time_goes": ("Measures today's binary from outside: the CLI has no `ingest`, so the CLI figure is `query FILE --count`, which parses every line into the trie and the in-memory store; the HTTP figure is the rate /ingest answers 200, which is buffered rows, not rows on disk. Informs how long a crash replay of N lines of WAL would take, whether HTTP parsing or the flush to disk is the ingest bottleneck, and whether more senders help.",),
 "A2_memory_and_startup": ("Measures today's binary with 20,000 lines per template count (more lines do not drain on this machine, see A1), the drain wait capped at 120 s, and a ps-sampled peak every 0.2 s. Informs whether per-template buffers, files and startup cost grow linearly with the template count, which decides whether a template cap or a memory budget is needed.",),
 "A3_search_under_load": ("Measures today's binary over a copy of the 300,000-row corpus with 4 senders looping the same 300,000 lines, so the ingest is a steady stream rather than a burst. Informs how much a query pays for the ingest write lock and how much ingest pays for queries, which decides whether the buffer lock design (B3) matters in practice.",),
 "A5_disk_full": ("Uses a RAM disk of a few dozen MB with a 1 s row-group timeout, so it fills in seconds rather than days and the flush path is exercised constantly. Informs what a shipper sees when the disk fills, whether the server survives, and whether rows already written still read back after a restart.",),
 "B10_file_layout_writes": ("Writes pre-built blobs with no encoding and no index; every producer appends a whole row group and calls sync_all once. Informs whether one shared file per day is ever the write bottleneck or whether files sharded by template or by size are needed. Rust's sync_all on macOS is F_FULLFSYNC (about 3 ms a call on this SSD), which is real durability and the floor under every row here.",),
 "B1_wal_batching": ("One writer thread, a real file and a real sync_all, but the WAL record is the raw line with no header and no checksum. Informs which group-commit wait and which acknowledgement mode (after or before the flush) the WAL should use, and whether compressing batches is worth its CPU. The ~4 ms floor on every sender wait is F_FULLFSYNC (Rust's sync_all on macOS); plain fsync(2) here takes 20 us but stops at the drive cache.",),
 "B3_buffer_lock": ("One template buffer with a synthetic 12-column row, a closer that encodes with zstd 3 and fsyncs, and searchers that only snapshot the buffer. Informs whether the close must hold the buffer lock end to end or can move rows aside and publish in one step, and whether the two-step version stays correct.",),
 "B2_appliers": ("Rows arrive pre-parsed from one channel and an applier only appends to per-template column vectors; no trie walk, no type inference. Informs whether one applier thread is enough or K appliers keyed by template pay for their skew.",),
 "B4_last_seen": ("Eight threads updating one integer with no other work around it, so the contention is the whole cost. Informs whether the per-template last-seen stamp should be a locked field, an atomic, or a once-per-batch update.",),
 "B5_filters": ("One million synthetic rows in memory, one filter of two predicates, no I/O and no decoding. Informs whether row-oriented closures or column-oriented plain-data filters should be the evaluation model.",),
 "B6_newest_n": ("Row groups are in-memory vectors with min/max time; opening one costs a zstd decode of its bytes. Informs whether newest-first opening with early stop is worth having, and what compaction that sorts rows by other columns costs the newest-N path.",),
 "B7_scan_threads": ("Decodes 2,000 zstd chunks of synthetic column bytes and does nothing with them. Informs how many threads a scan should use and where the speed-up stops on this machine.",),
 "B8_template_lookup": ("Templates are sets of 8 to 16 synthetic key ids drawn from a pool. Informs whether an inverted map from key to templates is needed at 10,000 and 100,000 templates or a linear check is enough.",),
 "B9_buffer_memory": ("Buffers are column vectors of the synthetic row, memory is the process RSS, and the burst is a rate change with no disk. Informs whether buffers should be pre-sized, grown, or grown and shrunk, and whether a total memory limit with largest-first closing is needed to survive a burst.",),
 "B11_compaction_cost": ("Two hundred small files of zstd row groups on a real disk; the swap crash test is a simulation with a process that stops at chosen points. Informs whether compaction should copy row groups or rebuild them, and whether the parent-name swap rule recovers every row at every stop point.",),
 "B12_encoding_cost": ("Encodes synthetic 8,192-row chunks with hand-written encoders; sizes are not the point (the Python lab has them), the CPU is. Informs whether trying every encoding and keeping the smallest is affordable against one fixed choice per column type.",),
 "B13_recovery_scan": ("An unfinished file of CRC-32C row groups with a synthetic index, walked once at startup. Informs how the file size limit sets the startup cost after a crash.",),
}
ORDER = ["machine","A4_baseline_searches","A1_where_time_goes","A2_memory_and_startup","A3_search_under_load","A5_disk_full",
         "B10_file_layout_writes","B1_wal_batching","B3_buffer_lock","B2_appliers","B5_filters","B6_newest_n","B11_compaction_cost",
         "B13_recovery_scan","B4_last_seen","B7_scan_threads","B8_template_lookup","B9_buffer_memory","B12_encoding_cost"]

FINDINGS = """## Findings

Read with the caveats in each section. Part A numbers are today's binary on this laptop; Part B numbers are models.

**Today's binary**

- **Ingest is single-threaded at ~30k lines/s.** 1, 2, 4 and 8 HTTP senders all land at 29.7-30.5k lines/s; `parse` alone does 35.6k, memory storage 33.6k. The per-body write lock serialises the work, and parsing is most of it. Crash replay estimate: ~34 s per 1,000,000 lines.
- **The flush cannot keep up with one file per template per day.** Every open segment holds a descriptor, and macOS caps a process at 10,240 regardless of `ulimit`. Past ~10k template-days the tick flush logs EMFILE, then `File exists` on every retry, and never drains: the 1M-line corpus and the 100k-line drain are ABSENT for that reason; 20k lines took 99 s to drain (60 s of which is the row-group timeout). Graceful shutdown seals one partition at a time and does write everything: 137 s for 300k rows.
- **Rows are lost across a clean restart.** SIGTERM then restart returned 18,356 / 12,940 / 10,625 of 20,000 accepted rows at 100 / 1,000 / 10,000 templates.
- **Cold start costs ~1 ms per file**: 8.5 s at 16.5k files, 28 s at 27.7k, 39 s at 38k.
- **Searches and ingest starve each other.** With 4 senders, S2 goes from 0.3-1 s to 4.8 s, S1 from 33 ms to 1.1 s, and S4's worst case is 154 s; ingest falls from 44.8k to 1.6k lines/s while searches run.
- **A full disk is silent to the sender.** Every batch got a 200, the server stayed up, the shutdown log still said "90,000 rows written", and after restart `COUNT LOG {}` was 0.

**Design models**

- **B10:** one shared file with `F_FULLFSYNC` per row group sustains ~380 MB/s (270 row groups/s) at any producer count, so one file per day is not the write limit. Sharding by template mod 8 reaches 528 MB/s at 8 producers but leaves files 1.45x uneven; least-bytes placement is even but its global lock costs a third of the throughput; one file per template is 4.7x uneven.
- **B1:** the ~4 ms `F_FULLFSYNC` is the whole shape: a closed-loop sender sees 4 ms at wait 0-1 ms, 9 ms at 5 ms, 27 ms at 20 ms, and throughput scales only with concurrent senders (2.1k lines/s at 8 senders, wait 1 ms). Acknowledging before the flush changes nothing for a closed-loop sender. zstd 3 cuts WAL bytes per line from 253 to 88 at 8 senders with no visible CPU cost.
- **B3:** moving rows aside and publishing in one step beats holding the lock: search p99 164 us versus 8.8 ms, ingest 803k versus 551k rows/s, more closes (291 versus 202) - and zero snapshot violations in either strategy.
- **B2:** one applier appends ~0.9-1.0M rows/s; K appliers keyed by template are no faster (991k at K=2, within noise) and get slower with skew (577k rows/s at K=8, busiest applier 1.48x the mean). One applier is enough for today's ingest rate by a factor of 30.
- **B5:** rows as HashMaps with a closure filter at ~11M rows/s; column vectors at 158-178M rows/s. The representation is the 15x; closure versus plain data is noise at this size.
- **B6:** newest-first with early stop opens 42 of 20,000 row groups in 0.5 ms; read-everything takes 204 ms. Row groups sorted by another column cost 0.2 ms more (sorting each opened group by time) - the compaction idea is affordable for newest-N.
- **B11:** copying row groups merges 200 files in 56 ms; decoding and rebuilding into 8,192-row groups at zstd 19 takes 19.0 s for 14% smaller output. The parent-name swap rule recovered every row exactly once at all four stop points.
- **B13:** an unfinished 1 GB file of CRC-32C row groups is walked and indexed in ~0.3 s with the hardware CRC (3.5-4.6 GB/s, ±45% run to run as the page cache on 8 GB of RAM comes and goes), 0.67 s in software. The file size limit sets startup cost at roughly 0.3 s per GB.
- **B4:** a write lock per line manages 109M updates/s across 8 threads, an atomic 33M/s (fetch_max on one line contends harder than a short critical section here), and once per 500 lines 4.0G/s. Any of them is thousands of times faster than ingest.
- **B7:** zstd decode scales 1.96x at 2 threads, 3.80x at 4, 5.44x at 8 on this 8-core M2 (6.3 GB/s decoded).
- **B8:** checking every template drops from 244k to 1.8k lookups/s between 1,000 and 100,000 templates; a key-to-templates map stays at 547k/s at 100,000. The map is needed from ~10,000 templates.
- **B9:** pre-sizing 10,000 buffers costs 1.06 GB of RSS and cuts ingest to 198k rows/s; growing on demand costs 146 MB at 935k rows/s; shrinking after each close saves nothing here (allocator churn leaves it slightly higher). In the burst, no limit peaked at 689 MB; a 256 MB limit closing the largest buffers first peaked at 423 MB at the same 300k rows/s, with 33,880 forced closes.
- **B12:** trying every encoding and keeping the smallest costs 3.0x the CPU of one fixed choice per column type for 4% fewer bytes. Fixed choices win.
"""
out = []
out.append("# TrieDB lab-rs - speed, locks, memory\n")
out.append("Throwaway benchmarks. Part A measures today's binary from outside; Part B models one design choice each and imports none of TrieDB's code. Every number is the median of 3 runs, `±` is (max-min)/median. `--release` throughout. Nothing here ships.\n")
out.append(f"Regenerated {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} from `results/results.csv` by `report.sh`.\n")
out.append(FINDINGS)
if "machine" in by:
    out.append("## Machine\n")
    out.append("| item | value |\n|---|---|")
    for r in by["machine"]:
        v = r["note"] if r["value"] in ("0",) else f'{r["value"]} {r["unit"]} {r["note"]}'.strip()
        out.append(f'| {r["variant"]} | {v} |')
    out.append("")
def fmt(v):
    try:
        f = float(v)
    except ValueError:
        return v
    if f == int(f) and abs(f) < 1e15: return f"{int(f):,}"
    return f"{f:,.3f}" if abs(f) < 100 else f"{f:,.1f}"
for b in ORDER + sorted(set(by) - set(ORDER)):
    if b == "machine" or b not in by: continue
    # Latest value per (variant, metric): a rerun replaces, a later single row adds.
    seen = {}
    for r in by[b]: seen[(r["variant"], r["metric"])] = r
    rs = list(seen.values())
    latest = max(r["when"] for r in rs)
    out.append(f"## {b}\n")
    if b in DESC: out.append(DESC[b][0] + "\n")
    out.append(f"Run at {latest}, git {rs[0]['git_sha']}.\n")
    spread = {(r["variant"], r["metric"][:-7]): r["value"] for r in rs if r["metric"].endswith("_spread")}
    out.append("| variant | metric | value | unit | note |\n|---|---|---|---|---|")
    for r in rs:
        if r["metric"].endswith("_spread"): continue
        val = fmt(r["value"])
        sp = spread.get((r["variant"], r["metric"]))
        if sp is not None: val += f" ±{float(sp):.0f}%"
        note = r["note"].replace("|", "/")
        if r["unit"] == "absent": val = "ABSENT"
        if r["unit"] == "text": val = ""
        out.append(f'| {r["variant"]} | {r["metric"]} | {val} | {r["unit"] if r["unit"] not in ("absent","text") else ""} | {note} |')
    out.append("")
open("RESULTS.md", "w").write("\n".join(out))
print(f"RESULTS.md: {len(by)} benchmarks")
PYEOF
