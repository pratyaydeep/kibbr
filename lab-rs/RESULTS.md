# TrieDB lab-rs - speed, locks, memory

Throwaway benchmarks. Part A measures today's binary from outside; Part B models one design choice each and imports none of TrieDB's code. Every number is the median of 3 runs, `±` is (max-min)/median. `--release` throughout. Nothing here ships.

Regenerated 2026-09-19 14:31 UTC from `results/results.csv` by `report.sh`.

## Findings

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

## Machine

| item | value |
|---|---|
| cpu | Apple M2 |
| cores | 8 cores logical; 8 physical |
| ram | 8589934592 bytes |
| filesystem | APFS |
| disk | SSD  |
| kernel | Darwin 23.3.0 |
| rustc | rustc 1.96.1 (31fca3adb 2026-06-26) |
| python | Python 3.12.2 |

## A4_baseline_searches

Measures today's binary from outside over the first 300,000 lines of the seed-42 corpus (1M lines cannot drain on this machine, see the ABSENT row) after a clean shutdown and a cold start, so the buffers are empty and every row is read from a segment file. Informs which of the four standard search shapes is slow today and where (files opened versus rows scanned), which is the baseline every design change is judged against.

Run at 2026-09-19T11:34:44Z, git 8920fb2.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| corpus_1M | status | ABSENT |  | 1M lines do not drain: ~27k template-day segments each hold an open fd; kern.maxfilesperproc=10240 on this Mac; server logged EMFILE, 433 leaves stayed buffered after 25 min. Corpus is 300k lines instead |
| corpus_300k | shutdown_flush_seconds | 137 | s | SIGTERM with ~290k rows still buffered: the final flush writes every leaf, one partition at a time |
| S1 | server_duration_ms | 50.937 ±27% | ms | COUNT LOG {"event":"login_ok"} |
| S1 | client_ms | 69.419 | ms | wall time seen by the HTTP client |
| S1 | rows_scanned | 41,656 | rows |  |
| S1 | files_opened | 144 | files |  |
| S1 | row_groups_read | 3,061 | row_groups |  |
| S1 | count | 41,656 | rows | matching rows (S4: total rows the LIMIT paged over) |
| S2 | server_duration_ms | 649.3 ±10% | ms | COUNT LOG {"level":"error"} |
| S2 | client_ms | 668.7 | ms | wall time seen by the HTTP client |
| S2 | rows_scanned | 150,053 | rows |  |
| S2 | files_opened | 654 | files |  |
| S2 | row_groups_read | 16,028 | row_groups |  |
| S2 | count | 29,967 | rows | matching rows (S4: total rows the LIMIT paged over) |
| S3 | server_duration_ms | 47.163 ±11% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"} |
| S3 | client_ms | 67.419 | ms | wall time seen by the HTTP client |
| S3 | rows_scanned | 24,930 | rows |  |
| S3 | files_opened | 4 | files |  |
| S3 | row_groups_read | 5 | row_groups |  |
| S3 | count | 1 | rows | matching rows (S4: total rows the LIMIT paged over) |
| S4 | server_duration_ms | 29.037 ±73% | ms | MATCH LOG {} LIMIT 500 |
| S4 | client_ms | 54.645 | ms | wall time seen by the HTTP client |
| S4 | rows_scanned | 3,920 | rows |  |
| S4 | files_opened | 870 | files |  |
| S4 | row_groups_read | 300 | row_groups |  |
| S4 | count | 300,000 | rows | matching rows (S4: total rows the LIMIT paged over) |
| S4_via_9200 | status | ABSENT |  | no :9200 _search surface; S4 uses TrieQL LIMIT 500 on :7700 |
| corpus_300k | emfile_lines_in_log | 0 | count | periodic flush ticks that hit kern.maxfilesperproc before the shutdown (none in the 12 s before SIGTERM) |

## A1_where_time_goes

Measures today's binary from outside: the CLI has no `ingest`, so the CLI figure is `query FILE --count`, which parses every line into the trie and the in-memory store; the HTTP figure is the rate /ingest answers 200, which is buffered rows, not rows on disk. Informs how long a crash replay of N lines of WAL would take, whether HTTP parsing or the flush to disk is the ingest bottleneck, and whether more senders help.

Run at 2026-09-19T11:34:38Z, git 8920fb2.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| parse_table | lines_per_sec | 35,630.3 ±1% | lines_per_sec | triedb parse table output > /dev/null, 1M lines |
| parse_json | lines_per_sec | 19,071.6 ±7% | lines_per_sec | triedb parse json output > /dev/null, 1M lines |
| cli_ingest | status | ABSENT |  | no 'triedb ingest' subcommand; measured 'query FILE --count' (parse + trie + in-memory store) instead |
| cli_query_count_load | lines_per_sec | 29,547.3 ±4% | lines_per_sec | query 1m.ndjson --count: every line parsed into trie + memory store |
| cli_query_count_load | seconds_per_1M_lines | 33.844 ±5% | s | estimate of crash replay time per 1,000,000 lines of WAL backlog |
| http_disk_1_senders | lines_per_sec | 29,664.1 ±1% | lines_per_sec | 300k lines, batches of 2000 lines, 1 concurrent senders, --storage disk; rate at which /ingest answered 200 |
| http_disk_2_senders | lines_per_sec | 30,056.9 ±2% | lines_per_sec | 300k lines, batches of 2000 lines, 2 concurrent senders, --storage disk; rate at which /ingest answered 200 |
| http_disk_4_senders | lines_per_sec | 30,530.4 ±1% | lines_per_sec | 300k lines, batches of 2000 lines, 4 concurrent senders, --storage disk; rate at which /ingest answered 200 |
| http_disk_8_senders | lines_per_sec | 30,259.3 ±1% | lines_per_sec | 300k lines, batches of 2000 lines, 8 concurrent senders, --storage disk; rate at which /ingest answered 200 |
| http_memory_4_senders | lines_per_sec | 33,558.4 ±0% | lines_per_sec | 300k lines, batches of 2000 lines, 4 concurrent senders, --storage memory; rate at which /ingest answered 200 |
| drain_100k | status | ABSENT |  | 100k seed-42 lines touch >10,240 template-day segments; each open segment holds a descriptor; the tick flush logs EMFILE forever. 20k lines (6.9k segments) measured instead |
| drain_20k | seconds | 98.670 ±2% | s | 20k lines accepted, then time until /stats buffered_rows==0 |
| drain_20k | segment_files | 6,068 ±0% | files | one segment file per template per day |
| drain_20k | files_per_sec | 61.500 ±2% | files_per_sec | the disk flush rate, in files |
| perf_profile | status | ABSENT |  | macOS: no perf(1); CARGO_PROFILE_RELEASE_DEBUG profile not taken |

## A2_memory_and_startup

Measures today's binary with 20,000 lines per template count (more lines do not drain on this machine, see A1), the drain wait capped at 120 s, and a ps-sampled peak every 0.2 s. Informs whether per-template buffers, files and startup cost grow linearly with the template count, which decides whether a template cap or a memory budget is needed.

Run at 2026-09-19T12:17:00Z, git 7c787eb.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| templates_100 | peak_rss_kb | 98,944 ±7% | kB | ps-sampled every 0.2s during ingest+flush; gen-logs --lines 20000 --templates 100 --seed 42; 4 senders |
| templates_100 | time_l_max_rss_bytes | 103,743,488 ±4% | bytes | /usr/bin/time -l maximum resident set size (whole life incl. shutdown flush) |
| templates_100 | idle_rss_kb | 70,464 ±0% | kB | after buffered_rows==0 |
| templates_100 | files_on_disk | 16,548 ±0% | files | find -type f in the data dir |
| templates_100 | drain_seconds | 119.5 ±3% | s | seconds from last /ingest answer until buffered_rows==0, capped at 180 |
| templates_100 | rows_still_buffered_at_cap | -1 ±-165400% | rows | buffered_rows when the drain wait ended (0 = drained) |
| templates_100 | write_failures | -1 ±-91200% | count | /stats write_failures (EMFILE flush ticks) |
| templates_100 | shutdown_seconds | 28 ±7% | s | SIGTERM until exit: the final flush writes every leaf |
| templates_100 | top_stderr_line |  |  | most frequent server stderr line, run 3: 8940 triedb: SEG: File exists (os error 17) |
| templates_100 | rows_after_restart | 18,356 ±0% | rows | COUNT LOG {} after SIGTERM + restart; 20000 were accepted with a 200 |
| templates_100 | cold_start_seconds | 8.473 ±4% | s | spawn until /health answers, on the data dir just written |
| templates_1000 | peak_rss_kb | 195,424 ±7% | kB | ps-sampled every 0.2s during ingest+flush; gen-logs --lines 20000 --templates 1000 --seed 42; 4 senders |
| templates_1000 | time_l_max_rss_bytes | 231,604,224 ±12% | bytes | /usr/bin/time -l maximum resident set size (whole life incl. shutdown flush) |
| templates_1000 | idle_rss_kb | 101,488 ±32% | kB | after buffered_rows==0 |
| templates_1000 | files_on_disk | 27,650 ±10% | files | find -type f in the data dir |
| templates_1000 | drain_seconds | 127.6 ±0% | s | seconds from last /ingest answer until buffered_rows==0, capped at 180 |
| templates_1000 | rows_still_buffered_at_cap | -1 ±-0% | rows | buffered_rows when the drain wait ended (0 = drained) |
| templates_1000 | write_failures | -1 ±-0% | count | /stats write_failures (EMFILE flush ticks) |
| templates_1000 | shutdown_seconds | 36 ±19% | s | SIGTERM until exit: the final flush writes every leaf |
| templates_1000 | top_stderr_line |  |  | most frequent server stderr line, run 3: 61209 triedb: SEG: File exists (os error 17) |
| templates_1000 | rows_after_restart | 12,940 ±0% | rows | COUNT LOG {} after SIGTERM + restart; 20000 were accepted with a 200 |
| templates_1000 | cold_start_seconds | 27.889 ±5% | s | spawn until /health answers, on the data dir just written |
| templates_10000 | peak_rss_kb | 265,424 ±12% | kB | ps-sampled every 0.2s during ingest+flush; gen-logs --lines 20000 --templates 10000 --seed 42; 4 senders |
| templates_10000 | time_l_max_rss_bytes | 288,636,928 ±5% | bytes | /usr/bin/time -l maximum resident set size (whole life incl. shutdown flush) |
| templates_10000 | idle_rss_kb | 174,544 ±45% | kB | after buffered_rows==0 |
| templates_10000 | files_on_disk | 38,243 ±16% | files | find -type f in the data dir |
| templates_10000 | drain_seconds | 126.2 ±0% | s | seconds from last /ingest answer until buffered_rows==0, capped at 180 |
| templates_10000 | rows_still_buffered_at_cap | 9,375 ±100% | rows | buffered_rows when the drain wait ended (0 = drained) |
| templates_10000 | write_failures | 72,744 ±100% | count | /stats write_failures (EMFILE flush ticks) |
| templates_10000 | shutdown_seconds | 32 ±59% | s | SIGTERM until exit: the final flush writes every leaf |
| templates_10000 | top_stderr_line |  |  | most frequent server stderr line, run 3: 127330 triedb: SEG: File exists (os error 17) |
| templates_10000 | rows_after_restart | 10,625 ±0% | rows | COUNT LOG {} after SIGTERM + restart; 20000 were accepted with a 200 |
| templates_10000 | cold_start_seconds | 38.762 ±17% | s | spawn until /health answers, on the data dir just written |

## A3_search_under_load

Measures today's binary over a copy of the 300,000-row corpus with 4 senders looping the same 300,000 lines, so the ingest is a steady stream rather than a burst. Informs how much a query pays for the ingest write lock and how much ingest pays for queries, which decides whether the buffer lock design (B3) matters in practice.

Run at 2026-09-19T13:22:00Z, git 7c787eb.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| S1_idle | client_p50_ms | 33.053 ±13% | ms | COUNT LOG {"event":"login_ok"}; idle = no ingest |
| S1_idle | client_p99_ms | 196.3 ±78% | ms | COUNT LOG {"event":"login_ok"}; idle = no ingest |
| S1_idle | server_p50_ms | 32.390 ±10% | ms | COUNT LOG {"event":"login_ok"}; idle = no ingest |
| S1_idle | server_p99_ms | 177.8 ±80% | ms | COUNT LOG {"event":"login_ok"}; idle = no ingest |
| S1_loaded | client_p50_ms | 1,133.4 ±36% | ms | COUNT LOG {"event":"login_ok"}; loaded = 4 senders ingesting at full speed |
| S1_loaded | client_p99_ms | 1,266.6 ±40% | ms | COUNT LOG {"event":"login_ok"}; loaded = 4 senders ingesting at full speed |
| S1_loaded | server_p50_ms | 775.6 ±61% | ms | COUNT LOG {"event":"login_ok"}; loaded = 4 senders ingesting at full speed |
| S1_loaded | server_p99_ms | 1,050.4 ±50% | ms | COUNT LOG {"event":"login_ok"}; loaded = 4 senders ingesting at full speed |
| S2_idle | client_p50_ms | 1,022.4 ±73% | ms | COUNT LOG {"level":"error"}; idle = no ingest |
| S2_idle | client_p99_ms | 1,361.9 ±67% | ms | COUNT LOG {"level":"error"}; idle = no ingest |
| S2_idle | server_p50_ms | 1,012.2 ±74% | ms | COUNT LOG {"level":"error"}; idle = no ingest |
| S2_idle | server_p99_ms | 1,345.9 ±68% | ms | COUNT LOG {"level":"error"}; idle = no ingest |
| S2_loaded | client_p50_ms | 4,823.9 ±60% | ms | COUNT LOG {"level":"error"}; loaded = 4 senders ingesting at full speed |
| S2_loaded | client_p99_ms | 5,177.7 ±47% | ms | COUNT LOG {"level":"error"}; loaded = 4 senders ingesting at full speed |
| S2_loaded | server_p50_ms | 4,630.3 ±63% | ms | COUNT LOG {"level":"error"}; loaded = 4 senders ingesting at full speed |
| S2_loaded | server_p99_ms | 4,712.4 ±53% | ms | COUNT LOG {"level":"error"}; loaded = 4 senders ingesting at full speed |
| S3_idle | client_p50_ms | 29.980 ±14% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; idle = no ingest |
| S3_idle | client_p99_ms | 44.398 ±18% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; idle = no ingest |
| S3_idle | server_p50_ms | 29.134 ±14% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; idle = no ingest |
| S3_idle | server_p99_ms | 32.507 ±34% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; idle = no ingest |
| S3_loaded | client_p50_ms | 417.0 ±22% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; loaded = 4 senders ingesting at full speed |
| S3_loaded | client_p99_ms | 837.6 ±18% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; loaded = 4 senders ingesting at full speed |
| S3_loaded | server_p50_ms | 209.2 ±34% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; loaded = 4 senders ingesting at full speed |
| S3_loaded | server_p99_ms | 321.8 ±29% | ms | MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}; loaded = 4 senders ingesting at full speed |
| S4_idle | client_p50_ms | 22.643 ±21% | ms | MATCH LOG {} LIMIT 500; idle = no ingest |
| S4_idle | client_p99_ms | 68.406 ±56% | ms | MATCH LOG {} LIMIT 500; idle = no ingest |
| S4_idle | server_p50_ms | 18.020 ±24% | ms | MATCH LOG {} LIMIT 500; idle = no ingest |
| S4_idle | server_p99_ms | 51.663 ±50% | ms | MATCH LOG {} LIMIT 500; idle = no ingest |
| S4_loaded | client_p50_ms | 4,436.9 ±69% | ms | MATCH LOG {} LIMIT 500; loaded = 4 senders ingesting at full speed |
| S4_loaded | client_p99_ms | 154,013.5 ±37% | ms | MATCH LOG {} LIMIT 500; loaded = 4 senders ingesting at full speed |
| S4_loaded | server_p50_ms | 931.5 ±65% | ms | MATCH LOG {} LIMIT 500; loaded = 4 senders ingesting at full speed |
| S4_loaded | server_p99_ms | 4,045.1 ±72% | ms | MATCH LOG {} LIMIT 500; loaded = 4 senders ingesting at full speed |
| ingest_alone | lines_per_sec | 44,821 ±12% | lines_per_sec | 4 senders, 32s, on top of the 300k-row corpus |
| ingest_with_searches | lines_per_sec | 1,645.4 ±26% | lines_per_sec | 4 senders, 32s, on top of the 300k-row corpus |

## A5_disk_full

Uses a RAM disk of a few dozen MB with a 1 s row-group timeout, so it fills in seconds rather than days and the flush path is exercised constantly. Informs what a shipper sees when the disk fills, whether the server survives, and whether rows already written still read back after a restart.

Run at 2026-09-19T13:50:52Z, git 7c787eb.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| ramdisk | size_mb | 48 | MB | APFS on hdiutil ram://, no root needed |
| run_1 | lines_accepted_200 | 90,000 | lines | lines the sender got a 200 for before/after the disk filled |
| run_1 | rows_reported | 90,000 | rows | /stats rows after the disk filled |
| run_1 | rows_buffered | 17,082 | rows | /stats buffered_rows (never reached disk) |
| run_1 | write_failures | 4,750 | count | /stats write_failures |
| run_1 | server_up | 1 | bool | /health after the disk filled |
| run_1 | rows_after_restart | 0 | rows | COUNT LOG {} on the same data dir after SIGTERM + restart |
| run_1 | sender_saw |  |  | response codes {"200":; first failure 45}; last_write_error null "/Volumes/labrs/data/leaf_672/2024-01-28/seg_000001.col: No space left on device (os error 28): No space left on device (os error 28)" |
| run_2 | lines_accepted_200 | 80,000 | lines | lines the sender got a 200 for before/after the disk filled |
| run_2 | rows_reported | 79,993 | rows | /stats rows after the disk filled |
| run_2 | rows_buffered | 15,323 | rows | /stats buffered_rows (never reached disk) |
| run_2 | write_failures | 4,623 | count | /stats write_failures |
| run_2 | server_up | 1 | bool | /health after the disk filled |
| run_2 | rows_after_restart | 0 | rows | COUNT LOG {} on the same data dir after SIGTERM + restart |
| run_2 | sender_saw |  |  | response codes {"200":; first failure 40}; last_write_error null "/Volumes/labrs/data/leaf_721/2024-02-08/seg_000001.col: No space left on device (os error 28): No space left on device (os error 28)" |
| run_3 | lines_accepted_200 | 90,000 | lines | lines the sender got a 200 for before/after the disk filled |
| run_3 | rows_reported | 90,000 | rows | /stats rows after the disk filled |
| run_3 | rows_buffered | 17,546 | rows | /stats buffered_rows (never reached disk) |
| run_3 | write_failures | 4,763 | count | /stats write_failures |
| run_3 | server_up | 1 | bool | /health after the disk filled |
| run_3 | rows_after_restart | 0 | rows | COUNT LOG {} on the same data dir after SIGTERM + restart |
| run_3 | sender_saw |  |  | response codes {"200":; first failure 45}; last_write_error null "/Volumes/labrs/data/leaf_663/undated/seg_000001.col: No space left on device (os error 28): No space left on device (os error 28)" |

## B10_file_layout_writes

Writes pre-built blobs with no encoding and no index; every producer appends a whole row group and calls sync_all once. Informs whether one shared file per day is ever the write bottleneck or whether files sharded by template or by size are needed. Rust's sync_all on macOS is F_FULLFSYNC (about 3 ms a call on this SSD), which is real durability and the floor under every row here.

Run at 2026-09-19T14:21:43Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| one_shared_file/1_producers | mb_per_sec | 371.9 ±10% | MB_per_sec | sync_all per row group |
| one_shared_file/1_producers | row_groups_per_sec | 267.0 ±10% | rg_per_sec |  |
| one_shared_file/1_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| one_shared_file/2_producers | mb_per_sec | 379.0 ±6% | MB_per_sec | sync_all per row group |
| one_shared_file/2_producers | row_groups_per_sec | 272.1 ±6% | rg_per_sec |  |
| one_shared_file/2_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| one_shared_file/4_producers | mb_per_sec | 380.9 ±19% | MB_per_sec | sync_all per row group |
| one_shared_file/4_producers | row_groups_per_sec | 273.5 ±19% | rg_per_sec |  |
| one_shared_file/4_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| one_shared_file/8_producers | mb_per_sec | 387.9 ±6% | MB_per_sec | sync_all per row group |
| one_shared_file/8_producers | row_groups_per_sec | 278.5 ±6% | rg_per_sec |  |
| one_shared_file/8_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/1_producers | mb_per_sec | 388.7 ±3% | MB_per_sec | sync_all per row group |
| template_mod_4/1_producers | row_groups_per_sec | 279.1 ±3% | rg_per_sec |  |
| template_mod_4/1_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/2_producers | mb_per_sec | 421.5 ±7% | MB_per_sec | sync_all per row group |
| template_mod_4/2_producers | row_groups_per_sec | 302.7 ±7% | rg_per_sec |  |
| template_mod_4/2_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/4_producers | mb_per_sec | 447.3 ±2% | MB_per_sec | sync_all per row group |
| template_mod_4/4_producers | row_groups_per_sec | 321.2 ±2% | rg_per_sec |  |
| template_mod_4/4_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/8_producers | mb_per_sec | 478.5 ±4% | MB_per_sec | sync_all per row group |
| template_mod_4/8_producers | row_groups_per_sec | 343.6 ±4% | rg_per_sec |  |
| template_mod_4/8_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/1_producers | mb_per_sec | 391.3 ±20% | MB_per_sec | sync_all per row group |
| template_mod_8/1_producers | row_groups_per_sec | 280.9 ±20% | rg_per_sec |  |
| template_mod_8/1_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/2_producers | mb_per_sec | 426.4 ±2% | MB_per_sec | sync_all per row group |
| template_mod_8/2_producers | row_groups_per_sec | 306.2 ±2% | rg_per_sec |  |
| template_mod_8/2_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/4_producers | mb_per_sec | 461.1 ±4% | MB_per_sec | sync_all per row group |
| template_mod_8/4_producers | row_groups_per_sec | 331.1 ±4% | rg_per_sec |  |
| template_mod_8/4_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/8_producers | mb_per_sec | 528.2 ±4% | MB_per_sec | sync_all per row group |
| template_mod_8/8_producers | row_groups_per_sec | 379.3 ±4% | rg_per_sec |  |
| template_mod_8/8_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/1_producers | mb_per_sec | 385.3 ±29% | MB_per_sec | sync_all per row group |
| least_bytes_4/1_producers | row_groups_per_sec | 276.7 ±29% | rg_per_sec |  |
| least_bytes_4/1_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/2_producers | mb_per_sec | 236.4 ±6% | MB_per_sec | sync_all per row group |
| least_bytes_4/2_producers | row_groups_per_sec | 169.7 ±6% | rg_per_sec |  |
| least_bytes_4/2_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/4_producers | mb_per_sec | 241.7 ±3% | MB_per_sec | sync_all per row group |
| least_bytes_4/4_producers | row_groups_per_sec | 173.6 ±3% | rg_per_sec |  |
| least_bytes_4/4_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/8_producers | mb_per_sec | 251.8 ±4% | MB_per_sec | sync_all per row group |
| least_bytes_4/8_producers | row_groups_per_sec | 180.8 ±4% | rg_per_sec |  |
| least_bytes_4/8_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/1_producers | mb_per_sec | 226.8 ±1% | MB_per_sec | sync_all per row group |
| least_bytes_8/1_producers | row_groups_per_sec | 162.9 ±1% | rg_per_sec |  |
| least_bytes_8/1_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/2_producers | mb_per_sec | 238.5 ±2% | MB_per_sec | sync_all per row group |
| least_bytes_8/2_producers | row_groups_per_sec | 171.3 ±2% | rg_per_sec |  |
| least_bytes_8/2_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/4_producers | mb_per_sec | 217.1 ±21% | MB_per_sec | sync_all per row group |
| least_bytes_8/4_producers | row_groups_per_sec | 155.9 ±21% | rg_per_sec |  |
| least_bytes_8/4_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/8_producers | mb_per_sec | 267.7 ±14% | MB_per_sec | sync_all per row group |
| least_bytes_8/8_producers | row_groups_per_sec | 192.2 ±14% | rg_per_sec |  |
| least_bytes_8/8_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/1_producers | mb_per_sec | 358.5 ±45% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/1_producers | row_groups_per_sec | 257.4 ±45% | rg_per_sec |  |
| one_file_per_template_500/1_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/2_producers | mb_per_sec | 395.7 ±6% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/2_producers | row_groups_per_sec | 284.1 ±6% | rg_per_sec |  |
| one_file_per_template_500/2_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/4_producers | mb_per_sec | 413.3 ±6% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/4_producers | row_groups_per_sec | 296.7 ±6% | rg_per_sec |  |
| one_file_per_template_500/4_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/8_producers | mb_per_sec | 559.6 ±28% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/8_producers | row_groups_per_sec | 401.8 ±28% | rg_per_sec |  |
| one_file_per_template_500/8_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |

## B1_wal_batching

One writer thread, a real file and a real sync_all, but the WAL record is the raw line with no header and no checksum. Informs which group-commit wait and which acknowledgement mode (after or before the flush) the WAL should use, and whether compressing batches is worth its CPU. The ~4 ms floor on every sender wait is F_FULLFSYNC (Rust's sync_all on macOS); plain fsync(2) here takes 20 us but stops at the drive cache.

Run at 2026-09-19T14:22:24Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| ack_after_flush/wait_0ms/1_senders | lines_per_sec | 318.4 ±10% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/1_senders | saved_wait_p50 | 2,983 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/1_senders | saved_wait_p99 | 8,983 ±8% | us | worst 1 percent |
| ack_after_flush/wait_0ms/1_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_0ms/2_senders | lines_per_sec | 321.0 ±16% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/2_senders | saved_wait_p50 | 5,983 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/2_senders | saved_wait_p99 | 13,751 ±42% | us | worst 1 percent |
| ack_after_flush/wait_0ms/2_senders | wal_bytes_per_line | 252.8 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_0ms/4_senders | lines_per_sec | 720.7 ±13% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/4_senders | saved_wait_p50 | 5,915 ±2% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/4_senders | saved_wait_p99 | 13,463 ±47% | us | worst 1 percent |
| ack_after_flush/wait_0ms/4_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_0ms/8_senders | lines_per_sec | 1,649.4 ±8% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/8_senders | saved_wait_p50 | 5,203 ±17% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/8_senders | saved_wait_p99 | 8,027 ±23% | us | worst 1 percent |
| ack_after_flush/wait_0ms/8_senders | wal_bytes_per_line | 252.8 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/1_senders | lines_per_sec | 257.4 ±7% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/1_senders | saved_wait_p50 | 3,991 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/1_senders | saved_wait_p99 | 8,983 ±39% | us | worst 1 percent |
| ack_after_flush/wait_1ms/1_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/2_senders | lines_per_sec | 500.9 ±2% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/2_senders | saved_wait_p50 | 4,003 ±2% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/2_senders | saved_wait_p99 | 7,903 ±67% | us | worst 1 percent |
| ack_after_flush/wait_1ms/2_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/4_senders | lines_per_sec | 1,008.2 ±4% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/4_senders | saved_wait_p50 | 4,011 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/4_senders | saved_wait_p99 | 7,275 ±69% | us | worst 1 percent |
| ack_after_flush/wait_1ms/4_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/8_senders | lines_per_sec | 2,053.8 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/8_senders | saved_wait_p50 | 4,003 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/8_senders | saved_wait_p99 | 5,507 ±11% | us | worst 1 percent |
| ack_after_flush/wait_1ms/8_senders | wal_bytes_per_line | 252.8 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/1_senders | lines_per_sec | 113.3 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/1_senders | saved_wait_p50 | 8,975 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/1_senders | saved_wait_p99 | 12,975 ±26% | us | worst 1 percent |
| ack_after_flush/wait_5ms/1_senders | wal_bytes_per_line | 252.1 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/2_senders | lines_per_sec | 227.9 ±1% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/2_senders | saved_wait_p50 | 8,991 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/2_senders | saved_wait_p99 | 10,311 ±4% | us | worst 1 percent |
| ack_after_flush/wait_5ms/2_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/4_senders | lines_per_sec | 453.4 ±1% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/4_senders | saved_wait_p50 | 8,991 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/4_senders | saved_wait_p99 | 10,311 ±14% | us | worst 1 percent |
| ack_after_flush/wait_5ms/4_senders | wal_bytes_per_line | 252.8 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/8_senders | lines_per_sec | 897.9 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/8_senders | saved_wait_p50 | 8,999 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/8_senders | saved_wait_p99 | 10,799 ±5% | us | worst 1 percent |
| ack_after_flush/wait_5ms/8_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/1_senders | lines_per_sec | 35.997 ±8% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/1_senders | saved_wait_p50 | 26,895 ±5% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/1_senders | saved_wait_p99 | 55,167 ±88% | us | worst 1 percent |
| ack_after_flush/wait_20ms/1_senders | wal_bytes_per_line | 251.8 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/2_senders | lines_per_sec | 74.436 ±1% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/2_senders | saved_wait_p50 | 27,039 ±2% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/2_senders | saved_wait_p99 | 36,127 ±22% | us | worst 1 percent |
| ack_after_flush/wait_20ms/2_senders | wal_bytes_per_line | 252.0 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/4_senders | lines_per_sec | 149.6 ±1% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/4_senders | saved_wait_p50 | 26,975 ±3% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/4_senders | saved_wait_p99 | 30,367 ±6% | us | worst 1 percent |
| ack_after_flush/wait_20ms/4_senders | wal_bytes_per_line | 252.1 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/8_senders | lines_per_sec | 294.5 ±2% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/8_senders | saved_wait_p50 | 27,119 ±3% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/8_senders | saved_wait_p99 | 30,015 ±15% | us | worst 1 percent |
| ack_after_flush/wait_20ms/8_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/1_senders | lines_per_sec | 114.8 ±0% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/1_senders | saved_wait_p50 | 8,935 ±1% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/1_senders | saved_wait_p99 | 10,327 ±14% | us | worst 1 percent |
| ack_before_flush/wait_5ms/1_senders | wal_bytes_per_line | 252.1 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/2_senders | lines_per_sec | 226.6 ±1% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/2_senders | saved_wait_p50 | 8,959 ±0% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/2_senders | saved_wait_p99 | 13,055 ±26% | us | worst 1 percent |
| ack_before_flush/wait_5ms/2_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/4_senders | lines_per_sec | 460.3 ±1% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/4_senders | saved_wait_p50 | 8,911 ±0% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/4_senders | saved_wait_p99 | 10,359 ±21% | us | worst 1 percent |
| ack_before_flush/wait_5ms/4_senders | wal_bytes_per_line | 252.8 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/8_senders | lines_per_sec | 904.7 ±3% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/8_senders | saved_wait_p50 | 8,959 ±1% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/8_senders | saved_wait_p99 | 11,031 ±68% | us | worst 1 percent |
| ack_before_flush/wait_5ms/8_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/1_senders | lines_per_sec | 112.7 ±2% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/1_senders | saved_wait_p50 | 8,991 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/1_senders | saved_wait_p99 | 12,103 ±22% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/1_senders | wal_bytes_per_line | 206.3 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/2_senders | lines_per_sec | 225.3 ±5% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/2_senders | saved_wait_p50 | 8,991 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/2_senders | saved_wait_p99 | 11,439 ±55% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/2_senders | wal_bytes_per_line | 150.5 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/4_senders | lines_per_sec | 444.9 ±2% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/4_senders | saved_wait_p50 | 8,991 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/4_senders | saved_wait_p99 | 13,767 ±28% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/4_senders | wal_bytes_per_line | 110.4 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/8_senders | lines_per_sec | 898.9 ±2% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/8_senders | saved_wait_p50 | 9,007 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/8_senders | saved_wait_p99 | 10,175 ±15% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/8_senders | wal_bytes_per_line | 88.298 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/1_senders | lines_per_sec | 37.727 ±3% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/1_senders | saved_wait_p50 | 27,007 ±4% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/1_senders | saved_wait_p99 | 29,055 ±21% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/1_senders | wal_bytes_per_line | 205.6 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/2_senders | lines_per_sec | 74.498 ±1% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/2_senders | saved_wait_p50 | 27,055 ±2% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/2_senders | saved_wait_p99 | 29,887 ±18% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/2_senders | wal_bytes_per_line | 150.0 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/4_senders | lines_per_sec | 148.6 ±2% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/4_senders | saved_wait_p50 | 26,927 ±3% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/4_senders | saved_wait_p99 | 30,335 ±32% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/4_senders | wal_bytes_per_line | 110.3 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/8_senders | lines_per_sec | 301.3 ±2% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/8_senders | saved_wait_p50 | 26,863 ±4% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/8_senders | saved_wait_p99 | 31,039 ±16% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/8_senders | wal_bytes_per_line | 88.168 ±0% | bytes | raw line avg 200 B |

## B3_buffer_lock

One template buffer with a synthetic 12-column row, a closer that encodes with zstd 3 and fsyncs, and searchers that only snapshot the buffer. Informs whether the close must hold the buffer lock end to end or can move rows aside and publish in one step, and whether the two-step version stays correct.

Run at 2026-09-19T14:24:31Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| X_hold_lock_for_whole_close | ingest_rows_per_sec | 550,761.5 ±29% | rows_per_sec |  |
| X_hold_lock_for_whole_close | search_wait_p50 | 1 ±0% | us | read lock + snapshot |
| X_hold_lock_for_whole_close | search_wait_p99 | 8,783 ±10% | us | worst 1 percent |
| X_hold_lock_for_whole_close | search_wait_max | 26,143 ±65% | us |  |
| X_hold_lock_for_whole_close | closes | 202 ±29% | count | row groups closed in 3 s |
| X_hold_lock_for_whole_close | snapshots | 59,860 ±29% | count | snapshots taken in 3 s by 4 searchers |
| X_hold_lock_for_whole_close | violations | 0 | count | snapshots that did not see every row exactly once (max over 3 runs) |
| Y_move_aside_publish_in_one_step | ingest_rows_per_sec | 803,327.3 ±10% | rows_per_sec |  |
| Y_move_aside_publish_in_one_step | search_wait_p50 | 8 ±0% | us | read lock + snapshot |
| Y_move_aside_publish_in_one_step | search_wait_p99 | 164 ±22% | us | worst 1 percent |
| Y_move_aside_publish_in_one_step | search_wait_max | 7,611 ±181% | us |  |
| Y_move_aside_publish_in_one_step | closes | 291 ±9% | count | row groups closed in 3 s |
| Y_move_aside_publish_in_one_step | snapshots | 142,198 ±10% | count | snapshots taken in 3 s by 4 searchers |
| Y_move_aside_publish_in_one_step | violations | 0 | count | snapshots that did not see every row exactly once (max over 3 runs) |

## B2_appliers

Rows arrive pre-parsed from one channel and an applier only appends to per-template column vectors; no trie walk, no type inference. Informs whether one applier thread is enough or K appliers keyed by template pay for their skew.

Run at 2026-09-19T14:24:50Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| one_applier | rows_per_sec | 889,111.7 ±20% | rows_per_sec | append to per-template column buffers |
| one_applier | busiest_applier_over_mean | 1 | ratio | rows on the busiest applier / mean (1 = even) |
| 2_appliers_by_template_mod_k | rows_per_sec | 991,403.0 ±4% | rows_per_sec | append to per-template column buffers |
| 2_appliers_by_template_mod_k | busiest_applier_over_mean | 1.000 | ratio | rows on the busiest applier / mean (1 = even) |
| 4_appliers_by_template_mod_k | rows_per_sec | 838,567.0 ±3% | rows_per_sec | append to per-template column buffers |
| 4_appliers_by_template_mod_k | busiest_applier_over_mean | 1.159 | ratio | rows on the busiest applier / mean (1 = even) |
| 8_appliers_by_template_mod_k | rows_per_sec | 576,709.8 ±3% | rows_per_sec | append to per-template column buffers |
| 8_appliers_by_template_mod_k | busiest_applier_over_mean | 1.479 | ratio | rows on the busiest applier / mean (1 = even) |

## B5_filters

One million synthetic rows in memory, one filter of two predicates, no I/O and no decoding. Informs whether row-oriented closures or column-oriented plain-data filters should be the evaluation model.

Run at 2026-09-19T14:25:07Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| rows_hashmap_closure | rows_per_sec | 11,393,291.0 ±63% | rows_per_sec | 44004 matches |
| columns_closure_per_row | rows_per_sec | 177,712,682.0 ±14% | rows_per_sec | 44004 matches |
| columns_plain_data_column_at_a_time | rows_per_sec | 158,010,984.3 ±25% | rows_per_sec | 44004 matches |

## B6_newest_n

Row groups are in-memory vectors with min/max time; opening one costs a zstd decode of its bytes. Informs whether newest-first opening with early stop is worth having, and what compaction that sorts rows by other columns costs the newest-N path.

Run at 2026-09-19T14:25:25Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| rows_sorted_by_time_inside/read_all_then_sort | time_ms | 207.5 ±4% | ms | 20000 of 20000 row groups opened |
| rows_sorted_by_time_inside/read_all_then_sort | row_groups_opened | 20,000 | count |  |
| rows_sorted_by_time_inside/newest_first_stop_early | time_ms | 0.527 ±10% | ms | 42 of 20000 row groups opened |
| rows_sorted_by_time_inside/newest_first_stop_early | row_groups_opened | 42 | count |  |
| rows_sorted_by_other_column_inside/read_all_then_sort | time_ms | 261.2 ±5% | ms | 20000 of 20000 row groups opened |
| rows_sorted_by_other_column_inside/read_all_then_sort | row_groups_opened | 20,000 | count |  |
| rows_sorted_by_other_column_inside/newest_first_stop_early | time_ms | 0.646 ±8% | ms | 42 of 20000 row groups opened |
| rows_sorted_by_other_column_inside/newest_first_stop_early | row_groups_opened | 42 | count |  |

## B11_compaction_cost

Two hundred small files of zstd row groups on a real disk; the swap crash test is a simulation with a process that stops at chosen points. Informs whether compaction should copy row groups or rebuild them, and whether the parent-name swap rule recovers every row at every stop point.

Run at 2026-09-19T14:25:28Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| copy_row_groups_untouched | input_bytes | 34,323,584 | bytes |  |
| copy_row_groups_untouched | seconds | 0.056 ±17% | s |  |
| copy_row_groups_untouched | output_bytes | 34,323,997 | bytes |  |
| copy_row_groups_untouched | row_groups | 1,600 | count |  |
| rebuild_8192_row_groups_zstd19 | input_bytes | 34,323,584 | bytes |  |
| rebuild_8192_row_groups_zstd19 | seconds | 18.971 ±2% | s |  |
| rebuild_8192_row_groups_zstd19 | output_bytes | 29,594,846 | bytes |  |
| rebuild_8192_row_groups_zstd19 | row_groups | 100 | count |  |
| crash_1_new_file_half_written | every_row_exactly_once | 1 | bool | 201 files before startup rule; deleted 0 named as parents, 1 incomplete; 200 files after;  |
| crash_2_new_file_complete_not_renamed | every_row_exactly_once | 1 | bool | 201 files before startup rule; deleted 200 named as parents, 0 incomplete; 1 files after;  |
| crash_3_renamed_no_old_file_removed | every_row_exactly_once | 1 | bool | 201 files before startup rule; deleted 200 named as parents, 0 incomplete; 1 files after;  |
| crash_4_half_the_old_files_removed | every_row_exactly_once | 1 | bool | 101 files before startup rule; deleted 100 named as parents, 0 incomplete; 1 files after;  |

## B13_recovery_scan

An unfinished file of CRC-32C row groups with a synthetic index, walked once at startup. Informs how the file size limit sets the startup cost after a crash.

Run at 2026-09-19T14:26:38Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| 128MB/crc_hardware | scan_ms | 28.909 ±2% | ms | 6255 row groups, torn tail detected |
| 128MB/crc_hardware | mb_per_sec | 4,643.3 | MB_per_sec |  |
| 128MB/crc_software | scan_ms | 83.198 ±1% | ms | 6255 row groups, torn tail detected |
| 128MB/crc_software | mb_per_sec | 1,613.4 | MB_per_sec |  |
| 512MB/crc_hardware | scan_ms | 123.2 ±44% | ms | 25017 row groups, torn tail detected |
| 512MB/crc_hardware | mb_per_sec | 4,356.8 | MB_per_sec |  |
| 512MB/crc_software | scan_ms | 332.5 ±3% | ms | 25017 row groups, torn tail detected |
| 512MB/crc_software | mb_per_sec | 1,614.7 | MB_per_sec |  |
| 1024MB/crc_hardware | scan_ms | 307.9 ±47% | ms | 50034 row groups, torn tail detected |
| 1024MB/crc_hardware | mb_per_sec | 3,487.7 | MB_per_sec |  |
| 1024MB/crc_software | scan_ms | 665.7 ±33% | ms | 50034 row groups, torn tail detected |
| 1024MB/crc_software | mb_per_sec | 1,613.1 | MB_per_sec |  |

## B4_last_seen

Eight threads updating one integer with no other work around it, so the contention is the whole cost. Informs whether the per-template last-seen stamp should be a locked field, an atomic, or a once-per-batch update.

Run at 2026-09-19T14:26:45Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| write_lock_per_line | updates_per_sec | 109,188,043.1 ±3% | updates_per_sec | 8 threads, one template |
| atomic_per_line | updates_per_sec | 33,211,745.5 ±8% | updates_per_sec | 8 threads, one template |
| once_per_500_lines | updates_per_sec | 3,969,041,476.5 ±10% | updates_per_sec | 8 threads, one template |

## B7_scan_threads

Decodes 2,000 zstd chunks of synthetic column bytes and does nothing with them. Informs how many threads a scan should use and where the speed-up stops on this machine.

Run at 2026-09-19T14:26:55Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| 1_threads | chunks_per_sec | 7,052.6 ±3% | chunks_per_sec | zstd decode of 8,192-string column chunks |
| 1_threads | speedup_over_1_thread | 1 | x |  |
| 2_threads | chunks_per_sec | 13,855.5 ±5% | chunks_per_sec | zstd decode of 8,192-string column chunks |
| 2_threads | speedup_over_1_thread | 1.965 | x |  |
| 4_threads | chunks_per_sec | 26,784.5 ±1% | chunks_per_sec | zstd decode of 8,192-string column chunks |
| 4_threads | speedup_over_1_thread | 3.798 | x |  |
| 8_threads | chunks_per_sec | 38,373.8 ±10% | chunks_per_sec | zstd decode of 8,192-string column chunks |
| 8_threads | speedup_over_1_thread | 5.441 | x |  |

## B8_template_lookup

Templates are sets of 8 to 16 synthetic key ids drawn from a pool. Informs whether an inverted map from key to templates is needed at 10,000 and 100,000 templates or a linear check is enough.

Run at 2026-09-19T14:26:57Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| 1000_templates/check_every_template | lookups_per_sec | 244,032.1 ±34% | lookups_per_sec | avg 274 templates per answer |
| 1000_templates/key_to_templates_map | lookups_per_sec | 12,917,810.4 ±4% | lookups_per_sec | avg 274 templates per answer |
| 10000_templates/check_every_template | lookups_per_sec | 18,303.4 ±2% | lookups_per_sec | avg 2824 templates per answer |
| 10000_templates/key_to_templates_map | lookups_per_sec | 4,064,834.1 ±11% | lookups_per_sec | avg 2824 templates per answer |
| 100000_templates/check_every_template | lookups_per_sec | 1,781.4 ±3% | lookups_per_sec | avg 27278 templates per answer |
| 100000_templates/key_to_templates_map | lookups_per_sec | 546,642.2 ±3% | lookups_per_sec | avg 27278 templates per answer |

## B9_buffer_memory

Buffers are column vectors of the synthetic row, memory is the process RSS, and the burst is a rate change with no disk. Informs whether buffers should be pre-sized, grown, or grown and shrunk, and whether a total memory limit with largest-first closing is needed to survive a burst.

Run at 2026-09-19T14:27:17Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| 1000_templates/presized_8192 | rows_per_sec | 949,075.4 ±5% | rows_per_sec |  |
| 1000_templates/presized_8192 | rss_end_bytes | 243,580,928 ±58% | bytes | RSS growth over the run (the 1M generated rows are excluded); fresh process per run |
| 1000_templates/presized_8192 | rss_peak_bytes | 259,964,928 ±9% | bytes | sampled every 100 ms |
| 1000_templates/grow_on_demand | rows_per_sec | 1,016,601.2 ±2% | rows_per_sec |  |
| 1000_templates/grow_on_demand | rss_end_bytes | 119,373,824 ±36% | bytes | RSS growth over the run (the 1M generated rows are excluded); fresh process per run |
| 1000_templates/grow_on_demand | rss_peak_bytes | 143,474,688 ±30% | bytes | sampled every 100 ms |
| 1000_templates/grow_and_shrink | rows_per_sec | 1,005,078.0 ±5% | rows_per_sec |  |
| 1000_templates/grow_and_shrink | rss_end_bytes | 164,511,744 ±34% | bytes | RSS growth over the run (the 1M generated rows are excluded); fresh process per run |
| 1000_templates/grow_and_shrink | rss_peak_bytes | 185,876,480 ±14% | bytes | sampled every 100 ms |
| 10000_templates/presized_8192 | rows_per_sec | 197,798.0 ±4% | rows_per_sec |  |
| 10000_templates/presized_8192 | rss_end_bytes | 988,872,704 ±17% | bytes | RSS growth over the run (the 1M generated rows are excluded); fresh process per run |
| 10000_templates/presized_8192 | rss_peak_bytes | 1,059,913,728 ±6% | bytes | sampled every 100 ms |
| 10000_templates/grow_on_demand | rows_per_sec | 934,453.8 ±6% | rows_per_sec |  |
| 10000_templates/grow_on_demand | rss_end_bytes | 146,063,360 ±28% | bytes | RSS growth over the run (the 1M generated rows are excluded); fresh process per run |
| 10000_templates/grow_on_demand | rss_peak_bytes | 146,063,360 ±25% | bytes | sampled every 100 ms |
| 10000_templates/grow_and_shrink | rows_per_sec | 920,148.4 ±7% | rows_per_sec |  |
| 10000_templates/grow_and_shrink | rss_end_bytes | 163,725,312 ±93% | bytes | RSS growth over the run (the 1M generated rows are excluded); fresh process per run |
| 10000_templates/grow_and_shrink | rss_peak_bytes | 177,733,632 ±23% | bytes | sampled every 100 ms |
| burst/no_memory_limit | rows_per_sec | 298,530.5 ±1% | rows_per_sec | target 300,000; lower means it fell behind |
| burst/no_memory_limit | rss_peak_bytes | 689,356,800 ±4% | bytes | whole-process RSS, fresh process; 870 closes at 8,192 rows, 0 forced by the limit |
| burst/limit_256MB_close_largest_first | rows_per_sec | 300,008.4 ±0% | rows_per_sec | target 300,000; lower means it fell behind |
| burst/limit_256MB_close_largest_first | rss_peak_bytes | 423,149,568 ±5% | bytes | whole-process RSS, fresh process; 328 closes at 8,192 rows, 33880 forced by the limit |

## B12_encoding_cost

Encodes synthetic 8,192-row chunks with hand-written encoders; sizes are not the point (the Python lab has them), the CPU is. Informs whether trying every encoding and keeping the smallest is affordable against one fixed choice per column type.

Run at 2026-09-19T14:31:11Z, git 30951fe.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| plain | rows_per_sec | 50,858,295.8 ±11% | rows_per_sec | 300 column-chunks applicable of 300 |
| plain | bytes_per_row_applicable_columns | 75.255 | bytes | size over the columns the encoding applies to |
| plain+zstd3 | rows_per_sec | 4,394,586.5 ±7% | rows_per_sec | 300 column-chunks applicable of 300 |
| plain+zstd3 | bytes_per_row_applicable_columns | 31.034 | bytes | size over the columns the encoding applies to |
| rle_same_value_once | rows_per_sec | 39,127,992.2 ±5% | rows_per_sec | 300 column-chunks applicable of 300 |
| rle_same_value_once | bytes_per_row_applicable_columns | 71.412 | bytes | size over the columns the encoding applies to |
| rle_same_value_once+zstd3 | rows_per_sec | 4,561,594.4 ±10% | rows_per_sec | 300 column-chunks applicable of 300 |
| rle_same_value_once+zstd3 | bytes_per_row_applicable_columns | 31.398 | bytes | size over the columns the encoding applies to |
| dict_word_list | rows_per_sec | 9,903,537.7 ±18% | rows_per_sec | 100 column-chunks applicable of 300 |
| dict_word_list | bytes_per_row_applicable_columns | 105.0 | bytes | size over the columns the encoding applies to |
| dict_word_list+zstd3 | rows_per_sec | 5,030,856.9 ±2% | rows_per_sec | 100 column-chunks applicable of 300 |
| dict_word_list+zstd3 | bytes_per_row_applicable_columns | 58.066 | bytes | size over the columns the encoding applies to |
| delta_timestamp_diff | rows_per_sec | 269,325,909.3 ±155% | rows_per_sec | 150 column-chunks applicable of 300 |
| delta_timestamp_diff | bytes_per_row_applicable_columns | 48 | bytes | size over the columns the encoding applies to |
| delta_timestamp_diff+zstd3 | rows_per_sec | 21,217,759.5 ±1% | rows_per_sec | 150 column-chunks applicable of 300 |
| delta_timestamp_diff+zstd3 | bytes_per_row_applicable_columns | 9.218 | bytes | size over the columns the encoding applies to |
| varint | rows_per_sec | 128,894,568.6 ±1% | rows_per_sec | 150 column-chunks applicable of 300 |
| varint | bytes_per_row_applicable_columns | 23.577 | bytes | size over the columns the encoding applies to |
| varint+zstd3 | rows_per_sec | 33,127,767.6 ±15% | rows_per_sec | 150 column-chunks applicable of 300 |
| varint+zstd3 | bytes_per_row_applicable_columns | 9.107 | bytes | size over the columns the encoding applies to |
| delta_varint | rows_per_sec | 137,578,563.5 ±50% | rows_per_sec | 150 column-chunks applicable of 300 |
| delta_varint | bytes_per_row_applicable_columns | 11.779 | bytes | size over the columns the encoding applies to |
| delta_varint+zstd3 | rows_per_sec | 49,927,625.7 ±4% | rows_per_sec | 150 column-chunks applicable of 300 |
| delta_varint+zstd3 | bytes_per_row_applicable_columns | 7.891 | bytes | size over the columns the encoding applies to |
| fixed_choice_per_column_type | rows_per_sec | 4,055,027.3 ±13% | rows_per_sec |  |
| fixed_choice_per_column_type | bytes_total | 12,633,336 | bytes |  |
| fixed_choice_per_column_type | size_vs_fixed | 1 | ratio |  |
| fixed_choice_per_column_type | cpu_vs_fixed | 1 | x | encode time relative to the fixed choice |
| try_all_keep_smallest | rows_per_sec | 1,348,406.3 ±4% | rows_per_sec |  |
| try_all_keep_smallest | bytes_total | 12,121,258 | bytes |  |
| try_all_keep_smallest | size_vs_fixed | 0.960 | ratio |  |
| try_all_keep_smallest | cpu_vs_fixed | 3.007 | x | encode time relative to the fixed choice |
