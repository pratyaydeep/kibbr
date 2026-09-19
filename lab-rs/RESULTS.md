# TrieDB lab-rs - speed, locks, memory

Throwaway benchmarks. Part A measures today's binary from outside; Part B models one design choice each and imports none of TrieDB's code. Every number is the median of 3 runs, `±` is (max-min)/median. `--release` throughout. Nothing here ships.

Regenerated 2026-09-19 13:51 UTC from `results/results.csv` by `report.sh`.

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

Run at 2026-09-19T13:35:07Z, git 7c787eb.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| one_shared_file/1_producers | mb_per_sec | 404.1 ±7% | MB_per_sec | sync_all per row group |
| one_shared_file/1_producers | row_groups_per_sec | 290.2 ±7% | rg_per_sec |  |
| one_shared_file/1_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| one_shared_file/2_producers | mb_per_sec | 384.1 ±17% | MB_per_sec | sync_all per row group |
| one_shared_file/2_producers | row_groups_per_sec | 275.8 ±17% | rg_per_sec |  |
| one_shared_file/2_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| one_shared_file/4_producers | mb_per_sec | 392.7 ±30% | MB_per_sec | sync_all per row group |
| one_shared_file/4_producers | row_groups_per_sec | 282.0 ±30% | rg_per_sec |  |
| one_shared_file/4_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| one_shared_file/8_producers | mb_per_sec | 390.7 ±15% | MB_per_sec | sync_all per row group |
| one_shared_file/8_producers | row_groups_per_sec | 280.6 ±15% | rg_per_sec |  |
| one_shared_file/8_producers | max_file_over_mean | 1 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/1_producers | mb_per_sec | 410.8 ±7% | MB_per_sec | sync_all per row group |
| template_mod_4/1_producers | row_groups_per_sec | 295.0 ±7% | rg_per_sec |  |
| template_mod_4/1_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/2_producers | mb_per_sec | 435.8 ±1% | MB_per_sec | sync_all per row group |
| template_mod_4/2_producers | row_groups_per_sec | 312.9 ±1% | rg_per_sec |  |
| template_mod_4/2_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/4_producers | mb_per_sec | 463.0 ±32% | MB_per_sec | sync_all per row group |
| template_mod_4/4_producers | row_groups_per_sec | 332.4 ±32% | rg_per_sec |  |
| template_mod_4/4_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_4/8_producers | mb_per_sec | 472.8 ±1% | MB_per_sec | sync_all per row group |
| template_mod_4/8_producers | row_groups_per_sec | 339.5 ±1% | rg_per_sec |  |
| template_mod_4/8_producers | max_file_over_mean | 1.276 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/1_producers | mb_per_sec | 400.1 ±9% | MB_per_sec | sync_all per row group |
| template_mod_8/1_producers | row_groups_per_sec | 287.3 ±9% | rg_per_sec |  |
| template_mod_8/1_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/2_producers | mb_per_sec | 445.3 ±4% | MB_per_sec | sync_all per row group |
| template_mod_8/2_producers | row_groups_per_sec | 319.7 ±4% | rg_per_sec |  |
| template_mod_8/2_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/4_producers | mb_per_sec | 477.1 ±3% | MB_per_sec | sync_all per row group |
| template_mod_8/4_producers | row_groups_per_sec | 342.6 ±3% | rg_per_sec |  |
| template_mod_8/4_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| template_mod_8/8_producers | mb_per_sec | 537.5 ±26% | MB_per_sec | sync_all per row group |
| template_mod_8/8_producers | row_groups_per_sec | 386.0 ±26% | rg_per_sec |  |
| template_mod_8/8_producers | max_file_over_mean | 1.450 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/1_producers | mb_per_sec | 221.2 ±23% | MB_per_sec | sync_all per row group |
| least_bytes_4/1_producers | row_groups_per_sec | 158.8 ±23% | rg_per_sec |  |
| least_bytes_4/1_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/2_producers | mb_per_sec | 239.6 ±1% | MB_per_sec | sync_all per row group |
| least_bytes_4/2_producers | row_groups_per_sec | 172.0 ±1% | rg_per_sec |  |
| least_bytes_4/2_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/4_producers | mb_per_sec | 237.5 ±12% | MB_per_sec | sync_all per row group |
| least_bytes_4/4_producers | row_groups_per_sec | 170.5 ±12% | rg_per_sec |  |
| least_bytes_4/4_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_4/8_producers | mb_per_sec | 241.7 ±3% | MB_per_sec | sync_all per row group |
| least_bytes_4/8_producers | row_groups_per_sec | 173.6 ±3% | rg_per_sec |  |
| least_bytes_4/8_producers | max_file_over_mean | 1.044 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/1_producers | mb_per_sec | 223.5 ±3% | MB_per_sec | sync_all per row group |
| least_bytes_8/1_producers | row_groups_per_sec | 160.5 ±3% | rg_per_sec |  |
| least_bytes_8/1_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/2_producers | mb_per_sec | 237.6 ±1% | MB_per_sec | sync_all per row group |
| least_bytes_8/2_producers | row_groups_per_sec | 170.6 ±1% | rg_per_sec |  |
| least_bytes_8/2_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/4_producers | mb_per_sec | 249.8 ±5% | MB_per_sec | sync_all per row group |
| least_bytes_8/4_producers | row_groups_per_sec | 179.4 ±5% | rg_per_sec |  |
| least_bytes_8/4_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| least_bytes_8/8_producers | mb_per_sec | 266.9 ±5% | MB_per_sec | sync_all per row group |
| least_bytes_8/8_producers | row_groups_per_sec | 191.7 ±5% | rg_per_sec |  |
| least_bytes_8/8_producers | max_file_over_mean | 1.100 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/1_producers | mb_per_sec | 222.1 ±59% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/1_producers | row_groups_per_sec | 159.5 ±59% | rg_per_sec |  |
| one_file_per_template_500/1_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/2_producers | mb_per_sec | 401.6 ±17% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/2_producers | row_groups_per_sec | 288.4 ±17% | rg_per_sec |  |
| one_file_per_template_500/2_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/4_producers | mb_per_sec | 424.9 ±5% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/4_producers | row_groups_per_sec | 305.1 ±5% | rg_per_sec |  |
| one_file_per_template_500/4_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |
| one_file_per_template_500/8_producers | mb_per_sec | 539.6 ±8% | MB_per_sec | sync_all per row group |
| one_file_per_template_500/8_producers | row_groups_per_sec | 387.5 ±8% | rg_per_sec |  |
| one_file_per_template_500/8_producers | max_file_over_mean | 4.740 | ratio | largest file / mean of non-empty files (1 = even) |

## B1_wal_batching

One writer thread, a real file and a real sync_all, but the WAL record is the raw line with no header and no checksum. Informs which group-commit wait and which acknowledgement mode (after or before the flush) the WAL should use, and whether compressing batches is worth its CPU. The ~4 ms floor on every sender wait is F_FULLFSYNC (Rust's sync_all on macOS); plain fsync(2) here takes 20 us but stops at the drive cache.

Run at 2026-09-19T13:35:48Z, git 7c787eb.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| ack_after_flush/wait_0ms/1_senders | lines_per_sec | 193.6 ±151% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/1_senders | saved_wait_p50 | 3,943 ±246% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/1_senders | saved_wait_p99 | 20,447 ±127% | us | worst 1 percent |
| ack_after_flush/wait_0ms/1_senders | wal_bytes_per_line | 252.5 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_0ms/2_senders | lines_per_sec | 383.3 ±4% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/2_senders | saved_wait_p50 | 5,967 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/2_senders | saved_wait_p99 | 8,855 ±18% | us | worst 1 percent |
| ack_after_flush/wait_0ms/2_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_0ms/4_senders | lines_per_sec | 747.4 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/4_senders | saved_wait_p50 | 5,955 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/4_senders | saved_wait_p99 | 9,903 ±22% | us | worst 1 percent |
| ack_after_flush/wait_0ms/4_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_0ms/8_senders | lines_per_sec | 1,629.3 ±8% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_0ms/8_senders | saved_wait_p50 | 5,407 ±13% | us | sender's wait for the ack |
| ack_after_flush/wait_0ms/8_senders | saved_wait_p99 | 7,575 ±23% | us | worst 1 percent |
| ack_after_flush/wait_0ms/8_senders | wal_bytes_per_line | 252.8 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/1_senders | lines_per_sec | 255.7 ±4% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/1_senders | saved_wait_p50 | 3,995 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/1_senders | saved_wait_p99 | 5,515 ±68% | us | worst 1 percent |
| ack_after_flush/wait_1ms/1_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/2_senders | lines_per_sec | 507.9 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/2_senders | saved_wait_p50 | 3,999 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/2_senders | saved_wait_p99 | 5,983 ±2% | us | worst 1 percent |
| ack_after_flush/wait_1ms/2_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/4_senders | lines_per_sec | 1,006.7 ±2% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/4_senders | saved_wait_p50 | 3,995 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/4_senders | saved_wait_p99 | 7,075 ±33% | us | worst 1 percent |
| ack_after_flush/wait_1ms/4_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_1ms/8_senders | lines_per_sec | 1,991.0 ±6% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_1ms/8_senders | saved_wait_p50 | 4,017 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_1ms/8_senders | saved_wait_p99 | 8,059 ±37% | us | worst 1 percent |
| ack_after_flush/wait_1ms/8_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/1_senders | lines_per_sec | 110.9 ±2% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/1_senders | saved_wait_p50 | 8,983 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/1_senders | saved_wait_p99 | 18,351 ±41% | us | worst 1 percent |
| ack_after_flush/wait_5ms/1_senders | wal_bytes_per_line | 252.2 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/2_senders | lines_per_sec | 224.3 ±1% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/2_senders | saved_wait_p50 | 8,999 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/2_senders | saved_wait_p99 | 11,143 ±45% | us | worst 1 percent |
| ack_after_flush/wait_5ms/2_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/4_senders | lines_per_sec | 434.9 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/4_senders | saved_wait_p50 | 9,015 ±0% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/4_senders | saved_wait_p99 | 13,999 ±65% | us | worst 1 percent |
| ack_after_flush/wait_5ms/4_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_5ms/8_senders | lines_per_sec | 857.9 ±2% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_5ms/8_senders | saved_wait_p50 | 9,031 ±1% | us | sender's wait for the ack |
| ack_after_flush/wait_5ms/8_senders | saved_wait_p99 | 15,703 ±18% | us | worst 1 percent |
| ack_after_flush/wait_5ms/8_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/1_senders | lines_per_sec | 37.258 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/1_senders | saved_wait_p50 | 26,991 ±3% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/1_senders | saved_wait_p99 | 37,567 ±18% | us | worst 1 percent |
| ack_after_flush/wait_20ms/1_senders | wal_bytes_per_line | 251.6 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/2_senders | lines_per_sec | 73.996 ±1% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/2_senders | saved_wait_p50 | 27,215 ±4% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/2_senders | saved_wait_p99 | 32,991 ±4% | us | worst 1 percent |
| ack_after_flush/wait_20ms/2_senders | wal_bytes_per_line | 252.2 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/4_senders | lines_per_sec | 148.1 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/4_senders | saved_wait_p50 | 26,991 ±3% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/4_senders | saved_wait_p99 | 36,191 ±7% | us | worst 1 percent |
| ack_after_flush/wait_20ms/4_senders | wal_bytes_per_line | 252.1 ±0% | bytes | raw line avg 200 B |
| ack_after_flush/wait_20ms/8_senders | lines_per_sec | 292.0 ±3% | lines_per_sec | closed-loop senders |
| ack_after_flush/wait_20ms/8_senders | saved_wait_p50 | 27,535 ±3% | us | sender's wait for the ack |
| ack_after_flush/wait_20ms/8_senders | saved_wait_p99 | 43,999 ±29% | us | worst 1 percent |
| ack_after_flush/wait_20ms/8_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/1_senders | lines_per_sec | 110.9 ±8% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/1_senders | saved_wait_p50 | 8,991 ±1% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/1_senders | saved_wait_p99 | 20,047 ±129% | us | worst 1 percent |
| ack_before_flush/wait_5ms/1_senders | wal_bytes_per_line | 252.1 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/2_senders | lines_per_sec | 224.2 ±1% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/2_senders | saved_wait_p50 | 8,967 ±0% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/2_senders | saved_wait_p99 | 12,247 ±7% | us | worst 1 percent |
| ack_before_flush/wait_5ms/2_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/4_senders | lines_per_sec | 441.5 ±1% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/4_senders | saved_wait_p50 | 9,007 ±0% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/4_senders | saved_wait_p99 | 13,311 ±21% | us | worst 1 percent |
| ack_before_flush/wait_5ms/4_senders | wal_bytes_per_line | 252.7 ±0% | bytes | raw line avg 200 B |
| ack_before_flush/wait_5ms/8_senders | lines_per_sec | 885.6 ±2% | lines_per_sec | closed-loop senders |
| ack_before_flush/wait_5ms/8_senders | saved_wait_p50 | 9,023 ±0% | us | sender's wait for the ack |
| ack_before_flush/wait_5ms/8_senders | saved_wait_p99 | 10,903 ±54% | us | worst 1 percent |
| ack_before_flush/wait_5ms/8_senders | wal_bytes_per_line | 252.6 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/1_senders | lines_per_sec | 112.0 ±3% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/1_senders | saved_wait_p50 | 8,983 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/1_senders | saved_wait_p99 | 18,879 ±62% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/1_senders | wal_bytes_per_line | 206.3 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/2_senders | lines_per_sec | 222.1 ±1% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/2_senders | saved_wait_p50 | 8,991 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/2_senders | saved_wait_p99 | 15,423 ±27% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/2_senders | wal_bytes_per_line | 150.5 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/4_senders | lines_per_sec | 443.7 ±1% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/4_senders | saved_wait_p50 | 9,015 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/4_senders | saved_wait_p99 | 14,399 ±25% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/4_senders | wal_bytes_per_line | 110.4 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_5ms/8_senders | lines_per_sec | 875.8 ±2% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_5ms/8_senders | saved_wait_p50 | 9,023 ±0% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_5ms/8_senders | saved_wait_p99 | 11,559 ±31% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_5ms/8_senders | wal_bytes_per_line | 88.346 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/1_senders | lines_per_sec | 37.129 ±1% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/1_senders | saved_wait_p50 | 27,039 ±1% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/1_senders | saved_wait_p99 | 31,327 ±125% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/1_senders | wal_bytes_per_line | 205.7 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/2_senders | lines_per_sec | 75.269 ±3% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/2_senders | saved_wait_p50 | 26,863 ±5% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/2_senders | saved_wait_p99 | 30,079 ±19% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/2_senders | wal_bytes_per_line | 150 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/4_senders | lines_per_sec | 147.8 ±1% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/4_senders | saved_wait_p50 | 27,071 ±1% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/4_senders | saved_wait_p99 | 34,335 ±20% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/4_senders | wal_bytes_per_line | 110.4 ±0% | bytes | raw line avg 200 B |
| zstd3_ack_after_flush/wait_20ms/8_senders | lines_per_sec | 297.8 ±1% | lines_per_sec | closed-loop senders |
| zstd3_ack_after_flush/wait_20ms/8_senders | saved_wait_p50 | 26,927 ±3% | us | sender's wait for the ack |
| zstd3_ack_after_flush/wait_20ms/8_senders | saved_wait_p99 | 30,287 ±32% | us | worst 1 percent |
| zstd3_ack_after_flush/wait_20ms/8_senders | wal_bytes_per_line | 88.112 ±0% | bytes | raw line avg 200 B |

## B3_buffer_lock

One template buffer with a synthetic 12-column row, a closer that encodes with zstd 3 and fsyncs, and searchers that only snapshot the buffer. Informs whether the close must hold the buffer lock end to end or can move rows aside and publish in one step, and whether the two-step version stays correct.

Run at 2026-09-19T13:37:56Z, git 7c787eb.

| variant | metric | value | unit | note |
|---|---|---|---|---|
| X_hold_lock_for_whole_close | ingest_rows_per_sec | 548,383.2 ±2% | rows_per_sec |  |
| X_hold_lock_for_whole_close | search_wait_p50 | 1 ±0% | us | read lock + snapshot |
| X_hold_lock_for_whole_close | search_wait_p99 | 9,223 ±7% | us | worst 1 percent |
| X_hold_lock_for_whole_close | search_wait_max | 16,863 ±16% | us |  |
| X_hold_lock_for_whole_close | closes | 201 ±1% | count | row groups closed in 3 s |
| X_hold_lock_for_whole_close | snapshots | 59,275 ±1% | count | snapshots taken in 3 s by 4 searchers |
| X_hold_lock_for_whole_close | violations | 0 | count | snapshots that did not see every row exactly once (max over 3 runs) |
| Y_move_aside_publish_in_one_step | ingest_rows_per_sec | 785,862.0 ±7% | rows_per_sec |  |
| Y_move_aside_publish_in_one_step | search_wait_p50 | 8 ±12% | us | read lock + snapshot |
| Y_move_aside_publish_in_one_step | search_wait_p99 | 165 ±33% | us | worst 1 percent |
| Y_move_aside_publish_in_one_step | search_wait_max | 4,787 ±136% | us |  |
| Y_move_aside_publish_in_one_step | closes | 284 ±7% | count | row groups closed in 3 s |
| Y_move_aside_publish_in_one_step | snapshots | 140,253 ±8% | count | snapshots taken in 3 s by 4 searchers |
| Y_move_aside_publish_in_one_step | violations | 0 | count | snapshots that did not see every row exactly once (max over 3 runs) |
