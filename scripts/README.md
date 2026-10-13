# scripts

Shell scripts that run the real binary. Each builds `target/release/kibbr` or takes `BIN`,
and its header says what it holds and why. One line each: what "pass" looks like.

Run from the repository root, for example `scripts/check/agree.sh`.

**The seed.** The four scripts that draw a query set - `agree.sh`, `store_agree.sh`,
`compact_agree.sh` and `parser_equivalence.sh` - read it from `SEED`, and `two_doors.sh`
reads it from `KIBBR_SEED`; the default is 7 in all five, so with nothing set they ask the
same queries. `GEN_SEED` is the seed of the generated lines.

## check/ - two things that must agree

| script | pass |
|---|---|
| `agree.sh` | `N combinations, 0 disagreements` between the engine and the brute-force oracle, exit 0 |
| `parser_equivalence.sh` | `N combinations, 0 disagreements` between a TrieQL string and the same query as flags |
| `store_agree.sh` | `stores agree`: the memory store and the disk store answer every query the same |
| `two_doors.sh` | `PASS two doors agree on N queries x 2 rounds`: `/ingest` and `kibbr load` cannot be told apart |
| `compact_agree.sh` | `PASS a live day of N rows answers the same compacted three ways as it did before` |
| `roundtrip.sh` | `N lines ingested, N rows returned, 0 differences`: every value back as it went in |
| `order_ids.sh` | `N strings in, the same N out`, twice: ten-digit ids and one `AX-99213` come back as ingested |
| `date_forms.sh` | `one seal, then text; every value back as it arrived`: at most 2 segment files |
| `received_at.sh` | `the system column is not a key`: 823 templates, as before the column existed |
| `time_columns.sh` | `every row has a time`: the count equals the windowed count, no `undated` directory |
| `strip_comments.py` | not a check of the binary: prints Rust source without comments, to show a diff changed comments only |

`store_agree.sh`, `two_doors.sh` and `roundtrip.sh` print `scan fan-outs ...`, and with
`NEED_FAN_OUTS=1` fail at none.

## fault/ - something is taken away

| script | pass |
|---|---|
| `crash_test.sh` | `N rounds: every acknowledged line stored exactly once after kill -9` |
| `crash_templates.sh` | `N rounds: every closed row group survived kill -9, no orphans` |
| `load_kill.sh` | `PASS K of N kills landed mid-load ..., none partial` |
| `compact_kill.sh` | `PASS N/N rounds: every row exactly once` |
| `lost_trie.sh` | `identical: deleting trie.json changed no answer` |
| `bad_version.sh` | `PASS a version 2 segment stopped the start before the listener, by path, and version 3 starts clean` |
| `start_unreadable_day.sh` | `PASS an unreadable day directory stopped serve, load and compact by name with the system's error` |
| `open_files.sh` | `PASS N lines ingested under ulimit -n M, drained, and all N came back` |
| `leaf_unwritable.sh` | `PASS a store that could not write refused nothing, and all N lines are stored once` |
| `disk_full.sh` | `PASS the full log was refused loudly, the server stayed up and exited 0, and all N acknowledged lines came back`; `SKIP` without passwordless sudo |
| `catalog_full_503.sh` | `PASS a body of N lines the catalog could not write a template for was answered 503 with a Retry-After`; `SKIP` without passwordless sudo |
| `shutdown_deadline.sh` | `PASS 5 of 5: gone within 30s of the signal, exit 0, and all N rows counted after the restart` |
| `shutdown_time.sh` | `PASS N buffered rows written and sealed in Ts, and all N came back`, T under the limit |
| `vector_check.sh` | no gate: prints what Vector 0.37.1 did when the server died mid-batch and when it was slow, to be read |

## measure/ - a number, and sometimes a gate on it

| script | pass |
|---|---|
| `ingest_scaling.sh` | `PASS 4 senders reach X lines/s against Y for 1`: at least 2.00x |
| `search_under_load.sh` | `PASS ingest keeps R of its rate with the searches running`: more than 0.50 |
| `backstop_rss.sh` | `ok    paging the backstop tiles the uncapped answer exactly, in order`, beside the memory each took |
| `dump_store.sh` | with `--verify`, `verified: all N rows identical to the engine, row for row`, after writing the templates and rows out |
| `explain_time.sh` | no gate: prints the walk and the wall time at 1,000 and 1,000,000 lines; the walk should not grow |
| `log_report.sh` | no gate: prints a file's size, compressibility, templates and columns, side by side for several files |
