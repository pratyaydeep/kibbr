# Settings: a flag, a variable, a file

Every setting is read the same way: **the flag, or else the variable, or else the file, or
else the default.**

```
kibbr serve --max-rows 500                        # the flag
KIBBR__QUERY__MAX_ROWS=500 kibbr serve            # the variable: KIBBR__, section, name
kibbr serve --config kibbr.toml                   # the file: TOML, read only if named
kibbr --config kibbr.toml serve                   # --config goes before the command or after
kibbr --log-format json serve --storage memory    # the server's own log as JSON, on stderr
```

The file's keys are the names the variables already had. These are the defaults, and every
key is optional:

```toml
[storage]
base_path = "kibbr-data"            # --data-dir             KIBBR__STORAGE__BASE_PATH
trie_file = "kibbr-data/trie.json"  # --trie-file            default: trie.json in base_path
max_open_segments = 512             # --max-open-segments
segment_max_bytes = 536870912       # --segment-max-bytes
max_buffer_bytes = 536870912        # --max-buffer-bytes
compact_interval_ms = 60000         # --compact-interval-ms  0 never looks
wal_segment_bytes = 67108864        # --wal-segment-bytes
wal_queue_bytes = 268435456         # --wal-queue-bytes

[ingestor]
flush_interval_ms = 500             # --flush-interval-ms
row_group_timeout_ms = 300000       # --row-group-timeout-ms
batch_size = 500                    # --batch-size

[api]
native_addr = "127.0.0.1:7700"      # --host and --port, each over its half
max_body_bytes = 10485760           # --max-body-bytes
max_decompressed_bytes = 67108864   # --max-decompressed-bytes
ingest_concurrency = 8              # --ingest-concurrency   default: one per core
ingest_wait_ms = 100                # --ingest-wait-ms
shutdown_deadline = "25s"           # --shutdown-deadline    ms, s, m or h; "0s" is none
request_timeout = "30s"             # --request-timeout      ms, s, m or h; "0s" is none

[query]
max_rows = 10000                    # --max-rows
max_buckets = 10000                 # --max-buckets
max_groups = 100000                 # --max-groups
scan_threads = 4                    # --scan-threads         default: half the cores, at most 4
scan_fan_rows = 32768               # --scan-fan-rows

[schema]
time_columns = ["@timestamp", "timestamp", "time", "ts", "ts_ms", "ts_unix",
                "datetime", "event_time", "created_at"]    # --time-columns
epoch_window = ["2000-01-01T00:00:00Z", "2100-01-01T00:00:00Z"]

[log]
level = "info"                      # --log-level            trace, debug, info, warn, error
format = "text"                     # --log-format           text or json
```

- **A key not in that list is refused**, with its line and the keys its section does have:
  a setting read and dropped would be one its writer believes is in force.

```
$ kibbr gen-logs --lines 1 --config bad.toml
kibbr: --config bad.toml: TOML parse error at line 2, column 1
  |
2 | wal_dir = "/x"
  | ^^^^^^^
unknown field `wal_dir`, expected one of `base_path`, `trie_file`, `max_open_segments`, `segment_max_bytes`, `max_buffer_bytes`, `compact_interval_ms`, `wal_segment_bytes`, `wal_queue_bytes`
```

- **Flag only**: `--storage memory`, and what one run of `load` or `compact` is asked to do
  (`--threads`, `--replace-day`, `--day` and the rest). On `serve`, `--threads` is
  `--scan-threads` under a shorter name.
- **`shutdown_deadline`**: how long a stopping server gives the requests in flight. One
  still in flight then is answered `503` where it stands; the final flush, the checkpoint
  and the trie snapshot follow, so the process exits 0 inside its grace (Kubernetes: 30
  seconds) and the next start has nothing to replay. `"0s"` waits for every request. A
  query cut off there stops scanning.
- `scripts/fault/shutdown_deadline.sh` holds a stop to the thirty seconds, five times:
  2,500,000 rows stored and 100,000 in buffers, one client that sent half an `/ingest`
  body, one idle socket, and `MATCH LOG {}` with `--max-rows 0` running when the signal
  arrives. The process was gone 27.1 to 27.6 seconds after it, exit 0, and a restart read
  nothing back from the log and counted 2,600,000. The build before was gone at 49.5.
- **`request_timeout`**: how long one request may take, from its headers to its answer. A
  body that has not arrived by then is answered `408` and its connection closed. A query
  still running is answered `504` and its scan stops at its next row group; the log says
  `query stopped` and `/stats` counts it as `queries.stopped`. The same happens to a query
  whose client hangs up.
- An `/ingest` body read whole is not timed out: its waits are `ingest_wait_ms`'s, and a
  `504` for lines the log has taken would have their shipper send them twice.
- **The log** is the server's lifecycle lines, warnings and errors, on stderr. As `text`:
  `kibbr: ` and a sentence, `WARN` or `ERROR` before a warning's or an error's. As `json`:
  one object a line, and at `debug` every request is a line too.
- What a command prints on stdout - `query`, `parse`, `explain`, `gen-logs` - is the same
  bytes whatever the log is set to.

```
$ kibbr --log-format json serve --storage memory
{"ts":"2026-10-10T14:50:07.252Z","level":"info","target":"kibbr_server::lifecycle","msg":"listening on http://127.0.0.1:61046","addr":"127.0.0.1:61046","storage":"memory","data_dir":"","max_rows":10000,"ingest_concurrency":8,"ingest_wait_ms":100,"shutdown_deadline_ms":25000,"request_timeout_ms":30000,"scan_threads":4}
{"ts":"2026-10-10T14:50:07.252Z","level":"warn","target":"kibbr_server::lifecycle","msg":"storage is in memory: everything ingested is lost when this process exits"}
{"ts":"2026-10-10T14:50:08.745Z","level":"info","target":"kibbr_server::lifecycle","msg":"SIGTERM received - shutting down"}
```
