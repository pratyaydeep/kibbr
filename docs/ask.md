# Ask a running server

```
kibbr templates --data-dir kibbr-data     # GET /templates: id, rows, time source, path
kibbr stats --data-dir kibbr-data         # GET /stats: a line a number
kibbr health --data-dir kibbr-data        # GET /health: prints ok, or degraded and why
kibbr stats --addr 127.0.0.1:7700 --json  # by address; the answer as the server sent it
```

- The server asked is the one holding `--data-dir` (its `LOCK` file says where it listens),
  or `--addr host:port`. `--config kibbr.toml` names the directory the same way.
- In `stats`, a name on the left is the path to the same value in the JSON.
- `health` is for a machine: exit 0 while the server answers `"ok":true`, exit 1 when
  nothing answers. A degraded server cannot write and still answers queries, so it is not
  one to restart, and exits 0.

```
$ kibbr templates --data-dir kibbr-data
 ID  ROWS  TIME            PATH
101    36  column:ts_unix  err.cause.code, err.cause.detail, err.code, err.trace_id, level, msg, service, ts_unix
169    20  column:ts_unix  err.cause.code, err.code, err.trace_id, level, msg, service, ts_unix
...
251 templates

$ kibbr stats --data-dir kibbr-data
uptime_seconds                        2.399409
started_at                            2026-10-12T23:04:38.069917Z
storage.kind                          disk
storage.durable                       true
...

$ kibbr health --data-dir kibbr-data
ok
```
