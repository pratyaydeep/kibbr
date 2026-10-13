# kibbr-server

The server, as a library: `run` is given a `Settings` and a token to stop it by, and serves
the native HTTP surface until the token is cancelled or a signal arrives.

Nothing here reads a flag or writes to stderr - the binary does both - and the flush clock
lives here, not in the store: deciding when to write is a fact about the process lifecycle.
Needs `kibbr-engine`, `kibbr-storage` and `kibbr-core`.

## Entry points

| name | where | what it does |
|---|---|---|
| `Settings` | `settings.rs` | every value a server runs on, plain data with a default: loopback, storage in memory, the default limits |
| `run()` | `lifecycle.rs` | `run(settings, cancel) -> Result<Report>`: the runtime, the flush clock, signals, shutdown, the banner |

## What it is responsible for

- **The native HTTP surface, port 7700**: `GET /`, `POST /ingest`, `POST /query`,
  `POST /load`, `POST /compact`, `GET /templates`, `GET /stats`, `GET /health`.
- **The settings**: the names in `Settings` are `kibbr.toml`'s; where a value comes
  from - flag, variable, file - is the binary's business.
- **A `200` from `/ingest` is the durability boundary**: every line of the body is
  in the write-ahead log, flushed. A store that then cannot write refuses nothing.
- **Shedding**: past capacity a body waits `ingest_wait_ms` and is answered `503` with a
  `Retry-After`, never left to time out.
- **The flush clock**: when a row group closes and bytes are written.
- **The shutdown order**: stop accepting, in-flight requests, final flush, WAL
  checkpoint, `trie.json`, close stores, close WAL - inside the shutdown deadline.
- **The two doors**: `/ingest` is the live one; `/load` is the bulk one, staged and
  committed in one manifest record. `/compact` and the look for a day that is due.
- **One result JSON** for `/query` and `kibbr query --format json`, built in `query.rs`.

## Files

```
src/lib.rs         what the handlers share, and a data directory restored
src/settings.rs    `Settings`: every value a server runs on, plain data with a default
src/lifecycle.rs   `run` - the runtime, the flush clock, signals, shutdown, the banner
src/routes.rs      the router, `/templates`, `/stats`, `/health`, the request timeout
src/handlers.rs    `/ingest`'s door, `/query`, `/load`
src/ingest.rs      a body's lines into the write-ahead log and the store
src/compaction.rs  `/compact`, and the look for a day that is due
src/query.rs       a TrieQL string into a query, and the result JSON both surfaces answer with
src/load.rs        the bulk door: staging, the load id, the commit
src/compact.rs     a compaction's run, and what a server keeps about the ones it runs
src/error.rs       what stops a server, and what stops the live door taking a body
src/viewer.html    the log viewer served at `/` - one file, compiled in
```

## Tests

```
cargo test -p kibbr-server
```

47 unit tests. The HTTP contract itself is `tests/serve.rs` at the repository root, over a
bare `TcpStream`; what the server does when killed or starved is `scripts/fault/`.
