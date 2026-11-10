# kibbr-server

Nothing here reaches past the engine's public API: the server is HTTP over one `Database`.

The server, as a library: `run` is given a `Settings` and a token to stop it by, and serves
the native HTTP surface until the token is cancelled or a signal arrives.

Nothing here reads a flag or writes to stderr - the binary does both - and the flush clock
lives here, not in the store: deciding when to write is a fact about the process lifecycle.
Needs `kibbr-engine` only.

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
- **The shutdown order**: stop accepting, in-flight requests, then the database's close
  (`Database::close`: final flush, compactions joined, WAL checkpoint, `trie.json`, close
  stores, close WAL) - inside the shutdown deadline.
- **The two doors**: `/ingest` is the live one; `/load` is the bulk one, staged and
  committed in one manifest record. `/compact` and the look for a day that is due.
- **One result JSON** for `/query` and `kibbr query --format json`, built in the engine
  (`Database::answer_until`).

## Files

```
src/lib.rs         what the handlers share, and the load job table
src/settings.rs    `Settings`: every value a server runs on, plain data with a default
src/lifecycle.rs   `run` - the runtime, the flush clock, signals, shutdown, the banner
src/routes.rs      the router, `/templates`, `/stats`, `/health`, the request timeout
src/handlers.rs    `/ingest`'s door, `/query`, `/load`
src/ingest.rs      the door's half: a body's lines through `Database::ingest`, and the counts
src/compaction.rs  `/compact`, and the look for a day that is due
src/error.rs       what stops a server, and what stops the live door taking a body
src/tests.rs       the unit tests
src/viewer.html    the log viewer served at `/` - one file, compiled in
```

## Tests

```
cargo test -p kibbr-server
```

25 unit tests, in `src/tests.rs`. The HTTP contract itself is `tests/serve.rs` at the
repository root, over a bare `TcpStream`; what the server does when killed or starved is
`scripts/fault/`.
