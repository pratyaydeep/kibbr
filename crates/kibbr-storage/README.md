# kibbr-storage

Where templates and rows are kept: the trie and its catalog, the stores under it, the
write-ahead log, `db` - the write path for one parsed line - and `lock`.

Above `kibbr-core` and under the engine: nothing here plans or answers a query.

## Entry points

| name | where | what it does |
|---|---|---|
| `Trie` | `trie.rs` | the templates: `ingest` a parsed line's tokens, `walk` to a leaf |
| `Catalog` | `catalog.rs` | `catalog.log`, the record of every template, which makes the trie durable |
| `StoreManager` | `store/mod.rs` | the leaf stores, memory or disk: `append` a row, flush, scan |
| `Wal` | `wal.rs` | the write-ahead log: `open`, `commit`, `replay`, `checkpoint` |
| `db::ingest_line` | `db.rs` | one line into the trie and its store |

## What it is responsible for

- **The trie** and its on-disk forms: `trie.json`, a readable snapshot, and the
  template catalog, written and flushed before the line that made a template counts.
- **A template's time source**, chosen once from the time columns and kept.
- **Format v3: one shared data file per UTC day** - frames and the schema record,
  row groups, chunks, forms, the footer and trailer, recovery of an unsealed file - and its
  encodings. The test vectors are `testdata/v3/`.
- **Row order and the paging cursor**: a row's position among its template's rows
  of the day never moves, compaction included.
- **The manifest**: `manifest.log` names every live data file; a load or a
  compaction is published in one record, and a start deletes what it does not name.
- **Compaction**: a day's small row groups rebuilt into full ones at zstd 9.
- **The write-ahead log**: record format, recovery, group commit. A line acknowledged is
  stored exactly once, whatever kills the process.
- **The write path**; a store write failing is not an error the sender hears.
- **Framing**: the catalog, the manifest, the day files and the log share `frame.rs`, every
  frame under a CRC-32C.
- **One owner**: `LOCK` - one process owns a data directory.

## Files

```
src/trie.rs       the trie - nodes, edges, leaf metadata, the key index
src/catalog.rs    the template catalog that makes the trie durable
src/triefile.rs   trie.json
src/store/        the leaf stores - memory, and format v3's day files - and the scan
src/wal.rs        the write-ahead log: group commit, the one applier, replay, the checkpoint
src/frame.rs      the framing the catalog, the manifest, the day files and the log share
src/db.rs         lines to a trie and its stores - the write path
src/lock.rs       the LOCK file: one process owns a data directory
```

## Tests

```
cargo test -p kibbr-storage
```

238 unit tests, about half a minute. The crash and fault cases that need a real process
are the scripts under `scripts/fault/`.
