# Glossary

These are the words that kibbr's comments and docs use. Each entry says what the word
means, and which name it has in the code. Paths are under `src/engine/` unless they start
with `src/`. [ARCHITECTURE.md](../ARCHITECTURE.md) is the map they are words of.

**Name mismatch** marks a word where the comments and the code use different names, or
where one name means two different things. Search for the code name, not the word.

## Data model

**Template.** A template is one shape of log line: its set of keys, the type of each
value, and a numeric id. Every line belongs to exactly one template.
Code: `template_id: u64`; `templates::trie::keys::Template` (what a query plan reads);
`database::Template` (what `/templates` lists).
Name mismatch: two different structs are both called `Template`.

**Fingerprint.** The fingerprint is the sorted list of a line's keys. A low-cardinality key
can be *promoted* into it as `key=value`. Lines with the same fingerprint share a template.
Code: `ParsedLog::fingerprint` (a `String`) in `parse/flatten.rs`; `model/tokens.rs`.
Name mismatch: `store/disk/rebuild.rs` has a private `fn fingerprint` that hashes a set of
data files. It is not related.

**Trie.** The trie is an in-memory tree of keys. Each path from the root to a leaf is one
template. A query walks it before it reads any disk.
Code: `Trie`, `Node` in `templates/trie.rs`; its key index is `templates/trie/keys.rs`.

**Leaf.** A leaf is the last node of a template's path in the trie. "A leaf's rows" means
the rows of one template.
Code: `Node::is_leaf`, `LeafMeta` (`templates/trie.rs`); the row storage of one leaf is
the trait `LeafStore` (`store.rs`), with `MemoryLeaf` and `DiskLeaf`.

**Type ladder (ladder).** The ladder is the ordered list of rules that gives a raw value
its type (`Datetime`, `Int64`, `Uuid`, `Ip`, `String`, and so on). The first rule that
matches wins, so the order is the specification. Each rule is a *rung*.
Code: `fn infer` in `model/infer.rs`; the types are `ColumnType` and `ColValue` in
`model/types.rs`.
Name mismatch: no identifier is called `ladder`. Search for `infer`.

## Writing

**Live door.** The live door is the path for log lines that arrive while the server runs:
`POST /ingest`. Each body goes through the write-ahead log, and the server answers `200`
only after that.
Code: `Database::ingest` (`database.rs`), `ingest.rs`; in the server `post_ingest` and
`admit` (`src/server/handlers.rs`) and `src/server/body.rs`.
Name mismatch: the code never says `live_door`. The server's struct `Door` holds the
admission counters of `/ingest` only, not of both doors.

**Bulk door.** The bulk door loads whole files of old lines: `kibbr load` or `POST /load`.
It skips the write-ahead log. It writes full row groups and publishes them all with one
manifest record.
Code: `Database::load`, module `load.rs` (`load::Options`).
Name mismatch: see *load* under "Name collisions".

**Place.** To place a line means to route it through the trie to its template and to build
its row. Both doors do this the same way.
Code: `ingest::place`, `ingest::place_in`, the result `Placed`.
Name mismatch: the server's `Place` (`src/server/handlers.rs`) is something else: a body's
slot among the bodies being processed. `Shed::place` counts bodies refused for lack of a slot.

**Applier.** The applier is the one thread that takes rows from the write-ahead log, in log
order, and hands them to the row stores.
Code: the `applier` thread handle in `Wal` (`wal.rs`); the work is `Held::apply`
(`database/lifecycle.rs`).

**Held.** "Held" rows are rows that the store refused to take from the write-ahead log.
They stay in the log, and the checkpoint does not move past them.
Code: `lifecycle::Held`.
Name mismatch: `load.rs` has another `Held` (the rows a load keeps in memory), and
`src/cli/bench_storage.rs` has a third (the size of a data directory). `Row::held_bytes`
is the memory a buffered row uses.

**Shed.** To shed a body means to refuse it with `503` and a `Retry-After` header, because
the server is past its capacity. The body is not read, and nothing of it is stored.
Code: struct `Shed` in `src/server.rs`; each field counts one cause; `/stats` reports
`ingest.requests_shed`.
Name mismatch: `Shed::line` counts bodies refused because the *queue* of waiting bodies was
full. Here "line" means a queue, not a log line.

**Write-ahead log (WAL).** The WAL is an append-only file where each line is written and
fsynced before `/ingest` answers. After a crash, kibbr replays it.
Code: `Wal`, module `wal.rs`; files `wal/seg_NNNNNN.wal`.

**Checkpoint.** The checkpoint is a position in the WAL. Every line before it is already in
a row group on disk, so replay can start there.
Code: `Wal::checkpoint`, `Database::checkpoint`, `lifecycle::move_checkpoint`; the file
`wal/checkpoint`.

**Catalog.** The catalog is an append-only file with one record for each template. kibbr
writes and flushes a template's record before any row of that template counts. The trie can
be rebuilt from it.
Code: `Catalog` in `templates/catalog.rs`; file `catalog.log`. (`trie.json` is only a
readable snapshot, `templates/snapshot.rs`.)

## Storing

**Day file.** A day file is one data file for one UTC day. Rows of all templates of that
day share it.
Code: the file `data/YYYY-MM-DD/seg_0_NNNNNN.col`; `format/file.rs` (`Writer`, `Reader`);
`day_name` in `store/disk.rs`.
Name mismatch: the code says "data file" (`data_files`) or "segment"
(`DEFAULT_MAX_OPEN_SEGMENTS`, the `segment` fuzz target), never "day file". The WAL's
files are also called segments (`seg_NNNNNN.wal`).

**Row group.** A row group is a block of rows of one template inside a day file. It holds
at most 8,192 rows.
Code: `RowGroup` in `format/build.rs`; `MAX_ROWS` and `KIND_ROW_GROUP` in `format.rs`.

**Chunk.** A chunk is the bytes of one column inside one row group. It is made of *pieces*,
each with its own encoding, and is usually compressed with zstd.
Code: private `Chunk` in `format/build.rs`; `ChunkDir` in `format/read.rs`; `Piece` in
`format/build.rs` and `format/column.rs`.

**Manifest.** The manifest is an append-only file that lists every live data file. A data
file that the manifest does not name is not part of the database. A load or a compaction
changes the list with one record.
Code: `store/manifest.rs` (`Record::Add`, `Record::Replace`, `Record::Seal`); file
`manifest.log`.

**Replace-a-day.** A replace-a-day is a load that becomes the whole of one day. In the same
manifest record, it removes every older file of that day.
Code: `LoadCommit::replace_day` (`store/disk.rs`), `load::Options::replace_day`; the flag
`kibbr load --replace-day`.
Name mismatch: the hyphenated word appears only in comments.

**Compact, compaction, compactor, rebuild.** One job at four levels. To *compact* a day
is to rewrite its data files as fewer, sorted ones (`kibbr compact`, `POST /compact`,
`Database::compact` and `compact_now`). A *compaction* is one such run and its record:
which days it takes, its report and the job table (`database/compaction.rs`, reached as
`engine::compaction`). The *compactor* is the server's part: the `/compact` handler and
the loop that starts due runs (`src/server/compactor.rs`). The *rebuild* is the store's
rewrite of one day's files, the work itself (`store/disk/rebuild.rs`).

**Cut.** "Cut" has no single meaning in the code. It is used for: (1) a file that a crash
stopped in the middle of a write (also called a *tear*; see
`tests/data/format/two_templates_cut.col`); (2) undoing a failed write (`Writer::rollback`);
(3) a `MATCH` answer stopped by the backstop; (4) the server's `cut_off` token at the
shutdown deadline.
Name mismatch: no identifier means "cut". Read the sentence around it.

## Querying

**TrieQL.** TrieQL is kibbr's query language: `COUNT` or `MATCH`, a pattern `LOG {...}`,
then `SINCE`, `BETWEEN`, `GROUP BY`, `ORDER BY`, `LIMIT`, `AFTER`.
Code: `trieql::parse` (`query/trieql.rs`) returns a `Query` (`query.rs`).

**Backstop.** The backstop is the row limit for a `MATCH` that has no `LIMIT`. An answer
stopped by it says so, and gives a cursor for the next page.
Code: `Limits::max_rows` (`query/limits.rs`), `LimitSource::Backstop` (`query.rs`);
the flag `--max-rows`.

**Visit.** To visit is what a scan does with each row that survives the filter: it hands
the row to a *visitor*, which keeps it, counts it or groups it.
Code: `store/visit.rs` (the trait `Visitor`, `Wants`, `ScanOptions`, `Term`); the query's
visitor is `Fetch` in `query/execute.rs`.

**Oracle (reference).** The oracle is a slow, simple re-implementation of query answers. It
checks the real engine. It shares no mechanism with the engine on purpose.
Code: `src/cli/query/oracle.rs`; `kibbr query --reference`; `scripts/check/agree.sh`.

## The code

**Inspect.** `engine::inspect` (in `src/engine.rs`) is the one door past the `Database`
handle: the trie and stores as they are, the frames of a data file, a live day written
without a log. Only the diagnostic commands (`demo`, `bench-storage`), the `cursor` fuzz
harness, some tests and `kibbr::testing` use it; the server never does
(`tests/source_rules.rs`).

## Name collisions

| Name | Means here | And also here |
|---|---|---|
| `load` | the bulk door: module `load.rs`, `Database::load` | `kibbr load`, the command (`src/cli/load.rs`), and `POST /load` |
| `compact` | `database/compaction.rs`: which days a run takes, its report, the job table | `store/disk/rebuild.rs`: the rebuild of a day itself; and `src/cli/compact.rs`, `src/server/compactor.rs` |
| `display` / `format` | `display.rs`: how a value is *printed* | `format.rs`, `format/`: the on-disk storage format; and `--format`: the output format of `kibbr query` |
| `Door` | struct `Door` in `src/server.rs`: `/ingest` admission counts | "live door" and "bulk door" in prose |
| `Template` | `templates::trie::keys::Template` | `database::Template` |
| `Held` | `lifecycle::Held` (`database/lifecycle.rs`) | `load.rs` `Held`, `src/cli/bench_storage.rs` `Held` |
| `Place` | `ingest::place` / `Placed` | `src/server/handlers.rs` `Place`, `Shed::place` |
| `line` | a log line | `Shed::line`: the queue at `/ingest` |
| `seg_` | data files `seg_0_NNNNNN.col` | WAL files `seg_NNNNNN.wal` |
| `error.rs` | `src/engine/error.rs` | `src/server/error.rs`, `src/cli/error.rs` |
