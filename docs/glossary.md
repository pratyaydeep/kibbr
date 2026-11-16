# kibbr's own words

Words that mean something particular in kibbr, with where each is in the code. Paths are
from the repository's root. The code's map is [ARCHITECTURE.md](../ARCHITECTURE.md).

## A line and its shape

| Word | Meaning | In the code |
|---|---|---|
| shape | The set of keys a log line has, after nested objects are flattened to dotted keys (`user.id`). | `src/engine/parse/flatten.rs` |
| template | A shape that kibbr has stored: its keys, the type of each column, and an id. Thousands of lines share one template. | `src/engine/templates/trie.rs` |
| fingerprint | The text a template is found by: its sorted keys, joined. Two lines with the same fingerprint have the same template. | `src/engine/parse/flatten.rs` |
| promoted key | A key with few different values, such as `event`. Its value goes into the fingerprint as `event=login_failed`, so lines with different events get different templates. | `src/engine/model/tokens.rs` |
| token | One step of a template's path in the trie: a key, or a promoted `key=value`. | `src/engine/model/tokens.rs` |
| trie | The tree of all templates, held in memory. A query walks it first and finds which templates can match, without reading a row. | `src/engine/templates/trie.rs` |
| leaf | The end of a template's path in the trie. A leaf has the template's rows in a store. | `src/engine/templates/trie.rs`, `src/engine/rows.rs` |
| type ladder | The ordered rules that read a value's text as a type: `Datetime`, `Int64`, `Uuid`, `Ip`, `String` and so on. The first rule that matches wins. | `src/engine/model/infer.rs` |
| epoch window | The range of instants inside which ten or thirteen digits are read as a timestamp and not as an integer. | `src/engine/model/infer.rs` |
| time column | The key a template takes each row's time from. It is chosen once, from a list of names (`ts`, `timestamp`, ...). | `src/engine/templates/trie.rs`, `src/cli/config.rs` |

## Writing

| Word | Meaning | In the code |
|---|---|---|
| live door | The way in for lines as they arrive: `POST /ingest`. Each line is in the write-ahead log before the request is answered. | `src/engine/ingest.rs`, `src/server/receive.rs` |
| bulk door | The way in for a closed day of lines, loaded whole: `kibbr load` and `POST /load`. It does not use the write-ahead log. | `src/engine/load.rs`, `src/cli/load.rs` |
| the door | Where an `/ingest` body waits for a free slot before it is read. | `src/server/handlers.rs` |
| shed | To refuse an `/ingest` body with `503` and a `Retry-After`, because it waited at the door longer than `--ingest-wait-ms`. | `src/server/handlers.rs` |
| write-ahead log (WAL) | The file a line is written to and flushed before it is acknowledged. After a crash the next start reads it back. | `src/engine/wal.rs` |
| checkpoint | The position in the write-ahead log behind which every line is already in a day file. The log before it can be removed. | `src/engine/wal.rs`, `src/engine/lifecycle.rs` |
| staging | The folder a load or a compaction builds its files in, before one record makes them count. | `src/engine/load.rs`, `src/engine/rows/disk.rs` |

## Stored data

| Word | Meaning | In the code |
|---|---|---|
| day file | One data file for one UTC day. It holds the rows of every template for that day. | `src/engine/rows/disk.rs`, `src/engine/format/file.rs` |
| segment | One file of a larger thing. A day's data is one or more segment files (`--segment-max-bytes`), and the write-ahead log is written in segments too. | `src/engine/rows/disk.rs`, `src/engine/wal.rs` |
| row group | A block of rows of one template inside a day file, with one compressed chunk for each column. | `src/engine/format/build.rs`, `src/engine/format/read.rs` |
| sealed | A day file that is finished: it has its footer, and nothing more is added to it. | `src/engine/format/file.rs` |
| catalog | `catalog.log`: the record of every template. The trie is rebuilt from it at a start. | `src/engine/templates/catalog.rs` |
| manifest | `manifest.log`: the record of which data files count. A load or a compaction is made visible by one record in it. | `src/engine/rows/manifest.rs` |
| compaction | The rebuild of a finished day: its many small row groups become a few full ones. No answer changes. | `src/engine/rows/disk/compact.rs`, `src/engine/compact.rs` |

## Asking

| Word | Meaning | In the code |
|---|---|---|
| TrieQL | kibbr's query language: `COUNT`, `MATCH`, `LOG {...}`, `SINCE`, `BETWEEN`, `GROUP BY`, `LIMIT`, `AFTER`. | `src/engine/query/trieql.rs` |
| plan | The list of templates a query must read, chosen from the trie. Making a plan reads no row. | `src/engine/query/plan.rs` |
| backstop | The limit on rows that a `MATCH` with no `LIMIT` is cut at (`--max-rows`). The answer says that it was cut, and gives a cursor. | `src/engine/query/limits.rs` |
| cursor | The token in an answer (`next_cursor`) that lets a client ask for the next page with `AFTER`. | `src/engine/query/cursor.rs` |
| bucket | One interval of a `GROUP BY time(5m)`. An empty bucket is a zero, not a gap. | `src/engine/query/buckets.rs` |
| scan fan-out | One query's scan shared out over several threads of the store's own small pool. | `src/engine/rows/scan_threads.rs` |
| oracle | A second, brute-force way to answer a query: every line, one closure, one counter. It is slow and plainly correct, and the engine is checked against it. | `src/cli/query/oracle.rs` |

## The rest

| Word | Meaning | In the code |
|---|---|---|
| farm, hostile | The two profiles of `kibbr gen-logs`. `farm` is one day of a small estate's logs, shaped like real traffic. `hostile` is every case the parser must survive. | `src/cli/tools/gen_logs.rs` |
| `internals` | A hidden module of the engine for the command line's diagnostics (`demo`, `bench-storage`). The server never uses it. | `src/engine/lib.rs` |
| `test-support`, `fuzzing` | The two features of the package. Both are for code that is not shipped. | `Cargo.toml`, `ARCHITECTURE.md` |
