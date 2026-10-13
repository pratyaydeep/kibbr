# kibbr-engine

Answering a query: the plan over the trie's key index, the execution over the stores, and
TrieQL, the text a query is written in.

Above `kibbr-storage`, whose trie and stores it is handed, and under the binary: it opens no
socket and reads no flag.

## Entry points

| name | where | what it does |
|---|---|---|
| `trieql::parse` | `trieql.rs` | a TrieQL string into a `Query`, or a `ParseError` that points at the text |
| `engine::plan` | `engine.rs` | a `Query` and a `Trie` into a `Plan`: the leaves that can match. Reads no row |
| `engine::execute` | `engine.rs` | a `Query` over the stores into a `QueryResult`; `execute_until` can be told to stop |
| `Query`, `QueryResult` | `engine.rs` | what is asked, and the one shape every surface answers with |
| `Limits` | `limits.rs` | what a query may not exceed: `max_rows`, `max_buckets`, `max_groups` |

## What it is responsible for

- **TrieQL**: `COUNT`, `MATCH`, `LOG {...}`, `SINCE`, `BETWEEN`, `GROUP BY`,
  `LIMIT`, `AFTER`. The flags of `kibbr query` build the same `Query`;
  `scripts/check/parser_equivalence.sh` holds the two to each other.
- **Two phases, one of them free**: the trie walk costs no I/O; only the leaves it
  finds are scanned.
- **Unknown is not empty**: a template that does not exist is `exists: false`, not a
  count of 0.
- **The result shape**: `exists`, `count`, `rows`, `groups`, and `meta` with what
  the query cost.
- **The backstop**: a `MATCH` with no `LIMIT` is cut at `max_rows` and says so, with a
  `next_cursor`; the `Cursor` resumes at a row's position in its template's day.
- **Buckets**: `GROUP BY time(w)` boundaries are multiples of `w` from the Unix epoch,
  empty buckets are zeros, and a range over `max_buckets` or `max_groups` is refused, not
  truncated (`check_buckets`).
- **Threads**: a scan fans out on the store's own small pool, never the global one.

## Files

```
src/engine.rs     the plan, the scan, the result
src/trieql.rs     the query grammar
src/limits.rs     what a query may not exceed
```

## Tests

```
cargo test -p kibbr-engine
```

53 unit tests. The engine is also held to a brute-force oracle (`kibbr query --reference`)
by `scripts/check/agree.sh`, and its parser is fuzzed by `fuzz/fuzz_targets/trieql.rs`.
