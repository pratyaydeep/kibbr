# clp-s: what it does, and what kibbr could take from it

Notes from reading `y-scope/clp` at tag `v0.13.0` while getting script 07 to run against
it. Source paths below are inside `components/core/src/clp_s/`. Everything here is either
read off that source or measured by this lab — where it is a measurement, the script is
named.

clp-s is the structured (JSON) half of CLP, the Compressed Log Processor out of the
University of Toronto group that became YScope, and the thing Uber uses. `clp` handles
unstructured text; `clp-s` handles NDJSON, which is why it is the one worth comparing to.

**It is not distributed as a binary.** No prebuilt `clp-s` and no container image holding
one — the `clp-json` release tarball is a deployment package (config, shell wrappers, and
a compose file wanting mariadb, mongo, rabbitmq, redis and a locally-built `clp-package`
image). `lab/others/Dockerfile.clps` compiles it from source instead; the comments in that
file record the four things that cost a failed build.

---

## 1. It already does kibbr's central idea

`SchemaTree.hpp` gives every distinct JSON path an integer node id. A `Schema` is the
ordered set of node ids one record touched. `SchemaMap.hpp` interns each distinct schema to
a `schema_id`, and every schema gets its own columnar table.

That is kibbr's template mechanism, arrived at independently: flatten a record to its key
set, intern the key set to an id, give each id its own column store.

**So "one table per distinct shape" is not what differentiates kibbr.** Script 07 measures
both on identical input, and the shape idea is not where the difference lands — the column
encodings underneath it are.

## 2. Node type is part of path identity — no type lattice

`NodeType` (SchemaTree.hpp) is `Integer`, `Float`, `ClpString`, `VarString`, `Boolean`,
`Object`, `NullValue`, `StructuredArray`, `UnstructuredArray`, `Metadata`. The **type is
part of the node**, so a field that is an integer in one record and a string in another
becomes **two nodes**, and each stays natively typed.

kibbr instead joins them over a type lattice (`Int64` with `Float64` gives `Float64`, any
other mix gives `String`) and the column widens to `String` permanently, never narrowing.
Script 04 on the `messy` dataset measures what that costs: **44% of all columns are
already String-by-widening with no merging at all**, across 8 distinct keys, from fault
rates between 0.2% and 3.1%. 461 empty strings out of 89,808 rows turn `http.status` from
an 8-byte integer into text, forever.

clp-s never has to make that trade. Two nodes cost two columns; they do not cost a type.

**And the knock-on is worse than the type.** Script 03's calibration found that kibbr's
writer *seals the segment* when a type widens or a datetime rung changes
(`src/store/disk.rs` `row_fits` / `extend_shape`), a trigger missing from the segment
format's documented ones: 128 MB, the day boundary, shutdown, a template gaining a key. On
`messy` that turned 130 (template, day) partitions into **34,891 segment files**, 60 MB of
bytes into 152 MB of disk blocks. Of those seals, **17,404 were `dt_form` alone**: a `ts`
column alternating between RFC 3339 and ten-digit epoch seals in *both* directions, every
time, because `extend_shape` refuses two rungs rather than merging them to `Mixed`.

A type-in-the-node design has no equivalent failure.

## 3. The per-column encoders

`ColumnWriter.hpp` ships: `Int64`, `DeltaEncodedInt64`, `Float`, `FormattedFloat`,
`DictionaryFloat`, `Boolean`, `ClpString`, `VariableString`, `Timestamp`.

Script 05 independently found the same winners on kibbr's data — delta-varint for
timestamps, dictionary for low-cardinality strings. Two clp-s has that the lab did not
test: `FormattedFloat` (keeping a float's original textual form, so `1.50` round-trips as
written) and `DictionaryFloat`.

## 4. CLP's actual trick, on string columns

`ClpStringColumnWriter` holds **two dictionaries**: a log-type dictionary and a variable
dictionary. A message is split into a static skeleton with holes plus the values that
filled them, so `"user 4922 from 10.1.2.3 failed"` becomes one log-type id — interned once
however many million times that shape recurs — plus encoded variables. Repeated values go
to the variable dictionary by id; one-off numbers are encoded in place. Then zstd on top.

Script 05's `clp_split` probe is a crude version of this: a regex splitting each value into
a fixed part and its changing parts. Even crude it helped — 0.79x–0.88x of the best other
encoding on the email columns of `flat30`. The real thing is that idea tuned, with
dictionaries scoped to the whole archive rather than per chunk.

**Dictionary scope is the part kibbr gets wrong for small templates.** Script 05 measured
per-chunk against per-file word lists and found per-file barely ahead (15,217,492 vs
15,227,549 bytes on `flat30`). clp-s shares its dictionaries across the entire archive,
which is why fragmentation costs it almost nothing and costs kibbr everything — see
"What the lab measured" below.

## 5. Queries resolve against the dictionaries first

`clp-s s <archive> 'level: ERROR AND message: "job*"'` looks the query's tokens up in the
dictionaries before touching column data. A token absent from the variable dictionary means
the archive cannot contain it, so it is skipped without decompression.

That is the same skip script 06 gets from bloom filters, done with structures the format
already needs. It is also why clp-s is the slowest of the four systems on every search
probe in script 07 — no per-row-group statistics and no inverted index, so once a
dictionary lookup hits, it scans.

## 6. What the lab measured

Ratios are bytes on disk over raw NDJSON. Full tables in RESULTS.md under `07_vs_others`.

| dataset | templates | rows/template median | clp-s | kibbr best | winner |
|---|---|---|---|---|---|
| `uneven` | 4,931 | 6 | **0.067** | 0.086 | clp-s |
| `flat30` | 870 | 69 | 0.182 | **0.144** | kibbr |
| `messy` | 130 | 1,116 | 0.178 | **0.153** | kibbr |

**The crossover is template size, not anything intrinsic to the encodings.** clp-s wins
exactly where templates fragment into single-digit row counts, because its dictionaries are
archive-wide and amortise over everything; kibbr's are per-chunk, so a 6-row chunk pays
full price for a dictionary it cannot amortise. Give kibbr templates with real rows in
them and its column encodings win.

That makes script 04's merging load-bearing rather than optional: J70 collapses `uneven`
from 4,931 templates to 277 and moves per-template Parquet from 0.575x to 0.097x (script
02). Merging is not a compression nicety — it is what makes the encoders work at all.

clp-s also compressed 200,000 lines in 0.7–1.1 s against ClickHouse's 6.5–7.6 s, and
round-tripped byte-exactly every time (script 07 verifies this by decompressing and
counting, rather than trusting the exit code).

## 7. Worth a closer look, in order

1. **Archive-wide or template-wide dictionaries instead of per-chunk.** The single change
   that would close the `uneven` gap. Script 05 already shows per-file beating per-chunk;
   clp-s shows what the limit looks like.
2. **Do not seal a segment on a type change.** 34,891 files from 130 partitions is a
   storage bug wearing a type-system hat, and `dt_form` causes half of it. Either merge
   rungs to `Mixed` (the variant `extend_shape` declines to build) or keep the rung per row
   group rather than per segment.
3. **Type in the node, not a lattice join.** The larger design question. It removes the
   failure kibbr's own design already warns about — one `N/A` in ten million timestamps
   turns the column to `String` for good and every time-range query on it into a full scan
   — and removes the sealing cost with it. It also breaks the trie's "one leaf per key set"
   assumption, so it is not a small change.
4. **Log-type plus variable dictionaries for text columns.** Script 05's crude probe
   already paid on the biggest text columns.
5. **`FormattedFloat`.** Cheap, and it makes float round-tripping exact.

## What kibbr has that clp-s does not

The trie over promoted values, which turns a dimension lookup into an edge walk rather than
a dictionary probe; and a planned Elasticsearch-compatible surface on `:9200` (not built
yet), which clp-s does not attempt. Script 04's promotion comparison puts a number on the
first: a search on a promoted `event` value scans 5,957 rows where the same search on an
unpromoted key scans 168,736.
