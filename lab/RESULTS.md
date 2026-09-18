# TrieDB experiment lab - results

Measurements, not opinions. Every size figure comes from a real file under
`lab/out/`, counted twice: `st_size` for the bytes a format spends and
`st_blocks * 512` for what the filesystem charges. Nothing here ships.

**Speed figures appear only where a C library or the disk did the work**
(zstd, fsync, file reads), and each says so. No throughput number measured over
Python code is a TrieDB number.

## Machine


-----------  -----------------------
alloc_block  4096 bytes (measured)
cores        8
cpu          Apple M2
disk         SSD (internal, assumed)
filesystem   apfs
memory       8.0GB
platform     Darwin 23.3.0 (arm64)
python       3.12.2

## Findings so far

- **The Python frontend agrees with the binary**: 0 disagreements over 3 datasets. Every template id and inferred type below is the one TrieDB would assign.
- **The byte model is calibrated on `flat30`**: it predicts +0.00% of the segment bytes the real binary wrote (bound: 10%).
- **The byte model is calibrated on `uneven`**: it predicts +0.00% of the segment bytes the real binary wrote (bound: 10%).
- **`uneven`**: the smallest layout is zstd -19 at 2,896,966 bytes. Per-template Parquet loses to gzip -9 by 6.33x on bytes; what TrieDB does today (one Parquet per template per day) costs 0.57x of raw NDJSON.
- **`flat30`**: the smallest layout is zstd -19 at 8,282,006 bytes. Per-template Parquet loses to gzip -9 by 1.14x on bytes; what TrieDB does today (one Parquet per template per day) costs 1.81x of raw NDJSON.
- **`oneday`**: the smallest layout is zstd -19 at 8,059,807 bytes. Per-template Parquet loses to gzip -9 by 1.15x on bytes; what TrieDB does today (one Parquet per template per day) costs 0.26x of raw NDJSON.
- **Encodings on `flat30`**: picking the smallest encoding per chunk reaches 0.144x of raw NDJSON and 0.618x of gzip -9; sorting rows inside each chunk is worth a further 2.52%.
- **Encodings on `uneven`**: picking the smallest encoding per chunk reaches 0.096x of raw NDJSON and 1.052x of gzip -9; sorting rows inside each chunk is worth a further 0.68%.
- **Bloom filters earn their bytes**: at 10 bits per value they skip 98.1% of row groups for an id that is not there, against 19.4% for min/max statistics alone, and cost 0.87% of the best encoded total.
- **Which durability call matters as much as batching**: a per-record `fsync(2)` reaches 39,898 records/sec on this platform, the same loop with `F_FULLFSYNC` reaches 360 - Darwin's `fsync` stops at the drive cache. Shape only.
- **WAL batching pays twice**: zstd 3 over batches of 500 lines compresses to 0.114 of raw, against 0.864 at one line per batch.
- **Grouping a template's row groups together** turns 51 separate byte ranges into 1 for a one-template search - on S3 at 15 ms a range, the difference between a second and nothing.
- **Against other systems on `flat30`**: Elasticsearch standard 1.249x of raw, Elasticsearch logsdb 0.982x of raw, ClickHouse 0.377x of raw, clp-s 0.182x of raw; TrieDB's best measured layout is BEST_PER_CHUNK sorted at 0.144x.
- **TrieDB's best layout is smaller than clp-s**: 0.144x of raw against 0.182x. Its archive was decompressed and counted back to 200,000 lines, so the ratio is not a partial compression.
- **One asymmetry in that comparison**: Elasticsearch and ClickHouse each received an added `@timestamp` field, roughly 30 bytes per document, because logsdb mode requires one and the dataset carries `ts`/`ts_unix`/`ts_ms` instead. clp-s and TrieDB were fed the raw payloads, so their ratios are flattered by that much.
- **Against other systems on `uneven`**: Elasticsearch standard 0.695x of raw, Elasticsearch logsdb 0.540x of raw, ClickHouse 0.158x of raw, clp-s 0.067x of raw; TrieDB's best measured layout is WIDE rg=122,880 at 0.086x.
- **clp-s stores these logs smaller than TrieDB does**: 0.067x of raw against 0.086x, on the same 200,000 lines. Its archive was decompressed and counted back to 200,000 lines, so the ratio is not a partial compression. Its searches are the slower side of that trade, but on the storage question this lab was built to answer, clp-s wins.
- **One asymmetry in that comparison**: Elasticsearch and ClickHouse each received an added `@timestamp` field, roughly 30 bytes per document, because logsdb mode requires one and the dataset carries `ts`/`ts_unix`/`ts_ms` instead. clp-s and TrieDB were fed the raw payloads, so their ratios are flattered by that much.
- **Scanning every template for an unpromoted key bends the wrong way**: 100x the templates costs 2.4x more than linear, not the same. A key-to-templates map is flat.

## Comparison systems (script 07)

Pinned in `lab/others/docker-compose.yml` and `lab/others/Dockerfile.clps`:

- `docker.elastic.co/elasticsearch/elasticsearch:8.19.20`
- `clickhouse/clickhouse-server:26.8.7.19`
- `y-scope/clp v0.13.0 (clp-json-x86_64 release tarball)`

## Contents

- [00_frontend_agree - Frontend agreement - the Python port against the binary](#00-frontend-agree)
- [01_profile_logs - What the logs look like](#01-profile-logs)
- [02_split_vs_wide - Splitting by template: does it help or hurt compression](#02-split-vs-wide)
- [03_layout_model - Files, row groups and overhead per layout](#03-layout-model)
- [04_merge_rules - Which merging rule](#04-merge-rules)
- [05_encodings - What each encoding saves](#05-encodings)
- [06_bloom - Is a bloom filter per row group worth its bytes](#06-bloom)
- [07_vs_others - The same logs in other systems](#07-vs-others)
- [08_io_shapes - Disk-dominated shapes](#08-io-shapes)
- [09_template_lookup - Searches on keys that are not promoted](#09-template-lookup)

## 00_frontend_agree

_Frontend agreement - the Python port against the binary_

Run 2026-09-18T21:51:12+00:00, git `b405451`, 6 rows.

### `00_frontend_agree` / dataset `flat30`

| variant   | metric         |   value | unit   |
|-----------|----------------|---------|--------|
| flat30    | lines_compared |   5,000 | lines  |
| flat30    | disagreements  |       0 | count  |

### `00_frontend_agree` / dataset `oneday`

| variant   | metric         |   value | unit   |
|-----------|----------------|---------|--------|
| oneday    | lines_compared |   5,000 | lines  |
| oneday    | disagreements  |       0 | count  |

### `00_frontend_agree` / dataset `uneven`

| variant   | metric         |   value | unit   |
|-----------|----------------|---------|--------|
| uneven    | lines_compared |   5,000 | lines  |
| uneven    | disagreements  |       0 | count  |

**Notes**

- triedb gen-logs --lines 200000 --templates 12 --seed 42, unmodified; 31 days, flat across templates
- flat30's lines, generator has no one-day flag; timestamps rewritten in Python - every ts/ts_unix/ts_ms mapped linearly from its 30.0-day span onto 2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits); line order unchanged, so oneday differs from flat30 in timestamps and nothing else
- generator cannot skew template shares, so built by sampling: pool of 300,000 lines at --templates 60 --seed 7, grouped by the binary's template_id, 4931 templates drawn to a Zipf profile where the top 10 hold 80% of 200,000 rows (seed 1315); generator has no one-day flag; timestamps rewritten in Python - every ts/ts_unix/ts_ms mapped linearly from its 30.0-day span onto 2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits); sorted by timestamp; 70.56% of rows are repeats of a pool line, drawn where a tail template had fewer distinct lines than its quota

## 01_profile_logs

_What the logs look like_

Run 2026-09-18T21:51:58+00:00, git `b405451`, 252 rows.

### `01_profile_logs` / dataset `flat30`

| variant                        | metric                                       |            value | unit    |
|--------------------------------|----------------------------------------------|------------------|---------|
| flat30                         | rows                                         | 200000           | count   |
| flat30                         | raw bytes                                    |      4.6592e+07  | count   |
| flat30                         | templates                                    |    870           | count   |
| flat30                         | rows/template min                            |      3           | count   |
| flat30                         | rows/template median                         |     69           | count   |
| flat30                         | rows/template p90                            |    545           | count   |
| flat30                         | rows/template max                            |  10711           | count   |
| flat30                         | top-10 share of rows %                       |     19.96        | count   |
| flat30                         | templates with <100 rows                     |    506           | count   |
| flat30                         | distinct promoted-value sets                 |      7           | count   |
| flat30                         | templates in the biggest promoted group      |    144           | count   |
| flat30                         | promoted groups holding one template         |      0           | count   |
| flat30                         | distinct column names                        |     56           | count   |
| flat30                         | days spanned by `ts`                         |     31           | count   |
| flat30                         | days spanned by every Datetime column pooled |  65933           | count   |
| flat30                         | days spanned by the widest column (order_id) |  44760           | count   |
| flat30                         | templates_with_ts                            |    722           | count   |
| flat30                         | templates_no_time_column                     |      0           | count   |
| flat30                         | templates_time_not_named_ts                  |    148           | count   |
| flat30                         | unskippable_templates                        |    148           | count   |
| flat30                         | unskippable_rows                             |  83250           | count   |
| flat30                         | unskippable_row_share                        |     41.62        | percent |
| flat30                         | unskippable_bytes                            |      1.81505e+07 | count   |
| flat30                         | unskippable_byte_share                       |     38.96        | percent |
| column:ts                      | raw_bytes                                    |      2.335e+06   | bytes   |
| column:event                   | raw_bytes                                    |      1.91691e+06 | bytes   |
| column:ext1.label              | raw_bytes                                    |      1.43334e+06 | bytes   |
| column:request_id              | raw_bytes                                    |      1.2051e+06  | bytes   |
| column:customer.id             | raw_bytes                                    |      1.18865e+06 | bytes   |
| column:client.ip               | raw_bytes                                    | 887582           | bytes   |
| column:user.email              | raw_bytes                                    | 833237           | bytes   |
| column:ts_unix                 | raw_bytes                                    | 831150           | bytes   |
| column:customer.email          | raw_bytes                                    | 821478           | bytes   |
| column:http.path               | raw_bytes                                    | 722398           | bytes   |
| column:user.profile.locale.tag | raw_bytes                                    | 718208           | bytes   |
| column:currency                | raw_bytes                                    | 708912           | bytes   |
| column:reason                  | raw_bytes                                    | 691978           | bytes   |
| column:order_id                | raw_bytes                                    | 664930           | bytes   |
| column:err.trace_id            | raw_bytes                                    | 598392           | bytes   |
| column:tags                    | raw_bytes                                    | 524005           | bytes   |
| column:level                   | raw_bytes                                    | 459797           | bytes   |
| column:ts_ms                   | raw_bytes                                    | 436930           | bytes   |
| column:device.owner.email      | raw_bytes                                    | 414784           | bytes   |
| column:ext1.ref_id             | raw_bytes                                    | 401200           | bytes   |
| column:service                 | raw_bytes                                    | 398791           | bytes   |
| column:cache.key               | raw_bytes                                    | 360261           | bytes   |
| column:device.owner.nickname   | raw_bytes                                    | 253040           | bytes   |
| column:amount                  | raw_bytes                                    | 220426           | bytes   |
| column:err.cause.detail        | raw_bytes                                    | 213585           | bytes   |
| column:http.duration_ms        | raw_bytes                                    | 202311           | bytes   |
| column:cache.origin.host       | raw_bytes                                    | 200288           | bytes   |
| column:client.ipv6             | raw_bytes                                    | 199501           | bytes   |
| column:shard.rows              | raw_bytes                                    | 197830           | bytes   |
| column:region                  | raw_bytes                                    | 177794           | bytes   |
| column:device.serial           | raw_bytes                                    | 166590           | bytes   |
| column:customer.vat_id         | raw_bytes                                    | 157885           | bytes   |
| column:shard.checkpoint        | raw_bytes                                    | 139243           | bytes   |
| column:http.method             | raw_bytes                                    | 134749           | bytes   |
| column:upstream.latency_ms     | raw_bytes                                    | 134110           | bytes   |
| column:deleted_at              | raw_bytes                                    | 133900           | bytes   |
| column:http.status             | raw_bytes                                    | 130718           | bytes   |
| column:user.id                 | raw_bytes                                    | 130249           | bytes   |
| column:user_id                 | raw_bytes                                    | 129266           | bytes   |
| column:device.fw               | raw_bytes                                    | 111147           | bytes   |
| column:upstream.trace          | raw_bytes                                    | 101930           | bytes   |
| column:device.last_seen        | raw_bytes                                    |  83580           | bytes   |
| column:cache.hit               | raw_bytes                                    |  75548           | bytes   |
| column:device.online           | raw_bytes                                    |  74984           | bytes   |
| column:shard.leader            | raw_bytes                                    |  74762           | bytes   |
| column:declined_reason         | raw_bytes                                    |  72596           | bytes   |
| column:cache.ttl               | raw_bytes                                    |  65185           | bytes   |
| column:attempt                 | raw_bytes                                    |  64819           | bytes   |
| column:err.code                | raw_bytes                                    |  64615           | bytes   |
| column:shard.id                | raw_bytes                                    |  64601           | bytes   |
| column:retry_after             | raw_bytes                                    |  58554           | bytes   |
| column:note                    | raw_bytes                                    |  48354           | bytes   |
| column:err.cause.code          | raw_bytes                                    |  38576           | bytes   |
| column:cache.origin.port       | raw_bytes                                    |  32769           | bytes   |
| column:msg                     | raw_bytes                                    |  23648           | bytes   |
| column:http.referrer           | raw_bytes                                    |  16971           | bytes   |

### `01_profile_logs` / dataset `oneday`

| variant                        | metric                                       |            value | unit    |
|--------------------------------|----------------------------------------------|------------------|---------|
| oneday                         | rows                                         | 200000           | count   |
| oneday                         | raw bytes                                    |      4.6592e+07  | count   |
| oneday                         | templates                                    |    870           | count   |
| oneday                         | rows/template min                            |      3           | count   |
| oneday                         | rows/template median                         |     69           | count   |
| oneday                         | rows/template p90                            |    545           | count   |
| oneday                         | rows/template max                            |  10711           | count   |
| oneday                         | top-10 share of rows %                       |     19.96        | count   |
| oneday                         | templates with <100 rows                     |    506           | count   |
| oneday                         | distinct promoted-value sets                 |      7           | count   |
| oneday                         | templates in the biggest promoted group      |    144           | count   |
| oneday                         | promoted groups holding one template         |      0           | count   |
| oneday                         | distinct column names                        |     56           | count   |
| oneday                         | days spanned by `ts`                         |      1           | count   |
| oneday                         | days spanned by every Datetime column pooled |  65933           | count   |
| oneday                         | days spanned by the widest column (order_id) |  44760           | count   |
| oneday                         | templates_with_ts                            |    722           | count   |
| oneday                         | templates_no_time_column                     |      0           | count   |
| oneday                         | templates_time_not_named_ts                  |    148           | count   |
| oneday                         | unskippable_templates                        |    148           | count   |
| oneday                         | unskippable_rows                             |  83250           | count   |
| oneday                         | unskippable_row_share                        |     41.62        | percent |
| oneday                         | unskippable_bytes                            |      1.81505e+07 | count   |
| oneday                         | unskippable_byte_share                       |     38.96        | percent |
| column:ts                      | raw_bytes                                    |      2.335e+06   | bytes   |
| column:event                   | raw_bytes                                    |      1.91691e+06 | bytes   |
| column:ext1.label              | raw_bytes                                    |      1.43334e+06 | bytes   |
| column:request_id              | raw_bytes                                    |      1.2051e+06  | bytes   |
| column:customer.id             | raw_bytes                                    |      1.18865e+06 | bytes   |
| column:client.ip               | raw_bytes                                    | 887582           | bytes   |
| column:user.email              | raw_bytes                                    | 833237           | bytes   |
| column:ts_unix                 | raw_bytes                                    | 831150           | bytes   |
| column:customer.email          | raw_bytes                                    | 821478           | bytes   |
| column:http.path               | raw_bytes                                    | 722398           | bytes   |
| column:user.profile.locale.tag | raw_bytes                                    | 718208           | bytes   |
| column:currency                | raw_bytes                                    | 708912           | bytes   |
| column:reason                  | raw_bytes                                    | 691978           | bytes   |
| column:order_id                | raw_bytes                                    | 664930           | bytes   |
| column:err.trace_id            | raw_bytes                                    | 598392           | bytes   |
| column:tags                    | raw_bytes                                    | 524005           | bytes   |
| column:level                   | raw_bytes                                    | 459797           | bytes   |
| column:ts_ms                   | raw_bytes                                    | 436930           | bytes   |
| column:device.owner.email      | raw_bytes                                    | 414784           | bytes   |
| column:ext1.ref_id             | raw_bytes                                    | 401200           | bytes   |
| column:service                 | raw_bytes                                    | 398791           | bytes   |
| column:cache.key               | raw_bytes                                    | 360261           | bytes   |
| column:device.owner.nickname   | raw_bytes                                    | 253040           | bytes   |
| column:amount                  | raw_bytes                                    | 220426           | bytes   |
| column:err.cause.detail        | raw_bytes                                    | 213585           | bytes   |
| column:http.duration_ms        | raw_bytes                                    | 202311           | bytes   |
| column:cache.origin.host       | raw_bytes                                    | 200288           | bytes   |
| column:client.ipv6             | raw_bytes                                    | 199501           | bytes   |
| column:shard.rows              | raw_bytes                                    | 197830           | bytes   |
| column:region                  | raw_bytes                                    | 177794           | bytes   |
| column:device.serial           | raw_bytes                                    | 166590           | bytes   |
| column:customer.vat_id         | raw_bytes                                    | 157885           | bytes   |
| column:shard.checkpoint        | raw_bytes                                    | 139243           | bytes   |
| column:http.method             | raw_bytes                                    | 134749           | bytes   |
| column:upstream.latency_ms     | raw_bytes                                    | 134110           | bytes   |
| column:deleted_at              | raw_bytes                                    | 133900           | bytes   |
| column:http.status             | raw_bytes                                    | 130718           | bytes   |
| column:user.id                 | raw_bytes                                    | 130249           | bytes   |
| column:user_id                 | raw_bytes                                    | 129266           | bytes   |
| column:device.fw               | raw_bytes                                    | 111147           | bytes   |
| column:upstream.trace          | raw_bytes                                    | 101930           | bytes   |
| column:device.last_seen        | raw_bytes                                    |  83580           | bytes   |
| column:cache.hit               | raw_bytes                                    |  75548           | bytes   |
| column:device.online           | raw_bytes                                    |  74984           | bytes   |
| column:shard.leader            | raw_bytes                                    |  74762           | bytes   |
| column:declined_reason         | raw_bytes                                    |  72596           | bytes   |
| column:cache.ttl               | raw_bytes                                    |  65185           | bytes   |
| column:attempt                 | raw_bytes                                    |  64819           | bytes   |
| column:err.code                | raw_bytes                                    |  64615           | bytes   |
| column:shard.id                | raw_bytes                                    |  64601           | bytes   |
| column:retry_after             | raw_bytes                                    |  58554           | bytes   |
| column:note                    | raw_bytes                                    |  48354           | bytes   |
| column:err.cause.code          | raw_bytes                                    |  38576           | bytes   |
| column:cache.origin.port       | raw_bytes                                    |  32769           | bytes   |
| column:msg                     | raw_bytes                                    |  23648           | bytes   |
| column:http.referrer           | raw_bytes                                    |  16971           | bytes   |

### `01_profile_logs` / dataset `uneven`

| variant                        | metric                                       |            value | unit    |
|--------------------------------|----------------------------------------------|------------------|---------|
| uneven                         | rows                                         | 200000           | count   |
| uneven                         | raw bytes                                    |      4.47597e+07 | count   |
| uneven                         | templates                                    |   4931           | count   |
| uneven                         | rows/template min                            |      4           | count   |
| uneven                         | rows/template median                         |      6           | count   |
| uneven                         | rows/template p90                            |     13           | count   |
| uneven                         | rows/template max                            |  54721           | count   |
| uneven                         | top-10 share of rows %                       |     80.05        | count   |
| uneven                         | templates with <100 rows                     |   4913           | count   |
| uneven                         | distinct promoted-value sets                 |      7           | count   |
| uneven                         | templates in the biggest promoted group      |    813           | count   |
| uneven                         | promoted groups holding one template         |      0           | count   |
| uneven                         | distinct column names                        |     68           | count   |
| uneven                         | days spanned by `ts`                         |      1           | count   |
| uneven                         | days spanned by every Datetime column pooled |  37443           | count   |
| uneven                         | days spanned by the widest column (order_id) |  14848           | count   |
| uneven                         | templates_with_ts                            |   4159           | count   |
| uneven                         | templates_no_time_column                     |      0           | count   |
| uneven                         | templates_time_not_named_ts                  |    772           | count   |
| uneven                         | unskippable_templates                        |    772           | count   |
| uneven                         | unskippable_rows                             |  17855           | count   |
| uneven                         | unskippable_row_share                        |      8.928       | percent |
| uneven                         | unskippable_bytes                            |      3.96502e+06 | count   |
| uneven                         | unskippable_byte_share                       |      8.858       | percent |
| column:ts                      | raw_bytes                                    |      3.6429e+06  | bytes   |
| column:shard.checkpoint        | raw_bytes                                    |      1.92565e+06 | bytes   |
| column:shard.rows              | raw_bytes                                    |      1.84557e+06 | bytes   |
| column:region                  | raw_bytes                                    |      1.6503e+06  | bytes   |
| column:service                 | raw_bytes                                    |      1.01218e+06 | bytes   |
| column:level                   | raw_bytes                                    | 843187           | bytes   |
| column:ext3.label              | raw_bytes                                    | 706791           | bytes   |
| column:shard.leader            | raw_bytes                                    | 698482           | bytes   |
| column:shard.id                | raw_bytes                                    | 602471           | bytes   |
| column:request_id              | raw_bytes                                    | 518976           | bytes   |
| column:ext4.label              | raw_bytes                                    | 513695           | bytes   |
| column:ext5.label              | raw_bytes                                    | 467816           | bytes   |
| column:ext6.label              | raw_bytes                                    | 412911           | bytes   |
| column:event                   | raw_bytes                                    | 409465           | bytes   |
| column:user.email              | raw_bytes                                    | 358774           | bytes   |
| column:err.trace_id            | raw_bytes                                    | 339192           | bytes   |
| column:ext2.label              | raw_bytes                                    | 313061           | bytes   |
| column:user.profile.locale.tag | raw_bytes                                    | 311405           | bytes   |
| column:ext3.ref_id             | raw_bytes                                    | 302360           | bytes   |
| column:ext1.label              | raw_bytes                                    | 282052           | bytes   |
| column:ts_unix                 | raw_bytes                                    | 279030           | bytes   |
| column:client.ip               | raw_bytes                                    | 248627           | bytes   |
| column:tags                    | raw_bytes                                    | 215118           | bytes   |
| column:ext4.ref_id             | raw_bytes                                    | 210940           | bytes   |
| column:order_id                | raw_bytes                                    | 184810           | bytes   |
| column:err.cause.detail        | raw_bytes                                    | 175835           | bytes   |
| column:ext6.ref_id             | raw_bytes                                    | 165670           | bytes   |
| column:client.ipv6             | raw_bytes                                    | 152115           | bytes   |
| column:reason                  | raw_bytes                                    | 151047           | bytes   |
| column:customer.id             | raw_bytes                                    | 146340           | bytes   |
| column:ext5.ref_id             | raw_bytes                                    | 138450           | bytes   |
| column:ext2.ref_id             | raw_bytes                                    | 120010           | bytes   |
| column:ext1.ref_id             | raw_bytes                                    | 107170           | bytes   |
| column:cache.key               | raw_bytes                                    | 103729           | bytes   |
| column:customer.email          | raw_bytes                                    | 101089           | bytes   |
| column:http.path               | raw_bytes                                    |  94456           | bytes   |
| column:device.owner.email      | raw_bytes                                    |  89902           | bytes   |
| column:currency                | raw_bytes                                    |  87748           | bytes   |
| column:ext7.label              | raw_bytes                                    |  76232           | bytes   |
| column:upstream.trace          | raw_bytes                                    |  65480           | bytes   |
| column:deleted_at              | raw_bytes                                    |  57664           | bytes   |
| column:cache.origin.host       | raw_bytes                                    |  57420           | bytes   |
| column:ts_ms                   | raw_bytes                                    |  56784           | bytes   |
| column:user.id                 | raw_bytes                                    |  55964           | bytes   |
| column:upstream.latency_ms     | raw_bytes                                    |  50183           | bytes   |
| column:device.owner.nickname   | raw_bytes                                    |  49272           | bytes   |
| column:err.code                | raw_bytes                                    |  36737           | bytes   |
| column:device.serial           | raw_bytes                                    |  36110           | bytes   |
| column:err.cause.code          | raw_bytes                                    |  32320           | bytes   |
| column:retry_after             | raw_bytes                                    |  27467           | bytes   |
| column:amount                  | raw_bytes                                    |  27158           | bytes   |
| column:device.fw               | raw_bytes                                    |  24117           | bytes   |
| column:http.duration_ms        | raw_bytes                                    |  22299           | bytes   |
| column:cache.hit               | raw_bytes                                    |  21629           | bytes   |
| column:note                    | raw_bytes                                    |  21488           | bytes   |
| column:customer.vat_id         | raw_bytes                                    |  20973           | bytes   |
| column:ext7.ref_id             | raw_bytes                                    |  19320           | bytes   |
| column:cache.ttl               | raw_bytes                                    |  18649           | bytes   |
| column:device.last_seen        | raw_bytes                                    |  18170           | bytes   |
| column:http.method             | raw_bytes                                    |  17462           | bytes   |
| column:http.status             | raw_bytes                                    |  16989           | bytes   |
| column:user_id                 | raw_bytes                                    |  16864           | bytes   |
| column:device.online           | raw_bytes                                    |  16281           | bytes   |
| column:msg                     | raw_bytes                                    |  13225           | bytes   |
| column:cache.origin.port       | raw_bytes                                    |   9401           | bytes   |
| column:declined_reason         | raw_bytes                                    |   8628           | bytes   |
| column:attempt                 | raw_bytes                                    |   8422           | bytes   |
| column:http.referrer           | raw_bytes                                    |   2344           | bytes   |

**Notes**

- triedb gen-logs --lines 200000 --templates 12 --seed 42, unmodified; 31 days, flat across templates
- a template whose time column is not named `ts` has no row-group min/max to skip on: its rows land in `undated/` and a time-range search reads every one
- type=Datetime distinct=114166 distinct/rows=0.9779 avg_len=20.0 null_wide=41.62% null_json=0.00% templates=722
- type=String distinct=6 distinct/rows=0.0000 avg_len=11.5 null_wide=16.62% null_json=0.00% templates=864
- type=String distinct=8 distinct/rows=0.0001 avg_len=21.5 null_wide=66.72% null_json=0.00% templates=528
- type=Uuid distinct=33475 distinct/rows=1.0000 avg_len=36.0 null_wide=83.26% null_json=0.00% templates=576
- type=Uuid distinct=33018 distinct/rows=1.0000 avg_len=36.0 null_wide=83.49% null_json=0.00% templates=72
- type=Ip distinct=67085 distinct/rows=1.0000 avg_len=13.2 null_wide=66.46% null_json=0.00% templates=648
- type=String distinct=32367 distinct/rows=0.9669 avg_len=24.9 null_wide=83.26% null_json=0.00% templates=576
- type=Datetime distinct=81822 distinct/rows=0.9844 avg_len=10.0 null_wide=58.44% null_json=0.00% templates=652
- type=String distinct=31966 distinct/rows=0.9681 avg_len=24.9 null_wide=83.49% null_json=0.00% templates=72
- type=String distinct=8 distinct/rows=0.0002 avg_len=21.5 null_wide=83.19% null_json=0.00% templates=72
- type=String distinct=8 distinct/rows=0.0002 avg_len=21.5 null_wide=83.26% null_json=0.00% templates=576
- type=String distinct=8 distinct/rows=0.0002 avg_len=21.5 null_wide=83.49% null_json=0.00% templates=72
- type=String distinct=5 distinct/rows=0.0001 avg_len=13.8 null_wide=74.95% null_json=0.00% templates=324
- type=Datetime distinct=66493 distinct/rows=1.0000 avg_len=10.0 null_wide=66.75% null_json=0.00% templates=648
- type=Uuid distinct=16622 distinct/rows=1.0000 avg_len=36.0 null_wide=91.69% null_json=0.00% templates=4
- type=String distinct=400 distinct/rows=0.0099 avg_len=13.0 null_wide=79.89% null_json=0.00% templates=600
- type=String distinct=5 distinct/rows=0.0001 avg_len=4.6 null_wide=50.03% null_json=0.00% templates=654
- type=Datetime distinct=33610 distinct/rows=1.0000 avg_len=13.0 null_wide=83.19% null_json=0.00% templates=72
- type=String distinct=16425 distinct/rows=0.9860 avg_len=24.9 null_wide=91.67% null_json=0.00% templates=24
- type=Datetime distinct=40120 distinct/rows=1.0000 avg_len=10.0 null_wide=79.94% null_json=0.00% templates=264
- type=String distinct=5 distinct/rows=0.0001 avg_len=6.0 null_wide=66.77% null_json=0.00% templates=78
- type=String distinct=8 distinct/rows=0.0005 avg_len=21.5 null_wide=91.61% null_json=0.00% templates=48
- type=String distinct=8 distinct/rows=0.0007 avg_len=21.6 null_wide=94.14% null_json=0.00% templates=12
- type=Float64 distinct=31931 distinct/rows=0.9671 avg_len=6.7 null_wide=83.49% null_json=0.00% templates=72
- type=String distinct=8 distinct/rows=0.0008 avg_len=21.4 null_wide=95.01% null_json=0.00% templates=2
- type=Float64 distinct=29375 distinct/rows=0.9696 avg_len=6.7 null_wide=84.85% null_json=0.00% templates=36
- type=Ip distinct=8414 distinct/rows=1.0000 avg_len=23.8 null_wide=95.79% null_json=0.00% templates=24
- type=Ip distinct=8383 distinct/rows=1.0000 avg_len=23.8 null_wide=95.81% null_json=0.00% templates=288
- type=Int64 distinct=16616 distinct/rows=1.0000 avg_len=11.9 null_wide=91.69% null_json=0.00% templates=2
- type=String distinct=3 distinct/rows=0.0002 avg_len=10.7 null_wide=91.69% null_json=0.00% templates=2
- type=Datetime distinct=16659 distinct/rows=1.0000 avg_len=10.0 null_wide=91.67% null_json=0.00% templates=24
- type=Int64 distinct=13259 distinct/rows=1.0000 avg_len=11.9 null_wide=93.37% null_json=0.00% templates=36
- type=Datetime distinct=10711 distinct/rows=1.0000 avg_len=13.0 null_wide=94.64% null_json=0.00% templates=1
- type=String distinct=4 distinct/rows=0.0001 avg_len=4.0 null_wide=83.19% null_json=0.00% templates=72
- type=Float64 distinct=19661 distinct/rows=0.9793 avg_len=6.7 null_wide=89.96% null_json=0.00% templates=288
- type=Null distinct=1 distinct/rows=0.0000 avg_len=4.0 null_wide=83.26% null_json=100.00% templates=576
- type=Int64 distinct=9640 distinct/rows=0.2868 avg_len=3.9 null_wide=83.19% null_json=0.00% templates=72
- type=Int64 distinct=9656 distinct/rows=0.2885 avg_len=3.9 null_wide=83.26% null_json=0.00% templates=576
- type=Int64 distinct=9627 distinct/rows=0.2897 avg_len=3.9 null_wide=83.39% null_json=0.00% templates=72
- type=Float64 distinct=16372 distinct/rows=0.9828 avg_len=6.7 null_wide=91.67% null_json=0.00% templates=24
- type=Datetime distinct=10173 distinct/rows=0.9980 avg_len=10.0 null_wide=94.90% null_json=0.00% templates=288
- type=Datetime distinct=8351 distinct/rows=0.9992 avg_len=10.0 null_wide=95.82% null_json=0.00% templates=12
- type=Bool distinct=2 distinct/rows=0.0001 avg_len=4.5 null_wide=91.61% null_json=0.00% templates=48
- type=Bool distinct=2 distinct/rows=0.0001 avg_len=4.5 null_wide=91.67% null_json=0.00% templates=24
- type=Bool distinct=2 distinct/rows=0.0001 avg_len=4.5 null_wide=91.69% null_json=0.00% templates=2
- type=Null distinct=1 distinct/rows=0.0001 avg_len=4.0 null_wide=90.93% null_json=100.00% templates=36
- type=Int64 distinct=8132 distinct/rows=0.4848 avg_len=3.9 null_wide=91.61% null_json=0.00% templates=48
- type=Int64 distinct=8078 distinct/rows=0.4855 avg_len=3.9 null_wide=91.68% null_json=0.00% templates=36
- type=Int64 distinct=8102 distinct/rows=0.4874 avg_len=3.9 null_wide=91.69% null_json=0.00% templates=4
- type=Int64 distinct=8096 distinct/rows=0.4872 avg_len=3.9 null_wide=91.69% null_json=0.00% templates=2
- type=Int64 distinct=7748 distinct/rows=0.5149 avg_len=3.9 null_wide=92.48% null_json=0.00% templates=288
- type=String distinct=8 distinct/rows=0.0002 avg_len=1.4 null_wide=83.26% null_json=0.00% templates=576
- type=Int64 distinct=6321 distinct/rows=0.6362 avg_len=3.9 null_wide=95.03% null_json=0.00% templates=2
- type=Int64 distinct=5698 distinct/rows=0.6756 avg_len=3.9 null_wide=95.78% null_json=0.00% templates=24
- type=String distinct=8 distinct/rows=0.0005 avg_len=1.4 null_wide=91.69% null_json=0.00% templates=4
- type=String distinct=8 distinct/rows=0.0007 avg_len=1.5 null_wide=94.18% null_json=0.00% templates=36
- flat30's lines, generator has no one-day flag; timestamps rewritten in Python - every ts/ts_unix/ts_ms mapped linearly from its 30.0-day span onto 2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits); line order unchanged, so oneday differs from flat30 in timestamps and nothing else
- type=Datetime distinct=64103 distinct/rows=0.5491 avg_len=20.0 null_wide=41.62% null_json=0.00% templates=722
- type=Datetime distinct=53310 distinct/rows=0.6414 avg_len=10.0 null_wide=58.44% null_json=0.00% templates=652
- type=Datetime distinct=33607 distinct/rows=0.9999 avg_len=13.0 null_wide=83.19% null_json=0.00% templates=72
- generator cannot skew template shares, so built by sampling: pool of 300,000 lines at --templates 60 --seed 7, grouped by the binary's template_id, 4931 templates drawn to a Zipf profile where the top 10 hold 80% of 200,000 rows (seed 1315); generator has no one-day flag; timestamps rewritten in Python - every ts/ts_unix/ts_ms mapped linearly from its 30.0-day span onto 2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits); sorted by timestamp; 70.56% of rows are repeats of a pool line, drawn where a tail template had fewer distinct lines than its quota
- type=Datetime distinct=35725 distinct/rows=0.1961 avg_len=20.0 null_wide=8.93% null_json=0.00% templates=4159
- type=Datetime distinct=17201 distinct/rows=0.1161 avg_len=13.0 null_wide=25.94% null_json=0.00% templates=13
- type=Int64 distinct=19693 distinct/rows=0.1271 avg_len=11.9 null_wide=22.51% null_json=0.00% templates=26
- type=String distinct=3 distinct/rows=0.0000 avg_len=10.6 null_wide=22.51% null_json=0.00% templates=26
- type=String distinct=5 distinct/rows=0.0000 avg_len=6.0 null_wide=15.63% null_json=0.00% templates=438
- type=String distinct=5 distinct/rows=0.0000 avg_len=4.6 null_wide=8.42% null_json=0.00% templates=3275
- type=String distinct=8 distinct/rows=0.0002 avg_len=21.6 null_wide=83.61% null_json=0.00% templates=677
- type=Bool distinct=2 distinct/rows=0.0000 avg_len=4.5 null_wide=22.51% null_json=0.00% templates=26
- type=Int64 distinct=8661 distinct/rows=0.0559 avg_len=3.9 null_wide=22.51% null_json=0.00% templates=26
- type=Uuid distinct=13577 distinct/rows=0.9418 avg_len=36.0 null_wide=92.79% null_json=0.00% templates=2837
- type=String distinct=8 distinct/rows=0.0003 avg_len=21.6 null_wide=88.13% null_json=0.00% templates=679
- type=String distinct=8 distinct/rows=0.0004 avg_len=21.6 null_wide=89.17% null_json=0.00% templates=678
- type=String distinct=8 distinct/rows=0.0004 avg_len=21.7 null_wide=90.47% null_json=0.00% templates=679
- type=String distinct=6 distinct/rows=0.0002 avg_len=11.5 null_wide=82.20% null_json=0.00% templates=4853
- type=String distinct=13383 distinct/rows=0.9283 avg_len=24.9 null_wide=92.79% null_json=0.00% templates=2837
- type=Uuid distinct=4425 distinct/rows=0.4696 avg_len=36.0 null_wide=95.29% null_json=0.00% templates=52
- type=String distinct=8 distinct/rows=0.0006 avg_len=21.6 null_wide=92.74% null_json=0.00% templates=678
- type=String distinct=8 distinct/rows=0.0006 avg_len=21.6 null_wide=92.79% null_json=0.00% templates=2837
- type=Datetime distinct=4859 distinct/rows=0.1607 avg_len=10.0 null_wide=84.88% null_json=0.00% templates=341
- type=String distinct=8 distinct/rows=0.0006 avg_len=21.3 null_wide=93.38% null_json=0.00% templates=674
- type=Datetime distinct=19452 distinct/rows=0.6971 avg_len=10.0 null_wide=86.05% null_json=0.00% templates=3249
- type=Ip distinct=17945 distinct/rows=0.9553 avg_len=13.2 null_wide=90.61% null_json=0.00% templates=3197
- type=String distinct=400 distinct/rows=0.0241 avg_len=13.0 null_wide=91.70% null_json=0.00% templates=3149
- type=Datetime distinct=4798 distinct/rows=0.2275 avg_len=10.0 null_wide=89.45% null_json=0.00% templates=341
- type=Datetime distinct=17642 distinct/rows=0.9546 avg_len=10.0 null_wide=90.76% null_json=0.00% templates=3197
- type=String distinct=8 distinct/rows=0.0010 avg_len=21.3 null_wide=95.87% null_json=0.00% templates=26
- type=Datetime distinct=4806 distinct/rows=0.2901 avg_len=10.0 null_wide=91.72% null_json=0.00% templates=340
- type=Ip distinct=5622 distinct/rows=0.8797 avg_len=23.8 null_wide=96.80% null_json=0.00% templates=1399
- type=String distinct=5 distinct/rows=0.0005 avg_len=13.8 null_wide=94.53% null_json=0.00% templates=1619
- type=Uuid distinct=4065 distinct/rows=1.0000 avg_len=36.0 null_wide=97.97% null_json=0.00% templates=360
- type=Datetime distinct=4812 distinct/rows=0.3476 avg_len=10.0 null_wide=93.08% null_json=0.00% templates=341
- type=Datetime distinct=4772 distinct/rows=0.3976 avg_len=10.0 null_wide=94.00% null_json=0.00% templates=340
- type=Datetime distinct=4784 distinct/rows=0.4464 avg_len=10.0 null_wide=94.64% null_json=0.00% templates=338
- type=String distinct=8 distinct/rows=0.0017 avg_len=21.6 null_wide=97.60% null_json=0.00% templates=624
- type=String distinct=4054 distinct/rows=0.9973 avg_len=24.9 null_wide=97.97% null_json=0.00% templates=360
- type=String distinct=8 distinct/rows=0.0018 avg_len=21.6 null_wide=97.82% null_json=0.00% templates=360
- type=String distinct=3598 distinct/rows=0.9964 avg_len=24.9 null_wide=98.19% null_json=0.00% templates=312
- type=String distinct=8 distinct/rows=0.0020 avg_len=21.6 null_wide=97.97% null_json=0.00% templates=360
- type=String distinct=8 distinct/rows=0.0023 avg_len=21.5 null_wide=98.23% null_json=0.00% templates=524
- type=Datetime distinct=5846 distinct/rows=0.8928 avg_len=10.0 null_wide=96.73% null_json=0.00% templates=1402
- type=Null distinct=1 distinct/rows=0.0001 avg_len=4.0 null_wide=92.79% null_json=100.00% templates=2837
- type=Ip distinct=2412 distinct/rows=1.0000 avg_len=23.8 null_wide=98.79% null_json=0.00% templates=312
- type=Datetime distinct=4368 distinct/rows=1.0000 avg_len=13.0 null_wide=97.82% null_json=0.00% templates=360
- type=Int64 distinct=7438 distinct/rows=0.5160 avg_len=3.9 null_wide=92.79% null_json=0.00% templates=2837
- type=Float64 distinct=7176 distinct/rows=0.9546 avg_len=6.7 null_wide=96.24% null_json=0.00% templates=1421
- type=String distinct=8 distinct/rows=0.0035 avg_len=21.5 null_wide=98.85% null_json=0.00% templates=156
- type=Int64 distinct=3582 distinct/rows=0.3802 avg_len=3.9 null_wide=95.29% null_json=0.00% templates=52
- type=Datetime distinct=3611 distinct/rows=1.0000 avg_len=10.0 null_wide=98.19% null_json=0.00% templates=312
- type=Int64 distinct=2841 distinct/rows=0.3415 avg_len=3.9 null_wide=95.84% null_json=0.00% templates=26
- type=Int64 distinct=4817 distinct/rows=0.6827 avg_len=3.9 null_wide=96.47% null_json=0.00% templates=1414
- type=Float64 distinct=4051 distinct/rows=0.9966 avg_len=6.7 null_wide=97.97% null_json=0.00% templates=360
- type=Float64 distinct=3601 distinct/rows=0.9972 avg_len=6.7 null_wide=98.19% null_json=0.00% templates=312
- type=Float64 distinct=3333 distinct/rows=0.9973 avg_len=6.7 null_wide=98.33% null_json=0.00% templates=180
- type=Bool distinct=2 distinct/rows=0.0004 avg_len=4.5 null_wide=97.60% null_json=0.00% templates=624
- type=String distinct=8 distinct/rows=0.0006 avg_len=1.5 null_wide=92.79% null_json=0.00% templates=2837
- type=Int64 distinct=1760 distinct/rows=1.0000 avg_len=11.9 null_wide=99.12% null_json=0.00% templates=180
- type=Datetime distinct=1881 distinct/rows=0.9736 avg_len=10.0 null_wide=99.03% null_json=0.00% templates=263
- type=Int64 distinct=3854 distinct/rows=0.8022 avg_len=3.9 null_wide=97.60% null_json=0.00% templates=624
- type=Datetime distinct=1815 distinct/rows=0.9989 avg_len=10.0 null_wide=99.09% null_json=0.00% templates=156
- type=String distinct=4 distinct/rows=0.0009 avg_len=4.0 null_wide=97.82% null_json=0.00% templates=360
- type=Int64 distinct=3531 distinct/rows=0.8084 avg_len=3.9 null_wide=97.82% null_json=0.00% templates=360
- type=Int64 distinct=3543 distinct/rows=0.8165 avg_len=3.9 null_wide=97.83% null_json=0.00% templates=360
- type=Bool distinct=2 distinct/rows=0.0006 avg_len=4.5 null_wide=98.19% null_json=0.00% templates=312
- type=String distinct=8 distinct/rows=0.0008 avg_len=1.4 null_wide=95.29% null_json=0.00% templates=52
- type=Int64 distinct=2134 distinct/rows=0.8840 avg_len=3.9 null_wide=98.79% null_json=0.00% templates=312
- type=Null distinct=1 distinct/rows=0.0005 avg_len=4.0 null_wide=98.92% null_json=100.00% templates=180
- type=Int64 distinct=1963 distinct/rows=0.9063 avg_len=3.9 null_wide=98.92% null_json=0.00% templates=180
- type=String distinct=8 distinct/rows=0.0048 avg_len=1.4 null_wide=99.17% null_json=0.00% templates=180

## 02_split_vs_wide

_Splitting by template: does it help or hurt compression_

Run 2026-09-18T22:03:05+00:00, git `b405451`, 363 rows.

### `02_split_vs_wide` / dataset `flat30`

| variant                    | metric                      |            value | unit   |
|----------------------------|-----------------------------|------------------|--------|
| raw NDJSON                 | bytes                       |      4.6792e+07  | bytes  |
| raw NDJSON                 | disk_blocks                 |      4.67927e+07 | bytes  |
| raw NDJSON                 | files                       |      1           | count  |
| raw NDJSON                 | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                 | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                 | ratio_to_gzip9              |      4.321       | ratio  |
| gzip -9                    | bytes                       |      1.0829e+07  | bytes  |
| gzip -9                    | disk_blocks                 |      1.12067e+07 | bytes  |
| gzip -9                    | files                       |      1           | count  |
| gzip -9                    | ratio_to_raw_bytes          |      0.2314      | ratio  |
| gzip -9                    | ratio_to_raw_blocks         |      0.2395      | ratio  |
| gzip -9                    | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                   | bytes                       |      8.28201e+06 | bytes  |
| zstd -19                   | disk_blocks                 |      8.43366e+06 | bytes  |
| zstd -19                   | files                       |      1           | count  |
| zstd -19                   | ratio_to_raw_bytes          |      0.177       | ratio  |
| zstd -19                   | ratio_to_raw_blocks         |      0.1802      | ratio  |
| zstd -19                   | ratio_to_gzip9              |      0.7648      | ratio  |
| WIDE rg=1,000              | bytes                       |      1.25008e+07 | bytes  |
| WIDE rg=1,000              | disk_blocks                 |      1.2501e+07  | bytes  |
| WIDE rg=1,000              | files                       |      1           | count  |
| WIDE rg=1,000              | ratio_to_raw_bytes          |      0.2672      | ratio  |
| WIDE rg=1,000              | ratio_to_raw_blocks         |      0.2672      | ratio  |
| WIDE rg=1,000              | ratio_to_gzip9              |      1.154       | ratio  |
| WIDE rg=8,192              | bytes                       |      9.64851e+06 | bytes  |
| WIDE rg=8,192              | disk_blocks                 |      9.65018e+06 | bytes  |
| WIDE rg=8,192              | files                       |      1           | count  |
| WIDE rg=8,192              | ratio_to_raw_bytes          |      0.2062      | ratio  |
| WIDE rg=8,192              | ratio_to_raw_blocks         |      0.2062      | ratio  |
| WIDE rg=8,192              | ratio_to_gzip9              |      0.891       | ratio  |
| WIDE rg=122,880            | bytes                       |      9.33521e+06 | bytes  |
| WIDE rg=122,880            | disk_blocks                 |      9.33888e+06 | bytes  |
| WIDE rg=122,880            | files                       |      1           | count  |
| WIDE rg=122,880            | ratio_to_raw_bytes          |      0.1995      | ratio  |
| WIDE rg=122,880            | ratio_to_raw_blocks         |      0.1996      | ratio  |
| WIDE rg=122,880            | ratio_to_gzip9              |      0.862       | ratio  |
| PER_TEMPLATE rg=1,000      | bytes                       |      1.23686e+07 | bytes  |
| PER_TEMPLATE rg=1,000      | disk_blocks                 |      1.4078e+07  | bytes  |
| PER_TEMPLATE rg=1,000      | files                       |    870           | count  |
| PER_TEMPLATE rg=1,000      | ratio_to_raw_bytes          |      0.2643      | ratio  |
| PER_TEMPLATE rg=1,000      | ratio_to_raw_blocks         |      0.3009      | ratio  |
| PER_TEMPLATE rg=1,000      | ratio_to_gzip9              |      1.142       | ratio  |
| PER_TEMPLATE rg=8,192      | bytes                       |      1.23308e+07 | bytes  |
| PER_TEMPLATE rg=8,192      | disk_blocks                 |      1.40411e+07 | bytes  |
| PER_TEMPLATE rg=8,192      | files                       |    870           | count  |
| PER_TEMPLATE rg=8,192      | ratio_to_raw_bytes          |      0.2635      | ratio  |
| PER_TEMPLATE rg=8,192      | ratio_to_raw_blocks         |      0.3001      | ratio  |
| PER_TEMPLATE rg=8,192      | ratio_to_gzip9              |      1.139       | ratio  |
| PER_TEMPLATE rg=122,880    | bytes                       |      1.23308e+07 | bytes  |
| PER_TEMPLATE rg=122,880    | disk_blocks                 |      1.40411e+07 | bytes  |
| PER_TEMPLATE rg=122,880    | files                       |    870           | count  |
| PER_TEMPLATE rg=122,880    | ratio_to_raw_bytes          |      0.2635      | ratio  |
| PER_TEMPLATE rg=122,880    | ratio_to_raw_blocks         |      0.3001      | ratio  |
| PER_TEMPLATE rg=122,880    | ratio_to_gzip9              |      1.139       | ratio  |
| PER_TEMPLATE_DAY rg=8,192  | bytes                       |      8.47189e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192  | disk_blocks                 |      1.2236e+08  | bytes  |
| PER_TEMPLATE_DAY rg=8,192  | files                       |  16694           | count  |
| PER_TEMPLATE_DAY rg=8,192  | ratio_to_raw_bytes          |      1.811       | ratio  |
| PER_TEMPLATE_DAY rg=8,192  | ratio_to_raw_blocks         |      2.615       | ratio  |
| PER_TEMPLATE_DAY rg=8,192  | ratio_to_gzip9              |      7.823       | ratio  |
| SHARED_DAY rg=1,000        | bytes                       |      8.51256e+07 | bytes  |
| SHARED_DAY rg=1,000        | disk_blocks                 |      8.51968e+07 | bytes  |
| SHARED_DAY rg=1,000        | files                       |     32           | count  |
| SHARED_DAY rg=1,000        | ratio_to_raw_bytes          |      1.819       | ratio  |
| SHARED_DAY rg=1,000        | ratio_to_raw_blocks         |      1.821       | ratio  |
| SHARED_DAY rg=1,000        | ratio_to_gzip9              |      7.861       | ratio  |
| SHARED_DAY rg=8,192        | bytes                       |      8.51202e+07 | bytes  |
| SHARED_DAY rg=8,192        | disk_blocks                 |      8.51886e+07 | bytes  |
| SHARED_DAY rg=8,192        | files                       |     32           | count  |
| SHARED_DAY rg=8,192        | ratio_to_raw_bytes          |      1.819       | ratio  |
| SHARED_DAY rg=8,192        | ratio_to_raw_blocks         |      1.821       | ratio  |
| SHARED_DAY rg=8,192        | ratio_to_gzip9              |      7.86        | ratio  |
| SHARED_DAY rg=122,880      | bytes                       |      8.51202e+07 | bytes  |
| SHARED_DAY rg=122,880      | disk_blocks                 |      8.51886e+07 | bytes  |
| SHARED_DAY rg=122,880      | files                       |     32           | count  |
| SHARED_DAY rg=122,880      | ratio_to_raw_bytes          |      1.819       | ratio  |
| SHARED_DAY rg=122,880      | ratio_to_raw_blocks         |      1.821       | ratio  |
| SHARED_DAY rg=122,880      | ratio_to_gzip9              |      7.86        | ratio  |
| PER_TEMPLATE[J70] rg=8,192 | bytes                       |      8.44564e+06 | bytes  |
| PER_TEMPLATE[J70] rg=8,192 | disk_blocks                 |      8.55245e+06 | bytes  |
| PER_TEMPLATE[J70] rg=8,192 | files                       |     68           | count  |
| PER_TEMPLATE[J70] rg=8,192 | ratio_to_raw_bytes          |      0.1805      | ratio  |
| PER_TEMPLATE[J70] rg=8,192 | ratio_to_raw_blocks         |      0.1828      | ratio  |
| PER_TEMPLATE[J70] rg=8,192 | ratio_to_gzip9              |      0.7799      | ratio  |
| SHARED_DAY[J70] rg=8,192   | bytes                       |      1.39182e+07 | bytes  |
| SHARED_DAY[J70] rg=8,192   | disk_blocks                 |      1.39837e+07 | bytes  |
| SHARED_DAY[J70] rg=8,192   | files                       |     32           | count  |
| SHARED_DAY[J70] rg=8,192   | ratio_to_raw_bytes          |      0.2974      | ratio  |
| SHARED_DAY[J70] rg=8,192   | ratio_to_raw_blocks         |      0.2989      | ratio  |
| SHARED_DAY[J70] rg=8,192   | ratio_to_gzip9              |      1.285       | ratio  |
| PER_TEMPLATE[J80] rg=8,192 | bytes                       |      8.58173e+06 | bytes  |
| PER_TEMPLATE[J80] rg=8,192 | disk_blocks                 |      8.82278e+06 | bytes  |
| PER_TEMPLATE[J80] rg=8,192 | files                       |    125           | count  |
| PER_TEMPLATE[J80] rg=8,192 | ratio_to_raw_bytes          |      0.1834      | ratio  |
| PER_TEMPLATE[J80] rg=8,192 | ratio_to_raw_blocks         |      0.1885      | ratio  |
| PER_TEMPLATE[J80] rg=8,192 | ratio_to_gzip9              |      0.7925      | ratio  |
| SHARED_DAY[J80] rg=8,192   | bytes                       |      1.86208e+07 | bytes  |
| SHARED_DAY[J80] rg=8,192   | disk_blocks                 |      1.869e+07   | bytes  |
| SHARED_DAY[J80] rg=8,192   | files                       |     32           | count  |
| SHARED_DAY[J80] rg=8,192   | ratio_to_raw_bytes          |      0.398       | ratio  |
| SHARED_DAY[J80] rg=8,192   | ratio_to_raw_blocks         |      0.3994      | ratio  |
| SHARED_DAY[J80] rg=8,192   | ratio_to_gzip9              |      1.72        | ratio  |
| window rg=1000             | rows                        |  10711           | count  |
| window rg=1000             | row_groups                  |     11           | count  |
| window rg=1000             | groups_touched              |      1           | count  |
| window rg=1000             | file_bytes                  | 292304           | bytes  |
| window rg=1000             | bytes_read                  |  27337           | bytes  |
| window rg=1000             | read_share_pct              |      9.352       | count  |
| window rg=1000             | matching_rows               |      2           | count  |
| window rg=1000             | rows_scanned                |   1000           | count  |
| window rg=8192             | rows                        |  10711           | count  |
| window rg=8192             | row_groups                  |      2           | count  |
| window rg=8192             | groups_touched              |      1           | count  |
| window rg=8192             | file_bytes                  | 297726           | bytes  |
| window rg=8192             | bytes_read                  | 224124           | bytes  |
| window rg=8192             | read_share_pct              |     75.28        | count  |
| window rg=8192             | matching_rows               |      2           | count  |
| window rg=8192             | rows_scanned                |   8192           | count  |
| window rg=122880           | rows                        |  10711           | count  |
| window rg=122880           | row_groups                  |      1           | count  |
| window rg=122880           | groups_touched              |      1           | count  |
| window rg=122880           | file_bytes                  | 295031           | bytes  |
| window rg=122880           | bytes_read                  | 295031           | bytes  |
| window rg=122880           | read_share_pct              |    100           | count  |
| window rg=122880           | matching_rows               |      2           | count  |
| window rg=122880           | rows_scanned                |  10711           | count  |
| verdict                    | per_template_vs_gzip9_bytes |      1.139       | ratio  |
| verdict                    | smallest_by_bytes           |      8.28201e+06 | bytes  |
| verdict                    | smallest_by_blocks          |      8.43366e+06 | bytes  |

### `02_split_vs_wide` / dataset `oneday`

| variant                   | metric                      |            value | unit   |
|---------------------------|-----------------------------|------------------|--------|
| raw NDJSON                | bytes                       |      4.6792e+07  | bytes  |
| raw NDJSON                | disk_blocks                 |      4.67927e+07 | bytes  |
| raw NDJSON                | files                       |      1           | count  |
| raw NDJSON                | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                | ratio_to_gzip9              |      4.443       | ratio  |
| gzip -9                   | bytes                       |      1.05325e+07 | bytes  |
| gzip -9                   | disk_blocks                 |      1.15343e+07 | bytes  |
| gzip -9                   | files                       |      1           | count  |
| gzip -9                   | ratio_to_raw_bytes          |      0.2251      | ratio  |
| gzip -9                   | ratio_to_raw_blocks         |      0.2465      | ratio  |
| gzip -9                   | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                  | bytes                       |      8.05981e+06 | bytes  |
| zstd -19                  | disk_blocks                 |      8.41728e+06 | bytes  |
| zstd -19                  | files                       |      1           | count  |
| zstd -19                  | ratio_to_raw_bytes          |      0.1722      | ratio  |
| zstd -19                  | ratio_to_raw_blocks         |      0.1799      | ratio  |
| zstd -19                  | ratio_to_gzip9              |      0.7652      | ratio  |
| WIDE rg=1,000             | bytes                       |      1.22868e+07 | bytes  |
| WIDE rg=1,000             | disk_blocks                 |      1.2288e+07  | bytes  |
| WIDE rg=1,000             | files                       |      1           | count  |
| WIDE rg=1,000             | ratio_to_raw_bytes          |      0.2626      | ratio  |
| WIDE rg=1,000             | ratio_to_raw_blocks         |      0.2626      | ratio  |
| WIDE rg=1,000             | ratio_to_gzip9              |      1.167       | ratio  |
| WIDE rg=8,192             | bytes                       |      9.49609e+06 | bytes  |
| WIDE rg=8,192             | disk_blocks                 |      9.49862e+06 | bytes  |
| WIDE rg=8,192             | files                       |      1           | count  |
| WIDE rg=8,192             | ratio_to_raw_bytes          |      0.2029      | ratio  |
| WIDE rg=8,192             | ratio_to_raw_blocks         |      0.203       | ratio  |
| WIDE rg=8,192             | ratio_to_gzip9              |      0.9016      | ratio  |
| WIDE rg=122,880           | bytes                       |      8.86479e+06 | bytes  |
| WIDE rg=122,880           | disk_blocks                 |      8.86784e+06 | bytes  |
| WIDE rg=122,880           | files                       |      1           | count  |
| WIDE rg=122,880           | ratio_to_raw_bytes          |      0.1895      | ratio  |
| WIDE rg=122,880           | ratio_to_raw_blocks         |      0.1895      | ratio  |
| WIDE rg=122,880           | ratio_to_gzip9              |      0.8417      | ratio  |
| PER_TEMPLATE rg=1,000     | bytes                       |      1.21796e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | disk_blocks                 |      1.39059e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | files                       |    870           | count  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_bytes          |      0.2603      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_blocks         |      0.2972      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_gzip9              |      1.156       | ratio  |
| PER_TEMPLATE rg=8,192     | bytes                       |      1.21528e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | disk_blocks                 |      1.3865e+07  | bytes  |
| PER_TEMPLATE rg=8,192     | files                       |    870           | count  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_bytes          |      0.2597      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_blocks         |      0.2963      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_gzip9              |      1.154       | ratio  |
| PER_TEMPLATE rg=122,880   | bytes                       |      1.21508e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | disk_blocks                 |      1.3865e+07  | bytes  |
| PER_TEMPLATE rg=122,880   | files                       |    870           | count  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_bytes          |      0.2597      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_blocks         |      0.2963      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_gzip9              |      1.154       | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | bytes                       |      1.21528e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | disk_blocks                 |      1.3865e+07  | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | files                       |    870           | count  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_bytes          |      0.2597      | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_blocks         |      0.2963      | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_gzip9              |      1.154       | ratio  |
| SHARED_DAY rg=1,000       | bytes                       |      1.22005e+07 | bytes  |
| SHARED_DAY rg=1,000       | disk_blocks                 |      1.2202e+07  | bytes  |
| SHARED_DAY rg=1,000       | files                       |      2           | count  |
| SHARED_DAY rg=1,000       | ratio_to_raw_bytes          |      0.2607      | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_raw_blocks         |      0.2608      | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_gzip9              |      1.158       | ratio  |
| SHARED_DAY rg=8,192       | bytes                       |      1.21737e+07 | bytes  |
| SHARED_DAY rg=8,192       | disk_blocks                 |      1.21774e+07 | bytes  |
| SHARED_DAY rg=8,192       | files                       |      2           | count  |
| SHARED_DAY rg=8,192       | ratio_to_raw_bytes          |      0.2602      | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_raw_blocks         |      0.2602      | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_gzip9              |      1.156       | ratio  |
| SHARED_DAY rg=122,880     | bytes                       |      1.21718e+07 | bytes  |
| SHARED_DAY rg=122,880     | disk_blocks                 |      1.21733e+07 | bytes  |
| SHARED_DAY rg=122,880     | files                       |      2           | count  |
| SHARED_DAY rg=122,880     | ratio_to_raw_bytes          |      0.2601      | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_raw_blocks         |      0.2602      | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_gzip9              |      1.156       | ratio  |
| window rg=1000            | rows                        |  10711           | count  |
| window rg=1000            | row_groups                  |     11           | count  |
| window rg=1000            | groups_touched              |      1           | count  |
| window rg=1000            | file_bytes                  | 281198           | bytes  |
| window rg=1000            | bytes_read                  |  26244           | bytes  |
| window rg=1000            | read_share_pct              |      9.333       | count  |
| window rg=1000            | matching_rows               |     44           | count  |
| window rg=1000            | rows_scanned                |   1000           | count  |
| window rg=8192            | rows                        |  10711           | count  |
| window rg=8192            | row_groups                  |      2           | count  |
| window rg=8192            | groups_touched              |      1           | count  |
| window rg=8192            | file_bytes                  | 280099           | bytes  |
| window rg=8192            | bytes_read                  | 213683           | bytes  |
| window rg=8192            | read_share_pct              |     76.29        | count  |
| window rg=8192            | matching_rows               |     44           | count  |
| window rg=8192            | rows_scanned                |   8192           | count  |
| window rg=122880          | rows                        |  10711           | count  |
| window rg=122880          | row_groups                  |      1           | count  |
| window rg=122880          | groups_touched              |      1           | count  |
| window rg=122880          | file_bytes                  | 280947           | bytes  |
| window rg=122880          | bytes_read                  | 280947           | bytes  |
| window rg=122880          | read_share_pct              |    100           | count  |
| window rg=122880          | matching_rows               |     44           | count  |
| window rg=122880          | rows_scanned                |  10711           | count  |
| verdict                   | per_template_vs_gzip9_bytes |      1.154       | ratio  |
| verdict                   | smallest_by_bytes           |      8.05981e+06 | bytes  |
| verdict                   | smallest_by_blocks          |      8.41728e+06 | bytes  |

### `02_split_vs_wide` / dataset `uneven`

| variant                          | metric                      |            value | unit   |
|----------------------------------|-----------------------------|------------------|--------|
| raw NDJSON                       | bytes                       |      4.49597e+07 | bytes  |
| raw NDJSON                       | disk_blocks                 |      4.49618e+07 | bytes  |
| raw NDJSON                       | files                       |      1           | count  |
| raw NDJSON                       | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                       | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                       | ratio_to_gzip9              |     11.07        | ratio  |
| gzip -9                          | bytes                       |      4.06238e+06 | bytes  |
| gzip -9                          | disk_blocks                 |      4.06323e+06 | bytes  |
| gzip -9                          | files                       |      1           | count  |
| gzip -9                          | ratio_to_raw_bytes          |      0.09036     | ratio  |
| gzip -9                          | ratio_to_raw_blocks         |      0.09038     | ratio  |
| gzip -9                          | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                         | bytes                       |      2.89697e+06 | bytes  |
| zstd -19                         | disk_blocks                 |      3.20717e+06 | bytes  |
| zstd -19                         | files                       |      1           | count  |
| zstd -19                         | ratio_to_raw_bytes          |      0.06443     | ratio  |
| zstd -19                         | ratio_to_raw_blocks         |      0.07133     | ratio  |
| zstd -19                         | ratio_to_gzip9              |      0.7131      | ratio  |
| WIDE rg=1,000                    | bytes                       |      7.73446e+06 | bytes  |
| WIDE rg=1,000                    | disk_blocks                 |      7.73734e+06 | bytes  |
| WIDE rg=1,000                    | files                       |      1           | count  |
| WIDE rg=1,000                    | ratio_to_raw_bytes          |      0.172       | ratio  |
| WIDE rg=1,000                    | ratio_to_raw_blocks         |      0.1721      | ratio  |
| WIDE rg=1,000                    | ratio_to_gzip9              |      1.904       | ratio  |
| WIDE rg=8,192                    | bytes                       |      4.17842e+06 | bytes  |
| WIDE rg=8,192                    | disk_blocks                 |      4.18202e+06 | bytes  |
| WIDE rg=8,192                    | files                       |      1           | count  |
| WIDE rg=8,192                    | ratio_to_raw_bytes          |      0.09294     | ratio  |
| WIDE rg=8,192                    | ratio_to_raw_blocks         |      0.09302     | ratio  |
| WIDE rg=8,192                    | ratio_to_gzip9              |      1.029       | ratio  |
| WIDE rg=122,880                  | bytes                       |      3.84562e+06 | bytes  |
| WIDE rg=122,880                  | disk_blocks                 |      3.84614e+06 | bytes  |
| WIDE rg=122,880                  | files                       |      1           | count  |
| WIDE rg=122,880                  | ratio_to_raw_bytes          |      0.08553     | ratio  |
| WIDE rg=122,880                  | ratio_to_raw_blocks         |      0.08555     | ratio  |
| WIDE rg=122,880                  | ratio_to_gzip9              |      0.9466      | ratio  |
| PER_TEMPLATE rg=1,000            | bytes                       |      2.59822e+07 | bytes  |
| PER_TEMPLATE rg=1,000            | disk_blocks                 |      3.48037e+07 | bytes  |
| PER_TEMPLATE rg=1,000            | files                       |   4931           | count  |
| PER_TEMPLATE rg=1,000            | ratio_to_raw_bytes          |      0.5779      | ratio  |
| PER_TEMPLATE rg=1,000            | ratio_to_raw_blocks         |      0.7741      | ratio  |
| PER_TEMPLATE rg=1,000            | ratio_to_gzip9              |      6.396       | ratio  |
| PER_TEMPLATE rg=8,192            | bytes                       |      2.57132e+07 | bytes  |
| PER_TEMPLATE rg=8,192            | disk_blocks                 |      3.45252e+07 | bytes  |
| PER_TEMPLATE rg=8,192            | files                       |   4931           | count  |
| PER_TEMPLATE rg=8,192            | ratio_to_raw_bytes          |      0.5719      | ratio  |
| PER_TEMPLATE rg=8,192            | ratio_to_raw_blocks         |      0.7679      | ratio  |
| PER_TEMPLATE rg=8,192            | ratio_to_gzip9              |      6.33        | ratio  |
| PER_TEMPLATE rg=122,880          | bytes                       |      2.56798e+07 | bytes  |
| PER_TEMPLATE rg=122,880          | disk_blocks                 |      3.44883e+07 | bytes  |
| PER_TEMPLATE rg=122,880          | files                       |   4931           | count  |
| PER_TEMPLATE rg=122,880          | ratio_to_raw_bytes          |      0.5712      | ratio  |
| PER_TEMPLATE rg=122,880          | ratio_to_raw_blocks         |      0.7671      | ratio  |
| PER_TEMPLATE rg=122,880          | ratio_to_gzip9              |      6.321       | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | bytes                       |      2.57132e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192        | disk_blocks                 |      3.45252e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192        | files                       |   4931           | count  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_raw_bytes          |      0.5719      | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_raw_blocks         |      0.7679      | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_gzip9              |      6.33        | ratio  |
| SHARED_DAY rg=1,000              | bytes                       |      2.61006e+07 | bytes  |
| SHARED_DAY rg=1,000              | disk_blocks                 |      2.6366e+07  | bytes  |
| SHARED_DAY rg=1,000              | files                       |      2           | count  |
| SHARED_DAY rg=1,000              | ratio_to_raw_bytes          |      0.5805      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_raw_blocks         |      0.5864      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_gzip9              |      6.425       | ratio  |
| SHARED_DAY rg=8,192              | bytes                       |      2.58316e+07 | bytes  |
| SHARED_DAY rg=8,192              | disk_blocks                 |      2.59604e+07 | bytes  |
| SHARED_DAY rg=8,192              | files                       |      2           | count  |
| SHARED_DAY rg=8,192              | ratio_to_raw_bytes          |      0.5746      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_raw_blocks         |      0.5774      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_gzip9              |      6.359       | ratio  |
| SHARED_DAY rg=122,880            | bytes                       |      2.57982e+07 | bytes  |
| SHARED_DAY rg=122,880            | disk_blocks                 |      2.67796e+07 | bytes  |
| SHARED_DAY rg=122,880            | files                       |      2           | count  |
| SHARED_DAY rg=122,880            | ratio_to_raw_bytes          |      0.5738      | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_raw_blocks         |      0.5956      | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_gzip9              |      6.351       | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | bytes                       |      4.36132e+06 | bytes  |
| PER_TEMPLATE[J70] rg=8,192       | disk_blocks                 |      5.0176e+06  | bytes  |
| PER_TEMPLATE[J70] rg=8,192       | files                       |    277           | count  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_raw_bytes          |      0.09701     | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_raw_blocks         |      0.1116      | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_gzip9              |      1.074       | ratio  |
| SHARED_DAY[J70] rg=8,192         | bytes                       |      4.36801e+06 | bytes  |
| SHARED_DAY[J70] rg=8,192         | disk_blocks                 |      5.03808e+06 | bytes  |
| SHARED_DAY[J70] rg=8,192         | files                       |      2           | count  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_raw_bytes          |      0.09715     | ratio  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_raw_blocks         |      0.1121      | ratio  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_gzip9              |      1.075       | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | bytes                       |      7.16069e+06 | bytes  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | disk_blocks                 |      8.99482e+06 | bytes  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | files                       |    982           | count  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_raw_bytes          |      0.1593      | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_raw_blocks         |      0.2001      | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_gzip9              |      1.763       | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | bytes                       |      7.1843e+06  | bytes  |
| SHARED_DAY[J80_GROWN] rg=8,192   | disk_blocks                 |      8.13056e+06 | bytes  |
| SHARED_DAY[J80_GROWN] rg=8,192   | files                       |      2           | count  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_raw_bytes          |      0.1598      | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_raw_blocks         |      0.1808      | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_gzip9              |      1.768       | ratio  |
| window rg=1000                   | rows                        |  54721           | count  |
| window rg=1000                   | row_groups                  |     55           | count  |
| window rg=1000                   | groups_touched              |      1           | count  |
| window rg=1000                   | file_bytes                  | 179672           | bytes  |
| window rg=1000                   | bytes_read                  |   3368           | bytes  |
| window rg=1000                   | read_share_pct              |      1.875       | count  |
| window rg=1000                   | matching_rows               |    201           | count  |
| window rg=1000                   | rows_scanned                |   1000           | count  |
| window rg=8192                   | rows                        |  54721           | count  |
| window rg=8192                   | row_groups                  |      7           | count  |
| window rg=8192                   | groups_touched              |      1           | count  |
| window rg=8192                   | file_bytes                  | 123372           | bytes  |
| window rg=8192                   | bytes_read                  |  18979           | bytes  |
| window rg=8192                   | read_share_pct              |     15.38        | count  |
| window rg=8192                   | matching_rows               |    201           | count  |
| window rg=8192                   | rows_scanned                |   8192           | count  |
| window rg=122880                 | rows                        |  54721           | count  |
| window rg=122880                 | row_groups                  |      1           | count  |
| window rg=122880                 | groups_touched              |      1           | count  |
| window rg=122880                 | file_bytes                  | 116525           | bytes  |
| window rg=122880                 | bytes_read                  | 116525           | bytes  |
| window rg=122880                 | read_share_pct              |    100           | count  |
| window rg=122880                 | matching_rows               |    201           | count  |
| window rg=122880                 | rows_scanned                |  54721           | count  |
| verdict                          | per_template_vs_gzip9_bytes |      6.33        | ratio  |
| verdict                          | smallest_by_bytes           |      2.89697e+06 | bytes  |
| verdict                          | smallest_by_blocks          |      3.20717e+06 | bytes  |

**Notes**

- C zlib, level 9
- C zstd, level 19
- what TrieDB does today
- per-template bytes without per-file block rounding
- 68 merged templates, from script 04
- 125 merged templates, from script 04
- 5-minute window over the busiest dated template (1055466456870197778); row groups are skipped on their ts statistics
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 1.2529 over 870 files
- zstd -19
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 1.2021 over 870 files
- 277 merged templates, from script 04
- 982 merged templates, from script 04
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 8.4970 over 4,931 files

## 03_layout_model

_Files, row groups and overhead per layout_

Run 2026-09-18T22:07:49+00:00, git `b405451`, 451 rows.

### `03_layout_model` / dataset `flat30`

| variant                        | metric                           |            value | unit    |
|--------------------------------|----------------------------------|------------------|---------|
| rate=12 wait=60                | closes_wait                      |  51458           | count   |
| rate=12 wait=60                | closes_shutdown                  |    112           | count   |
| rate=12 wait=60                | row_groups                       |  51570           | count   |
| rate=12 wait=60                | rows_per_rg_median               |      2           | rows    |
| rate=12 wait=60                | rows_per_rg_p90                  |      8           | rows    |
| rate=12 wait=60                | backlog_max_lines                |    720           | lines   |
| rate=12 wait=60                | backlog_median_lines             |    450           | lines   |
| rate=12 wait=300               | closes_wait                      |  21177           | count   |
| rate=12 wait=300               | closes_shutdown                  |    135           | count   |
| rate=12 wait=300               | row_groups                       |  21312           | count   |
| rate=12 wait=300               | rows_per_rg_median               |      3           | rows    |
| rate=12 wait=300               | rows_per_rg_p90                  |     23           | rows    |
| rate=12 wait=300               | backlog_max_lines                |   3597           | lines   |
| rate=12 wait=300               | backlog_median_lines             |   1905           | lines   |
| rate=12 wait=900               | closes_wait                      |  17565           | count   |
| rate=12 wait=900               | closes_shutdown                  |    146           | count   |
| rate=12 wait=900               | row_groups                       |  17711           | count   |
| rate=12 wait=900               | rows_per_rg_median               |      3           | rows    |
| rate=12 wait=900               | rows_per_rg_p90                  |     23           | rows    |
| rate=12 wait=900               | backlog_max_lines                |  10798           | lines   |
| rate=12 wait=900               | backlog_median_lines             |   8537           | lines   |
| rate=120 wait=60               | closes_wait                      |  18105           | count   |
| rate=120 wait=60               | closes_shutdown                  |    140           | count   |
| rate=120 wait=60               | row_groups                       |  18245           | count   |
| rate=120 wait=60               | rows_per_rg_median               |      3           | rows    |
| rate=120 wait=60               | rows_per_rg_p90                  |     26           | rows    |
| rate=120 wait=60               | backlog_max_lines                |   7198           | lines   |
| rate=120 wait=60               | backlog_median_lines             |   5035           | lines   |
| rate=120 wait=300              | closes_wait                      |  16842           | count   |
| rate=120 wait=300              | closes_shutdown                  |    148           | count   |
| rate=120 wait=300              | row_groups                       |  16990           | count   |
| rate=120 wait=300              | rows_per_rg_median               |      3           | rows    |
| rate=120 wait=300              | rows_per_rg_p90                  |     18           | rows    |
| rate=120 wait=300              | backlog_max_lines                |  35999           | lines   |
| rate=120 wait=300              | backlog_median_lines             |  33598           | lines   |
| rate=120 wait=900              | closes_wait                      |  13134           | count   |
| rate=120 wait=900              | closes_shutdown                  |   3560           | count   |
| rate=120 wait=900              | row_groups                       |  16694           | count   |
| rate=120 wait=900              | rows_per_rg_median               |      3           | rows    |
| rate=120 wait=900              | rows_per_rg_p90                  |     16           | rows    |
| rate=120 wait=900              | backlog_max_lines                | 107999           | lines   |
| rate=120 wait=900              | backlog_median_lines             |  99911           | lines   |
| rate=1200 wait=60              | closes_wait                      |  16694           | count   |
| rate=1200 wait=60              | closes_shutdown                  |    148           | count   |
| rate=1200 wait=60              | row_groups                       |  16842           | count   |
| rate=1200 wait=60              | rows_per_rg_median               |      3           | rows    |
| rate=1200 wait=60              | rows_per_rg_p90                  |     17           | rows    |
| rate=1200 wait=60              | backlog_max_lines                |  71999           | lines   |
| rate=1200 wait=60              | backlog_median_lines             |  69579           | lines   |
| rate=1200 wait=300             | closes_shutdown                  |  16694           | count   |
| rate=1200 wait=300             | row_groups                       |  16694           | count   |
| rate=1200 wait=300             | rows_per_rg_median               |      3           | rows    |
| rate=1200 wait=300             | rows_per_rg_p90                  |     16           | rows    |
| rate=1200 wait=300             | backlog_max_lines                | 199918           | lines   |
| rate=1200 wait=300             | backlog_median_lines             |  99911           | lines   |
| rate=1200 wait=900             | closes_shutdown                  |  16694           | count   |
| rate=1200 wait=900             | row_groups                       |  16694           | count   |
| rate=1200 wait=900             | rows_per_rg_median               |      3           | rows    |
| rate=1200 wait=900             | rows_per_rg_p90                  |     16           | rows    |
| rate=1200 wait=900             | backlog_max_lines                | 199918           | lines   |
| rate=1200 wait=900             | backlog_median_lines             |  99911           | lines   |
| rate=12 wait=900 tick          | closes_tick                      | 192516           | count   |
| rate=12 wait=900 tick          | closes_shutdown                  |      2           | count   |
| rate=12 wait=900 tick          | row_groups                       | 192518           | count   |
| rate=12 wait=900 tick          | rows_per_rg_median               |      1           | rows    |
| rate=12 wait=900 tick          | rows_per_rg_p90                  |      1           | rows    |
| rate=12 wait=900 tick          | backlog_max_lines                |      6           | lines   |
| rate=12 wait=900 tick          | backlog_median_lines             |      3           | lines   |
| rate=120 wait=900 tick         | closes_tick                      | 150794           | count   |
| rate=120 wait=900 tick         | closes_shutdown                  |     17           | count   |
| rate=120 wait=900 tick         | row_groups                       | 150811           | count   |
| rate=120 wait=900 tick         | rows_per_rg_median               |      1           | rows    |
| rate=120 wait=900 tick         | rows_per_rg_p90                  |      2           | rows    |
| rate=120 wait=900 tick         | backlog_max_lines                |     60           | lines   |
| rate=120 wait=900 tick         | backlog_median_lines             |     30           | lines   |
| rate=1200 wait=900 tick        | closes_tick                      |  67984           | count   |
| rate=1200 wait=900 tick        | closes_shutdown                  |     87           | count   |
| rate=1200 wait=900 tick        | row_groups                       |  68071           | count   |
| rate=1200 wait=900 tick        | rows_per_rg_median               |      2           | rows    |
| rate=1200 wait=900 tick        | rows_per_rg_p90                  |      6           | rows    |
| rate=1200 wait=900 tick        | backlog_max_lines                |    600           | lines   |
| rate=1200 wait=900 tick        | backlog_median_lines             |    300           | lines   |
| rate=12 wait=60                | wal_24h_bytes_raw                |      2.7471e+08  | bytes   |
| rate=12 wait=60                | wal_24h_bytes_zstd               |      3.13445e+07 | bytes   |
| rate=12 wait=300               | wal_24h_bytes_raw                |      2.7471e+08  | bytes   |
| rate=12 wait=300               | wal_24h_bytes_zstd               |      3.13445e+07 | bytes   |
| rate=12 wait=900               | wal_24h_bytes_raw                |      2.7471e+08  | bytes   |
| rate=12 wait=900               | wal_24h_bytes_zstd               |      3.13445e+07 | bytes   |
| rate=120 wait=60               | wal_24h_bytes_raw                |      2.7471e+09  | bytes   |
| rate=120 wait=60               | wal_24h_bytes_zstd               |      3.13445e+08 | bytes   |
| rate=120 wait=300              | wal_24h_bytes_raw                |      2.7471e+09  | bytes   |
| rate=120 wait=300              | wal_24h_bytes_zstd               |      3.13445e+08 | bytes   |
| rate=120 wait=900              | wal_24h_bytes_raw                |      2.7471e+09  | bytes   |
| rate=120 wait=900              | wal_24h_bytes_zstd               |      3.13445e+08 | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_raw                |      2.7471e+10  | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_zstd               |      3.13445e+09 | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_raw                |      2.7471e+10  | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_zstd               |      3.13445e+09 | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_raw                |      2.7471e+10  | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_zstd               |      3.13445e+09 | bytes   |
| per_template_day               | files                            |  16694           | count   |
| per_template_day               | row_groups                       |  21312           | count   |
| per_template_day               | bytes                            |      3.74804e+07 | bytes   |
| per_template_day               | overhead_bytes                   |      1.25853e+07 | bytes   |
| per_template_day               | overhead_share_pct               |     33.58        | percent |
| per_template_day               | disk_blocks                      |      8.11131e+07 | bytes   |
| per_template_day               | ratio_to_raw                     |      1.741       | ratio   |
| per_template_day               | one_template_files               |      1           | count   |
| per_template_day               | one_template_index_bytes         |    360           | bytes   |
| per_template_day               | one_template_ranges              |      1           | count   |
| per_template_day               | all_templates_files              |    550           | count   |
| per_template_day               | all_templates_index_bytes        | 221250           | bytes   |
| per_template_day               | all_templates_ranges             |    550           | count   |
| shared_day_limit limit=128.0MB | files                            |     32           | count   |
| shared_day_limit limit=128.0MB | row_groups                       |  21312           | count   |
| shared_day_limit limit=128.0MB | bytes                            |      5.18349e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_bytes                   |      2.69398e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_share_pct               |     51.97        | percent |
| shared_day_limit limit=128.0MB | disk_blocks                      |      5.18963e+07 | bytes   |
| shared_day_limit limit=128.0MB | ratio_to_raw                     |      1.114       | ratio   |
| shared_day_limit limit=128.0MB | one_template_files               |      1           | count   |
| shared_day_limit limit=128.0MB | one_template_index_bytes         | 305628           | bytes   |
| shared_day_limit limit=128.0MB | one_template_ranges              |      2           | count   |
| shared_day_limit limit=128.0MB | all_templates_files              |      1           | count   |
| shared_day_limit limit=128.0MB | all_templates_index_bytes        | 305628           | bytes   |
| shared_day_limit limit=128.0MB | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=128.0MB | open_file_index_memory_bytes     |      2.715e+07   | bytes   |
| shared_day_limit limit=512.0MB | files                            |     32           | count   |
| shared_day_limit limit=512.0MB | row_groups                       |  21312           | count   |
| shared_day_limit limit=512.0MB | bytes                            |      5.18349e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_bytes                   |      2.69398e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_share_pct               |     51.97        | percent |
| shared_day_limit limit=512.0MB | disk_blocks                      |      5.18963e+07 | bytes   |
| shared_day_limit limit=512.0MB | ratio_to_raw                     |      1.114       | ratio   |
| shared_day_limit limit=512.0MB | one_template_files               |      1           | count   |
| shared_day_limit limit=512.0MB | one_template_index_bytes         | 305628           | bytes   |
| shared_day_limit limit=512.0MB | one_template_ranges              |      2           | count   |
| shared_day_limit limit=512.0MB | all_templates_files              |      1           | count   |
| shared_day_limit limit=512.0MB | all_templates_index_bytes        | 305628           | bytes   |
| shared_day_limit limit=512.0MB | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=512.0MB | open_file_index_memory_bytes     |      1.08602e+08 | bytes   |
| shared_day_limit limit=1.0GB   | files                            |     32           | count   |
| shared_day_limit limit=1.0GB   | row_groups                       |  21312           | count   |
| shared_day_limit limit=1.0GB   | bytes                            |      5.18349e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_bytes                   |      2.69398e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_share_pct               |     51.97        | percent |
| shared_day_limit limit=1.0GB   | disk_blocks                      |      5.18963e+07 | bytes   |
| shared_day_limit limit=1.0GB   | ratio_to_raw                     |      1.114       | ratio   |
| shared_day_limit limit=1.0GB   | one_template_files               |      1           | count   |
| shared_day_limit limit=1.0GB   | one_template_index_bytes         | 305628           | bytes   |
| shared_day_limit limit=1.0GB   | one_template_ranges              |      2           | count   |
| shared_day_limit limit=1.0GB   | all_templates_files              |      1           | count   |
| shared_day_limit limit=1.0GB   | all_templates_index_bytes        | 305628           | bytes   |
| shared_day_limit limit=1.0GB   | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=1.0GB   | open_file_index_memory_bytes     |      2.17203e+08 | bytes   |
| mod_n n=4                      | files                            |    128           | count   |
| mod_n n=4                      | row_groups                       |  21312           | count   |
| mod_n n=4                      | bytes                            |      4.94897e+07 | bytes   |
| mod_n n=4                      | overhead_bytes                   |      2.45946e+07 | bytes   |
| mod_n n=4                      | overhead_share_pct               |     49.7         | percent |
| mod_n n=4                      | disk_blocks                      |      4.97746e+07 | bytes   |
| mod_n n=4                      | ratio_to_raw                     |      1.068       | ratio   |
| mod_n n=4                      | one_template_files               |      1           | count   |
| mod_n n=4                      | one_template_index_bytes         |  79800           | bytes   |
| mod_n n=4                      | one_template_ranges              |      2           | count   |
| mod_n n=4                      | all_templates_files              |      4           | count   |
| mod_n n=4                      | all_templates_index_bytes        | 279636           | bytes   |
| mod_n n=4                      | all_templates_ranges             |      4           | count   |
| mod_n n=4                      | byte_spread_largest_over_average |      1.264       | ratio   |
| mod_n n=8                      | files                            |    256           | count   |
| mod_n n=8                      | row_groups                       |  21312           | count   |
| mod_n n=8                      | bytes                            |      4.72305e+07 | bytes   |
| mod_n n=8                      | overhead_bytes                   |      2.23354e+07 | bytes   |
| mod_n n=8                      | overhead_share_pct               |     47.29        | percent |
| mod_n n=8                      | disk_blocks                      |      4.77389e+07 | bytes   |
| mod_n n=8                      | ratio_to_raw                     |      1.025       | ratio   |
| mod_n n=8                      | one_template_files               |      1           | count   |
| mod_n n=8                      | one_template_index_bytes         |  29874           | bytes   |
| mod_n n=8                      | one_template_ranges              |      2           | count   |
| mod_n n=8                      | all_templates_files              |      8           | count   |
| mod_n n=8                      | all_templates_index_bytes        | 258100           | bytes   |
| mod_n n=8                      | all_templates_ranges             |      8           | count   |
| mod_n n=8                      | byte_spread_largest_over_average |      1.529       | ratio   |
| least_full n=4                 | files                            |    128           | count   |
| least_full n=4                 | row_groups                       |  21312           | count   |
| least_full n=4                 | bytes                            |      5.08285e+07 | bytes   |
| least_full n=4                 | overhead_bytes                   |      2.59334e+07 | bytes   |
| least_full n=4                 | overhead_share_pct               |     51.02        | percent |
| least_full n=4                 | disk_blocks                      |      5.10976e+07 | bytes   |
| least_full n=4                 | ratio_to_raw                     |      1.097       | ratio   |
| least_full n=4                 | one_template_files               |      2           | count   |
| least_full n=4                 | one_template_index_bytes         | 150744           | bytes   |
| least_full n=4                 | one_template_ranges              |      2           | count   |
| least_full n=4                 | all_templates_files              |      4           | count   |
| least_full n=4                 | all_templates_index_bytes        | 295684           | bytes   |
| least_full n=4                 | all_templates_ranges             |      4           | count   |
| least_full n=4                 | byte_spread_largest_over_average |      1.06        | ratio   |
| least_full n=8                 | files                            |    256           | count   |
| least_full n=8                 | row_groups                       |  21312           | count   |
| least_full n=8                 | bytes                            |      4.98138e+07 | bytes   |
| least_full n=8                 | overhead_bytes                   |      2.49187e+07 | bytes   |
| least_full n=8                 | overhead_share_pct               |     50.02        | percent |
| least_full n=8                 | disk_blocks                      |      5.03439e+07 | bytes   |
| least_full n=8                 | ratio_to_raw                     |      1.081       | ratio   |
| least_full n=8                 | one_template_files               |      2           | count   |
| least_full n=8                 | one_template_index_bytes         |  71532           | bytes   |
| least_full n=8                 | one_template_ranges              |      2           | count   |
| least_full n=8                 | all_templates_files              |      8           | count   |
| least_full n=8                 | all_templates_index_bytes        | 284823           | bytes   |
| least_full n=8                 | all_templates_ranges             |      8           | count   |
| least_full n=8                 | byte_spread_largest_over_average |      1.113       | ratio   |
| calibration                    | real_col_bytes                   |      3.60378e+07 | bytes   |
| calibration                    | real_col_files                   |  16694           | count   |
| calibration                    | real_col_blocks                  |      7.98188e+07 | bytes   |
| calibration                    | real_meta_bytes                  |      2.5283e+06  | bytes   |
| calibration                    | real_meta_blocks                 |      5.8368e+06  | bytes   |
| calibration                    | real_meta_files                  |    871           | count   |
| calibration                    | real_segments_over_ceil          |      0           | count   |
| calibration                    | real_bytes_over_ceil             |      0           | bytes   |
| calibration                    | model_col_bytes                  |      3.60378e+07 | bytes   |
| calibration                    | model_col_files                  |  16694           | count   |
| calibration                    | model_col_blocks                 |      7.98392e+07 | bytes   |
| calibration                    | model_minus_real_pct             |      0           | percent |

### `03_layout_model` / dataset `uneven`

| variant                        | metric                           |           value | unit    |
|--------------------------------|----------------------------------|-----------------|---------|
| rate=12 wait=60                | closes_wait                      | 34898           | count   |
| rate=12 wait=60                | closes_shutdown                  |   277           | count   |
| rate=12 wait=60                | row_groups                       | 35175           | count   |
| rate=12 wait=60                | rows_per_rg_median               |     1           | rows    |
| rate=12 wait=60                | rows_per_rg_p90                  |     2           | rows    |
| rate=12 wait=60                | backlog_max_lines                |   712           | lines   |
| rate=12 wait=60                | backlog_median_lines             |   443           | lines   |
| rate=12 wait=300               | closes_wait                      | 26329           | count   |
| rate=12 wait=300               | closes_shutdown                  |   561           | count   |
| rate=12 wait=300               | row_groups                       | 26890           | count   |
| rate=12 wait=300               | rows_per_rg_median               |     1           | rows    |
| rate=12 wait=300               | rows_per_rg_p90                  |     3           | rows    |
| rate=12 wait=300               | backlog_max_lines                |  3590           | lines   |
| rate=12 wait=300               | backlog_median_lines             |  2054           | lines   |
| rate=12 wait=900               | closes_wait                      | 19981           | count   |
| rate=12 wait=900               | closes_shutdown                  |   718           | count   |
| rate=12 wait=900               | row_groups                       | 20699           | count   |
| rate=12 wait=900               | rows_per_rg_median               |     1           | rows    |
| rate=12 wait=900               | rows_per_rg_p90                  |     3           | rows    |
| rate=12 wait=900               | backlog_max_lines                | 10790           | lines   |
| rate=12 wait=900               | backlog_median_lines             |  5797           | lines   |
| rate=120 wait=60               | closes_wait                      | 22418           | count   |
| rate=120 wait=60               | closes_shutdown                  |   612           | count   |
| rate=120 wait=60               | row_groups                       | 23030           | count   |
| rate=120 wait=60               | rows_per_rg_median               |     1           | rows    |
| rate=120 wait=60               | rows_per_rg_p90                  |     3           | rows    |
| rate=120 wait=60               | backlog_max_lines                |  7200           | lines   |
| rate=120 wait=60               | backlog_median_lines             |  3901           | lines   |
| rate=120 wait=300              | closes_rows                      |     6           | count   |
| rate=120 wait=300              | closes_wait                      | 11016           | count   |
| rate=120 wait=300              | closes_shutdown                  |  1897           | count   |
| rate=120 wait=300              | row_groups                       | 12919           | count   |
| rate=120 wait=300              | rows_per_rg_median               |     2           | rows    |
| rate=120 wait=300              | rows_per_rg_p90                  |     5           | rows    |
| rate=120 wait=300              | backlog_max_lines                | 31801           | lines   |
| rate=120 wait=300              | backlog_median_lines             | 17868           | lines   |
| rate=120 wait=900              | closes_rows                      |    11           | count   |
| rate=120 wait=900              | closes_wait                      |  3959           | count   |
| rate=120 wait=900              | closes_shutdown                  |  3880           | count   |
| rate=120 wait=900              | row_groups                       |  7850           | count   |
| rate=120 wait=900              | rows_per_rg_median               |     4           | rows    |
| rate=120 wait=900              | rows_per_rg_p90                  |     9           | rows    |
| rate=120 wait=900              | backlog_max_lines                | 67002           | lines   |
| rate=120 wait=900              | backlog_median_lines             | 35075           | lines   |
| rate=1200 wait=60              | closes_rows                      |     9           | count   |
| rate=1200 wait=60              | closes_wait                      |  6333           | count   |
| rate=1200 wait=60              | closes_shutdown                  |  2868           | count   |
| rate=1200 wait=60              | row_groups                       |  9210           | count   |
| rate=1200 wait=60              | rows_per_rg_median               |     3           | rows    |
| rate=1200 wait=60              | rows_per_rg_p90                  |     8           | rows    |
| rate=1200 wait=60              | backlog_max_lines                | 47399           | lines   |
| rate=1200 wait=60              | backlog_median_lines             | 27796           | lines   |
| rate=1200 wait=300             | closes_rows                      |    14           | count   |
| rate=1200 wait=300             | closes_shutdown                  |  4931           | count   |
| rate=1200 wait=300             | row_groups                       |  4945           | count   |
| rate=1200 wait=300             | rows_per_rg_median               |     6           | rows    |
| rate=1200 wait=300             | rows_per_rg_p90                  |    13           | rows    |
| rate=1200 wait=300             | backlog_max_lines                | 85230           | lines   |
| rate=1200 wait=300             | backlog_median_lines             | 52267           | lines   |
| rate=1200 wait=900             | closes_rows                      |    14           | count   |
| rate=1200 wait=900             | closes_shutdown                  |  4931           | count   |
| rate=1200 wait=900             | row_groups                       |  4945           | count   |
| rate=1200 wait=900             | rows_per_rg_median               |     6           | rows    |
| rate=1200 wait=900             | rows_per_rg_p90                  |    13           | rows    |
| rate=1200 wait=900             | backlog_max_lines                | 85230           | lines   |
| rate=1200 wait=900             | backlog_median_lines             | 52267           | lines   |
| rate=12 wait=900 tick          | closes_tick                      | 80470           | count   |
| rate=12 wait=900 tick          | closes_shutdown                  |     1           | count   |
| rate=12 wait=900 tick          | row_groups                       | 80471           | count   |
| rate=12 wait=900 tick          | rows_per_rg_median               |     1           | rows    |
| rate=12 wait=900 tick          | rows_per_rg_p90                  |     6           | rows    |
| rate=12 wait=900 tick          | backlog_max_lines                |     6           | lines   |
| rate=12 wait=900 tick          | backlog_median_lines             |     3           | lines   |
| rate=120 wait=900 tick         | closes_tick                      | 53278           | count   |
| rate=120 wait=900 tick         | closes_shutdown                  |    14           | count   |
| rate=120 wait=900 tick         | row_groups                       | 53292           | count   |
| rate=120 wait=900 tick         | rows_per_rg_median               |     1           | rows    |
| rate=120 wait=900 tick         | rows_per_rg_p90                  |    11           | rows    |
| rate=120 wait=900 tick         | backlog_max_lines                |    60           | lines   |
| rate=120 wait=900 tick         | backlog_median_lines             |    30           | lines   |
| rate=1200 wait=900 tick        | closes_tick                      | 38022           | count   |
| rate=1200 wait=900 tick        | closes_shutdown                  |   111           | count   |
| rate=1200 wait=900 tick        | row_groups                       | 38133           | count   |
| rate=1200 wait=900 tick        | rows_per_rg_median               |     1           | rows    |
| rate=1200 wait=900 tick        | rows_per_rg_p90                  |     2           | rows    |
| rate=1200 wait=900 tick        | backlog_max_lines                |   600           | lines   |
| rate=1200 wait=900 tick        | backlog_median_lines             |   300           | lines   |
| rate=12 wait=60                | wal_24h_bytes_raw                |     2.65212e+08 | bytes   |
| rate=12 wait=60                | wal_24h_bytes_zstd               |     3.02607e+07 | bytes   |
| rate=12 wait=300               | wal_24h_bytes_raw                |     2.65212e+08 | bytes   |
| rate=12 wait=300               | wal_24h_bytes_zstd               |     3.02607e+07 | bytes   |
| rate=12 wait=900               | wal_24h_bytes_raw                |     2.65212e+08 | bytes   |
| rate=12 wait=900               | wal_24h_bytes_zstd               |     3.02607e+07 | bytes   |
| rate=120 wait=60               | wal_24h_bytes_raw                |     2.65212e+09 | bytes   |
| rate=120 wait=60               | wal_24h_bytes_zstd               |     3.02607e+08 | bytes   |
| rate=120 wait=300              | wal_24h_bytes_raw                |     2.65212e+09 | bytes   |
| rate=120 wait=300              | wal_24h_bytes_zstd               |     3.02607e+08 | bytes   |
| rate=120 wait=900              | wal_24h_bytes_raw                |     2.65212e+09 | bytes   |
| rate=120 wait=900              | wal_24h_bytes_zstd               |     3.02607e+08 | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_raw                |     2.65212e+10 | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_zstd               |     3.02607e+09 | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_raw                |     2.65212e+10 | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_zstd               |     3.02607e+09 | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_raw                |     2.65212e+10 | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_zstd               |     3.02607e+09 | bytes   |
| per_template_day               | files                            |  4931           | count   |
| per_template_day               | row_groups                       | 26890           | count   |
| per_template_day               | bytes                            |     3.29269e+07 | bytes   |
| per_template_day               | overhead_bytes                   |     1.23479e+07 | bytes   |
| per_template_day               | overhead_share_pct               |    37.5         | percent |
| per_template_day               | disk_blocks                      |     4.20413e+07 | bytes   |
| per_template_day               | ratio_to_raw                     |     0.9393      | ratio   |
| per_template_day               | one_template_files               |     1           | count   |
| per_template_day               | one_template_index_bytes         |  6240           | bytes   |
| per_template_day               | one_template_ranges              |     1           | count   |
| per_template_day               | all_templates_files              |  4159           | count   |
| per_template_day               | all_templates_index_bytes        |     5.05134e+06 | bytes   |
| per_template_day               | all_templates_ranges             |  4159           | count   |
| shared_day_limit limit=128.0MB | files                            |     2           | count   |
| shared_day_limit limit=128.0MB | row_groups                       | 26890           | count   |
| shared_day_limit limit=128.0MB | bytes                            |     6.58842e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_bytes                   |     4.53052e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_share_pct               |    68.77        | percent |
| shared_day_limit limit=128.0MB | disk_blocks                      |     6.58883e+07 | bytes   |
| shared_day_limit limit=128.0MB | ratio_to_raw                     |     1.472       | ratio   |
| shared_day_limit limit=128.0MB | one_template_files               |     1           | count   |
| shared_day_limit limit=128.0MB | one_template_index_bytes         |     1.52756e+07 | bytes   |
| shared_day_limit limit=128.0MB | one_template_ranges              |    51           | count   |
| shared_day_limit limit=128.0MB | all_templates_files              |     1           | count   |
| shared_day_limit limit=128.0MB | all_templates_index_bytes        |     1.52756e+07 | bytes   |
| shared_day_limit limit=128.0MB | all_templates_ranges             |     1           | count   |
| shared_day_limit limit=128.0MB | open_file_index_memory_bytes     |     3.0238e+07  | bytes   |
| shared_day_limit limit=512.0MB | files                            |     2           | count   |
| shared_day_limit limit=512.0MB | row_groups                       | 26890           | count   |
| shared_day_limit limit=512.0MB | bytes                            |     6.58842e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_bytes                   |     4.53052e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_share_pct               |    68.77        | percent |
| shared_day_limit limit=512.0MB | disk_blocks                      |     6.58883e+07 | bytes   |
| shared_day_limit limit=512.0MB | ratio_to_raw                     |     1.472       | ratio   |
| shared_day_limit limit=512.0MB | one_template_files               |     1           | count   |
| shared_day_limit limit=512.0MB | one_template_index_bytes         |     1.52756e+07 | bytes   |
| shared_day_limit limit=512.0MB | one_template_ranges              |    51           | count   |
| shared_day_limit limit=512.0MB | all_templates_files              |     1           | count   |
| shared_day_limit limit=512.0MB | all_templates_index_bytes        |     1.52756e+07 | bytes   |
| shared_day_limit limit=512.0MB | all_templates_ranges             |     1           | count   |
| shared_day_limit limit=512.0MB | open_file_index_memory_bytes     |     1.20953e+08 | bytes   |
| shared_day_limit limit=1.0GB   | files                            |     2           | count   |
| shared_day_limit limit=1.0GB   | row_groups                       | 26890           | count   |
| shared_day_limit limit=1.0GB   | bytes                            |     6.58842e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_bytes                   |     4.53052e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_share_pct               |    68.77        | percent |
| shared_day_limit limit=1.0GB   | disk_blocks                      |     6.58883e+07 | bytes   |
| shared_day_limit limit=1.0GB   | ratio_to_raw                     |     1.472       | ratio   |
| shared_day_limit limit=1.0GB   | one_template_files               |     1           | count   |
| shared_day_limit limit=1.0GB   | one_template_index_bytes         |     1.52756e+07 | bytes   |
| shared_day_limit limit=1.0GB   | one_template_ranges              |    51           | count   |
| shared_day_limit limit=1.0GB   | all_templates_files              |     1           | count   |
| shared_day_limit limit=1.0GB   | all_templates_index_bytes        |     1.52756e+07 | bytes   |
| shared_day_limit limit=1.0GB   | all_templates_ranges             |     1           | count   |
| shared_day_limit limit=1.0GB   | open_file_index_memory_bytes     |     2.41907e+08 | bytes   |
| mod_n n=4                      | files                            |     8           | count   |
| mod_n n=4                      | row_groups                       | 26890           | count   |
| mod_n n=4                      | bytes                            |     6.58882e+07 | bytes   |
| mod_n n=4                      | overhead_bytes                   |     4.53092e+07 | bytes   |
| mod_n n=4                      | overhead_share_pct               |    68.77        | percent |
| mod_n n=4                      | disk_blocks                      |     6.59087e+07 | bytes   |
| mod_n n=4                      | ratio_to_raw                     |     1.472       | ratio   |
| mod_n n=4                      | one_template_files               |     1           | count   |
| mod_n n=4                      | one_template_index_bytes         |     3.96877e+06 | bytes   |
| mod_n n=4                      | one_template_ranges              |    51           | count   |
| mod_n n=4                      | all_templates_files              |     4           | count   |
| mod_n n=4                      | all_templates_index_bytes        |     1.52779e+07 | bytes   |
| mod_n n=4                      | all_templates_ranges             |     4           | count   |
| mod_n n=4                      | byte_spread_largest_over_average |     1.403       | ratio   |
| mod_n n=8                      | files                            |    16           | count   |
| mod_n n=8                      | row_groups                       | 26890           | count   |
| mod_n n=8                      | bytes                            |     6.57883e+07 | bytes   |
| mod_n n=8                      | overhead_bytes                   |     4.52094e+07 | bytes   |
| mod_n n=8                      | overhead_share_pct               |    68.72        | percent |
| mod_n n=8                      | disk_blocks                      |     6.58268e+07 | bytes   |
| mod_n n=8                      | ratio_to_raw                     |     1.471       | ratio   |
| mod_n n=8                      | one_template_files               |     1           | count   |
| mod_n n=8                      | one_template_index_bytes         |     1.82735e+06 | bytes   |
| mod_n n=8                      | one_template_ranges              |    51           | count   |
| mod_n n=8                      | all_templates_files              |     8           | count   |
| mod_n n=8                      | all_templates_index_bytes        |     1.52477e+07 | bytes   |
| mod_n n=8                      | all_templates_ranges             |     8           | count   |
| mod_n n=8                      | byte_spread_largest_over_average |     1.846       | ratio   |
| least_full n=4                 | files                            |     8           | count   |
| least_full n=4                 | row_groups                       | 26890           | count   |
| least_full n=4                 | bytes                            |     6.58882e+07 | bytes   |
| least_full n=4                 | overhead_bytes                   |     4.53092e+07 | bytes   |
| least_full n=4                 | overhead_share_pct               |    68.77        | percent |
| least_full n=4                 | disk_blocks                      |     6.59046e+07 | bytes   |
| least_full n=4                 | ratio_to_raw                     |     1.472       | ratio   |
| least_full n=4                 | one_template_files               |     4           | count   |
| least_full n=4                 | one_template_index_bytes         |     1.52779e+07 | bytes   |
| least_full n=4                 | one_template_ranges              |    51           | count   |
| least_full n=4                 | all_templates_files              |     4           | count   |
| least_full n=4                 | all_templates_index_bytes        |     1.52779e+07 | bytes   |
| least_full n=4                 | all_templates_ranges             |     4           | count   |
| least_full n=4                 | byte_spread_largest_over_average |     1.06        | ratio   |
| least_full n=8                 | files                            |    16           | count   |
| least_full n=8                 | row_groups                       | 26890           | count   |
| least_full n=8                 | bytes                            |     6.58935e+07 | bytes   |
| least_full n=8                 | overhead_bytes                   |     4.53145e+07 | bytes   |
| least_full n=8                 | overhead_share_pct               |    68.77        | percent |
| least_full n=8                 | disk_blocks                      |     6.5921e+07  | bytes   |
| least_full n=8                 | ratio_to_raw                     |     1.473       | ratio   |
| least_full n=8                 | one_template_files               |     8           | count   |
| least_full n=8                 | one_template_index_bytes         |     1.52809e+07 | bytes   |
| least_full n=8                 | one_template_ranges              |    51           | count   |
| least_full n=8                 | all_templates_files              |     8           | count   |
| least_full n=8                 | all_templates_index_bytes        |     1.52809e+07 | bytes   |
| least_full n=8                 | all_templates_ranges             |     8           | count   |
| least_full n=8                 | byte_spread_largest_over_average |     1.144       | ratio   |
| calibration                    | real_col_bytes                   |     2.38031e+07 | bytes   |
| calibration                    | real_col_files                   |  4931           | count   |
| calibration                    | real_col_blocks                  |     3.7036e+07  | bytes   |
| calibration                    | real_meta_bytes                  |     1.37538e+07 | bytes   |
| calibration                    | real_meta_blocks                 |     3.25181e+07 | bytes   |
| calibration                    | real_meta_files                  |  4932           | count   |
| calibration                    | real_segments_over_ceil          |     3           | count   |
| calibration                    | real_bytes_over_ceil             |     2.48627e+06 | bytes   |
| calibration                    | model_col_bytes                  |     2.38031e+07 | bytes   |
| calibration                    | model_col_files                  |  4931           | count   |
| calibration                    | model_col_blocks                 |     3.45498e+07 | bytes   |
| calibration                    | model_minus_real_pct             |     0           | percent |

**Notes**

- close reason
- lines in memory and not yet in a closed row group - what a crash replays
- 32 bytes of section 7.1 record per line plus 233 bytes of line; zstd 3 over batches of 500 lines, from script 08
- zstd 3 over batches of 500 lines, from script 08
- what TrieDB does today: one segment per leaf per day
- one day (2024-01-19); the one-template search is template 1055466456870197778
- one shared file per day, rolled at 128.0MB
- 55,183 row groups x 39 columns x 12 bytes, plus 24 per row group, at the size limit
- one shared file per day, rolled at 512.0MB
- 220,735 row groups x 39 columns x 12 bytes, plus 24 per row group, at the size limit
- one shared file per day, rolled at 1.0GB
- 441,470 row groups x 39 columns x 12 bytes, plus 24 per row group, at the size limit
- 4 files per day, by template mod 4
- across the N files of one day; 1.0 is perfectly even
- 8 files per day, by template mod 8
- 4 files per day, each row group to the file with the fewest bytes
- 8 files per day, each row group to the file with the fewest bytes
- 200,000 lines ingested over POST /ingest, server stopped with SIGTERM so every segment is sealed
- the model's per-template-per-day bytes against the binary's
- 32 bytes of section 7.1 record per line plus 224 bytes of line; zstd 3 over batches of 500 lines, from script 08
- one day (2024-01-15); the one-template search is template 1055466456870197778
- 54,779 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit
- 219,118 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit
- 438,237 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit

## 04_merge_rules

_Which merging rule_

Run 2026-09-18T21:58:14+00:00, git `b405451`, 291 rows.

### `04_merge_rules` / dataset `flat30`

| variant                    | metric                       | value   | unit    |
|----------------------------|------------------------------|---------|---------|
| NONE                       | templates                    | 870     | count   |
| NONE                       | rows_per_template_min        | 3       | count   |
| NONE                       | rows_per_template_median     | 69      | count   |
| NONE                       | rows_per_template_p90        | 545     | count   |
| NONE                       | rows_per_template_max        | 10,711  | count   |
| NONE                       | null_cell_share_pct          | 0       | percent |
| NONE                       | columns_widened_to_string    | 0       | count   |
| NONE                       | mixed_promoted_groups        | 0       | count   |
| NONE                       | rows_in_mixed_promoted_group | 0       | count   |
| NONE                       | template_count_min           | 870     | count   |
| NONE                       | template_count_max           | 870     | count   |
| NONE                       | template_count_spread        | 0       | count   |
| NONE                       | moved_share_pct_max          | 0       | percent |
| NONE                       | moved_share_pct_mean         | 0       | percent |
| NONE                       | search_rows_scanned          | 137,439 | rows    |
| NONE                       | search_scan_amplification    | 1       | ratio   |
| J70                        | templates                    | 68      | count   |
| J70                        | rows_per_template_min        | 743     | count   |
| J70                        | rows_per_template_median     | 2,668   | count   |
| J70                        | rows_per_template_p90        | 4,353   | count   |
| J70                        | rows_per_template_max        | 16,622  | count   |
| J70                        | null_cell_share_pct          | 16.56   | percent |
| J70                        | columns_widened_to_string    | 0       | count   |
| J70                        | mixed_promoted_groups        | 0       | count   |
| J70                        | rows_in_mixed_promoted_group | 0       | count   |
| J70                        | template_count_min           | 67      | count   |
| J70                        | template_count_max           | 67      | count   |
| J70                        | template_count_spread        | 0       | count   |
| J70                        | moved_share_pct_max          | 19.32   | percent |
| J70                        | moved_share_pct_mean         | 19.03   | percent |
| J70                        | search_rows_scanned          | 181,058 | rows    |
| J70                        | search_scan_amplification    | 1.317   | ratio   |
| J80                        | templates                    | 125     | count   |
| J80                        | rows_per_template_min        | 42      | count   |
| J80                        | rows_per_template_median     | 1,256   | count   |
| J80                        | rows_per_template_p90        | 2,903   | count   |
| J80                        | rows_per_template_max        | 16,616  | count   |
| J80                        | null_cell_share_pct          | 13.96   | percent |
| J80                        | columns_widened_to_string    | 0       | count   |
| J80                        | mixed_promoted_groups        | 0       | count   |
| J80                        | rows_in_mixed_promoted_group | 0       | count   |
| J80                        | template_count_min           | 133     | count   |
| J80                        | template_count_max           | 137     | count   |
| J80                        | template_count_spread        | 4       | count   |
| J80                        | moved_share_pct_max          | 25.23   | percent |
| J80                        | moved_share_pct_mean         | 23.79   | percent |
| J80                        | search_rows_scanned          | 163,246 | rows    |
| J80                        | search_scan_amplification    | 1.188   | ratio   |
| J90                        | templates                    | 440     | count   |
| J90                        | rows_per_template_min        | 5       | count   |
| J90                        | rows_per_template_median     | 281     | count   |
| J90                        | rows_per_template_p90        | 982     | count   |
| J90                        | rows_per_template_max        | 10,711  | count   |
| J90                        | null_cell_share_pct          | 4.032   | percent |
| J90                        | columns_widened_to_string    | 0       | count   |
| J90                        | mixed_promoted_groups        | 0       | count   |
| J90                        | rows_in_mixed_promoted_group | 0       | count   |
| J90                        | template_count_min           | 439     | count   |
| J90                        | template_count_max           | 457     | count   |
| J90                        | template_count_spread        | 18      | count   |
| J90                        | moved_share_pct_max          | 9.572   | percent |
| J90                        | moved_share_pct_mean         | 9.022   | percent |
| J90                        | search_rows_scanned          | 143,332 | rows    |
| J90                        | search_scan_amplification    | 1.043   | ratio   |
| J80_GROWN                  | templates                    | 199     | count   |
| J80_GROWN                  | rows_per_template_min        | 38      | count   |
| J80_GROWN                  | rows_per_template_median     | 535     | count   |
| J80_GROWN                  | rows_per_template_p90        | 2,393   | count   |
| J80_GROWN                  | rows_per_template_max        | 16,616  | count   |
| J80_GROWN                  | null_cell_share_pct          | 7.404   | percent |
| J80_GROWN                  | columns_widened_to_string    | 0       | count   |
| J80_GROWN                  | mixed_promoted_groups        | 0       | count   |
| J80_GROWN                  | rows_in_mixed_promoted_group | 0       | count   |
| J80_GROWN                  | template_count_min           | 181     | count   |
| J80_GROWN                  | template_count_max           | 194     | count   |
| J80_GROWN                  | template_count_spread        | 13      | count   |
| J80_GROWN                  | moved_share_pct_max          | 19.95   | percent |
| J80_GROWN                  | moved_share_pct_mean         | 18.53   | percent |
| J80_GROWN                  | search_rows_scanned          | 151,465 | rows    |
| J80_GROWN                  | search_scan_amplification    | 1.102   | ratio   |
| J80_SPEC                   | templates                    | 24      | count   |
| J80_SPEC                   | rows_per_template_min        | 252     | count   |
| J80_SPEC                   | rows_per_template_median     | 9,096   | count   |
| J80_SPEC                   | rows_per_template_p90        | 16,616  | count   |
| J80_SPEC                   | rows_per_template_max        | 23,299  | count   |
| J80_SPEC                   | null_cell_share_pct          | 14.11   | percent |
| J80_SPEC                   | columns_widened_to_string    | 0       | count   |
| J80_SPEC                   | mixed_promoted_groups        | 21      | count   |
| J80_SPEC                   | rows_in_mixed_promoted_group | 166,762 | count   |
| J80_SPEC                   | template_count_min           | 24      | count   |
| J80_SPEC                   | template_count_max           | 28      | count   |
| J80_SPEC                   | template_count_spread        | 4       | count   |
| J80_SPEC                   | moved_share_pct_max          | 28      | percent |
| J80_SPEC                   | moved_share_pct_mean         | 21.9    | percent |
| J80_SPEC                   | search_rows_scanned          | 883,214 | rows    |
| J80_SPEC                   | search_scan_amplification    | 6.426   | ratio   |
| PER_EVENT                  | templates                    | 7       | count   |
| PER_EVENT                  | rows_per_template_min        | 27,694  | count   |
| PER_EVENT                  | rows_per_template_median     | 27,812  | count   |
| PER_EVENT                  | rows_per_template_p90        | 33,238  | count   |
| PER_EVENT                  | rows_per_template_max        | 33,238  | count   |
| PER_EVENT                  | null_cell_share_pct          | 77.77   | percent |
| PER_EVENT                  | columns_widened_to_string    | 0       | count   |
| PER_EVENT                  | mixed_promoted_groups        | 0       | count   |
| PER_EVENT                  | rows_in_mixed_promoted_group | 0       | count   |
| PER_EVENT                  | template_count_min           | 7       | count   |
| PER_EVENT                  | template_count_max           | 7       | count   |
| PER_EVENT                  | template_count_spread        | 0       | count   |
| PER_EVENT                  | moved_share_pct_max          | 0       | percent |
| PER_EVENT                  | moved_share_pct_mean         | 0       | percent |
| PER_EVENT                  | search_rows_scanned          | 566,762 | rows    |
| PER_EVENT                  | search_scan_amplification    | 4.124   | ratio   |
| all                        | keys_with_more_than_one_type | 0       | count   |
| promote=nothing/NONE       | templates                    | 150     | count   |
| promote=nothing/NONE       | rows_scanned_event_search    | 166,762 | rows    |
| promote=nothing/NONE       | rows_scanned_service_search  | 66,464  | rows    |
| promote=nothing/J80        | templates                    | 24      | count   |
| promote=nothing/J80        | rows_scanned_event_search    | 166,762 | rows    |
| promote=nothing/J80        | rows_scanned_service_search  | 66,464  | rows    |
| promote=event/NONE         | templates                    | 870     | count   |
| promote=event/NONE         | rows_scanned_event_search    | 27,892  | rows    |
| promote=event/NONE         | rows_scanned_service_search  | 66,464  | rows    |
| promote=event/J80          | templates                    | 125     | count   |
| promote=event/J80          | rows_scanned_event_search    | 27,892  | rows    |
| promote=event/J80          | rows_scanned_service_search  | 66,464  | rows    |
| promote=event+service/NONE | templates                    | 1,182   | count   |
| promote=event+service/NONE | rows_scanned_event_search    | 27,892  | rows    |
| promote=event+service/NONE | rows_scanned_service_search  | 13,447  | rows    |
| promote=event+service/J80  | templates                    | 259     | count   |
| promote=event+service/J80  | rows_scanned_event_search    | 27,892  | rows    |
| promote=event+service/J80  | rows_scanned_service_search  | 13,447  | rows    |
| promote=binary/NONE        | templates                    | 870     | count   |
| promote=binary/NONE        | rows_scanned_event_search    | 27,892  | rows    |
| promote=binary/NONE        | rows_scanned_service_search  | 66,464  | rows    |
| promote=binary/J80         | templates                    | 125     | count   |
| promote=binary/J80         | rows_scanned_event_search    | 27,892  | rows    |
| promote=binary/J80         | rows_scanned_service_search  | 66,464  | rows    |
| J80 cap=100                | leftover_rows                | 5,053   | count   |
| J80 cap=100                | leftover_row_share_pct       | 2.526   | percent |
| J80 cap=100                | leftover_columns             | 38      | count   |
| J80 cap=100                | leftover_null_cell_share_pct | 69.53   | percent |
| J80 cap=500                | leftover_rows                | 0       | rows    |
| J80 cap=2000               | leftover_rows                | 0       | rows    |
| verdict                    | best_two_rules               | J70 J80 | name    |

### `04_merge_rules` / dataset `uneven`

| variant                    | metric                       | value         | unit    |
|----------------------------|------------------------------|---------------|---------|
| NONE                       | templates                    | 4,931         | count   |
| NONE                       | rows_per_template_min        | 4             | count   |
| NONE                       | rows_per_template_median     | 6             | count   |
| NONE                       | rows_per_template_p90        | 13            | count   |
| NONE                       | rows_per_template_max        | 54,721        | count   |
| NONE                       | null_cell_share_pct          | 0             | percent |
| NONE                       | columns_widened_to_string    | 0             | count   |
| NONE                       | mixed_promoted_groups        | 0             | count   |
| NONE                       | rows_in_mixed_promoted_group | 0             | count   |
| NONE                       | template_count_min           | 4,931         | count   |
| NONE                       | template_count_max           | 4,931         | count   |
| NONE                       | template_count_spread        | 0             | count   |
| NONE                       | moved_share_pct_max          | 0             | percent |
| NONE                       | moved_share_pct_mean         | 0             | percent |
| NONE                       | search_rows_scanned          | 54,159        | rows    |
| NONE                       | search_scan_amplification    | 1             | ratio   |
| J70                        | templates                    | 277           | count   |
| J70                        | rows_per_template_min        | 30            | count   |
| J70                        | rows_per_template_median     | 79            | count   |
| J70                        | rows_per_template_p90        | 333           | count   |
| J70                        | rows_per_template_max        | 70,149        | count   |
| J70                        | null_cell_share_pct          | 22.55         | percent |
| J70                        | columns_widened_to_string    | 0             | count   |
| J70                        | mixed_promoted_groups        | 0             | count   |
| J70                        | rows_in_mixed_promoted_group | 0             | count   |
| J70                        | template_count_min           | 268           | count   |
| J70                        | template_count_max           | 278           | count   |
| J70                        | template_count_spread        | 10            | count   |
| J70                        | moved_share_pct_max          | 47.54         | percent |
| J70                        | moved_share_pct_mean         | 26.69         | percent |
| J70                        | search_rows_scanned          | 76,651        | rows    |
| J70                        | search_scan_amplification    | 1.415         | ratio   |
| J80                        | templates                    | 700           | count   |
| J80                        | rows_per_template_min        | 5             | count   |
| J80                        | rows_per_template_median     | 42            | count   |
| J80                        | rows_per_template_p90        | 141           | count   |
| J80                        | rows_per_template_max        | 71,200        | count   |
| J80                        | null_cell_share_pct          | 14.61         | percent |
| J80                        | columns_widened_to_string    | 0             | count   |
| J80                        | mixed_promoted_groups        | 0             | count   |
| J80                        | rows_in_mixed_promoted_group | 0             | count   |
| J80                        | template_count_min           | 674           | count   |
| J80                        | template_count_max           | 690           | count   |
| J80                        | template_count_spread        | 16            | count   |
| J80                        | moved_share_pct_max          | 45.57         | percent |
| J80                        | moved_share_pct_mean         | 37.79         | percent |
| J80                        | search_rows_scanned          | 118,477       | rows    |
| J80                        | search_scan_amplification    | 2.188         | ratio   |
| J90                        | templates                    | 2,522         | count   |
| J90                        | rows_per_template_min        | 4             | count   |
| J90                        | rows_per_template_median     | 10            | count   |
| J90                        | rows_per_template_p90        | 34            | count   |
| J90                        | rows_per_template_max        | 54,721        | count   |
| J90                        | null_cell_share_pct          | 1.943         | percent |
| J90                        | columns_widened_to_string    | 0             | count   |
| J90                        | mixed_promoted_groups        | 0             | count   |
| J90                        | rows_in_mixed_promoted_group | 0             | count   |
| J90                        | template_count_min           | 2,487         | count   |
| J90                        | template_count_max           | 2,562         | count   |
| J90                        | template_count_spread        | 75            | count   |
| J90                        | moved_share_pct_max          | 4.747         | percent |
| J90                        | moved_share_pct_mean         | 4.691         | percent |
| J90                        | search_rows_scanned          | 60,728        | rows    |
| J90                        | search_scan_amplification    | 1.121         | ratio   |
| J80_GROWN                  | templates                    | 982           | count   |
| J80_GROWN                  | rows_per_template_min        | 1             | count   |
| J80_GROWN                  | rows_per_template_median     | 26            | count   |
| J80_GROWN                  | rows_per_template_p90        | 75            | count   |
| J80_GROWN                  | rows_per_template_max        | 60,730        | count   |
| J80_GROWN                  | null_cell_share_pct          | 3.612         | percent |
| J80_GROWN                  | columns_widened_to_string    | 0             | count   |
| J80_GROWN                  | mixed_promoted_groups        | 0             | count   |
| J80_GROWN                  | rows_in_mixed_promoted_group | 0             | count   |
| J80_GROWN                  | template_count_min           | 958           | count   |
| J80_GROWN                  | template_count_max           | 989           | count   |
| J80_GROWN                  | template_count_spread        | 31            | count   |
| J80_GROWN                  | moved_share_pct_max          | 10.29         | percent |
| J80_GROWN                  | moved_share_pct_mean         | 8.023         | percent |
| J80_GROWN                  | search_rows_scanned          | 61,864        | rows    |
| J80_GROWN                  | search_scan_amplification    | 1.142         | ratio   |
| J80_SPEC                   | templates                    | 135           | count   |
| J80_SPEC                   | rows_per_template_min        | 30            | count   |
| J80_SPEC                   | rows_per_template_median     | 265           | count   |
| J80_SPEC                   | rows_per_template_p90        | 881           | count   |
| J80_SPEC                   | rows_per_template_max        | 71,200        | count   |
| J80_SPEC                   | null_cell_share_pct          | 14.26         | percent |
| J80_SPEC                   | columns_widened_to_string    | 0             | count   |
| J80_SPEC                   | mixed_promoted_groups        | 116           | count   |
| J80_SPEC                   | rows_in_mixed_promoted_group | 35,603        | count   |
| J80_SPEC                   | template_count_min           | 125           | count   |
| J80_SPEC                   | template_count_max           | 136           | count   |
| J80_SPEC                   | template_count_spread        | 11            | count   |
| J80_SPEC                   | moved_share_pct_max          | 45.87         | percent |
| J80_SPEC                   | moved_share_pct_mean         | 37.89         | percent |
| J80_SPEC                   | search_rows_scanned          | 260,443       | rows    |
| J80_SPEC                   | search_scan_amplification    | 4.809         | ratio   |
| PER_EVENT                  | templates                    | 7             | count   |
| PER_EVENT                  | rows_per_template_min        | 5,912         | count   |
| PER_EVENT                  | rows_per_template_median     | 5,938         | count   |
| PER_EVENT                  | rows_per_template_p90        | 164,397       | count   |
| PER_EVENT                  | rows_per_template_max        | 164,397       | count   |
| PER_EVENT                  | null_cell_share_pct          | 69.85         | percent |
| PER_EVENT                  | columns_widened_to_string    | 0             | count   |
| PER_EVENT                  | mixed_promoted_groups        | 0             | count   |
| PER_EVENT                  | rows_in_mixed_promoted_group | 0             | count   |
| PER_EVENT                  | template_count_min           | 7             | count   |
| PER_EVENT                  | template_count_max           | 7             | count   |
| PER_EVENT                  | template_count_spread        | 0             | count   |
| PER_EVENT                  | moved_share_pct_max          | 0             | percent |
| PER_EVENT                  | moved_share_pct_mean         | 0             | percent |
| PER_EVENT                  | search_rows_scanned          | 435,603       | rows    |
| PER_EVENT                  | search_scan_amplification    | 8.043         | ratio   |
| all                        | keys_with_more_than_one_type | 0             | count   |
| promote=nothing/NONE       | templates                    | 894           | count   |
| promote=nothing/NONE       | rows_scanned_event_search    | 35,603        | rows    |
| promote=nothing/NONE       | rows_scanned_service_search  | 168,736       | rows    |
| promote=nothing/J80        | templates                    | 135           | count   |
| promote=nothing/J80        | rows_scanned_event_search    | 35,603        | rows    |
| promote=nothing/J80        | rows_scanned_service_search  | 168,736       | rows    |
| promote=event/NONE         | templates                    | 4,931         | count   |
| promote=event/NONE         | rows_scanned_event_search    | 5,957         | rows    |
| promote=event/NONE         | rows_scanned_service_search  | 168,736       | rows    |
| promote=event/J80          | templates                    | 700           | count   |
| promote=event/J80          | rows_scanned_event_search    | 5,957         | rows    |
| promote=event/J80          | rows_scanned_service_search  | 168,736       | rows    |
| promote=event+service/NONE | templates                    | 6,459         | count   |
| promote=event+service/NONE | rows_scanned_event_search    | 5,957         | rows    |
| promote=event+service/NONE | rows_scanned_service_search  | 33,932        | rows    |
| promote=event+service/J80  | templates                    | 1,306         | count   |
| promote=event+service/J80  | rows_scanned_event_search    | 5,957         | rows    |
| promote=event+service/J80  | rows_scanned_service_search  | 33,932        | rows    |
| promote=binary/NONE        | templates                    | 4,931         | count   |
| promote=binary/NONE        | rows_scanned_event_search    | 5,957         | rows    |
| promote=binary/NONE        | rows_scanned_service_search  | 168,736       | rows    |
| promote=binary/J80         | templates                    | 700           | count   |
| promote=binary/J80         | rows_scanned_event_search    | 5,957         | rows    |
| promote=binary/J80         | rows_scanned_service_search  | 168,736       | rows    |
| J80 cap=100                | leftover_rows                | 22,427        | count   |
| J80 cap=100                | leftover_row_share_pct       | 11.21         | percent |
| J80 cap=100                | leftover_columns             | 63            | count   |
| J80 cap=100                | leftover_null_cell_share_pct | 84.18         | percent |
| J80 cap=500                | leftover_rows                | 2,156         | count   |
| J80 cap=500                | leftover_row_share_pct       | 1.078         | percent |
| J80 cap=500                | leftover_columns             | 58            | count   |
| J80 cap=500                | leftover_null_cell_share_pct | 85.92         | percent |
| J80 cap=2000               | leftover_rows                | 0             | rows    |
| verdict                    | best_two_rules               | J70 J80_GROWN | name    |

**Notes**

- 20 searches of 'one promoted value plus one optional key is present'; 137,439 rows actually match, so this is the scan cost
- rows scanned divided by rows that match
- the type join can only widen a column where one key carries two types; at zero, every columns->String figure above is vacuous
- one value of `event`; a promoted key is one trie edge, an unpromoted one is a scan of every template holding the column
- lines that would have created a template past the cap go into one shared leftover template, which is never itself a merge target
- the cap was never reached
- written to lab/out/04/best_rules.json for script 02 to pick up
- 20 searches of 'one promoted value plus one optional key is present'; 54,159 rows actually match, so this is the scan cost

## 05_encodings

_What each encoding saves_

Run 2026-09-18T22:09:14+00:00, git `b405451`, 614 rows.

### `05_encodings` / dataset `flat30`

| variant                | metric                        | value              | unit    |
|------------------------|-------------------------------|--------------------|---------|
| arrival/Bool           | bits                          | 6,290              | bytes   |
| arrival/Bool           | bits+zstd3                    | 6,968              | bytes   |
| arrival/Bool           | const                         | 50,049             | bytes   |
| arrival/Bool           | const+zstd3                   | 15,001             | bytes   |
| arrival/Bool           | delta_varint                  | 50,049             | bytes   |
| arrival/Bool           | delta_varint+zstd3            | 15,001             | bytes   |
| arrival/Bool           | dict                          | 50,574             | bytes   |
| arrival/Bool           | dict+zstd3                    | 15,243             | bytes   |
| arrival/Bool           | ip_bin                        | 50,049             | bytes   |
| arrival/Bool           | ip_bin+zstd3                  | 15,001             | bytes   |
| arrival/Bool           | plain                         | 50,049             | bytes   |
| arrival/Bool           | plain+zstd3                   | 15,001             | bytes   |
| arrival/Bool           | uuid16                        | 50,049             | bytes   |
| arrival/Bool           | uuid16+zstd3                  | 15,001             | bytes   |
| arrival/Bool           | varint                        | 50,049             | bytes   |
| arrival/Bool           | varint+zstd3                  | 15,001             | bytes   |
| arrival/Datetime       | bits                          | 3,088,072          | bytes   |
| arrival/Datetime       | bits+zstd3                    | 2,806,442          | bytes   |
| arrival/Datetime       | const                         | 3,088,072          | bytes   |
| arrival/Datetime       | const+zstd3                   | 2,806,442          | bytes   |
| arrival/Datetime       | delta_varint                  | 3,180,148          | bytes   |
| arrival/Datetime       | delta_varint+zstd3            | 2,930,476          | bytes   |
| arrival/Datetime       | dict                          | 3,288,132          | bytes   |
| arrival/Datetime       | dict+zstd3                    | 3,018,965          | bytes   |
| arrival/Datetime       | ip_bin                        | 3,088,072          | bytes   |
| arrival/Datetime       | ip_bin+zstd3                  | 2,806,442          | bytes   |
| arrival/Datetime       | plain                         | 3,088,072          | bytes   |
| arrival/Datetime       | plain+zstd3                   | 2,806,442          | bytes   |
| arrival/Datetime       | uuid16                        | 3,088,072          | bytes   |
| arrival/Datetime       | uuid16+zstd3                  | 2,806,442          | bytes   |
| arrival/Datetime       | varint                        | 3,547,920          | bytes   |
| arrival/Datetime       | varint+zstd3                  | 2,918,497          | bytes   |
| arrival/Float64        | bits                          | 800,392            | bytes   |
| arrival/Float64        | bits+zstd3                    | 441,505            | bytes   |
| arrival/Float64        | const                         | 800,392            | bytes   |
| arrival/Float64        | const+zstd3                   | 441,505            | bytes   |
| arrival/Float64        | delta_varint                  | 800,392            | bytes   |
| arrival/Float64        | delta_varint+zstd3            | 441,505            | bytes   |
| arrival/Float64        | dict                          | 870,842            | bytes   |
| arrival/Float64        | dict+zstd3                    | 499,890            | bytes   |
| arrival/Float64        | ip_bin                        | 800,392            | bytes   |
| arrival/Float64        | ip_bin+zstd3                  | 441,505            | bytes   |
| arrival/Float64        | plain                         | 800,392            | bytes   |
| arrival/Float64        | plain+zstd3                   | 441,505            | bytes   |
| arrival/Float64        | uuid16                        | 800,392            | bytes   |
| arrival/Float64        | uuid16+zstd3                  | 441,505            | bytes   |
| arrival/Float64        | varint                        | 800,392            | bytes   |
| arrival/Float64        | varint+zstd3                  | 441,505            | bytes   |
| arrival/Int64          | bits                          | 1,842,016          | bytes   |
| arrival/Int64          | bits+zstd3                    | 682,919            | bytes   |
| arrival/Int64          | const                         | 1,842,016          | bytes   |
| arrival/Int64          | const+zstd3                   | 682,919            | bytes   |
| arrival/Int64          | delta_varint                  | 583,286            | bytes   |
| arrival/Int64          | delta_varint+zstd3            | 591,270            | bytes   |
| arrival/Int64          | dict                          | 2,048,420          | bytes   |
| arrival/Int64          | dict+zstd3                    | 928,634            | bytes   |
| arrival/Int64          | ip_bin                        | 1,842,016          | bytes   |
| arrival/Int64          | ip_bin+zstd3                  | 682,919            | bytes   |
| arrival/Int64          | plain                         | 1,842,016          | bytes   |
| arrival/Int64          | plain+zstd3                   | 682,919            | bytes   |
| arrival/Int64          | uuid16                        | 1,842,016          | bytes   |
| arrival/Int64          | uuid16+zstd3                  | 682,919            | bytes   |
| arrival/Int64          | varint                        | 614,695            | bytes   |
| arrival/Int64          | varint+zstd3                  | 602,402            | bytes   |
| arrival/Ip             | bits                          | 1,622,899          | bytes   |
| arrival/Ip             | bits+zstd3                    | 682,612            | bytes   |
| arrival/Ip             | const                         | 1,622,899          | bytes   |
| arrival/Ip             | const+zstd3                   | 682,612            | bytes   |
| arrival/Ip             | delta_varint                  | 1,622,899          | bytes   |
| arrival/Ip             | delta_varint+zstd3            | 682,612            | bytes   |
| arrival/Ip             | dict                          | 1,623,017          | bytes   |
| arrival/Ip             | dict+zstd3                    | 682,669            | bytes   |
| arrival/Ip             | ip_bin                        | 620,974            | bytes   |
| arrival/Ip             | ip_bin+zstd3                  | 447,041            | bytes   |
| arrival/Ip             | plain                         | 1,622,899          | bytes   |
| arrival/Ip             | plain+zstd3                   | 682,612            | bytes   |
| arrival/Ip             | uuid16                        | 1,622,899          | bytes   |
| arrival/Ip             | uuid16+zstd3                  | 682,612            | bytes   |
| arrival/Ip             | varint                        | 1,622,899          | bytes   |
| arrival/Ip             | varint+zstd3                  | 682,612            | bytes   |
| arrival/Null           | bits                          | 6,714              | bytes   |
| arrival/Null           | bits+zstd3                    | 8,857              | bytes   |
| arrival/Null           | const                         | 6,714              | bytes   |
| arrival/Null           | const+zstd3                   | 8,857              | bytes   |
| arrival/Null           | delta_varint                  | 6,714              | bytes   |
| arrival/Null           | delta_varint+zstd3            | 8,857              | bytes   |
| arrival/Null           | dict                          | 6,714              | bytes   |
| arrival/Null           | dict+zstd3                    | 8,857              | bytes   |
| arrival/Null           | ip_bin                        | 6,714              | bytes   |
| arrival/Null           | ip_bin+zstd3                  | 8,857              | bytes   |
| arrival/Null           | plain                         | 6,714              | bytes   |
| arrival/Null           | plain+zstd3                   | 8,857              | bytes   |
| arrival/Null           | uuid16                        | 6,714              | bytes   |
| arrival/Null           | uuid16+zstd3                  | 8,857              | bytes   |
| arrival/Null           | varint                        | 6,714              | bytes   |
| arrival/Null           | varint+zstd3                  | 8,857              | bytes   |
| arrival/String         | bits                          | 14,167,083         | bytes   |
| arrival/String         | bits+zstd3                    | 1,804,360          | bytes   |
| arrival/String         | const                         | 11,599,826         | bytes   |
| arrival/String         | const+zstd3                   | 1,800,675          | bytes   |
| arrival/String         | delta_varint                  | 14,167,083         | bytes   |
| arrival/String         | delta_varint+zstd3            | 1,804,360          | bytes   |
| arrival/String         | dict                          | 4,015,191          | bytes   |
| arrival/String         | dict+zstd3                    | 1,371,427          | bytes   |
| arrival/String         | ip_bin                        | 14,167,083         | bytes   |
| arrival/String         | ip_bin+zstd3                  | 1,804,360          | bytes   |
| arrival/String         | plain                         | 14,167,083         | bytes   |
| arrival/String         | plain+zstd3                   | 1,804,360          | bytes   |
| arrival/String         | uuid16                        | 14,167,083         | bytes   |
| arrival/String         | uuid16+zstd3                  | 1,804,360          | bytes   |
| arrival/String         | varint                        | 14,167,083         | bytes   |
| arrival/String         | varint+zstd3                  | 1,804,360          | bytes   |
| arrival/Uuid           | bits                          | 3,324,600          | bytes   |
| arrival/Uuid           | bits+zstd3                    | 1,741,273          | bytes   |
| arrival/Uuid           | const                         | 3,324,600          | bytes   |
| arrival/Uuid           | const+zstd3                   | 1,741,273          | bytes   |
| arrival/Uuid           | delta_varint                  | 3,324,600          | bytes   |
| arrival/Uuid           | delta_varint+zstd3            | 1,741,273          | bytes   |
| arrival/Uuid           | dict                          | 3,324,659          | bytes   |
| arrival/Uuid           | dict+zstd3                    | 1,741,339          | bytes   |
| arrival/Uuid           | ip_bin                        | 3,324,600          | bytes   |
| arrival/Uuid           | ip_bin+zstd3                  | 1,741,273          | bytes   |
| arrival/Uuid           | plain                         | 3,324,600          | bytes   |
| arrival/Uuid           | plain+zstd3                   | 1,741,273          | bytes   |
| arrival/Uuid           | uuid16                        | 1,329,840          | bytes   |
| arrival/Uuid           | uuid16+zstd3                  | 1,336,256          | bytes   |
| arrival/Uuid           | varint                        | 3,324,600          | bytes   |
| arrival/Uuid           | varint+zstd3                  | 1,741,273          | bytes   |
| sorted/Bool            | bits                          | 6,290              | bytes   |
| sorted/Bool            | bits+zstd3                    | 1,641              | bytes   |
| sorted/Bool            | const                         | 50,049             | bytes   |
| sorted/Bool            | const+zstd3                   | 1,666              | bytes   |
| sorted/Bool            | delta_varint                  | 50,049             | bytes   |
| sorted/Bool            | delta_varint+zstd3            | 1,666              | bytes   |
| sorted/Bool            | dict                          | 50,574             | bytes   |
| sorted/Bool            | dict+zstd3                    | 2,116              | bytes   |
| sorted/Bool            | ip_bin                        | 50,049             | bytes   |
| sorted/Bool            | ip_bin+zstd3                  | 1,666              | bytes   |
| sorted/Bool            | plain                         | 50,049             | bytes   |
| sorted/Bool            | plain+zstd3                   | 1,666              | bytes   |
| sorted/Bool            | uuid16                        | 50,049             | bytes   |
| sorted/Bool            | uuid16+zstd3                  | 1,666              | bytes   |
| sorted/Bool            | varint                        | 50,049             | bytes   |
| sorted/Bool            | varint+zstd3                  | 1,666              | bytes   |
| sorted/Datetime        | bits                          | 3,088,072          | bytes   |
| sorted/Datetime        | bits+zstd3                    | 2,806,248          | bytes   |
| sorted/Datetime        | const                         | 3,088,072          | bytes   |
| sorted/Datetime        | const+zstd3                   | 2,806,248          | bytes   |
| sorted/Datetime        | delta_varint                  | 3,178,537          | bytes   |
| sorted/Datetime        | delta_varint+zstd3            | 2,929,654          | bytes   |
| sorted/Datetime        | dict                          | 3,288,132          | bytes   |
| sorted/Datetime        | dict+zstd3                    | 3,018,768          | bytes   |
| sorted/Datetime        | ip_bin                        | 3,088,072          | bytes   |
| sorted/Datetime        | ip_bin+zstd3                  | 2,806,248          | bytes   |
| sorted/Datetime        | plain                         | 3,088,072          | bytes   |
| sorted/Datetime        | plain+zstd3                   | 2,806,248          | bytes   |
| sorted/Datetime        | uuid16                        | 3,088,072          | bytes   |
| sorted/Datetime        | uuid16+zstd3                  | 2,806,248          | bytes   |
| sorted/Datetime        | varint                        | 3,547,920          | bytes   |
| sorted/Datetime        | varint+zstd3                  | 2,918,775          | bytes   |
| sorted/Float64         | bits                          | 800,392            | bytes   |
| sorted/Float64         | bits+zstd3                    | 439,998            | bytes   |
| sorted/Float64         | const                         | 800,392            | bytes   |
| sorted/Float64         | const+zstd3                   | 439,998            | bytes   |
| sorted/Float64         | delta_varint                  | 800,392            | bytes   |
| sorted/Float64         | delta_varint+zstd3            | 439,998            | bytes   |
| sorted/Float64         | dict                          | 870,842            | bytes   |
| sorted/Float64         | dict+zstd3                    | 498,304            | bytes   |
| sorted/Float64         | ip_bin                        | 800,392            | bytes   |
| sorted/Float64         | ip_bin+zstd3                  | 439,998            | bytes   |
| sorted/Float64         | plain                         | 800,392            | bytes   |
| sorted/Float64         | plain+zstd3                   | 439,998            | bytes   |
| sorted/Float64         | uuid16                        | 800,392            | bytes   |
| sorted/Float64         | uuid16+zstd3                  | 439,998            | bytes   |
| sorted/Float64         | varint                        | 800,392            | bytes   |
| sorted/Float64         | varint+zstd3                  | 439,998            | bytes   |
| sorted/Int64           | bits                          | 1,842,016          | bytes   |
| sorted/Int64           | bits+zstd3                    | 679,937            | bytes   |
| sorted/Int64           | const                         | 1,842,016          | bytes   |
| sorted/Int64           | const+zstd3                   | 679,937            | bytes   |
| sorted/Int64           | delta_varint                  | 572,499            | bytes   |
| sorted/Int64           | delta_varint+zstd3            | 569,882            | bytes   |
| sorted/Int64           | dict                          | 2,048,420          | bytes   |
| sorted/Int64           | dict+zstd3                    | 927,413            | bytes   |
| sorted/Int64           | ip_bin                        | 1,842,016          | bytes   |
| sorted/Int64           | ip_bin+zstd3                  | 679,937            | bytes   |
| sorted/Int64           | plain                         | 1,842,016          | bytes   |
| sorted/Int64           | plain+zstd3                   | 679,937            | bytes   |
| sorted/Int64           | uuid16                        | 1,842,016          | bytes   |
| sorted/Int64           | uuid16+zstd3                  | 679,937            | bytes   |
| sorted/Int64           | varint                        | 614,695            | bytes   |
| sorted/Int64           | varint+zstd3                  | 602,500            | bytes   |
| sorted/Ip              | bits                          | 1,622,899          | bytes   |
| sorted/Ip              | bits+zstd3                    | 682,455            | bytes   |
| sorted/Ip              | const                         | 1,622,899          | bytes   |
| sorted/Ip              | const+zstd3                   | 682,455            | bytes   |
| sorted/Ip              | delta_varint                  | 1,622,899          | bytes   |
| sorted/Ip              | delta_varint+zstd3            | 682,455            | bytes   |
| sorted/Ip              | dict                          | 1,623,017          | bytes   |
| sorted/Ip              | dict+zstd3                    | 682,500            | bytes   |
| sorted/Ip              | ip_bin                        | 620,974            | bytes   |
| sorted/Ip              | ip_bin+zstd3                  | 447,087            | bytes   |
| sorted/Ip              | plain                         | 1,622,899          | bytes   |
| sorted/Ip              | plain+zstd3                   | 682,455            | bytes   |
| sorted/Ip              | uuid16                        | 1,622,899          | bytes   |
| sorted/Ip              | uuid16+zstd3                  | 682,455            | bytes   |
| sorted/Ip              | varint                        | 1,622,899          | bytes   |
| sorted/Ip              | varint+zstd3                  | 682,455            | bytes   |
| sorted/Null            | bits                          | 6,714              | bytes   |
| sorted/Null            | bits+zstd3                    | 8,857              | bytes   |
| sorted/Null            | const                         | 6,714              | bytes   |
| sorted/Null            | const+zstd3                   | 8,857              | bytes   |
| sorted/Null            | delta_varint                  | 6,714              | bytes   |
| sorted/Null            | delta_varint+zstd3            | 8,857              | bytes   |
| sorted/Null            | dict                          | 6,714              | bytes   |
| sorted/Null            | dict+zstd3                    | 8,857              | bytes   |
| sorted/Null            | ip_bin                        | 6,714              | bytes   |
| sorted/Null            | ip_bin+zstd3                  | 8,857              | bytes   |
| sorted/Null            | plain                         | 6,714              | bytes   |
| sorted/Null            | plain+zstd3                   | 8,857              | bytes   |
| sorted/Null            | uuid16                        | 6,714              | bytes   |
| sorted/Null            | uuid16+zstd3                  | 8,857              | bytes   |
| sorted/Null            | varint                        | 6,714              | bytes   |
| sorted/Null            | varint+zstd3                  | 8,857              | bytes   |
| sorted/String          | bits                          | 14,167,083         | bytes   |
| sorted/String          | bits+zstd3                    | 1,330,359          | bytes   |
| sorted/String          | const                         | 11,599,826         | bytes   |
| sorted/String          | const+zstd3                   | 1,326,674          | bytes   |
| sorted/String          | delta_varint                  | 14,167,083         | bytes   |
| sorted/String          | delta_varint+zstd3            | 1,330,359          | bytes   |
| sorted/String          | dict                          | 4,015,191          | bytes   |
| sorted/String          | dict+zstd3                    | 1,231,340          | bytes   |
| sorted/String          | ip_bin                        | 14,167,083         | bytes   |
| sorted/String          | ip_bin+zstd3                  | 1,330,359          | bytes   |
| sorted/String          | plain                         | 14,167,083         | bytes   |
| sorted/String          | plain+zstd3                   | 1,330,359          | bytes   |
| sorted/String          | uuid16                        | 14,167,083         | bytes   |
| sorted/String          | uuid16+zstd3                  | 1,330,359          | bytes   |
| sorted/String          | varint                        | 14,167,083         | bytes   |
| sorted/String          | varint+zstd3                  | 1,330,359          | bytes   |
| sorted/Uuid            | bits                          | 3,324,600          | bytes   |
| sorted/Uuid            | bits+zstd3                    | 1,741,329          | bytes   |
| sorted/Uuid            | const                         | 3,324,600          | bytes   |
| sorted/Uuid            | const+zstd3                   | 1,741,329          | bytes   |
| sorted/Uuid            | delta_varint                  | 3,324,600          | bytes   |
| sorted/Uuid            | delta_varint+zstd3            | 1,741,329          | bytes   |
| sorted/Uuid            | dict                          | 3,324,659          | bytes   |
| sorted/Uuid            | dict+zstd3                    | 1,741,396          | bytes   |
| sorted/Uuid            | ip_bin                        | 3,324,600          | bytes   |
| sorted/Uuid            | ip_bin+zstd3                  | 1,741,329          | bytes   |
| sorted/Uuid            | plain                         | 3,324,600          | bytes   |
| sorted/Uuid            | plain+zstd3                   | 1,741,329          | bytes   |
| sorted/Uuid            | uuid16                        | 1,329,840          | bytes   |
| sorted/Uuid            | uuid16+zstd3                  | 1,336,256          | bytes   |
| sorted/Uuid            | varint                        | 3,324,600          | bytes   |
| sorted/Uuid            | varint+zstd3                  | 1,741,329          | bytes   |
| coverage/plain         | applies_to_pct_of_plain_bytes | 100                | percent |
| coverage/const         | applies_to_pct_of_plain_bytes | 10.38              | percent |
| coverage/dict          | applies_to_pct_of_plain_bytes | 60.54              | percent |
| coverage/delta_varint  | applies_to_pct_of_plain_bytes | 19.8               | percent |
| coverage/varint        | applies_to_pct_of_plain_bytes | 19.8               | percent |
| coverage/bits          | applies_to_pct_of_plain_bytes | 0.2                | percent |
| coverage/uuid16        | applies_to_pct_of_plain_bytes | 13.35              | percent |
| coverage/ip_bin        | applies_to_pct_of_plain_bytes | 6.52               | percent |
| zstd level 1           | bytes                         | 8,819,429          | bytes   |
| zstd level 1           | compress_MBps                 | 661.5              | MB/s    |
| zstd level 3           | bytes                         | 8,163,020          | bytes   |
| zstd level 3           | compress_MBps                 | 389.1              | MB/s    |
| zstd level 9           | bytes                         | 7,609,358          | bytes   |
| zstd level 9           | compress_MBps                 | 83.11              | MB/s    |
| zstd level 19          | bytes                         | 7,114,711          | bytes   |
| zstd level 19          | compress_MBps                 | 3.55               | MB/s    |
| wordlist per_chunk     | bytes                         | 15,227,549         | bytes   |
| wordlist per_file      | bytes                         | 15,217,492         | bytes   |
| column:event           | plain_bytes                   | 2,583,961          | bytes   |
| column:event           | best_encoding                 | const              | name    |
| column:ext1.label      | plain_bytes                   | 1,699,540          | bytes   |
| column:ext1.label      | best_encoding                 | dict+zstd3         | name    |
| column:request_id      | plain_bytes                   | 1,339,000          | bytes   |
| column:request_id      | best_encoding                 | uuid16             | name    |
| column:customer.id     | plain_bytes                   | 1,320,720          | bytes   |
| column:customer.id     | best_encoding                 | uuid16             | name    |
| column:client.ip       | plain_bytes                   | 1,155,922          | bytes   |
| column:client.ip       | best_encoding                 | ip_bin+zstd3       | name    |
| column:user.email      | plain_bytes                   | 967,137            | bytes   |
| column:user.email      | best_encoding                 | plain+zstd3        | name    |
| column:customer.email  | plain_bytes                   | 953,550            | bytes   |
| column:customer.email  | best_encoding                 | plain+zstd3        | name    |
| column:ts              | plain_bytes                   | 934,000            | bytes   |
| column:ts              | best_encoding                 | delta_varint+zstd3 | name    |
| column:reason          | plain_bytes                   | 892,342            | bytes   |
| column:reason          | best_encoding                 | dict+zstd3         | name    |
| column:level           | plain_bytes                   | 859,553            | bytes   |
| column:level           | best_encoding                 | dict+zstd3         | name    |
| column:event           | clp_split_bytes               | 16,848             | bytes   |
| column:ext1.label      | clp_split_bytes               | 147,787            | bytes   |
| column:user.email      | clp_split_bytes               | 199,543            | bytes   |
| column:customer.email  | clp_split_bytes               | 136,207            | bytes   |
| column:reason          | clp_split_bytes               | 52,895             | bytes   |
| BEST_PER_CHUNK arrival | bytes                         | 6,870,373          | bytes   |
| BEST_PER_CHUNK arrival | ratio_to_raw                  | 0.1475             | ratio   |
| BEST_PER_CHUNK arrival | ratio_to_gzip9                | 0.6344             | ratio   |
| BEST_PER_CHUNK sorted  | bytes                         | 6,696,927          | bytes   |
| BEST_PER_CHUNK sorted  | ratio_to_raw                  | 0.1437             | ratio   |
| BEST_PER_CHUNK sorted  | ratio_to_gzip9                | 0.6184             | ratio   |
| plain                  | bytes                         | 24,901,825         | bytes   |
| verdict                | sort_saving_pct               | 2.525              | percent |

### `05_encodings` / dataset `uneven`

| variant                 | metric                        | value              | unit    |
|-------------------------|-------------------------------|--------------------|---------|
| arrival/Bool            | bits                          | 20,745             | bytes   |
| arrival/Bool            | bits+zstd3                    | 19,673             | bytes   |
| arrival/Bool            | const                         | 163,381            | bytes   |
| arrival/Bool            | const+zstd3                   | 32,361             | bytes   |
| arrival/Bool            | delta_varint                  | 163,390            | bytes   |
| arrival/Bool            | delta_varint+zstd3            | 32,370             | bytes   |
| arrival/Bool            | dict                          | 170,217            | bytes   |
| arrival/Bool            | dict+zstd3                    | 38,922             | bytes   |
| arrival/Bool            | ip_bin                        | 163,390            | bytes   |
| arrival/Bool            | ip_bin+zstd3                  | 32,370             | bytes   |
| arrival/Bool            | plain                         | 163,390            | bytes   |
| arrival/Bool            | plain+zstd3                   | 32,370             | bytes   |
| arrival/Bool            | uuid16                        | 163,390            | bytes   |
| arrival/Bool            | uuid16+zstd3                  | 32,370             | bytes   |
| arrival/Bool            | varint                        | 163,390            | bytes   |
| arrival/Bool            | varint+zstd3                  | 32,370             | bytes   |
| arrival/Datetime        | bits                          | 3,995,136          | bytes   |
| arrival/Datetime        | bits+zstd3                    | 1,256,972          | bytes   |
| arrival/Datetime        | const                         | 3,985,776          | bytes   |
| arrival/Datetime        | const+zstd3                   | 1,256,036          | bytes   |
| arrival/Datetime        | delta_varint                  | 1,528,251          | bytes   |
| arrival/Datetime        | delta_varint+zstd3            | 1,264,473          | bytes   |
| arrival/Datetime        | dict                          | 1,991,927          | bytes   |
| arrival/Datetime        | dict+zstd3                    | 1,394,302          | bytes   |
| arrival/Datetime        | ip_bin                        | 3,995,136          | bytes   |
| arrival/Datetime        | ip_bin+zstd3                  | 1,256,972          | bytes   |
| arrival/Datetime        | plain                         | 3,995,136          | bytes   |
| arrival/Datetime        | plain+zstd3                   | 1,256,972          | bytes   |
| arrival/Datetime        | uuid16                        | 3,995,136          | bytes   |
| arrival/Datetime        | uuid16+zstd3                  | 1,256,972          | bytes   |
| arrival/Datetime        | varint                        | 4,571,189          | bytes   |
| arrival/Datetime        | varint+zstd3                  | 1,378,973          | bytes   |
| arrival/Float64         | bits                          | 148,280            | bytes   |
| arrival/Float64         | bits+zstd3                    | 162,295            | bytes   |
| arrival/Float64         | const                         | 147,540            | bytes   |
| arrival/Float64         | const+zstd3                   | 162,209            | bytes   |
| arrival/Float64         | delta_varint                  | 148,280            | bytes   |
| arrival/Float64         | delta_varint+zstd3            | 162,295            | bytes   |
| arrival/Float64         | dict                          | 148,708            | bytes   |
| arrival/Float64         | dict+zstd3                    | 164,114            | bytes   |
| arrival/Float64         | ip_bin                        | 148,280            | bytes   |
| arrival/Float64         | ip_bin+zstd3                  | 162,295            | bytes   |
| arrival/Float64         | plain                         | 148,280            | bytes   |
| arrival/Float64         | plain+zstd3                   | 162,295            | bytes   |
| arrival/Float64         | uuid16                        | 148,280            | bytes   |
| arrival/Float64         | uuid16+zstd3                  | 162,295            | bytes   |
| arrival/Float64         | varint                        | 148,280            | bytes   |
| arrival/Float64         | varint+zstd3                  | 162,295            | bytes   |
| arrival/Int64           | bits                          | 2,952,104          | bytes   |
| arrival/Int64           | bits+zstd3                    | 475,467            | bytes   |
| arrival/Int64           | const                         | 2,948,524          | bytes   |
| arrival/Int64           | const+zstd3                   | 475,631            | bytes   |
| arrival/Int64           | delta_varint                  | 554,600            | bytes   |
| arrival/Int64           | delta_varint+zstd3            | 375,590            | bytes   |
| arrival/Int64           | dict                          | 1,330,883          | bytes   |
| arrival/Int64           | dict+zstd3                    | 584,086            | bytes   |
| arrival/Int64           | ip_bin                        | 2,952,104          | bytes   |
| arrival/Int64           | ip_bin+zstd3                  | 475,467            | bytes   |
| arrival/Int64           | plain                         | 2,952,104          | bytes   |
| arrival/Int64           | plain+zstd3                   | 475,467            | bytes   |
| arrival/Int64           | uuid16                        | 2,952,104          | bytes   |
| arrival/Int64           | uuid16+zstd3                  | 475,467            | bytes   |
| arrival/Int64           | varint                        | 1,400,951          | bytes   |
| arrival/Int64           | varint+zstd3                  | 364,155            | bytes   |
| arrival/Ip              | bits                          | 568,510            | bytes   |
| arrival/Ip              | bits+zstd3                    | 392,448            | bytes   |
| arrival/Ip              | const                         | 555,067            | bytes   |
| arrival/Ip              | const+zstd3                   | 391,819            | bytes   |
| arrival/Ip              | delta_varint                  | 568,510            | bytes   |
| arrival/Ip              | delta_varint+zstd3            | 392,448            | bytes   |
| arrival/Ip              | dict                          | 546,190            | bytes   |
| arrival/Ip              | dict+zstd3                    | 395,986            | bytes   |
| arrival/Ip              | ip_bin                        | 243,571            | bytes   |
| arrival/Ip              | ip_bin+zstd3                  | 230,835            | bytes   |
| arrival/Ip              | plain                         | 568,510            | bytes   |
| arrival/Ip              | plain+zstd3                   | 392,448            | bytes   |
| arrival/Ip              | uuid16                        | 568,510            | bytes   |
| arrival/Ip              | uuid16+zstd3                  | 392,448            | bytes   |
| arrival/Ip              | varint                        | 568,510            | bytes   |
| arrival/Ip              | varint+zstd3                  | 392,448            | bytes   |
| arrival/Null            | bits                          | 3,220              | bytes   |
| arrival/Null            | bits+zstd3                    | 30,373             | bytes   |
| arrival/Null            | const                         | 3,220              | bytes   |
| arrival/Null            | const+zstd3                   | 30,373             | bytes   |
| arrival/Null            | delta_varint                  | 3,220              | bytes   |
| arrival/Null            | delta_varint+zstd3            | 30,373             | bytes   |
| arrival/Null            | dict                          | 3,220              | bytes   |
| arrival/Null            | dict+zstd3                    | 30,373             | bytes   |
| arrival/Null            | ip_bin                        | 3,220              | bytes   |
| arrival/Null            | ip_bin+zstd3                  | 30,373             | bytes   |
| arrival/Null            | plain                         | 3,220              | bytes   |
| arrival/Null            | plain+zstd3                   | 30,373             | bytes   |
| arrival/Null            | uuid16                        | 3,220              | bytes   |
| arrival/Null            | uuid16+zstd3                  | 30,373             | bytes   |
| arrival/Null            | varint                        | 3,220              | bytes   |
| arrival/Null            | varint+zstd3                  | 30,373             | bytes   |
| arrival/String          | bits                          | 11,635,444         | bytes   |
| arrival/String          | bits+zstd3                    | 2,584,306          | bytes   |
| arrival/String          | const                         | 11,131,140         | bytes   |
| arrival/String          | const+zstd3                   | 2,568,564          | bytes   |
| arrival/String          | delta_varint                  | 11,635,444         | bytes   |
| arrival/String          | delta_varint+zstd3            | 2,584,306          | bytes   |
| arrival/String          | dict                          | 3,123,115          | bytes   |
| arrival/String          | dict+zstd3                    | 2,524,106          | bytes   |
| arrival/String          | ip_bin                        | 11,635,444         | bytes   |
| arrival/String          | ip_bin+zstd3                  | 2,584,306          | bytes   |
| arrival/String          | plain                         | 11,635,444         | bytes   |
| arrival/String          | plain+zstd3                   | 2,584,306          | bytes   |
| arrival/String          | uuid16                        | 11,635,444         | bytes   |
| arrival/String          | uuid16+zstd3                  | 2,584,306          | bytes   |
| arrival/String          | varint                        | 11,635,444         | bytes   |
| arrival/String          | varint+zstd3                  | 2,584,306          | bytes   |
| arrival/Uuid            | bits                          | 1,116,120          | bytes   |
| arrival/Uuid            | bits+zstd3                    | 572,545            | bytes   |
| arrival/Uuid            | const                         | 1,103,244          | bytes   |
| arrival/Uuid            | const+zstd3                   | 572,103            | bytes   |
| arrival/Uuid            | delta_varint                  | 1,116,120          | bytes   |
| arrival/Uuid            | delta_varint+zstd3            | 572,545            | bytes   |
| arrival/Uuid            | dict                          | 903,739            | bytes   |
| arrival/Uuid            | dict+zstd3                    | 581,296            | bytes   |
| arrival/Uuid            | ip_bin                        | 1,116,120          | bytes   |
| arrival/Uuid            | ip_bin+zstd3                  | 572,545            | bytes   |
| arrival/Uuid            | plain                         | 1,116,120          | bytes   |
| arrival/Uuid            | plain+zstd3                   | 572,545            | bytes   |
| arrival/Uuid            | uuid16                        | 446,448            | bytes   |
| arrival/Uuid            | uuid16+zstd3                  | 387,331            | bytes   |
| arrival/Uuid            | varint                        | 1,116,120          | bytes   |
| arrival/Uuid            | varint+zstd3                  | 572,545            | bytes   |
| sorted/Bool             | bits                          | 20,745             | bytes   |
| sorted/Bool             | bits+zstd3                    | 10,659             | bytes   |
| sorted/Bool             | const                         | 163,381            | bytes   |
| sorted/Bool             | const+zstd3                   | 17,365             | bytes   |
| sorted/Bool             | delta_varint                  | 163,390            | bytes   |
| sorted/Bool             | delta_varint+zstd3            | 17,374             | bytes   |
| sorted/Bool             | dict                          | 170,217            | bytes   |
| sorted/Bool             | dict+zstd3                    | 24,067             | bytes   |
| sorted/Bool             | ip_bin                        | 163,390            | bytes   |
| sorted/Bool             | ip_bin+zstd3                  | 17,374             | bytes   |
| sorted/Bool             | plain                         | 163,390            | bytes   |
| sorted/Bool             | plain+zstd3                   | 17,374             | bytes   |
| sorted/Bool             | uuid16                        | 163,390            | bytes   |
| sorted/Bool             | uuid16+zstd3                  | 17,374             | bytes   |
| sorted/Bool             | varint                        | 163,390            | bytes   |
| sorted/Bool             | varint+zstd3                  | 17,374             | bytes   |
| sorted/Datetime         | bits                          | 3,995,136          | bytes   |
| sorted/Datetime         | bits+zstd3                    | 1,275,768          | bytes   |
| sorted/Datetime         | const                         | 3,985,776          | bytes   |
| sorted/Datetime         | const+zstd3                   | 1,274,832          | bytes   |
| sorted/Datetime         | delta_varint                  | 1,542,598          | bytes   |
| sorted/Datetime         | delta_varint+zstd3            | 1,352,112          | bytes   |
| sorted/Datetime         | dict                          | 1,991,927          | bytes   |
| sorted/Datetime         | dict+zstd3                    | 1,452,097          | bytes   |
| sorted/Datetime         | ip_bin                        | 3,995,136          | bytes   |
| sorted/Datetime         | ip_bin+zstd3                  | 1,275,768          | bytes   |
| sorted/Datetime         | plain                         | 3,995,136          | bytes   |
| sorted/Datetime         | plain+zstd3                   | 1,275,768          | bytes   |
| sorted/Datetime         | uuid16                        | 3,995,136          | bytes   |
| sorted/Datetime         | uuid16+zstd3                  | 1,275,768          | bytes   |
| sorted/Datetime         | varint                        | 4,571,189          | bytes   |
| sorted/Datetime         | varint+zstd3                  | 1,402,214          | bytes   |
| sorted/Float64          | bits                          | 148,280            | bytes   |
| sorted/Float64          | bits+zstd3                    | 162,208            | bytes   |
| sorted/Float64          | const                         | 147,540            | bytes   |
| sorted/Float64          | const+zstd3                   | 162,122            | bytes   |
| sorted/Float64          | delta_varint                  | 148,280            | bytes   |
| sorted/Float64          | delta_varint+zstd3            | 162,208            | bytes   |
| sorted/Float64          | dict                          | 148,708            | bytes   |
| sorted/Float64          | dict+zstd3                    | 164,047            | bytes   |
| sorted/Float64          | ip_bin                        | 148,280            | bytes   |
| sorted/Float64          | ip_bin+zstd3                  | 162,208            | bytes   |
| sorted/Float64          | plain                         | 148,280            | bytes   |
| sorted/Float64          | plain+zstd3                   | 162,208            | bytes   |
| sorted/Float64          | uuid16                        | 148,280            | bytes   |
| sorted/Float64          | uuid16+zstd3                  | 162,208            | bytes   |
| sorted/Float64          | varint                        | 148,280            | bytes   |
| sorted/Float64          | varint+zstd3                  | 162,208            | bytes   |
| sorted/Int64            | bits                          | 2,952,104          | bytes   |
| sorted/Int64            | bits+zstd3                    | 466,635            | bytes   |
| sorted/Int64            | const                         | 2,948,524          | bytes   |
| sorted/Int64            | const+zstd3                   | 466,799            | bytes   |
| sorted/Int64            | delta_varint                  | 543,346            | bytes   |
| sorted/Int64            | delta_varint+zstd3            | 369,374            | bytes   |
| sorted/Int64            | dict                          | 1,330,883          | bytes   |
| sorted/Int64            | dict+zstd3                    | 566,272            | bytes   |
| sorted/Int64            | ip_bin                        | 2,952,104          | bytes   |
| sorted/Int64            | ip_bin+zstd3                  | 466,635            | bytes   |
| sorted/Int64            | plain                         | 2,952,104          | bytes   |
| sorted/Int64            | plain+zstd3                   | 466,635            | bytes   |
| sorted/Int64            | uuid16                        | 2,952,104          | bytes   |
| sorted/Int64            | uuid16+zstd3                  | 466,635            | bytes   |
| sorted/Int64            | varint                        | 1,400,951          | bytes   |
| sorted/Int64            | varint+zstd3                  | 357,464            | bytes   |
| sorted/Ip               | bits                          | 568,510            | bytes   |
| sorted/Ip               | bits+zstd3                    | 392,571            | bytes   |
| sorted/Ip               | const                         | 555,067            | bytes   |
| sorted/Ip               | const+zstd3                   | 391,942            | bytes   |
| sorted/Ip               | delta_varint                  | 568,510            | bytes   |
| sorted/Ip               | delta_varint+zstd3            | 392,571            | bytes   |
| sorted/Ip               | dict                          | 546,190            | bytes   |
| sorted/Ip               | dict+zstd3                    | 396,118            | bytes   |
| sorted/Ip               | ip_bin                        | 243,571            | bytes   |
| sorted/Ip               | ip_bin+zstd3                  | 230,807            | bytes   |
| sorted/Ip               | plain                         | 568,510            | bytes   |
| sorted/Ip               | plain+zstd3                   | 392,571            | bytes   |
| sorted/Ip               | uuid16                        | 568,510            | bytes   |
| sorted/Ip               | uuid16+zstd3                  | 392,571            | bytes   |
| sorted/Ip               | varint                        | 568,510            | bytes   |
| sorted/Ip               | varint+zstd3                  | 392,571            | bytes   |
| sorted/Null             | bits                          | 3,220              | bytes   |
| sorted/Null             | bits+zstd3                    | 30,373             | bytes   |
| sorted/Null             | const                         | 3,220              | bytes   |
| sorted/Null             | const+zstd3                   | 30,373             | bytes   |
| sorted/Null             | delta_varint                  | 3,220              | bytes   |
| sorted/Null             | delta_varint+zstd3            | 30,373             | bytes   |
| sorted/Null             | dict                          | 3,220              | bytes   |
| sorted/Null             | dict+zstd3                    | 30,373             | bytes   |
| sorted/Null             | ip_bin                        | 3,220              | bytes   |
| sorted/Null             | ip_bin+zstd3                  | 30,373             | bytes   |
| sorted/Null             | plain                         | 3,220              | bytes   |
| sorted/Null             | plain+zstd3                   | 30,373             | bytes   |
| sorted/Null             | uuid16                        | 3,220              | bytes   |
| sorted/Null             | uuid16+zstd3                  | 30,373             | bytes   |
| sorted/Null             | varint                        | 3,220              | bytes   |
| sorted/Null             | varint+zstd3                  | 30,373             | bytes   |
| sorted/String           | bits                          | 11,635,444         | bytes   |
| sorted/String           | bits+zstd3                    | 2,398,961          | bytes   |
| sorted/String           | const                         | 11,131,140         | bytes   |
| sorted/String           | const+zstd3                   | 2,383,219          | bytes   |
| sorted/String           | delta_varint                  | 11,635,444         | bytes   |
| sorted/String           | delta_varint+zstd3            | 2,398,961          | bytes   |
| sorted/String           | dict                          | 3,123,115          | bytes   |
| sorted/String           | dict+zstd3                    | 2,455,543          | bytes   |
| sorted/String           | ip_bin                        | 11,635,444         | bytes   |
| sorted/String           | ip_bin+zstd3                  | 2,398,961          | bytes   |
| sorted/String           | plain                         | 11,635,444         | bytes   |
| sorted/String           | plain+zstd3                   | 2,398,961          | bytes   |
| sorted/String           | uuid16                        | 11,635,444         | bytes   |
| sorted/String           | uuid16+zstd3                  | 2,398,961          | bytes   |
| sorted/String           | varint                        | 11,635,444         | bytes   |
| sorted/String           | varint+zstd3                  | 2,398,961          | bytes   |
| sorted/Uuid             | bits                          | 1,116,120          | bytes   |
| sorted/Uuid             | bits+zstd3                    | 572,261            | bytes   |
| sorted/Uuid             | const                         | 1,103,244          | bytes   |
| sorted/Uuid             | const+zstd3                   | 571,819            | bytes   |
| sorted/Uuid             | delta_varint                  | 1,116,120          | bytes   |
| sorted/Uuid             | delta_varint+zstd3            | 572,261            | bytes   |
| sorted/Uuid             | dict                          | 903,739            | bytes   |
| sorted/Uuid             | dict+zstd3                    | 581,041            | bytes   |
| sorted/Uuid             | ip_bin                        | 1,116,120          | bytes   |
| sorted/Uuid             | ip_bin+zstd3                  | 572,261            | bytes   |
| sorted/Uuid             | plain                         | 1,116,120          | bytes   |
| sorted/Uuid             | plain+zstd3                   | 572,261            | bytes   |
| sorted/Uuid             | uuid16                        | 446,448            | bytes   |
| sorted/Uuid             | uuid16+zstd3                  | 387,212            | bytes   |
| sorted/Uuid             | varint                        | 1,116,120          | bytes   |
| sorted/Uuid             | varint+zstd3                  | 572,261            | bytes   |
| coverage/plain          | applies_to_pct_of_plain_bytes | 100                | percent |
| coverage/const          | applies_to_pct_of_plain_bytes | 3.3                | percent |
| coverage/dict           | applies_to_pct_of_plain_bytes | 85.28              | percent |
| coverage/delta_varint   | applies_to_pct_of_plain_bytes | 33.75              | percent |
| coverage/varint         | applies_to_pct_of_plain_bytes | 33.75              | percent |
| coverage/bits           | applies_to_pct_of_plain_bytes | 0.79               | percent |
| coverage/uuid16         | applies_to_pct_of_plain_bytes | 5.42               | percent |
| coverage/ip_bin         | applies_to_pct_of_plain_bytes | 2.76               | percent |
| zstd level 1            | bytes                         | 3,253,334          | bytes   |
| zstd level 1            | compress_MBps                 | 1,014              | MB/s    |
| zstd level 3            | bytes                         | 3,136,257          | bytes   |
| zstd level 3            | compress_MBps                 | 729.5              | MB/s    |
| zstd level 9            | bytes                         | 2,814,740          | bytes   |
| zstd level 9            | compress_MBps                 | 127.9              | MB/s    |
| zstd level 19           | bytes                         | 2,586,595          | bytes   |
| zstd level 19           | compress_MBps                 | 6.19               | MB/s    |
| wordlist per_chunk      | bytes                         | 8,217,999          | bytes   |
| wordlist per_file       | bytes                         | 8,234,095          | bytes   |
| column:region           | plain_bytes                   | 2,270,200          | bytes   |
| column:region           | best_encoding                 | dict+zstd3         | name    |
| column:service          | plain_bytes                   | 1,687,124          | bytes   |
| column:service          | best_encoding                 | dict+zstd3         | name    |
| column:level            | plain_bytes                   | 1,575,795          | bytes   |
| column:level            | best_encoding                 | dict+zstd3         | name    |
| column:ts               | plain_bytes                   | 1,457,160          | bytes   |
| column:ts               | best_encoding                 | delta_varint+zstd3 | name    |
| column:shard.id         | plain_bytes                   | 1,239,800          | bytes   |
| column:shard.id         | best_encoding                 | plain+zstd3        | name    |
| column:shard.rows       | plain_bytes                   | 1,239,800          | bytes   |
| column:shard.rows       | best_encoding                 | plain+zstd3        | name    |
| column:shard.checkpoint | plain_bytes                   | 1,185,016          | bytes   |
| column:shard.checkpoint | best_encoding                 | varint+zstd3       | name    |
| column:ext3.label       | plain_bytes                   | 837,887            | bytes   |
| column:ext3.label       | best_encoding                 | dict+zstd3         | name    |
| column:ext4.label       | plain_bytes                   | 608,647            | bytes   |
| column:ext4.label       | best_encoding                 | dict+zstd3         | name    |
| column:request_id       | plain_bytes                   | 576,640            | bytes   |
| column:request_id       | best_encoding                 | uuid16             | name    |
| column:region           | clp_split_bytes               | 22,114             | bytes   |
| column:service          | clp_split_bytes               | 54,370             | bytes   |
| column:level            | clp_split_bytes               | 155,130            | bytes   |
| column:ext3.label       | clp_split_bytes               | 89,133             | bytes   |
| column:ext4.label       | clp_split_bytes               | 89,358             | bytes   |
| BEST_PER_CHUNK arrival  | bytes                         | 4,303,865          | bytes   |
| BEST_PER_CHUNK arrival  | ratio_to_raw                  | 0.09615            | ratio   |
| BEST_PER_CHUNK arrival  | ratio_to_gzip9                | 1.059              | ratio   |
| BEST_PER_CHUNK sorted   | bytes                         | 4,274,800          | bytes   |
| BEST_PER_CHUNK sorted   | ratio_to_raw                  | 0.09551            | ratio   |
| BEST_PER_CHUNK sorted   | ratio_to_gzip9                | 1.052              | ratio   |
| plain                   | bytes                         | 20,582,204         | bytes   |
| verdict                 | sort_saving_pct               | 0.675              | percent |

**Notes**

- chunks of 8,192 rows, one column type
- chunks where the encoder declines are charged plain in the tables above
- one zstd call over every plain chunk concatenated
- median of 3 runs; zstd is a C library and this is its speed, not TrieDB's
- a per-file word list stores each distinct value once for the whole column; a per-chunk one repeats it, but indexes into a smaller list
- type String
- 16,848 bytes, 0.0065 of plain
- 149,477 bytes, 0.0880 of plain
- type Uuid
- 535,600 bytes, 0.4000 of plain
- 528,288 bytes, 0.4000 of plain
- type Ip
- 321,990 bytes, 0.2786 of plain
- 227,805 bytes, 0.2355 of plain
- 171,263 bytes, 0.1796 of plain
- type Datetime
- 825,456 bytes, 0.8838 of plain
- 52,942 bytes, 0.0593 of plain
- 80,096 bytes, 0.0932 of plain
- fixed part through a word list, each changing part its own column; 1.0000 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9887 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.8759 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.7953 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9991 of the best encoding above
- the smallest applicable encoding for every chunk of every column
- section 6.2 chunk sizes, no codec
- BEST_PER_CHUNK sorted against BEST_PER_CHUNK in arrival order
- 22,114 bytes, 0.0097 of plain
- 56,831 bytes, 0.0337 of plain
- 186,422 bytes, 0.1183 of plain
- 291,919 bytes, 0.2003 of plain
- type Int64
- 59,219 bytes, 0.0478 of plain
- 121,319 bytes, 0.0979 of plain
- 124,779 bytes, 0.1053 of plain
- 95,423 bytes, 0.1139 of plain
- 95,712 bytes, 0.1573 of plain
- 230,656 bytes, 0.4000 of plain
- fixed part through a word list, each changing part its own column; 0.9567 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.8321 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9341 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9336 of the best encoding above

## 06_bloom

_Is a bloom filter per row group worth its bytes_

Run 2026-09-18T22:10:15+00:00, git `b405451`, 428 rows.

### `06_bloom` / dataset `flat30`

| variant              | metric                       |       value | unit    |
|----------------------|------------------------------|-------------|---------|
| ts@8bits             | row_groups                   |    723      | count   |
| ts@8bits             | filter_bytes                 | 116750      | bytes   |
| ts@8bits             | k                            |      6      | count   |
| ts@8bits             | misses_generated             |     18      | count   |
| ts@8bits             | misses_rejected_out_of_range | 199982      | count   |
| ts@8bits             | present_bloom_skipped_pct    |     97.35   | percent |
| ts@8bits             | present_minmax_skipped_pct   |      5.78   | percent |
| ts@8bits             | present_ideal_skipped_pct    |     99.86   | percent |
| ts@8bits             | absent_bloom_skipped_pct     |     97.56   | percent |
| ts@8bits             | absent_minmax_skipped_pct    |      0.7223 | percent |
| ts@8bits             | absent_ideal_skipped_pct     |    100      | percent |
| ts@8bits             | share_of_best_total_pct      |      1.699  | percent |
| ts@10bits            | row_groups                   |    723      | count   |
| ts@10bits            | filter_bytes                 | 146212      | bytes   |
| ts@10bits            | k                            |      7      | count   |
| ts@10bits            | misses_generated             |     18      | count   |
| ts@10bits            | misses_rejected_out_of_range | 199982      | count   |
| ts@10bits            | present_bloom_skipped_pct    |     98.66   | percent |
| ts@10bits            | present_minmax_skipped_pct   |      5.78   | percent |
| ts@10bits            | present_ideal_skipped_pct    |     99.86   | percent |
| ts@10bits            | absent_bloom_skipped_pct     |     98.82   | percent |
| ts@10bits            | absent_minmax_skipped_pct    |      0.7223 | percent |
| ts@10bits            | absent_ideal_skipped_pct     |    100      | percent |
| ts@10bits            | share_of_best_total_pct      |      2.128  | percent |
| ts@16bits            | row_groups                   |    723      | count   |
| ts@16bits            | filter_bytes                 | 233500      | bytes   |
| ts@16bits            | k                            |     11      | count   |
| ts@16bits            | misses_generated             |     18      | count   |
| ts@16bits            | misses_rejected_out_of_range | 199982      | count   |
| ts@16bits            | present_bloom_skipped_pct    |     99.67   | percent |
| ts@16bits            | present_minmax_skipped_pct   |      5.78   | percent |
| ts@16bits            | present_ideal_skipped_pct    |     99.86   | percent |
| ts@16bits            | absent_bloom_skipped_pct     |     99.78   | percent |
| ts@16bits            | absent_minmax_skipped_pct    |      0.7223 | percent |
| ts@16bits            | absent_ideal_skipped_pct     |    100      | percent |
| ts@16bits            | share_of_best_total_pct      |      3.399  | percent |
| request_id@8bits     | row_groups                   |    576      | count   |
| request_id@8bits     | filter_bytes                 |  33475      | bytes   |
| request_id@8bits     | k                            |      6      | count   |
| request_id@8bits     | misses_generated             |   1000      | count   |
| request_id@8bits     | misses_rejected_out_of_range |      0      | count   |
| request_id@8bits     | present_bloom_skipped_pct    |     97.18   | percent |
| request_id@8bits     | present_minmax_skipped_pct   |      7.602  | percent |
| request_id@8bits     | present_ideal_skipped_pct    |     99.83   | percent |
| request_id@8bits     | absent_bloom_skipped_pct     |     97.39   | percent |
| request_id@8bits     | absent_minmax_skipped_pct    |      7.643  | percent |
| request_id@8bits     | absent_ideal_skipped_pct     |    100      | percent |
| request_id@8bits     | share_of_best_total_pct      |      0.4872 | percent |
| request_id@10bits    | row_groups                   |    576      | count   |
| request_id@10bits    | filter_bytes                 |  42063      | bytes   |
| request_id@10bits    | k                            |      7      | count   |
| request_id@10bits    | misses_generated             |   1000      | count   |
| request_id@10bits    | misses_rejected_out_of_range |      0      | count   |
| request_id@10bits    | present_bloom_skipped_pct    |     98.53   | percent |
| request_id@10bits    | present_minmax_skipped_pct   |      7.602  | percent |
| request_id@10bits    | present_ideal_skipped_pct    |     99.83   | percent |
| request_id@10bits    | absent_bloom_skipped_pct     |     98.69   | percent |
| request_id@10bits    | absent_minmax_skipped_pct    |      7.643  | percent |
| request_id@10bits    | absent_ideal_skipped_pct     |    100      | percent |
| request_id@10bits    | share_of_best_total_pct      |      0.6122 | percent |
| request_id@16bits    | row_groups                   |    576      | count   |
| request_id@16bits    | filter_bytes                 |  66950      | bytes   |
| request_id@16bits    | k                            |     11      | count   |
| request_id@16bits    | misses_generated             |   1000      | count   |
| request_id@16bits    | misses_rejected_out_of_range |      0      | count   |
| request_id@16bits    | present_bloom_skipped_pct    |     99.59   | percent |
| request_id@16bits    | present_minmax_skipped_pct   |      7.602  | percent |
| request_id@16bits    | present_ideal_skipped_pct    |     99.83   | percent |
| request_id@16bits    | absent_bloom_skipped_pct     |     99.77   | percent |
| request_id@16bits    | absent_minmax_skipped_pct    |      7.643  | percent |
| request_id@16bits    | absent_ideal_skipped_pct     |    100      | percent |
| request_id@16bits    | share_of_best_total_pct      |      0.9745 | percent |
| customer.id@8bits    | row_groups                   |     72      | count   |
| customer.id@8bits    | filter_bytes                 |  33018      | bytes   |
| customer.id@8bits    | k                            |      6      | count   |
| customer.id@8bits    | misses_generated             |   1000      | count   |
| customer.id@8bits    | misses_rejected_out_of_range |      0      | count   |
| customer.id@8bits    | present_bloom_skipped_pct    |     96.58   | percent |
| customer.id@8bits    | present_minmax_skipped_pct   |      0.8667 | percent |
| customer.id@8bits    | present_ideal_skipped_pct    |     98.61   | percent |
| customer.id@8bits    | absent_bloom_skipped_pct     |     97.78   | percent |
| customer.id@8bits    | absent_minmax_skipped_pct    |      0.7653 | percent |
| customer.id@8bits    | absent_ideal_skipped_pct     |    100      | percent |
| customer.id@8bits    | share_of_best_total_pct      |      0.4806 | percent |
| customer.id@10bits   | row_groups                   |     72      | count   |
| customer.id@10bits   | filter_bytes                 |  41298      | bytes   |
| customer.id@10bits   | k                            |      7      | count   |
| customer.id@10bits   | misses_generated             |   1000      | count   |
| customer.id@10bits   | misses_rejected_out_of_range |      0      | count   |
| customer.id@10bits   | present_bloom_skipped_pct    |     97.84   | percent |
| customer.id@10bits   | present_minmax_skipped_pct   |      0.8667 | percent |
| customer.id@10bits   | present_ideal_skipped_pct    |     98.61   | percent |
| customer.id@10bits   | absent_bloom_skipped_pct     |     99.14   | percent |
| customer.id@10bits   | absent_minmax_skipped_pct    |      0.7653 | percent |
| customer.id@10bits   | absent_ideal_skipped_pct     |    100      | percent |
| customer.id@10bits   | share_of_best_total_pct      |      0.6011 | percent |
| customer.id@16bits   | row_groups                   |     72      | count   |
| customer.id@16bits   | filter_bytes                 |  66036      | bytes   |
| customer.id@16bits   | k                            |     11      | count   |
| customer.id@16bits   | misses_generated             |   1000      | count   |
| customer.id@16bits   | misses_rejected_out_of_range |      0      | count   |
| customer.id@16bits   | present_bloom_skipped_pct    |     98.55   | percent |
| customer.id@16bits   | present_minmax_skipped_pct   |      0.8667 | percent |
| customer.id@16bits   | present_ideal_skipped_pct    |     98.61   | percent |
| customer.id@16bits   | absent_bloom_skipped_pct     |     99.94   | percent |
| customer.id@16bits   | absent_minmax_skipped_pct    |      0.7653 | percent |
| customer.id@16bits   | absent_ideal_skipped_pct     |    100      | percent |
| customer.id@16bits   | share_of_best_total_pct      |      0.9612 | percent |
| client.ip@8bits      | row_groups                   |    648      | count   |
| client.ip@8bits      | filter_bytes                 |  67085      | bytes   |
| client.ip@8bits      | k                            |      6      | count   |
| client.ip@8bits      | misses_generated             |   1000      | count   |
| client.ip@8bits      | misses_rejected_out_of_range |      1      | count   |
| client.ip@8bits      | present_bloom_skipped_pct    |     97.26   | percent |
| client.ip@8bits      | present_minmax_skipped_pct   |      6.747  | percent |
| client.ip@8bits      | present_ideal_skipped_pct    |     99.85   | percent |
| client.ip@8bits      | absent_bloom_skipped_pct     |     97.4    | percent |
| client.ip@8bits      | absent_minmax_skipped_pct    |      6.509  | percent |
| client.ip@8bits      | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@8bits      | share_of_best_total_pct      |      0.9764 | percent |
| client.ip@10bits     | row_groups                   |    648      | count   |
| client.ip@10bits     | filter_bytes                 |  84104      | bytes   |
| client.ip@10bits     | k                            |      7      | count   |
| client.ip@10bits     | misses_generated             |   1000      | count   |
| client.ip@10bits     | misses_rejected_out_of_range |      1      | count   |
| client.ip@10bits     | present_bloom_skipped_pct    |     98.57   | percent |
| client.ip@10bits     | present_minmax_skipped_pct   |      6.747  | percent |
| client.ip@10bits     | present_ideal_skipped_pct    |     99.85   | percent |
| client.ip@10bits     | absent_bloom_skipped_pct     |     98.71   | percent |
| client.ip@10bits     | absent_minmax_skipped_pct    |      6.509  | percent |
| client.ip@10bits     | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@10bits     | share_of_best_total_pct      |      1.224  | percent |
| client.ip@16bits     | row_groups                   |    648      | count   |
| client.ip@16bits     | filter_bytes                 | 134170      | bytes   |
| client.ip@16bits     | k                            |     11      | count   |
| client.ip@16bits     | misses_generated             |   1000      | count   |
| client.ip@16bits     | misses_rejected_out_of_range |      1      | count   |
| client.ip@16bits     | present_bloom_skipped_pct    |     99.63   | percent |
| client.ip@16bits     | present_minmax_skipped_pct   |      6.747  | percent |
| client.ip@16bits     | present_ideal_skipped_pct    |     99.85   | percent |
| client.ip@16bits     | absent_bloom_skipped_pct     |     99.78   | percent |
| client.ip@16bits     | absent_minmax_skipped_pct    |      6.509  | percent |
| client.ip@16bits     | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@16bits     | share_of_best_total_pct      |      1.953  | percent |
| user.email@8bits     | row_groups                   |    576      | count   |
| user.email@8bits     | filter_bytes                 |  33475      | bytes   |
| user.email@8bits     | k                            |      6      | count   |
| user.email@8bits     | misses_generated             |   1000      | count   |
| user.email@8bits     | misses_rejected_out_of_range |      0      | count   |
| user.email@8bits     | present_bloom_skipped_pct    |     97.1    | percent |
| user.email@8bits     | present_minmax_skipped_pct   |      7.062  | percent |
| user.email@8bits     | present_ideal_skipped_pct    |     99.82   | percent |
| user.email@8bits     | absent_bloom_skipped_pct     |     97.3    | percent |
| user.email@8bits     | absent_minmax_skipped_pct    |      7.162  | percent |
| user.email@8bits     | absent_ideal_skipped_pct     |    100      | percent |
| user.email@8bits     | share_of_best_total_pct      |      0.4872 | percent |
| user.email@10bits    | row_groups                   |    576      | count   |
| user.email@10bits    | filter_bytes                 |  42063      | bytes   |
| user.email@10bits    | k                            |      7      | count   |
| user.email@10bits    | misses_generated             |   1000      | count   |
| user.email@10bits    | misses_rejected_out_of_range |      0      | count   |
| user.email@10bits    | present_bloom_skipped_pct    |     98.51   | percent |
| user.email@10bits    | present_minmax_skipped_pct   |      7.062  | percent |
| user.email@10bits    | present_ideal_skipped_pct    |     99.82   | percent |
| user.email@10bits    | absent_bloom_skipped_pct     |     98.69   | percent |
| user.email@10bits    | absent_minmax_skipped_pct    |      7.162  | percent |
| user.email@10bits    | absent_ideal_skipped_pct     |    100      | percent |
| user.email@10bits    | share_of_best_total_pct      |      0.6122 | percent |
| user.email@16bits    | row_groups                   |    576      | count   |
| user.email@16bits    | filter_bytes                 |  66950      | bytes   |
| user.email@16bits    | k                            |     11      | count   |
| user.email@16bits    | misses_generated             |   1000      | count   |
| user.email@16bits    | misses_rejected_out_of_range |      0      | count   |
| user.email@16bits    | present_bloom_skipped_pct    |     99.6    | percent |
| user.email@16bits    | present_minmax_skipped_pct   |      7.062  | percent |
| user.email@16bits    | present_ideal_skipped_pct    |     99.82   | percent |
| user.email@16bits    | absent_bloom_skipped_pct     |     99.77   | percent |
| user.email@16bits    | absent_minmax_skipped_pct    |      7.162  | percent |
| user.email@16bits    | absent_ideal_skipped_pct     |    100      | percent |
| user.email@16bits    | share_of_best_total_pct      |      0.9745 | percent |
| ts_unix@8bits        | row_groups                   |    652      | count   |
| ts_unix@8bits        | filter_bytes                 |  83115      | bytes   |
| ts_unix@8bits        | k                            |      6      | count   |
| ts_unix@8bits        | misses_generated             |   1000      | count   |
| ts_unix@8bits        | misses_rejected_out_of_range |  39415      | count   |
| ts_unix@8bits        | present_bloom_skipped_pct    |     97.22   | percent |
| ts_unix@8bits        | present_minmax_skipped_pct   |      5.964  | percent |
| ts_unix@8bits        | present_ideal_skipped_pct    |     99.84   | percent |
| ts_unix@8bits        | absent_bloom_skipped_pct     |     97.39   | percent |
| ts_unix@8bits        | absent_minmax_skipped_pct    |      6.38   | percent |
| ts_unix@8bits        | absent_ideal_skipped_pct     |    100      | percent |
| ts_unix@8bits        | share_of_best_total_pct      |      1.21   | percent |
| ts_unix@10bits       | row_groups                   |    652      | count   |
| ts_unix@10bits       | filter_bytes                 | 104140      | bytes   |
| ts_unix@10bits       | k                            |      7      | count   |
| ts_unix@10bits       | misses_generated             |   1000      | count   |
| ts_unix@10bits       | misses_rejected_out_of_range |  39415      | count   |
| ts_unix@10bits       | present_bloom_skipped_pct    |     98.58   | percent |
| ts_unix@10bits       | present_minmax_skipped_pct   |      5.964  | percent |
| ts_unix@10bits       | present_ideal_skipped_pct    |     99.84   | percent |
| ts_unix@10bits       | absent_bloom_skipped_pct     |     98.74   | percent |
| ts_unix@10bits       | absent_minmax_skipped_pct    |      6.38   | percent |
| ts_unix@10bits       | absent_ideal_skipped_pct     |    100      | percent |
| ts_unix@10bits       | share_of_best_total_pct      |      1.516  | percent |
| ts_unix@16bits       | row_groups                   |    652      | count   |
| ts_unix@16bits       | filter_bytes                 | 166230      | bytes   |
| ts_unix@16bits       | k                            |     11      | count   |
| ts_unix@16bits       | misses_generated             |   1000      | count   |
| ts_unix@16bits       | misses_rejected_out_of_range |  39415      | count   |
| ts_unix@16bits       | present_bloom_skipped_pct    |     99.63   | percent |
| ts_unix@16bits       | present_minmax_skipped_pct   |      5.964  | percent |
| ts_unix@16bits       | present_ideal_skipped_pct    |     99.84   | percent |
| ts_unix@16bits       | absent_bloom_skipped_pct     |     99.79   | percent |
| ts_unix@16bits       | absent_minmax_skipped_pct    |      6.38   | percent |
| ts_unix@16bits       | absent_ideal_skipped_pct     |    100      | percent |
| ts_unix@16bits       | share_of_best_total_pct      |      2.42   | percent |
| words:ext1.label     | common_skipped_pct           |      6.274  | percent |
| words:ext1.label     | rare_skipped_pct             |      6.723  | percent |
| words:ext1.label     | never_skipped_pct            |     98.88   | percent |
| words:ext1.label     | filter_bytes                 |  10814      | bytes   |
| words:user.email     | common_skipped_pct           |     98.28   | percent |
| words:user.email     | rare_skipped_pct             |     98.54   | percent |
| words:user.email     | never_skipped_pct            |     98.76   | percent |
| words:user.email     | filter_bytes                 |  42054      | bytes   |
| words:customer.email | common_skipped_pct           |     96.19   | percent |
| words:customer.email | rare_skipped_pct             |     97.7    | percent |
| words:customer.email | never_skipped_pct            |     99.12   | percent |
| words:customer.email | filter_bytes                 |  41280      | bytes   |
| words:http.path      | common_skipped_pct           |      0      | percent |
| words:http.path      | rare_skipped_pct             |      0      | percent |
| words:http.path      | never_skipped_pct            |     99      | percent |
| words:http.path      | filter_bytes                 |   1584      | bytes   |

### `06_bloom` / dataset `uneven`

| variant           | metric                       |       value | unit    |
|-------------------|------------------------------|-------------|---------|
| request_id@8bits  | row_groups                   |   2837      | count   |
| request_id@8bits  | filter_bytes                 |  14416      | bytes   |
| request_id@8bits  | k                            |      6      | count   |
| request_id@8bits  | misses_generated             |   1000      | count   |
| request_id@8bits  | misses_rejected_out_of_range |      0      | count   |
| request_id@8bits  | present_bloom_skipped_pct    |     96.09   | percent |
| request_id@8bits  | present_minmax_skipped_pct   |     38.27   | percent |
| request_id@8bits  | present_ideal_skipped_pct    |     99.96   | percent |
| request_id@8bits  | absent_bloom_skipped_pct     |     96.08   | percent |
| request_id@8bits  | absent_minmax_skipped_pct    |     38.28   | percent |
| request_id@8bits  | absent_ideal_skipped_pct     |    100      | percent |
| request_id@8bits  | share_of_best_total_pct      |      0.335  | percent |
| request_id@10bits | row_groups                   |   2837      | count   |
| request_id@10bits | filter_bytes                 |  19283      | bytes   |
| request_id@10bits | k                            |      7      | count   |
| request_id@10bits | misses_generated             |   1000      | count   |
| request_id@10bits | misses_rejected_out_of_range |      0      | count   |
| request_id@10bits | present_bloom_skipped_pct    |     97.31   | percent |
| request_id@10bits | present_minmax_skipped_pct   |     38.27   | percent |
| request_id@10bits | present_ideal_skipped_pct    |     99.96   | percent |
| request_id@10bits | absent_bloom_skipped_pct     |     97.08   | percent |
| request_id@10bits | absent_minmax_skipped_pct    |     38.28   | percent |
| request_id@10bits | absent_ideal_skipped_pct     |    100      | percent |
| request_id@10bits | share_of_best_total_pct      |      0.448  | percent |
| request_id@16bits | row_groups                   |   2837      | count   |
| request_id@16bits | filter_bytes                 |  28832      | bytes   |
| request_id@16bits | k                            |     11      | count   |
| request_id@16bits | misses_generated             |   1000      | count   |
| request_id@16bits | misses_rejected_out_of_range |      0      | count   |
| request_id@16bits | present_bloom_skipped_pct    |     99.14   | percent |
| request_id@16bits | present_minmax_skipped_pct   |     38.27   | percent |
| request_id@16bits | present_ideal_skipped_pct    |     99.96   | percent |
| request_id@16bits | absent_bloom_skipped_pct     |     99.17   | percent |
| request_id@16bits | absent_minmax_skipped_pct    |     38.28   | percent |
| request_id@16bits | absent_ideal_skipped_pct     |    100      | percent |
| request_id@16bits | share_of_best_total_pct      |      0.6699 | percent |
| user.email@8bits  | row_groups                   |   2837      | count   |
| user.email@8bits  | filter_bytes                 |  14416      | bytes   |
| user.email@8bits  | k                            |      6      | count   |
| user.email@8bits  | misses_generated             |   1000      | count   |
| user.email@8bits  | misses_rejected_out_of_range |      0      | count   |
| user.email@8bits  | present_bloom_skipped_pct    |     96.12   | percent |
| user.email@8bits  | present_minmax_skipped_pct   |     38.66   | percent |
| user.email@8bits  | present_ideal_skipped_pct    |     99.96   | percent |
| user.email@8bits  | absent_bloom_skipped_pct     |     96.14   | percent |
| user.email@8bits  | absent_minmax_skipped_pct    |     38.6    | percent |
| user.email@8bits  | absent_ideal_skipped_pct     |    100      | percent |
| user.email@8bits  | share_of_best_total_pct      |      0.335  | percent |
| user.email@10bits | row_groups                   |   2837      | count   |
| user.email@10bits | filter_bytes                 |  19283      | bytes   |
| user.email@10bits | k                            |      7      | count   |
| user.email@10bits | misses_generated             |   1000      | count   |
| user.email@10bits | misses_rejected_out_of_range |      0      | count   |
| user.email@10bits | present_bloom_skipped_pct    |     97.17   | percent |
| user.email@10bits | present_minmax_skipped_pct   |     38.66   | percent |
| user.email@10bits | present_ideal_skipped_pct    |     99.96   | percent |
| user.email@10bits | absent_bloom_skipped_pct     |     97.12   | percent |
| user.email@10bits | absent_minmax_skipped_pct    |     38.6    | percent |
| user.email@10bits | absent_ideal_skipped_pct     |    100      | percent |
| user.email@10bits | share_of_best_total_pct      |      0.448  | percent |
| user.email@16bits | row_groups                   |   2837      | count   |
| user.email@16bits | filter_bytes                 |  28832      | bytes   |
| user.email@16bits | k                            |     11      | count   |
| user.email@16bits | misses_generated             |   1000      | count   |
| user.email@16bits | misses_rejected_out_of_range |      0      | count   |
| user.email@16bits | present_bloom_skipped_pct    |     99.12   | percent |
| user.email@16bits | present_minmax_skipped_pct   |     38.66   | percent |
| user.email@16bits | present_ideal_skipped_pct    |     99.96   | percent |
| user.email@16bits | absent_bloom_skipped_pct     |     99.16   | percent |
| user.email@16bits | absent_minmax_skipped_pct    |     38.6    | percent |
| user.email@16bits | absent_ideal_skipped_pct     |    100      | percent |
| user.email@16bits | share_of_best_total_pct      |      0.6699 | percent |
| ts_unix@8bits     | row_groups                   |   3249      | count   |
| ts_unix@8bits     | filter_bytes                 |  27903      | bytes   |
| ts_unix@8bits     | k                            |      6      | count   |
| ts_unix@8bits     | misses_generated             |    138      | count   |
| ts_unix@8bits     | misses_rejected_out_of_range | 199816      | count   |
| ts_unix@8bits     | present_bloom_skipped_pct    |     96.08   | percent |
| ts_unix@8bits     | present_minmax_skipped_pct   |     34.82   | percent |
| ts_unix@8bits     | present_ideal_skipped_pct    |     99.97   | percent |
| ts_unix@8bits     | absent_bloom_skipped_pct     |     96.09   | percent |
| ts_unix@8bits     | absent_minmax_skipped_pct    |     36.01   | percent |
| ts_unix@8bits     | absent_ideal_skipped_pct     |    100      | percent |
| ts_unix@8bits     | share_of_best_total_pct      |      0.6483 | percent |
| ts_unix@10bits    | row_groups                   |   3249      | count   |
| ts_unix@10bits    | filter_bytes                 |  36300      | bytes   |
| ts_unix@10bits    | k                            |      7      | count   |
| ts_unix@10bits    | misses_generated             |    138      | count   |
| ts_unix@10bits    | misses_rejected_out_of_range | 199816      | count   |
| ts_unix@10bits    | present_bloom_skipped_pct    |     97.39   | percent |
| ts_unix@10bits    | present_minmax_skipped_pct   |     34.82   | percent |
| ts_unix@10bits    | present_ideal_skipped_pct    |     99.97   | percent |
| ts_unix@10bits    | absent_bloom_skipped_pct     |     97.41   | percent |
| ts_unix@10bits    | absent_minmax_skipped_pct    |     36.01   | percent |
| ts_unix@10bits    | absent_ideal_skipped_pct     |    100      | percent |
| ts_unix@10bits    | share_of_best_total_pct      |      0.8434 | percent |
| ts_unix@16bits    | row_groups                   |   3249      | count   |
| ts_unix@16bits    | filter_bytes                 |  55806      | bytes   |
| ts_unix@16bits    | k                            |     11      | count   |
| ts_unix@16bits    | misses_generated             |    138      | count   |
| ts_unix@16bits    | misses_rejected_out_of_range | 199816      | count   |
| ts_unix@16bits    | present_bloom_skipped_pct    |     99.19   | percent |
| ts_unix@16bits    | present_minmax_skipped_pct   |     34.82   | percent |
| ts_unix@16bits    | present_ideal_skipped_pct    |     99.97   | percent |
| ts_unix@16bits    | absent_bloom_skipped_pct     |     99.25   | percent |
| ts_unix@16bits    | absent_minmax_skipped_pct    |     36.01   | percent |
| ts_unix@16bits    | absent_ideal_skipped_pct     |    100      | percent |
| ts_unix@16bits    | share_of_best_total_pct      |      1.297  | percent |
| client.ip@8bits   | row_groups                   |   3197      | count   |
| client.ip@8bits   | filter_bytes                 |  18784      | bytes   |
| client.ip@8bits   | k                            |      6      | count   |
| client.ip@8bits   | misses_generated             |   1000      | count   |
| client.ip@8bits   | misses_rejected_out_of_range |      0      | count   |
| client.ip@8bits   | present_bloom_skipped_pct    |     96.12   | percent |
| client.ip@8bits   | present_minmax_skipped_pct   |     35.91   | percent |
| client.ip@8bits   | present_ideal_skipped_pct    |     99.97   | percent |
| client.ip@8bits   | absent_bloom_skipped_pct     |     96.15   | percent |
| client.ip@8bits   | absent_minmax_skipped_pct    |     35.95   | percent |
| client.ip@8bits   | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@8bits   | share_of_best_total_pct      |      0.4364 | percent |
| client.ip@10bits  | row_groups                   |   3197      | count   |
| client.ip@10bits  | filter_bytes                 |  24916      | bytes   |
| client.ip@10bits  | k                            |      7      | count   |
| client.ip@10bits  | misses_generated             |   1000      | count   |
| client.ip@10bits  | misses_rejected_out_of_range |      0      | count   |
| client.ip@10bits  | present_bloom_skipped_pct    |     97.3    | percent |
| client.ip@10bits  | present_minmax_skipped_pct   |     35.91   | percent |
| client.ip@10bits  | present_ideal_skipped_pct    |     99.97   | percent |
| client.ip@10bits  | absent_bloom_skipped_pct     |     97.26   | percent |
| client.ip@10bits  | absent_minmax_skipped_pct    |     35.95   | percent |
| client.ip@10bits  | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@10bits  | share_of_best_total_pct      |      0.5789 | percent |
| client.ip@16bits  | row_groups                   |   3197      | count   |
| client.ip@16bits  | filter_bytes                 |  37568      | bytes   |
| client.ip@16bits  | k                            |     11      | count   |
| client.ip@16bits  | misses_generated             |   1000      | count   |
| client.ip@16bits  | misses_rejected_out_of_range |      0      | count   |
| client.ip@16bits  | present_bloom_skipped_pct    |     99.16   | percent |
| client.ip@16bits  | present_minmax_skipped_pct   |     35.91   | percent |
| client.ip@16bits  | present_ideal_skipped_pct    |     99.97   | percent |
| client.ip@16bits  | absent_bloom_skipped_pct     |     99.18   | percent |
| client.ip@16bits  | absent_minmax_skipped_pct    |     35.95   | percent |
| client.ip@16bits  | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@16bits  | share_of_best_total_pct      |      0.8729 | percent |
| order_id@8bits    | row_groups                   |   3197      | count   |
| order_id@8bits    | filter_bytes                 |  18481      | bytes   |
| order_id@8bits    | k                            |      6      | count   |
| order_id@8bits    | misses_generated             |   1000      | count   |
| order_id@8bits    | misses_rejected_out_of_range |      0      | count   |
| order_id@8bits    | present_bloom_skipped_pct    |     96.12   | percent |
| order_id@8bits    | present_minmax_skipped_pct   |     35.86   | percent |
| order_id@8bits    | present_ideal_skipped_pct    |     99.97   | percent |
| order_id@8bits    | absent_bloom_skipped_pct     |     96.16   | percent |
| order_id@8bits    | absent_minmax_skipped_pct    |     35.9    | percent |
| order_id@8bits    | absent_ideal_skipped_pct     |    100      | percent |
| order_id@8bits    | share_of_best_total_pct      |      0.4294 | percent |
| order_id@10bits   | row_groups                   |   3197      | count   |
| order_id@10bits   | filter_bytes                 |  24506      | bytes   |
| order_id@10bits   | k                            |      7      | count   |
| order_id@10bits   | misses_generated             |   1000      | count   |
| order_id@10bits   | misses_rejected_out_of_range |      0      | count   |
| order_id@10bits   | present_bloom_skipped_pct    |     97.41   | percent |
| order_id@10bits   | present_minmax_skipped_pct   |     35.86   | percent |
| order_id@10bits   | present_ideal_skipped_pct    |     99.97   | percent |
| order_id@10bits   | absent_bloom_skipped_pct     |     97.4    | percent |
| order_id@10bits   | absent_minmax_skipped_pct    |     35.9    | percent |
| order_id@10bits   | absent_ideal_skipped_pct     |    100      | percent |
| order_id@10bits   | share_of_best_total_pct      |      0.5694 | percent |
| order_id@16bits   | row_groups                   |   3197      | count   |
| order_id@16bits   | filter_bytes                 |  36962      | bytes   |
| order_id@16bits   | k                            |     11      | count   |
| order_id@16bits   | misses_generated             |   1000      | count   |
| order_id@16bits   | misses_rejected_out_of_range |      0      | count   |
| order_id@16bits   | present_bloom_skipped_pct    |     99.18   | percent |
| order_id@16bits   | present_minmax_skipped_pct   |     35.86   | percent |
| order_id@16bits   | present_ideal_skipped_pct    |     99.97   | percent |
| order_id@16bits   | absent_bloom_skipped_pct     |     99.22   | percent |
| order_id@16bits   | absent_minmax_skipped_pct    |     35.9    | percent |
| order_id@16bits   | absent_ideal_skipped_pct     |    100      | percent |
| order_id@16bits   | share_of_best_total_pct      |      0.8588 | percent |
| words:ext3.label  | common_skipped_pct           |     40.22   | percent |
| words:ext3.label  | rare_skipped_pct             |     44.28   | percent |
| words:ext3.label  | never_skipped_pct            |     97.75   | percent |
| words:ext3.label  | filter_bytes                 |   8418      | bytes   |
| words:ext4.label  | common_skipped_pct           |     40.57   | percent |
| words:ext4.label  | rare_skipped_pct             |     43.98   | percent |
| words:ext4.label  | never_skipped_pct            |     97.79   | percent |
| words:ext4.label  | filter_bytes                 |   8443      | bytes   |
| words:ext5.label  | common_skipped_pct           |     40.52   | percent |
| words:ext5.label  | rare_skipped_pct             |     43.06   | percent |
| words:ext5.label  | never_skipped_pct            |     97.86   | percent |
| words:ext5.label  | filter_bytes                 |   8476      | bytes   |
| words:ext6.label  | common_skipped_pct           |     40.57   | percent |
| words:ext6.label  | rare_skipped_pct             |     43.9    | percent |
| words:ext6.label  | never_skipped_pct            |     97.77   | percent |
| words:ext6.label  | filter_bytes                 |   8426      | bytes   |

**Notes**

- distinct/rows 0.978; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- against script 05's BEST_PER_CHUNK (6,870,373 bytes)
- distinct/rows 1.000; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- distinct/rows 0.967; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- distinct/rows 0.984; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- 10 bits per distinct word, one filter per row group; 8 probe words; this column has fewer distinct words than the probe set, so `common` and `rare` are drawn from the same list and the two figures are not a comparison
- 10 bits per distinct word, one filter per row group; 200 probe words; this column has fewer distinct words than the probe set, so `common` and `rare` are drawn from the same list and the two figures are not a comparison
- 528 row groups, 17 distinct words
- 10 bits per distinct word, one filter per row group; 200 probe words
- 576 row groups, 32,367 distinct words
- 72 row groups, 31,966 distinct words
- 72 row groups, 17 distinct words
- distinct/rows 0.942; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- against script 05's BEST_PER_CHUNK (4,303,865 bytes)
- distinct/rows 0.928; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- distinct/rows 0.697; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- distinct/rows 0.955; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- 680 row groups, 17 distinct words
- 681 row groups, 17 distinct words
- 679 row groups, 17 distinct words

## 07_vs_others

_The same logs in other systems_

Run 2026-09-18T23:17:24+00:00, git `3b5b8b0`, 72 rows.

### `07_vs_others` / dataset `flat30`

| variant                | metric                       | value                                                                                                             | unit    |
|------------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------|---------|
| elasticsearch          | version                      | docker.elastic.co/elasticsearch/elasticsearch:8.19.20                                                             | tag     |
| clickhouse             | version                      | clickhouse/clickhouse-server:26.8.7.19                                                                            | tag     |
| clp-s                  | version                      | y-scope/clp v0.13.0, clp-s built from source for the host architecture (no prebuilt binary or image is published) | tag     |
| raw NDJSON             | bytes                        | 46,591,991                                                                                                        | bytes   |
| all                    | at_timestamp_added           | 1                                                                                                                 | flag    |
| Elasticsearch          | heap                         | 1g                                                                                                                | setting |
| Elasticsearch standard | bytes_on_disk                | 58,215,037                                                                                                        | bytes   |
| Elasticsearch standard | ratio_to_raw                 | 1.249                                                                                                             | ratio   |
| Elasticsearch standard | load_seconds                 | 12.35                                                                                                             | s       |
| Elasticsearch standard | search_event value_seconds   | 0.0818                                                                                                            | s       |
| Elasticsearch standard | search_level = ERROR_seconds | 0.0105                                                                                                            | s       |
| Elasticsearch standard | search_one id_seconds        | 0.0053                                                                                                            | s       |
| Elasticsearch standard | search_newest 500_seconds    | 0.1061                                                                                                            | s       |
| Elasticsearch logsdb   | bytes_on_disk                | 45,742,451                                                                                                        | bytes   |
| Elasticsearch logsdb   | ratio_to_raw                 | 0.9818                                                                                                            | ratio   |
| Elasticsearch logsdb   | load_seconds                 | 10.76                                                                                                             | s       |
| Elasticsearch logsdb   | search_event value_seconds   | 0.0154                                                                                                            | s       |
| Elasticsearch logsdb   | search_level = ERROR_seconds | 0.0036                                                                                                            | s       |
| Elasticsearch logsdb   | search_one id_seconds        | 0.0029                                                                                                            | s       |
| Elasticsearch logsdb   | search_newest 500_seconds    | 0.0261                                                                                                            | s       |
| ClickHouse             | bytes_on_disk                | 17,578,014                                                                                                        | bytes   |
| ClickHouse             | ratio_to_raw                 | 0.3773                                                                                                            | ratio   |
| ClickHouse             | load_seconds                 | 7.12                                                                                                              | s       |
| ClickHouse             | search_event value_seconds   | 0.0121                                                                                                            | s       |
| ClickHouse             | search_level = ERROR_seconds | 0.0181                                                                                                            | s       |
| ClickHouse             | search_one id_seconds        | 0.0072                                                                                                            | s       |
| ClickHouse             | search_newest 500_seconds    | 0.05                                                                                                              | s       |
| clp-s                  | roundtrip_lines              | 200,000                                                                                                           | lines   |
| clp-s                  | roundtrip_ok                 | 1                                                                                                                 | flag    |
| clp-s                  | bytes_on_disk                | 8,482,536                                                                                                         | bytes   |
| clp-s                  | ratio_to_raw                 | 0.1821                                                                                                            | ratio   |
| clp-s                  | load_seconds                 | 0.83                                                                                                              | s       |
| clp-s                  | search_event value_seconds   | 0.2154                                                                                                            | s       |
| clp-s                  | search_level = ERROR_seconds | 0.1915                                                                                                            | s       |
| clp-s                  | search_one id_seconds        | 0.1797                                                                                                            | s       |
| clp-s                  | search_newest 500_seconds    | 0.4958                                                                                                            | s       |

### `07_vs_others` / dataset `uneven`

| variant                | metric                       | value                                                                                                             | unit    |
|------------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------|---------|
| elasticsearch          | version                      | docker.elastic.co/elasticsearch/elasticsearch:8.19.20                                                             | tag     |
| clickhouse             | version                      | clickhouse/clickhouse-server:26.8.7.19                                                                            | tag     |
| clp-s                  | version                      | y-scope/clp v0.13.0, clp-s built from source for the host architecture (no prebuilt binary or image is published) | tag     |
| raw NDJSON             | bytes                        | 44,759,683                                                                                                        | bytes   |
| all                    | at_timestamp_added           | 1                                                                                                                 | flag    |
| Elasticsearch          | heap                         | 1g                                                                                                                | setting |
| Elasticsearch standard | bytes_on_disk                | 31,115,869                                                                                                        | bytes   |
| Elasticsearch standard | ratio_to_raw                 | 0.6952                                                                                                            | ratio   |
| Elasticsearch standard | load_seconds                 | 11.09                                                                                                             | s       |
| Elasticsearch standard | search_event value_seconds   | 0.1127                                                                                                            | s       |
| Elasticsearch standard | search_level = ERROR_seconds | 0.0062                                                                                                            | s       |
| Elasticsearch standard | search_one id_seconds        | 0.0178                                                                                                            | s       |
| Elasticsearch standard | search_newest 500_seconds    | 0.158                                                                                                             | s       |
| Elasticsearch logsdb   | bytes_on_disk                | 24,149,646                                                                                                        | bytes   |
| Elasticsearch logsdb   | ratio_to_raw                 | 0.5395                                                                                                            | ratio   |
| Elasticsearch logsdb   | load_seconds                 | 9.92                                                                                                              | s       |
| Elasticsearch logsdb   | search_event value_seconds   | 0.0353                                                                                                            | s       |
| Elasticsearch logsdb   | search_level = ERROR_seconds | 0.0105                                                                                                            | s       |
| Elasticsearch logsdb   | search_one id_seconds        | 0.0054                                                                                                            | s       |
| Elasticsearch logsdb   | search_newest 500_seconds    | 0.0223                                                                                                            | s       |
| ClickHouse             | bytes_on_disk                | 7,055,540                                                                                                         | bytes   |
| ClickHouse             | ratio_to_raw                 | 0.1576                                                                                                            | ratio   |
| ClickHouse             | load_seconds                 | 7.56                                                                                                              | s       |
| ClickHouse             | search_event value_seconds   | 0.0123                                                                                                            | s       |
| ClickHouse             | search_level = ERROR_seconds | 0.0156                                                                                                            | s       |
| ClickHouse             | search_one id_seconds        | 0.0089                                                                                                            | s       |
| ClickHouse             | search_newest 500_seconds    | 0.0446                                                                                                            | s       |
| clp-s                  | roundtrip_lines              | 200,000                                                                                                           | lines   |
| clp-s                  | roundtrip_ok                 | 1                                                                                                                 | flag    |
| clp-s                  | bytes_on_disk                | 2,989,862                                                                                                         | bytes   |
| clp-s                  | ratio_to_raw                 | 0.0668                                                                                                            | ratio   |
| clp-s                  | load_seconds                 | 0.65                                                                                                              | s       |
| clp-s                  | search_event value_seconds   | 0.1614                                                                                                            | s       |
| clp-s                  | search_level = ERROR_seconds | 0.2045                                                                                                            | s       |
| clp-s                  | search_one id_seconds        | 0.1716                                                                                                            | s       |
| clp-s                  | search_newest 500_seconds    | 0.585                                                                                                             | s       |

**Notes**

- pinned in lab/others/docker-compose.yml
- the denominator for every ratio
- `@timestamp` is added to every document on the way into Elasticsearch and ClickHouse - logsdb mode requires one and the dataset has `ts`/`ts_unix`/`ts_ms` instead. Those bytes are not in the raw NDJSON denominator.
- -Xms1g -Xmx1g, not an Elasticsearch default: the Docker VM here holds under 4 GB and 07 runs one system at a time
- after _forcemerge to one segment and _flush
- rough, one machine, default settings - one laptop, no tuning of any system, containers sharing one disk
- sum(bytes_on_disk) over active parts after OPTIMIZE TABLE FINAL
- archive decompressed and counted; 200,000 went in. A ratio is this script's headline number, so the archive is expanded rather than trusted.
- archive directory size, round-trip verified; clp-s is fed the raw payloads with no @timestamp added, unlike Elasticsearch and ClickHouse

## 08_io_shapes

_Disk-dominated shapes_

Run 2026-09-18T21:59:12+00:00, git `b405451`, 40 rows.

### `08_io_shapes` / dataset `uneven`

| variant               | metric                 |            value | unit      |
|-----------------------|------------------------|------------------|-----------|
| fsync batch=1         | records_per_sec        |  39898           | records/s |
| fsync batch=1         | total_ms               |     75.19        | ms        |
| fsync batch=10        | records_per_sec        | 341711           | records/s |
| fsync batch=10        | total_ms               |      8.78        | ms        |
| fsync batch=100       | records_per_sec        |      1.43178e+06 | records/s |
| fsync batch=100       | total_ms               |      2.1         | ms        |
| fsync batch=500       | records_per_sec        |      1.65153e+06 | records/s |
| fsync batch=500       | total_ms               |      1.82        | ms        |
| F_FULLFSYNC batch=1   | records_per_sec        |    360           | records/s |
| F_FULLFSYNC batch=1   | total_ms               |   8320           | ms        |
| F_FULLFSYNC batch=10  | records_per_sec        |   3514           | records/s |
| F_FULLFSYNC batch=10  | total_ms               |    853.6         | ms        |
| F_FULLFSYNC batch=100 | records_per_sec        |  27832           | records/s |
| F_FULLFSYNC batch=100 | total_ms               |    107.8         | ms        |
| F_FULLFSYNC batch=500 | records_per_sec        | 125652           | records/s |
| F_FULLFSYNC batch=500 | total_ms               |     23.88        | ms        |
| wal batch=1           | compression_ratio      |      0.8638      | ratio     |
| wal batch=1           | zstd_MBps              |     54.8         | MB/s      |
| wal batch=10          | compression_ratio      |      0.2667      | ratio     |
| wal batch=10          | zstd_MBps              |    293.5         | MB/s      |
| wal batch=100         | compression_ratio      |      0.1421      | ratio     |
| wal batch=100         | zstd_MBps              |    709.9         | MB/s      |
| wal batch=500         | compression_ratio      |      0.1141      | ratio     |
| wal batch=500         | zstd_MBps              |    832.3         | MB/s      |
| 1000 files            | listdir_ms             |      0.453       | ms        |
| 1000 files            | manifest_read_ms       |      0.046       | ms        |
| 1000 files            | listdir_over_manifest  |      9.9         | ratio     |
| 10000 files           | listdir_ms             |      3.936       | ms        |
| 10000 files           | manifest_read_ms       |      0.222       | ms        |
| 10000 files           | listdir_over_manifest  |     17.7         | ratio     |
| 100000 files          | listdir_ms             |     43.9         | ms        |
| 100000 files          | manifest_read_ms       |      3.376       | ms        |
| 100000 files          | listdir_over_manifest  |     13           | ratio     |
| scattered vs grouped  | row_groups_of_template |     51           | count     |
| scattered vs grouped  | ranges_before          |     51           | count     |
| scattered vs grouped  | ranges_after           |      1           | count     |
| scattered vs grouped  | ssd_ms_before          |      5.1         | ms        |
| scattered vs grouped  | ssd_ms_after           |      0.1         | ms        |
| scattered vs grouped  | s3_ms_before           |    765           | ms        |
| scattered vs grouped  | s3_ms_after            |     15           | ms        |

**Notes**

- shape only - one laptop, Python around the call; not a TrieDB throughput; the disk does this work. fsync(2) on Darwin returns at the drive's write cache, not the platter
- shape only - one laptop, Python around the call; not a TrieDB throughput
- shape only - one laptop, Python around the call; not a TrieDB throughput; the disk does this work. F_FULLFSYNC is Darwin's real barrier to the platter
- shape only - one laptop, Python around the call; not a TrieDB throughput; zstd is a C library, so the MB/s is its own: 54.8
- shape only - one laptop, Python around the call; not a TrieDB throughput; zstd is a C library, so the MB/s is its own: 293.5
- shape only - one laptop, Python around the call; not a TrieDB throughput; zstd is a C library, so the MB/s is its own: 709.9
- shape only - one laptop, Python around the call; not a TrieDB throughput; zstd is a C library, so the MB/s is its own: 832.3
- shape only - one laptop, Python around the call; not a TrieDB throughput; the filesystem does this
- shape only - one laptop, Python around the call; not a TrieDB throughput; a range count multiplied by a per-seek constant, not a timed read. Day 2024-01-15, template 1055466456870197778

## 09_template_lookup

_Searches on keys that are not promoted_

Run 2026-09-18T22:10:27+00:00, git `b405451`, 22 rows.

### `09_template_lookup` / dataset `synthetic`

| variant          | metric               |           value | unit   |
|------------------|----------------------|-----------------|--------|
| 1000 templates   | scan_comparisons     | 200000          | count  |
| 1000 templates   | scan_growth_vs_1000  |      1          | ratio  |
| 1000 templates   | index_growth_vs_1000 |      1          | ratio  |
| 1000 templates   | scan_over_index      |   1173          | ratio  |
| 1000 templates   | index_entries        |  12259          | count  |
| 1000 templates   | rows_of_answer       |  10954          | count  |
| 1000 templates   | index_build_ms       |      0.482      | ms     |
| 10000 templates  | scan_comparisons     |      2e+06      | count  |
| 10000 templates  | scan_growth_vs_1000  |     10.63       | ratio  |
| 10000 templates  | index_growth_vs_1000 |      1          | ratio  |
| 10000 templates  | scan_over_index      |  12470          | ratio  |
| 10000 templates  | index_entries        | 124938          | count  |
| 10000 templates  | rows_of_answer       | 111338          | count  |
| 10000 templates  | index_build_ms       |      4.828      | ms     |
| 100000 templates | scan_comparisons     |      2e+07      | count  |
| 100000 templates | scan_growth_vs_1000  |    235.7        | ratio  |
| 100000 templates | index_growth_vs_1000 |      0.9842     | ratio  |
| 100000 templates | scan_over_index      | 280900          | ratio  |
| 100000 templates | index_entries        |      1.2508e+06 | count  |
| 100000 templates | rows_of_answer       |      1.115e+06  | count  |
| 100000 templates | index_build_ms       |     46.55       | ms     |
| verdict          | scan_superlinearity  |      2.357      | ratio  |

**Notes**

- shape only - growth against the 1,000-template case, not an absolute time
- scan growth 235.7x against the linear 100x expected from 1,000 to 100,000 templates

