# kibbr experiment lab - results

Measurements, not opinions. Every size figure comes from a real file under
`lab/out/`, counted twice: `st_size` for the bytes a format spends and
`st_blocks * 512` for what the filesystem charges. Nothing here ships.

**Speed figures appear only where a C library or the disk did the work**
(zstd, fsync, file reads), and each says so. No throughput number measured over
Python code is a kibbr number.

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

## Further reading

[`CLP_S_NOTES.md`](CLP_S_NOTES.md) - how clp-s works, read off its source at tag v0.13.0, and what kibbr could take from it. It is the only comparison system in script 07 that shares kibbr's central idea (one columnar table per distinct key set), so where it differs is where the design choices actually are.

## Findings so far

- **The Python frontend agrees with the binary**: 0 disagreements over 5 datasets. Every template id and inferred type below is the one kibbr would assign.
- **The byte model is calibrated on `flat30`**: it predicts +0.00% of the segment bytes the real binary wrote (bound: 10%).
- **The byte model is calibrated on `uneven`**: it predicts +0.00% of the segment bytes the real binary wrote (bound: 10%).
- **`uneven`**: the smallest layout is zstd -19 at 2,896,966 bytes. Per-template Parquet loses to gzip -9 by 6.33x on bytes; what kibbr does today (one Parquet per template per day) costs 0.57x of raw NDJSON.
- **`flat30`**: the smallest layout is zstd -19 at 8,282,006 bytes. Per-template Parquet loses to gzip -9 by 1.14x on bytes; what kibbr does today (one Parquet per template per day) costs 1.81x of raw NDJSON.
- **`oneday`**: the smallest layout is zstd -19 at 8,059,807 bytes. Per-template Parquet loses to gzip -9 by 1.15x on bytes; what kibbr does today (one Parquet per template per day) costs 0.26x of raw NDJSON.
- **`farm`**: the smallest layout is zstd -19 at 5,995,347 bytes. Per-template Parquet beats gzip -9 by 0.77x on bytes; what kibbr does today (one Parquet per template per day) costs 0.09x of raw NDJSON.
- **Encodings on `flat30`**: picking the smallest encoding per chunk reaches 0.144x of raw NDJSON and 0.618x of gzip -9; sorting rows inside each chunk is worth a further 2.52%.
- **Encodings on `uneven`**: picking the smallest encoding per chunk reaches 0.096x of raw NDJSON and 1.052x of gzip -9; sorting rows inside each chunk is worth a further 0.68%.
- **Encodings on `farm`**: picking the smallest encoding per chunk reaches 0.068x of raw NDJSON and 0.608x of gzip -9; sorting rows inside each chunk is worth a further -4.60%.
- **Encodings on `farm[J80]`**: picking the smallest encoding per chunk reaches 0.068x of raw NDJSON and 0.604x of gzip -9; sorting rows inside each chunk is worth a further -3.17%.
- **Bloom filters earn their bytes**: at 10 bits per value they skip 98.4% of row groups for an id that is not there, against 13.5% for min/max statistics alone, and cost 1.30% of the best encoded total.
- **Which durability call matters as much as batching**: a per-record `fsync(2)` reaches 30,664 records/sec on this platform, the same loop with `F_FULLFSYNC` reaches 369 - Darwin's `fsync` stops at the drive cache. Shape only.
- **WAL batching pays twice**: zstd 3 over batches of 500 lines compresses to 0.114 of raw, against 0.864 at one line per batch.
- **Grouping a template's row groups together** turns 51 separate byte ranges into 1 for a one-template search - on S3 at 15 ms a range, the difference between a second and nothing.
- **Against other systems on `flat30`**: Elasticsearch standard 1.249x of raw, Elasticsearch logsdb 0.982x of raw, ClickHouse 0.377x of raw, clp-s 0.182x of raw; kibbr's best measured layout is BEST_PER_CHUNK sorted at 0.144x.
- **kibbr's best layout is smaller than clp-s**: 0.144x of raw against 0.182x. Its archive was decompressed and counted back to 200,000 lines, so the ratio is not a partial compression.
- **One asymmetry in that comparison**: Elasticsearch and ClickHouse each received an added `@timestamp` field, roughly 30 bytes per document, because logsdb mode requires one and the dataset carries `ts`/`ts_unix`/`ts_ms` instead. clp-s and kibbr were fed the raw payloads, so their ratios are flattered by that much.
- **Against other systems on `messy`**: Elasticsearch standard 1.078x of raw, Elasticsearch logsdb 0.924x of raw, ClickHouse 0.327x of raw, clp-s 0.178x of raw; kibbr's best measured layout is BEST_PER_CHUNK sorted at 0.153x.
- **kibbr's best layout is smaller than clp-s**: 0.153x of raw against 0.178x. Its archive was decompressed and counted back to 200,000 lines, so the ratio is not a partial compression.
- **One asymmetry in that comparison**: Elasticsearch and ClickHouse each received an added `@timestamp` field, roughly 30 bytes per document, because logsdb mode requires one and the dataset carries `ts`/`ts_unix`/`ts_ms` instead. clp-s and kibbr were fed the raw payloads, so their ratios are flattered by that much.
- **Against other systems on `uneven`**: Elasticsearch standard 0.695x of raw, Elasticsearch logsdb 0.540x of raw, ClickHouse 0.158x of raw, clp-s 0.067x of raw; kibbr's best measured layout is WIDE rg=122,880 at 0.086x.
- **clp-s stores these logs smaller than kibbr does**: 0.067x of raw against 0.086x, on the same 200,000 lines. Its archive was decompressed and counted back to 200,000 lines, so the ratio is not a partial compression. Its searches are the slower side of that trade, but on the storage question this lab was built to answer, clp-s wins.
- **One asymmetry in that comparison**: Elasticsearch and ClickHouse each received an added `@timestamp` field, roughly 30 bytes per document, because logsdb mode requires one and the dataset carries `ts`/`ts_unix`/`ts_ms` instead. clp-s and kibbr were fed the raw payloads, so their ratios are flattered by that much.
- **Type widening is a ladder cost, not a merging cost** (`farm`, 7 keys carrying two types): 1.5% of columns are already String by widening with NO merging at all, across 8 distinct keys. The worst merge rule (PER_EVENT) takes that to 2.9% and adds no new keys. The monotonic type join does the damage inside a single template; merging contributes a few points on top.
- **Type widening is a ladder cost, not a merging cost** (`messy`, 7 keys carrying two types): 44.0% of columns are already String by widening with NO merging at all, across 8 distinct keys. The worst merge rule (J70) takes that to 50.0% and adds no new keys. The monotonic type join does the damage inside a single template; merging contributes a few points on top.
- **Scanning every template for an unpromoted key bends the wrong way**: 100x the templates costs 2.2x more than linear, not the same. A key-to-templates map is flat.

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

Run 2026-09-22T20:10:50+00:00, git `7e42f16`, 10 rows.

### `00_frontend_agree` / dataset `farm`

| variant   | metric         |   value | unit   |
|-----------|----------------|---------|--------|
| farm      | lines_compared |   5,000 | lines  |
| farm      | disagreements  |       0 | count  |

### `00_frontend_agree` / dataset `flat30`

| variant   | metric         |   value | unit   |
|-----------|----------------|---------|--------|
| flat30    | lines_compared |   5,000 | lines  |
| flat30    | disagreements  |       0 | count  |

### `00_frontend_agree` / dataset `messy`

| variant   | metric         |   value | unit   |
|-----------|----------------|---------|--------|
| messy     | lines_compared |   5,000 | lines  |
| messy     | disagreements  |       0 | count  |

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

- kibbr gen-logs --profile farm --lines 200,000 --seed 42, unmodified; one simulated day in time order, ten services, each with its own time field (@timestamp, time, ts, ts_ms), a request's id recurring across services, 0.5% dirty values on five named keys
- kibbr gen-logs --lines 200000 --templates 12 --seed 42, unmodified; 31 days, flat across templates
- HAND-AUTHORED BY THE LAB, NOT REAL LOGS. 200,000 lines over one day, 8 services and 20 event types, written to carry the type inconsistency that `gen-logs` never produces - across flat30, oneday, uneven and testdata, zero keys carry two types, so every `columns widened to String` figure was vacuous. Injected fault rates, as realised: duration_ms_na 1.20% (2,400 lines); duration_ms_null 0.38% (766 lines); user_id_legacy 1.98% (3,955 lines); order_id_alpha 0.39% (785 lines); http_status_empty 0.23% (461 lines); retry_stringy 1.49% (2,975 lines); bytes_sent_float 3.05% (6,108 lines); error_code_symbol 0.57% (1,147 lines); tags_scalar 1.00% (1,994 lines); ts_epoch 4.95% (9,902 lines); trace_id_empty 0.78% (1,559 lines); client_ip_unknown 0.57% (1,134 lines). Every number derived from this dataset is a statement about these rates and about nothing else.
- flat30's lines, generator has no one-day flag; timestamps rewritten in Python - every ts/ts_unix/ts_ms mapped linearly from its 30.0-day span onto 2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits); line order unchanged, so oneday differs from flat30 in timestamps and nothing else
- generator cannot skew template shares, so built by sampling: pool of 300,000 lines at --templates 60 --seed 7, grouped by the binary's template_id, 4931 templates drawn to a Zipf profile where the top 10 hold 80% of 200,000 rows (seed 1315); generator has no one-day flag; timestamps rewritten in Python - every ts/ts_unix/ts_ms mapped linearly from its 30.0-day span onto 2024-01-15, each field keeping its own form (RFC 3339 / 10 digits / 13 digits); sorted by timestamp; 70.56% of rows are repeats of a pool line, drawn where a tail template had fewer distinct lines than its quota

## 01_profile_logs

_What the logs look like_

Run 2026-09-22T15:48:59+00:00, git `7f71d96`, 514 rows.

### `01_profile_logs` / dataset `farm`

| variant                          | metric                                       |            value | unit    |
|----------------------------------|----------------------------------------------|------------------|---------|
| farm                             | rows                                         | 200000           | count   |
| farm                             | raw bytes                                    |      9.7513e+07  | count   |
| farm                             | templates                                    |    143           | count   |
| farm                             | rows/template min                            |      1           | count   |
| farm                             | rows/template median                         |     70           | count   |
| farm                             | rows/template p90                            |   2212           | count   |
| farm                             | rows/template max                            |  57786           | count   |
| farm                             | top-10 share of rows %                       |     77           | count   |
| farm                             | templates with <100 rows                     |     81           | count   |
| farm                             | distinct promoted-value sets                 |     43           | count   |
| farm                             | templates in the biggest promoted group      |     20           | count   |
| farm                             | promoted groups holding one template         |     23           | count   |
| farm                             | distinct column names                        |    178           | count   |
| farm                             | days spanned by `ts`                         |      1           | count   |
| farm                             | days spanned by every Datetime column pooled |      5           | count   |
| farm                             | days spanned by the widest Datetime column   |      2           | count   |
| farm                             | templates_with_ts                            |     15           | count   |
| farm                             | templates_no_time_column                     |      0           | count   |
| farm                             | templates_time_not_named_ts                  |    128           | count   |
| farm                             | unskippable_templates                        |    128           | count   |
| farm                             | unskippable_rows                             | 190658           | count   |
| farm                             | unskippable_row_share                        |     95.33        | percent |
| farm                             | unskippable_bytes                            |      9.50199e+07 | count   |
| farm                             | unskippable_byte_share                       |     97.44        | percent |
| farm                             | templates_with_time_column (list rule)       |    143           | count   |
| farm                             | templates_without (list rule)                |      0           | count   |
| farm                             | rows_on_arrival_time (list rule)             |      0           | count   |
| farm                             | rows_on_arrival_time_share (list rule)       |      0           | percent |
| column:http_user_agent           | raw_bytes                                    |      9.97334e+06 | bytes   |
| column:user_agent.original       | raw_bytes                                    |      5.25478e+06 | bytes   |
| column:time                      | raw_bytes                                    |      2.70291e+06 | bytes   |
| column:request_id                | raw_bytes                                    |      2.62416e+06 | bytes   |
| column:http_referer              | raw_bytes                                    |      2.14785e+06 | bytes   |
| column:request_uri               | raw_bytes                                    |      2.0617e+06  | bytes   |
| column:http.request.id           | raw_bytes                                    |      1.67066e+06 | bytes   |
| column:@timestamp                | raw_bytes                                    |      1.55298e+06 | bytes   |
| column:host.name                 | raw_bytes                                    |      1.53834e+06 | bytes   |
| column:message                   | raw_bytes                                    |      1.41858e+06 | bytes   |
| column:msec                      | raw_bytes                                    |      1.21278e+06 | bytes   |
| column:remote_addr               | raw_bytes                                    |      1.0955e+06  | bytes   |
| column:url.path                  | raw_bytes                                    |      1.04674e+06 | bytes   |
| column:statement                 | raw_bytes                                    | 999463           | bytes   |
| column:url.route                 | raw_bytes                                    | 811725           | bytes   |
| column:event                     | raw_bytes                                    | 795368           | bytes   |
| column:hostname                  | raw_bytes                                    | 659207           | bytes   |
| column:client.ip                 | raw_bytes                                    | 642033           | bytes   |
| column:upstream.address          | raw_bytes                                    | 597940           | bytes   |
| column:duration_ms               | raw_bytes                                    | 568908           | bytes   |
| column:job.id                    | raw_bytes                                    | 545112           | bytes   |
| column:server_protocol           | raw_bytes                                    | 462288           | bytes   |
| column:upstream_addr             | raw_bytes                                    | 452589           | bytes   |
| column:worker                    | raw_bytes                                    | 447143           | bytes   |
| column:request_time              | raw_bytes                                    | 433175           | bytes   |
| column:upstream.duration_ms      | raw_bytes                                    | 385684           | bytes   |
| column:service.name              | raw_bytes                                    | 365624           | bytes   |
| column:msg                       | raw_bytes                                    | 354511           | bytes   |
| column:body_bytes_sent           | raw_bytes                                    | 343048           | bytes   |
| column:service.version           | raw_bytes                                    | 313392           | bytes   |
| column:request_method            | raw_bytes                                    | 267548           | bytes   |
| column:upstream.name             | raw_bytes                                    | 262367           | bytes   |
| column:upstream_response_time    | raw_bytes                                    | 261531           | bytes   |
| column:ecs\.version              | raw_bytes                                    | 261160           | bytes   |
| column:status                    | raw_bytes                                    | 259881           | bytes   |
| column:ts_ms                     | raw_bytes                                    | 252733           | bytes   |
| column:host                      | raw_bytes                                    | 239313           | bytes   |
| column:ts                        | raw_bytes                                    | 235374           | bytes   |
| column:job.type                  | raw_bytes                                    | 217758           | bytes   |
| column:log\.level                | raw_bytes                                    | 208961           | bytes   |
| column:http.response.bytes       | raw_bytes                                    | 181909           | bytes   |
| column:logger                    | raw_bytes                                    | 177381           | bytes   |
| column:http.request.method       | raw_bytes                                    | 164291           | bytes   |
| column:user.id                   | raw_bytes                                    | 161034           | bytes   |
| column:http.response.status_code | raw_bytes                                    | 156624           | bytes   |
| column:client_addr               | raw_bytes                                    | 150071           | bytes   |
| column:user                      | raw_bytes                                    | 142555           | bytes   |
| column:queue                     | raw_bytes                                    | 130164           | bytes   |
| column:pool                      | raw_bytes                                    | 112889           | bytes   |
| column:caller                    | raw_bytes                                    | 109034           | bytes   |
| column:job.args.endpoint         | raw_bytes                                    | 101549           | bytes   |
| column:level                     | raw_bytes                                    |  99193           | bytes   |
| column:session_id                | raw_bytes                                    |  85968           | bytes   |
| column:req_id                    | raw_bytes                                    |  81568           | bytes   |
| column:query.text                | raw_bytes                                    |  80759           | bytes   |
| column:lvl                       | raw_bytes                                    |  77764           | bytes   |
| column:ip                        | raw_bytes                                    |  70623           | bytes   |
| column:index                     | raw_bytes                                    |  69289           | bytes   |
| column:error.message             | raw_bytes                                    |  66910           | bytes   |
| column:db                        | raw_bytes                                    |  64508           | bytes   |
| column:job.args.run_id           | raw_bytes                                    |  61520           | bytes   |
| column:run_id                    | raw_bytes                                    |  61035           | bytes   |
| column:job.args.topic            | raw_bytes                                    |  60176           | bytes   |
| column:user_id                   | raw_bytes                                    |  54064           | bytes   |
| column:tls.version               | raw_bytes                                    |  53040           | bytes   |
| column:severity                  | raw_bytes                                    |  52321           | bytes   |
| column:logger_name               | raw_bytes                                    |  50839           | bytes   |
| column:requestId                 | raw_bytes                                    |  46528           | bytes   |
| column:client                    | raw_bytes                                    |  43470           | bytes   |
| column:HOSTNAME                  | raw_bytes                                    |  39900           | bytes   |
| column:key                       | raw_bytes                                    |  31096           | bytes   |
| column:thread_name               | raw_bytes                                    |  29917           | bytes   |
| column:job                       | raw_bytes                                    |  28080           | bytes   |
| column:error.type                | raw_bytes                                    |  24428           | bytes   |
| column:spanId                    | raw_bytes                                    |  23264           | bytes   |
| column:node                      | raw_bytes                                    |  22011           | bytes   |
| column:schedule                  | raw_bytes                                    |  21830           | bytes   |
| column:job.args.template         | raw_bytes                                    |  21784           | bytes   |
| column:cluster.status            | raw_bytes                                    |  21279           | bytes   |
| column:token.kid                 | raw_bytes                                    |  20736           | bytes   |
| column:next_run                  | raw_bytes                                    |  19780           | bytes   |
| column:job.args.order_id         | raw_bytes                                    |  18920           | bytes   |
| column:method                    | raw_bytes                                    |  18288           | bytes   |
| column:token.exp                 | raw_bytes                                    |  17970           | bytes   |
| column:hits                      | raw_bytes                                    |  17950           | bytes   |
| column:took_ms                   | raw_bytes                                    |  17564           | bytes   |
| column:job.args.image_id         | raw_bytes                                    |  16686           | bytes   |
| column:job.attempt               | raw_bytes                                    |  15142           | bytes   |
| column:job.args.product_id       | raw_bytes                                    |  14755           | bytes   |
| column:order_id                  | raw_bytes                                    |  14540           | bytes   |
| column:psp_ref                   | raw_bytes                                    |  12798           | bytes   |
| column:oldest_age_ms             | raw_bytes                                    |  12544           | bytes   |
| column:mfa                       | raw_bytes                                    |  11456           | bytes   |
| column:template                  | raw_bytes                                    |  10728           | bytes   |
| column:result                    | raw_bytes                                    |   9968           | bytes   |
| column:job.args.width            | raw_bytes                                    |   8940           | bytes   |
| column:cmd                       | raw_bytes                                    |   8648           | bytes   |
| column:message_id                | raw_bytes                                    |   8470           | bytes   |
| column:keyspace_hits             | raw_bytes                                    |   8202           | bytes   |
| column:query.filters.category    | raw_bytes                                    |   8033           | bytes   |
| column:level_value               | raw_bytes                                    |   7980           | bytes   |
| column:job.args.export_id        | raw_bytes                                    |   7740           | bytes   |
| column:depth                     | raw_bytes                                    |   7599           | bytes   |
| column:latency_us                | raw_bytes                                    |   7355           | bytes   |
| column:keyspace_misses           | raw_bytes                                    |   7310           | bytes   |
| column:rows                      | raw_bytes                                    |   6881           | bytes   |
| column:name                      | raw_bytes                                    |   6856           | bytes   |
| column:reason                    | raw_bytes                                    |   6609           | bytes   |
| column:suggestion                | raw_bytes                                    |   6602           | bytes   |
| column:job.args.warehouse        | raw_bytes                                    |   6568           | bytes   |
| column:customer.ip               | raw_bytes                                    |   6270           | bytes   |
| column:psp                       | raw_bytes                                    |   6146           | bytes   |
| column:inflight                  | raw_bytes                                    |   6091           | bytes   |
| column:client_port               | raw_bytes                                    |   6000           | bytes   |
| column:error                     | raw_bytes                                    |   5855           | bytes   |
| column:customer.id               | raw_bytes                                    |   5826           | bytes   |
| column:query.page                | raw_bytes                                    |   5649           | bytes   |
| column:latency_ms                | raw_bytes                                    |   5590           | bytes   |
| column:provider                  | raw_bytes                                    |   5456           | bytes   |
| column:evicted_keys              | raw_bytes                                    |   5378           | bytes   |
| column:ops_per_sec               | raw_bytes                                    |   5304           | bytes   |
| column:amount                    | raw_bytes                                    |   5024           | bytes   |
| column:used_memory_mb            | raw_bytes                                    |   4920           | bytes   |
| column:job.args.user_id          | raw_bytes                                    |   4754           | bytes   |
| column:stack_trace               | raw_bytes                                    |   4402           | bytes   |
| column:channel                   | raw_bytes                                    |   4285           | bytes   |
| column:cluster.nodes             | raw_bytes                                    |   4248           | bytes   |
| column:stats.avg_query_us        | raw_bytes                                    |   3690           | bytes   |
| column:stats.query_per_s         | raw_bytes                                    |   3690           | bytes   |
| column:age_s                     | raw_bytes                                    |   3600           | bytes   |
| column:stats.xact_per_s          | raw_bytes                                    |   3501           | bytes   |
| column:connected_clients         | raw_bytes                                    |   3267           | bytes   |
| column:product_id                | raw_bytes                                    |   3250           | bytes   |
| column:job.args.shard            | raw_bytes                                    |   2965           | bytes   |
| column:shard                     | raw_bytes                                    |   2940           | bytes   |
| column:currency                  | raw_bytes                                    |   2865           | bytes   |
| column:card.brand                | raw_bytes                                    |   2820           | bytes   |
| column:decision                  | raw_bytes                                    |   2477           | bytes   |
| column:stats.avg_wait_us         | raw_bytes                                    |   2477           | bytes   |
| column:stats.cl_active           | raw_bytes                                    |   2460           | bytes   |
| column:stats.sv_active           | raw_bytes                                    |   2066           | bytes   |
| column:card.last4                | raw_bytes                                    |   1924           | bytes   |
| column:stats.sv_idle             | raw_bytes                                    |   1731           | bytes   |
| column:pid                       | raw_bytes                                    |   1714           | bytes   |
| column:@version                  | raw_bytes                                    |   1596           | bytes   |
| column:query.filters.price_max   | raw_bytes                                    |   1452           | bytes   |
| column:stats.cl_waiting          | raw_bytes                                    |   1287           | bytes   |
| column:rules_hit                 | raw_bytes                                    |   1221           | bytes   |
| column:error.stack_trace         | raw_bytes                                    |   1215           | bytes   |
| column:customer.country          | raw_bytes                                    |    962           | bytes   |
| column:score                     | raw_bytes                                    |    883           | bytes   |
| column:v                         | raw_bytes                                    |    857           | bytes   |
| column:job.args.format           | raw_bytes                                    |    704           | bytes   |
| column:three_ds                  | raw_bytes                                    |    580           | bytes   |
| column:backoff_ms                | raw_bytes                                    |    544           | bytes   |
| column:query.filters.in_stock    | raw_bytes                                    |    528           | bytes   |
| column:threshold_ms              | raw_bytes                                    |    513           | bytes   |
| column:error.class               | raw_bytes                                    |    363           | bytes   |
| column:attempt                   | raw_bytes                                    |    332           | bytes   |
| column:decline_code              | raw_bytes                                    |    328           | bytes   |
| column:mfa.method                | raw_bytes                                    |    322           | bytes   |
| column:signup.source             | raw_bytes                                    |    235           | bytes   |
| column:sqlstate                  | raw_bytes                                    |    205           | bytes   |
| column:err.message               | raw_bytes                                    |    175           | bytes   |
| column:locked_for_s              | raw_bytes                                    |    141           | bytes   |
| column:config.version            | raw_bytes                                    |    114           | bytes   |
| column:signup.referral           | raw_bytes                                    |    110           | bytes   |
| column:err.name                  | raw_bytes                                    |    110           | bytes   |
| column:keys                      | raw_bytes                                    |     74           | bytes   |
| column:go.version                | raw_bytes                                    |     48           | bytes   |
| column:failed_attempts           | raw_bytes                                    |     47           | bytes   |
| column:signal                    | raw_bytes                                    |     42           | bytes   |
| column:listen.address            | raw_bytes                                    |     30           | bytes   |
| column:keys.active               | raw_bytes                                    |     24           | bytes   |
| column:keys.retired              | raw_bytes                                    |     24           | bytes   |
| column:err.code                  | raw_bytes                                    |     24           | bytes   |
| column:drain_ms                  | raw_bytes                                    |     24           | bytes   |
| column:retry_in_ms               | raw_bytes                                    |     10           | bytes   |

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
| flat30                         | days spanned by the widest Datetime column   |  44760           | count   |
| flat30                         | templates_with_ts                            |    722           | count   |
| flat30                         | templates_no_time_column                     |      0           | count   |
| flat30                         | templates_time_not_named_ts                  |    148           | count   |
| flat30                         | unskippable_templates                        |    148           | count   |
| flat30                         | unskippable_rows                             |  83250           | count   |
| flat30                         | unskippable_row_share                        |     41.62        | percent |
| flat30                         | unskippable_bytes                            |      1.81505e+07 | count   |
| flat30                         | unskippable_byte_share                       |     38.96        | percent |
| flat30                         | templates_with_time_column (list rule)       |    870           | count   |
| flat30                         | templates_without (list rule)                |      0           | count   |
| flat30                         | rows_on_arrival_time (list rule)             |      0           | count   |
| flat30                         | rows_on_arrival_time_share (list rule)       |      0           | percent |
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

### `01_profile_logs` / dataset `messy`

| variant            | metric                                       |            value | unit    |
|--------------------|----------------------------------------------|------------------|---------|
| messy              | rows                                         | 200000           | count   |
| messy              | raw bytes                                    |      6.29598e+07 | count   |
| messy              | templates                                    |    130           | count   |
| messy              | rows/template min                            |     93           | count   |
| messy              | rows/template median                         |   1116           | count   |
| messy              | rows/template p90                            |   4134           | count   |
| messy              | rows/template max                            |   4873           | count   |
| messy              | top-10 share of rows %                       |     22.1         | count   |
| messy              | templates with <100 rows                     |      2           | count   |
| messy              | distinct promoted-value sets                 |     19           | count   |
| messy              | templates in the biggest promoted group      |      8           | count   |
| messy              | promoted groups holding one template         |      0           | count   |
| messy              | distinct column names                        |     16           | count   |
| messy              | days spanned by `ts`                         |      1           | count   |
| messy              | days spanned by every Datetime column pooled |  49780           | count   |
| messy              | days spanned by the widest Datetime column   |  49780           | count   |
| messy              | templates_with_ts                            |    130           | count   |
| messy              | templates_no_time_column                     |      0           | count   |
| messy              | templates_time_not_named_ts                  |      0           | count   |
| messy              | unskippable_templates                        |      0           | count   |
| messy              | unskippable_rows                             |      0           | count   |
| messy              | unskippable_row_share                        |      0           | percent |
| messy              | unskippable_bytes                            |      0           | count   |
| messy              | unskippable_byte_share                       |      0           | percent |
| messy              | templates_with_time_column (list rule)       |    130           | count   |
| messy              | templates_without (list rule)                |      0           | count   |
| messy              | rows_on_arrival_time (list rule)             |      0           | count   |
| messy              | rows_on_arrival_time_share (list rule)       |      0           | percent |
| column:trace_id    | raw_bytes                                    |      7.14388e+06 | bytes   |
| column:ts          | raw_bytes                                    |      3.90098e+06 | bytes   |
| column:tags        | raw_bytes                                    |      3.53097e+06 | bytes   |
| column:client.ip   | raw_bytes                                    |      2.64023e+06 | bytes   |
| column:event       | raw_bytes                                    |      1.96629e+06 | bytes   |
| column:region      | raw_bytes                                    |      1.8668e+06  | bytes   |
| column:duration_ms | raw_bytes                                    |      1.32458e+06 | bytes   |
| column:service     | raw_bytes                                    |      1.27515e+06 | bytes   |
| column:user_id     | raw_bytes                                    |      1.18259e+06 | bytes   |
| column:bytes_sent  | raw_bytes                                    | 990016           | bytes   |
| column:retry       | raw_bytes                                    | 958507           | bytes   |
| column:level       | raw_bytes                                    | 866413           | bytes   |
| column:order_id    | raw_bytes                                    | 777950           | bytes   |
| column:http.method | raw_bytes                                    | 358628           | bytes   |
| column:http.status | raw_bytes                                    | 268041           | bytes   |
| column:error.code  | raw_bytes                                    | 145101           | bytes   |

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
| oneday                         | days spanned by the widest Datetime column   |  44760           | count   |
| oneday                         | templates_with_ts                            |    722           | count   |
| oneday                         | templates_no_time_column                     |      0           | count   |
| oneday                         | templates_time_not_named_ts                  |    148           | count   |
| oneday                         | unskippable_templates                        |    148           | count   |
| oneday                         | unskippable_rows                             |  83250           | count   |
| oneday                         | unskippable_row_share                        |     41.62        | percent |
| oneday                         | unskippable_bytes                            |      1.81505e+07 | count   |
| oneday                         | unskippable_byte_share                       |     38.96        | percent |
| oneday                         | templates_with_time_column (list rule)       |    870           | count   |
| oneday                         | templates_without (list rule)                |      0           | count   |
| oneday                         | rows_on_arrival_time (list rule)             |      0           | count   |
| oneday                         | rows_on_arrival_time_share (list rule)       |      0           | percent |
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
| uneven                         | days spanned by the widest Datetime column   |  14848           | count   |
| uneven                         | templates_with_ts                            |   4159           | count   |
| uneven                         | templates_no_time_column                     |      0           | count   |
| uneven                         | templates_time_not_named_ts                  |    772           | count   |
| uneven                         | unskippable_templates                        |    772           | count   |
| uneven                         | unskippable_rows                             |  17855           | count   |
| uneven                         | unskippable_row_share                        |      8.928       | percent |
| uneven                         | unskippable_bytes                            |      3.96502e+06 | count   |
| uneven                         | unskippable_byte_share                       |      8.858       | percent |
| uneven                         | templates_with_time_column (list rule)       |   4931           | count   |
| uneven                         | templates_without (list rule)                |      0           | count   |
| uneven                         | rows_on_arrival_time (list rule)             |      0           | count   |
| uneven                         | rows_on_arrival_time_share (list rule)       |      0           | percent |
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

- kibbr gen-logs --profile farm --lines 200,000 --seed 42, unmodified; one simulated day in time order, ten services, each with its own time field (@timestamp, time, ts, ts_ms), a request's id recurring across services, 0.5% dirty values on five named keys
- a template whose time column is not named `ts` has no row-group min/max to skip on: its rows land in `undated/` and a time-range search reads every one
- type=String distinct=521 distinct/rows=0.0060 avg_len=115.1 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=521 distinct/rows=0.0100 avg_len=100.7 null_wide=73.90% null_json=0.00% templates=24
- type=Datetime distinct=56629 distinct/rows=0.5300 avg_len=25.3 null_wide=46.58% null_json=0.00% templates=21
- type=String distinct=63285 distinct/rows=0.7717 avg_len=32.0 null_wide=59.00% null_json=0.00% templates=45
- type=String distinct=1325 distinct/rows=0.0153 avg_len=24.8 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=17146 distinct/rows=0.1979 avg_len=23.8 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=52198 distinct/rows=0.9998 avg_len=32.0 null_wide=73.90% null_json=0.00% templates=24
- type=Datetime distinct=64348 distinct/rows=0.9996 avg_len=24.1 null_wide=67.81% null_json=0.00% templates=68
- type=String distinct=15 distinct/rows=0.0002 avg_len=24.5 null_wide=68.61% null_json=0.00% templates=58
- type=String distinct=1538 distinct/rows=0.0216 avg_len=19.9 null_wide=64.41% null_json=0.00% templates=48
- type=Float64 distinct=86252 distinct/rows=0.9957 avg_len=14.0 null_wide=56.69% null_json=0.00% templates=2
- type=Ip distinct=5645 distinct/rows=0.0652 avg_len=12.6 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=9945 distinct/rows=0.1905 avg_len=20.0 null_wide=73.90% null_json=0.00% templates=24
- type=String distinct=17 distinct/rows=0.0011 avg_len=67.0 null_wide=92.54% null_json=0.00% templates=8
- type=String distinct=17 distinct/rows=0.0003 avg_len=15.5 null_wide=73.90% null_json=0.00% templates=24
- type=String distinct=42 distinct/rows=0.0005 avg_len=8.7 null_wide=54.28% null_json=0.00% templates=123
- type=String distinct=8 distinct/rows=0.0001 avg_len=7.1 null_wide=53.45% null_json=0.00% templates=21
- type=Ip distinct=5586 distinct/rows=0.1070 avg_len=12.3 null_wide=73.90% null_json=0.00% templates=24
- type=String distinct=15 distinct/rows=0.0004 avg_len=14.3 null_wide=79.14% null_json=0.00% templates=17
- type=String distinct=59356 distinct/rows=0.7853 avg_len=7.5 null_wide=62.21% null_json=0.00% templates=51
- type=Uuid distinct=7367 distinct/rows=0.4865 avg_len=36.0 null_wide=92.43% null_json=0.00% templates=38
- type=String distinct=2 distinct/rows=0.0000 avg_len=8.0 null_wide=71.11% null_json=0.00% templates=1
- type=String distinct=18 distinct/rows=0.0003 avg_len=7.8 null_wide=71.11% null_json=0.00% templates=1
- type=String distinct=4 distinct/rows=0.0002 avg_len=23.0 null_wide=90.28% null_json=0.00% templates=39
- type=Float64 distinct=1485 distinct/rows=0.0171 avg_len=5.0 null_wide=56.69% null_json=0.00% templates=2
- type=Float64 distinct=41725 distinct/rows=0.9999 avg_len=9.2 null_wide=79.14% null_json=0.00% templates=17
- type=String distinct=1 distinct/rows=0.0000 avg_len=7.0 null_wide=73.88% null_json=0.00% templates=28
- type=String distinct=23 distinct/rows=0.0008 avg_len=12.2 null_wide=85.43% null_json=0.00% templates=62
- type=Int64 distinct=17056 distinct/rows=0.1969 avg_len=4.0 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=2 distinct/rows=0.0000 avg_len=6.0 null_wide=73.88% null_json=0.00% templates=28
- type=String distinct=2 distinct/rows=0.0000 avg_len=3.1 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=9 distinct/rows=0.0002 avg_len=6.3 null_wide=79.14% null_json=0.00% templates=17
- type=String distinct=1215 distinct/rows=0.0140 avg_len=3.0 null_wide=56.69% null_json=0.00% templates=2
- type=String distinct=1 distinct/rows=0.0000 avg_len=5.0 null_wide=73.88% null_json=0.00% templates=28
- type=Int64 distinct=13 distinct/rows=0.0002 avg_len=3.0 null_wide=56.69% null_json=0.00% templates=2
- type=Datetime distinct=19433 distinct/rows=0.9996 avg_len=13.0 null_wide=90.28% null_json=0.00% templates=39
- type=String distinct=4 distinct/rows=0.0002 avg_len=11.4 null_wide=89.46% null_json=0.00% templates=12
- type=Datetime distinct=9342 distinct/rows=1.0000 avg_len=25.2 null_wide=95.33% null_json=0.00% templates=15
- type=String distinct=17 distinct/rows=0.0011 avg_len=14.4 null_wide=92.43% null_json=0.00% templates=38
- type=String distinct=3 distinct/rows=0.0001 avg_len=4.0 null_wide=73.88% null_json=0.00% templates=28
- type=Int64 distinct=12182 distinct/rows=0.2333 avg_len=3.5 null_wide=73.90% null_json=0.00% templates=24
- type=String distinct=8 distinct/rows=0.0005 avg_len=11.0 null_wide=91.92% null_json=0.00% templates=43
- type=String distinct=2 distinct/rows=0.0000 avg_len=3.1 null_wide=73.90% null_json=0.00% templates=24
- type=Int64 distinct=3404 distinct/rows=0.1269 avg_len=6.0 null_wide=86.59% null_json=0.00% templates=10
- type=Int64 distinct=11 distinct/rows=0.0002 avg_len=3.0 null_wide=73.90% null_json=0.00% templates=24
- type=Ip distinct=14 distinct/rows=0.0009 avg_len=9.3 null_wide=91.94% null_json=0.00% templates=9
- type=String distinct=797 distinct/rows=0.0469 avg_len=8.4 null_wide=91.51% null_json=0.00% templates=15
- type=String distinct=8 distinct/rows=0.0004 avg_len=6.7 null_wide=90.28% null_json=0.00% templates=39
- type=String distinct=2 distinct/rows=0.0001 avg_len=7.0 null_wide=91.94% null_json=0.00% templates=9
- type=String distinct=6 distinct/rows=0.0011 avg_len=19.4 null_wide=97.19% null_json=0.00% templates=13
- type=String distinct=5 distinct/rows=0.0011 avg_len=22.5 null_wide=97.74% null_json=0.00% templates=4
- type=String distinct=11 distinct/rows=0.0005 avg_len=4.1 null_wide=87.83% null_json=0.00% templates=64
- type=Uuid distinct=2388 distinct/rows=1.0000 avg_len=36.0 null_wide=98.81% null_json=0.00% templates=1
- type=String distinct=535 distinct/rows=0.2099 avg_len=32.0 null_wide=98.73% null_json=0.00% templates=13
- type=String distinct=1488 distinct/rows=0.2634 avg_len=14.3 null_wide=97.18% null_json=0.00% templates=28
- type=String distinct=4 distinct/rows=0.0002 avg_len=4.0 null_wide=90.28% null_json=0.00% templates=39
- type=Ip distinct=2906 distinct/rows=0.5263 avg_len=12.8 null_wide=97.24% null_json=0.00% templates=11
- type=String distinct=1 distinct/rows=0.0002 avg_len=11.0 null_wide=96.85% null_json=0.00% templates=29
- type=String distinct=9 distinct/rows=0.0043 avg_len=31.7 null_wide=98.95% null_json=0.00% templates=21
- type=String distinct=1 distinct/rows=0.0001 avg_len=4.0 null_wide=91.94% null_json=0.00% templates=9
- type=String distinct=989 distinct/rows=0.4903 avg_len=30.5 null_wide=98.99% null_json=0.00% templates=4
- type=String distinct=1012 distinct/rows=0.5057 avg_len=30.5 null_wide=99.00% null_json=0.00% templates=3
- type=String distinct=3 distinct/rows=0.0007 avg_len=13.3 null_wide=97.74% null_json=0.00% templates=4
- type=String distinct=3124 distinct/rows=0.3466 avg_len=6.0 null_wide=95.49% null_json=0.00% templates=24
- type=Float64 distinct=2 distinct/rows=0.0001 avg_len=3.0 null_wide=91.16% null_json=0.00% templates=11
- type=String distinct=3 distinct/rows=0.0002 avg_len=3.0 null_wide=91.32% null_json=0.00% templates=10
- type=String distinct=5 distinct/rows=0.0031 avg_len=31.9 null_wide=99.20% null_json=0.00% templates=10
- type=String distinct=507 distinct/rows=0.3487 avg_len=32.0 null_wide=99.27% null_json=0.00% templates=9
- type=String distinct=1203 distinct/rows=0.3258 avg_len=11.8 null_wide=98.15% null_json=0.00% templates=2
- type=String distinct=3 distinct/rows=0.0019 avg_len=25.0 null_wide=99.20% null_json=0.00% templates=10
- type=String distinct=1726 distinct/rows=0.6926 avg_len=12.5 null_wide=98.75% null_json=0.00% templates=1
- type=String distinct=159 distinct/rows=0.0996 avg_len=18.7 null_wide=99.20% null_json=0.00% templates=10
- type=String distinct=7 distinct/rows=0.0035 avg_len=14.0 null_wide=99.00% null_json=0.00% templates=3
- type=String distinct=7 distinct/rows=0.0033 avg_len=11.6 null_wide=98.95% null_json=0.00% templates=19
- type=String distinct=964 distinct/rows=0.6630 avg_len=16.0 null_wide=99.27% null_json=0.00% templates=9
- type=String distinct=1 distinct/rows=0.0005 avg_len=11.0 null_wide=99.00% null_json=0.00% templates=3
- type=String distinct=7 distinct/rows=0.0035 avg_len=10.9 null_wide=99.00% null_json=0.00% templates=3
- type=String distinct=4 distinct/rows=0.0023 avg_len=12.6 null_wide=99.13% null_json=0.00% templates=10
- type=String distinct=2 distinct/rows=0.0005 avg_len=5.0 null_wide=97.88% null_json=0.00% templates=1
- type=String distinct=2 distinct/rows=0.0009 avg_len=9.0 null_wide=98.85% null_json=0.00% templates=2
- type=Datetime distinct=439 distinct/rows=0.4439 avg_len=20.0 null_wide=99.51% null_json=0.00% templates=1
- type=Datetime distinct=474 distinct/rows=0.2505 avg_len=10.0 null_wide=99.05% null_json=0.00% templates=8
- type=String distinct=3 distinct/rows=0.0013 avg_len=7.7 null_wide=98.81% null_json=0.00% templates=1
- type=Datetime distinct=1768 distinct/rows=0.9839 avg_len=10.0 null_wide=99.10% null_json=0.00% templates=1
- type=Int64 distinct=2201 distinct/rows=0.3896 avg_len=3.2 null_wide=97.18% null_json=0.00% templates=28
- type=Int64 distinct=248 distinct/rows=0.0235 avg_len=1.7 null_wide=94.73% null_json=0.00% templates=30
- type=Int64 distinct=1372 distinct/rows=0.4933 avg_len=6.0 null_wide=98.61% null_json=0.00% templates=4
- type=Int64 distinct=3 distinct/rows=0.0002 avg_len=1.0 null_wide=92.43% null_json=0.00% templates=38
- type=Int64 distinct=968 distinct/rows=0.3280 avg_len=5.0 null_wide=98.52% null_json=0.00% templates=8
- type=Datetime distinct=507 distinct/rows=0.3487 avg_len=10.0 null_wide=99.27% null_json=0.00% templates=9
- type=String distinct=474 distinct/rows=1.0000 avg_len=27.0 null_wide=99.76% null_json=0.00% templates=2
- type=Int64 distinct=542 distinct/rows=0.1261 avg_len=2.9 null_wide=97.85% null_json=0.00% templates=1
- type=Bool distinct=2 distinct/rows=0.0008 avg_len=4.8 null_wide=98.81% null_json=0.00% templates=1
- type=String distinct=7 distinct/rows=0.0082 avg_len=12.5 null_wide=99.57% null_json=0.00% templates=6
- type=String distinct=1 distinct/rows=0.0004 avg_len=4.0 null_wide=98.75% null_json=0.00% templates=1
- type=Int64 distinct=3 distinct/rows=0.0011 avg_len=3.2 null_wide=98.61% null_json=0.00% templates=4
- type=String distinct=2 distinct/rows=0.0008 avg_len=3.5 null_wide=98.75% null_json=0.00% templates=1
- type=Datetime distinct=847 distinct/rows=1.0000 avg_len=10.0 null_wide=99.58% null_json=0.00% templates=2
- type=Int64 distinct=1229 distinct/rows=0.9992 avg_len=6.7 null_wide=99.39% null_json=0.00% templates=1
- type=String distinct=12 distinct/rows=0.0082 avg_len=5.5 null_wide=99.27% null_json=0.00% templates=16
- type=Int64 distinct=4 distinct/rows=0.0025 avg_len=5.0 null_wide=99.20% null_json=0.00% templates=10
- type=Uuid distinct=103 distinct/rows=0.4791 avg_len=36.0 null_wide=99.89% null_json=0.00% templates=4
- type=Int64 distinct=73 distinct/rows=0.0170 avg_len=1.8 null_wide=97.85% null_json=0.00% templates=1
- type=Int64 distinct=790 distinct/rows=0.3170 avg_len=3.0 null_wide=98.75% null_json=0.00% templates=1
- type=Int64 distinct=1229 distinct/rows=0.9992 avg_len=5.9 null_wide=99.39% null_json=0.00% templates=1
- type=String distinct=116 distinct/rows=0.0180 avg_len=1.1 null_wide=96.77% null_json=0.00% templates=3
- type=String distinct=1 distinct/rows=0.0012 avg_len=8.0 null_wide=99.57% null_json=0.00% templates=6
- type=String distinct=11 distinct/rows=0.0232 avg_len=13.9 null_wide=99.76% null_json=0.00% templates=5
- type=String distinct=295 distinct/rows=0.6250 avg_len=14.0 null_wide=99.76% null_json=0.00% templates=10
- type=String distinct=3 distinct/rows=0.0018 avg_len=4.0 null_wide=99.18% null_json=0.00% templates=4
- type=Ip distinct=446 distinct/rows=0.9102 avg_len=12.8 null_wide=99.75% null_json=0.00% templates=2
- type=String distinct=2 distinct/rows=0.0019 avg_len=5.7 null_wide=99.46% null_json=0.00% templates=6
- type=Int64 distinct=22 distinct/rows=0.0051 avg_len=1.4 null_wide=97.85% null_json=0.00% templates=2
- type=Int64 distinct=1187 distinct/rows=0.9892 avg_len=5.0 null_wide=99.40% null_json=0.00% templates=1
- type=String distinct=8 distinct/rows=0.0452 avg_len=33.1 null_wide=99.91% null_json=0.00% templates=10
- type=Int64 distinct=422 distinct/rows=0.4346 avg_len=6.0 null_wide=99.51% null_json=0.00% templates=5
- type=Int64 distinct=5 distinct/rows=0.0009 avg_len=1.0 null_wide=97.18% null_json=0.00% templates=28
- type=Int64 distinct=656 distinct/rows=0.3447 avg_len=2.9 null_wide=99.05% null_json=0.00% templates=6
- type=String distinct=2 distinct/rows=0.0023 avg_len=6.4 null_wide=99.57% null_json=0.00% templates=6
- type=Int64 distinct=1139 distinct/rows=0.9260 avg_len=4.4 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=1128 distinct/rows=0.9171 avg_len=4.3 null_wide=99.39% null_json=0.00% templates=1
- type=String distinct=398 distinct/rows=0.4168 avg_len=5.3 null_wide=99.52% null_json=0.00% templates=5
- type=Int64 distinct=522 distinct/rows=0.4244 avg_len=4.0 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=383 distinct/rows=0.4961 avg_len=6.2 null_wide=99.61% null_json=0.00% templates=6
- type=String distinct=2 distinct/rows=0.2222 avg_len=489.1 null_wide=100.00% null_json=0.00% templates=2
- type=String distinct=1 distinct/rows=0.0012 avg_len=5.0 null_wide=99.57% null_json=0.00% templates=6
- type=Int64 distinct=1 distinct/rows=0.0002 avg_len=1.0 null_wide=97.88% null_json=0.00% templates=1
- type=Int64 distinct=511 distinct/rows=0.4154 avg_len=3.0 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=548 distinct/rows=0.4455 avg_len=3.0 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=1 distinct/rows=0.0008 avg_len=3.0 null_wide=99.40% null_json=0.00% templates=1
- type=Int64 distinct=267 distinct/rows=0.2171 avg_len=2.8 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=61 distinct/rows=0.0496 avg_len=2.7 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=495 distinct/rows=0.7615 avg_len=5.0 null_wide=99.67% null_json=0.00% templates=1
- type=Int64 distinct=287 distinct/rows=0.1423 avg_len=1.5 null_wide=98.99% null_json=0.00% templates=4
- type=Int64 distinct=292 distinct/rows=0.1459 avg_len=1.5 null_wide=99.00% null_json=0.00% templates=3
- type=String distinct=6 distinct/rows=0.0063 avg_len=3.0 null_wide=99.52% null_json=0.00% templates=5
- type=String distinct=4 distinct/rows=0.0083 avg_len=5.9 null_wide=99.76% null_json=0.00% templates=3
- type=String distinct=3 distinct/rows=0.0061 avg_len=5.1 null_wide=99.75% null_json=0.00% templates=2
- type=Int64 distinct=113 distinct/rows=0.0919 avg_len=2.0 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=41 distinct/rows=0.0333 avg_len=2.0 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=16 distinct/rows=0.0130 avg_len=1.7 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=405 distinct/rows=0.8420 avg_len=4.0 null_wide=99.76% null_json=0.00% templates=3
- type=Int64 distinct=15 distinct/rows=0.0122 avg_len=1.4 null_wide=99.39% null_json=0.00% templates=1
- type=Int64 distinct=2 distinct/rows=0.0023 avg_len=2.0 null_wide=99.57% null_json=0.00% templates=6
- type=Int64 distinct=1 distinct/rows=0.0006 avg_len=1.0 null_wide=99.20% null_json=0.00% templates=10
- type=Int64 distinct=20 distinct/rows=0.0391 avg_len=2.8 null_wide=99.74% null_json=0.00% templates=11
- type=Int64 distinct=33 distinct/rows=0.0268 avg_len=1.0 null_wide=99.39% null_json=0.00% templates=1
- type=String distinct=2 distinct/rows=0.0274 avg_len=16.7 null_wide=99.96% null_json=0.00% templates=1
- type=String distinct=1 distinct/rows=0.3333 avg_len=405.0 null_wide=100.00% null_json=0.00% templates=3
- type=String distinct=9 distinct/rows=0.0187 avg_len=2.0 null_wide=99.76% null_json=0.00% templates=3
- type=Int64 distinct=93 distinct/rows=0.1898 avg_len=1.8 null_wide=99.75% null_json=0.00% templates=2
- type=Int64 distinct=1 distinct/rows=0.0012 avg_len=1.0 null_wide=99.57% null_json=0.00% templates=6
- type=String distinct=2 distinct/rows=0.0093 avg_len=3.3 null_wide=99.89% null_json=0.00% templates=4
- type=Bool distinct=1 distinct/rows=0.0069 avg_len=4.0 null_wide=99.93% null_json=0.00% templates=1
- type=Int64 distinct=126 distinct/rows=0.9265 avg_len=4.0 null_wide=99.93% null_json=0.00% templates=9
- type=Bool distinct=1 distinct/rows=0.0076 avg_len=4.0 null_wide=99.93% null_json=0.00% templates=5
- type=Int64 distinct=1 distinct/rows=0.0058 avg_len=3.0 null_wide=99.91% null_json=0.00% templates=11
- type=String distinct=2 distinct/rows=0.2222 avg_len=40.3 null_wide=100.00% null_json=0.00% templates=2
- type=Int64 distinct=5 distinct/rows=0.0151 avg_len=1.0 null_wide=99.83% null_json=0.00% templates=2
- type=String distinct=5 distinct/rows=0.2083 avg_len=13.7 null_wide=99.99% null_json=0.00% templates=1
- type=String distinct=2 distinct/rows=0.0233 avg_len=3.7 null_wide=99.96% null_json=0.00% templates=1
- type=String distinct=3 distinct/rows=0.0492 avg_len=3.9 null_wide=99.97% null_json=0.00% templates=2
- type=String distinct=2 distinct/rows=0.0488 avg_len=5.0 null_wide=99.98% null_json=0.00% templates=1
- type=String distinct=2 distinct/rows=0.2500 avg_len=21.9 null_wide=100.00% null_json=0.00% templates=2
- type=Int64 distinct=1 distinct/rows=0.0213 avg_len=3.0 null_wide=99.98% null_json=0.00% templates=1
- type=String distinct=1 distinct/rows=0.1667 avg_len=19.0 null_wide=100.00% null_json=0.00% templates=1
- type=String distinct=11 distinct/rows=1.0000 avg_len=10.0 null_wide=99.99% null_json=0.00% templates=1
- type=String distinct=2 distinct/rows=0.2500 avg_len=13.8 null_wide=100.00% null_json=0.00% templates=2
- type=Int64 distinct=1 distinct/rows=0.0135 avg_len=1.0 null_wide=99.96% null_json=0.00% templates=1
- type=String distinct=1 distinct/rows=0.1667 avg_len=8.0 null_wide=100.00% null_json=0.00% templates=1
- type=Int64 distinct=1 distinct/rows=0.0213 avg_len=1.0 null_wide=99.98% null_json=0.00% templates=1
- type=String distinct=1 distinct/rows=0.1667 avg_len=7.0 null_wide=100.00% null_json=0.00% templates=1
- type=String distinct=1 distinct/rows=0.1667 avg_len=5.0 null_wide=100.00% null_json=0.00% templates=1
- type=Int64 distinct=1 distinct/rows=0.0417 avg_len=1.0 null_wide=99.99% null_json=0.00% templates=1
- type=Int64 distinct=2 distinct/rows=0.2500 avg_len=3.0 null_wide=100.00% null_json=0.00% templates=2
- type=Int64 distinct=6 distinct/rows=1.0000 avg_len=4.0 null_wide=100.00% null_json=0.00% templates=1
- type=Int64 distinct=2 distinct/rows=1.0000 avg_len=5.0 null_wide=100.00% null_json=0.00% templates=2
- kibbr gen-logs --lines 200000 --templates 12 --seed 42, unmodified; 31 days, flat across templates
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
- HAND-AUTHORED BY THE LAB, NOT REAL LOGS. 200,000 lines over one day, 8 services and 20 event types, written to carry the type inconsistency that `gen-logs` never produces - across flat30, oneday, uneven and testdata, zero keys carry two types, so every `columns widened to String` figure was vacuous. Injected fault rates, as realised: duration_ms_na 1.20% (2,400 lines); duration_ms_null 0.38% (766 lines); user_id_legacy 1.98% (3,955 lines); order_id_alpha 0.39% (785 lines); http_status_empty 0.23% (461 lines); retry_stringy 1.49% (2,975 lines); bytes_sent_float 3.05% (6,108 lines); error_code_symbol 0.57% (1,147 lines); tags_scalar 1.00% (1,994 lines); ts_epoch 4.95% (9,902 lines); trace_id_empty 0.78% (1,559 lines); client_ip_unknown 0.57% (1,134 lines). Every number derived from this dataset is a statement about these rates and about nothing else.
- type=String distinct=198442 distinct/rows=0.9922 avg_len=35.7 null_wide=0.00% null_json=0.00% templates=130
- type=Datetime distinct=86235 distinct/rows=0.4312 avg_len=19.5 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=88 distinct/rows=0.0004 avg_len=17.7 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=198861 distinct/rows=0.9943 avg_len=13.2 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=19 distinct/rows=0.0001 avg_len=9.8 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=3 distinct/rows=0.0000 avg_len=9.3 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=162736 distinct/rows=0.8137 avg_len=6.6 null_wide=0.00% null_json=0.38% templates=130
- type=String distinct=8 distinct/rows=0.0000 avg_len=6.4 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=181704 distinct/rows=0.9085 avg_len=5.9 null_wide=0.00% null_json=0.00% templates=130
- type=Float64 distinct=91765 distinct/rows=0.4588 avg_len=5.0 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=4 distinct/rows=0.0000 avg_len=4.8 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=4 distinct/rows=0.0000 avg_len=4.3 null_wide=0.00% null_json=0.00% templates=130
- type=String distinct=77948 distinct/rows=0.9999 avg_len=10.0 null_wide=61.02% null_json=0.00% templates=70
- type=String distinct=4 distinct/rows=0.0000 avg_len=4.0 null_wide=55.10% null_json=0.00% templates=65
- type=String distinct=6 distinct/rows=0.0001 avg_len=3.0 null_wide=55.10% null_json=0.00% templates=65
- type=String distinct=203 distinct/rows=0.0044 avg_len=3.1 null_wide=76.76% null_json=0.00% templates=70
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

Run 2026-09-22T15:55:26+00:00, git `90152d2`, 621 rows.

### `02_split_vs_wide` / dataset `farm`

| variant                          | metric                      |            value | unit   |
|----------------------------------|-----------------------------|------------------|--------|
| raw NDJSON                       | bytes                       |      9.7713e+07  | bytes  |
| raw NDJSON                       | disk_blocks                 |      9.77142e+07 | bytes  |
| raw NDJSON                       | files                       |      1           | count  |
| raw NDJSON                       | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                       | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                       | ratio_to_gzip9              |      8.905       | ratio  |
| gzip -9                          | bytes                       |      1.09724e+07 | bytes  |
| gzip -9                          | disk_blocks                 |      1.101e+07   | bytes  |
| gzip -9                          | files                       |      1           | count  |
| gzip -9                          | ratio_to_raw_bytes          |      0.1123      | ratio  |
| gzip -9                          | ratio_to_raw_blocks         |      0.1127      | ratio  |
| gzip -9                          | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                         | bytes                       |      5.99535e+06 | bytes  |
| zstd -19                         | disk_blocks                 |      6.31603e+06 | bytes  |
| zstd -19                         | files                       |      1           | count  |
| zstd -19                         | ratio_to_raw_bytes          |      0.06136     | ratio  |
| zstd -19                         | ratio_to_raw_blocks         |      0.06464     | ratio  |
| zstd -19                         | ratio_to_gzip9              |      0.5464      | ratio  |
| WIDE rg=1,000                    | bytes                       |      1.8409e+07  | bytes  |
| WIDE rg=1,000                    | disk_blocks                 |      1.84115e+07 | bytes  |
| WIDE rg=1,000                    | files                       |      1           | count  |
| WIDE rg=1,000                    | ratio_to_raw_bytes          |      0.1884      | ratio  |
| WIDE rg=1,000                    | ratio_to_raw_blocks         |      0.1884      | ratio  |
| WIDE rg=1,000                    | ratio_to_gzip9              |      1.678       | ratio  |
| WIDE rg=8,192                    | bytes                       |      9.95704e+06 | bytes  |
| WIDE rg=8,192                    | disk_blocks                 |      9.95738e+06 | bytes  |
| WIDE rg=8,192                    | files                       |      1           | count  |
| WIDE rg=8,192                    | ratio_to_raw_bytes          |      0.1019      | ratio  |
| WIDE rg=8,192                    | ratio_to_raw_blocks         |      0.1019      | ratio  |
| WIDE rg=8,192                    | ratio_to_gzip9              |      0.9075      | ratio  |
| WIDE rg=122,880                  | bytes                       |      8.74609e+06 | bytes  |
| WIDE rg=122,880                  | disk_blocks                 |      8.74906e+06 | bytes  |
| WIDE rg=122,880                  | files                       |      1           | count  |
| WIDE rg=122,880                  | ratio_to_raw_bytes          |      0.08951     | ratio  |
| WIDE rg=122,880                  | ratio_to_raw_blocks         |      0.08954     | ratio  |
| WIDE rg=122,880                  | ratio_to_gzip9              |      0.7971      | ratio  |
| PER_TEMPLATE rg=1,000            | bytes                       |      9.26977e+06 | bytes  |
| PER_TEMPLATE rg=1,000            | disk_blocks                 |      9.59693e+06 | bytes  |
| PER_TEMPLATE rg=1,000            | files                       |    143           | count  |
| PER_TEMPLATE rg=1,000            | ratio_to_raw_bytes          |      0.09487     | ratio  |
| PER_TEMPLATE rg=1,000            | ratio_to_raw_blocks         |      0.09822     | ratio  |
| PER_TEMPLATE rg=1,000            | ratio_to_gzip9              |      0.8448      | ratio  |
| PER_TEMPLATE rg=8,192            | bytes                       |      8.43898e+06 | bytes  |
| PER_TEMPLATE rg=8,192            | disk_blocks                 |      8.76544e+06 | bytes  |
| PER_TEMPLATE rg=8,192            | files                       |    143           | count  |
| PER_TEMPLATE rg=8,192            | ratio_to_raw_bytes          |      0.08636     | ratio  |
| PER_TEMPLATE rg=8,192            | ratio_to_raw_blocks         |      0.08971     | ratio  |
| PER_TEMPLATE rg=8,192            | ratio_to_gzip9              |      0.7691      | ratio  |
| PER_TEMPLATE rg=122,880          | bytes                       |      8.30124e+06 | bytes  |
| PER_TEMPLATE rg=122,880          | disk_blocks                 |      8.62618e+06 | bytes  |
| PER_TEMPLATE rg=122,880          | files                       |    143           | count  |
| PER_TEMPLATE rg=122,880          | ratio_to_raw_bytes          |      0.08496     | ratio  |
| PER_TEMPLATE rg=122,880          | ratio_to_raw_blocks         |      0.08828     | ratio  |
| PER_TEMPLATE rg=122,880          | ratio_to_gzip9              |      0.7566      | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | bytes                       |      8.43898e+06 | bytes  |
| PER_TEMPLATE_DAY rg=8,192        | disk_blocks                 |      8.76544e+06 | bytes  |
| PER_TEMPLATE_DAY rg=8,192        | files                       |    143           | count  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_raw_bytes          |      0.08636     | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_raw_blocks         |      0.08971     | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_gzip9              |      0.7691      | ratio  |
| SHARED_DAY rg=1,000              | bytes                       |      9.27322e+06 | bytes  |
| SHARED_DAY rg=1,000              | disk_blocks                 |      1.00475e+07 | bytes  |
| SHARED_DAY rg=1,000              | files                       |      1           | count  |
| SHARED_DAY rg=1,000              | ratio_to_raw_bytes          |      0.0949      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_raw_blocks         |      0.1028      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_gzip9              |      0.8451      | ratio  |
| SHARED_DAY rg=8,192              | bytes                       |      8.44243e+06 | bytes  |
| SHARED_DAY rg=8,192              | disk_blocks                 |      9.39213e+06 | bytes  |
| SHARED_DAY rg=8,192              | files                       |      1           | count  |
| SHARED_DAY rg=8,192              | ratio_to_raw_bytes          |      0.0864      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_raw_blocks         |      0.09612     | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_gzip9              |      0.7694      | ratio  |
| SHARED_DAY rg=122,880            | bytes                       |      8.3047e+06  | bytes  |
| SHARED_DAY rg=122,880            | disk_blocks                 |      9.26106e+06 | bytes  |
| SHARED_DAY rg=122,880            | files                       |      1           | count  |
| SHARED_DAY rg=122,880            | ratio_to_raw_bytes          |      0.08499     | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_raw_blocks         |      0.09478     | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_gzip9              |      0.7569      | ratio  |
| PER_TEMPLATE[PER_EVENT] rg=8,192 | bytes                       |      8.52326e+06 | bytes  |
| PER_TEMPLATE[PER_EVENT] rg=8,192 | disk_blocks                 |      8.5975e+06  | bytes  |
| PER_TEMPLATE[PER_EVENT] rg=8,192 | files                       |     43           | count  |
| PER_TEMPLATE[PER_EVENT] rg=8,192 | ratio_to_raw_bytes          |      0.08723     | ratio  |
| PER_TEMPLATE[PER_EVENT] rg=8,192 | ratio_to_raw_blocks         |      0.08799     | ratio  |
| PER_TEMPLATE[PER_EVENT] rg=8,192 | ratio_to_gzip9              |      0.7768      | ratio  |
| SHARED_DAY[PER_EVENT] rg=8,192   | bytes                       |      8.52431e+06 | bytes  |
| SHARED_DAY[PER_EVENT] rg=8,192   | disk_blocks                 |      9.3225e+06  | bytes  |
| SHARED_DAY[PER_EVENT] rg=8,192   | files                       |      1           | count  |
| SHARED_DAY[PER_EVENT] rg=8,192   | ratio_to_raw_bytes          |      0.08724     | ratio  |
| SHARED_DAY[PER_EVENT] rg=8,192   | ratio_to_raw_blocks         |      0.09541     | ratio  |
| SHARED_DAY[PER_EVENT] rg=8,192   | ratio_to_gzip9              |      0.7769      | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | bytes                       |      7.98845e+06 | bytes  |
| PER_TEMPLATE[J70] rg=8,192       | disk_blocks                 |      8.10598e+06 | bytes  |
| PER_TEMPLATE[J70] rg=8,192       | files                       |     60           | count  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_raw_bytes          |      0.08175     | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_raw_blocks         |      0.08296     | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_gzip9              |      0.728       | ratio  |
| SHARED_DAY[J70] rg=8,192         | bytes                       |      7.98991e+06 | bytes  |
| SHARED_DAY[J70] rg=8,192         | disk_blocks                 |      9.02758e+06 | bytes  |
| SHARED_DAY[J70] rg=8,192         | files                       |      1           | count  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_raw_bytes          |      0.08177     | ratio  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_raw_blocks         |      0.09239     | ratio  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_gzip9              |      0.7282      | ratio  |
| window rg=1000                   | rows                        |  57786           | count  |
| window rg=1000                   | row_groups                  |     58           | count  |
| window rg=1000                   | groups_touched              |      2           | count  |
| window rg=1000                   | file_bytes                  |      2.65148e+06 | bytes  |
| window rg=1000                   | bytes_read                  |  91951           | bytes  |
| window rg=1000                   | read_share_pct              |      3.468       | count  |
| window rg=1000                   | matching_rows               |    283           | count  |
| window rg=1000                   | rows_scanned                |   2000           | count  |
| window rg=8192                   | rows                        |  57786           | count  |
| window rg=8192                   | row_groups                  |      8           | count  |
| window rg=8192                   | groups_touched              |      1           | count  |
| window rg=8192                   | file_bytes                  |      2.36578e+06 | bytes  |
| window rg=8192                   | bytes_read                  | 332853           | bytes  |
| window rg=8192                   | read_share_pct              |     14.07        | count  |
| window rg=8192                   | matching_rows               |    283           | count  |
| window rg=8192                   | rows_scanned                |   8192           | count  |
| window rg=122880                 | rows                        |  57786           | count  |
| window rg=122880                 | row_groups                  |      1           | count  |
| window rg=122880                 | groups_touched              |      1           | count  |
| window rg=122880                 | file_bytes                  |      2.28763e+06 | bytes  |
| window rg=122880                 | bytes_read                  |      2.28763e+06 | bytes  |
| window rg=122880                 | read_share_pct              |    100           | count  |
| window rg=122880                 | matching_rows               |    283           | count  |
| window rg=122880                 | rows_scanned                |  57786           | count  |
| verdict                          | per_template_vs_gzip9_bytes |      0.7691      | ratio  |
| verdict                          | smallest_by_bytes           |      5.99535e+06 | bytes  |
| verdict                          | smallest_by_blocks          |      6.31603e+06 | bytes  |

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
| gzip -9                    | disk_blocks                 |      1.13377e+07 | bytes  |
| gzip -9                    | files                       |      1           | count  |
| gzip -9                    | ratio_to_raw_bytes          |      0.2314      | ratio  |
| gzip -9                    | ratio_to_raw_blocks         |      0.2423      | ratio  |
| gzip -9                    | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                   | bytes                       |      8.28201e+06 | bytes  |
| zstd -19                   | disk_blocks                 |      8.41728e+06 | bytes  |
| zstd -19                   | files                       |      1           | count  |
| zstd -19                   | ratio_to_raw_bytes          |      0.177       | ratio  |
| zstd -19                   | ratio_to_raw_blocks         |      0.1799      | ratio  |
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
| verdict                    | smallest_by_blocks          |      8.41728e+06 | bytes  |

### `02_split_vs_wide` / dataset `messy`

| variant                          | metric                      |            value | unit   |
|----------------------------------|-----------------------------|------------------|--------|
| raw NDJSON                       | bytes                       |      6.31598e+07 | bytes  |
| raw NDJSON                       | disk_blocks                 |      6.31603e+07 | bytes  |
| raw NDJSON                       | files                       |      1           | count  |
| raw NDJSON                       | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                       | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                       | ratio_to_gzip9              |      4.906       | ratio  |
| gzip -9                          | bytes                       |      1.28749e+07 | bytes  |
| gzip -9                          | disk_blocks                 |      1.33038e+07 | bytes  |
| gzip -9                          | files                       |      1           | count  |
| gzip -9                          | ratio_to_raw_bytes          |      0.2039      | ratio  |
| gzip -9                          | ratio_to_raw_blocks         |      0.2106      | ratio  |
| gzip -9                          | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                         | bytes                       |      1.02297e+07 | bytes  |
| zstd -19                         | disk_blocks                 |      1.05103e+07 | bytes  |
| zstd -19                         | files                       |      1           | count  |
| zstd -19                         | ratio_to_raw_bytes          |      0.162       | ratio  |
| zstd -19                         | ratio_to_raw_blocks         |      0.1664      | ratio  |
| zstd -19                         | ratio_to_gzip9              |      0.7945      | ratio  |
| WIDE rg=1,000                    | bytes                       |      1.23073e+07 | bytes  |
| WIDE rg=1,000                    | disk_blocks                 |      1.23085e+07 | bytes  |
| WIDE rg=1,000                    | files                       |      1           | count  |
| WIDE rg=1,000                    | ratio_to_raw_bytes          |      0.1949      | ratio  |
| WIDE rg=1,000                    | ratio_to_raw_blocks         |      0.1949      | ratio  |
| WIDE rg=1,000                    | ratio_to_gzip9              |      0.9559      | ratio  |
| WIDE rg=8,192                    | bytes                       |      1.20438e+07 | bytes  |
| WIDE rg=8,192                    | disk_blocks                 |      1.20463e+07 | bytes  |
| WIDE rg=8,192                    | files                       |      1           | count  |
| WIDE rg=8,192                    | ratio_to_raw_bytes          |      0.1907      | ratio  |
| WIDE rg=8,192                    | ratio_to_raw_blocks         |      0.1907      | ratio  |
| WIDE rg=8,192                    | ratio_to_gzip9              |      0.9355      | ratio  |
| WIDE rg=122,880                  | bytes                       |      1.1097e+07  | bytes  |
| WIDE rg=122,880                  | disk_blocks                 |      1.11002e+07 | bytes  |
| WIDE rg=122,880                  | files                       |      1           | count  |
| WIDE rg=122,880                  | ratio_to_raw_bytes          |      0.1757      | ratio  |
| WIDE rg=122,880                  | ratio_to_raw_blocks         |      0.1757      | ratio  |
| WIDE rg=122,880                  | ratio_to_gzip9              |      0.8619      | ratio  |
| PER_TEMPLATE rg=1,000            | bytes                       |      1.23036e+07 | bytes  |
| PER_TEMPLATE rg=1,000            | disk_blocks                 |      1.25583e+07 | bytes  |
| PER_TEMPLATE rg=1,000            | files                       |    130           | count  |
| PER_TEMPLATE rg=1,000            | ratio_to_raw_bytes          |      0.1948      | ratio  |
| PER_TEMPLATE rg=1,000            | ratio_to_raw_blocks         |      0.1988      | ratio  |
| PER_TEMPLATE rg=1,000            | ratio_to_gzip9              |      0.9556      | ratio  |
| PER_TEMPLATE rg=8,192            | bytes                       |      1.22314e+07 | bytes  |
| PER_TEMPLATE rg=8,192            | disk_blocks                 |      1.25051e+07 | bytes  |
| PER_TEMPLATE rg=8,192            | files                       |    130           | count  |
| PER_TEMPLATE rg=8,192            | ratio_to_raw_bytes          |      0.1937      | ratio  |
| PER_TEMPLATE rg=8,192            | ratio_to_raw_blocks         |      0.198       | ratio  |
| PER_TEMPLATE rg=8,192            | ratio_to_gzip9              |      0.95        | ratio  |
| PER_TEMPLATE rg=122,880          | bytes                       |      1.22314e+07 | bytes  |
| PER_TEMPLATE rg=122,880          | disk_blocks                 |      1.25051e+07 | bytes  |
| PER_TEMPLATE rg=122,880          | files                       |    130           | count  |
| PER_TEMPLATE rg=122,880          | ratio_to_raw_bytes          |      0.1937      | ratio  |
| PER_TEMPLATE rg=122,880          | ratio_to_raw_blocks         |      0.198       | ratio  |
| PER_TEMPLATE rg=122,880          | ratio_to_gzip9              |      0.95        | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | bytes                       |      1.22314e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192        | disk_blocks                 |      1.25051e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192        | files                       |    130           | count  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_raw_bytes          |      0.1937      | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_raw_blocks         |      0.198       | ratio  |
| PER_TEMPLATE_DAY rg=8,192        | ratio_to_gzip9              |      0.95        | ratio  |
| SHARED_DAY rg=1,000              | bytes                       |      1.23067e+07 | bytes  |
| SHARED_DAY rg=1,000              | disk_blocks                 |      1.25215e+07 | bytes  |
| SHARED_DAY rg=1,000              | files                       |      1           | count  |
| SHARED_DAY rg=1,000              | ratio_to_raw_bytes          |      0.1948      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_raw_blocks         |      0.1983      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_gzip9              |      0.9559      | ratio  |
| SHARED_DAY rg=8,192              | bytes                       |      1.22346e+07 | bytes  |
| SHARED_DAY rg=8,192              | disk_blocks                 |      1.31031e+07 | bytes  |
| SHARED_DAY rg=8,192              | files                       |      1           | count  |
| SHARED_DAY rg=8,192              | ratio_to_raw_bytes          |      0.1937      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_raw_blocks         |      0.2075      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_gzip9              |      0.9503      | ratio  |
| SHARED_DAY rg=122,880            | bytes                       |      1.22346e+07 | bytes  |
| SHARED_DAY rg=122,880            | disk_blocks                 |      1.23904e+07 | bytes  |
| SHARED_DAY rg=122,880            | files                       |      1           | count  |
| SHARED_DAY rg=122,880            | ratio_to_raw_bytes          |      0.1937      | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_raw_blocks         |      0.1962      | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_gzip9              |      0.9503      | ratio  |
| PER_TEMPLATE[J90] rg=8,192       | bytes                       |      1.20926e+07 | bytes  |
| PER_TEMPLATE[J90] rg=8,192       | disk_blocks                 |      1.22388e+07 | bytes  |
| PER_TEMPLATE[J90] rg=8,192       | files                       |     66           | count  |
| PER_TEMPLATE[J90] rg=8,192       | ratio_to_raw_bytes          |      0.1915      | ratio  |
| PER_TEMPLATE[J90] rg=8,192       | ratio_to_raw_blocks         |      0.1938      | ratio  |
| PER_TEMPLATE[J90] rg=8,192       | ratio_to_gzip9              |      0.9392      | ratio  |
| SHARED_DAY[J90] rg=8,192         | bytes                       |      1.20942e+07 | bytes  |
| SHARED_DAY[J90] rg=8,192         | disk_blocks                 |      1.28778e+07 | bytes  |
| SHARED_DAY[J90] rg=8,192         | files                       |      1           | count  |
| SHARED_DAY[J90] rg=8,192         | ratio_to_raw_bytes          |      0.1915      | ratio  |
| SHARED_DAY[J90] rg=8,192         | ratio_to_raw_blocks         |      0.2039      | ratio  |
| SHARED_DAY[J90] rg=8,192         | ratio_to_gzip9              |      0.9394      | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | bytes                       |      1.20004e+07 | bytes  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | disk_blocks                 |      1.20668e+07 | bytes  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | files                       |     33           | count  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_raw_bytes          |      0.19        | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_raw_blocks         |      0.191       | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_gzip9              |      0.9321      | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | bytes                       |      1.20013e+07 | bytes  |
| SHARED_DAY[J80_GROWN] rg=8,192   | disk_blocks                 |      1.2673e+07  | bytes  |
| SHARED_DAY[J80_GROWN] rg=8,192   | files                       |      1           | count  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_raw_bytes          |      0.19        | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_raw_blocks         |      0.2006      | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_gzip9              |      0.9321      | ratio  |
| window rg=1000                   | rows                        |   4873           | count  |
| window rg=1000                   | row_groups                  |      5           | count  |
| window rg=1000                   | groups_touched              |      1           | count  |
| window rg=1000                   | file_bytes                  | 264461           | bytes  |
| window rg=1000                   | bytes_read                  |  54226           | bytes  |
| window rg=1000                   | read_share_pct              |     20.5         | count  |
| window rg=1000                   | matching_rows               |     19           | count  |
| window rg=1000                   | rows_scanned                |   1000           | count  |
| window rg=8192                   | rows                        |   4873           | count  |
| window rg=8192                   | row_groups                  |      1           | count  |
| window rg=8192                   | groups_touched              |      1           | count  |
| window rg=8192                   | file_bytes                  | 271558           | bytes  |
| window rg=8192                   | bytes_read                  | 271558           | bytes  |
| window rg=8192                   | read_share_pct              |    100           | count  |
| window rg=8192                   | matching_rows               |     19           | count  |
| window rg=8192                   | rows_scanned                |   4873           | count  |
| window rg=122880                 | rows                        |   4873           | count  |
| window rg=122880                 | row_groups                  |      1           | count  |
| window rg=122880                 | groups_touched              |      1           | count  |
| window rg=122880                 | file_bytes                  | 271558           | bytes  |
| window rg=122880                 | bytes_read                  | 271558           | bytes  |
| window rg=122880                 | read_share_pct              |    100           | count  |
| window rg=122880                 | matching_rows               |     19           | count  |
| window rg=122880                 | rows_scanned                |   4873           | count  |
| verdict                          | per_template_vs_gzip9_bytes |      0.95        | ratio  |
| verdict                          | smallest_by_bytes           |      1.02297e+07 | bytes  |
| verdict                          | smallest_by_blocks          |      1.05103e+07 | bytes  |

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
| gzip -9                   | disk_blocks                 |      1.05513e+07 | bytes  |
| gzip -9                   | files                       |      1           | count  |
| gzip -9                   | ratio_to_raw_bytes          |      0.2251      | ratio  |
| gzip -9                   | ratio_to_raw_blocks         |      0.2255      | ratio  |
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
| zstd -19                         | disk_blocks                 |      3.16211e+06 | bytes  |
| zstd -19                         | files                       |      1           | count  |
| zstd -19                         | ratio_to_raw_bytes          |      0.06443     | ratio  |
| zstd -19                         | ratio_to_raw_blocks         |      0.07033     | ratio  |
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
| SHARED_DAY rg=1,000              | disk_blocks                 |      2.70049e+07 | bytes  |
| SHARED_DAY rg=1,000              | files                       |      2           | count  |
| SHARED_DAY rg=1,000              | ratio_to_raw_bytes          |      0.5805      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_raw_blocks         |      0.6007      | ratio  |
| SHARED_DAY rg=1,000              | ratio_to_gzip9              |      6.425       | ratio  |
| SHARED_DAY rg=8,192              | bytes                       |      2.58316e+07 | bytes  |
| SHARED_DAY rg=8,192              | disk_blocks                 |      2.65093e+07 | bytes  |
| SHARED_DAY rg=8,192              | files                       |      2           | count  |
| SHARED_DAY rg=8,192              | ratio_to_raw_bytes          |      0.5746      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_raw_blocks         |      0.5896      | ratio  |
| SHARED_DAY rg=8,192              | ratio_to_gzip9              |      6.359       | ratio  |
| SHARED_DAY rg=122,880            | bytes                       |      2.57982e+07 | bytes  |
| SHARED_DAY rg=122,880            | disk_blocks                 |      2.58089e+07 | bytes  |
| SHARED_DAY rg=122,880            | files                       |      2           | count  |
| SHARED_DAY rg=122,880            | ratio_to_raw_bytes          |      0.5738      | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_raw_blocks         |      0.574       | ratio  |
| SHARED_DAY rg=122,880            | ratio_to_gzip9              |      6.351       | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | bytes                       |      4.36132e+06 | bytes  |
| PER_TEMPLATE[J70] rg=8,192       | disk_blocks                 |      5.0176e+06  | bytes  |
| PER_TEMPLATE[J70] rg=8,192       | files                       |    277           | count  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_raw_bytes          |      0.09701     | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_raw_blocks         |      0.1116      | ratio  |
| PER_TEMPLATE[J70] rg=8,192       | ratio_to_gzip9              |      1.074       | ratio  |
| SHARED_DAY[J70] rg=8,192         | bytes                       |      4.36801e+06 | bytes  |
| SHARED_DAY[J70] rg=8,192         | disk_blocks                 |      4.93978e+06 | bytes  |
| SHARED_DAY[J70] rg=8,192         | files                       |      2           | count  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_raw_bytes          |      0.09715     | ratio  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_raw_blocks         |      0.1099      | ratio  |
| SHARED_DAY[J70] rg=8,192         | ratio_to_gzip9              |      1.075       | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | bytes                       |      7.16069e+06 | bytes  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | disk_blocks                 |      8.99482e+06 | bytes  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | files                       |    982           | count  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_raw_bytes          |      0.1593      | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_raw_blocks         |      0.2001      | ratio  |
| PER_TEMPLATE[J80_GROWN] rg=8,192 | ratio_to_gzip9              |      1.763       | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | bytes                       |      7.1843e+06  | bytes  |
| SHARED_DAY[J80_GROWN] rg=8,192   | disk_blocks                 |      8.1879e+06  | bytes  |
| SHARED_DAY[J80_GROWN] rg=8,192   | files                       |      2           | count  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_raw_bytes          |      0.1598      | ratio  |
| SHARED_DAY[J80_GROWN] rg=8,192   | ratio_to_raw_blocks         |      0.1821      | ratio  |
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
| verdict                          | smallest_by_blocks          |      3.16211e+06 | bytes  |

**Notes**

- C zlib, level 9
- C zstd, level 19
- what kibbr does today
- per-template bytes without per-file block rounding
- 43 merged templates, from script 04
- 60 merged templates, from script 04
- 5-minute window over the busiest dated template (16949071483150859787); row groups are skipped on their ts statistics
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 0.7961 over 143 files
- zstd -19
- 68 merged templates, from script 04
- 125 merged templates, from script 04
- 5-minute window over the busiest dated template (1055466456870197778); row groups are skipped on their ts statistics
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 1.2384 over 870 files
- 66 merged templates, from script 04
- 33 merged templates, from script 04
- 5-minute window over the busiest dated template (3977316063236355336); row groups are skipped on their ts statistics
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 0.9400 over 130 files
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 1.3141 over 870 files
- 277 merged templates, from script 04
- 982 merged templates, from script 04
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 8.4970 over 4,931 files

## 03_layout_model

_Files, row groups and overhead per layout_

Run 2026-09-19T10:01:36+00:00, git `90152d2`, 690 rows.

### `03_layout_model` / dataset `flat30`

| variant                        | metric                           |            value | unit    |
|--------------------------------|----------------------------------|------------------|---------|
| sealing                        | partitions                       |  16694           | count   |
| sealing                        | segments                         |  16694           | count   |
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

### `03_layout_model` / dataset `messy`

| variant                        | metric                           |            value | unit    |
|--------------------------------|----------------------------------|------------------|---------|
| sealing                        | partitions                       |    130           | count   |
| sealing                        | segments                         |  34891           | count   |
| rate=12 wait=60                | closes_seal                      |  32304           | count   |
| rate=12 wait=60                | closes_wait                      |  16830           | count   |
| rate=12 wait=60                | closes_shutdown                  |     97           | count   |
| rate=12 wait=60                | row_groups                       |  49231           | count   |
| rate=12 wait=60                | rows_per_rg_median               |      2           | rows    |
| rate=12 wait=60                | rows_per_rg_p90                  |     10           | rows    |
| rate=12 wait=60                | backlog_max_lines                |    405           | lines   |
| rate=12 wait=60                | backlog_median_lines             |    310           | lines   |
| rate=12 wait=300               | closes_seal                      |  34455           | count   |
| rate=12 wait=300               | closes_wait                      |   2085           | count   |
| rate=12 wait=300               | closes_shutdown                  |    121           | count   |
| rate=12 wait=300               | row_groups                       |  36661           | count   |
| rate=12 wait=300               | rows_per_rg_median               |      3           | rows    |
| rate=12 wait=300               | rows_per_rg_p90                  |     14           | rows    |
| rate=12 wait=300               | backlog_max_lines                |    922           | lines   |
| rate=12 wait=300               | backlog_median_lines             |    675           | lines   |
| rate=12 wait=900               | closes_seal                      |  34716           | count   |
| rate=12 wait=900               | closes_wait                      |    321           | count   |
| rate=12 wait=900               | closes_shutdown                  |    128           | count   |
| rate=12 wait=900               | row_groups                       |  35165           | count   |
| rate=12 wait=900               | rows_per_rg_median               |      2           | rows    |
| rate=12 wait=900               | rows_per_rg_p90                  |     15           | rows    |
| rate=12 wait=900               | backlog_max_lines                |   1098           | lines   |
| rate=12 wait=900               | backlog_median_lines             |    843           | lines   |
| rate=120 wait=60               | closes_seal                      |  34671           | count   |
| rate=120 wait=60               | closes_wait                      |    688           | count   |
| rate=120 wait=60               | closes_shutdown                  |    128           | count   |
| rate=120 wait=60               | row_groups                       |  35487           | count   |
| rate=120 wait=60               | rows_per_rg_median               |      3           | rows    |
| rate=120 wait=60               | rows_per_rg_p90                  |     15           | rows    |
| rate=120 wait=60               | backlog_max_lines                |   1080           | lines   |
| rate=120 wait=60               | backlog_median_lines             |    785           | lines   |
| rate=120 wait=300              | closes_seal                      |  34759           | count   |
| rate=120 wait=300              | closes_wait                      |     12           | count   |
| rate=120 wait=300              | closes_shutdown                  |    129           | count   |
| rate=120 wait=300              | row_groups                       |  34900           | count   |
| rate=120 wait=300              | rows_per_rg_median               |      2           | rows    |
| rate=120 wait=300              | rows_per_rg_p90                  |     15           | rows    |
| rate=120 wait=300              | backlog_max_lines                |   1213           | lines   |
| rate=120 wait=300              | backlog_median_lines             |    960           | lines   |
| rate=120 wait=900              | closes_seal                      |  34761           | count   |
| rate=120 wait=900              | closes_shutdown                  |    130           | count   |
| rate=120 wait=900              | row_groups                       |  34891           | count   |
| rate=120 wait=900              | rows_per_rg_median               |      2           | rows    |
| rate=120 wait=900              | rows_per_rg_p90                  |     15           | rows    |
| rate=120 wait=900              | backlog_max_lines                |   1235           | lines   |
| rate=120 wait=900              | backlog_median_lines             |    978           | lines   |
| rate=1200 wait=60              | closes_seal                      |  34761           | count   |
| rate=1200 wait=60              | closes_shutdown                  |    130           | count   |
| rate=1200 wait=60              | row_groups                       |  34891           | count   |
| rate=1200 wait=60              | rows_per_rg_median               |      2           | rows    |
| rate=1200 wait=60              | rows_per_rg_p90                  |     15           | rows    |
| rate=1200 wait=60              | backlog_max_lines                |   1235           | lines   |
| rate=1200 wait=60              | backlog_median_lines             |    978           | lines   |
| rate=1200 wait=300             | closes_seal                      |  34761           | count   |
| rate=1200 wait=300             | closes_shutdown                  |    130           | count   |
| rate=1200 wait=300             | row_groups                       |  34891           | count   |
| rate=1200 wait=300             | rows_per_rg_median               |      2           | rows    |
| rate=1200 wait=300             | rows_per_rg_p90                  |     15           | rows    |
| rate=1200 wait=300             | backlog_max_lines                |   1235           | lines   |
| rate=1200 wait=300             | backlog_median_lines             |    978           | lines   |
| rate=1200 wait=900             | closes_seal                      |  34761           | count   |
| rate=1200 wait=900             | closes_shutdown                  |    130           | count   |
| rate=1200 wait=900             | row_groups                       |  34891           | count   |
| rate=1200 wait=900             | rows_per_rg_median               |      2           | rows    |
| rate=1200 wait=900             | rows_per_rg_p90                  |     15           | rows    |
| rate=1200 wait=900             | backlog_max_lines                |   1235           | lines   |
| rate=1200 wait=900             | backlog_median_lines             |    978           | lines   |
| rate=12 wait=900 tick          | closes_tick                      | 193050           | count   |
| rate=12 wait=900 tick          | closes_seal                      |   1195           | count   |
| rate=12 wait=900 tick          | closes_shutdown                  |      2           | count   |
| rate=12 wait=900 tick          | row_groups                       | 194247           | count   |
| rate=12 wait=900 tick          | rows_per_rg_median               |      1           | rows    |
| rate=12 wait=900 tick          | rows_per_rg_p90                  |      1           | rows    |
| rate=12 wait=900 tick          | backlog_max_lines                |      6           | lines   |
| rate=12 wait=900 tick          | backlog_median_lines             |      3           | lines   |
| rate=120 wait=900 tick         | closes_seal                      |  10691           | count   |
| rate=120 wait=900 tick         | closes_tick                      | 137611           | count   |
| rate=120 wait=900 tick         | closes_shutdown                  |     17           | count   |
| rate=120 wait=900 tick         | row_groups                       | 148319           | count   |
| rate=120 wait=900 tick         | rows_per_rg_median               |      1           | rows    |
| rate=120 wait=900 tick         | rows_per_rg_p90                  |      2           | rows    |
| rate=120 wait=900 tick         | backlog_max_lines                |     60           | lines   |
| rate=120 wait=900 tick         | backlog_median_lines             |     29           | lines   |
| rate=1200 wait=900 tick        | closes_seal                      |  28658           | count   |
| rate=1200 wait=900 tick        | closes_tick                      |  34085           | count   |
| rate=1200 wait=900 tick        | closes_shutdown                  |     76           | count   |
| rate=1200 wait=900 tick        | row_groups                       |  62819           | count   |
| rate=1200 wait=900 tick        | rows_per_rg_median               |      2           | rows    |
| rate=1200 wait=900 tick        | rows_per_rg_p90                  |      7           | rows    |
| rate=1200 wait=900 tick        | backlog_max_lines                |    474           | lines   |
| rate=1200 wait=900 tick        | backlog_median_lines             |    227           | lines   |
| rate=12 wait=60                | wal_24h_bytes_raw                |      3.59561e+08 | bytes   |
| rate=12 wait=60                | wal_24h_bytes_zstd               |      4.10259e+07 | bytes   |
| rate=12 wait=300               | wal_24h_bytes_raw                |      3.59561e+08 | bytes   |
| rate=12 wait=300               | wal_24h_bytes_zstd               |      4.10259e+07 | bytes   |
| rate=12 wait=900               | wal_24h_bytes_raw                |      3.59561e+08 | bytes   |
| rate=12 wait=900               | wal_24h_bytes_zstd               |      4.10259e+07 | bytes   |
| rate=120 wait=60               | wal_24h_bytes_raw                |      3.59561e+09 | bytes   |
| rate=120 wait=60               | wal_24h_bytes_zstd               |      4.10259e+08 | bytes   |
| rate=120 wait=300              | wal_24h_bytes_raw                |      3.59561e+09 | bytes   |
| rate=120 wait=300              | wal_24h_bytes_zstd               |      4.10259e+08 | bytes   |
| rate=120 wait=900              | wal_24h_bytes_raw                |      3.59561e+09 | bytes   |
| rate=120 wait=900              | wal_24h_bytes_zstd               |      4.10259e+08 | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_raw                |      3.59561e+10 | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_zstd               |      4.10259e+09 | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_raw                |      3.59561e+10 | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_zstd               |      4.10259e+09 | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_raw                |      3.59561e+10 | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_zstd               |      4.10259e+09 | bytes   |
| per_template_day               | files                            |  34891           | count   |
| per_template_day               | row_groups                       |  36661           | count   |
| per_template_day               | bytes                            |      5.90709e+07 | bytes   |
| per_template_day               | overhead_bytes                   |      2.23682e+07 | bytes   |
| per_template_day               | overhead_share_pct               |     37.87        | percent |
| per_template_day               | disk_blocks                      |      1.53096e+08 | bytes   |
| per_template_day               | ratio_to_raw                     |      2.432       | ratio   |
| per_template_day               | one_template_files               |    841           | count   |
| per_template_day               | one_template_index_bytes         | 288463           | bytes   |
| per_template_day               | one_template_ranges              |    841           | count   |
| per_template_day               | all_templates_files              |  34891           | count   |
| per_template_day               | all_templates_index_bytes        |      1.28491e+07 | bytes   |
| per_template_day               | all_templates_ranges             |  34891           | count   |
| shared_day_limit limit=128.0MB | files                            |      1           | count   |
| shared_day_limit limit=128.0MB | row_groups                       |  36661           | count   |
| shared_day_limit limit=128.0MB | bytes                            |      5.56294e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_bytes                   |      1.89267e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_share_pct               |     34.02        | percent |
| shared_day_limit limit=128.0MB | disk_blocks                      |      5.56319e+07 | bytes   |
| shared_day_limit limit=128.0MB | ratio_to_raw                     |      0.8836      | ratio   |
| shared_day_limit limit=128.0MB | one_template_files               |      1           | count   |
| shared_day_limit limit=128.0MB | one_template_index_bytes         |      7.91898e+06 | bytes   |
| shared_day_limit limit=128.0MB | one_template_ranges              |    788           | count   |
| shared_day_limit limit=128.0MB | all_templates_files              |      1           | count   |
| shared_day_limit limit=128.0MB | all_templates_index_bytes        |      7.91898e+06 | bytes   |
| shared_day_limit limit=128.0MB | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=128.0MB | open_file_index_memory_bytes     |      1.91056e+07 | bytes   |
| shared_day_limit limit=512.0MB | files                            |      1           | count   |
| shared_day_limit limit=512.0MB | row_groups                       |  36661           | count   |
| shared_day_limit limit=512.0MB | bytes                            |      5.56294e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_bytes                   |      1.89267e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_share_pct               |     34.02        | percent |
| shared_day_limit limit=512.0MB | disk_blocks                      |      5.56319e+07 | bytes   |
| shared_day_limit limit=512.0MB | ratio_to_raw                     |      0.8836      | ratio   |
| shared_day_limit limit=512.0MB | one_template_files               |      1           | count   |
| shared_day_limit limit=512.0MB | one_template_index_bytes         |      7.91898e+06 | bytes   |
| shared_day_limit limit=512.0MB | one_template_ranges              |    788           | count   |
| shared_day_limit limit=512.0MB | all_templates_files              |      1           | count   |
| shared_day_limit limit=512.0MB | all_templates_index_bytes        |      7.91898e+06 | bytes   |
| shared_day_limit limit=512.0MB | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=512.0MB | open_file_index_memory_bytes     |      7.64227e+07 | bytes   |
| shared_day_limit limit=1.0GB   | files                            |      1           | count   |
| shared_day_limit limit=1.0GB   | row_groups                       |  36661           | count   |
| shared_day_limit limit=1.0GB   | bytes                            |      5.56294e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_bytes                   |      1.89267e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_share_pct               |     34.02        | percent |
| shared_day_limit limit=1.0GB   | disk_blocks                      |      5.56319e+07 | bytes   |
| shared_day_limit limit=1.0GB   | ratio_to_raw                     |      0.8836      | ratio   |
| shared_day_limit limit=1.0GB   | one_template_files               |      1           | count   |
| shared_day_limit limit=1.0GB   | one_template_index_bytes         |      7.91898e+06 | bytes   |
| shared_day_limit limit=1.0GB   | one_template_ranges              |    788           | count   |
| shared_day_limit limit=1.0GB   | all_templates_files              |      1           | count   |
| shared_day_limit limit=1.0GB   | all_templates_index_bytes        |      7.91898e+06 | bytes   |
| shared_day_limit limit=1.0GB   | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=1.0GB   | open_file_index_memory_bytes     |      1.52846e+08 | bytes   |
| mod_n n=4                      | files                            |      4           | count   |
| mod_n n=4                      | row_groups                       |  36661           | count   |
| mod_n n=4                      | bytes                            |      5.56301e+07 | bytes   |
| mod_n n=4                      | overhead_bytes                   |      1.89274e+07 | bytes   |
| mod_n n=4                      | overhead_share_pct               |     34.02        | percent |
| mod_n n=4                      | disk_blocks                      |      5.56401e+07 | bytes   |
| mod_n n=4                      | ratio_to_raw                     |      0.8837      | ratio   |
| mod_n n=4                      | one_template_files               |      1           | count   |
| mod_n n=4                      | one_template_index_bytes         |      1.5904e+06  | bytes   |
| mod_n n=4                      | one_template_ranges              |    676           | count   |
| mod_n n=4                      | all_templates_files              |      4           | count   |
| mod_n n=4                      | all_templates_index_bytes        |      7.9196e+06  | bytes   |
| mod_n n=4                      | all_templates_ranges             |      4           | count   |
| mod_n n=4                      | byte_spread_largest_over_average |      1.214       | ratio   |
| mod_n n=8                      | files                            |      8           | count   |
| mod_n n=8                      | row_groups                       |  36661           | count   |
| mod_n n=8                      | bytes                            |      5.56309e+07 | bytes   |
| mod_n n=8                      | overhead_bytes                   |      1.89282e+07 | bytes   |
| mod_n n=8                      | overhead_share_pct               |     34.02        | percent |
| mod_n n=8                      | disk_blocks                      |      5.56483e+07 | bytes   |
| mod_n n=8                      | ratio_to_raw                     |      0.8839      | ratio   |
| mod_n n=8                      | one_template_files               |      1           | count   |
| mod_n n=8                      | one_template_index_bytes         | 492253           | bytes   |
| mod_n n=8                      | one_template_ranges              |    443           | count   |
| mod_n n=8                      | all_templates_files              |      8           | count   |
| mod_n n=8                      | all_templates_index_bytes        |      7.92042e+06 | bytes   |
| mod_n n=8                      | all_templates_ranges             |      8           | count   |
| mod_n n=8                      | byte_spread_largest_over_average |      1.371       | ratio   |
| least_full n=4                 | files                            |      4           | count   |
| least_full n=4                 | row_groups                       |  36661           | count   |
| least_full n=4                 | bytes                            |      5.56301e+07 | bytes   |
| least_full n=4                 | overhead_bytes                   |      1.89274e+07 | bytes   |
| least_full n=4                 | overhead_share_pct               |     34.02        | percent |
| least_full n=4                 | disk_blocks                      |      5.5636e+07  | bytes   |
| least_full n=4                 | ratio_to_raw                     |      0.8837      | ratio   |
| least_full n=4                 | one_template_files               |      4           | count   |
| least_full n=4                 | one_template_index_bytes         |      7.9196e+06  | bytes   |
| least_full n=4                 | one_template_ranges              |    802           | count   |
| least_full n=4                 | all_templates_files              |      4           | count   |
| least_full n=4                 | all_templates_index_bytes        |      7.9196e+06  | bytes   |
| least_full n=4                 | all_templates_ranges             |      4           | count   |
| least_full n=4                 | byte_spread_largest_over_average |      1.002       | ratio   |
| least_full n=8                 | files                            |      8           | count   |
| least_full n=8                 | row_groups                       |  36661           | count   |
| least_full n=8                 | bytes                            |      5.56309e+07 | bytes   |
| least_full n=8                 | overhead_bytes                   |      1.89282e+07 | bytes   |
| least_full n=8                 | overhead_share_pct               |     34.02        | percent |
| least_full n=8                 | disk_blocks                      |      5.56524e+07 | bytes   |
| least_full n=8                 | ratio_to_raw                     |      0.8839      | ratio   |
| least_full n=8                 | one_template_files               |      8           | count   |
| least_full n=8                 | one_template_index_bytes         |      7.92042e+06 | bytes   |
| least_full n=8                 | one_template_ranges              |    818           | count   |
| least_full n=8                 | all_templates_files              |      8           | count   |
| least_full n=8                 | all_templates_index_bytes        |      7.92042e+06 | bytes   |
| least_full n=8                 | all_templates_ranges             |      8           | count   |
| least_full n=8                 | byte_spread_largest_over_average |      1.003       | ratio   |
| calibration                    | real_col_bytes                   |      6.00619e+07 | bytes   |
| calibration                    | real_col_files                   |  34892           | count   |
| calibration                    | real_col_blocks                  |      1.52396e+08 | bytes   |
| calibration                    | real_meta_bytes                  | 572719           | bytes   |
| calibration                    | real_meta_blocks                 |      1.06906e+06 | bytes   |
| calibration                    | real_meta_files                  |    131           | count   |
| calibration                    | real_segments_over_ceil          |      0           | count   |
| calibration                    | real_bytes_over_ceil             |      0           | bytes   |
| calibration                    | model_col_bytes                  |      5.82442e+07 | bytes   |
| calibration                    | model_col_files                  |  34891           | count   |
| calibration                    | model_col_blocks                 |      1.52371e+08 | bytes   |
| calibration                    | model_minus_real_pct             |     -3.026       | percent |

### `03_layout_model` / dataset `uneven`

| variant                        | metric                           |           value | unit    |
|--------------------------------|----------------------------------|-----------------|---------|
| sealing                        | partitions                       |  4931           | count   |
| sealing                        | segments                         |  4931           | count   |
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

- a (template, day) pair - what the model used to assume was one file
- segments the seal rule actually produces; the `.col` format is silent on sealing, so the first version of this model assumed one per partition
- close reason
- lines in memory and not yet in a closed row group - what a crash replays
- 32 bytes of WAL record header per line plus 233 bytes of line; zstd 3 over batches of 500 lines, from script 08
- zstd 3 over batches of 500 lines, from script 08
- what kibbr does today: one segment per leaf per day, resealed whenever a row would widen a column's kind or change a Datetime column's rung
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
- 32 bytes of WAL record header per line plus 315 bytes of line; zstd 3 over batches of 500 lines, from script 08
- one day (2024-01-15); the one-template search is template 3977316063236355336
- 88,452 row groups x 16 columns x 12 bytes, plus 24 per row group, at the size limit
- 353,809 row groups x 16 columns x 12 bytes, plus 24 per row group, at the size limit
- 707,619 row groups x 16 columns x 12 bytes, plus 24 per row group, at the size limit
- 32 bytes of WAL record header per line plus 224 bytes of line; zstd 3 over batches of 500 lines, from script 08
- one day (2024-01-15); the one-template search is template 1055466456870197778
- 54,779 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit
- 219,118 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit
- 438,237 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit

## 04_merge_rules

_Which merging rule_

Run 2026-09-22T15:52:17+00:00, git `90152d2`, 657 rows.

### `04_merge_rules` / dataset `farm`

| variant                    | metric                       | value         | unit    |
|----------------------------|------------------------------|---------------|---------|
| NONE                       | templates                    | 143           | count   |
| NONE                       | rows_per_template_min        | 1             | count   |
| NONE                       | rows_per_template_median     | 70            | count   |
| NONE                       | rows_per_template_p90        | 2,212         | count   |
| NONE                       | rows_per_template_max        | 57,786        | count   |
| NONE                       | null_cell_share_pct          | 0             | percent |
| NONE                       | columns_widened_to_string    | 29            | count   |
| NONE                       | columns_widened_share_pct    | 1.453         | percent |
| NONE                       | distinct_keys_widened        | 8             | count   |
| NONE                       | rows_under_a_widened_column  | 108,583       | count   |
| NONE                       | mixed_promoted_groups        | 0             | count   |
| NONE                       | rows_in_mixed_promoted_group | 0             | count   |
| NONE                       | template_count_min           | 143           | count   |
| NONE                       | template_count_max           | 143           | count   |
| NONE                       | template_count_spread        | 0             | count   |
| NONE                       | moved_share_pct_max          | 0             | percent |
| NONE                       | moved_share_pct_mean         | 0             | percent |
| NONE                       | search_rows_scanned          | 131,948       | rows    |
| NONE                       | search_scan_amplification    | 1             | ratio   |
| J70                        | templates                    | 60            | count   |
| J70                        | rows_per_template_min        | 3             | count   |
| J70                        | rows_per_template_median     | 385           | count   |
| J70                        | rows_per_template_p90        | 4,299         | count   |
| J70                        | rows_per_template_max        | 86,627        | count   |
| J70                        | null_cell_share_pct          | 8.795         | percent |
| J70                        | columns_widened_to_string    | 18            | count   |
| J70                        | columns_widened_share_pct    | 2.151         | percent |
| J70                        | distinct_keys_widened        | 8             | count   |
| J70                        | rows_under_a_widened_column  | 117,418       | count   |
| J70                        | mixed_promoted_groups        | 0             | count   |
| J70                        | rows_in_mixed_promoted_group | 0             | count   |
| J70                        | template_count_min           | 57            | count   |
| J70                        | template_count_max           | 59            | count   |
| J70                        | template_count_spread        | 2             | count   |
| J70                        | moved_share_pct_max          | 0.0055        | percent |
| J70                        | moved_share_pct_mean         | 0.0046        | percent |
| J70                        | search_rows_scanned          | 148,667       | rows    |
| J70                        | search_scan_amplification    | 1.127         | ratio   |
| J80                        | templates                    | 77            | count   |
| J80                        | rows_per_template_min        | 2             | count   |
| J80                        | rows_per_template_median     | 202           | count   |
| J80                        | rows_per_template_p90        | 2,492         | count   |
| J80                        | rows_per_template_max        | 86,627        | count   |
| J80                        | null_cell_share_pct          | 6.707         | percent |
| J80                        | columns_widened_to_string    | 24            | count   |
| J80                        | columns_widened_share_pct    | 2.36          | percent |
| J80                        | distinct_keys_widened        | 8             | count   |
| J80                        | rows_under_a_widened_column  | 117,415       | count   |
| J80                        | mixed_promoted_groups        | 0             | count   |
| J80                        | rows_in_mixed_promoted_group | 0             | count   |
| J80                        | template_count_min           | 77            | count   |
| J80                        | template_count_max           | 78            | count   |
| J80                        | template_count_spread        | 1             | count   |
| J80                        | moved_share_pct_max          | 4.767         | percent |
| J80                        | moved_share_pct_mean         | 2.735         | percent |
| J80                        | search_rows_scanned          | 147,606       | rows    |
| J80                        | search_scan_amplification    | 1.119         | ratio   |
| J90                        | templates                    | 92            | count   |
| J90                        | rows_per_template_min        | 1             | count   |
| J90                        | rows_per_template_median     | 102           | count   |
| J90                        | rows_per_template_p90        | 2,388         | count   |
| J90                        | rows_per_template_max        | 57,786        | count   |
| J90                        | null_cell_share_pct          | 2.541         | percent |
| J90                        | columns_widened_to_string    | 24            | count   |
| J90                        | columns_widened_share_pct    | 1.98          | percent |
| J90                        | distinct_keys_widened        | 8             | count   |
| J90                        | rows_under_a_widened_column  | 115,620       | count   |
| J90                        | mixed_promoted_groups        | 0             | count   |
| J90                        | rows_in_mixed_promoted_group | 0             | count   |
| J90                        | template_count_min           | 89            | count   |
| J90                        | template_count_max           | 91            | count   |
| J90                        | template_count_spread        | 2             | count   |
| J90                        | moved_share_pct_max          | 2.308         | percent |
| J90                        | moved_share_pct_mean         | 1.474         | percent |
| J90                        | search_rows_scanned          | 137,305       | rows    |
| J90                        | search_scan_amplification    | 1.041         | ratio   |
| J80_GROWN                  | templates                    | 80            | count   |
| J80_GROWN                  | rows_per_template_min        | 2             | count   |
| J80_GROWN                  | rows_per_template_median     | 202           | count   |
| J80_GROWN                  | rows_per_template_p90        | 2,964         | count   |
| J80_GROWN                  | rows_per_template_max        | 86,627        | count   |
| J80_GROWN                  | null_cell_share_pct          | 5.646         | percent |
| J80_GROWN                  | columns_widened_to_string    | 23            | count   |
| J80_GROWN                  | columns_widened_share_pct    | 2.172         | percent |
| J80_GROWN                  | distinct_keys_widened        | 8             | count   |
| J80_GROWN                  | rows_under_a_widened_column  | 109,884       | count   |
| J80_GROWN                  | mixed_promoted_groups        | 0             | count   |
| J80_GROWN                  | rows_in_mixed_promoted_group | 0             | count   |
| J80_GROWN                  | template_count_min           | 79            | count   |
| J80_GROWN                  | template_count_max           | 80            | count   |
| J80_GROWN                  | template_count_spread        | 1             | count   |
| J80_GROWN                  | moved_share_pct_max          | 1.085         | percent |
| J80_GROWN                  | moved_share_pct_mean         | 0.8754        | percent |
| J80_GROWN                  | search_rows_scanned          | 139,702       | rows    |
| J80_GROWN                  | search_scan_amplification    | 1.059         | ratio   |
| J80_SPEC                   | templates                    | 45            | count   |
| J80_SPEC                   | rows_per_template_min        | 2             | count   |
| J80_SPEC                   | rows_per_template_median     | 772           | count   |
| J80_SPEC                   | rows_per_template_p90        | 5,831         | count   |
| J80_SPEC                   | rows_per_template_max        | 86,627        | count   |
| J80_SPEC                   | null_cell_share_pct          | 11.59         | percent |
| J80_SPEC                   | columns_widened_to_string    | 22            | count   |
| J80_SPEC                   | columns_widened_share_pct    | 3.655         | percent |
| J80_SPEC                   | distinct_keys_widened        | 8             | count   |
| J80_SPEC                   | rows_under_a_widened_column  | 125,360       | count   |
| J80_SPEC                   | mixed_promoted_groups        | 21            | count   |
| J80_SPEC                   | rows_in_mixed_promoted_group | 78,996        | count   |
| J80_SPEC                   | template_count_min           | 42            | count   |
| J80_SPEC                   | template_count_max           | 45            | count   |
| J80_SPEC                   | template_count_spread        | 3             | count   |
| J80_SPEC                   | moved_share_pct_max          | 2.329         | percent |
| J80_SPEC                   | moved_share_pct_mean         | 1.39          | percent |
| J80_SPEC                   | search_rows_scanned          | 338,963       | rows    |
| J80_SPEC                   | search_scan_amplification    | 2.569         | ratio   |
| PER_EVENT                  | templates                    | 43            | count   |
| PER_EVENT                  | rows_per_template_min        | 3             | count   |
| PER_EVENT                  | rows_per_template_median     | 142           | count   |
| PER_EVENT                  | rows_per_template_p90        | 5,048         | count   |
| PER_EVENT                  | rows_per_template_max        | 108,563       | count   |
| PER_EVENT                  | null_cell_share_pct          | 66.61         | percent |
| PER_EVENT                  | columns_widened_to_string    | 20            | count   |
| PER_EVENT                  | columns_widened_share_pct    | 2.865         | percent |
| PER_EVENT                  | distinct_keys_widened        | 11            | count   |
| PER_EVENT                  | rows_under_a_widened_column  | 774,955       | count   |
| PER_EVENT                  | mixed_promoted_groups        | 0             | count   |
| PER_EVENT                  | rows_in_mixed_promoted_group | 0             | count   |
| PER_EVENT                  | template_count_min           | 43            | count   |
| PER_EVENT                  | template_count_max           | 43            | count   |
| PER_EVENT                  | template_count_spread        | 0             | count   |
| PER_EVENT                  | moved_share_pct_max          | 0             | percent |
| PER_EVENT                  | moved_share_pct_mean         | 0             | percent |
| PER_EVENT                  | search_rows_scanned          | 182,911       | rows    |
| PER_EVENT                  | search_scan_amplification    | 1.386         | ratio   |
| all                        | keys_with_more_than_one_type | 7             | count   |
| promote=nothing/NONE       | templates                    | 135           | count   |
| promote=nothing/NONE       | rows_scanned_event_search    | 91,437        | rows    |
| promote=nothing/NONE       | rows_scanned_service_search  | 0             | rows    |
| promote=nothing/J80        | templates                    | 45            | count   |
| promote=nothing/J80        | rows_scanned_event_search    | 91,437        | rows    |
| promote=nothing/J80        | rows_scanned_service_search  | 0             | rows    |
| promote=event/NONE         | templates                    | 143           | count   |
| promote=event/NONE         | rows_scanned_event_search    | 50,108        | rows    |
| promote=event/NONE         | rows_scanned_service_search  | 0             | rows    |
| promote=event/J80          | templates                    | 77            | count   |
| promote=event/J80          | rows_scanned_event_search    | 50,108        | rows    |
| promote=event/J80          | rows_scanned_service_search  | 0             | rows    |
| promote=event+service/NONE | templates                    | 143           | count   |
| promote=event+service/NONE | rows_scanned_event_search    | 50,108        | rows    |
| promote=event+service/NONE | rows_scanned_service_search  | 0             | rows    |
| promote=event+service/J80  | templates                    | 77            | count   |
| promote=event+service/J80  | rows_scanned_event_search    | 50,108        | rows    |
| promote=event+service/J80  | rows_scanned_service_search  | 0             | rows    |
| promote=binary/NONE        | templates                    | 143           | count   |
| promote=binary/NONE        | rows_scanned_event_search    | 50,108        | rows    |
| promote=binary/NONE        | rows_scanned_service_search  | 0             | rows    |
| promote=binary/J80         | templates                    | 77            | count   |
| promote=binary/J80         | rows_scanned_event_search    | 50,108        | rows    |
| promote=binary/J80         | rows_scanned_service_search  | 0             | rows    |
| J80 cap=100                | leftover_rows                | 0             | rows    |
| J80 cap=500                | leftover_rows                | 0             | rows    |
| J80 cap=2000               | leftover_rows                | 0             | rows    |
| verdict                    | best_two_rules               | PER_EVENT J70 | name    |

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
| NONE                       | columns_widened_share_pct    | 0       | percent |
| NONE                       | distinct_keys_widened        | 0       | count   |
| NONE                       | rows_under_a_widened_column  | 0       | count   |
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
| J70                        | columns_widened_share_pct    | 0       | percent |
| J70                        | distinct_keys_widened        | 0       | count   |
| J70                        | rows_under_a_widened_column  | 0       | count   |
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
| J80                        | columns_widened_share_pct    | 0       | percent |
| J80                        | distinct_keys_widened        | 0       | count   |
| J80                        | rows_under_a_widened_column  | 0       | count   |
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
| J90                        | columns_widened_share_pct    | 0       | percent |
| J90                        | distinct_keys_widened        | 0       | count   |
| J90                        | rows_under_a_widened_column  | 0       | count   |
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
| J80_GROWN                  | columns_widened_share_pct    | 0       | percent |
| J80_GROWN                  | distinct_keys_widened        | 0       | count   |
| J80_GROWN                  | rows_under_a_widened_column  | 0       | count   |
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
| J80_SPEC                   | columns_widened_share_pct    | 0       | percent |
| J80_SPEC                   | distinct_keys_widened        | 0       | count   |
| J80_SPEC                   | rows_under_a_widened_column  | 0       | count   |
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
| PER_EVENT                  | columns_widened_share_pct    | 0       | percent |
| PER_EVENT                  | distinct_keys_widened        | 0       | count   |
| PER_EVENT                  | rows_under_a_widened_column  | 0       | count   |
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

### `04_merge_rules` / dataset `messy`

| variant                    | metric                       | value         | unit    |
|----------------------------|------------------------------|---------------|---------|
| NONE                       | templates                    | 130           | count   |
| NONE                       | rows_per_template_min        | 93            | count   |
| NONE                       | rows_per_template_median     | 1,169         | count   |
| NONE                       | rows_per_template_p90        | 4,134         | count   |
| NONE                       | rows_per_template_max        | 4,873         | count   |
| NONE                       | null_cell_share_pct          | 0             | percent |
| NONE                       | columns_widened_to_string    | 806           | count   |
| NONE                       | columns_widened_share_pct    | 44.04         | percent |
| NONE                       | distinct_keys_widened        | 8             | count   |
| NONE                       | rows_under_a_widened_column  | 1,205,077     | count   |
| NONE                       | mixed_promoted_groups        | 0             | count   |
| NONE                       | rows_in_mixed_promoted_group | 0             | count   |
| NONE                       | template_count_min           | 130           | count   |
| NONE                       | template_count_max           | 130           | count   |
| NONE                       | template_count_spread        | 0             | count   |
| NONE                       | moved_share_pct_max          | 0             | percent |
| NONE                       | moved_share_pct_mean         | 0             | percent |
| NONE                       | search_rows_scanned          | 74,093        | rows    |
| NONE                       | search_scan_amplification    | 1             | ratio   |
| J70                        | templates                    | 19            | count   |
| J70                        | rows_per_template_min        | 8,256         | count   |
| J70                        | rows_per_template_median     | 12,296        | count   |
| J70                        | rows_per_template_p90        | 12,606        | count   |
| J70                        | rows_per_template_max        | 12,615        | count   |
| J70                        | null_cell_share_pct          | 15.5          | percent |
| J70                        | columns_widened_to_string    | 152           | count   |
| J70                        | columns_widened_share_pct    | 50            | percent |
| J70                        | distinct_keys_widened        | 8             | count   |
| J70                        | rows_under_a_widened_column  | 1,600,000     | count   |
| J70                        | mixed_promoted_groups        | 0             | count   |
| J70                        | rows_in_mixed_promoted_group | 0             | count   |
| J70                        | template_count_min           | 19            | count   |
| J70                        | template_count_max           | 19            | count   |
| J70                        | template_count_spread        | 0             | count   |
| J70                        | moved_share_pct_max          | 0             | percent |
| J70                        | moved_share_pct_mean         | 0             | percent |
| J70                        | search_rows_scanned          | 208,427       | rows    |
| J70                        | search_scan_amplification    | 2.813         | ratio   |
| J80                        | templates                    | 33            | count   |
| J80                        | rows_per_template_min        | 1,702         | count   |
| J80                        | rows_per_template_median     | 6,102         | count   |
| J80                        | rows_per_template_p90        | 8,343         | count   |
| J80                        | rows_per_template_max        | 12,615        | count   |
| J80                        | null_cell_share_pct          | 14.74         | percent |
| J80                        | columns_widened_to_string    | 258           | count   |
| J80                        | columns_widened_share_pct    | 49.62         | percent |
| J80                        | distinct_keys_widened        | 8             | count   |
| J80                        | rows_under_a_widened_column  | 1,577,732     | count   |
| J80                        | mixed_promoted_groups        | 0             | count   |
| J80                        | rows_in_mixed_promoted_group | 0             | count   |
| J80                        | template_count_min           | 33            | count   |
| J80                        | template_count_max           | 33            | count   |
| J80                        | template_count_spread        | 0             | count   |
| J80                        | moved_share_pct_max          | 15.12         | percent |
| J80                        | moved_share_pct_mean         | 13.62         | percent |
| J80                        | search_rows_scanned          | 204,153       | rows    |
| J80                        | search_scan_amplification    | 2.755         | ratio   |
| J90                        | templates                    | 66            | count   |
| J90                        | rows_per_template_min        | 225           | count   |
| J90                        | rows_per_template_median     | 2,899         | count   |
| J90                        | rows_per_template_p90        | 5,207         | count   |
| J90                        | rows_per_template_max        | 6,960         | count   |
| J90                        | null_cell_share_pct          | 7.992         | percent |
| J90                        | columns_widened_to_string    | 470           | count   |
| J90                        | columns_widened_share_pct    | 48.5          | percent |
| J90                        | distinct_keys_widened        | 8             | count   |
| J90                        | rows_under_a_widened_column  | 1,443,179     | count   |
| J90                        | mixed_promoted_groups        | 0             | count   |
| J90                        | rows_in_mixed_promoted_group | 0             | count   |
| J90                        | template_count_min           | 66            | count   |
| J90                        | template_count_max           | 66            | count   |
| J90                        | template_count_spread        | 0             | count   |
| J90                        | moved_share_pct_max          | 9.203         | percent |
| J90                        | moved_share_pct_mean         | 7.325         | percent |
| J90                        | search_rows_scanned          | 127,552       | rows    |
| J90                        | search_scan_amplification    | 1.722         | ratio   |
| J80_GROWN                  | templates                    | 33            | count   |
| J80_GROWN                  | rows_per_template_min        | 3,678         | count   |
| J80_GROWN                  | rows_per_template_median     | 5,655         | count   |
| J80_GROWN                  | rows_per_template_p90        | 8,315         | count   |
| J80_GROWN                  | rows_per_template_max        | 12,615        | count   |
| J80_GROWN                  | null_cell_share_pct          | 10.77         | percent |
| J80_GROWN                  | columns_widened_to_string    | 250           | count   |
| J80_GROWN                  | columns_widened_share_pct    | 50            | percent |
| J80_GROWN                  | distinct_keys_widened        | 8             | count   |
| J80_GROWN                  | rows_under_a_widened_column  | 1,515,225     | count   |
| J80_GROWN                  | mixed_promoted_groups        | 0             | count   |
| J80_GROWN                  | rows_in_mixed_promoted_group | 0             | count   |
| J80_GROWN                  | template_count_min           | 33            | count   |
| J80_GROWN                  | template_count_max           | 33            | count   |
| J80_GROWN                  | template_count_spread        | 0             | count   |
| J80_GROWN                  | moved_share_pct_max          | 0.11          | percent |
| J80_GROWN                  | moved_share_pct_mean         | 0.0935        | percent |
| J80_GROWN                  | search_rows_scanned          | 160,306       | rows    |
| J80_GROWN                  | search_scan_amplification    | 2.164         | ratio   |
| J80_SPEC                   | templates                    | 2             | count   |
| J80_SPEC                   | rows_per_template_min        | 80,368        | count   |
| J80_SPEC                   | rows_per_template_median     | 119,632       | count   |
| J80_SPEC                   | rows_per_template_p90        | 119,632       | count   |
| J80_SPEC                   | rows_per_template_max        | 119,632       | count   |
| J80_SPEC                   | null_cell_share_pct          | 15.5          | percent |
| J80_SPEC                   | columns_widened_to_string    | 16            | count   |
| J80_SPEC                   | columns_widened_share_pct    | 50            | percent |
| J80_SPEC                   | distinct_keys_widened        | 8             | count   |
| J80_SPEC                   | rows_under_a_widened_column  | 1,600,000     | count   |
| J80_SPEC                   | mixed_promoted_groups        | 2             | count   |
| J80_SPEC                   | rows_in_mixed_promoted_group | 200,000       | count   |
| J80_SPEC                   | template_count_min           | 2             | count   |
| J80_SPEC                   | template_count_max           | 2             | count   |
| J80_SPEC                   | template_count_spread        | 0             | count   |
| J80_SPEC                   | moved_share_pct_max          | 21.42         | percent |
| J80_SPEC                   | moved_share_pct_mean         | 14.38         | percent |
| J80_SPEC                   | search_rows_scanned          | 4,000,000     | rows    |
| J80_SPEC                   | search_scan_amplification    | 53.99         | ratio   |
| PER_EVENT                  | templates                    | 19            | count   |
| PER_EVENT                  | rows_per_template_min        | 8,256         | count   |
| PER_EVENT                  | rows_per_template_median     | 12,296        | count   |
| PER_EVENT                  | rows_per_template_p90        | 12,606        | count   |
| PER_EVENT                  | rows_per_template_max        | 12,615        | count   |
| PER_EVENT                  | null_cell_share_pct          | 15.5          | percent |
| PER_EVENT                  | columns_widened_to_string    | 152           | count   |
| PER_EVENT                  | columns_widened_share_pct    | 50            | percent |
| PER_EVENT                  | distinct_keys_widened        | 8             | count   |
| PER_EVENT                  | rows_under_a_widened_column  | 1,600,000     | count   |
| PER_EVENT                  | mixed_promoted_groups        | 0             | count   |
| PER_EVENT                  | rows_in_mixed_promoted_group | 0             | count   |
| PER_EVENT                  | template_count_min           | 19            | count   |
| PER_EVENT                  | template_count_max           | 19            | count   |
| PER_EVENT                  | template_count_spread        | 0             | count   |
| PER_EVENT                  | moved_share_pct_max          | 0             | percent |
| PER_EVENT                  | moved_share_pct_mean         | 0             | percent |
| PER_EVENT                  | search_rows_scanned          | 208,427       | rows    |
| PER_EVENT                  | search_scan_amplification    | 2.813         | ratio   |
| all                        | keys_with_more_than_one_type | 7             | count   |
| promote=nothing/NONE       | templates                    | 8             | count   |
| promote=nothing/NONE       | rows_scanned_event_search    | 200,000       | rows    |
| promote=nothing/NONE       | rows_scanned_service_search  | 200,000       | rows    |
| promote=nothing/J80        | templates                    | 2             | count   |
| promote=nothing/J80        | rows_scanned_event_search    | 200,000       | rows    |
| promote=nothing/J80        | rows_scanned_service_search  | 200,000       | rows    |
| promote=event/NONE         | templates                    | 130           | count   |
| promote=event/NONE         | rows_scanned_event_search    | 12,615        | rows    |
| promote=event/NONE         | rows_scanned_service_search  | 200,000       | rows    |
| promote=event/J80          | templates                    | 33            | count   |
| promote=event/J80          | rows_scanned_event_search    | 12,615        | rows    |
| promote=event/J80          | rows_scanned_service_search  | 200,000       | rows    |
| promote=event+service/NONE | templates                    | 130           | count   |
| promote=event+service/NONE | rows_scanned_event_search    | 12,615        | rows    |
| promote=event+service/NONE | rows_scanned_service_search  | 25,125        | rows    |
| promote=event+service/J80  | templates                    | 33            | count   |
| promote=event+service/J80  | rows_scanned_event_search    | 12,615        | rows    |
| promote=event+service/J80  | rows_scanned_service_search  | 25,125        | rows    |
| promote=binary/NONE        | templates                    | 130           | count   |
| promote=binary/NONE        | rows_scanned_event_search    | 12,615        | rows    |
| promote=binary/NONE        | rows_scanned_service_search  | 200,000       | rows    |
| promote=binary/J80         | templates                    | 33            | count   |
| promote=binary/J80         | rows_scanned_event_search    | 12,615        | rows    |
| promote=binary/J80         | rows_scanned_service_search  | 200,000       | rows    |
| J80 cap=100                | leftover_rows                | 0             | rows    |
| J80 cap=500                | leftover_rows                | 0             | rows    |
| J80 cap=2000               | leftover_rows                | 0             | rows    |
| verdict                    | best_two_rules               | J90 J80_GROWN | name    |

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
| NONE                       | columns_widened_share_pct    | 0             | percent |
| NONE                       | distinct_keys_widened        | 0             | count   |
| NONE                       | rows_under_a_widened_column  | 0             | count   |
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
| J70                        | columns_widened_share_pct    | 0             | percent |
| J70                        | distinct_keys_widened        | 0             | count   |
| J70                        | rows_under_a_widened_column  | 0             | count   |
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
| J80                        | columns_widened_share_pct    | 0             | percent |
| J80                        | distinct_keys_widened        | 0             | count   |
| J80                        | rows_under_a_widened_column  | 0             | count   |
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
| J90                        | columns_widened_share_pct    | 0             | percent |
| J90                        | distinct_keys_widened        | 0             | count   |
| J90                        | rows_under_a_widened_column  | 0             | count   |
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
| J80_GROWN                  | columns_widened_share_pct    | 0             | percent |
| J80_GROWN                  | distinct_keys_widened        | 0             | count   |
| J80_GROWN                  | rows_under_a_widened_column  | 0             | count   |
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
| J80_SPEC                   | columns_widened_share_pct    | 0             | percent |
| J80_SPEC                   | distinct_keys_widened        | 0             | count   |
| J80_SPEC                   | rows_under_a_widened_column  | 0             | count   |
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
| PER_EVENT                  | columns_widened_share_pct    | 0             | percent |
| PER_EVENT                  | distinct_keys_widened        | 0             | count   |
| PER_EVENT                  | rows_under_a_widened_column  | 0             | count   |
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

- 20 searches of 'one promoted value plus one optional key is present'; 131,948 rows actually match, so this is the scan cost
- rows scanned divided by rows that match
- the type join can only widen a column where one key carries two types; at zero, every columns->String figure above is vacuous
- one value of `event`; a promoted key is one trie edge, an unpromoted one is a scan of every template holding the column
- the cap was never reached
- written to lab/out/04/best_rules.json for script 02 to pick up
- 20 searches of 'one promoted value plus one optional key is present'; 137,439 rows actually match, so this is the scan cost
- lines that would have created a template past the cap go into one shared leftover template, which is never itself a merge target
- 20 searches of 'one promoted value plus one optional key is present'; 74,093 rows actually match, so this is the scan cost
- 20 searches of 'one promoted value plus one optional key is present'; 54,159 rows actually match, so this is the scan cost

## 05_encodings

_What each encoding saves_

Run 2026-09-22T15:55:51+00:00, git `90152d2`, 1441 rows.

### `05_encodings` / dataset `farm`

| variant                    | metric                        | value        | unit    |
|----------------------------|-------------------------------|--------------|---------|
| units                      | templates chunked             | 143          | count   |
| arrival/Bool               | bits                          | 337          | bytes   |
| arrival/Bool               | bits+zstd3                    | 365          | bytes   |
| arrival/Bool               | const                         | 2,418        | bytes   |
| arrival/Bool               | const+zstd3                   | 524          | bytes   |
| arrival/Bool               | delta_varint                  | 2,665        | bytes   |
| arrival/Bool               | delta_varint+zstd3            | 528          | bytes   |
| arrival/Bool               | dict                          | 2,708        | bytes   |
| arrival/Bool               | dict+zstd3                    | 560          | bytes   |
| arrival/Bool               | ip_bin                        | 2,665        | bytes   |
| arrival/Bool               | ip_bin+zstd3                  | 528          | bytes   |
| arrival/Bool               | plain                         | 2,665        | bytes   |
| arrival/Bool               | plain+zstd3                   | 528          | bytes   |
| arrival/Bool               | uuid16                        | 2,665        | bytes   |
| arrival/Bool               | uuid16+zstd3                  | 528          | bytes   |
| arrival/Bool               | varint                        | 2,665        | bytes   |
| arrival/Bool               | varint+zstd3                  | 528          | bytes   |
| arrival/Datetime           | bits                          | 1,655,832    | bytes   |
| arrival/Datetime           | bits+zstd3                    | 897,092      | bytes   |
| arrival/Datetime           | const                         | 1,655,864    | bytes   |
| arrival/Datetime           | const+zstd3                   | 897,124      | bytes   |
| arrival/Datetime           | delta_varint                  | 904,646      | bytes   |
| arrival/Datetime           | delta_varint+zstd3            | 512,681      | bytes   |
| arrival/Datetime           | dict                          | 1,568,213    | bytes   |
| arrival/Datetime           | dict+zstd3                    | 1,066,225    | bytes   |
| arrival/Datetime           | ip_bin                        | 1,655,832    | bytes   |
| arrival/Datetime           | ip_bin+zstd3                  | 897,092      | bytes   |
| arrival/Datetime           | plain                         | 1,655,832    | bytes   |
| arrival/Datetime           | plain+zstd3                   | 897,092      | bytes   |
| arrival/Datetime           | uuid16                        | 1,655,832    | bytes   |
| arrival/Datetime           | uuid16+zstd3                  | 897,092      | bytes   |
| arrival/Datetime           | varint                        | 1,862,811    | bytes   |
| arrival/Datetime           | varint+zstd3                  | 739,198      | bytes   |
| arrival/Float64            | bits                          | 2,398,248    | bytes   |
| arrival/Float64            | bits+zstd3                    | 1,213,038    | bytes   |
| arrival/Float64            | const                         | 2,398,224    | bytes   |
| arrival/Float64            | const+zstd3                   | 1,213,062    | bytes   |
| arrival/Float64            | delta_varint                  | 2,398,248    | bytes   |
| arrival/Float64            | delta_varint+zstd3            | 1,213,038    | bytes   |
| arrival/Float64            | dict                          | 1,974,376    | bytes   |
| arrival/Float64            | dict+zstd3                    | 1,369,995    | bytes   |
| arrival/Float64            | ip_bin                        | 2,398,248    | bytes   |
| arrival/Float64            | ip_bin+zstd3                  | 1,213,038    | bytes   |
| arrival/Float64            | plain                         | 2,398,248    | bytes   |
| arrival/Float64            | plain+zstd3                   | 1,213,038    | bytes   |
| arrival/Float64            | uuid16                        | 2,398,248    | bytes   |
| arrival/Float64            | uuid16+zstd3                  | 1,213,038    | bytes   |
| arrival/Float64            | varint                        | 2,398,248    | bytes   |
| arrival/Float64            | varint+zstd3                  | 1,213,038    | bytes   |
| arrival/Int64              | bits                          | 3,292,328    | bytes   |
| arrival/Int64              | bits+zstd3                    | 499,080      | bytes   |
| arrival/Int64              | const                         | 3,033,892    | bytes   |
| arrival/Int64              | const+zstd3                   | 498,953      | bytes   |
| arrival/Int64              | delta_varint                  | 725,326      | bytes   |
| arrival/Int64              | delta_varint+zstd3            | 501,351      | bytes   |
| arrival/Int64              | dict                          | 1,325,684    | bytes   |
| arrival/Int64              | dict+zstd3                    | 578,625      | bytes   |
| arrival/Int64              | ip_bin                        | 3,292,328    | bytes   |
| arrival/Int64              | ip_bin+zstd3                  | 499,080      | bytes   |
| arrival/Int64              | plain                         | 3,292,328    | bytes   |
| arrival/Int64              | plain+zstd3                   | 499,080      | bytes   |
| arrival/Int64              | uuid16                        | 3,292,328    | bytes   |
| arrival/Int64              | uuid16+zstd3                  | 499,080      | bytes   |
| arrival/Int64              | varint                        | 844,763      | bytes   |
| arrival/Int64              | varint+zstd3                  | 457,398      | bytes   |
| arrival/Ip                 | bits                          | 2,642,633    | bytes   |
| arrival/Ip                 | bits+zstd3                    | 469,772      | bytes   |
| arrival/Ip                 | const                         | 2,618,269    | bytes   |
| arrival/Ip                 | const+zstd3                   | 469,772      | bytes   |
| arrival/Ip                 | delta_varint                  | 2,642,633    | bytes   |
| arrival/Ip                 | delta_varint+zstd3            | 469,772      | bytes   |
| arrival/Ip                 | dict                          | 763,774      | bytes   |
| arrival/Ip                 | dict+zstd3                    | 350,310      | bytes   |
| arrival/Ip                 | ip_bin                        | 817,330      | bytes   |
| arrival/Ip                 | ip_bin+zstd3                  | 261,422      | bytes   |
| arrival/Ip                 | plain                         | 2,642,633    | bytes   |
| arrival/Ip                 | plain+zstd3                   | 469,772      | bytes   |
| arrival/Ip                 | uuid16                        | 2,642,633    | bytes   |
| arrival/Ip                 | uuid16+zstd3                  | 469,772      | bytes   |
| arrival/Ip                 | varint                        | 2,642,633    | bytes   |
| arrival/Ip                 | varint+zstd3                  | 469,772      | bytes   |
| arrival/String             | bits                          | 45,418,700   | bytes   |
| arrival/String             | bits+zstd3                    | 4,894,456    | bytes   |
| arrival/String             | const                         | 39,305,943   | bytes   |
| arrival/String             | const+zstd3                   | 4,892,113    | bytes   |
| arrival/String             | delta_varint                  | 45,418,700   | bytes   |
| arrival/String             | delta_varint+zstd3            | 4,894,456    | bytes   |
| arrival/String             | dict                          | 9,763,521    | bytes   |
| arrival/String             | dict+zstd3                    | 3,777,292    | bytes   |
| arrival/String             | ip_bin                        | 45,418,700   | bytes   |
| arrival/String             | ip_bin+zstd3                  | 4,894,456    | bytes   |
| arrival/String             | plain                         | 45,418,700   | bytes   |
| arrival/String             | plain+zstd3                   | 4,894,456    | bytes   |
| arrival/String             | uuid16                        | 45,418,700   | bytes   |
| arrival/String             | uuid16+zstd3                  | 4,894,456    | bytes   |
| arrival/String             | varint                        | 45,418,700   | bytes   |
| arrival/String             | varint+zstd3                  | 4,894,456    | bytes   |
| arrival/Uuid               | bits                          | 709,800      | bytes   |
| arrival/Uuid               | bits+zstd3                    | 357,554      | bytes   |
| arrival/Uuid               | const                         | 709,800      | bytes   |
| arrival/Uuid               | const+zstd3                   | 357,554      | bytes   |
| arrival/Uuid               | delta_varint                  | 709,800      | bytes   |
| arrival/Uuid               | delta_varint+zstd3            | 357,554      | bytes   |
| arrival/Uuid               | dict                          | 719,164      | bytes   |
| arrival/Uuid               | dict+zstd3                    | 380,374      | bytes   |
| arrival/Uuid               | ip_bin                        | 709,800      | bytes   |
| arrival/Uuid               | ip_bin+zstd3                  | 357,554      | bytes   |
| arrival/Uuid               | plain                         | 709,800      | bytes   |
| arrival/Uuid               | plain+zstd3                   | 357,554      | bytes   |
| arrival/Uuid               | uuid16                        | 283,920      | bytes   |
| arrival/Uuid               | uuid16+zstd3                  | 283,034      | bytes   |
| arrival/Uuid               | varint                        | 709,800      | bytes   |
| arrival/Uuid               | varint+zstd3                  | 357,554      | bytes   |
| sorted/Bool                | bits                          | 337          | bytes   |
| sorted/Bool                | bits+zstd3                    | 105          | bytes   |
| sorted/Bool                | const                         | 2,418        | bytes   |
| sorted/Bool                | const+zstd3                   | 107          | bytes   |
| sorted/Bool                | delta_varint                  | 2,665        | bytes   |
| sorted/Bool                | delta_varint+zstd3            | 111          | bytes   |
| sorted/Bool                | dict                          | 2,708        | bytes   |
| sorted/Bool                | dict+zstd3                    | 150          | bytes   |
| sorted/Bool                | ip_bin                        | 2,665        | bytes   |
| sorted/Bool                | ip_bin+zstd3                  | 111          | bytes   |
| sorted/Bool                | plain                         | 2,665        | bytes   |
| sorted/Bool                | plain+zstd3                   | 111          | bytes   |
| sorted/Bool                | uuid16                        | 2,665        | bytes   |
| sorted/Bool                | uuid16+zstd3                  | 111          | bytes   |
| sorted/Bool                | varint                        | 2,665        | bytes   |
| sorted/Bool                | varint+zstd3                  | 111          | bytes   |
| sorted/Datetime            | bits                          | 1,655,832    | bytes   |
| sorted/Datetime            | bits+zstd3                    | 1,115,645    | bytes   |
| sorted/Datetime            | const                         | 1,655,864    | bytes   |
| sorted/Datetime            | const+zstd3                   | 1,115,677    | bytes   |
| sorted/Datetime            | delta_varint                  | 1,300,446    | bytes   |
| sorted/Datetime            | delta_varint+zstd3            | 1,045,349    | bytes   |
| sorted/Datetime            | dict                          | 1,568,213    | bytes   |
| sorted/Datetime            | dict+zstd3                    | 1,265,223    | bytes   |
| sorted/Datetime            | ip_bin                        | 1,655,832    | bytes   |
| sorted/Datetime            | ip_bin+zstd3                  | 1,115,645    | bytes   |
| sorted/Datetime            | plain                         | 1,655,832    | bytes   |
| sorted/Datetime            | plain+zstd3                   | 1,115,645    | bytes   |
| sorted/Datetime            | uuid16                        | 1,655,832    | bytes   |
| sorted/Datetime            | uuid16+zstd3                  | 1,115,645    | bytes   |
| sorted/Datetime            | varint                        | 1,862,811    | bytes   |
| sorted/Datetime            | varint+zstd3                  | 1,072,998    | bytes   |
| sorted/Float64             | bits                          | 2,398,248    | bytes   |
| sorted/Float64             | bits+zstd3                    | 1,306,063    | bytes   |
| sorted/Float64             | const                         | 2,398,224    | bytes   |
| sorted/Float64             | const+zstd3                   | 1,306,087    | bytes   |
| sorted/Float64             | delta_varint                  | 2,398,248    | bytes   |
| sorted/Float64             | delta_varint+zstd3            | 1,306,063    | bytes   |
| sorted/Float64             | dict                          | 1,974,376    | bytes   |
| sorted/Float64             | dict+zstd3                    | 1,467,632    | bytes   |
| sorted/Float64             | ip_bin                        | 2,398,248    | bytes   |
| sorted/Float64             | ip_bin+zstd3                  | 1,306,063    | bytes   |
| sorted/Float64             | plain                         | 2,398,248    | bytes   |
| sorted/Float64             | plain+zstd3                   | 1,306,063    | bytes   |
| sorted/Float64             | uuid16                        | 2,398,248    | bytes   |
| sorted/Float64             | uuid16+zstd3                  | 1,306,063    | bytes   |
| sorted/Float64             | varint                        | 2,398,248    | bytes   |
| sorted/Float64             | varint+zstd3                  | 1,306,063    | bytes   |
| sorted/Int64               | bits                          | 3,292,328    | bytes   |
| sorted/Int64               | bits+zstd3                    | 429,911      | bytes   |
| sorted/Int64               | const                         | 3,033,892    | bytes   |
| sorted/Int64               | const+zstd3                   | 429,784      | bytes   |
| sorted/Int64               | delta_varint                  | 650,373      | bytes   |
| sorted/Int64               | delta_varint+zstd3            | 404,328      | bytes   |
| sorted/Int64               | dict                          | 1,325,684    | bytes   |
| sorted/Int64               | dict+zstd3                    | 525,203      | bytes   |
| sorted/Int64               | ip_bin                        | 3,292,328    | bytes   |
| sorted/Int64               | ip_bin+zstd3                  | 429,911      | bytes   |
| sorted/Int64               | plain                         | 3,292,328    | bytes   |
| sorted/Int64               | plain+zstd3                   | 429,911      | bytes   |
| sorted/Int64               | uuid16                        | 3,292,328    | bytes   |
| sorted/Int64               | uuid16+zstd3                  | 429,911      | bytes   |
| sorted/Int64               | varint                        | 844,763      | bytes   |
| sorted/Int64               | varint+zstd3                  | 399,515      | bytes   |
| sorted/Ip                  | bits                          | 2,642,633    | bytes   |
| sorted/Ip                  | bits+zstd3                    | 520,827      | bytes   |
| sorted/Ip                  | const                         | 2,618,269    | bytes   |
| sorted/Ip                  | const+zstd3                   | 520,827      | bytes   |
| sorted/Ip                  | delta_varint                  | 2,642,633    | bytes   |
| sorted/Ip                  | delta_varint+zstd3            | 520,827      | bytes   |
| sorted/Ip                  | dict                          | 763,774      | bytes   |
| sorted/Ip                  | dict+zstd3                    | 349,261      | bytes   |
| sorted/Ip                  | ip_bin                        | 817,330      | bytes   |
| sorted/Ip                  | ip_bin+zstd3                  | 281,956      | bytes   |
| sorted/Ip                  | plain                         | 2,642,633    | bytes   |
| sorted/Ip                  | plain+zstd3                   | 520,827      | bytes   |
| sorted/Ip                  | uuid16                        | 2,642,633    | bytes   |
| sorted/Ip                  | uuid16+zstd3                  | 520,827      | bytes   |
| sorted/Ip                  | varint                        | 2,642,633    | bytes   |
| sorted/Ip                  | varint+zstd3                  | 520,827      | bytes   |
| sorted/String              | bits                          | 45,418,700   | bytes   |
| sorted/String              | bits+zstd3                    | 3,922,942    | bytes   |
| sorted/String              | const                         | 39,305,943   | bytes   |
| sorted/String              | const+zstd3                   | 3,920,599    | bytes   |
| sorted/String              | delta_varint                  | 45,418,700   | bytes   |
| sorted/String              | delta_varint+zstd3            | 3,922,942    | bytes   |
| sorted/String              | dict                          | 9,763,521    | bytes   |
| sorted/String              | dict+zstd3                    | 3,479,707    | bytes   |
| sorted/String              | ip_bin                        | 45,418,700   | bytes   |
| sorted/String              | ip_bin+zstd3                  | 3,922,942    | bytes   |
| sorted/String              | plain                         | 45,418,700   | bytes   |
| sorted/String              | plain+zstd3                   | 3,922,942    | bytes   |
| sorted/String              | uuid16                        | 45,418,700   | bytes   |
| sorted/String              | uuid16+zstd3                  | 3,922,942    | bytes   |
| sorted/String              | varint                        | 45,418,700   | bytes   |
| sorted/String              | varint+zstd3                  | 3,922,942    | bytes   |
| sorted/Uuid                | bits                          | 709,800      | bytes   |
| sorted/Uuid                | bits+zstd3                    | 357,522      | bytes   |
| sorted/Uuid                | const                         | 709,800      | bytes   |
| sorted/Uuid                | const+zstd3                   | 357,522      | bytes   |
| sorted/Uuid                | delta_varint                  | 709,800      | bytes   |
| sorted/Uuid                | delta_varint+zstd3            | 357,522      | bytes   |
| sorted/Uuid                | dict                          | 719,164      | bytes   |
| sorted/Uuid                | dict+zstd3                    | 380,224      | bytes   |
| sorted/Uuid                | ip_bin                        | 709,800      | bytes   |
| sorted/Uuid                | ip_bin+zstd3                  | 357,522      | bytes   |
| sorted/Uuid                | plain                         | 709,800      | bytes   |
| sorted/Uuid                | plain+zstd3                   | 357,522      | bytes   |
| sorted/Uuid                | uuid16                        | 283,920      | bytes   |
| sorted/Uuid                | uuid16+zstd3                  | 284,180      | bytes   |
| sorted/Uuid                | varint                        | 709,800      | bytes   |
| sorted/Uuid                | varint+zstd3                  | 357,522      | bytes   |
| coverage/plain             | applies_to_pct_of_plain_bytes | 100          | percent |
| coverage/const             | applies_to_pct_of_plain_bytes | 11.43        | percent |
| coverage/dict              | applies_to_pct_of_plain_bytes | 87.97        | percent |
| coverage/delta_varint      | applies_to_pct_of_plain_bytes | 8.82         | percent |
| coverage/varint            | applies_to_pct_of_plain_bytes | 8.82         | percent |
| coverage/bits              | applies_to_pct_of_plain_bytes | 0            | percent |
| coverage/uuid16            | applies_to_pct_of_plain_bytes | 1.26         | percent |
| coverage/ip_bin            | applies_to_pct_of_plain_bytes | 4.71         | percent |
| zstd level 1               | bytes                         | 8,499,653    | bytes   |
| zstd level 1               | compress_MBps                 | 929.2        | MB/s    |
| zstd level 3               | bytes                         | 8,071,494    | bytes   |
| zstd level 3               | compress_MBps                 | 691.8        | MB/s    |
| zstd level 9               | bytes                         | 7,097,167    | bytes   |
| zstd level 9               | compress_MBps                 | 141.6        | MB/s    |
| zstd level 19              | bytes                         | 6,414,585    | bytes   |
| zstd level 19              | compress_MBps                 | 4.84         | MB/s    |
| wordlist per_chunk         | bytes                         | 16,117,440   | bytes   |
| wordlist per_file          | bytes                         | 15,367,733   | bytes   |
| column:http_user_agent     | plain_bytes                   | 10,319,845   | bytes   |
| column:http_user_agent     | best_encoding                 | dict+zstd3   | name    |
| column:user_agent.original | plain_bytes                   | 5,463,611    | bytes   |
| column:user_agent.original | best_encoding                 | dict+zstd3   | name    |
| column:request_id          | plain_bytes                   | 2,952,180    | bytes   |
| column:request_id          | best_encoding                 | plain+zstd3  | name    |
| column:http_referer        | plain_bytes                   | 2,494,361    | bytes   |
| column:http_referer        | best_encoding                 | dict+zstd3   | name    |
| column:request_uri         | plain_bytes                   | 2,408,205    | bytes   |
| column:request_uri         | best_encoding                 | dict+zstd3   | name    |
| column:http.request.id     | plain_bytes                   | 1,879,488    | bytes   |
| column:http.request.id     | best_encoding                 | plain+zstd3  | name    |
| column:host.name           | plain_bytes                   | 1,789,453    | bytes   |
| column:host.name           | best_encoding                 | dict+zstd3   | name    |
| column:message             | plain_bytes                   | 1,703,325    | bytes   |
| column:message             | best_encoding                 | dict+zstd3   | name    |
| column:remote_addr         | plain_bytes                   | 1,442,010    | bytes   |
| column:remote_addr         | best_encoding                 | ip_bin+zstd3 | name    |
| column:url.path            | plain_bytes                   | 1,255,575    | bytes   |
| column:url.path            | best_encoding                 | dict+zstd3   | name    |
| column:http_user_agent     | clp_split_bytes               | 105,279      | bytes   |
| column:user_agent.original | clp_split_bytes               | 85,282       | bytes   |
| column:request_id          | clp_split_bytes               | 1,477,252    | bytes   |
| column:http_referer        | clp_split_bytes               | 86,918       | bytes   |
| column:request_uri         | clp_split_bytes               | 321,831      | bytes   |
| BEST_PER_CHUNK arrival     | bytes                         | 6,372,460    | bytes   |
| BEST_PER_CHUNK arrival     | ratio_to_raw                  | 0.06535      | ratio   |
| BEST_PER_CHUNK arrival     | ratio_to_gzip9                | 0.5808       | ratio   |
| BEST_PER_CHUNK sorted      | bytes                         | 6,665,725    | bytes   |
| BEST_PER_CHUNK sorted      | ratio_to_raw                  | 0.06836      | ratio   |
| BEST_PER_CHUNK sorted      | ratio_to_gzip9                | 0.6075       | ratio   |
| plain                      | bytes                         | 56,120,206   | bytes   |
| verdict                    | sort_saving_pct               | -4.602       | percent |

### `05_encodings` / dataset `farm[J80]`

| variant                    | metric                        | value        | unit    |
|----------------------------|-------------------------------|--------------|---------|
| units                      | templates chunked             | 77           | count   |
| arrival/Bool               | bits                          | 580          | bytes   |
| arrival/Bool               | bits+zstd3                    | 536          | bytes   |
| arrival/Bool               | const                         | 2,652        | bytes   |
| arrival/Bool               | const+zstd3                   | 685          | bytes   |
| arrival/Bool               | delta_varint                  | 2,909        | bytes   |
| arrival/Bool               | delta_varint+zstd3            | 696          | bytes   |
| arrival/Bool               | dict                          | 2,940        | bytes   |
| arrival/Bool               | dict+zstd3                    | 709          | bytes   |
| arrival/Bool               | ip_bin                        | 2,909        | bytes   |
| arrival/Bool               | ip_bin+zstd3                  | 696          | bytes   |
| arrival/Bool               | plain                         | 2,909        | bytes   |
| arrival/Bool               | plain+zstd3                   | 696          | bytes   |
| arrival/Bool               | uuid16                        | 2,909        | bytes   |
| arrival/Bool               | uuid16+zstd3                  | 696          | bytes   |
| arrival/Bool               | varint                        | 2,909        | bytes   |
| arrival/Bool               | varint+zstd3                  | 696          | bytes   |
| arrival/Datetime           | bits                          | 1,655,834    | bytes   |
| arrival/Datetime           | bits+zstd3                    | 816,097      | bytes   |
| arrival/Datetime           | const                         | 1,655,834    | bytes   |
| arrival/Datetime           | const+zstd3                   | 816,097      | bytes   |
| arrival/Datetime           | delta_varint                  | 854,195      | bytes   |
| arrival/Datetime           | delta_varint+zstd3            | 484,647      | bytes   |
| arrival/Datetime           | dict                          | 1,524,978    | bytes   |
| arrival/Datetime           | dict+zstd3                    | 1,001,580    | bytes   |
| arrival/Datetime           | ip_bin                        | 1,655,834    | bytes   |
| arrival/Datetime           | ip_bin+zstd3                  | 816,097      | bytes   |
| arrival/Datetime           | plain                         | 1,655,834    | bytes   |
| arrival/Datetime           | plain+zstd3                   | 816,097      | bytes   |
| arrival/Datetime           | uuid16                        | 1,655,834    | bytes   |
| arrival/Datetime           | uuid16+zstd3                  | 816,097      | bytes   |
| arrival/Datetime           | varint                        | 1,862,813    | bytes   |
| arrival/Datetime           | varint+zstd3                  | 677,101      | bytes   |
| arrival/Float64            | bits                          | 2,411,035    | bytes   |
| arrival/Float64            | bits+zstd3                    | 1,212,544    | bytes   |
| arrival/Float64            | const                         | 2,410,967    | bytes   |
| arrival/Float64            | const+zstd3                   | 1,212,545    | bytes   |
| arrival/Float64            | delta_varint                  | 2,411,035    | bytes   |
| arrival/Float64            | delta_varint+zstd3            | 1,212,544    | bytes   |
| arrival/Float64            | dict                          | 2,038,345    | bytes   |
| arrival/Float64            | dict+zstd3                    | 1,424,650    | bytes   |
| arrival/Float64            | ip_bin                        | 2,411,035    | bytes   |
| arrival/Float64            | ip_bin+zstd3                  | 1,212,544    | bytes   |
| arrival/Float64            | plain                         | 2,411,035    | bytes   |
| arrival/Float64            | plain+zstd3                   | 1,212,544    | bytes   |
| arrival/Float64            | uuid16                        | 2,411,035    | bytes   |
| arrival/Float64            | uuid16+zstd3                  | 1,212,544    | bytes   |
| arrival/Float64            | varint                        | 2,411,035    | bytes   |
| arrival/Float64            | varint+zstd3                  | 1,212,544    | bytes   |
| arrival/Int64              | bits                          | 3,300,959    | bytes   |
| arrival/Int64              | bits+zstd3                    | 501,036      | bytes   |
| arrival/Int64              | const                         | 3,178,847    | bytes   |
| arrival/Int64              | const+zstd3                   | 500,928      | bytes   |
| arrival/Int64              | delta_varint                  | 740,890      | bytes   |
| arrival/Int64              | delta_varint+zstd3            | 508,755      | bytes   |
| arrival/Int64              | dict                          | 1,329,313    | bytes   |
| arrival/Int64              | dict+zstd3                    | 580,057      | bytes   |
| arrival/Int64              | ip_bin                        | 3,300,959    | bytes   |
| arrival/Int64              | ip_bin+zstd3                  | 501,036      | bytes   |
| arrival/Int64              | plain                         | 3,300,959    | bytes   |
| arrival/Int64              | plain+zstd3                   | 501,036      | bytes   |
| arrival/Int64              | uuid16                        | 3,300,959    | bytes   |
| arrival/Int64              | uuid16+zstd3                  | 501,036      | bytes   |
| arrival/Int64              | varint                        | 854,047      | bytes   |
| arrival/Int64              | varint+zstd3                  | 455,186      | bytes   |
| arrival/Ip                 | bits                          | 2,642,633    | bytes   |
| arrival/Ip                 | bits+zstd3                    | 463,889      | bytes   |
| arrival/Ip                 | const                         | 2,642,633    | bytes   |
| arrival/Ip                 | const+zstd3                   | 463,889      | bytes   |
| arrival/Ip                 | delta_varint                  | 2,642,633    | bytes   |
| arrival/Ip                 | delta_varint+zstd3            | 463,889      | bytes   |
| arrival/Ip                 | dict                          | 667,256      | bytes   |
| arrival/Ip                 | dict+zstd3                    | 309,961      | bytes   |
| arrival/Ip                 | ip_bin                        | 817,330      | bytes   |
| arrival/Ip                 | ip_bin+zstd3                  | 253,474      | bytes   |
| arrival/Ip                 | plain                         | 2,642,633    | bytes   |
| arrival/Ip                 | plain+zstd3                   | 463,889      | bytes   |
| arrival/Ip                 | uuid16                        | 2,642,633    | bytes   |
| arrival/Ip                 | uuid16+zstd3                  | 463,889      | bytes   |
| arrival/Ip                 | varint                        | 2,642,633    | bytes   |
| arrival/Ip                 | varint+zstd3                  | 463,889      | bytes   |
| arrival/String             | bits                          | 45,469,255   | bytes   |
| arrival/String             | bits+zstd3                    | 5,014,202    | bytes   |
| arrival/String             | const                         | 40,835,205   | bytes   |
| arrival/String             | const+zstd3                   | 5,012,713    | bytes   |
| arrival/String             | delta_varint                  | 45,469,255   | bytes   |
| arrival/String             | delta_varint+zstd3            | 5,014,202    | bytes   |
| arrival/String             | dict                          | 9,482,635    | bytes   |
| arrival/String             | dict+zstd3                    | 3,871,592    | bytes   |
| arrival/String             | ip_bin                        | 45,469,255   | bytes   |
| arrival/String             | ip_bin+zstd3                  | 5,014,202    | bytes   |
| arrival/String             | plain                         | 45,469,255   | bytes   |
| arrival/String             | plain+zstd3                   | 5,014,202    | bytes   |
| arrival/String             | uuid16                        | 45,469,255   | bytes   |
| arrival/String             | uuid16+zstd3                  | 5,014,202    | bytes   |
| arrival/String             | varint                        | 45,469,255   | bytes   |
| arrival/String             | varint+zstd3                  | 5,014,202    | bytes   |
| arrival/Uuid               | bits                          | 709,800      | bytes   |
| arrival/Uuid               | bits+zstd3                    | 358,011      | bytes   |
| arrival/Uuid               | const                         | 709,800      | bytes   |
| arrival/Uuid               | const+zstd3                   | 358,011      | bytes   |
| arrival/Uuid               | delta_varint                  | 709,800      | bytes   |
| arrival/Uuid               | delta_varint+zstd3            | 358,011      | bytes   |
| arrival/Uuid               | dict                          | 719,227      | bytes   |
| arrival/Uuid               | dict+zstd3                    | 381,109      | bytes   |
| arrival/Uuid               | ip_bin                        | 709,800      | bytes   |
| arrival/Uuid               | ip_bin+zstd3                  | 358,011      | bytes   |
| arrival/Uuid               | plain                         | 709,800      | bytes   |
| arrival/Uuid               | plain+zstd3                   | 358,011      | bytes   |
| arrival/Uuid               | uuid16                        | 283,920      | bytes   |
| arrival/Uuid               | uuid16+zstd3                  | 282,929      | bytes   |
| arrival/Uuid               | varint                        | 709,800      | bytes   |
| arrival/Uuid               | varint+zstd3                  | 358,011      | bytes   |
| sorted/Bool                | bits                          | 580          | bytes   |
| sorted/Bool                | bits+zstd3                    | 115          | bytes   |
| sorted/Bool                | const                         | 2,652        | bytes   |
| sorted/Bool                | const+zstd3                   | 128          | bytes   |
| sorted/Bool                | delta_varint                  | 2,909        | bytes   |
| sorted/Bool                | delta_varint+zstd3            | 123          | bytes   |
| sorted/Bool                | dict                          | 2,940        | bytes   |
| sorted/Bool                | dict+zstd3                    | 158          | bytes   |
| sorted/Bool                | ip_bin                        | 2,909        | bytes   |
| sorted/Bool                | ip_bin+zstd3                  | 123          | bytes   |
| sorted/Bool                | plain                         | 2,909        | bytes   |
| sorted/Bool                | plain+zstd3                   | 123          | bytes   |
| sorted/Bool                | uuid16                        | 2,909        | bytes   |
| sorted/Bool                | uuid16+zstd3                  | 123          | bytes   |
| sorted/Bool                | varint                        | 2,909        | bytes   |
| sorted/Bool                | varint+zstd3                  | 123          | bytes   |
| sorted/Datetime            | bits                          | 1,655,834    | bytes   |
| sorted/Datetime            | bits+zstd3                    | 1,059,855    | bytes   |
| sorted/Datetime            | const                         | 1,655,834    | bytes   |
| sorted/Datetime            | const+zstd3                   | 1,059,855    | bytes   |
| sorted/Datetime            | delta_varint                  | 1,267,824    | bytes   |
| sorted/Datetime            | delta_varint+zstd3            | 984,514      | bytes   |
| sorted/Datetime            | dict                          | 1,524,978    | bytes   |
| sorted/Datetime            | dict+zstd3                    | 1,215,410    | bytes   |
| sorted/Datetime            | ip_bin                        | 1,655,834    | bytes   |
| sorted/Datetime            | ip_bin+zstd3                  | 1,059,855    | bytes   |
| sorted/Datetime            | plain                         | 1,655,834    | bytes   |
| sorted/Datetime            | plain+zstd3                   | 1,059,855    | bytes   |
| sorted/Datetime            | uuid16                        | 1,655,834    | bytes   |
| sorted/Datetime            | uuid16+zstd3                  | 1,059,855    | bytes   |
| sorted/Datetime            | varint                        | 1,862,813    | bytes   |
| sorted/Datetime            | varint+zstd3                  | 1,008,149    | bytes   |
| sorted/Float64             | bits                          | 2,411,035    | bytes   |
| sorted/Float64             | bits+zstd3                    | 1,314,544    | bytes   |
| sorted/Float64             | const                         | 2,410,967    | bytes   |
| sorted/Float64             | const+zstd3                   | 1,314,544    | bytes   |
| sorted/Float64             | delta_varint                  | 2,411,035    | bytes   |
| sorted/Float64             | delta_varint+zstd3            | 1,314,544    | bytes   |
| sorted/Float64             | dict                          | 2,038,345    | bytes   |
| sorted/Float64             | dict+zstd3                    | 1,528,941    | bytes   |
| sorted/Float64             | ip_bin                        | 2,411,035    | bytes   |
| sorted/Float64             | ip_bin+zstd3                  | 1,314,544    | bytes   |
| sorted/Float64             | plain                         | 2,411,035    | bytes   |
| sorted/Float64             | plain+zstd3                   | 1,314,544    | bytes   |
| sorted/Float64             | uuid16                        | 2,411,035    | bytes   |
| sorted/Float64             | uuid16+zstd3                  | 1,314,544    | bytes   |
| sorted/Float64             | varint                        | 2,411,035    | bytes   |
| sorted/Float64             | varint+zstd3                  | 1,314,544    | bytes   |
| sorted/Int64               | bits                          | 3,300,959    | bytes   |
| sorted/Int64               | bits+zstd3                    | 431,972      | bytes   |
| sorted/Int64               | const                         | 3,178,847    | bytes   |
| sorted/Int64               | const+zstd3                   | 431,863      | bytes   |
| sorted/Int64               | delta_varint                  | 662,318      | bytes   |
| sorted/Int64               | delta_varint+zstd3            | 410,834      | bytes   |
| sorted/Int64               | dict                          | 1,329,313    | bytes   |
| sorted/Int64               | dict+zstd3                    | 525,504      | bytes   |
| sorted/Int64               | ip_bin                        | 3,300,959    | bytes   |
| sorted/Int64               | ip_bin+zstd3                  | 431,972      | bytes   |
| sorted/Int64               | plain                         | 3,300,959    | bytes   |
| sorted/Int64               | plain+zstd3                   | 431,972      | bytes   |
| sorted/Int64               | uuid16                        | 3,300,959    | bytes   |
| sorted/Int64               | uuid16+zstd3                  | 431,972      | bytes   |
| sorted/Int64               | varint                        | 854,047      | bytes   |
| sorted/Int64               | varint+zstd3                  | 404,461      | bytes   |
| sorted/Ip                  | bits                          | 2,642,633    | bytes   |
| sorted/Ip                  | bits+zstd3                    | 512,147      | bytes   |
| sorted/Ip                  | const                         | 2,642,633    | bytes   |
| sorted/Ip                  | const+zstd3                   | 512,147      | bytes   |
| sorted/Ip                  | delta_varint                  | 2,642,633    | bytes   |
| sorted/Ip                  | delta_varint+zstd3            | 512,147      | bytes   |
| sorted/Ip                  | dict                          | 667,256      | bytes   |
| sorted/Ip                  | dict+zstd3                    | 308,058      | bytes   |
| sorted/Ip                  | ip_bin                        | 817,330      | bytes   |
| sorted/Ip                  | ip_bin+zstd3                  | 266,511      | bytes   |
| sorted/Ip                  | plain                         | 2,642,633    | bytes   |
| sorted/Ip                  | plain+zstd3                   | 512,147      | bytes   |
| sorted/Ip                  | uuid16                        | 2,642,633    | bytes   |
| sorted/Ip                  | uuid16+zstd3                  | 512,147      | bytes   |
| sorted/Ip                  | varint                        | 2,642,633    | bytes   |
| sorted/Ip                  | varint+zstd3                  | 512,147      | bytes   |
| sorted/String              | bits                          | 45,469,255   | bytes   |
| sorted/String              | bits+zstd3                    | 4,026,470    | bytes   |
| sorted/String              | const                         | 40,835,205   | bytes   |
| sorted/String              | const+zstd3                   | 4,024,981    | bytes   |
| sorted/String              | delta_varint                  | 45,469,255   | bytes   |
| sorted/String              | delta_varint+zstd3            | 4,026,470    | bytes   |
| sorted/String              | dict                          | 9,482,635    | bytes   |
| sorted/String              | dict+zstd3                    | 3,514,987    | bytes   |
| sorted/String              | ip_bin                        | 45,469,255   | bytes   |
| sorted/String              | ip_bin+zstd3                  | 4,026,470    | bytes   |
| sorted/String              | plain                         | 45,469,255   | bytes   |
| sorted/String              | plain+zstd3                   | 4,026,470    | bytes   |
| sorted/String              | uuid16                        | 45,469,255   | bytes   |
| sorted/String              | uuid16+zstd3                  | 4,026,470    | bytes   |
| sorted/String              | varint                        | 45,469,255   | bytes   |
| sorted/String              | varint+zstd3                  | 4,026,470    | bytes   |
| sorted/Uuid                | bits                          | 709,800      | bytes   |
| sorted/Uuid                | bits+zstd3                    | 357,985      | bytes   |
| sorted/Uuid                | const                         | 709,800      | bytes   |
| sorted/Uuid                | const+zstd3                   | 357,985      | bytes   |
| sorted/Uuid                | delta_varint                  | 709,800      | bytes   |
| sorted/Uuid                | delta_varint+zstd3            | 357,985      | bytes   |
| sorted/Uuid                | dict                          | 719,227      | bytes   |
| sorted/Uuid                | dict+zstd3                    | 380,989      | bytes   |
| sorted/Uuid                | ip_bin                        | 709,800      | bytes   |
| sorted/Uuid                | ip_bin+zstd3                  | 357,985      | bytes   |
| sorted/Uuid                | plain                         | 709,800      | bytes   |
| sorted/Uuid                | plain+zstd3                   | 357,985      | bytes   |
| sorted/Uuid                | uuid16                        | 283,920      | bytes   |
| sorted/Uuid                | uuid16+zstd3                  | 284,075      | bytes   |
| sorted/Uuid                | varint                        | 709,800      | bytes   |
| sorted/Uuid                | varint+zstd3                  | 357,985      | bytes   |
| coverage/plain             | applies_to_pct_of_plain_bytes | 100          | percent |
| coverage/const             | applies_to_pct_of_plain_bytes | 8.48         | percent |
| coverage/dict              | applies_to_pct_of_plain_bytes | 89.13        | percent |
| coverage/delta_varint      | applies_to_pct_of_plain_bytes | 8.82         | percent |
| coverage/varint            | applies_to_pct_of_plain_bytes | 8.82         | percent |
| coverage/bits              | applies_to_pct_of_plain_bytes | 0.01         | percent |
| coverage/uuid16            | applies_to_pct_of_plain_bytes | 1.26         | percent |
| coverage/ip_bin            | applies_to_pct_of_plain_bytes | 4.7          | percent |
| zstd level 1               | bytes                         | 8,378,524    | bytes   |
| zstd level 1               | compress_MBps                 | 873.8        | MB/s    |
| zstd level 3               | bytes                         | 8,088,445    | bytes   |
| zstd level 3               | compress_MBps                 | 469.1        | MB/s    |
| zstd level 9               | bytes                         | 7,089,447    | bytes   |
| zstd level 9               | compress_MBps                 | 130.1        | MB/s    |
| zstd level 19              | bytes                         | 6,436,300    | bytes   |
| zstd level 19              | compress_MBps                 | 5.29         | MB/s    |
| wordlist per_chunk         | bytes                         | 15,764,694   | bytes   |
| wordlist per_file          | bytes                         | 14,958,826   | bytes   |
| column:http_user_agent     | plain_bytes                   | 10,319,845   | bytes   |
| column:http_user_agent     | best_encoding                 | dict+zstd3   | name    |
| column:user_agent.original | plain_bytes                   | 5,463,611    | bytes   |
| column:user_agent.original | best_encoding                 | dict+zstd3   | name    |
| column:request_id          | plain_bytes                   | 2,964,141    | bytes   |
| column:request_id          | best_encoding                 | plain+zstd3  | name    |
| column:http_referer        | plain_bytes                   | 2,494,361    | bytes   |
| column:http_referer        | best_encoding                 | dict+zstd3   | name    |
| column:request_uri         | plain_bytes                   | 2,408,205    | bytes   |
| column:request_uri         | best_encoding                 | dict+zstd3   | name    |
| column:http.request.id     | plain_bytes                   | 1,879,488    | bytes   |
| column:http.request.id     | best_encoding                 | plain+zstd3  | name    |
| column:host.name           | plain_bytes                   | 1,789,453    | bytes   |
| column:host.name           | best_encoding                 | dict+zstd3   | name    |
| column:message             | plain_bytes                   | 1,703,325    | bytes   |
| column:message             | best_encoding                 | dict+zstd3   | name    |
| column:remote_addr         | plain_bytes                   | 1,442,010    | bytes   |
| column:remote_addr         | best_encoding                 | ip_bin+zstd3 | name    |
| column:url.path            | plain_bytes                   | 1,255,575    | bytes   |
| column:url.path            | best_encoding                 | dict+zstd3   | name    |
| column:http_user_agent     | clp_split_bytes               | 85,822       | bytes   |
| column:user_agent.original | clp_split_bytes               | 82,830       | bytes   |
| column:request_id          | clp_split_bytes               | 1,599,621    | bytes   |
| column:http_referer        | clp_split_bytes               | 61,867       | bytes   |
| column:request_uri         | clp_split_bytes               | 304,946      | bytes   |
| BEST_PER_CHUNK arrival     | bytes                         | 6,426,378    | bytes   |
| BEST_PER_CHUNK arrival     | ratio_to_raw                  | 0.0659       | ratio   |
| BEST_PER_CHUNK arrival     | ratio_to_gzip9                | 0.5857       | ratio   |
| BEST_PER_CHUNK sorted      | bytes                         | 6,630,279    | bytes   |
| BEST_PER_CHUNK sorted      | ratio_to_raw                  | 0.06799      | ratio   |
| BEST_PER_CHUNK sorted      | ratio_to_gzip9                | 0.6043       | ratio   |
| plain                      | bytes                         | 56,192,425   | bytes   |
| verdict                    | sort_saving_pct               | -3.173       | percent |

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
| zstd level 1           | compress_MBps                 | 686                | MB/s    |
| zstd level 3           | bytes                         | 8,163,020          | bytes   |
| zstd level 3           | compress_MBps                 | 389.3              | MB/s    |
| zstd level 9           | bytes                         | 7,609,358          | bytes   |
| zstd level 9           | compress_MBps                 | 86.98              | MB/s    |
| zstd level 19          | bytes                         | 7,114,711          | bytes   |
| zstd level 19          | compress_MBps                 | 3.57               | MB/s    |
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

### `05_encodings` / dataset `messy`

| variant                | metric                        | value              | unit    |
|------------------------|-------------------------------|--------------------|---------|
| arrival/Bool           | bits                          | 59                 | bytes   |
| arrival/Bool           | bits+zstd3                    | 86                 | bytes   |
| arrival/Bool           | const                         | 458                | bytes   |
| arrival/Bool           | const+zstd3                   | 200                | bytes   |
| arrival/Bool           | delta_varint                  | 458                | bytes   |
| arrival/Bool           | delta_varint+zstd3            | 200                | bytes   |
| arrival/Bool           | dict                          | 479                | bytes   |
| arrival/Bool           | dict+zstd3                    | 208                | bytes   |
| arrival/Bool           | ip_bin                        | 458                | bytes   |
| arrival/Bool           | ip_bin+zstd3                  | 200                | bytes   |
| arrival/Bool           | plain                         | 458                | bytes   |
| arrival/Bool           | plain+zstd3                   | 200                | bytes   |
| arrival/Bool           | uuid16                        | 458                | bytes   |
| arrival/Bool           | uuid16+zstd3                  | 200                | bytes   |
| arrival/Bool           | varint                        | 458                | bytes   |
| arrival/Bool           | varint+zstd3                  | 200                | bytes   |
| arrival/Datetime       | bits                          | 1,607,936          | bytes   |
| arrival/Datetime       | bits+zstd3                    | 1,236,573          | bytes   |
| arrival/Datetime       | const                         | 1,607,936          | bytes   |
| arrival/Datetime       | const+zstd3                   | 1,236,573          | bytes   |
| arrival/Datetime       | delta_varint                  | 1,399,083          | bytes   |
| arrival/Datetime       | delta_varint+zstd3            | 1,228,936          | bytes   |
| arrival/Datetime       | dict                          | 1,962,502          | bytes   |
| arrival/Datetime       | dict+zstd3                    | 1,614,161          | bytes   |
| arrival/Datetime       | ip_bin                        | 1,607,936          | bytes   |
| arrival/Datetime       | ip_bin+zstd3                  | 1,236,573          | bytes   |
| arrival/Datetime       | plain                         | 1,607,936          | bytes   |
| arrival/Datetime       | plain+zstd3                   | 1,236,573          | bytes   |
| arrival/Datetime       | uuid16                        | 1,607,936          | bytes   |
| arrival/Datetime       | uuid16+zstd3                  | 1,236,573          | bytes   |
| arrival/Datetime       | varint                        | 1,809,526          | bytes   |
| arrival/Datetime       | varint+zstd3                  | 1,189,796          | bytes   |
| arrival/Float64        | bits                          | 1,609,293          | bytes   |
| arrival/Float64        | bits+zstd3                    | 604,155            | bytes   |
| arrival/Float64        | const                         | 1,609,293          | bytes   |
| arrival/Float64        | const+zstd3                   | 604,155            | bytes   |
| arrival/Float64        | delta_varint                  | 1,609,293          | bytes   |
| arrival/Float64        | delta_varint+zstd3            | 604,155            | bytes   |
| arrival/Float64        | dict                          | 1,970,597          | bytes   |
| arrival/Float64        | dict+zstd3                    | 948,380            | bytes   |
| arrival/Float64        | ip_bin                        | 1,609,293          | bytes   |
| arrival/Float64        | ip_bin+zstd3                  | 604,155            | bytes   |
| arrival/Float64        | plain                         | 1,609,293          | bytes   |
| arrival/Float64        | plain+zstd3                   | 604,155            | bytes   |
| arrival/Float64        | uuid16                        | 1,609,293          | bytes   |
| arrival/Float64        | uuid16+zstd3                  | 604,155            | bytes   |
| arrival/Float64        | varint                        | 1,609,293          | bytes   |
| arrival/Float64        | varint+zstd3                  | 604,155            | bytes   |
| arrival/Int64          | bits                          | 10,528             | bytes   |
| arrival/Int64          | bits+zstd3                    | 2,391              | bytes   |
| arrival/Int64          | const                         | 10,528             | bytes   |
| arrival/Int64          | const+zstd3                   | 2,391              | bytes   |
| arrival/Int64          | delta_varint                  | 2,696              | bytes   |
| arrival/Int64          | delta_varint+zstd3            | 2,149              | bytes   |
| arrival/Int64          | dict                          | 4,320              | bytes   |
| arrival/Int64          | dict+zstd3                    | 1,924              | bytes   |
| arrival/Int64          | ip_bin                        | 10,528             | bytes   |
| arrival/Int64          | ip_bin+zstd3                  | 2,391              | bytes   |
| arrival/Int64          | plain                         | 10,528             | bytes   |
| arrival/Int64          | plain+zstd3                   | 2,391              | bytes   |
| arrival/Int64          | uuid16                        | 10,528             | bytes   |
| arrival/Int64          | uuid16+zstd3                  | 2,391              | bytes   |
| arrival/Int64          | varint                        | 3,027              | bytes   |
| arrival/Int64          | varint+zstd3                  | 2,047              | bytes   |
| arrival/Ip             | bits                          | 47,548             | bytes   |
| arrival/Ip             | bits+zstd3                    | 20,508             | bytes   |
| arrival/Ip             | const                         | 47,548             | bytes   |
| arrival/Ip             | const+zstd3                   | 20,508             | bytes   |
| arrival/Ip             | delta_varint                  | 47,548             | bytes   |
| arrival/Ip             | delta_varint+zstd3            | 20,508             | bytes   |
| arrival/Ip             | dict                          | 47,548             | bytes   |
| arrival/Ip             | dict+zstd3                    | 20,508             | bytes   |
| arrival/Ip             | ip_bin                        | 13,785             | bytes   |
| arrival/Ip             | ip_bin+zstd3                  | 12,980             | bytes   |
| arrival/Ip             | plain                         | 47,548             | bytes   |
| arrival/Ip             | plain+zstd3                   | 20,508             | bytes   |
| arrival/Ip             | uuid16                        | 47,548             | bytes   |
| arrival/Ip             | uuid16+zstd3                  | 20,508             | bytes   |
| arrival/Ip             | varint                        | 47,548             | bytes   |
| arrival/Ip             | varint+zstd3                  | 20,508             | bytes   |
| arrival/String         | bits                          | 33,352,188         | bytes   |
| arrival/String         | bits+zstd3                    | 8,900,644          | bytes   |
| arrival/String         | const                         | 28,514,901         | bytes   |
| arrival/String         | const+zstd3                   | 8,899,389          | bytes   |
| arrival/String         | delta_varint                  | 33,352,188         | bytes   |
| arrival/String         | delta_varint+zstd3            | 8,900,644          | bytes   |
| arrival/String         | dict                          | 19,673,975         | bytes   |
| arrival/String         | dict+zstd3                    | 9,667,840          | bytes   |
| arrival/String         | ip_bin                        | 33,352,188         | bytes   |
| arrival/String         | ip_bin+zstd3                  | 8,900,644          | bytes   |
| arrival/String         | plain                         | 33,352,188         | bytes   |
| arrival/String         | plain+zstd3                   | 8,900,644          | bytes   |
| arrival/String         | uuid16                        | 33,352,188         | bytes   |
| arrival/String         | uuid16+zstd3                  | 8,900,644          | bytes   |
| arrival/String         | varint                        | 33,352,188         | bytes   |
| arrival/String         | varint+zstd3                  | 8,900,644          | bytes   |
| arrival/Uuid           | bits                          | 99,160             | bytes   |
| arrival/Uuid           | bits+zstd3                    | 51,040             | bytes   |
| arrival/Uuid           | const                         | 99,160             | bytes   |
| arrival/Uuid           | const+zstd3                   | 51,040             | bytes   |
| arrival/Uuid           | delta_varint                  | 99,160             | bytes   |
| arrival/Uuid           | delta_varint+zstd3            | 51,040             | bytes   |
| arrival/Uuid           | dict                          | 99,160             | bytes   |
| arrival/Uuid           | dict+zstd3                    | 51,040             | bytes   |
| arrival/Uuid           | ip_bin                        | 99,160             | bytes   |
| arrival/Uuid           | ip_bin+zstd3                  | 51,040             | bytes   |
| arrival/Uuid           | plain                         | 99,160             | bytes   |
| arrival/Uuid           | plain+zstd3                   | 51,040             | bytes   |
| arrival/Uuid           | uuid16                        | 39,664             | bytes   |
| arrival/Uuid           | uuid16+zstd3                  | 39,794             | bytes   |
| arrival/Uuid           | varint                        | 99,160             | bytes   |
| arrival/Uuid           | varint+zstd3                  | 51,040             | bytes   |
| sorted/Bool            | bits                          | 59                 | bytes   |
| sorted/Bool            | bits+zstd3                    | 63                 | bytes   |
| sorted/Bool            | const                         | 458                | bytes   |
| sorted/Bool            | const+zstd3                   | 62                 | bytes   |
| sorted/Bool            | delta_varint                  | 458                | bytes   |
| sorted/Bool            | delta_varint+zstd3            | 62                 | bytes   |
| sorted/Bool            | dict                          | 479                | bytes   |
| sorted/Bool            | dict+zstd3                    | 80                 | bytes   |
| sorted/Bool            | ip_bin                        | 458                | bytes   |
| sorted/Bool            | ip_bin+zstd3                  | 62                 | bytes   |
| sorted/Bool            | plain                         | 458                | bytes   |
| sorted/Bool            | plain+zstd3                   | 62                 | bytes   |
| sorted/Bool            | uuid16                        | 458                | bytes   |
| sorted/Bool            | uuid16+zstd3                  | 62                 | bytes   |
| sorted/Bool            | varint                        | 458                | bytes   |
| sorted/Bool            | varint+zstd3                  | 62                 | bytes   |
| sorted/Datetime        | bits                          | 1,607,936          | bytes   |
| sorted/Datetime        | bits+zstd3                    | 1,238,494          | bytes   |
| sorted/Datetime        | const                         | 1,607,936          | bytes   |
| sorted/Datetime        | const+zstd3                   | 1,238,494          | bytes   |
| sorted/Datetime        | delta_varint                  | 1,393,188          | bytes   |
| sorted/Datetime        | delta_varint+zstd3            | 1,219,257          | bytes   |
| sorted/Datetime        | dict                          | 1,962,502          | bytes   |
| sorted/Datetime        | dict+zstd3                    | 1,612,493          | bytes   |
| sorted/Datetime        | ip_bin                        | 1,607,936          | bytes   |
| sorted/Datetime        | ip_bin+zstd3                  | 1,238,494          | bytes   |
| sorted/Datetime        | plain                         | 1,607,936          | bytes   |
| sorted/Datetime        | plain+zstd3                   | 1,238,494          | bytes   |
| sorted/Datetime        | uuid16                        | 1,607,936          | bytes   |
| sorted/Datetime        | uuid16+zstd3                  | 1,238,494          | bytes   |
| sorted/Datetime        | varint                        | 1,809,526          | bytes   |
| sorted/Datetime        | varint+zstd3                  | 1,189,786          | bytes   |
| sorted/Float64         | bits                          | 1,609,293          | bytes   |
| sorted/Float64         | bits+zstd3                    | 604,236            | bytes   |
| sorted/Float64         | const                         | 1,609,293          | bytes   |
| sorted/Float64         | const+zstd3                   | 604,236            | bytes   |
| sorted/Float64         | delta_varint                  | 1,609,293          | bytes   |
| sorted/Float64         | delta_varint+zstd3            | 604,236            | bytes   |
| sorted/Float64         | dict                          | 1,970,597          | bytes   |
| sorted/Float64         | dict+zstd3                    | 948,425            | bytes   |
| sorted/Float64         | ip_bin                        | 1,609,293          | bytes   |
| sorted/Float64         | ip_bin+zstd3                  | 604,236            | bytes   |
| sorted/Float64         | plain                         | 1,609,293          | bytes   |
| sorted/Float64         | plain+zstd3                   | 604,236            | bytes   |
| sorted/Float64         | uuid16                        | 1,609,293          | bytes   |
| sorted/Float64         | uuid16+zstd3                  | 604,236            | bytes   |
| sorted/Float64         | varint                        | 1,609,293          | bytes   |
| sorted/Float64         | varint+zstd3                  | 604,236            | bytes   |
| sorted/Int64           | bits                          | 10,528             | bytes   |
| sorted/Int64           | bits+zstd3                    | 2,334              | bytes   |
| sorted/Int64           | const                         | 10,528             | bytes   |
| sorted/Int64           | const+zstd3                   | 2,334              | bytes   |
| sorted/Int64           | delta_varint                  | 2,555              | bytes   |
| sorted/Int64           | delta_varint+zstd3            | 2,023              | bytes   |
| sorted/Int64           | dict                          | 4,320              | bytes   |
| sorted/Int64           | dict+zstd3                    | 1,901              | bytes   |
| sorted/Int64           | ip_bin                        | 10,528             | bytes   |
| sorted/Int64           | ip_bin+zstd3                  | 2,334              | bytes   |
| sorted/Int64           | plain                         | 10,528             | bytes   |
| sorted/Int64           | plain+zstd3                   | 2,334              | bytes   |
| sorted/Int64           | uuid16                        | 10,528             | bytes   |
| sorted/Int64           | uuid16+zstd3                  | 2,334              | bytes   |
| sorted/Int64           | varint                        | 3,027              | bytes   |
| sorted/Int64           | varint+zstd3                  | 2,014              | bytes   |
| sorted/Ip              | bits                          | 47,548             | bytes   |
| sorted/Ip              | bits+zstd3                    | 20,469             | bytes   |
| sorted/Ip              | const                         | 47,548             | bytes   |
| sorted/Ip              | const+zstd3                   | 20,469             | bytes   |
| sorted/Ip              | delta_varint                  | 47,548             | bytes   |
| sorted/Ip              | delta_varint+zstd3            | 20,469             | bytes   |
| sorted/Ip              | dict                          | 47,548             | bytes   |
| sorted/Ip              | dict+zstd3                    | 20,469             | bytes   |
| sorted/Ip              | ip_bin                        | 13,785             | bytes   |
| sorted/Ip              | ip_bin+zstd3                  | 12,985             | bytes   |
| sorted/Ip              | plain                         | 47,548             | bytes   |
| sorted/Ip              | plain+zstd3                   | 20,469             | bytes   |
| sorted/Ip              | uuid16                        | 47,548             | bytes   |
| sorted/Ip              | uuid16+zstd3                  | 20,469             | bytes   |
| sorted/Ip              | varint                        | 47,548             | bytes   |
| sorted/Ip              | varint+zstd3                  | 20,469             | bytes   |
| sorted/String          | bits                          | 33,352,188         | bytes   |
| sorted/String          | bits+zstd3                    | 8,197,443          | bytes   |
| sorted/String          | const                         | 28,514,901         | bytes   |
| sorted/String          | const+zstd3                   | 8,196,188          | bytes   |
| sorted/String          | delta_varint                  | 33,352,188         | bytes   |
| sorted/String          | delta_varint+zstd3            | 8,197,443          | bytes   |
| sorted/String          | dict                          | 19,673,975         | bytes   |
| sorted/String          | dict+zstd3                    | 9,454,701          | bytes   |
| sorted/String          | ip_bin                        | 33,352,188         | bytes   |
| sorted/String          | ip_bin+zstd3                  | 8,197,443          | bytes   |
| sorted/String          | plain                         | 33,352,188         | bytes   |
| sorted/String          | plain+zstd3                   | 8,197,443          | bytes   |
| sorted/String          | uuid16                        | 33,352,188         | bytes   |
| sorted/String          | uuid16+zstd3                  | 8,197,443          | bytes   |
| sorted/String          | varint                        | 33,352,188         | bytes   |
| sorted/String          | varint+zstd3                  | 8,197,443          | bytes   |
| sorted/Uuid            | bits                          | 99,160             | bytes   |
| sorted/Uuid            | bits+zstd3                    | 50,997             | bytes   |
| sorted/Uuid            | const                         | 99,160             | bytes   |
| sorted/Uuid            | const+zstd3                   | 50,997             | bytes   |
| sorted/Uuid            | delta_varint                  | 99,160             | bytes   |
| sorted/Uuid            | delta_varint+zstd3            | 50,997             | bytes   |
| sorted/Uuid            | dict                          | 99,160             | bytes   |
| sorted/Uuid            | dict+zstd3                    | 50,997             | bytes   |
| sorted/Uuid            | ip_bin                        | 99,160             | bytes   |
| sorted/Uuid            | ip_bin+zstd3                  | 50,997             | bytes   |
| sorted/Uuid            | plain                         | 99,160             | bytes   |
| sorted/Uuid            | plain+zstd3                   | 50,997             | bytes   |
| sorted/Uuid            | uuid16                        | 39,664             | bytes   |
| sorted/Uuid            | uuid16+zstd3                  | 39,794             | bytes   |
| sorted/Uuid            | varint                        | 99,160             | bytes   |
| sorted/Uuid            | varint+zstd3                  | 50,997             | bytes   |
| coverage/plain         | applies_to_pct_of_plain_bytes | 100                | percent |
| coverage/const         | applies_to_pct_of_plain_bytes | 13.18              | percent |
| coverage/dict          | applies_to_pct_of_plain_bytes | 94.7               | percent |
| coverage/delta_varint  | applies_to_pct_of_plain_bytes | 4.41               | percent |
| coverage/varint        | applies_to_pct_of_plain_bytes | 4.41               | percent |
| coverage/bits          | applies_to_pct_of_plain_bytes | 0                  | percent |
| coverage/uuid16        | applies_to_pct_of_plain_bytes | 0.27               | percent |
| coverage/ip_bin        | applies_to_pct_of_plain_bytes | 0.13               | percent |
| zstd level 1           | bytes                         | 11,775,929         | bytes   |
| zstd level 1           | compress_MBps                 | 566.8              | MB/s    |
| zstd level 3           | bytes                         | 11,221,503         | bytes   |
| zstd level 3           | compress_MBps                 | 338.3              | MB/s    |
| zstd level 9           | bytes                         | 10,609,898         | bytes   |
| zstd level 9           | compress_MBps                 | 71.19              | MB/s    |
| zstd level 19          | bytes                         | 9,616,508          | bytes   |
| zstd level 19          | compress_MBps                 | 3.56               | MB/s    |
| wordlist per_chunk     | bytes                         | 23,758,581         | bytes   |
| wordlist per_file      | bytes                         | 23,758,581         | bytes   |
| column:trace_id        | plain_bytes                   | 7,943,876          | bytes   |
| column:trace_id        | best_encoding                 | uuid16+zstd3       | name    |
| column:tags            | plain_bytes                   | 4,330,972          | bytes   |
| column:tags            | best_encoding                 | dict+zstd3         | name    |
| column:client.ip       | plain_bytes                   | 3,440,226          | bytes   |
| column:client.ip       | best_encoding                 | ip_bin+zstd3       | name    |
| column:event           | plain_bytes                   | 2,766,289          | bytes   |
| column:event           | best_encoding                 | const              | name    |
| column:region          | plain_bytes                   | 2,666,800          | bytes   |
| column:region          | best_encoding                 | dict+zstd3         | name    |
| column:duration_ms     | plain_bytes                   | 2,139,758          | bytes   |
| column:duration_ms     | best_encoding                 | plain+zstd3        | name    |
| column:service         | plain_bytes                   | 2,075,148          | bytes   |
| column:service         | best_encoding                 | const              | name    |
| column:user_id         | plain_bytes                   | 1,981,845          | bytes   |
| column:user_id         | best_encoding                 | delta_varint+zstd3 | name    |
| column:retry           | plain_bytes                   | 1,754,913          | bytes   |
| column:retry           | best_encoding                 | dict+zstd3         | name    |
| column:level           | plain_bytes                   | 1,666,413          | bytes   |
| column:level           | best_encoding                 | dict+zstd3         | name    |
| column:trace_id        | clp_split_bytes               | 4,194,918          | bytes   |
| column:tags            | clp_split_bytes               | 210,958            | bytes   |
| column:client.ip       | clp_split_bytes               | 1,364,225          | bytes   |
| column:event           | clp_split_bytes               | 2,316              | bytes   |
| column:region          | clp_split_bytes               | 67,464             | bytes   |
| BEST_PER_CHUNK arrival | bytes                         | 9,828,235          | bytes   |
| BEST_PER_CHUNK arrival | ratio_to_raw                  | 0.1561             | ratio   |
| BEST_PER_CHUNK arrival | ratio_to_gzip9                | 0.7634             | ratio   |
| BEST_PER_CHUNK sorted  | bytes                         | 9,612,739          | bytes   |
| BEST_PER_CHUNK sorted  | ratio_to_raw                  | 0.1527             | ratio   |
| BEST_PER_CHUNK sorted  | ratio_to_gzip9                | 0.7466             | ratio   |
| plain                  | bytes                         | 36,727,111         | bytes   |
| verdict                | sort_saving_pct               | 2.193              | percent |

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
| zstd level 1            | compress_MBps                 | 1,071              | MB/s    |
| zstd level 3            | bytes                         | 3,136,257          | bytes   |
| zstd level 3            | compress_MBps                 | 775.8              | MB/s    |
| zstd level 9            | bytes                         | 2,814,740          | bytes   |
| zstd level 9            | compress_MBps                 | 149.2              | MB/s    |
| zstd level 19           | bytes                         | 2,586,595          | bytes   |
| zstd level 19           | compress_MBps                 | 6.23               | MB/s    |
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
- median of 3 runs; zstd is a C library and this is its speed, not kibbr's
- a per-file word list stores each distinct value once for the whole column; a per-chunk one repeats it, but indexes into a smaller list
- type String
- 105,279 bytes, 0.0102 of plain
- 85,394 bytes, 0.0156 of plain
- 1,477,261 bytes, 0.5004 of plain
- 86,918 bytes, 0.0348 of plain
- 321,871 bytes, 0.1337 of plain
- 959,275 bytes, 0.5104 of plain
- 24,745 bytes, 0.0138 of plain
- 30,439 bytes, 0.0179 of plain
- type Ip
- 135,947 bytes, 0.0943 of plain
- 165,212 bytes, 0.1316 of plain
- fixed part through a word list, each changing part its own column; 1.0000 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9987 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9999 of the best encoding above
- the smallest applicable encoding for every chunk of every column
- `.col` plain chunk sizes, no codec
- BEST_PER_CHUNK sorted against BEST_PER_CHUNK in arrival order
- 85,822 bytes, 0.0083 of plain
- 82,864 bytes, 0.0152 of plain
- 1,599,621 bytes, 0.5397 of plain
- 61,867 bytes, 0.0248 of plain
- 304,946 bytes, 0.1266 of plain
- 947,820 bytes, 0.5043 of plain
- 25,657 bytes, 0.0143 of plain
- 33,826 bytes, 0.0199 of plain
- 112,160 bytes, 0.0778 of plain
- 172,472 bytes, 0.1374 of plain
- fixed part through a word list, each changing part its own column; 0.9996 of the best encoding above
- 16,848 bytes, 0.0065 of plain
- 149,477 bytes, 0.0880 of plain
- type Uuid
- 535,600 bytes, 0.4000 of plain
- 528,288 bytes, 0.4000 of plain
- 321,990 bytes, 0.2786 of plain
- 227,805 bytes, 0.2355 of plain
- 171,263 bytes, 0.1796 of plain
- type Datetime
- 825,456 bytes, 0.8838 of plain
- 52,942 bytes, 0.0593 of plain
- 80,096 bytes, 0.0932 of plain
- fixed part through a word list, each changing part its own column; 0.9887 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.8759 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.7953 of the best encoding above
- fixed part through a word list, each changing part its own column; 0.9991 of the best encoding above
- 4,195,048 bytes, 0.5281 of plain
- 210,958 bytes, 0.0487 of plain
- 1,364,235 bytes, 0.3966 of plain
- 2,316 bytes, 0.0008 of plain
- 67,464 bytes, 0.0253 of plain
- type Float64
- 780,079 bytes, 0.3646 of plain
- 1,834 bytes, 0.0009 of plain
- 736,447 bytes, 0.3716 of plain
- 43,900 bytes, 0.0250 of plain
- 73,521 bytes, 0.0441 of plain
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

Run 2026-09-19T10:05:36+00:00, git `90152d2`, 612 rows.

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
| words:user.email     | rare_skipped_pct             |     98.52   | percent |
| words:user.email     | never_skipped_pct            |     98.76   | percent |
| words:user.email     | filter_bytes                 |  42054      | bytes   |
| words:customer.email | common_skipped_pct           |     96.18   | percent |
| words:customer.email | rare_skipped_pct             |     97.81   | percent |
| words:customer.email | never_skipped_pct            |     99.12   | percent |
| words:customer.email | filter_bytes                 |  41280      | bytes   |
| words:http.path      | common_skipped_pct           |      0      | percent |
| words:http.path      | rare_skipped_pct             |      0      | percent |
| words:http.path      | never_skipped_pct            |     99      | percent |
| words:http.path      | filter_bytes                 |   1584      | bytes   |

### `06_bloom` / dataset `messy`

| variant            | metric                       |       value | unit    |
|--------------------|------------------------------|-------------|---------|
| trace_id@8bits     | row_groups                   |    130      | count   |
| trace_id@8bits     | filter_bytes                 | 200000      | bytes   |
| trace_id@8bits     | k                            |      6      | count   |
| trace_id@8bits     | misses_generated             |   1000      | count   |
| trace_id@8bits     | misses_rejected_out_of_range |      0      | count   |
| trace_id@8bits     | present_bloom_skipped_pct    |     97.08   | percent |
| trace_id@8bits     | present_minmax_skipped_pct   |      0.3908 | percent |
| trace_id@8bits     | present_ideal_skipped_pct    |     99.23   | percent |
| trace_id@8bits     | absent_bloom_skipped_pct     |     97.81   | percent |
| trace_id@8bits     | absent_minmax_skipped_pct    |      0.3254 | percent |
| trace_id@8bits     | absent_ideal_skipped_pct     |    100      | percent |
| trace_id@8bits     | share_of_best_total_pct      |      2.035  | percent |
| trace_id@10bits    | row_groups                   |    130      | count   |
| trace_id@10bits    | filter_bytes                 | 250045      | bytes   |
| trace_id@10bits    | k                            |      7      | count   |
| trace_id@10bits    | misses_generated             |   1000      | count   |
| trace_id@10bits    | misses_rejected_out_of_range |      0      | count   |
| trace_id@10bits    | present_bloom_skipped_pct    |     98.45   | percent |
| trace_id@10bits    | present_minmax_skipped_pct   |      0.3908 | percent |
| trace_id@10bits    | present_ideal_skipped_pct    |     99.23   | percent |
| trace_id@10bits    | absent_bloom_skipped_pct     |     99.16   | percent |
| trace_id@10bits    | absent_minmax_skipped_pct    |      0.3254 | percent |
| trace_id@10bits    | absent_ideal_skipped_pct     |    100      | percent |
| trace_id@10bits    | share_of_best_total_pct      |      2.544  | percent |
| trace_id@16bits    | row_groups                   |    130      | count   |
| trace_id@16bits    | filter_bytes                 | 400000      | bytes   |
| trace_id@16bits    | k                            |     11      | count   |
| trace_id@16bits    | misses_generated             |   1000      | count   |
| trace_id@16bits    | misses_rejected_out_of_range |      0      | count   |
| trace_id@16bits    | present_bloom_skipped_pct    |     99.18   | percent |
| trace_id@16bits    | present_minmax_skipped_pct   |      0.3908 | percent |
| trace_id@16bits    | present_ideal_skipped_pct    |     99.23   | percent |
| trace_id@16bits    | absent_bloom_skipped_pct     |     99.94   | percent |
| trace_id@16bits    | absent_minmax_skipped_pct    |      0.3254 | percent |
| trace_id@16bits    | absent_ideal_skipped_pct     |    100      | percent |
| trace_id@16bits    | share_of_best_total_pct      |      4.07   | percent |
| client.ip@8bits    | row_groups                   |    130      | count   |
| client.ip@8bits    | filter_bytes                 | 200000      | bytes   |
| client.ip@8bits    | k                            |      6      | count   |
| client.ip@8bits    | misses_generated             |   1000      | count   |
| client.ip@8bits    | misses_rejected_out_of_range |      0      | count   |
| client.ip@8bits    | present_bloom_skipped_pct    |     97.12   | percent |
| client.ip@8bits    | present_minmax_skipped_pct   |      0.2285 | percent |
| client.ip@8bits    | present_ideal_skipped_pct    |     99.23   | percent |
| client.ip@8bits    | absent_bloom_skipped_pct     |     97.78   | percent |
| client.ip@8bits    | absent_minmax_skipped_pct    |      0.3046 | percent |
| client.ip@8bits    | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@8bits    | share_of_best_total_pct      |      2.035  | percent |
| client.ip@10bits   | row_groups                   |    130      | count   |
| client.ip@10bits   | filter_bytes                 | 250045      | bytes   |
| client.ip@10bits   | k                            |      7      | count   |
| client.ip@10bits   | misses_generated             |   1000      | count   |
| client.ip@10bits   | misses_rejected_out_of_range |      0      | count   |
| client.ip@10bits   | present_bloom_skipped_pct    |     98.41   | percent |
| client.ip@10bits   | present_minmax_skipped_pct   |      0.2285 | percent |
| client.ip@10bits   | present_ideal_skipped_pct    |     99.23   | percent |
| client.ip@10bits   | absent_bloom_skipped_pct     |     99.19   | percent |
| client.ip@10bits   | absent_minmax_skipped_pct    |      0.3046 | percent |
| client.ip@10bits   | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@10bits   | share_of_best_total_pct      |      2.544  | percent |
| client.ip@16bits   | row_groups                   |    130      | count   |
| client.ip@16bits   | filter_bytes                 | 400000      | bytes   |
| client.ip@16bits   | k                            |     11      | count   |
| client.ip@16bits   | misses_generated             |   1000      | count   |
| client.ip@16bits   | misses_rejected_out_of_range |      0      | count   |
| client.ip@16bits   | present_bloom_skipped_pct    |     99.18   | percent |
| client.ip@16bits   | present_minmax_skipped_pct   |      0.2285 | percent |
| client.ip@16bits   | present_ideal_skipped_pct    |     99.23   | percent |
| client.ip@16bits   | absent_bloom_skipped_pct     |     99.95   | percent |
| client.ip@16bits   | absent_minmax_skipped_pct    |      0.3046 | percent |
| client.ip@16bits   | absent_ideal_skipped_pct     |    100      | percent |
| client.ip@16bits   | share_of_best_total_pct      |      4.07   | percent |
| duration_ms@8bits  | row_groups                   |    130      | count   |
| duration_ms@8bits  | filter_bytes                 | 199234      | bytes   |
| duration_ms@8bits  | k                            |      6      | count   |
| duration_ms@8bits  | misses_generated             |   1000      | count   |
| duration_ms@8bits  | misses_rejected_out_of_range |      0      | count   |
| duration_ms@8bits  | present_bloom_skipped_pct    |     97      | percent |
| duration_ms@8bits  | present_minmax_skipped_pct   |      0.1738 | percent |
| duration_ms@8bits  | present_ideal_skipped_pct    |     99.06   | percent |
| duration_ms@8bits  | absent_bloom_skipped_pct     |     97.91   | percent |
| duration_ms@8bits  | absent_minmax_skipped_pct    |      0.1608 | percent |
| duration_ms@8bits  | absent_ideal_skipped_pct     |    100      | percent |
| duration_ms@8bits  | share_of_best_total_pct      |      2.027  | percent |
| duration_ms@10bits | row_groups                   |    130      | count   |
| duration_ms@10bits | filter_bytes                 | 249097      | bytes   |
| duration_ms@10bits | k                            |      7      | count   |
| duration_ms@10bits | misses_generated             |   1000      | count   |
| duration_ms@10bits | misses_rejected_out_of_range |      0      | count   |
| duration_ms@10bits | present_bloom_skipped_pct    |     98.31   | percent |
| duration_ms@10bits | present_minmax_skipped_pct   |      0.1738 | percent |
| duration_ms@10bits | present_ideal_skipped_pct    |     99.06   | percent |
| duration_ms@10bits | absent_bloom_skipped_pct     |     99.23   | percent |
| duration_ms@10bits | absent_minmax_skipped_pct    |      0.1608 | percent |
| duration_ms@10bits | absent_ideal_skipped_pct     |    100      | percent |
| duration_ms@10bits | share_of_best_total_pct      |      2.534  | percent |
| duration_ms@16bits | row_groups                   |    130      | count   |
| duration_ms@16bits | filter_bytes                 | 398468      | bytes   |
| duration_ms@16bits | k                            |     11      | count   |
| duration_ms@16bits | misses_generated             |   1000      | count   |
| duration_ms@16bits | misses_rejected_out_of_range |      0      | count   |
| duration_ms@16bits | present_bloom_skipped_pct    |     99      | percent |
| duration_ms@16bits | present_minmax_skipped_pct   |      0.1738 | percent |
| duration_ms@16bits | present_ideal_skipped_pct    |     99.06   | percent |
| duration_ms@16bits | absent_bloom_skipped_pct     |     99.95   | percent |
| duration_ms@16bits | absent_minmax_skipped_pct    |      0.1608 | percent |
| duration_ms@16bits | absent_ideal_skipped_pct     |    100      | percent |
| duration_ms@16bits | share_of_best_total_pct      |      4.054  | percent |
| user_id@8bits      | row_groups                   |    130      | count   |
| user_id@8bits      | filter_bytes                 | 200000      | bytes   |
| user_id@8bits      | k                            |      6      | count   |
| user_id@8bits      | misses_generated             |   1000      | count   |
| user_id@8bits      | misses_rejected_out_of_range |      0      | count   |
| user_id@8bits      | present_bloom_skipped_pct    |     97.02   | percent |
| user_id@8bits      | present_minmax_skipped_pct   |      0.4715 | percent |
| user_id@8bits      | present_ideal_skipped_pct    |     99.15   | percent |
| user_id@8bits      | absent_bloom_skipped_pct     |     97.83   | percent |
| user_id@8bits      | absent_minmax_skipped_pct    |      0.4454 | percent |
| user_id@8bits      | absent_ideal_skipped_pct     |    100      | percent |
| user_id@8bits      | share_of_best_total_pct      |      2.035  | percent |
| user_id@10bits     | row_groups                   |    130      | count   |
| user_id@10bits     | filter_bytes                 | 250045      | bytes   |
| user_id@10bits     | k                            |      7      | count   |
| user_id@10bits     | misses_generated             |   1000      | count   |
| user_id@10bits     | misses_rejected_out_of_range |      0      | count   |
| user_id@10bits     | present_bloom_skipped_pct    |     98.3    | percent |
| user_id@10bits     | present_minmax_skipped_pct   |      0.4715 | percent |
| user_id@10bits     | present_ideal_skipped_pct    |     99.15   | percent |
| user_id@10bits     | absent_bloom_skipped_pct     |     99.22   | percent |
| user_id@10bits     | absent_minmax_skipped_pct    |      0.4454 | percent |
| user_id@10bits     | absent_ideal_skipped_pct     |    100      | percent |
| user_id@10bits     | share_of_best_total_pct      |      2.544  | percent |
| user_id@16bits     | row_groups                   |    130      | count   |
| user_id@16bits     | filter_bytes                 | 400000      | bytes   |
| user_id@16bits     | k                            |     11      | count   |
| user_id@16bits     | misses_generated             |   1000      | count   |
| user_id@16bits     | misses_rejected_out_of_range |      0      | count   |
| user_id@16bits     | present_bloom_skipped_pct    |     99.09   | percent |
| user_id@16bits     | present_minmax_skipped_pct   |      0.4715 | percent |
| user_id@16bits     | present_ideal_skipped_pct    |     99.15   | percent |
| user_id@16bits     | absent_bloom_skipped_pct     |     99.94   | percent |
| user_id@16bits     | absent_minmax_skipped_pct    |      0.4454 | percent |
| user_id@16bits     | absent_ideal_skipped_pct     |    100      | percent |
| user_id@16bits     | share_of_best_total_pct      |      4.07   | percent |
| order_id@8bits     | row_groups                   |     70      | count   |
| order_id@8bits     | filter_bytes                 |  77952      | bytes   |
| order_id@8bits     | k                            |      6      | count   |
| order_id@8bits     | misses_generated             |   1000      | count   |
| order_id@8bits     | misses_rejected_out_of_range |      0      | count   |
| order_id@8bits     | present_bloom_skipped_pct    |     96.36   | percent |
| order_id@8bits     | present_minmax_skipped_pct   |      0.3871 | percent |
| order_id@8bits     | present_ideal_skipped_pct    |     98.57   | percent |
| order_id@8bits     | absent_bloom_skipped_pct     |     97.7    | percent |
| order_id@8bits     | absent_minmax_skipped_pct    |      0.4457 | percent |
| order_id@8bits     | absent_ideal_skipped_pct     |    100      | percent |
| order_id@8bits     | share_of_best_total_pct      |      0.7931 | percent |
| order_id@10bits    | row_groups                   |     70      | count   |
| order_id@10bits    | filter_bytes                 |  97464      | bytes   |
| order_id@10bits    | k                            |      7      | count   |
| order_id@10bits    | misses_generated             |   1000      | count   |
| order_id@10bits    | misses_rejected_out_of_range |      0      | count   |
| order_id@10bits    | present_bloom_skipped_pct    |     97.71   | percent |
| order_id@10bits    | present_minmax_skipped_pct   |      0.3871 | percent |
| order_id@10bits    | present_ideal_skipped_pct    |     98.57   | percent |
| order_id@10bits    | absent_bloom_skipped_pct     |     99.15   | percent |
| order_id@10bits    | absent_minmax_skipped_pct    |      0.4457 | percent |
| order_id@10bits    | absent_ideal_skipped_pct     |    100      | percent |
| order_id@10bits    | share_of_best_total_pct      |      0.9917 | percent |
| order_id@16bits    | row_groups                   |     70      | count   |
| order_id@16bits    | filter_bytes                 | 155904      | bytes   |
| order_id@16bits    | k                            |     11      | count   |
| order_id@16bits    | misses_generated             |   1000      | count   |
| order_id@16bits    | misses_rejected_out_of_range |      0      | count   |
| order_id@16bits    | present_bloom_skipped_pct    |     98.49   | percent |
| order_id@16bits    | present_minmax_skipped_pct   |      0.3871 | percent |
| order_id@16bits    | present_ideal_skipped_pct    |     98.57   | percent |
| order_id@16bits    | absent_bloom_skipped_pct     |     99.94   | percent |
| order_id@16bits    | absent_minmax_skipped_pct    |      0.4457 | percent |
| order_id@16bits    | absent_ideal_skipped_pct     |    100      | percent |
| order_id@16bits    | share_of_best_total_pct      |      1.586  | percent |
| words:trace_id     | common_skipped_pct           |     98.42   | percent |
| words:trace_id     | rare_skipped_pct             |     98.44   | percent |
| words:trace_id     | never_skipped_pct            |     99.13   | percent |
| words:trace_id     | filter_bytes                 | 248100      | bytes   |

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
| words:ext4.label  | rare_skipped_pct             |     44.05   | percent |
| words:ext4.label  | never_skipped_pct            |     97.79   | percent |
| words:ext4.label  | filter_bytes                 |   8443      | bytes   |
| words:ext5.label  | common_skipped_pct           |     40.52   | percent |
| words:ext5.label  | rare_skipped_pct             |     42.88   | percent |
| words:ext5.label  | never_skipped_pct            |     97.86   | percent |
| words:ext5.label  | filter_bytes                 |   8476      | bytes   |
| words:ext6.label  | common_skipped_pct           |     41.95   | percent |
| words:ext6.label  | rare_skipped_pct             |     43.86   | percent |
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
- distinct/rows 0.992; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- against script 05's BEST_PER_CHUNK (9,828,235 bytes)
- distinct/rows 0.994; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- distinct/rows 0.817; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- distinct/rows 0.909; 1000 ids that exist and 1000 that do not; the denominator is the row groups holding the column
- 130 row groups, 198,441 distinct words
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

Run 2026-09-18T23:59:58+00:00, git `3b5b8b0`, 116 rows.

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

### `07_vs_others` / dataset `messy`

| variant                | metric                       | value                                                                                                             | unit    |
|------------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------|---------|
| elasticsearch          | version                      | docker.elastic.co/elasticsearch/elasticsearch:8.19.20                                                             | tag     |
| clickhouse             | version                      | clickhouse/clickhouse-server:26.8.7.19                                                                            | tag     |
| clp-s                  | version                      | y-scope/clp v0.13.0, clp-s built from source for the host architecture (no prebuilt binary or image is published) | tag     |
| raw NDJSON             | bytes                        | 62,959,826                                                                                                        | bytes   |
| all                    | at_timestamp_added           | 1                                                                                                                 | flag    |
| Elasticsearch          | heap                         | 1g                                                                                                                | setting |
| Elasticsearch standard | ignore_malformed             | false                                                                                                             | setting |
| Elasticsearch standard | documents_indexed            | 188,968                                                                                                           | docs    |
| Elasticsearch standard | documents_rejected           | 11,032                                                                                                            | docs    |
| Elasticsearch standard | index_is_partial             | 1                                                                                                                 | flag    |
| Elasticsearch standard | bytes_on_disk                | 67,873,791                                                                                                        | bytes   |
| Elasticsearch standard | ratio_to_raw                 | 1.078                                                                                                             | ratio   |
| Elasticsearch standard | load_seconds                 | 17.92                                                                                                             | s       |
| Elasticsearch standard | search_event value_seconds   | 0.022                                                                                                             | s       |
| Elasticsearch standard | search_level = ERROR_seconds | 0.0038                                                                                                            | s       |
| Elasticsearch standard | search_one id_seconds        | 0.0113                                                                                                            | s       |
| Elasticsearch standard | search_newest 500_seconds    | 0.0942                                                                                                            | s       |
| Elasticsearch logsdb   | ignore_malformed             | true                                                                                                              | setting |
| Elasticsearch logsdb   | documents_indexed            | 200,000                                                                                                           | docs    |
| Elasticsearch logsdb   | documents_rejected           | 0                                                                                                                 | docs    |
| Elasticsearch logsdb   | bytes_on_disk                | 58,154,664                                                                                                        | bytes   |
| Elasticsearch logsdb   | ratio_to_raw                 | 0.9237                                                                                                            | ratio   |
| Elasticsearch logsdb   | load_seconds                 | 19.33                                                                                                             | s       |
| Elasticsearch logsdb   | search_event value_seconds   | 0.0045                                                                                                            | s       |
| Elasticsearch logsdb   | search_level = ERROR_seconds | 0.0025                                                                                                            | s       |
| Elasticsearch logsdb   | search_one id_seconds        | 0.0037                                                                                                            | s       |
| Elasticsearch logsdb   | search_newest 500_seconds    | 0.0224                                                                                                            | s       |
| ClickHouse             | rows_inserted                | 200,000                                                                                                           | rows    |
| ClickHouse             | bytes_on_disk                | 20,603,823                                                                                                        | bytes   |
| ClickHouse             | ratio_to_raw                 | 0.3272                                                                                                            | ratio   |
| ClickHouse             | load_seconds                 | 6.47                                                                                                              | s       |
| ClickHouse             | search_event value_seconds   | 0.0101                                                                                                            | s       |
| ClickHouse             | search_level = ERROR_seconds | 0.0081                                                                                                            | s       |
| ClickHouse             | search_one id_seconds        | 0.0125                                                                                                            | s       |
| ClickHouse             | search_newest 500_seconds    | 0.018                                                                                                             | s       |
| clp-s                  | roundtrip_lines              | 200,000                                                                                                           | lines   |
| clp-s                  | roundtrip_ok                 | 1                                                                                                                 | flag    |
| clp-s                  | bytes_on_disk                | 11,227,572                                                                                                        | bytes   |
| clp-s                  | ratio_to_raw                 | 0.1783                                                                                                            | ratio   |
| clp-s                  | load_seconds                 | 1.15                                                                                                              | s       |
| clp-s                  | search_event value_seconds   | 0.2263                                                                                                            | s       |
| clp-s                  | search_level = ERROR_seconds | 0.2443                                                                                                            | s       |
| clp-s                  | search_one id_seconds        | 0.2143                                                                                                            | s       |
| clp-s                  | search_newest 500_seconds    | 0.7202                                                                                                            | s       |

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
- resolved from the running index, not assumed. `true` keeps a document whose field will not coerce and drops that field from the index; `false` rejects the whole document.
- 200,000 were sent
- document_parsing_exception 11,032
- only 188,968 of 200,000 documents landed, so bytes_on_disk understates what this dataset costs
- after _forcemerge to one segment and _flush; PARTIAL: 188,968/200,000 docs

## 08_io_shapes

_Disk-dominated shapes_

Run 2026-09-19T09:43:08+00:00, git `90152d2`, 40 rows.

### `08_io_shapes` / dataset `uneven`

| variant               | metric                 |            value | unit      |
|-----------------------|------------------------|------------------|-----------|
| fsync batch=1         | records_per_sec        |  30664           | records/s |
| fsync batch=1         | total_ms               |     97.83        | ms        |
| fsync batch=10        | records_per_sec        | 243131           | records/s |
| fsync batch=10        | total_ms               |     12.34        | ms        |
| fsync batch=100       | records_per_sec        |      1.20603e+06 | records/s |
| fsync batch=100       | total_ms               |      2.49        | ms        |
| fsync batch=500       | records_per_sec        |      1.49508e+06 | records/s |
| fsync batch=500       | total_ms               |      2.01        | ms        |
| F_FULLFSYNC batch=1   | records_per_sec        |    369           | records/s |
| F_FULLFSYNC batch=1   | total_ms               |   8118           | ms        |
| F_FULLFSYNC batch=10  | records_per_sec        |   3736           | records/s |
| F_FULLFSYNC batch=10  | total_ms               |    802.9         | ms        |
| F_FULLFSYNC batch=100 | records_per_sec        |  35423           | records/s |
| F_FULLFSYNC batch=100 | total_ms               |     84.69        | ms        |
| F_FULLFSYNC batch=500 | records_per_sec        | 139498           | records/s |
| F_FULLFSYNC batch=500 | total_ms               |     21.51        | ms        |
| wal batch=1           | compression_ratio      |      0.8638      | ratio     |
| wal batch=1           | zstd_MBps              |     56.1         | MB/s      |
| wal batch=10          | compression_ratio      |      0.2667      | ratio     |
| wal batch=10          | zstd_MBps              |    296.7         | MB/s      |
| wal batch=100         | compression_ratio      |      0.1421      | ratio     |
| wal batch=100         | zstd_MBps              |    714.9         | MB/s      |
| wal batch=500         | compression_ratio      |      0.1141      | ratio     |
| wal batch=500         | zstd_MBps              |    863.2         | MB/s      |
| 1000 files            | listdir_ms             |      0.439       | ms        |
| 1000 files            | manifest_read_ms       |      0.057       | ms        |
| 1000 files            | listdir_over_manifest  |      7.8         | ratio     |
| 10000 files           | listdir_ms             |      5.109       | ms        |
| 10000 files           | manifest_read_ms       |      0.309       | ms        |
| 10000 files           | listdir_over_manifest  |     16.5         | ratio     |
| 100000 files          | listdir_ms             |     42.61        | ms        |
| 100000 files          | manifest_read_ms       |      3.022       | ms        |
| 100000 files          | listdir_over_manifest  |     14.1         | ratio     |
| scattered vs grouped  | row_groups_of_template |     51           | count     |
| scattered vs grouped  | ranges_before          |     51           | count     |
| scattered vs grouped  | ranges_after           |      1           | count     |
| scattered vs grouped  | ssd_ms_before          |      5.1         | ms        |
| scattered vs grouped  | ssd_ms_after           |      0.1         | ms        |
| scattered vs grouped  | s3_ms_before           |    765           | ms        |
| scattered vs grouped  | s3_ms_after            |     15           | ms        |

**Notes**

- shape only - one laptop, Python around the call; not a kibbr throughput; the disk does this work. fsync(2) on Darwin returns at the drive's write cache, not the platter
- shape only - one laptop, Python around the call; not a kibbr throughput
- shape only - one laptop, Python around the call; not a kibbr throughput; the disk does this work. F_FULLFSYNC is Darwin's real barrier to the platter
- shape only - one laptop, Python around the call; not a kibbr throughput; zstd is a C library, so the MB/s is its own: 56.1
- shape only - one laptop, Python around the call; not a kibbr throughput; zstd is a C library, so the MB/s is its own: 296.7
- shape only - one laptop, Python around the call; not a kibbr throughput; zstd is a C library, so the MB/s is its own: 714.9
- shape only - one laptop, Python around the call; not a kibbr throughput; zstd is a C library, so the MB/s is its own: 863.2
- shape only - one laptop, Python around the call; not a kibbr throughput; the filesystem does this
- shape only - one laptop, Python around the call; not a kibbr throughput; a range count multiplied by a per-seek constant, not a timed read. Day 2024-01-15, template 1055466456870197778

## 09_template_lookup

_Searches on keys that are not promoted_

Run 2026-09-19T10:05:48+00:00, git `90152d2`, 22 rows.

### `09_template_lookup` / dataset `synthetic`

| variant          | metric               |           value | unit   |
|------------------|----------------------|-----------------|--------|
| 1000 templates   | scan_comparisons     | 200000          | count  |
| 1000 templates   | scan_growth_vs_1000  |      1          | ratio  |
| 1000 templates   | index_growth_vs_1000 |      1          | ratio  |
| 1000 templates   | scan_over_index      |    437.8        | ratio  |
| 1000 templates   | index_entries        |  12259          | count  |
| 1000 templates   | rows_of_answer       |  10954          | count  |
| 1000 templates   | index_build_ms       |      0.656      | ms     |
| 10000 templates  | scan_comparisons     |      2e+06      | count  |
| 10000 templates  | scan_growth_vs_1000  |     10.3        | ratio  |
| 10000 templates  | index_growth_vs_1000 |      0.3627     | ratio  |
| 10000 templates  | scan_over_index      |  12440          | ratio  |
| 10000 templates  | index_entries        | 124938          | count  |
| 10000 templates  | rows_of_answer       | 111338          | count  |
| 10000 templates  | index_build_ms       |      4.611      | ms     |
| 100000 templates | scan_comparisons     |      2e+07      | count  |
| 100000 templates | scan_growth_vs_1000  |    218.8        | ratio  |
| 100000 templates | index_growth_vs_1000 |      0.3597     | ratio  |
| 100000 templates | scan_over_index      | 266300          | ratio  |
| 100000 templates | index_entries        |      1.2508e+06 | count  |
| 100000 templates | rows_of_answer       |      1.115e+06  | count  |
| 100000 templates | index_build_ms       |     46.52       | ms     |
| verdict          | scan_superlinearity  |      2.188      | ratio  |

**Notes**

- shape only - growth against the 1,000-template case, not an absolute time
- scan growth 218.8x against the linear 100x expected from 1,000 to 100,000 templates

