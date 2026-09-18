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

## Contents

- [00_frontend_agree - Frontend agreement - the Python port against the binary](#00-frontend-agree)
- [01_profile_logs - What the logs look like](#01-profile-logs)
- [02_split_vs_wide - Splitting by template: does it help or hurt compression](#02-split-vs-wide)
- [03_layout_model - Files, row groups and overhead per layout](#03-layout-model)

## 00_frontend_agree

_Frontend agreement - the Python port against the binary_

Run 2026-09-18T20:03:44+00:00, git `18c2e37`, 6 rows.

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

Run 2026-09-18T20:08:35+00:00, git `7f71815`, 252 rows.

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

Run 2026-09-18T20:15:52+00:00, git `365bb93`, 315 rows.

### `02_split_vs_wide` / dataset `flat30`

| variant                   | metric                      |            value | unit   |
|---------------------------|-----------------------------|------------------|--------|
| raw NDJSON                | bytes                       |      4.6792e+07  | bytes  |
| raw NDJSON                | disk_blocks                 |      4.67927e+07 | bytes  |
| raw NDJSON                | files                       |      1           | count  |
| raw NDJSON                | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                | ratio_to_gzip9              |      4.321       | ratio  |
| gzip -9                   | bytes                       |      1.0829e+07  | bytes  |
| gzip -9                   | disk_blocks                 |      1.16654e+07 | bytes  |
| gzip -9                   | files                       |      1           | count  |
| gzip -9                   | ratio_to_raw_bytes          |      0.2314      | ratio  |
| gzip -9                   | ratio_to_raw_blocks         |      0.2493      | ratio  |
| gzip -9                   | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                  | bytes                       |      8.28201e+06 | bytes  |
| zstd -19                  | disk_blocks                 |      8.3968e+06  | bytes  |
| zstd -19                  | files                       |      1           | count  |
| zstd -19                  | ratio_to_raw_bytes          |      0.177       | ratio  |
| zstd -19                  | ratio_to_raw_blocks         |      0.1794      | ratio  |
| zstd -19                  | ratio_to_gzip9              |      0.7648      | ratio  |
| WIDE rg=1,000             | bytes                       |      1.25716e+07 | bytes  |
| WIDE rg=1,000             | disk_blocks                 |      1.25747e+07 | bytes  |
| WIDE rg=1,000             | files                       |      1           | count  |
| WIDE rg=1,000             | ratio_to_raw_bytes          |      0.2687      | ratio  |
| WIDE rg=1,000             | ratio_to_raw_blocks         |      0.2687      | ratio  |
| WIDE rg=1,000             | ratio_to_gzip9              |      1.161       | ratio  |
| WIDE rg=8,192             | bytes                       |      9.69406e+06 | bytes  |
| WIDE rg=8,192             | disk_blocks                 |      9.69523e+06 | bytes  |
| WIDE rg=8,192             | files                       |      1           | count  |
| WIDE rg=8,192             | ratio_to_raw_bytes          |      0.2072      | ratio  |
| WIDE rg=8,192             | ratio_to_raw_blocks         |      0.2072      | ratio  |
| WIDE rg=8,192             | ratio_to_gzip9              |      0.8952      | ratio  |
| WIDE rg=122,880           | bytes                       |      9.3771e+06  | bytes  |
| WIDE rg=122,880           | disk_blocks                 |      9.37984e+06 | bytes  |
| WIDE rg=122,880           | files                       |      1           | count  |
| WIDE rg=122,880           | ratio_to_raw_bytes          |      0.2004      | ratio  |
| WIDE rg=122,880           | ratio_to_raw_blocks         |      0.2005      | ratio  |
| WIDE rg=122,880           | ratio_to_gzip9              |      0.8659      | ratio  |
| PER_TEMPLATE rg=1,000     | bytes                       |      1.23934e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | disk_blocks                 |      1.40984e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | files                       |    870           | count  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_bytes          |      0.2649      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_blocks         |      0.3013      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_gzip9              |      1.144       | ratio  |
| PER_TEMPLATE rg=8,192     | bytes                       |      1.23556e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | disk_blocks                 |      1.40616e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | files                       |    870           | count  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_bytes          |      0.2641      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_blocks         |      0.3005      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_gzip9              |      1.141       | ratio  |
| PER_TEMPLATE rg=122,880   | bytes                       |      1.23556e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | disk_blocks                 |      1.40616e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | files                       |    870           | count  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_bytes          |      0.2641      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_blocks         |      0.3005      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_gzip9              |      1.141       | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | bytes                       |      8.52018e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | disk_blocks                 |      1.2236e+08  | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | files                       |  16694           | count  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_bytes          |      1.821       | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_blocks         |      2.615       | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_gzip9              |      7.868       | ratio  |
| SHARED_DAY rg=1,000       | bytes                       |      8.56085e+07 | bytes  |
| SHARED_DAY rg=1,000       | disk_blocks                 |      8.56801e+07 | bytes  |
| SHARED_DAY rg=1,000       | files                       |     32           | count  |
| SHARED_DAY rg=1,000       | ratio_to_raw_bytes          |      1.83        | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_raw_blocks         |      1.831       | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_gzip9              |      7.905       | ratio  |
| SHARED_DAY rg=8,192       | bytes                       |      8.56031e+07 | bytes  |
| SHARED_DAY rg=8,192       | disk_blocks                 |      8.5676e+07  | bytes  |
| SHARED_DAY rg=8,192       | files                       |     32           | count  |
| SHARED_DAY rg=8,192       | ratio_to_raw_bytes          |      1.829       | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_raw_blocks         |      1.831       | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_gzip9              |      7.905       | ratio  |
| SHARED_DAY rg=122,880     | bytes                       |      8.56031e+07 | bytes  |
| SHARED_DAY rg=122,880     | disk_blocks                 |      8.5676e+07  | bytes  |
| SHARED_DAY rg=122,880     | files                       |     32           | count  |
| SHARED_DAY rg=122,880     | ratio_to_raw_bytes          |      1.829       | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_raw_blocks         |      1.831       | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_gzip9              |      7.905       | ratio  |
| window rg=1000            | rows                        |  10711           | count  |
| window rg=1000            | row_groups                  |     11           | count  |
| window rg=1000            | groups_touched              |      1           | count  |
| window rg=1000            | file_bytes                  | 292304           | bytes  |
| window rg=1000            | bytes_read                  |  27337           | bytes  |
| window rg=1000            | read_share_pct              |      9.352       | count  |
| window rg=1000            | matching_rows               |      2           | count  |
| window rg=1000            | rows_scanned                |   1000           | count  |
| window rg=8192            | rows                        |  10711           | count  |
| window rg=8192            | row_groups                  |      2           | count  |
| window rg=8192            | groups_touched              |      1           | count  |
| window rg=8192            | file_bytes                  | 297726           | bytes  |
| window rg=8192            | bytes_read                  | 224124           | bytes  |
| window rg=8192            | read_share_pct              |     75.28        | count  |
| window rg=8192            | matching_rows               |      2           | count  |
| window rg=8192            | rows_scanned                |   8192           | count  |
| window rg=122880          | rows                        |  10711           | count  |
| window rg=122880          | row_groups                  |      1           | count  |
| window rg=122880          | groups_touched              |      1           | count  |
| window rg=122880          | file_bytes                  | 295031           | bytes  |
| window rg=122880          | bytes_read                  | 295031           | bytes  |
| window rg=122880          | read_share_pct              |    100           | count  |
| window rg=122880          | matching_rows               |      2           | count  |
| window rg=122880          | rows_scanned                |  10711           | count  |
| verdict                   | per_template_vs_gzip9_bytes |      1.141       | ratio  |
| verdict                   | smallest_by_bytes           |      8.28201e+06 | bytes  |
| verdict                   | smallest_by_blocks          |      8.3968e+06  | bytes  |

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
| gzip -9                   | disk_blocks                 |      1.06168e+07 | bytes  |
| gzip -9                   | files                       |      1           | count  |
| gzip -9                   | ratio_to_raw_bytes          |      0.2251      | ratio  |
| gzip -9                   | ratio_to_raw_blocks         |      0.2269      | ratio  |
| gzip -9                   | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                  | bytes                       |      8.05981e+06 | bytes  |
| zstd -19                  | disk_blocks                 |      8.44186e+06 | bytes  |
| zstd -19                  | files                       |      1           | count  |
| zstd -19                  | ratio_to_raw_bytes          |      0.1722      | ratio  |
| zstd -19                  | ratio_to_raw_blocks         |      0.1804      | ratio  |
| zstd -19                  | ratio_to_gzip9              |      0.7652      | ratio  |
| WIDE rg=1,000             | bytes                       |      1.23576e+07 | bytes  |
| WIDE rg=1,000             | disk_blocks                 |      1.23576e+07 | bytes  |
| WIDE rg=1,000             | files                       |      1           | count  |
| WIDE rg=1,000             | ratio_to_raw_bytes          |      0.2641      | ratio  |
| WIDE rg=1,000             | ratio_to_raw_blocks         |      0.2641      | ratio  |
| WIDE rg=1,000             | ratio_to_gzip9              |      1.173       | ratio  |
| WIDE rg=8,192             | bytes                       |      9.54164e+06 | bytes  |
| WIDE rg=8,192             | disk_blocks                 |      9.54368e+06 | bytes  |
| WIDE rg=8,192             | files                       |      1           | count  |
| WIDE rg=8,192             | ratio_to_raw_bytes          |      0.2039      | ratio  |
| WIDE rg=8,192             | ratio_to_raw_blocks         |      0.204       | ratio  |
| WIDE rg=8,192             | ratio_to_gzip9              |      0.9059      | ratio  |
| WIDE rg=122,880           | bytes                       |      8.90667e+06 | bytes  |
| WIDE rg=122,880           | disk_blocks                 |      8.9088e+06  | bytes  |
| WIDE rg=122,880           | files                       |      1           | count  |
| WIDE rg=122,880           | ratio_to_raw_bytes          |      0.1903      | ratio  |
| WIDE rg=122,880           | ratio_to_raw_blocks         |      0.1904      | ratio  |
| WIDE rg=122,880           | ratio_to_gzip9              |      0.8456      | ratio  |
| PER_TEMPLATE rg=1,000     | bytes                       |      1.22044e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | disk_blocks                 |      1.39264e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | files                       |    870           | count  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_bytes          |      0.2608      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_blocks         |      0.2976      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_gzip9              |      1.159       | ratio  |
| PER_TEMPLATE rg=8,192     | bytes                       |      1.21776e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | disk_blocks                 |      1.38854e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | files                       |    870           | count  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_bytes          |      0.2602      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_blocks         |      0.2968      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_gzip9              |      1.156       | ratio  |
| PER_TEMPLATE rg=122,880   | bytes                       |      1.21756e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | disk_blocks                 |      1.38854e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | files                       |    870           | count  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_bytes          |      0.2602      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_blocks         |      0.2968      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_gzip9              |      1.156       | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | bytes                       |      1.21776e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | disk_blocks                 |      1.38854e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | files                       |    870           | count  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_bytes          |      0.2602      | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_blocks         |      0.2968      | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_gzip9              |      1.156       | ratio  |
| SHARED_DAY rg=1,000       | bytes                       |      1.22253e+07 | bytes  |
| SHARED_DAY rg=1,000       | disk_blocks                 |      1.22307e+07 | bytes  |
| SHARED_DAY rg=1,000       | files                       |      2           | count  |
| SHARED_DAY rg=1,000       | ratio_to_raw_bytes          |      0.2613      | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_raw_blocks         |      0.2614      | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_gzip9              |      1.161       | ratio  |
| SHARED_DAY rg=8,192       | bytes                       |      1.21985e+07 | bytes  |
| SHARED_DAY rg=8,192       | disk_blocks                 |      1.2202e+07  | bytes  |
| SHARED_DAY rg=8,192       | files                       |      2           | count  |
| SHARED_DAY rg=8,192       | ratio_to_raw_bytes          |      0.2607      | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_raw_blocks         |      0.2608      | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_gzip9              |      1.158       | ratio  |
| SHARED_DAY rg=122,880     | bytes                       |      1.21966e+07 | bytes  |
| SHARED_DAY rg=122,880     | disk_blocks                 |      1.2202e+07  | bytes  |
| SHARED_DAY rg=122,880     | files                       |      2           | count  |
| SHARED_DAY rg=122,880     | ratio_to_raw_bytes          |      0.2606      | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_raw_blocks         |      0.2608      | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_gzip9              |      1.158       | ratio  |
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
| verdict                   | per_template_vs_gzip9_bytes |      1.156       | ratio  |
| verdict                   | smallest_by_bytes           |      8.05981e+06 | bytes  |
| verdict                   | smallest_by_blocks          |      8.44186e+06 | bytes  |

### `02_split_vs_wide` / dataset `uneven`

| variant                   | metric                      |            value | unit   |
|---------------------------|-----------------------------|------------------|--------|
| raw NDJSON                | bytes                       |      4.49597e+07 | bytes  |
| raw NDJSON                | disk_blocks                 |      4.49618e+07 | bytes  |
| raw NDJSON                | files                       |      1           | count  |
| raw NDJSON                | ratio_to_raw_bytes          |      1           | ratio  |
| raw NDJSON                | ratio_to_raw_blocks         |      1           | ratio  |
| raw NDJSON                | ratio_to_gzip9              |     11.07        | ratio  |
| gzip -9                   | bytes                       |      4.06238e+06 | bytes  |
| gzip -9                   | disk_blocks                 |      4.06323e+06 | bytes  |
| gzip -9                   | files                       |      1           | count  |
| gzip -9                   | ratio_to_raw_bytes          |      0.09036     | ratio  |
| gzip -9                   | ratio_to_raw_blocks         |      0.09038     | ratio  |
| gzip -9                   | ratio_to_gzip9              |      1           | ratio  |
| zstd -19                  | bytes                       |      2.89697e+06 | bytes  |
| zstd -19                  | disk_blocks                 |      3.20307e+06 | bytes  |
| zstd -19                  | files                       |      1           | count  |
| zstd -19                  | ratio_to_raw_bytes          |      0.06443     | ratio  |
| zstd -19                  | ratio_to_raw_blocks         |      0.07124     | ratio  |
| zstd -19                  | ratio_to_gzip9              |      0.7131      | ratio  |
| WIDE rg=1,000             | bytes                       |      7.77741e+06 | bytes  |
| WIDE rg=1,000             | disk_blocks                 |      7.7783e+06  | bytes  |
| WIDE rg=1,000             | files                       |      1           | count  |
| WIDE rg=1,000             | ratio_to_raw_bytes          |      0.173       | ratio  |
| WIDE rg=1,000             | ratio_to_raw_blocks         |      0.173       | ratio  |
| WIDE rg=1,000             | ratio_to_gzip9              |      1.915       | ratio  |
| WIDE rg=8,192             | bytes                       |      4.20096e+06 | bytes  |
| WIDE rg=8,192             | disk_blocks                 |      4.2025e+06  | bytes  |
| WIDE rg=8,192             | files                       |      1           | count  |
| WIDE rg=8,192             | ratio_to_raw_bytes          |      0.09344     | ratio  |
| WIDE rg=8,192             | ratio_to_raw_blocks         |      0.09347     | ratio  |
| WIDE rg=8,192             | ratio_to_gzip9              |      1.034       | ratio  |
| WIDE rg=122,880           | bytes                       |      3.86484e+06 | bytes  |
| WIDE rg=122,880           | disk_blocks                 |      3.86662e+06 | bytes  |
| WIDE rg=122,880           | files                       |      1           | count  |
| WIDE rg=122,880           | ratio_to_raw_bytes          |      0.08596     | ratio  |
| WIDE rg=122,880           | ratio_to_raw_blocks         |      0.086       | ratio  |
| WIDE rg=122,880           | ratio_to_gzip9              |      0.9514      | ratio  |
| PER_TEMPLATE rg=1,000     | bytes                       |      2.61029e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | disk_blocks                 |      3.48488e+07 | bytes  |
| PER_TEMPLATE rg=1,000     | files                       |   4931           | count  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_bytes          |      0.5806      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_raw_blocks         |      0.7751      | ratio  |
| PER_TEMPLATE rg=1,000     | ratio_to_gzip9              |      6.426       | ratio  |
| PER_TEMPLATE rg=8,192     | bytes                       |      2.58339e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | disk_blocks                 |      3.45702e+07 | bytes  |
| PER_TEMPLATE rg=8,192     | files                       |   4931           | count  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_bytes          |      0.5746      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_raw_blocks         |      0.7689      | ratio  |
| PER_TEMPLATE rg=8,192     | ratio_to_gzip9              |      6.359       | ratio  |
| PER_TEMPLATE rg=122,880   | bytes                       |      2.58005e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | disk_blocks                 |      3.45334e+07 | bytes  |
| PER_TEMPLATE rg=122,880   | files                       |   4931           | count  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_bytes          |      0.5739      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_raw_blocks         |      0.7681      | ratio  |
| PER_TEMPLATE rg=122,880   | ratio_to_gzip9              |      6.351       | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | bytes                       |      2.58339e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | disk_blocks                 |      3.45702e+07 | bytes  |
| PER_TEMPLATE_DAY rg=8,192 | files                       |   4931           | count  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_bytes          |      0.5746      | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_raw_blocks         |      0.7689      | ratio  |
| PER_TEMPLATE_DAY rg=8,192 | ratio_to_gzip9              |      6.359       | ratio  |
| SHARED_DAY rg=1,000       | bytes                       |      2.62213e+07 | bytes  |
| SHARED_DAY rg=1,000       | disk_blocks                 |      2.66609e+07 | bytes  |
| SHARED_DAY rg=1,000       | files                       |      2           | count  |
| SHARED_DAY rg=1,000       | ratio_to_raw_bytes          |      0.5832      | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_raw_blocks         |      0.593       | ratio  |
| SHARED_DAY rg=1,000       | ratio_to_gzip9              |      6.455       | ratio  |
| SHARED_DAY rg=8,192       | bytes                       |      2.59523e+07 | bytes  |
| SHARED_DAY rg=8,192       | disk_blocks                 |      2.6751e+07  | bytes  |
| SHARED_DAY rg=8,192       | files                       |      2           | count  |
| SHARED_DAY rg=8,192       | ratio_to_raw_bytes          |      0.5772      | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_raw_blocks         |      0.595       | ratio  |
| SHARED_DAY rg=8,192       | ratio_to_gzip9              |      6.388       | ratio  |
| SHARED_DAY rg=122,880     | bytes                       |      2.59189e+07 | bytes  |
| SHARED_DAY rg=122,880     | disk_blocks                 |      2.69148e+07 | bytes  |
| SHARED_DAY rg=122,880     | files                       |      2           | count  |
| SHARED_DAY rg=122,880     | ratio_to_raw_bytes          |      0.5765      | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_raw_blocks         |      0.5986      | ratio  |
| SHARED_DAY rg=122,880     | ratio_to_gzip9              |      6.38        | ratio  |
| window rg=1000            | rows                        |  54721           | count  |
| window rg=1000            | row_groups                  |     55           | count  |
| window rg=1000            | groups_touched              |      1           | count  |
| window rg=1000            | file_bytes                  | 179672           | bytes  |
| window rg=1000            | bytes_read                  |   3368           | bytes  |
| window rg=1000            | read_share_pct              |      1.875       | count  |
| window rg=1000            | matching_rows               |    201           | count  |
| window rg=1000            | rows_scanned                |   1000           | count  |
| window rg=8192            | rows                        |  54721           | count  |
| window rg=8192            | row_groups                  |      7           | count  |
| window rg=8192            | groups_touched              |      1           | count  |
| window rg=8192            | file_bytes                  | 123372           | bytes  |
| window rg=8192            | bytes_read                  |  18979           | bytes  |
| window rg=8192            | read_share_pct              |     15.38        | count  |
| window rg=8192            | matching_rows               |    201           | count  |
| window rg=8192            | rows_scanned                |   8192           | count  |
| window rg=122880          | rows                        |  54721           | count  |
| window rg=122880          | row_groups                  |      1           | count  |
| window rg=122880          | groups_touched              |      1           | count  |
| window rg=122880          | file_bytes                  | 116525           | bytes  |
| window rg=122880          | bytes_read                  | 116525           | bytes  |
| window rg=122880          | read_share_pct              |    100           | count  |
| window rg=122880          | matching_rows               |    201           | count  |
| window rg=122880          | rows_scanned                |  54721           | count  |
| verdict                   | per_template_vs_gzip9_bytes |      6.359       | ratio  |
| verdict                   | smallest_by_bytes           |      2.89697e+06 | bytes  |
| verdict                   | smallest_by_blocks          |      3.20307e+06 | bytes  |

**Notes**

- C zlib, level 9
- C zstd, level 19
- what TrieDB does today
- per-template bytes without per-file block rounding
- 5-minute window over the busiest dated template (1055466456870197778); row groups are skipped on their ts statistics
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 1.2054 over 870 files
- zstd -19
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 1.3079 over 870 files
- below 1 means per-template Parquet spends fewer bytes than gzip -9; the blocks ratio is 8.5081 over 4,931 files

## 03_layout_model

_Files, row groups and overhead per layout_

Run 2026-09-18T20:31:25+00:00, git `32baccc`, 429 rows.

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
| rate=12 wait=300               | wal_24h_bytes_raw                |      2.7471e+08  | bytes   |
| rate=12 wait=900               | wal_24h_bytes_raw                |      2.7471e+08  | bytes   |
| rate=120 wait=60               | wal_24h_bytes_raw                |      2.7471e+09  | bytes   |
| rate=120 wait=300              | wal_24h_bytes_raw                |      2.7471e+09  | bytes   |
| rate=120 wait=900              | wal_24h_bytes_raw                |      2.7471e+09  | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_raw                |      2.7471e+10  | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_raw                |      2.7471e+10  | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_raw                |      2.7471e+10  | bytes   |
| per_template_day               | files                            |  16694           | count   |
| per_template_day               | row_groups                       |  21312           | count   |
| per_template_day               | bytes                            |      3.7878e+07  | bytes   |
| per_template_day               | overhead_bytes                   |      1.25699e+07 | bytes   |
| per_template_day               | overhead_share_pct               |     33.19        | percent |
| per_template_day               | disk_blocks                      |      8.13056e+07 | bytes   |
| per_template_day               | ratio_to_raw                     |      1.745       | ratio   |
| per_template_day               | one_template_files               |      1           | count   |
| per_template_day               | one_template_index_bytes         |    360           | bytes   |
| per_template_day               | one_template_ranges              |      1           | count   |
| per_template_day               | all_templates_files              |    550           | count   |
| per_template_day               | all_templates_index_bytes        | 221250           | bytes   |
| per_template_day               | all_templates_ranges             |    550           | count   |
| shared_day_limit limit=128.0MB | files                            |     32           | count   |
| shared_day_limit limit=128.0MB | row_groups                       |  21312           | count   |
| shared_day_limit limit=128.0MB | bytes                            |      5.22325e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_bytes                   |      2.69244e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_share_pct               |     51.55        | percent |
| shared_day_limit limit=128.0MB | disk_blocks                      |      5.22977e+07 | bytes   |
| shared_day_limit limit=128.0MB | ratio_to_raw                     |      1.122       | ratio   |
| shared_day_limit limit=128.0MB | one_template_files               |      1           | count   |
| shared_day_limit limit=128.0MB | one_template_index_bytes         | 305628           | bytes   |
| shared_day_limit limit=128.0MB | one_template_ranges              |      2           | count   |
| shared_day_limit limit=128.0MB | all_templates_files              |      1           | count   |
| shared_day_limit limit=128.0MB | all_templates_index_bytes        | 305628           | bytes   |
| shared_day_limit limit=128.0MB | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=128.0MB | open_file_index_memory_bytes     |      2.69434e+07 | bytes   |
| shared_day_limit limit=512.0MB | files                            |     32           | count   |
| shared_day_limit limit=512.0MB | row_groups                       |  21312           | count   |
| shared_day_limit limit=512.0MB | bytes                            |      5.22325e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_bytes                   |      2.69244e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_share_pct               |     51.55        | percent |
| shared_day_limit limit=512.0MB | disk_blocks                      |      5.22977e+07 | bytes   |
| shared_day_limit limit=512.0MB | ratio_to_raw                     |      1.122       | ratio   |
| shared_day_limit limit=512.0MB | one_template_files               |      1           | count   |
| shared_day_limit limit=512.0MB | one_template_index_bytes         | 305628           | bytes   |
| shared_day_limit limit=512.0MB | one_template_ranges              |      2           | count   |
| shared_day_limit limit=512.0MB | all_templates_files              |      1           | count   |
| shared_day_limit limit=512.0MB | all_templates_index_bytes        | 305628           | bytes   |
| shared_day_limit limit=512.0MB | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=512.0MB | open_file_index_memory_bytes     |      1.07775e+08 | bytes   |
| shared_day_limit limit=1.0GB   | files                            |     32           | count   |
| shared_day_limit limit=1.0GB   | row_groups                       |  21312           | count   |
| shared_day_limit limit=1.0GB   | bytes                            |      5.22325e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_bytes                   |      2.69244e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_share_pct               |     51.55        | percent |
| shared_day_limit limit=1.0GB   | disk_blocks                      |      5.22977e+07 | bytes   |
| shared_day_limit limit=1.0GB   | ratio_to_raw                     |      1.122       | ratio   |
| shared_day_limit limit=1.0GB   | one_template_files               |      1           | count   |
| shared_day_limit limit=1.0GB   | one_template_index_bytes         | 305628           | bytes   |
| shared_day_limit limit=1.0GB   | one_template_ranges              |      2           | count   |
| shared_day_limit limit=1.0GB   | all_templates_files              |      1           | count   |
| shared_day_limit limit=1.0GB   | all_templates_index_bytes        | 305628           | bytes   |
| shared_day_limit limit=1.0GB   | all_templates_ranges             |      1           | count   |
| shared_day_limit limit=1.0GB   | open_file_index_memory_bytes     |      2.1555e+08  | bytes   |
| mod_n n=4                      | files                            |    128           | count   |
| mod_n n=4                      | row_groups                       |  21312           | count   |
| mod_n n=4                      | bytes                            |      4.98873e+07 | bytes   |
| mod_n n=4                      | overhead_bytes                   |      2.45792e+07 | bytes   |
| mod_n n=4                      | overhead_share_pct               |     49.27        | percent |
| mod_n n=4                      | disk_blocks                      |      5.01268e+07 | bytes   |
| mod_n n=4                      | ratio_to_raw                     |      1.076       | ratio   |
| mod_n n=4                      | one_template_files               |      1           | count   |
| mod_n n=4                      | one_template_index_bytes         |  79800           | bytes   |
| mod_n n=4                      | one_template_ranges              |      2           | count   |
| mod_n n=4                      | all_templates_files              |      4           | count   |
| mod_n n=4                      | all_templates_index_bytes        | 279636           | bytes   |
| mod_n n=4                      | all_templates_ranges             |      4           | count   |
| mod_n n=4                      | byte_spread_largest_over_average |      1.265       | ratio   |
| mod_n n=8                      | files                            |    256           | count   |
| mod_n n=8                      | row_groups                       |  21312           | count   |
| mod_n n=8                      | bytes                            |      4.76281e+07 | bytes   |
| mod_n n=8                      | overhead_bytes                   |      2.232e+07   | bytes   |
| mod_n n=8                      | overhead_share_pct               |     46.86        | percent |
| mod_n n=8                      | disk_blocks                      |      4.8169e+07  | bytes   |
| mod_n n=8                      | ratio_to_raw                     |      1.034       | ratio   |
| mod_n n=8                      | one_template_files               |      1           | count   |
| mod_n n=8                      | one_template_index_bytes         |  29874           | bytes   |
| mod_n n=8                      | one_template_ranges              |      2           | count   |
| mod_n n=8                      | all_templates_files              |      8           | count   |
| mod_n n=8                      | all_templates_index_bytes        | 258100           | bytes   |
| mod_n n=8                      | all_templates_ranges             |      8           | count   |
| mod_n n=8                      | byte_spread_largest_over_average |      1.528       | ratio   |
| least_full n=4                 | files                            |    128           | count   |
| least_full n=4                 | row_groups                       |  21312           | count   |
| least_full n=4                 | bytes                            |      5.11718e+07 | bytes   |
| least_full n=4                 | overhead_bytes                   |      2.58637e+07 | bytes   |
| least_full n=4                 | overhead_share_pct               |     50.54        | percent |
| least_full n=4                 | disk_blocks                      |      5.14335e+07 | bytes   |
| least_full n=4                 | ratio_to_raw                     |      1.104       | ratio   |
| least_full n=4                 | one_template_files               |      2           | count   |
| least_full n=4                 | one_template_index_bytes         | 148284           | bytes   |
| least_full n=4                 | one_template_ranges              |      2           | count   |
| least_full n=4                 | all_templates_files              |      4           | count   |
| least_full n=4                 | all_templates_index_bytes        | 295576           | bytes   |
| least_full n=4                 | all_templates_ranges             |      4           | count   |
| least_full n=4                 | byte_spread_largest_over_average |      1.053       | ratio   |
| least_full n=8                 | files                            |    256           | count   |
| least_full n=8                 | row_groups                       |  21312           | count   |
| least_full n=8                 | bytes                            |      5.01936e+07 | bytes   |
| least_full n=8                 | overhead_bytes                   |      2.48855e+07 | bytes   |
| least_full n=8                 | overhead_share_pct               |     49.58        | percent |
| least_full n=8                 | disk_blocks                      |      5.06921e+07 | bytes   |
| least_full n=8                 | ratio_to_raw                     |      1.088       | ratio   |
| least_full n=8                 | one_template_files               |      1           | count   |
| least_full n=8                 | one_template_index_bytes         |  32076           | bytes   |
| least_full n=8                 | one_template_ranges              |      2           | count   |
| least_full n=8                 | all_templates_files              |      8           | count   |
| least_full n=8                 | all_templates_index_bytes        | 280458           | bytes   |
| least_full n=8                 | all_templates_ranges             |      8           | count   |
| least_full n=8                 | byte_spread_largest_over_average |      1.095       | ratio   |
| calibration                    | real_col_bytes                   |      3.60378e+07 | bytes   |
| calibration                    | real_col_files                   |  16694           | count   |
| calibration                    | real_col_blocks                  |      7.98188e+07 | bytes   |
| calibration                    | real_meta_bytes                  |      2.5283e+06  | bytes   |
| calibration                    | real_meta_blocks                 |      5.8368e+06  | bytes   |
| calibration                    | real_meta_files                  |    871           | count   |
| calibration                    | model_col_bytes                  |      3.6436e+07  | bytes   |
| calibration                    | model_col_files                  |  16694           | count   |
| calibration                    | model_col_blocks                 |      8.00236e+07 | bytes   |
| calibration                    | model_minus_real_pct             |      1.105       | percent |

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
| rate=12 wait=300               | wal_24h_bytes_raw                |     2.65212e+08 | bytes   |
| rate=12 wait=900               | wal_24h_bytes_raw                |     2.65212e+08 | bytes   |
| rate=120 wait=60               | wal_24h_bytes_raw                |     2.65212e+09 | bytes   |
| rate=120 wait=300              | wal_24h_bytes_raw                |     2.65212e+09 | bytes   |
| rate=120 wait=900              | wal_24h_bytes_raw                |     2.65212e+09 | bytes   |
| rate=1200 wait=60              | wal_24h_bytes_raw                |     2.65212e+10 | bytes   |
| rate=1200 wait=300             | wal_24h_bytes_raw                |     2.65212e+10 | bytes   |
| rate=1200 wait=900             | wal_24h_bytes_raw                |     2.65212e+10 | bytes   |
| per_template_day               | files                            |  4931           | count   |
| per_template_day               | row_groups                       | 26890           | count   |
| per_template_day               | bytes                            |     3.30463e+07 | bytes   |
| per_template_day               | overhead_bytes                   |     1.23347e+07 | bytes   |
| per_template_day               | overhead_share_pct               |    37.33        | percent |
| per_template_day               | disk_blocks                      |     4.24428e+07 | bytes   |
| per_template_day               | ratio_to_raw                     |     0.9482      | ratio   |
| per_template_day               | one_template_files               |     1           | count   |
| per_template_day               | one_template_index_bytes         |  6240           | bytes   |
| per_template_day               | one_template_ranges              |     1           | count   |
| per_template_day               | all_templates_files              |  4159           | count   |
| per_template_day               | all_templates_index_bytes        |     5.05134e+06 | bytes   |
| per_template_day               | all_templates_ranges             |  4159           | count   |
| shared_day_limit limit=128.0MB | files                            |     2           | count   |
| shared_day_limit limit=128.0MB | row_groups                       | 26890           | count   |
| shared_day_limit limit=128.0MB | bytes                            |     6.60036e+07 | bytes   |
| shared_day_limit limit=128.0MB | overhead_bytes                   |     4.5292e+07  | bytes   |
| shared_day_limit limit=128.0MB | overhead_share_pct               |    68.62        | percent |
| shared_day_limit limit=128.0MB | disk_blocks                      |     6.6007e+07  | bytes   |
| shared_day_limit limit=128.0MB | ratio_to_raw                     |     1.475       | ratio   |
| shared_day_limit limit=128.0MB | one_template_files               |     1           | count   |
| shared_day_limit limit=128.0MB | one_template_index_bytes         |     1.52756e+07 | bytes   |
| shared_day_limit limit=128.0MB | one_template_ranges              |    51           | count   |
| shared_day_limit limit=128.0MB | all_templates_files              |     1           | count   |
| shared_day_limit limit=128.0MB | all_templates_index_bytes        |     1.52756e+07 | bytes   |
| shared_day_limit limit=128.0MB | all_templates_ranges             |     1           | count   |
| shared_day_limit limit=128.0MB | open_file_index_memory_bytes     |     3.01834e+07 | bytes   |
| shared_day_limit limit=512.0MB | files                            |     2           | count   |
| shared_day_limit limit=512.0MB | row_groups                       | 26890           | count   |
| shared_day_limit limit=512.0MB | bytes                            |     6.60036e+07 | bytes   |
| shared_day_limit limit=512.0MB | overhead_bytes                   |     4.5292e+07  | bytes   |
| shared_day_limit limit=512.0MB | overhead_share_pct               |    68.62        | percent |
| shared_day_limit limit=512.0MB | disk_blocks                      |     6.6007e+07  | bytes   |
| shared_day_limit limit=512.0MB | ratio_to_raw                     |     1.475       | ratio   |
| shared_day_limit limit=512.0MB | one_template_files               |     1           | count   |
| shared_day_limit limit=512.0MB | one_template_index_bytes         |     1.52756e+07 | bytes   |
| shared_day_limit limit=512.0MB | one_template_ranges              |    51           | count   |
| shared_day_limit limit=512.0MB | all_templates_files              |     1           | count   |
| shared_day_limit limit=512.0MB | all_templates_index_bytes        |     1.52756e+07 | bytes   |
| shared_day_limit limit=512.0MB | all_templates_ranges             |     1           | count   |
| shared_day_limit limit=512.0MB | open_file_index_memory_bytes     |     1.20735e+08 | bytes   |
| shared_day_limit limit=1.0GB   | files                            |     2           | count   |
| shared_day_limit limit=1.0GB   | row_groups                       | 26890           | count   |
| shared_day_limit limit=1.0GB   | bytes                            |     6.60036e+07 | bytes   |
| shared_day_limit limit=1.0GB   | overhead_bytes                   |     4.5292e+07  | bytes   |
| shared_day_limit limit=1.0GB   | overhead_share_pct               |    68.62        | percent |
| shared_day_limit limit=1.0GB   | disk_blocks                      |     6.6007e+07  | bytes   |
| shared_day_limit limit=1.0GB   | ratio_to_raw                     |     1.475       | ratio   |
| shared_day_limit limit=1.0GB   | one_template_files               |     1           | count   |
| shared_day_limit limit=1.0GB   | one_template_index_bytes         |     1.52756e+07 | bytes   |
| shared_day_limit limit=1.0GB   | one_template_ranges              |    51           | count   |
| shared_day_limit limit=1.0GB   | all_templates_files              |     1           | count   |
| shared_day_limit limit=1.0GB   | all_templates_index_bytes        |     1.52756e+07 | bytes   |
| shared_day_limit limit=1.0GB   | all_templates_ranges             |     1           | count   |
| shared_day_limit limit=1.0GB   | open_file_index_memory_bytes     |     2.41469e+08 | bytes   |
| mod_n n=4                      | files                            |     8           | count   |
| mod_n n=4                      | row_groups                       | 26890           | count   |
| mod_n n=4                      | bytes                            |     6.60076e+07 | bytes   |
| mod_n n=4                      | overhead_bytes                   |     4.5296e+07  | bytes   |
| mod_n n=4                      | overhead_share_pct               |    68.62        | percent |
| mod_n n=4                      | disk_blocks                      |     6.60234e+07 | bytes   |
| mod_n n=4                      | ratio_to_raw                     |     1.475       | ratio   |
| mod_n n=4                      | one_template_files               |     1           | count   |
| mod_n n=4                      | one_template_index_bytes         |     3.96877e+06 | bytes   |
| mod_n n=4                      | one_template_ranges              |    51           | count   |
| mod_n n=4                      | all_templates_files              |     4           | count   |
| mod_n n=4                      | all_templates_index_bytes        |     1.52779e+07 | bytes   |
| mod_n n=4                      | all_templates_ranges             |     4           | count   |
| mod_n n=4                      | byte_spread_largest_over_average |     1.403       | ratio   |
| mod_n n=8                      | files                            |    16           | count   |
| mod_n n=8                      | row_groups                       | 26890           | count   |
| mod_n n=8                      | bytes                            |     6.59077e+07 | bytes   |
| mod_n n=8                      | overhead_bytes                   |     4.51962e+07 | bytes   |
| mod_n n=8                      | overhead_share_pct               |    68.58        | percent |
| mod_n n=8                      | disk_blocks                      |     6.59415e+07 | bytes   |
| mod_n n=8                      | ratio_to_raw                     |     1.473       | ratio   |
| mod_n n=8                      | one_template_files               |     1           | count   |
| mod_n n=8                      | one_template_index_bytes         |     1.82735e+06 | bytes   |
| mod_n n=8                      | one_template_ranges              |    51           | count   |
| mod_n n=8                      | all_templates_files              |     8           | count   |
| mod_n n=8                      | all_templates_index_bytes        |     1.52477e+07 | bytes   |
| mod_n n=8                      | all_templates_ranges             |     8           | count   |
| mod_n n=8                      | byte_spread_largest_over_average |     1.843       | ratio   |
| least_full n=4                 | files                            |     8           | count   |
| least_full n=4                 | row_groups                       | 26890           | count   |
| least_full n=4                 | bytes                            |     6.60076e+07 | bytes   |
| least_full n=4                 | overhead_bytes                   |     4.5296e+07  | bytes   |
| least_full n=4                 | overhead_share_pct               |    68.62        | percent |
| least_full n=4                 | disk_blocks                      |     6.60275e+07 | bytes   |
| least_full n=4                 | ratio_to_raw                     |     1.475       | ratio   |
| least_full n=4                 | one_template_files               |     4           | count   |
| least_full n=4                 | one_template_index_bytes         |     1.52779e+07 | bytes   |
| least_full n=4                 | one_template_ranges              |    51           | count   |
| least_full n=4                 | all_templates_files              |     4           | count   |
| least_full n=4                 | all_templates_index_bytes        |     1.52779e+07 | bytes   |
| least_full n=4                 | all_templates_ranges             |     4           | count   |
| least_full n=4                 | byte_spread_largest_over_average |     1.033       | ratio   |
| least_full n=8                 | files                            |    16           | count   |
| least_full n=8                 | row_groups                       | 26890           | count   |
| least_full n=8                 | bytes                            |     6.60128e+07 | bytes   |
| least_full n=8                 | overhead_bytes                   |     4.53013e+07 | bytes   |
| least_full n=8                 | overhead_share_pct               |    68.62        | percent |
| least_full n=8                 | disk_blocks                      |     6.6048e+07  | bytes   |
| least_full n=8                 | ratio_to_raw                     |     1.476       | ratio   |
| least_full n=8                 | one_template_files               |     8           | count   |
| least_full n=8                 | one_template_index_bytes         |     1.52809e+07 | bytes   |
| least_full n=8                 | one_template_ranges              |    51           | count   |
| least_full n=8                 | all_templates_files              |     8           | count   |
| least_full n=8                 | all_templates_index_bytes        |     1.52809e+07 | bytes   |
| least_full n=8                 | all_templates_ranges             |     8           | count   |
| least_full n=8                 | byte_spread_largest_over_average |     1.147       | ratio   |
| calibration                    | real_col_bytes                   |     2.38031e+07 | bytes   |
| calibration                    | real_col_files                   |  4931           | count   |
| calibration                    | real_col_blocks                  |     3.7036e+07  | bytes   |
| calibration                    | real_meta_bytes                  |     1.37538e+07 | bytes   |
| calibration                    | real_meta_blocks                 |     3.25181e+07 | bytes   |
| calibration                    | real_meta_files                  |  4932           | count   |
| calibration                    | model_col_bytes                  |     2.39325e+07 | bytes   |
| calibration                    | model_col_files                  |  4931           | count   |
| calibration                    | model_col_blocks                 |     3.4558e+07  | bytes   |
| calibration                    | model_minus_real_pct             |     0.543       | percent |

**Notes**

- close reason
- lines in memory and not yet in a closed row group - what a crash replays
- 32 bytes of section 7.1 record per line plus 233 bytes of line; script 08 has not run, so only the raw figure is given
- what TrieDB does today: one segment per leaf per day
- one day (2024-01-19); the one-template search is template 1055466456870197778
- one shared file per day, rolled at 128.0MB
- 54,763 row groups x 39 columns x 12 bytes, plus 24 per row group, at the size limit
- one shared file per day, rolled at 512.0MB
- 219,055 row groups x 39 columns x 12 bytes, plus 24 per row group, at the size limit
- one shared file per day, rolled at 1.0GB
- 438,110 row groups x 39 columns x 12 bytes, plus 24 per row group, at the size limit
- 4 files per day, by template mod 4
- across the N files of one day; 1.0 is perfectly even
- 8 files per day, by template mod 8
- 4 files per day, each row group to the file with the fewest bytes
- 8 files per day, each row group to the file with the fewest bytes
- 200,000 lines ingested over POST /ingest, server stopped with SIGTERM so every segment is sealed
- the model's per-template-per-day bytes against the binary's
- 32 bytes of section 7.1 record per line plus 224 bytes of line; script 08 has not run, so only the raw figure is given
- one day (2024-01-15); the one-template search is template 1055466456870197778
- 54,680 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit
- 218,722 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit
- 437,444 row groups x 44 columns x 12 bytes, plus 24 per row group, at the size limit

