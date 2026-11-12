# kibbr storage on hdfs-logs, 2026-11-12

Quickwit's HDFS logs with tenant ids, the dataset of its tutorial "Index a logging dataset locally" (quickwit.io/docs/get-started/tutorials/tutorial-hdfs-logs). Quickwit does not state where the logs come from. hdfs-logs-multitenants.json.gz: 470,061,890 bytes, sha256 673c1b681089fb7ce78745f520a0837fcffce64651b6c4cdc8d1fd3e82a42e08, 20,000,000 lines, 7,194,479,974 bytes decoded, over 393 UTC days from 2015-08-27 to 2016-10-22. Given as `urls`, so it is decoded as it arrives and retried

- **Source:** https://quickwit-datasets-public.s3.amazonaws.com/hdfs-logs-multitenants.json.gz
- **Download:** 1 files, 470,061,890 bytes, decoded as they arrived and not kept; sha256 of their `sha256sum` listing `296ef6d995a930a97967ff4ae6d877c1c366736ea0d71d696f0cae04a4e92af8`
- **Measured:** `hdfs-logs-multitenants.json.gz`: 20,000,000 lines, 20,000,000 rows

**7,194,479,974 bytes ingested.** As a plain file that takes 7,194,484,736 bytes on this disk; kibbr keeps it in 224,305,152, 96.9% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 7,194,479,974 | 7,194,484,736 | 1.000 | - | - |
| gzip -9 | 419,379,067 | 419,381,248 | 0.058 | - | - |
| zstd -19, 8 thread(s) | 213,107,089 | 213,110,784 | 0.030 | - | - |
| kibbr, bulk load | 223,428,964 | 224,305,152 | 0.031 | 0.533 | 1.048 |
| kibbr, live day compacted | 216,993,903 | 217,645,056 | 0.030 | 0.517 | 1.018 |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /opt/kibbr/testdata/compression/hdfs-logs/urls-bf4f92c165e6.ndjson
  20,000,000 lines, 20,000,000 rows as load 9ff7e2be254073ca: 7,443 row groups in 534 file(s) over 393 day(s), loaded in 173.1s

                                   bytes   disk blocks    x raw
  raw                      7,194,479,974     1,756,466    1.000
  gzip -9                    419,379,067       102,388    0.058
  zstd -19                   213,107,089        52,029    0.030
  kibbr total                223,428,964        54,762    0.031
    data files               223,330,262        54,737    0.031
      column data            221,570,522                  0.031
      row group headers        1,320,378                  0.000
      schema records              57,672                  0.000
      footers                    368,874                  0.000
      file headers                12,816                  0.000
    catalog.log                      804             1    0.000
    manifest.log                  97,898            24    0.000

  kibbr total is 0.533 of gzip -9 and 1.048 of zstd -19
  gzip -9 ran on 1 thread in 114.1s; zstd -19 on 8 thread(s) in 1002.1s
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 20,000,000 lines, 20,000,000 rows, 14,450 row groups in 393 file(s) over 393 day(s), ingested in 222.2s
  after compaction: 20,000,000 rows, 2,791 row groups in 393 file(s) over 393 day(s), compacted in 41.7s

                                   bytes   disk blocks    x raw
  kibbr total, before        262,277,470        64,436    0.036
    data files               262,146,010        64,402    0.036
      column data            258,950,721                  0.036
      row group headers        2,548,936                  0.000
      schema records              42,444                  0.000
      footers                    594,477                  0.000
      file headers                 9,432                  0.000
    catalog.log                      804             1    0.000
    manifest.log                 130,656            33    0.000
  kibbr total, after         216,993,903        53,136    0.030
    data files               216,753,435        53,075    0.030
      column data            216,009,327                  0.030
      row group headers          499,794                  0.000
      schema records              42,444                  0.000
      footers                    192,438                  0.000
      file headers                 9,432                  0.000
    catalog.log                      804             1    0.000
    manifest.log                 239,664            60    0.000

  after compaction is 0.827 of before in bytes, 1.000 in files and 0.193 in row groups
```

## What it was measured with

- kibbr 0.4.0, commit `4a77f39f5165adeae9f91f62a48c560908205e56`
- `kibbr bench-storage urls-bf4f92c165e6.ndjson --scratch <scratch> --time-columns timestamp`
- Linux 7.0.0-1014-aws, x86_64, Intel(R) Xeon(R) Platinum 8488C, 8 cores (4 physical), 16,462,835,712 bytes of memory, on aws c7i.2xlarge, ap-southeast-2
- 0 warning(s) in bench-storage's log; 1501.3 s start to end, download included
