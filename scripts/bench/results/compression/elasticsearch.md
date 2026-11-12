# kibbr storage on elasticsearch, 2026-11-12

Elasticsearch 8 server JSON logs while it indexed another dataset (Zenodo 10516227), from YScope's CLP datasets: 7.98 GiB, 14,001,234 lines

- **Source:** https://zenodo.org/records/10516227/files/elasticsearch.tar.gz?download=1
- **Download:** `elasticsearch.tar.gz`, 173,964,916 bytes, sha256 `0c11428f9046d7839d80eb1fab0a4a6eb8a97f2184ef708900dab5534aae53d6`
- **Measured:** `elasticsearch/elasticsearch.log`: 14,001,234 lines, 14,001,234 rows

**8,565,304,619 bytes ingested.** As a plain file that takes 8,565,309,440 bytes on this disk; kibbr keeps it in 41,451,520, 99.5% less.

| store | bytes | on disk | x raw | x gzip -9 | x zstd -19 |
|---|---:|---:|---:|---:|---:|
| ingested, as a plain file | 8,565,304,619 | 8,565,309,440 | 1.000 | - | - |
| gzip -9 | 158,895,336 | 158,896,128 | 0.019 | - | - |
| zstd -19, 8 thread(s) | 78,123,227 | 78,127,104 | 0.009 | - | - |
| kibbr, bulk load | 41,443,190 | 41,451,520 | 0.005 | 0.261 | 0.530 |
| kibbr, live day compacted | 41,443,634 | 41,451,520 | 0.005 | 0.261 | 0.530 |

On disk is whole 4,096-byte blocks, as `bench-storage` measures them. kibbr's is its whole data directory: the data files, `catalog.log` and `manifest.log`.

## `kibbr bench-storage`

```
bench-storage /opt/kibbr/testdata/compression/elasticsearch/logs-f9ffa51dfe60.ndjson
  14,001,234 lines, 14,001,234 rows as load 396d76abc370d015: 1,746 row groups in 1 file(s) over 1 day(s), loaded in 207.8s

                                   bytes   disk blocks    x raw
  raw                      8,565,304,619     2,091,140    1.000
  gzip -9                    158,895,336        38,793    0.019
  zstd -19                    78,123,227        19,074    0.009
  kibbr total                 41,443,190        10,120    0.005
    data files                41,437,756        10,117    0.005
      column data             40,903,648                  0.005
      row group headers          469,381                  0.000
      schema records                 864                  0.000
      footers                     63,839                  0.000
      file headers                    24                  0.000
    catalog.log                    5,181             2    0.000
    manifest.log                     253             1    0.000

  kibbr total is 0.261 of gzip -9 and 0.530 of zstd -19
  gzip -9 ran on 1 thread in 67.7s; zstd -19 on 8 thread(s) in 473.1s
  disk blocks are 4,096 bytes: what each file takes on this disk, rounded up to whole blocks

the same lines as a live day: flushed 1,440 time(s) over the file, as a day that arrives evenly is flushed once a minute, then compacted
  before compaction: 14,001,234 lines, 14,001,234 rows, 3,234 row groups in 1 file(s) over 1 day(s), ingested in 247.2s
  after compaction: 14,001,234 rows, 1,746 row groups in 1 file(s) over 1 day(s), compacted in 41.8s

                                   bytes   disk blocks    x raw
  kibbr total, before         49,088,684        11,987    0.006
    data files                49,083,133        11,984    0.006
      column data             48,089,893                  0.006
      row group headers          874,945                  0.000
      schema records                 864                  0.000
      footers                    117,407                  0.000
      file headers                    24                  0.000
    catalog.log                    5,181             2    0.000
    manifest.log                     370             1    0.000
  kibbr total, after          41,443,634        10,120    0.005
    data files                41,437,801        10,117    0.005
      column data             40,903,648                  0.005
      row group headers          469,381                  0.000
      schema records                 864                  0.000
      footers                     63,884                  0.000
      file headers                    24                  0.000
    catalog.log                    5,181             2    0.000
    manifest.log                     652             1    0.000

  after compaction is 0.844 of before in bytes, 1.000 in files and 0.540 in row groups
```

## What it was measured with

- kibbr 0.4.0, commit `4a77f39f5165adeae9f91f62a48c560908205e56`
- `kibbr bench-storage logs-f9ffa51dfe60.ndjson --scratch <scratch> --time-columns @timestamp`
- Linux 7.0.0-1014-aws, x86_64, Intel(R) Xeon(R) Platinum 8488C, 8 cores (4 physical), 16,462,835,712 bytes of memory, on aws c7i.2xlarge, ap-southeast-2
- 0 warning(s) in bench-storage's log; 1231.3 s start to end, download included
