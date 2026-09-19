# The FOUR STANDARD SEARCHES, defined once from real `gen-logs --seed 42` data.
#   S1 one template, by a promoted value: `event` is promoted into the fingerprint
#      (`event=login_ok` appears in the template path), so this walks to those templates.
#   S2 many templates, by a key that is not promoted: `level` is a plain column, and
#      "error" appears in most templates, so this scans across them.
#   S3 one id: a real `err.trace_id` (a UUID column, every value different), taken from
#      line 1 of the seed-42 corpus.
#   S4 the newest 500 rows across everything, via TrieQL's LIMIT (there is no :9200
#      `_search` - that surface is ABSENT).
S1='COUNT LOG {"event":"login_ok"}'
S2='COUNT LOG {"level":"error"}'
S3='MATCH LOG {"err.trace_id":"aeb53b34-0c10-4971-8a69-db9873af8965"}'
S4='MATCH LOG {} LIMIT 500'
SEARCH_NAMES=(S1 S2 S3 S4)
search_q() { case "$1" in S1) echo "$S1";; S2) echo "$S2";; S3) echo "$S3";; S4) echo "$S4";; esac; }
