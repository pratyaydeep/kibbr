#!/bin/sh
# Runs every fuzz target at once, each for SECONDS (default 1800, thirty minutes), and
# prints each one's last status line.
#
#   fuzz/run.sh [seconds] [target...]
#
# Each target runs in libFuzzer's fork mode so that a crash is filed under
# fuzz/artifacts/<target>/ and the run goes on to its full time. The memory limits are
# what holds "never allocates unbounded memory from a length prefix": no harness in
# src/fuzzing/ allocates anything large itself, so 512MB in one malloc is the subject's.
#
# A crasher is fixed, then committed under fuzz/regressions/<target>/ with a name that
# says what it broke; tests/fuzz_regressions.rs replays those and the corpus on stable.
set -u
cd "$(dirname "$0")"
secs=${1:-1800}
[ $# -gt 0 ] && shift
[ $# -eq 0 ] && set -- trieql wal_record cursor v3_segment record_log
cargo +nightly fuzz build || exit 1
mkdir -p logs
for t in "$@"; do
    cargo +nightly fuzz run "$t" -- -max_total_time="$secs" -malloc_limit_mb=512 \
        -rss_limit_mb=2048 -fork=1 -ignore_crashes=1 -ignore_ooms=1 -ignore_timeouts=1 \
        >"logs/$t.log" 2>&1 &
done
wait
bad=0
for t in "$@"; do
    echo "$t: $(grep -E '^#[0-9]+: cov' "logs/$t.log" | tail -1)"
    n=$(ls "artifacts/$t" 2>/dev/null | wc -l | tr -d ' ')
    [ "$n" -gt 0 ] && { echo "$t: $n artifact(s) in fuzz/artifacts/$t"; bad=1; }
done
exit $bad
