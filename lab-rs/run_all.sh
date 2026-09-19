#!/usr/bin/env bash
# Runs everything the machine allows, in the order the lab prompt gives, then rebuilds
# RESULTS.md. Each step can also be run on its own.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
(cd .. && cargo build --release 2>&1 | tail -1)
cargo build --release 2>&1 | tail -1
A=(baseline_searches where_time_goes memory_and_startup search_under_load disk_full)
B=(file_layout_writes wal_batching buffer_lock appliers filters newest_n compaction_cost recovery_scan last_seen scan_threads template_lookup buffer_memory encoding_cost)
STEPS="${STEPS:-a b}"
if [[ " $STEPS " == *" a "* ]]; then
  for s in "${A[@]}"; do echo "##### blackbox/$s.sh"; ./blackbox/$s.sh 2>&1 | tee "out/$s.log"; done
fi
if [[ " $STEPS " == *" b "* ]]; then
  for s in "${B[@]}"; do echo "##### $s"; ./target/release/$s 2>&1 | tee "out/$s.log"; done
fi
./report.sh
