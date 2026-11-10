// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[test]
fn a_run_with_no_day_due_prints_nothing_as_lines_and_no_days_as_json() {
    let mut out = Vec::new();
    print_report(
        &Report {
            ok: true,
            days: Vec::new(),
        },
        false,
        &mut out,
    )
    .expect("print");
    assert!(out.is_empty(), "no day due printed {out:?}");
    print_report(
        &Report {
            ok: true,
            days: Vec::new(),
        },
        true,
        &mut out,
    )
    .expect("print");
    assert_eq!(out, b"{\"ok\":true,\"days\":[]}\n");
}

/// `LOCK` says `load` too, so a second command gives up at once rather than waiting for
/// a server still starting.
#[test]
fn a_compaction_alone_says_so_in_lock_and_a_second_command_is_refused_at_once() {
    let dir = std::env::temp_dir().join(format!("kibbr-compact-lock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let storage = crate::config::Storage {
        base_path: Some(dir.clone()),
        trie_file: Some(dir.join("trie.json")),
        ..crate::config::Storage::default()
    };
    let schema = crate::config::Schema::default();
    let args = || Args {
        day: None,
        threads: Some(1),
        segment_max_bytes: None,
        json: false,
    };

    // A directory with no day in it: the run is over at once, and what it wrote to
    // `LOCK` while it ran is still there to read.
    let db = crate::engine::Database::open_offline(
        crate::config::options(&storage, &schema),
        crate::engine::Opener::Compact,
    )
    .expect("the directory, alone");
    let mut out = Vec::new();
    alone(db, &args(), None, &mut out).expect("compact alone");
    assert!(out.is_empty(), "no day was due, and it printed {out:?}");
    let said = crate::engine::holder(&dir).expect("LOCK does not say who held it");
    assert!(
        said.compact && said.load && said.address.is_none(),
        "{said:?}"
    );

    // Those words under a lock that is held are a compaction still running.
    let _held = crate::engine::internals::DataLock::acquire(&dir).expect("the lock");
    let asked = Instant::now();
    match command(args(), None, storage, schema, &mut out) {
        Err(CompactError::Input(why)) => {
            assert!(why.contains("a compaction running alone"), "{why}")
        }
        other => panic!("a second command beside a compaction alone: {other:?}"),
    }
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "it waited {:?} for a server that was never starting",
        asked.elapsed()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
