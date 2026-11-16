// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[test]
fn a_second_holder_is_refused_and_told_who_holds_it() {
    let dir = std::env::temp_dir().join(format!("kibbr-lock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut first = DataLock::acquire(&dir).expect("first");
    first
        .announce(&Holder::server(Some("127.0.0.1:7700".into()), None))
        .expect("announce");
    match DataLock::acquire(&dir) {
        Err(LockError::Held { holder, .. }) => {
            assert!(holder.contains("127.0.0.1:7700"), "{holder}");
        }
        other => panic!("a second holder got {other:?}"),
    }
    assert_eq!(
        holder(&dir).and_then(|h| h.address).as_deref(),
        Some("127.0.0.1:7700")
    );
    drop(first);
    assert!(DataLock::acquire(&dir).is_ok(), "released on drop");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_compaction_alone_is_named_as_one_and_reads_as_a_load_to_an_older_build() {
    let words = serde_json::to_value(Holder::compact()).expect("serialize");
    assert_eq!(words["compact"], true, "{words}");
    assert_eq!(words["load"], true, "{words}");
    let said = Holder::compact().describe();
    assert!(said.contains("a compaction running alone"), "{said}");
    // What a build before the field wrote: no `compact`, and still a load.
    let older: Holder = serde_json::from_str(r#"{"pid":7,"load":true}"#).expect("parse");
    assert!(!older.compact);
    assert!(older.describe().contains("a load running alone"));
}
