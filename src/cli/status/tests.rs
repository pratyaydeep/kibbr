// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use serde_json::json;

#[test]
fn the_table_lists_each_template_and_dashes_what_is_missing() {
    let answer = json!({"count": 2, "templates": [
        {"template_id": 101, "rows": 1204, "time_source": "column:ts",
         "path": ["event=login", "ts", "user"]},
        {"template_id": 102, "path": []},
    ]});
    assert_eq!(
        templates_table(&answer),
        " ID   ROWS  TIME       PATH\n\
             101  1,204  column:ts  event=login, ts, user\n\
             102      -  -\n\
             2 templates\n"
    );
    assert_eq!(
        templates_table(&json!({"templates": []})),
        "ID  ROWS  TIME  PATH\n0 templates\n"
    );
}

#[test]
fn stats_are_a_line_a_value_under_its_dotted_name() {
    let answer = json!({
        "uptime_seconds": 1.5,
        "storage": {"mode": "disk", "last_write_error": null, "days": []},
        "shed": {"by": [3, 0]},
    });
    assert_eq!(
        stats_lines(&answer),
        "uptime_seconds            1.5\n\
             storage.mode              disk\n\
             storage.last_write_error  null\n\
             storage.days              []\n\
             shed.by.0                 3\n\
             shed.by.1                 0\n"
    );
}

#[test]
fn health_is_a_word_and_then_what_is_wrong() {
    assert_eq!(health_line(&json!({"ok": true, "status": "ok"})), "ok");
    assert_eq!(
        health_line(&json!({"ok": true, "status": "degraded", "detail": "the disk is full"})),
        "degraded: the disk is full"
    );
    assert_eq!(health_line(&json!({"ok": true})), "ok");
}

/// The address of the settings is `tests/operations.rs`'s: it reads a variable, which is
/// the process's and not one test's.
#[test]
fn the_server_is_addr_and_then_the_one_the_directory_names() {
    let none = std::env::temp_dir().join(format!("kibbr-ask-none-{}", std::process::id()));
    let file = File::parse("[api]\nnative_addr = \"0.0.0.0:7811\"\n").expect("a file");

    let named = server(&file, Some("10.0.0.9:1".to_string()), Some(none));
    assert_eq!(named.expect("named").address, "10.0.0.9:1");

    let held = std::env::temp_dir().join(format!("kibbr-ask-held-{}", std::process::id()));
    std::fs::create_dir_all(&held).expect("mkdir");
    std::fs::write(
        held.join(crate::engine::LOCK_FILE),
        r#"{"pid":1,"address":"127.0.0.1:40123"}"#,
    )
    .expect("a LOCK");
    let by_lock = server(&file, None, Some(held.clone())).expect("by its LOCK");
    assert_eq!(by_lock.address, "127.0.0.1:40123");
    let _ = std::fs::remove_dir_all(&held);
}
