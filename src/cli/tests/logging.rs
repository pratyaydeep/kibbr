// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// What the scripts and the tests read off a server's stderr.
#[test]
fn a_text_line_is_the_line_it_always_was() {
    assert_eq!(text_line(Level::INFO, "exit 0"), "kibbr: exit 0");
    assert_eq!(
        text_line(Level::WARN, "trie.json: no such file"),
        "kibbr: WARN trie.json: no such file"
    );
    assert_eq!(
        text_line(Level::ERROR, "write-ahead log - full"),
        "kibbr: ERROR write-ahead log - full"
    );
    assert_eq!(
        text_line(Level::DEBUG, "GET /health"),
        "kibbr: DEBUG GET /health"
    );
}

/// One object on one line whatever the sentence holds, the line's own four keys first
/// and never replaced by a field of the same name.
#[test]
fn a_json_line_is_one_object_on_one_line() {
    let line = json_line(
        "2026-10-10T14:18:26.123Z",
        Level::WARN,
        "kibbr::serve",
        "leaf 7 - \"data\\7\":\nPermission denied",
        vec![("requests", 3.into()), ("msg", "not this one".into())],
    );
    assert!(!line.contains('\n'), "{line}");
    let read: Value = serde_json::from_str(&line).expect("an object");
    assert_eq!(read["ts"], "2026-10-10T14:18:26.123Z");
    assert_eq!(read["level"], "warn");
    assert_eq!(read["target"], "kibbr::serve");
    assert_eq!(read["msg"], "leaf 7 - \"data\\7\":\nPermission denied");
    assert_eq!(read["requests"], 3);
    let keys: Vec<&String> = read.as_object().expect("an object").keys().collect();
    assert_eq!(keys, ["ts", "level", "target", "msg", "requests"]);
}

#[test]
fn a_level_lets_through_itself_and_what_is_louder() {
    let passes = |set: LogLevel, event: Level| event <= level_of(set);
    assert!(passes(LogLevel::Info, Level::INFO));
    assert!(passes(LogLevel::Info, Level::WARN));
    assert!(passes(LogLevel::Info, Level::ERROR));
    assert!(!passes(LogLevel::Info, Level::DEBUG));
    assert!(!passes(LogLevel::Error, Level::WARN));
    assert!(passes(LogLevel::Trace, Level::TRACE));
}
