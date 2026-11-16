// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What the topics share. A helper is here only if every file that had it had the same
//! one, word for word. Where two files' helpers of one name differed, each file keeps its
//! own: a `Server` that reads the whole banner and one that reads a line of it hold their
//! tests to different things.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;

use serde_json::Value;

/// The binary under test, with nothing set on its command.
pub fn kibbr() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kibbr"))
}

/// A text that must be JSON, as a value.
pub fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

/// A JSON array of strings as a set; anything else is the empty set.
pub fn strings(v: &Value) -> BTreeSet<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|s| s.as_str().expect("an array of strings").to_string())
        .collect()
}

/// FNV-1a over bytes: a digest two runs of a generator are compared by.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// One HTTP request on a connection of its own, read to its end: the status and the body.
/// A body the server stops reading is not a failure here; the answer is what is asserted.
pub fn request(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    let _ = socket.write_all(body);
    let _ = socket.flush();

    let mut response = Vec::new();
    socket
        .read_to_end(&mut response)
        .expect("failed to read the response");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body split in the response");
    let headers = String::from_utf8_lossy(&response[..split]).into_owned();
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{headers}"));
    (
        status,
        String::from_utf8_lossy(&response[split + 4..]).into_owned(),
    )
}
