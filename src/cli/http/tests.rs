// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

#[test]
fn a_url_splits_into_an_address_and_a_prefix() {
    assert_eq!(
        parse_url("http://localhost:9200/").expect("url"),
        Url {
            address: "localhost:9200".into(),
            prefix: String::new()
        }
    );
    assert_eq!(parse_url("http://es/x").expect("url").address, "es:80");
    assert!(parse_url("https://es:9200").is_err());
}

#[test]
fn a_chunked_answer_is_put_back_together() {
    let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
    assert_eq!(
        parse_response(raw).expect("parse"),
        (200, b"abcde".to_vec())
    );
}
