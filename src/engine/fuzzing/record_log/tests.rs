// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

/// The table is Castagnoli's, and so is the reader's: the two agree on the check
/// value, so a frame built here is a frame the reader takes.
#[test]
fn the_harness_crc_is_the_readers() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    assert_eq!(crc32c(b"123456789"), frame::crc32c(b"123456789"));
    assert_eq!(framed([&b"{}"[..]].into_iter()), frame::frame(b"{}"));
}

/// Each mode reaches the record decoders: a line is a record in the file it belongs
/// to and an unknown kind in the other, and under a new kind it is unknown to both.
#[test]
fn a_line_is_a_record_in_its_own_file_and_unknown_to_the_other() {
    let line = br#"{"kind":"time_source","template_id":101,"source":"arrival"}"#;
    let seen = check_file(&framed([&line[..]].into_iter()));
    assert_eq!(seen.catalog, vec![Outcome::Record]);
    assert_eq!(seen.manifest, vec![Outcome::Unknown("time_source".into())]);
    let (kind, payload) = of_an_unknown_kind(line);
    assert_eq!(kind, nearly("time_source", line.len()));
    let seen = check_file(&framed([payload.as_slice()].into_iter()));
    assert_eq!(seen.catalog, vec![Outcome::Unknown(kind.clone())]);
    assert_eq!(seen.manifest, vec![Outcome::Unknown(kind)]);
    for mode in 0..8u8 {
        let mut data = vec![mode];
        data.extend_from_slice(line);
        data.push(b'\n');
        run(&data);
    }
}

/// Every near spelling of every known kind is a kind neither file has, and each of
/// the eight is a spelling of its own.
#[test]
fn a_kind_spelled_nearly_right_is_unknown_to_both() {
    for kind in CATALOG_KINDS.iter().chain(&MANIFEST_KINDS) {
        let near: BTreeSet<String> = (0..8).map(|pick| nearly(kind, pick)).collect();
        assert!(near.len() >= 7, "{kind}: {near:?}");
        for near in near {
            assert!(!CATALOG_KINDS.contains(&near.as_str()), "{near}");
            assert!(!MANIFEST_KINDS.contains(&near.as_str()), "{near}");
        }
    }
}

/// A payload past 64KiB is one frame and one record. The fuzzer's inputs stop at a
/// few kilobytes, so a length read from fewer than its four bytes is caught here.
#[test]
fn a_payload_longer_than_two_bytes_can_count_is_one_record() {
    let name = "n".repeat(70_000);
    let line =
        format!(r#"{{"kind":"time_source","template_id":101,"source":"column:{name}"}}"#);
    let seen = check_file(&framed([line.as_bytes()].into_iter()));
    assert_eq!(seen.tear, None);
    assert_eq!(seen.catalog, vec![Outcome::Record]);
    assert_eq!(seen.manifest, vec![Outcome::Unknown("time_source".into())]);
    for mode in 0..8u8 {
        let mut data = vec![mode];
        data.extend_from_slice(line.as_bytes());
        data.push(b'\n');
        run(&data);
    }
}

/// The fifth mode builds every kind of both files, so what it holds the readers to is
/// not held of an empty file: seven records from seven choices, each a different kind.
#[test]
fn the_choices_build_every_kind_of_record() {
    let mut kinds = BTreeSet::new();
    for first in 0..7u8 {
        let mut choices = Choices {
            bytes: &[first, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        };
        let payload = match choices.record(&mut Vec::new(), &mut BTreeSet::new()) {
            Written::Catalog(record) => serde_json::to_value(record),
            Written::Manifest(record) => serde_json::to_value(record),
        };
        let payload = payload.expect("a record is JSON");
        kinds.insert(payload["kind"].as_str().expect("a kind").to_string());
    }
    let mut known: Vec<&str> = CATALOG_KINDS
        .iter()
        .chain(&MANIFEST_KINDS)
        .copied()
        .collect();
    known.sort_unstable();
    assert_eq!(kinds.iter().map(String::as_str).collect::<Vec<_>>(), known);
    run(&[
        4, 0, 7, 1, 2, 3, 1, 7, 2, 4, 3, 1, 0, 6, 1, 0, 1, 1, 9, 1, 1, 2, 3,
    ]);
}
