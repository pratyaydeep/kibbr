// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;

fn file(path: &str, sealed: bool) -> FileRecord {
    FileRecord {
        path: path.into(),
        file_id: hex(&[7; 16]),
        sealed,
        rows: sealed.then_some(3),
        min_ts: sealed.then_some(1),
        max_ts: sealed.then_some(2),
        finished: false,
    }
}

#[test]
fn the_last_word_on_a_path_wins_and_a_removed_path_is_gone() {
    let records = vec![
        Record::Add {
            files: vec![file("d/a.col", false), file("d/b.col", false)],
            source: None,
        },
        Record::Seal {
            files: vec![file("d/a.col", true)],
        },
        Record::Remove {
            files: vec!["d/b.col".into()],
        },
        Record::Replace {
            add: vec![file("d/c.col", true)],
            remove: vec!["d/a.col".into()],
            source: None,
            wal: None,
        },
    ];
    let live = live(&records);
    assert_eq!(live.keys().collect::<Vec<_>>(), vec!["d/c.col"]);
}

#[test]
fn a_file_id_reads_back_from_its_hex_and_nothing_else_does() {
    let id: [u8; 16] = std::array::from_fn(|at| at as u8 * 17);
    assert_eq!(file_id_of(&hex(&id)), Some(id));
    assert_eq!(file_id_of("00ff"), None);
    assert_eq!(file_id_of(&"g".repeat(32)), None);
    assert_eq!(
        load_of(COMPACT_SOURCE),
        None,
        "a compaction reads as a load"
    );
}

/// A sign is no digit, though `from_str_radix` takes one, and neither is a capital, which
/// a start would not match to a file.
#[test]
fn a_sign_or_a_capital_is_not_a_hex_digit() {
    assert_eq!(file_id_of(&"+f".repeat(16)), None);
    assert_eq!(file_id_of(&"-0".repeat(16)), None);
    assert_eq!(file_id_of(&"0A".repeat(16)), None);
    assert_eq!(file_id_of(&"é".repeat(16)), None);
    assert_eq!(file_id_of(&"0a".repeat(16)), Some([0x0a; 16]));
    assert_eq!(file_id_of(&hex(&[0xff; 16])), Some([0xff; 16]));

    assert_eq!(load_of("load:+000000000000001"), None);
    assert_eq!(load_of("load:-000000000000001"), None);
    assert_eq!(load_of("load:9F3A000000000001"), None);
    assert_eq!(load_of("load:0000000000000001"), Some(1));
    assert_eq!(load_of(&load_source(u64::MAX)), Some(u64::MAX));
    let signed = Record::Add {
        files: vec![],
        source: Some("load:+000000000000001".into()),
    };
    assert!(load_ids(&[signed]).is_empty());
}

/// serde's derived readers take an array for a record, a file or a `wal` mark, and each
/// would read as a record nobody wrote.
#[test]
fn a_json_array_where_the_format_has_an_object_is_malformed() {
    let id = hex(&[7; 16]);
    let arrays = [
        (
            r#"["remove",["2024-01-15/seg_0_000001.col"]]"#.to_string(),
            "the payload is a JSON array",
        ),
        (
            format!(r#"{{"kind":"add","files":[["d/a.col","{id}"]]}}"#),
            "`files[0]` is a JSON array",
        ),
        (
            format!(r#"{{"kind":"seal","files":[{{"path":"d/a.col","file_id":"{id}"}},["d/b.col","{id}",true]]}}"#),
            "`files[1]` is a JSON array",
        ),
        (
            r#"{"kind":"replace","add":[],"remove":[],"wal":["1122334455667788","2024-01-15",5]}"#
                .to_string(),
            "`wal` is a JSON array",
        ),
    ];
    for (payload, want) in &arrays {
        match decode(payload.as_bytes()) {
            Decoded::Malformed(why) => {
                assert!(
                    why.contains(want),
                    "{payload}: `{why}` does not say `{want}`"
                );
            }
            other => panic!("{payload} read as {other:?}"),
        }
    }
    // An array has no kind, so it is not an unknown one either.
    assert!(matches!(
        decode(br#"["compact",[]]"#),
        Decoded::Malformed(_)
    ));

    // The same as objects are records, and an unknown kind is still one.
    let objects = [
        r#"{"kind":"remove","files":["2024-01-15/seg_0_000001.col"]}"#.to_string(),
        format!(r#"{{"kind":"add","files":[{{"path":"d/a.col","file_id":"{id}"}}]}}"#),
        r#"{"kind":"replace","add":[],"remove":[],"wal":{"stream":"1122334455667788","day":"2024-01-15","through":5}}"#
            .to_string(),
    ];
    for payload in &objects {
        assert!(
            matches!(decode(payload.as_bytes()), Decoded::Record(_)),
            "{payload}"
        );
    }
    assert_eq!(
        decode(br#"{"kind":"compact","files":[]}"#),
        Decoded::Unknown("compact".into())
    );
}

/// A load's id comes back from its `source`, on an `add` and on a `replace` alike, and a
/// `replace` on disk with no `source` still reads.
#[test]
fn a_load_is_named_by_its_source_on_add_and_replace() {
    let id = 0x9f3a_0000_0000_0001;
    assert_eq!(load_source(id), "load:9f3a000000000001");
    assert_eq!(load_of(&load_source(id)), Some(id));
    assert_eq!(load_of("load:9f3a"), None);
    let records = vec![
        Record::Add {
            files: vec![file("d/a.col", true)],
            source: Some(load_source(id)),
        },
        Record::Replace {
            add: vec![file("d/b.col", true)],
            remove: vec!["d/a.col".into()],
            source: Some(load_source(id + 1)),
            wal: None,
        },
    ];
    assert_eq!(
        load_ids(&records).into_iter().collect::<Vec<_>>(),
        vec![id, id + 1]
    );
    let by = published_by_load(&records);
    assert_eq!(by.get("d/b.col"), Some(&(id + 1)));
    let old: Record =
        serde_json::from_str(r#"{"kind":"replace","add":[],"remove":["d/a.col"]}"#)
            .expect("a replace with no source");
    assert!(matches!(old, Record::Replace { source: None, .. }));
}

/// The catalog's reader, so the catalog's truncation rule: every cut of a manifest
/// reads back the records wholly before it, and reopening leaves the file there.
#[test]
fn a_manifest_cut_at_every_offset_keeps_the_records_before_the_cut() {
    let dir = std::env::temp_dir().join(format!("kibbr-manifest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");
    let path = path_in(&dir);
    let records = vec![
        Record::Add {
            files: vec![file("2024-01-15/seg_0_000001.col", false)],
            source: None,
        },
        Record::Seal {
            files: vec![file("2024-01-15/seg_0_000001.col", true)],
        },
    ];
    {
        let (mut m, got) = Manifest::open(&path, &mut |_| {}).expect("create");
        assert!(got.is_empty());
        m.append(&records).expect("append");
    }
    let whole = std::fs::read(&path).expect("read");
    let first = frame::HEADER + serde_json::to_vec(&records[0]).expect("json").len();
    for cut in 0..=whole.len() {
        std::fs::write(&path, &whole[..cut]).expect("write");
        let (m, got) = Manifest::open(&path, &mut |_| {}).expect("open");
        let want = if cut == whole.len() {
            2
        } else if cut >= first {
            1
        } else {
            0
        };
        assert_eq!(got.len(), want, "cut at {cut}");
        let kept = if want == 2 {
            whole.len()
        } else if want == 1 {
            first
        } else {
            0
        };
        assert_eq!(m.len(), kept as u64, "cut at {cut}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
