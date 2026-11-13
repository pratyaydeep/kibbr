// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use std::fs::{self, File};

fn template(id: u64) -> Record {
    Record::Template {
        id,
        created_at: DateTime::from_timestamp(1_705_314_600, 0).expect("a valid timestamp"),
        tokens: vec![
            Token::Promoted {
                promoted: "event".into(),
                value: "login_failed".into(),
            },
            Token::Key { key: "ts".into() },
            Token::Key {
                key: "user.id".into(),
            },
        ],
    }
}

fn column(id: u64, name: &str, col_type: Option<ColumnType>) -> Record {
    Record::Column {
        template_id: id,
        name: name.into(),
        col_type,
        epoch_digits: None,
        widened_from: None,
        at: None,
    }
}

fn corpus() -> Vec<Record> {
    vec![
        template(101),
        column(101, "ts", Some(ColumnType::Datetime)),
        column(101, "user.id", Some(ColumnType::Uuid)),
        column(101, "event", Some(ColumnType::String)),
        template(102),
        column(102, "ip", Some(ColumnType::Ip)),
        column(102, "note", None),
        Record::Column {
            template_id: 101,
            name: "ts".into(),
            col_type: Some(ColumnType::String),
            epoch_digits: None,
            widened_from: Some(ColumnType::Datetime),
            at: Some(DateTime::from_timestamp(1_705_314_700, 0).expect("valid")),
        },
        Record::TimeSource {
            template_id: 101,
            source: "column:ts".into(),
        },
    ]
}

fn bytes_of(records: &[Record]) -> Vec<u8> {
    records
        .iter()
        .flat_map(|r| frame(r).expect("a record frames"))
        .collect()
}

#[test]
fn framing_is_length_crc_payload() {
    let bytes = frame(&template(101)).expect("frames");
    let len = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes")) as usize;
    assert_eq!(bytes.len(), HEADER + len);
    let payload = &bytes[HEADER..];
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")),
        frame::crc32c(payload)
    );
    let json: serde_json::Value = serde_json::from_slice(payload).expect("json");
    assert_eq!(json["kind"], "template");
    assert_eq!(json["id"], 101);
    assert_eq!(json["tokens"][0]["promoted"], "event");
    assert_eq!(json["tokens"][0]["value"], "login_failed");
    assert_eq!(json["tokens"][1]["key"], "ts");
}

#[test]
fn column_record_reads_as_the_spec_spells_it() {
    let bytes = frame(&column(101, "ts", Some(ColumnType::Datetime))).expect("frames");
    let json: serde_json::Value = serde_json::from_slice(&bytes[HEADER..]).expect("json");
    assert_eq!(
        json,
        serde_json::json!({"kind":"column","template_id":101,"name":"ts","type":"Datetime"})
    );
}

#[test]
fn a_whole_file_round_trips() {
    let records = corpus();
    let parsed = parse(&bytes_of(&records));
    assert_eq!(parsed.records, records);
    assert_eq!(parsed.torn_at, None);
    assert!(parsed.unknown.is_empty());
    assert!(parsed.malformed.is_empty());
}

/// Truncated at EVERY byte offset: the intact prefix comes back, whole records only,
/// and nothing panics.
#[test]
fn a_catalog_truncated_at_every_offset_rebuilds_the_intact_prefix() {
    let records = corpus();
    let whole = bytes_of(&records);
    let mut boundaries = vec![0usize];
    for record in &records {
        let next = boundaries[boundaries.len() - 1] + frame(record).expect("frames").len();
        boundaries.push(next);
    }
    for cut in 0..=whole.len() {
        let parsed = parse(&whole[..cut]);
        let intact = boundaries.iter().filter(|b| **b <= cut).count() - 1;
        assert_eq!(
            parsed.records.len(),
            intact,
            "cut at {cut}: expected {intact} whole records"
        );
        assert_eq!(&parsed.records[..], &records[..intact]);
        let on_boundary = boundaries.contains(&cut);
        assert_eq!(
            parsed.torn_at,
            (!on_boundary).then_some(boundaries[intact] as u64),
            "cut at {cut}: torn offset"
        );
    }
}

/// A flipped byte anywhere in the payload is a CRC failure and a tear at that record.
#[test]
fn a_corrupted_byte_is_a_tear_at_that_record() {
    let records = corpus();
    let mut bytes = bytes_of(&records);
    let first_len = frame(&records[0]).expect("frames").len();
    // Inside the second record's payload.
    let at = first_len + HEADER + 3;
    bytes[at] ^= 0x40;
    let parsed = parse(&bytes);
    assert_eq!(parsed.records, records[..1]);
    assert_eq!(parsed.torn_at, Some(first_len as u64));
}

#[test]
fn an_unknown_kind_is_skipped_and_named() {
    let mut bytes = bytes_of(&[template(101)]);
    let foreign = br#"{"kind":"merge","template_id":101,"into":102}"#;
    let skip_at = bytes.len();
    bytes.extend_from_slice(&(foreign.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&frame::crc32c(foreign).to_le_bytes());
    bytes.extend_from_slice(foreign);
    bytes.extend(bytes_of(&[template(102)]));

    let parsed = parse(&bytes);
    assert_eq!(parsed.records, vec![template(101), template(102)]);
    assert_eq!(parsed.unknown, vec![(skip_at as u64, "merge".to_string())]);
    assert_eq!(parsed.torn_at, None);
}

/// A record is one JSON object, and so is each token in it: serde's derived reader also
/// takes an array, which is intact, so it is skipped as malformed and the next record read.
#[test]
fn a_json_array_where_the_format_has_an_object_is_malformed() {
    let arrays: [(&[u8], &str); 4] = [
        (br#"["time_source",101,"arrival"]"#, "the payload is a JSON array"),
        (
            br#"["template",101,"2024-01-15T10:30:00Z",[{"key":"ts"}]]"#,
            "the payload is a JSON array",
        ),
        // serde refuses this one by itself, with a reason of its own: a token is
        // read by its fields' names, and an array has none.
        (
            br#"{"kind":"template","id":101,"created_at":"2024-01-15T10:30:00Z","tokens":[{"key":"ts"},["event","login"]]}"#,
            "did not match any variant",
        ),
        // An array has no kind: this is not the unknown kind `merge`.
        (br#"["merge",101,102]"#, ""),
    ];
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for (payload, _) in &arrays {
        offsets.push(bytes.len() as u64);
        bytes.extend(frame::frame(payload));
    }
    bytes.extend(bytes_of(&[template(102)]));

    let parsed = parse(&bytes);
    assert_eq!(parsed.records, vec![template(102)]);
    assert_eq!(parsed.torn_at, None);
    assert!(parsed.unknown.is_empty(), "{:?}", parsed.unknown);
    assert_eq!(
        parsed
            .malformed
            .iter()
            .map(|(at, _)| *at)
            .collect::<Vec<_>>(),
        offsets
    );
    for ((_, why), (_, want)) in parsed.malformed.iter().zip(&arrays) {
        assert!(why.contains(want), "`{why}` does not say `{want}`");
    }

    // A name is a string: serde also takes a one-key object for it.
    let named = br#"{"kind":"column","template_id":101,"name":"ip","type":{"Ip":null}}"#;
    let parsed = parse(&frame::frame(named));
    assert!(parsed.records.is_empty(), "{:?}", parsed.records);
    assert!(
        parsed.malformed[0].1.contains("`type` is a JSON object"),
        "{:?}",
        parsed.malformed
    );

    // The same records as objects, tokens and all, are records.
    let objects = [
        &br#"{"kind":"time_source","template_id":101,"source":"arrival"}"#[..],
        br#"{"kind":"template","id":101,"created_at":"2024-01-15T10:30:00Z","tokens":[{"key":"ts"},{"promoted":"event","value":"login"}]}"#,
    ];
    let bytes: Vec<u8> = objects.iter().flat_map(|p| frame::frame(p)).collect();
    let parsed = parse(&bytes);
    assert_eq!(parsed.records.len(), 2);
    assert!(parsed.malformed.is_empty(), "{:?}", parsed.malformed);
}

/// A payload the typed read takes and that is not JSON - here a number no float
/// holds, in a field the record does not have - is not one JSON object either.
#[test]
fn a_payload_that_is_not_json_is_malformed_whatever_the_record_ignores() {
    let payload =
        br#"{"kind":"time_source","template_id":101,"source":"arrival","later":1e999}"#;
    let parsed = parse(&frame::frame(payload));
    assert!(parsed.records.is_empty(), "{:?}", parsed.records);
    assert_eq!(parsed.malformed.len(), 1);
}

#[test]
fn an_absurd_length_is_a_tear_not_a_wait() {
    let mut bytes = bytes_of(&[template(101)]);
    let at = bytes.len();
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    bytes.extend_from_slice(&[0, 0, 0, 0, 1, 2, 3]);
    let parsed = parse(&bytes);
    assert_eq!(parsed.records.len(), 1);
    assert_eq!(parsed.torn_at, Some(at as u64));
}

#[test]
fn open_truncates_a_torn_tail_on_disk_and_appends_after_it() {
    let dir = std::env::temp_dir().join(format!("kibbr-catalog-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");
    let path = path_in(&dir);

    let mut bytes = bytes_of(&[template(101)]);
    let good = bytes.len();
    bytes.extend_from_slice(&[9, 0, 0, 0, 1, 1]);
    fs::write(&path, &bytes).expect("write");

    let mut warnings = Vec::new();
    let (mut catalog, records) = Catalog::open(&path, &mut |w| warnings.push(w)).expect("open");
    assert_eq!(records, vec![template(101)]);
    assert_eq!(catalog.len(), good as u64);
    assert_eq!(fs::metadata(&path).expect("stat").len(), good as u64);
    assert!(
        warnings
            .iter()
            .any(|w| w.contains(&format!("torn record at offset {good}"))),
        "{warnings:?}"
    );

    catalog.append(&[template(102)]).expect("append");
    drop(catalog);
    let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
    assert_eq!(records, vec![template(101), template(102)]);
    let _ = fs::remove_dir_all(&dir);
}

/// The directory is fsynced exactly once, when the file is created, and a failed
/// fsync is the open's error rather than a line in a log.
#[test]
fn creating_the_catalog_syncs_its_directory_once_and_a_failed_sync_is_an_error() {
    let dir = std::env::temp_dir().join(format!("kibbr-catalog-sync-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");
    let path = path_in(&dir);

    let synced = std::cell::RefCell::new(Vec::new());
    let mut counting = |d: &Path| {
        synced.borrow_mut().push(d.to_path_buf());
        Ok(())
    };
    let (mut catalog, _) =
        Catalog::open_with(&path, &mut |_| {}, &mut counting).expect("create");
    catalog.append(&[template(101)]).expect("append");
    drop(catalog);
    assert_eq!(
        *synced.borrow(),
        vec![dir.clone()],
        "the directory was not synced on creation"
    );

    // Reopening an existing catalog creates nothing, so it syncs nothing.
    let (_, records) = Catalog::open_with(&path, &mut |_| {}, &mut counting).expect("reopen");
    assert_eq!(records, vec![template(101)]);
    assert_eq!(
        synced.borrow().len(),
        1,
        "an existing catalog's directory was synced again"
    );

    fs::remove_file(&path).expect("rm");
    let mut failing = |_: &Path| Err(std::io::Error::other("injected fsync failure"));
    match Catalog::open_with(&path, &mut |_| {}, &mut failing) {
        Err(e @ CatalogError::SyncDir { .. }) => {
            assert!(e.to_string().contains("injected fsync failure"), "{e}");
        }
        Err(other) => panic!("failed for another reason: {other}"),
        Ok(_) => panic!("a failed directory fsync was ignored"),
    }
    assert!(
        !path.exists(),
        "a catalog whose directory was never synced was left for the next start to trust"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// The real directory sync, on a directory it cannot succeed on: write and search
/// permission without read, so the file can be created and the directory cannot be opened.
#[cfg(unix)]
#[test]
fn a_directory_that_cannot_be_synced_fails_the_first_open() {
    use std::os::unix::fs::PermissionsExt;

    let dir =
        std::env::temp_dir().join(format!("kibbr-catalog-nosync-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o300)).expect("chmod -wx");
    if File::open(&dir).is_ok() {
        // Root, or a filesystem that ignores the mode: nothing here can fail the sync.
        let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));
        let _ = fs::remove_dir_all(&dir);
        return;
    }
    let path = path_in(&dir);

    let opened = Catalog::open(&path, &mut |_| {});
    let left = path.exists();
    let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));
    let _ = fs::remove_dir_all(&dir);

    match opened {
        Err(CatalogError::SyncDir { path: named, .. }) => {
            assert_eq!(named, dir.display().to_string());
        }
        Err(other) => panic!("failed for another reason: {other}"),
        Ok(_) => panic!("created {} and never synced its directory", path.display()),
    }
    assert!(
        !left,
        "a catalog whose directory entry was never synced was left behind"
    );
}

/// **What a `Datetime` column says of its values is one optional field, `epoch_digits`**:
/// written only where there is something to say, so every other record is the bytes a
/// build from before the field wrote, and a record from such a build - which has no such
/// field - reads as a column that does not say.
#[test]
fn a_column_record_keeps_epoch_digits_and_reads_a_record_without_it() {
    let said = |epoch_digits| Record::Column {
        template_id: 101,
        name: "ts".into(),
        col_type: Some(ColumnType::Datetime),
        epoch_digits,
        widened_from: None,
        at: None,
    };
    // As ae7d8ca wrote a column record, byte for byte: no field.
    let older = br#"{"kind":"column","template_id":101,"name":"ts","type":"Datetime"}"#;
    assert_eq!(serde_json::to_vec(&said(None)).expect("json"), older);
    assert_eq!(read_record::<Record>(older), Ok(said(None)));
    assert_eq!(
        serde_json::to_string(&column(101, "n", Some(ColumnType::Int64))).expect("json"),
        r#"{"kind":"column","template_id":101,"name":"n","type":"Int64"}"#
    );

    for epoch_digits in [Some(true), Some(false)] {
        let record = said(epoch_digits);
        let payload = serde_json::to_vec(&record).expect("json");
        let text = String::from_utf8(payload.clone()).expect("utf-8");
        assert!(
            text.contains(&format!(r#""epoch_digits":{}"#, epoch_digits == Some(true))),
            "{text}"
        );
        assert_eq!(read_record::<Record>(&payload), Ok(record.clone()));
        // And through the file, framed, beside records of the other kinds.
        let mut records = corpus();
        records.push(record);
        assert_eq!(parse(&bytes_of(&records)).records, records);
    }
    // The field is a boolean: any other JSON there is a malformed record, not a guess.
    let wrong = br#"{"kind":"column","template_id":101,"name":"ts","type":"Datetime","epoch_digits":"yes"}"#;
    assert!(read_record::<Record>(wrong).is_err());
}
