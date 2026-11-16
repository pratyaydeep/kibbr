// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::format::build::{build, schema_of, Level};
use crate::format::Source;
use crate::model::row::Row;
use crate::model::types::ColValue;

const T0: i64 = 1_705_314_600_000_000_000;

/// One row group of one row keyed `key`, and a schema record first if its column set is
/// new to the file.
fn group(w: &mut Writer, key: &str, n: i64) {
    let mut row = Row::new();
    row.insert("ts".into(), ColValue::Datetime(T0 + n));
    row.insert(key.into(), ColValue::Int64(n));
    let columns = schema_of([&row]);
    let schema_id = w.schema_id(&columns).expect("schema");
    let rg = build(
        7,
        schema_id,
        &columns,
        &[&row],
        &[T0 + n],
        Source::NotRecorded,
        Level::Live,
    )
    .expect("build");
    w.append(7, &rg).expect("append");
}

/// A rollback takes back the bytes, the schema records and the row groups written since
/// the mark: the next schema takes the number the forgotten one had.
#[test]
fn a_rollback_forgets_everything_written_since_the_mark() {
    let dir = std::env::temp_dir().join(format!("kibbr-rollback-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("seg_0_000001.col");
    let mut w = Writer::create(&path).expect("create");
    group(&mut w, "a", 1);
    let (mark, len) = (w.mark(), w.len());
    group(&mut w, "b", 2);
    assert_eq!((w.index().schemas.len(), w.index().entries.len()), (2, 2));
    w.rollback(mark).expect("rollback");
    assert_eq!(
        (w.index().schemas.len(), w.index().entries.len(), w.len()),
        (1, 1, len)
    );
    assert_eq!(fs::metadata(&path).expect("stat").len(), len);

    group(&mut w, "c", 3);
    let written = w.seal().expect("seal");
    let read = open_sealed(&path).expect("readable").expect("sealed");
    let _ = fs::remove_dir_all(&dir);
    assert_eq!(read.schemas, written.schemas);
    assert_eq!(read.schemas[1], vec!["c".to_string(), "ts".to_string()]);
    assert_eq!(read.entries.len(), 2);
}

/// A sealed file of two row groups of one row each, in a directory of its own: the
/// directory, the file, and the index its footer gives.
fn two_groups(name: &str) -> (PathBuf, PathBuf, Index) {
    let dir = std::env::temp_dir().join(format!("kibbr-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("mkdir");
    let path = dir.join("seg_0_000001.col");
    let mut w = Writer::create(&path).expect("create");
    group(&mut w, "a", 1);
    group(&mut w, "b", 2);
    w.seal().expect("seal");
    let index = open_sealed(&path).expect("readable").expect("sealed");
    (dir, path, index)
}

/// `open_sealed` over a file held in memory.
fn sealed_bytes(bytes: &[u8]) -> Result<Option<Index>, FormatError> {
    let read = |at: u64, buf: &mut [u8]| {
        buf.copy_from_slice(&bytes[at as usize..at as usize + buf.len()]);
        Ok(())
    };
    sealed(Path::new("seg_0_000001.col"), bytes.len() as u64, read)
}

/// The offset and the words of a refusal.
fn corrupt<T: std::fmt::Debug>(got: Result<T, FormatError>) -> (u64, String) {
    match got {
        Err(FormatError::Corrupt { offset, what, .. }) => (offset, what),
        other => panic!("not refused as corrupt: {other:?}"),
    }
}

/// A footer entry that is not a header frame inside the file is refused, and the offset
/// the error names is where the entry is written in the footer, not the one it claims.
#[test]
fn a_footer_entry_that_is_not_in_the_file_is_refused_where_it_is_written() {
    let id = [7u8; 16];
    for claimed in [1_000_000, u64::MAX - 3, u64::MAX - 47, HEADER_LEN as u64] {
        let mut index = Index::new(id);
        index.schemas.push(vec!["ts".to_string()]);
        index.entries.push(Entry {
            template_id: 7,
            offset: claimed,
            frame_len: 48,
            schema_id: 0,
            row_count: 1,
            min_time: T0,
            max_time: T0,
            chunks_len: 0,
        });
        let mut bytes = header_bytes(&id).to_vec();
        bytes.extend(index.seal_bytes());
        // The one entry is the 36 bytes before the part count and the trailer.
        let written = (bytes.len() - TRAILER_LEN - 4 - 36) as u64;
        let (offset, what) = corrupt(sealed_bytes(&bytes));
        assert_eq!(offset, written, "{what}");
        assert!(
            what.contains(&format!("at offset {claimed}")) && what.contains("the footer"),
            "{what}"
        );
    }
}

/// An entry that runs into the row group after it is refused the same way, and one of
/// more rows than a row group holds is refused before anything is read by it.
#[test]
fn a_footer_entry_over_the_next_or_past_the_row_cap_is_refused() {
    let (dir, path, index) = two_groups("footer-entries");
    let bytes = fs::read(&path).expect("read");
    let _ = fs::remove_dir_all(&dir);
    let footer_at = bytes.len() - index.seal_bytes().len();
    let resealed = |edit: &dyn Fn(&mut Index)| {
        let mut index = index.clone();
        edit(&mut index);
        let mut out = bytes[..footer_at].to_vec();
        out.extend(index.seal_bytes());
        out
    };
    assert_eq!(
        sealed_bytes(&resealed(&|_| {})).expect("readable"),
        Some(index.clone())
    );

    let second = index.entries[1].offset;
    let (offset, what) = corrupt(sealed_bytes(&resealed(&|i| {
        i.entries[0].frame_len = (second - i.entries[0].offset) as u32 + 1;
    })));
    assert!(offset >= footer_at as u64 && offset < bytes.len() as u64);
    assert!(what.contains("the row group after it"), "{what}");

    for (rows, refused) in [(MAX_ROWS, false), (MAX_ROWS + 1, true), (u32::MAX, true)] {
        let got = sealed_bytes(&resealed(&|i| i.entries[0].row_count = rows));
        if refused {
            let (offset, what) = corrupt(got);
            assert!(offset >= footer_at as u64 && offset < bytes.len() as u64);
            assert!(what.contains("a rule of its fields"), "{what}");
        } else {
            assert!(got.expect("readable").is_some(), "{rows} rows");
        }
    }
}

/// A read is held to the file's length before its buffer is made: a length the file
/// cannot meet is a refusal naming a place in the file.
#[test]
fn a_read_past_the_end_of_the_file_is_refused_before_a_buffer_is_made() {
    let (dir, path, index) = two_groups("read-past-the-end");
    let len = fs::metadata(&path).expect("stat").len();
    let mut reader = Reader::open(&path).expect("open");
    let e = index.entries[0];
    let columns = index.schemas[e.schema_id as usize].len();
    let header = reader.header(&e, columns).expect("header");
    let (cells, read) = reader.chunks(&e, &header, &[0, 1]).expect("chunks");
    assert_eq!((cells.len(), read), (2, u64::from(header.chunks_len)));

    // A header frame of four gibibytes, in a file of a few hundred bytes.
    let long = Entry {
        frame_len: u32::MAX,
        ..e
    };
    let (offset, what) = corrupt(reader.header(&long, columns));
    assert_eq!(offset, e.offset, "{what}");
    assert!(what.contains("runs past the end of the file"), "{what}");
    // One that starts past the end is named by the end, and one whose end no u64
    // holds is refused and not wrapped round.
    for start in [len + 1, u64::MAX - 3] {
        let far = Entry { offset: start, ..e };
        let (offset, what) = corrupt(reader.header(&far, columns));
        assert_eq!(offset, len, "{what}");
        assert!(what.contains(&format!("at offset {start}")), "{what}");
        let (offset, _) = corrupt(reader.chunks(&far, &header, &[0]));
        assert_eq!(offset, len);
    }
    // A chunk the directory says is longer than the file.
    let mut lying = header.clone();
    lying.chunks[1].stored_len = MAX_BODY;
    let (offset, what) = corrupt(reader.chunks(&e, &lying, &[1]));
    let _ = fs::remove_dir_all(&dir);
    assert!(offset > e.offset && offset < len, "{what}");
    assert!(what.contains("a chunk of 268435456 bytes"), "{what}");
}

/// A chunk read as it is stored is the bytes `chunks` decodes, read and checked the
/// same way, with the same refusal at the same offset in the same words.
#[test]
fn a_stored_chunk_is_the_bytes_chunks_decodes() {
    let (dir, path, index) = two_groups("stored-chunks");
    let bytes = fs::read(&path).expect("read");
    let len = bytes.len() as u64;
    let mut reader = Reader::open(&path).expect("open");
    for e in &index.entries {
        let columns = index.schemas[e.schema_id as usize].len();
        let header = reader.header(e, columns).expect("header");
        let (cells, read) = reader.chunks(e, &header, &[0, 1]).expect("chunks");
        let (stored, read_stored) = reader.stored(e, &header, &[0, 1]).expect("stored");
        assert_eq!((stored.len(), read_stored), (2, read));
        let area = e.offset + u64::from(e.frame_len);
        let offsets = header.chunk_offsets();
        for (at, chunk) in stored.iter().enumerate() {
            let dir = &header.chunks[at];
            assert_eq!(chunk.at, area + offsets[at]);
            let from = chunk.at as usize;
            assert_eq!(chunk.bytes[..], bytes[from..from + dir.stored_len as usize]);
            assert_eq!(frame::crc32c(&chunk.bytes), dir.stored_crc);
            // Not decoded, and nothing lost by that: decoding them gives the cells.
            assert_eq!(
                read::chunk(&chunk.bytes, chunk.at, dir, 1).as_ref(),
                Ok(&cells[at])
            );
        }
        // One column asked for is one chunk read.
        let (one, read_one) = reader.stored(e, &header, &[1]).expect("stored");
        assert_eq!(read_one, u64::from(header.chunks[1].stored_len));
        assert_eq!(one, stored[1..]);

        let far = Entry {
            offset: len + 1,
            ..*e
        };
        let refusal = corrupt(reader.stored(&far, &header, &[0]));
        assert_eq!(refusal.0, len, "{}", refusal.1);
        assert_eq!(refusal, corrupt(reader.chunks(&far, &header, &[0])));

        let mut lying = header.clone();
        for (stored_len, why) in [
            (MAX_BODY, "runs past the end of the file"),
            (MAX_BODY + 1, "a chunk of 268435457 bytes"),
        ] {
            lying.chunks[1].stored_len = stored_len;
            let refusal = corrupt(reader.stored(e, &lying, &[1]));
            assert!(refusal.1.contains(why), "{}", refusal.1);
            assert_eq!(refusal, corrupt(reader.chunks(e, &lying, &[1])));
        }

        let mut wrong = header.clone();
        wrong.chunks[0].stored_crc ^= 1;
        let refusal = corrupt(reader.stored(e, &wrong, &[0]));
        assert_eq!(refusal, (area, "a chunk's CRC does not hold".to_string()));
        assert_eq!(refusal, corrupt(reader.chunks(e, &wrong, &[0])));
    }
    let _ = fs::remove_dir_all(&dir);
}

/// A header that says more chunk bytes than its entry has is refused when it is read,
/// before a copy allocates the area it names or a chunk is read out of the next row group.
#[test]
fn a_header_that_says_more_chunk_bytes_than_its_entry_has_is_refused() {
    let (dir, path, index) = two_groups("chunk-area");
    let mut reader = Reader::open(&path).expect("open");
    for e in &index.entries {
        let columns = index.schemas[e.schema_id as usize].len();
        let header = reader.header(e, columns).expect("header");
        let copied = reader.copy(e, columns, 9).expect("copy");
        assert_eq!(copied.chunks.len(), header.chunks_len as usize);

        let short = Entry {
            chunks_len: header.chunks_len - 1,
            ..*e
        };
        let (offset, what) = corrupt(reader.header(&short, columns));
        assert_eq!(offset, e.offset, "{what}");
        assert!(what.contains("a chunk area of"), "{what}");
        let (offset, _) = corrupt(reader.copy(&short, columns, 9));
        assert_eq!(offset, e.offset);
    }
    let _ = fs::remove_dir_all(&dir);
}
