// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use std::collections::BTreeSet;

use super::super::tests::encodings::chunk_both;
use super::super::file::{open_sealed, walk, Reader};
use super::super::read::{ALL_NULL, TWO_PIECES};
use crate::engine::testkit::row_groups::{self, Sample};
use super::super::{type_byte, NO_COMPRESSION, PER_ROW, PLAIN};
use super::*;

fn sample(name: &str) -> Sample {
    row_groups::layouts()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no sample named `{name}`"))
}

/// Column `v` of a sample, decoded.
fn decoded(sample: &Sample) -> Column {
    let chunk = &sample.chunks[sample.place("v").expect("a column v")];
    Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows).expect("decodes")
}

/// Every file under `testdata/format/`, read as compaction reads it and as a query does: the
/// typed decoder prints every row as the cell `Reader::chunks` made.
#[test]
fn the_vectors_read_the_same_through_both_readers() {
    let (mut row_groups, mut cells_held) = (0, 0);
    for path in row_groups::vector_files() {
        let bytes = std::fs::read(&path).expect("read");
        let index = match open_sealed(&path).expect("a vector opens") {
            Some(index) => index,
            None => walk(&path, &bytes).expect("a vector walks").index,
        };
        let mut reader = Reader::open(&path).expect("open for reading");
        for entry in &index.entries {
            let columns = index.schemas[entry.schema_id as usize].len();
            let header = reader.header(entry, columns).expect("header");
            let all: Vec<usize> = (0..columns).collect();
            let (cells, read) = reader.chunks(entry, &header, &all).expect("chunks");
            let (stored, read_stored) = reader.stored(entry, &header, &all).expect("stored");
            assert_eq!(read_stored, read, "{}: the bytes read", path.display());
            assert_eq!(stored.len(), cells.len());
            row_groups += 1;
            for ((chunk, dir), cells) in stored.iter().zip(&header.chunks).zip(&cells) {
                let from = chunk.at as usize;
                assert_eq!(chunk.bytes[..], bytes[from..from + dir.stored_len as usize]);
                let column = Column::decode(&chunk.bytes, chunk.at, dir, cells.len())
                    .expect("a vector's chunk decodes");
                let col = column.over(&chunk.bytes).expect("over its own bytes");
                let mut memo = Memo::default();
                for (row, cell) in cells.iter().enumerate() {
                    assert_eq!(col.cell(row).as_ref(), Ok(cell), "{}", path.display());
                    assert_eq!(col.cell_with(row, &mut memo).as_ref(), Ok(cell));
                    cells_held += 1;
                }
            }
        }
    }
    // The two-template vector whole and cut, and the encodings' file.
    assert!(
        row_groups >= 6 && cells_held > 100,
        "{row_groups} {cells_held}"
    );
}

/// Every layout, decoded both ways and held cell for cell - and the list of samples
/// held to being every layout.
#[test]
fn every_layout_decodes_to_the_cells_the_reader_prints() {
    let mut pairs = BTreeSet::new();
    let mut compressions = BTreeSet::new();
    let mut layouts = BTreeSet::new();
    let mut shapes = BTreeSet::new();
    let (mut mixed, mut per_row, mut slotted, mut bare) = (0, 0, 0, 0);
    for sample in row_groups::all() {
        for (chunk, name) in sample.chunks.iter().zip(&sample.columns) {
            let cells = chunk_both(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
                .unwrap_or_else(|bad| panic!("{}, `{name}`: {}", sample.name, bad.what));
            assert_eq!(cells.len(), sample.rows);

            compressions.insert(chunk.dir.compression);
            layouts.insert(chunk.dir.layout);
            for piece in [chunk.dir.a, chunk.dir.b].iter().flatten() {
                pairs.insert((type_byte(piece.form.column_type()), piece.encoding));
                mixed += usize::from(piece.form.kind() == JsonKind::Mixed);
                per_row += usize::from(piece.form.per_row_digits());
            }
            let column = Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
                .expect("decodes");
            let col = column.over(&chunk.bytes).expect("over its own bytes");
            match col.slots() {
                Some(slots) => {
                    slotted += 1;
                    assert_eq!(slots.len(), sample.rows);
                }
                None => bare += 1,
            }
            for piece in col.pieces().expect("pieces").iter().flatten() {
                shapes.insert(match piece.values {
                    ValuesRef::Ints(_) => "ints",
                    ValuesRef::Floats(_) => "floats",
                    ValuesRef::Times(_) => "times",
                    ValuesRef::Bools(_) => "bools",
                    ValuesRef::Words { .. } => "words",
                    ValuesRef::Texts(_) => "texts",
                    ValuesRef::Fixed { .. } => "fixed",
                    ValuesRef::SameInt(_) => "same int",
                    ValuesRef::SameFloat(_) => "same float",
                    ValuesRef::SameTime(_) => "same time",
                    ValuesRef::SameBool(_) => "same bool",
                    ValuesRef::SameText(_) => "same text",
                    ValuesRef::SameFixed(_) => "same fixed",
                });
            }
        }
    }
    // Plain and constant for each of the seven types, and each type's own encoding.
    let mut every = BTreeSet::new();
    for ty in 0..7u8 {
        every.insert((ty, PLAIN));
        every.insert((ty, CONSTANT));
    }
    every.extend([(0, DICTIONARY), (1, VARINT), (3, DELTA), (4, BITS)]);
    assert_eq!(pairs, every, "the type and encoding pairs the samples hold");
    assert_eq!(compressions, BTreeSet::from([NO_COMPRESSION, ZSTD]));
    assert_eq!(
        layouts,
        BTreeSet::from([0, SOME_NULL, TWO_PIECES, SOME_NULL | TWO_PIECES, ALL_NULL])
    );
    assert_eq!(shapes.len(), 13, "{shapes:?}");
    assert!(
        mixed > 3 && per_row > 2 && slotted > 3 && bare > 3,
        "{mixed} {per_row} {slotted} {bare}"
    );
}

/// What a column holds for a few chunks, by hand: the values as their encoding has them,
/// a slot a row only where a row's number is not its value's.
#[test]
fn a_column_holds_what_its_chunk_lays_out() {
    let numbers = sample("integers, bare and quoted");
    let (column, chunk) = (decoded(&numbers), &numbers.chunks[1]);
    let col = column.over(&chunk.bytes).expect("over");
    assert_eq!((col.rows(), col.slots()), (7, None));
    let [Some(a), None] = col.pieces().expect("pieces") else {
        panic!("one piece");
    };
    assert!(
        matches!(a.values, ValuesRef::Ints(v) if v == [200, 404, 503, 200, -17, 0, 45]),
        "{:?}",
        a.values
    );
    let kinds: Vec<bool> = (0..a.n).map(|i| a.lit(i)).collect();
    assert_eq!(kinds, [true, true, false, true, true, true, true]);
    assert_eq!(
        a.cell(2),
        Ok((ColValue::Int64(503), Some(serde_json::json!("503"))))
    );
    assert!(a.cell(7).is_err(), "a value past the piece's last");

    // Two pieces and a null: piece A is the integers, piece B the one word.
    let beside = sample("integers beside text and nulls");
    let (column, chunk) = (decoded(&beside), &beside.chunks[1]);
    let col = column.over(&chunk.bytes).expect("over");
    let slots: Vec<Option<(bool, usize)>> = col
        .slots()
        .expect("a slot a row")
        .iter()
        .map(|slot| slot.get())
        .collect();
    assert_eq!(
        slots,
        [
            Some((false, 0)),
            Some((false, 1)),
            Some((false, 2)),
            Some((true, 0)),
            Some((true, 1)),
            None,
            Some((false, 3)),
        ]
    );
    let [Some(a), Some(b)] = col.pieces().expect("pieces") else {
        panic!("two pieces");
    };
    assert!(matches!(a.values, ValuesRef::Ints(v) if v == [7, 8, 9, 45]));
    assert!(matches!(b.values, ValuesRef::SameText(word) if word == b"N/A"));
    assert_eq!((a.n, b.n), (4, 2));
    assert_eq!(col.cell(5), Ok((ColValue::Null, None)));
    assert_eq!(
        b.text_cell(b"N/A", false),
        Ok((ColValue::String("N/A".into()), None))
    );

    // A word list: the words in the order rows first name them, a number a row.
    let words = sample("words");
    let (column, chunk) = (decoded(&words), &words.chunks[1]);
    let col = column.over(&chunk.bytes).expect("over");
    let [Some(a), None] = col.pieces().expect("pieces") else {
        panic!("one piece");
    };
    let ValuesRef::Words { words, codes } = &a.values else {
        panic!("a word list: {:?}", a.values);
    };
    assert_eq!(words, &[&b"GET"[..], b"POST", b"DELETE", b""]);
    assert_eq!(codes, &[0, 1, 0, 0, 2, 1, 0, 3]);

    // Ids are their sixteen stored bytes, and one id on every row is stored once.
    let ids = sample("ids");
    let (column, chunk) = (decoded(&ids), &ids.chunks[1]);
    let col = column.over(&chunk.bytes).expect("over");
    let [Some(a), None] = col.pieces().expect("pieces") else {
        panic!("one piece");
    };
    let ValuesRef::Fixed { bytes, width } = a.values else {
        panic!("stored bytes: {:?}", a.values);
    };
    assert_eq!(
        (bytes.len(), width, bytes[0], bytes[16]),
        (48, 16, 0x55, 0x6b)
    );
    let one = sample("one id");
    let (column, chunk) = (decoded(&one), &one.chunks[1]);
    let col = column.over(&chunk.bytes).expect("over");
    let [Some(a), None] = col.pieces().expect("pieces") else {
        panic!("one piece");
    };
    assert!(matches!(a.values, ValuesRef::SameFixed(id) if id.len() == 16 && id[0] == 0x55));

    // Nothing but nulls is no piece at all.
    let nulls = sample("nothing but nulls");
    let (column, chunk) = (decoded(&nulls), &nulls.chunks[1]);
    let col = column.over(&chunk.bytes).expect("over");
    assert!(matches!(col.pieces(), Ok([None, None])) && col.slots().is_none());
    assert_eq!(col.cell(3), Ok((ColValue::Null, None)));
}

/// A column reads over the bytes it was decoded from and over no others: the wrong
/// bytes give an error or other text, never a panic.
#[test]
fn a_column_is_read_over_its_own_bytes() {
    for sample in row_groups::all() {
        for chunk in &sample.chunks {
            let column = Column::decode(&chunk.bytes, chunk.at, &chunk.dir, sample.rows)
                .expect("decodes");
            let mut longer = chunk.bytes.clone();
            longer.push(0);
            let bad = column.over(&longer).expect_err("another length");
            assert_eq!(bad.offset, chunk.at);
            assert!(bad.what.contains("was put over"), "{}", bad.what);
            if !chunk.bytes.is_empty() {
                assert!(column.over(&chunk.bytes[1..]).is_err());
            }
            let col = column.over(&chunk.bytes).expect("over its own bytes");
            let bad = col.cell(sample.rows).expect_err("a row past the last");
            assert!(bad.what.contains("of a chunk of"), "{}", bad.what);

            let other = vec![0xffu8; chunk.bytes.len()];
            let col = column.over(&other).expect("the same length");
            let mut memo = Memo::default();
            for row in 0..sample.rows {
                let _ = col.cell(row);
                let _ = col.cell_with(row, &mut memo);
            }
            let _ = col.pieces();
        }
    }
}

/// A value that does not print is refused when its row is printed, by the chunk's
/// offset and the row; the rows beside it print, and decoding alone refuses nothing.
#[test]
fn a_value_that_does_not_print_is_refused_at_its_row() {
    let text = Form::String {
        kind: JsonKind::Str,
    };
    let mut body = 3u32.to_le_bytes().to_vec();
    body.extend_from_slice(b"abc");
    body.extend_from_slice(&2u32.to_le_bytes());
    body.extend_from_slice(&[0xff, 0xfe]);
    let dir = one_piece(text, PLAIN, &body);
    let refused = read::chunk(&body, 40, &dir, 2).expect_err("not UTF-8");
    assert!(refused.what.contains("not UTF-8"), "{}", refused.what);
    assert!(chunk_both(&body, 40, &dir, 2).is_err());

    let column = Column::decode(&body, 40, &dir, 2).expect("the layout holds");
    let col = column.over(&body).expect("over");
    assert_eq!(col.cell(0), Ok((ColValue::String("abc".into()), None)));
    let bad = col.cell(1).expect_err("not UTF-8");
    assert_eq!(bad.offset, 40);
    assert_eq!(
        bad.what,
        "a string value is not UTF-8, at row 1 of the chunk"
    );
}

// ── A broken layout ────────────────────────────────────────────────────────────

/// The directory entry of a chunk of one piece, stored as it is.
fn one_piece(form: Form, encoding: u8, body: &[u8]) -> ChunkDir {
    ChunkDir {
        layout: 0,
        compression: NO_COMPRESSION,
        stored_len: body.len() as u32,
        stored_crc: 0,
        body_len: body.len() as u32,
        a: Some(PieceDir { form, encoding }),
        b: None,
    }
}

/// A word list of these words, then these indexes, one byte each.
fn words(words: &[&str], indexes: &[u8]) -> Vec<u8> {
    let mut out = (words.len() as u32).to_le_bytes().to_vec();
    for w in words {
        out.extend_from_slice(&(w.len() as u32).to_le_bytes());
        out.extend_from_slice(w.as_bytes());
    }
    out.extend_from_slice(indexes);
    out
}

/// Both readers refuse this chunk in the same words at the same offset, stored as it is
/// and through zstd.
fn refused_alike(body: &[u8], dir: &ChunkDir, rows: usize, why: &str) {
    const AT: u64 = 40;
    let read = read::chunk(body, AT, dir, rows).expect_err(why);
    let typed = Column::decode(body, AT, dir, rows).expect_err(why);
    assert_eq!(typed, read, "{why}");
    assert!(read.what.contains(why), "{why}: {}", read.what);
    assert!(read.offset >= AT, "{why}: offset {}", read.offset);

    let frame = zstd::bulk::compress(body, 3).expect("compress");
    let zdir = ChunkDir {
        compression: ZSTD,
        stored_len: frame.len() as u32,
        ..dir.clone()
    };
    let read = read::chunk(&frame, AT, &zdir, rows).expect_err(why);
    let typed = Column::decode(&frame, AT, &zdir, rows).expect_err(why);
    assert_eq!(typed, read, "{why}, through zstd");
    assert_eq!(read.offset, AT);
    if rows <= MAX_ROWS as usize {
        assert!(
            read.what.contains(why) && read.what.contains("of the chunk's body"),
            "{why}: {}",
            read.what
        );
    }
}

/// The typed decoder makes every check of the layout that `read::chunk` makes, in its
/// words and at its offset, on chunks whose values would all print.
#[test]
fn a_broken_layout_is_refused_in_the_reader_s_words() {
    let text = Form::String {
        kind: JsonKind::Str,
    };
    let int = |kind| Form::Int64 {
        kind,
        plus: false,
        width: 0,
    };
    let float = |kind| Form::Float64 {
        kind,
        digits: PER_ROW,
    };
    let flag = Form::Bool {
        kind: JsonKind::Lit,
        case: 0,
    };
    // Nine fraction digits print any instant, a nanosecond after the epoch included.
    let date = Form::Datetime {
        kind: JsonKind::Str,
        unit: 0,
        separator: b'T',
        zone: b'Z',
        offset: 0,
        digits: 9,
    };
    let floats = |values: &[f64]| -> Vec<u8> {
        values
            .iter()
            .flat_map(|f| f.to_bits().to_le_bytes())
            .collect()
    };

    // A word list.
    for (body, rows, why) in [
        (words(&["a", "b"], &[0, 2, 1]), 3, "names word 2"),
        (words(&[], &[]), 1, "of 0 words"),
        (words(&["a", "b", "c"], &[0, 1]), 2, "of 3 words for 2 rows"),
        (words(&["a", "b"], &[0, 1, 1, 0]), 3, "left over"),
        (
            words(&["a", "b"], &[1, 0, 1]),
            3,
            "names word 1 before word 0",
        ),
        (words(&["a", "b", "c"], &[0, 1, 0]), 3, "whose rows name 2"),
        (words(&["a", "b"], &[0, 1]), 3, "dictionary indexes"),
        (vec![1, 0, 0, 0, 9, 0, 0, 0, b'a'], 1, "string wants 9"),
        (vec![1, 0], 1, "dictionary word count"),
    ] {
        refused_alike(&body, &one_piece(text, DICTIONARY, &body), rows, why);
    }
    // A word that stands for more than a body may hold, as a constant and in a list.
    let word = vec![b'x'; 1 << 20];
    let mut constant = (word.len() as u32).to_le_bytes().to_vec();
    constant.extend_from_slice(&word);
    refused_alike(
        &constant,
        &one_piece(text, CONSTANT, &constant),
        300,
        "past the 268435456",
    );
    let mut list = 1u32.to_le_bytes().to_vec();
    list.extend_from_slice(&constant);
    list.extend(std::iter::repeat_n(0u8, 300));
    refused_alike(
        &list,
        &one_piece(text, DICTIONARY, &list),
        300,
        "past the 268435456",
    );

    // Numbers, bits and differences.
    for (body, rows, why) in [
        (vec![0x80, 0x00], 1, "does not need"),
        ([vec![0xff; 9], vec![0x02]].concat(), 1, "64 bits"),
        (vec![], 1, "only 0 remain"),
        (vec![0x02, 0x04], 1, "left over"),
    ] {
        refused_alike(
            &body,
            &one_piece(int(JsonKind::Lit), VARINT, &body),
            rows,
            why,
        );
        refused_alike(&body, &one_piece(date, DELTA, &body), rows, why);
    }
    refused_alike(
        &[0b1000_0101],
        &one_piece(flag, BITS, &[0]),
        3,
        "past its last row",
    );
    refused_alike(&[], &one_piece(flag, BITS, &[]), 3, "Bool bits wants 1");
    refused_alike(&[1, 2], &one_piece(flag, PLAIN, &[1, 2]), 2, "Bool byte 2");
    refused_alike(
        &[1],
        &one_piece(flag, CONSTANT, &[1]),
        300_000_000,
        "past the 8192",
    );

    // The kind bitmap and the digits bytes.
    refused_alike(
        &[0b111, 2, 4, 6],
        &one_piece(int(JsonKind::Mixed), VARINT, &[0; 4]),
        3,
        "all one kind",
    );
    refused_alike(
        &[0b1001, 2, 4, 6],
        &one_piece(int(JsonKind::Mixed), VARINT, &[0; 4]),
        3,
        "kind bitmap has bits set past",
    );
    let numbers = floats(&[1.5, 2.5]);
    let spelled = [vec![0b01, 2, 1], numbers.clone()].concat();
    refused_alike(
        &spelled,
        &one_piece(float(JsonKind::Mixed), PLAIN, &spelled),
        2,
        "a JSON number with a digits byte",
    );
    let same = [vec![0b10, 1, 3], floats(&[1.5])].concat();
    refused_alike(
        &same,
        &one_piece(float(JsonKind::Mixed), CONSTANT, &same),
        2,
        "a JSON number with a digits byte",
    );
    refused_alike(
        &[1],
        &one_piece(float(JsonKind::Str), PLAIN, &[1]),
        2,
        "per-row digits wants 2",
    );

    // Nulls and a second piece.
    let nullable = |layout: u8, body: &[u8]| ChunkDir {
        layout,
        b: (layout & TWO_PIECES != 0).then_some(PieceDir {
            form: text,
            encoding: CONSTANT,
        }),
        ..one_piece(int(JsonKind::Lit), VARINT, body)
    };
    for (layout, body, why) in [
        (SOME_NULL, vec![0b11, 2, 4], "marks 2 of 2 present"),
        (SOME_NULL, vec![0b00], "marks 0 of 2 present"),
        (
            SOME_NULL,
            vec![0b101, 2],
            "presence bitmap has bits set past",
        ),
        (
            TWO_PIECES,
            vec![0b00, 2, 0, 0, 0, 2, 4],
            "a piece of two holds no rows",
        ),
        (
            TWO_PIECES,
            vec![0b11, 0, 0, 0, 0],
            "a piece of two holds no rows",
        ),
        (TWO_PIECES, vec![0b10, 9, 0, 0, 0, 2], "piece A wants 9"),
        (TWO_PIECES, vec![0b10, 1, 0], "piece_a_len"),
        (
            TWO_PIECES,
            vec![0b10, 1, 0, 0, 0, 2, 1, 0, 0, 0, b'a', 0],
            "left over",
        ),
    ] {
        refused_alike(&body, &nullable(layout, &body), 2, why);
    }

    // What only a zstd frame can break: both refuse it at the chunk, before a body.
    let body = words(&["a", "b"], &[0, 1, 1]);
    let frame = zstd::bulk::compress(&body, 3).expect("compress");
    let len = body.len() as u32;
    let mut longer = frame.clone();
    longer.push(0);
    for (stored, body_len, why) in [
        (longer, len, "after the chunk's zstd frame"),
        (frame.clone(), len + 1, "where body_len says"),
        (
            frame.clone(),
            len - 1,
            "does not decompress to its body_len",
        ),
        (body.clone(), len, "not a zstd frame"),
    ] {
        let dir = ChunkDir {
            compression: ZSTD,
            stored_len: stored.len() as u32,
            body_len,
            ..one_piece(text, DICTIONARY, &body)
        };
        let read = read::chunk(&stored, 40, &dir, 3).expect_err(why);
        assert_eq!(Column::decode(&stored, 40, &dir, 3).expect_err(why), read);
        assert!(read.what.contains(why), "{why}: {}", read.what);
    }
}

/// The typed decoder reads an encoding only on the type that has it, as the header
/// refuses any other pair where the directory names it.
#[test]
fn an_encoding_on_another_type_is_refused_as_the_header_refuses_it() {
    let int = Form::Int64 {
        kind: JsonKind::Lit,
        plus: false,
        width: 0,
    };
    let mut body = 1u32.to_le_bytes().to_vec();
    body.extend_from_slice(&7i64.to_le_bytes());
    body.push(0);
    for encoding in [DICTIONARY, DELTA, BITS, 6, 0xff] {
        let dir = one_piece(int, encoding, &body);
        let bad = Column::decode(&body, 40, &dir, 1).expect_err("another type's encoding");
        assert_eq!(bad.offset, 40);
        assert_eq!(
            bad.what,
            format!("encoding {encoding} on a Int64 piece, which this build does not read")
        );
        assert!(!encodes(int.column_type(), encoding));
    }
}

/// Every chunk of the samples with a bit changed, a byte cut off or added: both readers
/// take the same ones, refuse the same ones, and print the same cells.
#[test]
fn every_edit_of_a_body_is_taken_or_refused_by_both_readers() {
    let (mut taken, mut refused, mut chunks) = (0u32, 0u32, 0u32);
    let mut both = |body: &[u8], dir: &ChunkDir, rows: usize, zstd: bool| {
        let plain = ChunkDir {
            compression: NO_COMPRESSION,
            stored_len: body.len() as u32,
            body_len: body.len() as u32,
            ..dir.clone()
        };
        match chunk_both(body, 40, &plain, rows) {
            Ok(_) => taken += 1,
            Err(_) => refused += 1,
        }
        if zstd {
            let frame = zstd::bulk::compress(body, 3).expect("compress");
            let dir = ChunkDir {
                compression: ZSTD,
                stored_len: frame.len() as u32,
                ..plain
            };
            let _ = chunk_both(&frame, 40, &dir, rows);
        }
    };
    for sample in row_groups::all() {
        for chunk in &sample.chunks {
            let body = match chunk.dir.compression {
                ZSTD => zstd::bulk::decompress(&chunk.bytes, chunk.dir.body_len as usize)
                    .expect("a sample's frame"),
                _ => chunk.bytes.clone(),
            };
            if body.is_empty() || body.len() > 160 {
                continue;
            }
            chunks += 1;
            for at in 0..body.len() {
                for bit in 0..8 {
                    let mut edited = body.clone();
                    edited[at] ^= 1 << bit;
                    both(&edited, &chunk.dir, sample.rows, (at + bit) % 8 == 0);
                }
            }
            both(&body[..body.len() - 1], &chunk.dir, sample.rows, true);
            let mut longer = body.clone();
            longer.push(0);
            both(&longer, &chunk.dir, sample.rows, true);
            // One row more and one fewer than the body was laid out for.
            both(&body, &chunk.dir, sample.rows + 1, true);
            both(&body, &chunk.dir, sample.rows - 1, true);
        }
    }
    // Not agreement on nothing: thousands of edits read, and thousands refused.
    assert!(
        chunks > 60 && taken > 2_000 && refused > 2_000,
        "{chunks} chunks, {taken} taken, {refused} refused"
    );
}
