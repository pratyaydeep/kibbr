//! The encodings held to what they promise: each piece gets the one encoding its
//! values call for, every value comes back as the JSON that was ingested, and a reader
//! refuses bytes an encoding cannot have written.

use serde_json::Value;

use super::build::{build, schema_of, BuildError, Level, RowGroup};
use super::column::{Column, Memo};
use super::read::{self, ChunkDir, Header, PieceDir};
use super::{
    index_width, put_varint, unzigzag, zigzag, Bad, Bytes, Form, JsonKind, Source, BITS, CONSTANT,
    DELTA, DICTIONARY, MAX_BODY, MAX_ROWS, NO_COMPRESSION, PLAIN, VARINT, ZSTD,
};
use crate::db::row_of;
use crate::frame::Next;
use crate::store::{effective_ts, Row};
use kibbr_core::flatten::parse_line;

/// `read::chunk`, with the typed decoder held to it on the way: what every test here
/// decodes a chunk through, so that each chunk they build - the ones built to be refused
/// among them - is read both ways.
///
/// The two readers walk one layout and must not drift. `read::chunk` gives cells exactly
/// when the typed decoder takes the chunk and prints every row of it, and then each row
/// prints as the cell `read::chunk` made, asked for alone or through a memo. Where
/// `read::chunk` refuses, the typed decoder refuses the chunk or one of its rows: which,
/// and in what words, is `read::chunk`'s to say and is what the tests here go on to pin.
pub(super) fn chunk_both(
    stored: &[u8],
    at: u64,
    dir: &ChunkDir,
    rows: usize,
) -> Result<Vec<read::Cell>, Bad> {
    let cells = read::chunk(stored, at, dir, rows);
    let column = Column::decode(stored, at, dir, rows);
    match (&cells, &column) {
        (Ok(cells), Ok(column)) => {
            let col = column
                .over(stored)
                .expect("a column over the bytes it was decoded from");
            assert_eq!(col.rows(), cells.len(), "a column of another length");
            let mut memo = Memo::default();
            for (row, cell) in cells.iter().enumerate() {
                assert_eq!(col.cell(row).as_ref(), Ok(cell), "row {row}");
                assert_eq!(
                    col.cell_with(row, &mut memo).as_ref(),
                    Ok(cell),
                    "row {row}, through the memo"
                );
            }
            // A second pass finds every word and constant already printed.
            for (row, cell) in cells.iter().enumerate() {
                assert_eq!(
                    col.cell_with(row, &mut memo).as_ref(),
                    Ok(cell),
                    "row {row}, out of the memo"
                );
            }
            assert!(col.cell(cells.len()).is_err(), "a row past the last");
        }
        (Ok(_), Err(bad)) => panic!(
            "the typed decoder refused a chunk `read::chunk` decodes: {}",
            bad.what
        ),
        (Err(_), Ok(column)) => {
            let col = column
                .over(stored)
                .expect("a column over the bytes it was decoded from");
            assert!(
                (0..col.rows()).any(|row| col.cell(row).is_err()),
                "the typed decoder printed every row of a chunk `read::chunk` refuses"
            );
        }
        (Err(_), Err(_)) => {}
    }
    cells
}

/// Lines through the real parser into one row group, as the store would build it.
fn group_of(lines: &[String]) -> (Vec<String>, RowGroup) {
    group_at(lines, Level::Live)
}

fn group_at(lines: &[String], level: Level) -> (Vec<String>, RowGroup) {
    try_group(lines, level).expect("build")
}

/// The same, when the builder may refuse the rows.
fn try_group(lines: &[String], level: Level) -> Result<(Vec<String>, RowGroup), BuildError> {
    let rows: Vec<Row> = lines
        .iter()
        .map(|l| row_of(&parse_line(l.as_bytes(), 1).expect("a line that parses")))
        .collect();
    let refs: Vec<&Row> = rows.iter().collect();
    let times: Vec<i64> = rows
        .iter()
        .map(|r| effective_ts(r, "ts").expect("a time"))
        .collect();
    let columns = schema_of(refs.iter().copied());
    let rg = build(7, 0, &columns, &refs, &times, Source::NotRecorded, level)?;
    Ok((columns, rg))
}

fn header_of(rg: &RowGroup, columns: usize) -> Header {
    let Next::Frame { payload, .. } = crate::frame::read(&rg.frame, 0, u32::MAX) else {
        panic!("not a frame");
    };
    read::header(payload, 0, |_| Some(columns)).expect("header")
}

/// Every row of the row group as the JSON object an answer prints.
fn rows_of(columns: &[String], rg: &RowGroup) -> Vec<Value> {
    let header = header_of(rg, columns.len());
    let cells: Vec<Vec<read::Cell>> = header
        .chunks
        .iter()
        .zip(header.chunk_offsets())
        .map(|(dir, at)| {
            let body = &rg.chunks[at as usize..at as usize + dir.stored_len as usize];
            chunk_both(body, at, dir, header.row_count as usize).expect("chunk")
        })
        .collect();
    (0..header.row_count as usize)
        .map(|r| {
            let mut row = Row::new();
            for (at, column) in cells.iter().enumerate() {
                let (value, written) = column[r].clone();
                row.insert_written(columns[at].clone(), value, written);
            }
            Value::Object(
                columns
                    .iter()
                    .map(|c| (c.clone(), row.json(c).unwrap_or(Value::Null)))
                    .collect(),
            )
        })
        .collect()
}

/// One line per value, each `{"ts": ..., "v": <value>}` a second apart.
fn lines_of(values: &[&str]) -> Vec<String> {
    values
        .iter()
        .enumerate()
        .map(|(n, v)| format!(r#"{{"ts":"2024-01-15T10:30:{:02}Z","v":{v}}}"#, n % 60))
        .collect()
}

/// Column `v`'s directory entry.
fn dir_of(lines: &[String]) -> ChunkDir {
    let (columns, rg) = group_of(lines);
    let at = columns.iter().position(|c| c == "v").expect("a column v");
    header_of(&rg, columns.len()).chunks[at].clone()
}

/// The encoding of each piece of column `v`, and its chunk's stored length.
fn pieces_of(lines: &[String]) -> (Vec<u8>, u32) {
    let dir = dir_of(lines);
    let encodings = [dir.a, dir.b]
        .iter()
        .flatten()
        .map(|p| p.encoding)
        .collect();
    (encodings, dir.stored_len)
}

/// The lines come back exactly as they went in.
fn round_trips(lines: &[String]) {
    let (columns, rg) = group_of(lines);
    let want: Vec<Value> = lines
        .iter()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    assert_eq!(rows_of(&columns, &rg), want);
}

// ── Constant ───────────────────────────────────────────────────────────────────────

/// The same value on every row is stored once, whatever its type: the chunk holds one
/// value's plain bytes and nothing else, and every row reads back as itself.
#[test]
fn the_same_value_on_every_row_is_stored_once() {
    for (value, bytes) in [
        (r#""GET""#, 4 + 3),
        ("200", 8),
        (r#""0045000012""#, 8),
        ("0.25", 8),
        (r#""0.010""#, 8),
        (r#""2024-01-15T10:30:00.120+05:30""#, 8),
        ("1705312200", 8),
        ("true", 1),
        (r#""True""#, 1),
        (r#""550E8400-E29B-41D4-A716-446655440000""#, 16),
        (r#""10.0.0.1""#, 4),
        (r#""2001:db8::1""#, 16),
    ] {
        let lines = lines_of(&[value; 100]);
        assert_eq!(pieces_of(&lines), (vec![CONSTANT], bytes), "{value}");
        round_trips(&lines);
    }
}

/// One value, spelled two ways - a JSON number and a string, or with two digit counts - is
/// still one value: the kind bitmap and the digits keep each row's spelling.
#[test]
fn a_constant_keeps_each_row_s_spelling() {
    for values in [
        &["200", r#""200""#, "200", r#""200""#][..],
        &[r#""12.5""#, r#""12.50""#, r#""12.500""#][..],
        &[
            r#""2024-01-15T10:30:00.5Z""#,
            r#""2024-01-15T10:30:00.500Z""#,
        ][..],
        &["true", r#""true""#][..],
    ] {
        let lines = lines_of(values);
        assert_eq!(pieces_of(&lines).0, vec![CONSTANT], "{values:?}");
        round_trips(&lines);
    }
}

/// Equal is byte for byte in plain layout: `-0.0` is not `0.0`, though they compare equal.
#[test]
fn negative_zero_is_not_the_same_value_as_zero() {
    let lines = lines_of(&["0.0", "-0.0"]);
    assert_eq!(pieces_of(&lines).0, vec![PLAIN]);
    round_trips(&lines);
}

/// A null row is a clear bit beside a constant, and a second piece has its own encoding.
#[test]
fn a_constant_sits_beside_nulls_and_another_piece() {
    let lines = lines_of(&["7", "null", "7", r#""N/A""#, "7"]);
    assert_eq!(pieces_of(&lines).0, vec![CONSTANT, CONSTANT]);
    round_trips(&lines);
    let lines = lines_of(&["7", "8", "9", r#""N/A""#, r#""N/A""#, "null"]);
    assert_eq!(pieces_of(&lines).0, vec![VARINT, CONSTANT]);
    round_trips(&lines);
}

// ── Word lists ─────────────────────────────────────────────────────────────────────

/// Text is a word list, compressed with zstd, and reads back as itself.
#[test]
fn text_is_stored_as_a_word_list_and_compressed() {
    let methods = ["GET", "POST", "GET", "GET", "DELETE", "POST", "GET"];
    let values: Vec<String> = methods.iter().map(|m| format!(r#""{m}""#)).collect();
    let lines = lines_of(&values.iter().map(String::as_str).collect::<Vec<_>>());
    let dir = dir_of(&lines);
    assert_eq!(dir.a.map(|p| p.encoding), Some(DICTIONARY));
    assert_eq!(dir.compression, ZSTD);
    // Three words of 4 + n bytes each, a count, and one byte per row.
    assert_eq!(dir.body_len, 4 + (4 + 3) + (4 + 4) + (4 + 6) + 7);
    round_trips(&lines);
}

/// An index is one byte up to 256 words, two up to 65,536, and four past that. A row
/// group holds 8,192 rows and a list no more words than rows, so a file reaches two bytes
/// and never four: past the row cap the widths are held as the rule alone.
#[test]
fn a_word_list_s_indexes_are_as_wide_as_its_words_need() {
    for (words, width) in [(256usize, 1u32), (257, 2), (MAX_ROWS as usize, 2)] {
        let values: Vec<String> = (0..words).map(|n| format!(r#""w{n:08}""#)).collect();
        let lines = lines_of(&values.iter().map(String::as_str).collect::<Vec<_>>());
        let dir = dir_of(&lines);
        let n = words as u32;
        assert_eq!(dir.body_len, 4 + n * (4 + 9) + n * width, "{words} words");
        round_trips(&lines);
    }
    for (words, width) in [(1, 1), (256, 1), (257, 2), (65_536, 2), (65_537, 4)] {
        assert_eq!(index_width(words), width, "{words} words");
    }
}

/// A JSON number stored as text and the same digits as a string are one word, printed
/// once each way.
#[test]
fn a_word_keeps_each_row_s_spelling() {
    let lines = lines_of(&[
        r#""a""#,
        "9223372036854775809",
        r#""9223372036854775809""#,
        r#""a""#,
        "9223372036854775809",
    ]);
    let dir = dir_of(&lines);
    assert_eq!(
        dir.a.map(|p| (p.form, p.encoding)),
        Some((
            Form::String {
                kind: JsonKind::Mixed
            },
            DICTIONARY
        ))
    );
    round_trips(&lines);
}

/// Live and finished files differ only in how hard zstd worked.
#[test]
fn either_level_reads_back_the_same() {
    let values: Vec<String> = (0..500).map(|n| format!(r#""user-{}""#, n % 37)).collect();
    let lines = lines_of(&values.iter().map(String::as_str).collect::<Vec<_>>());
    let (columns, live) = group_at(&lines, Level::Live);
    let (_, finished) = group_at(&lines, Level::Finished);
    assert_eq!(rows_of(&columns, &live), rows_of(&columns, &finished));
}

// ── Variable-length integers ───────────────────────────────────────────────────────

/// Integers are zigzag LEB128, compressed, in every spelling an Int64 piece prints, out to
/// both ends of the type.
#[test]
fn integers_are_stored_in_as_few_bytes_as_they_need() {
    for values in [
        &["200", "404", r#""503""#, "200", "-17"][..],
        &[r#""0045000012""#, r#""0000000001""#][..],
        &[r#""+4500000120""#, r#""+12""#][..],
        &["9223372036854775807", "-9223372036854775808", "0"][..],
    ] {
        let lines = lines_of(values);
        let dir = dir_of(&lines);
        assert_eq!(dir.a.map(|p| p.encoding), Some(VARINT), "{values:?}");
        assert_eq!(dir.compression, ZSTD);
        round_trips(&lines);
    }
    // Status codes take two bytes each, not eight.
    let values: Vec<String> = (0..300).map(|n| (200 + n % 7).to_string()).collect();
    let lines = lines_of(&values.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(dir_of(&lines).body_len, 300 * 2);
    round_trips(&lines);
}

// ── Bytes ──────────────────────────────────────────────────────────────────────────

/// A UUID is its sixteen bytes, as they are: random bytes give zstd nothing to find.
#[test]
fn uuids_are_sixteen_raw_bytes() {
    let ids = [
        r#""550e8400-e29b-41d4-a716-446655440000""#,
        r#""6ba7b810-9dad-11d1-80b4-00c04fd430c8""#,
        r#""550e8400-e29b-41d4-a716-446655440000""#,
    ];
    let lines = lines_of(&ids);
    let dir = dir_of(&lines);
    assert_eq!(dir.a.map(|p| p.encoding), Some(PLAIN));
    assert_eq!((dir.compression, dir.stored_len), (NO_COMPRESSION, 3 * 16));
    round_trips(&lines);
}

/// An address is its 4 or 16 bytes, through zstd: a session's addresses repeat. IPv4 and
/// IPv6 in one column are two pieces, and one of them wanting zstd compresses the chunk.
#[test]
fn addresses_are_their_bytes_through_zstd() {
    for (values, encodings) in [
        (
            &[r#""10.0.0.1""#, r#""10.0.0.2""#, r#""10.0.0.1""#][..],
            vec![PLAIN],
        ),
        (&[r#""2001:db8::1""#, r#""2001:db8::2""#][..], vec![PLAIN]),
        (
            &[
                r#""10.0.0.1""#,
                r#""2001:db8::1""#,
                r#""10.0.0.1""#,
                r#""2001:db8::2""#,
                r#""10.0.0.1""#,
            ][..],
            vec![CONSTANT, PLAIN],
        ),
    ] {
        let lines = lines_of(values);
        let dir = dir_of(&lines);
        let got: Vec<u8> = [dir.a, dir.b]
            .iter()
            .flatten()
            .map(|p| p.encoding)
            .collect();
        assert_eq!(got, encodings, "{values:?}");
        assert_eq!(dir.compression, ZSTD, "{values:?}");
        round_trips(&lines);
    }
}

/// Booleans are a bit a row, not a byte, and not compressed.
#[test]
fn booleans_are_a_bit_a_row() {
    let values: Vec<&str> = (0..20)
        .map(|n| match n % 4 {
            0 => "true",
            1 => "false",
            2 => r#""true""#,
            _ => "false",
        })
        .collect();
    let lines = lines_of(&values);
    let dir = dir_of(&lines);
    assert_eq!(dir.a.map(|p| p.encoding), Some(BITS));
    // The kind bitmap and the values, three bytes each for twenty rows.
    assert_eq!((dir.compression, dir.stored_len), (NO_COMPRESSION, 3 + 3));
    round_trips(&lines);
    let err = read_edited(&["true", "false", "true"], |b| {
        *b.last_mut().unwrap() |= 0x80
    })
    .expect_err("a bit past the last row");
    assert!(err.contains("past its last row"), "{err}");
}

/// Everything else - a float, in every spelling a Float64 piece prints - is plain bytes
/// through zstd.
#[test]
fn floats_are_plain_through_zstd() {
    let lines = lines_of(&[
        r#""0.010""#,
        "1.5",
        r#""149.00""#,
        r#""12.5""#,
        "1e3",
        r#""-0.0""#,
        r#""9223372036854775808""#,
    ]);
    let dir = dir_of(&lines);
    assert_eq!(dir.a.map(|p| p.encoding), Some(PLAIN));
    assert_eq!(dir.compression, ZSTD);
    let want: Vec<Value> = lines
        .iter()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    let (columns, rg) = group_of(&lines);
    // `1e3` reads back as the number it is, 1000.0.
    let got = rows_of(&columns, &rg);
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) {
        match &w["v"] {
            Value::String(_) => assert_eq!(g["v"], w["v"]),
            number => assert_eq!(g["v"].as_f64(), number.as_f64()),
        }
    }
}

// ── Differences ────────────────────────────────────────────────────────────────────

/// Times are differences, compressed, in any order and in every spelling a Datetime piece
/// prints - and the two ends of the range, whose difference wraps.
#[test]
fn times_are_stored_as_differences() {
    for values in [
        &[
            r#""2024-01-15T10:30:00Z""#,
            r#""2024-01-15T10:30:00.5Z""#,
            r#""2024-01-15T10:29:59.25Z""#,
            r#""2024-01-15T10:31:00Z""#,
        ][..],
        &["1705312200", "1705312201", "1705312199", r#""1705312300""#][..],
        &["1705312200123", "1705312200124", "1705312100000"][..],
        &[
            r#""2024-01-15T16:00:00.120+05:30""#,
            r#""2024-01-15T16:00:01.000+05:30""#,
        ][..],
        &[
            r#""1677-09-21T00:12:43.145224192Z""#,
            r#""2262-04-11T23:47:16.854775807Z""#,
            r#""1677-09-21T00:12:43.145224192Z""#,
        ][..],
    ] {
        let lines = lines_of(values);
        let dir = dir_of(&lines);
        assert_eq!(dir.a.map(|p| p.encoding), Some(DELTA), "{values:?}");
        assert_eq!(dir.compression, ZSTD);
        round_trips(&lines);
    }
}

/// A second a row, as the farm writes them, is a byte or two a row before zstd.
#[test]
fn times_a_second_apart_are_a_few_bytes_each() {
    let values: Vec<String> = (0..600)
        .map(|n| format!(r#""2024-01-15T10:{:02}:{:02}Z""#, n / 60, n % 60))
        .collect();
    let lines = lines_of(&values.iter().map(String::as_str).collect::<Vec<_>>());
    let dir = dir_of(&lines);
    // The first time whole, then 599 differences of 1e9 nanoseconds: 5 bytes each.
    assert_eq!(dir.body_len, 9 + 599 * 5);
    round_trips(&lines);
}

#[test]
fn zigzag_keeps_small_numbers_small_either_side_of_zero() {
    for (n, z) in [
        (0i64, 0u64),
        (-1, 1),
        (1, 2),
        (-2, 3),
        (i64::MAX, u64::MAX - 1),
        (i64::MIN, u64::MAX),
    ] {
        assert_eq!(zigzag(n), z);
        assert_eq!(unzigzag(z), n);
    }
}

#[test]
fn a_varint_has_one_spelling_and_a_reader_takes_no_other() {
    for (n, bytes) in [
        (0u64, vec![0x00]),
        (1, vec![0x01]),
        (127, vec![0x7f]),
        (128, vec![0x80, 0x01]),
        (300, vec![0xac, 0x02]),
        (
            u64::MAX,
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
        ),
    ] {
        let mut out = Vec::new();
        put_varint(n, &mut out);
        assert_eq!(out, bytes, "{n}");
        let mut r = Bytes::new(&out, 0);
        assert_eq!(r.varint("n"), Ok(n));
        assert!(r.is_empty());
    }
    for (bytes, why) in [
        (vec![0x80, 0x00], "does not need"),
        (vec![0xff, 0x80, 0x00], "does not need"),
        (vec![0xff; 9].into_iter().chain([0x02]).collect(), "64 bits"),
        (
            vec![0xff; 9].into_iter().chain([0x81, 0x01]).collect(),
            "64 bits",
        ),
        (vec![0x80, 0x80], "only 0 remain"),
        (vec![], "only 0 remain"),
    ] {
        let err = Bytes::new(&bytes, 0).varint("n").expect_err(why);
        assert!(err.what.contains(why), "{bytes:02x?}: {}", err.what);
    }
}

// ── What a reader refuses ──────────────────────────────────────────────────────────

/// Column `v`'s directory entry rewritten by `edit` - given the entry's bytes, returning
/// new ones - and the row group's header read back.
fn header_with(lines: &[String], edit: impl Fn(&[u8]) -> Vec<u8>) -> Result<Header, String> {
    let (columns, rg) = group_of(lines);
    let header = header_of(&rg, columns.len());
    let v = columns.iter().position(|c| c == "v").expect("v");
    let Next::Frame { payload, .. } = crate::frame::read(&rg.frame, 0, u32::MAX) else {
        panic!("not a frame");
    };
    let fixed = 1 + 8 + 4 + 4 + 8 + 8 + 1 + 8 + 8 + 8 + 4;
    let from = fixed + header.chunks[..v].iter().map(entry_len).sum::<usize>();
    let to = from + entry_len(&header.chunks[v]);
    let mut edited = payload[..from].to_vec();
    edited.extend(edit(&payload[from..to]));
    edited.extend_from_slice(&payload[to..]);
    read::header(&edited, 0, |_| Some(columns.len())).map_err(|bad| bad.what)
}

/// An all-null chunk has no bytes for a frame to hold, so it never says zstd.
#[test]
fn an_all_null_chunk_that_says_zstd_is_refused() {
    let lines = lines_of(&["null", "null"]);
    assert!(header_with(&lines, |e| e.to_vec()).is_ok());
    let err = header_with(&lines, |e| {
        let mut e = e.to_vec();
        e[1] = ZSTD;
        e.extend_from_slice(&0u32.to_le_bytes());
        e
    })
    .expect_err("refused");
    assert!(err.contains("all-null chunk with bytes"), "{err}");
}

/// A reader is told what it will allocate before it decompresses, and refuses past 256MiB.
#[test]
fn a_body_past_256mib_is_refused_before_anything_is_allocated() {
    let lines = lines_of(&[r#""a""#, r#""b""#]);
    for (body_len, refused) in [(MAX_BODY, false), (MAX_BODY + 1, true), (u32::MAX, true)] {
        let got = header_with(&lines, |e| {
            let mut e = e.to_vec();
            assert_eq!(e[1], ZSTD);
            e[10..14].copy_from_slice(&body_len.to_le_bytes());
            e
        });
        match got {
            Err(err) => assert!(refused && err.contains("a chunk body of"), "{err}"),
            Ok(_) => assert!(!refused, "{body_len} was read"),
        }
    }
}

/// A piece of `rows` rows of `form`, laid out as `encoding`, from these body bytes.
fn read_piece(
    body: &[u8],
    rows: usize,
    form: Form,
    encoding: u8,
) -> Result<Vec<read::Cell>, String> {
    let dir = ChunkDir {
        layout: 0,
        compression: NO_COMPRESSION,
        stored_len: body.len() as u32,
        stored_crc: 0,
        body_len: body.len() as u32,
        a: Some(PieceDir { form, encoding }),
        b: None,
    };
    chunk_both(body, 0, &dir, rows).map_err(|bad| bad.what)
}

/// A few stored bytes never decode to more than a plain body could hold: a constant or a
/// word list stands for its rows written out, and that is held to 256MiB like a body.
#[test]
fn a_piece_that_stands_for_more_than_a_body_is_refused() {
    let text = Form::String {
        kind: JsonKind::Str,
    };
    // One word of a mebibyte, on 300 rows: 300MiB written plain.
    let word = vec![b'x'; 1 << 20];
    let mut constant = (word.len() as u32).to_le_bytes().to_vec();
    constant.extend_from_slice(&word);
    let err = read_piece(&constant, 300, text, CONSTANT).expect_err("refused");
    assert!(err.contains("past the 268435456"), "{err}");
    assert_eq!(
        read_piece(&constant, 3, text, CONSTANT).map(|c| c.len()),
        Ok(3)
    );
    let mut list = 1u32.to_le_bytes().to_vec();
    list.extend_from_slice(&constant);
    list.extend(std::iter::repeat_n(0u8, 300));
    let err = read_piece(&list, 300, text, DICTIONARY).expect_err("refused");
    assert!(err.contains("past the 268435456"), "{err}");
}

// ── The row cap ────────────────────────────────────────────────────────────────────

/// A row group holds 8,192 rows and no more. The builder refuses to write one of more,
/// and a reader refuses a header that claims one where its `row_count` is written, before
/// a cell is made: what is stored cannot vouch for the count, since an all-null chunk has
/// no bytes at all and a constant has one value.
#[test]
fn a_row_group_past_the_row_cap_is_neither_written_nor_read() {
    let full = lines_of(&vec!["null"; MAX_ROWS as usize]);
    let (columns, rg) = group_of(&full);
    assert_eq!(header_of(&rg, columns.len()).row_count, MAX_ROWS);
    assert_eq!(rows_of(&columns, &rg).len(), MAX_ROWS as usize);

    let mut over = full.clone();
    over.push(full[0].clone());
    assert_eq!(
        try_group(&over, Level::Live).map(|_| ()),
        Err(BuildError::TooManyRows(MAX_ROWS as usize + 1))
    );

    // The full row group's own header, claiming more rows than it has.
    let Next::Frame { payload, .. } = crate::frame::read(&rg.frame, 0, u32::MAX) else {
        panic!("not a frame");
    };
    for rows in [MAX_ROWS + 1, 6_000_000, u32::MAX] {
        let mut edited = payload.to_vec();
        edited[13..17].copy_from_slice(&rows.to_le_bytes());
        let bad = read::header(&edited, 0, |_| Some(columns.len())).expect_err("refused");
        assert_eq!(bad.offset, 13, "{}", bad.what);
        assert!(bad.what.contains("past the 8192"), "{}", bad.what);
    }

    // A chunk is not decoded for more rows either, whoever asks: no bytes, and one byte.
    let null = ChunkDir {
        layout: read::ALL_NULL,
        compression: NO_COMPRESSION,
        stored_len: 0,
        stored_crc: 0,
        body_len: 0,
        a: None,
        b: None,
    };
    let cells = chunk_both(&[], 0, &null, MAX_ROWS as usize).expect("chunk");
    assert_eq!(cells.len(), MAX_ROWS as usize);
    for rows in [MAX_ROWS as usize + 1, 1_000_000_000] {
        let bad = chunk_both(&[], 0, &null, rows).expect_err("refused");
        assert!(bad.what.contains("past the 8192"), "{}", bad.what);
    }
    let flag = Form::Bool {
        kind: JsonKind::Lit,
        case: 0,
    };
    let err = read_piece(&[1], 300_000_000, flag, CONSTANT).expect_err("refused");
    assert!(err.contains("past the 8192"), "{err}");
}

/// The cap is the row count every writer closes a row group at, so a reader refuses no
/// row group a writer makes; and it keeps one column's cells under a mebibyte.
#[test]
fn the_row_cap_is_the_threshold_the_writers_close_a_row_group_at() {
    assert_eq!(
        crate::store::RowGroupLimits::default().rows,
        MAX_ROWS as usize
    );
    assert!(MAX_ROWS as usize * std::mem::size_of::<read::Cell>() <= 1 << 20);
}

/// A word list's body for `rows` rows, read as one String piece: `zstd` compresses it
/// first, or `stored` replaces what is stored.
fn read_words(body: &[u8], rows: usize, zstd: bool) -> Result<Vec<read::Cell>, String> {
    let stored = if zstd {
        zstd::bulk::compress(body, 3).expect("compress")
    } else {
        body.to_vec()
    };
    read_stored(&stored, body.len() as u32, rows, zstd)
}

fn read_stored(
    stored: &[u8],
    body_len: u32,
    rows: usize,
    zstd: bool,
) -> Result<Vec<read::Cell>, String> {
    let dir = ChunkDir {
        layout: 0,
        compression: if zstd { ZSTD } else { NO_COMPRESSION },
        stored_len: stored.len() as u32,
        stored_crc: 0,
        body_len,
        a: Some(PieceDir {
            form: Form::String {
                kind: JsonKind::Str,
            },
            encoding: DICTIONARY,
        }),
        b: None,
    };
    chunk_both(stored, 0, &dir, rows).map_err(|bad| bad.what)
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

#[test]
fn a_word_list_that_breaks_its_layout_is_refused() {
    let ok = read_words(&words(&["a", "b"], &[0, 1, 1]), 3, false).expect("reads");
    assert_eq!(ok.len(), 3);
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
        (words(&["a", "\u{0}"], &[0, 0, 0]), 3, "whose rows name 1"),
        (words(&["a", "b"], &[0, 1]), 3, "dictionary indexes"),
    ] {
        let err = read_words(&body, rows, false).expect_err(why);
        assert!(err.contains(why), "{why}: {err}");
        let err = read_words(&body, rows, true).expect_err(why);
        assert!(
            err.contains(why) && err.contains("of the chunk's body"),
            "{why}: {err}"
        );
    }
}

/// zstd's frame must be one frame, the whole of what is stored, holding exactly
/// `body_len` bytes.
#[test]
fn a_zstd_chunk_that_is_not_exactly_its_body_is_refused() {
    let body = words(&["a", "b"], &[0, 1, 1]);
    let frame = zstd::bulk::compress(&body, 3).expect("compress");
    let len = body.len() as u32;
    assert!(read_stored(&frame, len, 3, true).is_ok());
    let mut longer = frame.clone();
    longer.push(0);
    for (stored, body_len, why) in [
        (longer, len, "after the chunk's zstd frame"),
        (
            [frame.clone(), frame.clone()].concat(),
            len,
            "after the chunk's zstd frame",
        ),
        (frame.clone(), len + 1, "where body_len says"),
        (
            frame.clone(),
            len - 1,
            "does not decompress to its body_len",
        ),
        (body.clone(), len, "not a zstd frame"),
        (frame[..frame.len() - 1].to_vec(), len, "zstd"),
    ] {
        let err = read_stored(&stored, body_len, 3, true).expect_err(why);
        assert!(err.contains(why), "{why}: {err}");
    }
}

// ── What a reader refuses, by encoding ─────────────────────────────────────────────

/// The directory entry and body of column `v` of a one-piece row group of these values,
/// with `edit` applied to the body, read back.
fn read_edited(values: &[&str], edit: impl Fn(&mut Vec<u8>)) -> Result<Vec<read::Cell>, String> {
    let (columns, rg) = group_of(&lines_of(values));
    let at = columns.iter().position(|c| c == "v").expect("a column v");
    let header = header_of(&rg, columns.len());
    let offset = header.chunk_offsets()[at] as usize;
    let dir = header.chunks[at].clone();
    let mut body = rg.chunks[offset..offset + dir.stored_len as usize].to_vec();
    edit(&mut body);
    chunk_both(&body, 0, &dir, header.row_count as usize).map_err(|bad| bad.what)
}

#[test]
fn a_constant_with_a_byte_left_over_is_refused() {
    let err = read_edited(&["5", "5", "5"], |b| b.push(0)).expect_err("refused");
    assert!(err.contains("left over"), "{err}");
}

/// The bytes of a chunk's directory entry: its fixed fields, then each piece's type,
/// encoding and form.
fn entry_len(dir: &read::ChunkDir) -> usize {
    10 + if dir.compression == ZSTD { 4 } else { 0 }
        + [dir.a, dir.b]
            .iter()
            .flatten()
            .map(|p| 2 + p.form.bytes().len())
            .sum::<usize>()
}

/// An encoding number nobody assigned, or one outside its type's row of the table, is
/// refused where the directory names it.
#[test]
fn an_unknown_encoding_is_refused() {
    let (columns, rg) = group_of(&lines_of(&["5", "6"]));
    let header = header_of(&rg, columns.len());
    let v = columns.iter().position(|c| c == "v").expect("v");
    // The fields before the directory, the entries before v's, then v's own fixed fields
    // and its first piece's type byte.
    let fixed = 1 + 8 + 4 + 4 + 8 + 8 + 1 + 8 + 8 + 8 + 4;
    let before: usize = header.chunks[..v].iter().map(entry_len).sum();
    let at = 8 + fixed + before + entry_len(&header.chunks[v])
        - header.chunks[v].a.map_or(0, |p| p.form.bytes().len())
        - 1;
    let mut frame = rg.frame.clone();
    let len = frame.len() - 8;
    assert_eq!(frame[at], header.chunks[v].a.expect("a piece").encoding);
    // 6 and 0xff are numbers nobody assigned; 2, 4 and 5 are another type's encodings.
    for bad in [6u8, 0xff, 2, 4, 5] {
        frame[at] = bad;
        let crc = crate::frame::crc32c(&frame[8..8 + len]);
        frame[4..8].copy_from_slice(&crc.to_le_bytes());
        let Next::Frame { payload, .. } = crate::frame::read(&frame, 0, u32::MAX) else {
            panic!("not a frame");
        };
        let err = read::header(payload, 0, |_| Some(columns.len())).expect_err("refused");
        assert!(
            err.what.contains(&format!("encoding {bad}")),
            "{}",
            err.what
        );
    }
}
