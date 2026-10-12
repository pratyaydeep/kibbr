//! The format held to its test vectors and to its own recovery rule:
//! a reader must read them back value for value, the builder must make the same rows into
//! the same pieces, and recovery must append the exact footer the cut file's dump ends with.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::build::{build, schema_of, Level};
use super::file::{open_sealed, recover, walk, Index, Reader, Writer};
use super::{read, FormatError, Source};
use crate::db::row_of;
use crate::frame::Next;
use crate::store::{effective_ts, Row};
use kibbr_core::flatten::parse_line;

/// At the workspace's root, two directories up: the binary crate's fuzz harness reads
/// the same files.
fn vectors() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata")
        .join("v3")
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "kibbr-v3-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A line through the real parser into the row the store is handed.
fn row(line: &str) -> Row {
    row_of(&parse_line(line.as_bytes(), 1).expect("a line that parses"))
}

/// Every row of a file, in file order, as the JSON object an answer prints.
fn rows_of(path: &Path, index: &Index) -> Vec<Value> {
    let mut reader = Reader::open(path).expect("open for reading");
    let mut out = Vec::new();
    for entry in &index.entries {
        let columns = &index.schemas[entry.schema_id as usize];
        let header = reader.header(entry, columns.len()).expect("header");
        let all: Vec<usize> = (0..columns.len()).collect();
        let (cells, _) = reader.chunks(entry, &header, &all).expect("chunks");
        for r in 0..entry.row_count as usize {
            let mut row = Row::new();
            for (at, column) in cells.iter().enumerate() {
                let (value, written) = column[r].clone();
                row.insert_written(columns[at].clone(), value, written);
            }
            let object: serde_json::Map<String, Value> = columns
                .iter()
                .map(|c| (c.clone(), row.json(c).unwrap_or(Value::Null)))
                .collect();
            out.push(Value::Object(object));
        }
    }
    out
}

/// The nine lines of `two_templates.ndjson`, in the order the file stores them: row group
/// 0 is lines 1, 3 and 5; row group 1 lines 2, 4 and 6; row group 2 lines 7 to 9.
fn vector_lines_in_file_order() -> Vec<Value> {
    let text = std::fs::read_to_string(vectors().join("two_templates.ndjson")).expect("read");
    let lines: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    [0, 2, 4, 1, 3, 5, 6, 7, 8]
        .iter()
        .map(|n| lines[*n].clone())
        .collect()
}

#[test]
fn the_empty_vector_is_sealed_and_holds_nothing() {
    let index = open_sealed(&vectors().join("empty.col"))
        .expect("readable")
        .expect("sealed");
    assert!(index.entries.is_empty() && index.schemas.is_empty());
    assert_eq!((index.min_time(), index.max_time()), (i64::MAX, i64::MIN));
    assert_eq!(&index.file_id, b"v3 empty sealed.");
}

#[test]
fn the_two_template_vector_reads_back_value_for_value() {
    let path = vectors().join("two_templates.col");
    let index = open_sealed(&path).expect("readable").expect("sealed");
    assert!(index.finished);
    assert_eq!(index.replaced.len(), 2);
    assert_eq!(index.replaced[0].0, "2024-01-15/seg_0_000001.col");
    let order: Vec<(u64, u64)> = index
        .entries
        .iter()
        .map(|e| (e.template_id, e.offset))
        .collect();
    assert_eq!(order, vec![(101, 83), (102, 395), (101, 791)]);
    assert_eq!(rows_of(&path, &index), vector_lines_in_file_order());

    // The same file walked as if it were unsealed finds the same three row groups, having
    // stepped over the optional section and frame, and stops at the footer.
    let bytes = std::fs::read(&path).expect("read");
    let walked = walk(&path, &bytes).expect("walk");
    assert_eq!(walked.end, 0x403);
    assert_eq!(walked.index.entries.len(), 3);
    assert_eq!(walked.index.schemas, index.schemas);
}

/// The choice of groups, pieces and forms is a rule, so the builder makes the
/// vector's pieces; its bytes differ, the vector being plain, and are held by `encodings.col`.
#[test]
fn the_builder_makes_the_vectors_row_groups_into_the_same_pieces() {
    let bytes = std::fs::read(vectors().join("two_templates.col")).expect("read");
    let text = std::fs::read_to_string(vectors().join("two_templates.ndjson")).expect("read");
    let lines: Vec<&str> = text.lines().collect();
    let stream = u64::from_le_bytes(*b"WAL12345");
    // A row group's header frame, read: the schema has as many columns as its directory.
    let header = |frame: &[u8], columns: usize| {
        let Next::Frame { payload, .. } = crate::frame::read(frame, 0, u32::MAX) else {
            panic!("not a frame");
        };
        read::header(payload, 0, |_| Some(columns)).expect("header")
    };
    let cells = |h: &read::Header, area: &[u8]| -> Vec<Vec<read::Cell>> {
        let offsets = h.chunk_offsets();
        h.chunks
            .iter()
            .zip(offsets)
            .map(|(dir, at)| {
                let body = &area[at as usize..at as usize + dir.stored_len as usize];
                read::chunk(body, 0, dir, h.row_count as usize).expect("chunk")
            })
            .collect()
    };
    for (picked, first, last, at, frame_len) in [
        ([0usize, 2, 4], 10u64, 14u64, 0x53usize, 134),
        ([6, 7, 8], 20, 27, 0x317, 140),
    ] {
        let rows: Vec<Row> = picked.iter().map(|n| row(lines[*n])).collect();
        let refs: Vec<&Row> = rows.iter().collect();
        let times: Vec<i64> = rows
            .iter()
            .map(|r| effective_ts(r, "ts").expect("a time"))
            .collect();
        let columns = schema_of(refs.iter().copied());
        let rg = build(
            101,
            0,
            &columns,
            &refs,
            &times,
            Source::Wal {
                stream,
                first,
                last,
            },
            Level::Live,
        )
        .expect("build");
        let want = header(&bytes[at..at + frame_len], columns.len());
        let got = header(&rg.frame, columns.len());
        assert_eq!(
            (got.template_id, got.schema_id, got.row_count),
            (want.template_id, want.schema_id, want.row_count),
            "row group at 0x{at:x}"
        );
        assert_eq!(
            (got.min_time, got.max_time, got.source),
            (want.min_time, want.max_time, want.source),
            "row group at 0x{at:x}"
        );
        assert_eq!(got.chunks.len(), want.chunks.len());
        for (n, (g, w)) in got.chunks.iter().zip(&want.chunks).enumerate() {
            let form = |p: Option<read::PieceDir>| p.map(|p| p.form);
            assert_eq!(
                (g.layout, form(g.a), form(g.b)),
                (w.layout, form(w.a), form(w.b)),
                "row group at 0x{at:x}, column {n} `{}`",
                columns[n]
            );
        }
        let area = &bytes[at + frame_len..at + frame_len + want.chunks_len as usize];
        assert_eq!(
            cells(&got, &rg.chunks),
            cells(&want, area),
            "row group at 0x{at:x}"
        );
    }
}

// ── What the encodings wrote ──────────────────────────────────────────────────────

/// `encodings.col` holds every encoding and both compressions in one row group;
/// every later build is held to reading it value for value.
#[test]
fn the_encodings_vector_reads_back_value_for_value() {
    let path = vectors().join("encodings.col");
    let index = open_sealed(&path).expect("readable").expect("sealed");
    assert!(index.finished);
    let text = std::fs::read_to_string(vectors().join("encodings.ndjson")).expect("read");
    let want: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    assert_eq!(rows_of(&path, &index), want);

    let entry = &index.entries[0];
    let columns = index.schemas[entry.schema_id as usize].len();
    let header = Reader::open(&path)
        .expect("reader")
        .header(entry, columns)
        .expect("header");
    let mut encodings: Vec<u8> = header
        .chunks
        .iter()
        .flat_map(|c| [c.a, c.b])
        .flatten()
        .map(|p| p.encoding)
        .collect();
    encodings.sort_unstable();
    encodings.dedup();
    let mut compressions: Vec<u8> = header.chunks.iter().map(|c| c.compression).collect();
    compressions.sort_unstable();
    compressions.dedup();
    assert_eq!(
        (encodings, compressions),
        (vec![0, 1, 2, 3, 4, 5], vec![0, 1])
    );
}

/// The encodings are rules too, so the same rows always make the same bytes; bodies are
/// compared before zstd, whose bytes belong to the library's version, not the format.
#[test]
fn the_builder_makes_the_encodings_vector_body_for_body() {
    let path = vectors().join("encodings.col");
    let bytes = std::fs::read(&path).expect("read");
    let index = open_sealed(&path).expect("readable").expect("sealed");
    let entry = index.entries[0];
    let columns = index.schemas[entry.schema_id as usize].clone();
    let want = Reader::open(&path)
        .expect("reader")
        .header(&entry, columns.len())
        .expect("header");
    let text = std::fs::read_to_string(vectors().join("encodings.ndjson")).expect("read");
    let rows: Vec<Row> = text.lines().map(row).collect();
    let refs: Vec<&Row> = rows.iter().collect();
    let times: Vec<i64> = rows
        .iter()
        .map(|r| effective_ts(r, "ts").expect("a time"))
        .collect();
    assert_eq!(schema_of(refs.iter().copied()), columns);
    let rg = build(
        entry.template_id,
        entry.schema_id,
        &columns,
        &refs,
        &times,
        want.source,
        Level::Finished,
    )
    .expect("build");
    let Next::Frame { payload, .. } = crate::frame::read(&rg.frame, 0, u32::MAX) else {
        panic!("not a frame");
    };
    let got = read::header(payload, 0, |_| Some(columns.len())).expect("header");
    assert_eq!(
        (got.row_count, got.min_time, got.max_time, got.source),
        (want.row_count, want.min_time, want.max_time, want.source)
    );
    let body = |dir: &read::ChunkDir, stored: &[u8]| match dir.compression {
        super::ZSTD => zstd::bulk::decompress(stored, dir.body_len as usize).expect("zstd"),
        _ => stored.to_vec(),
    };
    let area = (entry.offset + u64::from(entry.frame_len)) as usize;
    for (n, ((g, w), (g_at, w_at))) in got
        .chunks
        .iter()
        .zip(&want.chunks)
        .zip(got.chunk_offsets().into_iter().zip(want.chunk_offsets()))
        .enumerate()
    {
        let column = &columns[n];
        assert_eq!(
            (g.layout, g.compression, g.body_len, g.a, g.b),
            (w.layout, w.compression, w.body_len, w.a, w.b),
            "column `{column}`"
        );
        let g_stored = &rg.chunks[g_at as usize..(g_at + u64::from(g.stored_len)) as usize];
        let w_from = area + w_at as usize;
        let w_stored = &bytes[w_from..w_from + w.stored_len as usize];
        assert_eq!(body(g, g_stored), body(w, w_stored), "column `{column}`");
    }
}

/// Hex bytes out of an annotated dump, from the first line after `marker` to the end.
fn dump_bytes_after(dump: &str, marker: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut on = false;
    for line in dump.lines() {
        if line.contains(marker) {
            on = true;
            continue;
        }
        if !on || line.starts_with('#') || line.len() < 10 {
            continue;
        }
        let field = &line[10..line.len().min(59)];
        out.extend(
            field
                .split_whitespace()
                .map(|h| u8::from_str_radix(h, 16).expect("hex")),
        );
    }
    out
}

#[test]
fn recovery_cuts_the_torn_vector_and_appends_exactly_the_dumps_footer() {
    let scratch = Scratch::new("cut");
    let path = scratch.0.join("cut.col");
    std::fs::copy(vectors().join("two_templates_cut.col"), &path).expect("copy");
    assert_eq!(
        open_sealed(&path).expect("readable"),
        None,
        "a cut file is unsealed"
    );
    let recovered = recover(&path).expect("recover");
    assert_eq!(recovered.torn_at, Some((791, 979)));
    let bytes = std::fs::read(&path).expect("read");
    assert_eq!(bytes.len(), 1102);
    let dump = std::fs::read_to_string(vectors().join("two_templates_cut.col.txt")).expect("dump");
    let footer = dump_bytes_after(&dump, "What recovery appends");
    assert_eq!(bytes[791..], footer[..], "the appended footer and trailer");
    let index = open_sealed(&path).expect("readable").expect("sealed");
    assert_eq!(index.row_count(), 6);
    assert_eq!(rows_of(&path, &index), vector_lines_in_file_order()[..6]);
}

// ── Every byte offset ──────────────────────────────────────────────────────────────

/// A file with more in it than the vector: two templates over several row groups, nulls,
/// two pieces, text, a second schema, and a row group every chunk of which is null.
fn built_file(dir: &Path) -> (PathBuf, Vec<Value>) {
    let lines = [
        r#"{"ts":"2024-01-15T10:30:00Z","n":"007","ip":"10.0.0.1","f":1.5}"#,
        r#"{"ts":"2024-01-15T10:30:01.5Z","n":"12","ip":"2001:db8::1","f":"0.010"}"#,
        r#"{"ts":"2024-01-15T10:30:02+05:30","n":null,"ip":"::ffff:192.0.2.1","f":null}"#,
        r#"{"at":1705314600,"id":"550E8400-E29B-41D4-A716-446655440000","ok":"True"}"#,
        r#"{"at":1705314601,"id":"550e8400-e29b-41d4-a716-446655440000","ok":false}"#,
        r#"{"ts":"2024-01-15T10:31:00Z","n":"-0","ip":"not an ip","f":"1e3"}"#,
        r#"{"ts":"2024-01-15T10:31:01Z","n":null,"ip":null,"f":null}"#,
    ];
    let groups: [(u64, &str, &[usize]); 4] = [
        (1, "ts", &[0, 1, 2]),
        (2, "at", &[3, 4]),
        (1, "ts", &[5]),
        (1, "ts", &[6]),
    ];
    let path = dir.join("built.col");
    let mut writer = Writer::create(&path).expect("create");
    let mut want = Vec::new();
    for (template, ts, picked) in groups {
        let rows: Vec<Row> = picked.iter().map(|n| row(lines[*n])).collect();
        let refs: Vec<&Row> = rows.iter().collect();
        let times: Vec<i64> = rows
            .iter()
            .map(|r| effective_ts(r, ts).expect("time"))
            .collect();
        let columns = schema_of(refs.iter().copied());
        let schema_id = writer.schema_id(&columns).expect("schema");
        let rg = build(
            template,
            schema_id,
            &columns,
            &refs,
            &times,
            Source::NotRecorded,
            Level::Live,
        )
        .expect("build");
        writer.append(template, &rg).expect("append");
        for r in &rows {
            let object: serde_json::Map<String, Value> = columns
                .iter()
                .map(|c| (c.clone(), r.json(c).unwrap_or(Value::Null)))
                .collect();
            want.push(Value::Object(object));
        }
    }
    writer.seal().expect("seal");
    (path, want)
}

/// A file cut at every byte offset is either the whole sealed file, or recovered to exactly
/// the row groups wholly before the cut - none partly, none twice - and sealed.
fn every_cut(original: &Path, want: &[Value], scratch: &Scratch) {
    let whole = std::fs::read(original).expect("read");
    let full = open_sealed(original).expect("readable").expect("sealed");
    // Where each row group's chunk area ends, and how many rows are before it.
    let mut ends = Vec::new();
    let mut rows = 0usize;
    let mut reader = Reader::open(original).expect("reader");
    for e in &full.entries {
        let columns = full.schemas[e.schema_id as usize].len();
        let header = reader.header(e, columns).expect("header");
        rows += e.row_count as usize;
        ends.push((
            e.offset + u64::from(e.frame_len) + u64::from(header.chunks_len),
            rows,
        ));
    }
    let cut_path = scratch.0.join("cut.col");
    for cut in 0..=whole.len() {
        std::fs::write(&cut_path, &whole[..cut]).expect("write");
        let index = match open_sealed(&cut_path).expect("a cut never fails to open") {
            Some(index) => {
                assert_eq!(cut, whole.len(), "a prefix of {cut} bytes read as sealed");
                index
            }
            None => {
                let recovered = recover(&cut_path)
                    .unwrap_or_else(|e| panic!("cut at {cut}: recovery failed: {e}"));
                let again = open_sealed(&cut_path)
                    .expect("readable")
                    .unwrap_or_else(|| panic!("cut at {cut}: not sealed after recovery"));
                // The footer does not carry a chunk area's length, so that field of an
                // entry opened through it is an estimate; everything else must agree.
                let plain = |i: &Index| {
                    i.entries
                        .iter()
                        .map(|e| super::file::Entry {
                            chunks_len: 0,
                            ..*e
                        })
                        .collect::<Vec<_>>()
                };
                assert_eq!(plain(&again), plain(&recovered.index), "cut at {cut}");
                again
            }
        };
        let kept = ends
            .iter()
            .filter(|(end, _)| *end <= cut as u64)
            .map(|(_, rows)| *rows)
            .next_back()
            .unwrap_or(0);
        let got = rows_of(&cut_path, &index);
        assert_eq!(got.len(), kept, "cut at {cut}: rows kept");
        assert_eq!(got, want[..kept], "cut at {cut}: values");
    }
}

#[test]
fn the_vector_cut_at_every_byte_offset_recovers_the_whole_row_groups_before_the_cut() {
    let scratch = Scratch::new("every-vector");
    every_cut(
        &vectors().join("two_templates.col"),
        &vector_lines_in_file_order(),
        &scratch,
    );
}

/// The encodings vector, cut anywhere inside its compressed chunks, is a tear like any other.
#[test]
fn the_encodings_vector_cut_at_every_byte_offset_recovers_the_whole_row_groups_before_the_cut() {
    let scratch = Scratch::new("every-encodings");
    let text = std::fs::read_to_string(vectors().join("encodings.ndjson")).expect("read");
    let want: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).expect("json"))
        .collect();
    every_cut(&vectors().join("encodings.col"), &want, &scratch);
}

#[test]
fn a_built_file_cut_at_every_byte_offset_recovers_the_whole_row_groups_before_the_cut() {
    let scratch = Scratch::new("every-built");
    let (path, want) = built_file(&scratch.0);
    every_cut(&path, &want, &scratch);
}

// ── What goes in comes back out ────────────────────────────────────────────────────

/// Every spelling a value must come back in, through the real parser, the builder and the
/// reader: a string character for character, a number as the same number.
#[test]
fn every_named_spelling_comes_back_as_it_was_ingested() {
    let values = [
        r#""0045000012""#,
        r#""+4500000120""#,
        r#""-0""#,
        r#""200""#,
        "200",
        r#""2024-01-15T16:00:00+05:30""#,
        r#""2024-01-15T10:30:00-00:00""#,
        r#""2024-01-15T10:30:00.120Z""#,
        r#""2024-01-15t10:30:00z""#,
        r#""2024-01-15 10:30:00Z""#,
        "1705312200",
        r#""1705312200""#,
        "1705312200123",
        r#""True""#,
        r#""TRUE""#,
        "true",
        r#""550E8400-E29B-41D4-A716-446655440000""#,
        r#""550e8400-E29B-41d4-a716-446655440000""#,
        r#""2001:db8::1""#,
        r#""2001:0db8:0:0:0:0:0:1""#,
        r#""2001:0db8:0000:0000:0000:0000:0000:0001""#,
        r#""::ffff:192.0.2.1""#,
        r#""::192.0.2.1""#,
        "9223372036854775809",
        r#""9223372036854775809""#,
        r#""9223372036854775808""#,
        "1.50",
        "1e3",
        r#""0.010""#,
        r#""1e3""#,
        r#""1.""#,
        r#"".5""#,
        r#""-0.0""#,
        r#""1e-400""#,
        r#""0946684800""#,
        r#"["auth","login"]"#,
        "null",
        r#""""#,
    ];
    let scratch = Scratch::new("spellings");
    let path = scratch.0.join("one.col");
    let mut writer = Writer::create(&path).expect("create");
    // Each value alone in a column, and all of them together in one column as well, so
    // the grouping and the two-piece rule see every mix.
    let mut lines = Vec::new();
    for (n, v) in values.iter().enumerate() {
        lines.push(format!(
            r#"{{"ts":"2024-01-15T10:30:00Z","k{n:02}":{v},"all":{v}}}"#
        ));
    }
    let mut want = Vec::new();
    let mut template = 0;
    for line in &lines {
        template += 1;
        let r = row(line);
        let columns = schema_of([&r]);
        let schema_id = writer.schema_id(&columns).expect("schema");
        let times = [effective_ts(&r, "ts").expect("time")];
        let rg = build(
            template,
            schema_id,
            &columns,
            &[&r],
            &times,
            Source::NotRecorded,
            Level::Live,
        )
        .expect("build");
        writer.append(template, &rg).expect("append");
        want.push(serde_json::from_str::<Value>(line).expect("json"));
    }
    // And one row group holding every value of `all`.
    let rows: Vec<Row> = lines
        .iter()
        .map(|l| {
            let mut v: serde_json::Map<String, Value> = serde_json::from_str(l).expect("json");
            v.retain(|k, _| k == "ts" || k == "all");
            row(&Value::Object(v).to_string())
        })
        .collect();
    let refs: Vec<&Row> = rows.iter().collect();
    let columns = schema_of(refs.iter().copied());
    let schema_id = writer.schema_id(&columns).expect("schema");
    let times: Vec<i64> = rows
        .iter()
        .map(|r| effective_ts(r, "ts").expect("t"))
        .collect();
    let rg = build(
        999,
        schema_id,
        &columns,
        &refs,
        &times,
        Source::NotRecorded,
        Level::Live,
    )
    .expect("build");
    writer.append(999, &rg).expect("append");
    for l in &lines {
        let mut v: serde_json::Map<String, Value> = serde_json::from_str(l).expect("json");
        v.retain(|k, _| k == "ts" || k == "all");
        want.push(Value::Object(v));
    }
    let index = writer.seal().expect("seal");

    // Arrays are text: stored as their JSON and returned as that string.
    let as_stored = |v: &Value| -> Value {
        let Value::Object(map) = v else {
            return v.clone();
        };
        Value::Object(
            map.iter()
                .map(|(k, v)| {
                    let v = match v {
                        Value::Array(_) => Value::String(v.to_string()),
                        other => other.clone(),
                    };
                    (k.clone(), v)
                })
                .collect(),
        )
    };
    let got = rows_of(&path, &index);
    assert_eq!(got.len(), want.len());
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(*g, as_stored(w));
    }
}

/// Bytes whose CRC holds and which break a rule are the writer's, not a tear: the walk
/// names the path and offset rather than cutting there.
#[test]
fn a_frame_whose_crc_holds_and_whose_contents_break_a_rule_is_refused() {
    let scratch = Scratch::new("corrupt");
    let mut bytes = std::fs::read(vectors().join("two_templates.col")).expect("read");
    // Row group 0's row_count, set to 0 and its frame's CRC made to hold again.
    let payload = 0x53 + 8;
    let len = u32::from_le_bytes(bytes[0x53..0x57].try_into().expect("4 bytes")) as usize;
    bytes[payload + 13..payload + 17].copy_from_slice(&0u32.to_le_bytes());
    let crc = crate::frame::crc32c(&bytes[payload..payload + len]);
    bytes[0x57..0x5b].copy_from_slice(&crc.to_le_bytes());
    let path = scratch.0.join("bad.col");
    std::fs::write(&path, &bytes[..0x403]).expect("write");
    match recover(&path) {
        Err(FormatError::Corrupt { offset, what, .. }) => {
            assert_eq!(offset, (payload + 13) as u64, "{what}");
        }
        other => panic!("not refused: {other:?}"),
    }
    // A required frame kind nobody assigned, whole and intact, is refused the same way.
    let mut bytes = bytes[..0x53].to_vec();
    bytes.extend(crate::frame::frame(&[0x07, 1, 2, 3]));
    std::fs::write(&path, &bytes).expect("write");
    assert!(matches!(recover(&path), Err(FormatError::Corrupt { .. })));
}
