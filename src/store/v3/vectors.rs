//! The format held to its hand-built test vectors and to its own recovery rule.
//!
//! `testdata/v3/` holds three files built by hand to the bytes, each with an annotated dump
//! beside it, and the nine lines they hold. A reader written from the byte specification
//! must read them back value for value; the builder must write the same rows to the same
//! bytes; and recovery must append the exact footer the cut file's dump ends with.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::build::{build, schema_of};
use super::file::{open_sealed, recover, walk, Index, Reader, Writer};
use super::{FormatError, Source};
use crate::db::row_of;
use crate::flatten::parse_line;
use crate::store::{effective_ts, Row};

fn vectors() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata")
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

/// The builder writes these rows to exactly the vector's bytes: the choice of groups,
/// pieces and forms is a rule, so the same rows always make the same bytes. Row groups 0
/// and 2; row group 1 carries an optional section no writer here produces.
#[test]
fn the_builder_writes_the_vectors_row_groups_byte_for_byte() {
    let bytes = std::fs::read(vectors().join("two_templates.col")).expect("read");
    let text = std::fs::read_to_string(vectors().join("two_templates.ndjson")).expect("read");
    let lines: Vec<&str> = text.lines().collect();
    let stream = u64::from_le_bytes(*b"WAL12345");
    for (picked, first, last, at, len) in [
        ([0usize, 2, 4], 10u64, 14u64, 0x53usize, 134 + 98),
        ([6, 7, 8], 20, 27, 0x317, 140 + 96),
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
        )
        .expect("build");
        let mut built = rg.frame.clone();
        built.extend_from_slice(&rg.chunks);
        assert_eq!(built, bytes[at..at + len], "row group at 0x{at:x}");
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

/// The truncation test: a copy of a file cut at every byte offset from 0 to its length is
/// either the whole sealed file, or unsealed and recovered to exactly the row groups
/// wholly before the cut - every one read back value for value, none partly, none twice -
/// and sealed so that it opens through its footer.
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

#[test]
fn a_built_file_cut_at_every_byte_offset_recovers_the_whole_row_groups_before_the_cut() {
    let scratch = Scratch::new("every-built");
    let (path, want) = built_file(&scratch.0);
    every_cut(&path, &want, &scratch);
}

// ── What goes in comes back out ────────────────────────────────────────────────────

/// Every spelling a value must come back in - zero-padded and signed integers, each RFC 3339
/// variation, epochs as numbers and as strings, a boolean's case, UUIDs and IPv6 in each
/// spelling, integers past `i64`, floats that do not print back, an array, null and the
/// empty string: through the real parser, the builder and the reader, each value comes back
/// as the JSON value that was ingested - a string character for character, a number as the
/// same number.
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
    let rg = build(999, schema_id, &columns, &refs, &times, Source::NotRecorded).expect("build");
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
