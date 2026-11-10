// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::PathBuf;

use super::*;
use crate::parse::flatten::parse_line;
use crate::ingest::row_of;
use crate::format::build::{build as build_group, schema_of, Level};
use crate::format::file::Writer;
use crate::format::{put_varint, zigzag};
use crate::rows::{effective_ts, Row};

fn vector(name: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata")
        .join("format")
        .join(name);
    std::fs::read(path).expect("a test vector")
}

/// A file as the instructions that build it. With `squeeze`, a zstd chunk is given as its
/// body and the file is left unsealed, since its footer's offsets no longer hold.
fn instructions(file: &[u8], squeeze: bool) -> Vec<u8> {
    let run = |out: &mut Vec<u8>, bytes: &[u8]| {
        out.extend_from_slice(
            &u16::try_from(bytes.len())
                .expect("a short run")
                .to_le_bytes(),
        );
        out.extend_from_slice(bytes);
    };
    let mut out = vec![BUILT];
    let mut columns: Vec<usize> = Vec::new();
    let mut at = HEADER_LEN;
    let mut last = 0usize;
    while let Some((payload, end)) = frame_at(file, at) {
        let header = (payload[0] == KIND_ROW_GROUP)
            .then(|| read::header(payload, 0, |id| columns.get(id as usize).copied()).ok())
            .flatten()
            .filter(|h| end + h.chunks_len as usize <= file.len());
        last = end - at;
        at = end;
        let Some(header) = header else {
            if payload[0] == KIND_FOOTER && squeeze {
                return out;
            }
            if payload[0] == KIND_SCHEMA {
                columns.push(u32_at(payload, 5) as usize);
            }
            out.push(0);
            run(&mut out, payload);
            continue;
        };
        out.extend_from_slice(&[1, 0]);
        out.extend_from_slice(&payload[1..1 + FIXED]);
        out.push(u8::try_from(header.chunks.len()).expect("few columns"));
        let spans = spans(&header);
        for (dir, (from, to, _)) in header.chunks.iter().zip(&spans) {
            let stored = &file[at + from..at + to];
            let body = (squeeze && dir.compression == ZSTD)
                .then(|| zstd::bulk::decompress(stored, dir.body_len as usize).expect("zstd"));
            out.extend_from_slice(&[
                dir.layout,
                dir.compression,
                if body.is_some() { 0x80 } else { 0 },
            ]);
            let pieces: Vec<u8> = [dir.a, dir.b]
                .iter()
                .flatten()
                .flat_map(piece_bytes)
                .collect();
            out.push(u8::try_from(pieces.len()).expect("short pieces"));
            out.extend(pieces);
            run(&mut out, body.as_deref().unwrap_or(stored));
        }
        out.push(u8::try_from(header.sections.len()).expect("few sections"));
        for (s, (from, to, _)) in header.sections.iter().zip(&spans[header.chunks.len()..]) {
            out.push(s.kind);
            out.push(u8::try_from(s.column).expect("a low column"));
            out.push(u8::try_from(s.len).expect("a short section"));
            out.extend_from_slice(&file[at + from..at + to]);
        }
        out.push(0);
        at += header.chunks_len as usize;
    }
    let rest = &file[at..];
    let mut trailer = (last as u32).to_le_bytes().to_vec();
    trailer.extend_from_slice(&MAGIC);
    if rest == trailer {
        out.push(2);
    } else if !rest.is_empty() {
        out.push(3);
        run(&mut out, rest);
    }
    out
}

/// How many of the row groups' chunks the walk keeps are zstd frames.
fn zstd_chunks(file: &[u8]) -> usize {
    let mut columns: Vec<usize> = Vec::new();
    let mut found = 0;
    let mut at = HEADER_LEN;
    while let Some((payload, end)) = frame_at(file, at) {
        at = end;
        if payload[0] == KIND_SCHEMA {
            columns.push(u32_at(payload, 5) as usize);
        }
        if payload[0] == KIND_ROW_GROUP {
            let header = read::header(payload, 0, |id| columns.get(id as usize).copied())
                .expect("a header");
            found += header
                .chunks
                .iter()
                .filter(|c| c.compression == ZSTD)
                .count();
            at += header.chunks_len as usize;
        }
    }
    found
}

/// A file the store's own writer makes of these lines, one row group per group of
/// them: `(template, time column, lines)`.
fn written(name: &str, groups: &[(u64, &str, &[&str])], level: Level) -> Vec<u8> {
    let path = std::env::temp_dir().join(format!(
        "kibbr-fuzz-seed-{name}-{}-{:?}.col",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&path);
    let mut writer = Writer::create(&path).expect("create");
    for (template, ts, lines) in groups {
        let rows: Vec<Row> = lines
            .iter()
            .map(|l| row_of(&parse_line(l.as_bytes(), 1).expect("a line that parses")))
            .collect();
        let refs: Vec<&Row> = rows.iter().collect();
        let times: Vec<i64> = rows
            .iter()
            .map(|r| effective_ts(r, ts).expect("a time"))
            .collect();
        let columns = schema_of(refs.iter().copied());
        let schema_id = writer.schema_id(&columns).expect("schema");
        let source = Source::Wal {
            stream: 7,
            first: 1,
            last: lines.len() as u64,
        };
        let rg = build_group(*template, schema_id, &columns, &refs, &times, source, level)
            .expect("build");
        writer.append(*template, &rg).expect("append");
    }
    if level == Level::Finished {
        writer.finish();
    }
    writer.seal().expect("seal");
    let bytes = std::fs::read(&path).expect("read");
    let _ = std::fs::remove_file(&path);
    bytes
}

/// Two templates, nulls, two pieces, text beside typed values, a second schema and a
/// row group every chunk of which but its time is null.
fn mixed() -> Vec<u8> {
    written(
        "mixed",
        &[
            (
                1,
                "ts",
                &[
                    r#"{"ts":"2024-01-15T10:30:00Z","n":"007","ip":"10.0.0.1","f":1.5}"#,
                    r#"{"ts":"2024-01-15T10:30:01.5Z","n":"12","ip":"2001:db8::1","f":"0.010"}"#,
                    r#"{"ts":"2024-01-15T10:30:02+05:30","n":null,"ip":"::ffff:192.0.2.1","f":null}"#,
                ],
            ),
            (
                2,
                "at",
                &[
                    r#"{"at":1705314600,"id":"550E8400-E29B-41D4-A716-446655440000","ok":"True"}"#,
                    r#"{"at":1705314601,"id":"550e8400-e29b-41d4-a716-446655440000","ok":false}"#,
                ],
            ),
            (
                1,
                "ts",
                &[r#"{"ts":"2024-01-15T10:31:00Z","n":"-0","ip":"not an ip","f":"1e3"}"#],
            ),
            (
                1,
                "ts",
                &[r#"{"ts":"2024-01-15T10:31:01Z","n":null,"ip":null,"f":null}"#],
            ),
        ],
        Level::Live,
    )
}

/// Every spelling a form names in one column, then rows enough that a word list, varints
/// and differences give zstd something to do, then three hundred distinct words.
fn spellings() -> Vec<u8> {
    let values = [
        r#""0045000012""#,
        r#""+4500000120""#,
        r#""-0""#,
        "200",
        r#""2024-01-15T16:00:00+05:30""#,
        r#""2024-01-15t10:30:00z""#,
        "1705312200",
        "1705312200123",
        r#""True""#,
        "true",
        r#""550E8400-E29B-41D4-A716-446655440000""#,
        r#""2001:0db8:0000:0000:0000:0000:0000:0001""#,
        r#""::ffff:192.0.2.1""#,
        "9223372036854775809",
        "1.50",
        r#""0.010""#,
        r#""1e3""#,
        "null",
        r#""""#,
    ];
    let one: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(n, v)| format!(r#"{{"ts":"2024-01-15T10:30:{n:02}Z","v":{v}}}"#))
        .collect();
    let many: Vec<String> = (0..40)
        .map(|n| {
            format!(
                r#"{{"ts":"2024-01-15T10:31:{n:02}.{:03}Z","w":"word-{}","n":{},"ok":{},"f":"{}.{:02}"}}"#,
                n * 7,
                n % 5,
                200 + n * n,
                n % 3 == 0,
                n,
                n % 100
            )
        })
        .collect();
    // Three hundred words, so a word list's indexes are two bytes each, beside an
    // epoch written as a string.
    let words: Vec<String> = (0..300)
        .map(|n| {
            format!(
                r#"{{"ts":"2024-01-15T10:{:02}:{:02}Z","w":"w{n}","e":"17053122{:02}"}}"#,
                40 + n / 60,
                n % 60,
                n % 100
            )
        })
        .collect();
    let one: Vec<&str> = one.iter().map(String::as_str).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    written(
        "spellings",
        &[(1, "ts", &one), (2, "ts", &many), (3, "ts", &words)],
        Level::Finished,
    )
}

/// A built row group of one column that names its own schema: `rows` rows, and the
/// column's instruction bytes as given.
fn one_column(rows: u32, column: &[u8]) -> Vec<u8> {
    let mut out = vec![BUILT, 1, 1];
    out.extend_from_slice(&7u64.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
    let t0 = 1_705_314_600_000_000_000i64;
    out.extend_from_slice(&t0.to_le_bytes());
    out.extend_from_slice(&t0.to_le_bytes());
    out.extend_from_slice(&[0; 25]);
    out.push(1);
    out.extend_from_slice(column);
    out.extend_from_slice(&[0, 0]);
    out
}

/// The seeds: every valid file there is to hand, as it is and as instructions; cuts of
/// them; and files that are one rule short of valid.
fn seeds() -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = Vec::new();
    let mut files = vec![
        ("empty", vector("empty.col")),
        ("two-templates", vector("two_templates.col")),
        ("two-templates-cut", vector("two_templates_cut.col")),
        ("encodings", vector("encodings.col")),
        ("written-mixed", mixed()),
        ("written-spellings", spellings()),
    ];
    // The first vector's first row group alone, sealed by a footer of its own.
    let two = vector("two_templates.col");
    let mut index = Index::new(*b"one row group...");
    index.schemas.push(
        ["http.status", "level", "order_id", "ts"]
            .map(String::from)
            .to_vec(),
    );
    let walked = file::walk(Path::new(NAME), &two).expect("walk").index;
    index.entries.push(walked.entries[0]);
    let mut one = file::header_bytes(&index.file_id).to_vec();
    one.extend_from_slice(&two[HEADER_LEN..0x13b]);
    one.extend(index.seal_bytes());
    files.push(("one-row-group", one));

    for (name, file) in &files {
        out.push((format!("file-{name}"), file.clone()));
        out.push((format!("built-{name}"), instructions(file, false)));
        // Only a file with a zstd chunk in it reads differently for being squeezed.
        if zstd_chunks(file) > 0 {
            out.push((format!("built-{name}-squeezed"), instructions(file, true)));
        }
        // Its frames up to the footer, and then the builder's own seal: the form in
        // which a changed row group still has a footer entry that points at it.
        let mut resealed = instructions(file, true);
        resealed.extend_from_slice(&[6, 0]);
        out.push((format!("built-{name}-resealed"), resealed));
        // Cut inside the header, after it, inside the first frame, in the middle, at
        // the footer, inside it, before the trailer and one byte short.
        let end = file::walk(Path::new(NAME), file).map_or(0, |w| w.end as usize);
        let mut cuts = vec![3, 24, 29, 40, file.len() / 2, end, end + 9];
        cuts.extend([file.len().saturating_sub(8), file.len() - 1]);
        cuts.sort_unstable();
        cuts.dedup();
        for cut in cuts {
            if cut < file.len() && (cut != end || *name != "two-templates-cut") {
                out.push((format!("file-{name}-cut-at-{cut}"), file[..cut].to_vec()));
            }
        }
    }

    // One rule short of valid, as files.
    let header = file::header_bytes(&FILE_ID);
    let empty = Index::new(FILE_ID).seal_bytes();
    let with = |edit: &dyn Fn(&mut Vec<u8>)| {
        let mut file = header.to_vec();
        edit(&mut file);
        file
    };
    let near: Vec<(&str, Vec<u8>)> = vec![
        ("header-alone", header.to_vec()),
        (
            "header-pre-rename",
            with(&|f| f[..4].copy_from_slice(&PRE_RENAME_MAGIC)),
        ),
        ("header-version-2", with(&|f| f[4] = 2)),
        ("header-reserved-byte-set", with(&|f| f[6] = 1)),
        ("header-other-magic", with(&|f| f[0] = b'k')),
        (
            "tear-of-zero-bytes",
            with(&|f| f.extend_from_slice(&[0; 16])),
        ),
        (
            "tear-length-past-the-end",
            with(&|f| f.extend_from_slice(&[0xff; 12])),
        ),
        (
            "frame-optional",
            with(&|f| f.extend(frame::frame(&[0x80, 1, 2, 3]))),
        ),
        ("frame-kind-ff", with(&|f| f.extend(frame::frame(&[0xff])))),
        (
            "frame-kind-nobody-assigned",
            with(&|f| f.extend(frame::frame(&[7, 1, 2, 3]))),
        ),
        (
            "schema-of-no-columns",
            with(&|f| f.extend(frame::frame(&[1, 0, 0, 0, 0, 0, 0, 0, 0]))),
        ),
        (
            "schema-id-skipped",
            with(&|f| f.extend(frame::frame(&[1, 1, 0, 0, 0, 0, 0, 0, 0]))),
        ),
        (
            "schema-names-out-of-order",
            with(&|f| {
                let mut p = vec![1, 0, 0, 0, 0, 2, 0, 0, 0];
                p.extend_from_slice(&[1, 0, 0, 0, b'b', 1, 0, 0, 0, b'a']);
                f.extend(frame::frame(&p));
            }),
        ),
        (
            "row-group-of-no-schema",
            with(&|f| {
                let mut p = vec![KIND_ROW_GROUP];
                p.extend_from_slice(&[0; FIXED + 4 + 2]);
                p[13] = 1;
                f.extend(frame::frame(&p));
            }),
        ),
        (
            "footer-twice",
            with(&|f| {
                f.extend_from_slice(&empty);
                f.extend_from_slice(&empty);
            }),
        ),
        (
            "footer-flag-nobody-assigned",
            with(&|f| {
                let mut p = empty[frame::HEADER..empty.len() - TRAILER_LEN].to_vec();
                p[1] = 2;
                f.extend(frame::frame(&p));
                f.extend_from_slice(&empty[empty.len() - TRAILER_LEN..]);
            }),
        ),
        (
            "footer-with-an-optional-part",
            with(&|f| {
                let mut p = empty[frame::HEADER..empty.len() - TRAILER_LEN - 4].to_vec();
                p.extend_from_slice(&[1, 0, 0, 0, 0xff, 2, 0, 0, 0, 9, 9]);
                let framed = frame::frame(&p);
                f.extend_from_slice(&framed);
                f.extend_from_slice(&(framed.len() as u32).to_le_bytes());
                f.extend_from_slice(&MAGIC);
            }),
        ),
        (
            "trailer-over-no-footer",
            with(&|f| {
                f.extend(frame::frame(&[0x80; 40]));
                f.extend_from_slice(&48u32.to_le_bytes());
                f.extend_from_slice(&MAGIC);
            }),
        ),
        (
            "trailer-length-past-the-header",
            with(&|f| {
                f.extend_from_slice(&empty[..empty.len() - TRAILER_LEN]);
                f.extend_from_slice(&0xffff_ff00u32.to_le_bytes());
                f.extend_from_slice(&MAGIC);
            }),
        ),
    ];
    for (name, file) in &near {
        out.push((format!("file-{name}"), file.clone()));
        // The same as instructions, where a changed byte is not behind a CRC. A built
        // file has a header of its own, so the ones that differ only there are one.
        if !name.starts_with("header-") {
            out.push((format!("built-{name}"), instructions(file, false)));
        }
    }

    // And as instructions: one column of each layout an instruction can say in a few
    // bytes, and the knobs that make a worked-out number lie.
    let bits = [0, 0, 0, 4, 4, 5, 1, 0, 1, 0, 0b01];
    // A column whose pieces and body are given apart, stored as it is.
    let column = |pieces: &[u8], body: &[u8]| {
        let mut out = vec![0, 0, 0, pieces.len() as u8];
        out.extend_from_slice(pieces);
        out.extend_from_slice(&(body.len() as u16).to_le_bytes());
        out.extend_from_slice(body);
        out
    };
    let t0 = 1_705_314_600_000_000_000i64;
    let mut delta = Vec::new();
    put_varint(zigzag(t0), &mut delta);
    put_varint(zigzag(1_000_000_000), &mut delta);
    let mut floats = vec![0b01, 0, 2];
    floats.extend_from_slice(&1.5f64.to_bits().to_le_bytes());
    floats.extend_from_slice(&0.25f64.to_bits().to_le_bytes());
    let mut ints = 7i64.to_le_bytes().to_vec();
    ints.extend_from_slice(&12i64.to_le_bytes());
    let words = [
        2, 0, 0, 0, 1, 0, 0, 0, b'a', 2, 0, 0, 0, b'b', b'c', 0, 1, 0,
    ];
    let kinds = [0b01, 2, 0, 0, 0, b'1', b'2', 1, 0, 0, 0, b'x'];
    let built: Vec<(&str, Vec<u8>)> = vec![
        // One piece of each plain layout and of each encoding, small enough that one
        // changed byte is one changed value.
        (
            "bool-plain",
            one_column(3, &column(&[4, 0, 1, 0], &[1, 0, 1])),
        ),
        ("int-plain", one_column(2, &column(&[1, 0, 0, 0, 3], &ints))),
        (
            "int-varint",
            one_column(3, &column(&[1, 3, 1, 0, 0], &[2, 4, 6])),
        ),
        (
            "float-digits-per-row",
            one_column(2, &column(&[2, 0, 2, 255], &floats)),
        ),
        (
            "uuid-plain",
            one_column(1, &column(&[5, 0, 0, 1], &[0xab; 16])),
        ),
        (
            "ip-v4-constant",
            one_column(3, &column(&[6, 1, 0, 4], &[10, 0, 0, 1])),
        ),
        (
            "text-dictionary",
            one_column(3, &column(&[0, 2, 0], &words)),
        ),
        (
            "text-of-both-kinds",
            one_column(2, &column(&[0, 0, 2], &kinds)),
        ),
        (
            "datetime-delta",
            one_column(2, &column(&[3, 4, 0, 0, b'T', b'Z', 0, 0, 0], &delta)),
        ),
        ("all-null", one_column(3, &[4, 0, 0, 0, 0, 0])),
        // The most rows a row group holds, and one more: a count no byte vouches for.
        (
            "all-null-at-the-row-cap",
            one_column(MAX_ROWS, &[4, 0, 0, 0, 0, 0]),
        ),
        (
            "all-null-past-the-row-cap",
            one_column(MAX_ROWS + 1, &[4, 0, 0, 0, 0, 0]),
        ),
        (
            "bool-constant-past-the-row-cap",
            one_column(MAX_ROWS + 1, &[0, 0, 0, 4, 4, 1, 1, 0, 1, 0, 1]),
        ),
        ("bool-bits", one_column(2, &bits)),
        (
            "bool-constant",
            one_column(5, &[0, 0, 0, 4, 4, 1, 1, 0, 1, 0, 1]),
        ),
        (
            "int-varint-squeezed",
            one_column(3, &[0, 1, 0x80, 5, 1, 3, 1, 0, 0, 3, 0, 2, 4, 6]),
        ),
        (
            "text-with-nulls",
            one_column(
                3,
                &[
                    1, 0, 0, 3, 0, 0, 0, 12, 0, 0b101, 1, 0, 0, 0, b'a', 2, 0, 0, 0, b'b', b'c',
                ],
            ),
        ),
        (
            "two-pieces",
            one_column(
                3,
                &[
                    2, 0, 0, 7, 4, 5, 1, 0, 0, 1, 0, 11, 0, 0b100, 1, 0, 0, 0, 0b01, 1, 0, 0,
                    0, b'x',
                ],
            ),
        ),
        (
            "stored-len-lies",
            one_column(2, &[0, 0, 1, 4, 4, 5, 1, 0, 1, 0, 0b01]),
        ),
        (
            "stored-crc-lies",
            one_column(2, &[0, 0, 2, 4, 4, 5, 1, 0, 1, 0, 0b01]),
        ),
        (
            "body-len-lies",
            one_column(
                3,
                &[0, 1, 0x80 | 3 | 5 << 2, 5, 1, 3, 1, 0, 0, 3, 0, 2, 4, 6],
            ),
        ),
        (
            "type-nobody-assigned",
            one_column(2, &[0, 0, 0, 4, 7, 0, 0, 0, 1, 0, 0]),
        ),
        (
            "encoding-of-another-type",
            one_column(2, &[0, 0, 0, 4, 4, 2, 1, 0, 1, 0, 0]),
        ),
        (
            "compression-nobody-assigned",
            one_column(2, &[0, 2, 0, 4, 4, 5, 1, 0, 1, 0, 0b01]),
        ),
        (
            "layout-bit-nobody-assigned",
            one_column(2, &[8, 0, 0, 4, 4, 5, 1, 0, 1, 0, 0b01]),
        ),
    ];
    for (name, mut feed) in built {
        out.push((format!("built-{name}"), feed.clone()));
        // And sealed by the builder, so the row group is read through a footer whether
        // or not a walk would keep it.
        let mut sealed = feed.clone();
        sealed.extend_from_slice(&[6, 0]);
        out.push((format!("built-{name}-sealed"), sealed));
        // The same, with an optional section, a required one, and a chunks_len that lies.
        if name == "bool-bits" {
            let at = feed.len() - 2;
            feed.splice(at.., [1, 0x81, 0, 3, 1, 2, 3, 0]);
            out.push(("built-section-optional".into(), feed.clone()));
            feed[at + 1] = 0x01;
            out.push(("built-section-required".into(), feed.clone()));
            feed[at + 1] = 0x81;
            *feed.last_mut().expect("a knob") = 1;
            out.push(("built-chunks-len-lies".into(), feed));
        }
    }
    out
}

/// Instructions say what a file says: each vector, turned into them and built again,
/// is the vector, byte for byte after the header and its file id.
#[test]
fn a_file_as_instructions_builds_back_byte_for_byte() {
    let files = [
        vector("empty.col"),
        vector("two_templates.col"),
        vector("two_templates_cut.col"),
        vector("encodings.col"),
        mixed(),
        spellings(),
    ];
    for file in files {
        let built = build(&instructions(&file, false)[1..]);
        assert!(built[HEADER_LEN..] == file[HEADER_LEN..]);
        // Its bodies compressed here and not as stored, it holds the same row groups.
        let squeezed = build(&instructions(&file, true)[1..]);
        let rows = |file: &[u8]| {
            let walked = file::walk(Path::new(NAME), file).expect("walk").index;
            (walked.row_count(), walked.entries.len(), walked.schemas)
        };
        assert_eq!(rows(&squeezed), rows(&file));
    }
}

/// The seeds hold every encoding and both compressions, read without complaint, and
/// the ones meant to be whole are: the reader keeps their row groups.
#[test]
fn the_seeds_read_clean_and_the_whole_ones_are_kept() {
    let seeds = seeds();
    for (name, seed) in &seeds {
        assert!(seed.len() < 64 * 1024, "{name} is {} bytes", seed.len());
        run(seed);
    }
    let kept = |name: &str| {
        let (_, seed) = seeds.iter().find(|(n, _)| n == name).expect("a seed");
        let file = if seed[0] == BUILT {
            build(&seed[1..])
        } else {
            seed.clone()
        };
        unsealed(&file).map_or(0, |(index, _)| index.row_count())
    };
    for (name, rows) in [
        ("file-two-templates", 9),
        ("built-two-templates", 9),
        ("built-encodings-squeezed", 12),
        ("built-written-spellings-squeezed", 359),
        ("built-all-null", 3),
        ("built-all-null-at-the-row-cap", 8_192),
        ("built-all-null-past-the-row-cap", 0),
        ("built-bool-constant-past-the-row-cap", 0),
        ("built-all-null-at-the-row-cap-sealed", 8_192),
        ("built-bool-bits", 2),
        ("built-bool-constant", 5),
        ("built-int-varint-squeezed", 3),
        ("built-text-with-nulls", 3),
        ("built-two-pieces", 3),
        ("built-bool-plain", 3),
        ("built-int-plain", 2),
        ("built-int-varint", 3),
        ("built-float-digits-per-row", 2),
        ("built-uuid-plain", 1),
        ("built-ip-v4-constant", 3),
        ("built-text-dictionary", 3),
        ("built-text-of-both-kinds", 2),
        ("built-datetime-delta", 2),
        ("built-bool-bits-sealed", 2),
        ("built-two-templates-resealed", 9),
        ("built-written-spellings-resealed", 359),
        ("built-footer-with-an-optional-part", 0),
        ("built-section-optional", 2),
        ("built-stored-len-lies", 0),
        ("built-type-nobody-assigned", 0),
    ] {
        assert_eq!(kept(name), rows, "{name}");
    }
}

/// Every cut of a vector is a file some crash could leave, and reads clean.
#[test]
fn every_cut_of_a_vector_reads_clean() {
    for name in ["two_templates.col", "encodings.col"] {
        let file = vector(name);
        for cut in 0..file.len() {
            run(&file[..cut]);
        }
    }
}

/// Writes the seed corpus: `cargo test -p kibbr-engine --features fuzzing --lib fuzzing::segment -- --ignored`.
#[test]
#[ignore = "writes fuzz/corpus/segment"]
fn write_the_seed_corpus() {
    let dir: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fuzz")
        .join("corpus")
        .join("segment");
    std::fs::create_dir_all(&dir).expect("mkdir");
    for (name, seed) in seeds() {
        std::fs::write(dir.join(name), seed).expect("write");
    }
}
