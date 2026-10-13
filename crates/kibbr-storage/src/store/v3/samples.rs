// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The row groups the typed column's tests are run over: one shared list, since a layout
//! one of the tests never saw is a layout nothing says they agree on.
//! Three sources: every file under `testdata/v3/`, row groups the builder makes, and
//! pieces laid out plain by hand, which a file from before the encodings still holds.

use std::path::{Path, PathBuf};

use super::build::{build, schema_of, Level};
use super::file::{open_sealed, walk, Reader};
use super::read::{self, Cell, ChunkDir, PieceDir};
use super::{Form, JsonKind, Source, EPOCH_SECS, NO_COMPRESSION, PLAIN, RFC3339};
use crate::frame::Next;
use crate::store::{effective_ts, Row, RECEIVED_AT_COLUMN};
use kibbr_core::types::ColValue;

/// One chunk as a file stores it, with the directory entry that says how to read it.
pub struct Stored {
    pub dir: ChunkDir,
    pub bytes: Vec<u8>,
    /// The offset the bytes start at, in whatever holds them.
    pub at: u64,
}

impl Stored {
    /// The chunk's cells, as `read::chunk` decodes them.
    pub fn cells(&self, rows: usize) -> Vec<Cell> {
        read::chunk(&self.bytes, self.at, &self.dir, rows).expect("a sample decodes")
    }
}

/// One row group: its schema, its row count and each column's chunk.
pub struct Sample {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: usize,
    /// One for each column, in the schema's order.
    pub chunks: Vec<Stored>,
}

impl Sample {
    /// A column's place in the schema, when the row group has it.
    pub fn place(&self, column: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == column)
    }

    /// Every row as a query would build it before rendering: each column's cell, put in
    /// with what its line wrote.
    pub fn built(&self) -> Vec<Row> {
        let cells: Vec<Vec<Cell>> = self.chunks.iter().map(|c| c.cells(self.rows)).collect();
        (0..self.rows)
            .map(|r| {
                let mut row = Row::new();
                for (at, column) in cells.iter().enumerate() {
                    let (value, written) = column[r].clone();
                    row.insert_written(self.columns[at].clone(), value, written);
                }
                row
            })
            .collect()
    }
}

/// Rows of one template as the row group the store would build of them, each row's time
/// its effective time under the time column `ts`.
pub fn of_rows(name: &str, rows: &[Row]) -> Sample {
    let refs: Vec<&Row> = rows.iter().collect();
    let times: Vec<i64> = rows
        .iter()
        .map(|r| effective_ts(r, "ts").expect("a row with a time"))
        .collect();
    let columns = schema_of(refs.iter().copied());
    let rg = build(
        7,
        0,
        &columns,
        &refs,
        &times,
        Source::NotRecorded,
        Level::Live,
    )
    .expect("rows the builder takes");
    let Next::Frame { payload, .. } = crate::frame::read(&rg.frame, 0, u32::MAX) else {
        panic!("the builder's header is not a frame");
    };
    let header = read::header(payload, 0, |_| Some(columns.len())).expect("header");
    let chunks = header
        .chunks
        .iter()
        .zip(header.chunk_offsets())
        .map(|(dir, at)| Stored {
            dir: dir.clone(),
            bytes: rg.chunks[at as usize..at as usize + dir.stored_len as usize].to_vec(),
            at,
        })
        .collect();
    Sample {
        name: name.to_string(),
        columns,
        rows: rows.len(),
        chunks,
    }
}

/// A line through the real parser into the row a store is handed.
pub fn row(line: &str) -> Row {
    crate::db::row_of(
        &kibbr_core::flatten::parse_line(line.as_bytes(), 1).expect("a line that parses"),
    )
}

/// Lines of one template, through the parser, as one row group.
pub fn of_lines(name: &str, lines: &[String]) -> Sample {
    let rows: Vec<Row> = lines.iter().map(|line| row(line)).collect();
    of_rows(name, &rows)
}

/// One line for each value, `{"ts": .., "v": <value>}`, a second apart within one hour.
pub fn lines_of(values: &[&str]) -> Vec<String> {
    values
        .iter()
        .enumerate()
        .map(|(n, v)| {
            format!(
                r#"{{"ts":"2024-01-15T10:{:02}:{:02}Z","v":{v}}}"#,
                n / 60 % 60,
                n % 60
            )
        })
        .collect()
}

/// A row group of the columns `ts` and `v`, `v` holding these JSON values.
pub fn of_values(name: &str, values: &[&str]) -> Sample {
    of_lines(name, &lines_of(values))
}

/// One column, `v`, of one piece laid out plain and stored as it is: `values` are the
/// piece's bytes for `rows` rows.
fn plain(name: &str, form: Form, rows: usize, values: Vec<u8>) -> Sample {
    Sample {
        name: name.to_string(),
        columns: vec!["v".to_string()],
        rows,
        chunks: vec![Stored {
            dir: ChunkDir {
                layout: 0,
                compression: NO_COMPRESSION,
                stored_len: values.len() as u32,
                stored_crc: crate::frame::crc32c(&values),
                body_len: values.len() as u32,
                a: Some(PieceDir {
                    form,
                    encoding: PLAIN,
                }),
                b: None,
            },
            bytes: values,
            at: 0,
        }],
    }
}

/// Text in plain layout: each value its length, then its bytes.
fn plain_text(values: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

/// At the workspace's root, two directories up: the binary crate's fuzz harness reads
/// the same files.
fn vectors_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../testdata")
        .join("v3")
}

/// The `.col` files under `testdata/v3/`, by name.
pub fn vector_files() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(vectors_dir())
        .expect("testdata/v3")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "col"))
        .collect();
    files.sort();
    files
}

/// Every row group of every file under `testdata/v3/`: a sealed file through its footer,
/// the cut one through the walk, each chunk read as a query reads it.
pub fn vectors() -> Vec<Sample> {
    let mut out = Vec::new();
    for path in vector_files() {
        let index = match open_sealed(&path).expect("a vector opens") {
            Some(index) => index,
            None => {
                let bytes = std::fs::read(&path).expect("read");
                walk(&path, &bytes).expect("a vector walks").index
            }
        };
        let mut reader = Reader::open(&path).expect("open for reading");
        let file = path.file_name().unwrap_or_default().to_string_lossy();
        for (n, entry) in index.entries.iter().enumerate() {
            let columns = index.schemas[entry.schema_id as usize].clone();
            let header = reader.header(entry, columns.len()).expect("header");
            let all: Vec<usize> = (0..columns.len()).collect();
            let (stored, _) = reader.stored(entry, &header, &all).expect("stored chunks");
            let chunks = stored
                .into_iter()
                .zip(&header.chunks)
                .map(|(chunk, dir)| Stored {
                    dir: dir.clone(),
                    bytes: chunk.bytes,
                    at: chunk.at,
                })
                .collect();
            out.push(Sample {
                name: format!("{file}, row group {n}"),
                columns,
                rows: entry.row_count as usize,
                chunks,
            });
        }
    }
    out
}

/// Row groups of `ts` and `v` that between them hold every layout a writer makes, and the
/// plain ones a writer no longer does.
pub fn layouts() -> Vec<Sample> {
    const LOWER: &str = r#""550e8400-e29b-41d4-a716-446655440000""#;
    const OTHER: &str = r#""6ba7b810-9dad-11d1-80b4-00c04fd430c8""#;
    const UPPER: &str = r#""550E8400-E29B-41D4-A716-446655440000""#;
    let mut out = vec![
        // Text.
        of_values(
            "words",
            &[
                r#""GET""#,
                r#""POST""#,
                r#""GET""#,
                r#""GET""#,
                r#""DELETE""#,
                r#""POST""#,
                r#""GET""#,
                r#""""#,
            ],
        ),
        of_values("one word", &[r#""GET""#; 5]),
        of_values(
            "words and nulls",
            &[r#""a""#, "null", r#""b""#, r#""a""#, "null", r#""c""#],
        ),
        of_values("nothing but nulls", &["null"; 4]),
        of_values(
            "a word as a string and as a number",
            &[
                r#""a""#,
                "9223372036854775809",
                r#""9223372036854775809""#,
                r#""a""#,
                "9223372036854775809",
            ],
        ),
        // Values the ladder types and no form prints back, so they are kept as text:
        // what a term that is a number, a date or a bool has to be put to word by word.
        of_values(
            "text the ladder types",
            &[
                r#""-0""#,
                r#""0""#,
                r#"".5""#,
                r#""1e3""#,
                r#""TRUE""#,
                r#""true""#,
                r#""inf""#,
                r#""2024-01-15T10:30:00Z""#,
                r#""1705314600""#,
                r#""0045""#,
                r#""+45""#,
                r#""45""#,
                r#""INFO""#,
                r#""-0""#,
                r#""10.0.0.1""#,
                LOWER,
            ],
        ),
        // Integers.
        of_values(
            "integers, bare and quoted",
            &["200", "404", r#""503""#, "200", "-17", "0", "45"],
        ),
        of_values(
            "integers padded with zeros",
            &[r#""0045000012""#, r#""0000000001""#, r#""0000000045""#],
        ),
        of_values(
            "integers with a plus",
            &[r#""+4500000120""#, r#""+12""#, r#""+45""#, r#""+0""#],
        ),
        of_values(
            "one integer spelled two ways",
            &["200", r#""200""#, "200", r#""200""#],
        ),
        of_values(
            "the ends of an integer",
            &["9223372036854775807", "-9223372036854775808", "0"],
        ),
        of_values(
            "integers beside text and nulls",
            &["7", "8", "9", r#""N/A""#, r#""N/A""#, "null", "45"],
        ),
        of_values(
            "one integer beside one word",
            &["7", "null", "7", r#""N/A""#, "7"],
        ),
        // Floats.
        of_values(
            "floats of mixed digit counts",
            &[
                r#""0.010""#,
                "1.5",
                r#""149.00""#,
                r#""12.5""#,
                "1e3",
                r#""-0.0""#,
                r#""9223372036854775808""#,
                r#""0.01""#,
                "200.0",
            ],
        ),
        of_values(
            "one float with three digit counts",
            &[r#""12.5""#, r#""12.50""#, r#""12.500""#],
        ),
        of_values("a zero of each sign", &["0.0", "-0.0", "0.0"]),
        of_values("one float", &["0.25"; 4]),
        of_values(
            "integers beside floats",
            &["1", "2.5", "3", "200", "200.0", "0.5"],
        ),
        // Dates.
        of_values(
            "dates of mixed fraction digits",
            &[
                r#""2024-01-15T10:30:00Z""#,
                r#""2024-01-15T10:30:00.5Z""#,
                r#""2024-01-15T10:29:59.25Z""#,
                r#""2024-01-15T10:30:00.000Z""#,
                r#""2024-01-15T10:31:00Z""#,
            ],
        ),
        of_values(
            "dates with an offset",
            &[
                r#""2024-01-15T16:00:00+05:30""#,
                r#""2024-01-15T16:00:01+05:30""#,
                r#""2024-01-15T16:00:00+05:30""#,
            ],
        ),
        of_values(
            "dates in small letters",
            &[r#""2024-01-15t10:30:00z""#, r#""2024-01-15t10:30:01z""#],
        ),
        of_values(
            "one instant with two digit counts",
            &[
                r#""2024-01-15T10:30:00.5Z""#,
                r#""2024-01-15T10:30:00.500Z""#,
            ],
        ),
        of_values(
            "epoch seconds, bare and quoted",
            &[
                "1705314600",
                "1705312201",
                "1705312199",
                r#""1705314600""#,
                r#""1705312300""#,
            ],
        ),
        of_values(
            "epoch milliseconds",
            &["1705314600000", "1705312200124", "1705312100000"],
        ),
        of_values("one epoch second", &["1705314600"; 3]),
        of_values(
            "dates in two forms",
            &[
                r#""2024-01-15T10:30:00Z""#,
                "1705314600",
                r#""2024-01-15T10:30:01Z""#,
                "1705312200",
                r#""2024-01-15T10:30:02Z""#,
            ],
        ),
        of_values(
            "dates beside text and nulls",
            &[
                r#""2024-01-15T10:30:00Z""#,
                r#""N/A""#,
                "null",
                r#""2024-01-15T10:30:01Z""#,
                r#""2024-01-15T10:30:00Z""#,
                r#""never""#,
            ],
        ),
        // Booleans.
        of_values(
            "booleans, bare and quoted",
            &["true", "false", r#""true""#, "false", r#""false""#, "true"],
        ),
        of_values(
            "booleans with a capital",
            &[r#""True""#, r#""False""#, r#""True""#],
        ),
        of_values(
            "one boolean spelled two ways",
            &["true", r#""true""#, "true"],
        ),
        // Ids and addresses.
        of_values("ids", &[LOWER, OTHER, LOWER]),
        of_values(
            "ids in capitals",
            &[UPPER, UPPER, r#""6BA7B810-9DAD-11D1-80B4-00C04FD430C8""#],
        ),
        of_values("one id", &[LOWER; 3]),
        of_values("ids in both cases", &[LOWER, UPPER, OTHER, UPPER, LOWER]),
        of_values(
            "addresses, v4",
            &[
                r#""10.0.0.1""#,
                r#""10.0.0.2""#,
                r#""10.0.0.1""#,
                r#""192.0.2.1""#,
            ],
        ),
        of_values(
            "addresses, v6",
            &[
                r#""2001:db8::1""#,
                r#""2001:db8::2""#,
                r#""::ffff:192.0.2.1""#,
                r#""::""#,
                r#""1:0:0:2::3""#,
                r#""fe80::1""#,
            ],
        ),
        of_values(
            "addresses, v6 in capitals",
            &[r#""2001:DB8::1""#, r#""2001:DB8::2""#, r#""FE80::1""#],
        ),
        of_values(
            "addresses, v6 with every group",
            &[
                r#""2001:0db8:0000:0000:0000:0000:0000:0001""#,
                r#""2001:0db8:0000:0000:0000:0000:0000:0002""#,
            ],
        ),
        of_values(
            "addresses, v6 with every group in capitals",
            &[
                r#""2001:0DB8:0000:0000:0000:0000:0000:0001""#,
                r#""2001:0DB8:0000:0000:0000:0000:0000:0002""#,
            ],
        ),
        of_values(
            "addresses of both families",
            &[
                r#""10.0.0.1""#,
                r#""2001:db8::1""#,
                r#""10.0.0.1""#,
                r#""2001:db8::2""#,
                r#""10.0.0.1""#,
            ],
        ),
        // Three kinds in one column: the largest is a piece and the rest are text beside
        // it, a bare `true` among them.
        of_values(
            "a level that is a number, a word and a bool",
            &[
                r#""INFO""#,
                "30",
                r#""30""#,
                "true",
                "null",
                r#""WARN""#,
                "30",
                "45",
            ],
        ),
    ];

    // Enough rows for a second word of presence bits, and enough words for an index of
    // two bytes.
    let many: Vec<String> = (0..300)
        .map(|n| match n % 7 {
            0 => "null".to_string(),
            1 | 2 => (n % 5 * 100).to_string(),
            _ => format!(r#""w{n}""#),
        })
        .collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    out.push(of_values(
        "three hundred rows of words, numbers and nulls",
        &many,
    ));

    // A time column that gives some rows no instant, and the arrival time those rows were
    // stored with: what the window falls back to.
    let mut late = Vec::new();
    for (n, ts) in [
        r#""2024-01-15T10:30:00Z""#,
        r#""N/A""#,
        "null",
        r#""2024-01-15T10:30:03Z""#,
        r#""soon""#,
    ]
    .iter()
    .enumerate()
    {
        let mut row = row(&format!(r#"{{"ts":{ts},"v":{n}}}"#));
        if effective_ts(&row, "ts").is_none() {
            let arrived = 1_705_314_600_000_000_000 + n as i64 * 1_000_000_000;
            row.insert(RECEIVED_AT_COLUMN.to_string(), ColValue::Datetime(arrived));
        }
        late.push(row);
    }
    out.push(of_rows("rows that fall back to their arrival time", &late));

    // Plain pieces, by hand.
    let text = Form::String {
        kind: JsonKind::Str,
    };
    out.push(plain(
        "plain text",
        text,
        7,
        plain_text(&[
            "INFO",
            "-0",
            "45",
            "",
            "2024-01-15T10:30:00Z",
            "true",
            "INFO",
        ]),
    ));
    out.push(plain(
        "plain integers",
        Form::Int64 {
            kind: JsonKind::Lit,
            plus: false,
            width: 0,
        },
        4,
        [200i64, -17, 45, 200]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect(),
    ));
    out.push(plain(
        "plain dates",
        Form::Datetime {
            kind: JsonKind::Str,
            unit: RFC3339,
            separator: b'T',
            zone: b'Z',
            offset: 0,
            digits: 0,
        },
        3,
        [1_705_314_600i64, 1_705_314_601, 1_705_314_600]
            .iter()
            .flat_map(|v| (v * 1_000_000_000).to_le_bytes())
            .collect(),
    ));
    out.push(plain(
        "plain epoch seconds",
        Form::Datetime {
            kind: JsonKind::Lit,
            unit: EPOCH_SECS,
            separator: 0,
            zone: 0,
            offset: 0,
            digits: 0,
        },
        2,
        [1_705_314_600i64, 1_705_312_200]
            .iter()
            .flat_map(|v| (v * 1_000_000_000).to_le_bytes())
            .collect(),
    ));
    out.push(plain(
        "plain booleans",
        Form::Bool {
            kind: JsonKind::Lit,
            case: 0,
        },
        3,
        vec![1, 0, 1],
    ));
    out
}

/// Every sample: the vectors, then the layouts.
pub fn all() -> Vec<Sample> {
    let mut out = vectors();
    out.extend(layouts());
    out
}
