// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The v3 segment reader: one `.col` file's bytes through every way the store reads
//! one - the unsealed walk, the sealed open, and each row group through `file::Reader` and
//! `column::Column`. It never panics, never reads outside the file, and refuses what it
//! does not know. A first byte of `0xfb` makes the rest instructions (`build`) with lengths
//! and CRCs put right, since a fuzzer does not guess a CRC.
//!
//! The instructions (`build`), each number taken modulo 8, so 4, 5 and 7 are 0, 1 and 3 again:
//!
//! ```text
//! 0  frame      [2] len  [len] payload      framed: its length and CRC are worked out
//! 1  row group  see `group`                 a header frame and the chunk area after it
//! 2  trailer                                the last frame's length, then 'KBBR'
//! 3  bytes      [2] len  [len] bytes        appended as they are
//! 6  seal       [1] flags                   the footer of what was built, and its trailer
//! ```
//!
//! A row group (`group`):
//!
//! ```text
//! [1]   flags      bit 0: write a schema record of this many columns first, and name it
//! [57]  the header's fields from template_id to last, as they are
//! [1]   columns, modulo 33; per column:
//!         [1] layout  [1] compression
//!         [1] knob    bit 7: with compression 1, the body is what is given, compressed
//!                     here; bits 0-1: 1, 2 or 3 turns one bit of stored_len, stored_crc
//!                     or body_len; bits 2-6: which bit
//!         [1] len, modulo 32  [len] the pieces: type, encoding and form, as they are
//!         [2] len  [len] the chunk's bytes
//! [1]   sections, modulo 5; per section:
//!         [1] kind  [1] column  [1] len  [len] bytes
//! [1]   knob       bits 0-1: 1 turns one bit of chunks_len; bits 2-6: which bit
//! ```

use std::io::{self, Read, Seek, SeekFrom};
use std::mem::size_of;
use std::path::Path;

use crate::core::types::ColValue;
use crate::storage::frame::{self, Next};
use crate::storage::store::v3::build::RowGroup;
use crate::storage::store::v3::column::{Column, Memo};
use crate::storage::store::v3::file::{self, Entry, Index, Reader};
use crate::storage::store::v3::read::{self, Cell, ChunkDir, Header, PieceDir};
use crate::storage::store::v3::{
    type_byte, Bad, Bytes, Form, FormatError, JsonKind, Source, DAY_NS, HEADER_LEN, KIND_FOOTER,
    KIND_ROW_GROUP, KIND_SCHEMA, MAGIC, MAX_BODY, MAX_ROWS, NO_COMPRESSION, OPTIONAL,
    PRE_RENAME_MAGIC, TRAILER_LEN, VERSION, ZSTD,
};

/// The first byte of an input that is instructions and not a file.
pub const BUILT: u8 = 0xfb;

/// The largest file read. Nothing the reader does depends on a file being larger, and it
/// keeps what the harness itself holds - the built file, the recovered copy - small.
const MAX_FILE: usize = 1 << 20;

/// What one allocation may take under the fuzzer: `-malloc_limit_mb=512`.
const ALLOC_LIMIT: u64 = 512 << 20;

/// The cells one reading of a file decodes before it stops.
const WORK: u64 = 1 << 18;

/// The largest zstd body the harness decompresses a second time to read it itself. A
/// larger one is still decoded by the reader and held to everything but `hold_body`.
const BODY_CHECK: u32 = 4 << 20;

/// The schema number a copied row group is given: one no built file reaches, so a copy
/// that kept its old number shows.
const RENUMBERED: u32 = 0x5eed_0001;

/// The path the reader is given: it names it in errors and never opens it.
const NAME: &str = "seg_0_000001.col";

/// The file id of a built file.
const FILE_ID: [u8; 16] = *b"kibbr fuzz file.";

pub fn run(data: &[u8]) {
    match data.split_first() {
        Some((&BUILT, feed)) => check(&build(feed)),
        _ => check(data),
    }
}

/// One file, read every way.
fn check(file: &[u8]) {
    if file.len() > MAX_FILE {
        return;
    }
    if file.len() < HEADER_LEN {
        cut_in_header(file);
    }
    let kept = unsealed(file);
    // What recovery leaves on disk: the bytes the walk kept, and the footer of them.
    let healed = kept.as_ref().map(|(index, end)| {
        let mut out = file[..*end].to_vec();
        out.extend(index.seal_bytes());
        out
    });
    match (&kept, &healed) {
        // The file is already what recovery would make of it, so one reading does for both.
        (Some((index, _)), Some(healed)) if healed == file => same_rows(sealed(file, true), index),
        (Some((index, _)), Some(healed)) => {
            sealed(file, false);
            same_rows(sealed(healed, true), index);
        }
        _ => {
            sealed(file, false);
        }
    }
}

// ── What a reading may spend ───────────────────────────────────────────────────────

/// A reading that stopped without a verdict, past `WORK` cells.
struct Declined;

/// Whether a chunk of `rows` rows is still within the cells this reading decodes.
fn weigh(rows: u32, work: &mut u64) -> Result<(), Declined> {
    if u64::from(rows) > *work {
        return Err(Declined);
    }
    *work -= u64::from(rows);
    Ok(())
}

/// An error the reader gave: a refusal, which is a right answer, held to naming a place
/// inside the file.
fn refused(err: &FormatError, len: usize) {
    match err {
        FormatError::Corrupt { offset, what, .. } => assert!(
            *offset <= len as u64,
            "an error names offset {offset} of a file of {len} bytes: {what}"
        ),
        FormatError::Version { .. } | FormatError::PreRename { .. } => {}
        FormatError::Io { source, .. } => panic!("a read of bytes in memory failed: {source}"),
    }
}

fn refused_at(bad: &Bad, len: usize) {
    assert!(
        bad.offset <= len as u64,
        "an error names offset {} of a file of {len} bytes: {}",
        bad.offset,
        bad.what
    );
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(b)
}

// ── The header and the frames ──────────────────────────────────────────────────────

/// The file's id, when its first 24 bytes are a v3 header - and the reader's answer held
/// to the header's layout read here a second time.
fn header(file: &[u8]) -> Option<[u8; 16]> {
    let good = file.len() >= HEADER_LEN
        && file[..4] == MAGIC
        && file[4] == VERSION
        && file[5..8] == [0, 0, 0];
    match file::check_header(Path::new(NAME), file) {
        Ok(id) => {
            assert!(good, "a header that is not a v3 header was accepted");
            assert_eq!(
                id[..],
                file[8..HEADER_LEN],
                "the file id is not the header's"
            );
            Some(id)
        }
        Err(err) => {
            assert!(!good, "a v3 header was refused: {err}");
            if let FormatError::PreRename { .. } = err {
                assert_eq!(file[..4], PRE_RENAME_MAGIC);
            }
            refused(&err, file.len());
            None
        }
    }
}

/// The frame at `at`, read here from its layout, and `frame::read` held to the same answer:
/// its payload and where it ends, or `None` at a tear or the end.
fn frame_at(file: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let mine = (|| {
        let head = file.get(at..at.checked_add(frame::HEADER)?)?;
        let len = u32_at(head, 0) as usize;
        if len == 0 {
            return None;
        }
        let start = at + frame::HEADER;
        let payload = file.get(start..start.checked_add(len)?)?;
        (frame::crc32c(payload) == u32_at(head, 4)).then_some((payload, start + len))
    })();
    match (frame::read(file, at, u32::MAX), mine) {
        (Next::Frame { payload, end }, Some((mine, mine_end))) => {
            assert!(payload == mine && end == mine_end, "another frame at {at}");
        }
        (Next::End, None) => assert_eq!(at, file.len(), "an end before the end"),
        (Next::Tear, None) => assert_ne!(at, file.len(), "a tear at the end"),
        (got, mine) => panic!(
            "at {at} the frame reader found {got:?} where the layout has {}",
            if mine.is_some() { "a frame" } else { "none" }
        ),
    }
    mine
}

/// A file that ends inside its header, as recovery judges it: the start of a v3
/// header and nothing else is a file a crash cut, and anything else is refused.
fn cut_in_header(file: &[u8]) {
    let head = [MAGIC[0], MAGIC[1], MAGIC[2], MAGIC[3], VERSION, 0, 0, 0];
    let n = file.len().min(head.len());
    let magic = file.len().min(4);
    match file::cut_in_header(Path::new(NAME), file) {
        Ok(()) => assert!(
            file[..n] == head[..n],
            "a file cut inside something that is not a v3 header was taken for one"
        ),
        Err(FormatError::PreRename { .. }) => assert!(
            magic > 0 && file[..magic] == PRE_RENAME_MAGIC[..magic],
            "a file that does not start as a pre-rename file was called one"
        ),
        Err(FormatError::Version { found, .. }) => assert!(
            file.len() > 4 && file[..4] == MAGIC && found == file[4] && found != VERSION,
            "a version that is not the file's was named"
        ),
        Err(err) => {
            assert!(
                file[..n] != head[..n],
                "the start of a v3 header was refused: {err}"
            );
            refused(&err, file.len());
        }
    }
}

// ── The unsealed walk ──────────────────────────────────────────────────────────────

/// The file walked from byte 24 as recovery walks it, here, and `file::walk` held to the same
/// answer: what it kept and where it stopped, or `None` when refused or declined.
fn unsealed(file: &[u8]) -> Option<(Index, usize)> {
    let path = Path::new(NAME);
    let mine = match header(file) {
        Some(id) => walk(file, id),
        None => Ok(None),
    };
    let Ok(mine) = mine else {
        return None;
    };
    match (file::walk(path, file), mine) {
        (Ok(walked), Some((index, end))) => {
            assert_eq!(walked.end, end as u64, "the walk stopped somewhere else");
            assert_eq!(walked.index, index, "the walk kept something else");
            Some((index, end))
        }
        (Err(err), None) => {
            refused(&err, file.len());
            None
        }
        (Ok(walked), None) => panic!(
            "the walk kept {} row group(s) of a file that breaks a rule",
            walked.index.entries.len()
        ),
        (Err(err), Some(_)) => panic!("the walk refused a file that breaks no rule: {err}"),
    }
}

/// The walk itself: `Ok(None)` when a frame whose CRC holds breaks a rule.
fn walk(file: &[u8], id: [u8; 16]) -> Result<Option<(Index, usize)>, Declined> {
    let mut index = Index::new(id);
    let mut work = WORK;
    let mut at = HEADER_LEN;
    while let Some((payload, end)) = frame_at(file, at) {
        let base = (at + frame::HEADER) as u64;
        match payload[0] {
            KIND_SCHEMA => {
                let Some(columns) = schema(payload, base, index.schemas.len(), file.len()) else {
                    return Ok(None);
                };
                index.schemas.push(columns);
                at = end;
            }
            KIND_ROW_GROUP => {
                let found = read::header(payload, base, |id| {
                    index.schemas.get(id as usize).map(Vec::len)
                });
                let header = match found {
                    Ok(header) => header,
                    Err(bad) => {
                        refused_at(&bad, file.len());
                        return Ok(None);
                    }
                };
                hold_header(
                    &header,
                    payload,
                    index.schemas[header.schema_id as usize].len(),
                );
                // A chunk area that runs past the end, or one whose CRCs fail, is a tear.
                let Some(area) = end
                    .checked_add(header.chunks_len as usize)
                    .and_then(|area_end| file.get(end..area_end))
                else {
                    break;
                };
                let spans = spans(&header);
                if spans
                    .iter()
                    .any(|(from, to, crc)| frame::crc32c(&area[*from..*to]) != *crc)
                {
                    break;
                }
                for (dir, (from, to, _)) in header.chunks.iter().zip(&spans) {
                    weigh(header.row_count, &mut work)?;
                    let stored = &area[*from..*to];
                    let at = (end + from) as u64;
                    let chunk = Chunk {
                        dir,
                        rows: header.row_count,
                        stored,
                        at,
                    };
                    match read::chunk(stored, at, dir, header.row_count as usize) {
                        Ok(cells) => {
                            hold_cells(dir, header.row_count, stored, &cells);
                            hold_column(&chunk, file.len(), Some(&cells));
                        }
                        Err(bad) => {
                            hold_column(&chunk, file.len(), None);
                            refused_at(&bad, file.len());
                            return Ok(None);
                        }
                    }
                }
                index.entries.push(Entry {
                    template_id: header.template_id,
                    offset: at as u64,
                    frame_len: (end - at) as u32,
                    schema_id: header.schema_id,
                    row_count: header.row_count,
                    min_time: header.min_time,
                    max_time: header.max_time,
                    chunks_len: header.chunks_len,
                });
                at = end + area.len();
            }
            KIND_FOOTER => break,
            kind if kind & OPTIONAL != 0 => at = end,
            _ => return Ok(None),
        }
    }
    Ok(Some((index, at)))
}

/// A schema record's columns, when it is the record that comes next: its id the
/// number of records before it, its names sorted and distinct, its payload used up.
fn schema(payload: &[u8], base: u64, next: usize, len: usize) -> Option<Vec<String>> {
    let mut r = Bytes::new(payload, base);
    let read = (|| {
        r.u8("kind")?;
        let id = r.u32("schema_id")?;
        let columns = file::schema_columns(&mut r)?;
        r.done("a schema record")?;
        Ok::<_, Bad>((id, columns))
    })();
    let (id, columns) = match read {
        Ok(read) => read,
        Err(bad) => {
            refused_at(&bad, len);
            return None;
        }
    };
    if id as usize != next {
        return None;
    }
    assert!(
        columns.windows(2).all(|w| w[0] < w[1]),
        "a schema's names are not sorted and distinct"
    );
    let mut spelled = vec![KIND_SCHEMA];
    spelled.extend_from_slice(&id.to_le_bytes());
    spelled.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for name in &columns {
        spelled.extend_from_slice(&(name.len() as u32).to_le_bytes());
        spelled.extend_from_slice(name.as_bytes());
    }
    assert!(
        spelled == payload,
        "an accepted schema record has another spelling"
    );
    Some(columns)
}

/// Where each chunk, and then each section, lies in the chunk area, and its CRC. The
/// header's lengths add up to the area's (`hold_header`), so every span is inside it.
fn spans(header: &Header) -> Vec<(usize, usize, u32)> {
    let mut at = 0usize;
    let lens = header
        .chunks
        .iter()
        .map(|c| (c.stored_len, c.stored_crc))
        .chain(header.sections.iter().map(|s| (s.len, s.crc)));
    let spans: Vec<_> = lens
        .map(|(len, crc)| {
            let from = at;
            at += len as usize;
            (from, at, crc)
        })
        .collect();
    let starts: Vec<u64> = spans.iter().map(|s| s.0 as u64).collect();
    assert_eq!(
        header.chunk_offsets()[..],
        starts[..header.chunks.len()],
        "a chunk's offset is not the lengths before it"
    );
    spans
}

// ── What an accepted header holds ──────────────────────────────────────────────────

/// A row group header the reader accepted, held to the format: each field in its
/// closed set, the lengths adding up, and the payload the one spelling of all of it.
fn hold_header(h: &Header, payload: &[u8], columns: usize) {
    assert_eq!(
        h.chunks.len(),
        columns,
        "a directory entry per schema column"
    );
    assert!(h.row_count >= 1, "a row group of no rows");
    assert!(
        h.row_count <= MAX_ROWS,
        "a row group of {} rows, where no writer makes one of more than {MAX_ROWS}",
        h.row_count
    );
    assert!(
        h.min_time <= h.max_time && h.min_time.div_euclid(DAY_NS) == h.max_time.div_euclid(DAY_NS),
        "a time range that is not ordered within one UTC day"
    );
    match h.source {
        Source::NotRecorded => {}
        Source::Wal {
            stream,
            first,
            last,
        }
        | Source::Load {
            stream,
            first,
            last,
        } => assert!(
            stream != 0 && first <= last,
            "a source with no stream or range"
        ),
    }
    let mut total = 0u64;
    for dir in &h.chunks {
        hold_dir(dir);
        total += u64::from(dir.stored_len);
    }
    for s in &h.sections {
        assert!(
            s.kind & OPTIONAL != 0,
            "a required section kind 0x{:02x}",
            s.kind
        );
        assert!((s.column as usize) < columns, "a section on no column");
        total += u64::from(s.len);
    }
    assert_eq!(
        total,
        u64::from(h.chunks_len),
        "the directory does not add up"
    );
    assert!(
        spell(h) == payload,
        "an accepted row group header has another spelling: {h:?}"
    );
}

fn hold_dir(dir: &ChunkDir) {
    assert!(dir.layout <= 0b100, "chunk layout 0x{:02x}", dir.layout);
    assert!(
        matches!(dir.compression, NO_COMPRESSION | ZSTD),
        "chunk compression {}",
        dir.compression
    );
    // `body_len` is a field only a compressed chunk has, and the one a reader allocates
    // from before it decompresses; a stored chunk's `stored_len` is held by the walk.
    if dir.compression == NO_COMPRESSION {
        assert_eq!(
            dir.body_len, dir.stored_len,
            "a stored body of another length"
        );
    } else {
        assert!(dir.body_len <= MAX_BODY, "a body of {} bytes", dir.body_len);
    }
    if dir.layout == read::ALL_NULL {
        assert!(
            dir.stored_len == 0
                && dir.stored_crc == 0
                && dir.compression == NO_COMPRESSION
                && dir.a.is_none()
                && dir.b.is_none(),
            "an all-null chunk with bytes or pieces"
        );
        return;
    }
    assert!(dir.a.is_some(), "a chunk with rows and no piece");
    assert_eq!(
        dir.b.is_some(),
        dir.layout & read::TWO_PIECES != 0,
        "piece B and the layout disagree"
    );
    for piece in [dir.a, dir.b].iter().flatten() {
        hold_piece(piece);
    }
}

/// The table of which encoding lays out which type, and the range for every
/// field of a form, with the combinations it forbids.
fn hold_piece(piece: &PieceDir) {
    let lit = piece.form.kind() == JsonKind::Lit;
    let in_table = matches!(
        (piece.form, piece.encoding),
        (_, 0 | 1)
            | (Form::String { .. }, 2)
            | (Form::Int64 { .. }, 3)
            | (Form::Datetime { .. }, 4)
            | (Form::Bool { .. }, 5)
    );
    assert!(in_table, "encoding {} on {:?}", piece.encoding, piece.form);
    let in_range = match piece.form {
        Form::String { .. } => true,
        Form::Int64 { plus, width, .. } => !lit || (!plus && width == 0),
        Form::Float64 { digits, .. } => !lit || digits == 0,
        Form::Datetime {
            kind,
            unit: 0,
            separator,
            zone,
            offset,
            digits,
        } => {
            kind == JsonKind::Str
                && matches!(separator, b'T' | b't' | b' ')
                && matches!(zone, b'Z' | b'z' | b'+' | b'-')
                && offset <= 1439
                && (offset == 0 || matches!(zone, b'+' | b'-'))
                && (digits <= 9 || digits == 255)
        }
        Form::Datetime {
            unit: 1 | 2,
            separator: 0,
            zone: 0,
            offset: 0,
            digits: 0,
            ..
        } => true,
        Form::Datetime { .. } => false,
        Form::Bool { case, .. } => case <= 1 && (!lit || case == 0),
        Form::Uuid { case } => case <= 1,
        Form::Ip { spelling } => matches!(spelling, 4 | 6..=9),
    };
    assert!(in_range, "a form outside its range: {:?}", piece.form);
}

/// A header's payload, written from what the reader says it holds.
fn spell(h: &Header) -> Vec<u8> {
    let mut out = vec![KIND_ROW_GROUP];
    out.extend_from_slice(&h.template_id.to_le_bytes());
    out.extend_from_slice(&h.schema_id.to_le_bytes());
    out.extend_from_slice(&h.row_count.to_le_bytes());
    out.extend_from_slice(&h.min_time.to_le_bytes());
    out.extend_from_slice(&h.max_time.to_le_bytes());
    let (source, stream, first, last) = h.source.bytes();
    out.push(source);
    out.extend_from_slice(&stream.to_le_bytes());
    out.extend_from_slice(&first.to_le_bytes());
    out.extend_from_slice(&last.to_le_bytes());
    out.extend_from_slice(&h.chunks_len.to_le_bytes());
    for dir in &h.chunks {
        out.push(dir.layout);
        out.push(dir.compression);
        out.extend_from_slice(&dir.stored_len.to_le_bytes());
        out.extend_from_slice(&dir.stored_crc.to_le_bytes());
        if dir.compression != NO_COMPRESSION {
            out.extend_from_slice(&dir.body_len.to_le_bytes());
        }
        for piece in [dir.a, dir.b].iter().flatten() {
            out.extend_from_slice(&piece_bytes(piece));
        }
    }
    out.extend_from_slice(&(h.sections.len() as u16).to_le_bytes());
    for s in &h.sections {
        out.push(s.kind);
        out.extend_from_slice(&s.column.to_le_bytes());
        out.extend_from_slice(&s.len.to_le_bytes());
        out.extend_from_slice(&s.crc.to_le_bytes());
    }
    out
}

/// A piece as the directory spells it: type, encoding, form.
fn piece_bytes(piece: &PieceDir) -> Vec<u8> {
    let mut out = vec![type_byte(piece.form.column_type()), piece.encoding];
    out.extend(piece.form.bytes());
    out
}

// ── What a decoded chunk holds ─────────────────────────────────────────────────────

/// A chunk the reader decoded: one cell a row, nulls where the layout has them, each value
/// of its piece's type, and a zstd frame that is the whole of what was stored.
fn hold_cells(dir: &ChunkDir, rows: u32, stored: &[u8], cells: &[Cell]) {
    assert_eq!(
        cells.len(),
        rows as usize,
        "a chunk that is not one cell a row"
    );
    assert!(
        u64::from(rows) * size_of::<Cell>() as u64 <= ALLOC_LIMIT,
        "a chunk of {} stored byte(s) was decoded into {rows} cells of {} bytes each, past \
         the {ALLOC_LIMIT} one allocation may take",
        stored.len(),
        size_of::<Cell>()
    );
    let nulls = cells
        .iter()
        .filter(|(value, _)| matches!(value, ColValue::Null))
        .count();
    if dir.layout == read::ALL_NULL {
        assert_eq!(nulls, cells.len(), "a value in an all-null chunk");
    } else if dir.layout & read::SOME_NULL != 0 {
        assert!(
            nulls > 0 && nulls < cells.len(),
            "a presence bitmap of one kind"
        );
    } else {
        assert_eq!(nulls, 0, "a null in a chunk with no presence bitmap");
    }
    // Text goes through the type ladder and comes back as whatever it reads as; every
    // other piece holds its own type and nothing else.
    let pieces: Vec<Form> = [dir.a, dir.b].iter().flatten().map(|p| p.form).collect();
    if !pieces
        .iter()
        .any(|form| matches!(form, Form::String { .. }))
    {
        for (value, _) in cells {
            let fits = pieces.iter().any(|form| match (form, value) {
                (Form::Int64 { .. }, ColValue::Int64(_)) => true,
                (Form::Float64 { .. }, ColValue::Float64(f)) => f.is_finite(),
                (Form::Datetime { .. }, ColValue::Datetime(_)) => true,
                (Form::Bool { .. }, ColValue::Bool(_)) => true,
                (Form::Uuid { .. } | Form::Ip { .. }, ColValue::String(_)) => true,
                _ => false,
            });
            assert!(
                fits || matches!(value, ColValue::Null),
                "{value:?} out of pieces {pieces:?}"
            );
        }
    }
    if dir.compression == ZSTD {
        assert_eq!(
            zstd::zstd_safe::find_frame_compressed_size(stored),
            Ok(stored.len()),
            "a zstd chunk that is not exactly one frame"
        );
        if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(stored) {
            assert_eq!(
                size,
                u64::from(dir.body_len),
                "a zstd frame of another size"
            );
        }
        if dir.body_len <= BODY_CHECK {
            let body = zstd::bulk::decompress(stored, dir.body_len as usize)
                .expect("a zstd chunk the reader decoded does not decompress");
            assert_eq!(
                body.len(),
                dir.body_len as usize,
                "a body of another length"
            );
            hold_body(dir, rows as usize, &body);
        }
    } else {
        hold_body(dir, rows as usize, stored);
    }
}

// ── The typed decoder ──────────────────────────────────────────────────────────────

/// One chunk as a reader was handed it: its directory entry, its row group's row count,
/// its stored bytes and the file offset they start at.
struct Chunk<'a> {
    dir: &'a ChunkDir,
    rows: u32,
    stored: &'a [u8],
    at: u64,
}

/// The typed decoder over a chunk `read::chunk` has judged: with `cells` every row prints
/// as its cell, with `None` the chunk or one of its rows is refused, inside the file.
fn hold_column(chunk: &Chunk<'_>, len: usize, cells: Option<&[Cell]>) {
    let rows = chunk.rows as usize;
    let column = match Column::decode(chunk.stored, chunk.at, chunk.dir, rows) {
        Ok(column) => column,
        Err(bad) => {
            assert!(
                cells.is_none(),
                "the typed decoder refused a chunk the reader decoded: {}",
                bad.what
            );
            refused_at(&bad, len);
            return;
        }
    };
    let col = match column.over(chunk.stored) {
        Ok(col) => col,
        Err(bad) => panic!(
            "a column does not read over the bytes it was decoded from: {}",
            bad.what
        ),
    };
    assert_eq!(col.rows(), rows, "a column that is not one value a row");
    assert!(
        col.cell(rows).is_err(),
        "a column printed a row past its last"
    );
    let other: &[u8] = match chunk.stored.split_last() {
        Some((_, shorter)) => shorter,
        None => &[0],
    };
    assert!(
        column.over(other).is_err(),
        "a column read over bytes of another length"
    );
    let Some(cells) = cells else {
        let refusal = (0..rows).find_map(|row| col.cell(row).err());
        match refusal {
            Some(bad) => refused_at(&bad, len),
            None => panic!("the typed decoder printed every row of a chunk the reader refused"),
        }
        return;
    };
    let mut memo = Memo::default();
    for pass in 0..2 {
        for (row, cell) in cells.iter().enumerate() {
            if pass == 0 {
                match col.cell(row) {
                    Ok(printed) => assert!(printed == *cell, "row {row} prints as another cell"),
                    Err(bad) => panic!("row {row} of a chunk the reader decoded: {}", bad.what),
                }
            }
            match col.cell_with(row, &mut memo) {
                Ok(printed) => assert!(
                    printed == *cell,
                    "row {row} prints as another cell through the memo"
                ),
                Err(bad) => panic!(
                    "row {row} of a chunk the reader decoded, through the memo: {}",
                    bad.what
                ),
            }
        }
    }
}

// ── What a decoded chunk's body holds ──────────────────────────────────────────────

/// The next `n` bytes of a body the reader decoded, which must be there.
fn cut<'a>(rest: &mut &'a [u8], n: usize, what: &str) -> &'a [u8] {
    assert!(
        n <= rest.len(),
        "a decoded chunk's {what} wants {n} byte(s) and {} remain",
        rest.len()
    );
    let (taken, left) = rest.split_at(n);
    *rest = left;
    taken
}

/// A bitmap of `n` bits: `ceil(n/8)` bytes, and no bit set past the last row.
/// How many are set.
fn bitmap<'a>(rest: &mut &'a [u8], n: usize, what: &str) -> (&'a [u8], usize) {
    let map = cut(rest, n.div_ceil(8), what);
    if !n.is_multiple_of(8) {
        assert_eq!(
            map[map.len() - 1] >> (n % 8),
            0,
            "a decoded chunk's {what} has a bit set past its last row"
        );
    }
    (map, map.iter().map(|b| b.count_ones() as usize).sum())
}

/// The body of a chunk the reader decoded, read a second time from its layout,
/// each piece's bytes used up exactly.
fn hold_body(dir: &ChunkDir, rows: usize, body: &[u8]) {
    let Some(a) = dir.a else {
        assert!(body.is_empty(), "an all-null chunk with a body");
        return;
    };
    let mut rest = body;
    let mut present = rows;
    if dir.layout & read::SOME_NULL != 0 {
        present = bitmap(&mut rest, rows, "presence bitmap").1;
        assert!(
            present > 0 && present < rows,
            "a presence bitmap over rows that are all present or all null"
        );
    }
    match dir.b {
        Some(b) => {
            let in_b = bitmap(&mut rest, present, "selector bitmap").1;
            assert!(
                in_b > 0 && in_b < present,
                "a piece of two that holds no rows"
            );
            let a_len = u32_at(cut(&mut rest, 4, "piece_a_len"), 0) as usize;
            hold_values(&a, cut(&mut rest, a_len, "piece A"), present - in_b);
            hold_values(&b, rest, in_b);
        }
        None => hold_values(&a, rest, present),
    }
}

/// One piece of `n` rows the reader decoded, read from its layout: each value one
/// its type may hold, and no byte after the last.
fn hold_values(piece: &PieceDir, bytes: &[u8], n: usize) {
    assert!(n > 0, "a piece of no rows");
    let mut rest = bytes;
    let form = &piece.form;
    let kinds = (form.kind() == JsonKind::Mixed).then(|| {
        let (map, literals) = bitmap(&mut rest, n, "kind bitmap");
        assert!(
            literals > 0 && literals < n,
            "json_kind 2 over rows that are all one kind"
        );
        map
    });
    let per_row = match form {
        Form::Float64 { digits: 255, .. } => Some(254),
        Form::Datetime { digits: 255, .. } => Some(9),
        _ => None,
    };
    if let Some(most) = per_row {
        let digits = cut(&mut rest, n, "per-row digits");
        for (row, d) in digits.iter().enumerate() {
            assert!(*d <= most, "a per-row digits byte of {d}");
            let literal = kinds.is_some_and(|map| map[row / 8] >> (row % 8) & 1 == 1);
            assert!(!literal || *d == 0, "a JSON number with a digits byte");
        }
    }
    match piece.encoding {
        0 => (0..n).for_each(|_| hold_value(&mut rest, form)),
        1 => hold_value(&mut rest, form),
        2 => {
            let count = u32_at(cut(&mut rest, 4, "word count"), 0) as usize;
            assert!(
                count >= 1 && count <= n,
                "a word list of {count} words for {n} rows"
            );
            (0..count).for_each(|_| hold_value(&mut rest, form));
            let width = match count {
                0..=256 => 1,
                257..=65_536 => 2,
                _ => 4,
            };
            // Numbered in the order rows first name them, and every word named.
            let mut named = 0usize;
            for index in cut(&mut rest, n * width, "word indexes").chunks(width) {
                let mut le = [0u8; 4];
                le[..width].copy_from_slice(index);
                let index = u32::from_le_bytes(le) as usize;
                assert!(index <= named && index < count, "a word named out of turn");
                named += usize::from(index == named);
            }
            assert_eq!(named, count, "a word list with a word no row names");
        }
        3 | 4 => (0..n).for_each(|_| hold_leb128(&mut rest)),
        5 => {
            bitmap(&mut rest, n, "Bool bits");
        }
        other => panic!("a piece laid out with encoding {other} was decoded"),
    }
    assert!(
        rest.is_empty(),
        "{} byte(s) left over after a decoded piece's values",
        rest.len()
    );
}

/// One value in plain layout: its bytes there, and of the values its type holds.
fn hold_value(rest: &mut &[u8], form: &Form) {
    match form {
        Form::String { .. } => {
            let len = u32_at(cut(rest, 4, "string length"), 0) as usize;
            let text = cut(rest, len, "string");
            assert!(
                std::str::from_utf8(text).is_ok(),
                "a string that is not UTF-8"
            );
        }
        Form::Int64 { .. } | Form::Datetime { .. } => {
            cut(rest, 8, "eight-byte value");
        }
        Form::Float64 { .. } => {
            let mut bits = [0u8; 8];
            bits.copy_from_slice(cut(rest, 8, "Float64"));
            assert!(
                f64::from_bits(u64::from_le_bytes(bits)).is_finite(),
                "a Float64 that is not finite"
            );
        }
        Form::Bool { .. } => {
            let byte = cut(rest, 1, "Bool")[0];
            assert!(byte <= 1, "a Bool byte of {byte}");
        }
        Form::Uuid { .. } => {
            cut(rest, 16, "Uuid");
        }
        Form::Ip { spelling } => {
            cut(rest, if *spelling == 4 { 4 } else { 16 }, "Ip");
        }
    }
}

/// One LEB128 in its one spelling: the shortest, at most ten bytes, and nothing
/// past the 64th bit.
fn hold_leb128(rest: &mut &[u8]) {
    for n in 0..10 {
        let byte = cut(rest, 1, "LEB128")[0];
        assert!(n < 9 || byte <= 1, "a LEB128 of more than 64 bits");
        if byte & 0x80 == 0 {
            assert!(byte != 0 || n == 0, "a LEB128 with a byte it does not need");
            return;
        }
    }
    panic!("a LEB128 that runs past ten bytes");
}

// ── The sealed open ────────────────────────────────────────────────────────────────

/// Where `len` bytes at `offset` start in the file. The reader allocates its buffer first,
/// so a request the file cannot meet is a length used before anything compared it.
fn inside(file: &[u8], offset: u64, len: usize) -> usize {
    let fits = usize::try_from(offset)
        .ok()
        .and_then(|from| from.checked_add(len))
        .is_some_and(|to| to <= file.len());
    assert!(
        fits,
        "the reader asked for {len} byte(s) at offset {offset} of a file of {} bytes",
        file.len()
    );
    offset as usize
}

/// A file in memory as `file::Reader` reads one: it seeks, and it reads exactly.
struct Mem<'a> {
    file: &'a [u8],
    at: u64,
}

impl Read for Mem<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let from = inside(self.file, self.at, buf.len());
        buf.copy_from_slice(&self.file[from..from + buf.len()]);
        self.at += buf.len() as u64;
        Ok(buf.len())
    }
}

impl Seek for Mem<'_> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        match to {
            SeekFrom::Start(at) => self.at = at,
            other => panic!("the reader seeks from the start, and asked for {other:?}"),
        }
        Ok(self.at)
    }
}

/// The file opened through its trailer and footer, and every row group its index lists read
/// through `file::Reader`. With `kept` it is one recovery made, so all of it must read.
fn sealed(file: &[u8], kept: bool) -> Option<Index> {
    let path = Path::new(NAME);
    let opened = file::sealed(path, file.len() as u64, |offset, buf| {
        let from = inside(file, offset, buf.len());
        buf.copy_from_slice(&file[from..from + buf.len()]);
        Ok(())
    });
    let index = match opened {
        Ok(Some(index)) => index,
        Ok(None) => {
            assert!(!kept, "a file recovery sealed does not open as sealed");
            return None;
        }
        Err(err) => {
            assert!(!kept, "a file recovery sealed is refused: {err}");
            refused(&err, file.len());
            return None;
        }
    };
    hold_index(file, &index);

    let mut reader = Reader::over(path, Mem { file, at: 0 }, file.len() as u64);
    let mut work = WORK;
    for e in &index.entries {
        let columns = index.schemas[e.schema_id as usize].len();
        let header = match reader.header(e, columns) {
            Ok(header) => header,
            Err(err) => {
                assert!(!kept, "a row group the walk kept has no header: {err}");
                refused(&err, file.len());
                continue;
            }
        };
        let from = e.offset as usize;
        let area = from + e.frame_len as usize;
        // A sealed read: the header a query plans from is a whole frame whose CRC holds.
        let (payload, _) = frame_at(file, from)
            .filter(|(_, end)| *end == area)
            .expect("a header that is not a whole frame with its CRC was read");
        hold_header(&header, payload, columns);
        assert!(
            header.template_id == e.template_id
                && header.schema_id == e.schema_id
                && header.row_count == e.row_count
                && header.min_time == e.min_time
                && header.max_time == e.max_time,
            "a header that disagrees with its entry was read"
        );
        // What compaction does with a row group it cannot rebuild.
        match reader.copy(e, columns, RENUMBERED) {
            Ok(copied) => hold_copy(file, area, &header, payload, &copied),
            Err(err) => {
                assert!(!kept, "a row group the walk kept does not copy: {err}");
                refused(&err, file.len());
            }
        }
        for (at, (dir, (from, to, crc))) in header.chunks.iter().zip(spans(&header)).enumerate() {
            if weigh(header.row_count, &mut work).is_err() {
                return Some(index);
            }
            // What a query reads: the chunk as it is stored, checked and not decoded.
            let undecoded = match reader.stored(e, &header, &[at]) {
                Ok((chunks, read)) => {
                    assert_eq!(chunks.len(), 1, "one column was asked for");
                    assert_eq!(read, u64::from(dir.stored_len), "the bytes read");
                    let chunk = &chunks[0];
                    assert!(
                        file.get(area + from..area + to) == Some(&chunk.bytes[..])
                            && chunk.at == (area + from) as u64,
                        "a stored chunk is not the file's bytes where the header puts it"
                    );
                    assert_eq!(
                        frame::crc32c(&chunk.bytes),
                        crc,
                        "a chunk whose CRC does not hold was handed over"
                    );
                    chunks.into_iter().next()
                }
                Err(err) => {
                    refused(&err, file.len());
                    None
                }
            };
            let typed = undecoded.as_ref().map(|chunk| Chunk {
                dir,
                rows: header.row_count,
                stored: &chunk.bytes,
                at: chunk.at,
            });
            match reader.chunks(e, &header, &[at]) {
                Ok((cells, read)) => {
                    assert_eq!(cells.len(), 1, "one column was asked for");
                    assert_eq!(read, u64::from(dir.stored_len), "the bytes read");
                    let stored = &file[area + from..area + to];
                    // A sealed read: each `stored_crc` is checked before decoding.
                    assert_eq!(
                        frame::crc32c(stored),
                        crc,
                        "a chunk whose CRC does not hold was decoded"
                    );
                    hold_cells(dir, header.row_count, stored, &cells[0]);
                    // The two readers check the same bytes the same way before either
                    // decodes, so a chunk one of them reads the other hands over.
                    let Some(typed) = &typed else {
                        panic!("a chunk the reader decoded was not handed over undecoded");
                    };
                    hold_column(typed, file.len(), Some(&cells[0]));
                }
                Err(err) => {
                    assert!(!kept, "a chunk the walk kept does not read: {err}");
                    refused(&err, file.len());
                    // Refused for its bytes, and then neither reader hands it over; or
                    // for what they decode to, and then the typed decoder refuses it too.
                    if let Some(typed) = &typed {
                        hold_column(typed, file.len(), None);
                    }
                }
            }
        }
    }
    Some(index)
}

/// A row group as `Reader::copy` gave it: the header frame with its schema renumbered and
/// its CRC put right, and the chunk area the header measures, byte for byte.
fn hold_copy(file: &[u8], area: usize, header: &Header, payload: &[u8], copied: &RowGroup) {
    let mut renumbered = payload.to_vec();
    renumbered[9..13].copy_from_slice(&RENUMBERED.to_le_bytes());
    assert!(
        copied.frame == frame::frame(&renumbered),
        "a copied header is not the header renumbered"
    );
    let chunks = area
        .checked_add(header.chunks_len as usize)
        .and_then(|end| file.get(area..end));
    assert!(
        chunks == Some(&copied.chunks[..]),
        "a copied chunk area is not the {} byte(s) after the header",
        header.chunks_len
    );
    assert!(
        copied.row_count == header.row_count
            && copied.min_time == header.min_time
            && copied.max_time == header.max_time,
        "a copy that counts or dates its rows differently"
    );
}

/// An index the sealed open gave, held to the footer's rules: the footer is the
/// one spelling of the index, and every entry lies between header and footer, in file order.
fn hold_index(file: &[u8], index: &Index) {
    assert!(
        file.len() >= HEADER_LEN + TRAILER_LEN,
        "sealed, and too short"
    );
    let trailer = file.len() - TRAILER_LEN;
    assert_eq!(file[trailer + 4..], MAGIC, "sealed with no trailer");
    let footer_len = u32_at(file, trailer) as usize;
    let start = trailer
        .checked_sub(footer_len)
        .filter(|start| *start >= HEADER_LEN)
        .expect("a footer that starts before the header ends");
    let (payload, end) = frame_at(file, start).expect("a footer that is not a whole frame");
    assert!(
        end == trailer && payload[0] == KIND_FOOTER,
        "a footer of another shape"
    );
    assert_eq!(
        index.file_id[..],
        file[8..HEADER_LEN],
        "the file id is not the header's"
    );

    let spelled = index.seal_bytes();
    let canon = &spelled[frame::HEADER..spelled.len() - TRAILER_LEN - 4];
    assert!(
        payload.starts_with(canon),
        "an accepted footer has another spelling"
    );
    let mut r = Bytes::new(&payload[canon.len()..], 0);
    let parts = (|| {
        for _ in 0..r.u32("part_count")? {
            let kind = r.u8("part kind")?;
            let len = r.u32("part len")? as usize;
            assert!(kind & OPTIONAL != 0, "a required footer part 0x{kind:02x}");
            r.take(len, "part")?;
        }
        r.done("the footer")
    })();
    assert!(
        parts.is_ok(),
        "an accepted footer's parts do not use it up: {parts:?}"
    );

    let mut next = start as u64;
    for e in index.entries.iter().rev() {
        let area = e.offset.checked_add(u64::from(e.frame_len));
        assert!(
            e.offset >= HEADER_LEN as u64 && area.is_some_and(|area| area <= next),
            "the index lists a row group at {} of {} bytes, where {next} is the next thing",
            e.offset,
            e.frame_len
        );
        let room = next - e.offset - u64::from(e.frame_len);
        assert_eq!(
            e.chunks_len,
            u32::try_from(room).unwrap_or(u32::MAX),
            "a chunk area that is not the bytes up to the next row group"
        );
        assert!(
            e.frame_len as usize > frame::HEADER,
            "an entry with no frame"
        );
        assert!(
            (e.schema_id as usize) < index.schemas.len(),
            "an entry of no schema"
        );
        assert!(
            (1..=MAX_ROWS).contains(&e.row_count),
            "an entry of {} rows",
            e.row_count
        );
        assert!(
            e.min_time <= e.max_time
                && e.min_time.div_euclid(DAY_NS) == e.max_time.div_euclid(DAY_NS),
            "an entry whose time range is not ordered within one UTC day"
        );
        next = e.offset;
    }
}

/// What recovery sealed is what the walk kept. A chunk area's length is not in a footer,
/// so an index opened through one measures it up to the next row group, never less.
fn same_rows(sealed: Option<Index>, walked: &Index) {
    let sealed = sealed.expect("a file recovery sealed opens as sealed");
    assert!(
        sealed.entries.len() == walked.entries.len()
            && sealed
                .entries
                .iter()
                .zip(&walked.entries)
                .all(|(s, w)| s.chunks_len >= w.chunks_len
                    && Entry {
                        chunks_len: 0,
                        ..*s
                    } == Entry {
                        chunks_len: 0,
                        ..*w
                    }),
        "recovery sealed other row groups than the walk kept"
    );
    assert!(
        sealed.schemas == walked.schemas
            && sealed.file_id == walked.file_id
            && !sealed.finished
            && sealed.replaced.is_empty(),
        "recovery sealed another file than the walk kept"
    );
}

// ── A file from instructions ───────────────────────────────────────────────────────

/// The most columns one built row group has, and the most sections.
const CHUNKS: usize = 32;
const SECTIONS: usize = 4;

/// The row group header between its kind and `chunks_len`: template id, schema id, row
/// count, the two times, the source and its three numbers.
const FIXED: usize = 8 + 4 + 4 + 8 + 8 + 1 + 8 + 8 + 8;

/// The instructions, read front to back. Past the end every number is 0 and every run of
/// bytes is short, so an input cut anywhere still builds.
struct Feed<'a> {
    bytes: &'a [u8],
}

impl<'a> Feed<'a> {
    fn next(&mut self) -> Option<u8> {
        let (first, rest) = self.bytes.split_first()?;
        self.bytes = rest;
        Some(*first)
    }

    fn byte(&mut self) -> u8 {
        self.next().unwrap_or(0)
    }

    fn take(&mut self, n: usize) -> &'a [u8] {
        let (taken, rest) = self.bytes.split_at(n.min(self.bytes.len()));
        self.bytes = rest;
        taken
    }

    /// A length as two bytes, and that many bytes.
    fn run(&mut self) -> &'a [u8] {
        let len = u16::from_le_bytes([self.byte(), self.byte()]);
        self.take(usize::from(len))
    }
}

/// A number the builder worked out, or that number with one bit turned when the knob
/// names this field: its low two bits pick the field, the five above them the bit.
fn knob(knob: u8, field: u8, honest: u32) -> u32 {
    if knob & 0b11 == field {
        honest ^ (1 << ((knob >> 2) & 31))
    } else {
        honest
    }
}

/// A v3 file from instructions: a header, then whatever each instruction appends. The seal
/// is the footer of what was built, kept or not, so a changed row group still has an entry.
fn build(feed: &[u8]) -> Vec<u8> {
    let mut feed = Feed { bytes: feed };
    let mut out = file::header_bytes(&FILE_ID).to_vec();
    // The schema records and row groups the file holds so far, and how long its last
    // frame was.
    let mut index = Index::new(FILE_ID);
    let mut last = 0usize;
    while let Some(op) = feed.next() {
        if out.len() > MAX_FILE {
            break;
        }
        match op % 8 {
            0 | 4 => {
                let payload = feed.run();
                if payload.first() == Some(&KIND_SCHEMA) {
                    index.schemas.push(columns_of(payload));
                }
                last = frame::HEADER + payload.len();
                out.extend(frame::frame(payload));
            }
            1 | 5 => last = group(&mut feed, &mut out, &mut index),
            2 => {
                out.extend_from_slice(&(last as u32).to_le_bytes());
                out.extend_from_slice(&MAGIC);
            }
            6 => {
                index.finished = feed.byte() & 1 != 0;
                let seal = index.seal_bytes();
                last = seal.len() - TRAILER_LEN;
                out.extend(seal);
            }
            _ => out.extend_from_slice(feed.run()),
        }
    }
    out
}

/// The names a schema record given as bytes holds, for the footer a seal writes; none
/// when the record does not read, which keeps the records after it at their numbers.
fn columns_of(payload: &[u8]) -> Vec<String> {
    let mut r = Bytes::new(&payload[1..], 0);
    r.u32("schema_id")
        .and_then(|_| file::schema_columns(&mut r))
        .unwrap_or_default()
}

/// One row group: its header frame and its chunk area, every length and checksum worked
/// out from the bytes given, or made to lie by a knob. Returns the header frame's length.
fn group(feed: &mut Feed<'_>, out: &mut Vec<u8>, index: &mut Index) -> usize {
    let flags = feed.byte();
    let mut fixed = [0u8; FIXED];
    let given = feed.take(FIXED);
    fixed[..given.len()].copy_from_slice(given);
    let columns = usize::from(feed.byte()) % (CHUNKS + 1);
    if flags & 1 != 0 {
        let id = index.schemas.len() as u32;
        let names: Vec<String> = (0..columns).map(|n| format!("c{n:02}")).collect();
        let mut record = vec![KIND_SCHEMA];
        record.extend_from_slice(&id.to_le_bytes());
        record.extend_from_slice(&(columns as u32).to_le_bytes());
        for name in &names {
            record.extend_from_slice(&3u32.to_le_bytes());
            record.extend_from_slice(name.as_bytes());
        }
        out.extend(frame::frame(&record));
        fixed[8..12].copy_from_slice(&id.to_le_bytes());
        index.schemas.push(names);
    }

    let mut directory = Vec::new();
    let mut area = Vec::new();
    // What the directory's lengths add up to, lies included.
    let mut listed_len = 0u32;
    for _ in 0..columns {
        let layout = feed.byte();
        let compression = feed.byte();
        let turn = feed.byte();
        let pieces_len = usize::from(feed.byte()) % 32;
        let pieces = feed.take(pieces_len);
        let body = feed.run();
        let squeezed = (compression == ZSTD && turn & 0x80 != 0)
            .then(|| zstd::bulk::compress(body, 1).ok())
            .flatten();
        let (stored, body_len) = match &squeezed {
            Some(frame) => (&frame[..], body.len()),
            // Bytes given as they are stored: a zstd frame says what it holds, and
            // anything else holds what it is.
            None => match zstd::zstd_safe::get_frame_content_size(body) {
                Ok(Some(size)) => (body, usize::try_from(size).unwrap_or(usize::MAX)),
                _ => (body, body.len()),
            },
        };
        let stored_len = knob(turn, 1, stored.len() as u32);
        listed_len = listed_len.wrapping_add(stored_len);
        directory.push(layout);
        directory.push(compression);
        directory.extend_from_slice(&stored_len.to_le_bytes());
        directory.extend_from_slice(&knob(turn, 2, frame::crc32c(stored)).to_le_bytes());
        if compression != NO_COMPRESSION {
            let body_len = u32::try_from(body_len).unwrap_or(u32::MAX);
            directory.extend_from_slice(&knob(turn, 3, body_len).to_le_bytes());
        }
        directory.extend_from_slice(pieces);
        area.extend_from_slice(stored);
    }
    let sections = usize::from(feed.byte()) % (SECTIONS + 1);
    let mut listed = (sections as u16).to_le_bytes().to_vec();
    for _ in 0..sections {
        let kind = feed.byte();
        let column = feed.byte();
        let len = usize::from(feed.byte());
        let bytes = feed.take(len);
        listed.push(kind);
        listed.extend_from_slice(&u32::from(column).to_le_bytes());
        listed.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        listed.extend_from_slice(&frame::crc32c(bytes).to_le_bytes());
        area.extend_from_slice(bytes);
        listed_len = listed_len.wrapping_add(bytes.len() as u32);
    }
    let turn = feed.byte();

    let mut payload = vec![KIND_ROW_GROUP];
    payload.extend_from_slice(&fixed);
    payload.extend_from_slice(&knob(turn, 1, listed_len).to_le_bytes());
    payload.extend(directory);
    payload.extend(listed);
    let framed = frame::frame(&payload);
    let number = |at: usize| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&fixed[at..at + 8]);
        u64::from_le_bytes(b)
    };
    index.entries.push(Entry {
        template_id: number(0),
        offset: out.len() as u64,
        frame_len: framed.len() as u32,
        schema_id: u32_at(&fixed, 8),
        row_count: u32_at(&fixed, 12),
        min_time: number(16) as i64,
        max_time: number(24) as i64,
        chunks_len: 0,
    });
    out.extend_from_slice(&framed);
    out.extend(area);
    framed.len()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::core::flatten::parse_line;
    use crate::storage::db::row_of;
    use crate::storage::store::v3::build::{build as build_group, schema_of, Level};
    use crate::storage::store::v3::file::Writer;
    use crate::storage::store::v3::{put_varint, zigzag};
    use crate::storage::store::{effective_ts, Row};

    fn vector(name: &str) -> Vec<u8> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("v3")
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

    /// Writes the seed corpus: `cargo test --lib fuzzing::v3_segment -- --ignored`.
    #[test]
    #[ignore = "writes fuzz/corpus/v3_segment"]
    fn write_the_seed_corpus() {
        let dir: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fuzz")
            .join("corpus")
            .join("v3_segment");
        std::fs::create_dir_all(&dir).expect("mkdir");
        for (name, seed) in seeds() {
            std::fs::write(dir.join(name), seed).expect("write");
        }
    }
}
