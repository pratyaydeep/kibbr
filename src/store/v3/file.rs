//! A whole v3 file: writing it, sealing it, opening a sealed one through its footer,
//! walking an unsealed one frame by frame, and recovering it.
//!
//! **Sealed** means the file ends in a trailer whose footer frame checks out: whole, its
//! CRC holding, of kind 3, ending exactly where the trailer begins. Anything else is
//! **unsealed** - the file being written, or one a crash left - and is read by walking its
//! frames from byte 24. The walk stops at the first tear, the normal signature of a
//! process killed mid-write; recovery truncates there, appends the footer of what the walk
//! kept and seals the file.
//!
//! A frame whose CRC holds but whose contents break a rule is not a tear: those are the
//! bytes a writer wrote. It is named by path and offset and refused, because a writer bug
//! found at startup costs less than one found in an answer.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::build::RowGroup;
use super::read::{self, Cell, Header};
use super::{
    Bad, Bytes, FormatError, DAY_NS, HEADER_LEN, KIND_FOOTER, KIND_ROW_GROUP, KIND_SCHEMA, MAGIC,
    MAX_BODY, OPTIONAL, TRAILER_LEN, VERSION,
};
use crate::frame::{self, Next};

/// One row group, as the footer lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub template_id: u64,
    /// Of its header frame.
    pub offset: u64,
    /// 8 + the header frame's payload length; the chunk area starts at `offset +
    /// frame_len`.
    pub frame_len: u32,
    pub schema_id: u32,
    pub row_count: u32,
    pub min_time: i64,
    pub max_time: i64,
    /// The chunk area's length. Not in the footer, which does not need it: it is in the
    /// header frame, read before any chunk. Held here for the bytes `/stats` reports.
    pub chunks_len: u32,
}

/// What a file holds, as its footer says or as the walk found it: the same template
/// table and entries either way, which is why the open file's in-memory copy of this is
/// what queries on it read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub file_id: [u8; 16],
    pub schemas: Vec<Vec<String>>,
    /// In file order.
    pub entries: Vec<Entry>,
    /// Written whole by compaction or a bulk load. The live door never sets it.
    pub finished: bool,
    pub replaced: Vec<(String, [u8; 16])>,
}

impl Index {
    pub fn new(file_id: [u8; 16]) -> Self {
        Self {
            file_id,
            schemas: Vec::new(),
            entries: Vec::new(),
            finished: false,
            replaced: Vec::new(),
        }
    }

    pub fn row_count(&self) -> u64 {
        self.entries.iter().map(|e| u64::from(e.row_count)).sum()
    }

    /// `i64::MAX` when there are no rows: the minimum over nothing, which no window
    /// reaches, so an empty file needs no special case anywhere.
    pub fn min_time(&self) -> i64 {
        self.entries
            .iter()
            .map(|e| e.min_time)
            .min()
            .unwrap_or(i64::MAX)
    }

    pub fn max_time(&self) -> i64 {
        self.entries
            .iter()
            .map(|e| e.max_time)
            .max()
            .unwrap_or(i64::MIN)
    }

    /// The footer frame and the trailer that seal a file holding this.
    pub fn seal_bytes(&self) -> Vec<u8> {
        let mut p = vec![KIND_FOOTER, u8::from(self.finished)];
        p.extend_from_slice(&self.row_count().to_le_bytes());
        p.extend_from_slice(&self.min_time().to_le_bytes());
        p.extend_from_slice(&self.max_time().to_le_bytes());
        p.extend_from_slice(&(self.schemas.len() as u32).to_le_bytes());
        for schema in &self.schemas {
            put_schema(&mut p, schema);
        }
        p.extend_from_slice(&(self.replaced.len() as u32).to_le_bytes());
        for (path, id) in &self.replaced {
            p.extend_from_slice(&(path.len() as u16).to_le_bytes());
            p.extend_from_slice(path.as_bytes());
            p.extend_from_slice(id);
        }
        // Grouped by template, ascending; within a template in file order - a binary
        // search, then a contiguous run of fixed-size entries.
        let mut by_template: BTreeMap<u64, Vec<&Entry>> = BTreeMap::new();
        for e in &self.entries {
            by_template.entry(e.template_id).or_default().push(e);
        }
        p.extend_from_slice(&(by_template.len() as u32).to_le_bytes());
        let mut first = 0u32;
        for (id, entries) in &by_template {
            p.extend_from_slice(&id.to_le_bytes());
            p.extend_from_slice(&first.to_le_bytes());
            p.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            let min = entries.iter().map(|e| e.min_time).min().unwrap_or(i64::MAX);
            let max = entries.iter().map(|e| e.max_time).max().unwrap_or(i64::MIN);
            p.extend_from_slice(&min.to_le_bytes());
            p.extend_from_slice(&max.to_le_bytes());
            first += entries.len() as u32;
        }
        p.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entries in by_template.values() {
            for e in entries {
                p.extend_from_slice(&e.offset.to_le_bytes());
                p.extend_from_slice(&e.frame_len.to_le_bytes());
                p.extend_from_slice(&e.schema_id.to_le_bytes());
                p.extend_from_slice(&e.row_count.to_le_bytes());
                p.extend_from_slice(&e.min_time.to_le_bytes());
                p.extend_from_slice(&e.max_time.to_le_bytes());
            }
        }
        // No parts: none is assigned.
        p.extend_from_slice(&0u32.to_le_bytes());
        let mut out = frame::frame(&p);
        let footer_len = out.len() as u32;
        out.extend_from_slice(&footer_len.to_le_bytes());
        out.extend_from_slice(&MAGIC);
        out
    }
}

fn put_schema(out: &mut Vec<u8>, columns: &[String]) {
    out.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for name in columns {
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
    }
}

/// The 24-byte header.
pub fn header_bytes(file_id: &[u8; 16]) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[..4].copy_from_slice(&MAGIC);
    out[4] = VERSION;
    out[8..].copy_from_slice(file_id);
    out
}

/// A fresh file id from the operating system's random source. A path is a place, not an
/// identity: a day directory emptied by retention hands its file names out again, and an
/// id cannot come back.
pub fn new_file_id() -> std::io::Result<[u8; 16]> {
    let mut id = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut id)?;
    Ok(id)
}

/// The header checked: magic, version 3, zero reserved bytes. Its file id back.
pub fn check_header(path: &Path, bytes: &[u8]) -> Result<[u8; 16], FormatError> {
    let corrupt = |offset: u64, what: &str| FormatError::Corrupt {
        path: path.to_path_buf(),
        offset,
        what: what.to_string(),
    };
    if bytes.len() < HEADER_LEN {
        return Err(corrupt(0, "shorter than the 24-byte header"));
    }
    if bytes[..4] != MAGIC {
        return Err(corrupt(
            0,
            "not a TrieDB file - it does not start with TRDB",
        ));
    }
    if bytes[4] != VERSION {
        return Err(FormatError::Version {
            path: path.to_path_buf(),
            found: bytes[4],
        });
    }
    if bytes[5..8] != [0, 0, 0] {
        return Err(corrupt(5, "nonzero reserved header bytes"));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&bytes[8..HEADER_LEN]);
    Ok(id)
}

/// The version byte of a file that starts with the magic, without reading the rest -
/// what startup reads, before the port is taken, to refuse a directory holding another
/// version.
pub fn version_of(path: &Path) -> std::io::Result<Option<u8>> {
    let mut head = [0u8; 5];
    let mut file = File::open(path)?;
    let mut got = 0;
    while got < head.len() {
        match file.read(&mut head[got..])? {
            0 => break,
            n => got += n,
        }
    }
    Ok((got == head.len() && head[..4] == MAGIC).then_some(head[4]))
}

// ── The footer ─────────────────────────────────────────────────────────────────────

/// A footer frame's payload, every rule checked. `base` is its file offset.
pub fn footer(payload: &[u8], base: u64, file_id: [u8; 16]) -> Result<Index, Bad> {
    let mut r = Bytes::new(payload, base);
    let kind = r.u8("footer kind")?;
    if kind != KIND_FOOTER {
        return Err(Bad::at(base, format!("frame kind {kind} is not a footer")));
    }
    let at = r.offset();
    let flags = r.u8("footer flags")?;
    if flags & !1 != 0 {
        return Err(Bad::at(at, format!("footer flags 0x{flags:02x}")));
    }
    let totals_at = r.offset();
    let row_count = r.u64("footer row_count")?;
    let min_time = r.i64("footer min_time")?;
    let max_time = r.i64("footer max_time")?;
    let schema_count = r.u32("schema_count")?;
    let mut schemas = Vec::new();
    for _ in 0..schema_count {
        schemas.push(schema_columns(&mut r)?);
    }
    let replaced_count = r.u32("replaced_count")?;
    let mut replaced = Vec::new();
    for _ in 0..replaced_count {
        let len = r.u16("replaced path_len")? as usize;
        let at = r.offset();
        let raw = r.take(len, "replaced path")?;
        let path = String::from_utf8(raw.to_vec())
            .map_err(|_| Bad::at(at, "a replaced path is not UTF-8"))?;
        replaced.push((path, r.id("replaced file_id")?));
    }
    let template_count = r.u32("template_count")?;
    let mut templates = Vec::new();
    for _ in 0..template_count {
        let at = r.offset();
        let id = r.u64("template_id")?;
        let first = r.u32("template first")?;
        let count = r.u32("template count")?;
        let min = r.i64("template min_time")?;
        let max = r.i64("template max_time")?;
        templates.push((at, id, first, count, min, max));
    }
    let entry_count = r.u32("entry_count")?;
    let mut listed = Vec::new();
    for _ in 0..entry_count {
        let at = r.offset();
        let offset = r.u64("entry offset")?;
        let frame_len = r.u32("entry frame_len")?;
        let schema_id = r.u32("entry schema_id")?;
        let rows = r.u32("entry row_count")?;
        let min = r.i64("entry min_time")?;
        let max = r.i64("entry max_time")?;
        if rows == 0
            || min > max
            || min.div_euclid(DAY_NS) != max.div_euclid(DAY_NS)
            || schema_id >= schema_count
            || offset < HEADER_LEN as u64
            || (frame_len as usize) <= frame::HEADER
        {
            return Err(Bad::at(at, "a footer entry breaks a rule of its fields"));
        }
        listed.push((at, offset, frame_len, schema_id, rows, min, max));
    }
    let part_count = r.u32("part_count")?;
    for _ in 0..part_count {
        let at = r.offset();
        let kind = r.u8("footer part kind")?;
        let len = r.u32("footer part len")? as usize;
        if kind & OPTIONAL == 0 {
            return Err(Bad::at(
                at,
                format!(
                    "footer part kind 0x{kind:02x} is required and this build does not know it"
                ),
            ));
        }
        r.take(len, "footer part")?;
    }
    r.done("the footer")?;

    // The template table: ascending, distinct, back to back over every entry once, each
    // template's entries in file order and its range theirs.
    let mut entries = Vec::with_capacity(listed.len());
    let mut next = 0u32;
    let mut last_id = None;
    for (at, id, first, count, min, max) in templates {
        let end = first.checked_add(count).filter(|e| *e <= entry_count);
        if last_id.is_some_and(|l| id <= l) || first != next || count == 0 || end.is_none() {
            return Err(Bad::at(at, "the footer's template table breaks a rule"));
        }
        let run = &listed[first as usize..(first + count) as usize];
        if run.windows(2).any(|w| w[0].1 >= w[1].1)
            || run.iter().map(|e| e.5).min() != Some(min)
            || run.iter().map(|e| e.6).max() != Some(max)
        {
            return Err(Bad::at(
                at,
                format!("template {id}'s entries disagree with it"),
            ));
        }
        for (_, offset, frame_len, schema_id, rows, min, max) in run {
            entries.push(Entry {
                template_id: id,
                offset: *offset,
                frame_len: *frame_len,
                schema_id: *schema_id,
                row_count: *rows,
                min_time: *min,
                max_time: *max,
                chunks_len: 0,
            });
        }
        next = first + count;
        last_id = Some(id);
    }
    if next != entry_count {
        return Err(Bad::at(
            base,
            "the template table does not cover every entry",
        ));
    }
    entries.sort_by_key(|e| e.offset);
    if entries.windows(2).any(|w| w[0].offset == w[1].offset) {
        return Err(Bad::at(base, "two footer entries at one offset"));
    }
    let index = Index {
        file_id,
        schemas,
        entries,
        finished: flags & 1 == 1,
        replaced,
    };
    if index.row_count() != row_count
        || index.min_time() != min_time
        || index.max_time() != max_time
    {
        return Err(Bad::at(
            totals_at,
            "the footer's totals disagree with its entries",
        ));
    }
    Ok(index)
}

/// A schema's columns: sorted, distinct, UTF-8.
fn schema_columns(r: &mut Bytes<'_>) -> Result<Vec<String>, Bad> {
    let count = r.u32("column_count")?;
    let mut columns: Vec<String> = Vec::new();
    for _ in 0..count {
        let at = r.offset();
        let name = r.name("column name")?;
        if columns.last().is_some_and(|last| *last >= name) {
            return Err(Bad::at(
                at,
                format!("column `{name}` is not after the one before it"),
            ));
        }
        columns.push(name);
    }
    Ok(columns)
}

// ── Sealed files ───────────────────────────────────────────────────────────────────

/// The file's index through its footer, or `None` when it is not sealed. Three reads -
/// header, trailer, footer - and no row group.
pub fn open_sealed(path: &Path) -> Result<Option<Index>, FormatError> {
    let io = |source| FormatError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mut file = File::open(path).map_err(io)?;
    let len = file.metadata().map_err(io)?.len();
    // Shorter than a header and a trailer: a file cut before anything was sealed, which
    // recovery reads - or refuses, if its first bytes are not a header.
    if len < (HEADER_LEN + TRAILER_LEN) as u64 {
        return Ok(None);
    }
    let mut head = [0u8; HEADER_LEN];
    file.read_exact(&mut head).map_err(io)?;
    let file_id = check_header(path, &head)?;
    let mut trailer = [0u8; TRAILER_LEN];
    file.seek(SeekFrom::Start(len - TRAILER_LEN as u64))
        .map_err(io)?;
    file.read_exact(&mut trailer).map_err(io)?;
    if trailer[4..] != MAGIC {
        return Ok(None);
    }
    let footer_len = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    let room = len - (HEADER_LEN + TRAILER_LEN) as u64;
    if footer_len > room || footer_len as usize <= frame::HEADER {
        return Ok(None);
    }
    let start = len - TRAILER_LEN as u64 - footer_len;
    let mut bytes = vec![0u8; footer_len as usize];
    file.seek(SeekFrom::Start(start)).map_err(io)?;
    file.read_exact(&mut bytes).map_err(io)?;
    match frame::read(&bytes, 0, u32::MAX) {
        Next::Frame { payload, end } if end == bytes.len() && payload[0] == KIND_FOOTER => {
            let mut index = footer(payload, start + frame::HEADER as u64, file_id)
                .map_err(|bad| bad.in_file(path))?;
            // The footer does not carry a chunk area's length; the bytes up to the next
            // entry, or to the footer, bound it, and only `/stats` reads it.
            let mut next = start;
            for e in index.entries.iter_mut().rev() {
                let area = e.offset + u64::from(e.frame_len);
                if area > next {
                    return Err(FormatError::Corrupt {
                        path: path.to_path_buf(),
                        offset: e.offset,
                        what: "a footer entry overlaps the row group after it".into(),
                    });
                }
                e.chunks_len = u32::try_from(next - area).unwrap_or(u32::MAX);
                next = e.offset;
            }
            Ok(Some(index))
        }
        _ => Ok(None),
    }
}

// ── Unsealed files ─────────────────────────────────────────────────────────────────

/// What a walk kept, and where it stopped.
#[derive(Debug)]
pub struct Walk {
    pub index: Index,
    /// The first byte the walk did not keep: a tear, a footer frame, or the end.
    pub end: u64,
}

/// Walk an unsealed file's bytes from its header, frame by frame, every CRC checked -
/// the header frames', and after each row group's header its chunks' and sections'. The
/// walk stops at the first of: fewer than 8 bytes left; a `payload_len` of 0 or past the
/// end; a CRC that fails; a chunk area that runs past the end or whose CRCs fail; a footer
/// frame. Every chunk the walk keeps is also decoded, so a row group it keeps is one a
/// query can read.
pub fn walk(path: &Path, bytes: &[u8]) -> Result<Walk, FormatError> {
    let file_id = check_header(path, bytes)?;
    let mut index = Index::new(file_id);
    let mut at = HEADER_LEN;
    while let Next::Frame {
        payload,
        end: frame_end,
    } = frame::read(bytes, at, u32::MAX)
    {
        let base = (at + frame::HEADER) as u64;
        match payload[0] {
            KIND_SCHEMA => {
                let mut r = Bytes::new(payload, base);
                r.u8("kind").map_err(|b| b.in_file(path))?;
                let id_at = r.offset();
                let id = r.u32("schema_id").map_err(|b| b.in_file(path))?;
                if id as usize != index.schemas.len() {
                    return Err(Bad::at(
                        id_at,
                        format!("schema_id {id} where {} was next", index.schemas.len()),
                    )
                    .in_file(path));
                }
                let columns = schema_columns(&mut r).map_err(|b| b.in_file(path))?;
                r.done("a schema record").map_err(|b| b.in_file(path))?;
                index.schemas.push(columns);
                at = frame_end;
            }
            KIND_ROW_GROUP => {
                let header = read::header(payload, base, |id| {
                    index.schemas.get(id as usize).map(Vec::len)
                })
                .map_err(|b| b.in_file(path))?;
                let area = frame_end;
                let Some(area_end) = area
                    .checked_add(header.chunks_len as usize)
                    .filter(|e| *e <= bytes.len())
                else {
                    break;
                };
                if !chunks_intact(&header, &bytes[area..area_end]) {
                    break;
                }
                let columns = &index.schemas[header.schema_id as usize];
                check_chunks(
                    path,
                    &header,
                    &bytes[area..area_end],
                    area as u64,
                    columns.len(),
                )?;
                index.entries.push(Entry {
                    template_id: header.template_id,
                    offset: at as u64,
                    frame_len: (frame_end - at) as u32,
                    schema_id: header.schema_id,
                    row_count: header.row_count,
                    min_time: header.min_time,
                    max_time: header.max_time,
                    chunks_len: header.chunks_len,
                });
                at = area_end;
            }
            KIND_FOOTER => break,
            kind if kind & OPTIONAL != 0 => at = frame_end,
            kind => {
                return Err(Bad::at(
                    base,
                    format!("frame kind 0x{kind:02x} is required and this build does not know it"),
                )
                .in_file(path))
            }
        }
    }
    Ok(Walk {
        index,
        end: at as u64,
    })
}

/// Every chunk's and section's CRC over a chunk area that is all there.
fn chunks_intact(header: &Header, area: &[u8]) -> bool {
    let mut at = 0usize;
    let crcs = header
        .chunks
        .iter()
        .map(|c| (c.stored_len, c.stored_crc))
        .chain(header.sections.iter().map(|s| (s.len, s.crc)));
    for (len, crc) in crcs {
        let Some(bytes) = area.get(at..at + len as usize) else {
            return false;
        };
        if frame::crc32c(bytes) != crc {
            return false;
        }
        at += len as usize;
    }
    true
}

/// Decode every chunk of an intact chunk area. CRCs have held, so a failure here is a
/// writer's bug, and fatal.
fn check_chunks(
    path: &Path,
    header: &Header,
    area: &[u8],
    base: u64,
    _columns: usize,
) -> Result<(), FormatError> {
    let offsets = header.chunk_offsets();
    for (dir, off) in header.chunks.iter().zip(offsets) {
        let body = &area[off as usize..off as usize + dir.stored_len as usize];
        read::chunk(body, base + off, dir, header.row_count as usize)
            .map_err(|b| b.in_file(path))?;
    }
    Ok(())
}

/// What recovering a file did.
#[derive(Debug)]
pub struct Recovered {
    pub index: Index,
    /// Where the file was cut, and how long it was.
    pub torn_at: Option<(u64, u64)>,
}

/// Seal an unsealed file: walk it, truncate at the tear, append the footer of what the
/// walk kept - not finished, replacing nothing - and the trailer, and flush. A file that
/// ends inside its header holds no rows and becomes an empty sealed file with a new id.
pub fn recover(path: &Path) -> Result<Recovered, FormatError> {
    let io = |source| FormatError::Io {
        path: path.to_path_buf(),
        source,
    };
    let bytes = fs::read(path).map_err(io)?;
    let len = bytes.len() as u64;
    if bytes.len() < HEADER_LEN {
        let mut expect = MAGIC.to_vec();
        expect.extend_from_slice(&[VERSION, 0, 0, 0]);
        let n = bytes.len().min(expect.len());
        if bytes.len() > 4 && bytes[..4] == MAGIC && bytes[4] != VERSION {
            return Err(FormatError::Version {
                path: path.to_path_buf(),
                found: bytes[4],
            });
        }
        if bytes[..n] != expect[..n] {
            return Err(FormatError::Corrupt {
                path: path.to_path_buf(),
                offset: 0,
                what: "not a TrieDB file - it does not start with TRDB".into(),
            });
        }
        let index = Index::new(new_file_id().map_err(io)?);
        let mut out = header_bytes(&index.file_id).to_vec();
        out.extend(index.seal_bytes());
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(io)?;
        file.write_all(&out).map_err(io)?;
        file.sync_all().map_err(io)?;
        return Ok(Recovered {
            index,
            torn_at: Some((0, len)),
        });
    }
    let walked = walk(path, &bytes)?;
    let mut file = OpenOptions::new().write(true).open(path).map_err(io)?;
    file.set_len(walked.end).map_err(io)?;
    file.seek(SeekFrom::Start(walked.end)).map_err(io)?;
    file.write_all(&walked.index.seal_bytes()).map_err(io)?;
    file.sync_all().map_err(io)?;
    Ok(Recovered {
        torn_at: (walked.end < len).then_some((walked.end, len)),
        index: walked.index,
    })
}

// ── Writing ────────────────────────────────────────────────────────────────────────

/// A file being written: append only. A schema record goes before the first row group
/// that names it; row groups follow as they close; the footer and trailer go once, at
/// seal.
pub struct Writer {
    path: PathBuf,
    file: File,
    index: Index,
    len: u64,
    /// Each column set's schema id, found by its names.
    schema_ids: HashMap<Vec<String>, u32>,
    /// Set when an append failed and could not be cut back, so the tail is not a frame
    /// boundary. Nothing more is appended; the next start's recovery cuts it.
    torn: bool,
}

impl Writer {
    /// Create the file with its header, and make both the bytes and the directory entry
    /// durable before anything names it.
    pub fn create(path: &Path) -> Result<Self, FormatError> {
        let io = |source| FormatError::Io {
            path: path.to_path_buf(),
            source,
        };
        let file_id = new_file_id().map_err(io)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(io)?;
        let written = file
            .write_all(&header_bytes(&file_id))
            .and_then(|()| file.sync_all())
            .and_then(|()| frame::sync_dir(frame::parent_of(path)));
        if let Err(source) = written {
            drop(file);
            let _ = fs::remove_file(path);
            return Err(io(source));
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            index: Index::new(file_id),
            len: HEADER_LEN as u64,
            schema_ids: HashMap::new(),
            torn: false,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    /// Bytes are written whole or the file is cut back to where they began.
    fn write(&mut self, bytes: &[u8]) -> Result<(), FormatError> {
        if self.torn {
            return Err(FormatError::Io {
                path: self.path.clone(),
                source: std::io::Error::other(
                    "an earlier write could not be cut back; nothing more is appended until a \
                     restart recovers the file",
                ),
            });
        }
        let start = self.len;
        match self.file.write_all(bytes) {
            Ok(()) => {
                self.len += bytes.len() as u64;
                Ok(())
            }
            Err(source) => {
                let cut = self
                    .file
                    .set_len(start)
                    .and_then(|()| self.file.seek(SeekFrom::Start(start)).map(|_| ()));
                if cut.is_err() {
                    self.torn = true;
                }
                Err(FormatError::Io {
                    path: self.path.clone(),
                    source,
                })
            }
        }
    }

    /// The schema id for this column set, writing its record first if the file has none.
    pub fn schema_id(&mut self, columns: &[String]) -> Result<u32, FormatError> {
        if let Some(id) = self.schema_ids.get(columns) {
            return Ok(*id);
        }
        let id = self.index.schemas.len() as u32;
        let mut payload = vec![KIND_SCHEMA];
        payload.extend_from_slice(&id.to_le_bytes());
        put_schema(&mut payload, columns);
        self.write(&frame::frame(&payload))?;
        self.index.schemas.push(columns.to_vec());
        self.schema_ids.insert(columns.to_vec(), id);
        Ok(id)
    }

    /// Append a row group built against a schema id from `schema_id`. Not flushed: the
    /// caller appends every row group that is due and then calls `sync` once.
    pub fn append(&mut self, template_id: u64, rg: &RowGroup) -> Result<Entry, FormatError> {
        let offset = self.len;
        let mut bytes = Vec::with_capacity(rg.frame.len() + rg.chunks.len());
        bytes.extend_from_slice(&rg.frame);
        bytes.extend_from_slice(&rg.chunks);
        self.write(&bytes)?;
        let schema_id = u32::from_le_bytes([
            rg.frame[frame::HEADER + 9],
            rg.frame[frame::HEADER + 10],
            rg.frame[frame::HEADER + 11],
            rg.frame[frame::HEADER + 12],
        ]);
        let entry = Entry {
            template_id,
            offset,
            frame_len: rg.frame.len() as u32,
            schema_id,
            row_count: rg.row_count,
            min_time: rg.min_time,
            max_time: rg.max_time,
            chunks_len: rg.chunks.len() as u32,
        };
        self.index.entries.push(entry);
        Ok(entry)
    }

    pub fn sync(&mut self) -> Result<(), FormatError> {
        self.file.sync_all().map_err(|source| FormatError::Io {
            path: self.path.clone(),
            source,
        })
    }

    /// Append the footer and trailer and flush. The file is sealed once this returns.
    pub fn seal(mut self) -> Result<Index, FormatError> {
        let bytes = self.index.seal_bytes();
        self.write(&bytes)?;
        self.sync()?;
        Ok(self.index)
    }
}

// ── Reading row groups ─────────────────────────────────────────────────────────────

/// A file open for reading row groups.
pub struct Reader {
    path: PathBuf,
    file: File,
}

impl Reader {
    pub fn open(path: &Path) -> Result<Self, FormatError> {
        let file = File::open(path).map_err(|source| FormatError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
        })
    }

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, FormatError> {
        let io = |source| FormatError::Io {
            path: self.path.clone(),
            source,
        };
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(offset)).map_err(io)?;
        self.file.read_exact(&mut buf).map_err(io)?;
        Ok(buf)
    }

    /// A row group's header frame, its CRC checked and its fields held to the entry that
    /// pointed at it.
    pub fn header(&mut self, entry: &Entry, columns: usize) -> Result<Header, FormatError> {
        let bytes = self.read_at(entry.offset, entry.frame_len as usize)?;
        let corrupt = |what: &str| FormatError::Corrupt {
            path: self.path.clone(),
            offset: entry.offset,
            what: what.to_string(),
        };
        let Next::Frame { payload, end } = frame::read(&bytes, 0, u32::MAX) else {
            return Err(corrupt("the row group's header frame does not check out"));
        };
        if end != bytes.len() {
            return Err(corrupt(
                "the header frame's length disagrees with the index",
            ));
        }
        let header = read::header(payload, entry.offset + frame::HEADER as u64, |id| {
            (id == entry.schema_id).then_some(columns)
        })
        .map_err(|b| b.in_file(&self.path))?;
        if header.template_id != entry.template_id
            || header.row_count != entry.row_count
            || header.min_time != entry.min_time
            || header.max_time != entry.max_time
        {
            return Err(corrupt("the row group's header disagrees with the index"));
        }
        Ok(header)
    }

    /// The chunks of the columns at `wanted`, each CRC-checked before it is decoded; and
    /// the chunk bytes read.
    pub fn chunks(
        &mut self,
        entry: &Entry,
        header: &Header,
        wanted: &[usize],
    ) -> Result<(Vec<Vec<Cell>>, u64), FormatError> {
        let area = entry.offset + u64::from(entry.frame_len);
        let offsets = header.chunk_offsets();
        let mut out = Vec::with_capacity(wanted.len());
        let mut read = 0u64;
        for at in wanted {
            let dir = &header.chunks[*at];
            let start = area + offsets[*at];
            if dir.stored_len > MAX_BODY {
                return Err(FormatError::Corrupt {
                    path: self.path.clone(),
                    offset: start,
                    what: format!("a chunk of {} bytes", dir.stored_len),
                });
            }
            let body = self.read_at(start, dir.stored_len as usize)?;
            read += body.len() as u64;
            if frame::crc32c(&body) != dir.stored_crc {
                return Err(FormatError::Corrupt {
                    path: self.path.clone(),
                    offset: start,
                    what: "a chunk's CRC does not hold".into(),
                });
            }
            out.push(
                read::chunk(&body, start, dir, header.row_count as usize)
                    .map_err(|b| b.in_file(&self.path))?,
            );
        }
        Ok((out, read))
    }
}
