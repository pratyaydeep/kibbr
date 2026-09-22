//! The `.col` segment format: **every column for a batch of rows in one file**, in row
//! groups, with an index footer at the end.
//!
//! ```text
//! Header (8 bytes)
//!   [4]  magic       'T' 'R' 'D' 'B' in that byte order
//!   [1]  version     2
//!   [3]  reserved    zero
//!
//! Row group - a complete stripe of every column, appended as the segment grows
//!   [4]  payload_len u32 - bytes following the crc field
//!   [4]  crc         u32 - CRC-32C over the next payload_len bytes
//!        ── payload ──
//!   [8]  row_count   u64
//!   [8]  min_ts      i64 - min of the timestamp column, i64::MIN if none
//!   [8]  max_ts      i64 - max, same sentinel
//!   [2]  column_count u16
//!        per column, in the segment's fixed column order:
//!          [2]  name_len  u16
//!          [N]  name      utf-8
//!          [1]  col_type  0=Str 1=I64 2=F64 3=DT 4=Bool
//!          [1]  dt_form   0=mixed 1=rfc3339 2=epoch_s 3=epoch_ms  (col_type=DT only)
//!          [1]  encoding  0 = plain with bitmap, 1 = plain, all rows present
//!          [4]  chunk_len u32
//!        then the column chunks back to back, same order
//!
//! Footer
//!   [8]  total_rows  u64
//!   [2]  column_count u16
//!        per column: [2] name_len [N] name [1] col_type [1] dt_form
//!   [4]  row_group_count u32
//!        per row group: [8] row_count [8] min_ts [8] max_ts
//!          per column: [8] offset u64 (absolute) [4] length u32
//!   [4]  footer_len  u32 - byte length of everything above in this footer
//!   [4]  magic       'T' 'R' 'D' 'B'
//! ```
//!
//! This replaces one file per (template, day, column). That layout's bytes were fine -
//! 0.52x of raw - but 200,000 lines became 246,484 files holding 58MB and costing 973MB of
//! disk, because a file takes a 4KB block minimum. **File count, not byte count, was the
//! problem.**
//!
//! Three decisions whose reasons outlive whoever wrote them:
//!
//! - **Nulls are a leading bitmap, never an inline sentinel.** The old sentinel was a
//!   leading `0xFF`, indistinguishable from the first byte of `-1`, of `255`, and of a
//!   255-byte string length - a silent wrong-answer bug no call site can guard against.
//!   The per-cell tag byte that papered over it goes with it: one bit per row, not eight.
//! - **Column names live in the row group *and* the footer.** A segment killed before its
//!   footer must still be salvageable, and types alone cannot rebuild a schema. Costs
//!   ~100 bytes per row group.
//! - **The footer goes last.** Offsets are unknown until the data is written, so a
//!   header-first layout must buffer every column or write a placeholder and seek back -
//!   and a crash between the two leaves a header pointing at offsets that were never
//!   written. That file looks valid and reads garbage. Footer-last, the file is valid
//!   exactly when its footer is complete, and a torn write shows in 8 bytes.
//!
//! Integers are little-endian through `to_le_bytes`, never a struct cast; floats go
//! through `to_bits`.

use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::types::{ColValue, DtForm};

/// Written as four bytes and not as a little-endian `u32` of the packed value - that puts
/// `BDRT` on disk and defeats the point of a magic you can spot in a hex dump.
pub const MAGIC: [u8; 4] = *b"TRDB";
pub const VERSION: u8 = 2;
/// magic + version + 3 reserved.
pub const HEADER_BYTES: u64 = 8;
/// `footer_len` + the trailing magic.
pub const TRAILER_BYTES: u64 = 8;
/// `payload_len` + `crc`.
pub const FRAME_BYTES: u64 = 8;

/// Plain cells behind a null bitmap.
pub const ENCODING_BITMAP: u8 = 0;
/// Plain cells, no bitmap at all, because every row has a value.
pub const ENCODING_ALL_PRESENT: u8 = 1;

/// The file name a segment takes inside its day directory. Lexical order is write order,
/// and numbering restarts in each day directory.
pub fn segment_name(seq: u32) -> String {
    format!("seg_{seq:06}.col")
}

/// The sequence number a segment file name carries, or `None` if it is not one of ours.
pub fn segment_seq(name: &str) -> Option<u32> {
    name.strip_prefix("seg_")?
        .strip_suffix(".col")?
        .parse()
        .ok()
}

// ── CRC-32C ────────────────────────────────────────────────────────────────────────

/// Castagnoli, reflected - the polynomial iSCSI and ext4 use.
///
/// Hand-rolled rather than depended on, as `key::fnv1a` is: twenty lines, checked against
/// the standard `"123456789"` vector below, and one crate until it hurts.
const CRC_TABLE: [u32; 256] = crc_table();

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

pub fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for byte in bytes {
        crc = (crc >> 8) ^ CRC_TABLE[((crc ^ u32::from(*byte)) & 0xFF) as usize];
    }
    !crc
}

// ── Column kinds ───────────────────────────────────────────────────────────────────

/// The five cell encodings. Narrower than `ColumnType`: `Uuid` and `Ip` decode to
/// `ColValue::String` and are stored as one, because the ladder's finer types are a
/// statement about the *column* and the file only has to hold the *value*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColKind {
    Str,
    I64,
    F64,
    Dt,
    Bool,
}

impl ColKind {
    pub fn byte(self) -> u8 {
        match self {
            ColKind::Str => 0,
            ColKind::I64 => 1,
            ColKind::F64 => 2,
            ColKind::Dt => 3,
            ColKind::Bool => 4,
        }
    }

    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(ColKind::Str),
            1 => Some(ColKind::I64),
            2 => Some(ColKind::F64),
            3 => Some(ColKind::Dt),
            4 => Some(ColKind::Bool),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ColKind::Str => "Str",
            ColKind::I64 => "I64",
            ColKind::F64 => "F64",
            ColKind::Dt => "DT",
            ColKind::Bool => "Bool",
        }
    }

    /// `None` for `ColValue::Null`: a null claims nothing about the column, here as in
    /// the type lattice.
    pub fn of(value: &ColValue) -> Option<Self> {
        match value {
            ColValue::String(_) => Some(ColKind::Str),
            ColValue::Int64(_) => Some(ColKind::I64),
            ColValue::Float64(_) => Some(ColKind::F64),
            ColValue::Datetime(_) => Some(ColKind::Dt),
            ColValue::Bool(_) => Some(ColKind::Bool),
            ColValue::Null => None,
        }
    }

    /// The storage half of `types::join`, over the five things a file can hold.
    ///
    /// The same lattice and for the same reason: a column never narrows, so a column
    /// promotes at most twice in its life (`I64 → F64 → Str`) and never back.
    pub fn join(a: ColKind, b: ColKind) -> ColKind {
        match (a, b) {
            _ if a == b => a,
            (ColKind::I64, ColKind::F64) | (ColKind::F64, ColKind::I64) => ColKind::F64,
            _ => ColKind::Str,
        }
    }

    /// Bytes one non-null cell costs, or `None` for the variable-width one.
    fn width(self) -> Option<usize> {
        match self {
            ColKind::Str => None,
            ColKind::I64 | ColKind::F64 | ColKind::Dt => Some(8),
            ColKind::Bool => Some(1),
        }
    }
}

/// One value widened into `to`. Only the moves the lattice allows ever arrive here.
///
/// `DT → Str` is the case the `dt_form` byte exists for - it inverts the rung that read
/// the value instead of rendering a canonical timestamp, so a ten-digit order id that
/// rung 2 mistook for a date comes back as ten digits. See `DtForm::render`.
pub fn coerce(value: ColValue, to: ColKind, dt_form: Option<DtForm>) -> ColValue {
    match (to, value) {
        (_, ColValue::Null) => ColValue::Null,
        (ColKind::F64, ColValue::Int64(i)) => ColValue::Float64(i as f64),
        (ColKind::Str, ColValue::Int64(i)) => ColValue::String(i.to_string()),
        (ColKind::Str, ColValue::Float64(f)) => ColValue::String(f.to_string()),
        (ColKind::Str, ColValue::Bool(b)) => ColValue::String(b.to_string()),
        (ColKind::Str, ColValue::Datetime(ns)) => {
            ColValue::String(dt_form.unwrap_or(DtForm::Mixed).render(ns))
        }
        (_, value) => value,
    }
}

// ── The index ──────────────────────────────────────────────────────────────────────

/// One column, fixed for the life of a segment.
///
/// **The footer is what fixes it:** it names one `col_type` and one `dt_form` per column
/// for the whole segment, and the read path locates chunks from the footer alone, so every
/// row group must agree with it. A column whose type widens, or whose `dt_form` changes,
/// seals the segment and starts the next one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub kind: ColKind,
    /// `Some` only for `ColKind::Dt`. See `types::DtForm`.
    pub dt_form: Option<DtForm>,
}

impl ColumnSpec {
    fn dt_byte(&self) -> u8 {
        match self.kind {
            ColKind::Dt => self.dt_form.unwrap_or(DtForm::Mixed).byte(),
            _ => 0,
        }
    }

    /// Bytes this column costs in a row group header.
    fn header_bytes(&self) -> usize {
        2 + self.name.len() + 1 + 1 + 1 + 4
    }
}

/// Where one column's cells for one row group live.
#[derive(Debug, Clone, Copy)]
pub struct Chunk {
    /// Absolute file offset. A reader needs no arithmetic to find it.
    pub offset: u64,
    pub len: u32,
    /// Read out of the row group header at open time, not out of the footer - the footer
    /// carries offsets and lengths only. See `SegmentIndex::open`.
    pub encoding: u8,
}

#[derive(Debug, Clone)]
pub struct RowGroup {
    pub row_count: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    /// One per column, in the segment's fixed column order.
    pub chunks: Vec<Chunk>,
}

impl RowGroup {
    /// The timestamps in this row group, or `None` where it holds none.
    ///
    /// `i64::MIN` in both is the "no timestamp" sentinel, read back as unknown rather than
    /// as an instant: a row group that cannot be placed in time is never pruned away by a
    /// window, which is the conservative direction.
    pub fn span(&self) -> Option<(i64, i64)> {
        (self.min_ts != i64::MIN || self.max_ts != i64::MIN).then_some((self.min_ts, self.max_ts))
    }

    /// Does any row here fall inside `[since, until)`?
    pub fn overlaps(&self, since: Option<i64>, until: Option<i64>) -> bool {
        let Some((lo, hi)) = self.span() else {
            return true;
        };
        since.is_none_or(|from| hi >= from) && until.is_none_or(|to| lo < to)
    }
}

/// A segment's footer, parsed. Everything a query needs before it reads a byte of data.
#[derive(Debug, Clone)]
pub struct SegmentIndex {
    pub path: PathBuf,
    pub columns: Vec<ColumnSpec>,
    pub groups: Vec<RowGroup>,
    pub total_rows: u64,
    pub bytes: u64,
    /// Where the row group data ends and the footer begins.
    ///
    /// Distinct from `bytes`, which is the whole file: sealing a segment adds a footer,
    /// so the two differ by exactly its length. Recorded rather than derived because
    /// `reopen` has to truncate back to it, and the alternatives are both wrong on an
    /// edge - the last chunk's end is unknown for a row group with no columns, and
    /// re-reading `footer_len` means trusting a number the file has already been opened
    /// to check.
    pub data_end: u64,
}

impl SegmentIndex {
    pub fn column_at(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn open_file(&self) -> Result<File, SegError> {
        File::open(&self.path).map_err(|source| io(&self.path, source))
    }

    /// One chunk, decoded into one `ColValue` per row - `Null` where the bitmap says the
    /// row has no value.
    pub fn values(
        &self,
        file: &mut File,
        group: usize,
        column: usize,
    ) -> Result<Vec<ColValue>, SegError> {
        let rg = self.groups.get(group).ok_or_else(|| SegError::Truncated {
            path: self.path.clone(),
            what: "row group index",
            need: group as u64 + 1,
            have: self.groups.len() as u64,
        })?;
        let chunk = rg.chunks[column];
        let spec = &self.columns[column];

        // The length came out of the footer, and the footer is not to be trusted further
        // than the file it sits in: a chunk that claims to run past the end is a decode
        // error, never a read that walks off.
        let end = chunk.offset.saturating_add(u64::from(chunk.len));
        if end > self.bytes {
            return Err(SegError::Truncated {
                path: self.path.clone(),
                what: "column chunk",
                need: end,
                have: self.bytes,
            });
        }
        let mut raw = vec![0u8; chunk.len as usize];
        file.seek(SeekFrom::Start(chunk.offset))
            .map_err(|e| io(&self.path, e))?;
        file.read_exact(&mut raw).map_err(|e| io(&self.path, e))?;
        decode_chunk(&self.path, &raw, spec.kind, rg.row_count, chunk.encoding)
    }
}

// ── Errors ─────────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SegError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: not a TrieDB segment - it does not start with the four bytes TRDB")]
    NotASegment { path: PathBuf },
    #[error(
        "{path}: segment format version {found}; this build reads version 2 only, and there \
         is no upgrade path from the per-column layout"
    )]
    BadVersion { path: PathBuf, found: u8 },
    #[error("{path}: column type byte {found} is not one of 0..=4")]
    BadType { path: PathBuf, found: u8 },
    #[error("{path}: datetime form byte {found} is not one of 0..=3")]
    BadDtForm { path: PathBuf, found: u8 },
    #[error(
        "{path}: encoding byte {found} is reserved - only 0 (bitmap) and 1 (all rows \
         present) are defined, and a reserved value is rejected rather than ignored"
    )]
    BadEncoding { path: PathBuf, found: u8 },
    #[error("{path}: {what} wants {need} byte(s) and only {have} remain")]
    Truncated {
        path: PathBuf,
        what: &'static str,
        need: u64,
        have: u64,
    },
    #[error("{path}: a column name is not utf-8")]
    BadName { path: PathBuf },
    #[error("{path}: a string cell is not utf-8")]
    BadUtf8 { path: PathBuf },
    /// A value handed to a column whose type it is not. Not a fact about the data: the
    /// write path coerces every value into its column before it gets here, so this is
    /// that path having gone wrong. It costs the flush and the rows stay buffered.
    #[error(
        "{path}: column `{column}` is {kind} and was handed {value} - writing it would put \
         a cell on disk the footer does not describe"
    )]
    CellType {
        path: PathBuf,
        column: String,
        kind: &'static str,
        value: String,
    },
    /// Unreachable while a line is capped at 64 MiB by `lines::MAX_LINE_BYTES`; an error
    /// rather than a null cell, because a dropped cell nobody is told about is the thing
    /// this format stopped doing.
    #[error(
        "{path}: column `{column}` was handed a {len} byte string, past the u32 its length \
         prefix is written as"
    )]
    CellTooLong {
        path: PathBuf,
        column: String,
        len: usize,
    },
    #[error("{path}: column `{column}` has {given} cell(s) in a row group of {expected} row(s)")]
    ChunkLength {
        path: PathBuf,
        column: String,
        given: usize,
        expected: u64,
    },
    #[error("{path}: a row group of {given} column(s) in a segment of {expected}")]
    ColumnCount {
        path: PathBuf,
        given: usize,
        expected: usize,
    },
    #[error("{path}: a row group header of {wrote} byte(s) where {expected} was reserved")]
    HeaderLength {
        path: PathBuf,
        wrote: usize,
        expected: usize,
    },
    #[error(
        "{path}: row group {group} describes column {column} as {found}, and the footer \
         says {expected} - the segment is corrupt"
    )]
    FooterDisagrees {
        path: PathBuf,
        group: usize,
        column: usize,
        found: String,
        expected: String,
    },
}

fn io(path: &Path, source: std::io::Error) -> SegError {
    SegError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn short(path: &Path, what: &'static str, need: u64, have: u64) -> SegError {
    SegError::Truncated {
        path: path.to_path_buf(),
        what,
        need,
        have,
    }
}

// ── Reading bytes without running past the end ─────────────────────────────────────

/// A cursor over a byte region that cannot over-read.
///
/// **Every length prefix is checked against the bytes remaining before anything is
/// allocated:** a corrupt `u32` of `0xFFFFFFFF` is a decode error, never a 4GB allocation.
/// Hence a slice and not a `Read` - a stream can be asked for four gigabytes and will try.
struct Cursor<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, at: 0 }
    }

    fn left(&self) -> u64 {
        (self.buf.len() - self.at) as u64
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let slice = self.buf.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u16(&mut self) -> Option<u16> {
        self.take(2)
            .map(|b| u16::from_le_bytes(b.try_into().unwrap_or([0; 2])))
    }

    fn u32(&mut self) -> Option<u32> {
        self.take(4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap_or([0; 4])))
    }

    fn u64(&mut self) -> Option<u64> {
        self.take(8)
            .map(|b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
    }

    fn i64(&mut self) -> Option<i64> {
        self.take(8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
    }
}

// ── Chunks ─────────────────────────────────────────────────────────────────────────

/// Whether one cell's bytes went into `out`, or the mismatch that stopped them.
///
/// **A value whose type is not the column's is an error and not a skipped cell.** The
/// presence bitmap is built from the values and the bytes from this, so a mismatch that
/// merely wrote nothing left the column marked present with no cell behind it - every
/// later cell in that chunk shifted by its width, and every row in it read back a
/// neighbour's value. That was a debug assertion, which is to say it was nothing at all
/// in the build that ships. The caller coerces before it gets here, so
/// reaching this is a bug in the caller rather than in the data; it costs the flush, the
/// rows stay buffered, and the next tick retries.
fn cell_bytes(value: &ColValue, kind: ColKind, out: &mut Vec<u8>) -> Result<bool, CellMismatch> {
    Ok(match (kind, value) {
        (_, ColValue::Null) => false,
        // Unreachable: the prefix is a u32 and a line is capped well below 4 GiB
        // (`lines::MAX_LINE_BYTES`). An error rather than truncation *or* a null - half a
        // string is worse than none and would split a character besides, and a null here
        // is the silent dropped cell this whole change is about.
        (ColKind::Str, ColValue::String(s)) => match u32::try_from(s.len()) {
            Ok(len) => {
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(s.as_bytes());
                true
            }
            Err(_) => return Err(CellMismatch::TooLong { len: s.len() }),
        },
        (ColKind::I64, ColValue::Int64(i)) => {
            out.extend_from_slice(&i.to_le_bytes());
            true
        }
        // Through `to_bits`, not a transmute of the value.
        (ColKind::F64, ColValue::Float64(f)) => {
            out.extend_from_slice(&f.to_bits().to_le_bytes());
            true
        }
        (ColKind::Dt, ColValue::Datetime(ns)) => {
            out.extend_from_slice(&ns.to_le_bytes());
            true
        }
        (ColKind::Bool, ColValue::Bool(b)) => {
            out.push(u8::from(*b));
            true
        }
        (kind, value) => {
            return Err(CellMismatch::Type {
                kind,
                value: rendered(value),
            })
        }
    })
}

/// A value for an error message: its debug form, cut short.
///
/// Cut because the value that got here may be the 64 MiB stack trace in a JSON field that
/// this format exists to store, and an error message is not somewhere to put one.
fn rendered(value: &ColValue) -> String {
    const MAX: usize = 120;
    let text = format!("{value:?}");
    match text.char_indices().nth(MAX) {
        Some((at, _)) => format!("{}…", &text[..at]),
        None => text,
    }
}

/// A cell whose type is not its column's, on its way up to the caller that knows which
/// file and which column it was going into.
///
/// A struct rather than a `SegError` because `encode_chunk` encodes bytes and has no
/// path: naming the file is `append_row_group`'s job, and threading a `&Path` down here
/// to build an error that is never constructed would be a parameter on every caller for
/// a case that must not happen.
#[derive(Debug)]
pub enum CellMismatch {
    /// A value whose type is not the column's.
    Type { kind: ColKind, value: String },
    /// A string past the `u32` its length prefix is written as.
    TooLong { len: usize },
}

/// Encode one column's cells for one row group.
///
/// **Encoding 1 when every row has a value** - which `ts`, `level` and `user_id` usually
/// do: an all-ones 1,024-byte bitmap per column per row group is pure waste.
///
/// **The bitmap and the cells are built from one reading of the values.** They used to be
/// built from two - `ColKind::of(v).is_some()` for the bitmap and a match on `(kind,
/// value)` for the bytes - and the two disagreed for a value of the wrong type: present in
/// the bitmap, absent from the cells, and every cell after it in the chunk read back one
/// position out. `present` now asks the same question the cells answer.
pub fn encode_chunk(values: &[ColValue], kind: ColKind) -> Result<(u8, Vec<u8>), CellMismatch> {
    let present: Vec<bool> = values.iter().map(|v| ColKind::of(v) == Some(kind)).collect();
    let all_present = present.iter().all(|p| *p);

    let mut out = Vec::with_capacity(values.len() * kind.width().unwrap_or(16));
    if !all_present {
        // Row r is byte r/8, bit r%8, **lowest bit within each byte is the earliest row**.
        // Backwards and every null moves.
        let mut bitmap = vec![0u8; values.len().div_ceil(8)];
        for (r, _) in present.iter().enumerate().filter(|(_, p)| **p) {
            bitmap[r / 8] |= 1 << (r % 8);
        }
        out.extend_from_slice(&bitmap);
    }
    for (value, is_present) in values.iter().zip(&present) {
        // The two readings have to agree cell for cell. They cannot disagree any more -
        // `cell_bytes` writes exactly when the type matches, which is what `present` asks -
        // and that is the point: the bug was the two of them disagreeing silently.
        if cell_bytes(value, kind, &mut out)? != *is_present {
            return Err(CellMismatch::Type {
                kind,
                value: rendered(value),
            });
        }
    }
    Ok((
        if all_present {
            ENCODING_ALL_PRESENT
        } else {
            ENCODING_BITMAP
        },
        out,
    ))
}

/// Decode one chunk back into one `ColValue` per row.
pub fn decode_chunk(
    path: &Path,
    raw: &[u8],
    kind: ColKind,
    row_count: u64,
    encoding: u8,
) -> Result<Vec<ColValue>, SegError> {
    // Reserved means rejected, not ignored: a chunk written by a future zstd or dictionary
    // encoding must not be read as if it were plain.
    if encoding != ENCODING_BITMAP && encoding != ENCODING_ALL_PRESENT {
        return Err(SegError::BadEncoding {
            path: path.to_path_buf(),
            found: encoding,
        });
    }
    let rows = usize::try_from(row_count).unwrap_or(usize::MAX);
    let mut cur = Cursor::new(raw);

    let present: Vec<bool> = if encoding == ENCODING_ALL_PRESENT {
        vec![true; rows]
    } else {
        let bitmap_len = rows.div_ceil(8);
        let bitmap = cur
            .take(bitmap_len)
            .ok_or_else(|| short(path, "null bitmap", bitmap_len as u64, cur.left()))?;
        (0..rows)
            .map(|r| bitmap[r / 8] & (1 << (r % 8)) != 0)
            .collect()
    };

    let mut out = Vec::with_capacity(rows);
    for is_present in present {
        if !is_present {
            out.push(ColValue::Null);
            continue;
        }
        let value = match kind {
            ColKind::Str => {
                let len = cur
                    .u32()
                    .ok_or_else(|| short(path, "string length", 4, cur.left()))?
                    as usize;
                let bytes = cur
                    .take(len)
                    .ok_or_else(|| short(path, "string cell", len as u64, cur.left()))?;
                ColValue::String(
                    std::str::from_utf8(bytes)
                        .map_err(|_| SegError::BadUtf8 {
                            path: path.to_path_buf(),
                        })?
                        .to_string(),
                )
            }
            ColKind::I64 => ColValue::Int64(
                cur.i64()
                    .ok_or_else(|| short(path, "i64 cell", 8, cur.left()))?,
            ),
            ColKind::F64 => ColValue::Float64(f64::from_bits(
                cur.u64()
                    .ok_or_else(|| short(path, "f64 cell", 8, cur.left()))?,
            )),
            ColKind::Dt => ColValue::Datetime(
                cur.i64()
                    .ok_or_else(|| short(path, "datetime cell", 8, cur.left()))?,
            ),
            ColKind::Bool => ColValue::Bool(
                cur.u8()
                    .ok_or_else(|| short(path, "bool cell", 1, cur.left()))?
                    != 0,
            ),
        };
        out.push(value);
    }
    Ok(out)
}

// ── Writing ────────────────────────────────────────────────────────────────────────

/// One segment open for append.
///
/// The index it is building is readable while it builds: a row group is recorded only
/// after it has been written and fsynced, so a query reading `index()` sees exactly the
/// rows that are on the platter.
pub struct SegmentWriter {
    index: SegmentIndex,
    file: File,
    /// Where the next row group frame begins - which is also how many bytes are written.
    at: u64,
}

/// A segment file that was already there when a new one was about to be created, and what
/// reopening it cost.
///
/// Reported rather than swallowed: the only ways one gets here are a previous process
/// killed between creating the file and writing its header, or a create that failed
/// part way on a build that did not clean up. Both are worth a line in the log.
#[derive(Debug)]
pub struct Reopened {
    pub rows: u64,
    pub groups: usize,
    pub dropped_bytes: u64,
    /// The file held no row group at all, so it was rewritten with the columns this run
    /// needs rather than appended to. An empty segment is not a fact about the data.
    pub was_empty: bool,
}

impl std::fmt::Display for Reopened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.was_empty {
            return write!(
                f,
                "it held no row group that could be read, and its {} byte(s) were \
                 rewritten as a fresh segment",
                self.dropped_bytes
            );
        }
        write!(
            f,
            "appending to the {} row(s) in {} row group(s) already in it; {} byte(s) past \
             the last good row group were dropped",
            self.rows, self.groups, self.dropped_bytes
        )
    }
}

impl SegmentWriter {
    /// Create the file, write the header, and fsync **the containing directory too** -
    /// otherwise the file's existence is not durable, whatever is inside it.
    ///
    /// **A create that fails part way leaves nothing behind.** `create_new` has made the
    /// file by the time the header is written, so a failure after that point - a full
    /// disk, a descriptor spent on the directory fsync - used to leave a zero-byte file
    /// that every later attempt at the same sequence number met with `File exists`. The
    /// rows behind it never left memory and a clean shutdown exited 0 without them
    /// The file is removed on the way out of every error path here, so a retry retries
    /// rather than repeating one failure forever.
    pub fn create(path: PathBuf, columns: Vec<ColumnSpec>) -> Result<Self, SegError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;

        // From here the file exists, so every failure has to take it away again.
        let mut write_header = || -> Result<(), SegError> {
            let mut header = [0u8; HEADER_BYTES as usize];
            header[..4].copy_from_slice(&MAGIC);
            header[4] = VERSION;
            // `write_all` is the loop on short writes: it retries until the buffer is gone
            // or the error is real.
            file.write_all(&header).map_err(|e| io(&path, e))?;
            file.sync_all().map_err(|e| io(&path, e))?;
            if let Some(dir) = path.parent() {
                sync_dir(dir).map_err(|e| io(dir, e))?;
            }
            Ok(())
        };
        if let Err(e) = write_header() {
            // Best effort, and it has to be: the disk that would not take eight bytes may
            // not take an unlink either. Reporting the original failure is what matters -
            // a second error about the cleanup would hide the one that caused it.
            let _ = std::fs::remove_file(&path);
            if let Some(dir) = path.parent() {
                let _ = sync_dir(dir);
            }
            return Err(e);
        }

        Ok(Self {
            index: SegmentIndex {
                path,
                columns,
                groups: Vec::new(),
                total_rows: 0,
                bytes: HEADER_BYTES,
                data_end: HEADER_BYTES,
            },
            file,
            at: HEADER_BYTES,
        })
    }

    /// Create the segment, or take over the one already there.
    ///
    /// The second case is the retry path. A file at the sequence number about to be used
    /// is opened through `open` - the same recovery every other reader gets, so whatever a
    /// crash left is truncated to the last row group whose CRC checks out - and then
    /// appended to. A file holding no row group is rewritten with the columns this run
    /// needs: there is nothing in it to preserve, and sealing it empty would leave a file
    /// per retry.
    ///
    /// The columns of a reopened file are **its own**, not the ones passed in. The caller
    /// asks again what fits.
    pub fn create_or_reopen(
        path: PathBuf,
        columns: Vec<ColumnSpec>,
    ) -> Result<(Self, Option<Reopened>), SegError> {
        let exists = match Self::create(path.clone(), columns.clone()) {
            Ok(writer) => return Ok((writer, None)),
            Err(SegError::Io { source, .. }) if source.kind() == ErrorKind::AlreadyExists => source,
            Err(e) => return Err(e),
        };
        let _ = exists;

        let (index, opened) = open(&path)?;
        let dropped_bytes = match opened {
            Opened::Sealed => 0,
            Opened::Recovered { dropped_bytes, .. } => dropped_bytes,
        };
        let mut found = Reopened {
            rows: index.total_rows,
            groups: index.groups.len(),
            dropped_bytes,
            was_empty: index.groups.is_empty(),
        };
        if found.was_empty {
            // What recovery could not read. It has already truncated the file to its
            // header, so nothing is being thrown away here that was not thrown away
            // there - but the count belongs in the log either way.
            found.dropped_bytes = dropped_bytes.max(index.bytes.saturating_sub(HEADER_BYTES));
            std::fs::remove_file(&path).map_err(|e| io(&path, e))?;
            if let Some(dir) = path.parent() {
                sync_dir(dir).map_err(|e| io(dir, e))?;
            }
            found.rows = 0;
            found.groups = 0;
            return Ok((Self::create(path, columns)?, Some(found)));
        }
        Ok((Self::reopen(index)?, Some(found)))
    }

    /// Cut the footer back off a sealed index and carry on writing where its data ended.
    ///
    /// Only ever reached from `create_or_reopen`: `open` seals whatever it recovers, so
    /// there is always a footer here, and it is always the one this process just wrote.
    fn reopen(mut index: SegmentIndex) -> Result<Self, SegError> {
        let at = index.data_end;
        let path = index.path.clone();
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        file.set_len(at).map_err(|e| io(&path, e))?;
        file.seek(SeekFrom::Start(at)).map_err(|e| io(&path, e))?;
        file.sync_all().map_err(|e| io(&path, e))?;
        index.bytes = at;
        Ok(Self { index, file, at })
    }

    pub fn index(&self) -> &SegmentIndex {
        &self.index
    }

    pub fn columns(&self) -> &[ColumnSpec] {
        &self.index.columns
    }

    pub fn bytes(&self) -> u64 {
        self.at
    }

    pub fn rows(&self) -> u64 {
        self.index.total_rows
    }

    /// Append one row group: encode the chunks, write them, fsync.
    ///
    /// `columns` holds one `Vec<ColValue>` per column of the segment, in the segment's
    /// fixed column order, each the same length.
    pub fn append_row_group(
        &mut self,
        columns: &[Vec<ColValue>],
        row_count: u64,
        span: Option<(i64, i64)>,
    ) -> Result<(), SegError> {
        // A real check, because `zip` below would otherwise write whichever list is
        // shorter and leave a row group the footer describes wrongly - silently, in the
        // build that ships.
        if columns.len() != self.index.columns.len() {
            return Err(SegError::ColumnCount {
                path: self.index.path.clone(),
                given: columns.len(),
                expected: self.index.columns.len(),
            });
        }
        // Every chunk has to be as long as the row group says it is. A short one would
        // write a header claiming `row_count` rows over fewer cells, and the reader
        // decodes `row_count` of them - reading past the chunk into its neighbour, which
        // the CRC vouches for because the bytes on disk are exactly what was written.
        if let Some((at, values)) = columns
            .iter()
            .enumerate()
            .find(|(_, values)| values.len() as u64 != row_count)
        {
            return Err(SegError::ChunkLength {
                path: self.index.path.clone(),
                column: self.index.columns[at].name.clone(),
                given: values.len(),
                expected: row_count,
            });
        }
        let (min_ts, max_ts) = span.unwrap_or((i64::MIN, i64::MIN));

        let mut encoded = Vec::with_capacity(columns.len());
        for (values, spec) in columns.iter().zip(&self.index.columns) {
            let chunk = encode_chunk(values, spec.kind).map_err(|bad| match bad {
                CellMismatch::Type { kind, value } => SegError::CellType {
                    path: self.index.path.clone(),
                    column: spec.name.clone(),
                    kind: kind.as_str(),
                    value,
                },
                CellMismatch::TooLong { len } => SegError::CellTooLong {
                    path: self.index.path.clone(),
                    column: spec.name.clone(),
                    len,
                },
            })?;
            encoded.push(chunk);
        }

        let header_bytes: usize = 8
            + 8
            + 8
            + 2
            + self
                .index
                .columns
                .iter()
                .map(ColumnSpec::header_bytes)
                .sum::<usize>();
        let body_bytes: usize = encoded.iter().map(|(_, bytes)| bytes.len()).sum();

        let mut payload = Vec::with_capacity(header_bytes + body_bytes);
        payload.extend_from_slice(&row_count.to_le_bytes());
        payload.extend_from_slice(&min_ts.to_le_bytes());
        payload.extend_from_slice(&max_ts.to_le_bytes());
        payload.extend_from_slice(&(self.index.columns.len() as u16).to_le_bytes());
        for (spec, (encoding, bytes)) in self.index.columns.iter().zip(&encoded) {
            payload.extend_from_slice(&(spec.name.len() as u16).to_le_bytes());
            payload.extend_from_slice(spec.name.as_bytes());
            payload.push(spec.kind.byte());
            payload.push(spec.dt_byte());
            payload.push(*encoding);
            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        }
        // The offsets below are `header_bytes` past the frame, so the header's predicted
        // length and its written length have to be the same number. Checked rather than
        // asserted: a release build that got this wrong would write a footer pointing at
        // offsets nothing was written to, and the file would look valid and read garbage.
        if payload.len() != header_bytes {
            return Err(SegError::HeaderLength {
                path: self.index.path.clone(),
                wrote: payload.len(),
                expected: header_bytes,
            });
        }

        // Absolute offsets, recorded as the chunks go down, so the footer needs no
        // arithmetic later and a reader needs none at all.
        let mut chunks = Vec::with_capacity(encoded.len());
        let mut offset = self.at + FRAME_BYTES + header_bytes as u64;
        for (encoding, bytes) in &encoded {
            chunks.push(Chunk {
                offset,
                len: bytes.len() as u32,
                encoding: *encoding,
            });
            offset += bytes.len() as u64;
            payload.extend_from_slice(bytes);
        }

        let mut frame = Vec::with_capacity(FRAME_BYTES as usize + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32c(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);

        let path = self.index.path.clone();
        self.file.write_all(&frame).map_err(|e| io(&path, e))?;
        self.file.sync_all().map_err(|e| io(&path, e))?;

        self.at += frame.len() as u64;
        self.index.bytes = self.at;
        self.index.data_end = self.at;
        self.index.total_rows += row_count;
        self.index.groups.push(RowGroup {
            row_count,
            min_ts,
            max_ts,
            chunks,
        });
        Ok(())
    }

    /// Write the footer, fsync, close. A sealed file is never reopened for writing.
    ///
    /// **The index comes back whether or not the footer did.** The writer is consumed
    /// either way - the descriptor is gone and there is nothing left to retry with - so a
    /// `Result<SegmentIndex, _>` would hand the caller an error and take the description
    /// of every row group it had already fsynced with it. Those rows are on the platter;
    /// dropping the index makes them vanish from every query in this process while the
    /// partition's own count still includes them, which is a count that disagrees with
    /// its own rows and nothing reporting it.
    ///
    /// On failure the index describes the file up to `data_end` - the footerless form the
    /// next `open` recovers, walking the frames and sealing what survives.
    pub fn seal(mut self) -> (SegmentIndex, Option<SegError>) {
        let footer = encode_footer(&self.index);
        let path = self.index.path.clone();
        if let Err(e) = self.file.write_all(&footer) {
            return (self.index, Some(io(&path, e)));
        }
        if let Err(e) = self.file.sync_all() {
            return (self.index, Some(io(&path, e)));
        }
        self.index.bytes = self.at + footer.len() as u64;
        (self.index, None)
    }
}

fn encode_footer(index: &SegmentIndex) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&index.total_rows.to_le_bytes());
    out.extend_from_slice(&(index.columns.len() as u16).to_le_bytes());
    for spec in &index.columns {
        out.extend_from_slice(&(spec.name.len() as u16).to_le_bytes());
        out.extend_from_slice(spec.name.as_bytes());
        out.push(spec.kind.byte());
        out.push(spec.dt_byte());
    }
    out.extend_from_slice(&(index.groups.len() as u32).to_le_bytes());
    for group in &index.groups {
        out.extend_from_slice(&group.row_count.to_le_bytes());
        out.extend_from_slice(&group.min_ts.to_le_bytes());
        out.extend_from_slice(&group.max_ts.to_le_bytes());
        for chunk in &group.chunks {
            out.extend_from_slice(&chunk.offset.to_le_bytes());
            out.extend_from_slice(&chunk.len.to_le_bytes());
        }
    }
    let footer_len = out.len() as u32;
    out.extend_from_slice(&footer_len.to_le_bytes());
    out.extend_from_slice(&MAGIC);
    out
}

fn sync_dir(dir: &Path) -> Result<(), std::io::Error> {
    File::open(dir)?.sync_all()
}

// ── Opening, and recovering what was not sealed ────────────────────────────────────

/// What `open` had to do to the file on the way in.
#[derive(Debug)]
pub enum Opened {
    Sealed,
    /// A crash left the newest segment with no footer. The prefix that survived was
    /// sealed, and this says what it cost.
    Recovered {
        groups: usize,
        rows: u64,
        dropped_bytes: u64,
    },
}

impl std::fmt::Display for Opened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Opened::Sealed => write!(f, "sealed"),
            Opened::Recovered {
                groups,
                rows,
                dropped_bytes,
            } => write!(
                f,
                "recovered {groups} row group(s) holding {rows} row(s); {dropped_bytes} \
                 byte(s) past the last good row group were dropped"
            ),
        }
    }
}

/// Refuse a segment from another format version, reading its header and nothing else.
///
/// The startup check that runs before the server takes its port, so it only reads: a file
/// too short to hold a header, one that is not a segment, or one that cannot be read is
/// `Ok` here and left to `open`, which recovers it or reports it. A whole header naming
/// another version is the one thing `open` would refuse that the server must not start
/// over.
pub fn check_version(path: &Path) -> Result<(), SegError> {
    let mut header = [0u8; HEADER_BYTES as usize];
    let Ok(mut file) = File::open(path) else {
        return Ok(());
    };
    if file.read_exact(&mut header).is_err() || header[..4] != MAGIC || header[4] == VERSION {
        return Ok(());
    }
    Err(SegError::BadVersion {
        path: path.to_path_buf(),
        found: header[4],
    })
}

/// Open a segment, recovering it first if it was never sealed.
///
/// 1. Read the last 8 bytes. A magic that does not match means unsealed → recovery.
/// 2. Read `footer_len` bytes at `size - 8 - footer_len` and parse the index.
/// 3. Check each row group's header against the footer, which is also where the per-chunk
///    `encoding` byte comes from - the footer carries offsets and lengths only.
///
/// **Recovery.** Past the header, repeatedly read `payload_len`, `crc` and the payload,
/// and verify. Stop on a CRC mismatch, a `payload_len` larger than the bytes remaining, or
/// a short read: that is the signature of a process killed mid-write, not of corruption,
/// so it is a WARN and not a failed startup. Truncate at the end of the last good row
/// group, build a footer from what survived - what the duplicated column names are for -
/// and seal.
pub fn open(path: &Path) -> Result<(SegmentIndex, Opened), SegError> {
    let len = std::fs::metadata(path).map_err(|e| io(path, e))?.len();
    let mut file = File::open(path).map_err(|e| io(path, e))?;

    // Too short to hold a header: created and never written to, a crash between the two.
    // Salvaged as the empty segment it is - refusing to start over it would be refusing
    // to start over nothing.
    if len < HEADER_BYTES {
        drop(file);
        return recover(path, 0, Vec::new(), Vec::new(), len);
    }

    let mut header = [0u8; HEADER_BYTES as usize];
    file.read_exact(&mut header).map_err(|e| io(path, e))?;
    if header[..4] != MAGIC {
        return Err(SegError::NotASegment {
            path: path.to_path_buf(),
        });
    }
    if header[4] != VERSION {
        return Err(SegError::BadVersion {
            path: path.to_path_buf(),
            found: header[4],
        });
    }

    if len >= HEADER_BYTES + TRAILER_BYTES {
        let mut trailer = [0u8; TRAILER_BYTES as usize];
        file.seek(SeekFrom::End(-(TRAILER_BYTES as i64)))
            .map_err(|e| io(path, e))?;
        file.read_exact(&mut trailer).map_err(|e| io(path, e))?;
        if trailer[4..] == MAGIC {
            let footer_len = u64::from(u32::from_le_bytes(
                trailer[..4].try_into().unwrap_or([0; 4]),
            ));
            // Validated against the file before a byte is allocated for it.
            if footer_len <= len - HEADER_BYTES - TRAILER_BYTES {
                let mut raw = vec![0u8; footer_len as usize];
                file.seek(SeekFrom::Start(len - TRAILER_BYTES - footer_len))
                    .map_err(|e| io(path, e))?;
                file.read_exact(&mut raw).map_err(|e| io(path, e))?;
                let mut index = parse_footer(path, &raw, len)?;
                verify_row_groups(path, &mut file, &mut index)?;
                return Ok((index, Opened::Sealed));
            }
        }
    }

    drop(file);
    walk_and_recover(path, len)
}

fn parse_footer(path: &Path, raw: &[u8], file_len: u64) -> Result<SegmentIndex, SegError> {
    let mut cur = Cursor::new(raw);
    let total_rows = cur
        .u64()
        .ok_or_else(|| short(path, "footer total_rows", 8, cur.left()))?;
    let column_count =
        cur.u16()
            .ok_or_else(|| short(path, "footer column_count", 2, cur.left()))? as usize;

    let mut columns = Vec::with_capacity(column_count.min(cur.left() as usize));
    for _ in 0..column_count {
        columns.push(read_column_spec(path, &mut cur)?);
    }

    let group_count =
        cur.u32()
            .ok_or_else(|| short(path, "footer row_group_count", 4, cur.left()))? as usize;
    // Each row group costs at least 24 bytes plus 12 per column, so a count that cannot
    // fit in what is left is rejected before a `Vec` is reserved for it.
    let per_group = 24 + 12 * column_count;
    let need = (group_count as u64).saturating_mul(per_group as u64);
    if need > cur.left() {
        return Err(short(path, "footer row groups", need, cur.left()));
    }

    let mut groups = Vec::with_capacity(group_count);
    for _ in 0..group_count {
        let row_count = cur
            .u64()
            .ok_or_else(|| short(path, "row group row_count", 8, cur.left()))?;
        let min_ts = cur
            .i64()
            .ok_or_else(|| short(path, "row group min_ts", 8, cur.left()))?;
        let max_ts = cur
            .i64()
            .ok_or_else(|| short(path, "row group max_ts", 8, cur.left()))?;
        let mut chunks = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let offset = cur
                .u64()
                .ok_or_else(|| short(path, "chunk offset", 8, cur.left()))?;
            let chunk_len = cur
                .u32()
                .ok_or_else(|| short(path, "chunk length", 4, cur.left()))?;
            if offset.saturating_add(u64::from(chunk_len)) > file_len {
                return Err(short(
                    path,
                    "column chunk",
                    offset.saturating_add(u64::from(chunk_len)),
                    file_len,
                ));
            }
            chunks.push(Chunk {
                offset,
                len: chunk_len,
                // Filled from the row group's own header; see `verify_row_groups`.
                encoding: ENCODING_ALL_PRESENT,
            });
        }
        groups.push(RowGroup {
            row_count,
            min_ts,
            max_ts,
            chunks,
        });
    }

    Ok(SegmentIndex {
        path: path.to_path_buf(),
        columns,
        groups,
        total_rows,
        bytes: file_len,
        // The footer begins where the data ends, and the trailer names its length.
        data_end: file_len
            .saturating_sub(TRAILER_BYTES)
            .saturating_sub(raw.len() as u64),
    })
}

fn read_column_spec(path: &Path, cur: &mut Cursor<'_>) -> Result<ColumnSpec, SegError> {
    let name_len =
        cur.u16()
            .ok_or_else(|| short(path, "column name length", 2, cur.left()))? as usize;
    let name = cur
        .take(name_len)
        .ok_or_else(|| short(path, "column name", name_len as u64, cur.left()))?;
    let name = std::str::from_utf8(name)
        .map_err(|_| SegError::BadName {
            path: path.to_path_buf(),
        })?
        .to_string();
    let type_byte = cur
        .u8()
        .ok_or_else(|| short(path, "column type", 1, cur.left()))?;
    let kind = ColKind::from_byte(type_byte).ok_or(SegError::BadType {
        path: path.to_path_buf(),
        found: type_byte,
    })?;
    let form_byte = cur
        .u8()
        .ok_or_else(|| short(path, "datetime form", 1, cur.left()))?;
    // The byte is written 0 for every non-datetime column, and 0 *is* `Mixed`. Only a
    // datetime column's zero means anything, so only there is it read back as a form -
    // and a non-datetime column carrying anything else is malformed rather than ignorable.
    let dt_form = if kind == ColKind::Dt {
        Some(DtForm::from_byte(form_byte).ok_or(SegError::BadDtForm {
            path: path.to_path_buf(),
            found: form_byte,
        })?)
    } else if form_byte == 0 {
        None
    } else {
        return Err(SegError::BadDtForm {
            path: path.to_path_buf(),
            found: form_byte,
        });
    };
    Ok(ColumnSpec {
        name,
        kind,
        dt_form,
    })
}

/// Read each row group's header, check it against the footer, and take the `encoding`
/// bytes from it.
///
/// **Both halves are load bearing.** The footer records offsets and lengths, not how a
/// chunk was written, so the encoding can only come from the row group. And a footer that
/// disagrees with a row group is a corrupt segment, so this is where that is caught -
/// once per open, rather than nowhere.
///
/// The header sits immediately before the first chunk and its length follows from the
/// footer's column list: one small read per row group, no scan.
fn verify_row_groups(
    path: &Path,
    file: &mut File,
    index: &mut SegmentIndex,
) -> Result<(), SegError> {
    let header_bytes: usize = 8
        + 8
        + 8
        + 2
        + index
            .columns
            .iter()
            .map(ColumnSpec::header_bytes)
            .sum::<usize>();

    for g in 0..index.groups.len() {
        let Some(first) = index.groups[g].chunks.first() else {
            continue;
        };
        let start = first
            .offset
            .checked_sub(header_bytes as u64)
            .ok_or_else(|| short(path, "row group header", header_bytes as u64, first.offset))?;
        let mut raw = vec![0u8; header_bytes];
        file.seek(SeekFrom::Start(start)).map_err(|e| io(path, e))?;
        file.read_exact(&mut raw).map_err(|e| io(path, e))?;

        let mut cur = Cursor::new(&raw);
        let row_count = cur
            .u64()
            .ok_or_else(|| short(path, "row group row_count", 8, cur.left()))?;
        let _min = cur.i64();
        let _max = cur.i64();
        let columns = cur
            .u16()
            .ok_or_else(|| short(path, "row group column_count", 2, cur.left()))?
            as usize;
        if row_count != index.groups[g].row_count || columns != index.columns.len() {
            return Err(SegError::FooterDisagrees {
                path: path.to_path_buf(),
                group: g,
                column: 0,
                found: format!("{row_count} row(s) over {columns} column(s)"),
                expected: format!(
                    "{} row(s) over {} column(s)",
                    index.groups[g].row_count,
                    index.columns.len()
                ),
            });
        }
        for c in 0..columns {
            let spec = read_column_spec(path, &mut cur)?;
            let encoding = cur
                .u8()
                .ok_or_else(|| short(path, "encoding byte", 1, cur.left()))?;
            let chunk_len = cur
                .u32()
                .ok_or_else(|| short(path, "chunk length", 4, cur.left()))?;
            // The `dt_form` check is the one this exists for: it is per column in the row
            // group AND per column in the footer, and the two must agree.
            if spec != index.columns[c] || chunk_len != index.groups[g].chunks[c].len {
                return Err(SegError::FooterDisagrees {
                    path: path.to_path_buf(),
                    group: g,
                    column: c,
                    found: format!("{spec:?} of {chunk_len} byte(s)"),
                    expected: format!(
                        "{:?} of {} byte(s)",
                        index.columns[c], index.groups[g].chunks[c].len
                    ),
                });
            }
            if encoding != ENCODING_BITMAP && encoding != ENCODING_ALL_PRESENT {
                return Err(SegError::BadEncoding {
                    path: path.to_path_buf(),
                    found: encoding,
                });
            }
            index.groups[g].chunks[c].encoding = encoding;
        }
    }
    Ok(())
}

/// Walk the row groups forward from the header, verifying each CRC, and seal what survives.
fn walk_and_recover(path: &Path, len: u64) -> Result<(SegmentIndex, Opened), SegError> {
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    let mut at = HEADER_BYTES;
    let mut columns: Vec<ColumnSpec> = Vec::new();
    let mut groups: Vec<RowGroup> = Vec::new();
    let mut header_bytes = 0usize;

    loop {
        if len.saturating_sub(at) < FRAME_BYTES {
            break;
        }
        let mut frame = [0u8; FRAME_BYTES as usize];
        if file.seek(SeekFrom::Start(at)).is_err() || file.read_exact(&mut frame).is_err() {
            break;
        }
        let payload_len = u64::from(u32::from_le_bytes(frame[..4].try_into().unwrap_or([0; 4])));
        let want_crc = u32::from_le_bytes(frame[4..].try_into().unwrap_or([0; 4]));
        // Validated against the bytes actually remaining before the payload is allocated.
        if payload_len > len - at - FRAME_BYTES {
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        if file.read_exact(&mut payload).is_err() || crc32c(&payload) != want_crc {
            break;
        }

        let Some((group, group_columns, group_header_bytes)) =
            parse_row_group(path, &payload, at + FRAME_BYTES)
        else {
            break;
        };
        // The first surviving row group establishes the segment's columns; one that
        // disagrees ends the valid prefix, so a rebuilt footer can never disagree with a
        // row group it describes.
        if groups.is_empty() {
            columns = group_columns;
            header_bytes = group_header_bytes;
        } else if columns != group_columns || header_bytes != group_header_bytes {
            break;
        }
        groups.push(group);
        at += FRAME_BYTES + payload_len;
    }

    drop(file);
    recover(path, at, columns, groups, len)
}

/// One row group's header, parsed out of a verified payload.
fn parse_row_group(
    path: &Path,
    payload: &[u8],
    payload_at: u64,
) -> Option<(RowGroup, Vec<ColumnSpec>, usize)> {
    let mut cur = Cursor::new(payload);
    let row_count = cur.u64()?;
    let min_ts = cur.i64()?;
    let max_ts = cur.i64()?;
    let column_count = cur.u16()? as usize;

    let mut columns = Vec::with_capacity(column_count.min(payload.len()));
    let mut encodings = Vec::with_capacity(column_count.min(payload.len()));
    let mut lens = Vec::with_capacity(column_count.min(payload.len()));
    for _ in 0..column_count {
        columns.push(read_column_spec(path, &mut cur).ok()?);
        let encoding = cur.u8()?;
        if encoding != ENCODING_BITMAP && encoding != ENCODING_ALL_PRESENT {
            return None;
        }
        encodings.push(encoding);
        lens.push(cur.u32()?);
    }
    let header_bytes = cur.at;
    // The chunks have to fit inside the payload the CRC just vouched for.
    let body: u64 = lens.iter().map(|l| u64::from(*l)).sum();
    if body != cur.left() {
        return None;
    }

    let mut offset = payload_at + header_bytes as u64;
    let chunks = lens
        .iter()
        .zip(&encodings)
        .map(|(len, encoding)| {
            let chunk = Chunk {
                offset,
                len: *len,
                encoding: *encoding,
            };
            offset += u64::from(*len);
            chunk
        })
        .collect();

    Some((
        RowGroup {
            row_count,
            min_ts,
            max_ts,
            chunks,
        },
        columns,
        header_bytes,
    ))
}

/// Truncate at the end of the last good row group, write a footer over it, seal.
fn recover(
    path: &Path,
    good_to: u64,
    columns: Vec<ColumnSpec>,
    groups: Vec<RowGroup>,
    was: u64,
) -> Result<(SegmentIndex, Opened), SegError> {
    let total_rows = groups.iter().map(|g| g.row_count).sum();
    let mut index = SegmentIndex {
        path: path.to_path_buf(),
        columns,
        groups,
        total_rows,
        bytes: good_to,
        data_end: good_to,
    };

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(|e| io(path, e))?;
    // The header first, if the crash landed inside it: a file that cannot hold one is
    // rewritten as an empty segment rather than left as a fragment nothing can parse.
    if good_to <= HEADER_BYTES {
        let mut header = [0u8; HEADER_BYTES as usize];
        header[..4].copy_from_slice(&MAGIC);
        header[4] = VERSION;
        file.set_len(0).map_err(|e| io(path, e))?;
        file.write_all(&header).map_err(|e| io(path, e))?;
        index.bytes = HEADER_BYTES;
        index.data_end = HEADER_BYTES;
    } else {
        file.set_len(good_to).map_err(|e| io(path, e))?;
        file.seek(SeekFrom::Start(good_to))
            .map_err(|e| io(path, e))?;
    }

    let footer = encode_footer(&index);
    file.write_all(&footer).map_err(|e| io(path, e))?;
    file.sync_all().map_err(|e| io(path, e))?;
    index.bytes += footer.len() as u64;

    let recovered = Opened::Recovered {
        groups: index.groups.len(),
        rows: index.total_rows,
        dropped_bytes: was.saturating_sub(good_to),
    };
    Ok((index, recovered))
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

/// The format's own tests. Here rather than in `tests/` because the crate is a binary -
/// there is no library for an integration test to import. What a segment does through the
/// store and the server is checked from outside, in `tests/segments.rs`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Rng;

    /// A scratch directory that removes itself.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "triedb-segment-{}-{}-{}",
                name,
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("scratch dir");
            Self(dir)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn spec(name: &str, kind: ColKind, dt_form: Option<DtForm>) -> ColumnSpec {
        ColumnSpec {
            name: name.to_string(),
            kind,
            dt_form,
        }
    }

    /// Write one segment of `groups` row groups and seal it.
    fn write(path: &Path, columns: Vec<ColumnSpec>, groups: &[Vec<Vec<ColValue>>]) -> SegmentIndex {
        let mut writer =
            SegmentWriter::create(path.to_path_buf(), columns).expect("create the segment");
        for chunks in groups {
            let rows = chunks.first().map_or(0, Vec::len) as u64;
            writer
                .append_row_group(chunks, rows, None)
                .expect("append a row group");
        }
        let (index, failure) = writer.seal();
        assert!(failure.is_none(), "seal failed: {failure:?}");
        index
    }

    /// Every value in a sealed segment, column by column.
    fn read_back(index: &SegmentIndex) -> Vec<Vec<ColValue>> {
        let mut file = index.open_file().expect("open the segment");
        let mut out = vec![Vec::new(); index.columns.len()];
        for g in 0..index.groups.len() {
            for (c, column) in out.iter_mut().enumerate() {
                column.extend(index.values(&mut file, g, c).expect("decode a chunk"));
            }
        }
        out
    }

    // ── The polynomial ─────────────────────────────────────────────────────────────

    /// The standard check value, so the hand-rolled table is the CRC-32C everyone else
    /// means by the name and not a private checksum that happens to be stable.
    #[test]
    fn crc32c_matches_the_published_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    // ── What the bytes are ─────────────────────────────────────────────────────────

    /// `TRDB` as four bytes in a hex dump, not `BDRT`. This is the check the magic
    /// exists for, and writing the number instead of the bytes is the way to lose it.
    #[test]
    fn the_magic_reads_as_trdb_at_both_ends() {
        let scratch = Scratch::new("magic");
        let path = scratch.path("seg_000001.col");
        write(
            &path,
            vec![spec("n", ColKind::I64, None)],
            &[vec![vec![ColValue::Int64(1)]]],
        );

        let raw = std::fs::read(&path).expect("read the segment");
        assert_eq!(
            &raw[..4],
            b"TRDB",
            "the header magic is not four bytes of TRDB"
        );
        assert_eq!(raw[4], VERSION);
        assert_eq!(&raw[5..8], &[0, 0, 0], "the reserved bytes are not zero");
        assert_eq!(&raw[raw.len() - 4..], b"TRDB", "the footer magic is wrong");
    }

    // ── Nulls ──────────────────────────────────────────────────────────────────────

    /// The bug the old sentinel could not express.
    ///
    /// A leading `0xFF` is the first byte of `-1`, of `255`, and of a 255-byte string
    /// length, so a reader walking cells forward could not tell one of those from a null.
    /// Each of the three sits next to a null here, and each has to come back as itself.
    #[test]
    fn boundary_values_survive_beside_nulls() {
        let scratch = Scratch::new("boundaries");
        let long = "x".repeat(255);
        let columns = vec![
            spec("text", ColKind::Str, None),
            spec("n", ColKind::I64, None),
        ];
        let text = vec![
            ColValue::String(long.clone()),
            ColValue::Null,
            ColValue::String(String::new()),
            ColValue::Null,
            ColValue::String(long.clone()),
        ];
        let ints = vec![
            ColValue::Int64(-1),
            ColValue::Null,
            ColValue::Int64(255),
            ColValue::Int64(-1),
            ColValue::Null,
        ];

        let index = write(
            &scratch.path("seg_000001.col"),
            columns,
            &[vec![text.clone(), ints.clone()]],
        );
        assert_eq!(read_back(&index), vec![text, ints]);
    }

    /// Row `r` is byte `r/8`, bit `r%8`, lowest bit first. Backwards and every null moves,
    /// which is why the bit positions are asserted against the bytes rather than only
    /// round-tripped.
    #[test]
    fn the_bitmap_puts_the_earliest_row_in_the_lowest_bit() {
        // Rows 0 and 9 present, everything else null: bit 0 of byte 0, bit 1 of byte 1.
        let mut values = vec![ColValue::Null; 12];
        values[0] = ColValue::Bool(true);
        values[9] = ColValue::Bool(false);

        let (encoding, bytes) = encode_chunk(&values, ColKind::Bool).expect("encode");
        assert_eq!(encoding, ENCODING_BITMAP);
        assert_eq!(bytes[0], 0b0000_0001);
        assert_eq!(bytes[1], 0b0000_0010);
        // Two present rows, one payload byte each, after a two-byte bitmap.
        assert_eq!(bytes.len(), 2 + 2);

        let back = decode_chunk(Path::new("x"), &bytes, ColKind::Bool, 12, encoding)
            .expect("decode the chunk");
        assert_eq!(back, values);
    }

    /// A chunk with a value on every row writes **no bitmap at all**. An all-ones
    /// 1,024-byte bitmap per column per row group is pure waste, and `ts`, `level` and
    /// `user_id` will usually take this path.
    #[test]
    fn a_full_column_gets_encoding_one_and_no_bitmap() {
        let full: Vec<ColValue> = (0..64).map(ColValue::Int64).collect();
        let (encoding, bytes) = encode_chunk(&full, ColKind::I64).expect("encode");
        assert_eq!(encoding, ENCODING_ALL_PRESENT);
        assert_eq!(bytes.len(), 64 * 8, "a bitmap was written anyway");

        let mut holed = full.clone();
        holed[7] = ColValue::Null;
        let (encoding, bytes) = encode_chunk(&holed, ColKind::I64).expect("encode");
        assert_eq!(encoding, ENCODING_BITMAP);
        assert_eq!(
            bytes.len(),
            8 + 63 * 8,
            "the bitmap is not 8 bytes for 64 rows"
        );

        let back = decode_chunk(Path::new("x"), &bytes, ColKind::I64, 64, encoding)
            .expect("decode the chunk");
        assert_eq!(back[7], ColValue::Null);
        assert_eq!(back[6], ColValue::Int64(6));
        assert_eq!(back[8], ColValue::Int64(8));
    }

    /// Reserved means rejected, not ignored. A chunk written by a future zstd or
    /// dictionary encoding must not be read as if it were plain.
    #[test]
    fn a_reserved_encoding_byte_is_refused() {
        let bytes = [0u8; 8];
        for encoding in 2u8..=255 {
            let out = decode_chunk(Path::new("x"), &bytes, ColKind::I64, 1, encoding);
            assert!(
                matches!(out, Err(SegError::BadEncoding { .. })),
                "encoding {encoding} was not refused"
            );
        }
    }

    // ── dt_form ────────────────────────────────────────────────────────────────────

    /// The rung travels in the row group **and** in the footer, and `open` checks that
    /// the two agree - which is what `verify_row_groups` is for.
    #[test]
    fn dt_form_round_trips_and_the_two_copies_agree() {
        let scratch = Scratch::new("dtform");
        for form in [
            DtForm::Rfc3339,
            DtForm::EpochSecs,
            DtForm::EpochMillis,
            DtForm::Mixed,
        ] {
            let path = scratch.path(&format!("seg_{}.col", form.byte()));
            let written = write(
                &path,
                vec![spec("ts", ColKind::Dt, Some(form))],
                &[
                    vec![vec![ColValue::Datetime(1), ColValue::Null]],
                    vec![vec![ColValue::Datetime(2)]],
                ],
            );
            assert_eq!(written.columns[0].dt_form, Some(form));

            let (reopened, _) = open(&path).expect("reopen the segment");
            assert_eq!(reopened.columns[0].dt_form, Some(form));

            // The row group's own copy, read off the bytes rather than off the index:
            // the byte after the column name and the type byte.
            let raw = std::fs::read(&path).expect("read the segment");
            let at = raw
                .windows(2)
                .position(|w| w == b"ts")
                .expect("the column name is in the row group header");
            assert_eq!(raw[at + 2], ColKind::Dt.byte());
            assert_eq!(raw[at + 3], form.byte(), "the row group's dt_form differs");
        }
    }

    /// A non-datetime column carrying a rung is malformed rather than ignorable: the byte
    /// is specified as zero for every other type.
    #[test]
    fn a_dt_form_on_a_non_datetime_column_is_refused() {
        let scratch = Scratch::new("dtform-bad");
        let path = scratch.path("seg_000001.col");
        write(
            &path,
            vec![spec("n", ColKind::I64, None)],
            &[vec![vec![ColValue::Int64(1)]]],
        );

        let mut raw = std::fs::read(&path).expect("read the segment");
        // The footer's copy: name, then the type byte, then the rung byte.
        let at = raw
            .windows(1)
            .rposition(|w| w == b"n")
            .expect("the column name is in the footer");
        raw[at + 2] = DtForm::EpochSecs.byte();
        std::fs::write(&path, &raw).expect("rewrite the segment");

        assert!(
            matches!(open(&path), Err(SegError::BadDtForm { .. })),
            "a rung on an I64 column was accepted"
        );
    }

    // ── The round trip ─────────────────────────────────────────────────────────────

    /// Random rows of every type, nulls included, across several row groups.
    ///
    /// A property and not a corpus: the failure it watches for is a cell that
    /// desynchronises a column - one wrong width, one bitmap bit off - which shows up as
    /// *some* rows wrong for *some* data, and no hand-picked fixture is likely to be it.
    #[test]
    fn random_rows_of_every_type_round_trip() {
        let scratch = Scratch::new("property");
        let kinds = [
            ColKind::Str,
            ColKind::I64,
            ColKind::F64,
            ColKind::Dt,
            ColKind::Bool,
        ];

        for iteration in 0..400u64 {
            let mut rng = Rng::new(iteration);
            let columns: Vec<ColumnSpec> = (0..rng.between(1, 6))
                .map(|c| {
                    let kind = *rng.pick(&kinds);
                    spec(
                        &format!("col{c}"),
                        kind,
                        (kind == ColKind::Dt).then_some(DtForm::Rfc3339),
                    )
                })
                .collect();

            let group_count = rng.between(1, 4) as usize;
            let mut groups = Vec::with_capacity(group_count);
            for _ in 0..group_count {
                let rows = rng.between(0, 300) as usize;
                let chunks: Vec<Vec<ColValue>> = columns
                    .iter()
                    .map(|column| {
                        (0..rows)
                            .map(|_| {
                                // A quarter null, so most chunks carry a bitmap and some
                                // are full - both encodings get exercised by the same test.
                                if rng.chance(25) {
                                    return ColValue::Null;
                                }
                                match column.kind {
                                    ColKind::Str => {
                                        ColValue::String("\u{00e9}x".repeat(rng.below(40) as usize))
                                    }
                                    ColKind::I64 => ColValue::Int64(rng.next_u64() as i64),
                                    ColKind::F64 => {
                                        ColValue::Float64(f64::from_bits(rng.next_u64()))
                                    }
                                    ColKind::Dt => ColValue::Datetime(rng.next_u64() as i64),
                                    ColKind::Bool => ColValue::Bool(rng.chance(50)),
                                }
                            })
                            .collect()
                    })
                    .collect();
                groups.push(chunks);
            }

            let path = scratch.path(&format!("seg_{iteration:06}.col"));
            let written = write(&path, columns.clone(), &groups);
            let (reopened, opened) = open(&path).expect("reopen the segment");
            assert!(matches!(opened, Opened::Sealed), "iteration {iteration}");
            assert_eq!(reopened.columns, columns, "iteration {iteration}");
            assert_eq!(reopened.total_rows, written.total_rows);

            let mut expected = vec![Vec::new(); columns.len()];
            for chunks in &groups {
                for (c, column) in chunks.iter().enumerate() {
                    expected[c].extend(column.iter().cloned());
                }
            }
            let back = read_back(&reopened);
            for (c, column) in expected.iter().enumerate() {
                // NaN is not equal to itself, so the comparison is over the bits the
                // format actually wrote - which is also what `to_bits` promised.
                let same = column.len() == back[c].len()
                    && column.iter().zip(&back[c]).all(|(a, b)| match (a, b) {
                        (ColValue::Float64(a), ColValue::Float64(b)) => a.to_bits() == b.to_bits(),
                        (a, b) => a == b,
                    });
                assert!(
                    same,
                    "iteration {iteration}, column {c} did not round-trip\n{:?}\n{:?}",
                    &column[..column.len().min(6)],
                    &back[c][..back[c].len().min(6)]
                );
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    // ── Truncation ─────────────────────────────────────────────────────────────────

    /// **Every byte offset from 0 to the file length.**
    ///
    /// A crash can land anywhere, so the test lands everywhere. At each offset recovery
    /// either salvages a valid prefix - and then every row it claims must actually read
    /// back - or returns a clean error. Never a panic, never a hang, and never an
    /// allocation from a length the file does not back.
    #[test]
    fn truncation_at_every_offset_salvages_or_refuses() {
        let scratch = Scratch::new("truncate");
        let columns = vec![
            spec("ts", ColKind::Dt, Some(DtForm::Rfc3339)),
            spec("name", ColKind::Str, None),
            spec("n", ColKind::I64, None),
        ];
        let groups: Vec<Vec<Vec<ColValue>>> = (0..3)
            .map(|g| {
                vec![
                    (0..20).map(|r| ColValue::Datetime(g * 100 + r)).collect(),
                    (0..20)
                        .map(|r| {
                            if r % 4 == 0 {
                                ColValue::Null
                            } else {
                                ColValue::String(format!("row-{g}-{r}"))
                            }
                        })
                        .collect(),
                    (0..20).map(ColValue::Int64).collect(),
                ]
            })
            .collect();

        let whole = scratch.path("whole.col");
        let sealed = write(&whole, columns.clone(), &groups);
        let full = std::fs::read(&whole).expect("read the segment");
        assert_eq!(sealed.total_rows, 60);

        for cut in 0..=full.len() {
            let path = scratch.path("cut.col");
            std::fs::write(&path, &full[..cut]).expect("write the truncated segment");

            match open(&path) {
                Ok((index, _)) => {
                    // Whatever it says survived has to be readable, or "salvaged" is a
                    // claim about rows that are not there.
                    let mut file = index.open_file().expect("open the salvaged segment");
                    let mut rows = 0;
                    for (g, group) in index.groups.iter().enumerate() {
                        for c in 0..index.columns.len() {
                            let values = index
                                .values(&mut file, g, c)
                                .unwrap_or_else(|e| panic!("cut at {cut}: {e}"));
                            assert_eq!(values.len() as u64, group.row_count);
                        }
                        rows += group.row_count;
                    }
                    assert_eq!(rows, index.total_rows, "cut at {cut}");
                    assert!(
                        index.total_rows <= 60,
                        "cut at {cut} salvaged more rows than were written"
                    );
                    // A salvaged segment is sealed, so opening it again is a no-op rather
                    // than a second recovery.
                    let (again, opened) = open(&path).expect("reopen the salvaged segment");
                    assert!(matches!(opened, Opened::Sealed), "cut at {cut}");
                    assert_eq!(again.total_rows, index.total_rows, "cut at {cut}");
                }
                // A clean error is the other acceptable answer. What is not acceptable is
                // a panic, and reaching this line at all means there was not one.
                Err(e) => {
                    let text = e.to_string();
                    assert!(!text.is_empty(), "cut at {cut} produced an empty error");
                }
            }
            let _ = std::fs::remove_file(&path);
        }
    }

    /// A row group whose CRC does not match ends the valid prefix, and everything before
    /// it survives. This is the per-row-group CRC doing the job it exists for: a crash
    /// costs one row group rather than the whole segment.
    #[test]
    fn a_corrupt_row_group_costs_only_itself() {
        let scratch = Scratch::new("crc");
        let path = scratch.path("seg_000001.col");
        let columns = vec![spec("n", ColKind::I64, None)];
        let groups: Vec<Vec<Vec<ColValue>>> = (0..3)
            .map(|g| vec![(0..10).map(|r| ColValue::Int64(g * 10 + r)).collect()])
            .collect();
        let sealed = write(&path, columns, &groups);
        assert_eq!(sealed.total_rows, 30);

        // Flip a byte inside the second row group's payload, then remove the footer so
        // the file has to be walked rather than trusted.
        let mut raw = std::fs::read(&path).expect("read the segment");
        let second = sealed.groups[1].chunks[0].offset as usize;
        raw[second] ^= 0xFF;
        raw.truncate(sealed.groups[2].chunks[0].offset as usize);
        std::fs::write(&path, &raw).expect("rewrite the segment");

        let (index, opened) = open(&path).expect("recover the segment");
        assert_eq!(index.total_rows, 10, "the first row group did not survive");
        assert!(matches!(opened, Opened::Recovered { groups: 1, .. }));

        let mut file = index.open_file().expect("open the recovered segment");
        assert_eq!(
            index.values(&mut file, 0, 0).expect("decode"),
            (0..10).map(ColValue::Int64).collect::<Vec<_>>()
        );
    }

    /// A file that is not a segment, and one from a version this build does not read, are
    /// both refused by name rather than guessed at. There is no migration.
    #[test]
    fn a_foreign_or_older_file_is_refused() {
        let scratch = Scratch::new("foreign");

        let alien = scratch.path("alien.col");
        std::fs::write(
            &alien,
            b"not a segment at all, but long enough to have a footer",
        )
        .expect("write");
        assert!(matches!(open(&alien), Err(SegError::NotASegment { .. })));

        let old = scratch.path("old.col");
        let mut bytes = vec![0u8; 64];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4] = 1;
        std::fs::write(&old, &bytes).expect("write");
        assert!(matches!(
            open(&old),
            Err(SegError::BadVersion { found: 1, .. })
        ));
    }

    /// A newer version is refused as an older one is: a segment this build wrote whole,
    /// with only the version byte changed, and the error names the file. The server's
    /// refusal to start is built on this, and an operator acts on the path.
    #[test]
    fn a_valid_segment_with_version_3_is_refused_by_path() {
        let scratch = Scratch::new("version3");
        let path = scratch.path("seg_000001.col");
        write(
            &path,
            vec![spec("n", ColKind::I64, None)],
            &[vec![vec![ColValue::Int64(1), ColValue::Int64(2)]]],
        );
        open(&path).expect("the segment reads before the flip");

        let mut raw = std::fs::read(&path).expect("read the segment");
        assert_eq!(raw[4], VERSION);
        raw[4] = 3;
        std::fs::write(&path, &raw).expect("write the segment back");

        match open(&path) {
            Err(e @ SegError::BadVersion { found: 3, .. }) => {
                let SegError::BadVersion { path: named, .. } = &e else {
                    unreachable!()
                };
                assert_eq!(named, &path, "the wrong path was named");
                assert!(
                    e.to_string().contains(&path.display().to_string()),
                    "the message does not name the path: {e}"
                );
            }
            other => panic!("version 3 was not refused: {other:?}"),
        }
    }

    /// Time pruning is the footer's whole point: a row group that cannot hold a row in
    /// the window is dropped before a byte of column data is read.
    #[test]
    fn row_groups_prune_on_their_own_timestamps() {
        let scratch = Scratch::new("prune");
        let mut writer = SegmentWriter::create(
            scratch.path("seg_000001.col"),
            vec![spec("ts", ColKind::Dt, Some(DtForm::Rfc3339))],
        )
        .expect("create");
        for g in 0..50i64 {
            let base = g * 1_000;
            let values: Vec<ColValue> = (0..10).map(|r| ColValue::Datetime(base + r)).collect();
            writer
                .append_row_group(&[values], 10, Some((base, base + 9)))
                .expect("append");
        }
        let (index, failure) = writer.seal();
        assert!(failure.is_none(), "seal failed: {failure:?}");

        let hits = index
            .groups
            .iter()
            .filter(|g| g.overlaps(Some(7_000), Some(7_010)))
            .count();
        assert_eq!(hits, 1, "a window inside one row group touched {hits}");

        let all = index
            .groups
            .iter()
            .filter(|g| g.overlaps(None, None))
            .count();
        assert_eq!(all, 50, "an open window pruned something");
    }

    // ── What used to be a debug assertion ─────────────────────────────────────────

    /// A value whose type is not its column's is refused, **in a release build**.
    ///
    /// It was a debug assertion, so the build that ships wrote the bitmap from one
    /// reading of the values and the cells from another. A `String` in an `I64` column
    /// came out marked present with no eight bytes behind it, and every cell after it in
    /// the chunk shifted - each row reading back its neighbour's value, with nothing
    /// reporting an error.
    #[test]
    fn a_wrong_typed_cell_is_an_error_and_not_a_shifted_chunk() {
        let values = vec![
            ColValue::Int64(1),
            ColValue::String("N/A".into()),
            ColValue::Int64(3),
        ];
        match encode_chunk(&values, ColKind::I64).expect_err("a string is not an i64") {
            CellMismatch::Type { kind, value } => {
                assert_eq!(kind, ColKind::I64);
                assert!(value.contains("N/A"), "the value is not named: {value}");
            }
            other => panic!("wrong mismatch: {other:?}"),
        }

        // And the same value through a writer names the file and the column.
        let scratch = Scratch::new("celltype");
        let path = scratch.path("seg_000001.col");
        let mut writer =
            SegmentWriter::create(path.clone(), vec![spec("x", ColKind::I64, None)]).expect("create");
        let e = writer
            .append_row_group(&[values], 3, None)
            .expect_err("a wrong-typed cell is not written");
        match e {
            SegError::CellType { column, kind, .. } => {
                assert_eq!(column, "x");
                assert_eq!(kind, "I64");
            }
            other => panic!("wrong error: {other}"),
        }

        // Nothing of the refused row group reached the file: the encode happens before
        // the write, so the segment is still the header it was created with.
        assert_eq!(writer.bytes(), HEADER_BYTES);
    }

    /// The same check the other way: a chunk of the right type round-trips every cell,
    /// nulls included, to the position it went in at.
    #[test]
    fn a_holed_chunk_reads_every_cell_back_where_it_was_written() {
        let values = vec![
            ColValue::Int64(1),
            ColValue::Null,
            ColValue::Int64(3),
            ColValue::Null,
            ColValue::Int64(5),
        ];
        let (encoding, bytes) = encode_chunk(&values, ColKind::I64).expect("encode");
        assert_eq!(encoding, ENCODING_BITMAP);
        let back = decode_chunk(Path::new("x"), &bytes, ColKind::I64, 5, encoding).expect("decode");
        assert_eq!(back, values);
    }

    /// A row group of the wrong width is refused rather than zipped short.
    #[test]
    fn a_row_group_of_the_wrong_width_is_refused() {
        let scratch = Scratch::new("width");
        let mut writer = SegmentWriter::create(
            scratch.path("seg_000001.col"),
            vec![spec("a", ColKind::I64, None), spec("b", ColKind::I64, None)],
        )
        .expect("create");
        let e = writer
            .append_row_group(&[vec![ColValue::Int64(1)]], 1, None)
            .expect_err("one chunk for two columns");
        match e {
            SegError::ColumnCount { given, expected, .. } => {
                assert_eq!((given, expected), (1, 2));
            }
            other => panic!("wrong error: {other}"),
        }
    }

    // ── Retry-safe creation ────────────────────────────────────────────────────────

    /// A create that fails leaves no file behind for the next attempt to trip over.
    ///
    /// The file it used to leave was a zero-byte one that every later attempt at the same
    /// sequence number met with `File exists`, so the rows behind it never left memory
    /// and a clean shutdown exited 0 without them.
    ///
    /// Two halves, and only one of them is reachable from here. A create that fails at
    /// `create_new` never made a file, which is what this pins. A create that fails
    /// *after* it - the header write or the directory fsync, on a disk with no room for
    /// eight bytes - is what the `remove_file` in `create` is for, and there is no way to
    /// force that from a unit test without a filesystem that can be filled.
    /// `scripts/disk_full.sh` and `scripts/open_files.sh` are where it is exercised, and
    /// both assert that no retry ever answers `File exists`.
    #[test]
    fn a_create_that_fails_leaves_nothing_to_trip_over() {
        let scratch = Scratch::new("partial");
        let nowhere = scratch.path("no-such-dir").join(segment_name(1));
        match SegmentWriter::create(nowhere.clone(), vec![spec("x", ColKind::I64, None)]) {
            Ok(_) => panic!("created a segment under a directory that does not exist"),
            Err(e) => assert!(matches!(e, SegError::Io { .. }), "wrong error: {e}"),
        }
        assert!(!nowhere.exists(), "a failed create left a file behind");
    }

    /// A segment file already where one is about to be created is opened through the
    /// normal recovery path and appended to, not refused.
    #[test]
    fn an_existing_segment_is_reopened_and_appended_to() {
        let scratch = Scratch::new("reopen");
        let path = scratch.path("seg_000001.col");
        let columns = vec![spec("x", ColKind::I64, None)];

        // One sealed segment with three rows in it.
        write(
            &path,
            columns.clone(),
            &[vec![vec![
                ColValue::Int64(1),
                ColValue::Int64(2),
                ColValue::Int64(3),
            ]]],
        );

        let (mut writer, found) =
            SegmentWriter::create_or_reopen(path.clone(), columns).expect("reopen");
        let found = found.expect("the file was already there");
        assert_eq!(found.rows, 3);
        assert_eq!(found.groups, 1);
        assert!(!found.was_empty);
        assert_eq!(writer.rows(), 3, "the rows already in the file are not lost");

        writer
            .append_row_group(&[vec![ColValue::Int64(4)]], 1, None)
            .expect("append");
        let (index, failure) = writer.seal();
        assert!(failure.is_none(), "seal failed: {failure:?}");
        assert_eq!(index.total_rows, 4);

        // And a reader sees all four, in the order they were written.
        let (reopened, opened) = open(&path).expect("open");
        assert!(matches!(opened, Opened::Sealed));
        assert_eq!(reopened.total_rows, 4);
        let mut file = reopened.open_file().expect("file");
        let first = reopened.values(&mut file, 0, 0).expect("group 0");
        let second = reopened.values(&mut file, 1, 0).expect("group 1");
        assert_eq!(
            [first, second].concat(),
            vec![
                ColValue::Int64(1),
                ColValue::Int64(2),
                ColValue::Int64(3),
                ColValue::Int64(4)
            ]
        );
    }

    /// A file holding no row group - the crash between `create_new` and the header - is
    /// rewritten with the columns the run needs rather than sealed empty.
    #[test]
    fn an_empty_leftover_file_is_rewritten_rather_than_sealed() {
        let scratch = Scratch::new("leftover");
        let path = scratch.path("seg_000001.col");
        std::fs::write(&path, b"").expect("a zero-byte leftover");

        let columns = vec![spec("x", ColKind::Str, None)];
        let (mut writer, found) =
            SegmentWriter::create_or_reopen(path.clone(), columns.clone()).expect("reopen");
        let found = found.expect("the file was already there");
        assert!(found.was_empty);
        assert_eq!(writer.rows(), 0);
        assert_eq!(writer.columns(), columns.as_slice());

        writer
            .append_row_group(&[vec![ColValue::String("a".into())]], 1, None)
            .expect("append");
        let (index, failure) = writer.seal();
        assert!(failure.is_none(), "seal failed: {failure:?}");
        assert_eq!(index.total_rows, 1);
    }

    /// A seal whose footer will not write still hands back the index of everything it had
    /// already fsynced.
    ///
    /// Row groups are fsynced as they close and the footer comes later, so a footer that
    /// fails has lost nothing that was written. The signature used to be
    /// `Result<SegmentIndex, _>`, which handed the caller an error and took the
    /// description of every one of those row groups with it: `store::disk::DiskLeaf`
    /// dropped it, and rows physically on the platter vanished from every query in the
    /// process while the partition's own count still included them.
    ///
    /// The handle is swapped for a read-only one, which is the portable way to make the
    /// footer write fail where the row groups before it succeeded - permissions on Unix
    /// are checked when a file is opened, not when it is written.
    #[test]
    fn a_seal_that_cannot_write_its_footer_still_hands_back_the_index() {
        let scratch = Scratch::new("seal-fails");
        let path = scratch.path("seg_000001.col");
        let mut writer = SegmentWriter::create(path.clone(), vec![spec("x", ColKind::I64, None)])
            .expect("create");
        writer
            .append_row_group(&[vec![ColValue::Int64(1), ColValue::Int64(2)]], 2, Some((10, 20)))
            .expect("append");
        let data_end = writer.bytes();

        writer.file = File::open(&path).expect("a read-only handle to the same file");
        let (index, failure) = writer.seal();
        assert!(failure.is_some(), "a read-only handle took the footer");
        assert_eq!(index.total_rows, 2, "the fsynced rows went with the error");
        assert_eq!(index.groups.len(), 1);
        assert_eq!(index.data_end, data_end);

        // And the footerless file on disk recovers to the same two rows.
        let (recovered, opened) = open(&path).expect("open");
        assert!(
            matches!(opened, Opened::Recovered { .. }),
            "expected a recovery, got {opened}"
        );
        assert_eq!(recovered.total_rows, 2);
        let mut file = recovered.open_file().expect("file");
        assert_eq!(
            recovered.values(&mut file, 0, 0).expect("values"),
            vec![ColValue::Int64(1), ColValue::Int64(2)]
        );
    }
}
