//! Storage format v3: one shared file per day, row groups of one template each, a typed
//! piece per column that prints every value back as it was ingested.
//!
//! ```text
//! header        24 bytes: 'KBBR', version 3, three zero bytes, a 16-byte random file id
//! frames        schema records, row groups (each followed by its chunk area), optional
//!               frames, in the order they were written
//! footer frame  written once, when the file is sealed
//! trailer       8 bytes: the footer frame's length, then 'KBBR'
//! ```
//!
//! Every frame is `frame::read`'s: `[4] payload_len [4] crc [N] payload`, the payload's
//! first byte its kind. The catalog and the manifest are framed the same way, so one
//! reader reads all three.
//!
//! The split:
//!
//! - this file - the numbers, the forms, and the print rules the writer's check and the
//!   reader both go through, so what is checked on the way in is what is printed on the
//!   way out;
//! - `build` - **the segment builder**: rows of one template in, one encoded row group
//!   out. No clock, no lock, no knowledge of who is calling it - closing a live row group,
//!   compaction and the bulk loader all hand it rows the same way;
//! - `read` - a row group's header and chunks, decoded and checked;
//! - `file` - a whole file: writing it, sealing it, opening a sealed one through its
//!   footer, walking an unsealed one frame by frame, and recovering it.
//!
//! **Reject, never guess.** Every field has a closed set of values, and a reader that meets
//! one it does not know - a type, a form, an encoding, a compression, a flag bit, a
//! nonzero reserved byte - refuses the file and names its path and offset. The one
//! exception is the high bit of a kind: a frame, a section or a footer part whose kind has
//! bit 7 set is optional, and a reader that does not know it steps over it by its length.
//! What a reader needs to produce an answer is refused when unknown; what only speeds an
//! answer up is skipped.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};

use chrono::{Datelike, Timelike};
use serde_json::Value;
use thiserror::Error;

use crate::types::ColumnType;

pub mod build;
#[cfg(test)]
mod encodings;
pub mod file;
pub mod read;
#[cfg(test)]
mod vectors;

pub const MAGIC: [u8; 4] = *b"KBBR";
/// The magic 0.1.0-alpha.1 wrote, before the rename. A file that starts with it is refused
/// by path, like one of another version: nothing reads it and there is nothing to migrate.
pub const PRE_RENAME_MAGIC: [u8; 4] = [0x54, 0x52, 0x44, 0x42];
pub const VERSION: u8 = 3;
pub const HEADER_LEN: usize = 24;
pub const TRAILER_LEN: usize = 8;

pub const KIND_SCHEMA: u8 = 1;
pub const KIND_ROW_GROUP: u8 = 2;
pub const KIND_FOOTER: u8 = 3;

/// A kind with this bit set is optional: a reader that does not know it steps over it.
pub const OPTIONAL: u8 = 0x80;

/// The largest chunk body a reader will allocate. No writer produces one: a row group
/// closes at about 8MB, and a line is at most 64MiB.
pub const MAX_BODY: u32 = 256 * 1024 * 1024;

pub const DAY_NS: i64 = 86_400 * 1_000_000_000;
const NANOS: i64 = 1_000_000_000;

// ── Errors ─────────────────────────────────────────────────────────────────────────

/// What reading or writing a v3 file can fail on.
///
/// `Corrupt` is bytes whose CRC held and which break a rule of the format - a writer's
/// bug, found at startup or in a query and always named by path and offset. A **tear**
/// is not an error: it is where a crash stopped a write, and `file::recover` cuts the file
/// back to it.
#[derive(Debug, Error)]
pub enum FormatError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: offset {offset}: {what}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        what: String,
    },
    /// A kibbr file from another format version. There is no upgrade path: the path is
    /// named and the process exits non-zero rather than reading bytes it would have to
    /// guess the meaning of.
    #[error(
        "{path}: segment format version {found}; this build reads version 3 only, and there \
         is no migration - point --data-dir somewhere else, or remove it and re-ingest"
    )]
    Version { path: PathBuf, found: u8 },
    /// A file written by a pre-rename build: the same refusal as another version.
    #[error(
        "{path}: written by a pre-rename build; this build does not read it, and there is \
         no migration - point --data-dir somewhere else, or remove it and re-ingest"
    )]
    PreRename { path: PathBuf },
}

/// A rule broken at one offset, before the path is known. `at` is relative to whatever
/// `Bytes` was reading, and `Bytes::base` makes it a file offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bad {
    pub offset: u64,
    pub what: String,
}

impl Bad {
    pub fn at(offset: u64, what: impl Into<String>) -> Self {
        Self {
            offset,
            what: what.into(),
        }
    }

    pub fn in_file(self, path: &Path) -> FormatError {
        FormatError::Corrupt {
            path: path.to_path_buf(),
            offset: self.offset,
            what: self.what,
        }
    }
}

// ── Reading bytes ──────────────────────────────────────────────────────────────────

/// A region of a file, read front to back. **Every length is compared with what remains
/// before anything is sliced or allocated**, so a `u32` of `0xFFFFFFFF` is a `Bad`, never
/// a 4GB allocation.
pub struct Bytes<'a> {
    bytes: &'a [u8],
    at: usize,
    /// The file offset of `bytes[0]`, so every error names a place in the file.
    base: u64,
}

impl<'a> Bytes<'a> {
    pub fn new(bytes: &'a [u8], base: u64) -> Self {
        Self { bytes, at: 0, base }
    }

    pub fn offset(&self) -> u64 {
        self.base + self.at as u64
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.at
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    pub fn bad(&self, what: impl Into<String>) -> Bad {
        Bad::at(self.offset(), what)
    }

    pub fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8], Bad> {
        if n > self.remaining() {
            return Err(self.bad(format!(
                "{what} wants {n} byte(s) and only {} remain",
                self.remaining()
            )));
        }
        let out = &self.bytes[self.at..self.at + n];
        self.at += n;
        Ok(out)
    }

    fn array<const N: usize>(&mut self, what: &str) -> Result<[u8; N], Bad> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N, what)?);
        Ok(out)
    }

    pub fn u8(&mut self, what: &str) -> Result<u8, Bad> {
        Ok(self.array::<1>(what)?[0])
    }

    pub fn u16(&mut self, what: &str) -> Result<u16, Bad> {
        Ok(u16::from_le_bytes(self.array(what)?))
    }

    pub fn u32(&mut self, what: &str) -> Result<u32, Bad> {
        Ok(u32::from_le_bytes(self.array(what)?))
    }

    pub fn u64(&mut self, what: &str) -> Result<u64, Bad> {
        Ok(u64::from_le_bytes(self.array(what)?))
    }

    pub fn i64(&mut self, what: &str) -> Result<i64, Bad> {
        Ok(i64::from_le_bytes(self.array(what)?))
    }

    pub fn id(&mut self, what: &str) -> Result<[u8; 16], Bad> {
        self.array(what)
    }

    /// A `u32` length, then that many bytes of UTF-8.
    pub fn name(&mut self, what: &str) -> Result<String, Bad> {
        let len = self.u32(what)? as usize;
        let at = self.offset();
        let raw = self.take(len, what)?;
        String::from_utf8(raw.to_vec()).map_err(|_| Bad::at(at, format!("{what} is not UTF-8")))
    }

    /// The region must be used up exactly: a byte left over is corruption.
    pub fn done(&self, what: &str) -> Result<(), Bad> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(self.bad(format!("{n} byte(s) left over at the end of {what}"))),
        }
    }

    /// An unsigned LEB128 of at most ten bytes, in its one shortest form: a last byte of 0
    /// after others is a byte the number did not need, and a tenth byte above 1 holds bits
    /// past the 64th. Either is refused, so a value has exactly one spelling.
    pub fn varint(&mut self, what: &str) -> Result<u64, Bad> {
        let at = self.offset();
        let mut value = 0u64;
        for n in 0..10 {
            let byte = self.u8(what)?;
            if n == 9 && byte > 1 {
                return Err(Bad::at(at, format!("{what} holds more than 64 bits")));
            }
            value |= u64::from(byte & 0x7f) << (7 * n);
            if byte & 0x80 == 0 {
                if byte == 0 && n > 0 {
                    return Err(Bad::at(at, format!("{what} has a byte it does not need")));
                }
                return Ok(value);
            }
        }
        Err(Bad::at(at, format!("{what} runs past ten bytes")))
    }
}

/// A signed number as an unsigned one with its sign in the lowest bit, so a small negative
/// number is a small number: 0, -1, 1, -2 become 0, 1, 2, 3.
pub fn zigzag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

pub fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

/// An unsigned LEB128: seven bits a byte, lowest first, the high bit set on every byte but
/// the last.
pub fn put_varint(mut n: u64, out: &mut Vec<u8>) {
    while n >= 0x80 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

// ── Numbers ────────────────────────────────────────────────────────────────────────

/// The type byte. The first five are v2's.
pub fn type_byte(ty: ColumnType) -> u8 {
    match ty {
        ColumnType::String => 0,
        ColumnType::Int64 => 1,
        ColumnType::Float64 => 2,
        ColumnType::Datetime => 3,
        ColumnType::Bool => 4,
        ColumnType::Uuid => 5,
        ColumnType::Ip => 6,
    }
}

pub fn type_of_byte(b: u8) -> Option<ColumnType> {
    Some(match b {
        0 => ColumnType::String,
        1 => ColumnType::Int64,
        2 => ColumnType::Float64,
        3 => ColumnType::Datetime,
        4 => ColumnType::Bool,
        5 => ColumnType::Uuid,
        6 => ColumnType::Ip,
        _ => return None,
    })
}

/// Encodings: how a piece's values are laid out. Plain and constant hold every
/// type, and each of the others one type. A reader refuses any other number, and any
/// encoding on a type outside its row of the table, as it refuses an unknown one - a byte
/// this build cannot decode is not a byte it may guess at.
pub const PLAIN: u8 = 0;
/// One value, in plain layout, that every row of the piece holds.
pub const CONSTANT: u8 = 1;
/// `String`: `[4]` word count, at least 1; the distinct words in plain layout, in the order
/// they first appear; one index per row - 1 byte if there are at most 256 words, 2 if at
/// most 65,536, else 4 - little-endian.
pub const DICTIONARY: u8 = 2;
/// `Int64`: per value zigzag, then LEB128, at most 10 bytes.
pub const VARINT: u8 = 3;
/// `Datetime`: the first value, then each value minus the one before it, wrapping in i64;
/// each zigzag, then LEB128.
pub const DELTA: u8 = 4;
/// `Bool`: `ceil(n/8)` bytes, bit i = row i, the bits past the last row zero.
pub const BITS: u8 = 5;

/// Whether this build reads a piece of type `ty` laid out with `encoding`.
pub fn encodes(ty: ColumnType, encoding: u8) -> bool {
    match encoding {
        PLAIN | CONSTANT => true,
        DICTIONARY => ty == ColumnType::String,
        VARINT => ty == ColumnType::Int64,
        DELTA => ty == ColumnType::Datetime,
        BITS => ty == ColumnType::Bool,
        _ => false,
    }
}

/// The width of a dictionary's indexes, by how many words it has.
pub fn index_width(words: usize) -> usize {
    match words {
        0..=256 => 1,
        257..=65_536 => 2,
        _ => 4,
    }
}

/// Compression 0, none: the body is stored as it is.
pub const NO_COMPRESSION: u8 = 0;
/// Compression 1, zstd: one zstd frame (RFC 8878) holding the whole body, and nothing else,
/// decompressing to exactly `body_len` bytes.
pub const ZSTD: u8 = 1;

/// Where a row group's rows came from.
///
/// For the live door the stream is the WAL and the positions are its sequence numbers;
/// for the bulk door the stream is the load and the positions are line numbers. Replay
/// skips exactly what a durable row group's range covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Source {
    /// Rows that came by no log - the live door before it had one, and any row appended
    /// with no position since: nothing to record, and a byte that says so is better than
    /// a zero that looks like a position.
    #[default]
    NotRecorded,
    Wal {
        stream: u64,
        first: u64,
        last: u64,
    },
    Load {
        stream: u64,
        first: u64,
        last: u64,
    },
}

impl Source {
    pub fn bytes(self) -> (u8, u64, u64, u64) {
        match self {
            Source::NotRecorded => (0, 0, 0, 0),
            Source::Wal {
                stream,
                first,
                last,
            } => (1, stream, first, last),
            Source::Load {
                stream,
                first,
                last,
            } => (2, stream, first, last),
        }
    }
}

// ── Forms ──────────────────────────────────────────────────────────────────────────

/// Whether a piece's rows were JSON strings, JSON numbers (or `true`/`false`), or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JsonKind {
    Str = 0,
    Lit = 1,
    /// The piece starts with a kind bitmap: set = a number or bool, clear = a string.
    Mixed = 2,
}

impl JsonKind {
    fn of_byte(b: u8) -> Option<Self> {
        Some(match b {
            0 => JsonKind::Str,
            1 => JsonKind::Lit,
            2 => JsonKind::Mixed,
            _ => return None,
        })
    }

    /// The kind a piece holding these many string and literal rows is written with.
    pub fn of_counts(strings: usize, literals: usize) -> Self {
        match (strings, literals) {
            (_, 0) => JsonKind::Str,
            (0, _) => JsonKind::Lit,
            _ => JsonKind::Mixed,
        }
    }
}

/// One byte per row, where a form's digit count is `PER_ROW`.
pub const PER_ROW: u8 = 255;

/// `Datetime`'s units.
pub const RFC3339: u8 = 0;
pub const EPOCH_SECS: u8 = 1;
pub const EPOCH_MILLIS: u8 = 2;

/// How a piece's values were written, so the reader writes them back the same way: the
/// form byte beside the type, grown into a small record. Its first byte is `json_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Form {
    String {
        kind: JsonKind,
    },
    Int64 {
        kind: JsonKind,
        /// A string row that is zero or more is written with a `+`.
        plus: bool,
        /// A string row's digits are zero-padded on the left to at least this many.
        width: u8,
    },
    Float64 {
        kind: JsonKind,
        /// Digits after the point in every string row, 0 meaning no point; or `PER_ROW`.
        digits: u8,
    },
    Datetime {
        kind: JsonKind,
        unit: u8,
        /// RFC 3339 only: `T`, `t` or a space, as the byte itself.
        separator: u8,
        /// RFC 3339 only: `Z`, `z`, `+` or `-`, as the byte itself.
        zone: u8,
        /// Minutes, 0 to 1439; 0 unless the zone is `+` or `-`.
        offset: u16,
        /// Fraction digits 0 to 9 in every row, or `PER_ROW`.
        digits: u8,
    },
    Bool {
        kind: JsonKind,
        /// String rows: 0 = `true`/`false`, 1 = `True`/`False`.
        case: u8,
    },
    Uuid {
        /// 0 = lower-case hex, 1 = upper.
        case: u8,
    },
    Ip {
        /// 4 = IPv4; 6/7 = IPv6 RFC 5952 lower/upper; 8/9 = all eight groups lower/upper.
        spelling: u8,
    },
}

impl Form {
    pub fn column_type(&self) -> ColumnType {
        match self {
            Form::String { .. } => ColumnType::String,
            Form::Int64 { .. } => ColumnType::Int64,
            Form::Float64 { .. } => ColumnType::Float64,
            Form::Datetime { .. } => ColumnType::Datetime,
            Form::Bool { .. } => ColumnType::Bool,
            Form::Uuid { .. } => ColumnType::Uuid,
            Form::Ip { .. } => ColumnType::Ip,
        }
    }

    pub fn kind(&self) -> JsonKind {
        match *self {
            Form::String { kind }
            | Form::Int64 { kind, .. }
            | Form::Float64 { kind, .. }
            | Form::Datetime { kind, .. }
            | Form::Bool { kind, .. } => kind,
            Form::Uuid { .. } | Form::Ip { .. } => JsonKind::Str,
        }
    }

    /// Whether the piece carries a digits byte per row.
    pub fn per_row_digits(&self) -> bool {
        matches!(
            self,
            Form::Float64 {
                digits: PER_ROW,
                ..
            } | Form::Datetime {
                digits: PER_ROW,
                ..
            }
        )
    }

    /// The bytes after type and encoding. Their length is fixed by the type.
    pub fn bytes(&self) -> Vec<u8> {
        match *self {
            Form::String { kind } => vec![kind as u8],
            Form::Int64 { kind, plus, width } => vec![kind as u8, u8::from(plus), width],
            Form::Float64 { kind, digits } => vec![kind as u8, digits],
            Form::Datetime {
                kind,
                unit,
                separator,
                zone,
                offset,
                digits,
            } => {
                let o = offset.to_le_bytes();
                vec![kind as u8, unit, separator, zone, o[0], o[1], digits]
            }
            Form::Bool { kind, case } => vec![kind as u8, case],
            Form::Uuid { case } => vec![0, case],
            Form::Ip { spelling } => vec![0, spelling],
        }
    }

    /// A form read back, every field held to its range, and none of the combinations no
    /// writer makes: spelling fields on the form of a JSON number or bool, RFC 3339 for
    /// anything but a JSON string, RFC 3339 fields on an epoch, a `Uuid` or an `Ip` that was
    /// not a string. `at` is where the form starts, for the error.
    pub fn read(ty: ColumnType, r: &mut Bytes<'_>) -> Result<Self, Bad> {
        let at = r.offset();
        let kind_byte = r.u8("form json_kind")?;
        let kind = JsonKind::of_byte(kind_byte)
            .ok_or_else(|| Bad::at(at, format!("json_kind {kind_byte} is not 0, 1 or 2")))?;
        let bad = |what: String| Bad::at(at, what);
        let form = match ty {
            ColumnType::String => Form::String { kind },
            ColumnType::Int64 => {
                let plus = r.u8("Int64 plus")?;
                let width = r.u8("Int64 width")?;
                if plus > 1 {
                    return Err(bad(format!("Int64 plus {plus} is not 0 or 1")));
                }
                if kind == JsonKind::Lit && (plus != 0 || width != 0) {
                    return Err(bad("Int64 json_kind 1 with plus or width set".into()));
                }
                Form::Int64 {
                    kind,
                    plus: plus == 1,
                    width,
                }
            }
            ColumnType::Float64 => {
                let digits = r.u8("Float64 digits")?;
                if kind == JsonKind::Lit && digits != 0 {
                    return Err(bad("Float64 json_kind 1 with digits set".into()));
                }
                Form::Float64 { kind, digits }
            }
            ColumnType::Datetime => {
                let unit = r.u8("Datetime unit")?;
                let separator = r.u8("Datetime separator")?;
                let zone = r.u8("Datetime zone")?;
                let offset = r.u16("Datetime offset")?;
                let digits = r.u8("Datetime digits")?;
                match unit {
                    RFC3339 => {
                        if kind != JsonKind::Str {
                            return Err(bad("RFC 3339 with json_kind other than 0".into()));
                        }
                        if !matches!(separator, b'T' | b't' | b' ') {
                            return Err(bad(format!("Datetime separator 0x{separator:02x}")));
                        }
                        if !matches!(zone, b'Z' | b'z' | b'+' | b'-') {
                            return Err(bad(format!("Datetime zone 0x{zone:02x}")));
                        }
                        let signed = matches!(zone, b'+' | b'-');
                        if offset > 1439 || (!signed && offset != 0) {
                            return Err(bad(format!("Datetime offset {offset}")));
                        }
                        if digits > 9 && digits != PER_ROW {
                            return Err(bad(format!("Datetime digits {digits}")));
                        }
                    }
                    EPOCH_SECS | EPOCH_MILLIS => {
                        if separator != 0 || zone != 0 || offset != 0 || digits != 0 {
                            return Err(bad("an epoch Datetime with RFC 3339 fields set".into()));
                        }
                    }
                    other => return Err(bad(format!("Datetime unit {other}"))),
                }
                Form::Datetime {
                    kind,
                    unit,
                    separator,
                    zone,
                    offset,
                    digits,
                }
            }
            ColumnType::Bool => {
                let case = r.u8("Bool case")?;
                if case > 1 || (kind == JsonKind::Lit && case != 0) {
                    return Err(bad(format!("Bool case {case} with json_kind {kind_byte}")));
                }
                Form::Bool { kind, case }
            }
            ColumnType::Uuid => {
                let case = r.u8("Uuid case")?;
                if kind != JsonKind::Str || case > 1 {
                    return Err(bad(format!("Uuid json_kind {kind_byte} case {case}")));
                }
                Form::Uuid { case }
            }
            ColumnType::Ip => {
                let spelling = r.u8("Ip spelling")?;
                if kind != JsonKind::Str || !matches!(spelling, 4 | 6..=9) {
                    return Err(bad(format!("Ip json_kind {kind_byte} spelling {spelling}")));
                }
                Form::Ip { spelling }
            }
        };
        Ok(form)
    }
}

// ── Printing ───────────────────────────────────────────────────────────────────────
//
// One function per rule, called by the writer's check and by the reader. A value goes
// into a typed piece only if printing it through the piece's form gives back what was
// ingested - a JSON string byte for byte, a JSON number as the same value - so a spelling
// these miss costs its row its typing and never its value.

/// An `Int64` string row: an optional sign, then the digits zero-padded to `width`.
pub fn print_int(value: i64, plus: bool, width: u8) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len().max(width as usize) + 1);
    if value < 0 {
        out.push('-');
    } else if plus {
        out.push('+');
    }
    for _ in digits.len()..width as usize {
        out.push('0');
    }
    out.push_str(&digits);
    out
}

/// A `Float64` string row: plain decimal with exactly `digits` places, the double's exact
/// value rounded ties-to-even - what `{:.N}` prints.
pub fn print_float(value: f64, digits: u8) -> String {
    format!("{value:.prec$}", prec = digits as usize)
}

/// A `Float64` literal row: the shortest decimal that reads back as the same double,
/// which is what serde_json prints.
pub fn float_json(value: f64) -> Option<Value> {
    serde_json::Number::from_f64(value).map(Value::Number)
}

/// The number of fraction digits canonical RFC 3339 (`AutoSi`) prints for this instant:
/// 0, 3, 6 or 9, whichever holds it exactly.
pub fn autosi_digits(ns: i64) -> u8 {
    let sub = ns.rem_euclid(NANOS);
    if sub == 0 {
        0
    } else if sub % 1_000_000 == 0 {
        3
    } else if sub % 1_000 == 0 {
        6
    } else {
        9
    }
}

/// RFC 3339 for an instant: moved by the offset, then date, separator, time, `digits`
/// fraction digits and the zone as written. `None` when the instant moved by the offset is
/// outside what the type holds, or when `digits` would drop a nonzero sub-second digit -
/// a value with more sub-second digits than its row prints is not one this form prints.
pub fn print_rfc3339(ns: i64, separator: u8, zone: u8, offset: u16, digits: u8) -> Option<String> {
    if digits > 9 {
        return None;
    }
    let shift = i64::from(offset) * 60 * NANOS;
    let local = match zone {
        b'+' => ns.checked_add(shift)?,
        b'-' => ns.checked_sub(shift)?,
        _ => ns,
    };
    let sub = local.rem_euclid(NANOS);
    let drop = 10i64.pow(9 - u32::from(digits));
    if sub % drop != 0 {
        return None;
    }
    let dt = chrono::DateTime::from_timestamp(local.div_euclid(NANOS), sub as u32)?;
    let mut out = format!(
        "{:04}-{:02}-{:02}{}{:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        separator as char,
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    if digits > 0 {
        out.push('.');
        let frac = format!("{sub:09}");
        out.push_str(&frac[..digits as usize]);
    }
    match zone {
        b'Z' | b'z' => out.push(zone as char),
        _ => out.push_str(&format!(
            "{}{:02}:{:02}",
            zone as char,
            offset / 60,
            offset % 60
        )),
    }
    // Years before 1000 or after 9999 cannot come from an i64 of nanoseconds, but the
    // check costs nothing and keeps `{:04}` honest.
    (dt.year() >= 0 && dt.year() <= 9999).then_some(out)
}

/// A `Datetime` in whole epoch seconds or milliseconds; `None` when it is not a whole
/// number of its unit.
pub fn epoch_of(ns: i64, unit: u8) -> Option<i64> {
    let per = match unit {
        EPOCH_SECS => NANOS,
        EPOCH_MILLIS => 1_000_000,
        _ => return None,
    };
    (ns % per == 0).then_some(ns / per)
}

/// A UUID's sixteen bytes as 8-4-4-4-12 hex.
pub fn print_uuid(bytes: &[u8; 16], upper: bool) -> String {
    let mut out = String::with_capacity(36);
    for (n, b) in bytes.iter().enumerate() {
        if matches!(n, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        if upper {
            out.push_str(&format!("{b:02X}"));
        } else {
            out.push_str(&format!("{b:02x}"));
        }
    }
    out
}

/// A UUID's text as its bytes, in the order the text reads. `None` for anything that is
/// not 8-4-4-4-12 hex.
pub fn uuid_bytes(text: &str) -> Option<[u8; 16]> {
    let raw = text.as_bytes();
    if raw.len() != 36 {
        return None;
    }
    let mut hex = Vec::with_capacity(32);
    for (n, b) in raw.iter().enumerate() {
        if matches!(n, 8 | 13 | 18 | 23) {
            if *b != b'-' {
                return None;
            }
        } else {
            hex.push((*b as char).to_digit(16)? as u8);
        }
    }
    let mut out = [0u8; 16];
    for (n, pair) in hex.chunks(2).enumerate() {
        out[n] = pair[0] << 4 | pair[1];
    }
    Some(out)
}

/// An address in one of the spellings a form names.
pub fn print_ip(bytes: &[u8], spelling: u8) -> Option<String> {
    match (spelling, bytes.len()) {
        (4, 4) => Some(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]).to_string()),
        (6..=9, 16) => {
            let mut groups = [0u16; 8];
            for (n, g) in groups.iter_mut().enumerate() {
                *g = u16::from_be_bytes([bytes[2 * n], bytes[2 * n + 1]]);
            }
            let text = if spelling >= 8 {
                groups
                    .iter()
                    .map(|g| format!("{g:04x}"))
                    .collect::<Vec<_>>()
                    .join(":")
            } else {
                rfc5952(&groups)
            };
            Some(if spelling % 2 == 1 {
                text.to_ascii_uppercase()
            } else {
                text
            })
        }
        _ => None,
    }
}

/// RFC 5952 as the format fixes it: groups in hex without leading zeros; the longest run
/// of two or more zero groups written `::`, the first such run on a tie; and the last 32
/// bits in dotted decimal exactly when the first 96 are `::ffff`, an IPv4-mapped address.
fn rfc5952(groups: &[u16; 8]) -> String {
    if groups[..5].iter().all(|g| *g == 0) && groups[5] == 0xffff {
        let [a, b] = groups[6].to_be_bytes();
        let [c, d] = groups[7].to_be_bytes();
        return format!("::ffff:{a}.{b}.{c}.{d}");
    }
    let (mut best, mut best_len) = (None, 1);
    let mut n = 0;
    while n < 8 {
        if groups[n] == 0 {
            let start = n;
            while n < 8 && groups[n] == 0 {
                n += 1;
            }
            if n - start > best_len {
                best = Some(start);
                best_len = n - start;
            }
        } else {
            n += 1;
        }
    }
    let hex = |gs: &[u16]| {
        gs.iter()
            .map(|g| format!("{g:x}"))
            .collect::<Vec<_>>()
            .join(":")
    };
    match best {
        Some(start) => format!(
            "{}::{}",
            hex(&groups[..start]),
            hex(&groups[start + best_len..])
        ),
        None => hex(groups),
    }
}

/// An address's text as its bytes: 4 for IPv4, 16 for IPv6.
pub fn ip_bytes(text: &str) -> Option<Vec<u8>> {
    match text.parse::<IpAddr>().ok()? {
        IpAddr::V4(v4) => Some(v4.octets().to_vec()),
        IpAddr::V6(v6) => Some(Ipv6Addr::octets(&v6).to_vec()),
    }
}

/// UTC day of an instant, in days since 1970-01-01.
pub fn day_of(ns: i64) -> i64 {
    ns.div_euclid(DAY_NS)
}

// ── Bitmaps ────────────────────────────────────────────────────────────────────────

/// Bit i is bit (i mod 8) of byte (i div 8), lowest first; `ceil(n/8)` bytes.
pub fn bitmap(bits: impl ExactSizeIterator<Item = bool>) -> Vec<u8> {
    let mut out = vec![0u8; bits.len().div_ceil(8)];
    for (i, set) in bits.enumerate() {
        if set {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

pub fn bit(map: &[u8], i: usize) -> bool {
    map[i / 8] & (1 << (i % 8)) != 0
}

/// Read a bitmap of `n` bits, refusing one with a bit set past the last row.
pub fn read_bitmap<'a>(r: &mut Bytes<'a>, n: usize, what: &str) -> Result<&'a [u8], Bad> {
    let at = r.offset();
    let map = r.take(n.div_ceil(8), what)?;
    if !n.is_multiple_of(8) {
        if let Some(last) = map.last() {
            if last >> (n % 8) != 0 {
                return Err(Bad::at(
                    at,
                    format!("{what} has bits set past its last row"),
                ));
            }
        }
    }
    Ok(map)
}

pub fn count_set(map: &[u8]) -> usize {
    map.iter().map(|b| b.count_ones() as usize).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_forms_print_the_spellings_the_format_names() {
        assert_eq!(print_int(45_000_012, false, 10), "0045000012");
        assert_eq!(print_int(4_500_000_120, true, 0), "+4500000120");
        assert_eq!(print_int(-17, true, 0), "-17");
        assert_eq!(print_int(0, false, 0), "0");
        assert_eq!(print_int(i64::MIN, false, 0), "-9223372036854775808");
        assert_eq!(print_float(0.01, 3), "0.010");
        assert_eq!(print_float(149.0, 2), "149.00");
        assert_eq!(print_float(-0.0, 1), "-0.0");
        assert_eq!(
            print_float(9_223_372_036_854_775_808.0, 0),
            "9223372036854775808"
        );
        let ns = 1_705_314_600_120_000_000;
        assert_eq!(
            print_rfc3339(ns, b'T', b'+', 330, 3).as_deref(),
            Some("2024-01-15T16:00:00.120+05:30")
        );
        assert_eq!(
            print_rfc3339(ns, b' ', b'-', 0, 3).as_deref(),
            Some("2024-01-15 10:30:00.120-00:00")
        );
        assert_eq!(
            print_rfc3339(ns, b'T', b'Z', 0, 1),
            None,
            "drops a nonzero digit"
        );
        assert_eq!(
            epoch_of(1_705_314_600_500_000_000, EPOCH_MILLIS),
            Some(1_705_314_600_500)
        );
        assert_eq!(epoch_of(1_705_314_600_500_000_000, EPOCH_SECS), None);
    }

    #[test]
    fn addresses_print_in_the_spellings_the_form_names() {
        let v6 = |t: &str| ip_bytes(t).expect("an address");
        assert_eq!(
            print_ip(&v6("2001:db8::1"), 6).as_deref(),
            Some("2001:db8::1")
        );
        assert_eq!(
            print_ip(&v6("2001:db8::1"), 7).as_deref(),
            Some("2001:DB8::1")
        );
        assert_eq!(
            print_ip(&v6("2001:db8::1"), 8).as_deref(),
            Some("2001:0db8:0000:0000:0000:0000:0000:0001")
        );
        assert_eq!(
            print_ip(&v6("::ffff:192.0.2.1"), 6).as_deref(),
            Some("::ffff:192.0.2.1")
        );
        assert_eq!(print_ip(&v6("::"), 6).as_deref(), Some("::"));
        assert_eq!(
            print_ip(&v6("1:0:0:2:0:0:0:3"), 6).as_deref(),
            Some("1:0:0:2::3")
        );
        assert_eq!(
            print_ip(&v6("1:0:2:3:4:5:6:7"), 6).as_deref(),
            Some("1:0:2:3:4:5:6:7")
        );
        assert_ne!(
            print_ip(&v6("2001:0db8:0:0:0:0:0:1"), 6).as_deref(),
            Some("2001:0db8:0:0:0:0:0:1")
        );
        let id = "550E8400-E29B-41D4-A716-446655440000";
        let bytes = uuid_bytes(id).expect("a uuid");
        assert_eq!(bytes[0], 0x55);
        assert_eq!(print_uuid(&bytes, true), id);
    }
}
