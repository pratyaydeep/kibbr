//! The `.col` binary format: one column, one partition, one file.
//!
//! ```text
//! [4]  magic      0x54524442   "TRDB"
//! [1]  col_type   0=Str 1=I64 2=F64 3=DT 4=Bool
//! [1]  dt_form    0=mixed 1=rfc3339 2=epoch_s 3=epoch_ms   (col_type=DT only, else 0)
//! [8]  row_count  u64 LE
//!      ── then row_count cells ──
//!      [1] tag    0x01 value follows, 0xFF null
//!      Str   [u32 len][utf-8 bytes]
//!      I64   [8 bytes LE]
//!      F64   [8 bytes LE]
//!      DT    [8 bytes LE, unix nanos]
//!      Bool  [1 byte]
//! ```
//!
//! **The tag byte is a disambiguation the format needs and its written form does not
//! state.** A null is specified as the sentinel `0xFF` sitting where a cell would be, but
//! little-endian puts the low byte first, so `-1i64`, `255i64` and every value ending
//! `…FF` *begin* with `0xFF`. A reader walking the file forward cannot tell one from a
//! null, and a single wrong call desynchronises every remaining row of every column -
//! silently, and only for some data. One tag byte per cell is the smallest fix that keeps
//! the sentinel exactly the byte the format names, and keeps it as obvious under `xxd` as
//! the magic number is. The denser alternative - a validity bitmap after the header -
//! deletes the sentinel the format argues for.
//!
//! The magic and the type byte earn their four-and-one bytes the same way: a corrupt file
//! should be obvious to a human with `xxd`, which matters more than it sounds like it
//! will the first time a flush is interrupted.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::Durability;
use crate::types::{ColValue, DtForm};

pub const MAGIC: [u8; 4] = *b"TRDB";
/// magic + col_type + dt_form + row_count.
pub const HEADER_BYTES: u64 = 4 + 1 + 1 + 8;
const DT_FORM_AT: u64 = 5;
const ROW_COUNT_AT: u64 = 6;

const TAG_VALUE: u8 = 0x01;
const TAG_NULL: u8 = 0xFF;

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
    /// The same lattice and for the same reason: a column never narrows, so a file
    /// promotes at most twice in its life (`I64 → F64 → Str`) and never back.
    pub fn join(a: ColKind, b: ColKind) -> ColKind {
        match (a, b) {
            _ if a == b => a,
            (ColKind::I64, ColKind::F64) | (ColKind::F64, ColKind::I64) => ColKind::F64,
            _ => ColKind::Str,
        }
    }
}

#[derive(Debug, Error)]
pub enum ColError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: not a .col file - the first four bytes are not TRDB")]
    BadMagic { path: PathBuf },
    #[error("{path}: column type byte {found} is not one of 0..=4")]
    BadType { path: PathBuf, found: u8 },
    #[error("{path}: datetime form byte {found} is not one of 0..=3")]
    BadDtForm { path: PathBuf, found: u8 },
    #[error(
        "{path}: cell {cell} starts with {found:#04x}, which is neither a value (0x01) nor a null (0xff)"
    )]
    BadTag { path: PathBuf, cell: u64, found: u8 },
    #[error("{path}: ends inside cell {cell} of {row_count}")]
    Truncated {
        path: PathBuf,
        cell: u64,
        row_count: u64,
    },
    #[error("{path}: cell {cell} is not utf-8")]
    BadUtf8 { path: PathBuf, cell: u64 },
}

fn io(path: &Path, source: std::io::Error) -> ColError {
    ColError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// What a file's header says.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub kind: ColKind,
    pub dt_form: Option<DtForm>,
    pub row_count: u64,
}

fn read_header(path: &Path, r: &mut impl Read) -> Result<Header, ColError> {
    let mut buf = [0u8; HEADER_BYTES as usize];
    r.read_exact(&mut buf).map_err(|e| io(path, e))?;
    if buf[0..4] != MAGIC {
        return Err(ColError::BadMagic {
            path: path.to_path_buf(),
        });
    }
    let kind = ColKind::from_byte(buf[4]).ok_or(ColError::BadType {
        path: path.to_path_buf(),
        found: buf[4],
    })?;
    // The byte is written 0 for every non-datetime column, and 0 *is* `Mixed`. Only a
    // datetime column's zero means anything, so only there is it read back as a form.
    let dt_form = if kind == ColKind::Dt {
        Some(DtForm::from_byte(buf[5]).ok_or(ColError::BadDtForm {
            path: path.to_path_buf(),
            found: buf[5],
        })?)
    } else {
        None
    };
    let row_count = u64::from_le_bytes(buf[6..14].try_into().unwrap_or([0; 8]));
    Ok(Header {
        kind,
        dt_form,
        row_count,
    })
}

/// The header of an existing file, without reading a cell.
pub fn header_of(path: &Path) -> Result<Header, ColError> {
    let file = File::open(path).map_err(|e| io(path, e))?;
    let mut reader = BufReader::new(file);
    read_header(path, &mut reader)
}

// ── Reading ────────────────────────────────────────────────────────────────────────

/// One column, walked forward.
///
/// Forward-only and no seek table: a `Str` cell is variable-width, so there is no
/// arithmetic that finds row *k*. A partition is read by holding one of these per column
/// and stepping them all in lockstep, which is what makes a column-major file readable
/// row-major without materialising it.
pub struct ColReader {
    path: PathBuf,
    inner: BufReader<File>,
    header: Header,
    at: u64,
}

impl ColReader {
    pub fn open(path: &Path) -> Result<Self, ColError> {
        let file = File::open(path).map_err(|e| io(path, e))?;
        let mut inner = BufReader::new(file);
        let header = read_header(path, &mut inner)?;
        Ok(Self {
            path: path.to_path_buf(),
            inner,
            header,
            at: 0,
        })
    }

    pub fn header(&self) -> Header {
        self.header
    }

    fn tag(&mut self) -> Result<u8, ColError> {
        let mut t = [0u8; 1];
        self.inner
            .read_exact(&mut t)
            .map_err(|_| ColError::Truncated {
                path: self.path.clone(),
                cell: self.at,
                row_count: self.header.row_count,
            })?;
        match t[0] {
            TAG_VALUE | TAG_NULL => Ok(t[0]),
            found => Err(ColError::BadTag {
                path: self.path.clone(),
                cell: self.at,
                found,
            }),
        }
    }

    fn exact(&mut self, buf: &mut [u8]) -> Result<(), ColError> {
        self.inner.read_exact(buf).map_err(|_| ColError::Truncated {
            path: self.path.clone(),
            cell: self.at,
            row_count: self.header.row_count,
        })
    }

    /// The next cell, decoded.
    pub fn cell(&mut self) -> Result<ColValue, ColError> {
        if self.tag()? == TAG_NULL {
            self.at += 1;
            return Ok(ColValue::Null);
        }
        let value = match self.header.kind {
            ColKind::Str => {
                let mut len = [0u8; 4];
                self.exact(&mut len)?;
                let len = u32::from_le_bytes(len) as usize;
                let mut bytes = vec![0u8; len];
                self.exact(&mut bytes)?;
                let text = String::from_utf8(bytes).map_err(|_| ColError::BadUtf8 {
                    path: self.path.clone(),
                    cell: self.at,
                })?;
                ColValue::String(text)
            }
            ColKind::I64 => {
                let mut b = [0u8; 8];
                self.exact(&mut b)?;
                ColValue::Int64(i64::from_le_bytes(b))
            }
            ColKind::F64 => {
                let mut b = [0u8; 8];
                self.exact(&mut b)?;
                ColValue::Float64(f64::from_le_bytes(b))
            }
            ColKind::Dt => {
                let mut b = [0u8; 8];
                self.exact(&mut b)?;
                ColValue::Datetime(i64::from_le_bytes(b))
            }
            ColKind::Bool => {
                let mut b = [0u8; 1];
                self.exact(&mut b)?;
                ColValue::Bool(b[0] != 0)
            }
        };
        self.at += 1;
        Ok(value)
    }

    /// Step past the next cell without decoding it.
    ///
    /// What a row rejected by the time window costs: the timestamp column is decoded, and
    /// every other column pays a seek rather than a `String` allocation.
    pub fn skip_cell(&mut self) -> Result<(), ColError> {
        if self.tag()? == TAG_NULL {
            self.at += 1;
            return Ok(());
        }
        let skip = match self.header.kind {
            ColKind::Str => {
                let mut len = [0u8; 4];
                self.exact(&mut len)?;
                i64::from(u32::from_le_bytes(len))
            }
            ColKind::I64 | ColKind::F64 | ColKind::Dt => 8,
            ColKind::Bool => 1,
        };
        self.inner
            .seek_relative(skip)
            .map_err(|_| ColError::Truncated {
                path: self.path.clone(),
                cell: self.at,
                row_count: self.header.row_count,
            })?;
        self.at += 1;
        Ok(())
    }
}

// ── Writing ────────────────────────────────────────────────────────────────────────

/// One column open for append.
///
/// `row_count` is patched **after** the cells are on disk, never before: a crash between
/// the two leaves bytes past the recorded count, which no reader looks at and the next
/// open truncates. The other order would publish a count for rows that are not there.
pub struct ColFile {
    path: PathBuf,
    out: BufWriter<File>,
    kind: ColKind,
    dt_form: Option<DtForm>,
    row_count: u64,
}

impl ColFile {
    /// Open for append, creating the file with `kind` if it does not exist.
    ///
    /// An existing file keeps the kind its header names; the caller is responsible for
    /// having promoted it first if the batch needs a wider one - see `promote`.
    pub fn open_append(path: &Path, kind: ColKind) -> Result<Self, ColError> {
        let existing = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| io(path, e))?;

        let (kind, dt_form, row_count) = if existing {
            let mut head = BufReader::new(&mut file);
            let h = read_header(path, &mut head)?;
            (h.kind, h.dt_form, h.row_count)
        } else {
            let mut header = Vec::with_capacity(HEADER_BYTES as usize);
            header.extend_from_slice(&MAGIC);
            header.push(kind.byte());
            header.push(0);
            header.extend_from_slice(&0u64.to_le_bytes());
            file.write_all(&header).map_err(|e| io(path, e))?;
            // A fresh datetime file has seen no rung yet, so it claims none: `None` is
            // the identity of `DtForm::merge`, where `Mixed` absorbs everything after it
            // and the first row could never undo it.
            (kind, None, 0)
        };

        file.seek(SeekFrom::End(0)).map_err(|e| io(path, e))?;
        Ok(Self {
            path: path.to_path_buf(),
            out: BufWriter::new(file),
            kind,
            dt_form,
            row_count,
        })
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Fold one row's rung into the file's. See `DtForm::merge`.
    pub fn observe_form(&mut self, form: Option<DtForm>) {
        if self.kind == ColKind::Dt {
            self.dt_form = DtForm::merge(self.dt_form, form);
        }
    }

    /// One cell. `ColValue::Null` - and a column this row does not carry, which reaches
    /// here as the same thing - is the sentinel and no payload.
    pub fn push(&mut self, value: &ColValue) -> Result<(), ColError> {
        let w = &mut self.out;
        let write = |w: &mut BufWriter<File>, bytes: &[u8]| w.write_all(bytes);
        let r = match (self.kind, value) {
            (_, ColValue::Null) => write(w, &[TAG_NULL]),
            // The length prefix is a u32, and a line is capped well below 4 GiB
            // (`lines::MAX_LINE_BYTES`), so the fallback is unreachable. Written as a
            // null rather than a truncation: half a string is worse than no string, and
            // truncating at a byte offset would split a character besides.
            (ColKind::Str, ColValue::String(s)) => match u32::try_from(s.len()) {
                Ok(len) => write(w, &[TAG_VALUE])
                    .and_then(|()| write(w, &len.to_le_bytes()))
                    .and_then(|()| write(w, s.as_bytes())),
                Err(_) => write(w, &[TAG_NULL]),
            },
            (ColKind::I64, ColValue::Int64(i)) => {
                write(w, &[TAG_VALUE]).and_then(|()| write(w, &i.to_le_bytes()))
            }
            (ColKind::F64, ColValue::Float64(f)) => {
                write(w, &[TAG_VALUE]).and_then(|()| write(w, &f.to_le_bytes()))
            }
            (ColKind::Dt, ColValue::Datetime(ns)) => {
                write(w, &[TAG_VALUE]).and_then(|()| write(w, &ns.to_le_bytes()))
            }
            (ColKind::Bool, ColValue::Bool(b)) => {
                write(w, &[TAG_VALUE]).and_then(|()| write(w, &[u8::from(*b)]))
            }
            // The caller promotes the file, or coerces the value, before it gets here.
            // Reaching this arm would write a cell the header does not describe, so the
            // row is dropped to a null rather than written as one type and read as
            // another. Loud in debug; a wrong cell is worse than a missing one.
            (kind, value) => {
                debug_assert!(false, "{kind:?} column handed a {value:?}");
                write(w, &[TAG_NULL])
            }
        };
        r.map_err(|e| io(&self.path, e))?;
        self.row_count += 1;
        Ok(())
    }

    /// Push the cells out, then patch the header. Returns the file's byte length - which
    /// the manifest records so a torn write can be trimmed on open - and the datetime form
    /// the whole partition ended up with.
    ///
    /// The count is patched **after** the body has been written, never before: interrupted
    /// between the two, the file holds bytes past the count it declares, which no reader
    /// looks at and the next open trims. The other order publishes a count for rows that
    /// are not there.
    ///
    /// Under `Durability::Fsync` each step is also forced to the platter. Under `Deferred`
    /// it is not - see `Durability` for why that is the tick's setting and not a shortcut.
    pub fn finish(self, durability: Durability) -> Result<(u64, Option<DtForm>), ColError> {
        let path = self.path;
        let mut file = self
            .out
            .into_inner()
            .map_err(|e| io(&path, e.into_error()))?;
        if durability.is_fsync() {
            file.sync_all().map_err(|e| io(&path, e))?;
        }

        if let Some(form) = self.dt_form {
            file.seek(SeekFrom::Start(DT_FORM_AT))
                .map_err(|e| io(&path, e))?;
            file.write_all(&[form.byte()]).map_err(|e| io(&path, e))?;
        }
        file.seek(SeekFrom::Start(ROW_COUNT_AT))
            .map_err(|e| io(&path, e))?;
        file.write_all(&self.row_count.to_le_bytes())
            .map_err(|e| io(&path, e))?;
        file.flush().map_err(|e| io(&path, e))?;
        if durability.is_fsync() {
            file.sync_all().map_err(|e| io(&path, e))?;
        }

        let len = file.metadata().map_err(|e| io(&path, e))?.len();
        Ok((len, self.dt_form))
    }
}

// ── Promotion ──────────────────────────────────────────────────────────────────────

/// Rewrite a column file into a wider type.
///
/// `types::join` lets a column widen part-way through a day, and a `.col` file holds one
/// type byte for the whole partition, so the file has to be rewritten when it happens.
/// `.tmp` → fsync → rename, the same way `trie.json` is written: a reader either sees the
/// old file or the new one.
///
/// `DT → Str` is the case the `dt_form` byte exists for - it inverts the rung that read
/// the value instead of rendering a canonical timestamp, so a ten-digit order id that
/// rung 2 mistook for a date comes back as ten digits. See `DtForm::render`.
pub fn promote(path: &Path, to: ColKind) -> Result<u64, ColError> {
    let mut reader = ColReader::open(path)?;
    let from = reader.header();
    if from.kind == to {
        return Ok(std::fs::metadata(path).map_err(|e| io(path, e))?.len());
    }

    let tmp = path.with_extension("col.tmp");
    let mut out = ColFile::open_append(&tmp, to)?;
    if to == ColKind::Dt {
        out.observe_form(from.dt_form);
    }
    for _ in 0..from.row_count {
        let cell = reader.cell()?;
        out.push(&coerce(cell, to, from.dt_form))?;
    }
    // Always fsynced: a promotion rewrites a file that already held data, and the rename
    // replacing it is the one step here that cannot be redone from the buffer.
    let (len, _) = out.finish(Durability::Fsync)?;
    std::fs::rename(&tmp, path).map_err(|e| io(path, e))?;
    Ok(len)
}

/// One value widened into `to`. Only the moves the lattice allows ever arrive here.
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
