//! `_columns.json`: what one day partition holds.
//!
//! The layout is one directory per leaf, partitioned by day, one file per column - and
//! nothing about that is TrieDB-specific, which is a property worth protecting. The
//! manifest does not weaken it: a reader that understands `.col` can ignore this file
//! entirely, and a future column format carrying its own footer needs none of it.
//!
//! It earns its place by doing three things a directory listing cannot:
//!
//! - **Recover the column name.** A flattened key can hold `/`, backslashes and control
//!   bytes, so the filename is an escaping of the name and not the name. Inverting the
//!   escape on every read would be a second implementation of it.
//! - **Record each file's byte length.** A flush that is interrupted between writing
//!   cells and patching `row_count` leaves bytes past the end of the file the header
//!   describes. With the length recorded, recovery is a `set_len`; without it, a re-scan.
//! - **Hold the authoritative row count**, which every column header is then checked
//!   against. One disagreement is a corrupt partition rather than a silently short read.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::col::ColKind;
use super::Durability;
use crate::types::DtForm;

/// What `repair` found wrong with a partition on the way in.
#[derive(Debug)]
pub enum Repaired {
    Trimmed {
        path: PathBuf,
        was: u64,
        now: u64,
    },
    Short {
        path: PathBuf,
        expected: u64,
        found: u64,
    },
}

impl std::fmt::Display for Repaired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Repaired::Trimmed { path, was, now } => write!(
                f,
                "{}: trimmed {was} bytes to {now} - a flush wrote cells and did not reach \
                 the step that records them",
                path.display()
            ),
            Repaired::Short {
                path,
                expected,
                found,
            } => write!(
                f,
                "{}: {found} bytes where {expected} were recorded - rows were lost to an \
                 unclean exit, and this partition is read as far as it goes",
                path.display()
            ),
        }
    }
}

pub const MANIFEST: &str = "_columns.json";
/// The partition holding rows whose timestamp column is not a readable `Datetime`.
pub const UNDATED: &str = "undated";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnEntry {
    /// The column's real name, as the flattener rendered it.
    pub name: String,
    /// Its filename inside this directory.
    pub file: String,
    pub col_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dt_form: Option<DtForm>,
    /// Byte length at the last successful flush.
    pub bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub row_count: u64,
    /// The extremes of the partitioning column over these rows. `None` on the undated
    /// partition, and on a partition of rows that carry no timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ts: Option<i64>,
    pub columns: Vec<ColumnEntry>,
}

impl Manifest {
    pub fn read(dir: &Path) -> Option<Self> {
        let text = fs::read(dir.join(MANIFEST)).ok()?;
        serde_json::from_slice(&text).ok()
    }

    /// `.tmp` → rename, so a reader sees either the whole manifest or the previous one and
    /// never half of either. Under `Durability::Fsync` the write and the directory entry are
    /// forced down too; under `Deferred` the rename is still atomic within this process,
    /// which is what a query needs. Surviving a crash needs the write-ahead log.
    pub fn write(&self, dir: &Path, durability: Durability) -> Result<(), std::io::Error> {
        let final_path = dir.join(MANIFEST);
        let tmp = dir.join(format!("{MANIFEST}.tmp"));
        let body = serde_json::to_vec_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        {
            use std::io::Write;
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&body)?;
            if durability.is_fsync() {
                file.sync_all()?;
            }
        }
        fs::rename(&tmp, &final_path)?;
        // The rename itself is only durable once the directory entry is. Skipped rather
        // than failed where the platform will not open a directory: the data is already
        // written, and refusing to finish a flush over it would be worse.
        if durability.is_fsync() {
            if let Ok(handle) = fs::File::open(dir) {
                let _ = handle.sync_all();
            }
        }
        Ok(())
    }

    /// Reconcile each file against the length the last flush recorded.
    ///
    /// Two directions, two different faults:
    ///
    /// - **Longer than recorded.** A flush wrote cells and did not reach the step that
    ///   patches the count. Harmless to read - nothing looks past `row_count` - but fatal
    ///   to append onto, because the next batch would land after the stray bytes and every
    ///   cell would be one gap out for the rest of the file's life. Trimmed.
    /// - **Shorter than recorded.** The manifest was renamed into place and the cells it
    ///   describes never reached the disk, which is what an unclean exit buys while the
    ///   flush tick does not fsync. Nothing here can put them back; it is reported so the
    ///   loss is visible, and the reader stops at the tear rather than failing the query.
    pub fn repair(&self, dir: &Path) -> Vec<Repaired> {
        let mut found = Vec::new();
        for entry in &self.columns {
            let path = dir.join(&entry.file);
            let Ok(meta) = fs::metadata(&path) else {
                continue;
            };
            if meta.len() > entry.bytes {
                if let Ok(file) = fs::OpenOptions::new().write(true).open(&path) {
                    if file.set_len(entry.bytes).is_ok() {
                        let _ = file.sync_all();
                        found.push(Repaired::Trimmed {
                            path,
                            was: meta.len(),
                            now: entry.bytes,
                        });
                    }
                }
            } else if meta.len() < entry.bytes {
                found.push(Repaired::Short {
                    path,
                    expected: entry.bytes,
                    found: meta.len(),
                });
            }
        }
        found
    }
}

pub fn kind_name(kind: ColKind) -> String {
    kind.as_str().to_string()
}

pub fn kind_of_name(name: &str) -> Option<ColKind> {
    match name {
        "Str" => Some(ColKind::Str),
        "I64" => Some(ColKind::I64),
        "F64" => Some(ColKind::F64),
        "DT" => Some(ColKind::Dt),
        "Bool" => Some(ColKind::Bool),
        _ => None,
    }
}

// ── Column names are not filenames ─────────────────────────────────────────────────

/// The longest escaped name written literally. Past this the name is hashed, because
/// every filesystem worth supporting refuses a component of 256 bytes and a flattened key
/// has no length bound of its own.
const MAX_LITERAL: usize = 180;

/// A column's filename inside a partition directory.
///
/// A flattened key is rendered by `key::KeyPath`, which escapes `.` and `\` and passes
/// everything else through - so a column can be named `a/b`, can hold control bytes, can
/// be empty, and can be kilobytes long. None of those is a filename.
///
/// **Uppercase is escaped too, and that is not fussiness.** APFS is case-insensitive by
/// default, so `userId` and `userid` - two distinct columns, both perfectly ordinary -
/// would open the same file on the machine this is written on and two different files in
/// CI. The second column's cells would append into the first column's file and destroy
/// both, on one platform only. Folding the escape over case removes the case entirely.
///
/// A leading `_` is escaped so `_columns.json` cannot be reached by a column name.
pub fn file_name(column: &str) -> String {
    let mut out = String::with_capacity(column.len() + 8);
    for (i, byte) in column.bytes().enumerate() {
        let safe =
            matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-') || (byte == b'_' && i > 0);
        if safe {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    if out.len() > MAX_LITERAL {
        // The manifest carries the real name, so the filename only has to be stable and
        // distinct. A collision inside one partition is caught at write time, where two
        // manifest entries would claim one file.
        out = format!("h{:016x}", fnv1a(column.as_bytes()));
    }
    out.push_str(".col");
    out
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
