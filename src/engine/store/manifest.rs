// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! `manifest.log`: the truth about which data files are live.
//!
//! Framed as `catalog.log` is. A file the manifest does not name is not part of the
//! database, so every change to the set of files is one appended record and a crash lands
//! on one side of it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::engine::templates::catalog;
use crate::engine::format::frame::{self, Log, LogError};

pub(crate) const MANIFEST_FILE_NAME: &str = "manifest.log";

/// A payload longer than this is a bad length, not a record.
const MAX_PAYLOAD: u32 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("{path}: could not serialise a manifest record: {source}")]
    Serialize {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

/// One data file, by its path under `data/` and the id in its header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileRecord {
    pub path: String,
    /// The header's file id, as 32 hex digits: a path is a place, not an identity.
    pub file_id: String,
    #[serde(default)]
    pub sealed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_ts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ts: Option<i64>,
    #[serde(default)]
    pub finished: bool,
}

impl FileRecord {
    /// A file just created by the live door: named, not yet sealed.
    pub(crate) fn open(path: String, file_id: &[u8; 16]) -> Self {
        Self {
            path,
            file_id: hex(file_id),
            sealed: false,
            rows: None,
            min_ts: None,
            max_ts: None,
            finished: false,
        }
    }

    /// A sealed file, with the numbers its footer holds.
    pub(crate) fn sealed(path: String, index: &crate::engine::format::file::Index) -> Self {
        Self {
            path,
            file_id: hex(&index.file_id),
            sealed: true,
            rows: Some(index.row_count()),
            min_ts: Some(index.min_time()),
            max_ts: Some(index.max_time()),
            finished: index.finished,
        }
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// What one of the digits `hex` writes is worth. Not `from_str_radix`, which takes a sign,
/// and no capitals: a start matches a file's id to the manifest's as the text `hex` writes.
fn hex_digit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

/// The file id 32 hex digits spell, `None` for anything `hex` does not write.
pub(crate) fn file_id_of(hex: &str) -> Option<[u8; 16]> {
    let digits = hex.as_bytes();
    if digits.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (byte, pair) in id.iter_mut().zip(digits.chunks_exact(2)) {
        *byte = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Some(id)
}

/// A compaction's `source`, never read as a load's: compaction writes its files in place,
/// so none is looked for in `staging/`.
pub(crate) const COMPACT_SOURCE: &str = "compact";

/// A bulk load's `source`: `load:` and its id as 16 hex digits.
pub(crate) fn load_source(load_id: u64) -> String {
    format!("load:{load_id:016x}")
}

/// The load id a `source` names, `None` for anything `load_source` does not write.
pub(crate) fn load_of(source: &str) -> Option<u64> {
    let digits = source.strip_prefix("load:")?.as_bytes();
    if digits.len() != 16 {
        return None;
    }
    digits.iter().try_fold(0u64, |id, digit| {
        Some((id << 4) | u64::from(hex_digit(*digit)?))
    })
}

/// Every load a record names as its source, committed or since retired, so running one
/// again is refused by its id.
pub(crate) fn load_ids(records: &[Record]) -> std::collections::BTreeSet<u64> {
    records
        .iter()
        .filter_map(|record| match record {
            Record::Add { source, .. } | Record::Replace { source, .. } => {
                source.as_deref().and_then(load_of)
            }
            _ => None,
        })
        .collect()
}

/// The load that published each path: where startup looks for a file a commit named and
/// a crash left in `staging/<load id>/`.
pub(crate) fn published_by_load(records: &[Record]) -> BTreeMap<String, u64> {
    let mut out = BTreeMap::new();
    for record in records {
        let (files, source) = match record {
            Record::Add { files, source } => (files, source),
            Record::Replace { add, source, .. } => (add, source),
            _ => continue,
        };
        match source.as_deref().and_then(load_of) {
            Some(id) => {
                for f in files {
                    out.insert(f.path.clone(), id);
                }
            }
            None => {
                for f in files {
                    out.remove(&f.path);
                }
            }
        }
    }
    out
}

/// What a replace-a-day tells the write-ahead log's replay: `day`'s lines up to `through`
/// were replaced, and without it a replayed day comes back beside its replacement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WalMark {
    /// The log's id, as 16 hex digits.
    pub stream: String,
    /// The day's directory name.
    pub day: String,
    pub through: u64,
}

impl WalMark {
    pub(crate) fn new(stream: u64, day: String, through: u64) -> Self {
        Self {
            stream: format!("{stream:016x}"),
            day,
            through,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Record {
    Add {
        files: Vec<FileRecord>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
    },
    Seal {
        files: Vec<FileRecord>,
    },
    Remove {
        files: Vec<String>,
    },
    /// Files added and files retired in one step: replace-a-day, and compaction.
    Replace {
        add: Vec<FileRecord>,
        remove: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<String>,
        /// A replace-a-day's word to the write-ahead log; never a compaction's, whose rebuilt
        /// row groups carry the ranges of the ones they replace.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wal: Option<WalMark>,
    },
}

const KNOWN_KINDS: &[&str] = &["add", "seal", "remove", "replace"];

/// What one intact payload is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decoded {
    Record(Record),
    /// A kind this build does not know, skipped so a later version can add kinds.
    Unknown(String),
    /// An intact frame whose payload is not one of the shapes above, with the reason;
    /// skipped too.
    Malformed(String),
}

/// One intact payload, read through the catalog's `read_record` so an array in the place
/// of an object is malformed; never an error.
pub(crate) fn decode(payload: &[u8]) -> Decoded {
    match catalog::read_record::<Record>(payload) {
        Ok(record) => Decoded::Record(record),
        Err(e) => match catalog::kind_of(payload) {
            Some(kind) if !KNOWN_KINDS.contains(&kind.as_str()) => Decoded::Unknown(kind),
            _ => Decoded::Malformed(e),
        },
    }
}

/// Every intact frame of `bytes` as `(offset, what it is)` and the offset of the first
/// torn byte: what `Manifest::open` does, without the file, for the `record_log` fuzz target.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn parse(bytes: &[u8]) -> (Vec<(u64, Decoded)>, Option<u64>) {
    let (frames, torn_at) = frame::read_all(bytes, MAX_PAYLOAD);
    let decoded = frames
        .into_iter()
        .map(|(at, payload)| (at, decode(payload)))
        .collect();
    (decoded, torn_at)
}

/// The live files, by path, after every record is applied in order.
pub(crate) fn live(records: &[Record]) -> BTreeMap<String, FileRecord> {
    let mut files = BTreeMap::new();
    for record in records {
        apply(&mut files, record);
    }
    files
}

/// One record applied to a live set, shared by `live` and a running store so the two
/// cannot disagree.
pub(crate) fn apply(files: &mut BTreeMap<String, FileRecord>, record: &Record) {
    match record {
        Record::Add { files: added, .. } | Record::Seal { files: added } => {
            for f in added {
                files.insert(f.path.clone(), f.clone());
            }
        }
        Record::Remove { files: gone } => {
            for path in gone {
                files.remove(path);
            }
        }
        Record::Replace { add, remove, .. } => {
            for path in remove {
                files.remove(path);
            }
            for f in add {
                files.insert(f.path.clone(), f.clone());
            }
        }
    }
}

/// An open manifest, positioned at its end.
pub(crate) struct Manifest {
    log: Log,
}

impl Manifest {
    /// Open for appending, reading every record already there; a torn tail is truncated and
    /// an unknown or malformed record skipped, each with a warning.
    pub(crate) fn open(
        path: &Path,
        warn: &mut dyn FnMut(String),
    ) -> Result<(Self, Vec<Record>), ManifestError> {
        let display = path.display().to_string();
        let (log, opened) = Log::open_with(path, MAX_PAYLOAD, &mut frame::sync_dir)?;
        let mut records = Vec::new();
        for (at, payload) in opened.payloads() {
            match decode(payload) {
                Decoded::Record(record) => records.push(record),
                Decoded::Unknown(kind) => warn(format!(
                    "{display}: record at offset {at} has kind `{kind}`, which this build \
                     does not know; skipped"
                )),
                Decoded::Malformed(e) => warn(format!(
                    "{display}: record at offset {at} is intact but not readable ({e}); \
                     skipped"
                )),
            }
        }
        if let Some(offset) = opened.torn_at {
            warn(format!(
                "{display}: torn record at offset {offset} of {}; truncated to the {} intact \
                 record(s) before it",
                opened.bytes.len(),
                records.len()
            ));
        }
        Ok((Self { log }, records))
    }

    /// Append and return only once every byte is on disk.
    pub(crate) fn append(&mut self, records: &[Record]) -> Result<(), ManifestError> {
        let mut payloads = Vec::with_capacity(records.len());
        for record in records {
            payloads.push(serde_json::to_vec(record).map_err(|source| {
                ManifestError::Serialize {
                    path: self.log.path().display().to_string(),
                    source,
                }
            })?);
        }
        Ok(self.log.append(&payloads)?)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> u64 {
        self.log.len()
    }

    /// Whether a failed append could not be cut back, so no record is written until a restart.
    pub(crate) fn is_torn(&self) -> bool {
        self.log.is_torn()
    }

    /// Flush the log as it stands: what tells a failed append that was cut back from one that
    /// may be on disk.
    pub(crate) fn sync(&mut self) -> Result<(), ManifestError> {
        Ok(self.log.sync()?)
    }

    /// A manifest whose every append fails and cannot be cut back, as on a full disk.
    #[cfg(test)]
    pub fn read_only(path: &Path) -> Result<Self, ManifestError> {
        Ok(Self {
            log: Log::read_only(path)?,
        })
    }
}

pub fn path_in(base: &Path) -> PathBuf {
    base.join(MANIFEST_FILE_NAME)
}

#[cfg(test)]
mod tests;
