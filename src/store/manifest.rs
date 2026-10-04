//! `manifest.log`: the truth about which data files are live.
//!
//! Framed exactly as `catalog.log` is - `frame::Log` underneath both - one JSON object
//! per record:
//!
//! ```text
//! {"kind":"add",    "files":[{"path":"2024-01-15/seg_0_000007.col","file_id":"…",
//!                             "sealed":true,"rows":81234,"min_ts":…,"max_ts":…,
//!                             "finished":true}], "source":"load:9f3a…"}
//! {"kind":"seal",   "files":[{…the same fields, now known…}]}
//! {"kind":"remove", "files":["2024-01-15/seg_0_000003.col", …]}
//! {"kind":"replace","add":[…],"remove":[…],"source":"load:…"}   one step: compaction, or replace-a-day
//! ```
//!
//! A replace-a-day under a write-ahead log also carries `"wal":{"stream":"…","day":
//! "2024-01-15","through":81234}`: the lines of that day the log held up to that position
//! were in the files this record retired, and a replay must not bring them back
//! (`WalMark`).
//!
//! **A file the manifest does not name is not part of the database.** Startup deletes
//! such files. A load's files it does name but that a crash left in `staging/` are moved
//! into place first - the record was the commit - and then the rest of `staging/` is
//! deleted. So every change to the set of files is one appended record, and a crash lands
//! cleanly on one side of it.
//!
//! **Why `seal` is a kind of its own.** The live door names a file the moment it creates
//! it - before its first row group, so a crash cannot leave durable row groups in a file
//! the next start would delete - and only knows its row count and time range once it is
//! sealed. An `add` says "this file is live and still being written"; the `seal` for the
//! same path says it is closed, with the numbers startup reads instead of opening it. A
//! file named and never sealed is one a crash left, and startup recovers it. A load's
//! files are sealed before they are named, so its `add` carries `"sealed":true` and needs
//! no second record.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::catalog;
use crate::frame::{self, Log, LogError};

pub const MANIFEST_FILE_NAME: &str = "manifest.log";

/// A payload longer than this is a bad length, not a record. A replace naming every file of
/// a busy day is a few hundred kilobytes.
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
pub struct FileRecord {
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
    pub fn open(path: String, file_id: &[u8; 16]) -> Self {
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
    pub fn sealed(path: String, index: &crate::store::v3::file::Index) -> Self {
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

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// What one of the digits `hex` writes is worth: `0` to `9` and `a` to `f`, and nothing
/// else.
///
/// Not `from_str_radix`, which reads a number and so takes a sign: `+f` was the byte 15
/// to it, and `load:+000000000000001` the load 1. And not the capitals: no build has
/// written one, and a start holds a file's id to the manifest's by comparing the text
/// `hex` writes, so an id spelled `0A…` names no file even when it reads as one.
fn hex_digit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

/// The file id 32 hex digits spell: `hex` read back, and `None` for anything `hex` does
/// not write.
pub fn file_id_of(hex: &str) -> Option<[u8; 16]> {
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

/// A compaction's `source`. Not a load's, so no load id is read out of it and no file it
/// added is ever looked for in `staging/`: compaction writes its files in place.
pub const COMPACT_SOURCE: &str = "compact";

/// A bulk load's `source`: `load:` and its id as 16 hex digits.
pub fn load_source(load_id: u64) -> String {
    format!("load:{load_id:016x}")
}

/// The load id a `source` names, if it names one: `load_source` read back, and `None` for
/// anything it does not write.
pub fn load_of(source: &str) -> Option<u64> {
    let digits = source.strip_prefix("load:")?.as_bytes();
    if digits.len() != 16 {
        return None;
    }
    digits.iter().try_fold(0u64, |id, digit| {
        Some((id << 4) | u64::from(hex_digit(*digit)?))
    })
}

/// Every load a record names as its source, committed or since retired: running one of them
/// again is refused by its id, whatever became of its files.
pub fn load_ids(records: &[Record]) -> std::collections::BTreeSet<u64> {
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

/// The load that published each path, by the record that added it: where startup looks for
/// a file a commit named and a crash left in `staging/<load id>/`.
pub fn published_by_load(records: &[Record]) -> BTreeMap<String, u64> {
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

/// What a replace-a-day tells the write-ahead log's replay: every line of `day` the log
/// `stream` holds at a position up to `through` was replaced, and is not to be stored
/// again.
///
/// **Without it a replaced day comes back.** A replay stores every line no durable row
/// group's range covers, and the row groups that covered this day's lines are in the
/// files the record retires: after a crash the lines the log still holds past its
/// checkpoint would be covered by nothing, and stored again beside the load that replaced
/// them. `through` is the last position applied when the day was written out and retired,
/// with the log's applier held still across both - so it is exactly the lines the retired
/// files held, and a line applied after it is one that arrived after the replace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalMark {
    /// The log's id, as 16 hex digits.
    pub stream: String,
    /// The day's directory name.
    pub day: String,
    pub through: u64,
}

impl WalMark {
    pub fn new(stream: u64, day: String, through: u64) -> Self {
        Self {
            stream: format!("{stream:016x}"),
            day,
            through,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
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
        /// A replace-a-day's word to the write-ahead log. Never a compaction's: its
        /// rebuilt row groups carry the ranges of the ones they replace.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wal: Option<WalMark>,
    },
}

const KNOWN_KINDS: &[&str] = &["add", "seal", "remove", "replace"];

/// What one intact payload is.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    Record(Record),
    /// A kind this build does not know, by its name. Skipped, so a later version can add
    /// kinds.
    Unknown(String),
    /// The CRC held and the payload is not one of the shapes above, with the reason.
    /// Skipped too: the framing says these are the bytes that were written.
    Malformed(String),
}

/// One intact payload, read. Never an error: a payload that is not a record is one of the
/// two skips.
///
/// Through the catalog's `read_record`, so a record is one JSON object and each file and
/// each `wal` mark in it is one: an array in the place of any of them is malformed, where
/// serde alone read `["remove",[…]]` as a record retiring a live file.
pub fn decode(payload: &[u8]) -> Decoded {
    match catalog::read_record::<Record>(payload) {
        Ok(record) => Decoded::Record(record),
        Err(e) => match catalog::kind_of(payload) {
            Some(kind) if !KNOWN_KINDS.contains(&kind.as_str()) => Decoded::Unknown(kind),
            _ => Decoded::Malformed(e),
        },
    }
}

/// Every intact frame from the start of `bytes` as `(offset, what it is)`, and the offset
/// of the first byte that is not one - `None` when the bytes end on a record boundary.
///
/// What `Manifest::open` does with a file's bytes, without the file: the same frame
/// reader under the same length cap, and the same `decode`. The `record_log` fuzz target
/// reads through this.
pub fn parse(bytes: &[u8]) -> (Vec<(u64, Decoded)>, Option<u64>) {
    let (frames, torn_at) = frame::read_all(bytes, MAX_PAYLOAD);
    let decoded = frames
        .into_iter()
        .map(|(at, payload)| (at, decode(payload)))
        .collect();
    (decoded, torn_at)
}

/// The live files, by path, after every record is applied in order: the last word on a
/// path wins, and a removed path is gone.
pub fn live(records: &[Record]) -> BTreeMap<String, FileRecord> {
    let mut files = BTreeMap::new();
    for record in records {
        apply(&mut files, record);
    }
    files
}

/// One record applied to a live set: what `live` does for each record in turn, and what a
/// running store does for each record it appends, so the two cannot disagree.
pub fn apply(files: &mut BTreeMap<String, FileRecord>, record: &Record) {
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
pub struct Manifest {
    log: Log,
}

impl Manifest {
    /// Open for appending, reading every record already there. A torn tail is truncated
    /// on disk and warned about with its offset; a record of a kind this build does not
    /// know, or one whose CRC holds and whose payload is not one of the shapes above, is
    /// skipped with a WARN - the catalog's rules, through the catalog's reader.
    pub fn open(
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
    pub fn append(&mut self, records: &[Record]) -> Result<(), ManifestError> {
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

    pub fn len(&self) -> u64 {
        self.log.len()
    }

    /// Whether a failed append could not be cut back, so no record is written until a
    /// restart.
    pub fn is_torn(&self) -> bool {
        self.log.is_torn()
    }

    /// Flush the log as it stands: what tells a failed append that was cut back - and is
    /// then certainly not on disk - from one that may be.
    pub fn sync(&mut self) -> Result<(), ManifestError> {
        Ok(self.log.sync()?)
    }

    /// A manifest whose every append fails, and cannot be cut back: what a full disk or a
    /// dead mount looks like.
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
mod tests {
    use super::*;

    fn file(path: &str, sealed: bool) -> FileRecord {
        FileRecord {
            path: path.into(),
            file_id: hex(&[7; 16]),
            sealed,
            rows: sealed.then_some(3),
            min_ts: sealed.then_some(1),
            max_ts: sealed.then_some(2),
            finished: false,
        }
    }

    #[test]
    fn the_last_word_on_a_path_wins_and_a_removed_path_is_gone() {
        let records = vec![
            Record::Add {
                files: vec![file("d/a.col", false), file("d/b.col", false)],
                source: None,
            },
            Record::Seal {
                files: vec![file("d/a.col", true)],
            },
            Record::Remove {
                files: vec!["d/b.col".into()],
            },
            Record::Replace {
                add: vec![file("d/c.col", true)],
                remove: vec!["d/a.col".into()],
                source: None,
                wal: None,
            },
        ];
        let live = live(&records);
        assert_eq!(live.keys().collect::<Vec<_>>(), vec!["d/c.col"]);
    }

    #[test]
    fn a_file_id_reads_back_from_its_hex_and_nothing_else_does() {
        let id: [u8; 16] = std::array::from_fn(|at| at as u8 * 17);
        assert_eq!(file_id_of(&hex(&id)), Some(id));
        assert_eq!(file_id_of("00ff"), None);
        assert_eq!(file_id_of(&"g".repeat(32)), None);
        assert_eq!(
            load_of(COMPACT_SOURCE),
            None,
            "a compaction reads as a load"
        );
    }

    /// The digits are the sixteen `hex` writes. A sign is not one - `from_str_radix` took
    /// `+f` for a byte and `+000000000000001` for a load - and neither is a capital,
    /// which nothing has written and a start would not match to a file.
    #[test]
    fn a_sign_or_a_capital_is_not_a_hex_digit() {
        assert_eq!(file_id_of(&"+f".repeat(16)), None);
        assert_eq!(file_id_of(&"-0".repeat(16)), None);
        assert_eq!(file_id_of(&"0A".repeat(16)), None);
        assert_eq!(file_id_of(&"é".repeat(16)), None);
        assert_eq!(file_id_of(&"0a".repeat(16)), Some([0x0a; 16]));
        assert_eq!(file_id_of(&hex(&[0xff; 16])), Some([0xff; 16]));

        assert_eq!(load_of("load:+000000000000001"), None);
        assert_eq!(load_of("load:-000000000000001"), None);
        assert_eq!(load_of("load:9F3A000000000001"), None);
        assert_eq!(load_of("load:0000000000000001"), Some(1));
        assert_eq!(load_of(&load_source(u64::MAX)), Some(u64::MAX));
        let signed = Record::Add {
            files: vec![],
            source: Some("load:+000000000000001".into()),
        };
        assert!(load_ids(&[signed]).is_empty());
    }

    /// A record is one JSON object, and so is each file and each `wal` mark in it.
    /// serde's derived readers take an array for any of the three, and each of these read
    /// as a record nobody wrote: one retiring a live file, one naming a file, one telling
    /// a replay which lines to drop. All are malformed, and the reason names the place.
    #[test]
    fn a_json_array_where_the_format_has_an_object_is_malformed() {
        let id = hex(&[7; 16]);
        let arrays = [
            (
                r#"["remove",["2024-01-15/seg_0_000001.col"]]"#.to_string(),
                "the payload is a JSON array",
            ),
            (
                format!(r#"{{"kind":"add","files":[["d/a.col","{id}"]]}}"#),
                "`files[0]` is a JSON array",
            ),
            (
                format!(r#"{{"kind":"seal","files":[{{"path":"d/a.col","file_id":"{id}"}},["d/b.col","{id}",true]]}}"#),
                "`files[1]` is a JSON array",
            ),
            (
                r#"{"kind":"replace","add":[],"remove":[],"wal":["1122334455667788","2024-01-15",5]}"#
                    .to_string(),
                "`wal` is a JSON array",
            ),
        ];
        for (payload, want) in &arrays {
            match decode(payload.as_bytes()) {
                Decoded::Malformed(why) => {
                    assert!(
                        why.contains(want),
                        "{payload}: `{why}` does not say `{want}`"
                    );
                }
                other => panic!("{payload} read as {other:?}"),
            }
        }
        // An array has no kind, so it is not an unknown one either.
        assert!(matches!(
            decode(br#"["compact",[]]"#),
            Decoded::Malformed(_)
        ));

        // The same as objects are records, and an unknown kind is still one.
        let objects = [
            r#"{"kind":"remove","files":["2024-01-15/seg_0_000001.col"]}"#.to_string(),
            format!(r#"{{"kind":"add","files":[{{"path":"d/a.col","file_id":"{id}"}}]}}"#),
            r#"{"kind":"replace","add":[],"remove":[],"wal":{"stream":"1122334455667788","day":"2024-01-15","through":5}}"#
                .to_string(),
        ];
        for payload in &objects {
            assert!(
                matches!(decode(payload.as_bytes()), Decoded::Record(_)),
                "{payload}"
            );
        }
        assert_eq!(
            decode(br#"{"kind":"compact","files":[]}"#),
            Decoded::Unknown("compact".into())
        );
    }

    /// A load's id comes back from its `source`, on an `add` and on a `replace` alike, and a
    /// record from before replace carried one still reads.
    #[test]
    fn a_load_is_named_by_its_source_on_add_and_replace() {
        let id = 0x9f3a_0000_0000_0001;
        assert_eq!(load_source(id), "load:9f3a000000000001");
        assert_eq!(load_of(&load_source(id)), Some(id));
        assert_eq!(load_of("load:9f3a"), None);
        let records = vec![
            Record::Add {
                files: vec![file("d/a.col", true)],
                source: Some(load_source(id)),
            },
            Record::Replace {
                add: vec![file("d/b.col", true)],
                remove: vec!["d/a.col".into()],
                source: Some(load_source(id + 1)),
                wal: None,
            },
        ];
        assert_eq!(
            load_ids(&records).into_iter().collect::<Vec<_>>(),
            vec![id, id + 1]
        );
        let by = published_by_load(&records);
        assert_eq!(by.get("d/b.col"), Some(&(id + 1)));
        let old: Record =
            serde_json::from_str(r#"{"kind":"replace","add":[],"remove":["d/a.col"]}"#)
                .expect("a replace with no source");
        assert!(matches!(old, Record::Replace { source: None, .. }));
    }

    /// The catalog's reader, so the catalog's truncation rule: every cut of a manifest
    /// reads back the records wholly before it, and reopening leaves the file there.
    #[test]
    fn a_manifest_cut_at_every_offset_keeps_the_records_before_the_cut() {
        let dir = std::env::temp_dir().join(format!("kibbr-manifest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = path_in(&dir);
        let records = vec![
            Record::Add {
                files: vec![file("2024-01-15/seg_0_000001.col", false)],
                source: None,
            },
            Record::Seal {
                files: vec![file("2024-01-15/seg_0_000001.col", true)],
            },
        ];
        {
            let (mut m, got) = Manifest::open(&path, &mut |_| {}).expect("create");
            assert!(got.is_empty());
            m.append(&records).expect("append");
        }
        let whole = std::fs::read(&path).expect("read");
        let first = frame::HEADER + serde_json::to_vec(&records[0]).expect("json").len();
        for cut in 0..=whole.len() {
            std::fs::write(&path, &whole[..cut]).expect("write");
            let (m, got) = Manifest::open(&path, &mut |_| {}).expect("open");
            let want = if cut == whole.len() {
                2
            } else if cut >= first {
                1
            } else {
                0
            };
            assert_eq!(got.len(), want, "cut at {cut}");
            let kept = if want == 2 {
                whole.len()
            } else if want == 1 {
                first
            } else {
                0
            };
            assert_eq!(m.len(), kept as u64, "cut at {cut}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
