//! `catalog.log`: the truth about templates. Append-only, CRC-framed, one JSON object per
//! record.
//!
//! ```text
//! record   [4] payload_len  u32 LE
//!          [4] crc          u32 LE, CRC-32C over the payload
//!          [N] payload      one JSON object
//! ```
//!
//! **Why this file exists.** `trie.json` is written in one place, the clean shutdown path,
//! and nothing else on disk could rebuild a trie: `_leaf.json` holds a key set but no
//! promoted values and no column types. Delete the snapshot, crash before it is written,
//! or fill the disk, and every leaf directory became an orphan and every query answered 0.
//! The catalog is written *before* the line that needs it counts as ingested, so a
//! template exists on disk from the moment it exists at all.
//!
//! **The write rule.** A record is appended and `sync_all()` has returned Ok before the
//! line that caused it counts as ingested. New templates and widenings are rare, so the
//! cost at steady state is nothing. If the append fails the line is rejected with a
//! per-line error saying why, and the template is not created in memory either - a trie
//! that knows a template the disk does not is exactly the state this file removes.
//!
//! **Reading.** From the start. A bad length or CRC at the tail is a torn write: the file
//! is truncated there and a WARN names the offset. A record whose `kind` this build does
//! not know is skipped with a WARN, so a later version can add kinds, and so is an intact
//! payload that is not one JSON object of a shape this file has (`read_record`). None of
//! these is ever a failed start: the same reasoning `triefile` gives - an operator cannot
//! repair this by hand, and a crash loop is worse than a warning.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::frame::{self, Log, LogError};
use kibbr_core::flatten::{key_token, promoted_token};
use kibbr_core::types::ColumnType;

/// The file, inside the data directory so one path moves everything.
pub const CATALOG_FILE_NAME: &str = "catalog.log";

/// `payload_len` + `crc`.
#[cfg(test)]
const HEADER: usize = frame::HEADER;

/// A payload longer than this is a bad length, not a record. A template record is a few
/// hundred bytes; a length field reading in the megabytes is a torn or foreign write,
/// and treating it as a tear stops the reader from waiting on bytes that will never come.
const MAX_PAYLOAD: u32 = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: could not serialise a {kind} record: {source}")]
    Serialize {
        path: String,
        kind: &'static str,
        #[source]
        source: serde_json::Error,
    },
    /// An earlier append failed *and* the partial write could not be rolled back, so the
    /// tail of the file is not a record boundary. Anything appended after it would sit
    /// behind a tear and be truncated away on the next start, so nothing is.
    #[error("{path}: an earlier append left a torn record at offset {offset} that could not be removed; no further records are written until the process restarts")]
    Torn { path: String, offset: u64 },
    /// The catalog was just created and its directory could not be fsynced, so the file's
    /// own entry may not survive a crash - and a record fsynced into a file that is not
    /// there after the crash was never durable.
    #[error("{path}: created {CATALOG_FILE_NAME} here and could not fsync the directory, so the file itself may not survive a crash: {source}")]
    SyncDir {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<LogError> for CatalogError {
    fn from(e: LogError) -> Self {
        match e {
            LogError::Io { path, source } => CatalogError::Io { path, source },
            LogError::Torn { path, offset } => CatalogError::Torn { path, offset },
            LogError::SyncDir { path, source, .. } => CatalogError::SyncDir { path, source },
        }
    }
}

// ── Records ────────────────────────────────────────────────────────────────────────

/// One element of a template's path, in walk order: promoted tokens first, then the
/// rest sorted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Token {
    Promoted { promoted: String, value: String },
    Key { key: String },
}

impl Token {
    /// The edge label this token is in the trie: the one `ParsedLog::walk_tokens` gave
    /// the line that made the template, `=` in the key escaped. The record holds the key;
    /// spelled here any other way, a template comes back from a restart under a label no
    /// line of its shape walks to.
    pub fn label(&self) -> String {
        match self {
            Token::Promoted { promoted, value } => promoted_token(promoted, value),
            Token::Key { key } => key_token(key).into_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Template {
        id: u64,
        created_at: DateTime<Utc>,
        tokens: Vec<Token>,
    },
    /// A column's type. Written once when the template is created and again whenever
    /// the type join changes it, so the last record for a `(template_id, name)` wins.
    /// `type` is `null` for a column that has only ever held nulls - a different fact from
    /// `String`, and the trie keeps them apart.
    Column {
        template_id: u64,
        name: String,
        #[serde(rename = "type")]
        col_type: Option<ColumnType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        widened_from: Option<ColumnType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<DateTime<Utc>>,
    },
    /// The time source, `column:<name>` or `arrival`. Recorded so the choice is durable:
    /// the next start makes the same one whatever the configured list says by then.
    TimeSource { template_id: u64, source: String },
}

impl Record {
    fn kind(&self) -> &'static str {
        match self {
            Record::Template { .. } => "template",
            Record::Column { .. } => "column",
            Record::TimeSource { .. } => "time_source",
        }
    }
}

/// What one pass over the bytes found.
#[derive(Debug, Default)]
pub struct Parsed {
    pub records: Vec<Record>,
    /// The offset of the first byte that is not a whole, intact record - `None` when the
    /// file ends on a record boundary. Everything from here on is a torn write.
    pub torn_at: Option<u64>,
    /// `(offset, kind)` of every record skipped for a kind this build does not know.
    pub unknown: Vec<(u64, String)>,
    /// `(offset, error)` of every record whose CRC held but whose payload is not one of
    /// the shapes above. Skipped, like an unknown kind: the framing says the bytes are
    /// what was written, so this is a writer bug or a newer field layout, not a tear.
    pub malformed: Vec<(u64, String)>,
}

/// The frame for one record.
#[cfg(test)]
pub fn frame(record: &Record) -> Result<Vec<u8>, serde_json::Error> {
    Ok(frame::frame(&serde_json::to_vec(record)?))
}

/// Every intact record from the start of `bytes`, stopping at the first tear.
///
/// Never panics and never fails: the worst input is a file that reads as empty. The
/// restart test truncates a catalog at every byte offset and asks for the intact prefix,
/// and the `record_log` fuzz target holds every input to the same.
pub fn parse(bytes: &[u8]) -> Parsed {
    let (frames, torn_at) = frame::read_all(bytes, MAX_PAYLOAD);
    let mut parsed = Parsed {
        torn_at,
        ..Parsed::default()
    };
    for (at, payload) in frames {
        decode(&mut parsed, at, payload);
    }
    parsed
}

/// One intact payload into `parsed`: a record, an unknown kind, or a malformed one.
fn decode(parsed: &mut Parsed, at: u64, payload: &[u8]) {
    match read_record::<Record>(payload) {
        Ok(record) => parsed.records.push(record),
        Err(e) => {
            // The CRC held, so these are the bytes that were written. Look for the kind
            // before calling it malformed: a kind this build does not know is the one
            // case the format promises to step over quietly.
            match kind_of(payload) {
                Some(kind) if !KNOWN_KINDS.contains(&kind.as_str()) => {
                    parsed.unknown.push((at, kind));
                }
                _ => parsed.malformed.push((at, e)),
            }
        }
    }
}

const KNOWN_KINDS: &[&str] = &["template", "column", "time_source"];

// ── One JSON object ────────────────────────────────────────────────────────────────

/// One intact payload as a `T`, or the reason it is not one. `manifest.log` reads its
/// records through this too, and a third file framed this way should.
///
/// **A record is one JSON object, and each thing in it is the JSON the writer writes
/// there.** The readers serde derives take more: a JSON array for any struct - the
/// fields in order, an internally tagged enum's tag first - and an object for a name,
/// `{"Ip":null}` for `"Ip"`. So `["time_source",101,"arrival"]` read as a record here,
/// `["remove",["2024-01-15/seg_0_000001.col"]]` as a manifest record retiring a live
/// file, and `"wal":["…","2024-01-15",5]` as the mark that tells a replay which lines to
/// drop. Nothing writes those, and a payload that is not one of the shapes the format
/// lists is skipped, not guessed at.
///
/// So what was read is written back, as the writer spells it, and held against the
/// payload: an object where the payload has an object, an array for an array, a string
/// for a string. That is one rule for every struct and every name a record holds today
/// or is given later, with no attribute to remember on the new field, and it is the
/// writer's own spelling that says what belongs where.
///
/// The payload is read a second time for this, as plain JSON. A payload the first read
/// took and the second cannot - a number no float holds or text that is not UTF-8, in a
/// field the record does not have and so never looked at - is not a JSON object either.
pub(crate) fn read_record<T>(payload: &[u8]) -> Result<T, String>
where
    T: Serialize + DeserializeOwned,
{
    let record: T = serde_json::from_slice(payload).map_err(|e| e.to_string())?;
    let said: Value = serde_json::from_slice(payload).map_err(|e| e.to_string())?;
    let written = serde_json::to_value(&record).map_err(|e| e.to_string())?;
    let mut path = String::new();
    match spelled_otherwise(&written, &said, &mut path) {
        Some((found, wanted)) if path.is_empty() => {
            Err(format!("the payload is {found} where a record is {wanted}"))
        }
        Some((found, wanted)) => Err(format!("`{path}` is {found} where a record has {wanted}")),
        None => Ok(record),
    }
}

/// What kind of JSON a value is, as a WARN says it.
fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "a JSON null",
        Value::Bool(_) => "a JSON boolean",
        Value::Number(_) => "a JSON number",
        Value::String(_) => "a JSON string",
        Value::Array(_) => "a JSON array",
        Value::Object(_) => "a JSON object",
    }
}

/// The first place `written` and `said` hold different kinds of JSON: what `said` has
/// there and what `written` has, with `path` left naming the place - `files[0]`, `wal`,
/// or nothing for the payload itself. `None`, and `path` as it was, when they agree
/// everywhere.
///
/// Kinds and not values: a time is written one way and read from several. A key the
/// payload leaves out is one the reader filled in, and holds nothing to check.
fn spelled_otherwise(
    written: &Value,
    said: &Value,
    path: &mut String,
) -> Option<(&'static str, &'static str)> {
    let len = path.len();
    let found = match (written, said) {
        (Value::Object(written), Value::Object(said)) => written.iter().find_map(|(key, value)| {
            let theirs = said.get(key)?;
            path.truncate(len);
            if len > 0 {
                path.push('.');
            }
            path.push_str(key);
            spelled_otherwise(value, theirs, path)
        }),
        (Value::Array(written), Value::Array(said)) => {
            written
                .iter()
                .zip(said)
                .enumerate()
                .find_map(|(index, (value, theirs))| {
                    path.truncate(len);
                    // Writing to a `String` cannot fail.
                    let _ = write!(path, "[{index}]");
                    spelled_otherwise(value, theirs, path)
                })
        }
        (written, said) if std::mem::discriminant(written) == std::mem::discriminant(said) => None,
        (written, said) => return Some((json_type(said), json_type(written))),
    };
    if found.is_none() {
        path.truncate(len);
    }
    found
}

/// The `kind` an intact payload names, when the payload is a JSON object naming one as a
/// string: what tells a kind this build does not know from a malformed record. An array
/// has no kind, whatever its first element says.
pub(crate) fn kind_of(payload: &[u8]) -> Option<String> {
    match serde_json::from_slice::<Value>(payload) {
        Ok(Value::Object(said)) => said.get("kind")?.as_str().map(str::to_string),
        _ => None,
    }
}

// ── The file ───────────────────────────────────────────────────────────────────────

/// An open catalog, positioned at its end. The file mechanics - truncating a torn tail,
/// fsyncing a new file's directory, rolling back a failed append - are `frame::Log`'s,
/// shared with `manifest.log`.
pub struct Catalog {
    log: Log,
}

impl Catalog {
    /// Open for appending, reading every record that is already there.
    ///
    /// A torn tail is truncated *on disk* here, not just skipped, so the next append lands
    /// on a record boundary rather than behind bytes the next start would cut off again.
    /// Unknown kinds and malformed payloads are reported through `warn` and left in place;
    /// they are intact records this build does not use.
    pub fn open(
        path: &Path,
        warn: &mut dyn FnMut(String),
    ) -> Result<(Self, Vec<Record>), CatalogError> {
        Self::open_with(path, warn, &mut frame::sync_dir)
    }

    /// `open`, with the directory fsync handed in so a test can count it and fail it.
    fn open_with(
        path: &Path,
        warn: &mut dyn FnMut(String),
        sync_dir: &mut dyn FnMut(&Path) -> std::io::Result<()>,
    ) -> Result<(Self, Vec<Record>), CatalogError> {
        let display = path.display().to_string();
        let (log, opened) = Log::open_with(path, MAX_PAYLOAD, sync_dir)?;
        let mut parsed = Parsed {
            torn_at: opened.torn_at,
            ..Parsed::default()
        };
        for (at, payload) in opened.payloads() {
            decode(&mut parsed, at, payload);
        }
        for (offset, kind) in &parsed.unknown {
            warn(format!(
                "{display}: record at offset {offset} has kind `{kind}`, which this build does \
                 not know; skipped"
            ));
        }
        for (offset, error) in &parsed.malformed {
            warn(format!(
                "{display}: record at offset {offset} is intact but not readable ({error}); \
                 skipped"
            ));
        }
        if let Some(offset) = parsed.torn_at {
            warn(format!(
                "{display}: torn record at offset {offset} of {}; truncated to the {} \
                 intact record(s) before it",
                opened.bytes.len(),
                parsed.records.len()
            ));
        }
        Ok((Self { log }, parsed.records))
    }

    pub fn path(&self) -> &Path {
        self.log.path()
    }

    /// A catalog whose every append fails: what a full disk or a dead mount looks like.
    #[cfg(any(test, feature = "test-support"))]
    pub fn read_only(path: &Path) -> Result<Self, CatalogError> {
        Ok(Self {
            log: Log::read_only(path)?,
        })
    }

    /// Append the records and return only once every byte is on disk.
    ///
    /// Several records in one call so a template and its columns are one durable unit:
    /// one `write_all`, one `sync_all`. A tear between them would leave a template with
    /// no column types, which the reader would accept and the trie would carry as a
    /// template whose every column has only held nulls.
    ///
    /// On failure the file is cut back to where the records began. If *that* fails the
    /// catalog refuses every later append (`CatalogError::Torn`): a record written behind
    /// a tear is a record the next start cannot see, and rejecting the line now is the
    /// honest answer.
    pub fn append(&mut self, records: &[Record]) -> Result<(), CatalogError> {
        let mut payloads = Vec::with_capacity(records.len());
        for record in records {
            payloads.push(serde_json::to_vec(record).map_err(|source| {
                CatalogError::Serialize {
                    path: self.log.path().display().to_string(),
                    kind: record.kind(),
                    source,
                }
            })?);
        }
        Ok(self.log.append(&payloads)?)
    }

    /// Bytes on disk, as of the last successful append.
    pub fn len(&self) -> u64 {
        self.log.len()
    }

    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }
}

/// The path of the catalog inside `data_dir`.
pub fn path_in(data_dir: &Path) -> PathBuf {
    data_dir.join(CATALOG_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};

    fn template(id: u64) -> Record {
        Record::Template {
            id,
            created_at: DateTime::from_timestamp(1_705_314_600, 0).expect("a valid timestamp"),
            tokens: vec![
                Token::Promoted {
                    promoted: "event".into(),
                    value: "login_failed".into(),
                },
                Token::Key { key: "ts".into() },
                Token::Key {
                    key: "user.id".into(),
                },
            ],
        }
    }

    fn column(id: u64, name: &str, col_type: Option<ColumnType>) -> Record {
        Record::Column {
            template_id: id,
            name: name.into(),
            col_type,
            widened_from: None,
            at: None,
        }
    }

    fn corpus() -> Vec<Record> {
        vec![
            template(101),
            column(101, "ts", Some(ColumnType::Datetime)),
            column(101, "user.id", Some(ColumnType::Uuid)),
            column(101, "event", Some(ColumnType::String)),
            template(102),
            column(102, "ip", Some(ColumnType::Ip)),
            column(102, "note", None),
            Record::Column {
                template_id: 101,
                name: "ts".into(),
                col_type: Some(ColumnType::String),
                widened_from: Some(ColumnType::Datetime),
                at: Some(DateTime::from_timestamp(1_705_314_700, 0).expect("valid")),
            },
            Record::TimeSource {
                template_id: 101,
                source: "column:ts".into(),
            },
        ]
    }

    fn bytes_of(records: &[Record]) -> Vec<u8> {
        records
            .iter()
            .flat_map(|r| frame(r).expect("a record frames"))
            .collect()
    }

    #[test]
    fn framing_is_length_crc_payload() {
        let bytes = frame(&template(101)).expect("frames");
        let len = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes")) as usize;
        assert_eq!(bytes.len(), HEADER + len);
        let payload = &bytes[HEADER..];
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes")),
            frame::crc32c(payload)
        );
        let json: serde_json::Value = serde_json::from_slice(payload).expect("json");
        assert_eq!(json["kind"], "template");
        assert_eq!(json["id"], 101);
        assert_eq!(json["tokens"][0]["promoted"], "event");
        assert_eq!(json["tokens"][0]["value"], "login_failed");
        assert_eq!(json["tokens"][1]["key"], "ts");
    }

    #[test]
    fn column_record_reads_as_the_spec_spells_it() {
        let bytes = frame(&column(101, "ts", Some(ColumnType::Datetime))).expect("frames");
        let json: serde_json::Value = serde_json::from_slice(&bytes[HEADER..]).expect("json");
        assert_eq!(
            json,
            serde_json::json!({"kind":"column","template_id":101,"name":"ts","type":"Datetime"})
        );
    }

    #[test]
    fn a_whole_file_round_trips() {
        let records = corpus();
        let parsed = parse(&bytes_of(&records));
        assert_eq!(parsed.records, records);
        assert_eq!(parsed.torn_at, None);
        assert!(parsed.unknown.is_empty());
        assert!(parsed.malformed.is_empty());
    }

    /// Truncated at EVERY byte offset: the intact prefix comes back, whole records only,
    /// and nothing panics.
    #[test]
    fn a_catalog_truncated_at_every_offset_rebuilds_the_intact_prefix() {
        let records = corpus();
        let whole = bytes_of(&records);
        let mut boundaries = vec![0usize];
        for record in &records {
            let next = boundaries[boundaries.len() - 1] + frame(record).expect("frames").len();
            boundaries.push(next);
        }
        for cut in 0..=whole.len() {
            let parsed = parse(&whole[..cut]);
            let intact = boundaries.iter().filter(|b| **b <= cut).count() - 1;
            assert_eq!(
                parsed.records.len(),
                intact,
                "cut at {cut}: expected {intact} whole records"
            );
            assert_eq!(&parsed.records[..], &records[..intact]);
            let on_boundary = boundaries.contains(&cut);
            assert_eq!(
                parsed.torn_at,
                (!on_boundary).then_some(boundaries[intact] as u64),
                "cut at {cut}: torn offset"
            );
        }
    }

    /// A flipped byte anywhere in the payload is a CRC failure and a tear at that record.
    #[test]
    fn a_corrupted_byte_is_a_tear_at_that_record() {
        let records = corpus();
        let mut bytes = bytes_of(&records);
        let first_len = frame(&records[0]).expect("frames").len();
        // Inside the second record's payload.
        let at = first_len + HEADER + 3;
        bytes[at] ^= 0x40;
        let parsed = parse(&bytes);
        assert_eq!(parsed.records, records[..1]);
        assert_eq!(parsed.torn_at, Some(first_len as u64));
    }

    #[test]
    fn an_unknown_kind_is_skipped_and_named() {
        let mut bytes = bytes_of(&[template(101)]);
        let foreign = br#"{"kind":"merge","template_id":101,"into":102}"#;
        let skip_at = bytes.len();
        bytes.extend_from_slice(&(foreign.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&frame::crc32c(foreign).to_le_bytes());
        bytes.extend_from_slice(foreign);
        bytes.extend(bytes_of(&[template(102)]));

        let parsed = parse(&bytes);
        assert_eq!(parsed.records, vec![template(101), template(102)]);
        assert_eq!(parsed.unknown, vec![(skip_at as u64, "merge".to_string())]);
        assert_eq!(parsed.torn_at, None);
    }

    /// A record is one JSON object, and so is each token in it. serde's derived reader
    /// takes an array for a record - the tag first, then the fields in order - and the
    /// first two of these read as records nobody wrote. All are intact, so all are
    /// skipped as malformed, and the record after them is still read.
    #[test]
    fn a_json_array_where_the_format_has_an_object_is_malformed() {
        let arrays: [(&[u8], &str); 4] = [
            (br#"["time_source",101,"arrival"]"#, "the payload is a JSON array"),
            (
                br#"["template",101,"2024-01-15T10:30:00Z",[{"key":"ts"}]]"#,
                "the payload is a JSON array",
            ),
            // serde refuses this one by itself, with a reason of its own: a token is
            // read by its fields' names, and an array has none.
            (
                br#"{"kind":"template","id":101,"created_at":"2024-01-15T10:30:00Z","tokens":[{"key":"ts"},["event","login"]]}"#,
                "did not match any variant",
            ),
            // An array has no kind: this is not the unknown kind `merge`.
            (br#"["merge",101,102]"#, ""),
        ];
        let mut bytes = Vec::new();
        let mut offsets = Vec::new();
        for (payload, _) in &arrays {
            offsets.push(bytes.len() as u64);
            bytes.extend(frame::frame(payload));
        }
        bytes.extend(bytes_of(&[template(102)]));

        let parsed = parse(&bytes);
        assert_eq!(parsed.records, vec![template(102)]);
        assert_eq!(parsed.torn_at, None);
        assert!(parsed.unknown.is_empty(), "{:?}", parsed.unknown);
        assert_eq!(
            parsed
                .malformed
                .iter()
                .map(|(at, _)| *at)
                .collect::<Vec<_>>(),
            offsets
        );
        for ((_, why), (_, want)) in parsed.malformed.iter().zip(&arrays) {
            assert!(why.contains(want), "`{why}` does not say `{want}`");
        }

        // A name is a string: serde also takes a one-key object for it.
        let named = br#"{"kind":"column","template_id":101,"name":"ip","type":{"Ip":null}}"#;
        let parsed = parse(&frame::frame(named));
        assert!(parsed.records.is_empty(), "{:?}", parsed.records);
        assert!(
            parsed.malformed[0].1.contains("`type` is a JSON object"),
            "{:?}",
            parsed.malformed
        );

        // The same records as objects, tokens and all, are records.
        let objects = [
            &br#"{"kind":"time_source","template_id":101,"source":"arrival"}"#[..],
            br#"{"kind":"template","id":101,"created_at":"2024-01-15T10:30:00Z","tokens":[{"key":"ts"},{"promoted":"event","value":"login"}]}"#,
        ];
        let bytes: Vec<u8> = objects.iter().flat_map(|p| frame::frame(p)).collect();
        let parsed = parse(&bytes);
        assert_eq!(parsed.records.len(), 2);
        assert!(parsed.malformed.is_empty(), "{:?}", parsed.malformed);
    }

    /// A payload the typed read takes and that is not JSON - here a number no float
    /// holds, in a field the record does not have - is not one JSON object either.
    #[test]
    fn a_payload_that_is_not_json_is_malformed_whatever_the_record_ignores() {
        let payload =
            br#"{"kind":"time_source","template_id":101,"source":"arrival","later":1e999}"#;
        let parsed = parse(&frame::frame(payload));
        assert!(parsed.records.is_empty(), "{:?}", parsed.records);
        assert_eq!(parsed.malformed.len(), 1);
    }

    #[test]
    fn an_absurd_length_is_a_tear_not_a_wait() {
        let mut bytes = bytes_of(&[template(101)]);
        let at = bytes.len();
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        bytes.extend_from_slice(&[0, 0, 0, 0, 1, 2, 3]);
        let parsed = parse(&bytes);
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.torn_at, Some(at as u64));
    }

    #[test]
    fn open_truncates_a_torn_tail_on_disk_and_appends_after_it() {
        let dir = std::env::temp_dir().join(format!("kibbr-catalog-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        let path = path_in(&dir);

        let mut bytes = bytes_of(&[template(101)]);
        let good = bytes.len();
        bytes.extend_from_slice(&[9, 0, 0, 0, 1, 1]);
        fs::write(&path, &bytes).expect("write");

        let mut warnings = Vec::new();
        let (mut catalog, records) = Catalog::open(&path, &mut |w| warnings.push(w)).expect("open");
        assert_eq!(records, vec![template(101)]);
        assert_eq!(catalog.len(), good as u64);
        assert_eq!(fs::metadata(&path).expect("stat").len(), good as u64);
        assert!(
            warnings
                .iter()
                .any(|w| w.contains(&format!("torn record at offset {good}"))),
            "{warnings:?}"
        );

        catalog.append(&[template(102)]).expect("append");
        drop(catalog);
        let (_, records) = Catalog::open(&path, &mut |_| {}).expect("reopen");
        assert_eq!(records, vec![template(101), template(102)]);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The directory is fsynced exactly once, when the file is created, and a failed
    /// fsync is the open's error rather than a line in a log.
    #[test]
    fn creating_the_catalog_syncs_its_directory_once_and_a_failed_sync_is_an_error() {
        let dir = std::env::temp_dir().join(format!("kibbr-catalog-sync-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        let path = path_in(&dir);

        let synced = std::cell::RefCell::new(Vec::new());
        let mut counting = |d: &Path| {
            synced.borrow_mut().push(d.to_path_buf());
            Ok(())
        };
        let (mut catalog, _) =
            Catalog::open_with(&path, &mut |_| {}, &mut counting).expect("create");
        catalog.append(&[template(101)]).expect("append");
        drop(catalog);
        assert_eq!(
            *synced.borrow(),
            vec![dir.clone()],
            "the directory was not synced on creation"
        );

        // Reopening an existing catalog creates nothing, so it syncs nothing.
        let (_, records) = Catalog::open_with(&path, &mut |_| {}, &mut counting).expect("reopen");
        assert_eq!(records, vec![template(101)]);
        assert_eq!(
            synced.borrow().len(),
            1,
            "an existing catalog's directory was synced again"
        );

        fs::remove_file(&path).expect("rm");
        let mut failing = |_: &Path| Err(std::io::Error::other("injected fsync failure"));
        match Catalog::open_with(&path, &mut |_| {}, &mut failing) {
            Err(e @ CatalogError::SyncDir { .. }) => {
                assert!(e.to_string().contains("injected fsync failure"), "{e}");
            }
            Err(other) => panic!("failed for another reason: {other}"),
            Ok(_) => panic!("a failed directory fsync was ignored"),
        }
        assert!(
            !path.exists(),
            "a catalog whose directory was never synced was left for the next start to trust"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The real directory sync, on a directory it cannot succeed on: write and search
    /// permission without read, so the file can be created and the directory cannot be
    /// opened to fsync. The open fails rather than carrying on with an entry that may not
    /// survive a crash, and leaves no file behind.
    #[cfg(unix)]
    #[test]
    fn a_directory_that_cannot_be_synced_fails_the_first_open() {
        use std::os::unix::fs::PermissionsExt;

        let dir =
            std::env::temp_dir().join(format!("kibbr-catalog-nosync-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o300)).expect("chmod -wx");
        if File::open(&dir).is_ok() {
            // Root, or a filesystem that ignores the mode: nothing here can fail the sync.
            let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));
            let _ = fs::remove_dir_all(&dir);
            return;
        }
        let path = path_in(&dir);

        let opened = Catalog::open(&path, &mut |_| {});
        let left = path.exists();
        let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));
        let _ = fs::remove_dir_all(&dir);

        match opened {
            Err(CatalogError::SyncDir { path: named, .. }) => {
                assert_eq!(named, dir.display().to_string());
            }
            Err(other) => panic!("failed for another reason: {other}"),
            Ok(_) => panic!("created {} and never synced its directory", path.display()),
        }
        assert!(
            !left,
            "a catalog whose directory entry was never synced was left behind"
        );
    }
}
