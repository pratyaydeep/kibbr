//! The `record_log` target: the one record reader under `catalog.log` and `manifest.log`
//! (the catalog and the manifest), held to what the format promises about bytes nobody
//! vouched for.
//!
//! The input is the whole of a log file, and it is read as both files, because the framing
//! is one and a catalog's records are a manifest's unknown kinds. What is asserted:
//!
//! - **The frames are the ones a second reader finds.** `walk` below reads the framing from
//!   its description, with a CRC table of its own, and `frame::read_all` must agree with
//!   it frame for frame: the same payloads, each one inside the input, and the same tear -
//!   which is at most the input's length and always a boundary reached over intact frames.
//! - **A tear is where the file ends.** Cut at the tear, the same records come back and
//!   nothing is torn; cut inside any frame, the frames before it come back and the tear is
//!   that frame's first byte; framing the payloads again spells the input up to the tear.
//! - **A record is what its payload says.** A payload read as a record is one JSON object
//!   of a kind the file has, and writing the record back spells each field as the payload
//!   did. Anything else whose CRC holds is skipped, as an unknown kind or as malformed, and
//!   never ends the read.
//! - **A record is read on its own.** Each frame read alone is what it was read as in the
//!   file, at its offset.
//! - **What startup does with the records does not panic either**: the trie rebuilt from
//!   a catalog's, whose next template id is one no record names, and the live set, the
//!   load ids and the published paths from a manifest's.
//! - **What the writer writes, the reader reads.** Every check above holds of a reader
//!   that skips every record it is given, so one mode builds records, writes them as the
//!   two files' writers do and asks for the same records back, in order, none skipped.
//!
//! **Five ways to read the input**, chosen by its first byte, because a mutated byte under
//! a CRC is a tear and nothing behind the frame reader would ever be reached:
//!
//! ```text
//! byte       the rest is
//! % 4 == 0   the file, as it is
//! % 4 == 1   payloads, one a line, framed here with their true length and CRC
//! % 4 == 2   the same, then what follows the last newline unframed - a tail - and one
//!            whole frame after it, which a reader that steps over a tear would read
//! % 4 == 3   payloads, one a line, each given a kind neither file knows: a known kind
//!            spelled nearly right, or the line's own
//! % 8 == 4   choices: which records to build, written by the writers into one file
//! ```
//!
//! The fifth takes half of the first's bytes and none of the seeds': they start with an
//! ASCII `0` to `4`, which is its own mode under either rule.
//!
//! Nothing here allocates more than a few times the input, so the fuzzer's malloc limit
//! is a limit on the reader.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::ops::Range;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

use kibbr_core::types::{ColumnType, TimeSource};
use kibbr_storage::catalog::{self, Token};
use kibbr_storage::frame::{self, Next, Opened, HEADER};
use kibbr_storage::store::manifest::{self, Decoded, FileRecord, WalMark};
use kibbr_storage::trie::Trie;

/// The kinds the catalog has, written out here and not borrowed from the reader: a
/// kind dropped from the reader's own list would otherwise be dropped from the check too.
const CATALOG_KINDS: [&str; 3] = ["template", "column", "time_source"];

/// The kinds the manifest has, `seal` included.
const MANIFEST_KINDS: [&str; 4] = ["add", "seal", "remove", "replace"];

/// A longer input is not run. Both readers take a payload of 16MiB and more, so this is
/// the harness keeping its own copies small and not a limit of the format.
const MAX_INPUT: usize = 1 << 20;

/// The trie is rebuilt only from catalogs naming at most this many tokens. A template's
/// path is one node a token and the trie walks it by recursion, so a deeper one is a
/// question about the stack and about `trie.rs`, which is not this target's subject.
const MAX_TOKENS: usize = 512;

/// How many frames of one file are read a second time, alone or cut short. Each is a
/// read of its own by both readers, and most of a run's time; which ones are taken moves
/// with the file's length, so every frame of a file is taken in some input near it.
const SAMPLED: usize = 4;

/// The most records the fifth mode builds. A template here has at most three tokens, so
/// every file it writes is under `MAX_TOKENS` and its trie is rebuilt.
const MAX_WRITTEN: usize = 128;

pub fn run(data: &[u8]) {
    let Some((mode, rest)) = data.split_first() else {
        return;
    };
    if rest.len() > MAX_INPUT {
        return;
    }
    if mode % 8 == 4 {
        check_written(rest);
        return;
    }
    match mode % 4 {
        0 => {
            check_file(rest);
        }
        1 => {
            let payloads = lines(rest);
            let file = framed(payloads.iter().copied());
            let seen = check_file(&file);
            assert_eq!(seen.tear, None, "a file of whole frames is torn");
            holds_the_payloads(&file, &seen, &payloads);
            assert_eq!(seen.frames.len(), payloads.len(), "frames nobody wrote");
        }
        2 => {
            let (body, tail) = match rest.iter().rposition(|b| *b == b'\n') {
                Some(at) => (&rest[..at], &rest[at + 1..]),
                None => (&rest[..0], rest),
            };
            let payloads = lines(body);
            let mut file = framed(payloads.iter().copied());
            let whole = file.len();
            file.extend_from_slice(tail);
            // One whole frame past the tail. A tail that is not whole frames is where the
            // file ends, and this is what a reader that looked for the next frame beyond
            // it would find; `check_frames` holds the reader to stopping.
            let beyond = payloads.first().copied().unwrap_or(b"{}");
            file.extend_from_slice(&framed([beyond].into_iter()));
            let seen = check_file(&file);
            holds_the_payloads(&file, &seen, &payloads);
            if let Some(tear) = seen.tear {
                assert!(
                    tear >= whole,
                    "torn at {tear}, inside the {whole} bytes of whole frames"
                );
            }
        }
        _ => {
            let payloads: Vec<(String, Vec<u8>)> =
                lines(rest).into_iter().map(of_an_unknown_kind).collect();
            let file = framed(payloads.iter().map(|(_, payload)| payload.as_slice()));
            let seen = check_file(&file);
            assert_eq!(seen.tear, None, "a file of whole frames is torn");
            assert_eq!(seen.frames.len(), payloads.len(), "frames nobody wrote");
            for (reader, outcomes) in [("catalog", &seen.catalog), ("manifest", &seen.manifest)] {
                for ((kind, _), outcome) in payloads.iter().zip(outcomes) {
                    assert_eq!(
                        outcome,
                        &Outcome::Unknown(kind.clone()),
                        "{reader}: a record of the unknown kind `{kind}` was not skipped as one"
                    );
                }
            }
        }
    }
}

/// What one reader made of one frame.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Record,
    Unknown(String),
    Malformed,
}

/// What `check_file` found, for the checks only one mode can make.
struct Seen {
    /// Each intact frame's payload, as a range of the file.
    frames: Vec<Range<usize>>,
    tear: Option<usize>,
    catalog: Vec<Outcome>,
    manifest: Vec<Outcome>,
}

/// Everything that holds of any bytes at all, read as both files.
fn check_file(bytes: &[u8]) -> Seen {
    let (frames, tear) = check_frames(bytes);
    // Each payload as JSON by a reading that knows no record, once for both files.
    let said: Vec<Option<Value>> = frames
        .iter()
        .map(|range| serde_json::from_slice(&bytes[range.clone()]).ok())
        .collect();
    let catalog = check_catalog(bytes, &frames, &said, tear);
    let manifest = check_manifest(bytes, &frames, &said, tear);
    Seen {
        frames,
        tear,
        catalog,
        manifest,
    }
}

// ── The framing ────────────────────────────────────────────────────────────────────

/// CRC-32C by table, built here from the polynomial. The reader's comes from a crate that
/// takes the CPU's instruction; two that agree on every input are not both wrong.
const CRC_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut byte = 0;
    while byte < 256 {
        let mut crc = byte as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }
    table
};

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc = CRC_TABLE[((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

/// What the framing says is at one offset: `frame::Next`, with the payload as a range.
enum At {
    Frame(Range<usize>),
    End,
    Tear,
}

/// The framing - a u32 length, a u32 CRC-32C, the payload - written a second time: what
/// is at `at`, which is at most the input's length.
fn frame_at(bytes: &[u8], at: usize, max: u32) -> At {
    let left = bytes.len() - at;
    if left == 0 {
        return At::End;
    }
    if left < HEADER {
        return At::Tear;
    }
    let len = u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]);
    let crc = u32::from_le_bytes([bytes[at + 4], bytes[at + 5], bytes[at + 6], bytes[at + 7]]);
    if len == 0 || len > max || len as usize > left - HEADER {
        return At::Tear;
    }
    let payload = at + HEADER..at + HEADER + len as usize;
    if crc32c(&bytes[payload.clone()]) != crc {
        return At::Tear;
    }
    At::Frame(payload)
}

/// Every intact frame's payload from the start, and where the first thing that is not
/// one begins.
fn walk(bytes: &[u8], max: u32) -> (Vec<Range<usize>>, Option<usize>) {
    let mut frames = Vec::new();
    let mut at = 0usize;
    loop {
        match frame_at(bytes, at, max) {
            At::Frame(payload) => {
                at = payload.end;
                frames.push(payload);
            }
            At::End => return (frames, None),
            At::Tear => return (frames, Some(at)),
        }
    }
}

/// One frame a payload, by this file's own CRC and not the writer's.
fn framed<'a>(payloads: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
    let mut file = Vec::new();
    for payload in payloads {
        file.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        file.extend_from_slice(&crc32c(payload).to_le_bytes());
        file.extend_from_slice(payload);
    }
    file
}

/// The payloads of the line modes: every line that holds a byte. An empty one is left out
/// because a frame's length is never 0.
fn lines(bytes: &[u8]) -> Vec<&[u8]> {
    bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .collect()
}

/// Whether `part` is a slice of `bytes` and not of anything else.
fn inside(bytes: &[u8], part: &[u8]) -> bool {
    let start = bytes.as_ptr() as usize;
    let at = part.as_ptr() as usize;
    at >= start && at + part.len() <= start + bytes.len()
}

/// `frame::read_all` against `walk`, and the tear rules.
fn check_frames(bytes: &[u8]) -> (Vec<Range<usize>>, Option<usize>) {
    let (frames, tear) = walk(bytes, u32::MAX);
    let (got, torn_at) = frame::read_all(bytes, u32::MAX);
    assert_eq!(
        torn_at,
        tear.map(|at| at as u64),
        "the tear is not where the frames end"
    );
    assert_eq!(got.len(), frames.len(), "a different number of frames");
    for ((at, payload), range) in got.iter().zip(&frames) {
        assert_eq!(*at as usize + HEADER, range.start, "a frame's offset");
        assert!(
            inside(bytes, payload),
            "the payload at {at} is not in the input"
        );
        assert_eq!(*payload, &bytes[range.clone()], "the payload at {at}");
    }
    let end = tear.unwrap_or(bytes.len());
    assert!(end <= bytes.len(), "a tear past the end");

    // Framed again by the writer, the payloads are the file up to the tear: no byte
    // between two frames, none skipped, and the writer's CRC is the reader's.
    let mut again = Vec::with_capacity(end);
    for (_, payload) in &got {
        again.extend_from_slice(&frame::frame(payload));
    }
    assert!(
        again == bytes[..end],
        "the payloads framed again are not the input's first {end} bytes"
    );

    // What truncating at the tear leaves is a whole file with the same frames.
    let (kept, torn_again) = frame::read_all(&bytes[..end], u32::MAX);
    assert_eq!(torn_again, None, "torn again after the cut at {end}");
    assert_eq!(
        kept, got,
        "the cut at the tear changed the frames before it"
    );

    // The end is not a tear, and an offset past it is one and not a panic.
    assert_eq!(frame::read(bytes, bytes.len(), u32::MAX), Next::End);
    for past in [bytes.len().saturating_add(1), usize::MAX - 7, usize::MAX] {
        if past > bytes.len() {
            assert_eq!(frame::read(bytes, past, u32::MAX), Next::Tear);
        }
    }

    // A frame is asked for at any offset, not only from the start - a v3 file's and a
    // log segment's are - and what is there is what the framing says is there: inside a
    // frame, usually a tear, and never a read that leaves the input.
    let len = bytes.len();
    let inside_frames = sampled(bytes, &frames).map(|(_, range)| range.start);
    for at in [
        1,
        HEADER,
        len / 2,
        len.saturating_sub(HEADER),
        len.saturating_sub(1),
    ]
    .into_iter()
    .chain(inside_frames)
    .filter(|at| *at <= len)
    {
        match (
            frame::read(bytes, at, u32::MAX),
            frame_at(bytes, at, u32::MAX),
        ) {
            (Next::Frame { payload, end }, At::Frame(range)) => {
                assert!(inside(bytes, payload), "the payload read at {at}");
                assert!(*payload == bytes[range.clone()], "the payload read at {at}");
                assert_eq!(end, range.end, "the end of the frame read at {at}");
            }
            (Next::End, At::End) | (Next::Tear, At::Tear) => {}
            (got, _) => panic!("read at {at}: {got:?} is not what the framing says is there"),
        }
    }

    // Under a cap, the first frame longer than it is the tear.
    for max in [1u32, 16] {
        let (capped, capped_tear) = frame::read_all(bytes, max);
        let (want, want_tear) = walk(bytes, max);
        assert_eq!(capped_tear, want_tear.map(|at| at as u64), "max {max}");
        assert_eq!(capped.len(), want.len(), "max {max}");
        for ((at, payload), range) in capped.iter().zip(&want) {
            assert_eq!(*at as usize + HEADER, range.start, "max {max}");
            assert!(**payload == bytes[range.clone()], "max {max}");
        }
    }

    // What opening the file keeps: the bytes, and each payload as a range of them. The
    // ranges are the ones walked here, and slicing by them gives the payloads back.
    let (spans, span_tear) = frame::spans(bytes, u32::MAX);
    assert_eq!(span_tear, torn_at, "the ranges tear somewhere else");
    let want: Vec<(u64, Range<usize>)> = frames
        .iter()
        .map(|range| ((range.start - HEADER) as u64, range.clone()))
        .collect();
    assert_eq!(spans, want, "the ranges are not the payloads'");
    let opened = Opened {
        bytes: bytes.to_vec(),
        frames: spans,
        torn_at: span_tear,
    };
    assert!(
        opened.payloads().eq(got.iter().copied()),
        "an opened file's payloads are not the ones read"
    );

    // A cut inside a frame leaves the frames before it and tears at its first byte.
    for (index, range) in sampled(bytes, &frames) {
        let start = range.start - HEADER;
        for cut in [start + 1, range.start, range.end - 1] {
            let (before, torn) = frame::read_all(&bytes[..cut], u32::MAX);
            assert_eq!(before.len(), index, "cut at {cut}");
            assert_eq!(torn, Some(start as u64), "cut at {cut}");
        }
    }
    (frames, tear)
}

/// The line modes' own check: the frames start with the payloads that were framed.
fn holds_the_payloads(file: &[u8], seen: &Seen, payloads: &[&[u8]]) {
    assert!(
        seen.frames.len() >= payloads.len(),
        "{} frames were written and {} read",
        payloads.len(),
        seen.frames.len()
    );
    for (range, payload) in seen.frames.iter().zip(payloads) {
        assert!(file[range.clone()] == **payload, "a payload changed");
    }
}

// ── What a payload says ────────────────────────────────────────────────────────────

/// The `kind` of a payload that is a JSON object.
fn kind_of(said: Option<&Value>) -> Option<&str> {
    said?.as_object()?.get("kind")?.as_str()
}

/// Every frame of a short file and `SAMPLED` spread over a long one, for the checks that
/// cost a read of their own for each frame: the index and the range of each one taken.
fn sampled<'a>(
    bytes: &[u8],
    frames: &'a [Range<usize>],
) -> impl Iterator<Item = (usize, &'a Range<usize>)> {
    let step = frames.len().div_ceil(SAMPLED).max(1);
    frames
        .iter()
        .enumerate()
        .skip(bytes.len() % step)
        .step_by(step)
}

/// A known kind spelled nearly right, which is a kind neither file knows: in another case,
/// with a space, a letter short or long, with the other separator, or as the next
/// version's. `pick` chooses which. A reader that matched kinds any more loosely than
/// byte for byte takes one of these for a record.
fn nearly(kind: &str, pick: usize) -> String {
    let near = match pick % 8 {
        0 => kind.to_ascii_uppercase(),
        1 => {
            let (first, rest) = kind.split_at(1);
            format!("{}{rest}", first.to_ascii_uppercase())
        }
        2 => format!("{kind} "),
        3 => format!(" {kind}"),
        4 => kind[..kind.len() - 1].to_string(),
        5 => format!("{kind}s"),
        6 => kind.replace('_', "-"),
        _ => format!("{kind}_v2"),
    };
    if CATALOG_KINDS.contains(&near.as_str()) || MANIFEST_KINDS.contains(&near.as_str()) {
        format!("{kind}_v2")
    } else {
        near
    }
}

/// A line as a payload of a kind neither file knows, and the kind. A line that is a JSON
/// object keeps its fields, so a known record's whole shape under a kind spelled nearly
/// right is tried, the spelling chosen by the line's length; a kind the line already has
/// is kept when it is unknown to both.
fn of_an_unknown_kind(line: &[u8]) -> (String, Vec<u8>) {
    let mut object = match serde_json::from_slice::<Value>(line) {
        Ok(Value::Object(object)) => object,
        _ => {
            let mut object = Map::new();
            let body = String::from_utf8_lossy(line).into_owned();
            object.insert("body".to_string(), Value::String(body));
            object
        }
    };
    let known = |kind: &str| CATALOG_KINDS.contains(&kind) || MANIFEST_KINDS.contains(&kind);
    let kind = match object.get("kind").and_then(Value::as_str) {
        Some(kind) if !known(kind) => kind.to_string(),
        Some(kind) => nearly(kind, line.len()),
        None => "future".to_string(),
    };
    object.insert("kind".to_string(), Value::String(kind.clone()));
    let payload = match serde_json::to_vec(&Value::Object(object)) {
        Ok(payload) => payload,
        Err(e) => panic!("a JSON object could not be written: {e}"),
    };
    (kind, payload)
}

/// The fields a record may leave out, with what the reader takes them to be: `sealed` and
/// `finished` are false unless said, and a column with no `type` has only held nulls.
fn defaulted(key: &str, value: &Value) -> bool {
    match key {
        "sealed" | "finished" => *value == Value::Bool(false),
        "type" => value.is_null(),
        _ => false,
    }
}

/// Whether everything `written` holds, `said` holds too, spelled the same way: an object
/// for an object, an array of the same length for an array, the same string, number or
/// bool. `loose` names the keys of this object held only to being strings - the times,
/// which the writer spells one way and a reader takes several.
fn says(written: &Value, said: &Value, loose: &[&str]) -> bool {
    match (written, said) {
        (Value::Object(written), Value::Object(said)) => {
            written.iter().all(|(key, value)| match said.get(key) {
                Some(theirs) if loose.contains(&key.as_str()) => theirs.is_string(),
                Some(theirs) => says(value, theirs, &[]),
                None => defaulted(key, value),
            })
        }
        (Value::Array(written), Value::Array(said)) => {
            written.len() == said.len() && written.iter().zip(said).all(|(w, s)| says(w, s, &[]))
        }
        (written, said) => written == said,
    }
}

/// A record against the payload it was read from.
///
/// The payload is one JSON object, at every level, which a reader derived from
/// the record's type does not hold to by itself. The record can be written, what is
/// written reads back as the same record, and each field written is the field the payload
/// spelled: a reader that took one shape for another, or a value for a different one, has
/// invented a record nobody wrote.
fn check_written_back<T>(reader: &str, at: usize, record: &T, said: Option<&Value>, loose: &[&str])
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    let said = match said {
        Some(said @ Value::Object(_)) => said,
        _ => panic!(
            "{reader}: the payload at {at} is not one JSON object and was read as {record:?}"
        ),
    };
    let bytes = match serde_json::to_vec(record) {
        Ok(bytes) => bytes,
        Err(e) => panic!("{reader}: the record at {at} cannot be written back ({e}): {record:?}"),
    };
    match serde_json::from_slice::<T>(&bytes) {
        Ok(again) => assert_eq!(
            &again, record,
            "{reader}: the record at {at} is a different record once written and read"
        ),
        Err(e) => panic!("{reader}: the record at {at}, written back, does not read ({e})"),
    }
    let written = match serde_json::from_slice::<Value>(&bytes) {
        Ok(written) => written,
        Err(e) => panic!("{reader}: the record at {at}, written back, is not JSON ({e})"),
    };
    assert!(
        says(&written, said, loose),
        "{reader}: the record at {at} is not what its payload says: read as {written}, from {said}"
    );
    if let Some(field) = lost(&written, said) {
        panic!(
            "{reader}: the record at {at} lost the `{field}` its payload has: read as {written}, \
             from {said}"
        );
    }
}

/// The fields a record of `kind` may leave out and the writer leaves out when they are
/// not there, in the catalog and in the manifest.
fn optional(kind: &str) -> &'static [&'static str] {
    match kind {
        "column" => &["widened_from", "at"],
        "add" => &["source"],
        "replace" => &["source", "wal"],
        _ => &[],
    }
}

/// The same of one file a manifest record names.
const FILE_OPTIONAL: [&str; 3] = ["rows", "min_ts", "max_ts"];

/// The first of `fields` that `said` gives a value and `written` does not have.
fn dropped<'a>(written: &Value, said: &Value, fields: &[&'a str]) -> Option<&'a str> {
    fields.iter().copied().find(|field| {
        said.get(field).is_some_and(|value| !value.is_null()) && written.get(field).is_none()
    })
}

/// A field the payload gives a value that the record, written back, does not have.
///
/// `says` holds what was read to the payload; this holds the payload to what was read,
/// for the fields a reader could drop and still return a record: a `replace` read without
/// its `wal` brings a replaced day back on the next replay, an `add` read without its
/// `source` is a load that can be run twice, and a file read without its `rows` is one
/// startup has no count for. Only the fields the two sections name, because a key they do
/// not name is one the reader is right to pass over.
fn lost(written: &Value, said: &Value) -> Option<String> {
    let kind = written.get("kind").and_then(Value::as_str).unwrap_or("");
    if let Some(field) = dropped(written, said, optional(kind)) {
        return Some(field.to_string());
    }
    for list in ["files", "add"] {
        let (Some(Value::Array(written)), Some(Value::Array(said))) =
            (written.get(list), said.get(list))
        else {
            continue;
        };
        for (index, (written, said)) in written.iter().zip(said).enumerate() {
            if let Some(field) = dropped(written, said, &FILE_OPTIONAL) {
                return Some(format!("{list}[{index}].{field}"));
            }
        }
    }
    None
}

/// The one rule both files share: a kind the file does not have is never a record.
fn check_kind(reader: &str, known: &[&str], at: usize, said: Option<&Value>, outcome: &Outcome) {
    if let Some(kind) = kind_of(said) {
        if !known.contains(&kind) {
            assert!(
                *outcome != Outcome::Record,
                "{reader}: the payload at {at} has the unknown kind `{kind}` and was read as a record"
            );
        }
    }
    if let Outcome::Unknown(kind) = outcome {
        assert!(
            !known.contains(&kind.as_str()),
            "{reader}: the payload at {at} was skipped as the unknown kind `{kind}`, which is known"
        );
    }
}

// ── catalog.log ────────────────────────────────────────────────────────────────────

fn check_catalog(
    bytes: &[u8],
    frames: &[Range<usize>],
    said: &[Option<Value>],
    tear: Option<usize>,
) -> Vec<Outcome> {
    let whole = catalog::parse(bytes);
    assert_eq!(whole.torn_at, tear.map(|at| at as u64), "catalog: the tear");

    // Every frame is one of three things, and the reader says which by offset: a skip is
    // listed at its frame's, and a frame not listed is the next record.
    let mut records = whole.records.iter();
    let mut unknown = whole.unknown.iter().peekable();
    let mut malformed = whole.malformed.iter().peekable();
    let mut outcomes = Vec::with_capacity(frames.len());
    let mut read_as = Vec::with_capacity(frames.len());
    for (range, said) in frames.iter().zip(said) {
        let at = range.start - HEADER;
        let skipped = unknown.next_if(|(offset, _)| *offset == at as u64);
        let unreadable = malformed.next_if(|(offset, _)| *offset == at as u64);
        let (outcome, record) = match (skipped, unreadable) {
            (Some((_, kind)), None) => (Outcome::Unknown(kind.clone()), None),
            (None, Some(_)) => (Outcome::Malformed, None),
            (None, None) => match records.next() {
                Some(record) => {
                    let loose = ["created_at", "at"];
                    check_written_back("catalog", at, record, said.as_ref(), &loose);
                    (Outcome::Record, Some(record))
                }
                None => panic!("catalog: the frame at {at} is neither a record nor a skip"),
            },
            (Some(_), Some(_)) => panic!("catalog: the frame at {at} was skipped twice"),
        };
        check_kind("catalog", &CATALOG_KINDS, at, said.as_ref(), &outcome);
        outcomes.push(outcome);
        read_as.push(record);
    }
    assert!(
        records.next().is_none() && unknown.next().is_none() && malformed.next().is_none(),
        "catalog: a record or a skip that no frame holds"
    );

    // A frame read alone is what it was in the file: nothing before it decides it.
    for (index, range) in sampled(bytes, frames) {
        let at = range.start - HEADER;
        let one = catalog::parse(&bytes[at..range.end]);
        assert_eq!(one.torn_at, None, "catalog: the frame at {at}, read alone");
        let alone = match (&one.records[..], &one.unknown[..], &one.malformed[..]) {
            ([record], [], []) => {
                assert_eq!(
                    Some(record),
                    read_as[index],
                    "catalog: the frame at {at}, alone"
                );
                Outcome::Record
            }
            ([], [(0, kind)], []) => Outcome::Unknown(kind.clone()),
            ([], [], [(0, _)]) => Outcome::Malformed,
            _ => panic!("catalog: the frame at {at}, read alone, is not one record or one skip"),
        };
        assert_eq!(
            alone, outcomes[index],
            "catalog: the frame at {at}, read alone"
        );
    }

    if let Some(tear) = tear {
        let cut = catalog::parse(&bytes[..tear]);
        assert_eq!(cut.torn_at, None, "catalog: torn again after the cut");
        assert_eq!(
            cut.records, whole.records,
            "catalog: the cut changed the records"
        );
        assert_eq!(
            cut.unknown, whole.unknown,
            "catalog: the cut changed the skips"
        );
        assert_eq!(
            cut.malformed, whole.malformed,
            "catalog: the cut changed the skips"
        );
    }
    check_rebuild(&whole.records);
    outcomes
}

/// What a start does with a catalog's records: the trie, rebuilt. Any records at all are
/// a trie and never a failed start, the id the next template takes is one no template
/// record names, the time sources it hands back to be made durable name templates the
/// catalog holds, and with those appended a second start asks for none.
fn check_rebuild(records: &[catalog::Record]) {
    let mut ids = BTreeSet::new();
    let mut tokens = 0usize;
    for record in records {
        if let catalog::Record::Template {
            id, tokens: path, ..
        } = record
        {
            ids.insert(*id);
            tokens += path.len();
            for token in path {
                let _ = token.label();
            }
        }
    }
    if tokens > MAX_TOKENS {
        return;
    }
    let time_columns = || crate::config::Schema::default().time_columns;
    let (trie, backfill) =
        match Trie::from_catalog_with("fuzz", records, time_columns(), &mut |_| {}) {
            Ok(rebuilt) => rebuilt,
            Err(e) => panic!("catalog: the records do not rebuild a trie: {e}"),
        };
    let templates = trie.template_count();
    assert!(
        templates <= ids.len() as u64,
        "catalog: {templates} templates from {} template ids",
        ids.len()
    );
    // What a plan reads in place of the nodes is built beside them, one template at a
    // time, and has to be the nodes' whatever records they were built from. Asserted
    // here and not left to the trie's own check, which only a debug build makes.
    if let Err(why) = trie.keys_agree() {
        panic!("catalog: the key index a plan reads is not the rebuilt trie's: {why}");
    }
    // Each id the next templates take is above every id a template record names and
    // above the one before it, until there is none left to take: never an id twice. A
    // counter that wraps is no panic in a release build, and only this sees it. Every id
    // named and not only the ones held: a record skipped for its path is still in the
    // file, and a template given its id is the second record under it, which the next
    // start does not keep. `u64::MAX` alone is left out, the one id that is never given.
    let held: BTreeSet<u64> = trie
        .all_leaves()
        .iter()
        .filter_map(|leaf| {
            let meta = leaf.leaf_meta.read().ok()?;
            meta.as_ref().map(|meta| meta.template_id)
        })
        .collect();
    assert_eq!(
        held.len() as u64,
        templates,
        "catalog: two templates share an id"
    );
    assert!(
        held.is_subset(&ids),
        "catalog: a template with an id no record names: {held:?}"
    );
    assert!(
        !held.contains(&u64::MAX),
        "catalog: a template holds the id past the last one"
    );
    let mut highest = ids.range(..u64::MAX).next_back().copied();
    for _ in 0..2 {
        let Ok(next) = trie.next_template_id() else {
            break;
        };
        assert!(
            highest.is_none_or(|highest| highest < next),
            "catalog: the next template id is {next}, and {highest:?} is taken"
        );
        highest = Some(next);
    }
    for record in &backfill {
        match record {
            catalog::Record::TimeSource {
                template_id,
                source,
            } => {
                assert!(
                    ids.contains(template_id),
                    "catalog: a time source for template {template_id}, which no record names"
                );
                assert!(
                    TimeSource::from_record(source).is_some(),
                    "catalog: a time source `{source}` the reader does not know"
                );
            }
            other => panic!("catalog: the rebuild asks for {other:?} to be appended"),
        }
    }
    let mut durable = records.to_vec();
    durable.extend(backfill);
    match Trie::from_catalog_with("fuzz", &durable, time_columns(), &mut |_| {}) {
        Ok((again, more)) => {
            assert!(
                more.is_empty(),
                "catalog: a second start still has time sources to record: {more:?}"
            );
            assert_eq!(again.template_count(), templates, "catalog: a second start");
        }
        Err(e) => panic!("catalog: the records do not rebuild a trie a second time: {e}"),
    }
}

// ── manifest.log ───────────────────────────────────────────────────────────────────

fn check_manifest(
    bytes: &[u8],
    frames: &[Range<usize>],
    said: &[Option<Value>],
    tear: Option<usize>,
) -> Vec<Outcome> {
    let (whole, torn_at) = manifest::parse(bytes);
    assert_eq!(torn_at, tear.map(|at| at as u64), "manifest: the tear");
    assert_eq!(whole.len(), frames.len(), "manifest: one outcome a frame");

    let mut outcomes = Vec::with_capacity(frames.len());
    let mut records = Vec::new();
    for (((offset, decoded), range), said) in whole.iter().zip(frames).zip(said) {
        let at = range.start - HEADER;
        assert_eq!(*offset, at as u64, "manifest: a frame's offset");
        let outcome = match decoded {
            Decoded::Record(record) => {
                check_written_back("manifest", at, record, said.as_ref(), &[]);
                records.push(record.clone());
                Outcome::Record
            }
            Decoded::Unknown(kind) => Outcome::Unknown(kind.clone()),
            Decoded::Malformed(_) => Outcome::Malformed,
        };
        check_kind("manifest", &MANIFEST_KINDS, at, said.as_ref(), &outcome);
        outcomes.push(outcome);
    }

    // A frame read alone is what it was in the file: nothing before it decides it.
    for (index, range) in sampled(bytes, frames) {
        let at = range.start - HEADER;
        let (mut one, torn) = manifest::parse(&bytes[at..range.end]);
        assert_eq!(torn, None, "manifest: the frame at {at}, read alone");
        assert_eq!(one.len(), 1, "manifest: the frame at {at}, read alone");
        let alone = one.pop().map(|(offset, decoded)| {
            assert_eq!(offset, 0, "manifest: the frame at {at}, read alone");
            decoded
        });
        assert_eq!(
            alone.as_ref(),
            Some(&whole[index].1),
            "manifest: the frame at {at}, read alone"
        );
        assert_eq!(
            manifest::decode(&bytes[range.clone()]),
            whole[index].1,
            "manifest: the payload at {at}, decoded outside the file"
        );
    }

    if let Some(tear) = tear {
        let (cut, torn) = manifest::parse(&bytes[..tear]);
        assert_eq!(torn, None, "manifest: torn again after the cut");
        assert_eq!(
            cut, whole,
            "manifest: the cut changed the records before it"
        );
    }
    check_live(&records);
    outcomes
}

/// Whether `text` is `len` of the hex digits the writers write: `0` to `9` and `a` to
/// `f`. Not a sign, which a number read by radix takes, and not a capital: no build has
/// written one, and a start matches a file to its id by the text `hex` writes, so a
/// reader that took `0A` for `0a` would name a file no start can find.
fn written_hex(text: &str, len: usize) -> bool {
    text.len() == len
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// A file id is the 32 digits `hex` writes or it is not one: nothing else reads as
/// sixteen bytes, and what does read is the id `hex` spells that way.
fn check_file_id(text: &str) {
    let digits = written_hex(text, 32);
    match manifest::file_id_of(text) {
        Some(id) => {
            assert!(
                digits,
                "manifest: the file id `{text}` is not 32 of the digits `hex` writes and read \
                 as {}",
                manifest::hex(&id)
            );
            assert!(
                manifest::hex(&id) == text,
                "manifest: the file id `{text}` read as {}",
                manifest::hex(&id)
            );
        }
        None => assert!(!digits, "manifest: the file id `{text}` did not read"),
    }
}

/// A source names a load when it is `load:` and the 16 digits `load_source` writes, and
/// only then.
fn check_source(source: &str) -> Option<u64> {
    let digits = source
        .strip_prefix("load:")
        .filter(|rest| written_hex(rest, 16));
    let load = manifest::load_of(source);
    match (load, digits) {
        (Some(id), Some(digits)) => assert!(
            format!("{id:016x}") == digits,
            "manifest: the source `{source}` read as load {id:016x}"
        ),
        (Some(id), None) => panic!(
            "manifest: the source `{source}` is not `load:` and 16 of the digits a load's id \
             is written in, and read as load {id:016x}"
        ),
        (None, Some(_)) => panic!("manifest: the source `{source}` did not read as a load"),
        (None, None) => {}
    }
    load
}

/// What a start does with a manifest's records: the live set, the loads and where each
/// load's files are looked for. The live set is worked out a second way here - the last
/// word on a path, a `replace` retiring before it adds - and the two must agree.
fn check_live(records: &[manifest::Record]) {
    let mut last: BTreeMap<&str, Option<&FileRecord>> = BTreeMap::new();
    let mut stepped = BTreeMap::new();
    let mut loads = BTreeSet::new();
    let mut published: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        manifest::apply(&mut stepped, record);
        let (named, gone, source): (&[FileRecord], &[String], Option<&String>) = match record {
            manifest::Record::Add { files, source } => (files, &[], source.as_ref()),
            manifest::Record::Seal { files } => (files, &[], None),
            manifest::Record::Remove { files } => (&[], files, None),
            manifest::Record::Replace {
                add,
                remove,
                source,
                ..
            } => (add, remove, source.as_ref()),
        };
        for path in gone {
            last.insert(path, None);
        }
        for file in named {
            check_file_id(&file.file_id);
            last.insert(&file.path, Some(file));
        }
        // The load that published a path is the one named by the last record to add it;
        // a record that adds it under no load takes it from the load that had it, and a
        // `seal` adds nothing.
        let load = source.and_then(|source| check_source(source));
        if !matches!(record, manifest::Record::Seal { .. }) {
            for file in named {
                match load {
                    Some(load) => published.insert(file.path.clone(), load),
                    None => published.remove(&file.path),
                };
            }
        }
        loads.extend(load);
    }

    let live = manifest::live(records);
    assert_eq!(
        stepped, live,
        "manifest: applied one by one, a different live set"
    );
    assert_eq!(
        live.len(),
        last.values().flatten().count(),
        "manifest: the live set"
    );
    for (path, file) in &live {
        assert_eq!(
            path, &file.path,
            "manifest: a live file under another's path"
        );
        assert_eq!(
            last.get(path.as_str()).copied().flatten(),
            Some(file),
            "manifest: `{path}` is live and is not the last word on that path"
        );
    }

    assert_eq!(manifest::load_ids(records), loads, "manifest: the loads");
    assert_eq!(
        manifest::published_by_load(records),
        published,
        "manifest: the load that published each path"
    );
}

// ── What the writers write ─────────────────────────────────────────────────────────

/// One record of either file.
enum Written {
    Catalog(catalog::Record),
    Manifest(manifest::Record),
}

/// The fuzz bytes as a run of choices. Past the end every choice is 0, so any bytes at
/// all build something and a short input builds a small file.
struct Choices<'a> {
    bytes: &'a [u8],
}

impl Choices<'_> {
    fn byte(&mut self) -> u8 {
        match self.bytes.split_first() {
            Some((first, rest)) => {
                self.bytes = rest;
                *first
            }
            None => 0,
        }
    }

    fn flag(&mut self) -> bool {
        self.byte() & 1 == 1
    }

    /// Mostly one byte, so that template ids meet and a column finds its template, and
    /// now and then all eight: the numbers at the far end of the range.
    fn number(&mut self) -> u64 {
        let first = self.byte();
        if first < 0xF0 {
            return u64::from(first);
        }
        let mut wide = [0u8; 8];
        for byte in &mut wide {
            *byte = self.byte();
        }
        u64::from_le_bytes(wide)
    }

    /// Up to 23 bytes as text; what is not UTF-8 becomes U+FFFD.
    fn text(&mut self) -> String {
        let len = usize::from(self.byte() % 24).min(self.bytes.len());
        let (text, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        String::from_utf8_lossy(text).into_owned()
    }

    /// A time between 1970 and 2100, to the nanosecond: the range the writer's spelling
    /// of a time is known to read back in.
    fn time(&mut self) -> DateTime<Utc> {
        let secs = (self.number() % 4_102_444_800) as i64;
        let nanos = u32::from(self.byte()) * 3_921_568;
        DateTime::from_timestamp(secs, nanos).unwrap_or_default()
    }

    /// The seven types a column record names, and the null of a column that has only
    /// held nulls.
    fn column_type(&mut self) -> Option<ColumnType> {
        match self.byte() % 8 {
            0 => None,
            1 => Some(ColumnType::String),
            2 => Some(ColumnType::Int64),
            3 => Some(ColumnType::Float64),
            4 => Some(ColumnType::Datetime),
            5 => Some(ColumnType::Bool),
            6 => Some(ColumnType::Uuid),
            _ => Some(ColumnType::Ip),
        }
    }

    /// One of eight paths most of the time, so that records name each other's files and
    /// the live set has something to decide.
    fn path(&mut self) -> String {
        match self.byte() {
            byte if byte < 0xE0 => format!("2024-01-15/seg_0_{:06}.col", byte % 8),
            _ => self.text(),
        }
    }

    fn paths(&mut self) -> Vec<String> {
        (0..self.byte() % 4).map(|_| self.path()).collect()
    }

    /// Up to three files, each with the id it must read back as.
    fn files(&mut self, ids: &mut Vec<(String, [u8; 16])>) -> Vec<FileRecord> {
        (0..self.byte() % 4)
            .map(|_| {
                let path = self.path();
                let mut id = [0u8; 16];
                for byte in &mut id {
                    *byte = self.byte();
                }
                let file_id = manifest::hex(&id);
                ids.push((file_id.clone(), id));
                let sealed = self.flag();
                FileRecord {
                    path,
                    file_id,
                    sealed,
                    rows: sealed.then(|| self.number()),
                    min_ts: sealed.then(|| self.number() as i64),
                    max_ts: sealed.then(|| self.number() as i64),
                    finished: self.flag(),
                }
            })
            .collect()
    }

    /// No source, a load's, a compaction's, or some other word that is not a load's; with
    /// the load it names.
    fn source(&mut self, loads: &mut BTreeSet<u64>) -> Option<String> {
        match self.byte() % 4 {
            0 => None,
            1 => {
                let load = self.number();
                loads.insert(load);
                Some(manifest::load_source(load))
            }
            2 => Some(manifest::COMPACT_SOURCE.to_string()),
            _ => Some(self.text()).filter(|text| !text.starts_with("load:")),
        }
    }

    fn record(&mut self, ids: &mut Vec<(String, [u8; 16])>, loads: &mut BTreeSet<u64>) -> Written {
        match self.byte() % 7 {
            0 => Written::Catalog(catalog::Record::Template {
                id: self.number(),
                created_at: self.time(),
                tokens: (0..self.byte() % 4)
                    .map(|_| match self.flag() {
                        true => Token::Promoted {
                            promoted: self.text(),
                            value: self.text(),
                        },
                        false => Token::Key { key: self.text() },
                    })
                    .collect(),
            }),
            1 => Written::Catalog(catalog::Record::Column {
                template_id: self.number(),
                name: self.text(),
                col_type: self.column_type(),
                widened_from: self.column_type(),
                at: self.flag().then(|| self.time()),
            }),
            2 => Written::Catalog(catalog::Record::TimeSource {
                template_id: self.number(),
                source: match self.byte() % 3 {
                    0 => "arrival".to_string(),
                    1 => format!("column:{}", self.text()),
                    _ => self.text(),
                },
            }),
            3 => Written::Manifest(manifest::Record::Add {
                files: self.files(ids),
                source: self.source(loads),
            }),
            4 => Written::Manifest(manifest::Record::Seal {
                files: self.files(ids),
            }),
            5 => Written::Manifest(manifest::Record::Remove {
                files: self.paths(),
            }),
            _ => Written::Manifest(manifest::Record::Replace {
                add: self.files(ids),
                remove: self.paths(),
                source: self.source(loads),
                wal: self
                    .flag()
                    .then(|| WalMark::new(self.number(), self.text(), self.number())),
            }),
        }
    }
}

/// The fifth mode: records built from the input, written as `Catalog::append` and
/// `Manifest::append` write them - `serde_json::to_vec`, then `frame::frame` - into one
/// file, and read back by both readers.
///
/// Each reader returns its own records, equal and in order, and skips the other's as an
/// unknown kind by its name: nothing malformed, nothing torn. A file id reads back as the
/// sixteen bytes it was spelled from, and the loads are the ones the sources were made
/// from. This is the one mode in which a reader that refuses a record it should take is
/// caught, whatever it refuses it for: a type, a `null`, a number at the end of the range.
fn check_written(choices: &[u8]) {
    let mut choices = Choices { bytes: choices };
    let mut ids = Vec::new();
    let mut loads = BTreeSet::new();
    let mut records = Vec::new();
    while !choices.bytes.is_empty() && records.len() < MAX_WRITTEN {
        records.push(choices.record(&mut ids, &mut loads));
    }

    let mut file = Vec::new();
    for record in &records {
        let payload = match record {
            Written::Catalog(record) => serde_json::to_vec(record),
            Written::Manifest(record) => serde_json::to_vec(record),
        };
        match payload {
            Ok(payload) => file.extend_from_slice(&frame::frame(&payload)),
            Err(e) => panic!("a record could not be written: {e}"),
        }
    }

    let catalog = catalog::parse(&file);
    let (manifest, torn_at) = manifest::parse(&file);
    assert_eq!(catalog.torn_at, None, "catalog: the writer's file is torn");
    assert_eq!(torn_at, None, "manifest: the writer's file is torn");
    assert!(
        catalog.malformed.is_empty(),
        "catalog: could not read what the writer wrote: {:?}",
        catalog.malformed
    );
    assert_eq!(
        manifest.len(),
        records.len(),
        "manifest: one outcome a frame"
    );

    let mut read = catalog.records.iter();
    let mut skipped = catalog.unknown.iter();
    for (record, (at, decoded)) in records.iter().zip(&manifest) {
        match record {
            Written::Catalog(record) => {
                assert_eq!(
                    read.next(),
                    Some(record),
                    "catalog: the record written at {at} is not the next one read"
                );
                assert!(
                    matches!(decoded, Decoded::Unknown(kind) if CATALOG_KINDS.contains(&kind.as_str())),
                    "manifest: the catalog's record at {at} was not skipped by its kind: {decoded:?}"
                );
            }
            Written::Manifest(record) => {
                assert!(
                    matches!(decoded, Decoded::Record(read) if read == record),
                    "manifest: wrote {record:?} at {at} and read {decoded:?}"
                );
                assert!(
                    matches!(skipped.next(), Some((offset, kind))
                        if offset == at && MANIFEST_KINDS.contains(&kind.as_str())),
                    "catalog: the manifest's record at {at} was not skipped by its kind"
                );
            }
        }
    }
    assert!(
        read.next().is_none() && skipped.next().is_none(),
        "catalog: a record or a skip nobody wrote"
    );

    for (text, id) in &ids {
        assert_eq!(
            manifest::file_id_of(text),
            Some(*id),
            "manifest: the file id `{text}` did not read back"
        );
    }
    let written: Vec<manifest::Record> = records
        .into_iter()
        .filter_map(|record| match record {
            Written::Manifest(record) => Some(record),
            Written::Catalog(_) => None,
        })
        .collect();
    assert_eq!(
        manifest::load_ids(&written),
        loads,
        "manifest: the loads are not the ones written"
    );

    // And everything that holds of any file holds of this one.
    let seen = check_file(&file);
    assert_eq!(seen.tear, None, "the writer's file is torn");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table is Castagnoli's, and so is the reader's: the two agree on the check
    /// value, so a frame built here is a frame the reader takes.
    #[test]
    fn the_harness_crc_is_the_readers() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b"123456789"), frame::crc32c(b"123456789"));
        assert_eq!(framed([&b"{}"[..]].into_iter()), frame::frame(b"{}"));
    }

    /// Each mode reaches the record decoders: a line is a record in the file it belongs
    /// to and an unknown kind in the other, and under a new kind it is unknown to both.
    #[test]
    fn a_line_is_a_record_in_its_own_file_and_unknown_to_the_other() {
        let line = br#"{"kind":"time_source","template_id":101,"source":"arrival"}"#;
        let seen = check_file(&framed([&line[..]].into_iter()));
        assert_eq!(seen.catalog, vec![Outcome::Record]);
        assert_eq!(seen.manifest, vec![Outcome::Unknown("time_source".into())]);
        let (kind, payload) = of_an_unknown_kind(line);
        assert_eq!(kind, nearly("time_source", line.len()));
        let seen = check_file(&framed([payload.as_slice()].into_iter()));
        assert_eq!(seen.catalog, vec![Outcome::Unknown(kind.clone())]);
        assert_eq!(seen.manifest, vec![Outcome::Unknown(kind)]);
        for mode in 0..8u8 {
            let mut data = vec![mode];
            data.extend_from_slice(line);
            data.push(b'\n');
            run(&data);
        }
    }

    /// Every near spelling of every known kind is a kind neither file has, and each of
    /// the eight is a spelling of its own.
    #[test]
    fn a_kind_spelled_nearly_right_is_unknown_to_both() {
        for kind in CATALOG_KINDS.iter().chain(&MANIFEST_KINDS) {
            let near: BTreeSet<String> = (0..8).map(|pick| nearly(kind, pick)).collect();
            assert!(near.len() >= 7, "{kind}: {near:?}");
            for near in near {
                assert!(!CATALOG_KINDS.contains(&near.as_str()), "{near}");
                assert!(!MANIFEST_KINDS.contains(&near.as_str()), "{near}");
            }
        }
    }

    /// A payload past 64KiB is one frame and one record. The fuzzer's inputs stop at a
    /// few kilobytes, so a length read from fewer than its four bytes is caught here.
    #[test]
    fn a_payload_longer_than_two_bytes_can_count_is_one_record() {
        let name = "n".repeat(70_000);
        let line =
            format!(r#"{{"kind":"time_source","template_id":101,"source":"column:{name}"}}"#);
        let seen = check_file(&framed([line.as_bytes()].into_iter()));
        assert_eq!(seen.tear, None);
        assert_eq!(seen.catalog, vec![Outcome::Record]);
        assert_eq!(seen.manifest, vec![Outcome::Unknown("time_source".into())]);
        for mode in 0..8u8 {
            let mut data = vec![mode];
            data.extend_from_slice(line.as_bytes());
            data.push(b'\n');
            run(&data);
        }
    }

    /// The fifth mode builds every kind of both files, so what it holds the readers to is
    /// not held of an empty file: seven records from seven choices, each a different kind.
    #[test]
    fn the_choices_build_every_kind_of_record() {
        let mut kinds = BTreeSet::new();
        for first in 0..7u8 {
            let mut choices = Choices {
                bytes: &[first, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            };
            let payload = match choices.record(&mut Vec::new(), &mut BTreeSet::new()) {
                Written::Catalog(record) => serde_json::to_value(record),
                Written::Manifest(record) => serde_json::to_value(record),
            };
            let payload = payload.expect("a record is JSON");
            kinds.insert(payload["kind"].as_str().expect("a kind").to_string());
        }
        let mut known: Vec<&str> = CATALOG_KINDS
            .iter()
            .chain(&MANIFEST_KINDS)
            .copied()
            .collect();
        known.sort_unstable();
        assert_eq!(kinds.iter().map(String::as_str).collect::<Vec<_>>(), known);
        run(&[
            4, 0, 7, 1, 2, 3, 1, 7, 2, 4, 3, 1, 0, 6, 1, 0, 1, 1, 9, 1, 1, 2, 3,
        ]);
    }
}
