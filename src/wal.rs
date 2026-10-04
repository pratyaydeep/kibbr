//! The write-ahead log: where a line is durable before `/ingest` says it was taken.
//!
//! ```text
//! <data-dir>/wal/
//! ├── seg_000001.wal      segments, numbered in the order they were written
//! ├── seg_000002.wal
//! └── checkpoint          one u64 as text: every position up to it is in a row group
//!
//! segment  [4]  'K' 'W' 'A' 'L'
//!          [1]  version      1
//!          [3]  reserved     zero
//!          [8]  stream       u64 - the log's id: random, never 0
//!          [8]  base         u64 - no record here is below it, none before it reaches it
//!          [4]  crc          CRC-32C of the 24 bytes before it
//!          then one frame per flush, framed as catalog.log is (frame.rs):
//!          [4]  payload_len
//!          [4]  crc          CRC-32C of the payload
//!          [1]  kind         1 = a batch of records, zstd
//!          [4]  raw_len      the batch's bytes before compression
//!          [..]              one zstd frame holding the batch
//!
//! batch    [4]  count        u32 - how many records
//!          [8]  time         i64, once per record, in their order: the effective time
//!                            the line's row was first filed under, unix nanoseconds
//!          then the records, back to back
//!
//! record   [4]  crc          CRC-32C of the rest of the record
//!          [8]  seq          u64 - its position in the stream
//!          [8]  template_id  u64 - the template the line was routed to: the catalog's id
//!          [8]  received_at  i64 - unix nanoseconds
//!          [4]  len          u32
//!          [N]               the line, as it arrived
//! ```
//!
//! Integers are little-endian. The CRC is CRC-32C, the one every other record in the data
//! directory is checked with.
//!
//! **The time beside each record is what a replay is held to, and the record is not.** A
//! row group covers a position of one template on one UTC day, so a replay has to know
//! the day a line was filed under before it can tell whether a row group holds it. Read
//! back off the line, that day would depend on what parses it - the epoch window in force,
//! the build - and a start under other settings would look for the line in the wrong day:
//! skip one that nothing holds, or store one twice. So the day the row *was* filed under
//! is written down when it is, beside the template it was routed to: neither is decided
//! again. It also means a line a row group already holds is skipped without being parsed,
//! which is nearly every line a start reads.
//!
//! **What a `200` means now.** A line is parsed and routed to its template first - the
//! record carries the template's id, and a template's catalog record is durable before any
//! log record names it - then written here and flushed, and only then acknowledged. From
//! that point nothing loses it: a store that cannot take the row delays it until the next
//! start replays it, and never turns into an error for the sender (`error::IngestError`).
//!
//! **One writer, and no timer.** Bodies queue; one thread takes whatever has queued,
//! compresses it, writes it and flushes once, and then takes whatever queued while it did.
//! A sender waits one flush, not a flush and a timer: measured with a 4ms flush, 4ms
//! against 9ms behind a 5ms wait and 27ms behind a 20ms one, for no more throughput.
//!
//! **A body is in the log whole or not at all.** A flush takes bodies, never part of one:
//! `batch_lines` is where it stops taking *more* of them, so a body larger than that is
//! the last of its flush, whole. So one frame is one or more whole bodies, a kill leaves a
//! body's lines all in the log or none of them, and a body the log refuses has no line in
//! it - a shipper that sends it again stores nothing twice. Split at the bound instead, a body
//! could be durable up to a line and refused from there: the `503` would be true of half
//! of it, and the retry would store the other half a second time.
//!
//! **One applier, in log order.** A flushed batch goes down a channel to the one thread
//! that hands rows to the store, each with its position. That order is what a row group's
//! range rests on: every buffer of one template and one day holds its rows in position
//! order, so the rows a row group is cut from are a run with nothing of that template and
//! day skipped inside it, and "positions `first` to `last`" says exactly which lines the
//! row group made durable. Replay skips a record a durable row group's range holds and
//! puts back every other - by the range and not by the checkpoint, because a row group
//! reaches the disk before the checkpoint moves past it, and a replay that trusted the
//! checkpoint alone would store those rows twice.
//!
//! **A position a row group could name is never given out twice.** Positions start at 1
//! and only grow, across segments and across restarts: a flush that fails burns the
//! positions it had taken, and a start resumes above everything it can see - the last
//! record, the newest segment's base, the checkpoint - and above anything a stretch it
//! could not read might have held. A position given out twice would be two lines that one
//! row group's range covers, and replay would drop the second. The only positions that
//! come round again are ones no flush ever made durable, which no row was ever given.
//!
//! **A run writes only segments it made.** A start never appends to a segment an earlier
//! process left: it opens the next number. So a record torn by a kill can only be the
//! last thing in a segment, the bytes after a tear are never a later run's records, and
//! a reader never has to cut a file to keep them apart - nothing here is ever truncated
//! but by the writer taking back its own failed write.
//!
//! **The checkpoint moves while the server runs** (`Wal::advance`): to just before the
//! oldest position that is still only in memory. Segments wholly behind it are deleted
//! then - never the newest, which is what carries the stream's id and its next position
//! when nothing else is left. The file is a hint: it saves replay from holding each line
//! behind it against the row groups, and it is what lets segments go. It is written by
//! rename, and one that is lost costs a longer replay and no row.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use thiserror::Error;

use crate::db::Placed;
use crate::frame;

/// The log's directory inside the data directory. Beside `data/`, never under it: a start
/// deletes every file there that the manifest does not name.
pub const DIR: &str = "wal";
const CHECKPOINT_FILE: &str = "checkpoint";

const MAGIC: [u8; 4] = *b"KWAL";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 28;
const KIND_BATCH: u8 = 1;
/// `crc`, `seq`, `template_id`, `received_at`, `len`.
const RECORD_HEADER: usize = 32;

/// A segment rolls to the next number once it is this long.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Where a flush stops taking more bodies: once it holds this many lines. An upper bound
/// on what rides together and not a target or a wait - a flush takes what has queued - and
/// not a bound on a body, which is never split: one that takes a flush past this is the
/// last the flush takes.
pub const DEFAULT_BATCH_LINES: usize = 500;

/// And once it holds this many bytes of lines: 500 lines are 125KB of ordinary logs, and
/// can be 32GB of the largest lines there are.
const BATCH_BYTES: usize = 8 * 1024 * 1024;

/// The most one body may come to as records: its lines, and forty bytes of header and
/// time for each. 1GiB, which the largest body the door takes by default stays under - 64MiB
/// of `{}` lines, twenty-two million of them, are 896MiB here - and which keeps a frame's
/// lengths inside a `u32`. A body past it is refused before it is queued (`Refused::TooLarge`):
/// written, it would be a frame no start could read.
const MAX_BODY_RAW: usize = 1024 * 1024 * 1024;

/// The most a batch may claim to decompress to: one body, and the bodies a flush took
/// before it - under `BATCH_BYTES` of records, and their times. What is written is held to
/// this as what is read is, so a frame this build wrote is one it reads.
const MAX_RAW: usize = MAX_BODY_RAW + 2 * BATCH_BYTES;

// A frame says how long its batch is in four bytes.
const _: () = assert!(MAX_RAW <= u32::MAX as usize);

/// The least a log that could not write writes, and takes back, to find out whether it
/// can again: enough to need a block the volume may not have. It writes as much as the
/// flush that failed, when that was more.
const PROBE_BYTES: usize = 64 * 1024;

/// zstd 3: 253 bytes a line down to 88 on the generated logs, with no CPU cost that
/// shows beside the flush.
const ZSTD_LEVEL: i32 = 3;

/// The most bytes of lines that may wait for the writer: what bounds the memory a stalled
/// disk can take. Past it a body is shed with a `503`, and its sender keeps it. How many
/// bodies may wait, each a parked thread, is the door's to bound, before it reads one.
const MAX_QUEUED_BYTES: usize = 256 * 1024 * 1024;

/// The most positions one unreadable byte of a segment is taken to have held: what a
/// start skips, per byte it could not read, before it gives out a position. A record is
/// forty bytes and more before compression and zstd does not reach a thousand to one on
/// records whose positions all differ, so this is past anything a frame can hold.
const POSITIONS_PER_LOST_BYTE: u64 = 1024;

/// How long a sender sleeps between looks at whether the log's threads are still there.
/// A flush wakes its senders; this is how one learns of a thread that ended instead.
const STOPPED_POLL: Duration = Duration::from_millis(200);

#[derive(Debug, Error)]
pub enum WalError {
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A frame whose checksum holds and whose contents this build cannot read. Not a tear
    /// - those are the bytes a writer wrote - so nothing is guessed: the start stops.
    #[error(
        "{path}: offset {offset}: {what}. The frame's checksum holds, so these are bytes a \
         writer wrote and not a write a stop cut short; this build does not guess at them. \
         Nothing was replayed past it: start the build that wrote the log, or move the whole \
         wal/ directory aside - the next start then begins a new log, and the lines only \
         the old one holds are not stored. Never one segment: its positions would be given \
         out again"
    )]
    Corrupt {
        path: PathBuf,
        offset: u64,
        what: String,
    },
    /// A segment whose header will not read and which is longer than a header: not a
    /// segment cut short as it was created, which holds nothing, but one that may hold
    /// lines - of another version, or damaged.
    #[error(
        "{path}: {why}, and the file is longer than a header, so it may hold lines this \
         build cannot read. Nothing was replayed: start the build that wrote the log, or \
         move the whole wal/ directory aside - the next start then begins a new log, and \
         the lines only the old one holds are not stored"
    )]
    Header { path: PathBuf, why: String },
    /// Two streams in one directory. A row group's range names one stream, and a record
    /// of another would be held against ranges that were never about it.
    #[error(
        "{a} and {b} carry different stream ids: segments of two write-ahead logs are in one \
         directory, and this build does not guess which one the stored row groups came from"
    )]
    Streams { a: PathBuf, b: PathBuf },
    /// What the replay's caller could not do with a record. Carried as text: the caller's
    /// error is the store's, and the log does not depend on the store.
    #[error("replaying position {seq}: {detail}")]
    Replay { seq: u64, detail: String },
}

fn io(path: &Path) -> impl Fn(std::io::Error) -> WalError + '_ {
    move |source| WalError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// A line on stderr that never panics: `eprintln!` does when stderr is gone, and a panic
/// here would end the thread every `/ingest` waits on.
fn say(message: &str) {
    let _ = writeln!(std::io::stderr().lock(), "kibbr: {message}");
}

/// Take a lock, recovering from a poisoning: what these guard is changed in steps that do
/// not stop halfway - a push, a pop, a counter - so there is no torn state to inherit.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn segment_name(number: u64) -> String {
    format!("seg_{number:06}.wal")
}

/// The number in `seg_NNNNNN.wal`, and `None` for any other name.
fn number_of(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("seg_")?.strip_suffix(".wal")?;
    if digits.len() < 6 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn header_bytes(stream: u64, base: u64) -> [u8; HEADER_LEN] {
    let mut out = [0u8; HEADER_LEN];
    out[..4].copy_from_slice(&MAGIC);
    out[4] = VERSION;
    out[8..16].copy_from_slice(&stream.to_le_bytes());
    out[16..24].copy_from_slice(&base.to_le_bytes());
    let crc = frame::crc32c(&out[..24]);
    out[24..].copy_from_slice(&crc.to_le_bytes());
    out
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(b)
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(b)
}

/// A segment's `(stream, base)`, or what is wrong with its header.
fn read_header(bytes: &[u8]) -> Result<(u64, u64), String> {
    let Some(header) = bytes.get(..HEADER_LEN) else {
        return Err(format!(
            "{} byte(s), short of the {HEADER_LEN}-byte header",
            bytes.len()
        ));
    };
    if header[..4] != MAGIC {
        return Err("it does not begin with KWAL".to_string());
    }
    if frame::crc32c(&header[..24]) != u32_at(header, 24) {
        return Err("its header's checksum fails".to_string());
    }
    if header[4] != VERSION || header[5..8] != [0, 0, 0] {
        return Err(format!(
            "header version {}, and this build reads version {VERSION}",
            header[4]
        ));
    }
    let (stream, base) = (u64_at(header, 8), u64_at(header, 16));
    if stream == 0 {
        return Err("its stream id is 0, which no log is given".to_string());
    }
    Ok((stream, base))
}

fn put_record(out: &mut Vec<u8>, seq: u64, template_id: u64, received_at: i64, line: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&template_id.to_le_bytes());
    out.extend_from_slice(&received_at.to_le_bytes());
    out.extend_from_slice(&(line.len() as u32).to_le_bytes());
    out.extend_from_slice(line);
    let crc = frame::crc32c(&out[start + 4..]);
    out[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

/// One record, as replay hands it over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record<'a> {
    pub seq: u64,
    pub template_id: u64,
    pub received_at: i64,
    /// The effective time the line's row was first filed under: its day is the day a
    /// row group must be of to hold this position.
    pub time: i64,
    pub line: &'a [u8],
}

/// What reading the log back found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Scanned {
    pub segments: usize,
    /// Records past the checkpoint: the ones handed to the caller.
    pub records: u64,
    /// Records at or behind the checkpoint, not handed over.
    pub behind: u64,
    /// Segments that ended in a record a stop cut short.
    pub torn: usize,
    /// Stretches of a segment that would not read, with whole frames after them.
    pub corrupt: u64,
    /// The last position read, at or behind the checkpoint or past it; 0 for none.
    pub last_seq: u64,
}

/// Where the checkpoint went and what it let go of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Advanced {
    pub checkpoint: u64,
    pub deleted: usize,
}

/// One line on its way into the log: its bytes, the row the applier hands the store, and
/// the effective time that row is filed under.
pub struct Line {
    pub raw: Vec<u8>,
    pub time: i64,
    pub placed: Placed,
}

/// Why the log did not take a body. Nothing of the body is in the log - but for the one
/// case no reader can settle, a writer that died with the body in its hands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The log cannot write: the error of the flush that failed.
    Cannot(String),
    /// More is waiting on the log than it is allowed to hold: a shed, as the door's
    /// are, and not a verdict on the disk.
    Busy,
    /// The body is more, as records, than one frame holds. The sender's to fix: no retry
    /// of the same body will fit.
    TooLarge { bytes: usize, limit: usize },
}

/// How the log is written.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub segment_bytes: u64,
    pub batch_lines: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            batch_lines: DEFAULT_BATCH_LINES,
        }
    }
}

/// What `/stats` reports.
#[derive(Debug, Clone)]
pub struct Stats {
    pub stream: u64,
    pub segments: usize,
    pub first_segment: u64,
    pub bytes: u64,
    pub last_seq: u64,
    pub applied: u64,
    pub checkpoint: u64,
    pub flushes: u64,
    pub healthy: bool,
    pub last_error: Option<String>,
}

/// What has become of one body. `Logged` is the writer's word the moment a flush returns,
/// and `Applied` the applier's: a body's sender waits for the second, so a client that
/// ingests and then queries sees what it sent.
struct Outcome {
    progress: Mutex<Progress>,
    moved: Condvar,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Progress {
    Queued,
    Logged,
    Applied,
    Refused(Refused),
}

impl Outcome {
    fn new() -> Self {
        Self {
            progress: Mutex::new(Progress::Queued),
            moved: Condvar::new(),
        }
    }

    fn set(&self, to: Progress) {
        *lock(&self.progress) = to;
        self.moved.notify_all();
    }

    /// Until the body is applied, or refused. `Err` carries why the log could not take it.
    fn wait(&self, shared: &Shared) -> Result<(), Refused> {
        let mut progress = lock(&self.progress);
        loop {
            match &*progress {
                Progress::Applied => return Ok(()),
                Progress::Refused(why) => return Err(why.clone()),
                // In the log, and the applier is gone: durable, so it is acknowledged,
                // and the next start applies it.
                Progress::Logged if shared.stopped.load(Ordering::Acquire) => return Ok(()),
                // Taken off the queue by a writer that then died: whether its flush
                // landed nobody is left to say. A writer still running says, and is
                // waited for - told "refused" here, a body it then wrote would be stored
                // by the next start and again by its sender's retry.
                Progress::Queued if shared.writer_gone.load(Ordering::Acquire) => {
                    return Err(Refused::Cannot(
                        "the write-ahead log's writer stopped while this body was being \
                         written; it may or may not be in the log"
                            .to_string(),
                    ))
                }
                Progress::Queued | Progress::Logged => {}
            }
            progress = self
                .moved
                .wait_timeout(progress, STOPPED_POLL)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

/// Whether a body is past what one frame holds, told from its bytes before it is parsed:
/// every line of it that is not blank counted as a record.
///
/// An upper bound - a line that will not parse is no record - so a body this lets by is
/// one `commit` takes, and one it refuses is refused before gigabytes are spent parsing a
/// body no frame would hold. Bodies of ordinary size are not walked: under a twenty-first
/// of the bound in bytes, no number of lines reaches it.
pub fn too_large(body: &[u8]) -> Option<Refused> {
    const PER_LINE: usize = RECORD_HEADER + 8;
    // The shortest line is a byte and its newline.
    if (body.len() / 2 + 1) * PER_LINE + body.len() <= MAX_BODY_RAW {
        return None;
    }
    let bytes: usize = body
        .split(|b| *b == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .map(|line| PER_LINE + line.len())
        .sum();
    (bytes > MAX_BODY_RAW).then_some(Refused::TooLarge {
        bytes,
        limit: MAX_BODY_RAW,
    })
}

struct Submission {
    lines: Vec<Line>,
    bytes: usize,
    received_at: i64,
    outcome: Arc<Outcome>,
}

#[derive(Default)]
struct Queue {
    waiting: VecDeque<Submission>,
    /// The bytes of the lines waiting: held under `MAX_QUEUED_BYTES`.
    bytes: usize,
    /// The flush tick asks, while the log cannot write, whether it can again.
    probe: bool,
    closing: bool,
}

/// One segment on disk.
#[derive(Debug, Clone)]
struct Seg {
    number: u64,
    base: u64,
    path: PathBuf,
    /// Its length, as the writer last left it: what `/stats` adds up without asking the
    /// file system.
    len: u64,
}

struct Shared {
    queue: Mutex<Queue>,
    wake: Condvar,
    /// Every segment on disk, in order. The last is the one being written.
    segments: Mutex<Vec<Seg>>,
    /// The last position a flush made durable.
    written: AtomicU64,
    /// The last position the applier has handed to the store.
    applied: AtomicU64,
    checkpoint: AtomicU64,
    /// The checkpoint its file holds: behind `checkpoint` for as long as the file could
    /// not be written.
    filed: AtomicU64,
    /// Whether the last flush could write. Read by the door without a lock.
    healthy: AtomicBool,
    last_error: Mutex<Option<String>>,
    /// No thread is taking lines: before `start`, after `close`, or because one died.
    stopped: AtomicBool,
    /// The writer has ended, and whatever it had taken off the queue it will not finish.
    writer_gone: AtomicBool,
    /// The checkpoint has reached the last position written, and the segment being
    /// written holds records: once it is worth it (`Log::ensure`) the writer opens the
    /// next at its next flush, so this one - every line of it in a row group - is
    /// deleted, where it would have stayed until it was full.
    drained: AtomicBool,
    flushes: AtomicU64,
}

/// What a thread of the log's leaves behind when it ends, however it ends: the log takes
/// nothing more, and every body still queued is told so rather than left waiting.
struct Stopping(Arc<Shared>);

impl Drop for Stopping {
    fn drop(&mut self) {
        let shared = &self.0;
        let mut queue = lock(&shared.queue);
        shared.stopped.store(true, Ordering::Release);
        if queue.closing {
            return;
        }
        // Not at a close: a thread died. Unhealthy, so the door refuses before it reads,
        // and it stays so - the writer, if it is the one left, does not say otherwise.
        let why = "a thread of the write-ahead log stopped; restart the server".to_string();
        shared.healthy.store(false, Ordering::Release);
        *lock(&shared.last_error) = Some(why.clone());
        queue.bytes = 0;
        for waiting in queue.waiting.drain(..) {
            waiting
                .outcome
                .set(Progress::Refused(Refused::Cannot(why.clone())));
        }
    }
}

/// A flushed batch on its way to the applier.
struct Batch {
    first: u64,
    rows: Vec<Placed>,
    /// The bodies whose lines these are.
    taken: Vec<Arc<Outcome>>,
}

/// What the applier does with a batch: every row with its position, in order.
pub type Apply = Box<dyn FnMut(Vec<(u64, Placed)>) + Send>;

enum State {
    /// Opened or created, and read back: `next_seq` is the first position a writer gives.
    Idle {
        next_seq: u64,
        next_number: u64,
    },
    Running {
        writer: JoinHandle<()>,
        applier: JoinHandle<()>,
    },
    Closed,
}

pub struct Wal {
    dir: PathBuf,
    stream: u64,
    shared: Arc<Shared>,
    state: Mutex<State>,
}

impl Wal {
    /// The log in `base`, if there is one: its segments' headers and its checkpoint, and
    /// nothing read past them yet. `None` for a directory with no segment a start can use.
    ///
    /// **A segment no longer than a header, whose header will not read, was cut short as it
    /// was made** - by a stop, or by a write that failed and could not be cleared away. The
    /// header is written and flushed before a record can follow it, so such a file holds
    /// no line: it is removed with a warning. One that is longer and will not read stops
    /// the start (`WalError::Header`): it may hold lines, and leaving it out would be
    /// guessing that it does not.
    pub fn open(base: &Path, warn: &mut dyn FnMut(String)) -> Result<Option<Self>, WalError> {
        let dir = base.join(DIR);
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io(&dir)(e)),
        };
        // An entry that will not list stops the start: a segment left out would be lines
        // not replayed, and a next position below ones already given out.
        let mut numbers: Vec<u64> = Vec::new();
        for entry in entries {
            let name = entry.map_err(io(&dir))?.file_name();
            numbers.extend(number_of(&name.to_string_lossy()));
        }
        numbers.sort_unstable();
        let highest = numbers.last().copied();

        let mut segments: Vec<Seg> = Vec::new();
        let mut stream: Option<(u64, PathBuf)> = None;
        for number in &numbers {
            let path = dir.join(segment_name(*number));
            let mut head = Vec::with_capacity(HEADER_LEN);
            File::open(&path)
                .and_then(|f| f.take(HEADER_LEN as u64).read_to_end(&mut head))
                .map_err(io(&path))?;
            match read_header(&head) {
                Ok((id, base)) => {
                    match &stream {
                        Some((first, a)) if *first != id => {
                            return Err(WalError::Streams {
                                a: a.clone(),
                                b: path,
                            })
                        }
                        Some(_) => {}
                        None => stream = Some((id, path.clone())),
                    }
                    let len = fs::metadata(&path).map_err(io(&path))?.len();
                    segments.push(Seg {
                        number: *number,
                        base,
                        path,
                        len,
                    });
                }
                Err(why) => {
                    if fs::metadata(&path).map_err(io(&path))?.len() > HEADER_LEN as u64 {
                        return Err(WalError::Header { path, why });
                    }
                    // One that will not go is left: it holds nothing, its number is not
                    // used again, and the next start tries once more.
                    let removed = match fs::remove_file(&path) {
                        Ok(()) => "is removed".to_string(),
                        Err(e) => format!("could not be removed ({e})"),
                    };
                    warn(format!(
                        "{}: {why} - a segment cut short as it was created; it holds no \
                         line, and {removed}",
                        path.display()
                    ));
                }
            }
        }
        let Some((stream, _)) = stream else {
            return Ok(None);
        };
        let checkpoint = read_checkpoint(&dir);
        let newest_base = segments.last().map_or(1, |s| s.base);
        Ok(Some(Self {
            dir,
            stream,
            shared: Arc::new(Shared::new(segments, checkpoint)),
            state: Mutex::new(State::Idle {
                next_seq: newest_base.max(checkpoint.saturating_add(1)),
                next_number: highest.map_or(1, |n| n + 1),
            }),
        }))
    }

    /// A new log in `base`: a new stream, with its id drawn here. The id reaches the disk
    /// with the first segment (`start`), before any row can carry a position of it.
    ///
    /// **A new id and never the old one**, whatever is left in the directory: a log that
    /// is lost and made again starts its positions again, and under the old id its record
    /// 17 would be skipped on replay because some week-old row group covers 17.
    pub fn create(base: &Path) -> Result<Self, WalError> {
        let dir = base.join(DIR);
        fs::create_dir_all(&dir).map_err(io(&dir))?;
        frame::sync_dir(base).map_err(io(base))?;
        // What an earlier stream left says nothing about this one.
        match fs::remove_file(dir.join(CHECKPOINT_FILE)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(io(&dir)(e)),
            _ => {}
        }
        let mut id = [0u8; 8];
        File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut id))
            .map_err(io(Path::new("/dev/urandom")))?;
        let highest = fs::read_dir(&dir)
            .map_err(io(&dir))?
            .flatten()
            .filter_map(|e| number_of(&e.file_name().to_string_lossy()))
            .max();
        Ok(Self {
            dir,
            // 0 is "no stream" in a row group's header.
            stream: u64::from_le_bytes(id).max(1),
            shared: Arc::new(Shared::new(Vec::new(), 0)),
            state: Mutex::new(State::Idle {
                next_seq: 1,
                next_number: highest.map_or(1, |n| n + 1),
            }),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The log's id: what a row group written from it carries as its stream.
    pub fn stream(&self) -> u64 {
        self.stream
    }

    /// Every position up to this one is in a row group.
    pub fn checkpoint(&self) -> u64 {
        self.shared.checkpoint.load(Ordering::Acquire)
    }

    /// The last position the applier has handed to the store.
    pub fn applied(&self) -> u64 {
        self.shared.applied.load(Ordering::Acquire)
    }

    /// Whether the last flush could write: what the door reads before it takes a body.
    pub fn is_healthy(&self) -> bool {
        self.shared.healthy.load(Ordering::Acquire)
    }

    pub fn last_error(&self) -> Option<String> {
        lock(&self.shared.last_error).clone()
    }

    /// Read every record past the checkpoint back, in order, and hand each to `each`. An
    /// `Err` from it stops the replay and is returned.
    ///
    /// **A record a stop cut short is the normal end of a segment, and not corruption.** A
    /// frame with fewer bytes than its length says, or one at the end of the file whose
    /// checksum fails, is what a process killed mid-write leaves: it is warned about and
    /// skipped, and the start goes on. Refusing to start over it would turn a clean crash
    /// into an outage. The line it held was never acknowledged - the flush that would
    /// have acknowledged it never returned.
    ///
    /// **Nothing is cut.** The bytes after the last whole frame stay where they are: no
    /// run appends to a segment it did not make, so they are in nobody's way, and the
    /// checkpoint deletes the segment whole. A reader that truncated at the first thing
    /// it could not read would, on a segment with one damaged length in the middle,
    /// destroy every acknowledged frame after it.
    ///
    /// **A stretch that will not read with whole frames after it is stepped over**, and
    /// warned about as corrupt: the lines in it are lost and the frames after it are not.
    /// The next frame is found by looking for one - a length that fits, the batch's kind,
    /// a checksum that holds, contents that decode.
    ///
    /// What stops the start is a frame whose checksum holds and whose contents will not
    /// decode: see `WalError::Corrupt`.
    ///
    /// **Every segment is flushed before it is read.** After a kill the last frames can
    /// be whole in the page cache and never have reached the disk. Read as they are and
    /// acted on - a row group written from them, a position given out above them - they
    /// would be relied on by things that are durable, and lost with the power.
    pub fn replay(
        &self,
        warn: &mut dyn FnMut(String),
        each: &mut dyn FnMut(Record<'_>) -> Result<(), String>,
    ) -> Result<Scanned, WalError> {
        let segments = lock(&self.shared.segments).clone();
        let checkpoint = self.checkpoint();
        let mut found = Scanned {
            segments: segments.len(),
            ..Scanned::default()
        };
        // Bytes that would not read. They may have been frames, and those frames lines
        // that were stored: see where `next_seq` is set below.
        let mut lost = 0u64;
        for seg in &segments {
            File::open(&seg.path)
                .and_then(|f| f.sync_all())
                .map_err(io(&seg.path))?;
            let bytes = fs::read(&seg.path).map_err(io(&seg.path))?;
            let mut at = HEADER_LEN;
            loop {
                // Checked, as every frame in the data directory is, against the bytes that
                // remain before anything is sliced: a length of 0xFFFFFFFF is a tear,
                // never a 4GB read.
                let payload = match frame::read(&bytes, at, u32::MAX) {
                    frame::Next::End => break,
                    frame::Next::Frame { payload, end } => Some((payload, end)),
                    frame::Next::Tear => None,
                };
                let Some((payload, end)) = payload else {
                    // Not a frame. A whole one further on makes this damage in the
                    // middle of the segment; none makes it the segment's torn end.
                    match resync(&bytes, at + 1, found.last_seq) {
                        Some(next) => {
                            lost += (next - at) as u64;
                            found.corrupt += 1;
                            warn(format!(
                                "{}: corrupt WAL entry at offset {at}: {} byte(s) that will \
                                 not read, with whole entries after them; skipping, and the \
                                 lines in them are lost",
                                seg.path.display(),
                                next - at
                            ));
                            at = next;
                            continue;
                        }
                        None => {
                            lost += (bytes.len() - at) as u64;
                            found.torn += 1;
                            warn(format!(
                                "{}: torn record at offset {at} of {}: a write a stop cut \
                                 short, whose lines were never acknowledged; skipped",
                                seg.path.display(),
                                bytes.len(),
                            ));
                            break;
                        }
                    }
                };
                let corrupt = |what: String| WalError::Corrupt {
                    path: seg.path.clone(),
                    offset: at as u64,
                    what,
                };
                let raw = unpack(payload).map_err(corrupt)?;
                for record in records(&raw).map_err(corrupt)? {
                    // Positions only grow. One that does not is a record a second time,
                    // and applying it would be the line a second time.
                    if record.seq <= found.last_seq {
                        return Err(corrupt(format!(
                            "position {} follows position {}",
                            record.seq, found.last_seq
                        )));
                    }
                    found.last_seq = record.seq;
                    if record.seq <= checkpoint {
                        found.behind += 1;
                        continue;
                    }
                    found.records += 1;
                    let seq = record.seq;
                    each(record).map_err(|detail| WalError::Replay { seq, detail })?;
                }
                at = end;
            }
        }
        // Above everything read - and above whatever the bytes that would not read could
        // have held. After a kill those are a frame that was never flushed, and so never
        // applied; but if they were once whole frames, rows were stored from them, and a
        // row group somewhere names their positions. Given out again, those positions
        // would be new lines a replay took for stored ones. Positions are not scarce: a
        // gap costs nothing.
        if let State::Idle { next_seq, .. } = &mut *lock(&self.state) {
            let gap = lost.saturating_mul(POSITIONS_PER_LOST_BYTE);
            *next_seq = (*next_seq).max(found.last_seq.saturating_add(1).saturating_add(gap));
        }
        // Everything read back is the caller's to apply before a writer starts: applied
        // is where the log ends.
        let end = found.last_seq.max(checkpoint);
        self.shared.written.store(end, Ordering::Release);
        self.shared.applied.store(end, Ordering::Release);
        Ok(found)
    }

    /// Start writing: a new segment, the writer and the applier. Called once, after
    /// `replay`; the log takes no line before it.
    ///
    /// **A segment that cannot be made does not stop the start.** A full volume is the
    /// usual reason, and a server on one still has rows to answer queries about: it
    /// starts with the log unhealthy - `/ingest` answers `503` with the reason - and the
    /// flush tick's probe makes the segment once the volume has room. Only a thread that
    /// will not start is an error.
    pub fn start(&self, options: Options, apply: Apply) -> Result<(), WalError> {
        let mut state = lock(&self.state);
        let State::Idle {
            next_seq,
            next_number,
        } = *state
        else {
            return Ok(());
        };
        let mut log = Log {
            dir: self.dir.clone(),
            stream: self.stream,
            next_seq,
            next_number,
            segment: None,
            segment_bytes: options.segment_bytes.max(1),
            batch_lines: options.batch_lines.max(1),
            failed_bytes: 0,
            shared: Arc::clone(&self.shared),
        };
        // Every position below the first this run gives out was applied by the caller, or
        // was never given out - burned by a flush that failed, or skipped past a stretch
        // that would not read. Said here, so the checkpoint can cross such a gap and the
        // segments before it go; left at the last record read, they would stay until a
        // new line's row group closed, and every start meanwhile would leave one more.
        let before = next_seq.saturating_sub(1);
        self.shared.written.fetch_max(before, Ordering::AcqRel);
        self.shared.applied.fetch_max(before, Ordering::AcqRel);
        // Before a line is taken: the stream's id is on disk before any row carries a
        // position of it.
        if let Err(e) = log.ensure(next_seq) {
            let why = e.to_string();
            say(&format!(
                "ERROR write-ahead log - {why}; /ingest answers 503 until it can write"
            ));
            self.shared.healthy.store(false, Ordering::Release);
            *lock(&self.shared.last_error) = Some(why);
        }
        self.shared.stopped.store(false, Ordering::Release);
        let (tx, rx) = channel::<Batch>();
        let spawn = |name: &str, run: Box<dyn FnOnce() + Send>| {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(run)
                .map_err(io(&self.dir))
        };
        let shared = Arc::clone(&self.shared);
        let applier = spawn(
            "wal-apply",
            Box::new(move || apply_loop(&shared, &rx, apply)),
        )?;
        let writer = spawn("wal-write", Box::new(move || write_loop(log, &tx)))?;
        *state = State::Running { writer, applier };
        Ok(())
    }

    /// Put a body's lines in the log - all of them, in one flush - and return once they
    /// are durable and applied. `Err` is why the log could not take the body: none of its
    /// lines is in the log then, and the sender must keep them. `received_at` is the
    /// body's arrival time, in unix nanoseconds: what a row with no time of its own is
    /// stored under, now and at every replay.
    ///
    /// Blocks for a flush, so it is called from a blocking thread.
    pub fn commit(&self, lines: Vec<Line>, received_at: i64) -> Result<(), Refused> {
        if lines.is_empty() {
            return Ok(());
        }
        let bytes: usize = lines.iter().map(|l| RECORD_HEADER + 8 + l.raw.len()).sum();
        if bytes > MAX_BODY_RAW {
            return Err(Refused::TooLarge {
                bytes,
                limit: MAX_BODY_RAW,
            });
        }
        let outcome = Arc::new(Outcome::new());
        {
            // The health is read under the queue's lock, which a failing flush holds while
            // it empties the queue: a body is refused here or by that flush, never left
            // waiting behind it.
            let mut queue = lock(&self.shared.queue);
            if !self.shared.healthy.load(Ordering::Acquire) {
                return Err(Refused::Cannot(
                    self.last_error()
                        .unwrap_or_else(|| "the last flush could not write".to_string()),
                ));
            }
            if queue.closing || self.shared.stopped.load(Ordering::Acquire) {
                return Err(Refused::Cannot("the write-ahead log is closed".to_string()));
            }
            // A body always fits an empty queue: the bound is on what waits, and a
            // body larger than it is still a body.
            if !queue.waiting.is_empty() && queue.bytes + bytes > MAX_QUEUED_BYTES {
                return Err(Refused::Busy);
            }
            queue.bytes += bytes;
            queue.waiting.push_back(Submission {
                lines,
                bytes,
                received_at,
                outcome: Arc::clone(&outcome),
            });
        }
        self.shared.wake.notify_one();
        outcome.wait(&self.shared)
    }

    /// Ask the writer, while the log cannot write, to try again: make a segment if there
    /// is none, write to it, flush it, and take the write back (`Log::probe`). The flush
    /// tick calls this, so the door opens again within a tick of the disk coming back and
    /// not at the first body brave enough to try - and stays shut while it has not.
    ///
    /// Not once a thread of the log has died: there is nothing a write could show then,
    /// and what `/ingest` says - restart the server - is not to be written over.
    pub fn probe(&self) {
        if self.is_healthy() || self.shared.stopped.load(Ordering::Acquire) {
            return;
        }
        lock(&self.shared.queue).probe = true;
        self.shared.wake.notify_one();
    }

    /// Move the checkpoint to just before the oldest position still only in memory, and
    /// delete the segments wholly behind it.
    ///
    /// `oldest` is asked for that position - the lowest one in any buffer, or held back
    /// for any other reason - **after** the applied position is read here. In that order
    /// a row is never missed: one applied before the read is in a buffer when `oldest`
    /// looks, or already in a row group; one applied after it is above the bound.
    ///
    /// **The file is a hint, and a file that cannot be written does not keep a segment.**
    /// Every position up to the checkpoint is in a row group whether or not the file says
    /// so, and on a full volume the segments behind it are the room the file needs: so
    /// they go either way, and the failure is handed back beside what moved. A start
    /// after it reads an older checkpoint, finds the segments gone, and holds the rest
    /// against the row groups as it would have.
    ///
    /// Segments are looked for even when the checkpoint has not moved: each start opens
    /// one, and a server started and stopped with nothing sent would otherwise leave one
    /// behind every time. They leave the list under its lock and the disk after it, so
    /// nothing waits on an unlink.
    ///
    /// One caller at a time - the flush tick, and then the shutdown once the tick's task
    /// is joined. `None` when nothing moved and nothing was deleted.
    pub fn advance(
        &self,
        oldest: impl FnOnce() -> Option<u64>,
    ) -> (Option<Advanced>, Option<WalError>) {
        let applied = self.applied();
        let before = self.checkpoint();
        let target = oldest()
            .map_or(applied, |o| o.saturating_sub(1).min(applied))
            .max(before);
        let mut problem = None;
        // Whenever the file is behind: when the checkpoint moves, and at every look after
        // a write that failed, until one succeeds.
        if target > self.shared.filed.load(Ordering::Acquire) {
            let path = self.dir.join(CHECKPOINT_FILE);
            let tmp = self.dir.join(format!("{CHECKPOINT_FILE}.tmp"));
            let written = File::create(&tmp)
                .and_then(|mut f| {
                    f.write_all(target.to_string().as_bytes())?;
                    f.sync_all()
                })
                .and_then(|()| fs::rename(&tmp, &path));
            match written {
                Ok(()) => self.shared.filed.store(target, Ordering::Release),
                Err(e) => problem = Some(io(&path)(e)),
            }
        }
        self.shared.checkpoint.store(target, Ordering::Release);

        let gone: Vec<Seg> = {
            let mut segments = lock(&self.shared.segments);
            // Nothing in the segment being written is still needed: the writer leaves it
            // at a flush soon after (`Log::ensure`), and the tick after that deletes it.
            if segments.last().is_some_and(|newest| newest.base <= target)
                && target == self.shared.written.load(Ordering::Acquire)
            {
                self.shared.drained.store(true, Ordering::Release);
            }
            // Every record of a segment is below the next one's base. Never the newest:
            // it is being written, and it is what says which stream this is and where it
            // had got to.
            let behind = segments
                .windows(2)
                .take_while(|pair| pair[1].base <= target.saturating_add(1))
                .count();
            segments.drain(..behind).collect()
        };
        for seg in &gone {
            match fs::remove_file(&seg.path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                // Left on the disk and off the list: the next start finds it, finds
                // every line of it behind the checkpoint or in a row group, and deletes
                // it at its first tick.
                Err(e) => problem = problem.or(Some(io(&seg.path)(e))),
            }
        }
        let moved = (target > before || !gone.is_empty()).then_some(Advanced {
            checkpoint: target,
            deleted: gone.len(),
        });
        (moved, problem)
    }

    pub fn stats(&self) -> Stats {
        let segments = lock(&self.shared.segments).clone();
        Stats {
            stream: self.stream,
            segments: segments.len(),
            first_segment: segments.first().map_or(0, |s| s.number),
            bytes: segments.iter().map(|s| s.len).sum(),
            last_seq: self.shared.written.load(Ordering::Acquire),
            applied: self.applied(),
            checkpoint: self.checkpoint(),
            flushes: self.shared.flushes.load(Ordering::Relaxed),
            healthy: self.is_healthy(),
            last_error: self.last_error(),
        }
    }

    /// Stop the writer and the applier, once what is queued is written and applied.
    pub fn close(&self) {
        let state = std::mem::replace(&mut *lock(&self.state), State::Closed);
        if let State::Running { writer, applier } = state {
            lock(&self.shared.queue).closing = true;
            self.shared.wake.notify_all();
            let _ = writer.join();
            let _ = applier.join();
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        self.close();
    }
}

impl Shared {
    fn new(segments: Vec<Seg>, checkpoint: u64) -> Self {
        Self {
            queue: Mutex::new(Queue::default()),
            wake: Condvar::new(),
            segments: Mutex::new(segments),
            written: AtomicU64::new(checkpoint),
            applied: AtomicU64::new(checkpoint),
            checkpoint: AtomicU64::new(checkpoint),
            filed: AtomicU64::new(checkpoint),
            healthy: AtomicBool::new(true),
            last_error: Mutex::new(None),
            // No writer yet: `start` clears it.
            stopped: AtomicBool::new(true),
            writer_gone: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            flushes: AtomicU64::new(0),
        }
    }
}

/// Missing or unparseable is 0: the checkpoint is a hint, and without it a replay reads
/// more and skips by the row groups' ranges.
fn read_checkpoint(dir: &Path) -> u64 {
    fs::read_to_string(dir.join(CHECKPOINT_FILE))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

/// A frame's payload as the batch it holds.
fn unpack(payload: &[u8]) -> Result<Vec<u8>, String> {
    let (Some(kind), Some(len)) = (payload.first(), payload.get(1..5)) else {
        return Err("a frame too short to hold a batch".to_string());
    };
    if *kind != KIND_BATCH {
        return Err(format!(
            "frame kind {kind}, and this build reads kind {KIND_BATCH}"
        ));
    }
    let raw_len = u32_at(len, 0) as usize;
    if raw_len > MAX_RAW {
        return Err(format!(
            "a batch of {raw_len} bytes, past the {MAX_RAW} a flush can hold"
        ));
    }
    let raw = zstd::bulk::decompress(&payload[5..], raw_len)
        .map_err(|e| format!("a batch that does not decompress ({e})"))?;
    if raw.len() != raw_len {
        return Err(format!(
            "a batch that decompresses to {} bytes and says {raw_len}",
            raw.len()
        ));
    }
    Ok(raw)
}

/// A batch's records, every one checked before the first is handed over: the count, each
/// length against the bytes that remain, each record's own checksum, and the batch used
/// up exactly.
///
/// **A record that fails here is not skipped.** The frame's checksum held, so these are
/// the bytes the writer wrote: a record inside them that will not check is a writer's
/// fault, not a tear, and stepping over it by a length the failed check did not vouch
/// for would be guessing at an acknowledged line.
fn records(raw: &[u8]) -> Result<Vec<Record<'_>>, String> {
    let Some(count) = raw.get(..4) else {
        return Err("a batch too short to hold its count".to_string());
    };
    let count = u32_at(count, 0) as usize;
    let Some(times) = count.checked_mul(8).and_then(|len| raw.get(4..4 + len)) else {
        return Err(format!(
            "a batch of {count} records with no room for their times"
        ));
    };
    let mut out = Vec::with_capacity(count);
    let mut at = 4 + times.len();
    for n in 0..count {
        let Some(head) = raw.get(at..at + RECORD_HEADER) else {
            return Err(format!("record {n} of {count} runs past its batch"));
        };
        let len = u32_at(head, 28) as usize;
        let Some(body) = raw.get(at + 4..at + RECORD_HEADER + len) else {
            return Err(format!(
                "record {n} of {count}: its line runs past its batch"
            ));
        };
        let seq = u64_at(head, 4);
        if frame::crc32c(body) != u32_at(head, 0) {
            return Err(format!(
                "corrupt WAL entry at seq {seq}: the record's own checksum fails"
            ));
        }
        out.push(Record {
            seq,
            template_id: u64_at(head, 12),
            received_at: u64_at(head, 20) as i64,
            time: u64_at(times, n * 8) as i64,
            line: &body[RECORD_HEADER - 4..],
        });
        at += RECORD_HEADER + len;
    }
    if at != raw.len() {
        return Err(format!(
            "{} byte(s) after the last of {count} records",
            raw.len() - at
        ));
    }
    Ok(out)
}

/// The offset of the next whole frame at or after `from`, if there is one: what tells
/// damage in the middle of a segment from its torn end.
///
/// A frame begins where a length that fits is followed, eight bytes on, by the batch's
/// kind and zstd's magic, its checksum holds, and its contents decode into records above
/// `after`. All of it, because this looks at every offset: on bytes that are not a frame
/// each holds by chance now and then, and all of them together do not.
fn resync(bytes: &[u8], from: usize, after: u64) -> Option<usize> {
    // A batch's payload begins with its kind, its length, and then the zstd frame, whose
    // first four bytes are zstd's own. Looked for before any checksum is taken: without
    // it a long stretch of compressed bytes offers a length that fits every few hundred
    // offsets, and each would cost a checksum over megabytes.
    const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
    let last = bytes
        .len()
        .checked_sub(frame::HEADER + 5 + ZSTD_MAGIC.len())?;
    (from..=last).find(|at| {
        let payload = at + frame::HEADER;
        if bytes[payload] != KIND_BATCH || bytes[payload + 5..payload + 9] != ZSTD_MAGIC {
            return false;
        }
        let frame::Next::Frame { payload, .. } = frame::read(bytes, *at, u32::MAX) else {
            return false;
        };
        unpack(payload).is_ok_and(|raw| {
            records(&raw).is_ok_and(|found| found.first().is_some_and(|r| r.seq > after))
        })
    })
}

// ── The writer ─────────────────────────────────────────────────────────────────────

struct Open {
    file: File,
    path: PathBuf,
    len: u64,
    frames: u64,
}

/// The writer's own: the segment being written and the next position.
struct Log {
    dir: PathBuf,
    stream: u64,
    next_seq: u64,
    next_number: u64,
    segment: Option<Open>,
    segment_bytes: u64,
    batch_lines: usize,
    /// How long the frame of the last flush that failed was; 0 once a flush has
    /// succeeded. What the probe has to be able to write before the door opens again.
    failed_bytes: usize,
    shared: Arc<Shared>,
}

impl Log {
    /// A segment to write to: the open one, or the next number once that is past its size
    /// or was abandoned. `base` is the position of the first record that would go into a
    /// new one: what its header says no record in it is below.
    ///
    /// **The header is flushed, and the directory with it, before a record follows.** A
    /// record flushed into a file whose entry a crash then loses was never durable.
    fn ensure(&mut self, base: u64) -> Result<(), WalError> {
        if let Some(open) = &self.segment {
            // Left early only once it is worth a new file and three flushes to be rid of:
            // a sixty-fourth of a segment. A server that takes a line a second and writes
            // each out at once would otherwise open a segment a line.
            let drained = self.shared.drained.swap(false, Ordering::AcqRel)
                && open.len >= (self.segment_bytes / 64).max(1);
            // Asked for with nothing in it, the segment in hand is already the next one.
            if open.frames == 0 || (open.len < self.segment_bytes && !drained) {
                return Ok(());
            }
        }
        let number = self.next_number;
        let path = self.dir.join(segment_name(number));
        let made = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .and_then(|mut file| {
                file.write_all(&header_bytes(self.stream, base))?;
                file.sync_all()?;
                frame::sync_dir(&self.dir)?;
                Ok(file)
            });
        let file = match made {
            Ok(file) => file,
            Err(e) => {
                // A name that is taken stays taken: the next try is the next number.
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    self.next_number += 1;
                } else {
                    let _ = fs::remove_file(&path);
                }
                return Err(io(&path)(e));
            }
        };
        self.next_number += 1;
        lock(&self.shared.segments).push(Seg {
            number,
            base,
            path: path.clone(),
            len: HEADER_LEN as u64,
        });
        self.segment = Some(Open {
            file,
            path,
            len: HEADER_LEN as u64,
            frames: 0,
        });
        Ok(())
    }

    /// One flush: the lines as one frame, written and flushed once.
    ///
    /// **A flush that fails is cut back**, so the frame is not found by a later start and
    /// stored for a sender that was told it was refused. If the cut fails too the segment
    /// is abandoned as it is and the next flush opens the next number. What is left at
    /// its end is then usually part of a frame, which a reader stops at - but it can be
    /// the whole frame, if it was the flush and not the write that failed: a start would
    /// then store lines whose senders were told `503`, and store them again when those
    /// senders retried. Two failures in a row on one file are past what this can promise.
    fn append(&mut self, first: u64, lines: &[(Line, i64)]) -> Result<(), WalError> {
        self.ensure(first)?;
        let size: usize = lines.iter().map(|(l, _)| RECORD_HEADER + l.raw.len()).sum();
        let mut raw = Vec::with_capacity(4 + lines.len() * 8 + size);
        raw.extend_from_slice(&(lines.len() as u32).to_le_bytes());
        for (line, _) in lines {
            raw.extend_from_slice(&line.time.to_le_bytes());
        }
        for (at, (line, received_at)) in lines.iter().enumerate() {
            put_record(
                &mut raw,
                first + at as u64,
                line.placed.template_id,
                *received_at,
                &line.raw,
            );
        }
        let Some(open) = self.segment.as_mut() else {
            return Err(io(&self.dir)(std::io::Error::other(
                "no segment to write to",
            )));
        };
        // Held to what a reader will take: `commit` and the writer's own bound keep a
        // flush under it, and a frame past it would be acknowledged and never read.
        if raw.len() > MAX_RAW {
            return Err(io(&open.path)(std::io::Error::other(format!(
                "a flush of {} bytes, past the {MAX_RAW} one frame holds",
                raw.len()
            ))));
        }
        let packed = zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(io(&open.path))?;
        let mut payload = Vec::with_capacity(5 + packed.len());
        payload.push(KIND_BATCH);
        payload.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        payload.extend_from_slice(&packed);
        let bytes = frame::frame(&payload);

        let start = open.len;
        let written = open
            .file
            .write_all(&bytes)
            .and_then(|()| open.file.sync_all());
        match written {
            Ok(()) => {
                open.len += bytes.len() as u64;
                open.frames += 1;
                self.failed_bytes = 0;
                if let Some(newest) = lock(&self.shared.segments).last_mut() {
                    newest.len = open.len;
                }
                Ok(())
            }
            Err(source) => {
                self.failed_bytes = bytes.len();
                let path = open.path.clone();
                let cut = open
                    .file
                    .set_len(start)
                    .and_then(|()| open.file.seek(SeekFrom::Start(start)).map(drop))
                    .and_then(|()| open.file.sync_all());
                if cut.is_err() {
                    self.segment = None;
                }
                Err(WalError::Io { path, source })
            }
        }
    }

    /// Whether the log can write again: a segment, and a block written to its end,
    /// flushed, and cut back off.
    ///
    /// **A write, because that is what failed, and as long as the one that did.** A flush
    /// of a file with nothing new in it succeeds on a full volume, and so does a small
    /// write on a volume with a little room: a probe that passed either way would open
    /// the door for a body that is then read, parsed and refused - every tick, for as
    /// long as the volume stayed full. The block is zeros, which a reader takes for a
    /// tear: a stop between the write and the cut leaves a tail the next start skips.
    fn probe(&mut self) -> Result<(), WalError> {
        self.ensure(self.next_seq)?;
        let Some(open) = self.segment.as_mut() else {
            return Ok(());
        };
        let start = open.len;
        let zeros = [0u8; PROBE_BYTES];
        let mut left = self.failed_bytes.max(PROBE_BYTES);
        let mut written = Ok(());
        while left > 0 && written.is_ok() {
            let take = left.min(zeros.len());
            written = open.file.write_all(&zeros[..take]);
            left -= take;
        }
        let written = written.and_then(|()| open.file.sync_all());
        let cut = open
            .file
            .set_len(start)
            .and_then(|()| open.file.seek(SeekFrom::Start(start)).map(drop))
            .and_then(|()| open.file.sync_all());
        let path = open.path.clone();
        if cut.is_err() {
            self.segment = None;
        }
        written.and(cut).map_err(io(&path))
    }
}

/// Says the writer has ended, before `Stopping` empties the queue it leaves.
struct WriterGone(Arc<Shared>);

impl Drop for WriterGone {
    fn drop(&mut self) {
        self.0.writer_gone.store(true, Ordering::Release);
    }
}

fn write_loop(mut log: Log, tx: &Sender<Batch>) {
    let shared = Arc::clone(&log.shared);
    // Dropped in reverse: gone first, then the queue emptied.
    let _stopping = Stopping(Arc::clone(&shared));
    let _gone = WriterGone(Arc::clone(&shared));
    loop {
        let mut lines: Vec<(Line, i64)> = Vec::new();
        let mut taken: Vec<Arc<Outcome>> = Vec::new();
        {
            let mut queue = lock(&shared.queue);
            while queue.waiting.is_empty() && !queue.probe && !queue.closing {
                queue = shared.wake.wait(queue).unwrap_or_else(|e| e.into_inner());
            }
            if queue.waiting.is_empty() && !queue.probe {
                return;
            }
            queue.probe = false;
            // Whatever has queued, in the order it queued, a whole body at a time: no more
            // bodies once the bound is reached, and never part of one.
            // So a flush is under `BATCH_BYTES` and one body, which is under `MAX_RAW`.
            let mut bytes = 0usize;
            while lines.len() < log.batch_lines && bytes < BATCH_BYTES {
                let Some(body) = queue.waiting.pop_front() else {
                    break;
                };
                queue.bytes -= body.bytes.min(queue.bytes);
                bytes += body.bytes;
                lines.extend(body.lines.into_iter().map(|line| (line, body.received_at)));
                taken.push(body.outcome);
            }
        }

        let result = if lines.is_empty() {
            log.probe()
        } else {
            // Taken whatever happens next: a position is never given out twice.
            let first = log.next_seq;
            log.next_seq += lines.len() as u64;
            log.append(first, &lines).map(|()| {
                let last = first + lines.len() as u64 - 1;
                shared.written.store(last, Ordering::Release);
                shared.flushes.fetch_add(1, Ordering::Relaxed);
                for outcome in &taken {
                    outcome.set(Progress::Logged);
                }
                let rows = lines.drain(..).map(|(line, _)| line.placed).collect();
                // The applier only ends before the writer if it died, and then every
                // sender is told so: `Outcome::wait`.
                let _ = tx.send(Batch {
                    first,
                    rows,
                    taken: std::mem::take(&mut taken),
                });
            })
        };
        match result {
            Ok(()) => {
                // Not while the applier is gone: the log can write, and nothing it wrote
                // would reach the store. `Stopping` said so under this lock, and it stays
                // said.
                let _queue = lock(&shared.queue);
                if !shared.stopped.load(Ordering::Acquire)
                    && !shared.healthy.swap(true, Ordering::AcqRel)
                {
                    say("write-ahead log - writing again");
                }
            }
            Err(e) => {
                // `to_string`, not the chain: a `WalError` prints its cause itself.
                let why = e.to_string();
                // Under the queue's lock, so no body joins the queue behind a flush that
                // has failed: see `Wal::commit`.
                let mut queue = lock(&shared.queue);
                if shared.healthy.swap(false, Ordering::AcqRel) {
                    say(&format!(
                        "ERROR write-ahead log - {why}; /ingest answers 503 until it can \
                         write again"
                    ));
                }
                *lock(&shared.last_error) = Some(why.clone());
                let refused = Progress::Refused(Refused::Cannot(why));
                for outcome in &taken {
                    outcome.set(refused.clone());
                }
                queue.bytes = 0;
                for waiting in queue.waiting.drain(..) {
                    waiting.outcome.set(refused.clone());
                }
            }
        }
    }
}

fn apply_loop(shared: &Arc<Shared>, rx: &Receiver<Batch>, mut apply: Apply) {
    let _stopping = Stopping(Arc::clone(shared));
    while let Ok(batch) = rx.recv() {
        let last = batch.first + batch.rows.len() as u64 - 1;
        apply(
            batch
                .rows
                .into_iter()
                .enumerate()
                .map(|(at, placed)| (batch.first + at as u64, placed))
                .collect(),
        );
        shared.applied.store(last, Ordering::Release);
        for outcome in batch.taken {
            outcome.set(Progress::Applied);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Row;

    /// A directory of this test's own, removed when it goes.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("kibbr-wal-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("scratch dir");
            Self(dir)
        }

        fn segments(&self) -> Vec<PathBuf> {
            let mut out: Vec<PathBuf> = fs::read_dir(self.0.join(DIR))
                .expect("wal dir")
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|e| e == "wal"))
                .collect();
            out.sort();
            out
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn line(n: u64) -> Line {
        Line {
            raw: format!("{{\"n\":{n}}}").into_bytes(),
            time: 1_000 + n as i64,
            placed: Placed {
                template_id: 100 + n % 3,
                time_column: "ts".to_string(),
                row: Row::new(),
            },
        }
    }

    fn body(from: u64, to: u64) -> Vec<Line> {
        (from..to).map(line).collect()
    }

    /// A started log, and the positions its applier was handed, in the order it was.
    fn started(scratch: &Scratch, options: Options) -> (Wal, Arc<Mutex<Vec<u64>>>) {
        let mut silent = |_: String| {};
        let wal = match Wal::open(&scratch.0, &mut silent).expect("open") {
            Some(wal) => {
                wal.replay(&mut silent, &mut |_| Ok(())).expect("replay");
                wal
            }
            None => Wal::create(&scratch.0).expect("create"),
        };
        let applied = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&applied);
        wal.start(
            options,
            Box::new(move |rows| lock(&seen).extend(rows.iter().map(|(seq, _)| *seq))),
        )
        .expect("start");
        (wal, applied)
    }

    /// Every record past the checkpoint as `(seq, template_id, received_at, time, line)`,
    /// what the read found, and what it warned.
    type Read = (Vec<(u64, u64, i64, i64, String)>, Scanned, Vec<String>);

    fn read_back(scratch: &Scratch) -> Result<Read, WalError> {
        let mut warnings = Vec::new();
        let mut records = Vec::new();
        let wal = Wal::open(&scratch.0, &mut |m| warnings.push(m))?.expect("a log");
        let scanned = wal.replay(&mut |m| warnings.push(m), &mut |r| {
            records.push((
                r.seq,
                r.template_id,
                r.received_at,
                r.time,
                String::from_utf8_lossy(r.line).into_owned(),
            ));
            Ok(())
        })?;
        Ok((records, scanned, warnings))
    }

    fn tiny() -> Options {
        Options {
            segment_bytes: 1,
            batch_lines: DEFAULT_BATCH_LINES,
        }
    }

    #[test]
    fn lines_come_back_in_order_with_their_positions_templates_and_times() {
        let scratch = Scratch::new("roundtrip");
        let (wal, applied) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 3), 111), Ok(()));
        assert_eq!(wal.commit(body(3, 5), -222), Ok(()));
        // Applied by the time `commit` returns, and in the log's order.
        assert_eq!(*lock(&applied), [1, 2, 3, 4, 5]);
        assert_eq!(wal.applied(), 5);
        wal.close();

        let (records, scanned, warnings) = read_back(&scratch).expect("read");
        assert_eq!(warnings, Vec::<String>::new());
        assert_eq!(scanned.records, 5);
        let want: Vec<(u64, u64, i64, i64, String)> = (0..5u64)
            .map(|n| {
                let at = if n < 3 { 111 } else { -222 };
                let time = 1_000 + n as i64;
                (n + 1, 100 + n % 3, at, time, format!("{{\"n\":{n}}}"))
            })
            .collect();
        assert_eq!(records, want);
    }

    /// A body is never split: one larger than `batch_lines` is one flush, whole, so a
    /// kill or a failed write leaves all of its lines in the log or none.
    #[test]
    fn a_body_larger_than_the_bound_is_one_flush() {
        let scratch = Scratch::new("bound");
        let options = Options {
            batch_lines: 2,
            ..Options::default()
        };
        let (wal, applied) = started(&scratch, options);
        assert_eq!(wal.commit(body(0, 7), 1), Ok(()));
        assert_eq!(*lock(&applied), [1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(wal.stats().flushes, 1);
        wal.close();
        let (records, _, _) = read_back(&scratch).expect("read");
        assert_eq!(
            records.iter().map(|r| r.0).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5, 6, 7]
        );
    }

    /// What a write a stop cut short leaves at the end of a segment, in each shape: fewer
    /// bytes than a frame's header, a frame whose length says more than is there, a whole
    /// frame whose checksum fails, and a run of zeros. Each is warned about and skipped,
    /// the start goes on, and nothing is cut: the file is as the stop left it.
    #[test]
    fn a_torn_last_record_of_any_shape_is_warned_about_and_skipped() {
        let mut bad_crc = frame::frame(b"\x01torn");
        bad_crc[4] ^= 0xff;
        let shapes: [(&str, Vec<u8>); 4] = [
            ("short", vec![7, 0, 0]),
            ("cut", [&4096u32.to_le_bytes()[..], &[9u8; 40][..]].concat()),
            ("crc", bad_crc),
            ("zeros", vec![0u8; 64]),
        ];
        for (name, junk) in shapes {
            let scratch = Scratch::new(&format!("torn-{name}"));
            let (wal, _) = started(&scratch, Options::default());
            assert_eq!(wal.commit(body(0, 4), 9), Ok(()));
            wal.close();
            let newest = scratch.segments().pop().expect("a segment");
            OpenOptions::new()
                .append(true)
                .open(&newest)
                .and_then(|mut f| f.write_all(&junk))
                .expect("append");
            let left = fs::read(&newest).expect("read");

            for _ in 0..2 {
                let (records, scanned, warnings) =
                    read_back(&scratch).expect("a tear stopped the start");
                assert_eq!(records.len(), 4, "{name}");
                assert_eq!((scanned.torn, scanned.corrupt), (1, 0), "{name}");
                assert!(
                    warnings.iter().any(|w| w.contains("torn record")),
                    "{name}: {warnings:?}"
                );
                assert_eq!(fs::read(&newest).expect("read"), left, "{name}");
            }
            // And the next run writes past it, in a segment of its own - at a position
            // past any the torn bytes could have held.
            let (wal, applied) = started(&scratch, Options::default());
            assert_eq!(wal.commit(body(4, 5), 9), Ok(()), "{name}");
            let next = lock(&applied)[0];
            assert!(
                next > 4 + junk.len() as u64 * POSITIONS_PER_LOST_BYTE,
                "{name}: {next}"
            );
        }
    }

    /// Damage in the middle of a segment costs the frame it is in and no other, whether
    /// it is in the frame's contents or in its length: the frames after it are found and
    /// read, and nothing is cut. A reader that stopped at the first thing it could not
    /// read - or cut the file there - would lose every acknowledged frame behind one bad
    /// length.
    #[test]
    fn damage_mid_segment_costs_its_frame_and_the_frames_after_it_are_read() {
        for (name, at_length) in [("payload", false), ("length", true)] {
            let scratch = Scratch::new(&format!("mid-{name}"));
            let (wal, _) = started(&scratch, Options::default());
            for n in 0..4 {
                assert_eq!(wal.commit(body(n, n + 1), 0), Ok(()));
            }
            wal.close();
            let path = scratch.segments().pop().expect("a segment");
            let mut bytes = fs::read(&path).expect("read");
            let first_len = u32_at(&bytes, HEADER_LEN) as usize;
            let second = HEADER_LEN + frame::HEADER + first_len;
            if at_length {
                bytes[second..second + 4].copy_from_slice(&0x00ff_ffffu32.to_le_bytes());
            } else {
                let second_len = u32_at(&bytes, second) as usize;
                bytes[second + frame::HEADER + second_len - 1] ^= 0x55;
            }
            fs::write(&path, &bytes).expect("write");

            let (records, scanned, warnings) = read_back(&scratch).expect("read");
            assert_eq!(
                records.iter().map(|r| r.0).collect::<Vec<_>>(),
                [1, 3, 4],
                "{name}"
            );
            assert_eq!((scanned.corrupt, scanned.torn), (1, 0), "{name}");
            assert!(
                warnings.iter().any(|w| w.contains("corrupt WAL entry")),
                "{name}: {warnings:?}"
            );
            assert_eq!(fs::read(&path).expect("read"), bytes, "{name}");
        }
    }

    /// A frame whose checksum holds and whose contents this build cannot read is not a
    /// tear, and the start does not guess: it stops, naming the segment.
    #[test]
    fn a_checksummed_frame_that_does_not_decode_stops_the_start() {
        let scratch = Scratch::new("undecodable");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        wal.close();
        let path = scratch.segments().pop().expect("a segment");
        OpenOptions::new()
            .append(true)
            .open(&path)
            .and_then(|mut f| f.write_all(&frame::frame(&[9, 0, 0, 0, 0])))
            .expect("append");
        match read_back(&scratch) {
            Err(WalError::Corrupt { what, .. }) => assert!(what.contains("kind 9"), "{what}"),
            other => panic!("read a frame of an unknown kind: {:?}", other.map(|r| r.1)),
        }
    }

    /// The checkpoint moves to just before the oldest position still in memory, deletes
    /// the segments wholly behind it and never the newest; and a start resumes above
    /// every position it gave out, whether or not the checkpoint file survived.
    #[test]
    fn the_checkpoint_deletes_what_is_behind_it_and_positions_are_never_reused() {
        let scratch = Scratch::new("checkpoint");
        let (wal, _) = started(&scratch, tiny());
        for n in 0..5 {
            assert_eq!(wal.commit(body(n, n + 1), 0), Ok(()));
        }
        // One byte to a segment: a segment a flush.
        assert_eq!(scratch.segments().len(), 5);

        // Position 3 is still only in memory: the checkpoint stops at 2, and the
        // segments holding 1 and 2 go.
        let moved = wal.advance(|| Some(3)).0;
        assert_eq!(
            moved,
            Some(Advanced {
                checkpoint: 2,
                deleted: 2
            })
        );
        assert_eq!(scratch.segments().len(), 3);
        assert_eq!(wal.advance(|| Some(3)).0, None);
        // An oldest position behind the checkpoint does not move it back.
        assert_eq!(wal.advance(|| Some(1)).0, None);
        assert_eq!(wal.checkpoint(), 2);

        // Nothing in memory: up to the last position applied. The newest segment stays.
        let moved = wal.advance(|| None).0.expect("moved");
        assert_eq!((moved.checkpoint, moved.deleted), (5, 2));
        assert_eq!(scratch.segments().len(), 1);
        let stream = wal.stream();
        wal.close();

        // The checkpoint file lost, and nothing left but the newest segment and the
        // empty one each start opens: the same stream, and the next position is still 6.
        fs::remove_file(scratch.0.join(DIR).join(CHECKPOINT_FILE)).expect("remove");
        let (wal, applied) = started(&scratch, tiny());
        assert_eq!(wal.stream(), stream);
        assert_eq!(wal.commit(body(5, 6), 0), Ok(()));
        assert_eq!(*lock(&applied), [6]);
        wal.close();
        let (records, _, _) = read_back(&scratch).expect("read");
        assert_eq!(records.iter().map(|r| r.0).collect::<Vec<_>>(), [5, 6]);
    }

    /// Records at or behind the checkpoint are not handed over.
    #[test]
    fn replay_starts_past_the_checkpoint() {
        let scratch = Scratch::new("past");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 6), 0), Ok(()));
        assert!(wal.advance(|| Some(5)).0.is_some());
        wal.close();
        let (records, scanned, _) = read_back(&scratch).expect("read");
        assert_eq!(records.iter().map(|r| r.0).collect::<Vec<_>>(), [5, 6]);
        assert_eq!((scanned.records, scanned.behind), (2, 4));
    }

    /// A segment a stop cut short as it was created holds no line: it is removed, and
    /// the start goes on with the ones before it.
    #[test]
    fn a_segment_cut_short_as_it_was_created_is_removed() {
        let scratch = Scratch::new("roll");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 2), 0), Ok(()));
        wal.close();
        let half = scratch.0.join(DIR).join(segment_name(7));
        fs::write(&half, &header_bytes(1, 1)[..11]).expect("write");
        // And one that is not the newest: a roll whose write failed and whose file could
        // not be cleared away, with segments made after it.
        let stale = scratch.0.join(DIR).join(segment_name(0));
        fs::write(&stale, [0u8; HEADER_LEN]).expect("write");

        let (records, _, warnings) = read_back(&scratch).expect("read");
        assert_eq!(records.len(), 2);
        assert_eq!(
            warnings.iter().filter(|w| w.contains("is removed")).count(),
            2,
            "{warnings:?}"
        );
        assert!(!half.exists() && !stale.exists());
        // And the log goes on from where its last whole segment left it.
        let (wal, applied) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(2, 3), 0), Ok(()));
        assert_eq!(*lock(&applied), [3]);
    }

    #[test]
    fn two_streams_in_one_directory_stop_the_start() {
        let scratch = Scratch::new("streams");
        let (wal, _) = started(&scratch, Options::default());
        let other = wal.stream() ^ 1;
        wal.close();
        fs::write(
            scratch.0.join(DIR).join(segment_name(9)),
            header_bytes(other.max(1), 1),
        )
        .expect("write");
        let mut silent = |_: String| {};
        assert!(matches!(
            Wal::open(&scratch.0, &mut silent),
            Err(WalError::Streams { .. })
        ));
    }

    /// A log made again in a directory that had one is given another id: its positions
    /// start again, and the id is what keeps them from being held against the old one's
    /// row groups.
    #[test]
    fn a_log_made_again_is_a_new_stream() {
        let scratch = Scratch::new("again");
        let first = Wal::create(&scratch.0).expect("create").stream();
        let second = Wal::create(&scratch.0).expect("create").stream();
        assert!(first != 0 && second != 0);
        assert_ne!(first, second);
    }

    /// A log that cannot write refuses the body with the reason, says it is unhealthy,
    /// refuses the next body without trying, and writes again once asked to try - above
    /// the positions the failed flush had taken.
    #[test]
    #[cfg(unix)]
    fn a_log_that_cannot_write_refuses_and_then_writes_again() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = Scratch::new("readonly");
        let (wal, applied) = started(&scratch, tiny());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        let dir = scratch.0.join(DIR);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

        // The next flush needs a new segment, and the directory will not take one.
        let refused = wal.commit(body(1, 3), 0);
        assert!(matches!(refused, Err(Refused::Cannot(_))), "{refused:?}");
        assert!(!wal.is_healthy());
        assert_eq!(
            wal.commit(body(3, 4), 0).err(),
            wal.last_error().map(Refused::Cannot)
        );
        wal.probe();
        std::thread::sleep(Duration::from_millis(50));
        assert!(!wal.is_healthy());

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
        wal.probe();
        for _ in 0..200 {
            if wal.is_healthy() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(wal.is_healthy());
        assert_eq!(wal.commit(body(4, 5), 0), Ok(()));
        // Positions 2 and 3 went with the flush that failed.
        assert_eq!(*lock(&applied), [1, 4]);
        wal.close();
        let (records, _, _) = read_back(&scratch).expect("read");
        assert_eq!(records.iter().map(|r| r.0).collect::<Vec<_>>(), [1, 4]);
    }

    /// Once the checkpoint has reached the end of the log, the segment being written is
    /// left at the next flush and deleted at the tick after: a small store does not keep
    /// every line twice until 64MB of them have gone by.
    #[test]
    fn a_segment_with_nothing_left_to_replay_is_left_and_deleted() {
        let scratch = Scratch::new("drained");
        // Small enough that three lines are worth leaving it for.
        let options = Options {
            segment_bytes: 4096,
            ..Options::default()
        };
        let (wal, _) = started(&scratch, options);
        assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
        assert_eq!(scratch.segments().len(), 1);
        assert_eq!(
            wal.advance(|| None).0,
            Some(Advanced {
                checkpoint: 3,
                deleted: 0
            })
        );
        // Still the one segment: nothing has been written since.
        assert_eq!(scratch.segments().len(), 1);
        assert_eq!(wal.commit(body(3, 4), 0), Ok(()));
        assert_eq!(scratch.segments().len(), 2);
        let moved = wal.advance(|| None).0.expect("moved");
        assert_eq!((moved.checkpoint, moved.deleted), (4, 1));
        assert_eq!(scratch.segments().len(), 1);
        wal.close();
        let (records, scanned, _) = read_back(&scratch).expect("read");
        assert!(records.is_empty());
        assert_eq!((scanned.behind, scanned.last_seq), (1, 4));
    }

    /// A log that was never started takes nothing, and says so, where it used to be able
    /// to queue a body nothing would ever write.
    #[test]
    fn a_log_that_is_not_started_refuses() {
        let scratch = Scratch::new("idle");
        let wal = Wal::create(&scratch.0).expect("create");
        assert!(wal.commit(body(0, 1), 0).is_err());
    }

    /// A segment that is longer than a header and whose header will not read stops the
    /// start: it may hold lines, and only one no longer than a header is a roll cut short.
    #[test]
    fn a_segment_longer_than_a_header_that_will_not_read_stops_the_start() {
        let scratch = Scratch::new("header");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 2), 0), Ok(()));
        wal.close();
        let path = scratch.segments().pop().expect("a segment");
        let mut bytes = fs::read(&path).expect("read");
        bytes[4] = 2;
        fs::write(&path, &bytes).expect("write");
        let mut silent = |_: String| {};
        match Wal::open(&scratch.0, &mut silent) {
            Err(WalError::Header { why, .. }) => assert!(why.contains("checksum"), "{why}"),
            other => panic!(
                "opened a segment whose header will not read: {:?}",
                other.map(|_| ())
            ),
        }
        assert!(path.exists(), "the segment was removed");
    }

    /// A start that cannot make its segment starts anyway, refusing: the server still has
    /// queries to answer, and the probe makes the segment once the directory will take it.
    #[test]
    #[cfg(unix)]
    fn a_start_that_cannot_make_a_segment_starts_refusing() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = Scratch::new("start-readonly");
        let wal = Wal::create(&scratch.0).expect("create");
        let dir = scratch.0.join(DIR);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");
        wal.start(Options::default(), Box::new(|_| {}))
            .expect("start");
        assert!(!wal.is_healthy());
        assert!(wal.commit(body(0, 1), 0).is_err());

        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
        wal.probe();
        for _ in 0..200 {
            if wal.is_healthy() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(wal.is_healthy());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    }

    /// The number in a test line, `{"n":N}`.
    fn n_of(line: &str) -> u64 {
        line.trim_start_matches("{\"n\":")
            .trim_end_matches('}')
            .parse()
            .expect("a test line")
    }

    /// Bodies from many senders at once: whatever the flushes took together, every body
    /// is whole - its lines at positions in a row, in its own order, under its own
    /// arrival time - and the applier is handed every position in the log's order.
    #[test]
    fn bodies_sent_together_each_stay_whole_and_in_order() {
        let scratch = Scratch::new("together");
        let (wal, applied) = started(&scratch, Options::default());
        let wal = Arc::new(wal);
        let senders: Vec<_> = (0..8u64)
            .map(|sender| {
                let wal = Arc::clone(&wal);
                std::thread::spawn(move || {
                    for b in 0..25u64 {
                        let from = (sender * 25 + b) * 4;
                        assert_eq!(wal.commit(body(from, from + 4), sender as i64), Ok(()));
                    }
                })
            })
            .collect();
        for sender in senders {
            sender.join().expect("a sender");
        }
        assert_eq!(*lock(&applied), (1..=800).collect::<Vec<u64>>());
        wal.close();

        let (records, _, _) = read_back(&scratch).expect("read");
        assert_eq!(records.len(), 800);
        for body in records.chunks(4) {
            let first = n_of(&body[0].4);
            assert_eq!(first % 4, 0, "a body was split: {body:?}");
            for (at, record) in body.iter().enumerate() {
                assert_eq!(n_of(&record.4), first + at as u64, "{body:?}");
                assert_eq!(record.2, (first / 100) as i64, "{body:?}");
            }
        }
    }

    /// A segment that ends in a torn record is some run's last, and the runs after it
    /// wrote segments of their own: they are read. And the positions those runs gave out
    /// begin past anything the torn bytes could have held - if they were once whole
    /// frames, rows were stored from them, and their positions are some row group's.
    #[test]
    fn the_segments_after_a_torn_one_are_read_and_their_positions_are_past_it() {
        let scratch = Scratch::new("torn-then-more");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
        wal.close();
        let torn = scratch.segments().pop().expect("a segment");
        OpenOptions::new()
            .append(true)
            .open(&torn)
            .and_then(|mut f| f.write_all(&[9u8; 100]))
            .expect("append");

        let (wal, applied) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(3, 5), 0), Ok(()));
        let after = lock(&applied).clone();
        assert_eq!(after.len(), 2);
        assert!(
            after[0] > 3 + 100 * POSITIONS_PER_LOST_BYTE,
            "position {} could have been one the torn bytes held",
            after[0]
        );
        wal.close();

        let (records, scanned, _) = read_back(&scratch).expect("read");
        assert_eq!(
            records.iter().map(|r| n_of(&r.4)).collect::<Vec<_>>(),
            [0, 1, 2, 3, 4]
        );
        assert_eq!(scanned.torn, 1);
        assert_eq!(records[3].0, after[0]);
    }

    /// A record that fails its own checksum inside a frame whose checksum holds is not a
    /// tear - the writer wrote those bytes - and is not stepped over: the start stops.
    #[test]
    fn a_record_that_fails_its_own_checksum_in_a_whole_frame_stops_the_start() {
        let scratch = Scratch::new("record-crc");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        wal.close();

        let mut raw = Vec::new();
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&0i64.to_le_bytes());
        put_record(&mut raw, 2, 101, 0, b"{\"n\":1}");
        raw[12] ^= 0xff;
        let mut payload = vec![KIND_BATCH];
        payload.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        payload.extend_from_slice(&zstd::bulk::compress(&raw, ZSTD_LEVEL).expect("compress"));
        OpenOptions::new()
            .append(true)
            .open(scratch.segments().pop().expect("a segment"))
            .and_then(|mut f| f.write_all(&frame::frame(&payload)))
            .expect("append");
        match read_back(&scratch) {
            Err(WalError::Corrupt { what, .. }) => {
                assert!(what.contains("own checksum"), "{what}")
            }
            other => panic!(
                "read past a record that does not check: {:?}",
                other.map(|r| r.1)
            ),
        }
    }

    /// A body that is more, as records, than one frame may hold is refused before it is
    /// queued: written, it would be acknowledged and then stop every start.
    #[test]
    fn a_body_past_what_a_frame_holds_is_refused_and_not_written() {
        let scratch = Scratch::new("too-large");
        let (wal, applied) = started(&scratch, Options::default());
        // Zeros: asked for and never touched, so the test holds a gigabyte of nothing.
        let big = Line {
            raw: vec![0u8; MAX_BODY_RAW],
            time: 0,
            placed: line(0).placed,
        };
        assert!(matches!(
            wal.commit(vec![big], 0),
            Err(Refused::TooLarge { .. })
        ));
        assert!(wal.is_healthy());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        assert_eq!(*lock(&applied), [1]);
    }

    /// A start that skipped positions past a torn record still lets the checkpoint cross
    /// them: with nothing left to replay, the torn segment goes at the first tick, and a
    /// server started and stopped with nothing sent does not leave a segment each time.
    #[test]
    fn an_idle_start_after_a_torn_record_leaves_no_segment_behind() {
        let scratch = Scratch::new("torn-idle");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
        wal.close();
        OpenOptions::new()
            .append(true)
            .open(scratch.segments().pop().expect("a segment"))
            .and_then(|mut f| f.write_all(&[9u8; 100]))
            .expect("append");

        for _ in 0..3 {
            let (wal, _) = started(&scratch, Options::default());
            let moved = wal.advance(|| None).0.expect("moved");
            assert!(moved.deleted >= 1, "{moved:?}");
            assert_eq!(scratch.segments().len(), 1);
            wal.close();
        }
        // And the positions go on from past the gap, not from inside it.
        let (wal, applied) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(3, 4), 0), Ok(()));
        assert!(lock(&applied)[0] > 3 + 100 * POSITIONS_PER_LOST_BYTE);
    }

    /// The look at a body before it is parsed refuses only what `commit` would: blank
    /// lines are no records, however many, and a body under the bound is not refused.
    #[test]
    fn the_early_look_counts_lines_and_not_bytes() {
        assert_eq!(too_large(b"{\"n\":1}\n{\"n\":2}\n"), None);
        // Sixty megabytes of blank lines: walked, and nothing in it is a record.
        assert_eq!(too_large(&vec![b'\n'; 60_000_000]), None);
        // Twenty-six million lines of two bytes: 78MB of body, and past a gigabyte of
        // records.
        let many = b"{}\n".repeat(26_000_000);
        assert_eq!(
            too_large(&many),
            Some(Refused::TooLarge {
                bytes: 26_000_000 * (RECORD_HEADER + 8 + 2),
                limit: MAX_BODY_RAW
            })
        );
    }

    /// The checkpoint's file is a hint, and one that cannot be written keeps no segment:
    /// on a full volume the segments behind the checkpoint are the room it needs. The
    /// checkpoint moves, the segments go, and the failure is handed back beside them.
    #[test]
    fn a_checkpoint_file_that_cannot_be_written_does_not_keep_a_segment() {
        let scratch = Scratch::new("no-checkpoint-file");
        let options = Options {
            segment_bytes: 1,
            ..Options::default()
        };
        let (wal, _) = started(&scratch, options);
        for at in 0..3 {
            assert_eq!(wal.commit(body(at, at + 1), 0), Ok(()));
        }
        assert_eq!(scratch.segments().len(), 3);
        // Where the file is written before it is renamed: a directory cannot be opened
        // for writing, and nothing else in `wal/` is touched.
        fs::create_dir(scratch.0.join(DIR).join(format!("{CHECKPOINT_FILE}.tmp"))).expect("mkdir");

        let (moved, problem) = wal.advance(|| None);
        assert_eq!(
            moved,
            Some(Advanced {
                checkpoint: 3,
                deleted: 2
            })
        );
        assert!(matches!(problem, Some(WalError::Io { .. })), "{problem:?}");
        assert_eq!(wal.checkpoint(), 3);
        assert_eq!(scratch.segments().len(), 1);
        assert_eq!(read_checkpoint(&scratch.0.join(DIR)), 0);

        // And the file is tried again at the next look, though nothing has moved since.
        fs::remove_dir(scratch.0.join(DIR).join(format!("{CHECKPOINT_FILE}.tmp"))).expect("rmdir");
        let (moved, problem) = wal.advance(|| None);
        assert!(
            moved.is_none() && problem.is_none(),
            "{moved:?} {problem:?}"
        );
        assert_eq!(read_checkpoint(&scratch.0.join(DIR)), 3);
        wal.close();

        let (records, scanned, _) = read_back(&scratch).expect("read");
        assert!(records.is_empty());
        assert_eq!(scanned.behind, 1);
    }

    /// With the checkpoint's file gone and nothing left in the log to read, the newest
    /// segment's base is all that says where the positions had got to: the next one is
    /// taken from it, and not from 1 - which some row group already holds a line at.
    #[test]
    fn positions_go_on_from_the_newest_segments_base_when_nothing_else_is_left() {
        let scratch = Scratch::new("base-only");
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
        wal.close();
        // A start with nothing sent: its segment stays empty, and its first tick deletes
        // the one before it.
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.advance(|| None).0.expect("moved").deleted, 1);
        wal.close();
        fs::remove_file(scratch.0.join(DIR).join(CHECKPOINT_FILE)).expect("remove");

        let (wal, applied) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(3, 4), 0), Ok(()));
        assert_eq!(*lock(&applied), [4]);
    }
}
