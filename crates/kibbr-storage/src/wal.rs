//! The write-ahead log: where a line is durable before `/ingest` says it was taken.
//!
//! It holds the segment, frame, batch and record layouts, replay, the one writer and
//! the bound on what waits. One applier hands rows to the store in log order, which is
//! what a row group's range of positions rests on.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::db::Placed;
use crate::frame;

/// The log's directory inside the data directory, beside `data/` and never under it: a
/// start deletes every file there that the manifest does not name.
pub const DIR: &str = "wal";
const CHECKPOINT_FILE: &str = "checkpoint";

const MAGIC: [u8; 4] = *b"KWAL";
const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 28;
pub const KIND_BATCH: u8 = 1;
/// `crc`, `seq`, `template_id`, `received_at`, `len`.
const RECORD_HEADER: usize = 32;

/// A segment rolls to the next number once it is this long.
pub const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// Where a flush stops taking more bodies, once it holds this many lines: an upper bound
/// and not a wait, and never a bound on a body, which is not split.
pub const DEFAULT_BATCH_LINES: usize = 500;

/// And once it holds this many bytes of lines: a count of lines says nothing of their size.
const BATCH_BYTES: usize = 8 * 1024 * 1024;

/// The most one body may come to as records, which keeps a frame's lengths inside a `u32`;
/// a body past it is refused before it is queued (`Refused::TooLarge`).
const MAX_BODY_RAW: usize = 1024 * 1024 * 1024;

/// The most a batch may claim to decompress to: one body and the bodies a flush took before
/// it, held on writing as on reading so a frame this build wrote is one it reads.
pub const MAX_RAW: usize = MAX_BODY_RAW + 2 * BATCH_BYTES;

// A frame says how long its batch is in four bytes.
const _: () = assert!(MAX_RAW <= u32::MAX as usize);

/// The least a log that could not write writes, and takes back, to find out whether it can
/// again: enough to need a block the volume may not have.
const PROBE_BYTES: usize = 64 * 1024;

/// zstd 3: no CPU cost that shows beside the flush.
const ZSTD_LEVEL: i32 = 3;

/// The most bytes of lines that may be between a body's sender and the store by default,
/// which bounds the memory a stalled disk or a slow store can take.
pub const DEFAULT_QUEUE_BYTES: usize = 256 * 1024 * 1024;

/// The most positions one unreadable byte of a segment is taken to have held: past anything
/// a frame can hold, so a start skips enough before it gives out a position.
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
    /// A frame whose checksum holds and whose contents this build cannot read: not a tear, so
    /// nothing is guessed and the start stops.
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
    /// A segment whose header will not read and which is longer than a header, so it may hold
    /// lines.
    #[error(
        "{path}: {why}, and the file is longer than a header, so it may hold lines this \
         build cannot read. Nothing was replayed: start the build that wrote the log, or \
         move the whole wal/ directory aside - the next start then begins a new log, and \
         the lines only the old one holds are not stored"
    )]
    Header { path: PathBuf, why: String },
    /// Two streams in one directory: a record of another stream would be held against row
    /// group ranges that were never about it.
    #[error(
        "{a} and {b} carry different stream ids: segments of two write-ahead logs are in one \
         directory, and this build does not guess which one the stored row groups came from"
    )]
    Streams { a: PathBuf, b: PathBuf },
    /// What the replay's caller could not do with a record, carried as text so the log does
    /// not depend on the store.
    #[error("replaying position {seq}: {detail}")]
    Replay { seq: u64, detail: String },
}

fn io(path: &Path) -> impl Fn(std::io::Error) -> WalError + '_ {
    move |source| WalError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// A line of the process's log, at `info`. Not `eprintln!`, which panics when stderr is
/// gone: a panic here would end the thread every `/ingest` waits on.
fn say(message: &str) {
    tracing::info!("{message}");
}

/// The log cannot write: an `error` event, `ERROR` before the sentence as text.
fn fail(message: &str) {
    tracing::error!("{message}");
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

pub fn header_bytes(stream: u64, base: u64) -> [u8; HEADER_LEN] {
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
pub fn read_header(bytes: &[u8]) -> Result<(u64, u64), String> {
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

pub fn put_record(out: &mut Vec<u8>, seq: u64, template_id: u64, received_at: i64, line: &[u8]) {
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

/// Why the log did not take a body, nothing of which is in the log - but for the one case
/// no reader can settle, a writer that died with the body in its hands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// The log cannot write: the error of the flush that failed.
    Cannot(String),
    /// More is waiting on the log than it may hold, and no room came in the time the body was
    /// given to wait: a shed, as the door's are, and not a verdict on the disk.
    Busy,
    /// The body is more, as records, than one frame holds: the sender's to fix, since no retry
    /// of the same body will fit.
    TooLarge { bytes: usize, limit: usize },
    /// The body's sender had gone, and the log had no room for the body: it was let go
    /// rather than kept waiting for room that nobody would be answered from.
    Left,
}

/// How long a body may wait for room in the log, and what ends the wait early.
#[derive(Debug, Clone, Copy)]
pub struct Wait<'a> {
    /// From the moment the log is found full; zero is no wait.
    pub at_most: Duration,
    /// Set, with no lock, once the body's sender has gone; a waiting body sees it the next
    /// time it looks, and `Wal::wake_room` has it look at once.
    pub left: &'a AtomicBool,
}

/// How the log is written.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub segment_bytes: u64,
    pub batch_lines: usize,
    /// The most bytes of lines that may be in the log and not yet in the store: see
    /// `DEFAULT_QUEUE_BYTES`.
    pub queue_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            segment_bytes: DEFAULT_SEGMENT_BYTES,
            batch_lines: DEFAULT_BATCH_LINES,
            queue_bytes: DEFAULT_QUEUE_BYTES,
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
    /// The bytes of the lines waiting for the writer.
    pub queue_bytes: usize,
    /// The bytes of the lines the writer has taken and the store does not have yet.
    pub unapplied_bytes: usize,
    /// The most the two have come to together.
    pub queue_peak_bytes: usize,
    /// Bodies waiting for room now: each parsed, and holding its permit and its place.
    pub queue_waiting: usize,
    /// Bodies that have had to wait for room, whatever came of the wait.
    pub queue_waits: u64,
    /// Bodies shed because no room came while they were allowed to wait for it.
    pub queue_sheds: u64,
    /// Bodies let go because their sender left while they waited for room.
    pub queue_left: u64,
}

/// What has become of one body. A sender waits for `Applied`, not `Logged`, so a client
/// that ingests and then queries sees what it sent.
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
                // Taken off the queue by a writer that then died: nobody is left to say whether
                // its flush landed. A writer still running is waited for, lest a retry store twice.
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

/// A body in the log's queue: what its sender waits on.
pub struct Ticket<'w> {
    /// `None` for a body with no line in it, which has nothing to wait for.
    outcome: Option<Arc<Outcome>>,
    shared: &'w Shared,
}

impl Ticket<'_> {
    /// Wait until the body's lines are durable and applied, or refused; it blocks for a flush,
    /// so it is called from a blocking thread.
    pub fn wait(self) -> Result<(), Refused> {
        match &self.outcome {
            Some(outcome) => outcome.wait(self.shared),
            None => Ok(()),
        }
    }
}

/// Whether a body is past what one frame holds, told from its bytes before it is parsed:
/// an upper bound, so a body it refuses costs no parse.
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

/// What a body counts for against the log's bound, every line that is not blank as a
/// record: for a test that sets the bound in bodies.
#[cfg(any(test, feature = "test-support"))]
pub fn counted(body: &[u8]) -> usize {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .map(|line| RECORD_HEADER + 8 + line.len())
        .sum()
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
    /// The most bytes of lines that may be queued and unapplied together: what `start`
    /// was given.
    limit: usize,
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
    /// Where a body waits for room. Notified with the queue's lock held, and only while
    /// `room_waiters` says somebody is there.
    room: Condvar,
    /// The bytes of the lines waiting for the writer. Written only with the queue's lock
    /// held, and an atomic so that `/stats` reads it without queueing behind the writer.
    queued: AtomicUsize,
    /// The bytes of lines the writer has taken and the applier has not yet handed to the
    /// store. Raised under the queue's lock; lowered with none, which can only make room.
    unapplied: AtomicUsize,
    /// The most `queued` and `unapplied` have come to together. Kept under the queue's
    /// lock, where the two are read as one.
    peak: AtomicUsize,
    /// Bodies waiting on `room` now. Written only with the queue's lock held, and an
    /// atomic so that the door reads it with no lock, to word a refusal.
    room_waiters: AtomicUsize,
    /// Nobody waits for room from here on: the server is stopping.
    hurry: AtomicBool,
    /// Bodies that have had to wait for room.
    room_waits: AtomicU64,
    /// Bodies shed for want of room.
    room_sheds: AtomicU64,
    /// Bodies let go because their sender left while they waited.
    room_left: AtomicU64,
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
    /// Whether the last flush could write, and why not, as one value read in one step.
    /// Its lock is taken alone or inside the queue's, never the other way round.
    health: RwLock<Health>,
    /// No thread is taking lines: before `start`, after `close`, or because one died.
    stopped: AtomicBool,
    /// The writer has ended, and whatever it had taken off the queue it will not finish.
    writer_gone: AtomicBool,
    /// The checkpoint has reached the last position written and the segment being written
    /// holds records: the writer opens the next (`Log::ensure`), so this one can be deleted.
    drained: AtomicBool,
    flushes: AtomicU64,
}

/// Whether the log can write. Either it can, or it cannot and here is why: there is no
/// value for "it cannot" that has no reason in it.
#[derive(Debug)]
enum Health {
    /// The last flush could write. `last_error` is what the last one that could not
    /// failed with: kept, so `/stats` still says it once the disk is back.
    Writing { last_error: Option<String> },
    /// It could not, and every refusal, `/health` and `/stats` say `why`.
    Cannot { why: String },
    /// A thread of the log has died, and `why` says to restart the server. Nothing a flush
    /// finds changes that.
    Died { why: String },
}

impl Health {
    /// Whether the log can write, and what it last could not write for - now, or before
    /// it wrote again.
    fn report(&self) -> (bool, Option<String>) {
        match self {
            Health::Writing { last_error } => (true, last_error.clone()),
            Health::Cannot { why } | Health::Died { why } => (false, Some(why.clone())),
        }
    }
}

impl Shared {
    /// The health as it is now. Recovering from a poisoning as `lock` does: it is one
    /// value, replaced whole.
    fn health(&self) -> RwLockReadGuard<'_, Health> {
        self.health.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Why the log cannot write, and `None` while it can.
    fn refusal(&self) -> Option<String> {
        match &*self.health() {
            Health::Writing { .. } => None,
            Health::Cannot { why } | Health::Died { why } => Some(why.clone()),
        }
    }

    /// A flush could not write, for this reason. Returns whether the log could until now.
    fn cannot(&self, why: &str) -> bool {
        let mut health = self.health.write().unwrap_or_else(|e| e.into_inner());
        let could = match *health {
            Health::Writing { .. } => true,
            Health::Cannot { .. } => false,
            Health::Died { .. } => return false,
        };
        *health = Health::Cannot {
            why: why.to_string(),
        };
        could
    }

    /// A flush could write. Returns whether the log could not until now.
    fn writing(&self) -> bool {
        let mut health = self.health.write().unwrap_or_else(|e| e.into_inner());
        let Health::Cannot { why } = &mut *health else {
            return false;
        };
        let last_error = Some(std::mem::take(why));
        *health = Health::Writing { last_error };
        true
    }

    /// Wake every body waiting for room, if any is. The caller holds the queue's lock, which
    /// a waiter holds from its look until it is parked: a wake-up under it is never lost.
    fn wake_room(&self) {
        if self.room_waiters.load(Ordering::Acquire) > 0 {
            self.room.notify_all();
        }
    }

    /// A thread of the log has died, and this is what every refusal says from here on.
    fn died(&self, why: &str) {
        *self.health.write().unwrap_or_else(|e| e.into_inner()) = Health::Died {
            why: why.to_string(),
        };
    }
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
        shared.died(&why);
        shared.queued.store(0, Ordering::Release);
        for waiting in queue.waiting.drain(..) {
            waiting
                .outcome
                .set(Progress::Refused(Refused::Cannot(why.clone())));
        }
        // And whoever is waiting for room, which will not come now: each looks again,
        // and is told what the queued bodies were.
        shared.wake_room();
    }
}

/// Take `by` off a count of bytes, and never below nothing: a count that had lost a
/// byte somewhere would otherwise wrap, and read as a log that is full for ever.
fn take_off(count: &AtomicUsize, by: usize) {
    let _ = count.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
        Some(held.saturating_sub(by))
    });
}

/// A flushed batch on its way to the applier.
struct Batch {
    first: u64,
    rows: Vec<Placed>,
    /// What these rows' lines count for against the bound, until the store has them.
    bytes: usize,
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
    /// The log in `base`, if there is one: its segments' headers and checkpoint, nothing read
    /// past them yet, and a segment cut short as it was made removed with a warning.
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

    /// A new log in `base`, with a new stream id and never the old one: its positions start
    /// again, and under the old id they would be held against the old row groups.
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

    /// Whether the last flush could write.
    pub fn is_healthy(&self) -> bool {
        matches!(*self.shared.health(), Health::Writing { .. })
    }

    /// Why the log cannot write, and `None` while it can: whether and why in one read, so
    /// there is never the one without the other.
    pub fn refusal(&self) -> Option<String> {
        self.shared.refusal()
    }

    /// What the last flush that could not write failed with: the reason the log is
    /// refusing for now, or the one kept from before it wrote again.
    pub fn last_error(&self) -> Option<String> {
        self.shared.health().report().1
    }

    /// Read every record past the checkpoint back, in order, and hand each to `each`, whose
    /// `Err` stops the replay; a torn end is skipped and nothing is cut.
    /// Every segment is flushed before it is read: after a kill its last frames may be cache only.
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
            read_segment(
                &seg.path, &bytes, checkpoint, &mut found, &mut lost, &mut *warn, &mut *each,
            )?;
        }
        // Above everything read, and above whatever the unreadable bytes could have held: if they
        // were once whole frames, a row group names their positions. A gap costs nothing.
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

    /// Start writing, once, after `replay` - a new segment, the writer and the applier; a
    /// segment that cannot be made leaves the log unhealthy for the probe to mend, and is no error.
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
        // Every position below the first this run gives out was applied or never given out.
        // Said here, so the checkpoint can cross such a gap and the segments before it go.
        let before = next_seq.saturating_sub(1);
        self.shared.written.fetch_max(before, Ordering::AcqRel);
        self.shared.applied.fetch_max(before, Ordering::AcqRel);
        // Before a line is taken: the stream's id is on disk before any row carries a
        // position of it.
        if let Err(e) = log.ensure(next_seq) {
            let why = e.to_string();
            self.shared.cannot(&why);
            fail(&format!(
                "write-ahead log - {why}; /ingest answers 503 until it can write"
            ));
        }
        lock(&self.shared.queue).limit = options.queue_bytes.max(1);
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

    /// Put a body's lines in the queue, all for one flush, waiting up to `wait` for room, and
    /// return what its sender waits on; `Err` means none of its lines is in the log.
    /// `received_at` is the body's arrival time; it blocks, so it is called from a blocking thread.
    pub fn enqueue(
        &self,
        lines: Vec<Line>,
        received_at: i64,
        wait: Wait<'_>,
    ) -> Result<Ticket<'_>, Refused> {
        let shared: &Shared = &self.shared;
        if lines.is_empty() {
            return Ok(Ticket {
                outcome: None,
                shared,
            });
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
            // The health is read under the queue's lock, which a failing flush holds while it
            // empties the queue: a body is refused here or by that flush, never left behind it.
            let mut queue = lock(&shared.queue);
            // When this body's wait for room ends: set the first time it finds none.
            let mut until: Option<Instant> = None;
            let pending = loop {
                if let Some(why) = shared.refusal() {
                    return Err(Refused::Cannot(why));
                }
                if queue.closing || shared.stopped.load(Ordering::Acquire) {
                    return Err(Refused::Cannot("the write-ahead log is closed".to_string()));
                }
                // A body always fits an empty log: the bound is on what is between the
                // senders and the store, and a body larger than it is still a body.
                let pending = shared.queued.load(Ordering::Relaxed)
                    + shared.unapplied.load(Ordering::Acquire);
                if pending == 0 || pending.saturating_add(bytes) <= queue.limit {
                    break pending;
                }
                if wait.left.load(Ordering::Acquire) {
                    shared.room_left.fetch_add(1, Ordering::Relaxed);
                    return Err(Refused::Left);
                }
                let left = match until {
                    _ if shared.hurry.load(Ordering::Acquire) => None,
                    Some(until) => until
                        .checked_duration_since(Instant::now())
                        .filter(|left| !left.is_zero()),
                    None if wait.at_most.is_zero() => None,
                    None => {
                        until = Some(Instant::now() + wait.at_most);
                        shared.room_waits.fetch_add(1, Ordering::Relaxed);
                        Some(wait.at_most)
                    }
                };
                let Some(left) = left else {
                    shared.room_sheds.fetch_add(1, Ordering::Relaxed);
                    return Err(Refused::Busy);
                };
                shared.room_waiters.fetch_add(1, Ordering::AcqRel);
                queue = shared
                    .room
                    .wait_timeout(queue, left)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
                shared.room_waiters.fetch_sub(1, Ordering::AcqRel);
            };
            let queued = shared.queued.load(Ordering::Relaxed);
            shared.queued.store(queued + bytes, Ordering::Release);
            shared.peak.fetch_max(pending + bytes, Ordering::Relaxed);
            queue.waiting.push_back(Submission {
                lines,
                bytes,
                received_at,
                outcome: Arc::clone(&outcome),
            });
        }
        shared.wake.notify_one();
        Ok(Ticket {
            outcome: Some(outcome),
            shared,
        })
    }

    /// `enqueue` with no wait for room, and then the wait for the answer: the tests' way in,
    /// where the server calls the two apart to give its permit back between them.
    #[cfg(test)]
    pub fn commit(&self, lines: Vec<Line>, received_at: i64) -> Result<(), Refused> {
        // A sender that never leaves.
        static STAYS: AtomicBool = AtomicBool::new(false);
        let wait = Wait {
            at_most: Duration::ZERO,
            left: &STAYS,
        };
        self.enqueue(lines, received_at, wait)?.wait()
    }

    /// Have whoever is waiting for room look again, because its sender has left; it takes the
    /// queue's lock, so it is called from a thread that may block.
    pub fn wake_room(&self) {
        let _queue = lock(&self.shared.queue);
        self.shared.wake_room();
    }

    /// The server is stopping, so nobody waits for room from here on - a stop must not take as
    /// long as the longest wait; it takes the queue's lock, as `wake_room` does.
    pub fn hurry(&self) {
        self.shared.hurry.store(true, Ordering::Release);
        self.wake_room();
    }

    /// Bodies waiting for room now, read with no lock: the door asks it on a runtime worker,
    /// to say the right thing to a body it has kept waiting for nothing.
    pub fn waiting_for_room(&self) -> usize {
        self.shared.room_waiters.load(Ordering::Acquire)
    }

    /// What is queued and what is written and not yet stored, read as one under the
    /// queue's lock: what the bound holds.
    #[cfg(test)]
    fn pending(&self) -> usize {
        let _queue = lock(&self.shared.queue);
        self.shared.queued.load(Ordering::Relaxed) + self.shared.unapplied.load(Ordering::Acquire)
    }

    /// Ask the writer, while the log cannot write, to try again (`Log::probe`), so the door
    /// opens within a tick of the disk coming back; not once a thread of the log has died.
    pub fn probe(&self) {
        if self.is_healthy() || self.shared.stopped.load(Ordering::Acquire) {
            return;
        }
        lock(&self.shared.queue).probe = true;
        self.shared.wake.notify_one();
    }

    /// Move the checkpoint to just before the oldest position still only in memory and delete the
    /// segments behind it; `oldest` is asked after `applied` is read, so no row is missed.
    /// The file is a hint: a failed write keeps no segment; they are looked for even with no move.
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
            // Every record of a segment is below the next one's base. Never the newest: it says
            // which stream this is and where it had got to.
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
                // Left on the disk and off the list: the next start finds every line of it behind
                // the checkpoint or in a row group, and deletes it at its first tick.
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
        // The two from one read of the health: never unhealthy beside no reason, or beside
        // the reason of a failure before this one.
        let (healthy, last_error) = self.shared.health().report();
        // `unapplied` before `queued`: the writer moves a body out of `queued` and then into
        // `unapplied`, so in this order the pair can miss a body and never hold it twice.
        let unapplied_bytes = self.shared.unapplied.load(Ordering::Acquire);
        let queue_bytes = self.shared.queued.load(Ordering::Acquire);
        Stats {
            stream: self.stream,
            segments: segments.len(),
            first_segment: segments.first().map_or(0, |s| s.number),
            bytes: segments.iter().map(|s| s.len).sum(),
            last_seq: self.shared.written.load(Ordering::Acquire),
            applied: self.applied(),
            checkpoint: self.checkpoint(),
            flushes: self.shared.flushes.load(Ordering::Relaxed),
            healthy,
            last_error,
            queue_bytes,
            unapplied_bytes,
            queue_peak_bytes: self.shared.peak.load(Ordering::Relaxed),
            queue_waiting: self.shared.room_waiters.load(Ordering::Acquire),
            queue_waits: self.shared.room_waits.load(Ordering::Relaxed),
            queue_sheds: self.shared.room_sheds.load(Ordering::Relaxed),
            queue_left: self.shared.room_left.load(Ordering::Relaxed),
        }
    }

    /// Stop the writer and the applier, once what is queued is written and applied.
    pub fn close(&self) {
        let state = std::mem::replace(&mut *lock(&self.state), State::Closed);
        if let State::Running { writer, applier } = state {
            {
                let mut queue = lock(&self.shared.queue);
                queue.closing = true;
                // Nobody waits for room in a log that is closing: each looks, and is
                // told so.
                self.shared.wake_room();
            }
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
            // The default until `start` says: never the derive's zero, under which every
            // body after the first would be one too many.
            queue: Mutex::new(Queue {
                limit: DEFAULT_QUEUE_BYTES,
                ..Queue::default()
            }),
            wake: Condvar::new(),
            room: Condvar::new(),
            queued: AtomicUsize::new(0),
            unapplied: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            room_waiters: AtomicUsize::new(0),
            hurry: AtomicBool::new(false),
            room_waits: AtomicU64::new(0),
            room_sheds: AtomicU64::new(0),
            room_left: AtomicU64::new(0),
            segments: Mutex::new(segments),
            written: AtomicU64::new(checkpoint),
            applied: AtomicU64::new(checkpoint),
            checkpoint: AtomicU64::new(checkpoint),
            filed: AtomicU64::new(checkpoint),
            health: RwLock::new(Health::Writing { last_error: None }),
            // No writer yet: `start` clears it.
            stopped: AtomicBool::new(true),
            writer_gone: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            flushes: AtomicU64::new(0),
        }
    }
}

/// Missing or unparseable is 0: the checkpoint is a hint. Digits only, which is
/// all `advance` writes: `parse` alone takes a sign, and `+5` is no checkpoint.
fn read_checkpoint(dir: &Path) -> u64 {
    fs::read_to_string(dir.join(CHECKPOINT_FILE))
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|digits| digits.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|digits| digits.parse().ok())
        .unwrap_or(0)
}

/// One segment's frames, read as `Wal::replay` reads them, returning the offset the reading
/// stopped at; it takes the file's bytes and no file, so the fuzz target can call it.
pub fn read_segment(
    path: &Path,
    bytes: &[u8],
    checkpoint: u64,
    found: &mut Scanned,
    lost: &mut u64,
    warn: &mut dyn FnMut(String),
    each: &mut dyn FnMut(Record<'_>) -> Result<(), String>,
) -> Result<usize, WalError> {
    let mut at = HEADER_LEN;
    loop {
        // Checked against the bytes that remain before anything is sliced: a length of
        // 0xFFFFFFFF is a tear, never a 4GB read.
        let payload = match frame::read(bytes, at, u32::MAX) {
            frame::Next::End => return Ok(at),
            frame::Next::Frame { payload, end } => Some((payload, end)),
            frame::Next::Tear => None,
        };
        let Some((payload, end)) = payload else {
            // Not a frame. A whole one further on makes this damage in the
            // middle of the segment; none makes it the segment's torn end.
            match resync(bytes, at + 1, found.last_seq) {
                Some(next) => {
                    *lost += (next - at) as u64;
                    found.corrupt += 1;
                    warn(format!(
                        "{}: corrupt WAL entry at offset {at}: {} byte(s) that will \
                         not read, with whole entries after them; skipping, and the \
                         lines in them are lost",
                        path.display(),
                        next - at
                    ));
                    at = next;
                    continue;
                }
                None => {
                    *lost += (bytes.len() - at) as u64;
                    found.torn += 1;
                    warn(format!(
                        "{}: torn record at offset {at} of {}: a write a stop cut \
                         short, whose lines were never acknowledged; skipped",
                        path.display(),
                        bytes.len(),
                    ));
                    return Ok(at);
                }
            }
        };
        let corrupt = |what: String| WalError::Corrupt {
            path: path.to_path_buf(),
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

/// A frame's payload as the batch it holds.
pub fn unpack(payload: &[u8]) -> Result<Vec<u8>, String> {
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

/// A batch's records, every one checked before the first is handed over; one that fails is
/// a writer's fault and not a tear, so it is not skipped.
pub fn records(raw: &[u8]) -> Result<Vec<Record<'_>>, String> {
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
fn resync(bytes: &[u8], from: usize, after: u64) -> Option<usize> {
    // zstd's magic is looked for before any checksum is taken: without it compressed bytes
    // offer a length that fits often, and each would cost a checksum over megabytes.
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
    /// A segment to write to: the open one, or the next number, whose header says no record
    /// is below `base`. Header and directory are flushed before a record follows.
    fn ensure(&mut self, base: u64) -> Result<(), WalError> {
        if let Some(open) = &self.segment {
            // Left early only once it is worth a new file: a sixty-fourth of a segment. A server
            // that takes a line a second would otherwise open a segment a line.
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

    /// One flush: the lines as one frame, written and flushed once. A flush that fails is cut
    /// back, so a later start does not store a frame whose sender was refused.
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
        // Held to what a reader will take: `enqueue` and the writer's own bound keep a
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

    /// Whether the log can write again: a block as long as the write that failed, flushed and
    /// cut back off. Zeros, which a reader takes for a tear if a stop comes between.
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
        // What this flush takes off the queue, kept for whichever of the applier and a
        // failed write gives it back.
        let mut bytes = 0usize;
        {
            let mut queue = lock(&shared.queue);
            while queue.waiting.is_empty() && !queue.probe && !queue.closing {
                queue = shared.wake.wait(queue).unwrap_or_else(|e| e.into_inner());
            }
            if queue.waiting.is_empty() && !queue.probe {
                return;
            }
            queue.probe = false;
            // A whole body at a time, and no more bodies once the bound is reached: so a flush
            // is under `BATCH_BYTES` and one body, which is under `MAX_RAW`.
            while lines.len() < log.batch_lines && bytes < BATCH_BYTES {
                let Some(body) = queue.waiting.pop_front() else {
                    break;
                };
                // Out of one count, then into the other: a reader with no lock that reads
                // `unapplied` first (`Wal::stats`) can miss a body, and never count it twice.
                let queued = shared.queued.load(Ordering::Relaxed);
                shared
                    .queued
                    .store(queued.saturating_sub(body.bytes), Ordering::Release);
                shared.unapplied.fetch_add(body.bytes, Ordering::AcqRel);
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
                    bytes,
                    taken: std::mem::take(&mut taken),
                });
            })
        };
        match result {
            Ok(()) => {
                // Not while the applier is gone: the log can write, and nothing it wrote would
                // reach the store.
                let again = {
                    let _queue = lock(&shared.queue);
                    !shared.stopped.load(Ordering::Acquire) && shared.writing()
                };
                // Said once the lock is let go: a stderr that will not take the line
                // keeps nobody from the queue.
                if again {
                    say("write-ahead log - writing again");
                }
            }
            Err(e) => {
                // `to_string`, not the chain: a `WalError` prints its cause itself.
                let why = e.to_string();
                let first = {
                    // Under the queue's lock, so no body joins the queue behind a flush
                    // that has failed: see `Wal::enqueue`.
                    let mut queue = lock(&shared.queue);
                    // The reason goes in with the verdict, before any body is refused. Unless a
                    // thread has died: what `Stopping` said then is kept.
                    let first = shared.cannot(&why);
                    let refused = Progress::Refused(Refused::Cannot(why.clone()));
                    for outcome in &taken {
                        outcome.set(refused.clone());
                    }
                    // This flush's lines go to no applier: they leave the count here.
                    take_off(&shared.unapplied, bytes);
                    shared.queued.store(0, Ordering::Release);
                    for waiting in queue.waiting.drain(..) {
                        waiting.outcome.set(refused.clone());
                    }
                    // And the bodies waiting for room are told as the queued ones were:
                    // each looks again, finds the log cannot write, and says why.
                    shared.wake_room();
                    first
                };
                // Said last, and once the lock is let go: a stderr that will not take the
                // line delays the line, and not a `503` or the reason in it.
                if first {
                    fail(&format!(
                        "write-ahead log - {why}; /ingest answers 503 until it can \
                         write again"
                    ));
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
        // Off the count before the answer: the answer wakes the sender at once, and one that
        // came straight back would be measured against lines the store already has.
        take_off(&shared.unapplied, batch.bytes);
        for outcome in batch.taken {
            outcome.set(Progress::Applied);
        }
        // Told after the answers, so no sender waits on this lock. Taken even with nobody
        // waiting: without it a body could look, be passed by the wake-up, and park beside room.
        let _queue = lock(&shared.queue);
        shared.wake_room();
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
            ..Options::default()
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

    /// What a write a stop cut short leaves at the end of a segment, in each shape: warned
    /// about and skipped, the start goes on, and nothing is cut.
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

    /// Damage in the middle of a segment costs the frame it is in and no other, whether in
    /// the frame's contents or its length: the frames after it are read, and nothing is cut.
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

    /// The checkpoint deletes the segments wholly behind it and never the newest, and a start
    /// resumes above every position it gave out, whether or not the checkpoint file survived.
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

    /// A checkpoint is the digits `advance` writes. A sign is not one of them, so `+5`
    /// is no checkpoint and reads as the 0 a missing file does.
    #[test]
    fn a_checkpoint_with_a_sign_is_not_a_checkpoint() {
        let scratch = Scratch::new("signed-checkpoint");
        let file = scratch.0.join(CHECKPOINT_FILE);
        assert_eq!(read_checkpoint(&scratch.0), 0);
        for (text, want) in [
            ("5", 5),
            ("5\n", 5),
            ("+5", 0),
            ("-0", 0),
            ("5 5", 0),
            ("", 0),
        ] {
            fs::write(&file, text).expect("write");
            assert_eq!(read_checkpoint(&scratch.0), want, "`{text}`");
        }
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

    /// A log made again in a directory that had one is given another id, which keeps its
    /// positions from being held against the old one's row groups.
    #[test]
    fn a_log_made_again_is_a_new_stream() {
        let scratch = Scratch::new("again");
        let first = Wal::create(&scratch.0).expect("create").stream();
        let second = Wal::create(&scratch.0).expect("create").stream();
        assert!(first != 0 && second != 0);
        assert_ne!(first, second);
    }

    /// A log that cannot write refuses with the reason, refuses the next body without trying,
    /// and writes again once asked to try - above the positions the failed flush had taken.
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

    /// Once the checkpoint has reached the end of the log, the segment being written is left
    /// at the next flush and deleted at the tick after, not kept until it fills.
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

    /// A log that was never started takes nothing, and says so: nothing would ever write the body.
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

    /// Readers that ask `wrong` of the log until `stop` is set, half in two reads and half as
    /// `/stats` does. Each returns how often the answer was yes, and how often it asked.
    #[cfg(unix)]
    fn readers_asking(
        wal: &Arc<Wal>,
        stop: &Arc<AtomicBool>,
        wrong: impl Fn(bool, Option<String>) -> bool + Send + Sync + 'static,
    ) -> Vec<JoinHandle<(u64, u64)>> {
        const READERS: usize = 4;
        let wrong = Arc::new(wrong);
        let ready = Arc::new(std::sync::Barrier::new(READERS + 1));
        let readers = (0..READERS)
            .map(|n| {
                let (wal, stop) = (Arc::clone(wal), Arc::clone(stop));
                let (wrong, ready) = (Arc::clone(&wrong), Arc::clone(&ready));
                std::thread::spawn(move || {
                    ready.wait();
                    let (mut times, mut looks) = (0u64, 0u64);
                    while !stop.load(Ordering::Relaxed) {
                        looks += 1;
                        times += u64::from(if n % 2 == 0 {
                            let healthy = wal.is_healthy();
                            wrong(healthy, wal.last_error())
                        } else {
                            let stats = wal.stats();
                            wrong(stats.healthy, stats.last_error)
                        });
                    }
                    (times, looks)
                })
            })
            .collect();
        ready.wait();
        // Every reader is asking before the log is made to fail.
        std::thread::sleep(Duration::from_millis(5));
        readers
    }

    /// The log's first failure, with readers asking "can it write?" and then "why not?" across
    /// it: none is ever told the log cannot write and given no reason.
    #[test]
    #[cfg(unix)]
    fn no_reader_sees_the_log_unhealthy_without_a_reason() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = Scratch::new("reason-race");
        let (wal, _) = started(&scratch, tiny());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        let dir = scratch.0.join(DIR);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

        let wal = Arc::new(wal);
        let stop = Arc::new(AtomicBool::new(false));
        let readers = readers_asking(&wal, &stop, |healthy, reason| !healthy && reason.is_none());
        // The next flush needs a new segment, and the directory will not take one.
        let refused = wal.commit(body(1, 3), 0);
        std::thread::sleep(Duration::from_millis(5));
        stop.store(true, Ordering::Relaxed);
        let seen: Vec<(u64, u64)> = readers
            .into_iter()
            .map(|r| r.join().expect("reader"))
            .collect();
        let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));

        assert!(matches!(refused, Err(Refused::Cannot(_))), "{refused:?}");
        assert!(!wal.is_healthy());
        let bare: u64 = seen.iter().map(|(bare, _)| bare).sum();
        assert_eq!(
            bare, 0,
            "the log was unhealthy with no reason given, (times, looks) per reader: {seen:?}"
        );
    }

    /// A second failure, once the log has written again: the first failure's reason is still
    /// there to be read while the log writes, and is never the reason given for the second.
    #[test]
    #[cfg(unix)]
    fn a_second_failure_is_never_told_with_the_reason_of_the_first() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = Scratch::new("second-reason");
        let (wal, _) = started(&scratch, tiny());
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        let dir = scratch.0.join(DIR);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");
        let Err(Refused::Cannot(first)) = wal.commit(body(1, 2), 0) else {
            panic!("the log wrote to a directory that takes no file");
        };
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
        wal.probe();
        for _ in 0..200 {
            if wal.is_healthy() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(wal.is_healthy());
        // Kept, now that the log writes again.
        assert_eq!(wal.last_error(), Some(first.clone()));
        // Into the segment the probe made: the flush after it needs the next one, whose
        // name is in its error, so the two reasons differ.
        assert_eq!(wal.commit(body(2, 3), 0), Ok(()));
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

        let wal = Arc::new(wal);
        let stop = Arc::new(AtomicBool::new(false));
        let earlier = first.clone();
        let readers = readers_asking(&wal, &stop, move |healthy, reason| {
            !healthy && reason.as_deref() == Some(earlier.as_str())
        });
        let refused = wal.commit(body(3, 4), 0);
        std::thread::sleep(Duration::from_millis(5));
        stop.store(true, Ordering::Relaxed);
        let seen: Vec<(u64, u64)> = readers
            .into_iter()
            .map(|r| r.join().expect("reader"))
            .collect();
        let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));

        let Err(Refused::Cannot(second)) = refused else {
            panic!("the log wrote to a directory that takes no file: {refused:?}");
        };
        assert_ne!(first, second);
        assert_eq!(wal.last_error(), Some(second));
        let stale: u64 = seen.iter().map(|(stale, _)| stale).sum();
        assert_eq!(
            stale, 0,
            "the second failure was given the first's reason, {first}; (times, looks) per \
             reader: {seen:?}"
        );
    }

    /// Once a thread of the log has died the reason is to restart the server, and a flush
    /// under way does not change it, by failing or by succeeding.
    #[test]
    fn a_flush_does_not_change_the_reason_once_a_thread_has_died() {
        let shared = Shared::new(Vec::new(), 0);
        assert_eq!(shared.refusal(), None);
        assert!(shared.cannot("no room"), "the first failure is the news");
        assert!(!shared.cannot("still no room"));
        assert_eq!(shared.refusal().as_deref(), Some("still no room"));

        shared.died("restart the server");
        assert!(!shared.cannot("no room"));
        assert!(!shared.writing());
        assert_eq!(shared.refusal().as_deref(), Some("restart the server"));
        assert_eq!(
            shared.health().report(),
            (false, Some("restart the server".to_string()))
        );
    }

    /// A thread of the log that dies leaves the log saying so: the body already in the log is
    /// acknowledged, and neither a probe nor a close takes the one reason away.
    #[test]
    fn a_thread_that_dies_leaves_the_log_unhealthy_and_saying_to_restart() {
        let scratch = Scratch::new("died");
        let wal = Wal::create(&scratch.0).expect("create");
        wal.start(tiny(), Box::new(|_| panic!("the applier dies")))
            .expect("start");
        // Written before the applier is handed it: durable, so acknowledged.
        assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
        for _ in 0..200 {
            if !wal.is_healthy() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let why = "a thread of the write-ahead log stopped; restart the server".to_string();
        let told = |wal: &Wal| (wal.is_healthy(), wal.refusal(), wal.stats().last_error);
        assert_eq!(told(&wal), (false, Some(why.clone()), Some(why.clone())));
        assert_eq!(wal.commit(body(1, 2), 0), Err(Refused::Cannot(why.clone())));
        // The writer is still there to be asked, and is not: a close waits for it.
        wal.probe();
        wal.close();
        assert_eq!(told(&wal), (false, Some(why.clone()), Some(why)));
    }

    /// The number in a test line, `{"n":N}`.
    fn n_of(line: &str) -> u64 {
        line.trim_start_matches("{\"n\":")
            .trim_end_matches('}')
            .parse()
            .expect("a test line")
    }

    /// Bodies from many senders at once: whatever the flushes took together, every body is
    /// whole and in its own order, and the applier is handed every position in the log's order.
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

    /// The segments after one that ends in a torn record are read, and their positions begin
    /// past anything the torn bytes could have held: a row group may name those.
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

    /// A start that skipped positions past a torn record still lets the checkpoint cross them,
    /// so a server started and stopped with nothing sent does not leave a segment each time.
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

    /// The checkpoint's file is a hint, and one that cannot be written keeps no segment: on a
    /// full volume the segments behind the checkpoint are the room it needs.
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

    /// With the checkpoint's file gone and nothing left to read, the newest segment's base is
    /// all that says where the positions had got to: the next is taken from it, not from 1.
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

    // ── The bound on what is between a sender and the store ───────────────────────────
    //
    // The slow hop in these is the applier, and the test holds it. Nothing here sleeps to
    // let a thing happen; a state is waited for, and the wait's limit fails the test.

    /// How many lines the bodies of these tests hold.
    const EVEN: u64 = 4;

    /// Body `nth` of a run of bodies that all come to the same number of bytes: its
    /// lines' numbers have seven digits each.
    fn even(nth: u64) -> Vec<Line> {
        let from = 1_000_000 + nth * EVEN;
        assert!(from + EVEN <= 10_000_000, "a line of another length");
        body(from, from + EVEN)
    }

    /// What a body counts for against the bound.
    fn bytes_of(lines: &[Line]) -> usize {
        lines
            .iter()
            .map(|line| RECORD_HEADER + 8 + line.raw.len())
            .sum()
    }

    /// The test's hold on the applier of a log started by `gated`.
    struct Gate {
        /// The rows in each batch the applier is handed, said before it stores one.
        handed: Receiver<usize>,
        /// A send lets the batch in hand through; dropped, every batch goes through.
        go: Option<Sender<()>>,
        /// The positions the applier has stored, in the order it did.
        applied: Arc<Mutex<Vec<u64>>>,
    }

    impl Gate {
        /// The rows of the next batch the applier is handed: it holds them, stored
        /// nowhere, until `pass` or `open`.
        fn handed(&self) -> usize {
            self.handed
                .recv_timeout(Duration::from_secs(30))
                .expect("the applier was handed no batch")
        }

        /// Let the batch in hand through, and no other.
        fn pass(&self) {
            let go = self.go.as_ref().expect("the gate is open for good");
            go.send(()).expect("the applier is gone");
        }

        /// Let every batch through from here on.
        fn open(&mut self) {
            self.go = None;
        }
    }

    /// A started log whose applier the test holds still. The gate is returned second, so it
    /// is dropped first: a close waiting on a held applier would hang a failing test.
    fn gated(scratch: &Scratch, options: Options) -> (Arc<Wal>, Gate) {
        let wal = Wal::create(&scratch.0).expect("create");
        let (said, handed) = channel::<usize>();
        let (go, told) = channel::<()>();
        let applied = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&applied);
        wal.start(
            options,
            Box::new(move |rows| {
                let _ = said.send(rows.len());
                // An error is the gate dropped: open for good.
                let _ = told.recv();
                lock(&seen).extend(rows.iter().map(|(seq, _)| *seq));
            }),
        )
        .expect("start");
        let gate = Gate {
            handed,
            go: Some(go),
            applied,
        };
        (Arc::new(wal), gate)
    }

    /// `commit` on a thread of its own: the body's sender, parked until its answer.
    fn sender(wal: &Arc<Wal>, lines: Vec<Line>) -> JoinHandle<Result<(), Refused>> {
        let wal = Arc::clone(wal);
        std::thread::spawn(move || wal.commit(lines, 0))
    }

    /// Wait for a state, looking every few milliseconds. The limit fails the test: it is
    /// far past what any machine needs, and nothing passes by reaching it.
    fn until(what: &str, mut reached: impl FnMut() -> bool) {
        let began = std::time::Instant::now();
        while !reached() {
            assert!(
                began.elapsed() < Duration::from_secs(30),
                "timed out waiting for {what}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// A line counts against the bound until the store has it, and no longer: its bytes leave
    /// the bound before its sender is woken, so one that comes straight back is not shed.
    #[test]
    fn the_bound_counts_a_line_until_it_is_applied() {
        let scratch = Scratch::new("bound-applied");
        let one = bytes_of(&even(0));
        let options = Options {
            queue_bytes: one,
            ..Options::default()
        };
        let (wal, mut gate) = gated(&scratch, options);

        let first = sender(&wal, even(0));
        assert_eq!(gate.handed(), EVEN as usize);
        // Written, and with the applier: out of the queue, and still in the count.
        let stats = wal.stats();
        assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, one));
        assert_eq!(stats.last_seq, EVEN);
        assert_eq!(wal.commit(even(1), 0), Err(Refused::Busy));
        assert_eq!(wal.stats().queue_sheds, 1);

        gate.open();
        assert_eq!(first.join().expect("the sender"), Ok(()));
        for nth in 2..1002 {
            assert_eq!(wal.commit(even(nth), 0), Ok(()), "body {nth}");
        }
        let stats = wal.stats();
        assert_eq!(stats.queue_sheds, 1);
        assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, 0));
        assert_eq!(stats.queue_peak_bytes, one);
        assert_eq!(lock(&gate.applied).len() as u64, 1001 * EVEN);
    }

    /// A body's bytes leave the bound before the body is answered, on every run: the test
    /// holds the first answer shut, so no luck with the processor is needed.
    #[test]
    fn a_body_is_out_of_the_bound_before_it_is_answered() {
        let scratch = Scratch::new("bound-before-answer");
        let options = Options {
            queue_bytes: bytes_of(&even(0)),
            ..Options::default()
        };
        let (wal, mut gate) = gated(&scratch, options);
        let stays = AtomicBool::new(false);
        let no_wait = || Wait {
            at_most: Duration::ZERO,
            left: &stays,
        };

        let first = wal
            .enqueue(even(0), 0, no_wait())
            .expect("an empty log has room");
        // Handed to the applier: the writer has said `Logged` and is done with the answer.
        assert_eq!(gate.handed(), EVEN as usize);
        let answer = Arc::clone(first.outcome.as_ref().expect("a body with lines"));
        let held = lock(&answer.progress);
        assert_eq!(*held, Progress::Logged);
        assert_eq!(wal.pending(), bytes_of(&even(0)));

        gate.open();
        until("the first body's bytes to leave the count", || {
            wal.pending() == 0
        });
        assert_eq!(
            *held,
            Progress::Logged,
            "answered before its bytes left the count"
        );
        assert_eq!(wal.stats().applied, EVEN);
        let second = wal
            .enqueue(even(1), 0, no_wait())
            .expect("room, with the first body stored and not yet answered");

        drop(held);
        assert_eq!(first.wait(), Ok(()));
        assert_eq!(second.wait(), Ok(()));
        let stats = wal.stats();
        assert_eq!((stats.queue_sheds, stats.queue_waits), (0, 0));
        assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, 0));
        assert_eq!(*lock(&gate.applied), (1..=2 * EVEN).collect::<Vec<u64>>());
    }

    /// How long a body a test expects to be woken may wait for room, and how long the test
    /// gives it to come back: far apart, so that the wake-up is the only way out.
    const LONG_WAIT: Duration = Duration::from_secs(60);
    const WOKEN_WITHIN: Duration = Duration::from_secs(10);

    /// `enqueue` and the wait for the answer, on a thread of its own: a sender that may
    /// wait `at_most` for room, and that has gone once `left` is set.
    fn waiter(
        wal: &Arc<Wal>,
        lines: Vec<Line>,
        at_most: Duration,
        left: &Arc<AtomicBool>,
    ) -> JoinHandle<Result<(), Refused>> {
        let (wal, left) = (Arc::clone(wal), Arc::clone(left));
        std::thread::spawn(move || {
            let wait = Wait {
                at_most,
                left: &left,
            };
            wal.enqueue(lines, 0, wait)?.wait()
        })
    }

    /// A flag nobody sets: a sender that stays.
    fn staying() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    /// A log whose bound is one body, with one body written and held at the applier: no
    /// other fits until the gate lets that one through.
    fn full(
        scratch: &Scratch,
        options: Options,
    ) -> (Arc<Wal>, Gate, JoinHandle<Result<(), Refused>>) {
        let options = Options {
            queue_bytes: bytes_of(&even(0)),
            ..options
        };
        let (wal, gate) = gated(scratch, options);
        let first = sender(&wal, even(0));
        assert_eq!(gate.handed(), EVEN as usize);
        (wal, gate, first)
    }

    /// The numbers of the lines a log holds, read back from its files.
    fn stored(scratch: &Scratch) -> Vec<u64> {
        let (records, _, warnings) = read_back(scratch).expect("read");
        assert_eq!(warnings, Vec::<String>::new());
        records.iter().map(|record| n_of(&record.4)).collect()
    }

    /// The numbers of the lines of these bodies, in order.
    fn lines_of(bodies: &[u64]) -> Vec<u64> {
        bodies
            .iter()
            .flat_map(|nth| even(*nth))
            .map(|line| n_of(&String::from_utf8_lossy(&line.raw)))
            .collect()
    }

    /// A body the log has no room for waits, with no position and no byte in the log, and is
    /// taken when the applier storing the body ahead of it wakes it.
    #[test]
    fn a_body_waits_for_room_and_is_taken_when_there_is_some() {
        let scratch = Scratch::new("room-wait");
        let (wal, mut gate, first) = full(&scratch, Options::default());
        let second = waiter(&wal, even(1), LONG_WAIT, &staying());
        until("the second body to wait for room", || {
            wal.stats().queue_waiting == 1
        });
        assert!(!second.is_finished());
        let stats = wal.stats();
        assert_eq!((stats.last_seq, stats.queue_bytes), (EVEN, 0));

        let began = std::time::Instant::now();
        gate.open();
        assert_eq!(first.join().expect("the first sender"), Ok(()));
        assert_eq!(second.join().expect("the second sender"), Ok(()));
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "the body waiting for room was not woken by the batch that made it"
        );
        assert_eq!(*lock(&gate.applied), (1..=2 * EVEN).collect::<Vec<u64>>());
        let stats = wal.stats();
        assert_eq!(
            (stats.queue_sheds, stats.queue_waits, stats.queue_waiting),
            (0, 1, 0)
        );
        wal.close();
        assert_eq!(stored(&scratch), lines_of(&[0, 1]));
    }

    /// What is between the senders and the store never passes the bound, however many senders
    /// there are, and none of them is shed for it.
    #[test]
    fn memory_stays_under_the_bound_while_senders_wait() {
        const SENDERS: u64 = 8;
        const BODIES: u64 = 5;
        let scratch = Scratch::new("room-bound");
        let bound = 2 * bytes_of(&even(0));
        let options = Options {
            queue_bytes: bound,
            ..Options::default()
        };
        let (wal, mut gate) = gated(&scratch, options);
        let senders: Vec<_> = (0..SENDERS)
            .map(|sender| {
                let wal = Arc::clone(&wal);
                std::thread::spawn(move || {
                    let left = AtomicBool::new(false);
                    (0..BODIES)
                        .map(|nth| {
                            let wait = Wait {
                                at_most: LONG_WAIT,
                                left: &left,
                            };
                            wal.enqueue(even(sender * BODIES + nth), 0, wait)?.wait()
                        })
                        .collect::<Vec<Result<(), Refused>>>()
                })
            })
            .collect();

        let mut seen = 0;
        while seen < SENDERS * BODIES * EVEN {
            seen += gate.handed() as u64;
            let pending = wal.pending();
            assert!(
                pending <= bound,
                "{pending} bytes against a bound of {bound}"
            );
            gate.pass();
        }
        gate.open();
        for sender in senders {
            let answers = sender.join().expect("a sender");
            assert_eq!(answers, vec![Ok(()); BODIES as usize]);
        }
        let stats = wal.stats();
        assert_eq!((stats.queue_sheds, stats.queue_left), (0, 0));
        assert!(stats.queue_peak_bytes <= bound, "{stats:?}");
        assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, 0));
        wal.close();

        // Every line once, whatever order the bodies went in.
        let mut found = stored(&scratch);
        found.sort_unstable();
        let every: Vec<u64> = (0..SENDERS * BODIES).collect();
        assert_eq!(found, lines_of(&every));
    }

    /// A wait that runs out is a shed. The gate is never opened while the two bodies ask, so
    /// no speed turns either into an `Ok`.
    #[test]
    fn a_wait_that_runs_out_is_a_shed() {
        let scratch = Scratch::new("room-shed");
        let (wal, mut gate, first) = full(&scratch, Options::default());

        assert_eq!(wal.commit(even(1), 0), Err(Refused::Busy));
        let left = AtomicBool::new(false);
        let wait = Wait {
            at_most: Duration::from_millis(20),
            left: &left,
        };
        assert_eq!(wal.enqueue(even(2), 0, wait).map(drop), Err(Refused::Busy));
        let stats = wal.stats();
        assert_eq!(
            (stats.queue_sheds, stats.queue_waits, stats.queue_waiting),
            (2, 1, 0)
        );
        assert_eq!(stats.last_seq, EVEN, "a body that was shed is in the log");

        gate.open();
        assert_eq!(first.join().expect("the first sender"), Ok(()));
        wal.close();
        assert_eq!(stored(&scratch), lines_of(&[0]));
    }

    /// A log that fails tells every body waiting for room why: the failed flush wakes the
    /// others, which are refused with the same reason and not left to wait out their minute.
    #[test]
    #[cfg(unix)]
    fn a_log_that_fails_tells_every_body_waiting_for_room_why() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = Scratch::new("room-failed");
        // A segment a frame long: the flush after the first needs a new file.
        let (wal, mut gate, first) = full(&scratch, tiny());
        let waiting = [
            waiter(&wal, even(1), LONG_WAIT, &staying()),
            waiter(&wal, even(2), LONG_WAIT, &staying()),
        ];
        until("both bodies to wait for room", || {
            wal.stats().queue_waiting == 2
        });
        let dir = scratch.0.join(DIR);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

        let began = std::time::Instant::now();
        gate.open();
        let first = first.join().expect("the first sender");
        let refused = waiting.map(|waiter| waiter.join().expect("a waiting sender"));
        let took = began.elapsed();
        let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));

        assert_eq!(first, Ok(()));
        let why = wal
            .stats()
            .last_error
            .expect("the log says why it cannot write");
        assert_eq!(refused, [(); 2].map(|()| Err(Refused::Cannot(why.clone()))));
        assert!(
            took < WOKEN_WITHIN,
            "a body waiting for room was not woken by the flush that failed"
        );
        assert!(!wal.is_healthy());
        assert_eq!(wal.stats().queue_waiting, 0);
    }

    /// A sender that leaves gives up its wait, and nothing of its body is in the log: the
    /// body after it takes the positions it never had.
    #[test]
    fn a_sender_that_leaves_gives_up_its_wait() {
        let scratch = Scratch::new("room-left");
        let (wal, mut gate, first) = full(&scratch, Options::default());
        let left = staying();
        let second = waiter(&wal, even(1), LONG_WAIT, &left);
        until("the second body to wait for room", || {
            wal.stats().queue_waiting == 1
        });

        let began = std::time::Instant::now();
        left.store(true, Ordering::Release);
        wal.wake_room();
        assert_eq!(
            second.join().expect("the second sender"),
            Err(Refused::Left)
        );
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "a body whose sender had left went on waiting for room"
        );
        let stats = wal.stats();
        assert_eq!(
            (stats.queue_left, stats.queue_sheds, stats.queue_waiting),
            (1, 0, 0)
        );

        gate.open();
        assert_eq!(first.join().expect("the first sender"), Ok(()));
        assert_eq!(wal.commit(even(2), 0), Ok(()));
        assert_eq!(*lock(&gate.applied), (1..=2 * EVEN).collect::<Vec<u64>>());
        wal.close();
        assert_eq!(stored(&scratch), lines_of(&[0, 2]));
    }

    /// A sender that has left is still taken when there is room: room is looked for before
    /// the flag, so a body whose sender hung up while it was parsed goes in whole.
    #[test]
    fn a_body_whose_sender_has_left_is_taken_when_there_is_room() {
        let scratch = Scratch::new("room-left-fits");
        let (wal, applied) = started(&scratch, Options::default());
        let left = AtomicBool::new(true);
        let wait = Wait {
            at_most: Duration::ZERO,
            left: &left,
        };
        let taken = wal.enqueue(even(0), 0, wait).and_then(Ticket::wait);
        assert_eq!(taken, Ok(()));
        assert_eq!(lock(&applied).len() as u64, EVEN);
        assert_eq!(wal.stats().queue_left, 0);
    }

    /// Nobody waits for room once the log has been told to hurry: a stop must not take as
    /// long as the longest wait.
    #[test]
    fn nobody_waits_once_the_log_is_told_to_hurry() {
        let scratch = Scratch::new("room-hurry");
        let (wal, mut gate, first) = full(&scratch, Options::default());
        let second = waiter(&wal, even(1), LONG_WAIT, &staying());
        until("the second body to wait for room", || {
            wal.stats().queue_waiting == 1
        });

        let began = std::time::Instant::now();
        wal.hurry();
        assert_eq!(
            second.join().expect("the second sender"),
            Err(Refused::Busy)
        );
        let third = waiter(&wal, even(2), LONG_WAIT, &staying());
        assert_eq!(third.join().expect("the third sender"), Err(Refused::Busy));
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "a body went on waiting for room in a log told to hurry"
        );
        let stats = wal.stats();
        assert_eq!(
            (stats.queue_sheds, stats.queue_waits, stats.queue_waiting),
            (2, 1, 0)
        );
        assert_eq!(stats.last_seq, EVEN);

        // The body inside is finished, as a stop finishes it.
        gate.open();
        assert_eq!(first.join().expect("the first sender"), Ok(()));
    }

    /// A thread of the log that dies tells the bodies waiting for room. The applier dies only
    /// once the second body is seen waiting, or the test could pass with nobody having waited.
    #[test]
    fn a_thread_that_dies_tells_the_bodies_waiting_for_room() {
        let scratch = Scratch::new("room-died");
        let wal = Arc::new(Wal::create(&scratch.0).expect("create"));
        let (said, handed) = channel::<usize>();
        // Declared after the log, so let go first: the applier is never left held.
        let (go, told) = channel::<()>();
        let options = Options {
            queue_bytes: bytes_of(&even(0)),
            ..Options::default()
        };
        wal.start(
            options,
            Box::new(move |rows| {
                let _ = said.send(rows.len());
                let _ = told.recv();
                panic!("the applier dies");
            }),
        )
        .expect("start");

        let first = sender(&wal, even(0));
        let got = handed.recv_timeout(Duration::from_secs(30));
        assert_eq!(got, Ok(EVEN as usize));
        let second = waiter(&wal, even(1), LONG_WAIT, &staying());
        until("the second body to wait for room", || {
            wal.stats().queue_waiting == 1
        });

        let began = std::time::Instant::now();
        go.send(()).expect("the applier is there");
        let why = "a thread of the write-ahead log stopped; restart the server".to_string();
        assert_eq!(
            second.join().expect("the second sender"),
            Err(Refused::Cannot(why.clone()))
        );
        assert!(
            began.elapsed() < WOKEN_WITHIN,
            "a body waiting for room was not told that the log's thread had died"
        );
        // Written before the applier was handed it: durable, so acknowledged.
        assert_eq!(first.join().expect("the first sender"), Ok(()));
        assert_eq!(wal.refusal(), Some(why));
        assert_eq!(wal.stats().queue_waiting, 0);
    }
}
