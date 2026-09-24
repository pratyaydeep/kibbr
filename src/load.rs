//! The bulk door: `triedb load`, a closed day of lines - or six months of them - loaded
//! whole.
//!
//! ```text
//!  inputs ─► load id ─► arrival check on the head ─► parse, on the load's own threads
//!         ─► per template and UTC day, batches in memory (invisible to queries)
//!         ─► full row groups ─► staging/<load id>/<day>/NNNNNN.col, sealed, finished
//!         ─► one manifest record ─► data/<day>/seg_0_NNNNNN.col ─► the leaves
//! ```
//!
//! - **One write path.** A line goes through `db::place`, which `/ingest` goes through
//!   too: the same template, the same types, the same catalog records, the same stored
//!   row with the same arrival stamp. What differs is only where the row goes - a batch
//!   of the load's, not a leaf's buffer - so no query sees a row of a load before the
//!   whole load is committed.
//! - **Full row groups.** The loader sees all of its input, so a template's rows of a day
//!   close at 8,192 rows or about 8MB of column data, the thresholds every writer uses -
//!   never at a timeout. A memory limit closes the largest batches early, as the live
//!   door's does: at 10,000 templates a batch each is more memory than the machine has.
//! - **Born finished.** Every file is flagged finished, so compaction will skip it: there
//!   is nothing a rewrite would improve. It needs no write-ahead log - the input is the
//!   log - and a stop at any moment before the commit costs only the staging directory.
//! - **All or nothing.** `DiskStore::commit_load` publishes every file of the load in one
//!   manifest record, however many days they span.
//!
//! **The load id** is FNV-1a 64 over each input in the order the loader reads them -
//! sorted by file name, then by length and content (`order`) - of its file name (`-` for
//! standard input), a zero
//! byte, its length after decompression and the FNV-1a 64 of its bytes after
//! decompression, both as `u64` little-endian; 0 is taken as 1, because 0 means no stream.
//! The name and not the path, so the same files read from another mount are the same load;
//! the bytes after decompression, so a file recompressed under the same name is the same
//! file. Two inputs of one name are ordered by their length and content, not by path. The
//! manifest records the id as the commit's source, and a load whose id it already names is
//! refused before a line is parsed: the same inputs twice would be every row twice, and
//! nothing later could tell the copies apart. Standard input is spooled to a file first, so
//! its id is known - and a repeat refused - before anything is written, like any other
//! input's.
//!
//! **What the id does not catch.** It is over the whole set of inputs, by name: a file
//! renamed (`day.ndjson` loaded again as `day.ndjson.gz`), the same bytes piped through
//! `-`, or a file loaded again beside new ones is a new load, and its rows are stored
//! twice. TrieDB keeps no row identity to catch that later; one source per day, and
//! `--replace-day` to change it, is the rule that keeps a day's rows once.
//!
//! **Positions.** Each row group records the load id as its stream and the line numbers of
//! its first and last rows, counted from 1 across every input in load order - where each
//! row came from, and the range a replay would skip.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use flate2::read::MultiGzDecoder;
use serde::Serialize;
use thiserror::Error;

use crate::db::{self, Prepared, SinkError};
use crate::error::{DetectorError, DetectorErrorKind};
use crate::flatten::{parse_line, ParsedLog};
use crate::lines::{read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES};
use crate::store::disk::{day_name, Committed, LoadCommit, StagedFile};
use crate::store::v3::build::{self, schema_of};
use crate::store::v3::file::Writer;
use crate::store::v3::{day_of, Source};
use crate::store::{
    effective_ts, ts_of, value_bytes, DiskStore, Row, RowGroupLimits, StoreError, StoreManager,
};
use crate::trie::Trie;
use crate::types::TimeSource;

/// How much of the input the arrival-time check reads: enough rows that a share of 1% is
/// a hundred of them, few enough that it costs nothing beside the load.
pub const HEAD_ROWS: usize = 10_000;

/// The share of the head, in percent, that may fall back to arrival time before a load
/// stops. Loading old files, arrival time is the wrong day by construction - today - so a
/// row that takes it is misfiled rather than late; a load that would misfile more than
/// one row in a hundred is more likely missing a `time_columns` name than meant.
pub const DEFAULT_MAX_ARRIVAL_SHARE: f64 = 1.0;

/// Lines per parse job: large enough that handing one to a thread costs nothing beside
/// parsing it.
const CHUNK_LINES: usize = 2_048;

/// Raw bytes a round of chunks may hold before it is parsed and applied, whatever the line
/// count: rounds are read whole into memory, and only rows in batches are held against the
/// memory limit.
const ROUND_BYTES: usize = 64 * 1024 * 1024;

/// Staging files a load keeps open at once. A load of six months has a day per file; past
/// this many, the least recently written is sealed and its day's next rows open a file of
/// their own, which keeps the load inside the process's open-file limit whatever it spans.
const MAX_OPEN_STAGED: usize = 64;

/// FNV-1a 64: the load id's hash. Sixty-four bits is enough: a collision refuses a new load
/// as a repeat, which is loud, and a cryptographic hash would be a dependency against a
/// threat - an operator's own files - that does not exist.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv(mut h: u64, bytes: &[u8]) -> u64 {
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

// ── What a load is asked to do ─────────────────────────────────────────────────────

/// One input, as the load id and the line numbers see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Input {
    pub path: PathBuf,
    /// Its file name - `-` for standard input - which is what the load id hashes.
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Options {
    /// In load order.
    pub inputs: Vec<Input>,
    /// Retire every other file of this day, as days since the epoch, in the commit.
    pub replace_day: Option<i64>,
    /// Decompressed input bytes a second, across both passes over the input.
    pub max_bytes_per_sec: Option<f64>,
    pub threads: usize,
    /// The arrival time of every row with no time of its own, and every template's
    /// `created_at`: one instant for the whole load, as one `/ingest` body has one.
    pub received_at: DateTime<Utc>,
    /// Load even when more of the head than `max_arrival_share` falls back to arrival time.
    pub proceed: bool,
    /// In percent.
    pub max_arrival_share: f64,
}

/// What a running load has done so far, and the switch that stops it: shared with whoever
/// is waiting on it.
#[derive(Debug, Default)]
pub struct Control {
    /// Lines read so far, in the pass that loads.
    pub lines: AtomicU64,
    /// Set to stop the load before it commits: a server shutting down.
    pub cancel: Arc<AtomicBool>,
}

impl Control {
    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::Relaxed)
    }
}

/// Threads a load runs on when it is alone: one per core, up to eight - the most the
/// encoder has been measured to use well.
pub fn alone_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get().min(8))
}

/// Threads a load runs on inside a server: half of what it would take alone, so the live
/// door keeps the other half.
pub fn server_threads() -> usize {
    (alone_threads() / 2).max(1)
}

// ── What a load did ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct Arrival {
    /// Rows of the head read.
    pub head: u64,
    /// Of those, rows that would take their arrival time.
    pub fell_back: u64,
    /// In percent.
    pub share: f64,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct LineError {
    pub line: u64,
    pub error: String,
}

/// A committed load, as `--json` prints it and `GET /load/<job>` returns it.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct Report {
    pub ok: bool,
    pub load_id: String,
    /// Physical lines read, blanks included: the numbering an editor shows.
    pub lines: u64,
    pub blank: u64,
    pub rejected: u64,
    pub rows: u64,
    pub row_groups: u64,
    /// Published, by path under `data/`.
    pub files: Vec<String>,
    pub days: u64,
    /// Bytes of the published files.
    pub bytes: u64,
    /// Retired by `--replace-day`.
    #[serde(default)]
    pub replaced: Vec<String>,
    pub arrival: Arrival,
    pub seconds: f64,
    /// The first rejected lines, and how many more there were.
    pub errors: Vec<LineError>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub errors_omitted: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// Rejected lines a report carries by number. Past this they are counted: a million bad
/// lines are one fault, and a report a client has to page through is not a report.
const MAX_REPORTED_ERRORS: usize = 1_000;

impl Report {
    /// One line for a terminal.
    pub fn summary(&self) -> String {
        use crate::fmt::commas;
        let mut out = format!(
            "loaded {} rows from {} lines ({} rejected) as load {}: {} row groups in {} \
             file(s) over {} day(s), {:.1}s",
            commas(self.rows),
            commas(self.lines),
            commas(self.rejected),
            self.load_id,
            commas(self.row_groups),
            self.files.len(),
            self.days,
            self.seconds
        );
        if !self.replaced.is_empty() {
            out.push_str(&format!(
                "; replaced {} file(s) of that day",
                self.replaced.len()
            ));
        }
        out
    }
}

#[derive(Debug, Error)]
pub enum LoadError {
    #[error("{path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{0}")]
    Input(String),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Format(#[from] crate::store::v3::FormatError),
    #[error(
        "{share:.1}% of the first {head} rows ({fell_back}) have no time of their own and \
         would be stored at their arrival time, {day} - more than the {limit}% \
         --max-arrival-share allows. Nothing was loaded. If their templates are new, add \
         their time key to the time columns (--time-columns, or TRIEDB__SCHEMA__TIME_COLUMNS \
         for a server); a template that exists already keeps the time source it was made \
         with. Or pass --proceed to load them at their arrival time"
    )]
    ArrivalShare {
        share: f64,
        head: u64,
        fell_back: u64,
        day: String,
        limit: f64,
    },
    #[error(
        "line {line} is on {day}, outside --replace-day {replace}: a replace publishes one \
         day's rows in place of what that day holds. Nothing was loaded"
    )]
    OutsideDay {
        line: u64,
        day: String,
        replace: String,
    },
    #[error("the load was stopped before it committed; nothing was loaded")]
    Cancelled,
    #[error(
        "this store keeps its rows in memory; a load writes files, and needs a server started \
         with a data directory"
    )]
    NoDisk,
    #[error(transparent)]
    Cli(Box<crate::error::CliError>),
    /// The server that holds the data directory ran the load, and it failed there.
    #[error("{0}")]
    Remote(String),
    /// The server that holds the data directory could not be asked.
    #[error("the server at {address}, which holds the data directory: {detail}")]
    Server { address: String, detail: String },
}

impl From<crate::error::CliError> for LoadError {
    fn from(e: crate::error::CliError) -> Self {
        LoadError::Cli(Box::new(e))
    }
}

// ── Inputs ─────────────────────────────────────────────────────────────────────────

/// What makes two inputs the same file, however they were reached: a symlinked
/// directory, a hard link, or the same path given twice. Loaded twice in one load, each of
/// its rows would be stored twice under one id, and no repeat check could catch it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Identity {
    Stdin,
    #[cfg(unix)]
    Inode(u64, u64),
    #[cfg(not(unix))]
    Path(PathBuf),
}

fn identity(path: &Path, meta: &fs::Metadata) -> Identity {
    #[cfg(unix)]
    {
        let _ = path;
        use std::os::unix::fs::MetadataExt;
        Identity::Inode(meta.dev(), meta.ino())
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Identity::Path(fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()))
    }
}

/// The command line's inputs, expanded: a directory to every regular file under it, at any
/// depth, skipping names that start with `.`; `-` to standard input, copied to `spool`
/// first. Each file once, whichever ways it was reached, at its canonical path under the
/// name it was found by.
///
/// **Regular files only.** A load reads its input three times - for the id, for the
/// arrival check and to stage it - and a pipe or a device gives its bytes once: the second
/// read would find nothing, and an empty load would be committed and its id recorded.
/// Standard input is spooled to a file for that reason, and `-` is how to pipe a load.
pub fn resolve(args: &[PathBuf], spool: Option<&Path>) -> Result<Vec<Input>, LoadError> {
    let mut found: Vec<(Input, Identity)> = Vec::new();
    let mut stdin_seen = false;
    for arg in args {
        if arg.as_os_str() == "-" {
            if stdin_seen {
                return Err(LoadError::Input(
                    "standard input (`-`) is named twice".to_string(),
                ));
            }
            stdin_seen = true;
            let spool = spool.ok_or_else(|| {
                LoadError::Input("standard input cannot be read here".to_string())
            })?;
            let mut to = File::create(spool).map_err(|source| LoadError::Read {
                path: spool.to_path_buf(),
                source,
            })?;
            io::copy(&mut io::stdin().lock(), &mut to).map_err(|source| LoadError::Read {
                path: PathBuf::from("-"),
                source,
            })?;
            found.push((
                Input {
                    path: spool.to_path_buf(),
                    name: "-".to_string(),
                },
                Identity::Stdin,
            ));
            continue;
        }
        let meta = fs::metadata(arg).map_err(|source| LoadError::Read {
            path: arg.clone(),
            source,
        })?;
        if meta.is_dir() {
            // Its own set of directories seen: a directory also given as another argument,
            // or inside one, is walked again and its files found again - and then kept once
            // below - rather than taken for an empty one.
            let before = found.len();
            walk_dir(arg, &meta, &mut found, &mut BTreeSet::new())?;
            if found.len() == before {
                return Err(LoadError::Input(format!(
                    "{}: a directory holding no files to load",
                    arg.display()
                )));
            }
        } else if meta.is_file() {
            found.push(found_file(arg, &meta)?);
        } else {
            return Err(LoadError::Input(format!(
                "{}: not a regular file. A load reads its input more than once - for its id, \
                 its arrival check and its rows - and a pipe or a device gives its bytes only \
                 once; pipe it to `triedb load -` instead",
                arg.display()
            )));
        }
    }
    if found.is_empty() {
        return Err(LoadError::Input("nothing to load".to_string()));
    }
    Ok(once(found))
}

/// Each file once: the first of its names in name order, so which one is kept does not
/// depend on the order they were given in.
fn once(mut found: Vec<(Input, Identity)>) -> Vec<Input> {
    found.sort_by(|a, b| {
        a.0.name
            .cmp(&b.0.name)
            .then_with(|| a.0.path.cmp(&b.0.path))
    });
    let mut seen = BTreeSet::new();
    found
        .into_iter()
        .filter(|(_, id)| *id == Identity::Stdin || seen.insert(id.clone()))
        .map(|(input, _)| input)
        .collect()
}

/// The same for inputs that did not come through `resolve` - a server's `/load` request,
/// whose client may be anything that speaks HTTP: a file named twice there would be loaded
/// twice under one id just the same.
fn each_file_once(inputs: &[Input]) -> Result<Vec<Input>, LoadError> {
    let mut found = Vec::with_capacity(inputs.len());
    for input in inputs {
        let meta = fs::metadata(&input.path).map_err(|source| LoadError::Read {
            path: input.path.clone(),
            source,
        })?;
        found.push((input.clone(), identity(&input.path, &meta)));
    }
    Ok(once(found))
}

fn found_file(path: &Path, meta: &fs::Metadata) -> Result<(Input, Identity), LoadError> {
    let name = path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let canonical = fs::canonicalize(path).map_err(|source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok((
        Input {
            path: canonical,
            name,
        },
        identity(path, meta),
    ))
}

/// Every regular file under `dir`, through links, each directory walked once: a link back
/// to an ancestor - `current -> .` - is a directory already seen, not a loop to follow
/// until the kernel's link limit.
fn walk_dir(
    dir: &Path,
    meta: &fs::Metadata,
    out: &mut Vec<(Input, Identity)>,
    visited: &mut BTreeSet<Identity>,
) -> Result<(), LoadError> {
    if !visited.insert(identity(dir, meta)) {
        return Ok(());
    }
    let entries = fs::read_dir(dir).map_err(|source| LoadError::Read {
        path: dir.to_path_buf(),
        source,
    })?;
    for entry in entries {
        let entry = entry.map_err(|source| LoadError::Read {
            path: dir.to_path_buf(),
            source,
        })?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let path = entry.path();
        // Through the link, as `fs::metadata` does for a file named on the command line.
        match fs::metadata(&path) {
            Ok(meta) if meta.is_dir() => walk_dir(&path, &meta, out, visited)?,
            Ok(meta) if meta.is_file() => out.push(found_file(&path, &meta)?),
            Ok(_) => {}
            Err(source) => return Err(LoadError::Read { path, source }),
        }
    }
    Ok(())
}

/// A file's bytes after decompression: gzip and zstd are told apart from NDJSON by their
/// first bytes and not by a name, which a shipper chooses and a rename changes. A gzip
/// file of several members, and a zstd file of several frames, read whole.
pub fn open_decoded(path: &Path) -> Result<Box<dyn Read + Send>, LoadError> {
    let io_err = |source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    };
    let mut file = BufReader::with_capacity(1 << 20, File::open(path).map_err(io_err)?);
    let head = file.fill_buf().map_err(io_err)?;
    Ok(if head.starts_with(&[0x1f, 0x8b]) {
        Box::new(MultiGzDecoder::new(file))
    } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        Box::new(zstd::stream::read::Decoder::with_buffer(file).map_err(io_err)?)
    } else {
        Box::new(file)
    })
}

/// The pace `--max-mb-per-sec` holds a load to: the bytes read so far, decompressed, never
/// ahead of the rate times the time since the load began.
///
/// On the decompressed bytes, because they are what the parse and the encode cost - the
/// CPU the live door shares - where a compressed file's bytes on disk understate it tenfold.
///
/// **Every read checks for a stop**, and a wait is slept in short slices that check too: a
/// load paced to a trickle must not hold up a server's shutdown - and its final flush of the
/// live door's rows - for as long as its next megabyte is due.
struct Pace {
    bytes_per_sec: Option<f64>,
    start: Instant,
    read: u64,
    cancel: Arc<AtomicBool>,
}

/// How long a paced wait sleeps before it looks for a stop again.
const PACE_SLICE: Duration = Duration::from_millis(50);

impl Pace {
    fn new(bytes_per_sec: Option<f64>, cancel: Arc<AtomicBool>) -> Self {
        Self {
            bytes_per_sec: bytes_per_sec.filter(|r| *r > 0.0),
            start: Instant::now(),
            read: 0,
            cancel,
        }
    }

    fn stopped(&self) -> io::Result<()> {
        if self.cancel.load(Ordering::Relaxed) {
            return Err(io::Error::other("the load was stopped"));
        }
        Ok(())
    }

    fn took(&mut self, n: usize) -> io::Result<()> {
        self.stopped()?;
        self.read += n as u64;
        if let Some(rate) = self.bytes_per_sec {
            let due = Duration::from_secs_f64(self.read as f64 / rate);
            loop {
                let spent = self.start.elapsed();
                if due <= spent {
                    break;
                }
                std::thread::sleep((due - spent).min(PACE_SLICE));
                self.stopped()?;
            }
        }
        Ok(())
    }
}

struct Paced<'a, R> {
    inner: R,
    pace: &'a RefCell<Pace>,
}

impl<R: Read> Read for Paced<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pace.borrow_mut().took(n)?;
        Ok(n)
    }
}

fn paced<'a>(
    path: &Path,
    pace: &'a RefCell<Pace>,
) -> Result<BufReader<Paced<'a, Box<dyn Read + Send>>>, LoadError> {
    Ok(BufReader::with_capacity(
        1 << 20,
        Paced {
            inner: open_decoded(path)?,
            pace,
        },
    ))
}

/// The load id of these inputs. See the module comment.
#[cfg(test)]
pub fn load_id(inputs: &[Input]) -> Result<u64, LoadError> {
    let pace = RefCell::new(Pace::new(None, Arc::default()));
    Ok(order(inputs, &pace)?.1)
}

/// One input's length after decompression and the FNV-1a 64 of those bytes.
fn digest(input: &Input, pace: &RefCell<Pace>) -> Result<(u64, u64), LoadError> {
    let mut reader = paced(&input.path, pace)?;
    let (mut len, mut hash) = (0u64, FNV_OFFSET);
    loop {
        let buf = reader.fill_buf().map_err(|source| LoadError::Read {
            path: input.path.clone(),
            source,
        })?;
        if buf.is_empty() {
            return Ok((len, hash));
        }
        len += buf.len() as u64;
        hash = fnv(hash, buf);
        let n = buf.len();
        reader.consume(n);
    }
}

/// The inputs in load order, and the load id.
///
/// **By name, then by what the file holds.** Two inputs of one name - `app.log` from two
/// hosts' directories - are ordered by their length and content hash, not by their paths:
/// a path depends on the mount, the working directory and how it was typed, and ordering by
/// it would give the same files two ids, and a repeat of them a way through. The path
/// breaks a tie only between two files alike in name and bytes, where the order changes
/// neither the id nor a line.
fn order(inputs: &[Input], pace: &RefCell<Pace>) -> Result<(Vec<Input>, u64), LoadError> {
    let mut keyed = Vec::with_capacity(inputs.len());
    for input in inputs {
        let (len, hash) = digest(input, pace)?;
        keyed.push((input.clone(), len, hash));
    }
    keyed.sort_by(|a, b| {
        a.0.name
            .cmp(&b.0.name)
            .then(a.1.cmp(&b.1))
            .then(a.2.cmp(&b.2))
            .then_with(|| a.0.path.cmp(&b.0.path))
    });
    let mut id = FNV_OFFSET;
    for (input, len, hash) in &keyed {
        id = fnv(id, input.name.as_bytes());
        id = fnv(id, &[0]);
        id = fnv(id, &len.to_le_bytes());
        id = fnv(id, &hash.to_le_bytes());
    }
    let id = if id == 0 { 1 } else { id };
    Ok((keyed.into_iter().map(|(input, ..)| input).collect(), id))
}

// ── Reading lines ──────────────────────────────────────────────────────────────────

/// The physical lines of every input in load order, numbered from 1 across all of them,
/// handed out in chunks. Blank lines are counted and dropped; a line over the cap is
/// rejected by number and stepped over, as every read loop does.
struct Lines<'a> {
    inputs: &'a [Input],
    at: usize,
    reader: Option<BufReader<Paced<'a, Box<dyn Read + Send>>>>,
    pace: &'a RefCell<Pace>,
    line: u64,
    blank: u64,
    raw: Vec<u8>,
}

/// One line read: its number, and its bytes - `None` for a line over the cap, which is
/// rejected in its place among the others.
type Got = (u64, Option<Vec<u8>>);

impl<'a> Lines<'a> {
    fn new(inputs: &'a [Input], pace: &'a RefCell<Pace>) -> Self {
        Self {
            inputs,
            at: 0,
            reader: None,
            pace,
            line: 0,
            blank: 0,
            raw: Vec::new(),
        }
    }

    /// The next non-blank line, or `None` at the end of the last input.
    fn next_line(&mut self) -> Result<Option<Got>, LoadError> {
        loop {
            if self.reader.is_none() {
                let Some(input) = self.inputs.get(self.at) else {
                    return Ok(None);
                };
                self.reader = Some(paced(&input.path, self.pace)?);
            }
            let path = &self.inputs[self.at].path;
            let reader = self.reader.as_mut().expect("opened above");
            self.raw.clear();
            let read = read_line(reader, &mut self.raw).map_err(|source| LoadError::Read {
                path: path.clone(),
                source,
            })?;
            match read {
                Line::Eof => {
                    self.reader = None;
                    self.at += 1;
                }
                Line::TooLong => {
                    self.line += 1;
                    let at = self.line;
                    match skip_to_eol(reader).map_err(|source| LoadError::Read {
                        path: path.clone(),
                        source,
                    })? {
                        Resync::Done => return Ok(Some((at, None))),
                        Resync::NotDelimited { scanned } => {
                            return Err(crate::error::CliError::NotNewlineDelimited {
                                line: at,
                                scanned,
                                limit: MAX_LINE_BYTES,
                            }
                            .into())
                        }
                    }
                }
                Line::Read => {
                    self.line += 1;
                    let line = strip_bom(strip_eol(&self.raw));
                    if std::str::from_utf8(line).is_ok_and(|s| s.trim().is_empty()) {
                        self.blank += 1;
                        continue;
                    }
                    return Ok(Some((self.line, Some(line.to_vec()))));
                }
            }
        }
    }
}

// ── The arrival-time check ─────────────────────────────────────────────────────────

/// The share of the head of the input that would be stored at its arrival time.
///
/// **Nothing is created.** A line of a template the catalog knows takes that template's
/// time source; a line of one it does not takes the source its first line would choose -
/// the same rule `Trie::ingest` applies - remembered here for the head's later lines of it.
/// Creating the templates for real would make the check's answer permanent: a template's
/// time source is chosen once, and one created while a load is refused would keep arrival
/// time after the operator added its time key and ran the load again.
fn arrival_head(trie: &Trie, inputs: &[Input], pace: &RefCell<Pace>) -> Result<Arrival, LoadError> {
    let mut lines = Lines::new(inputs, pace);
    let mut unseen: HashMap<Vec<String>, TimeSource> = HashMap::new();
    let (mut head, mut fell_back) = (0u64, 0u64);
    while (head as usize) < HEAD_ROWS {
        let Some(got) = lines.next_line()? else {
            break;
        };
        let (no, Some(raw)) = got else {
            continue;
        };
        let Ok(parsed) = parse_line(&raw, no as usize) else {
            continue;
        };
        let tokens = parsed.walk_tokens();
        let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
        let known = trie.walk(&borrowed).and_then(|walked| {
            walked
                .node
                .leaf_meta
                .read()
                .ok()
                .and_then(|slot| slot.as_ref().map(|meta| meta.time_source.clone()))
        });
        let source = match known {
            Some(source) => source,
            None => unseen
                .entry(tokens)
                .or_insert_with(|| {
                    TimeSource::choose(trie.time_columns(), |name| parsed.column_type(name))
                })
                .clone(),
        };
        if ts_of(&db::row_of(&parsed), source.column()).is_none() {
            fell_back += 1;
        }
        head += 1;
    }
    let share = if head == 0 {
        0.0
    } else {
        fell_back as f64 * 100.0 / head as f64
    };
    Ok(Arrival {
        head,
        fell_back,
        share,
    })
}

// ── Batches and the files they go into ─────────────────────────────────────────────

/// One template's rows of one UTC day, in line order, not yet a row group.
#[derive(Default)]
struct Batch {
    rows: Vec<Row>,
    times: Vec<i64>,
    first: u64,
    last: u64,
    bytes: usize,
    held: usize,
}

/// A batch whose rows are due: closed, waiting to be encoded and written.
struct Due {
    template_id: u64,
    day: i64,
    batch: Batch,
}

/// A staging file being written.
struct Staging {
    writer: Writer,
    day: i64,
    /// Its number among its day's staging files: storage order, which the commit keeps.
    seq: u32,
    path: PathBuf,
    /// The write that last used it, for sealing the least recently written first.
    used: u64,
}

/// The load's files: one open per day at most, sealed at the size cap or when too many are
/// open, each flagged finished.
struct Stage<'a> {
    dir: &'a Path,
    max_bytes: u64,
    open: BTreeMap<i64, Staging>,
    next: BTreeMap<i64, u32>,
    done: Vec<(i64, u32, StagedFile)>,
    writes: u64,
    row_groups: u64,
}

impl Stage<'_> {
    fn io(path: &Path) -> impl FnOnce(io::Error) -> LoadError + '_ {
        move |source| LoadError::Read {
            path: path.to_path_buf(),
            source,
        }
    }

    /// The day's open staging file, made - and its directory synced when new - if there is
    /// none.
    fn file(&mut self, day: i64) -> Result<&mut Staging, LoadError> {
        if !self.open.contains_key(&day) {
            if self.open.len() >= MAX_OPEN_STAGED {
                let lru = self
                    .open
                    .iter()
                    .min_by_key(|(_, s)| s.used)
                    .map(|(d, _)| *d)
                    .expect("more than zero open");
                self.seal(lru)?;
            }
            let day_dir = self.dir.join(day_name(day));
            if !day_dir.exists() {
                fs::create_dir_all(&day_dir).map_err(Self::io(&day_dir))?;
                crate::frame::sync_dir(self.dir).map_err(Self::io(self.dir))?;
            }
            let seq = {
                let n = self.next.entry(day).or_insert(1);
                *n += 1;
                *n - 1
            };
            let path = day_dir.join(format!("{seq:06}.col"));
            let mut writer = Writer::create(&path)?;
            writer.finish();
            self.open.insert(
                day,
                Staging {
                    writer,
                    day,
                    seq,
                    path,
                    used: 0,
                },
            );
        }
        self.writes += 1;
        let writes = self.writes;
        let staging = self.open.get_mut(&day).expect("opened above");
        staging.used = writes;
        Ok(staging)
    }

    fn seal(&mut self, day: i64) -> Result<(), LoadError> {
        if let Some(s) = self.open.remove(&day) {
            let index = s.writer.seal()?;
            self.done.push((
                s.day,
                s.seq,
                StagedFile {
                    day: s.day,
                    path: s.path,
                    index,
                },
            ));
        }
        Ok(())
    }

    /// Append one encoded row group to its day's file, and seal that file once it is past
    /// the size cap - after the append, as the live door checks after a flush, so a file
    /// passes the cap by at most one row group.
    fn append(
        &mut self,
        day: i64,
        template_id: u64,
        columns: &[String],
        encoded: build::Encoded,
    ) -> Result<(), LoadError> {
        let max = self.max_bytes;
        let staging = self.file(day)?;
        let schema_id = staging.writer.schema_id(columns)?;
        staging
            .writer
            .append(template_id, &encoded.frame(schema_id))?;
        let full = staging.writer.len() >= max;
        self.row_groups += 1;
        if full {
            self.seal(day)?;
        }
        Ok(())
    }

    /// Every file sealed, in storage order: by day, then as each day's were made.
    fn finish(mut self) -> Result<(Vec<StagedFile>, u64), LoadError> {
        let days: Vec<i64> = self.open.keys().copied().collect();
        for day in days {
            self.seal(day)?;
        }
        self.done.sort_by_key(|(day, seq, _)| (*day, *seq));
        Ok((
            self.done.into_iter().map(|(_, _, f)| f).collect(),
            self.row_groups,
        ))
    }
}

/// One batch's schema and encoded row group, or why it has none.
type EncodedBatch = Result<(Vec<String>, build::Encoded), LoadError>;

/// What staging a load leaves for its commit: the sealed files, the counts, and each
/// template it touched with its time column, whose leaves open before the commit.
type StagedLoad = (Vec<StagedFile>, Counts, BTreeMap<u64, String>);

/// Encode every due batch - on up to `threads` threads - and append each to its day's
/// staging file, in the order they closed: a template's row groups of a day are then in
/// line order, which is the order the live door would have stored them in.
fn write_due(
    due: &mut Vec<Due>,
    stage: &mut Stage<'_>,
    load_id: u64,
    threads: usize,
) -> Result<(), LoadError> {
    if due.is_empty() {
        return Ok(());
    }
    let batches = std::mem::take(due);
    let encode_batch = |d: &Due, load_id: u64| -> EncodedBatch {
        let refs: Vec<&Row> = d.batch.rows.iter().collect();
        let columns = schema_of(refs.iter().copied());
        let encoded = build::encode(
            d.template_id,
            &columns,
            &refs,
            &d.batch.times,
            Source::Load {
                stream: load_id,
                first: d.batch.first,
                last: d.batch.last,
            },
        )
        .map_err(StoreError::from)?;
        Ok((columns, encoded))
    };
    // An encoder that panics fails the load, as it fails its batch in the live door: a
    // panic let out of a scoped thread would take the whole server job with it.
    let encode = |d: &Due| -> EncodedBatch {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| encode_batch(d, load_id)))
            .unwrap_or_else(|_| {
                Err(LoadError::Input(format!(
                    "the row group encoder failed on template {}; nothing was loaded",
                    d.template_id
                )))
            })
    };
    let threads = threads.clamp(1, batches.len());
    let results: Vec<EncodedBatch> = if threads == 1 {
        batches.iter().map(encode).collect()
    } else {
        let next = AtomicUsize::new(0);
        let slots: Vec<OnceLock<EncodedBatch>> = batches.iter().map(|_| OnceLock::new()).collect();
        std::thread::scope(|scope| {
            for _ in 0..threads {
                scope.spawn(|| loop {
                    let at = next.fetch_add(1, Ordering::Relaxed);
                    let Some(d) = batches.get(at) else {
                        break;
                    };
                    let _ = slots[at].set(encode(d));
                });
            }
        });
        slots
            .into_iter()
            .map(|slot| {
                slot.into_inner().unwrap_or_else(|| {
                    Err(LoadError::Input(
                        "a row group was never encoded".to_string(),
                    ))
                })
            })
            .collect()
    };
    for (d, result) in batches.iter().zip(results) {
        let (columns, encoded) = result?;
        stage.append(d.day, d.template_id, &columns, encoded)?;
    }
    Ok(())
}

// ── The load ───────────────────────────────────────────────────────────────────────

/// A line parsed on a pool thread: its number, and its row or why it has none.
type Parsed = (u64, Result<(ParsedLog, Prepared), DetectorError>);

fn parse_chunks(chunks: Vec<Vec<Got>>, threads: usize) -> Result<Vec<Vec<Parsed>>, LoadError> {
    let parse = |chunk: &Vec<Got>| -> Vec<Parsed> {
        chunk
            .iter()
            .map(|(no, raw)| {
                let parsed = match raw {
                    Some(raw) => parse_line(raw, *no as usize).map(|parsed| {
                        let prepared = Prepared::of(&parsed);
                        (parsed, prepared)
                    }),
                    None => Err(DetectorError::new(
                        *no as usize,
                        DetectorErrorKind::LineTooLong {
                            limit: MAX_LINE_BYTES,
                        },
                    )),
                };
                (*no, parsed)
            })
            .collect()
    };
    // A chunk whose parse panicked fails the load: an empty result in its place would drop
    // two thousand lines from a load that then commits, uncounted, under an id that refuses
    // them for good.
    let failed = |chunk: &Vec<Got>| {
        LoadError::Input(format!(
            "the parser failed on lines {} to {}; nothing was loaded",
            chunk.first().map_or(0, |g| g.0),
            chunk.last().map_or(0, |g| g.0)
        ))
    };
    if threads <= 1 || chunks.len() <= 1 {
        return chunks
            .iter()
            .map(|chunk| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parse(chunk)))
                    .map_err(|_| failed(chunk))
            })
            .collect();
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .iter()
            .map(|chunk| scope.spawn(move || parse(chunk)))
            .collect();
        // Every handle joined before any error is returned: one left unjoined whose thread
        // panicked makes the scope itself panic, and the error naming the lines is lost.
        let joined: Vec<Result<Vec<Parsed>, LoadError>> = handles
            .into_iter()
            .zip(&chunks)
            .map(|(h, chunk)| h.join().map_err(|_| failed(chunk)))
            .collect();
        joined.into_iter().collect()
    })
}

/// Run a load against an open store, from the id to the commit.
///
/// `on_error` is told each rejected line as it is found; the report carries the first
/// thousand as well. The caller owns the data directory - it holds `LOCK` - and has opened
/// the leaves it serves; the templates this load creates get theirs here, before the
/// commit, so its rows appear in each leaf in the same step as the commit publishes them.
pub fn run(
    trie: &Trie,
    stores: &StoreManager,
    opts: &Options,
    control: &Control,
    on_error: &mut dyn FnMut(&DetectorError),
) -> Result<Report, LoadError> {
    match run_inner(trie, stores, opts, control, on_error) {
        // A stop shows up as whatever read or wait it interrupted; it is still a stop.
        Err(e) if control.cancelled() && !committed_or_uncertain(&e) => Err(LoadError::Cancelled),
        other => other,
    }
}

/// An error after which the staged files may be the only copy of committed rows.
fn committed_or_uncertain(e: &LoadError) -> bool {
    matches!(
        e,
        LoadError::Store(StoreError::NotMoved { .. } | StoreError::CommitUncertain { .. })
    )
}

/// The most threads a load takes, whatever it is asked for: one per core. Past that a
/// thread per chunk buys nothing, and a round holds a chunk per thread in memory.
fn max_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

fn run_inner(
    trie: &Trie,
    stores: &StoreManager,
    opts: &Options,
    control: &Control,
    on_error: &mut dyn FnMut(&DetectorError),
) -> Result<Report, LoadError> {
    let started = Instant::now();
    let disk = stores.disk_store().ok_or(LoadError::NoDisk)?;
    if opts.inputs.is_empty() {
        return Err(LoadError::Input("nothing to load".to_string()));
    }
    let pace = RefCell::new(Pace::new(
        opts.max_bytes_per_sec,
        Arc::clone(&control.cancel),
    ));

    // Refused by its id before a line is parsed: every later step would be wasted.
    let (inputs, load_id) = order(&each_file_once(&opts.inputs)?, &pace)?;
    if opts.replace_day.is_none() && disk.was_loaded(load_id) {
        return Err(StoreError::LoadRepeated { load_id }.into());
    }
    let arrival = arrival_head(trie, &inputs, &pace)?;
    if !opts.proceed && arrival.share > opts.max_arrival_share {
        return Err(LoadError::ArrivalShare {
            share: arrival.share,
            head: arrival.head,
            fell_back: arrival.fell_back,
            day: day_name(day_of(crate::store::nanos_of(crate::query::system_time(
                opts.received_at,
            )))),
            limit: opts.max_arrival_share,
        });
    }

    let dir = disk.stage(load_id)?;
    let staged = stage_all(
        trie, disk, opts, &inputs, control, &dir, load_id, &pace, on_error,
    );
    let (files, counts, templates) = match staged {
        Ok(done) => done,
        Err(e) => {
            disk.unstage(load_id);
            return Err(e);
        }
    };
    if control.cancelled() {
        disk.unstage(load_id);
        return Err(LoadError::Cancelled);
    }
    for (id, time_column) in &templates {
        if let Err(e) = stores.open_existing(*id, time_column) {
            disk.unstage(load_id);
            return Err(e.into());
        }
    }
    let bytes: u64 = files
        .iter()
        .map(|f| fs::metadata(&f.path).map_or(0, |m| m.len()))
        .sum();
    let days = files.iter().map(|f| f.day).collect::<BTreeSet<_>>().len() as u64;
    let rows: u64 = files.iter().map(|f| f.index.row_count()).sum();
    // Nothing to publish is refused, not committed: its id would be recorded as a load
    // that happened - and with `--replace-day` it would retire the whole day for nothing,
    // which is what an empty file or a pipe from a command that failed looks like.
    if rows == 0 {
        disk.unstage(load_id);
        return Err(LoadError::Input(match opts.replace_day {
            Some(day) => format!(
                "--replace-day {}: the input holds no rows ({} line(s), {} rejected), and \
                 replacing the day with it would empty the day. Nothing was loaded",
                day_name(day),
                counts.lines,
                counts.rejected
            ),
            None => format!(
                "the input holds no rows ({} line(s), {} rejected). Nothing was loaded",
                counts.lines, counts.rejected
            ),
        }));
    }
    let committed = disk.commit_load(LoadCommit {
        load_id,
        files,
        replace_day: opts.replace_day,
    });
    let Committed { added, replaced } = match committed {
        Ok(c) => c,
        // Past the record - or maybe past it - the staged files are the only copy of the
        // rows, and only the next start can tell which.
        Err(e @ (StoreError::NotMoved { .. } | StoreError::CommitUncertain { .. })) => {
            return Err(e.into())
        }
        Err(e) => {
            disk.unstage(load_id);
            return Err(e.into());
        }
    };
    Ok(Report {
        ok: true,
        load_id: format!("{load_id:016x}"),
        lines: counts.lines,
        blank: counts.blank,
        rejected: counts.rejected,
        rows,
        row_groups: counts.row_groups,
        files: added,
        days,
        bytes,
        replaced,
        arrival,
        seconds: started.elapsed().as_secs_f64(),
        errors_omitted: counts.errors_omitted,
        errors: counts.errors,
    })
}

#[derive(Default)]
struct Counts {
    lines: u64,
    blank: u64,
    rejected: u64,
    row_groups: u64,
    errors: Vec<LineError>,
    errors_omitted: u64,
}

impl Counts {
    fn reject(&mut self, e: &DetectorError, on_error: &mut dyn FnMut(&DetectorError)) {
        self.rejected += 1;
        on_error(e);
        if self.errors.len() < MAX_REPORTED_ERRORS {
            self.errors.push(LineError {
                line: e.line as u64,
                error: e.kind.to_string(),
            });
        } else {
            self.errors_omitted += 1;
        }
    }
}

/// Every line through the trie into a batch, every batch into a staging file, every file
/// sealed: the load, less its commit.
#[allow(clippy::too_many_arguments)]
fn stage_all(
    trie: &Trie,
    disk: &DiskStore,
    opts: &Options,
    inputs: &[Input],
    control: &Control,
    dir: &Path,
    load_id: u64,
    pace: &RefCell<Pace>,
    on_error: &mut dyn FnMut(&DetectorError),
) -> Result<StagedLoad, LoadError> {
    let limits: RowGroupLimits = disk.row_group_limits();
    let max_held = disk.max_buffer_bytes() as usize;
    let threads = opts.threads.clamp(1, max_threads());
    let mut stage = Stage {
        dir,
        max_bytes: disk.segment_max_bytes(),
        open: BTreeMap::new(),
        next: BTreeMap::new(),
        done: Vec::new(),
        writes: 0,
        row_groups: 0,
    };
    let mut counts = Counts::default();
    let mut templates: BTreeMap<u64, String> = BTreeMap::new();
    let mut batches: HashMap<(u64, i64), Batch> = HashMap::new();
    let mut due: Vec<Due> = Vec::new();
    let mut held = 0usize;
    let mut lines = Lines::new(inputs, pace);

    loop {
        if control.cancelled() {
            return Err(LoadError::Cancelled);
        }
        // A round: a chunk per thread, parsed side by side, then applied in line order.
        let mut chunks: Vec<Vec<Got>> = Vec::with_capacity(threads);
        let mut chunk: Vec<Got> = Vec::with_capacity(CHUNK_LINES);
        let mut round_bytes = 0usize;
        let mut chunk_bytes = 0usize;
        let mut ended = false;
        loop {
            // A chunk also closes at its share of the round's bytes, so a round of wide
            // lines that meets the byte budget is still spread across every thread.
            if chunk.len() >= CHUNK_LINES || chunk_bytes >= ROUND_BYTES / threads {
                chunk_bytes = 0;
                chunks.push(std::mem::take(&mut chunk));
                if chunks.len() >= threads {
                    break;
                }
            }
            // A line may be 64MiB: a round of thousands of them is gigabytes before one
            // row reaches a batch the memory limit counts.
            if round_bytes >= ROUND_BYTES {
                break;
            }
            match lines.next_line()? {
                None => {
                    ended = true;
                    break;
                }
                Some(got) => {
                    let bytes = got.1.as_ref().map_or(0, Vec::len);
                    round_bytes += bytes;
                    chunk_bytes += bytes;
                    chunk.push(got);
                }
            }
        }
        if !chunk.is_empty() {
            chunks.push(chunk);
        }
        control.lines.store(lines.line, Ordering::Relaxed);

        for chunk in parse_chunks(chunks, threads)? {
            for (no, parsed) in chunk {
                let (parsed, prepared) = match parsed {
                    Ok(p) => p,
                    Err(e) => {
                        counts.reject(&e, on_error);
                        continue;
                    }
                };
                let placed = match db::place(trie, &parsed, prepared, opts.received_at) {
                    Ok(Some(placed)) => placed,
                    Ok(None) => {
                        let e = DetectorError::new(
                            no as usize,
                            DetectorErrorKind::NotStored {
                                detail: "the template's lock was poisoned".to_string(),
                            },
                        );
                        counts.reject(&e, on_error);
                        continue;
                    }
                    Err(SinkError::Line(kind)) => {
                        counts.reject(&DetectorError::new(no as usize, kind), on_error);
                        continue;
                    }
                    Err(SinkError::Fatal(e)) => return Err(e.into()),
                };
                // The day the live door would file it under: its effective time's.
                let time = effective_ts(&placed.row, &placed.time_column).unwrap_or(0);
                let day = day_of(time);
                if let Some(replace) = opts.replace_day {
                    if day != replace {
                        return Err(LoadError::OutsideDay {
                            line: no,
                            day: day_name(day),
                            replace: day_name(replace),
                        });
                    }
                }
                templates
                    .entry(placed.template_id)
                    .or_insert_with(|| placed.time_column.clone());
                let bytes: usize = placed
                    .row
                    .iter()
                    .map(|(key, value)| key.len() + value_bytes(value))
                    .sum();
                let row_held = placed.row.held_bytes();
                let key = (placed.template_id, day);
                let batch = batches.entry(key).or_default();
                if batch.rows.is_empty() {
                    batch.first = no;
                }
                batch.last = no;
                batch.rows.push(placed.row);
                batch.times.push(time);
                batch.bytes += bytes;
                batch.held += row_held;
                held += row_held;
                if batch.rows.len() >= limits.rows || batch.bytes >= limits.bytes {
                    let batch = batches.remove(&key).unwrap_or_default();
                    held -= batch.held;
                    due.push(Due {
                        template_id: key.0,
                        day: key.1,
                        batch,
                    });
                }
            }
        }

        // Past the memory limit, the largest batches close early - down to three quarters
        // of it, so one burst does not buy a round of small row groups on every chunk after.
        if held > max_held {
            let mut sizes: Vec<((u64, i64), usize)> =
                batches.iter().map(|(k, b)| (*k, b.held)).collect();
            sizes.sort_unstable_by_key(|(_, h)| std::cmp::Reverse(*h));
            for (key, _) in sizes {
                if held <= max_held / 4 * 3 {
                    break;
                }
                if let Some(batch) = batches.remove(&key) {
                    held -= batch.held;
                    due.push(Due {
                        template_id: key.0,
                        day: key.1,
                        batch,
                    });
                }
            }
        }
        write_due(&mut due, &mut stage, load_id, threads)?;
        if ended {
            break;
        }
    }

    // The rest, each a row group of what its template had left on its day - in the order
    // the batches began, so the files come out the same for the same input every time.
    let mut rest: Vec<((u64, i64), Batch)> = batches.into_iter().collect();
    rest.sort_by_key(|(_, b)| b.first);
    due.extend(rest.into_iter().map(|((template_id, day), batch)| Due {
        template_id,
        day,
        batch,
    }));
    write_due(&mut due, &mut stage, load_id, threads)?;
    let (files, row_groups) = stage.finish()?;
    counts.lines = lines.line;
    counts.blank = lines.blank;
    counts.row_groups = row_groups;
    control.lines.store(lines.line, Ordering::Relaxed);
    Ok((files, counts, templates))
}

// ── The command ────────────────────────────────────────────────────────────────────

/// `triedb load`, as the command line gave it.
#[derive(Debug)]
pub struct Args {
    pub inputs: Vec<PathBuf>,
    pub replace_day: Option<String>,
    pub max_mb_per_sec: Option<f64>,
    pub threads: Option<usize>,
    pub received_at: Option<String>,
    pub proceed: bool,
    pub max_arrival_share: Option<f64>,
    pub time_columns: Option<Vec<String>>,
    pub json: bool,
}

/// How long `triedb load` waits for a server that holds the directory's lock to say where
/// it listens: a server writes its address once it can answer, a moment after it starts.
const SERVER_STARTING_WAIT: Duration = Duration::from_secs(30);

/// How often a client asks a server how its load is going.
const POLL: Duration = Duration::from_millis(100);

/// Run `triedb load`: alone, if no process holds the data directory; otherwise as a
/// client of the server that does.
///
/// **Who holds `LOCK` decides where the load runs**, because one process writes the
/// catalog and the manifest. A server is asked to run the load - its trie already has the
/// catalog's every template, and its leaves take the load's rows the moment it commits.
/// With no server, the command takes the lock and opens the directory exactly as a
/// server's start would, recovery included, so an interrupted load is finished or undone
/// here too.
pub fn command(
    args: Args,
    storage: crate::config::Storage,
    schema: crate::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), LoadError> {
    let base = storage
        .base_path
        .clone()
        .ok_or_else(|| LoadError::Input("no data directory".to_string()))?;
    let replace_day = match args.replace_day.as_deref() {
        None => None,
        Some(text) => Some(crate::store::disk::day_of_name(text).ok_or_else(|| {
            LoadError::Input(format!("--replace-day {text}: not a YYYY-MM-DD day"))
        })?),
    };
    let received_at = match args.received_at.as_deref() {
        None => Utc::now(),
        Some(text) => DateTime::parse_from_rfc3339(text)
            .map_err(|e| LoadError::Input(format!("--received-at {text}: {e}")))?
            .with_timezone(&Utc),
    };
    let share = args.max_arrival_share.unwrap_or(DEFAULT_MAX_ARRIVAL_SHARE);
    if !(share.is_finite() && share >= 0.0) {
        return Err(LoadError::Input(format!(
            "--max-arrival-share {share}: not a percentage"
        )));
    }
    let waited = Instant::now();
    loop {
        match crate::lock::DataLock::acquire(&base) {
            Ok(lock) => {
                let opts = Options {
                    inputs: Vec::new(),
                    replace_day,
                    max_bytes_per_sec: args.max_mb_per_sec.map(|mb| mb * 1_000_000.0),
                    threads: args.threads.unwrap_or_else(alone_threads).max(1),
                    received_at,
                    proceed: args.proceed,
                    max_arrival_share: share,
                };
                return alone(lock, &args, opts, &storage, &schema, out);
            }
            Err(crate::lock::LockError::Held { path, holder }) => {
                match crate::lock::holder(&base) {
                    Some(crate::lock::Holder {
                        address: Some(address),
                        token,
                        ..
                    }) => {
                        let token = token.unwrap_or_default();
                        return client(&address, &token, &base, &args, share, out);
                    }
                    Some(crate::lock::Holder { load: true, .. }) => {
                        return Err(crate::lock::LockError::Held { path, holder }.into())
                    }
                    // A server still starting: it writes its address once it can answer.
                    _ if waited.elapsed() < SERVER_STARTING_WAIT => {
                        std::thread::sleep(POLL);
                    }
                    _ => return Err(crate::lock::LockError::Held { path, holder }.into()),
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
}

impl From<crate::lock::LockError> for LoadError {
    fn from(e: crate::lock::LockError) -> Self {
        LoadError::Input(e.to_string())
    }
}

fn print_report(report: &Report, json: bool, out: &mut dyn io::Write) -> Result<(), LoadError> {
    let text = if json {
        serde_json::to_string(report).unwrap_or_default()
    } else {
        report.summary()
    };
    writeln!(out, "{text}").map_err(|e| crate::error::CliError::Write(e).into())
}

/// The load in this process, holding the directory's lock.
fn alone(
    mut lock: crate::lock::DataLock,
    args: &Args,
    mut opts: Options,
    storage: &crate::config::Storage,
    schema: &crate::config::Schema,
    out: &mut dyn io::Write,
) -> Result<(), LoadError> {
    let base = storage.base_path.clone().unwrap_or_default();
    let _ = lock.announce(&crate::lock::Holder::load());
    crate::store::disk::refuse_other_versions(&base)?;
    let (trie, stores) =
        crate::serve::open_durable(storage, schema).map_err(crate::error::CliError::from)?;
    // Standard input is spooled inside `staging/`, which the next start deletes: a load
    // that dies leaves nothing behind that anything names.
    let staging = base.join(crate::store::disk::STAGING_DIR);
    let spool = staging.join(format!("stdin-{}.spool", std::process::id()));
    if args.inputs.iter().any(|a| a.as_os_str() == "-") {
        fs::create_dir_all(&staging).map_err(|source| LoadError::Read {
            path: staging.clone(),
            source,
        })?;
    }
    let result = resolve(&args.inputs, Some(&spool)).and_then(|inputs| {
        opts.inputs = inputs;
        run(&trie, &stores, &opts, &Control::default(), &mut |e| {
            crate::error::report(e)
        })
    });
    let _ = fs::remove_file(&spool);
    print_report(&result?, args.json, out)
}

/// The load run by the server that holds the directory: asked over HTTP, then polled.
///
/// The inputs are resolved here - the command line's directories, and its standard input,
/// spooled to a temporary file - and sent as absolute paths: the server reads them itself,
/// on the same machine, since it holds a lock on a directory here.
fn client(
    address: &str,
    token: &str,
    base: &Path,
    args: &Args,
    share: f64,
    out: &mut dyn io::Write,
) -> Result<(), LoadError> {
    if args.time_columns.is_some() {
        eprintln!(
            "triedb: WARN --time-columns is ignored: the server at {address} holds this data \
             directory, and a template's time source is chosen by the server's own list"
        );
    }
    // Spooled inside the data directory's `staging/`, which the server can read because it
    // owns it - a temporary directory of the client's may be one the server cannot see - and
    // which the server's next start clears if this client is killed before it removes it.
    let staging = base.join(crate::store::disk::STAGING_DIR);
    let spool = staging.join(format!(
        "stdin-{}-{}.spool",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    if args.inputs.iter().any(|a| a.as_os_str() == "-") {
        fs::create_dir_all(&staging).map_err(|source| LoadError::Read {
            path: staging.clone(),
            source,
        })?;
    }
    let result = client_run(address, token, base, args, share, &spool);
    let _ = fs::remove_file(&spool);
    let report = result?;
    for e in &report.errors {
        eprintln!("triedb: line {}: {}", e.line, e.error);
    }
    if report.errors_omitted > 0 {
        eprintln!(
            "triedb: {} more rejected line(s) not listed",
            report.errors_omitted
        );
    }
    print_report(&report, args.json, out)
}

/// A server's answer as JSON, or what it said instead, with its status - a proxy's error
/// page or a body-size refusal is not JSON, and "not JSON" alone would hide which it was.
fn json_of(status: u16, answer: &[u8]) -> Result<serde_json::Value, String> {
    serde_json::from_slice(answer).map_err(|_| {
        let text = String::from_utf8_lossy(&answer[..answer.len().min(300)]);
        format!("answered {status}: {}", text.trim())
    })
}

fn client_run(
    address: &str,
    token: &str,
    base: &Path,
    args: &Args,
    share: f64,
    spool: &Path,
) -> Result<Report, LoadError> {
    let server = |detail: String| LoadError::Server {
        address: address.to_string(),
        detail,
    };
    let inputs = resolve(&args.inputs, Some(spool))?;
    // JSON carries text: a path that is not UTF-8 cannot be sent, and is named rather than
    // mangled into another file's name.
    let text = |path: &Path| {
        path.to_str().map(str::to_string).ok_or_else(|| {
            LoadError::Input(format!(
                "{}: not UTF-8, so it cannot be sent to the server holding the data \
                 directory; rename it, or stop the server and load alone",
                path.display()
            ))
        })
    };
    let mut sent = Vec::with_capacity(inputs.len());
    for input in &inputs {
        sent.push(serde_json::json!({"path": text(&input.path)?, "name": input.name}));
    }
    let data_dir = fs::canonicalize(base).map_err(|source| LoadError::Read {
        path: base.to_path_buf(),
        source,
    })?;
    let body = serde_json::json!({
        "inputs": sent,
        "replace_day": args.replace_day,
        "max_mb_per_sec": args.max_mb_per_sec,
        "threads": args.threads,
        "received_at": args.received_at,
        "proceed": args.proceed,
        "max_arrival_share": share,
        "data_dir": text(&data_dir)?,
    });
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let shown = [("x-triedb-load-token", token)];
    let (status, answer) = crate::http::request(
        address,
        "POST",
        "/load",
        &shown,
        Some(("application/json", &bytes)),
        Duration::from_secs(60),
    )
    .map_err(|e| server(e.to_string()))?;
    let answer = json_of(status, &answer).map_err(server)?;
    if status != 202 {
        return Err(LoadError::Remote(
            answer["error"]
                .as_str()
                .map_or_else(|| format!("answered {status}"), str::to_string),
        ));
    }
    let job = answer["job"]
        .as_u64()
        .ok_or_else(|| server("no job number in its answer".to_string()))?;
    loop {
        std::thread::sleep(POLL);
        let (status, answer) = crate::http::request(
            address,
            "GET",
            &format!("/load/{job}"),
            &shown,
            None,
            Duration::from_secs(60),
        )
        .map_err(|e| {
            server(format!(
                "{e} while load job {job} was running; if the server stopped, the load either \
                 committed before it did or left nothing behind"
            ))
        })?;
        if status != 200 {
            return Err(server(format!("answered {status} about load job {job}")));
        }
        let answer = json_of(status, &answer).map_err(server)?;
        match answer["state"].as_str() {
            Some("running") => continue,
            Some("done") => {
                return serde_json::from_value(answer["report"].clone())
                    .map_err(|e| server(format!("a report this build cannot read ({e})")))
            }
            Some("failed") => {
                return Err(LoadError::Remote(
                    answer["error"].as_str().unwrap_or("failed").to_string(),
                ))
            }
            other => return Err(server(format!("load job {job} in state {other:?}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "triedb-load-{name}-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("mkdir");
            Self(dir)
        }

        fn write(&self, rel: &str, bytes: &[u8]) -> PathBuf {
            let path = self.0.join(rel);
            fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            fs::write(&path, bytes).expect("write");
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const LINES: &[u8] = b"{\"a\":1}\n{\"a\":2}\n";

    /// The id, against values worked out apart from this code: FNV-1a 64 over the name, a
    /// zero byte, the length and the content's own FNV-1a 64, input by input in name order.
    #[test]
    fn the_load_id_is_the_documented_hash_in_name_order() {
        let scratch = Scratch::new("id");
        let x = scratch.write("one/x.ndjson", LINES);
        assert_eq!(
            load_id(&resolve(std::slice::from_ref(&x), None).expect("resolve")).expect("id"),
            0xf4ac_ae0a_82d2_f931
        );
        let a = scratch.write("two/a.ndjson", b"{\"b\":1}\n");
        let want = 0xa7c9_adc7_d15f_1550;
        assert_eq!(
            load_id(&resolve(&[x.clone(), a.clone()], None).expect("resolve")).expect("id"),
            want
        );
        assert_eq!(
            load_id(&resolve(&[a, x], None).expect("resolve")).expect("id"),
            want,
            "the order given changed the id"
        );
    }

    /// The same name and the same bytes after decompression are the same load, whatever
    /// compressed them and wherever they sit.
    #[test]
    fn a_recompressed_file_elsewhere_is_the_same_load() {
        let scratch = Scratch::new("recompressed");
        let plain = scratch.write("plain/x.ndjson", LINES);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(9));
        gz.write_all(LINES).expect("gzip");
        let gz = scratch.write("gz/x.ndjson", &gz.finish().expect("gzip"));
        let zst = scratch.write(
            "zst/x.ndjson",
            &zstd::stream::encode_all(LINES, 3).expect("zstd"),
        );
        let id = |p: &PathBuf| {
            load_id(&resolve(std::slice::from_ref(p), None).expect("resolve")).expect("id")
        };
        assert_eq!(id(&plain), id(&gz));
        assert_eq!(id(&plain), id(&zst));
        let renamed = scratch.write("plain/y.ndjson", LINES);
        assert_ne!(id(&plain), id(&renamed), "the name is part of the id");
    }

    /// A directory is every file under it, at any depth, but not the ones named with a dot.
    #[test]
    fn a_directory_is_its_files_in_name_order_without_hidden_ones() {
        let scratch = Scratch::new("dir");
        scratch.write("in/b.ndjson", LINES);
        scratch.write("in/deeper/a.ndjson", LINES);
        scratch.write("in/.partial", LINES);
        let names: Vec<String> = resolve(&[scratch.0.join("in")], None)
            .expect("resolve")
            .into_iter()
            .map(|i| i.name)
            .collect();
        assert_eq!(names, vec!["a.ndjson", "b.ndjson"]);
        assert!(resolve(&[scratch.0.join("missing")], None).is_err());
    }
}
