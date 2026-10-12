//! The bulk door: a closed day of lines - or six months of them - loaded whole. What
//! `kibbr load` runs alone and what a server runs for it (`POST /load`); the command
//! itself - who runs the load, and what it prints - is the binary's.
//!
//! ```text
//!  inputs ─► load id ─► arrival check on the head ─► parse, on the load's own threads
//!         ─► per template and UTC day, batches in memory (invisible to queries)
//!         ─► row groups ─► staging/<load id>/<day>/NNNNNN.col, sealed, finished
//!         ─► arrival check on every row ─► one manifest record
//!         ─► data/<day>/seg_0_NNNNNN.col ─► the leaves
//! ```
//!
//! - **One write path.** A line goes through `db::place`, which `/ingest` goes through
//!   too: the same template, the same types, the same catalog records, the same stored
//!   row with the same arrival stamp. What differs is only where the row goes - a batch
//!   of the load's, not a leaf's buffer - so no query sees a row of a load before the
//!   whole load is committed.
//! - **Full row groups.** The loader sees all of its input, so a template's rows of a day
//!   close at 8,192 rows or about 8MB of column data, the thresholds every writer uses -
//!   never at a timeout. A hard bound on the rows held, `--load-buffer-bytes`, writes the
//!   largest batches early: at 10,000 templates a batch each is more memory than the
//!   machine has, and a load inside a server shares the machine with the live door.
//! - **Every row's time checked.** A row with no time of its own is filed under its
//!   arrival day, which for an old file is the wrong one. The loader counts them all as it
//!   stages, and stops before the commit if too many would be. A stop there leaves the
//!   load's new templates made, each on arrival time for good, so the head of the input is
//!   looked at first: a file untimed from its first line stops with nothing made.
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
//! twice. Kibbr keeps no row identity to catch that later; one source per day, and
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

use kibbr_core::error::{DetectorError, DetectorErrorKind};
use kibbr_core::flatten::{parse_line, ParsedLog};
use kibbr_core::lines::{
    read_line, skip_to_eol, strip_bom, strip_eol, Line, Resync, MAX_LINE_BYTES,
};
use kibbr_core::types::TimeSource;
use kibbr_storage::db::{self, Prepared, SinkError};
use kibbr_storage::store::disk::{day_name, Committed, LoadCommit, StagedFile};
use kibbr_storage::store::v3::build::{self, schema_of, Level};
use kibbr_storage::store::v3::file::Writer;
use kibbr_storage::store::v3::{day_of, Source};
use kibbr_storage::store::{
    effective_ts, ts_of, value_bytes, DiskStore, Row, RowGroupLimits, StoreError, StoreManager,
    RECEIVED_AT_COLUMN,
};
use kibbr_storage::trie::Trie;

/// How much of the input the early arrival-time check reads before anything is staged:
/// enough rows that a share of 1% is a hundred of them, few enough that it costs nothing
/// beside the load. It is an early exit only - the count that decides is over every row.
pub const HEAD_ROWS: usize = 10_000;

/// Lines the head check reads at most, rows or not: a rejected line is no row, and an input
/// opening on gigabytes of text that is not JSON would otherwise be parsed whole, unpaced,
/// before the paced pass read it again. The whole-input count still decides.
const HEAD_LINES: usize = 10 * HEAD_ROWS;

/// The share of the rows, in percent, that may fall back to arrival time before a load
/// stops. Loading old files, arrival time is the wrong day by construction - today - so a
/// row that takes it is misfiled rather than late; a load that would misfile more than
/// one row in a hundred is more likely missing a `time_columns` name than meant.
pub const DEFAULT_MAX_ARRIVAL_SHARE: f64 = 1.0;

/// The memory a load holds its rows in unless told otherwise (`--load-buffer-bytes`): open
/// batches and closed ones not yet written, together, as `Row::held_bytes` counts them.
///
/// **Its own bound, well under the live door's 512MiB.** Held to the live door's limit, a
/// load inside a busy server cost about half a core but raised its resident size by 0.5
/// to 0.9GB, which on an 8GB machine was a swap storm: 40 to 57s with nothing ingested and
/// `/ingest` answering 503. `--max-mb-per-sec` and `--threads` bound what a load costs in
/// CPU; this bounds its memory. 64MiB is about 28,000 farm rows (2.4KB held each): a
/// template that dominates the input still closes row groups of thousands of rows, and a
/// load of thousands of templates writes its largest batches early, as smaller row
/// groups. That is the trade taken for a load that cannot push the live door out of memory.
pub const DEFAULT_LOAD_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// Lines per parse job: large enough that handing one to a thread costs nothing beside
/// parsing it.
const CHUNK_LINES: usize = 2_048;

/// A round's raw lines are at most this fraction of the load's buffer - and one line at
/// least, whatever its size. A round is read whole, then parsed whole, before its first
/// row is placed, and none of that is counted as held: a farm line of 488 bytes parses
/// into about 9.6KB of line and row, twenty times its size, so a round of even a quarter
/// of the bound would parse into five times the bound. At a sixty-fourth the default's
/// rounds are 1MiB of lines, about 20MB parsed.
const ROUND_SHARE: usize = 64;

/// Templates a refusal names; past this it counts the rest. A load of old files missing a
/// time column can leave thousands of templates on arrival time, and a message that lists
/// them all is not read.
const MAX_NAMED: usize = 10;

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
    /// Decompressed input bytes a second the pass that parses and encodes may read - the
    /// pass whose CPU the live door shares. The id's digest and the head check are not held
    /// to it; see `Pace`.
    pub max_bytes_per_sec: Option<f64>,
    pub threads: usize,
    /// The rows the load may hold in memory - open batches and closed ones not yet written,
    /// as `Row::held_bytes` counts them - before it writes the largest early. At least 1.
    pub load_buffer_bytes: usize,
    /// The arrival time of every row with no time of its own, and every template's
    /// `created_at`: one instant for the whole load, as one `/ingest` body has one.
    pub received_at: DateTime<Utc>,
    /// Load even when more than `max_arrival_share` of the rows - of the head, or of them
    /// all - fall back to arrival time.
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

/// Rows with no time of their own, which took the load's arrival time.
///
/// The new fields default when absent, so a client reads the report of a server a build
/// older than itself.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub struct Arrival {
    /// Rows the early check on the head read, before anything was staged.
    pub head: u64,
    /// Rows checked: every row the load stored.
    #[serde(default)]
    pub rows: u64,
    /// Of `rows`, those stored at their arrival time.
    pub fell_back: u64,
    /// `fell_back` of `rows`, in percent.
    pub share: f64,
    /// The UTC day they are filed under, YYYY-MM-DD.
    #[serde(default)]
    pub day: String,
    /// The `--max-arrival-share` the load was held to, in percent.
    #[serde(default)]
    pub limit: f64,
    /// Past `limit` - over every row or over the head - and loaded because of `--proceed`.
    #[serde(default)]
    pub proceeded: bool,
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
        use kibbr_core::fmt::commas;
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
        // Always, zero included: the line an operator reads is where a load filed under
        // the wrong day has to show, and a count said only when it is large is one they
        // learn to expect absent.
        let a = &self.arrival;
        if a.day.is_empty() {
            // A server a build older than this client: its report counted the head only.
            out.push_str(&format!(
                "; {} of the first {} rows ({:.1}%) had no time of their own",
                commas(a.fell_back),
                commas(a.head),
                a.share
            ));
            return out;
        }
        out.push_str(&format!(
            "; {} of {} rows ({:.1}%) had no time of their own and took their arrival time, \
             {} - {}",
            commas(a.fell_back),
            commas(a.rows),
            a.share,
            a.day,
            if a.share > a.limit {
                format!(
                    "past --max-arrival-share {}%, loaded because of --proceed",
                    a.limit
                )
            } else if a.proceeded {
                format!(
                    "within --max-arrival-share {}% over every row, though the head was past \
                     it; loaded because of --proceed",
                    a.limit
                )
            } else {
                format!("within --max-arrival-share {}%", a.limit)
            }
        ));
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
    Format(#[from] kibbr_storage::store::v3::FormatError),
    /// `counted` says which rows the share is of - the head's, or every row - and `advice`
    /// differs with it: see `Checked`.
    #[error(
        "{share:.1}% of {counted} ({fell_back}) have no time of their own and would be \
         stored at their arrival time, {day} - more than the {limit}% --max-arrival-share \
         allows. Nothing was loaded. On arrival time: {templates}. {advice}"
    )]
    ArrivalShare {
        share: f64,
        counted: String,
        fell_back: u64,
        day: String,
        limit: f64,
        templates: String,
        advice: &'static str,
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
    /// What ended the read of the input's lines: the read loop's own error, printed as
    /// the sentence it wrote.
    #[error(transparent)]
    Lines(#[from] db::ReadError),
    /// The data directory could not be opened as a server's start opens it.
    #[error(transparent)]
    Open(#[from] crate::Error),
    /// What whoever ran the load ended it with, around the load itself - the command's
    /// own output it could not write, a server it could not ask. Printed as itself.
    #[error(transparent)]
    Caller(Box<dyn std::error::Error + Send + Sync>),
}

impl From<kibbr_storage::lock::LockError> for LoadError {
    fn from(e: kibbr_storage::lock::LockError) -> Self {
        LoadError::Input(e.to_string())
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
                 once; pipe it to `kibbr load -` instead",
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

/// The pace `--max-mb-per-sec` holds a load to: the bytes the staging pass has read,
/// decompressed, never ahead of the rate times the time since that pass began.
///
/// On the decompressed bytes, because they are what the parse and the encode cost - the
/// CPU the live door shares - where a compressed file's bytes on disk understate it tenfold.
///
/// **Only the pass that parses and encodes is paced.** The id's digest and the head check
/// read the input before it, and cost a decompress, a hash and ten thousand rows' parse -
/// little beside the pass that parses every line. Charged to one pace with that pass, as
/// they once were, they spent the rate on bytes read twice, and a load delivered about
/// 0.47 of what it was asked for: 40 MB/s gave 15.9 to 17.0, 20 gave 9.3, 10 gave 4.8, 5
/// gave 2.4 and 1 gave 0.44, over a 70MB farm day, zstd or plain, four threads or one. Paced
/// alone, a 98MB farm day at 10 MB/s takes 10.3s (9.5 MB/s) and at 5 takes 20.1s (4.9):
/// the rest is the reads before it and the last row groups and the commit after.
///
/// **The time the reads before it took is credit, up to `PACE_CREDIT`.** The rate is what a
/// load reads over its whole run - what its report's seconds divide - and those reads took
/// 0.5s of that day: started at the staging pass, the clock left 20 MB/s delivering 17.9.
/// Capped, because the credit is a burst at full speed: the id's digest took 0.3s of a
/// 98MB day, so tens of seconds of six months', and the pass would spend them all on the
/// cores the live door shares before the rate held it.
///
/// **Every read checks for a stop**, unpaced ones included, and a wait is slept in short
/// slices that check too: a load paced to a trickle must not hold up a server's shutdown -
/// and its final flush of the live door's rows - for as long as its next megabyte is due.
struct Pace {
    bytes_per_sec: Option<f64>,
    start: Instant,
    read: u64,
    cancel: Arc<AtomicBool>,
}

/// How long a paced wait sleeps before it looks for a stop again.
const PACE_SLICE: Duration = Duration::from_millis(50);

/// The most of the reads before the staging pass its pace counts as already spent: see
/// `Pace`. About what they take for a day of logs.
const PACE_CREDIT: Duration = Duration::from_secs(1);

impl Pace {
    fn new(bytes_per_sec: Option<f64>, cancel: Arc<AtomicBool>) -> Self {
        Self::since(Instant::now(), bytes_per_sec, cancel)
    }

    /// A pace whose clock started at `start`.
    fn since(start: Instant, bytes_per_sec: Option<f64>, cancel: Arc<AtomicBool>) -> Self {
        Self {
            bytes_per_sec: bytes_per_sec.filter(|r| *r > 0.0),
            start,
            read: 0,
            cancel,
        }
    }

    /// Held to no rate, but stopped when told to: the reads before the staging pass.
    fn unpaced(cancel: Arc<AtomicBool>) -> Self {
        Self::new(None, cancel)
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
    let pace = RefCell::new(Pace::unpaced(Arc::default()));
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
                            return Err(db::ReadError::NotNewlineDelimited {
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

/// How a pass reads its input: a round of chunks at a time, one chunk per thread, parsed
/// side by side and then applied in line order.
struct Rounds {
    threads: usize,
    /// Raw bytes a round may hold past its first line: see `ROUND_SHARE`.
    max_bytes: usize,
}

impl Rounds {
    fn of(opts: &Options) -> Self {
        Self {
            threads: opts.threads.clamp(1, max_threads()),
            max_bytes: (opts.load_buffer_bytes / ROUND_SHARE).max(1),
        }
    }
}

/// The next round of lines, and whether the input ended in it.
fn read_round(
    lines: &mut Lines<'_>,
    threads: usize,
    max_bytes: usize,
) -> Result<(Vec<Vec<Got>>, bool), LoadError> {
    let mut chunks: Vec<Vec<Got>> = Vec::with_capacity(threads);
    let mut chunk: Vec<Got> = Vec::new();
    let mut round_bytes = 0usize;
    let mut chunk_bytes = 0usize;
    let mut ended = false;
    loop {
        // A chunk also closes at its share of the round's bytes, so a round of wide lines
        // that meets the byte budget is still spread across every thread. Never empty:
        // under a bound so small a thread's share rounds to nothing, that would be a round
        // of empty chunks, and no line read.
        if !chunk.is_empty() && (chunk.len() >= CHUNK_LINES || chunk_bytes >= max_bytes / threads) {
            chunk_bytes = 0;
            chunks.push(std::mem::take(&mut chunk));
            if chunks.len() >= threads {
                break;
            }
        }
        // A line may be 64MiB: a round of thousands of them is gigabytes before one row
        // reaches a batch the bound counts. A round starts at no bytes, so it takes its
        // first line whatever that line's size.
        if round_bytes >= max_bytes {
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
    Ok((chunks, ended))
}

// ── The arrival-time check ─────────────────────────────────────────────────────────

/// Whose rows fell back to arrival time: a template by id, or - in the head check, which
/// makes nothing - one not made yet, by the line it was first met on and the keys that
/// line could have taken its time from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Who {
    Template(u64),
    New { line: u64, keys: String },
}

/// Rows checked for a time of their own, and those that had none, by template.
#[derive(Debug, Default)]
struct Tally {
    rows: u64,
    fell_back: u64,
    by: HashMap<Who, u64>,
}

/// Which check stopped a load: it decides what the refusal can honestly advise.
#[derive(Debug, Clone, Copy)]
enum Checked {
    /// The early one, before anything was staged. It made no template, so a time key added
    /// to the time columns reaches every new one.
    Head,
    /// The one over every row, after staging. Staging put each line through the trie, so
    /// the load's new templates are made - on arrival time, for good.
    Whole,
}

impl Tally {
    fn count(&mut self, fell_back: bool, who: impl FnOnce() -> Who) {
        self.rows += 1;
        if fell_back {
            self.fell_back += 1;
            *self.by.entry(who()).or_default() += 1;
        }
    }

    /// In percent.
    fn share(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.fell_back as f64 * 100.0 / self.rows as f64
        }
    }

    /// The templates that fell back, most rows first, each with its Datetime-typed keys -
    /// what an operator would add to the time columns - and at most `MAX_NAMED` of them.
    fn named(&self, trie: &Trie) -> String {
        let mut by: Vec<(&Who, u64)> = self.by.iter().map(|(who, n)| (who, *n)).collect();
        by.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        let wanted: BTreeSet<u64> = by
            .iter()
            .take(MAX_NAMED)
            .filter_map(|(who, _)| match who {
                Who::Template(id) => Some(*id),
                Who::New { .. } => None,
            })
            .collect();
        // A template on arrival time, by the keys an operator could name as its time; one
        // with a time column, by that column - its rows fell back because the column held
        // no instant, and no time key would change that.
        let mut said: HashMap<u64, String> = HashMap::new();
        if !wanted.is_empty() {
            for leaf in trie.all_leaves() {
                let Ok(slot) = leaf.leaf_meta.read() else {
                    continue;
                };
                if let Some(meta) = slot.as_ref().filter(|m| wanted.contains(&m.template_id)) {
                    said.insert(
                        meta.template_id,
                        match &meta.time_source {
                            TimeSource::Arrival => format!(
                                "Datetime-typed keys: {}",
                                kibbr_storage::trie::datetime_keys(
                                    meta.columns.iter().map(|c| (c.name.as_str(), c.col_type)),
                                )
                            ),
                            TimeSource::Column(name) => {
                                format!("its time column `{name}` held no time")
                            }
                        },
                    );
                }
            }
        }
        let mut parts: Vec<String> = by
            .iter()
            .take(MAX_NAMED)
            .map(|(who, n)| match who {
                Who::Template(id) => format!(
                    "template {id} ({n} rows; {})",
                    said.get(id)
                        .map_or("Datetime-typed keys: unknown", String::as_str)
                ),
                Who::New { line, keys } => format!(
                    "a new template first on line {line} ({n} rows; Datetime-typed keys: {keys})"
                ),
            })
            .collect();
        if by.len() > MAX_NAMED {
            parts.push(format!("and {} more", by.len() - MAX_NAMED));
        }
        parts.join(", ")
    }

    fn refusal(&self, checked: Checked, trie: &Trie, day: &str, limit: f64) -> LoadError {
        let (counted, advice) = match checked {
            Checked::Head => (
                format!("the first {} rows", self.rows),
                "If their templates are new, add their time key to the time columns \
                 (--time-columns, or KIBBR__SCHEMA__TIME_COLUMNS for a server); a template \
                 that exists already keeps the time source it was made with. Or pass \
                 --proceed to load them at their arrival time",
            ),
            Checked::Whole => (
                format!("the {} rows", self.rows),
                "Their templates exist now - a load makes its new ones, and widens a column's \
                 type, as it reads, before it can count; those stay - and a template keeps \
                 the time source it was made with, so a time key added to the time columns \
                 reaches only templates still to come. Or pass --proceed to load them at \
                 their arrival time",
            ),
        };
        LoadError::ArrivalShare {
            share: self.share(),
            counted,
            fell_back: self.fell_back,
            day: day.to_string(),
            limit,
            templates: self.named(trie),
            advice,
        }
    }
}

/// The early look at the head of the input: how much of it would be stored at its arrival
/// time. An exit before anything is staged, for the file untimed from its first line; the
/// count that decides is over every row, as they are staged.
///
/// **Nothing is created.** A line of a template the catalog knows takes that template's
/// time source; a line of one it does not takes the source its first line would choose -
/// the same rule `Trie::ingest` applies - remembered here for the head's later lines of it.
/// Creating the templates for real would make the check's answer permanent: a template's
/// time source is chosen once, and one created while a load is refused would keep arrival
/// time after the operator added its time key and ran the load again.
///
/// **Parsed on the load's threads**, in the staging pass's rounds. On one thread it took
/// 0.66s of a 98MB farm day loaded at 10 MB/s: unpaced time the report's seconds count and
/// the rate does not, which alone put the load 7% under the rate it was asked for.
fn arrival_head(
    trie: &Trie,
    inputs: &[Input],
    pace: &RefCell<Pace>,
    rounds: &Rounds,
) -> Result<Tally, LoadError> {
    let mut lines = Lines::new(inputs, pace);
    let mut unseen: HashMap<Vec<String>, (TimeSource, Who)> = HashMap::new();
    let mut tally = Tally::default();
    let mut seen = 0usize;
    let done = |tally: &Tally, seen: usize| tally.rows as usize >= HEAD_ROWS || seen >= HEAD_LINES;
    loop {
        let (chunks, ended) = read_round(&mut lines, rounds.threads, rounds.max_bytes)?;
        for chunk in parse_chunks(chunks, rounds.threads)? {
            for (no, parsed) in chunk {
                if done(&tally, seen) {
                    return Ok(tally);
                }
                seen += 1;
                if let Ok((parsed, prepared)) = parsed {
                    head_row(trie, &mut unseen, &mut tally, no, &parsed, &prepared.row);
                }
            }
        }
        if ended || done(&tally, seen) {
            return Ok(tally);
        }
    }
}

/// One row of the head: counted against its template's time source, or - for a template
/// not made yet - the one its first line would choose.
fn head_row(
    trie: &Trie,
    unseen: &mut HashMap<Vec<String>, (TimeSource, Who)>,
    tally: &mut Tally,
    no: u64,
    parsed: &ParsedLog,
    row: &Row,
) {
    let tokens = parsed.walk_tokens();
    let borrowed: Vec<&str> = tokens.iter().map(String::as_str).collect();
    let known = trie.walk(&borrowed).and_then(|walked| {
        walked.node.leaf_meta.read().ok().and_then(|slot| {
            slot.as_ref()
                .map(|meta| (meta.time_source.clone(), Who::Template(meta.template_id)))
        })
    });
    let (source, who) = match known {
        Some(known) => known,
        None => unseen
            .entry(tokens)
            .or_insert_with(|| {
                let source =
                    TimeSource::choose(trie.time_columns(), |name| parsed.column_type(name));
                let keys = kibbr_storage::trie::datetime_keys(
                    parsed
                        .keys
                        .iter()
                        .map(|key| (key.as_str(), parsed.column_type(key))),
                );
                (source, Who::New { line: no, keys })
            })
            .clone(),
    };
    let fell_back = ts_of(row, source.column()).is_none();
    tally.count(fell_back, || who);
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
                kibbr_storage::frame::sync_dir(self.dir).map_err(Self::io(self.dir))?;
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
            // A load writes finished files, read for months: the slower, smaller level.
            Level::Finished,
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

/// Every row a load holds: its open batches, and the batches closed and waiting to be
/// encoded and written.
///
/// **Both count against the bound.** A batch closed at the row group thresholds is in
/// memory until it is written, and one round can close many: counted only while open, as
/// they once were, a round's worth of full batches sat outside a bound that said nothing
/// was held.
#[derive(Default)]
struct Held {
    open: HashMap<(u64, i64), Batch>,
    due: Vec<Due>,
    /// `Row::held_bytes` of the rows in `open`, and of those in `due`.
    open_bytes: usize,
    due_bytes: usize,
    /// The most held once a row's place was paid for - see `bound`.
    peak: usize,
}

impl Held {
    /// One row into its template-day's batch, which is closed once it is full.
    fn push(&mut self, key: (u64, i64), no: u64, row: Row, time: i64, limits: &RowGroupLimits) {
        let bytes: usize = row
            .iter()
            .map(|(key, value)| key.len() + value_bytes(value))
            .sum();
        let held = row.held_bytes();
        let batch = self.open.entry(key).or_default();
        if batch.rows.is_empty() {
            batch.first = no;
        }
        batch.last = no;
        batch.rows.push(row);
        batch.times.push(time);
        batch.bytes += bytes;
        batch.held += held;
        self.open_bytes += held;
        if batch.rows.len() >= limits.rows || batch.bytes >= limits.bytes {
            self.close(key);
        }
    }

    fn close(&mut self, key: (u64, i64)) {
        if let Some(batch) = self.open.remove(&key) {
            self.open_bytes -= batch.held;
            self.due_bytes += batch.held;
            self.due.push(Due {
                template_id: key.0,
                day: key.1,
                batch,
            });
        }
    }

    /// Every closed batch encoded and written, in the order they closed.
    fn write(
        &mut self,
        stage: &mut Stage<'_>,
        load_id: u64,
        threads: usize,
    ) -> Result<(), LoadError> {
        write_due(&mut self.due, stage, load_id, threads)?;
        self.due_bytes = 0;
        Ok(())
    }

    /// Back within `bound` if the last row took it past: the closed batches written, and if
    /// that is not enough, the largest open ones closed and written too.
    ///
    /// Called after every row, so the bound is passed by one row at most, for as long as
    /// the writes take. Once a round, as it was, a round of rows could close more full
    /// batches than the bound holds before anything looked.
    ///
    /// **Down to three quarters of it**, not to just under it: at the bound, closing only
    /// what the last row passed it by would close a batch on nearly every row after, and one
    /// burst would buy a long run of tiny row groups.
    fn bound(
        &mut self,
        bound: usize,
        stage: &mut Stage<'_>,
        load_id: u64,
        threads: usize,
    ) -> Result<(), LoadError> {
        if self.open_bytes + self.due_bytes > bound {
            self.write(stage, load_id, threads)?;
        }
        if self.open_bytes > bound {
            // Ties broken by the first line, so the same input closes the same batches -
            // and writes the same files - every time, whatever order the map keeps.
            let mut sizes: Vec<((u64, i64), usize, u64)> = self
                .open
                .iter()
                .map(|(key, b)| (*key, b.held, b.first))
                .collect();
            sizes.sort_unstable_by_key(|(_, held, first)| (std::cmp::Reverse(*held), *first));
            for (key, ..) in sizes {
                if self.open_bytes <= bound / 4 * 3 {
                    break;
                }
                self.close(key);
            }
            self.write(stage, load_id, threads)?;
        }
        // The tests recount it from the batches rather than trust the sums they check.
        #[cfg(test)]
        let now = self.open.values().map(|b| b.held).sum::<usize>()
            + self.due.iter().map(|d| d.batch.held).sum::<usize>();
        #[cfg(not(test))]
        let now = self.open_bytes + self.due_bytes;
        self.peak = self.peak.max(now);
        Ok(())
    }

    /// The rest, each a row group of what its template had left on its day - in the order
    /// the batches began, so the files come out the same for the same input every time.
    fn finish(
        &mut self,
        stage: &mut Stage<'_>,
        load_id: u64,
        threads: usize,
    ) -> Result<(), LoadError> {
        let mut rest: Vec<((u64, i64), Batch)> = self.open.drain().collect();
        rest.sort_by_key(|(_, b)| b.first);
        for ((template_id, day), batch) in rest {
            self.open_bytes -= batch.held;
            self.due_bytes += batch.held;
            self.due.push(Due {
                template_id,
                day,
                batch,
            });
        }
        self.write(stage, load_id, threads)
    }
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
    run_measured(trie, stores, opts, control, on_error).map(|(report, _)| report)
}

/// What a load measured of itself that its report does not carry: what the tests hold it to.
#[derive(Debug, Default, Clone, Copy)]
#[cfg_attr(not(test), allow(dead_code))]
struct Measured {
    /// The most rows held at once, open batches and closed ones not yet written together.
    peak_held: usize,
    /// Decompressed bytes the pace that holds `--max-mb-per-sec` was charged.
    paced: u64,
}

fn run_measured(
    trie: &Trie,
    stores: &StoreManager,
    opts: &Options,
    control: &Control,
    on_error: &mut dyn FnMut(&DetectorError),
) -> Result<(Report, Measured), LoadError> {
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
) -> Result<(Report, Measured), LoadError> {
    let started = Instant::now();
    let disk = stores.disk_store().ok_or(LoadError::NoDisk)?;
    if opts.inputs.is_empty() {
        return Err(LoadError::Input("nothing to load".to_string()));
    }
    // The reads before the staging pass: held to no rate - see `Pace` - but stopped.
    let unpaced = RefCell::new(Pace::unpaced(Arc::clone(&control.cancel)));

    // Refused by its id before a line is parsed: every later step would be wasted.
    let (inputs, load_id) = order(&each_file_once(&opts.inputs)?, &unpaced)?;
    if opts.replace_day.is_none() && disk.was_loaded(load_id) {
        return Err(StoreError::LoadRepeated { load_id }.into());
    }
    let limit = opts.max_arrival_share;
    let day = day_name(day_of(kibbr_storage::store::nanos_of(
        kibbr_storage::store::system_time(opts.received_at),
    )));
    let head = arrival_head(trie, &inputs, &unpaced, &Rounds::of(opts))?;
    if !opts.proceed && head.share() > limit {
        return Err(head.refusal(Checked::Head, trie, &day, limit));
    }

    let dir = disk.stage(load_id)?;
    let staged = stage_all(
        trie, disk, opts, &inputs, control, &dir, load_id, started, on_error,
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
    // Every row counted and nothing published: the check that decides. The head's saw ten
    // thousand rows in load order, and a clean head says nothing of an untimed tail or of a
    // later file with no time at all.
    let tally = &counts.arrival;
    if !opts.proceed && tally.share() > limit {
        disk.unstage(load_id);
        return Err(tally.refusal(Checked::Whole, trie, &day, limit));
    }
    let arrival = Arrival {
        head: head.rows,
        rows: tally.rows,
        fell_back: tally.fell_back,
        share: tally.share(),
        day,
        limit,
        // Only `--proceed` gets a load this far past either check.
        proceeded: head.share() > limit || tally.share() > limit,
    };
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
    let measured = Measured {
        peak_held: counts.peak_held,
        paced: counts.paced,
    };
    let report = Report {
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
    };
    Ok((report, measured))
}

#[derive(Default)]
struct Counts {
    lines: u64,
    blank: u64,
    rejected: u64,
    row_groups: u64,
    errors: Vec<LineError>,
    errors_omitted: u64,
    /// Every row placed, and those that took their arrival time.
    arrival: Tally,
    peak_held: usize,
    paced: u64,
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
/// sealed: the load, less its commit. The only pass `--max-mb-per-sec` holds.
#[allow(clippy::too_many_arguments)]
fn stage_all(
    trie: &Trie,
    disk: &DiskStore,
    opts: &Options,
    inputs: &[Input],
    control: &Control,
    dir: &Path,
    load_id: u64,
    started: Instant,
    on_error: &mut dyn FnMut(&DetectorError),
) -> Result<StagedLoad, LoadError> {
    let limits: RowGroupLimits = disk.row_group_limits();
    let bound = opts.load_buffer_bytes.max(1);
    let Rounds { threads, max_bytes } = Rounds::of(opts);
    // Made here, with the reads since `started` as credit up to `PACE_CREDIT`: see `Pace`.
    let now = Instant::now();
    let credit = now.saturating_duration_since(started).min(PACE_CREDIT);
    let pace = RefCell::new(Pace::since(
        now.checked_sub(credit).unwrap_or(now),
        opts.max_bytes_per_sec,
        Arc::clone(&control.cancel),
    ));
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
    let mut held = Held::default();
    let mut lines = Lines::new(inputs, &pace);

    loop {
        if control.cancelled() {
            return Err(LoadError::Cancelled);
        }
        let (chunks, ended) = read_round(&mut lines, threads, max_bytes)?;
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
                let template_id = placed.template_id;
                templates
                    .entry(template_id)
                    .or_insert_with(|| placed.time_column.clone());
                // Fell back exactly when `place` gave it the arrival column: no flattened key
                // can be spelled that way, so the column is there only because the template's
                // own time column held no instant.
                let fell_back = placed.row.contains_key(RECEIVED_AT_COLUMN);
                counts
                    .arrival
                    .count(fell_back, || Who::Template(template_id));
                held.push((template_id, day), no, placed.row, time, &limits);
                held.bound(bound, &mut stage, load_id, threads)?;
            }
        }
        // The batches this round filled, written by its end at the latest.
        held.write(&mut stage, load_id, threads)?;
        if ended {
            break;
        }
    }

    held.finish(&mut stage, load_id, threads)?;
    let (files, row_groups) = stage.finish()?;
    counts.lines = lines.line;
    counts.blank = lines.blank;
    counts.row_groups = row_groups;
    counts.peak_held = held.peak;
    counts.paced = pace.borrow().read;
    control.lines.store(lines.line, Ordering::Relaxed);
    Ok((files, counts, templates))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "kibbr-load-{name}-{}-{:?}",
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

    // ── Loads into a data directory of the test's own ──

    /// A data directory opened as `kibbr load` alone opens one. The lock is dropped last,
    /// after the stores that write under it.
    struct Store {
        trie: Trie,
        stores: StoreManager,
        _lock: kibbr_storage::lock::DataLock,
    }

    impl Store {
        fn open(scratch: &Scratch) -> Self {
            // What the binary's settings make of `--data-dir`: the directory made, its
            // path absolute, and the trie's snapshot named inside it.
            let base = scratch.0.join("store");
            fs::create_dir_all(&base).expect("mkdir");
            let base = fs::canonicalize(&base).expect("the directory just made");
            let storage = crate::Storage {
                base_path: Some(base.clone()),
                trie_file: Some(base.join("trie.json")),
                ..crate::Storage::default()
            };
            let lock = kibbr_storage::lock::DataLock::acquire(&base).expect("lock");
            let (trie, stores) = crate::open_durable(&storage, &crate::Schema::default())
                .expect("open the data directory");
            Self {
                trie,
                stores,
                _lock: lock,
            }
        }

        fn load(&self, opts: &Options) -> Result<(Report, Measured), LoadError> {
            run_measured(
                &self.trie,
                &self.stores,
                opts,
                &Control::default(),
                &mut |_| {},
            )
        }

        /// Every template's rows, as a scan hands them out: each day in storage order.
        fn rows(&self) -> Vec<(u64, Row)> {
            let mut out = Vec::new();
            for leaf in self.trie.all_leaves() {
                let Some(id) = leaf
                    .leaf_meta
                    .read()
                    .expect("leaf meta")
                    .as_ref()
                    .map(|m| m.template_id)
                else {
                    continue;
                };
                let Some(store) = self.stores.lookup(id) else {
                    continue;
                };
                let scan = store
                    .scan(&kibbr_storage::store::ScanOptions::default(), &[])
                    .expect("scan");
                out.extend(scan.rows.into_iter().map(|row| (id, row)));
            }
            out
        }
    }

    fn options(inputs: Vec<Input>) -> Options {
        Options {
            inputs,
            replace_day: None,
            max_bytes_per_sec: None,
            threads: 2,
            load_buffer_bytes: DEFAULT_LOAD_BUFFER_BYTES,
            received_at: "2024-02-20T12:00:00Z".parse().expect("an instant"),
            proceed: false,
            max_arrival_share: DEFAULT_MAX_ARRIVAL_SHARE,
        }
    }

    /// `count` timed lines across `templates` key sets and `days` days, interleaved so every
    /// template-day is open at once: line `i` holds key `t<i*7 % templates>`, falls on
    /// 2024-01-<1 + i*13 % days>, and carries `n` = `i`.
    fn spread(count: usize, templates: usize, days: usize) -> Vec<u8> {
        (0..count)
            .map(|i| {
                format!(
                    "{{\"ts\":\"2024-01-{:02}T10:00:00Z\",\"t{}\":\"x\",\"n\":{i}}}\n",
                    1 + i * 13 % days,
                    i * 7 % templates
                )
            })
            .collect::<String>()
            .into_bytes()
    }

    /// `count` lines of template `k` with no time of their own; `logged_at` is a Datetime,
    /// but no name on the default time columns.
    fn untimed(count: usize, k: usize) -> Vec<u8> {
        (0..count)
            .map(|i| format!("{{\"logged_at\":\"2023-06-01T09:00:00Z\",\"u{k}\":{i}}}\n"))
            .collect::<String>()
            .into_bytes()
    }

    /// A hard bound: open batches and closed ones not yet written together never hold more
    /// than `--load-buffer-bytes`, however many template-days are open - and every row is
    /// still stored once, each template-day's rows in line order.
    #[test]
    fn a_tiny_load_buffer_holds_its_rows_to_the_bound_and_loses_none() {
        const LINES: usize = 6_000;
        const BOUND: usize = 16 * 1024;
        let scratch = Scratch::new("bound");
        let input = scratch.write("in/spread.ndjson", &spread(LINES, 12, 7));
        let store = Store::open(&scratch);
        let opts = Options {
            load_buffer_bytes: BOUND,
            ..options(resolve(&[input], None).expect("resolve"))
        };
        let (report, measured) = store.load(&opts).expect("load");
        assert_eq!(report.rows, LINES as u64);
        assert!(
            measured.peak_held <= BOUND,
            "held {} bytes of rows against a bound of {BOUND}",
            measured.peak_held
        );

        let mut seen = vec![false; LINES];
        let mut last: HashMap<(u64, i64), i64> = HashMap::new();
        for (template, row) in store.rows() {
            let Some(kibbr_core::types::ColValue::Int64(n)) = row.get("n") else {
                panic!("a row with no n: {row:?}");
            };
            let day = day_of(effective_ts(&row, "ts").expect("a time"));
            let before = last.insert((template, day), *n);
            assert!(
                before.is_none_or(|b| b < *n),
                "template {template} on {}: n {n} after {before:?}",
                day_name(day)
            );
            assert!(
                !std::mem::replace(&mut seen[*n as usize], true),
                "n {n} twice"
            );
        }
        assert!(seen.iter().all(|s| *s), "rows missing");
        assert_eq!(last.len(), 84, "not every template-day was stored");
    }

    /// `--max-mb-per-sec` holds the pass that parses and encodes, and that pass reads each
    /// input byte once: its pace is charged the inputs' decompressed length exactly - not
    /// the id's digest of them, nor the head check's, which read them first.
    #[test]
    fn the_pace_is_charged_each_input_byte_once() {
        let scratch = Scratch::new("paced");
        let plain = spread(3_000, 5, 3);
        let other = spread(2_000, 4, 2);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
        gz.write_all(&other).expect("gzip");
        let a = scratch.write("in/a.ndjson", &plain);
        let b = scratch.write("in/b.ndjson.gz", &gz.finish().expect("gzip"));
        let store = Store::open(&scratch);
        let opts = Options {
            // A rate no load reaches: charged, never waited on.
            max_bytes_per_sec: Some(1e15),
            ..options(resolve(&[a, b], None).expect("resolve"))
        };
        let (_, measured) = store.load(&opts).expect("load");
        assert_eq!(measured.paced, (plain.len() + other.len()) as u64);
    }

    /// And the rate is what the load delivers: bytes at N a second take about bytes / N
    /// seconds, not twice or three times that. Wide margins, for a debug build on a busy
    /// machine; a pace charged for the digest and the head as well is still past them.
    #[test]
    fn a_paced_load_takes_about_its_bytes_over_the_rate() {
        let scratch = Scratch::new("rate");
        let bytes = spread(1_500, 5, 3);
        let input = scratch.write("in/a.ndjson", &bytes);
        let store = Store::open(&scratch);
        let opts = Options {
            max_bytes_per_sec: Some(bytes.len() as f64),
            ..options(resolve(&[input], None).expect("resolve"))
        };
        let (report, _) = store.load(&opts).expect("load");
        assert!(
            (0.8..2.5).contains(&report.seconds),
            "{} bytes at {} a second took {}s",
            bytes.len(),
            bytes.len(),
            report.seconds
        );
    }

    /// The head check makes nothing - a template made while the load is refused would keep
    /// arrival time after its time key was added - so it names a template it has not seen
    /// by the line it first met it on, with the keys that could be its time.
    #[test]
    fn the_head_check_names_a_new_template_by_its_line_and_makes_none() {
        let scratch = Scratch::new("head");
        let mut body = spread(50, 2, 2);
        body.extend_from_slice(&untimed(150, 0));
        let input = scratch.write("in/a.ndjson", &body);
        let store = Store::open(&scratch);
        let err = store
            .load(&options(resolve(&[input], None).expect("resolve")))
            .expect_err("refused");
        let text = err.to_string();
        for phrase in [
            "75.0% of the first 200 rows (150)",
            "2024-02-20",
            "a new template first on line 51 (150 rows; Datetime-typed keys: logged_at)",
            "--proceed",
        ] {
            assert!(text.contains(phrase), "no `{phrase}`: {text}");
        }
        assert!(
            store.trie.all_leaves().is_empty(),
            "the check made a template"
        );
    }

    /// The head check reads at most `HEAD_LINES` lines, rows or not: an input opening on
    /// text that is not JSON is not parsed whole, unpaced, before the paced pass. Here the
    /// head finds no row at all and lets the load through to the count over every row,
    /// which stops it.
    #[test]
    fn the_head_check_stops_at_its_line_cap_and_the_count_over_every_row_decides() {
        let scratch = Scratch::new("head-lines");
        let mut body: Vec<u8> = (0..HEAD_LINES + 5)
            .flat_map(|i| format!("not json {i}\n").into_bytes())
            .collect();
        body.extend_from_slice(&untimed(50, 0));
        let input = scratch.write("in/a.ndjson", &body);
        let store = Store::open(&scratch);
        let text = store
            .load(&options(resolve(&[input], None).expect("resolve")))
            .expect_err("refused")
            .to_string();
        assert!(
            text.contains("100.0% of the 50 rows (50)"),
            "not the count over every row: {text}"
        );
    }

    /// A template with a time column whose rows held no instant there is named by that
    /// column, not by Datetime-typed keys an operator could add: no time key would change
    /// where its rows go.
    #[test]
    fn a_template_with_a_time_column_is_named_by_it_when_its_rows_fall_back() {
        let scratch = Scratch::new("column-fell-back");
        let mut body = spread(10_500, 1, 1);
        for i in 0..500 {
            body.extend_from_slice(
                format!("{{\"ts\":\"N/A\",\"t0\":\"x\",\"n\":{i}}}\n").as_bytes(),
            );
        }
        let input = scratch.write("in/a.ndjson", &body);
        let store = Store::open(&scratch);
        let text = store
            .load(&options(resolve(&[input], None).expect("resolve")))
            .expect_err("refused")
            .to_string();
        assert!(
            text.contains("(500 rows; its time column `ts` held no time)"),
            "{text}"
        );
    }

    /// Past the head's limit and within it over every row, `--proceed` loads - and the
    /// summary says the load was within the limit, not past it.
    #[test]
    fn a_load_past_only_the_head_says_it_was_within_the_limit() {
        let scratch = Scratch::new("head-only");
        let mut body = untimed(150, 0);
        body.extend_from_slice(&spread(49_850, 2, 2));
        let input = scratch.write("in/a.ndjson", &body);
        let store = Store::open(&scratch);
        let refused = store
            .load(&options(
                resolve(std::slice::from_ref(&input), None).expect("resolve"),
            ))
            .expect_err("the head is past the limit");
        assert!(
            refused.to_string().contains("of the first 10000 rows"),
            "{refused}"
        );
        let (report, _) = store
            .load(&Options {
                proceed: true,
                ..options(resolve(&[input], None).expect("resolve"))
            })
            .expect("loaded with --proceed");
        let summary = report.summary();
        assert!(
            summary.contains("150 of 50,000 rows (0.3%)")
                && summary.contains("within --max-arrival-share 1% over every row"),
            "{summary}"
        );
    }

    /// Past a clean head, the count is over every row, and the refusal names the templates
    /// that fell back by id, most rows first, ten at most.
    #[test]
    fn the_whole_input_check_names_ten_templates_most_rows_first() {
        let scratch = Scratch::new("whole");
        let mut body = spread(10_500, 3, 2);
        for k in 0..12 {
            body.extend_from_slice(&untimed(100 + k, k));
        }
        let input = scratch.write("in/a.ndjson", &body);
        let store = Store::open(&scratch);
        let err = store
            .load(&options(resolve(&[input], None).expect("resolve")))
            .expect_err("refused");
        let text = err.to_string();
        let fell_back: usize = (0..12).map(|k| 100 + k).sum();
        let rows = 10_500 + fell_back;
        for phrase in [
            format!("of the {rows} rows ({fell_back})"),
            "(111 rows; Datetime-typed keys: logged_at)".to_string(),
            "and 2 more".to_string(),
        ] {
            assert!(text.contains(&phrase), "no `{phrase}`: {text}");
        }
        assert!(
            !text.contains("(100 rows;"),
            "the fewest rows named first: {text}"
        );
        let staging = scratch
            .0
            .join("store")
            .join(kibbr_storage::store::disk::STAGING_DIR);
        let left: Vec<_> = fs::read_dir(&staging)
            .map(|d| d.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        assert!(left.is_empty(), "staging left: {left:?}");
    }
}
