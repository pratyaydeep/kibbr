// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The bytes every number is measured over: the loaded directory and the live stream.
//! Loaded once, then only copied: two loads of one file are not the same bytes, so results
//! compare only on the one load, and every server starts on a verified copy. The generator
//! and the load are children of this binary, so no command-line default is copied here.
//! Nothing is removed that this invocation did not make: `--dir` may be the wrong place.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::engine::display::commas;

use super::child::scrub;
use super::{
    crc_hex, host, tail, wire, SpeedError, FREE_BYTES_PER_LINE, LIVE_SEED, LOADED_SEED, PROFILE,
    RECEIVED_AT, SEGMENT_MAX_BYTES, TIME_COLUMNS,
};

/// The data directory's lock file, at its root. It names the process that last held the
/// directory, so it differs from one load to the next and is no part of the loaded bytes.
const LOCK: &str = "LOCK";

/// The two children, as an error names them.
const GEN_LOGS: &str = "kibbr gen-logs";
const LOAD: &str = "kibbr load";

/// Lines of a child's standard error an error quotes.
const QUOTED_LINES: usize = 20;

/// Bytes read at a time from a file or a child's pipe.
const CHUNK: usize = 1 << 20;

/// Room made for each line of the live stream before it is read: a little over a farm
/// line, so that the buffer is not grown and copied on a machine the servers need.
const LIVE_BYTES_PER_LINE: usize = 520;

/// Differences between a directory and its marker that an error spells out; past this it
/// counts the rest.
const MAX_NAMED: usize = 5;

/// One file of the loaded directory: its path from the root with `/` between its parts on
/// every system, its length and its CRC-32C as eight hex digits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileDigest {
    pub path: String,
    pub bytes: u64,
    pub crc32c: String,
}

/// A directory's files in path order, and the names that are no part of it: `LOCK`, and
/// whatever begins with `.`, which a file manager leaves in any folder it is shown.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    pub files: Vec<FileDigest>,
    pub left_out: Vec<PathBuf>,
}

/// The generated file that was loaded, which is deleted once the marker has its digest.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputDigest {
    pub bytes: u64,
    pub crc32c: String,
}

/// What `kibbr load --json` said, where a field it did not give is `null`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoadFacts {
    pub load_id: Option<String>,
    pub rows: u64,
    pub rejected: Option<u64>,
    pub row_groups: Option<u64>,
    pub days: u64,
    pub bytes: Option<u64>,
    pub seconds: Option<f64>,
}

/// The build that made the loaded directory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuiltBy {
    pub sha: Option<String>,
    pub binary_crc32c: String,
}

/// The marker beside the loaded directory, written last: its presence says the load
/// finished, and its list is what every copy is held to.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Marker {
    pub lines: u64,
    pub seed: u64,
    pub profile: String,
    pub input: InputDigest,
    pub load: LoadFacts,
    pub files: Vec<FileDigest>,
    /// The first `job.id` in the loaded stream: the id S3 asks for.
    pub s3_id: String,
    pub built_at: String,
    pub built_by: BuiltBy,
}

/// The loaded directory, verified: where it is, what its marker says, and whether this
/// invocation built it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Corpus {
    pub dir: PathBuf,
    pub marker_path: PathBuf,
    pub marker: Marker,
    /// True when the marker was there and the directory was found to be what it lists.
    pub reused: bool,
    /// Names under the directory that are not part of the loaded bytes and are not
    /// copied, from the directory's root.
    pub left_out: Vec<PathBuf>,
}

/// One `/ingest` body: where it lies in the live stream, and how many lines it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Body {
    pub range: Range<usize>,
    pub lines: usize,
}

/// The live stream, whole and in memory, cut into bodies before any server starts: nothing
/// is generated, read or cut inside a timed window.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveStream {
    pub bytes: Vec<u8>,
    pub lines: u64,
    pub crc32c: String,
    pub bodies: Vec<Body>,
}

// ── Where things are ───────────────────────────────────────────────────────────────

/// Where the loaded directory for `lines` is, under `--dir`.
pub fn loaded_dir(dir: &Path, lines: u64) -> PathBuf {
    dir.join(format!("farm-{lines}-seed{LOADED_SEED}"))
}

/// Where its marker is: beside it, so that a copy of the directory never carries one.
pub fn marker_path(dir: &Path, lines: u64) -> PathBuf {
    dir.join(format!("farm-{lines}-seed{LOADED_SEED}.json"))
}

/// Where the standard error of the load that made the directory is kept, with the
/// generator's before it.
pub fn load_log(dir: &Path, lines: u64) -> PathBuf {
    dir.join(format!("farm-{lines}-seed{LOADED_SEED}.load.log"))
}

/// Where the generated file stands between the generator and the marker. Its name is part
/// of the load's id, so it is the same name in every invocation.
fn input_path(dir: &Path, lines: u64) -> PathBuf {
    dir.join(format!("farm-{lines}-seed{LOADED_SEED}.ndjson"))
}

/// Where this invocation keeps its copies and every child's standard error, `.noindex` to
/// keep the system's indexer out of files that live for a second.
pub fn work_dir(dir: &Path) -> PathBuf {
    dir.join(format!("work-{}.noindex", std::process::id()))
}

/// Work directories under `dir` that another invocation left: named in the log, never
/// removed - the bench removes nothing it did not make.
pub fn stale_work(dir: &Path) -> Vec<PathBuf> {
    let own = work_dir(dir);
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut left: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("work-") && name.ends_with(".noindex")
        })
        .map(|entry| entry.path())
        .filter(|path| *path != own)
        .collect();
    left.sort();
    left
}

/// Hold the volume of `dir` to the room a run of `lines` needs and return what it has
/// free, before anything is built: a volume that fills under a server is a run lost.
pub fn free_space(dir: &Path, lines: u64) -> Result<u64, SpeedError> {
    let free = host::disk_free(dir)?;
    enough(dir, free, lines)?;
    Ok(free)
}

/// Whether `free` bytes are the room `lines` lines need.
fn enough(dir: &Path, free: u64, lines: u64) -> Result<(), SpeedError> {
    let need = lines.saturating_mul(FREE_BYTES_PER_LINE);
    if free < need {
        return Err(SpeedError::Host(format!(
            "the volume that holds {} has {} bytes free, and kibbr bench --lines {} needs {} \
             ({} a line) before it builds anything. Make room on it, or name a directory on \
             another volume with --dir",
            dir.display(),
            commas(free),
            commas(lines),
            commas(need),
            commas(FREE_BYTES_PER_LINE),
        )));
    }
    Ok(())
}

// ── A directory as a list of files ─────────────────────────────────────────────────

/// A file's length and CRC-32C, read from its first byte to its last. The length is what
/// was read and not what the directory says, so the two numbers are of the same bytes.
pub(super) fn digest(path: &Path) -> io::Result<(u64, u32)> {
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; CHUNK];
    let (mut bytes, mut crc) = (0u64, 0u32);
    loop {
        let n = match file.read(&mut buf) {
            Ok(0) => return Ok((bytes, crc)),
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        bytes += n as u64;
    }
}

/// Every file under `dir` in path order with its length and CRC-32C, less the root's
/// `LOCK` and every name that begins with `.`; a link or a socket is an error.
pub fn listing(dir: &Path) -> io::Result<Listing> {
    let mut found = Listing::default();
    list_into(dir, "", &mut found)?;
    found.files.sort_by(|a, b| a.path.cmp(&b.path));
    found.left_out.sort();
    Ok(found)
}

/// List the directory `below` the root `dir` (`""` is the root itself) into `found`.
fn list_into(dir: &Path, below: &str, found: &mut Listing) -> io::Result<()> {
    let here = dir.join(below);
    for entry in fs::read_dir(&here).map_err(|e| named(&here, e))? {
        let entry = entry.map_err(|e| named(&here, e))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} has a name that is not UTF-8", path.display()),
            ));
        };
        let from_root = if below.is_empty() {
            name.to_string()
        } else {
            format!("{below}/{name}")
        };
        if name.starts_with('.') || (below.is_empty() && name == LOCK) {
            found.left_out.push(PathBuf::from(from_root));
            continue;
        }
        let kind = entry.file_type().map_err(|e| named(&path, e))?;
        if kind.is_dir() {
            list_into(dir, &from_root, found)?;
        } else if kind.is_file() {
            let (bytes, crc) = digest(&path).map_err(|e| named(&path, e))?;
            found.files.push(FileDigest {
                path: from_root,
                bytes,
                crc32c: crc_hex(crc),
            });
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is neither a file nor a directory", path.display()),
            ));
        }
    }
    Ok(())
}

/// An error of the file system with the path it was met at: a listing walks many, and
/// "permission denied" alone does not say which.
fn named(path: &Path, e: io::Error) -> io::Error {
    io::Error::new(e.kind(), format!("{}: {e}", path.display()))
}

/// Copy the listed files from `from` to a new directory `to`, with every directory not
/// named `.`-something, refusing a listed path that would leave `to`: a marker can be edited.
pub fn copy_tree(from: &Path, files: &[FileDigest], to: &Path) -> io::Result<(u64, u64)> {
    // Every path is held to the directory before anything is made.
    for file in files {
        let inside = Path::new(&file.path);
        let stays = inside
            .components()
            .all(|part| matches!(part, Component::Normal(_)));
        if file.path.is_empty() || !stays {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "\"{}\" is not a path inside the loaded directory",
                    file.path.escape_debug()
                ),
            ));
        }
    }
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).map_err(|e| named(parent, e))?;
    }
    fs::create_dir(to).map_err(|e| named(to, e))?;
    make_directories(from, to)?;

    let mut bytes = 0u64;
    for file in files {
        let source = from.join(&file.path);
        bytes += fs::copy(&source, to.join(&file.path)).map_err(|e| named(&source, e))?;
    }
    Ok((files.len() as u64, bytes))
}

/// Make under `to` every directory under `from` whose name does not begin with `.`.
fn make_directories(from: &Path, to: &Path) -> io::Result<()> {
    for entry in fs::read_dir(from).map_err(|e| named(from, e))? {
        let entry = entry.map_err(|e| named(from, e))?;
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        if entry
            .file_type()
            .map_err(|e| named(&entry.path(), e))?
            .is_dir()
        {
            let made = to.join(&name);
            fs::create_dir(&made).map_err(|e| named(&made, e))?;
            make_directories(&entry.path(), &made)?;
        }
    }
    Ok(())
}

/// How a directory's files differ from the list recorded for it, one phrase a
/// difference; empty when they are the same files with the same bytes.
pub fn differences(listed: &[FileDigest], recorded: &[FileDigest]) -> Vec<String> {
    let by_path = |files: &[FileDigest]| -> BTreeMap<String, FileDigest> {
        files
            .iter()
            .map(|file| (file.path.clone(), file.clone()))
            .collect()
    };
    let (listed, recorded) = (by_path(listed), by_path(recorded));
    let mut found = Vec::new();
    for (path, was) in &recorded {
        match listed.get(path) {
            None => found.push(format!(
                "{path}: not there, recorded with {} bytes",
                commas(was.bytes)
            )),
            Some(is) if is.bytes != was.bytes => found.push(format!(
                "{path}: {} bytes, recorded {}",
                commas(is.bytes),
                commas(was.bytes)
            )),
            Some(is) if is.crc32c != was.crc32c => found.push(format!(
                "{path}: crc32c {}, recorded {}",
                is.crc32c, was.crc32c
            )),
            Some(_) => {}
        }
    }
    for path in listed.keys().filter(|path| !recorded.contains_key(*path)) {
        found.push(format!("{path}: not recorded"));
    }
    found
}

// ── The loaded directory ───────────────────────────────────────────────────────────

/// The loaded directory for `lines` under `dir`, built by this call or, with its marker
/// there, held to it; never loaded again in place, or a baseline would compare with nothing.
pub fn prepare(
    exe: &Path,
    dir: &Path,
    lines: u64,
    built_by: &BuiltBy,
) -> Result<Corpus, SpeedError> {
    let loaded = loaded_dir(dir, lines);
    let marker_path = marker_path(dir, lines);
    let has_marker = marker_path.try_exists().map_err(io_at(&marker_path))?;
    let has_loaded = loaded.try_exists().map_err(io_at(&loaded))?;

    let (marker, left_out) = match (has_marker, has_loaded) {
        (true, true) => verify(&loaded, &marker_path, lines)?,
        (false, false) => build(exe, dir, lines, built_by)?,
        (false, true) => {
            return Err(SpeedError::Corpus(format!(
                "{} is there and its marker {} is not: a load that did not finish, or a \
                 directory that is not the bench's. Nothing was removed. Move {} away and \
                 the bench will load again - a baseline taken on the old bytes will then \
                 not compare",
                loaded.display(),
                marker_path.display(),
                loaded.display(),
            )))
        }
        (true, false) => {
            return Err(SpeedError::Corpus(format!(
                "the marker {} is there and the directory it describes, {}, is not. Nothing \
                 was removed. Put the directory back, or move the marker away and the bench \
                 will load again - a baseline taken on the old bytes will then not compare",
                marker_path.display(),
                loaded.display(),
            )))
        }
    };
    Ok(Corpus {
        dir: loaded,
        marker_path,
        marker,
        reused: has_marker,
        left_out,
    })
}

/// Read the marker and hold the directory to its list.
fn verify(
    loaded: &Path,
    marker_path: &Path,
    lines: u64,
) -> Result<(Marker, Vec<PathBuf>), SpeedError> {
    let move_away = format!(
        "Move {} and {} away to load again - a baseline taken on the old bytes will then not \
         compare",
        loaded.display(),
        marker_path.display(),
    );
    let text = fs::read(marker_path).map_err(io_at(marker_path))?;
    let marker: Marker = serde_json::from_slice(&text).map_err(|e| {
        SpeedError::Corpus(format!(
            "{} cannot be read as the marker of a loaded directory ({e}). {move_away}",
            marker_path.display(),
        ))
    })?;
    if (marker.lines, marker.seed, marker.profile.as_str()) != (lines, LOADED_SEED, PROFILE) {
        return Err(SpeedError::Corpus(format!(
            "{} is the marker of other data: {} lines of the {} profile at seed {}, where \
             this run loads {} lines of the {PROFILE} profile at seed {LOADED_SEED}. \
             {move_away}",
            marker_path.display(),
            commas(marker.lines),
            marker.profile,
            marker.seed,
            commas(lines),
        )));
    }
    let listed = listing(loaded).map_err(io_at(loaded))?;
    let differing = differences(&listed.files, &marker.files);
    if !differing.is_empty() {
        let more = match differing.len().saturating_sub(MAX_NAMED) {
            0 => String::new(),
            more => format!("; and {more} more"),
        };
        return Err(SpeedError::Corpus(format!(
            "the loaded directory is not the bytes that were loaded ({}{more}). {move_away}",
            differing[..differing.len().min(MAX_NAMED)].join("; "),
        )));
    }
    Ok((marker, listed.left_out))
}

/// Generate the day, find S3's id in it, load it, list what the load wrote, and write the
/// marker last. Neither the directory nor the marker is there when this is called.
fn build(
    exe: &Path,
    dir: &Path,
    lines: u64,
    built_by: &BuiltBy,
) -> Result<(Marker, Vec<PathBuf>), SpeedError> {
    fs::create_dir_all(dir).map_err(io_at(dir))?;
    free_space(dir, lines)?;

    let input_path = input_path(dir, lines);
    let log = load_log(dir, lines);
    let input = generate_to_file(exe, lines, &input_path, &log)?;
    let made = load_and_mark(exe, dir, lines, built_by, input);
    // The generated file does not outlive this call, whichever way it went: the marker has
    // its digest and the generator writes the same bytes again. Of two failures the first is told.
    let removed = fs::remove_file(&input_path).map_err(io_at(&input_path));
    let made = made?;
    removed?;
    Ok(made)
}

/// What is done with the generated file once it is whole. S3's id is found in it before
/// the load, so that a stream with none costs no load; the marker is written last.
fn load_and_mark(
    exe: &Path,
    dir: &Path,
    lines: u64,
    built_by: &BuiltBy,
    input: InputDigest,
) -> Result<(Marker, Vec<PathBuf>), SpeedError> {
    let loaded = loaded_dir(dir, lines);
    let input_path = input_path(dir, lines);

    let s3_id = s3_id(&input_path, lines)?;
    let load = load(exe, &input_path, &loaded, &load_log(dir, lines))?;
    let listed = listing(&loaded).map_err(io_at(&loaded))?;
    if listed.files.is_empty() {
        return Err(SpeedError::Corpus(format!(
            "`{LOAD}` reported {} rows and left no file under {}: there is nothing to copy \
             and nothing to hold a copy to. The directory is left as it is; move it away \
             before the bench is run again",
            commas(load.rows),
            loaded.display(),
        )));
    }
    let marker = Marker {
        lines,
        seed: LOADED_SEED,
        profile: PROFILE.to_string(),
        input,
        load,
        files: listed.files,
        s3_id,
        built_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        built_by: built_by.clone(),
    };
    write_marker(&marker_path(dir, lines), &marker)?;
    Ok((marker, listed.left_out))
}

/// Write the marker beside its directory: to a `.tmp` name, flushed, then renamed, so
/// that a marker that is there is a whole one.
fn write_marker(path: &Path, marker: &Marker) -> Result<(), SpeedError> {
    let mut text = serde_json::to_string_pretty(marker)?;
    text.push('\n');
    let tmp = with_tmp(path);
    let mut file = File::create(&tmp).map_err(io_at(&tmp))?;
    file.write_all(text.as_bytes()).map_err(io_at(&tmp))?;
    file.sync_all().map_err(io_at(&tmp))?;
    drop(file);
    fs::rename(&tmp, path).map_err(io_at(path))
}

/// `path` with `.tmp` after its whole name.
fn with_tmp(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

// ── The two children ───────────────────────────────────────────────────────────────

/// A child that does not outlive the function that started it: killed and reaped when
/// this is dropped, which after a `wait` does nothing.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start `kibbr gen-logs --profile farm --lines <lines> --seed <seed>`, its standard
/// output a pipe and its standard error the file `log`, made anew.
fn generator(exe: &Path, lines: u64, seed: u64, log: &Path) -> Result<Reaped, SpeedError> {
    let stderr = File::create(log).map_err(io_at(log))?;
    let mut command = Command::new(exe);
    command
        .args(["gen-logs", "--profile", PROFILE])
        .args(["--lines", &lines.to_string()])
        .args(["--seed", &seed.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(stderr);
    scrub(&mut command);
    command.spawn().map(Reaped).map_err(io_at(exe))
}

/// The pipe a generator writes to.
fn piped(child: &mut Reaped) -> Result<impl Read, SpeedError> {
    child.0.stdout.take().ok_or_else(|| SpeedError::Io {
        path: generator_output(),
        source: io::Error::other("the child was started without a pipe"),
    })
}

/// What an error calls a generator's standard output, where a file's path would stand.
fn generator_output() -> PathBuf {
    PathBuf::from(format!("the standard output of `{GEN_LOGS}`"))
}

/// Wait for a generator that has closed its output, and hold it to exit 0 and to the
/// lines that were asked of it.
fn finish(mut child: Reaped, log: &Path, wrote: u64, asked: u64) -> Result<(), SpeedError> {
    let status = child.0.wait().map_err(|source| SpeedError::Io {
        path: generator_output(),
        source,
    })?;
    if !status.success() {
        return Err(SpeedError::Child {
            what: GEN_LOGS,
            code: status.code(),
            stderr: said(None, log),
        });
    }
    if wrote != asked {
        return Err(SpeedError::Child {
            what: GEN_LOGS,
            code: status.code(),
            stderr: said(
                Some(format!(
                    "{} lines were asked of it and it wrote {}",
                    commas(asked),
                    commas(wrote)
                )),
                log,
            ),
        });
    }
    Ok(())
}

/// What an error says of a child: what was wrong where the exit code does not say it,
/// then the end of what the child wrote to standard error.
fn said(wrong: Option<String>, log: &Path) -> String {
    let wrote = String::from_utf8_lossy(&fs::read(log).unwrap_or_default()).into_owned();
    let wrote = tail(&wrote, QUOTED_LINES);
    match (wrong, wrote.is_empty()) {
        (Some(wrong), true) => wrong,
        (Some(wrong), false) => format!("{wrong}\n{wrote}"),
        (None, true) => "it wrote nothing to standard error".to_string(),
        (None, false) => wrote,
    }
}

/// Lines in a stream, counted as it passes: every newline ends one, and bytes after the
/// last newline are one more.
#[derive(Default)]
struct Lines {
    ended: u64,
    open: bool,
}

impl Lines {
    fn eat(&mut self, bytes: &[u8]) {
        self.ended += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        if let Some(last) = bytes.last() {
            self.open = *last != b'\n';
        }
    }

    fn count(&self) -> u64 {
        self.ended + u64::from(self.open)
    }
}

/// Generate the loaded day into `path`: to a `.tmp` name, digested as it is written,
/// then renamed. A generator that failed leaves no file.
fn generate_to_file(
    exe: &Path,
    lines: u64,
    path: &Path,
    log: &Path,
) -> Result<InputDigest, SpeedError> {
    let tmp = with_tmp(path);
    let written = generate_into(exe, lines, &tmp, log)
        .and_then(|input| fs::rename(&tmp, path).map_err(io_at(path)).map(|()| input));
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    written
}

/// Run the generator of the loaded day with its output copied into the new file `tmp`,
/// and hold it to the lines asked for.
fn generate_into(
    exe: &Path,
    lines: u64,
    tmp: &Path,
    log: &Path,
) -> Result<InputDigest, SpeedError> {
    let mut child = generator(exe, lines, LOADED_SEED, log)?;
    let mut from = piped(&mut child)?;
    let mut to = File::create(tmp).map_err(io_at(tmp))?;

    let mut buf = vec![0u8; CHUNK];
    let (mut bytes, mut crc, mut counted) = (0u64, 0u32, Lines::default());
    loop {
        let n = match from.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(SpeedError::Io {
                    path: generator_output(),
                    source,
                })
            }
        };
        crc = crc32c::crc32c_append(crc, &buf[..n]);
        counted.eat(&buf[..n]);
        bytes += n as u64;
        to.write_all(&buf[..n]).map_err(io_at(tmp))?;
    }
    to.sync_all().map_err(io_at(tmp))?;
    drop(to);
    finish(child, log, counted.count(), lines)?;
    Ok(InputDigest {
        bytes,
        crc32c: crc_hex(crc),
    })
}

/// The id S3 asks for: the first string at `job`, then `id`, in the loaded stream. Taken
/// from the stream because the farm's stream depends on `--lines`.
fn s3_id(input: &Path, lines: u64) -> Result<String, SpeedError> {
    let file = File::open(input).map_err(io_at(input))?;
    let found = first_job_id(BufReader::with_capacity(CHUNK, file)).map_err(io_at(input))?;
    let Some(id) = found else {
        return Err(SpeedError::Corpus(format!(
            "no line of `gen-logs --profile {PROFILE} --lines {lines} --seed {LOADED_SEED}` \
             holds a string at job.id, the id the third query asks for: at so few lines the \
             farm ran no job. Nothing was loaded; run the bench with more --lines"
        )));
    };
    // The id is written into the query's text between two quotes, as it stands.
    if id.is_empty() || id.chars().any(|c| c == '"' || c == '\\' || c.is_control()) {
        return Err(SpeedError::Corpus(format!(
            "the first job.id of `gen-logs --profile {PROFILE} --lines {lines} --seed \
             {LOADED_SEED}` is \"{}\", which cannot stand in a query's text as it is. \
             Nothing was loaded: the generator no longer writes the ids the bench was \
             written for",
            id.escape_debug()
        )));
    }
    Ok(id)
}

/// The first string at `job`, then `id`, in NDJSON. A line that is not JSON, or whose
/// `job` is not an object with a string `id`, is stepped over.
fn first_job_id(mut lines: impl BufRead) -> io::Result<Option<String>> {
    let mut line = Vec::new();
    loop {
        line.clear();
        if lines.read_until(b'\n', &mut line)? == 0 {
            return Ok(None);
        }
        let id = serde_json::from_slice::<serde_json::Value>(&line)
            .ok()
            .and_then(|value| Some(value.get("job")?.get("id")?.as_str()?.to_string()));
        if id.is_some() {
            return Ok(id);
        }
    }
}

/// Run `kibbr load` on the generated file into a new directory, and return what it said.
/// Every flag a number depends on is typed out, so a default that moves moves nothing loaded.
fn load(exe: &Path, input: &Path, loaded: &Path, log: &Path) -> Result<LoadFacts, SpeedError> {
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .map_err(io_at(log))?;
    let mut command = Command::new(exe);
    command
        .arg("load")
        .arg(input)
        .arg("--data-dir")
        .arg(loaded)
        .args(["--received-at", RECEIVED_AT])
        .args(["--time-columns", &TIME_COLUMNS.join(",")])
        .args(["--segment-max-bytes", &SEGMENT_MAX_BYTES.to_string()])
        .arg("--json")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(stderr);
    scrub(&mut command);
    let output = command.output().map_err(io_at(exe))?;

    let failed = |wrong: Option<String>| {
        let left = format!(
            "{} is left as the load left it, with no marker; move it away before the bench \
             is run again",
            loaded.display()
        );
        SpeedError::Child {
            what: LOAD,
            code: output.status.code(),
            stderr: format!("{}\n{left}", said(wrong, log)),
        }
    };
    if !output.status.success() {
        return Err(failed(None));
    }
    load_facts(&output.stdout).map_err(|wrong| failed(Some(wrong)))
}

/// Read what `kibbr load --json` printed: `ok`, at least one row, all on one day. A field
/// the bench only records is `None` where the answer does not hold it as that type.
fn load_facts(stdout: &[u8]) -> Result<LoadFacts, String> {
    let answer: serde_json::Value = serde_json::from_slice(stdout).map_err(|e| {
        format!(
            "its standard output is not the one JSON object `load --json` prints ({e}): {}",
            String::from_utf8_lossy(stdout).trim()
        )
    })?;
    let count = |name: &str| answer.get(name).and_then(serde_json::Value::as_u64);
    if answer.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err(format!("its answer does not say `\"ok\":true`: {answer}"));
    }
    let (Some(rows), Some(days)) = (count("rows"), count("days")) else {
        return Err(format!(
            "its answer does not give `rows` and `days` as numbers: {answer}"
        ));
    };
    if rows == 0 || days != 1 {
        return Err(format!(
            "it loaded {} rows over {} days, where the farm's day is at least one row on one \
             day",
            commas(rows),
            commas(days)
        ));
    }
    Ok(LoadFacts {
        load_id: answer
            .get("load_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        rows,
        rejected: count("rejected"),
        row_groups: count("row_groups"),
        days,
        bytes: count("bytes"),
        seconds: answer.get("seconds").and_then(serde_json::Value::as_f64),
    })
}

// ── The live stream ────────────────────────────────────────────────────────────────

/// The live stream: `kibbr gen-logs --profile farm` as a child, its output held whole,
/// digested and cut into bodies; on no disk, so its digest is what two results compare on.
pub fn live_stream(
    exe: &Path,
    lines: u64,
    body_lines: usize,
    work: &Path,
) -> Result<LiveStream, SpeedError> {
    let log = work.join(format!("gen-logs-seed{LIVE_SEED}.log"));
    let mut child = generator(exe, lines, LIVE_SEED, &log)?;
    let mut from = piped(&mut child)?;

    let mut bytes = Vec::new();
    let room = usize::try_from(lines)
        .unwrap_or(usize::MAX)
        .saturating_mul(LIVE_BYTES_PER_LINE);
    bytes.try_reserve_exact(room).map_err(|_| {
        SpeedError::Host(format!(
            "the live stream of {} lines, about {} bytes, is held in memory whole, and this \
             machine did not give the room. Run the bench with fewer --lines",
            commas(lines),
            commas(room as u64),
        ))
    })?;
    from.read_to_end(&mut bytes)
        .map_err(|source| SpeedError::Io {
            path: generator_output(),
            source,
        })?;

    let bodies = cut(&bytes, body_lines);
    let wrote: u64 = bodies.iter().map(|body| body.lines as u64).sum();
    finish(child, &log, wrote, lines)?;
    Ok(LiveStream {
        crc32c: crc_hex(crc32c::crc32c(&bytes)),
        lines: wrote,
        bytes,
        bodies,
    })
}

/// Cut NDJSON into bodies of `body_lines` lines, each ending at a line's end and the last
/// possibly short, so that every byte is in exactly one body, in order.
pub fn cut(bytes: &[u8], body_lines: usize) -> Vec<Body> {
    let full = body_lines.max(1);
    let mut bodies = Vec::new();
    let (mut start, mut lines) = (0, 0);
    for (at, byte) in bytes.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        lines += 1;
        if lines == full {
            bodies.push(Body {
                range: start..at + 1,
                lines,
            });
            (start, lines) = (at + 1, 0);
        }
    }
    if start < bytes.len() {
        // What is left: fewer lines than a body, and the last of them a line whether or
        // not the stream ended it.
        let open = bytes.last() != Some(&b'\n');
        bodies.push(Body {
            range: start..bytes.len(),
            lines: lines + usize::from(open),
        });
    }
    bodies
}

/// One `/ingest` request head for each body of the stream, built before any server
/// starts, so that a sender formats nothing.
pub fn heads(stream: &LiveStream) -> Vec<Vec<u8>> {
    stream
        .bodies
        .iter()
        .map(|body| wire::ingest_head(body.range.len()))
        .collect()
}

/// The bodies sender `sender` of `senders` sends: its own number, then every
/// `senders`-th after it, so the shares are disjoint and together are every body.
pub fn share(bodies: usize, sender: usize, senders: usize) -> Vec<usize> {
    (sender..bodies).step_by(senders.max(1)).collect()
}

/// An error of the file system as the bench's own, with the path it was met at.
fn io_at(path: &Path) -> impl Fn(io::Error) -> SpeedError + '_ {
    move |source| SpeedError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
#[path = "corpus_tests.rs"]
mod tests;
