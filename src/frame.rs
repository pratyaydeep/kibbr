//! The one record framing, and the append-only file built on it.
//!
//! ```text
//! frame   [4] payload_len  u32 LE, at least 1
//!         [4] crc          u32 LE, CRC-32C over the payload
//!         [N] payload
//! ```
//!
//! **Four kinds of file read through this and nothing else**: `catalog.log`,
//! `manifest.log`, every frame of a v3 data file, and every flush in a segment of the
//! write-ahead log. One reader is one thing to fuzz, and one place where a length is
//! checked against the bytes that remain before anything trusts it.
//!
//! **`payload_len` is never 0.** CRC-32C of nothing is 0, so without the rule a run of
//! zero bytes - which some file systems leave past the last durable write after a crash -
//! would read as an endless run of valid empty records. Every payload holds at least one
//! byte (a JSON object's `{`, a data frame's kind), so eight zero bytes are a tear.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// `payload_len` + `crc`.
pub const HEADER: usize = 8;

/// CRC-32C: Castagnoli, reflected, initial value and final XOR `0xFFFFFFFF`. The crate
/// takes the CPU's CRC instruction where there is one - SSE 4.2, or ARMv8's CRC
/// extension - and a table where there is not; the answer is the same either way.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// One frame around `payload`.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// What is at one offset.
#[derive(Debug, PartialEq, Eq)]
pub enum Next<'a> {
    /// A whole frame whose CRC holds. `end` is the offset just past it.
    Frame { payload: &'a [u8], end: usize },
    /// Exactly at the end of the bytes: nothing torn, nothing more.
    End,
    /// Not a whole, intact frame: fewer than eight bytes, a zero or oversized length, a
    /// length past the end, or a CRC that fails. After a crash this is the normal
    /// signature of a write that did not finish.
    Tear,
}

/// The frame starting at `at`, with its length checked against `max` and against the
/// bytes that actually remain *before* anything is sliced - a length of `0xFFFFFFFF` is a
/// tear, never a 4GB read.
pub fn read(bytes: &[u8], at: usize, max: u32) -> Next<'_> {
    if at == bytes.len() {
        return Next::End;
    }
    let Some(header) = bytes.get(at..).and_then(|rest| rest.get(..HEADER)) else {
        return Next::Tear;
    };
    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let want = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if len == 0 || len > max {
        return Next::Tear;
    }
    let start = at + HEADER;
    let Some(payload) = bytes.get(start..).and_then(|rest| rest.get(..len as usize)) else {
        return Next::Tear;
    };
    if crc32c(payload) != want {
        return Next::Tear;
    }
    Next::Frame {
        payload,
        end: start + len as usize,
    }
}

/// Every intact frame from the start of `bytes` as `(offset, payload)`, and the offset of
/// the first byte that is not one - `None` when the bytes end on a frame boundary.
pub fn read_all(bytes: &[u8], max: u32) -> (Vec<(u64, &[u8])>, Option<u64>) {
    let mut frames = Vec::new();
    let mut at = 0usize;
    loop {
        match read(bytes, at, max) {
            Next::Frame { payload, end } => {
                frames.push((at as u64, payload));
                at = end;
            }
            Next::End => return (frames, None),
            Next::Tear => return (frames, Some(at as u64)),
        }
    }
}

/// `read_all`, with each payload as its range of `bytes` and not a borrow of them: what
/// `Log::open_with` keeps of a file it has read, so the bytes can move into `Opened`
/// beside the ranges. A function of its own so the `record_log` fuzz target holds the
/// ranges a start slices by to the payloads `read_all` found, without a file.
pub fn spans(bytes: &[u8], max: u32) -> (Vec<(u64, std::ops::Range<usize>)>, Option<u64>) {
    let (found, torn_at) = read_all(bytes, max);
    let frames = found
        .iter()
        .map(|(offset, payload)| {
            let start = *offset as usize + HEADER;
            (*offset, start..start + payload.len())
        })
        .collect();
    (frames, torn_at)
}

// ── The append-only file ───────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// An earlier append failed *and* the partial write could not be rolled back, so the
    /// tail of the file is not a frame boundary. Anything appended after it would sit
    /// behind a tear and be truncated away on the next start, so nothing is.
    #[error("{path}: an earlier append left a torn record at offset {offset} that could not be removed; no further records are written until the process restarts")]
    Torn { path: String, offset: u64 },
    /// The file was just created and its directory could not be fsynced, so the file's
    /// own entry may not survive a crash - and a record fsynced into a file that is not
    /// there after the crash was never durable.
    #[error("{path}: created {name} here and could not fsync the directory, so the file itself may not survive a crash: {source}")]
    SyncDir {
        path: String,
        name: String,
        #[source]
        source: std::io::Error,
    },
}

/// What `Log::open` found in the file.
pub struct Opened {
    /// The whole file as it was read, before any truncation.
    pub bytes: Vec<u8>,
    /// `(offset, start, end)` of every intact frame's payload in `bytes`.
    pub frames: Vec<(u64, std::ops::Range<usize>)>,
    /// Where the torn tail began, now truncated away on disk.
    pub torn_at: Option<u64>,
}

impl Opened {
    pub fn payloads(&self) -> impl Iterator<Item = (u64, &[u8])> {
        self.frames
            .iter()
            .map(|(offset, range)| (*offset, &self.bytes[range.clone()]))
    }
}

/// An open log, positioned at its end: `catalog.log` and `manifest.log`.
pub struct Log {
    path: PathBuf,
    file: File,
    /// The byte the next frame starts at. Kept here rather than asked of the file so a
    /// failed append can be rolled back to a known boundary.
    end: u64,
    /// Set when a rollback failed. See `LogError::Torn`.
    torn: Option<u64>,
}

impl Log {
    /// Open for appending, reading every frame that is already there.
    ///
    /// A torn tail is truncated *on disk* here, not just skipped, so the next append lands
    /// on a frame boundary rather than behind bytes the next start would cut off again.
    ///
    /// **A file this call creates has its directory fsynced before anything is appended.**
    /// `sync_all` on the file makes its bytes durable and says nothing about the entry
    /// naming it: after a crash the first record can be on disk in a file that is not in
    /// the directory. Once, on creation - an existing file's entry was made durable by the
    /// start that created it. If that fsync fails the new file is removed before the error
    /// is returned, so the next start does not find an empty file and take it for truth.
    ///
    /// **A file that was already there is flushed before its records are used.** A process
    /// killed inside an append's flush leaves the record whole in the page cache, where
    /// this start reads it like any other - and then acts on it: a template the record
    /// defines is named by lines in the write-ahead log, a file it names takes row groups.
    /// Those are flushed, and would outlive a power cut that the record they rest on did
    /// not. One flush a start makes what was read what is on the disk.
    pub fn open_with(
        path: &Path,
        max: u32,
        sync_dir: &mut dyn FnMut(&Path) -> std::io::Result<()>,
    ) -> Result<(Self, Opened), LogError> {
        let display = path.display().to_string();
        let io = |source| LogError::Io {
            path: display.clone(),
            source,
        };
        let (bytes, created) = match fs::read(path) {
            Ok(bytes) => (bytes, false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (Vec::new(), true),
            Err(e) => return Err(io(e)),
        };
        let (frames, torn_at) = spans(&bytes, max);
        let end = torn_at.unwrap_or(bytes.len() as u64);

        // Not `append(true)`: an append-mode handle ignores `set_len` + seek for the
        // rollback in `append`, and `O_APPEND` writes land wherever the file ends, which
        // after a failed rollback is not a frame boundary.
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(io)?;
        if created {
            let dir = parent_of(path);
            if let Err(source) = sync_dir(dir) {
                drop(file);
                let _ = fs::remove_file(path);
                return Err(LogError::SyncDir {
                    path: dir.display().to_string(),
                    name: path
                        .file_name()
                        .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
                    source,
                });
            }
        }
        if end < bytes.len() as u64 {
            file.set_len(end).map_err(io)?;
        }
        if !created {
            file.sync_all().map_err(io)?;
        }
        file.seek(SeekFrom::Start(end)).map_err(io)?;
        Ok((
            Self {
                path: path.to_path_buf(),
                file,
                end,
                torn: None,
            },
            Opened {
                bytes,
                frames,
                torn_at,
            },
        ))
    }

    /// A log whose every append fails: the handle is read-only, so `write_all` is refused
    /// by the kernel. What a full disk or a dead mount looks like from here.
    #[cfg(test)]
    pub fn read_only(path: &Path) -> Result<Self, LogError> {
        let io = |source| LogError::Io {
            path: path.display().to_string(),
            source,
        };
        fs::write(path, b"").map_err(io)?;
        let file = File::open(path).map_err(io)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            end: 0,
            torn: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one frame per payload and return only once every byte is on disk: one
    /// `write_all`, one `sync_all`, so several records are one durable unit.
    ///
    /// On failure the file is cut back to where the frames began. If *that* fails the log
    /// refuses every later append (`LogError::Torn`): a record written behind a tear is a
    /// record the next start cannot see.
    pub fn append(&mut self, payloads: &[Vec<u8>]) -> Result<(), LogError> {
        let display = self.path.display().to_string();
        if let Some(offset) = self.torn {
            return Err(LogError::Torn {
                path: display,
                offset,
            });
        }
        let bytes: Vec<u8> = payloads.iter().flat_map(|p| frame(p)).collect();
        let start = self.end;
        let written = self
            .file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all());
        match written {
            Ok(()) => {
                self.end = start + bytes.len() as u64;
                Ok(())
            }
            Err(source) => {
                let rolled_back = self
                    .file
                    .set_len(start)
                    .and_then(|()| self.file.seek(SeekFrom::Start(start)))
                    .is_ok();
                if !rolled_back {
                    self.torn = Some(start);
                }
                Err(LogError::Io {
                    path: display,
                    source,
                })
            }
        }
    }

    /// Bytes on disk, as of the last successful append.
    pub fn len(&self) -> u64 {
        self.end
    }

    /// Whether a failed append could not be cut back: see `LogError::Torn`.
    pub fn is_torn(&self) -> bool {
        self.torn.is_some()
    }

    /// Flush the file as it stands. After a failed append that was cut back, this is what
    /// makes the cut durable: once it returns, no part of that append can be on disk.
    pub fn sync(&mut self) -> Result<(), LogError> {
        self.file.sync_all().map_err(|source| LogError::Io {
            path: self.path.display().to_string(),
            source,
        })
    }
}

/// The directory holding `path`; `.` for a bare file name.
pub fn parent_of(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// fsync a directory, so the entries in it survive a crash.
pub fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir).and_then(|handle| handle.sync_all())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_check_value_is_castagnolis() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn eight_zero_bytes_are_a_tear_and_not_an_empty_frame() {
        assert_eq!(read(&[0u8; 8], 0, u32::MAX), Next::Tear);
        assert_eq!(read(&[0u8; 64], 0, u32::MAX), Next::Tear);
    }

    #[test]
    fn a_length_past_the_end_is_a_tear_before_anything_is_read() {
        let mut bytes = frame(b"{}");
        bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(read(&bytes, 0, u32::MAX), Next::Tear);
    }

    #[test]
    fn every_cut_of_three_frames_reads_the_whole_ones_before_it() {
        let parts: Vec<Vec<u8>> = vec![b"a".to_vec(), b"bcd".to_vec(), b"efghij".to_vec()];
        let whole: Vec<u8> = parts.iter().flat_map(|p| frame(p)).collect();
        let mut ends = vec![0usize];
        for p in &parts {
            ends.push(ends.last().copied().unwrap_or(0) + HEADER + p.len());
        }
        for cut in 0..=whole.len() {
            let (frames, torn) = read_all(&whole[..cut], u32::MAX);
            let whole_ones = ends.iter().filter(|end| **end <= cut).count() - 1;
            assert_eq!(frames.len(), whole_ones, "cut at {cut}");
            let boundary = ends[whole_ones];
            assert_eq!(
                torn,
                (cut != boundary).then_some(boundary as u64),
                "cut at {cut}"
            );
        }
    }
}
