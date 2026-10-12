//! The one record framing, and the append-only file built on it.
//!
//! One reader for `catalog.log`, `manifest.log`, v3 data frames and the write-ahead log:
//! one place where a length is checked against the bytes that remain before it is trusted.
//! `payload_len` is never 0, so a run of zero bytes is a tear and not empty records.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// `payload_len` + `crc`.
pub const HEADER: usize = 8;

/// CRC-32C (Castagnoli), by the CPU's CRC instruction where there is one and a table
/// where there is not.
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
    /// A whole frame whose CRC holds, `end` being the offset just past it.
    Frame { payload: &'a [u8], end: usize },
    /// Exactly at the end of the bytes: nothing torn, nothing more.
    End,
    /// Not a whole, intact frame; after a crash, the normal signature of a write that did
    /// not finish.
    Tear,
}

/// The frame starting at `at`, its length checked against `max` and the bytes that remain
/// before anything is sliced, so `0xFFFFFFFF` is a tear and never a 4GB read.
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

/// `read_all`, with each payload as its range of `bytes`, so `Log::open_with` can move the
/// bytes into `Opened` beside the ranges.
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
    /// An earlier append failed and could not be rolled back, so anything appended after it
    /// would sit behind a tear and be truncated away on the next start.
    #[error("{path}: an earlier append left a torn record at offset {offset} that could not be removed; no further records are written until the process restarts")]
    Torn { path: String, offset: u64 },
    /// The file was just created and its directory could not be fsynced, so the file's own
    /// entry may not survive a crash.
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
    /// Open for appending and read every frame already there, cutting a torn tail on disk,
    /// fsyncing a created file's directory and flushing an existing file: what is read is durable.
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

        // Not `append(true)`: an append-mode handle ignores the `set_len` + seek rollback in
        // `append`, and after a failed rollback would write where there is no frame boundary.
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

    /// A log whose every append fails, as on a full disk or a dead mount: the handle is
    /// read-only.
    #[cfg(any(test, feature = "test-support"))]
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

    /// Append one frame per payload as one durable unit, cutting the file back on failure;
    /// if the cut fails the log refuses every later append (`LogError::Torn`).
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

    pub fn is_empty(&self) -> bool {
        self.end == 0
    }

    /// Whether a failed append could not be cut back: see `LogError::Torn`.
    pub fn is_torn(&self) -> bool {
        self.torn.is_some()
    }

    /// Flush the file as it stands, which after a failed append that was cut back is what
    /// makes the cut durable.
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
