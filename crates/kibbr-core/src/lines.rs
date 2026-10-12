//! What every read loop does to a line before anything looks at it.
//!
//! `parse`, `demo` and `query` each read NDJSON their own way - they disagree about what
//! a rejected line costs the run - but they must not disagree about where a line starts
//! and ends. Three copies of that is three chances for one of them to drift.

use std::io::{self, BufRead};

/// U+FEFF as UTF-8 - the three bytes a Windows writer puts at the front of a line.
const BOM: &[u8] = b"\xef\xbb\xbf";

/// The most one line may hold, 64 MiB: the same as `serve::MAX_INGEST_BYTES`, so
/// `LineTooLong` is unreachable over HTTP - raise them together.
pub const MAX_LINE_BYTES: usize = 64 * 1024 * 1024;

/// How far past the cap to look for a newline before the input is taken as not
/// newline-delimited and the run ends.
pub const MAX_SKIP_BYTES: u64 = MAX_LINE_BYTES as u64;

/// What one turn of a read loop found.
pub enum Line {
    /// A line is in the caller's buffer, terminator and all.
    Read,

    /// The line ran past `MAX_LINE_BYTES`; returned before the rest is read, so an endless
    /// line is still reported.
    TooLong,

    Eof,
}

/// Where `skip_to_eol` left the reader.
pub enum Resync {
    /// On the first byte of the next line, or at EOF.
    Done,

    /// No newline within `MAX_SKIP_BYTES`: no evidence the input has lines at all.
    NotDelimited { scanned: u64 },
}

/// Read one line of bytes into `raw`, never holding more than `MAX_LINE_BYTES`; on
/// `TooLong` the reader is mid-line and the caller must `skip_to_eol`.
pub fn read_line<R: BufRead>(input: &mut R, raw: &mut Vec<u8>) -> io::Result<Line> {
    // One over the cap, so a line of exactly `MAX_LINE_BYTES` still has room for its
    // terminator and is a line rather than the first line to be refused.
    let budget = MAX_LINE_BYTES as u64 + 1;
    // Borrowed, not moved: `take` consumes its receiver, and the reader has to survive
    // to be read from again on the next line.
    let mut capped = io::Read::take(&mut *input, budget);
    let read = capped.read_until(b'\n', raw)? as u64;

    if read == 0 {
        return Ok(Line::Eof);
    }
    // Terminated - or short of the budget, which under `take` means the input ran out:
    // the last line of a file that ends without a newline, which is a line.
    if raw.last() == Some(&b'\n') || read < budget {
        return Ok(Line::Read);
    }

    // The buffer is reused across lines, so without this one over-long line sets the
    // process's floor at 64 MiB.
    raw.clear();
    raw.shrink_to_fit();
    Ok(Line::TooLong)
}

/// Step over the rest of an over-long line a buffer at a time, bounded by
/// `MAX_SKIP_BYTES` so it returns on a stream with no further newline.
pub fn skip_to_eol<R: BufRead>(input: &mut R) -> io::Result<Resync> {
    let mut scanned = 0u64;
    loop {
        let available = match input.fill_buf() {
            Ok(available) => available,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        // EOF: the input ended inside the over-long line, so there is no next one and
        // nothing was lost by not finding it.
        if available.is_empty() {
            return Ok(Resync::Done);
        }
        if let Some(at) = available.iter().position(|&b| b == b'\n') {
            input.consume(at + 1);
            return Ok(Resync::Done);
        }
        let len = available.len();
        input.consume(len);
        scanned += len as u64;
        if scanned >= MAX_SKIP_BYTES {
            return Ok(Resync::NotDelimited { scanned });
        }
    }
}

/// Drop the line terminator, `\r\n` as well as `\n`.
pub fn strip_eol(raw: &[u8]) -> &[u8] {
    let line = raw.strip_suffix(b"\n").unwrap_or(raw);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Drop one leading UTF-8 byte order mark, per line because `/ingest` has no line 1;
/// Windows writers emit it and JSON rejects it at offset 0.
pub fn strip_bom(line: &[u8]) -> &[u8] {
    line.strip_prefix(BOM).unwrap_or(line)
}
