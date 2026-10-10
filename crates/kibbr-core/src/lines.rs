//! What every read loop does to a line before anything looks at it.
//!
//! `parse`, `demo` and `query` each read NDJSON their own way - they disagree about what
//! a rejected line costs the run - but they must not disagree about where a line starts
//! and ends. Three copies of that is three chances for one of them to drift.

use std::io::{self, BufRead};

/// U+FEFF as UTF-8 - the three bytes a Windows writer puts at the front of a line.
const BOM: &[u8] = b"\xef\xbb\xbf";

/// The most one line may hold, 64 MiB.
///
/// Every other limit in the system bounds the *shape* of a line - depth 8, depth 64 - and
/// each is checked after the bytes are in memory, which is fine because a line is small.
/// Nothing bounded the line itself, and the bytes of a line are read before anything can
/// have an opinion about it: a producer that writes no `\n` is not a malformed line the
/// detector rejects, it is a `read_until` that never returns. The cost is not a slow run,
/// it is an allocation that grows until the process is killed, with no line number
/// reported on the way because no line was ever completed.
///
/// Deliberately the same number as `serve::MAX_INGEST_BYTES`, and the equality is load
/// bearing: an `/ingest` body cannot exceed the body cap, so a line inside one cannot
/// exceed this, so `LineTooLong` is unreachable over HTTP and no shipper's batch changes
/// meaning. Raise one without the other and that stops being true.
///
/// Generous on purpose - a 10 MiB line is a stack trace in a JSON field, which is a real
/// log and must still parse.
pub const MAX_LINE_BYTES: usize = 64 * 1024 * 1024;

/// How far past the cap to look for the newline that ends an over-long line.
///
/// Derived rather than picked: the cap already says how long a line may be, so scanning a
/// second cap's worth without finding a terminator is not a long line, it is input that is
/// not newline-delimited - a binary file handed to `parse`, or a producer that never
/// writes a `\n`. Continuing to read that costs the whole input to reach an answer that
/// cannot arrive, so the run ends instead.
///
/// The price, stated where it is decided: a real file whose first line runs past
/// `MAX_LINE_BYTES + MAX_SKIP_BYTES` ends the run rather than skipping to the good lines
/// after it. Per-line isolation - one bad line costing only itself - still holds for every
/// line under the cap, which is every line anything sane produces.
pub const MAX_SKIP_BYTES: u64 = MAX_LINE_BYTES as u64;

/// What one turn of a read loop found.
pub enum Line {
    /// A line is in the caller's buffer, terminator and all.
    Read,

    /// The line ran past `MAX_LINE_BYTES`, and the buffer holds nothing worth looking at.
    ///
    /// Returned the moment the cap is passed, with the rest of the line still unread. The
    /// caller reports it and *then* calls `skip_to_eol`, and that order is the whole point:
    /// a line that never ends has no end to be diagnosed at, so the diagnostic has to come
    /// from the cap, which is a position that always arrives. Doing the skip first buys a
    /// more precise message - the true length of the line - at the price of never printing
    /// it on the input that most needs it.
    TooLong,

    Eof,
}

/// Where `skip_to_eol` left the reader.
pub enum Resync {
    /// On the first byte of the next line, or at EOF. Either way the loop carries on and
    /// the over-long line cost exactly one line number, leaving every other line intact.
    Done,

    /// No newline within `MAX_SKIP_BYTES`. Not a line the caller can step over, because
    /// there is no evidence the input has lines at all.
    NotDelimited { scanned: u64 },
}

/// Read one line into `raw`, refusing to hold more than `MAX_LINE_BYTES` of it.
///
/// Bytes, not `lines()`: that iterator turns one non-UTF-8 line into an `io::Error` and
/// kills the stream, which is the same abort by another route.
///
/// The cap is enforced by the reader rather than checked after the fact - the bytes past
/// it are never in memory at all - so an endless line costs a fixed amount of memory
/// instead of all of it.
///
/// On `TooLong` the reader is left mid-line. Nothing else here moves it, so a caller that
/// forgets `skip_to_eol` reads that line's next 64 MiB as a fresh line and inflates its own
/// numbering; the three read loops are the only callers and all three do both.
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

    // A cap's worth of a line nothing will look at. Handed back rather than carried for
    // the rest of the run - the buffer is reused across lines, so without this one
    // over-long line sets the process's floor at 64 MiB.
    raw.clear();
    raw.shrink_to_fit();
    Ok(Line::TooLong)
}

/// Step over the rest of an over-long line, so the next `read_line` starts on a real one.
///
/// Through `fill_buf`/`consume` rather than a second `read_until`, because the point is to
/// not hold these bytes: they are dropped a buffer at a time and never accumulate. This is
/// what keeps the numbering honest - the skipped bytes are the rest of *one* line, so the
/// run counts one, and an operator's line 3 is still the file's line 3.
///
/// Bounded by `MAX_SKIP_BYTES`, so this returns on every input. Unbounded it would not:
/// a stream that writes no further newline has no next line to resynchronise to, and the
/// search for one would consume the input forever. That was the shape of the original bug,
/// and reintroducing it here - after the diagnostic rather than before it - would be the
/// same silence one step further along.
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

/// Drop the line terminator, `\r\n` as well as `\n`. The final line of a file that ends
/// without one has nothing to strip.
pub fn strip_eol(raw: &[u8]) -> &[u8] {
    let line = raw.strip_suffix(b"\n").unwrap_or(raw);
    line.strip_suffix(b"\r").unwrap_or(line)
}

/// Drop a leading byte order mark.
///
/// U+FEFF is not whitespace and JSON has no provision for it, so a line starting with one
/// is rejected at offset 0 as `expected value` - a fault invisible in an editor, hiding
/// whatever else is wrong with the line behind it. Nothing obliges a reader to tolerate
/// the mark, but PowerShell's `Out-File` and several Windows log shippers write one by
/// default, so the bytes arrive in real input.
///
/// Per line, not once at the head of the file: `/ingest` is handed a body at a time, with
/// no line 1 to special-case, and a line opening with a mark is not valid JSON at any
/// position - so this only ever turns a rejection into a record, and never changes the
/// meaning of a line that parses today.
///
/// Called before `parse_line`, so the mark is gone from `raw_line` too and every offset a
/// rejection reports still indexes into the line that `/ingest` echoes back.
///
/// Only the UTF-8 encoding of it: a UTF-16 mark (`0xff 0xfe`) is left to be reported as
/// the encoding fault it is, where the byte value is most of the diagnosis. One mark, not
/// a run - a second is data, and stripping repeatedly has no principled place to stop.
pub fn strip_bom(line: &[u8]) -> &[u8] {
    line.strip_prefix(BOM).unwrap_or(line)
}
