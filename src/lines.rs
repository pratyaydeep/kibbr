//! What every read loop does to a line before anything looks at it.
//!
//! `parse`, `demo` and `query` each read NDJSON their own way - they disagree about what
//! a rejected line costs the run - but they must not disagree about where a line starts
//! and ends. Three copies of that is three chances for one of them to drift.

/// U+FEFF as UTF-8 - the three bytes a Windows writer puts at the front of a line.
const BOM: &[u8] = b"\xef\xbb\xbf";

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
