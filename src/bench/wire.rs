// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! One kept HTTP/1.1 connection, and the bytes of every request sent down it.
//! The bench has a client of its own because the crate's opens a connection per request,
//! which would time the handshake, and hides a refusal's headers. `send` reads one answer
//! whether or not the write succeeded: a server may refuse a body before reading it, and a
//! body not all written (`wrote_all`) cannot have been taken, so sending it again is safe.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::ops::Range;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};
use thiserror::Error;

use super::report::Expect;
use super::{SpeedError, RECEIVED_AT};

/// The host every request names. No port, so the bytes of a request are the same for
/// every server of every run; this server answers it.
const HOST: &str = "127.0.0.1";

/// What the buffer grows to on the first read: the head and the body of every answer but
/// a fetch's arrive in one read of this size.
const FIRST_READ: usize = 16 * 1024;

/// A head that has not ended by here is not one, and the reading stops.
const HEAD_LIMIT: usize = 64 * 1024;

/// A body announced as longer than this is not read: the largest answer the bench asks
/// for is a fetch of 10,000 rows, and a length past this is a length misread.
const BODY_LIMIT: usize = 1 << 30;

/// Bytes of a body an error quotes.
const EXCERPT: usize = 300;

/// One kept connection to a server on loopback.
pub struct Conn {
    stream: TcpStream,
    addr: SocketAddr,
    /// The last answer, head and body; `Answer::body` is a range of it. It keeps the length
    /// it has grown to, so that no byte of it is cleared again for each answer.
    buf: Vec<u8>,
    timeout: Duration,
}

/// One answer, as far as the bench reads it; `body` is a range of the connection's buffer
/// and holds until the next `send`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    /// The answer said `connection: close`, or ended before its announced end: either
    /// way nothing more can be sent down this socket.
    pub close: bool,
    /// `retry-after`, in seconds: recorded, not obeyed.
    pub retry_after: Option<u64>,
    pub body: Range<usize>,
    /// Whether the whole request was written before the answer was read; false is a
    /// refusal that came before the server had read the body.
    pub wrote_all: bool,
}

/// No status line was read; if `wrote_all` is false the server cannot have taken the
/// request's body.
#[derive(Debug, Error)]
#[error("no answer was read: {source}")]
pub struct SendError {
    pub wrote_all: bool,
    pub source: io::Error,
}

/// What the bench reads of a query's answer: `count`, and of `meta` what is there, with
/// `rows` and `groups` stepped over and never built.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryAnswer {
    pub count: u64,
    pub duration_ms: Option<f64>,
    pub rows_scanned: Option<u64>,
    pub rows_returned: Option<u64>,
    pub templates_matched: Option<u64>,
    pub row_groups_read: Option<u64>,
    pub row_groups_pruned: Option<u64>,
    pub bytes_read: Option<u64>,
    pub files_opened: Option<u64>,
    pub leaves_opened: Option<u64>,
    pub truncated: Option<bool>,
    pub limit_source: Option<String>,
}

impl QueryAnswer {
    /// The part of an answer that two asks of one query over the same bytes share.
    pub fn expect(&self) -> Expect {
        Expect {
            count: self.count,
            rows_returned: self.rows_returned,
            templates_matched: self.templates_matched,
            truncated: self.truncated,
            limit_source: self.limit_source.clone(),
        }
    }
}

/// What the head of an answer says about the answer.
struct Head {
    status: u16,
    close: bool,
    retry_after: Option<u64>,
    /// `content-length`, where the head has one that is a number.
    length: Option<usize>,
    chunked: bool,
}

impl Conn {
    /// Connect with `timeout` on connect, read and write, and with `TCP_NODELAY`, since a
    /// small body on a default socket can wait behind a delayed acknowledgement.
    pub fn open(addr: SocketAddr, timeout: Duration) -> io::Result<Conn> {
        Ok(Conn {
            stream: connect(addr, timeout)?,
            addr,
            buf: vec![0; FIRST_READ],
            timeout,
        })
    }

    /// Make room for an answer of `bytes` before it is asked for, so that a connection asked
    /// a large one once does not grow inside that one sample.
    pub fn reserve(&mut self, bytes: usize) {
        if self.buf.len() < bytes {
            self.buf.resize(bytes, 0);
        }
    }

    /// Drop the socket and connect again to the same address, keeping the buffer: after an
    /// answer that said `connection: close`, or an attempt that read none.
    pub fn reopen(&mut self) -> io::Result<()> {
        self.stream = connect(self.addr, self.timeout)?;
        Ok(())
    }

    /// Write `head` and then `body`, and read one answer: `Ok` whenever a status line was
    /// read, even after a failed write or a cut-short answer, since a refusal is an answer.
    pub fn send(&mut self, head: &[u8], body: &[u8]) -> Result<Answer, SendError> {
        let written = self.write(head, body);
        match (self.read(), written) {
            (Ok(answer), written) => Ok(Answer {
                wrote_all: written.is_ok(),
                ..answer
            }),
            (Err(_), Err(source)) => Err(SendError {
                wrote_all: false,
                source,
            }),
            (Err(source), Ok(())) => Err(SendError {
                wrote_all: true,
                source,
            }),
        }
    }

    /// The body of the last answer.
    pub fn body(&self, a: &Answer) -> &[u8] {
        self.buf.get(a.body.clone()).unwrap_or(&[])
    }

    /// The request, in at most two writes: a query's head and body are one buffer, and
    /// an `/ingest` body is not copied behind its head to make them one.
    fn write(&mut self, head: &[u8], body: &[u8]) -> io::Result<()> {
        self.stream.write_all(head)?;
        if !body.is_empty() {
            self.stream.write_all(body)?;
        }
        Ok(())
    }

    /// One answer off the socket. `Err` when no status line could be read.
    fn read(&mut self) -> io::Result<Answer> {
        // Bytes of this answer in the buffer so far. What the buffer holds past it is the
        // answer before, or nothing.
        let mut end = 0usize;

        // The head, up to its empty line. Each look starts three bytes back, where an
        // empty line that the last read cut in two would begin.
        let mut looked = 0usize;
        let body = loop {
            let from = looked.saturating_sub(3);
            if let Some(at) = find(&self.buf[from..end], b"\r\n\r\n") {
                break from + at + 4;
            }
            looked = end;
            let more = if end < HEAD_LIMIT {
                self.fill(&mut end, usize::MAX)
            } else {
                Err(invalid("an answer whose head does not end"))
            };
            if let Err(error) = more {
                // Cut short inside the head. With the status line read it is still an
                // answer, on a socket that is done.
                return match parse_head(&self.buf[..end]) {
                    Some(head) => Ok(head.answer(end..end, false)),
                    None => Err(error),
                };
            }
        };
        let Some(head) = parse_head(&self.buf[..body]) else {
            return Err(invalid("an answer that does not begin with a status line"));
        };

        let (stop, whole) = if matches!(head.status, 100..=199 | 204 | 304) {
            // These carry no body, whatever their head says.
            (body, true)
        } else if head.chunked {
            self.chunked(&mut end, body)
        } else if let Some(length) = head.length {
            self.counted(&mut end, body, length)
        } else {
            // Neither a length nor chunks: the body is what comes before the server
            // closes the socket, which is then done.
            while self.fill(&mut end, usize::MAX).is_ok() {}
            (end, false)
        };
        Ok(head.answer(body..stop, whole))
    }

    /// A body of `length` bytes from `body`: where it stops, and whether all of it came.
    fn counted(&mut self, end: &mut usize, body: usize, length: usize) -> (usize, bool) {
        if length > BODY_LIMIT {
            return (body, false);
        }
        let stop = body + length;
        if self.buf.len() < stop {
            self.buf.resize(stop, 0);
        }
        while *end < stop {
            if self.fill(end, stop - *end).is_err() {
                return (*end, false);
            }
        }
        (stop, true)
    }

    /// A chunked body from `body`, its chunks moved down to stand in one piece: where it
    /// stops, and whether it came whole. A chunk only moves forward, over size lines already read.
    fn chunked(&mut self, end: &mut usize, body: usize) -> (usize, bool) {
        // Where the next size line begins, and where the joined body ends so far.
        let mut at = body;
        let mut stop = body;
        loop {
            let Some(line) = self.line(end, at) else {
                return (stop, false);
            };
            let Some(size) = chunk_size(&self.buf[at..line]) else {
                return (stop, false);
            };
            let data = line + 2;
            if size == 0 {
                // The last chunk. Trailer lines may follow, up to an empty one.
                at = data;
                loop {
                    match self.line(end, at) {
                        Some(line) if line == at => return (stop, true),
                        Some(line) => at = line + 2,
                        None => return (stop, false),
                    }
                }
            }
            if size > BODY_LIMIT {
                return (stop, false);
            }
            // The chunk, and the `\r\n` that closes it.
            let next = data + size + 2;
            if self.buf.len() < next {
                self.buf.resize(next, 0);
            }
            while *end < next {
                if self.fill(end, next - *end).is_err() {
                    return (stop, false);
                }
            }
            self.buf.copy_within(data..data + size, stop);
            stop += size;
            at = next;
        }
    }

    /// Where the line that begins at `at` ends - the place of its `\r\n` - reading more
    /// until it has one. `None` when the answer ends first.
    fn line(&mut self, end: &mut usize, at: usize) -> Option<usize> {
        let mut looked = at;
        loop {
            let from = looked.saturating_sub(1).max(at);
            if let Some(found) = find(&self.buf[from..*end], b"\r\n") {
                return Some(from + found);
            }
            looked = *end;
            if *end - at > HEAD_LIMIT || self.fill(end, usize::MAX).is_err() {
                return None;
            }
        }
    }

    /// Read once, `most` bytes at most, behind the `*end` bytes this answer has so far.
    /// The end of the stream is an error here: every caller is waiting for more.
    fn fill(&mut self, end: &mut usize, most: usize) -> io::Result<()> {
        if *end == self.buf.len() {
            // Doubled, so a long answer is not grown a read at a time; cleared once,
            // here, and not again for the answers that follow.
            let longer = (self.buf.len() * 2).max(FIRST_READ);
            self.buf.resize(longer, 0);
        }
        let upto = self.buf.len().min(end.saturating_add(most));
        loop {
            match self.stream.read(&mut self.buf[*end..upto]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "the server closed the connection",
                    ))
                }
                Ok(read) => {
                    *end += read;
                    return Ok(());
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl Head {
    /// The answer this head begins, with `body` of the buffer as its body. An answer
    /// that did not come whole leaves the socket where no next answer can be found.
    fn answer(&self, body: Range<usize>, whole: bool) -> Answer {
        Answer {
            status: self.status,
            close: self.close || !whole,
            retry_after: self.retry_after,
            body,
            // `send` knows; a read does not.
            wrote_all: true,
        }
    }
}

/// A socket to `addr` with `TCP_NODELAY` and `timeout` on both directions.
fn connect(addr: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&addr, timeout)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(stream)
}

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_string())
}

/// Where `needle` first stands in `bytes`.
fn find(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes.windows(needle.len()).position(|w| w == needle)
}

/// Read a head, or as much of one as has arrived: only lines that have their `\r\n` are
/// read, so a header cut in two is not taken for a shorter one.
fn parse_head(bytes: &[u8]) -> Option<Head> {
    let mut rest = bytes;
    let mut next = || {
        let at = find(rest, b"\r\n")?;
        let line = &rest[..at];
        rest = &rest[at + 2..];
        Some(line)
    };

    // `HTTP/1.1 503 Service Unavailable`: the second word, three digits.
    let mut words = next()?.split(|byte| *byte == b' ');
    let version = words.next().filter(|word| word.starts_with(b"HTTP/"))?;
    let status = words.next().filter(|word| word.len() == 3)?;
    // Before 1.1 a socket is closed after its answer unless the answer says it is kept.
    let mut kept = version != b"HTTP/1.0";
    let mut head = Head {
        status: number(status)?,
        close: false,
        retry_after: None,
        length: None,
        chunked: false,
    };

    while let Some(line) = next() {
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        let name = line[..colon].trim_ascii();
        let value = line[colon + 1..].trim_ascii();
        // Without regard to case, names and values both: this server writes them in
        // lower case, a proxy in front of a later one may not.
        if name.eq_ignore_ascii_case(b"content-length") {
            head.length = number(value);
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            head.chunked |= lists(value, b"chunked");
        } else if name.eq_ignore_ascii_case(b"connection") {
            head.close |= lists(value, b"close");
            kept |= lists(value, b"keep-alive");
        } else if name.eq_ignore_ascii_case(b"retry-after") {
            // Seconds. The other form, a date, is not read: the wait is the bench's own.
            head.retry_after = number(value);
        }
    }
    head.close |= !kept;
    Some(head)
}

/// Whether a header's value, a list with commas between, names `word`.
fn lists(value: &[u8], word: &[u8]) -> bool {
    value
        .split(|byte| *byte == b',')
        .any(|item| item.trim_ascii().eq_ignore_ascii_case(word))
}

/// A decimal number that is all of `text`.
fn number<T: std::str::FromStr>(text: &[u8]) -> Option<T> {
    if text.is_empty() || !text.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(text).ok()?.parse().ok()
}

/// The length a chunk's size line gives: hex, up to a `;` where an extension begins.
fn chunk_size(line: &[u8]) -> Option<usize> {
    let digits = line.split(|byte| *byte == b';').next()?.trim_ascii();
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    usize::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()
}

/// A query's request, `POST /query` with `{"q":"<trieql>"}`, head and body in one buffer
/// so that it is one write.
pub fn query_request(trieql: &str) -> Vec<u8> {
    // The text as a JSON string, with its quotes escaped.
    let body = format!(r#"{{"q":{}}}"#, serde_json::Value::from(trieql));
    format!(
        "POST /query HTTP/1.1\r\nHost: {HOST}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// The head of an `/ingest` request for a body of `body_len` bytes of NDJSON, not
/// compressed, to `POST /ingest?received_at=` the arrival time every body is given.
pub fn ingest_head(body_len: usize) -> Vec<u8> {
    format!(
        "POST /ingest?received_at={RECEIVED_AT} HTTP/1.1\r\nHost: {HOST}\r\n\
         Content-Type: application/x-ndjson\r\nContent-Length: {body_len}\r\n\r\n"
    )
    .into_bytes()
}

/// A `GET` of `path`, for `/health` and `/stats`.
pub fn get_request(path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: {HOST}\r\n\r\n").into_bytes()
}

/// The start of a body, for an error that has to show what a server said, with whatever
/// is not UTF-8 replaced.
pub fn excerpt(body: &[u8]) -> String {
    let shown = String::from_utf8_lossy(&body[..body.len().min(EXCERPT)]);
    if body.len() > EXCERPT {
        format!("{shown}...")
    } else {
        shown.into_owned()
    }
}

/// A field as a `T`, or nothing where the answer holds something that is not one: a field
/// the bench only records must not stop a run because a later build spells it another way.
fn lenient<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(serde_json::from_value(value).ok())
}

/// A query's answer as it is read: the fields below, and every other one - `rows` and
/// `groups` among them - stepped over without being built.
#[derive(Deserialize)]
struct QueryBody {
    #[serde(default, deserialize_with = "lenient")]
    count: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    meta: Option<Meta>,
}

#[derive(Default, Deserialize)]
struct Meta {
    #[serde(default, deserialize_with = "lenient")]
    duration_ms: Option<f64>,
    #[serde(default, deserialize_with = "lenient")]
    rows_scanned: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    rows_returned: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    templates_matched: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    row_groups_read: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    row_groups_pruned: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    bytes_read: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    files_opened: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    leaves_opened: Option<u64>,
    #[serde(default, deserialize_with = "lenient")]
    truncated: Option<bool>,
    #[serde(default, deserialize_with = "lenient")]
    limit_source: Option<String>,
}

/// Read a 200's body from `/query`, where a body with no `count` or that is not JSON is
/// `SpeedError::Surface` and a `meta` field the bench cannot read is `None`.
pub fn query_answer(body: &[u8]) -> Result<QueryAnswer, SpeedError> {
    let surface = |detail: String| SpeedError::Surface {
        surface: "POST /query",
        detail,
    };
    let read: QueryBody = serde_json::from_slice(body).map_err(|error| {
        surface(format!(
            "its {} byte body is not a JSON object ({error}): {}",
            body.len(),
            excerpt(body)
        ))
    })?;
    let count = read.count.ok_or_else(|| {
        surface(format!(
            "its body holds no `count` that is a whole number: {}",
            excerpt(body)
        ))
    })?;
    let meta = read.meta.unwrap_or_default();
    Ok(QueryAnswer {
        count,
        duration_ms: meta.duration_ms,
        rows_scanned: meta.rows_scanned,
        rows_returned: meta.rows_returned,
        templates_matched: meta.templates_matched,
        row_groups_read: meta.row_groups_read,
        row_groups_pruned: meta.row_groups_pruned,
        bytes_read: meta.bytes_read,
        files_opened: meta.files_opened,
        leaves_opened: meta.leaves_opened,
        truncated: meta.truncated,
        limit_source: meta.limit_source,
    })
}

/// The `ingested` count of a 200 from `/ingest`, the lines the server took of the body;
/// `None` where the body does not hold one.
pub fn ingested(body: &[u8]) -> Option<u64> {
    #[derive(Deserialize)]
    struct Taken {
        ingested: Option<u64>,
    }
    serde_json::from_slice::<Taken>(body).ok()?.ingested
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod tests;
