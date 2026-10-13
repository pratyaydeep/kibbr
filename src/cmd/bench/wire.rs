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
mod tests {
    use super::*;
    use std::net::{Shutdown, TcpListener};
    use std::thread::{self, JoinHandle};

    const TIMEOUT: Duration = Duration::from_secs(20);

    /// `/health`, byte for byte as this server answers it.
    const HEALTH: &str = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                          content-length: 25\r\ndate: Wed, 07 Oct 2026 17:33:34 GMT\r\n\r\n\
                          {\"ok\":true,\"status\":\"ok\"}";

    /// An `/ingest` body this server refused at the door without reading it, byte for
    /// byte.
    const REFUSED: &str = "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\n\
                           retry-after: 1\r\nconnection: close\r\ncontent-length: 79\r\n\
                           date: Wed, 07 Oct 2026 17:33:47 GMT\r\n\r\n\
                           {\"ok\":false,\"error\":\"ingest is at capacity (1 concurrent bodies); \
                           retry in 1s\"}";

    /// A listener on a thread of its own, playing `script`; what the script returns is
    /// what the test asks it afterwards.
    fn listener<T: Send + 'static>(
        script: impl FnOnce(TcpListener) -> T + Send + 'static,
    ) -> (SocketAddr, JoinHandle<T>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        (addr, thread::spawn(move || script(listener)))
    }

    /// Read one request's head, a byte at a time so that nothing behind it is taken.
    fn read_head(stream: &mut TcpStream) -> Vec<u8> {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            assert_eq!(stream.read(&mut byte).unwrap(), 1, "a request cut short");
            head.push(byte[0]);
        }
        head
    }

    /// Read one request whole, head and announced body.
    fn read_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = read_head(stream);
        let length = parse_request_length(&request);
        let head = request.len();
        request.resize(head + length, 0);
        stream.read_exact(&mut request[head..]).unwrap();
        request
    }

    fn parse_request_length(head: &[u8]) -> usize {
        String::from_utf8_lossy(head)
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: ")?.parse().ok())
            .unwrap_or(0)
    }

    #[test]
    fn two_requests_go_down_one_socket_and_each_answer_is_read_to_its_length() {
        // The second answer is longer than one read, and arrives in two pieces.
        let long = "x".repeat(100_000);
        let sent = long.clone();
        let (addr, served) = listener(move |listener| {
            let (mut stream, _) = listener.accept().unwrap();
            let first = read_request(&mut stream);
            stream.write_all(HEALTH.as_bytes()).unwrap();
            let second = read_request(&mut stream);
            let (a, b) = sent.split_at(70_000);
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-length: 100000\r\n\r\n{a}"
            )
            .unwrap();
            thread::sleep(Duration::from_millis(30));
            stream.write_all(b.as_bytes()).unwrap();
            let third = read_request(&mut stream);
            stream.write_all(HEALTH.as_bytes()).unwrap();
            (first, second, third)
        });

        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let get = get_request("/health");
        let answer = conn.send(&get, &[]).unwrap();
        assert_eq!(
            (
                answer.status,
                answer.close,
                answer.retry_after,
                answer.wrote_all
            ),
            (200, false, None, true)
        );
        assert_eq!(conn.body(&answer), br#"{"ok":true,"status":"ok"}"#);

        let head = ingest_head(5);
        let answer = conn.send(&head, b"{}\n{}").unwrap();
        assert_eq!(answer.status, 200);
        assert_eq!(conn.body(&answer), long.as_bytes(), "one piece, all of it");

        // A short answer after a long one: the buffer still holds the long one's bytes
        // behind it, and they are not this answer's.
        let answer = conn.send(&get, &[]).unwrap();
        assert_eq!(conn.body(&answer), br#"{"ok":true,"status":"ok"}"#);

        let (first, second, third) = served.join().unwrap();
        assert_eq!(first, get, "the request's bytes, as built");
        assert_eq!(second, [&head[..], b"{}\n{}"].concat());
        assert_eq!(third, get, "and all three on the one socket accepted");
    }

    #[test]
    fn header_names_and_values_are_read_whatever_their_case() {
        // This server's spelling, the canonical one, and upper case: a reader that
        // matched one of them would wait out its timeout on the others.
        for (length, connection, retry) in [
            ("content-length", "connection: close", "retry-after"),
            ("Content-Length", "Connection: Close", "Retry-After"),
            ("CONTENT-LENGTH", "CONNECTION: CLOSE", "RETRY-AFTER"),
        ] {
            let text = format!(
                "HTTP/1.1 429 Too Many Requests\r\n{length}: 4\r\n{connection}\r\n{retry}: 7\r\n\r\nbusy"
            );
            let (addr, served) = listener(move |listener| {
                let (mut stream, _) = listener.accept().unwrap();
                read_request(&mut stream);
                stream.write_all(text.as_bytes()).unwrap();
                // Held open: only the length can say where this answer ends.
                read_request(&mut stream);
                stream.write_all(HEALTH.as_bytes()).unwrap();
            });
            let mut conn = Conn::open(addr, TIMEOUT).unwrap();
            let get = get_request("/stats");
            let answer = conn.send(&get, &[]).unwrap();
            assert_eq!(
                (answer.status, answer.close, answer.retry_after),
                (429, true, Some(7)),
                "{length}"
            );
            assert_eq!(conn.body(&answer), b"busy", "{length}");
            assert_eq!(conn.send(&get, &[]).unwrap().status, 200);
            served.join().unwrap();
        }
    }

    #[test]
    fn a_chunked_answer_is_joined_into_one_piece_and_the_connection_is_kept() {
        for encoding in [
            "transfer-encoding: chunked",
            "Transfer-Encoding: chunked",
            "TRANSFER-ENCODING: CHUNKED",
        ] {
            let (addr, served) = listener(move |listener| {
                let (mut stream, _) = listener.accept().unwrap();
                read_request(&mut stream);
                // Cut in the middle of a size line, of a chunk and of the last line, with
                // an extension on one chunk and a trailer after the last.
                for piece in [
                    &format!("HTTP/1.1 200 OK\r\n{encoding}\r\n\r\n9\r\n{{\"count\":\r"),
                    "\n1",
                    "5;ext=1\r\n1234,\"rows\":[]",
                    ",\"m\":1}\r\n0\r\nx-trailer: 1\r",
                    "\n\r\n",
                ] {
                    stream.write_all(piece.as_bytes()).unwrap();
                    thread::sleep(Duration::from_millis(20));
                }
                read_request(&mut stream);
                stream.write_all(HEALTH.as_bytes()).unwrap();
            });
            let mut conn = Conn::open(addr, TIMEOUT).unwrap();
            let request = query_request("COUNT LOG {}");
            let answer = conn.send(&request, &[]).unwrap();
            assert_eq!((answer.status, answer.close), (200, false), "{encoding}");
            assert_eq!(
                conn.body(&answer),
                br#"{"count":1234,"rows":[],"m":1}"#,
                "{encoding}"
            );
            // The next answer begins where the chunks ended.
            let answer = conn.send(&get_request("/health"), &[]).unwrap();
            assert_eq!(conn.body(&answer), br#"{"ok":true,"status":"ok"}"#);
            served.join().unwrap();
        }
    }

    #[test]
    fn a_refusal_says_close_and_the_next_request_goes_down_a_new_socket() {
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream.write_all(REFUSED.as_bytes()).unwrap();
            drop(stream);
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-length: 39\r\n\r\n\
                      {\"ok\":true,\"ingested\":2000,\"errors\":[]}",
                )
                .unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let head = ingest_head(3);
        let answer = conn.send(&head, b"{}\n").unwrap();
        assert_eq!(
            answer,
            Answer {
                status: 503,
                close: true,
                retry_after: Some(1),
                body: answer.body.clone(),
                wrote_all: true,
            }
        );
        assert_eq!(
            conn.body(&answer),
            br#"{"ok":false,"error":"ingest is at capacity (1 concurrent bodies); retry in 1s"}"#
        );
        assert_eq!(ingested(conn.body(&answer)), None);

        conn.reopen().unwrap();
        let answer = conn.send(&head, b"{}\n").unwrap();
        assert_eq!((answer.status, answer.close), (200, false));
        assert_eq!(ingested(conn.body(&answer)), Some(2_000));
        served.join().unwrap();
    }

    #[test]
    fn a_refusal_sent_before_the_body_was_read_is_still_an_answer() {
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            // The head only. The body is left in the socket, so closing it resets the
            // connection under the sender, as a server that refuses at the door does.
            read_head(&mut stream);
            thread::sleep(Duration::from_millis(50));
            stream.write_all(REFUSED.as_bytes()).unwrap();
            drop(stream);
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        // More than a socket's two buffers hold on any system, so the write is still
        // going when the refusal comes and the reset ends it.
        let body = vec![b'x'; 16 << 20];
        let answer = conn
            .send(&ingest_head(body.len()), &body)
            .expect("the refusal was in the socket, whichever way the write ended");
        assert_eq!(
            (answer.status, answer.close, answer.retry_after),
            (503, true, Some(1))
        );
        assert!(
            !answer.wrote_all,
            "16 MiB cannot have gone into a socket nobody reads"
        );
        served.join().unwrap();
    }

    #[test]
    fn a_socket_closed_without_an_answer_is_an_error_that_says_what_was_written() {
        // Closed after the whole request was read: it may have been taken.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream.shutdown(Shutdown::Both).unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let error = conn.send(&ingest_head(3), b"{}\n").unwrap_err();
        assert!(error.wrote_all);
        assert!(
            error.to_string().starts_with("no answer was read: "),
            "{error}"
        );
        served.join().unwrap();

        // Closed with the body unread and nothing said: it cannot have been taken.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_head(&mut stream);
            thread::sleep(Duration::from_millis(50));
            drop(stream);
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let body = vec![b'x'; 16 << 20];
        let error = conn.send(&ingest_head(body.len()), &body).unwrap_err();
        assert!(!error.wrote_all);
        served.join().unwrap();

        // Half a status line is not an answer.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream.write_all(b"HTTP/1.1 20").unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        assert!(conn.send(&get_request("/health"), &[]).is_err());
        served.join().unwrap();

        // Nor is something that is not HTTP.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream.write_all(b"220 smtp ready\r\n\r\n").unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let error = conn.send(&get_request("/health"), &[]).unwrap_err();
        assert_eq!(error.source.kind(), io::ErrorKind::InvalidData);
        served.join().unwrap();
    }

    #[test]
    fn an_answer_cut_short_after_its_status_is_an_answer_on_a_socket_that_is_done() {
        // Inside the head: the status and the headers that came whole are read.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 503 Service Unavailable\r\nretry-after: 1\r\ncontent-len")
                .unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let answer = conn.send(&get_request("/health"), &[]).unwrap();
        assert_eq!(
            (answer.status, answer.close, answer.retry_after),
            (503, true, Some(1))
        );
        assert_eq!(conn.body(&answer), b"");
        served.join().unwrap();

        // Inside the body: what came is the body, and it does not parse.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 39\r\n\r\n{\"ok\":true,\"inges")
                .unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let answer = conn.send(&ingest_head(3), b"{}\n").unwrap();
        assert_eq!((answer.status, answer.close), (200, true));
        assert_eq!(conn.body(&answer), b"{\"ok\":true,\"inges");
        assert_eq!(ingested(conn.body(&answer)), None);
        served.join().unwrap();

        // A chunked body that stops before its last chunk.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n5\r\nde",
                )
                .unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let answer = conn.send(&get_request("/health"), &[]).unwrap();
        assert_eq!((answer.status, answer.close), (200, true));
        assert_eq!(conn.body(&answer), b"abc");
        served.join().unwrap();
    }

    #[test]
    fn an_answer_with_no_length_ends_where_the_server_closes() {
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.0 200 OK\r\n\r\nall of it")
                .unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let answer = conn.send(&get_request("/health"), &[]).unwrap();
        assert_eq!((answer.status, answer.close), (200, true));
        assert_eq!(conn.body(&answer), b"all of it");
        served.join().unwrap();

        // An answer that has no body by its status is not waited on for one.
        let (addr, served) = listener(|listener| {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
                .unwrap();
            read_request(&mut stream);
            stream.write_all(HEALTH.as_bytes()).unwrap();
        });
        let mut conn = Conn::open(addr, TIMEOUT).unwrap();
        let answer = conn.send(&get_request("/health"), &[]).unwrap();
        assert_eq!((answer.status, answer.close), (204, false));
        assert_eq!(conn.body(&answer), b"");
        assert_eq!(conn.send(&get_request("/health"), &[]).unwrap().status, 200);
        served.join().unwrap();
    }

    #[test]
    fn a_head_is_read_line_by_whole_line() {
        let head = parse_head(
            b"HTTP/1.1 503 Service Unavailable\r\nConnection: keep-alive, Close\r\n\
              Transfer-Encoding: gzip, Chunked\r\ncontent-length: 12\r\nretry-after: 1\r\n\r\n",
        )
        .unwrap();
        assert_eq!(
            (
                head.status,
                head.close,
                head.chunked,
                head.length,
                head.retry_after
            ),
            (503, true, true, Some(12), Some(1))
        );

        // A length whose line has not ended may have more digits coming.
        let head = parse_head(b"HTTP/1.1 200 OK\r\ncontent-length: 12").unwrap();
        assert_eq!(head.length, None);
        // A date is the other form of `retry-after`, and is not seconds.
        let head =
            parse_head(b"HTTP/1.1 503 Busy\r\nretry-after: Mon, 15 Jan 2024 12:00:00 GMT\r\n\r\n")
                .unwrap();
        assert_eq!(head.retry_after, None);

        // An answer in the older protocol closes its socket unless it says otherwise.
        let old = |head: &[u8]| parse_head(head).unwrap().close;
        assert!(old(b"HTTP/1.0 200 OK\r\ncontent-length: 2\r\n\r\n"));
        assert!(!old(
            b"HTTP/1.0 200 OK\r\nConnection: Keep-Alive\r\ncontent-length: 2\r\n\r\n"
        ));
        assert!(!old(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n"));

        assert!(parse_head(b"HTTP/1.1 200 OK").is_none(), "no line yet");
        assert!(parse_head(b"HTTP/1.1 20\r\n").is_none());
        assert!(parse_head(b"HTTP/1.1 2000 OK\r\n").is_none());
        assert!(parse_head(b"ICY 200 OK\r\n").is_none());

        assert_eq!(chunk_size(b"1a"), Some(26));
        assert_eq!(chunk_size(b"1A ; name=value"), Some(26));
        assert_eq!(chunk_size(b"0"), Some(0));
        assert_eq!(chunk_size(b""), None);
        assert_eq!(chunk_size(b"+1"), None);
        assert_eq!(chunk_size(b"ffffffffffffffffff"), None);
    }

    #[test]
    fn the_bytes_of_each_request_are_fixed() {
        assert_eq!(
            String::from_utf8(query_request(r#"COUNT LOG {"event":"login_ok"}"#)).unwrap(),
            "POST /query HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
             Content-Length: 42\r\n\r\n{\"q\":\"COUNT LOG {\\\"event\\\":\\\"login_ok\\\"}\"}"
        );
        assert_eq!(
            String::from_utf8(ingest_head(978_112)).unwrap(),
            "POST /ingest?received_at=2024-01-15T12:00:00Z HTTP/1.1\r\nHost: 127.0.0.1\r\n\
             Content-Type: application/x-ndjson\r\nContent-Length: 978112\r\n\r\n"
        );
        assert_eq!(
            String::from_utf8(get_request("/stats")).unwrap(),
            "GET /stats HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        );

        // The body is the text as JSON reads it back, whatever the text holds.
        let text = "MATCH LOG {\"msg\":\"a\\\\b \u{e9}\"}";
        let request = query_request(text);
        let body = &request[find(&request, b"\r\n\r\n").unwrap() + 4..];
        assert_eq!(parse_request_length(&request), body.len());
        let read: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(read, serde_json::json!({ "q": text }));
    }

    #[test]
    fn a_query_answer_is_its_count_and_what_meta_holds() {
        // An answer of this server's, with a row put in to be stepped over.
        let body = br#"{"exists":true,"count":227,"rows":[{"a":{"b":[1,2]}}],"groups":[],"meta":{"templates_matched":1,"template_ids":[119],"files_opened":1,"leaves_opened":1,"row_groups_read":1,"row_groups_pruned":0,"bytes_read":1249,"rows_scanned":227,"rows_returned":0,"truncated":false,"limit_source":"unlimited","next_cursor":null,"duration_ms":0.2455}}"#;
        let answer = query_answer(body).unwrap();
        assert_eq!(
            answer,
            QueryAnswer {
                count: 227,
                duration_ms: Some(0.2455),
                rows_scanned: Some(227),
                rows_returned: Some(0),
                templates_matched: Some(1),
                row_groups_read: Some(1),
                row_groups_pruned: Some(0),
                bytes_read: Some(1249),
                files_opened: Some(1),
                leaves_opened: Some(1),
                truncated: Some(false),
                limit_source: Some("unlimited".to_string()),
            }
        );
        assert_eq!(
            answer.expect(),
            Expect {
                count: 227,
                rows_returned: Some(0),
                templates_matched: Some(1),
                truncated: Some(false),
                limit_source: Some("unlimited".to_string()),
            }
        );

        // A field that is gone, or is spelled as another type, is missing; the rest
        // of the answer is still read.
        let answer = query_answer(
            br#"{"count":3,"meta":{"rows_scanned":"many","duration_ms":null,"truncated":0,"bytes_read":9}}"#,
        )
        .unwrap();
        assert_eq!(
            answer,
            QueryAnswer {
                count: 3,
                bytes_read: Some(9),
                ..QueryAnswer::default()
            }
        );
        assert_eq!(query_answer(br#"{"count":3,"meta":[]}"#).unwrap().count, 3);
        assert_eq!(query_answer(br#"{"count":3}"#).unwrap().rows_scanned, None);
    }

    #[test]
    fn an_answer_with_no_count_is_not_a_sample() {
        for body in [
            &br#"{"exists":true,"rows":[]}"#[..],
            br#"{"count":"227"}"#,
            br#"{"count":-1}"#,
            br#"{"count":22"#,
            b"not found",
        ] {
            let error = query_answer(body).unwrap_err();
            assert!(
                matches!(
                    error,
                    SpeedError::Surface {
                        surface: "POST /query",
                        ..
                    }
                ),
                "{error}"
            );
            assert!(
                error.to_string().contains(&*String::from_utf8_lossy(body)),
                "the body is in the sentence: {error}"
            );
        }
        // A long body is shown by its start.
        let long = format!(r#"{{"rows":["{}"]}}"#, "x".repeat(1_000));
        let error = query_answer(long.as_bytes()).unwrap_err().to_string();
        assert!(error.len() < 500 && error.ends_with("xxx..."), "{error}");
    }

    #[test]
    fn the_lines_a_server_took_are_read_from_its_answer() {
        assert_eq!(
            ingested(br#"{"ok":true,"ingested":2000,"errors":[]}"#),
            Some(2_000)
        );
        assert_eq!(
            ingested(br#"{"ok":false,"ingested":1998,"errors":[{"line":3,"error":"x"}]}"#),
            Some(1_998)
        );
        assert_eq!(ingested(br#"{"ok":true}"#), None);
        assert_eq!(ingested(br#"{"ingested":"2000"}"#), None);
        assert_eq!(ingested(b""), None);
    }
}
