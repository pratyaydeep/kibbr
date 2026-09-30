//! The least HTTP/1.1 client that `kibbr load` and `bench-storage --es` need: one request
//! per connection, a body in, a status and a body out.
//!
//! Hand-written over `TcpStream` rather than a client crate because the two callers ask
//! for so little - a JSON post to a server on this machine, and a few Elasticsearch calls -
//! and the tests already speak HTTP this way. `Connection: close` and reading to EOF is the
//! whole framing on the way out; on the way back a chunked body is undone, because an
//! Elasticsearch behind a proxy may send one.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Where an `http://host:port/prefix` URL points: the address to connect to, and the path
/// every request goes under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Url {
    pub address: String,
    pub prefix: String,
}

/// An `http://` URL split for `request`. `https://` is refused by name: this client speaks
/// plain HTTP, and a TLS endpoint would otherwise fail as an unreadable status line.
pub fn parse_url(url: &str) -> Result<Url, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("{url}: only http:// URLs are supported"))?;
    let (host, path) = match rest.find('/') {
        Some(at) => (&rest[..at], rest[at..].trim_end_matches('/')),
        None => (rest, ""),
    };
    if host.is_empty() {
        return Err(format!("{url}: no host"));
    }
    let address = if host.contains(':') {
        host.to_string()
    } else {
        format!("{host}:80")
    };
    Ok(Url {
        address,
        prefix: path.to_string(),
    })
}

/// One request, with any extra headers, and its answer's status and body.
pub fn request(
    address: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<(&str, &[u8])>,
    timeout: Duration,
) -> io::Result<(u16, Vec<u8>)> {
    let target = address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::other(format!("{address}: no address")))?;
    let mut stream = TcpStream::connect_timeout(&target, timeout.min(Duration::from_secs(10)))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    match body {
        Some((content_type, bytes)) => head.push_str(&format!(
            "Content-Type: {content_type}\r\nContent-Length: {}\r\n\r\n",
            bytes.len()
        )),
        None => head.push_str("Content-Length: 0\r\n\r\n"),
    }
    stream.write_all(head.as_bytes())?;
    if let Some((_, bytes)) = body {
        stream.write_all(bytes)?;
    }
    stream.flush()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    parse_response(&raw)
}

fn parse_response(raw: &[u8]) -> io::Result<(u16, Vec<u8>)> {
    let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| bad("an answer with no end to its headers"))?;
    let head = std::str::from_utf8(&raw[..split]).map_err(|_| bad("headers that are not UTF-8"))?;
    let body = &raw[split + 4..];
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| bad("an answer with no status"))?;
    let chunked = head.lines().skip(1).any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.trim().eq_ignore_ascii_case("transfer-encoding")
                && value.trim().eq_ignore_ascii_case("chunked")
        })
    });
    Ok((
        status,
        if chunked {
            dechunk(body)?
        } else {
            body.to_vec()
        },
    ))
}

fn dechunk(mut body: &[u8]) -> io::Result<Vec<u8>> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "a malformed chunked body");
    let mut out = Vec::new();
    loop {
        let eol = body.windows(2).position(|w| w == b"\r\n").ok_or_else(bad)?;
        let size_text = std::str::from_utf8(&body[..eol]).map_err(|_| bad())?;
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| bad())?;
        body = &body[eol + 2..];
        if size == 0 {
            return Ok(out);
        }
        if size.checked_add(2).is_none_or(|need| body.len() < need) {
            return Err(bad());
        }
        out.extend_from_slice(&body[..size]);
        body = &body[size + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_url_splits_into_an_address_and_a_prefix() {
        assert_eq!(
            parse_url("http://localhost:9200/").expect("url"),
            Url {
                address: "localhost:9200".into(),
                prefix: String::new()
            }
        );
        assert_eq!(parse_url("http://es/x").expect("url").address, "es:80");
        assert!(parse_url("https://es:9200").is_err());
    }

    #[test]
    fn a_chunked_answer_is_put_back_together() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
        assert_eq!(
            parse_response(raw).expect("parse"),
            (200, b"abcde".to_vec())
        );
    }
}
