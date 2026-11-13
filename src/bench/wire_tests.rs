// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

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
            .write_all(b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n5\r\nde")
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
