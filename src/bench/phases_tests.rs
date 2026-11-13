// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use super::*;

fn upto(n: u32) -> Vec<f64> {
    // Not in order, so that the rule is seen to sort.
    (1..=n).rev().map(f64::from).collect()
}

#[test]
fn a_percentile_is_the_nearest_rank_for_every_count() {
    assert_eq!(percentile::<f64>(&[], 0.50), None);

    assert_eq!(percentile(&[7.0], 0.50), Some(7.0));
    assert_eq!(percentile(&[7.0], 0.99), Some(7.0));

    assert_eq!(percentile(&[2.0, 1.0], 0.50), Some(1.0));
    assert_eq!(percentile(&[2.0, 1.0], 0.99), Some(2.0));

    // With 100, the 50th and the 99th smallest: the p99 is the second largest.
    assert_eq!(percentile(&upto(100), 0.50), Some(50.0));
    assert_eq!(percentile(&upto(100), 0.99), Some(99.0));

    assert_eq!(percentile(&upto(101), 0.50), Some(51.0));
    assert_eq!(percentile(&upto(101), 0.99), Some(100.0));
}

#[test]
fn a_rank_that_is_a_whole_number_is_not_rounded_up_past_itself() {
    // 0.07 x 100 is 7.000000000000001 in floating point, and its ceiling is 8.
    assert_eq!(percentile(&upto(100), 0.07), Some(7.0));
    assert_eq!(percentile(&upto(100), 0.14), Some(14.0));
    assert_eq!(percentile(&upto(200), 0.99), Some(198.0));
    assert_eq!(percentile(&upto(100), 1.0), Some(100.0));
    assert_eq!(percentile(&upto(100), 0.0), Some(1.0));
}

#[test]
fn a_median_is_one_of_the_values() {
    assert_eq!(median(&[4.0]), Some(4.0));
    assert_eq!(median(&[9.0, 1.0, 5.0]), Some(5.0));
    assert_eq!(median(&[3_u64, 1, 2, 4]), Some(2));
    assert_eq!(median::<u64>(&[]), None);
}

// ── One body, against a listener that plays the server ───────────────────────────

/// What the listener does with one accepted connection.
#[derive(Clone, Copy)]
enum Play {
    /// Close it without reading or writing.
    Close,
    /// Read the head only, refuse with a 503 as this server writes one, and close
    /// while the client is still writing the body.
    RefuseEarly,
    /// Read the whole request and answer 200 with this `ingested` count.
    Take(u64),
}

/// The refusal this server writes when its ingest is at capacity, byte for byte.
const REFUSAL: &str = "{\"ok\":false,\"error\":\"ingest is at capacity (1 concurrent \
                           bodies); retry in 1s\"}";

/// Read up to the end of a request's head; return the head and what came after it.
fn read_head(stream: &mut TcpStream) -> (String, Vec<u8>) {
    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        if let Some(at) = seen.windows(4).position(|w| w == b"\r\n\r\n") {
            let rest = seen.split_off(at + 4);
            return (String::from_utf8(seen).expect("a head is text"), rest);
        }
        let n = stream.read(&mut buf).expect("read a head");
        assert!(n > 0, "the request ended inside its head");
        seen.extend_from_slice(&buf[..n]);
    }
}

/// A listener that plays `plays` in order, one accepted connection each, and then
/// goes away. Returns its address.
fn listener(plays: Vec<Play>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("an address");
    thread::spawn(move || {
        for play in plays {
            let (mut stream, _) = listener.accept().expect("accept");
            match play {
                Play::Close => {}
                Play::RefuseEarly => {
                    read_head(&mut stream);
                    let _ = write!(
                        stream,
                        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: \
                             application/json\r\nretry-after: 1\r\nconnection: close\r\n\
                             content-length: {}\r\n\r\n{REFUSAL}",
                        REFUSAL.len()
                    );
                }
                Play::Take(ingested) => {
                    let (head, rest) = read_head(&mut stream);
                    let length: usize = head
                        .lines()
                        .find_map(|line| line.strip_prefix("Content-Length: "))
                        .expect("a length")
                        .parse()
                        .expect("a number");
                    let mut left = length - rest.len();
                    let mut buf = vec![0u8; 1 << 16];
                    while left > 0 {
                        let n = stream.read(&mut buf).expect("read a body");
                        assert!(n > 0, "the request ended inside its body");
                        left -= n;
                    }
                    let body = format!("{{\"ok\":true,\"ingested\":{ingested}}}");
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                             content-length: {}\r\n\r\n{body}",
                        body.len()
                    )
                    .expect("answer");
                }
            }
        }
    });
    addr
}

/// A body far larger than two socket buffers, so that a server that closes without
/// reading it ends the write part way.
fn large_body() -> (Vec<u8>, Vec<u8>) {
    let body = vec![b'x'; 16 << 20];
    (wire::ingest_head(body.len()), body)
}

fn send_one(plays: Vec<Play>, lines: u64) -> (Result<Sent, SpeedError>, Shared) {
    let (head, body) = large_body();
    let shared = Shared::default();
    let mut conn = Conn::open(listener(plays), Duration::from_secs(20)).expect("connect");
    let sent = send_body(&mut conn, &head, &body, lines, 0, &shared);
    (sent, shared)
}

#[test]
fn a_refusal_that_arrives_before_the_body_was_read_is_a_refusal_and_the_body_is_sent_again() {
    let (sent, shared) = send_one(vec![Play::RefuseEarly, Play::Take(2_000)], 2_000);
    assert_eq!(sent.expect("the body is taken"), Sent::Taken(2_000));
    let mine = &shared.counts[0];
    // Whichever way the write ended, what was read was a 503.
    assert_eq!(mine.refused_503.load(Relaxed), 1);
    assert_eq!(mine.refused_429.load(Relaxed), 0);
    assert_eq!(
        mine.no_answer.load(Relaxed),
        0,
        "a refusal is not a silence"
    );
    assert_eq!(mine.unknown_outcome.load(Relaxed), 0);
    assert_eq!(
        mine.retry_after_max_s(),
        Some(1),
        "recorded, and not obeyed"
    );
    assert_eq!(mine.reconnects.load(Relaxed), 1);
    assert_eq!(mine.acked_bodies.load(Relaxed), 1);
    assert_eq!(shared.acked_lines.load(Relaxed), 2_000);
    assert_eq!(mine.short_bodies.load(Relaxed), 0);
}

#[test]
fn five_attempts_in_a_row_that_read_no_answer_end_the_sender() {
    let (sent, shared) = send_one(vec![Play::Close; 5], 2_000);
    let error = sent.expect_err("five silences").to_string();
    assert!(
        error.starts_with("ingest sender 1 of 4: 5 attempts in a row read no answer"),
        "{error}"
    );
    let mine = &shared.counts[0];
    assert_eq!(mine.no_answer.load(Relaxed), 5);
    assert_eq!(
        mine.unknown_outcome.load(Relaxed),
        0,
        "no body was written whole"
    );
    assert_eq!(mine.refused(), 0);
    assert_eq!(shared.acked_lines.load(Relaxed), 0);
}

#[test]
fn four_silences_and_then_a_refusal_do_not_end_the_sender() {
    let mut plays = vec![Play::Close; 4];
    plays.push(Play::RefuseEarly);
    // Four more after the refusal: the count began again at the answer.
    plays.extend([Play::Close; 4]);
    plays.push(Play::Take(1_990));
    let (sent, shared) = send_one(plays, 2_000);
    assert_eq!(sent.expect("the body is taken"), Sent::Taken(1_990));
    let mine = &shared.counts[0];
    assert_eq!(mine.no_answer.load(Relaxed), 8);
    assert_eq!(mine.refused_503.load(Relaxed), 1);
    assert_eq!(mine.acked_lines.load(Relaxed), 1_990);
    // Ten lines the server did not take of a body it answered 200.
    assert_eq!(mine.short_bodies.load(Relaxed), 1);
    assert_eq!(mine.short_lines.load(Relaxed), 10);
}

#[test]
fn a_sender_whose_connection_dies_after_the_flag_returns_quietly() {
    let (head, body) = large_body();
    let shared = Shared::default();
    shared.stop.store(true, Relaxed);
    let mut conn =
        Conn::open(listener(vec![Play::Close]), Duration::from_secs(20)).expect("connect");
    let sent = send_body(&mut conn, &head, &body, 2_000, 0, &shared);
    assert_eq!(sent.expect("quiet"), Sent::Stopped);
    assert_eq!(shared.counts[0].no_answer.load(Relaxed), 0);
}

// ── One query, against a listener that plays the server ──────────────────────────

/// Read one request whole, head and announced body. `None` once the client is done
/// with the connection.
fn read_request(stream: &mut TcpStream) -> Option<()> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).ok()? == 0 {
            return None;
        }
        head.push(byte[0]);
    }
    let length: usize = String::from_utf8_lossy(&head)
        .lines()
        .find_map(|line| line.strip_prefix("Content-Length: ")?.parse().ok())
        .unwrap_or(0);
    stream.read_exact(&mut vec![0u8; length]).ok()
}

/// A listener that answers every request, on every connection made to it, with the
/// next of `answers`, and with the last of them from then on. Returns its address.
fn answering(answers: Vec<String>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("an address");
    thread::spawn(move || {
        let mut asked = 0;
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else {
                return;
            };
            while read_request(&mut stream).is_some() {
                let answer = &answers[asked.min(answers.len() - 1)];
                asked += 1;
                if stream.write_all(answer.as_bytes()).is_err()
                    || answer.contains("connection: close")
                {
                    break;
                }
            }
        }
    });
    addr
}

/// An answer as this server writes one: header names in lower case.
fn answer(status: &str, extra: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{extra}content-length: \
             {}\r\n\r\n{body}",
        body.len()
    )
}

fn ask_a_listener(answers: Vec<String>, limit: Duration) -> Result<Asked, SpeedError> {
    let mut conn = Conn::open(answering(answers), Duration::from_secs(20)).expect("connect");
    ask_within(
        &mut conn,
        || "a listener of this test".to_string(),
        "S2",
        &wire::query_request(crate::bench::S2),
        limit,
    )
}

#[test]
fn a_query_that_is_refused_is_asked_again_and_its_sample_runs_from_the_first_try() {
    // Refused down a socket that is kept, refused again down one that is then done,
    // and answered on a third try down a new one.
    let asked = ask_a_listener(
        vec![
            answer("503 Service Unavailable", "retry-after: 1\r\n", REFUSAL),
            answer("429 Too Many Requests", "connection: close\r\n", REFUSAL),
            answer("200 OK", "", r#"{"count":56,"meta":{"rows_scanned":2400}}"#),
        ],
        Duration::from_secs(60),
    )
    .expect("answered on the third try");
    assert_eq!(asked.refused, 2);
    assert_eq!(asked.answer.count, 56);
    assert_eq!(asked.answer.rows_scanned, Some(2_400));
    // Both pauses are in what the caller waited.
    assert!(asked.took >= 2 * REFUSAL_PAUSE, "{:?}", asked.took);
}

#[test]
fn a_query_that_is_refused_for_ever_ends_in_an_error_when_the_limit_is_up() {
    let limit = Duration::from_millis(120);
    for (status, line, extra) in [
        (503, "503 Service Unavailable", ""),
        // As this server refuses a body: the socket is done, and each try is a new one.
        (429, "429 Too Many Requests", "connection: close\r\n"),
    ] {
        let began = Instant::now();
        let Err(error) = ask_a_listener(vec![answer(line, extra, REFUSAL)], limit) else {
            panic!("a query that was never answered is not a sample");
        };
        let waited = began.elapsed();
        assert!(waited >= limit, "{waited:?}");
        assert!(
            waited < Duration::from_secs(20),
            "it went on being asked: {waited:?}"
        );
        assert!(
            matches!(&error, SpeedError::Query { name: "S2", status: was, .. } if *was == status),
            "{error:?}"
        );
        let said = error.to_string();
        assert!(
            said.starts_with(&format!(
                "S2: the server's answer cannot be measured (status {status}): it was \
                     refused "
            )),
            "{said}"
        );
        assert!(
                said.contains(" and never answered, and a query still refused 0 s after its first try is not asked again. The last refusal: {\"ok\":false,")
                    && said.ends_with("retry in 1s\"} (a listener of this test)"),
                "{said}"
            );
    }
}

#[test]
fn a_query_answered_with_another_status_is_not_asked_again() {
    let error = ask_a_listener(
        vec![
            answer(
                "400 Bad Request",
                "",
                r#"{"ok":false,"error":"no such key"}"#,
            ),
            answer("200 OK", "", r#"{"count":1}"#),
        ],
        Duration::from_secs(60),
    )
    .expect_err("a 400 is not a refusal");
    assert_eq!(
        error.to_string(),
        "S2: the server's answer cannot be measured (status 400): \
             {\"ok\":false,\"error\":\"no such key\"}"
    );
}

#[test]
fn the_gate_holds_every_waiter_until_it_is_opened() {
    let shared = Shared::default();
    thread::scope(|scope| {
        for _ in 0..SENDERS {
            scope.spawn(|| {
                shared.waiting.fetch_add(1, Relaxed);
                shared.gate.wait();
                shared.acked_lines.fetch_add(1, Relaxed);
            });
        }
        shared.all_waiting();
        thread::sleep(Duration::from_millis(20));
        assert_eq!(shared.acked_lines.load(Relaxed), 0, "none is past the gate");
        // Dropped, as on any way out of a unit: the gate opens and the flag is up.
        drop(Release(&shared));
    });
    assert_eq!(shared.acked_lines.load(Relaxed), SENDERS as u64);
    assert!(shared.stop.load(Relaxed));
}

// ── What a result keeps of the laps ──────────────────────────────────────────────

#[test]
fn lines_by_the_second_add_up_to_every_line_acknowledged() {
    let opened = Instant::now();
    let at = |ms: u64| opened + Duration::from_millis(ms);
    let returned = |acks: Vec<(Instant, u64)>| Returned {
        result: Ok(()),
        acks,
        ended: opened,
    };
    let senders = [
        returned(vec![(at(200), 100), (at(1_100), 100), (at(3_500), 100)]),
        returned(vec![(at(999), 50), (at(1_000), 50)]),
    ];
    // Nothing in the third second, and the last value is the whole.
    assert_eq!(by_second(opened, &senders), [150, 300, 300, 400]);
    assert_eq!(by_second(opened, &[]), Vec::<u64>::new());
}

#[test]
fn a_rate_is_not_worked_out_over_no_seconds() {
    assert_eq!(per_second(100, Some(0.5)), Some(200.0));
    assert_eq!(per_second(100, Some(0.0)), None);
    assert_eq!(per_second(100, None), None);
}

#[test]
fn the_laps_are_kept_by_query_with_what_was_late_and_what_each_snapshot_held() {
    let loaded = Corpus::default();
    let stream = LiveStream::default();
    let requests = Seven::<Vec<u8>>::default();
    let mut expect = Seven::<Expect>::default();
    expect.s1.count = 22;
    expect.s4.count = 2_000;
    let work = Work {
        exe: Path::new("kibbr"),
        bench: Path::new("kibbr-bench"),
        loaded: &loaded,
        work: Path::new("work"),
        cores: 8,
        threads: None,
        stream: &stream,
        heads: &[],
        requests: &requests,
        expect: &expect,
        answer_room: 0,
        lines: 2_000,
        samples: 3,
        body_lines: 100,
    };
    // Places at 2,000 lines: 40, 60, 140, 280. A round of bodies is 400 lines.
    let lap = |s1_ms: f64, s4_at: u64, closes: Option<u64>, wraps: u64| Lap {
        samples: Four {
            s1: Sample {
                client_ms: s1_ms,
                server_ms: Some(s1_ms - 0.5),
                at_lines: 100,
                end_lines: 100,
                count: 25,
                rows_scanned: Some(25),
                refused: 0,
            },
            s4: Sample {
                client_ms: 9.0,
                server_ms: None,
                at_lines: s4_at,
                end_lines: s4_at + 300,
                count: 2_000 + s4_at,
                rows_scanned: None,
                refused: 1,
            },
            ..Four::default()
        },
        start_s: 0.2,
        lines: 1_000,
        seconds: 0.5,
        forced_closes: closes,
        buffered_bytes: Some(7),
        requests_shed: Some(0),
        write_failures: Some(0),
        degraded: Some(false),
        refused_bodies: 2,
        wraps,
    };
    let mut laps = [
        lap(3.0, 300, Some(0), 0),
        lap(1.0, 681, Some(2), 1),
        lap(2.0, 680, Some(1), 0),
    ];
    // One lap whose store failed to write and wrote again, and one that ended on a
    // flush that had failed.
    laps[1].write_failures = Some(2);
    laps[2].degraded = Some(true);
    let run = loaded_run(&laps, &work);

    assert_eq!(run.laps, 3);
    let s1 = &run.queries.s1;
    assert_eq!(s1.client_ms, [3.0, 1.0, 2.0], "in lap order");
    assert_eq!(
        (s1.p50_ms, s1.p99_ms, s1.min_ms, s1.max_ms),
        (2.0, 3.0, 1.0, 3.0)
    );
    assert_eq!((s1.server_p50_ms, s1.server_p99_ms), (Some(1.5), Some(2.5)));
    assert_eq!(s1.place_lines, 40);
    assert_eq!(s1.count_over_loaded, [3, 3, 3]);
    assert_eq!(s1.late, 0, "60 lines past its place is inside a round");

    let s4 = &run.queries.s4;
    assert_eq!(s4.place_lines, 280);
    assert_eq!(s4.at_lines, [300, 681, 680]);
    // 401 lines past the place is late; 400, one round exactly, is not.
    assert_eq!(s4.late, 1);
    assert_eq!(s4.acked_while_running, [300, 300, 300]);
    assert_eq!(s4.count_over_loaded, [300, 681, 680]);
    assert_eq!(s4.count_over_loaded_median, 680);
    assert_eq!((s4.server_p50_ms, s4.refused), (None, 3));

    assert_eq!(run.lap.lines, [1_000; 3]);
    // What each lap's server said of its store is kept by lap, as it was said.
    assert_eq!(run.lap.write_failures, [Some(0), Some(2), Some(0)]);
    assert_eq!(run.lap.degraded, [Some(false), Some(false), Some(true)]);
    assert_eq!(run.lines_per_s_median, Some(2_000.0));
    assert_eq!((run.refused_bodies, run.wrapped_laps), (6, 1));
    assert_eq!(run.laps_at_buffer_limit, Some(2));

    // One lap whose server did not say: the count is not known, and is not 0.
    let unknown = [lap(1.0, 300, None, 0), lap(1.0, 300, Some(1), 0)];
    assert_eq!(loaded_run(&unknown, &work).laps_at_buffer_limit, None);
}

/// `loaded`, under a name that does not hide the test's own `loaded` directory.
fn loaded_run(laps: &[Lap], work: &Work) -> LoadedRun {
    loaded(laps, work)
}

#[test]
fn an_answer_is_held_to_the_expected_one_in_every_part_two_asks_share() {
    let got = QueryAnswer {
        count: 2,
        duration_ms: Some(0.4),
        rows_scanned: Some(150),
        rows_returned: Some(2),
        templates_matched: Some(3),
        truncated: Some(false),
        limit_source: Some("backstop".to_string()),
        ..QueryAnswer::default()
    };
    let expect = got.expect();
    assert!(same("S3", &expect, &got).is_ok());
    assert!(within_backstop("S3", &got).is_ok());

    // What moves between two asks is not held: the time, and what was read.
    let again = QueryAnswer {
        duration_ms: Some(9.0),
        rows_scanned: Some(151),
        ..got.clone()
    };
    assert!(same("S3", &expect, &again).is_ok());

    let other = QueryAnswer {
        count: 3,
        ..got.clone()
    };
    let error = same("S3", &expect, &other)
        .expect_err("another count")
        .to_string();
    assert!(error.starts_with("S3: the server's answer cannot be measured (status 200)"));
    assert!(
        error.contains("\"count\":3") && error.contains("\"count\":2"),
        "{error}"
    );

    // Under ingest a count may grow and may not shrink.
    assert!(under_ingest("S3", &expect, &other).is_ok());
    let fewer = QueryAnswer {
        count: 1,
        ..got.clone()
    };
    assert!(under_ingest("S3", &expect, &fewer).is_err());
    // A fetch returns at least as many rows as it did.
    let short = QueryAnswer {
        count: 5,
        rows_returned: Some(1),
        ..got.clone()
    };
    assert!(under_ingest("S3", &expect, &short).is_err());
    let more = QueryAnswer {
        count: 5,
        rows_returned: Some(5),
        ..got.clone()
    };
    assert!(under_ingest("S3", &expect, &more).is_ok());

    let cut = QueryAnswer {
        truncated: Some(true),
        ..got.clone()
    };
    assert!(within_backstop("S3", &cut).is_err());
    let many = QueryAnswer {
        count: MAX_ROWS + 1,
        ..got
    };
    assert!(within_backstop("S3", &many).is_err());
}
