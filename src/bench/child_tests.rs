// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::net::TcpListener;

#[test]
fn a_child_loses_every_kibbr_variable_and_keeps_the_rest() {
    let scrubbed = |name: &str| begins(OsStr::new(name), &[SCRUBBED]);
    assert!(scrubbed("KIBBR__STORAGE__BASE_PATH"));
    assert!(scrubbed("KIBBR__SCHEMA__EPOCH_WINDOW"));
    assert!(!scrubbed("KIBBR_HOME"));
    assert!(!scrubbed("PATH"));

    let noted = |name: &str| begins(OsStr::new(name), &NOTED);
    assert!(noted("RAYON_NUM_THREADS"));
    assert!(noted("TOKIO_WORKER_THREADS"));
    assert!(noted("MIMALLOC_VERBOSE"));
    assert!(!noted("HOME"));
    assert!(!noted("KIBBR__API__INGEST_CONCURRENCY"));
}

#[test]
fn every_server_is_started_with_every_flag_typed_out() {
    let expected = "serve --host 127.0.0.1 --port 0 --data-dir <copy> --storage disk \
             --max-rows 10000 --max-buckets 10000 --max-groups 100000 \
             --max-body-bytes 10485760 --max-decompressed-bytes 67108864 \
             --ingest-concurrency 8 \
             --flush-interval-ms 500 --row-group-timeout-ms 300000 \
             --wal-segment-bytes 67108864 --batch-size 500 \
             --max-open-segments 512 --segment-max-bytes 536870912 \
             --max-buffer-bytes 536870912 \
             --compact-interval-ms 0 \
             --time-columns @timestamp,timestamp,time,ts,ts_ms,ts_unix,datetime,event_time,created_at";
    let argv = argv(COPY, 8, None);
    assert_eq!(argv, expected.split(' ').collect::<Vec<_>>());

    // Only the copy and the cores differ between two servers, and the copy's path
    // reaches the child as one argument whatever it holds.
    let copy = Path::new("target/scratch/bench/work-1.noindex/copy 7");
    let command = command(Path::new("/bin/kibbr"), copy, 4, None);
    let passed: Vec<&OsStr> = command.get_args().collect();
    assert_eq!(passed.len(), argv.len());
    for (at, (passed, recorded)) in passed.iter().zip(&argv).enumerate() {
        match recorded.as_str() {
            COPY => assert_eq!(*passed, copy.as_os_str()),
            "8" => assert_eq!(*passed, "4", "argument {at}"),
            recorded => assert_eq!(*passed, recorded, "argument {at}"),
        }
    }
    assert_eq!(command.get_program(), "/bin/kibbr");
}

/// **`--threads` reaches every server as `--scan-threads`, and with none given the
/// command line is the one every earlier result recorded.**
#[test]
fn the_scan_threads_asked_for_are_the_last_flag_and_none_is_no_flag() {
    let without = argv(COPY, 8, None);
    assert!(!without.iter().any(|argument| argument == "--scan-threads"));

    let with = argv(COPY, 8, Some(4));
    assert_eq!(with[..without.len()], without[..]);
    assert_eq!(with[without.len()..], ["--scan-threads", "4"]);

    let command = command(Path::new("/bin/kibbr"), Path::new("copy"), 8, Some(4));
    let passed: Vec<&OsStr> = command.get_args().collect();
    assert_eq!(passed[passed.len() - 2..], ["--scan-threads", "4"]);
    assert_eq!(passed.len(), with.len());
}

#[test]
fn the_port_is_the_first_one_a_server_wrote_whole() {
    let banner =
        b"kibbr: catalog.log - /d/catalog.log restored 80 template(s) from 1,204 record(s)\n\
                       kibbr serve\n  listening   http://127.0.0.1:24611\n  storage     disk\n";
    assert_eq!(banner_port(banner), Some(24_611));
    // Read while the line was being written: 2461 is not the port.
    assert_eq!(banner_port(&banner[..banner.len() - 20]), None);
    assert_eq!(banner_port(b"  listening   http://127.0.0.1:2461"), None);
    assert_eq!(banner_port(b"  listening   http://127.0.0.1:"), None);
    assert_eq!(banner_port(b"kibbr: nothing yet\n"), None);
    assert_eq!(banner_port(b""), None);
    // The first mention, and one that is not a port is passed over.
    assert_eq!(
        banner_port(
            b"see http://127.0.0.1:/docs\n http://127.0.0.1:7700/ and http://127.0.0.1:1 \n"
        ),
        Some(7_700)
    );
    assert_eq!(
        banner_port(b"http://127.0.0.1:99999 http://127.0.0.1:0 \n"),
        None
    );
}

/// `/stats` as a server of this build answers it, the counters the bench reads set apart
/// from one another, so that one read from another's place shows.
const STATS: &str = r#"{"uptime_seconds":0.026272959,"started_at":"2026-10-07T15:24:22.940221Z","storage":{"kind":"disk","durable":true,"note":"a line is written to the write-ahead log under wal/ and flushed before /ingest acknowledges it","base_path":"/s/data","trie_file":"/s/data/trie.json","catalog":"/s/data/catalog.log","flush_interval_ms":500,"row_group_timeout_ms":300000,"wal":{"dir":"/s/data/wal","stream":"ecca12cf6193f716","segments":1,"first_segment":1,"bytes":28,"last_seq":600,"applied":598,"checkpoint":0,"flushes":2,"healthy":true,"last_error":null,"segment_bytes":67108864,"batch_size":500,"held_rows":0,"replay":{"records":0,"applied":0,"covered":0,"held":0,"replaced":0,"behind_checkpoint":0,"torn":0,"corrupt":0,"seconds":0.0}},"stores_open":80,"open_segments":0,"max_open_segments":512,"segment_max_bytes":536870912,"rows":2000,"buffered_rows":7,"buffered_bytes":1536352,"max_buffer_bytes":536870912,"compact_interval_ms":0,"compaction":{"running":false,"runs":0,"days_compacted":0,"last":null,"last_error":null},"forced_closes":3,"write_failures":5,"last_write_error":null,"degraded":true,"approx_bytes":145294},"trie":{"templates":80,"nodes":780,"tokens":1044},"ingest":{"lines_read":600,"lines_ingested":600,"lines_rejected":1,"lines_per_second":0.0,"requests_shed":4,"ingest_concurrency":8,"ingest_bodies":256},"queries":{"served":0,"per_second":0.0}}"#;

#[test]
fn stats_are_the_two_required_fields_and_whatever_else_is_there() {
    let stats = parse_stats(STATS.as_bytes()).unwrap();
    let body = stats.body.clone();
    assert_eq!(
        stats,
        Stats {
            uptime_seconds: 0.026272959,
            lines_ingested: 600,
            lines_rejected: Some(1),
            requests_shed: Some(4),
            ingest_concurrency: Some(8),
            rows: Some(2_000),
            buffered_rows: Some(7),
            buffered_bytes: Some(1_536_352),
            max_buffer_bytes: Some(536_870_912),
            forced_closes: Some(3),
            write_failures: Some(5),
            degraded: Some(true),
            wal_last_seq: Some(600),
            wal_applied: Some(598),
            wal_flushes: Some(2),
            templates: Some(80),
            body: body.clone(),
        }
    );

    // The body is the answer in the answer's order, less the note and nothing else.
    let note = r#""note":"a line is written to the write-ahead log under wal/ and flushed before /ingest acknowledges it","#;
    assert!(STATS.contains(note));
    assert_eq!(body.to_string(), STATS.replace(note, ""));

    // An optional field that is gone, or is another type, is missing; a field the
    // bench does not know is kept in the body and not read.
    let stats = parse_stats(
            br#"{"uptime_seconds":3,"ingest":{"lines_ingested":0,"requests_shed":"none"},"scan":{"threads":4}}"#,
        )
        .unwrap();
    assert_eq!((stats.uptime_seconds, stats.lines_ingested), (3.0, 0));
    assert_eq!(
        (
            stats.requests_shed,
            stats.rows,
            stats.wal_last_seq,
            stats.templates
        ),
        (None, None, None, None)
    );
    assert_eq!((stats.write_failures, stats.degraded), (None, None));
    assert_eq!(stats.body["scan"]["threads"], 4);

    // Whether the store could write is read as a count and a flag, and a word in the
    // flag's place is not taken for one.
    let stats = parse_stats(
            br#"{"uptime_seconds":3,"ingest":{"lines_ingested":0},"storage":{"write_failures":0,"degraded":"no"}}"#,
        )
        .unwrap();
    assert_eq!((stats.write_failures, stats.degraded), (Some(0), None));
}

#[test]
fn stats_without_a_required_field_are_not_read() {
    for (body, says) in [
        (
            r#"{"ingest":{"lines_ingested":0}}"#,
            "it holds no `uptime_seconds`",
        ),
        (
            r#"{"uptime_seconds":"1.5","ingest":{"lines_ingested":0}}"#,
            "its `uptime_seconds` is \"1.5\", and a number of seconds is read there",
        ),
        (
            r#"{"uptime_seconds":1.5,"ingest":{}}"#,
            "it holds no `ingest.lines_ingested`",
        ),
        (
            r#"{"uptime_seconds":1.5,"ingest":{"lines_ingested":12.5}}"#,
            "its `ingest.lines_ingested` is 12.5, and a whole number is read there",
        ),
        (r#"{"uptime_seconds":1.5"#, "its body is not JSON"),
    ] {
        let error = parse_stats(body.as_bytes()).unwrap_err();
        assert!(
            matches!(
                error,
                SpeedError::Surface {
                    surface: "GET /stats",
                    ..
                }
            ),
            "{error}"
        );
        assert!(error.to_string().contains(says), "{error}");
    }
}

#[test]
fn health_is_its_status_and_the_reason_it_gives() {
    assert_eq!(
        parse_health(br#"{"ok":true,"status":"ok"}"#).unwrap(),
        Health {
            status: "ok".to_string(),
            detail: None
        }
    );
    assert_eq!(
        parse_health(br#"{"ok":true,"status":"degraded","detail":"the log cannot write"}"#)
            .unwrap(),
        Health {
            status: "degraded".to_string(),
            detail: Some("the log cannot write".to_string())
        }
    );
    for body in [&br#"{"ok":true}"#[..], br#"{"status":1}"#, b"ok"] {
        let error = parse_health(body).unwrap_err();
        assert!(
            matches!(
                error,
                SpeedError::Surface {
                    surface: "GET /health",
                    ..
                }
            ),
            "{error}"
        );
    }
}

#[test]
fn a_copy_that_differs_is_named_by_its_first_difference() {
    let file = |path: &str, bytes: u64, crc32c: &str| FileDigest {
        path: path.to_string(),
        bytes,
        crc32c: crc32c.to_string(),
    };
    let recorded = [
        file("catalog.log", 10, "17c0a2e1"),
        file("data/2024-01-15/seg_0_000001.col", 20, "b40d91f3"),
        file("manifest.log", 30, "0e5a7c22"),
    ];
    assert_eq!(difference(&recorded, &recorded), None);

    let mut copy = recorded.to_vec();
    copy[1].crc32c = "00000000".to_string();
    assert_eq!(
        difference(&copy, &recorded).unwrap(),
        "data/2024-01-15/seg_0_000001.col: crc32c 00000000, recorded b40d91f3"
    );
    copy[1].bytes = 19;
    assert_eq!(
        difference(&copy, &recorded).unwrap(),
        "data/2024-01-15/seg_0_000001.col: 19 bytes, recorded 20"
    );
    assert_eq!(
        difference(&recorded[..2], &recorded).unwrap(),
        "manifest.log is missing"
    );
    assert_eq!(
        difference(&[recorded[0].clone(), recorded[2].clone()], &recorded).unwrap(),
        "data/2024-01-15/seg_0_000001.col is missing"
    );
    let mut more = recorded.to_vec();
    more.insert(1, file("catalog.log.tmp", 1, "00000001"));
    assert_eq!(
        difference(&more, &recorded).unwrap(),
        "catalog.log.tmp is not in the recorded list"
    );
    more = recorded.to_vec();
    more.push(file("trie.json", 1, "00000001"));
    assert_eq!(
        difference(&more, &recorded).unwrap(),
        "trie.json is not in the recorded list"
    );
}

/// A directory of this test's own, with a copy in it for a server to be started on.
#[cfg(unix)]
fn scratch(name: &str) -> (PathBuf, Made) {
    let dir =
        std::env::temp_dir().join(format!("kibbr-speed-server-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    let copy = dir.join("copy-1");
    fs::create_dir_all(copy.join("data")).unwrap();
    fs::write(copy.join("catalog.log"), b"x").unwrap();
    let made = Made {
        log: dir.join("server-1.log"),
        copy,
        copy_ms: 4.0,
        copy_bytes: 1,
    };
    (dir, made)
}

/// Something that starts the way a server does, as far as the bench can see: a
/// shell that writes `script`'s lines to standard error and then stays.
#[cfg(unix)]
fn stand_in(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args(["-c", script]);
    command
}

/// Whether a process is there, running or not yet reaped.
#[cfg(unix)]
fn exists(pid: u32) -> bool {
    Command::new("/bin/kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}

/// A listener that answers every request on every connection with the next of
/// `answers`, and the last of them from then on. Returns its port.
#[cfg(unix)]
fn answering(answers: Vec<String>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        let mut asked = 0;
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while stream.read(&mut byte).unwrap_or(0) == 1 {
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    let answer = &answers[asked.min(answers.len() - 1)];
                    asked += 1;
                    request.clear();
                    if stream.write_all(answer.as_bytes()).is_err()
                        || answer.contains("connection: close")
                    {
                        break;
                    }
                }
            }
        }
    });
    port
}

#[cfg(unix)]
fn answer(status: &str, extra: &str, body: &str) -> String {
    format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{extra}content-length: {}\r\n\r\n{body}",
            body.len()
        )
}

#[cfg(unix)]
#[test]
fn a_server_is_up_at_its_first_200_of_health_and_goes_with_its_copy() {
    // Not ready twice - once on a socket it then closes - and then ready; after
    // that `/stats`, and a `/health` that is no longer `ok`.
    let port = answering(vec![
        answer("503 Service Unavailable", "connection: close\r\n", "{}"),
        answer("404 Not Found", "", "{}"),
        answer("200 OK", "", r#"{"ok":true,"status":"ok"}"#),
        answer("200 OK", "", STATS),
        answer(
            "200 OK",
            "connection: close\r\n",
            r#"{"ok":true,"status":"degraded","detail":"the log cannot write"}"#,
        ),
        answer("500 Internal Server Error", "", "no"),
    ]);
    let (dir, made) = scratch("up");
    let (copy, log) = (made.copy.clone(), made.log.clone());
    let script = format!(
        "echo 'kibbr serve' >&2; sleep 0.1; \
             echo '  listening   http://127.0.0.1:{port}' >&2; exec sleep 60"
    );
    let server = launch(stand_in(&script), made, Duration::from_secs(30)).unwrap();

    assert_eq!(server.addr, SocketAddr::from(([127, 0, 0, 1], port)));
    assert!(
        server.start_s >= 0.1 && server.start_s < 30.0,
        "{}",
        server.start_s
    );
    assert_eq!(
        server.unit(),
        ServerUnit {
            pid: server.pid,
            start_s: server.start_s,
            copy_ms: 4.0,
            copy_bytes: 1
        }
    );
    assert!(exists(server.pid));

    let mut conn = Conn::open(server.addr, Duration::from_secs(20)).unwrap();
    assert_eq!(server.stats(&mut conn).unwrap().lines_ingested, 600);
    // This answer said `connection: close`; the next question still goes through.
    assert_eq!(server.health(&mut conn).unwrap().status, "degraded");
    let error = server.stats(&mut conn).unwrap_err().to_string();
    assert!(
        error.starts_with(
            "GET /stats did not answer the way kibbr bench reads it: status 500: no (server pid "
        ) && error.ends_with("server-1.log)"),
        "{error}"
    );

    let pid = server.pid;
    drop(server);
    assert!(!exists(pid), "killed and reaped");
    assert!(!copy.exists(), "the copy goes with its server");
    assert!(log.exists(), "what it wrote stays");
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn a_server_that_ends_before_it_is_up_is_quoted_and_leaves_no_copy() {
    let (dir, made) = scratch("ends");
    let (copy, log) = (made.copy.clone(), made.log.clone());
    let Err(error) = launch(
        stand_in("echo 'kibbr: the data directory is held by another server' >&2; exit 3"),
        made,
        Duration::from_secs(30),
    ) else {
        panic!("a process that exited is not a server");
    };
    let SpeedError::ServerStart { log: named, tail } = &error else {
        panic!("{error}");
    };
    assert_eq!(named, &log);
    assert_eq!(
        tail,
        "kibbr: the data directory is held by another server\n\
             [the process ended before it was up: exit status: 3]"
    );
    assert!(!copy.exists());
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn a_server_that_never_says_where_or_never_answers_is_killed_when_the_wait_ends() {
    // Nothing written at all.
    let (dir, made) = scratch("silent");
    let copy = made.copy.clone();
    let began = Instant::now();
    let Err(error) = launch(stand_in("exec sleep 60"), made, Duration::from_millis(300)) else {
        panic!("nothing was written");
    };
    assert!(began.elapsed() < Duration::from_secs(20));
    let SpeedError::ServerStart { tail, .. } = &error else {
        panic!("{error}");
    };
    assert_eq!(tail, "[it had not written its address when the wait ended]");
    assert!(!copy.exists());
    fs::remove_dir_all(dir).unwrap();

    // An address, and behind it something that is never a 200.
    let port = answering(vec![answer("503 Service Unavailable", "", "{}")]);
    let (dir, made) = scratch("unready");
    let script = format!("echo '  listening   http://127.0.0.1:{port}' >&2; exec sleep 60");
    let Err(error) = launch(stand_in(&script), made, Duration::from_millis(300)) else {
        panic!("it never answered 200");
    };
    let SpeedError::ServerStart { tail, .. } = &error else {
        panic!("{error}");
    };
    assert_eq!(
        tail,
        &format!(
            "  listening   http://127.0.0.1:{port}\n\
                 [it had not answered /health with 200 when the wait ended]"
        )
    );
    fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[test]
fn a_server_that_starts_degraded_is_not_measured() {
    let port = answering(vec![answer(
        "200 OK",
        "",
        r#"{"ok":true,"status":"degraded","detail":"the log cannot write"}"#,
    )]);
    let (dir, made) = scratch("degraded");
    let copy = made.copy.clone();
    let script = format!("echo '  listening   http://127.0.0.1:{port}' >&2; exec sleep 60");
    let Err(error) = launch(stand_in(&script), made, Duration::from_secs(30)) else {
        panic!("a degraded server is not up");
    };
    let text = error.to_string();
    assert!(
        matches!(
            error,
            SpeedError::Surface {
                surface: "GET /health",
                ..
            }
        ),
        "{text}"
    );
    assert!(
        text.contains("said `degraded` (the log cannot write)"),
        "{text}"
    );
    assert!(!copy.exists());
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_binary_that_cannot_be_started_is_an_error_about_that_file() {
    let dir = std::env::temp_dir().join(format!(
        "kibbr-speed-server-{}-nobinary",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    let copy = dir.join("copy-1");
    fs::create_dir_all(&copy).unwrap();
    let made = Made {
        log: dir.join("server-1.log"),
        copy: copy.clone(),
        copy_ms: 0.0,
        copy_bytes: 0,
    };
    let missing = dir.join("no-such-kibbr");
    let Err(error) = launch(
        command(&missing, &copy, 8, None),
        made,
        Duration::from_secs(30),
    ) else {
        panic!("there is no such binary");
    };
    assert!(
        matches!(&error, SpeedError::Io { path, .. } if path == &missing),
        "{error}"
    );
    assert!(!copy.exists());
    fs::remove_dir_all(dir).unwrap();
}
