// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::rows::Row;

/// A directory of this test's own, removed when it goes.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("kibbr-wal-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }

    fn segments(&self) -> Vec<PathBuf> {
        let mut out: Vec<PathBuf> = fs::read_dir(self.0.join(DIR))
            .expect("wal dir")
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "wal"))
            .collect();
        out.sort();
        out
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn line(n: u64) -> Line {
    Line {
        raw: format!("{{\"n\":{n}}}").into_bytes(),
        time: 1_000 + n as i64,
        placed: Placed {
            template_id: 100 + n % 3,
            time_column: "ts".to_string(),
            row: Row::new(),
        },
    }
}

fn body(from: u64, to: u64) -> Vec<Line> {
    (from..to).map(line).collect()
}

/// A started log, and the positions its applier was handed, in the order it was.
fn started(scratch: &Scratch, options: Options) -> (Wal, Arc<Mutex<Vec<u64>>>) {
    let mut silent = |_: String| {};
    let wal = match Wal::open(&scratch.0, &mut silent).expect("open") {
        Some(wal) => {
            wal.replay(&mut silent, &mut |_| Ok(())).expect("replay");
            wal
        }
        None => Wal::create(&scratch.0).expect("create"),
    };
    let applied = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&applied);
    wal.start(
        options,
        Box::new(move |rows| lock(&seen).extend(rows.iter().map(|(seq, _)| *seq))),
    )
    .expect("start");
    (wal, applied)
}

/// Every record past the checkpoint as `(seq, template_id, received_at, time, line)`,
/// what the read found, and what it warned.
type Read = (Vec<(u64, u64, i64, i64, String)>, Scanned, Vec<String>);

fn read_back(scratch: &Scratch) -> Result<Read, WalError> {
    let mut warnings = Vec::new();
    let mut records = Vec::new();
    let wal = Wal::open(&scratch.0, &mut |m| warnings.push(m))?.expect("a log");
    let scanned = wal.replay(&mut |m| warnings.push(m), &mut |r| {
        records.push((
            r.seq,
            r.template_id,
            r.received_at,
            r.time,
            String::from_utf8_lossy(r.line).into_owned(),
        ));
        Ok(())
    })?;
    Ok((records, scanned, warnings))
}

fn tiny() -> Options {
    Options {
        segment_bytes: 1,
        ..Options::default()
    }
}

#[test]
fn lines_come_back_in_order_with_their_positions_templates_and_times() {
    let scratch = Scratch::new("roundtrip");
    let (wal, applied) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 3), 111), Ok(()));
    assert_eq!(wal.commit(body(3, 5), -222), Ok(()));
    // Applied by the time `commit` returns, and in the log's order.
    assert_eq!(*lock(&applied), [1, 2, 3, 4, 5]);
    assert_eq!(wal.applied(), 5);
    wal.close();

    let (records, scanned, warnings) = read_back(&scratch).expect("read");
    assert_eq!(warnings, Vec::<String>::new());
    assert_eq!(scanned.records, 5);
    let want: Vec<(u64, u64, i64, i64, String)> = (0..5u64)
        .map(|n| {
            let at = if n < 3 { 111 } else { -222 };
            let time = 1_000 + n as i64;
            (n + 1, 100 + n % 3, at, time, format!("{{\"n\":{n}}}"))
        })
        .collect();
    assert_eq!(records, want);
}

/// A body is never split: one larger than `batch_lines` is one flush, whole, so a
/// kill or a failed write leaves all of its lines in the log or none.
#[test]
fn a_body_larger_than_the_bound_is_one_flush() {
    let scratch = Scratch::new("bound");
    let options = Options {
        batch_lines: 2,
        ..Options::default()
    };
    let (wal, applied) = started(&scratch, options);
    assert_eq!(wal.commit(body(0, 7), 1), Ok(()));
    assert_eq!(*lock(&applied), [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(wal.stats().flushes, 1);
    wal.close();
    let (records, _, _) = read_back(&scratch).expect("read");
    assert_eq!(
        records.iter().map(|r| r.0).collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6, 7]
    );
}

/// What a write a stop cut short leaves at the end of a segment, in each shape: warned
/// about and skipped, the start goes on, and nothing is cut.
#[test]
fn a_torn_last_record_of_any_shape_is_warned_about_and_skipped() {
    let mut bad_crc = frame::frame(b"\x01torn");
    bad_crc[4] ^= 0xff;
    let shapes: [(&str, Vec<u8>); 4] = [
        ("short", vec![7, 0, 0]),
        ("cut", [&4096u32.to_le_bytes()[..], &[9u8; 40][..]].concat()),
        ("crc", bad_crc),
        ("zeros", vec![0u8; 64]),
    ];
    for (name, junk) in shapes {
        let scratch = Scratch::new(&format!("torn-{name}"));
        let (wal, _) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(0, 4), 9), Ok(()));
        wal.close();
        let newest = scratch.segments().pop().expect("a segment");
        OpenOptions::new()
            .append(true)
            .open(&newest)
            .and_then(|mut f| f.write_all(&junk))
            .expect("append");
        let left = fs::read(&newest).expect("read");

        for _ in 0..2 {
            let (records, scanned, warnings) =
                read_back(&scratch).expect("a tear stopped the start");
            assert_eq!(records.len(), 4, "{name}");
            assert_eq!((scanned.torn, scanned.corrupt), (1, 0), "{name}");
            assert!(
                warnings.iter().any(|w| w.contains("torn record")),
                "{name}: {warnings:?}"
            );
            assert_eq!(fs::read(&newest).expect("read"), left, "{name}");
        }
        // And the next run writes past it, in a segment of its own - at a position
        // past any the torn bytes could have held.
        let (wal, applied) = started(&scratch, Options::default());
        assert_eq!(wal.commit(body(4, 5), 9), Ok(()), "{name}");
        let next = lock(&applied)[0];
        assert!(
            next > 4 + junk.len() as u64 * POSITIONS_PER_LOST_BYTE,
            "{name}: {next}"
        );
    }
}

/// Damage in the middle of a segment costs the frame it is in and no other, whether in
/// the frame's contents or its length: the frames after it are read, and nothing is cut.
#[test]
fn damage_mid_segment_costs_its_frame_and_the_frames_after_it_are_read() {
    for (name, at_length) in [("payload", false), ("length", true)] {
        let scratch = Scratch::new(&format!("mid-{name}"));
        let (wal, _) = started(&scratch, Options::default());
        for n in 0..4 {
            assert_eq!(wal.commit(body(n, n + 1), 0), Ok(()));
        }
        wal.close();
        let path = scratch.segments().pop().expect("a segment");
        let mut bytes = fs::read(&path).expect("read");
        let first_len = u32_at(&bytes, HEADER_LEN) as usize;
        let second = HEADER_LEN + frame::HEADER + first_len;
        if at_length {
            bytes[second..second + 4].copy_from_slice(&0x00ff_ffffu32.to_le_bytes());
        } else {
            let second_len = u32_at(&bytes, second) as usize;
            bytes[second + frame::HEADER + second_len - 1] ^= 0x55;
        }
        fs::write(&path, &bytes).expect("write");

        let (records, scanned, warnings) = read_back(&scratch).expect("read");
        assert_eq!(
            records.iter().map(|r| r.0).collect::<Vec<_>>(),
            [1, 3, 4],
            "{name}"
        );
        assert_eq!((scanned.corrupt, scanned.torn), (1, 0), "{name}");
        assert!(
            warnings.iter().any(|w| w.contains("corrupt WAL entry")),
            "{name}: {warnings:?}"
        );
        assert_eq!(fs::read(&path).expect("read"), bytes, "{name}");
    }
}

/// A frame whose checksum holds and whose contents this build cannot read is not a
/// tear, and the start does not guess: it stops, naming the segment.
#[test]
fn a_checksummed_frame_that_does_not_decode_stops_the_start() {
    let scratch = Scratch::new("undecodable");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    wal.close();
    let path = scratch.segments().pop().expect("a segment");
    OpenOptions::new()
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(&frame::frame(&[9, 0, 0, 0, 0])))
        .expect("append");
    match read_back(&scratch) {
        Err(WalError::Corrupt { what, .. }) => assert!(what.contains("kind 9"), "{what}"),
        other => panic!("read a frame of an unknown kind: {:?}", other.map(|r| r.1)),
    }
}

/// The checkpoint deletes the segments wholly behind it and never the newest, and a start
/// resumes above every position it gave out, whether or not the checkpoint file survived.
#[test]
fn the_checkpoint_deletes_what_is_behind_it_and_positions_are_never_reused() {
    let scratch = Scratch::new("checkpoint");
    let (wal, _) = started(&scratch, tiny());
    for n in 0..5 {
        assert_eq!(wal.commit(body(n, n + 1), 0), Ok(()));
    }
    // One byte to a segment: a segment a flush.
    assert_eq!(scratch.segments().len(), 5);

    // Position 3 is still only in memory: the checkpoint stops at 2, and the
    // segments holding 1 and 2 go.
    let moved = wal.advance(|| Some(3)).0;
    assert_eq!(
        moved,
        Some(Advanced {
            checkpoint: 2,
            deleted: 2
        })
    );
    assert_eq!(scratch.segments().len(), 3);
    assert_eq!(wal.advance(|| Some(3)).0, None);
    // An oldest position behind the checkpoint does not move it back.
    assert_eq!(wal.advance(|| Some(1)).0, None);
    assert_eq!(wal.checkpoint(), 2);

    // Nothing in memory: up to the last position applied. The newest segment stays.
    let moved = wal.advance(|| None).0.expect("moved");
    assert_eq!((moved.checkpoint, moved.deleted), (5, 2));
    assert_eq!(scratch.segments().len(), 1);
    let stream = wal.stream();
    wal.close();

    // The checkpoint file lost, and nothing left but the newest segment and the
    // empty one each start opens: the same stream, and the next position is still 6.
    fs::remove_file(scratch.0.join(DIR).join(CHECKPOINT_FILE)).expect("remove");
    let (wal, applied) = started(&scratch, tiny());
    assert_eq!(wal.stream(), stream);
    assert_eq!(wal.commit(body(5, 6), 0), Ok(()));
    assert_eq!(*lock(&applied), [6]);
    wal.close();
    let (records, _, _) = read_back(&scratch).expect("read");
    assert_eq!(records.iter().map(|r| r.0).collect::<Vec<_>>(), [5, 6]);
}

/// Records at or behind the checkpoint are not handed over.
#[test]
fn replay_starts_past_the_checkpoint() {
    let scratch = Scratch::new("past");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 6), 0), Ok(()));
    assert!(wal.advance(|| Some(5)).0.is_some());
    wal.close();
    let (records, scanned, _) = read_back(&scratch).expect("read");
    assert_eq!(records.iter().map(|r| r.0).collect::<Vec<_>>(), [5, 6]);
    assert_eq!((scanned.records, scanned.behind), (2, 4));
}

/// A checkpoint is the digits `advance` writes. A sign is not one of them, so `+5`
/// is no checkpoint and reads as the 0 a missing file does.
#[test]
fn a_checkpoint_with_a_sign_is_not_a_checkpoint() {
    let scratch = Scratch::new("signed-checkpoint");
    let file = scratch.0.join(CHECKPOINT_FILE);
    assert_eq!(read_checkpoint(&scratch.0), 0);
    for (text, want) in [
        ("5", 5),
        ("5\n", 5),
        ("+5", 0),
        ("-0", 0),
        ("5 5", 0),
        ("", 0),
    ] {
        fs::write(&file, text).expect("write");
        assert_eq!(read_checkpoint(&scratch.0), want, "`{text}`");
    }
}

/// A segment a stop cut short as it was created holds no line: it is removed, and
/// the start goes on with the ones before it.
#[test]
fn a_segment_cut_short_as_it_was_created_is_removed() {
    let scratch = Scratch::new("roll");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 2), 0), Ok(()));
    wal.close();
    let half = scratch.0.join(DIR).join(segment_name(7));
    fs::write(&half, &header_bytes(1, 1)[..11]).expect("write");
    // And one that is not the newest: a roll whose write failed and whose file could
    // not be cleared away, with segments made after it.
    let stale = scratch.0.join(DIR).join(segment_name(0));
    fs::write(&stale, [0u8; HEADER_LEN]).expect("write");

    let (records, _, warnings) = read_back(&scratch).expect("read");
    assert_eq!(records.len(), 2);
    assert_eq!(
        warnings.iter().filter(|w| w.contains("is removed")).count(),
        2,
        "{warnings:?}"
    );
    assert!(!half.exists() && !stale.exists());
    // And the log goes on from where its last whole segment left it.
    let (wal, applied) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(2, 3), 0), Ok(()));
    assert_eq!(*lock(&applied), [3]);
}

#[test]
fn two_streams_in_one_directory_stop_the_start() {
    let scratch = Scratch::new("streams");
    let (wal, _) = started(&scratch, Options::default());
    let other = wal.stream() ^ 1;
    wal.close();
    fs::write(
        scratch.0.join(DIR).join(segment_name(9)),
        header_bytes(other.max(1), 1),
    )
    .expect("write");
    let mut silent = |_: String| {};
    assert!(matches!(
        Wal::open(&scratch.0, &mut silent),
        Err(WalError::Streams { .. })
    ));
}

/// A log made again in a directory that had one is given another id, which keeps its
/// positions from being held against the old one's row groups.
#[test]
fn a_log_made_again_is_a_new_stream() {
    let scratch = Scratch::new("again");
    let first = Wal::create(&scratch.0).expect("create").stream();
    let second = Wal::create(&scratch.0).expect("create").stream();
    assert!(first != 0 && second != 0);
    assert_ne!(first, second);
}

/// A log that cannot write refuses with the reason, refuses the next body without trying,
/// and writes again once asked to try - above the positions the failed flush had taken.
#[test]
#[cfg(unix)]
fn a_log_that_cannot_write_refuses_and_then_writes_again() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("readonly");
    let (wal, applied) = started(&scratch, tiny());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    let dir = scratch.0.join(DIR);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

    // The next flush needs a new segment, and the directory will not take one.
    let refused = wal.commit(body(1, 3), 0);
    assert!(matches!(refused, Err(Refused::Cannot(_))), "{refused:?}");
    assert!(!wal.is_healthy());
    assert_eq!(
        wal.commit(body(3, 4), 0).err(),
        wal.last_error().map(Refused::Cannot)
    );
    wal.probe();
    std::thread::sleep(Duration::from_millis(50));
    assert!(!wal.is_healthy());

    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
    wal.probe();
    for _ in 0..200 {
        if wal.is_healthy() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(wal.is_healthy());
    assert_eq!(wal.commit(body(4, 5), 0), Ok(()));
    // Positions 2 and 3 went with the flush that failed.
    assert_eq!(*lock(&applied), [1, 4]);
    wal.close();
    let (records, _, _) = read_back(&scratch).expect("read");
    assert_eq!(records.iter().map(|r| r.0).collect::<Vec<_>>(), [1, 4]);
}

/// Once the checkpoint has reached the end of the log, the segment being written is left
/// at the next flush and deleted at the tick after, not kept until it fills.
#[test]
fn a_segment_with_nothing_left_to_replay_is_left_and_deleted() {
    let scratch = Scratch::new("drained");
    // Small enough that three lines are worth leaving it for.
    let options = Options {
        segment_bytes: 4096,
        ..Options::default()
    };
    let (wal, _) = started(&scratch, options);
    assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
    assert_eq!(scratch.segments().len(), 1);
    assert_eq!(
        wal.advance(|| None).0,
        Some(Advanced {
            checkpoint: 3,
            deleted: 0
        })
    );
    // Still the one segment: nothing has been written since.
    assert_eq!(scratch.segments().len(), 1);
    assert_eq!(wal.commit(body(3, 4), 0), Ok(()));
    assert_eq!(scratch.segments().len(), 2);
    let moved = wal.advance(|| None).0.expect("moved");
    assert_eq!((moved.checkpoint, moved.deleted), (4, 1));
    assert_eq!(scratch.segments().len(), 1);
    wal.close();
    let (records, scanned, _) = read_back(&scratch).expect("read");
    assert!(records.is_empty());
    assert_eq!((scanned.behind, scanned.last_seq), (1, 4));
}

/// A log that was never started takes nothing, and says so: nothing would ever write the body.
#[test]
fn a_log_that_is_not_started_refuses() {
    let scratch = Scratch::new("idle");
    let wal = Wal::create(&scratch.0).expect("create");
    assert!(wal.commit(body(0, 1), 0).is_err());
}

/// A segment that is longer than a header and whose header will not read stops the
/// start: it may hold lines, and only one no longer than a header is a roll cut short.
#[test]
fn a_segment_longer_than_a_header_that_will_not_read_stops_the_start() {
    let scratch = Scratch::new("header");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 2), 0), Ok(()));
    wal.close();
    let path = scratch.segments().pop().expect("a segment");
    let mut bytes = fs::read(&path).expect("read");
    bytes[4] = 2;
    fs::write(&path, &bytes).expect("write");
    let mut silent = |_: String| {};
    match Wal::open(&scratch.0, &mut silent) {
        Err(WalError::Header { why, .. }) => assert!(why.contains("checksum"), "{why}"),
        other => panic!(
            "opened a segment whose header will not read: {:?}",
            other.map(|_| ())
        ),
    }
    assert!(path.exists(), "the segment was removed");
}

/// A start that cannot make its segment starts anyway, refusing: the server still has
/// queries to answer, and the probe makes the segment once the directory will take it.
#[test]
#[cfg(unix)]
fn a_start_that_cannot_make_a_segment_starts_refusing() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("start-readonly");
    let wal = Wal::create(&scratch.0).expect("create");
    let dir = scratch.0.join(DIR);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");
    wal.start(Options::default(), Box::new(|_| {}))
        .expect("start");
    assert!(!wal.is_healthy());
    assert!(wal.commit(body(0, 1), 0).is_err());

    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
    wal.probe();
    for _ in 0..200 {
        if wal.is_healthy() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(wal.is_healthy());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
}

/// Readers that ask `wrong` of the log until `stop` is set, half in two reads and half as
/// `/stats` does. Each returns how often the answer was yes, and how often it asked.
#[cfg(unix)]
fn readers_asking(
    wal: &Arc<Wal>,
    stop: &Arc<AtomicBool>,
    wrong: impl Fn(bool, Option<String>) -> bool + Send + Sync + 'static,
) -> Vec<JoinHandle<(u64, u64)>> {
    const READERS: usize = 4;
    let wrong = Arc::new(wrong);
    let ready = Arc::new(std::sync::Barrier::new(READERS + 1));
    let readers = (0..READERS)
        .map(|n| {
            let (wal, stop) = (Arc::clone(wal), Arc::clone(stop));
            let (wrong, ready) = (Arc::clone(&wrong), Arc::clone(&ready));
            std::thread::spawn(move || {
                ready.wait();
                let (mut times, mut looks) = (0u64, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    looks += 1;
                    times += u64::from(if n % 2 == 0 {
                        let healthy = wal.is_healthy();
                        wrong(healthy, wal.last_error())
                    } else {
                        let stats = wal.stats();
                        wrong(stats.healthy, stats.last_error)
                    });
                }
                (times, looks)
            })
        })
        .collect();
    ready.wait();
    // Every reader is asking before the log is made to fail.
    std::thread::sleep(Duration::from_millis(5));
    readers
}

/// The log's first failure, with readers asking "can it write?" and then "why not?" across
/// it: none is ever told the log cannot write and given no reason.
#[test]
#[cfg(unix)]
fn no_reader_sees_the_log_unhealthy_without_a_reason() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("reason-race");
    let (wal, _) = started(&scratch, tiny());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    let dir = scratch.0.join(DIR);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

    let wal = Arc::new(wal);
    let stop = Arc::new(AtomicBool::new(false));
    let readers = readers_asking(&wal, &stop, |healthy, reason| !healthy && reason.is_none());
    // The next flush needs a new segment, and the directory will not take one.
    let refused = wal.commit(body(1, 3), 0);
    std::thread::sleep(Duration::from_millis(5));
    stop.store(true, Ordering::Relaxed);
    let seen: Vec<(u64, u64)> = readers
        .into_iter()
        .map(|r| r.join().expect("reader"))
        .collect();
    let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));

    assert!(matches!(refused, Err(Refused::Cannot(_))), "{refused:?}");
    assert!(!wal.is_healthy());
    let bare: u64 = seen.iter().map(|(bare, _)| bare).sum();
    assert_eq!(
        bare, 0,
        "the log was unhealthy with no reason given, (times, looks) per reader: {seen:?}"
    );
}

/// A second failure, once the log has written again: the first failure's reason is still
/// there to be read while the log writes, and is never the reason given for the second.
#[test]
#[cfg(unix)]
fn a_second_failure_is_never_told_with_the_reason_of_the_first() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("second-reason");
    let (wal, _) = started(&scratch, tiny());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    let dir = scratch.0.join(DIR);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");
    let Err(Refused::Cannot(first)) = wal.commit(body(1, 2), 0) else {
        panic!("the log wrote to a directory that takes no file");
    };
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).expect("chmod");
    wal.probe();
    for _ in 0..200 {
        if wal.is_healthy() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(wal.is_healthy());
    // Kept, now that the log writes again.
    assert_eq!(wal.last_error(), Some(first.clone()));
    // Into the segment the probe made: the flush after it needs the next one, whose
    // name is in its error, so the two reasons differ.
    assert_eq!(wal.commit(body(2, 3), 0), Ok(()));
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

    let wal = Arc::new(wal);
    let stop = Arc::new(AtomicBool::new(false));
    let earlier = first.clone();
    let readers = readers_asking(&wal, &stop, move |healthy, reason| {
        !healthy && reason.as_deref() == Some(earlier.as_str())
    });
    let refused = wal.commit(body(3, 4), 0);
    std::thread::sleep(Duration::from_millis(5));
    stop.store(true, Ordering::Relaxed);
    let seen: Vec<(u64, u64)> = readers
        .into_iter()
        .map(|r| r.join().expect("reader"))
        .collect();
    let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));

    let Err(Refused::Cannot(second)) = refused else {
        panic!("the log wrote to a directory that takes no file: {refused:?}");
    };
    assert_ne!(first, second);
    assert_eq!(wal.last_error(), Some(second));
    let stale: u64 = seen.iter().map(|(stale, _)| stale).sum();
    assert_eq!(
        stale, 0,
        "the second failure was given the first's reason, {first}; (times, looks) per \
             reader: {seen:?}"
    );
}

/// Once a thread of the log has died the reason is to restart the server, and a flush
/// under way does not change it, by failing or by succeeding.
#[test]
fn a_flush_does_not_change_the_reason_once_a_thread_has_died() {
    let shared = Shared::new(Vec::new(), 0);
    assert_eq!(shared.refusal(), None);
    assert!(shared.cannot("no room"), "the first failure is the news");
    assert!(!shared.cannot("still no room"));
    assert_eq!(shared.refusal().as_deref(), Some("still no room"));

    shared.died("restart the server");
    assert!(!shared.cannot("no room"));
    assert!(!shared.writing());
    assert_eq!(shared.refusal().as_deref(), Some("restart the server"));
    assert_eq!(
        shared.health().report(),
        (false, Some("restart the server".to_string()))
    );
}

/// A thread of the log that dies leaves the log saying so: the body already in the log is
/// acknowledged, and neither a probe nor a close takes the one reason away.
#[test]
fn a_thread_that_dies_leaves_the_log_unhealthy_and_saying_to_restart() {
    let scratch = Scratch::new("died");
    let wal = Wal::create(&scratch.0).expect("create");
    wal.start(tiny(), Box::new(|_| panic!("the applier dies")))
        .expect("start");
    // Written before the applier is handed it: durable, so acknowledged.
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    for _ in 0..200 {
        if !wal.is_healthy() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let why = "a thread of the write-ahead log stopped; restart the server".to_string();
    let told = |wal: &Wal| (wal.is_healthy(), wal.refusal(), wal.stats().last_error);
    assert_eq!(told(&wal), (false, Some(why.clone()), Some(why.clone())));
    assert_eq!(wal.commit(body(1, 2), 0), Err(Refused::Cannot(why.clone())));
    // The writer is still there to be asked, and is not: a close waits for it.
    wal.probe();
    wal.close();
    assert_eq!(told(&wal), (false, Some(why.clone()), Some(why)));
}

/// The number in a test line, `{"n":N}`.
fn n_of(line: &str) -> u64 {
    line.trim_start_matches("{\"n\":")
        .trim_end_matches('}')
        .parse()
        .expect("a test line")
}

/// Bodies from many senders at once: whatever the flushes took together, every body is
/// whole and in its own order, and the applier is handed every position in the log's order.
#[test]
fn bodies_sent_together_each_stay_whole_and_in_order() {
    let scratch = Scratch::new("together");
    let (wal, applied) = started(&scratch, Options::default());
    let wal = Arc::new(wal);
    let senders: Vec<_> = (0..8u64)
        .map(|sender| {
            let wal = Arc::clone(&wal);
            std::thread::spawn(move || {
                for b in 0..25u64 {
                    let from = (sender * 25 + b) * 4;
                    assert_eq!(wal.commit(body(from, from + 4), sender as i64), Ok(()));
                }
            })
        })
        .collect();
    for sender in senders {
        sender.join().expect("a sender");
    }
    assert_eq!(*lock(&applied), (1..=800).collect::<Vec<u64>>());
    wal.close();

    let (records, _, _) = read_back(&scratch).expect("read");
    assert_eq!(records.len(), 800);
    for body in records.chunks(4) {
        let first = n_of(&body[0].4);
        assert_eq!(first % 4, 0, "a body was split: {body:?}");
        for (at, record) in body.iter().enumerate() {
            assert_eq!(n_of(&record.4), first + at as u64, "{body:?}");
            assert_eq!(record.2, (first / 100) as i64, "{body:?}");
        }
    }
}

/// The segments after one that ends in a torn record are read, and their positions begin
/// past anything the torn bytes could have held: a row group may name those.
#[test]
fn the_segments_after_a_torn_one_are_read_and_their_positions_are_past_it() {
    let scratch = Scratch::new("torn-then-more");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
    wal.close();
    let torn = scratch.segments().pop().expect("a segment");
    OpenOptions::new()
        .append(true)
        .open(&torn)
        .and_then(|mut f| f.write_all(&[9u8; 100]))
        .expect("append");

    let (wal, applied) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(3, 5), 0), Ok(()));
    let after = lock(&applied).clone();
    assert_eq!(after.len(), 2);
    assert!(
        after[0] > 3 + 100 * POSITIONS_PER_LOST_BYTE,
        "position {} could have been one the torn bytes held",
        after[0]
    );
    wal.close();

    let (records, scanned, _) = read_back(&scratch).expect("read");
    assert_eq!(
        records.iter().map(|r| n_of(&r.4)).collect::<Vec<_>>(),
        [0, 1, 2, 3, 4]
    );
    assert_eq!(scanned.torn, 1);
    assert_eq!(records[3].0, after[0]);
}

/// A record that fails its own checksum inside a frame whose checksum holds is not a
/// tear - the writer wrote those bytes - and is not stepped over: the start stops.
#[test]
fn a_record_that_fails_its_own_checksum_in_a_whole_frame_stops_the_start() {
    let scratch = Scratch::new("record-crc");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    wal.close();

    let mut raw = Vec::new();
    raw.extend_from_slice(&1u32.to_le_bytes());
    raw.extend_from_slice(&0i64.to_le_bytes());
    put_record(&mut raw, 2, 101, 0, b"{\"n\":1}");
    raw[12] ^= 0xff;
    let mut payload = vec![KIND_BATCH];
    payload.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    payload.extend_from_slice(&zstd::bulk::compress(&raw, ZSTD_LEVEL).expect("compress"));
    OpenOptions::new()
        .append(true)
        .open(scratch.segments().pop().expect("a segment"))
        .and_then(|mut f| f.write_all(&frame::frame(&payload)))
        .expect("append");
    match read_back(&scratch) {
        Err(WalError::Corrupt { what, .. }) => {
            assert!(what.contains("own checksum"), "{what}")
        }
        other => panic!(
            "read past a record that does not check: {:?}",
            other.map(|r| r.1)
        ),
    }
}

/// A body that is more, as records, than one frame may hold is refused before it is
/// queued: written, it would be acknowledged and then stop every start.
#[test]
fn a_body_past_what_a_frame_holds_is_refused_and_not_written() {
    let scratch = Scratch::new("too-large");
    let (wal, applied) = started(&scratch, Options::default());
    // Zeros: asked for and never touched, so the test holds a gigabyte of nothing.
    let big = Line {
        raw: vec![0u8; MAX_BODY_RAW],
        time: 0,
        placed: line(0).placed,
    };
    assert!(matches!(
        wal.commit(vec![big], 0),
        Err(Refused::TooLarge { .. })
    ));
    assert!(wal.is_healthy());
    assert_eq!(wal.commit(body(0, 1), 0), Ok(()));
    assert_eq!(*lock(&applied), [1]);
}

/// A start that skipped positions past a torn record still lets the checkpoint cross them,
/// so a server started and stopped with nothing sent does not leave a segment each time.
#[test]
fn an_idle_start_after_a_torn_record_leaves_no_segment_behind() {
    let scratch = Scratch::new("torn-idle");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
    wal.close();
    OpenOptions::new()
        .append(true)
        .open(scratch.segments().pop().expect("a segment"))
        .and_then(|mut f| f.write_all(&[9u8; 100]))
        .expect("append");

    for _ in 0..3 {
        let (wal, _) = started(&scratch, Options::default());
        let moved = wal.advance(|| None).0.expect("moved");
        assert!(moved.deleted >= 1, "{moved:?}");
        assert_eq!(scratch.segments().len(), 1);
        wal.close();
    }
    // And the positions go on from past the gap, not from inside it.
    let (wal, applied) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(3, 4), 0), Ok(()));
    assert!(lock(&applied)[0] > 3 + 100 * POSITIONS_PER_LOST_BYTE);
}

/// The look at a body before it is parsed refuses only what `commit` would: blank
/// lines are no records, however many, and a body under the bound is not refused.
#[test]
fn the_early_look_counts_lines_and_not_bytes() {
    assert_eq!(too_large(b"{\"n\":1}\n{\"n\":2}\n"), None);
    // Sixty megabytes of blank lines: walked, and nothing in it is a record.
    assert_eq!(too_large(&vec![b'\n'; 60_000_000]), None);
    // Twenty-six million lines of two bytes: 78MB of body, and past a gigabyte of
    // records.
    let many = b"{}\n".repeat(26_000_000);
    assert_eq!(
        too_large(&many),
        Some(Refused::TooLarge {
            bytes: 26_000_000 * (RECORD_HEADER + 8 + 2),
            limit: MAX_BODY_RAW
        })
    );
}

/// The checkpoint's file is a hint, and one that cannot be written keeps no segment: on a
/// full volume the segments behind the checkpoint are the room it needs.
#[test]
fn a_checkpoint_file_that_cannot_be_written_does_not_keep_a_segment() {
    let scratch = Scratch::new("no-checkpoint-file");
    let options = Options {
        segment_bytes: 1,
        ..Options::default()
    };
    let (wal, _) = started(&scratch, options);
    for at in 0..3 {
        assert_eq!(wal.commit(body(at, at + 1), 0), Ok(()));
    }
    assert_eq!(scratch.segments().len(), 3);
    // Where the file is written before it is renamed: a directory cannot be opened
    // for writing, and nothing else in `wal/` is touched.
    fs::create_dir(scratch.0.join(DIR).join(format!("{CHECKPOINT_FILE}.tmp"))).expect("mkdir");

    let (moved, problem) = wal.advance(|| None);
    assert_eq!(
        moved,
        Some(Advanced {
            checkpoint: 3,
            deleted: 2
        })
    );
    assert!(matches!(problem, Some(WalError::Io { .. })), "{problem:?}");
    assert_eq!(wal.checkpoint(), 3);
    assert_eq!(scratch.segments().len(), 1);
    assert_eq!(read_checkpoint(&scratch.0.join(DIR)), 0);

    // And the file is tried again at the next look, though nothing has moved since.
    fs::remove_dir(scratch.0.join(DIR).join(format!("{CHECKPOINT_FILE}.tmp"))).expect("rmdir");
    let (moved, problem) = wal.advance(|| None);
    assert!(
        moved.is_none() && problem.is_none(),
        "{moved:?} {problem:?}"
    );
    assert_eq!(read_checkpoint(&scratch.0.join(DIR)), 3);
    wal.close();

    let (records, scanned, _) = read_back(&scratch).expect("read");
    assert!(records.is_empty());
    assert_eq!(scanned.behind, 1);
}

/// With the checkpoint's file gone and nothing left to read, the newest segment's base is
/// all that says where the positions had got to: the next is taken from it, not from 1.
#[test]
fn positions_go_on_from_the_newest_segments_base_when_nothing_else_is_left() {
    let scratch = Scratch::new("base-only");
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(0, 3), 0), Ok(()));
    wal.close();
    // A start with nothing sent: its segment stays empty, and its first tick deletes
    // the one before it.
    let (wal, _) = started(&scratch, Options::default());
    assert_eq!(wal.advance(|| None).0.expect("moved").deleted, 1);
    wal.close();
    fs::remove_file(scratch.0.join(DIR).join(CHECKPOINT_FILE)).expect("remove");

    let (wal, applied) = started(&scratch, Options::default());
    assert_eq!(wal.commit(body(3, 4), 0), Ok(()));
    assert_eq!(*lock(&applied), [4]);
}

// ── The bound on what is between a sender and the store ───────────────────────────
//
// The slow hop in these is the applier, and the test holds it. Nothing here sleeps to
// let a thing happen; a state is waited for, and the wait's limit fails the test.

/// How many lines the bodies of these tests hold.
const EVEN: u64 = 4;

/// Body `nth` of a run of bodies that all come to the same number of bytes: its
/// lines' numbers have seven digits each.
fn even(nth: u64) -> Vec<Line> {
    let from = 1_000_000 + nth * EVEN;
    assert!(from + EVEN <= 10_000_000, "a line of another length");
    body(from, from + EVEN)
}

/// What a body counts for against the bound.
fn bytes_of(lines: &[Line]) -> usize {
    lines
        .iter()
        .map(|line| RECORD_HEADER + 8 + line.raw.len())
        .sum()
}

/// The test's hold on the applier of a log started by `gated`.
struct Gate {
    /// The rows in each batch the applier is handed, said before it stores one.
    handed: Receiver<usize>,
    /// A send lets the batch in hand through; dropped, every batch goes through.
    go: Option<Sender<()>>,
    /// The positions the applier has stored, in the order it did.
    applied: Arc<Mutex<Vec<u64>>>,
}

impl Gate {
    /// The rows of the next batch the applier is handed: it holds them, stored
    /// nowhere, until `pass` or `open`.
    fn handed(&self) -> usize {
        self.handed
            .recv_timeout(Duration::from_secs(30))
            .expect("the applier was handed no batch")
    }

    /// Let the batch in hand through, and no other.
    fn pass(&self) {
        let go = self.go.as_ref().expect("the gate is open for good");
        go.send(()).expect("the applier is gone");
    }

    /// Let every batch through from here on.
    fn open(&mut self) {
        self.go = None;
    }
}

/// A started log whose applier the test holds still. The gate is returned second, so it
/// is dropped first: a close waiting on a held applier would hang a failing test.
fn gated(scratch: &Scratch, options: Options) -> (Arc<Wal>, Gate) {
    let wal = Wal::create(&scratch.0).expect("create");
    let (said, handed) = channel::<usize>();
    let (go, told) = channel::<()>();
    let applied = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&applied);
    wal.start(
        options,
        Box::new(move |rows| {
            let _ = said.send(rows.len());
            // An error is the gate dropped: open for good.
            let _ = told.recv();
            lock(&seen).extend(rows.iter().map(|(seq, _)| *seq));
        }),
    )
    .expect("start");
    let gate = Gate {
        handed,
        go: Some(go),
        applied,
    };
    (Arc::new(wal), gate)
}

/// `commit` on a thread of its own: the body's sender, parked until its answer.
fn sender(wal: &Arc<Wal>, lines: Vec<Line>) -> JoinHandle<Result<(), Refused>> {
    let wal = Arc::clone(wal);
    std::thread::spawn(move || wal.commit(lines, 0))
}

/// Wait for a state, looking every few milliseconds. The limit fails the test: it is
/// far past what any machine needs, and nothing passes by reaching it.
fn until(what: &str, mut reached: impl FnMut() -> bool) {
    let began = std::time::Instant::now();
    while !reached() {
        assert!(
            began.elapsed() < Duration::from_secs(30),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// A line counts against the bound until the store has it, and no longer: its bytes leave
/// the bound before its sender is woken, so one that comes straight back is not shed.
#[test]
fn the_bound_counts_a_line_until_it_is_applied() {
    let scratch = Scratch::new("bound-applied");
    let one = bytes_of(&even(0));
    let options = Options {
        queue_bytes: one,
        ..Options::default()
    };
    let (wal, mut gate) = gated(&scratch, options);

    let first = sender(&wal, even(0));
    assert_eq!(gate.handed(), EVEN as usize);
    // Written, and with the applier: out of the queue, and still in the count.
    let stats = wal.stats();
    assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, one));
    assert_eq!(stats.last_seq, EVEN);
    assert_eq!(wal.commit(even(1), 0), Err(Refused::Busy));
    assert_eq!(wal.stats().queue_sheds, 1);

    gate.open();
    assert_eq!(first.join().expect("the sender"), Ok(()));
    for nth in 2..1002 {
        assert_eq!(wal.commit(even(nth), 0), Ok(()), "body {nth}");
    }
    let stats = wal.stats();
    assert_eq!(stats.queue_sheds, 1);
    assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, 0));
    assert_eq!(stats.queue_peak_bytes, one);
    assert_eq!(lock(&gate.applied).len() as u64, 1001 * EVEN);
}

/// A body's bytes leave the bound before the body is answered, on every run: the test
/// holds the first answer shut, so no luck with the processor is needed.
#[test]
fn a_body_is_out_of_the_bound_before_it_is_answered() {
    let scratch = Scratch::new("bound-before-answer");
    let options = Options {
        queue_bytes: bytes_of(&even(0)),
        ..Options::default()
    };
    let (wal, mut gate) = gated(&scratch, options);
    let stays = AtomicBool::new(false);
    let no_wait = || Wait {
        at_most: Duration::ZERO,
        left: &stays,
    };

    let first = wal
        .enqueue(even(0), 0, no_wait())
        .expect("an empty log has room");
    // Handed to the applier: the writer has said `Logged` and is done with the answer.
    assert_eq!(gate.handed(), EVEN as usize);
    let answer = Arc::clone(first.outcome.as_ref().expect("a body with lines"));
    let held = lock(&answer.progress);
    assert_eq!(*held, Progress::Logged);
    assert_eq!(wal.pending(), bytes_of(&even(0)));

    gate.open();
    until("the first body's bytes to leave the count", || {
        wal.pending() == 0
    });
    assert_eq!(
        *held,
        Progress::Logged,
        "answered before its bytes left the count"
    );
    assert_eq!(wal.stats().applied, EVEN);
    let second = wal
        .enqueue(even(1), 0, no_wait())
        .expect("room, with the first body stored and not yet answered");

    drop(held);
    assert_eq!(first.wait(), Ok(()));
    assert_eq!(second.wait(), Ok(()));
    let stats = wal.stats();
    assert_eq!((stats.queue_sheds, stats.queue_waits), (0, 0));
    assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, 0));
    assert_eq!(*lock(&gate.applied), (1..=2 * EVEN).collect::<Vec<u64>>());
}

/// How long a body a test expects to be woken may wait for room, and how long the test
/// gives it to come back: far apart, so that the wake-up is the only way out.
const LONG_WAIT: Duration = Duration::from_secs(60);
const WOKEN_WITHIN: Duration = Duration::from_secs(10);

/// `enqueue` and the wait for the answer, on a thread of its own: a sender that may
/// wait `at_most` for room, and that has gone once `left` is set.
fn waiter(
    wal: &Arc<Wal>,
    lines: Vec<Line>,
    at_most: Duration,
    left: &Arc<AtomicBool>,
) -> JoinHandle<Result<(), Refused>> {
    let (wal, left) = (Arc::clone(wal), Arc::clone(left));
    std::thread::spawn(move || {
        let wait = Wait {
            at_most,
            left: &left,
        };
        wal.enqueue(lines, 0, wait)?.wait()
    })
}

/// A flag nobody sets: a sender that stays.
fn staying() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// A log whose bound is one body, with one body written and held at the applier: no
/// other fits until the gate lets that one through.
fn full(
    scratch: &Scratch,
    options: Options,
) -> (Arc<Wal>, Gate, JoinHandle<Result<(), Refused>>) {
    let options = Options {
        queue_bytes: bytes_of(&even(0)),
        ..options
    };
    let (wal, gate) = gated(scratch, options);
    let first = sender(&wal, even(0));
    assert_eq!(gate.handed(), EVEN as usize);
    (wal, gate, first)
}

/// The numbers of the lines a log holds, read back from its files.
fn stored(scratch: &Scratch) -> Vec<u64> {
    let (records, _, warnings) = read_back(scratch).expect("read");
    assert_eq!(warnings, Vec::<String>::new());
    records.iter().map(|record| n_of(&record.4)).collect()
}

/// The numbers of the lines of these bodies, in order.
fn lines_of(bodies: &[u64]) -> Vec<u64> {
    bodies
        .iter()
        .flat_map(|nth| even(*nth))
        .map(|line| n_of(&String::from_utf8_lossy(&line.raw)))
        .collect()
}

/// A body the log has no room for waits, with no position and no byte in the log, and is
/// taken when the applier storing the body ahead of it wakes it.
#[test]
fn a_body_waits_for_room_and_is_taken_when_there_is_some() {
    let scratch = Scratch::new("room-wait");
    let (wal, mut gate, first) = full(&scratch, Options::default());
    let second = waiter(&wal, even(1), LONG_WAIT, &staying());
    until("the second body to wait for room", || {
        wal.stats().queue_waiting == 1
    });
    assert!(!second.is_finished());
    let stats = wal.stats();
    assert_eq!((stats.last_seq, stats.queue_bytes), (EVEN, 0));

    let began = std::time::Instant::now();
    gate.open();
    assert_eq!(first.join().expect("the first sender"), Ok(()));
    assert_eq!(second.join().expect("the second sender"), Ok(()));
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "the body waiting for room was not woken by the batch that made it"
    );
    assert_eq!(*lock(&gate.applied), (1..=2 * EVEN).collect::<Vec<u64>>());
    let stats = wal.stats();
    assert_eq!(
        (stats.queue_sheds, stats.queue_waits, stats.queue_waiting),
        (0, 1, 0)
    );
    wal.close();
    assert_eq!(stored(&scratch), lines_of(&[0, 1]));
}

/// What is between the senders and the store never passes the bound, however many senders
/// there are, and none of them is shed for it.
#[test]
fn memory_stays_under_the_bound_while_senders_wait() {
    const SENDERS: u64 = 8;
    const BODIES: u64 = 5;
    let scratch = Scratch::new("room-bound");
    let bound = 2 * bytes_of(&even(0));
    let options = Options {
        queue_bytes: bound,
        ..Options::default()
    };
    let (wal, mut gate) = gated(&scratch, options);
    let senders: Vec<_> = (0..SENDERS)
        .map(|sender| {
            let wal = Arc::clone(&wal);
            std::thread::spawn(move || {
                let left = AtomicBool::new(false);
                (0..BODIES)
                    .map(|nth| {
                        let wait = Wait {
                            at_most: LONG_WAIT,
                            left: &left,
                        };
                        wal.enqueue(even(sender * BODIES + nth), 0, wait)?.wait()
                    })
                    .collect::<Vec<Result<(), Refused>>>()
            })
        })
        .collect();

    let mut seen = 0;
    while seen < SENDERS * BODIES * EVEN {
        seen += gate.handed() as u64;
        let pending = wal.pending();
        assert!(
            pending <= bound,
            "{pending} bytes against a bound of {bound}"
        );
        gate.pass();
    }
    gate.open();
    for sender in senders {
        let answers = sender.join().expect("a sender");
        assert_eq!(answers, vec![Ok(()); BODIES as usize]);
    }
    let stats = wal.stats();
    assert_eq!((stats.queue_sheds, stats.queue_left), (0, 0));
    assert!(stats.queue_peak_bytes <= bound, "{stats:?}");
    assert_eq!((stats.queue_bytes, stats.unapplied_bytes), (0, 0));
    wal.close();

    // Every line once, whatever order the bodies went in.
    let mut found = stored(&scratch);
    found.sort_unstable();
    let every: Vec<u64> = (0..SENDERS * BODIES).collect();
    assert_eq!(found, lines_of(&every));
}

/// A wait that runs out is a shed. The gate is never opened while the two bodies ask, so
/// no speed turns either into an `Ok`.
#[test]
fn a_wait_that_runs_out_is_a_shed() {
    let scratch = Scratch::new("room-shed");
    let (wal, mut gate, first) = full(&scratch, Options::default());

    assert_eq!(wal.commit(even(1), 0), Err(Refused::Busy));
    let left = AtomicBool::new(false);
    let wait = Wait {
        at_most: Duration::from_millis(20),
        left: &left,
    };
    assert_eq!(wal.enqueue(even(2), 0, wait).map(drop), Err(Refused::Busy));
    let stats = wal.stats();
    assert_eq!(
        (stats.queue_sheds, stats.queue_waits, stats.queue_waiting),
        (2, 1, 0)
    );
    assert_eq!(stats.last_seq, EVEN, "a body that was shed is in the log");

    gate.open();
    assert_eq!(first.join().expect("the first sender"), Ok(()));
    wal.close();
    assert_eq!(stored(&scratch), lines_of(&[0]));
}

/// A log that fails tells every body waiting for room why: the failed flush wakes the
/// others, which are refused with the same reason and not left to wait out their minute.
#[test]
#[cfg(unix)]
fn a_log_that_fails_tells_every_body_waiting_for_room_why() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("room-failed");
    // A segment a frame long: the flush after the first needs a new file.
    let (wal, mut gate, first) = full(&scratch, tiny());
    let waiting = [
        waiter(&wal, even(1), LONG_WAIT, &staying()),
        waiter(&wal, even(2), LONG_WAIT, &staying()),
    ];
    until("both bodies to wait for room", || {
        wal.stats().queue_waiting == 2
    });
    let dir = scratch.0.join(DIR);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).expect("chmod");

    let began = std::time::Instant::now();
    gate.open();
    let first = first.join().expect("the first sender");
    let refused = waiting.map(|waiter| waiter.join().expect("a waiting sender"));
    let took = began.elapsed();
    let _ = fs::set_permissions(&dir, fs::Permissions::from_mode(0o755));

    assert_eq!(first, Ok(()));
    let why = wal
        .stats()
        .last_error
        .expect("the log says why it cannot write");
    assert_eq!(refused, [(); 2].map(|()| Err(Refused::Cannot(why.clone()))));
    assert!(
        took < WOKEN_WITHIN,
        "a body waiting for room was not woken by the flush that failed"
    );
    assert!(!wal.is_healthy());
    assert_eq!(wal.stats().queue_waiting, 0);
}

/// A sender that leaves gives up its wait, and nothing of its body is in the log: the
/// body after it takes the positions it never had.
#[test]
fn a_sender_that_leaves_gives_up_its_wait() {
    let scratch = Scratch::new("room-left");
    let (wal, mut gate, first) = full(&scratch, Options::default());
    let left = staying();
    let second = waiter(&wal, even(1), LONG_WAIT, &left);
    until("the second body to wait for room", || {
        wal.stats().queue_waiting == 1
    });

    let began = std::time::Instant::now();
    left.store(true, Ordering::Release);
    wal.wake_room();
    assert_eq!(
        second.join().expect("the second sender"),
        Err(Refused::Left)
    );
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "a body whose sender had left went on waiting for room"
    );
    let stats = wal.stats();
    assert_eq!(
        (stats.queue_left, stats.queue_sheds, stats.queue_waiting),
        (1, 0, 0)
    );

    gate.open();
    assert_eq!(first.join().expect("the first sender"), Ok(()));
    assert_eq!(wal.commit(even(2), 0), Ok(()));
    assert_eq!(*lock(&gate.applied), (1..=2 * EVEN).collect::<Vec<u64>>());
    wal.close();
    assert_eq!(stored(&scratch), lines_of(&[0, 2]));
}

/// A sender that has left is still taken when there is room: room is looked for before
/// the flag, so a body whose sender hung up while it was parsed goes in whole.
#[test]
fn a_body_whose_sender_has_left_is_taken_when_there_is_room() {
    let scratch = Scratch::new("room-left-fits");
    let (wal, applied) = started(&scratch, Options::default());
    let left = AtomicBool::new(true);
    let wait = Wait {
        at_most: Duration::ZERO,
        left: &left,
    };
    let taken = wal.enqueue(even(0), 0, wait).and_then(Ticket::wait);
    assert_eq!(taken, Ok(()));
    assert_eq!(lock(&applied).len() as u64, EVEN);
    assert_eq!(wal.stats().queue_left, 0);
}

/// Nobody waits for room once the log has been told to hurry: a stop must not take as
/// long as the longest wait.
#[test]
fn nobody_waits_once_the_log_is_told_to_hurry() {
    let scratch = Scratch::new("room-hurry");
    let (wal, mut gate, first) = full(&scratch, Options::default());
    let second = waiter(&wal, even(1), LONG_WAIT, &staying());
    until("the second body to wait for room", || {
        wal.stats().queue_waiting == 1
    });

    let began = std::time::Instant::now();
    wal.hurry();
    assert_eq!(
        second.join().expect("the second sender"),
        Err(Refused::Busy)
    );
    let third = waiter(&wal, even(2), LONG_WAIT, &staying());
    assert_eq!(third.join().expect("the third sender"), Err(Refused::Busy));
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "a body went on waiting for room in a log told to hurry"
    );
    let stats = wal.stats();
    assert_eq!(
        (stats.queue_sheds, stats.queue_waits, stats.queue_waiting),
        (2, 1, 0)
    );
    assert_eq!(stats.last_seq, EVEN);

    // The body inside is finished, as a stop finishes it.
    gate.open();
    assert_eq!(first.join().expect("the first sender"), Ok(()));
}

/// A thread of the log that dies tells the bodies waiting for room. The applier dies only
/// once the second body is seen waiting, or the test could pass with nobody having waited.
#[test]
fn a_thread_that_dies_tells_the_bodies_waiting_for_room() {
    let scratch = Scratch::new("room-died");
    let wal = Arc::new(Wal::create(&scratch.0).expect("create"));
    let (said, handed) = channel::<usize>();
    // Declared after the log, so let go first: the applier is never left held.
    let (go, told) = channel::<()>();
    let options = Options {
        queue_bytes: bytes_of(&even(0)),
        ..Options::default()
    };
    wal.start(
        options,
        Box::new(move |rows| {
            let _ = said.send(rows.len());
            let _ = told.recv();
            panic!("the applier dies");
        }),
    )
    .expect("start");

    let first = sender(&wal, even(0));
    let got = handed.recv_timeout(Duration::from_secs(30));
    assert_eq!(got, Ok(EVEN as usize));
    let second = waiter(&wal, even(1), LONG_WAIT, &staying());
    until("the second body to wait for room", || {
        wal.stats().queue_waiting == 1
    });

    let began = std::time::Instant::now();
    go.send(()).expect("the applier is there");
    let why = "a thread of the write-ahead log stopped; restart the server".to_string();
    assert_eq!(
        second.join().expect("the second sender"),
        Err(Refused::Cannot(why.clone()))
    );
    assert!(
        began.elapsed() < WOKEN_WITHIN,
        "a body waiting for room was not told that the log's thread had died"
    );
    // Written before the applier was handed it: durable, so acknowledged.
    assert_eq!(first.join().expect("the first sender"), Ok(()));
    assert_eq!(wal.refusal(), Some(why));
    assert_eq!(wal.stats().queue_waiting, 0);
}
