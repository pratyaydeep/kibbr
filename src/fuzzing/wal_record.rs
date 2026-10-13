// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The write-ahead log's reader: one segment's bytes through
//! `wal::read_segment`, held to a second reader written here from the format and none of
//! its code (`expected`). Every input is read as it is and with its checksums put right,
//! as a segment and as a batch, since a fuzzer does not guess a CRC. A segment whose
//! frames claim more than `FRAME_RAW` or `SEGMENT_RAW` is not read: the fuzzer caps memory.

use std::path::Path;

use kibbr_storage::frame;
use kibbr_storage::wal::{self, Record, Scanned, WalError};

/// The most one frame may claim to decompress to and still be read here: the reader
/// allocates the claim before a byte is decompressed, under the fuzzer's allocation limit.
const FRAME_RAW: usize = 4 * 1024 * 1024;

/// And the most every whole frame in one segment may claim between them: what bounds how
/// long one input can take, not how much it can hold at once.
const SEGMENT_RAW: usize = 16 * 1024 * 1024;

/// A record before its line, said again here and not borrowed from the reader,
/// which is what it is checked against.
const RECORD: usize = 32;

/// What a record costs its batch besides its line: its header, and its time.
const PER_RECORD: usize = RECORD + 8;

/// The four bytes a zstd frame begins with, which is where a frame of the log is looked
/// for after bytes that are not one.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// The path the reader is given: it names it in warnings and errors and never opens it.
const NAME: &str = "seg_000001.wal";

pub fn run(data: &[u8]) {
    header(data);
    as_segment(data, true);
    if let Some(sealed) = sealed_segment(data) {
        as_segment(&sealed, true);
    }
    as_batch(data);
    if let Some(sealed) = sealed_batch(data) {
        as_batch(&sealed);
    }
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(b)
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(b)
}

// ── The format, a second time ──────────────────────────────────────────────────────

/// A segment's `(stream, base)` when its first 28 bytes are a header this build reads.
fn header_of(bytes: &[u8]) -> Option<(u64, u64)> {
    let header = bytes.get(..wal::HEADER_LEN)?;
    let (stream, base) = (u64_at(header, 8), u64_at(header, 16));
    let reads = header[..4] == *b"KWAL"
        && header[4] == 1
        && header[5..8] == [0, 0, 0]
        && stream != 0
        && frame::crc32c(&header[..24]) == u32_at(header, 24);
    reads.then_some((stream, base))
}

/// The whole frame that begins at `at`, as its payload and the offset just past it; `None`
/// where the length or the checksum does not hold.
fn frame_at(bytes: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let head = bytes.get(at..)?.get(..frame::HEADER)?;
    let len = u32_at(head, 0) as usize;
    let start = at + frame::HEADER;
    if len == 0 || len > bytes.len() - start {
        return None;
    }
    let payload = &bytes[start..start + len];
    (frame::crc32c(payload) == u32_at(head, 4)).then_some((payload, start + len))
}

/// The batch a frame's payload holds, of the batch's kind and decompressing to exactly
/// the length it says; `None` is a frame the reader has to refuse.
fn batch_in(payload: &[u8]) -> Option<Vec<u8>> {
    if payload.len() < 5 || payload[0] != wal::KIND_BATCH {
        return None;
    }
    let says = u32_at(payload, 1) as usize;
    if says > wal::MAX_RAW {
        return None;
    }
    // Nothing here is to ask for more than the reader was let ask for.
    assert!(
        says <= FRAME_RAW,
        "a frame claiming {says} bytes that `claimed` let through"
    );
    let raw = zstd::bulk::decompress(&payload[5..], says).ok()?;
    (raw.len() == says).then_some(raw)
}

/// A batch's records, each with a checksum that holds, using the batch up exactly; `None`
/// is a batch the reader has to refuse.
fn records_in(raw: &[u8]) -> Option<Vec<Record<'_>>> {
    let count = u32_at(raw.get(..4)?, 0) as usize;
    let times = raw.get(4..)?.get(..count.checked_mul(8)?)?;
    let mut rest = &raw[4 + times.len()..];
    let mut out = Vec::new();
    for n in 0..count {
        let head = rest.get(..RECORD)?;
        let len = u32_at(head, 28) as usize;
        let line = rest.get(RECORD..)?.get(..len)?;
        if frame::crc32c(&rest[4..RECORD + len]) != u32_at(head, 0) {
            return None;
        }
        out.push(Record {
            seq: u64_at(head, 4),
            template_id: u64_at(head, 12),
            received_at: u64_at(head, 20) as i64,
            time: u64_at(times, n * 8) as i64,
            line,
        });
        rest = &rest[RECORD + len..];
    }
    rest.is_empty().then_some(out)
}

/// Where the next frame begins at or after `from`, as `resync`'s comment has it: one that
/// reads and whose first record is above `after`. A batch of no records is not one.
fn next_frame(bytes: &[u8], from: usize, after: u64) -> Option<usize> {
    (from..bytes.len()).find(|at| {
        // The kind and the magic first, which spares a checksum at nearly every offset.
        let payload = at + frame::HEADER;
        if bytes.get(payload) != Some(&wal::KIND_BATCH)
            || bytes.get(payload + 5..payload + 9) != Some(&ZSTD_MAGIC[..])
        {
            return false;
        }
        frame_at(bytes, *at)
            .and_then(|(payload, _)| batch_in(payload))
            .is_some_and(|raw| {
                records_in(&raw).is_some_and(|found| found.first().is_some_and(|r| r.seq > after))
            })
    })
}

/// What reading `bytes` has to come to, by `replay`'s comment: damage stepped over
/// when a frame follows it, the torn end when none does, a whole unreadable frame refused.
fn expected(bytes: &[u8], checkpoint: u64, before: Before) -> Read {
    let (mut found, mut lost) = before;
    let mut read = Read {
        end: End::Stopped(bytes.len()),
        found,
        lost,
        warnings: 0,
        records: Vec::new(),
        raw: 0,
    };
    let mut at = wal::HEADER_LEN;
    read.end = 'frames: loop {
        if at == bytes.len() {
            break End::Stopped(at);
        }
        let Some((payload, end)) = frame_at(bytes, at) else {
            read.warnings += 1;
            match next_frame(bytes, at + 1, found.last_seq) {
                Some(next) => {
                    lost += (next - at) as u64;
                    found.corrupt += 1;
                    at = next;
                    continue;
                }
                None => {
                    lost += (bytes.len() - at) as u64;
                    found.torn += 1;
                    break End::Stopped(at);
                }
            }
        };
        let Some(raw) = batch_in(payload) else {
            break End::Refused(at);
        };
        let Some(records) = records_in(&raw) else {
            break End::Refused(at);
        };
        for record in records {
            if record.seq <= found.last_seq {
                break 'frames End::Refused(at);
            }
            found.last_seq = record.seq;
            if record.seq <= checkpoint {
                found.behind += 1;
                continue;
            }
            found.records += 1;
            read.raw += PER_RECORD + record.line.len();
            read.records.push((record.seq, digest(&record)));
        }
        at = end;
    };
    read.found = found;
    read.lost = lost;
    read
}

// ── A header ───────────────────────────────────────────────────────────────────────

/// `wal::read_header` reads the headers the format says it reads and no others: over `data`
/// as it is, over its first 28 bytes alone, and over those with their checksum put right.
fn header(data: &[u8]) {
    let agrees = |bytes: &[u8]| match (wal::read_header(bytes), header_of(bytes)) {
        (Ok(said), Some(holds)) => assert_eq!(said, holds, "a header's stream and base"),
        (Err(why), None) => assert!(!why.is_empty(), "a header refused without a reason"),
        (Ok(said), None) => panic!("a header this build does not read was read as {said:?}"),
        (Err(why), Some(holds)) => panic!("the header of {holds:?} was refused: {why}"),
    };
    agrees(data);
    agrees(&data[..data.len().min(wal::HEADER_LEN)]);
    if let Some(head) = data.get(..wal::HEADER_LEN) {
        let mut sealed = [0u8; wal::HEADER_LEN];
        sealed.copy_from_slice(head);
        let crc = frame::crc32c(&sealed[..24]);
        sealed[24..].copy_from_slice(&crc.to_le_bytes());
        agrees(&sealed);
    }
}

// ── A segment ──────────────────────────────────────────────────────────────────────

/// How a read of one segment ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// At this offset: the segment's length, or where its torn end begins.
    Stopped(usize),
    /// At the frame at this offset, whose checksum holds and whose contents do not read.
    Refused(usize),
}

/// One read of one segment: everything the reader said.
#[derive(Debug, PartialEq, Eq)]
struct Read {
    end: End,
    found: Scanned,
    lost: u64,
    /// The warnings this read gave: one for each stretch it could not read.
    warnings: usize,
    /// Each record handed over: its position, and a digest of all of it.
    records: Vec<(u64, u64)>,
    /// What the records handed over came to in their batches.
    raw: usize,
}

/// What a replay has counted when it comes to a segment, and the bytes it has lost: it
/// carries both from one segment to the next.
type Before = (Scanned, u64);

/// Before the first segment: nothing.
const FIRST: Before = (
    Scanned {
        segments: 0,
        records: 0,
        behind: 0,
        torn: 0,
        corrupt: 0,
        last_seq: 0,
    },
    0,
);

/// A record as two checksums, one of its fields and one of its line: enough to tell two
/// reads apart without keeping a copy of every line either read handed over.
fn digest(record: &Record<'_>) -> u64 {
    let mut fields = [0u8; 36];
    fields[..8].copy_from_slice(&record.seq.to_le_bytes());
    fields[8..16].copy_from_slice(&record.template_id.to_le_bytes());
    fields[16..24].copy_from_slice(&record.received_at.to_le_bytes());
    fields[24..32].copy_from_slice(&record.time.to_le_bytes());
    fields[32..].copy_from_slice(&(record.line.len() as u32).to_le_bytes());
    u64::from(frame::crc32c(&fields)) << 32 | u64::from(frame::crc32c(record.line))
}

fn read(bytes: &[u8], checkpoint: u64, before: Before) -> Read {
    let (mut found, mut lost) = before;
    let mut warnings = 0usize;
    let mut records = Vec::new();
    let mut raw = 0usize;
    let result = wal::read_segment(
        Path::new(NAME),
        bytes,
        checkpoint,
        &mut found,
        &mut lost,
        &mut |_| warnings += 1,
        &mut |record| {
            raw += PER_RECORD + record.line.len();
            records.push((record.seq, digest(&record)));
            Ok(())
        },
    );
    let end = match result {
        Ok(stop) => End::Stopped(stop),
        Err(WalError::Corrupt { path, offset, what }) => {
            assert_eq!(path, Path::new(NAME), "the error names another file");
            assert!(!what.is_empty(), "a frame refused without a reason");
            End::Refused(usize::try_from(offset).expect("an offset past usize"))
        }
        // No file is opened, and the callback here never fails.
        Err(other) => panic!("reading a segment's bytes ended in: {other}"),
    };
    Read {
        end,
        found,
        lost,
        warnings,
        records,
        raw,
    }
}

/// What the whole frames in `bytes` claim to decompress to, added up at every offset, as
/// the reader looks at every offset after damage. `None` when more than is read here.
fn claimed(bytes: &[u8]) -> Option<usize> {
    let mut total = 0usize;
    for at in wal::HEADER_LEN..bytes.len() {
        // The kind first: it spares a checksum at every offset that could not be a batch.
        if bytes.get(at + frame::HEADER) != Some(&wal::KIND_BATCH) {
            continue;
        }
        let Some((payload, _)) = frame_at(bytes, at) else {
            continue;
        };
        if payload.len() < 5 {
            continue;
        }
        let raw_len = u32_at(payload, 1) as usize;
        // Past the reader's own bound a claim is refused before anything is allocated.
        if raw_len > wal::MAX_RAW {
            continue;
        }
        total += raw_len;
        if raw_len > FRAME_RAW || total > SEGMENT_RAW {
            return None;
        }
    }
    Some(total)
}

/// Read `bytes` as `Wal::open` then `Wal::replay` would; `None` for bytes the reader is
/// never given. `every_way` adds the second reader and the re-reads, for unknown contents.
fn as_segment(bytes: &[u8], every_way: bool) -> Option<Read> {
    // `open` reads the header before `replay` reads anything, and a segment whose header
    // will not read is removed or stops the start: its frames are never looked at.
    let header = header_of(bytes);
    assert_eq!(wal::read_header(bytes).ok(), header, "the segment's header");
    header?;
    let claimed = claimed(bytes)?;
    let whole = read(bytes, 0, FIRST);
    hold(bytes, &whole, claimed);
    if !every_way {
        return Some(whole);
    }
    assert_eq!(whole, expected(bytes, 0, FIRST), "what the segment holds");

    match whole.end {
        // Cut at its torn end, a segment reads as it did: the same records, the same
        // stretches stepped over, and no tear.
        End::Stopped(stop) if stop < bytes.len() => {
            let cut = read(&bytes[..stop], 0, FIRST);
            let tail = (bytes.len() - stop) as u64;
            assert_eq!(cut.end, End::Stopped(stop), "cut at its torn end");
            assert_eq!(cut.records, whole.records, "cut at its torn end");
            assert_eq!(
                cut.found,
                Scanned {
                    torn: 0,
                    ..whole.found
                },
                "cut at its torn end"
            );
            assert_eq!(cut.lost + tail, whole.lost, "cut at its torn end");
            assert_eq!(cut.warnings + 1, whole.warnings, "cut at its torn end");
        }
        End::Stopped(_) => {}
        // And cut just past the frame that was refused, it is refused the same way: what
        // follows a frame has no say in how the frame reads.
        End::Refused(offset) => {
            let Some((_, end)) = frame_at(bytes, offset) else {
                unreachable!("`hold` found a whole frame at {offset}");
            };
            if end < bytes.len() {
                assert_eq!(
                    read(&bytes[..end], 0, FIRST),
                    whole,
                    "cut after the refused frame"
                );
            }
        }
    }

    // A checkpoint keeps back the records at or behind it and changes nothing else: put
    // at the middle record, the read hands over the ones after it and counts the rest.
    if !whole.records.is_empty() {
        let behind = whole.records.len() / 2 + 1;
        let checkpoint = whole.records[behind - 1].0;
        let past = read(bytes, checkpoint, FIRST);
        assert_eq!(past.end, whole.end, "past a checkpoint");
        assert_eq!(past.records, whole.records[behind..], "past a checkpoint");
        assert_eq!(
            past.found,
            Scanned {
                records: (whole.records.len() - behind) as u64,
                behind: behind as u64,
                ..whole.found
            },
            "past a checkpoint"
        );
        assert_eq!(
            (past.lost, past.warnings),
            (whole.lost, whole.warnings),
            "past a checkpoint"
        );
    }

    // A segment is seldom the first a replay reads: earlier counts are added to, and its
    // records must be above the last position, put at or just under its first by the input.
    let first = whole.records.first().map_or(1, |r| r.0);
    let under = bytes.len().is_multiple_of(2) || whole.records.is_empty();
    let before = (
        Scanned {
            segments: 3,
            records: 5,
            behind: 2,
            torn: 1,
            corrupt: 4,
            last_seq: if under { first - 1 } else { first },
        },
        7,
    );
    let later = read(bytes, 0, before);
    assert_eq!(later, expected(bytes, 0, before), "as a later segment");
    if under {
        assert_eq!(
            (later.end, later.warnings, &later.records),
            (whole.end, whole.warnings, &whole.records),
            "as a later segment"
        );
        assert_eq!(
            (later.found, later.lost),
            (
                Scanned {
                    segments: 3,
                    records: 5 + whole.found.records,
                    behind: 2,
                    torn: 1 + whole.found.torn,
                    corrupt: 4 + whole.found.corrupt,
                    last_seq: whole.found.last_seq.max(first - 1),
                },
                7 + whole.lost
            ),
            "as a later segment"
        );
    } else {
        assert!(
            later.records.iter().all(|r| r.0 > first),
            "a position at or under {first} handed over after it"
        );
    }
    Some(whole)
}

/// What is true of a read of any segment, with nothing read before it and no checkpoint.
fn hold(bytes: &[u8], read: &Read, claimed: usize) {
    let len = bytes.len();
    let body = (len - wal::HEADER_LEN) as u64;
    let Scanned {
        segments,
        records,
        behind,
        torn,
        corrupt,
        last_seq,
    } = read.found;

    // Positions start at 1 and only grow.
    let mut last = 0u64;
    for (seq, _) in &read.records {
        assert!(*seq > last, "position {seq} was handed over after {last}");
        last = *seq;
    }
    assert_eq!(records, read.records.len() as u64, "records counted");
    assert_eq!((segments, behind), (0, 0), "counted with nothing to count");
    assert_eq!(
        last_seq,
        read.records.last().map_or(0, |r| r.0),
        "the last position"
    );

    // A record comes out of a whole frame's batch: no more of them than those hold.
    assert!(
        read.raw <= claimed,
        "{} bytes of records out of frames that hold {claimed}",
        read.raw
    );

    // One warning for each stretch that would not read, and each stretch is bytes of
    // this segment, counted once.
    assert_eq!(read.warnings as u64, corrupt + torn as u64, "warnings");
    assert!(torn <= 1, "{torn} torn ends in one segment");
    assert!(
        corrupt <= read.lost,
        "{corrupt} stretches in {} bytes",
        read.lost
    );
    assert!(
        read.lost <= body,
        "{} bytes lost out of {body} after the header",
        read.lost
    );

    match read.end {
        End::Stopped(stop) => {
            assert!(
                (wal::HEADER_LEN..=len).contains(&stop),
                "stopped at {stop} of {len}"
            );
            assert_eq!(torn, usize::from(stop < len), "stopped at {stop} of {len}");
            // The torn end is lost whole, and each stretch stepped over is more.
            let tail = (len - stop) as u64;
            assert!(read.lost >= tail + corrupt, "lost {} of {tail}", read.lost);
            assert!(
                corrupt > 0 || read.lost == tail,
                "lost {} of {tail}",
                read.lost
            );
            // Where it stopped there is no frame: the end, or bytes that are not one.
            assert!(
                frame_at(bytes, stop).is_none(),
                "stopped at {stop}, where a whole frame begins"
            );
        }
        End::Refused(offset) => {
            // The error says the frame's checksum holds: so there is a whole frame there.
            assert!(
                offset >= wal::HEADER_LEN && frame_at(bytes, offset).is_some(),
                "refused at {offset} of {len}, where no whole frame begins"
            );
            assert_eq!(torn, 0, "a torn end before the refused frame");
            assert!(
                read.lost <= (offset - wal::HEADER_LEN) as u64,
                "lost {} before offset {offset}",
                read.lost
            );
        }
    }
}

/// `data` as a segment with its header and frame checksums put right, as far as one frame's
/// length leads to the next. `None` when that is `data` as it stands, or under a header.
fn sealed_segment(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < wal::HEADER_LEN {
        return None;
    }
    let mut out = data.to_vec();
    // 0 is no stream, and no header carries it.
    let (stream, base) = (u64_at(data, 8).max(1), u64_at(data, 16));
    let header = wal::header_bytes(stream, base);
    assert_eq!(
        header_of(&header),
        Some((stream, base)),
        "the header a writer writes"
    );
    out[..wal::HEADER_LEN].copy_from_slice(&header);
    let mut at = wal::HEADER_LEN;
    while at + frame::HEADER <= out.len() {
        let len = u32_at(&out, at) as usize;
        let start = at + frame::HEADER;
        if len == 0 || len > out.len() - start {
            break;
        }
        let crc = frame::crc32c(&out[start..start + len]);
        out[at + 4..start].copy_from_slice(&crc.to_le_bytes());
        at = start + len;
    }
    (out != data).then_some(out)
}

// ── A batch ────────────────────────────────────────────────────────────────────────

/// A segment a writer could have written: its header, and then `parts` back to back.
fn segment_of(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = wal::header_bytes(1, 1).to_vec();
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

/// A frame's payload as the writer lays it out: the kind, the length the batch is said
/// to have, and the batch as one zstd frame.
fn payload_of(kind: u8, says: u32, packed: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(5 + packed.len());
    payload.push(kind);
    payload.extend_from_slice(&says.to_le_bytes());
    payload.extend_from_slice(packed);
    payload
}

fn packed(raw: &[u8]) -> Vec<u8> {
    zstd::bulk::compress(raw, 3).expect("zstd compresses a small batch")
}

/// One flush's frame around `raw`, built as the writer builds it.
fn frame_of(raw: &[u8]) -> Vec<u8> {
    frame::frame(&payload_of(wal::KIND_BATCH, raw.len() as u32, &packed(raw)))
}

/// `data` as one batch before compression: read by `wal::records`, held to the format,
/// and then written into segments that have to read back as what `records` said.
fn as_batch(data: &[u8]) {
    // What the harness builds from it is a few times its size.
    if data.len() > FRAME_RAW / 4 {
        return;
    }
    // The reader takes the batches the format says are batches and no others, and a
    // record's line is those bytes of the batch and no copy of them.
    let records = match (wal::records(data), records_in(data)) {
        (Ok(said), Some(holds)) => {
            assert_eq!(said, holds, "a batch's records");
            for (n, (said, holds)) in said.iter().zip(&holds).enumerate() {
                assert!(
                    std::ptr::eq(said.line, holds.line),
                    "record {n}: its line is not where the batch has it"
                );
            }
            Some(said)
        }
        (Err(why), None) => {
            assert!(!why.is_empty(), "a batch refused without a reason");
            None
        }
        (Ok(said), None) => panic!("{} records out of a batch that does not read", said.len()),
        (Err(why), Some(holds)) => panic!("a batch of {} records refused: {why}", holds.len()),
    };

    // A frame's payload is its batch only when it says what it is and how long it is: of
    // another kind, or a byte longer or shorter than it says, it is refused.
    let says = data.len() as u32;
    let packed = packed(data);
    match wal::unpack(&payload_of(wal::KIND_BATCH, says, &packed)) {
        Ok(raw) => assert!(raw == data, "a batch does not come back out of its frame"),
        Err(why) => panic!("a frame the writer could have written was refused: {why}"),
    }
    let pick = frame::crc32c(data);
    let kind = match pick.to_le_bytes()[0] {
        wal::KIND_BATCH => 0,
        other => other,
    };
    let lies = [
        payload_of(kind, says, &packed),
        payload_of(wal::KIND_BATCH, says + 1, &packed),
        payload_of(wal::KIND_BATCH, says.wrapping_sub(1), &packed),
    ];
    for lie in &lies {
        assert!(
            wal::unpack(lie).is_err(),
            "a frame of kind {} that says {} was read as a batch of {says} bytes",
            lie[0],
            u32_at(lie, 1)
        );
    }

    let whole = frame::frame(&payload_of(wal::KIND_BATCH, says, &packed));
    let one = segment_of(&[&whole]);
    // `None` is a line that holds a whole frame claiming more than is read here.
    let Some(read) = as_segment(&one, true) else {
        return;
    };

    // A batch that does not read is a frame that does not: refused where it begins,
    // with nothing handed over - every record is checked before the first is.
    let Some(records) = records else {
        assert_eq!(
            read.end,
            End::Refused(wal::HEADER_LEN),
            "a batch that does not read"
        );
        assert!(
            read.records.is_empty(),
            "records out of a batch that does not read"
        );
        return;
    };
    let digests: Vec<(u64, u64)> = records.iter().map(|r| (r.seq, digest(r))).collect();

    // In a segment, the records come back for as long as their positions grow. The first
    // that does not is where the frame is refused, with the ones before it handed over.
    let mut rising = 0;
    let mut last = 0u64;
    while rising < records.len() && records[rising].seq > last {
        last = records[rising].seq;
        rising += 1;
    }
    assert_eq!(read.records, digests[..rising], "one frame, read back");
    if rising < records.len() {
        assert_eq!(read.end, End::Refused(wal::HEADER_LEN), "a position twice");
        return;
    }
    assert_eq!(read.end, End::Stopped(one.len()), "one frame, read back");
    assert_eq!((read.lost, read.warnings), (0, 0), "one frame, read back");

    // Written again, the batch is the bytes it was read from: nothing in it is read two
    // ways, and nothing in it is skipped.
    let again = batch_of(&records, 0);
    assert!(again == data, "a batch written again from its records");

    // In a segment, a frame that lies is refused where it begins, and the frame before
    // it has been read by then. One of the three, picked by the input.
    let lie = frame::frame(&lies[pick as usize % lies.len()]);
    let lying = segment_of(&[&whole, &lie]);
    let Some(read) = as_segment(&lying, false) else {
        return;
    };
    assert_eq!(read.end, End::Refused(one.len()), "a frame that lies");
    assert_eq!(read.records, digests, "a frame that lies");

    // The rest takes a second flush: the same records, each as far past its position as
    // the last of them is, so every one is past the first flush and they still grow.
    let shift = last;
    if records.is_empty() || last.checked_add(shift).is_none() {
        return;
    }
    let later = frame_of(&batch_of(&records, shift));
    let later_digests: Vec<(u64, u64)> = records
        .iter()
        .map(|r| {
            let moved = Record {
                seq: r.seq + shift,
                ..*r
            };
            (moved.seq, digest(&moved))
        })
        .collect();
    let both: Vec<(u64, u64)> = digests.iter().chain(&later_digests).copied().collect();

    // Two flushes read back as both, in order.
    let two = segment_of(&[&whole, &later]);
    let Some(read) = as_segment(&two, false) else {
        return;
    };
    assert_eq!(read.end, End::Stopped(two.len()), "two frames, read back");
    assert_eq!(read.records, both, "two frames, read back");
    assert_eq!((read.lost, read.warnings), (0, 0), "two frames, read back");

    // The first flush a second time is positions a second time: refused, where it begins.
    let twice = segment_of(&[&whole, &later, &whole]);
    let Some(read) = as_segment(&twice, false) else {
        return;
    };
    assert_eq!(read.end, End::Refused(two.len()), "a frame twice");
    assert_eq!(read.records, both, "a frame twice");

    // Eight zero bytes between two flushes are a stretch that will not read with a whole
    // frame after it: stepped over, and all that is lost.
    let gap = segment_of(&[&whole, &[0u8; 8], &later]);
    let Some(read) = as_segment(&gap, false) else {
        return;
    };
    assert_eq!(
        read.end,
        End::Stopped(gap.len()),
        "a gap between two frames"
    );
    assert_eq!(read.records, both, "a gap between two frames");
    assert_eq!(
        (read.lost, read.found.corrupt, read.found.torn),
        (8, 1, 0),
        "a gap between two frames"
    );

    // The other way round the later frame holds positions already read, so the gap and it
    // are the torn end - unless a line holds bytes that read as a frame.
    let back = segment_of(&[&later, &[0u8; 8], &whole]);
    let Some(read) = as_segment(&back, false) else {
        return;
    };
    assert!(
        read.records.starts_with(&later_digests),
        "the frame before a gap"
    );
    if read.found.corrupt == 0 {
        assert_eq!(
            read.end,
            End::Stopped(wal::HEADER_LEN + later.len()),
            "an earlier frame after a gap"
        );
        assert_eq!(read.records, later_digests, "an earlier frame after a gap");
        assert_eq!(
            (read.lost, read.found.torn),
            (8 + whole.len() as u64, 1),
            "an earlier frame after a gap"
        );
    }

    // A second flush a stop cut short is a torn end, wherever in it the cut falls: the
    // input picks where. The same holds of a frame found inside it.
    let kept = 1 + pick as usize % (later.len() - 1);
    let torn = segment_of(&[&whole, &later[..kept]]);
    let Some(read) = as_segment(&torn, false) else {
        return;
    };
    assert!(
        read.records.starts_with(&digests),
        "the frame before a torn one"
    );
    if read.found.corrupt == 0 {
        assert_eq!(read.end, End::Stopped(one.len()), "a torn second frame");
        assert_eq!(read.records, digests, "a torn second frame");
        assert_eq!(
            (read.lost, read.found.torn),
            (kept as u64, 1),
            "a torn second frame"
        );
    }
}

/// A batch of `records`, each at `shift` past its position, built as the writer builds it.
fn batch_of(records: &[Record<'_>], shift: u64) -> Vec<u8> {
    let size: usize = records.iter().map(|r| PER_RECORD + r.line.len()).sum();
    let mut raw = Vec::with_capacity(4 + size);
    raw.extend_from_slice(&(records.len() as u32).to_le_bytes());
    for record in records {
        raw.extend_from_slice(&record.time.to_le_bytes());
    }
    for record in records {
        wal::put_record(
            &mut raw,
            record.seq + shift,
            record.template_id,
            record.received_at,
            record.line,
        );
    }
    raw
}

/// `data` as a batch with its records' checksums put right, for as far as one record's
/// length leads to the next. `None` when that is `data` as it stands.
fn sealed_batch(data: &[u8]) -> Option<Vec<u8>> {
    let count = u32_at(data.get(..4)?, 0) as usize;
    let mut at = count.checked_mul(8)?.checked_add(4)?;
    let mut out = data.to_vec();
    for _ in 0..count {
        let Some(head) = out.get(at..at + RECORD) else {
            break;
        };
        let end = at + RECORD + u32_at(head, 28) as usize;
        let Some(body) = out.get(at + 4..end) else {
            break;
        };
        let crc = frame::crc32c(body);
        out[at..at + 4].copy_from_slice(&crc.to_le_bytes());
        at = end;
    }
    (out != data).then_some(out)
}
