// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! What is timed: ingest alone, the idle server, and one lap under ingest.
//! Nothing of the bench's own sits inside a timed window but a write, a read and a clock:
//! requests are built beforehand and answers typed after their clock has stopped. The ingest
//! rate is the server's own count over its own clock, between two `/stats` answers. A lap's
//! senders are stopped by killing the server, since a flag is only read between attempts.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Condvar, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::engine::commas;

use super::child::{Server, Stats};
use super::corpus::{self, Corpus, LiveStream};
use super::host::{self, Cpu};
use super::report::{
    AtEnd, Beside, BesideKeyed, BesideRate, Expect, Four, Health, IdleBlock, IdleRun, IngestRun,
    Laps, LoadedBlock, LoadedRun, Refused, ScanBlock, ScanRun, SenderFacts, Seven,
};
use super::wire::{self, Conn, QueryAnswer};
use super::{
    place_lines, scan_asks, warmup, SpeedError, MAX_ROWS, REFUSAL_PAUSE, SCAN_TAIL, SENDERS,
    SILENT_LIMIT, SOCKET_TIMEOUT, STALL,
};

/// Between two reads of the lines acknowledged, while a lap waits for a query's place. A
/// place is then good to a fraction of one body.
const PLACE_POLL: Duration = Duration::from_micros(200);

/// Room for an answer's head, beside the room for its body.
const ANSWER_HEAD_ROOM: usize = 64 * 1024;

/// Between two looks, while the main thread waits for the senders to stand at the gate.
const GATE_POLL: Duration = Duration::from_micros(100);

/// How often the main thread looks at the lines acknowledged while ingest runs alone. It
/// asks the server nothing; the look is for a server that has stopped taking bodies.
const STALL_TICK: Duration = Duration::from_secs(1);

/// What every unit starts from, all made before the first run: a unit that needed
/// something built would build it inside its window.
pub struct Work<'a> {
    /// The `kibbr` measured: every server is it.
    pub exe: &'a Path,
    /// This binary, which the plan step is run as.
    pub bench: &'a Path,
    pub loaded: &'a Corpus,
    /// Where copies and each child's standard error go.
    pub work: &'a Path,
    /// `--ingest-concurrency` of every server.
    pub cores: usize,
    /// `--scan-threads` of every server, where the bench was given `--threads`.
    pub threads: Option<usize>,
    pub stream: &'a LiveStream,
    /// One `/ingest` request head for each body of the stream.
    pub heads: &'a [Vec<u8>],
    /// The seven query requests, head and body.
    pub requests: &'a Seven<Vec<u8>>,
    pub expect: &'a Seven<Expect>,
    /// Room each unit's connection is given before it asks anything, enough for the
    /// largest answer the first server gave, so that no sample grows a buffer.
    pub answer_room: usize,
    pub lines: u64,
    pub samples: usize,
    pub body_lines: usize,
}

/// One query's sample in a lap.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Sample {
    pub client_ms: f64,
    pub server_ms: Option<f64>,
    /// Live lines acknowledged when the query was asked, and when it was answered.
    pub at_lines: u64,
    pub end_lines: u64,
    pub count: u64,
    pub rows_scanned: Option<u64>,
    /// Times the query was refused before it was answered.
    pub refused: u64,
}

/// One lap: the four samples, and the server as the lap left it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Lap {
    pub samples: Four<Sample>,
    /// From the server's spawn to its first `/health`.
    pub start_s: f64,
    /// Lines the server took, and its own clock, between the `/stats` asked before the
    /// senders were released and the one asked after the fourth query.
    pub lines: u64,
    pub seconds: f64,
    /// The server's counters in that second answer, which are the lap's because the
    /// server is new.
    pub forced_closes: Option<u64>,
    pub buffered_bytes: Option<u64>,
    pub requests_shed: Option<u64>,
    /// Writes to the store that failed in that same answer: a store that cannot write goes
    /// on acknowledging lines, so a lap on one is not a lap on this build's store.
    pub write_failures: Option<u64>,
    pub degraded: Option<bool>,
    /// Times the senders were refused a body, by either status; a body refused twice
    /// counts twice.
    pub refused_bodies: u64,
    /// Times a sender reached the end of its share of the stream and began it again.
    pub wraps: u64,
}

/// The value at nearest rank `p` of `values`, the `ceil(p x n)`-th smallest, defined for
/// every `n` above 0 and `None` for none.
pub fn percentile<T: Copy + PartialOrd>(values: &[T], p: f64) -> Option<T> {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    // The product is a hair off a whole number wherever `p` is not a binary fraction, and
    // a hair above one would round up to the next rank.
    let rank = (p * sorted.len() as f64 - 1e-9).ceil() as usize;
    sorted.get(rank.clamp(1, sorted.len().max(1)) - 1).copied()
}

/// The middle of `values`: nearest rank at 0.50, so with an even count it is the lower of
/// the two in the middle, and always one of the values.
pub fn median<T: Copy + PartialOrd>(values: &[T]) -> Option<T> {
    percentile(values, 0.50)
}

// ── A server, and one connection to it ─────────────────────────────────────────────

/// Where to look when a server answered something the bench cannot use.
fn whose(server: &Server) -> String {
    format!(
        "server pid {}, whose standard error is in {}",
        server.pid,
        server.log.display()
    )
}

/// A connection to a server the bench has just started.
fn connect(server: &Server) -> Result<Conn, SpeedError> {
    Conn::open(server.addr, SOCKET_TIMEOUT).map_err(|error| SpeedError::Surface {
        surface: "the server's socket",
        detail: format!(
            "a connection to {} could not be opened: {error} ({})",
            server.addr,
            whose(server)
        ),
    })
}

/// A new server on a new copy of the loaded directory, and the one connection the main
/// thread keeps to it.
fn started(work: &Work) -> Result<(Server, Conn), SpeedError> {
    let server = Server::start(work.exe, work.loaded, work.work, work.cores, work.threads)?;
    let mut conn = connect(&server)?;
    conn.reserve(work.answer_room);
    Ok((server, conn))
}

/// Hold a `/stats` answer to a server that has taken no line: a new one, or the idle one
/// at its end.
fn no_line(server: &Server, stats: Stats, when: &str) -> Result<Stats, SpeedError> {
    if stats.lines_ingested != 0 {
        return Err(SpeedError::Surface {
            surface: "GET /stats",
            detail: format!(
                "ingest.lines_ingested is {} {when}, where no line has been sent ({})",
                commas(stats.lines_ingested),
                whose(server)
            ),
        });
    }
    Ok(stats)
}

/// `/stats` of a server that must not have taken a line.
fn stats_of_none(server: &Server, conn: &mut Conn, when: &str) -> Result<Stats, SpeedError> {
    no_line(server, server.stats(conn)?, when)
}

// ── One query ──────────────────────────────────────────────────────────────────────

/// One answer to one query, and what it cost the caller.
#[derive(Debug)]
struct Asked {
    /// From just before the request's first byte was written to just after the answer's
    /// last byte was read, the pause after each refusal included.
    took: Duration,
    /// Times it was refused before it was answered.
    refused: u64,
    /// The length of the answer's body.
    bytes: usize,
    answer: QueryAnswer,
}

fn as_ms(took: Duration) -> f64 {
    took.as_secs_f64() * 1e3
}

/// Ask one query down a kept connection, and time it. A refusal is counted and the query
/// sent again after `REFUSAL_PAUSE`: the sample is what the caller waited, from the first try.
fn ask(
    conn: &mut Conn,
    server: &Server,
    name: &'static str,
    request: &[u8],
) -> Result<Asked, SpeedError> {
    ask_within(conn, || whose(server), name, request, STALL)
}

/// `ask`, with how long a query may go on being refused. The limit is for a lap: the main
/// thread asks, so nothing else would stop senders storing the stream until the volume is full.
fn ask_within(
    conn: &mut Conn,
    whose: impl Fn() -> String,
    name: &'static str,
    request: &[u8],
    limit: Duration,
) -> Result<Asked, SpeedError> {
    let unread = |detail: String| SpeedError::Surface {
        surface: "POST /query",
        detail: format!("{name}: {detail} ({})", whose()),
    };
    let mut refused: u64 = 0;
    let started = Instant::now();
    let (took, answer) = loop {
        let sent = conn.send(request, &[]);
        let took = started.elapsed();
        match sent {
            Ok(answer) if answer.status == 200 => break (took, answer),
            Ok(answer) if matches!(answer.status, 503 | 429) => {
                refused += 1;
                if took >= limit {
                    return Err(SpeedError::Query {
                        name,
                        status: answer.status,
                        body: format!(
                            "it was refused {} over {:.0} s and never answered, and a query \
                             still refused {} s after its first try is not asked again. The \
                             last refusal: {} ({})",
                            if refused == 1 {
                                "once".to_string()
                            } else {
                                format!("{} times", commas(refused))
                            },
                            took.as_secs_f64(),
                            limit.as_secs(),
                            wire::excerpt(conn.body(&answer)),
                            whose()
                        ),
                    });
                }
                thread::sleep(REFUSAL_PAUSE);
                if answer.close || !answer.wrote_all {
                    conn.reopen().map_err(|error| {
                        unread(format!(
                            "it was refused with {}, and no connection could be made to ask \
                             again: {error}",
                            answer.status
                        ))
                    })?;
                }
            }
            Ok(answer) => {
                return Err(SpeedError::Query {
                    name,
                    status: answer.status,
                    body: wire::excerpt(conn.body(&answer)),
                })
            }
            Err(error) => return Err(unread(error.to_string())),
        }
    };
    let read = wire::query_answer(conn.body(&answer))?;
    if answer.close {
        conn.reopen().map_err(|error| {
            unread(format!(
                "its answer closed the connection, and the next was refused: {error}"
            ))
        })?;
    }
    Ok(Asked {
        took,
        refused,
        bytes: answer.body.len(),
        answer: read,
    })
}

/// Whether an answer is the expected one, in every part two asks of one query over the
/// same bytes share. Compared in place: this runs between two asks of a block.
fn is(expect: &Expect, got: &QueryAnswer) -> bool {
    expect.count == got.count
        && expect.rows_returned == got.rows_returned
        && expect.templates_matched == got.templates_matched
        && expect.truncated == got.truncated
        && expect.limit_source == got.limit_source
}

/// An answer in the words an error quotes it in.
fn quoted(expect: &Expect) -> String {
    serde_json::to_string(expect).unwrap_or_else(|_| format!("count {}", expect.count))
}

/// Hold an answer from a server that has taken no line to the one the preflight kept:
/// a sample of another answer is not a sample.
fn same(name: &'static str, expect: &Expect, got: &QueryAnswer) -> Result<(), SpeedError> {
    if is(expect, got) {
        return Ok(());
    }
    Err(SpeedError::Query {
        name,
        status: 200,
        body: format!(
            "a server that has taken no line answered {} where the first server on these \
             bytes answered {}",
            quoted(&got.expect()),
            quoted(expect)
        ),
    })
}

/// S3 carries no `LIMIT`, so the server's own limit on rows applies to it and must never
/// be reached: a truncated answer is one that stopped counting what it read.
fn within_backstop(name: &'static str, got: &QueryAnswer) -> Result<(), SpeedError> {
    if got.truncated != Some(true) && got.count <= MAX_ROWS {
        return Ok(());
    }
    Err(SpeedError::Query {
        name,
        status: 200,
        body: format!(
            "it counted {} rows{} where the server cuts a fetch with no LIMIT at {}: the one \
             id it asks for no longer names a few rows",
            commas(got.count),
            if got.truncated == Some(true) {
                ", truncated,"
            } else {
                ""
            },
            commas(MAX_ROWS)
        ),
    })
}

/// Hold an answer from a server that is taking lines to what can be held of it: the
/// loaded rows are still there, so no count and no fetch can be less than without the live ones.
fn under_ingest(name: &'static str, expect: &Expect, got: &QueryAnswer) -> Result<(), SpeedError> {
    if got.count < expect.count {
        return Err(SpeedError::Query {
            name,
            status: 200,
            body: format!(
                "under ingest it counted {} where the loaded rows alone count {}: rows that \
                 were loaded are no longer in its answer",
                commas(got.count),
                commas(expect.count)
            ),
        });
    }
    if let (Some(was), Some(now)) = (expect.rows_returned, got.rows_returned) {
        if now < was {
            return Err(SpeedError::Query {
                name,
                status: 200,
                body: format!(
                    "under ingest it returned {} rows where it returns {} of the loaded \
                     rows alone",
                    commas(now),
                    commas(was)
                ),
            });
        }
    }
    Ok(())
}

// ── Before the first run ───────────────────────────────────────────────────────────

/// A failure of the look before the first run, about what a query answered.
fn unexpected(detail: String) -> SpeedError {
    SpeedError::Surface {
        surface: "POST /query",
        detail,
    }
}

/// One server on a copy, asked everything a run will ask before a run is spent; returns
/// the seven expected answers and the room a unit's connection is given for an answer.
#[allow(clippy::too_many_arguments)]
pub fn preflight(
    exe: &Path,
    loaded: &Corpus,
    work: &Path,
    cores: usize,
    threads: Option<usize>,
    stream: &LiveStream,
    heads: &[Vec<u8>],
    requests: &Seven<Vec<u8>>,
) -> Result<(Seven<Expect>, usize), SpeedError> {
    let server = Server::start(exe, loaded, work, cores, threads)?;
    let mut conn = connect(&server)?;

    let health = server.health(&mut conn)?;
    if health.status != "ok" {
        return Err(SpeedError::Surface {
            surface: "GET /health",
            detail: format!(
                "a server on a copy of the loaded directory said `{}` ({}), and what a \
                 server in that state does is not measured ({})",
                health.status,
                health.detail.unwrap_or_default(),
                whose(&server)
            ),
        });
    }

    let stats = stats_of_none(&server, &mut conn, "on a server that has just started")?;
    let rows = loaded.marker.load.rows;
    if stats.rows.is_some_and(|stored| stored != rows) {
        return Err(SpeedError::Surface {
            surface: "GET /stats",
            detail: format!(
                "storage.rows is {} on a copy of {}, where the load that made it reported \
                 {} rows ({})",
                stats.rows.map_or_else(String::new, commas),
                loaded.dir.display(),
                commas(rows),
                whose(&server)
            ),
        });
    }

    let mut answers = Vec::with_capacity(Seven::<()>::NAMES.len());
    let mut largest = 0;
    for (name, request) in Seven::<()>::NAMES.into_iter().zip(requests.each()) {
        let asked = ask(&mut conn, &server, name, request)?;
        largest = largest.max(asked.bytes);
        answers.push(asked.answer);
    }
    let answers: [QueryAnswer; 7] = answers
        .try_into()
        .map_err(|_| unexpected("the seven queries did not give seven answers".to_string()))?;
    let [s1, s2, s3, s4, key, scan, scan_key] = &answers;
    let [n1, n2, n3, n4, n_key, n_scan, n_scan_key] = Seven::<()>::NAMES;

    // A count of 0 cannot tell a filter from a broken one, and lets a later build settle
    // the query from a dictionary without reading a row.
    for (name, answer) in [(n1, s1), (n2, s2), (n3, s3), (n_scan, scan)] {
        if answer.count == 0 {
            return Err(unexpected(format!(
                "{name} counts no row of the loaded directory: a query that matches nothing \
                 measures nothing, and can be settled without reading a row. A day of so few \
                 lines may not hold what it asks for; run the bench with more --lines"
            )));
        }
    }
    // The two counts are of the rows the two filters must look at, so neither filter can
    // match more than its count.
    for (name, counted, of, part) in [(n_key, key, n2, s2), (n_scan_key, scan_key, n_scan, scan)] {
        if counted.count < part.count {
            return Err(unexpected(format!(
                "{name} counts {} rows that hold the key, and {of}, which filters on that \
                 key, counts {}",
                commas(counted.count),
                commas(part.count)
            )));
        }
    }
    within_backstop(n3, s3)?;
    if s4.count != rows {
        return Err(unexpected(format!(
            "{n4} matches every row and counts {}, where the load reported {} rows",
            commas(s4.count),
            commas(rows)
        )));
    }
    if s4.rows_returned.is_some_and(|returned| returned == 0) {
        return Err(unexpected(format!(
            "{n4} returned no row of the {} it counted",
            commas(s4.count)
        )));
    }

    // One body, the way a sender sends it.
    let (Some(head), Some(body)) = (heads.first(), stream.bodies.first()) else {
        return Err(SpeedError::Corpus(
            "the live stream holds no body to send".to_string(),
        ));
    };
    let unread = |detail: String| SpeedError::Surface {
        surface: "POST /ingest",
        detail: format!("{detail} ({})", whose(&server)),
    };
    let answer = conn
        .send(head, &stream.bytes[body.range.clone()])
        .map_err(|error| unread(format!("the first body of the live stream: {error}")))?;
    if answer.status != 200 || !answer.wrote_all {
        return Err(unread(format!(
            "the first body of the live stream, {} lines, was answered {}: {}",
            commas(body.lines as u64),
            answer.status,
            wire::excerpt(conn.body(&answer))
        )));
    }
    let Some(taken) = wire::ingested(conn.body(&answer)) else {
        return Err(unread(format!(
            "its 200 holds no `ingested` count: {}",
            wire::excerpt(conn.body(&answer))
        )));
    };
    if answer.close {
        conn.reopen()
            .map_err(|error| unread(format!("its 200 closed the connection: {error}")))?;
    }
    let after = server.stats(&mut conn)?;
    if after.lines_ingested != taken {
        return Err(SpeedError::Surface {
            surface: "GET /stats",
            detail: format!(
                "ingest.lines_ingested is {} after one body whose answer said `ingested` \
                 {}: the count the ingest rate is read off does not move by what the server \
                 acknowledges ({})",
                commas(after.lines_ingested),
                commas(taken),
                whose(&server)
            ),
        });
    }

    // Where this system has a source of a child's CPU time, it must give one for a server.
    if host::cpu(std::process::id()).is_some() && host::cpu(server.pid).is_none() {
        return Err(SpeedError::Surface {
            surface: "the system's count of a child's CPU time",
            detail: format!(
                "it gives this process a CPU time and gives none for the server, so no scan \
                 rate could be worked out ({})",
                whose(&server)
            ),
        });
    }

    let room = largest.saturating_mul(2).saturating_add(ANSWER_HEAD_ROOM);
    Ok((
        Seven::from_array(answers.map(|answer| answer.expect())),
        room,
    ))
}

// ── The idle server ────────────────────────────────────────────────────────────────

/// One block of asks of one query, back to back, as it was taken.
struct Block {
    client_ms: Vec<f64>,
    server_ms: Vec<Option<f64>>,
    /// The first timed answer: every one after it was held to the same expected answer.
    first: QueryAnswer,
    /// `meta.rows_scanned` over the timed asks, where every one of them gave it.
    rows_scanned_sum: Option<u64>,
    /// The server process's CPU time just before the first timed ask and just after the
    /// last.
    cpu: Option<(Cpu, Cpu)>,
    wall_s: f64,
    /// The same reading after the server was left alone, where the block has a tail.
    cpu_tail: Option<Cpu>,
    refused: u64,
}

impl Block {
    fn cpu_user_s(&self) -> Option<f64> {
        self.cpu
            .map(|(before, after)| (after.user_s - before.user_s).max(0.0))
    }

    fn cpu_system_s(&self) -> Option<f64> {
        self.cpu
            .map(|(before, after)| (after.system_s - before.system_s).max(0.0))
    }

    /// User plus system CPU the server process used across the timed asks.
    fn cpu_s(&self) -> Option<f64> {
        self.cpu
            .map(|(before, after)| (after.total_s() - before.total_s()).max(0.0))
    }

    /// What it used after them, with nothing asked.
    fn cpu_tail_s(&self) -> Option<f64> {
        let (_, after) = self.cpu?;
        Some((self.cpu_tail?.total_s() - after.total_s()).max(0.0))
    }
}

/// The values of a list that has one for every sample, or `None`.
fn every<T: Copy>(values: &[Option<T>]) -> Option<Vec<T>> {
    values.iter().copied().collect()
}

/// `rows` over `seconds`, where there are seconds to divide by: a block over which the
/// system reported no CPU time at all has no rate, and nothing is divided by 0.
fn per_second(rows: u64, seconds: Option<f64>) -> Option<f64> {
    seconds
        .filter(|seconds| *seconds > 0.0)
        .map(|seconds| rows as f64 / seconds)
}

/// `unmeasured` asks whose answers are checked, then `asks` timed ones back to back with
/// the server's CPU time read around them, then the tail where the block has one.
#[allow(clippy::too_many_arguments)]
fn block(
    conn: &mut Conn,
    server: &Server,
    name: &'static str,
    request: &[u8],
    expect: &Expect,
    unmeasured: usize,
    asks: usize,
    tail: Option<Duration>,
) -> Result<Block, SpeedError> {
    for _ in 0..unmeasured {
        let asked = ask(conn, server, name, request)?;
        same(name, expect, &asked.answer)?;
    }

    let mut client_ms = Vec::with_capacity(asks);
    let mut server_ms = Vec::with_capacity(asks);
    let mut first: Option<QueryAnswer> = None;
    let mut rows_scanned_sum = Some(0u64);
    let mut refused = 0;

    let before = host::cpu(server.pid);
    let began = Instant::now();
    for _ in 0..asks {
        let asked = ask(conn, server, name, request)?;
        same(name, expect, &asked.answer)?;
        client_ms.push(as_ms(asked.took));
        server_ms.push(asked.answer.duration_ms);
        rows_scanned_sum = rows_scanned_sum
            .zip(asked.answer.rows_scanned)
            .map(|(sum, rows)| sum + rows);
        refused += asked.refused;
        if first.is_none() {
            first = Some(asked.answer);
        }
    }
    let wall_s = began.elapsed().as_secs_f64();
    let after = host::cpu(server.pid);

    let cpu_tail = tail.and_then(|tail| {
        thread::sleep(tail);
        host::cpu(server.pid)
    });
    Ok(Block {
        client_ms,
        server_ms,
        first: first.unwrap_or_default(),
        rows_scanned_sum,
        cpu: before.zip(after),
        wall_s,
        cpu_tail,
        refused,
    })
}

/// A block of one of the four queries, as a result keeps it.
fn idle_block(block: &Block) -> IdleBlock {
    let of = |p: f64| percentile(&block.client_ms, p).unwrap_or_default();
    let server = every(&block.server_ms);
    IdleBlock {
        p50_ms: of(0.50),
        p99_ms: of(0.99),
        min_ms: of(0.0),
        max_ms: of(1.0),
        server_p50_ms: server.as_deref().and_then(|ms| percentile(ms, 0.50)),
        server_p99_ms: server.as_deref().and_then(|ms| percentile(ms, 0.99)),
        client_ms: block.client_ms.clone(),
        server_ms: block.server_ms.clone(),
        answer: block.first.expect(),
        // A timed answer that was not the expected one stopped the run where it was read.
        answers_identical: true,
        rows_scanned: block.first.rows_scanned,
        row_groups_read: block.first.row_groups_read,
        row_groups_pruned: block.first.row_groups_pruned,
        bytes_read: block.first.bytes_read,
        files_opened: block.first.files_opened,
        leaves_opened: block.first.leaves_opened,
        rows_scanned_sum: block.rows_scanned_sum,
        cpu_s: block.cpu_s(),
        cpu_user_s: block.cpu_user_s(),
        cpu_system_s: block.cpu_system_s(),
        wall_s: block.wall_s,
        refused: block.refused,
    }
}

/// The scan rate. The rows are the data's - the key's count times the asks - and not what
/// the server says it scanned, which a later build is free to count another way.
fn scan_run(scan_key_rows: u64, key_rows: u64, scan: &Block, blocks: &Four<Block>) -> ScanRun {
    let asks = scan.client_ms.len();
    let rows = scan_key_rows * asks as u64;
    let cpu_s = scan.cpu_s();
    let wall = Some(scan.wall_s);
    let beside = |block: &Block| BesideRate {
        rows_per_cpu_s: block
            .rows_scanned_sum
            .and_then(|rows| per_second(rows, block.cpu_s())),
    };
    let s2_asks = blocks.s2.client_ms.len();
    let s2_rows = key_rows * s2_asks as u64;
    ScanRun {
        query: "SCAN".to_string(),
        key_rows: scan_key_rows,
        asks,
        rows,
        cpu_s,
        rows_per_cpu_s: per_second(rows, cpu_s),
        wall_s: scan.wall_s,
        cores_busy: cpu_s
            .filter(|_| scan.wall_s > 0.0)
            .map(|cpu| cpu / scan.wall_s),
        rows_per_wall_s: per_second(rows, wall),
        cpu_tail_s: scan.cpu_tail_s(),
        server_rows_scanned_sum: scan.rows_scanned_sum,
        beside: Beside {
            s2: BesideKeyed {
                key_rows,
                asks: s2_asks,
                rows: s2_rows,
                cpu_s: blocks.s2.cpu_s(),
                rows_per_cpu_s: per_second(s2_rows, blocks.s2.cpu_s()),
                leaves_opened: blocks.s2.first.leaves_opened,
            },
            s1: beside(&blocks.s1),
            s3: beside(&blocks.s3),
            s4: beside(&blocks.s4),
        },
    }
}

/// The idle server, a new one that is never sent a line: the two counts, the scan block
/// first so that what precedes it is the same in every build, then the four queries' blocks.
pub fn idle(work: &Work) -> Result<(IdleRun, ScanRun), SpeedError> {
    let (server, mut conn) = started(work)?;
    let (requests, expect) = (work.requests, work.expect);
    let [n1, n2, n3, n4, n_key, n_scan, n_scan_key] = Seven::<()>::NAMES;

    // The rows S2 and the scan block's query must look at, each from an answer.
    let key = ask(&mut conn, &server, n_key, &requests.key)?;
    same(n_key, &expect.key, &key.answer)?;
    let scan_key = ask(&mut conn, &server, n_scan_key, &requests.scan_key)?;
    same(n_scan_key, &expect.scan_key, &scan_key.answer)?;

    let unmeasured = warmup(work.samples);
    let scan = block(
        &mut conn,
        &server,
        n_scan,
        &requests.scan,
        &expect.scan,
        unmeasured,
        scan_asks(work.samples),
        Some(SCAN_TAIL),
    )?;

    let mut four = Vec::with_capacity(Four::<()>::NAMES.len());
    for ((name, request), expected) in [n1, n2, n3, n4]
        .into_iter()
        .zip(requests.four().each())
        .zip(expect.four().each())
    {
        let taken = block(
            &mut conn,
            &server,
            name,
            request,
            expected,
            unmeasured,
            work.samples,
            None,
        )?;
        if name == n3 {
            within_backstop(name, &taken.first)?;
        }
        four.push(taken);
    }
    let blocks: [Block; 4] = four.try_into().map_err(|_| SpeedError::Surface {
        surface: "POST /query",
        detail: "the four queries did not give four blocks".to_string(),
    })?;
    let blocks = Four::from_array(blocks);

    let stats = stats_of_none(&server, &mut conn, "at the end of the idle phase")?;
    let health = server.health(&mut conn)?;

    let key_rows = key.answer.count;
    let scan_key_rows = scan_key.answer.count;
    let run = scan_run(scan_key_rows, key_rows, &scan, &blocks);
    let idle = IdleRun {
        server: server.unit(),
        key_rows,
        scan_key_rows,
        scan: ScanBlock {
            client_ms: scan.client_ms.clone(),
            server_ms: scan.server_ms.clone(),
            answer: scan.first.expect(),
            answers_identical: true,
            asks: scan.client_ms.len(),
            rows_scanned: scan.first.rows_scanned,
            row_groups_read: scan.first.row_groups_read,
            files_opened: scan.first.files_opened,
            leaves_opened: scan.first.leaves_opened,
            rows_scanned_sum: scan.rows_scanned_sum,
            cpu_s: scan.cpu_s(),
            cpu_user_s: scan.cpu_user_s(),
            cpu_system_s: scan.cpu_system_s(),
            wall_s: scan.wall_s,
            cpu_tail_s: scan.cpu_tail_s(),
            tail_ms: SCAN_TAIL.as_millis() as u64,
            refused: scan.refused,
        },
        queries: blocks.map(idle_block),
        health_at_end: health,
        stats_at_end: stats.body,
    };
    Ok((idle, run))
}

// ── The senders ────────────────────────────────────────────────────────────────────

/// What the senders wait at with their connections open, so that no handshake is in a
/// window and all four start on the same instant.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    opened: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        while !*open {
            open = self
                .opened
                .wait(open)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    fn open(&self) {
        *self.open.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.opened.notify_all();
    }
}

/// What one sender counts. Atomics, because the main thread reads them for an error
/// while the sender is still running.
#[derive(Default)]
struct Counts {
    acked_bodies: AtomicU64,
    acked_lines: AtomicU64,
    refused_503: AtomicU64,
    refused_429: AtomicU64,
    /// Refusals read after a write that failed part way: the server answered before it
    /// had read the body.
    refused_before_body_end: AtomicU64,
    /// Attempts that read no status line and whose body was not all written.
    no_answer: AtomicU64,
    /// Attempts that read no status line after the body was written whole: it may have
    /// been taken.
    unknown_outcome: AtomicU64,
    reconnects: AtomicU64,
    wraps: AtomicU64,
    short_bodies: AtomicU64,
    short_lines: AtomicU64,
    /// The longest `retry-after` a refusal carried, in seconds, plus one; 0 for none.
    retry_after: AtomicU64,
}

impl Counts {
    fn refused(&self) -> u64 {
        self.refused_503.load(Relaxed) + self.refused_429.load(Relaxed)
    }

    fn retry_after_max_s(&self) -> Option<u64> {
        self.retry_after.load(Relaxed).checked_sub(1)
    }
}

/// What the senders and the main thread share.
#[derive(Default)]
struct Shared {
    gate: Gate,
    /// Senders that have reached the gate.
    waiting: AtomicUsize,
    /// Raised by the main thread when the unit is over, whichever way it ended.
    stop: AtomicBool,
    /// Raised by a sender that ended in an error, so that the main thread stops waiting
    /// for a place the others may never reach.
    failed: AtomicBool,
    /// Lines acknowledged by every sender together: what a lap's places are counted in.
    acked_lines: AtomicU64,
    counts: [Counts; SENDERS],
}

impl Shared {
    /// Wait until every sender stands at the gate, so that the `/stats` asked next is
    /// asked of a server that has all of their connections and none of their bodies.
    fn all_waiting(&self) {
        while self.waiting.load(Relaxed) < SENDERS {
            thread::sleep(GATE_POLL);
        }
    }

    /// Each sender's refusals and silences, for an error that has to say why no line
    /// came through.
    fn said(&self) -> String {
        let each: Vec<String> = self
            .counts
            .iter()
            .enumerate()
            .map(|(sender, counts)| {
                format!(
                    "sender {}: {} bodies answered 200, refused 503 x {}, 429 x {}, {} \
                     attempts with no answer",
                    sender + 1,
                    commas(counts.acked_bodies.load(Relaxed)),
                    commas(counts.refused_503.load(Relaxed)),
                    commas(counts.refused_429.load(Relaxed)),
                    commas(counts.no_answer.load(Relaxed) + counts.unknown_outcome.load(Relaxed)),
                )
            })
            .collect();
        each.join("; ")
    }
}

/// Raises the flag and opens the gate when the main thread leaves an ingesting unit by
/// any way, a panic among them: senders left waiting at a gate would never be joined.
struct Release<'a>(&'a Shared);

impl Drop for Release<'_> {
    fn drop(&mut self) {
        self.0.stop.store(true, Relaxed);
        self.0.gate.open();
    }
}

/// How one body ended.
#[derive(Debug, PartialEq, Eq)]
enum Sent {
    /// Answered 200, with the lines the server said it took.
    Taken(u64),
    /// The stop flag was up: the unit is over, and what became of the body is not asked.
    Stopped,
}

/// Connect again after an answer that closed the socket, or an attempt that read none.
/// A connection that cannot be made is met by the next attempt, which counts as silent.
fn reconnect(conn: &mut Conn, mine: &Counts) {
    if conn.reopen().is_ok() {
        mine.reconnects.fetch_add(1, Relaxed);
    }
}

/// Send one body until it is answered 200, each attempt classified by what was read.
/// `SILENT_LIMIT` attempts in a row with no status line end the sender; any answer resets it.
fn send_body(
    conn: &mut Conn,
    head: &[u8],
    body: &[u8],
    lines: u64,
    sender: usize,
    shared: &Shared,
) -> Result<Sent, SpeedError> {
    let mine = &shared.counts[sender];
    let stopped = || shared.stop.load(Relaxed);
    // Attempts in a row that read no status line, and the last of them.
    let mut silent = 0;
    let mut last = None;
    loop {
        match conn.send(head, body) {
            Ok(answer) if answer.status == 200 && answer.wrote_all => {
                let Some(taken) = wire::ingested(conn.body(&answer)) else {
                    // An answer cut short by the kill that ends a lap has no count.
                    if stopped() {
                        return Ok(Sent::Stopped);
                    }
                    return Err(SpeedError::Sender {
                        sender,
                        detail: format!(
                            "a body was answered 200 with no `ingested` count: {}",
                            wire::excerpt(conn.body(&answer))
                        ),
                    });
                };
                shared.acked_lines.fetch_add(taken, Relaxed);
                mine.acked_lines.fetch_add(taken, Relaxed);
                mine.acked_bodies.fetch_add(1, Relaxed);
                if taken < lines {
                    mine.short_bodies.fetch_add(1, Relaxed);
                    mine.short_lines.fetch_add(lines - taken, Relaxed);
                }
                if answer.close {
                    reconnect(conn, mine);
                }
                return Ok(Sent::Taken(taken));
            }
            Ok(answer) if matches!(answer.status, 503 | 429) => {
                if answer.status == 503 {
                    mine.refused_503.fetch_add(1, Relaxed);
                } else {
                    mine.refused_429.fetch_add(1, Relaxed);
                }
                silent = 0;
                if let Some(wait) = answer.retry_after {
                    mine.retry_after.fetch_max(wait.saturating_add(1), Relaxed);
                }
                if !answer.wrote_all {
                    mine.refused_before_body_end.fetch_add(1, Relaxed);
                }
                if answer.close || !answer.wrote_all {
                    reconnect(conn, mine);
                }
                thread::sleep(REFUSAL_PAUSE);
            }
            Ok(answer) => {
                if stopped() {
                    return Ok(Sent::Stopped);
                }
                return Err(SpeedError::Sender {
                    sender,
                    detail: format!(
                        "a body was answered {}{}: {}",
                        answer.status,
                        if answer.wrote_all {
                            ""
                        } else {
                            " before it was all written"
                        },
                        wire::excerpt(conn.body(&answer))
                    ),
                });
            }
            Err(_) if stopped() => return Ok(Sent::Stopped),
            Err(error) => {
                silent += 1;
                if error.wrote_all {
                    // Written whole and not answered: it may have been taken, so the wait
                    // before it is sent again is whatever the server then makes of it.
                    mine.unknown_outcome.fetch_add(1, Relaxed);
                    reconnect(conn, mine);
                } else {
                    mine.no_answer.fetch_add(1, Relaxed);
                    reconnect(conn, mine);
                    thread::sleep(REFUSAL_PAUSE);
                }
                last = Some(error);
            }
        }
        if silent == SILENT_LIMIT {
            return Err(SpeedError::Sender {
                sender,
                detail: format!(
                    "{SILENT_LIMIT} attempts in a row read no answer{}",
                    last.map_or_else(String::new, |error| format!("; the last: {}", error.source))
                ),
            });
        }
        if stopped() {
            return Ok(Sent::Stopped);
        }
    }
}

/// How long a sender goes on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rounds {
    /// Its share of the stream, once: ingest alone, whose window is the stream.
    Once,
    /// Its share again and again until the unit is over: a lap, which is bounded by its
    /// four queries, so that the load never stops under a sample.
    UntilStopped,
}

/// What a sender hands back when it returns.
struct Returned {
    result: Result<(), SpeedError>,
    /// When each of its bodies was acknowledged, with the lines taken, where the sender
    /// was asked to note them.
    acks: Vec<(Instant, u64)>,
    ended: Instant,
}

/// One sender: wait at the gate, then send its share of the stream in order, flat out,
/// with nothing generated, cut or allocated for a body.
fn send_share(at: usize, mut conn: Conn, work: &Work, shared: &Shared, rounds: Rounds) -> Returned {
    let share = corpus::share(work.stream.bodies.len(), at, SENDERS);
    // Sized before the gate. A lap notes nothing: its lines by the second are not read.
    let noting = rounds == Rounds::Once;
    let mut acks = Vec::with_capacity(if noting { share.len() } else { 0 });

    shared.waiting.fetch_add(1, Relaxed);
    shared.gate.wait();

    let mut result = Ok(());
    'sending: while !share.is_empty() && !shared.stop.load(Relaxed) {
        for body in &share {
            let (Some(head), Some(body)) = (work.heads.get(*body), work.stream.bodies.get(*body))
            else {
                break 'sending;
            };
            let bytes = &work.stream.bytes[body.range.clone()];
            match send_body(&mut conn, head, bytes, body.lines as u64, at, shared) {
                Ok(Sent::Taken(lines)) => {
                    if noting {
                        acks.push((Instant::now(), lines));
                    }
                }
                Ok(Sent::Stopped) => break 'sending,
                Err(error) => {
                    result = Err(error);
                    break 'sending;
                }
            }
        }
        if rounds == Rounds::Once {
            break;
        }
        // The end of the share was reached: those lines are now sent a second time.
        shared.counts[at].wraps.fetch_add(1, Relaxed);
    }
    if result.is_err() {
        shared.failed.store(true, Relaxed);
    }
    Returned {
        result,
        acks,
        ended: Instant::now(),
    }
}

/// Start a server's four senders, run `main` on this thread beside them, then end the
/// unit. A sender's own error is the unit's, before whatever the main thread then met.
fn beside_senders<T>(
    work: &Work,
    server: Server,
    rounds: Rounds,
    main: impl FnOnce(&Server, &Shared, &Receiver<usize>) -> Result<T, SpeedError>,
) -> Result<(T, Shared, Vec<Returned>), SpeedError> {
    let mut conns = Vec::with_capacity(SENDERS);
    for _ in 0..SENDERS {
        conns.push(connect(&server)?);
    }
    let shared = Shared::default();
    let (done, returned) = mpsc::channel();

    let (measured, joined) = thread::scope(|scope| {
        // Declared before the release, so that on any way out the flag is raised and the gate
        // opened first, the server killed second, and the senders joined after both.
        let server = server;
        let release = Release(&shared);
        let shared = &shared;
        let handles: Vec<_> = conns
            .into_iter()
            .enumerate()
            .map(|(at, conn)| {
                let done = done.clone();
                scope.spawn(move || {
                    let back = send_share(at, conn, work, shared, rounds);
                    // The main thread is woken by this, not by a clock.
                    let _ = done.send(at);
                    back
                })
            })
            .collect();
        drop(done);

        let measured = main(&server, shared, &returned);
        drop(release);
        drop(server);
        let joined: Vec<_> = handles.into_iter().map(|handle| handle.join()).collect();
        (measured, joined)
    });

    let mut back = Vec::with_capacity(SENDERS);
    let mut failed = None;
    for (at, joined) in joined.into_iter().enumerate() {
        match joined {
            Ok(mut returned) => {
                if let Err(error) = std::mem::replace(&mut returned.result, Ok(())) {
                    failed.get_or_insert(error);
                }
                back.push(returned);
            }
            Err(_) => {
                failed.get_or_insert(SpeedError::Sender {
                    sender: at,
                    detail: "its thread panicked".to_string(),
                });
            }
        }
    }
    if let Some(error) = failed {
        return Err(error);
    }
    Ok((measured?, shared, back))
}

/// A sender ended in an error, which the unit returns in place of this one once the
/// senders are joined.
fn a_sender_failed() -> SpeedError {
    SpeedError::Sender {
        sender: SENDERS,
        detail: "one of them ended in an error".to_string(),
    }
}

/// The `/stats` that opens a window, and the gate, opened the moment the answer's last
/// byte is read: the answer is typed after, so reading it is not at the head of the window.
fn open_the_gate(
    server: &Server,
    conn: &mut Conn,
    shared: &Shared,
) -> Result<(Stats, Instant), SpeedError> {
    shared.all_waiting();
    let answer = server.ask_stats(conn)?;
    let opened = Instant::now();
    shared.gate.open();
    let stats = server.read_stats(conn, &answer)?;
    let stats = no_line(server, stats, "before the senders were released")?;
    Ok((stats, opened))
}

// ── Ingest alone ───────────────────────────────────────────────────────────────────

/// What the main thread read around the ingest window.
struct Window {
    before: Stats,
    after: Stats,
    /// The gate's opening: what each sender's seconds and the lines by the second are
    /// counted from.
    opened: Instant,
    /// From just before the first `/stats` was asked to just after the second was read.
    bench_s: f64,
    cpu: Option<(Cpu, Cpu)>,
    health: Health,
}

/// The main thread's part of ingest alone.
fn ingest_window(
    server: &Server,
    conn: &mut Conn,
    shared: &Shared,
    returned: &Receiver<usize>,
) -> Result<Window, SpeedError> {
    shared.all_waiting();
    let cpu_before = host::cpu(server.pid);
    let began = Instant::now();
    let (before, opened) = open_the_gate(server, conn, shared)?;

    // Woken by each sender as it returns; between them, once a second, a look at this
    // process's own count for a server that has stopped taking bodies. The server is not asked.
    let mut left = SENDERS;
    let mut seen = 0;
    let mut moved = Instant::now();
    while left > 0 {
        match returned.recv_timeout(STALL_TICK) {
            Ok(_) => left -= 1,
            Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                let acked = shared.acked_lines.load(Relaxed);
                if acked != seen {
                    seen = acked;
                    moved = Instant::now();
                } else if moved.elapsed() >= STALL {
                    return Err(SpeedError::Sender {
                        sender: SENDERS,
                        detail: format!(
                            "no body was acknowledged for {} s, with {} lines acknowledged \
                             so far; {} ({})",
                            STALL.as_secs(),
                            commas(acked),
                            shared.said(),
                            whose(server)
                        ),
                    });
                }
            }
        }
        if shared.failed.load(Relaxed) {
            return Err(a_sender_failed());
        }
    }

    let after = server.stats(conn)?;
    let bench_s = began.elapsed().as_secs_f64();
    let cpu_after = host::cpu(server.pid);
    let health = server.health(conn)?;
    Ok(Window {
        before,
        after,
        opened,
        bench_s,
        cpu: cpu_before.zip(cpu_after),
        health,
    })
}

/// Lines acknowledged within the first second after `opened`, the first two, and so on,
/// from every sender's notes: the last value is every line that was acknowledged.
fn by_second(opened: Instant, senders: &[Returned]) -> Vec<u64> {
    let mut each: Vec<u64> = Vec::new();
    for (at, lines) in senders.iter().flat_map(|sender| &sender.acks) {
        let second = at.saturating_duration_since(opened).as_secs() as usize;
        if each.len() <= second {
            each.resize(second + 1, 0);
        }
        each[second] += lines;
    }
    let mut so_far = 0;
    for lines in &mut each {
        so_far += *lines;
        *lines = so_far;
    }
    each
}

/// Ingest alone, on a new server that is never sent a query: the window is between two
/// `/stats` answers, on the server's own clock, with nothing else asked inside it.
pub fn ingest_alone(work: &Work) -> Result<IngestRun, SpeedError> {
    let (server, mut conn) = started(work)?;
    let unit = server.unit();
    let (window, shared, senders) =
        beside_senders(work, server, Rounds::Once, |server, shared, returned| {
            ingest_window(server, &mut conn, shared, returned)
        })?;

    let (before, after) = (&window.before, &window.after);
    let lines = after.lines_ingested.saturating_sub(before.lines_ingested);
    let window_s = after.uptime_seconds - before.uptime_seconds;
    if window_s <= 0.0 {
        return Err(SpeedError::Surface {
            surface: "GET /stats",
            detail: format!(
                "uptime_seconds went {} -> {} across the ingest window: the server's clock \
                 did not move, and a rate cannot be read off it",
                before.uptime_seconds, after.uptime_seconds
            ),
        });
    }

    let sum = |of: fn(&Counts) -> &AtomicU64| -> u64 {
        shared
            .counts
            .iter()
            .map(|counts| of(counts).load(Relaxed))
            .sum()
    };
    let acked_lines = sum(|counts| &counts.acked_lines);
    let cpu_s = window
        .cpu
        .map(|(before, after)| (after.total_s() - before.total_s()).max(0.0));
    Ok(IngestRun {
        server: unit,
        lines_ingested_before: before.lines_ingested,
        lines_ingested_after: after.lines_ingested,
        lines,
        uptime_before_s: before.uptime_seconds,
        uptime_after_s: after.uptime_seconds,
        window_s,
        lines_per_s: lines as f64 / window_s,
        window_s_bench: window.bench_s,
        stream_lines: work.stream.lines,
        bodies: work.stream.bodies.len(),
        acked_bodies: sum(|counts| &counts.acked_bodies),
        acked_lines,
        counters_agree: acked_lines == lines,
        refused: Refused {
            s503: sum(|counts| &counts.refused_503),
            s429: sum(|counts| &counts.refused_429),
            before_body_end: sum(|counts| &counts.refused_before_body_end),
        },
        no_answer: sum(|counts| &counts.no_answer),
        retry_after_max_s: shared
            .counts
            .iter()
            .filter_map(Counts::retry_after_max_s)
            .max(),
        reconnects: sum(|counts| &counts.reconnects),
        short_bodies: sum(|counts| &counts.short_bodies),
        short_lines: sum(|counts| &counts.short_lines),
        unknown_outcome: sum(|counts| &counts.unknown_outcome),
        senders: shared
            .counts
            .iter()
            .zip(&senders)
            .map(|(counts, sender)| SenderFacts {
                bodies: counts.acked_bodies.load(Relaxed),
                lines: counts.acked_lines.load(Relaxed),
                refused: counts.refused(),
                no_answer: counts.no_answer.load(Relaxed),
                reconnects: counts.reconnects.load(Relaxed),
                seconds: sender
                    .ended
                    .saturating_duration_since(window.opened)
                    .as_secs_f64(),
            })
            .collect(),
        acked_lines_by_second: by_second(window.opened, &senders),
        cpu_s,
        cores_busy: cpu_s.map(|cpu| cpu / window_s),
        resident_bytes_at_end: window.cpu.map(|(_, after)| after.resident_bytes),
        pageins: window.cpu.map(|(_, after)| after.pageins),
        at_end: AtEnd {
            buffered_rows: after.buffered_rows,
            buffered_bytes: after.buffered_bytes,
            max_buffer_bytes: after.max_buffer_bytes,
            forced_closes: after.forced_closes,
            reached_buffer_limit: after.forced_closes.map(|closes| closes > 0),
            requests_shed: after.requests_shed,
            lines_rejected: after.lines_rejected,
            write_failures: after.write_failures,
            wal_behind: after
                .wal_last_seq
                .zip(after.wal_applied)
                .map(|(last, applied)| last.saturating_sub(applied)),
            templates_before: before.templates,
            templates_after: after.templates,
            rows: after.rows,
        },
        health_at_end: window.health,
        stats_at_end: window.after.body,
    })
}

// ── One lap under ingest ───────────────────────────────────────────────────────────

/// Wait until `place` live lines are acknowledged, and return how many were when the
/// wait ended. A place not reached in `STALL` is an error: the senders are not getting through.
fn reach(
    name: &'static str,
    place: u64,
    server: &Server,
    shared: &Shared,
) -> Result<u64, SpeedError> {
    let began = Instant::now();
    loop {
        let acked = shared.acked_lines.load(Relaxed);
        if acked >= place {
            return Ok(acked);
        }
        if shared.failed.load(Relaxed) {
            return Err(a_sender_failed());
        }
        if began.elapsed() >= STALL {
            return Err(SpeedError::Sender {
                sender: SENDERS,
                detail: format!(
                    "{name} is asked once {} live lines are acknowledged, and after {} s \
                     of waiting for them only {} were; {} ({})",
                    commas(place),
                    STALL.as_secs(),
                    commas(acked),
                    shared.said(),
                    whose(server)
                ),
            });
        }
        thread::sleep(PLACE_POLL);
    }
}

/// What the main thread took of a lap.
struct Taken {
    samples: Four<Sample>,
    before: Stats,
    after: Stats,
}

/// The main thread's part of a lap, from the senders at the gate to the last `/stats`.
fn lap_main(
    work: &Work,
    server: &Server,
    conn: &mut Conn,
    shared: &Shared,
) -> Result<Taken, SpeedError> {
    let (before, _) = open_the_gate(server, conn, shared)?;

    let places = place_lines(work.lines);
    let [_, _, n3, _] = Four::<()>::NAMES;
    let mut samples = Vec::with_capacity(places.len());
    for (((name, place), request), expect) in Four::<()>::NAMES
        .into_iter()
        .zip(places)
        .zip(work.requests.four().each())
        .zip(work.expect.four().each())
    {
        let at_lines = reach(name, place, server, shared)?;
        let asked = ask(conn, server, name, request)?;
        let end_lines = shared.acked_lines.load(Relaxed);
        under_ingest(name, expect, &asked.answer)?;
        if name == n3 {
            within_backstop(name, &asked.answer)?;
        }
        samples.push(Sample {
            client_ms: as_ms(asked.took),
            server_ms: asked.answer.duration_ms,
            at_lines,
            end_lines,
            count: asked.answer.count,
            rows_scanned: asked.answer.rows_scanned,
            refused: asked.refused,
        });
    }
    let after = server.stats(conn)?;
    let samples: [Sample; 4] = samples.try_into().map_err(|_| SpeedError::Surface {
        surface: "POST /query",
        detail: "the four queries of a lap did not give four samples".to_string(),
    })?;
    Ok(Taken {
        samples: Four::from_array(samples),
        before,
        after,
    })
}

/// One lap on a new server: the senders released, then each query asked once, timed, at
/// its place in the live stream, which fixes what it reads; a query that starts late is counted.
pub fn lap(work: &Work) -> Result<Lap, SpeedError> {
    let (server, mut conn) = started(work)?;
    let start_s = server.start_s;

    // The first answers of a process cost more than the ones after; these three are
    // asked once so that no sample is one. S4 is not: it would cost too much a run.
    let [_, _, n3, _] = Four::<()>::NAMES;
    for ((name, request), expect) in Four::<()>::NAMES
        .into_iter()
        .zip(work.requests.four().each())
        .zip(work.expect.four().each())
        .take(3)
    {
        let asked = ask(&mut conn, &server, name, request)?;
        same(name, expect, &asked.answer)?;
        if name == n3 {
            within_backstop(name, &asked.answer)?;
        }
    }

    let (taken, shared, _) =
        beside_senders(work, server, Rounds::UntilStopped, |server, shared, _| {
            lap_main(work, server, &mut conn, shared)
        })?;
    let sum = |of: fn(&Counts) -> u64| -> u64 { shared.counts.iter().map(of).sum() };
    Ok(Lap {
        samples: taken.samples,
        start_s,
        lines: taken
            .after
            .lines_ingested
            .saturating_sub(taken.before.lines_ingested),
        seconds: taken.after.uptime_seconds - taken.before.uptime_seconds,
        forced_closes: taken.after.forced_closes,
        buffered_bytes: taken.after.buffered_bytes,
        requests_shed: taken.after.requests_shed,
        write_failures: taken.after.write_failures,
        degraded: taken.after.degraded,
        refused_bodies: sum(Counts::refused),
        wraps: sum(|counts| counts.wraps.load(Relaxed)),
    })
}

/// A run's laps as a result keeps them: each query's samples in lap order with their
/// percentiles, a sample late when asked more than one round of bodies past its place.
pub fn loaded(laps: &[Lap], work: &Work) -> LoadedRun {
    let round = (SENDERS * work.body_lines) as u64;
    let places = place_lines(work.lines);
    let block = |at: usize| {
        let samples: Vec<&Sample> = laps.iter().map(|lap| lap.samples.each()[at]).collect();
        let client_ms: Vec<f64> = samples.iter().map(|sample| sample.client_ms).collect();
        let server_ms: Vec<Option<f64>> = samples.iter().map(|sample| sample.server_ms).collect();
        let of = |p: f64| percentile(&client_ms, p).unwrap_or_default();
        let server = every(&server_ms);
        let expected = work.expect.four().each()[at].count;
        let count_over_loaded: Vec<u64> = samples
            .iter()
            .map(|sample| sample.count.saturating_sub(expected))
            .collect();
        LoadedBlock {
            p50_ms: of(0.50),
            p99_ms: of(0.99),
            min_ms: of(0.0),
            max_ms: of(1.0),
            server_p50_ms: server.as_deref().and_then(|ms| percentile(ms, 0.50)),
            server_p99_ms: server.as_deref().and_then(|ms| percentile(ms, 0.99)),
            client_ms,
            server_ms,
            place_lines: places[at],
            at_lines: samples.iter().map(|sample| sample.at_lines).collect(),
            acked_while_running: samples
                .iter()
                .map(|sample| sample.end_lines.saturating_sub(sample.at_lines))
                .collect(),
            late: samples
                .iter()
                .filter(|sample| sample.at_lines.saturating_sub(places[at]) > round)
                .count() as u64,
            count: samples.iter().map(|sample| sample.count).collect(),
            count_over_loaded_median: median(&count_over_loaded).unwrap_or_default(),
            count_over_loaded,
            rows_scanned: samples.iter().map(|sample| sample.rows_scanned).collect(),
            refused: samples.iter().map(|sample| sample.refused).sum(),
        }
    };
    let rates: Vec<f64> = laps
        .iter()
        .filter(|lap| lap.seconds > 0.0)
        .map(|lap| lap.lines as f64 / lap.seconds)
        .collect();
    let forced_closes: Vec<Option<u64>> = laps.iter().map(|lap| lap.forced_closes).collect();
    LoadedRun {
        laps: laps.len(),
        queries: Four::from_array([0, 1, 2, 3].map(block)),
        lap: Laps {
            lines: laps.iter().map(|lap| lap.lines).collect(),
            seconds: laps.iter().map(|lap| lap.seconds).collect(),
            refused_bodies: laps.iter().map(|lap| lap.refused_bodies).collect(),
            wraps: laps.iter().map(|lap| lap.wraps).collect(),
            start_s: laps.iter().map(|lap| lap.start_s).collect(),
            buffered_bytes: laps.iter().map(|lap| lap.buffered_bytes).collect(),
            requests_shed: laps.iter().map(|lap| lap.requests_shed).collect(),
            forced_closes: forced_closes.clone(),
            write_failures: laps.iter().map(|lap| lap.write_failures).collect(),
            degraded: laps.iter().map(|lap| lap.degraded).collect(),
        },
        lines_per_s_median: median(&rates),
        refused_bodies: laps.iter().map(|lap| lap.refused_bodies).sum(),
        wrapped_laps: laps.iter().filter(|lap| lap.wraps > 0).count() as u64,
        // The server is new in every lap, so a count above 0 is the lap's own.
        laps_at_buffer_limit: every(&forced_closes)
            .map(|closes| closes.iter().filter(|closes| **closes > 0).count() as u64),
    }
}

#[cfg(test)]
#[path = "phases_tests.rs"]
mod tests;
