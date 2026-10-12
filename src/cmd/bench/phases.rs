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

use kibbr_core::fmt::commas;

use super::corpus::{self, Corpus, LiveStream};
use super::host::{self, Cpu};
use super::report::{
    AtEnd, Beside, BesideKeyed, BesideRate, Expect, Four, Health, IdleBlock, IdleRun, IngestRun,
    Laps, LoadedBlock, LoadedRun, Refused, ScanBlock, ScanRun, SenderFacts, Seven,
};
use super::server::{Server, Stats};
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
    /// This binary: the server measured is the binary measuring.
    pub exe: &'a Path,
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
mod tests {
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
            &wire::query_request(crate::cmd::bench::S2),
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
}
