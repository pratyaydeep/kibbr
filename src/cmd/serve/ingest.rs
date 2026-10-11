//! The ingest path: a body decoded, parsed, logged and placed.

use super::*;

// ── The ingest path ────────────────────────────────────────────────────────────────

/// Why a body could not be turned into NDJSON, before any line of it was read.
///
/// Distinct from `DetectorError`, which is one bad line among good ones that still land.
/// These three are the whole body. Answering a corrupt gzip with 500 per-line rejections
/// would describe the symptom - unreadable bytes - rather than the fault, which is that
/// nothing in the body was ever going to parse.
pub(super) enum BodyError {
    /// A `Content-Encoding` this server cannot decode.
    Unsupported(String),
    /// `gzip`, but not gzip: wrong magic, a truncated stream, a failed CRC.
    Corrupt(String),
    /// Valid gzip that inflates past the cap. Carries the compressed size because the
    /// *ratio* identifies a bomb: 10 MB from 1 MB is a log batch, 10 GB from 1 MB is not.
    TooLarge { cap: usize, compressed: usize },
}

impl BodyError {
    /// The status, and the reason it is that status and not another.
    pub(super) fn status(&self) -> StatusCode {
        match self {
            // 415 not 400: the bytes may be well formed. What is unacceptable is the
            // encoding they are in, which is what 415 names.
            Self::Unsupported(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            // Said gzip, sent something else. Re-sending the same bytes fails the same
            // way, so a 4xx - which tells a shipper not to retry it forever.
            Self::Corrupt(_) => StatusCode::BAD_REQUEST,
            // What the wire cap answers with, because it is the same statement: too big.
            // That it was small until inflated belongs in the message, not the code.
            Self::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    pub(super) fn message(&self) -> String {
        match self {
            Self::Unsupported(encoding) => format!(
                "unsupported Content-Encoding `{encoding}`; this endpoint accepts gzip or \
                 an unencoded body"
            ),
            Self::Corrupt(why) => {
                format!("Content-Encoding is gzip but the body could not be decoded: {why}")
            }
            Self::TooLarge { cap, compressed } => format!(
                "gzip body of {compressed} bytes expands past the {cap} byte decompressed \
                 limit; it was refused while inflating, not after"
            ),
        }
    }
}

/// The body as NDJSON, decompressing first when the client said it was gzipped.
///
/// **Borrowed when there is nothing to do.** An unencoded body is the common case, and
/// copying 10 MiB to hand it to the same reader buys nothing.
///
/// **The cap is enforced while inflating, not after.** `Read::take` is load bearing:
/// without it the check that a body was too big happens after the allocation that killed
/// the process. With it the reader stops at `cap + 1` bytes however the body was built -
/// the extra byte being what separates "filled the cap" from "would have gone further".
///
/// `MultiGzDecoder`, not `GzDecoder`: concatenated members are a valid stream and a
/// shipper that batches by appending produces them. `GzDecoder` stops silently after the
/// first, dropping the rest of a batch and reporting success.
pub(super) fn decode<'a>(
    body: &'a [u8],
    encoding: Option<&str>,
    cap: usize,
) -> Result<Cow<'a, [u8]>, BodyError> {
    let encoding = encoding.unwrap_or("").trim();
    if encoding.is_empty() || encoding.eq_ignore_ascii_case("identity") {
        return Ok(Cow::Borrowed(body));
    }
    if !encoding.eq_ignore_ascii_case("gzip") && !encoding.eq_ignore_ascii_case("x-gzip") {
        return Err(BodyError::Unsupported(encoding.to_string()));
    }

    let mut out = Vec::new();
    MultiGzDecoder::new(Cursor::new(body))
        .take(cap as u64 + 1)
        .read_to_end(&mut out)
        // Every decoder failure arrives here as an `io::Error` - and arrives, rather than
        // unwinding. A body is attacker-supplied input on a path a shipper hits thousands
        // of times an hour, and a panic in it takes the server, not the body.
        .map_err(|e| BodyError::Corrupt(e.to_string()))?;

    if out.len() > cap {
        return Err(BodyError::TooLarge {
            cap,
            compressed: body.len(),
        });
    }
    Ok(Cow::Owned(out))
}

/// One malformed line: `{"line":400,"msg":"..."}`.
///
/// The line number and the message are two fields, which is why `DetectorError` keeps
/// them as two things - see its own note.
pub(super) struct LineError {
    pub(super) line: usize,
    pub(super) msg: String,
}

/// What one `/ingest` body did.
pub(super) struct IngestReport {
    ingested: u64,
    pub(super) errors: Vec<LineError>,
    /// Rejections past `MAX_REPORTED_ERRORS`, counted but not described.
    omitted: u64,
    /// Of the lines rejected - described or not - the ones that parsed and were refused
    /// because the catalog could not write the record they needed. Counted where the
    /// append fails, by the error's type, and not read back out of a message. Not in the
    /// JSON: it decides the status (`nothing_taken`).
    unwritten: u64,
}

impl IngestReport {
    pub(super) fn to_json(&self) -> Value {
        let errors: Vec<Value> = self
            .errors
            .iter()
            .map(|e| json!({ "line": e.line, "msg": e.msg }))
            .collect();
        let mut report = json!({
            // "every line went in", not "the request was handled". A client that checks
            // one field should be told that 3 of its 500 lines are missing; the count and
            // the array are there for the client that wants to know which.
            "ok": self.errors.is_empty(),
            "ingested": self.ingested,
            "errors": errors,
        });

        // Present only when something was actually dropped, so a clean body is still
        // exactly `{"ok":true,"ingested":N,"errors":[]}` and no existing client sees a
        // field it has never seen. One that is almost always zero teaches clients to
        // ignore it, which is the opposite of what it is for.
        if self.omitted > 0 {
            report["errors_omitted"] = json!(self.omitted);
        }
        report
    }

    /// No line of the body was taken, and every line was refused for a write this server
    /// could not make. Then the answer is not a `200`: the sender got nothing wrong, has
    /// nothing to drop, and should send the body again (`nothing_taken`).
    ///
    /// False for a body with no refusal at all - an empty one, or blank lines - and for
    /// one where any refusal is the line's own: that line is refused the same way every
    /// time it is sent. `omitted` is in the sum because the rule is about every line, and
    /// `errors` stops at `MAX_REPORTED_ERRORS`.
    pub(super) fn nothing_taken(&self) -> bool {
        self.ingested == 0
            && self.unwritten > 0
            && self.unwritten == self.errors.len() as u64 + self.omitted
    }
}

/// The two ways a body fails whole: the client's fault and the server's, which is the
/// difference between a 4xx and a 5xx.
pub(super) enum IngestFailure {
    /// The body could not be decoded: wrong encoding, corrupt gzip, or a bomb.
    Body(BodyError),
    /// A lock poisoned mid-body. Fails every line after it too, so the body is abandoned
    /// rather than answered with 500 copies of one message. A catalog append that fails
    /// is not this: it costs its line, reported in `errors`, and the body goes on - and a
    /// body that lost every line that way is refused from its report, with the errors
    /// still in it, and not from here (`IngestReport::nothing_taken`). Nor is a store
    /// that cannot take a row, which costs nothing and is not an error at all.
    Fatal(IngestError),
}

/// An NDJSON body into the database: parsed, routed, logged, applied.
///
/// A plain function, not an `Ingestor` - see the module header.
///
/// **The order is the durability.** Every line is parsed with nothing held; each is then
/// routed to its template, which makes the template - or the widening the line causes -
/// durable in the catalog first; then the body's lines go to the write-ahead log, whose
/// record names that template; and this returns once the log has flushed them **and** the
/// log's one applier has handed their rows to the store. The answer is sent after that:
/// the flush is what makes a `200` true, and the wait for the applier is what lets a
/// client that ingests and then queries see all of what it sent.
///
/// **Per-line error isolation.** A bad line is recorded against its number and
/// the loop continues, so line 400 of 500 costs line 400 and nothing else. This is not a
/// second copy of that logic: `db::read_lines` is the same loop the CLI reads files with,
/// and the only thing this caller changes is where a rejection goes - to a `Vec` that
/// becomes the response body, instead of to a stderr an HTTP client cannot read.
///
/// **A body the log cannot take is refused, and so is one the catalog could write
/// nothing for.** Not a row the store cannot take: by then the line is durable, its
/// sender is told `Ok`, and the failure is the applier's to log (`Held`). The error this
/// returns cannot carry a store's - see `IngestError` - and is the log's, or the trie's
/// lock poisoned, which fails every line after it too, so the body is abandoned rather
/// than answered with 500 copies of one message. The catalog's refusal is not an error of
/// this function: each line it cost is in the report, the report counts them, and the
/// handler answers `503` when they are all there is (`IngestReport::nothing_taken`).
pub(super) fn ingest(
    server: &Server,
    body: &[u8],
    now: DateTime<Utc>,
    permit: Permit,
) -> Result<IngestReport, IngestError> {
    // `place` is this body's place among those between the door and their answer, kept
    // until this returns, and what says whether its sender is still there; `permit` is
    // for the parse and the routing.
    let Permit {
        parse: permit,
        body: place,
    } = permit;
    // A body no write of the log would hold is refused before it is parsed: it would
    // be refused after, and the parse of that many lines is the cost.
    if let Some(wal::Refused::TooLarge { bytes, limit }) =
        server.wal.as_ref().and_then(|_| wal::too_large(body))
    {
        return Err(IngestError::TooLarge { bytes, limit });
    }

    // ── Half one: split and parse, holding nothing ─────────────────────────────────
    //
    // The lock used to be taken here, before the body had been split into lines, so every
    // body's parse - which is most of the cost of a line - ran one at a time and ingest
    // measured flat at 30,000 lines a second from one sender to eight.
    let mut parse_errors = Vec::new();
    let (parsed, stats) = db::parse_and_prepare(Cursor::new(body), &mut |e: DetectorError| {
        // Past the cap the rejection is counted and dropped. `stats.rejected` is
        // still the true total, so nothing is lost but the text.
        if parse_errors.len() < MAX_REPORTED_ERRORS {
            parse_errors.push(LineError {
                line: e.line,
                msg: e.kind.to_string(),
            });
        }
    })
    // Reading a body already in memory does not fail. What can is a body with no newline
    // in more than a line may hold, which the default body cap rules out and a larger
    // `--max-body-bytes` does not: a message, and a 500.
    .map_err(|e| IngestError::Body(error::chain(&e)))?;

    // ── Half two: route each line to its template, durably ─────────────────────────
    //
    // Before the log, because the log's record carries the template's id: the catalog
    // record that defines the id is flushed by the time `place_in` returns.
    let mut apply_errors = Vec::new();
    let mut rejected = stats.rejected;
    let mut unwritten = 0u64;
    let mut lines: Vec<wal::Line> = Vec::with_capacity(parsed.len());
    for (line, parsed, prepared) in parsed {
        let kind = match db::place_in(&server.trie, &parsed, prepared, now) {
            Ok(Some(placed)) => {
                lines.push(wal::Line {
                    raw: parsed.raw_line,
                    // The time the row is filed under, as the store reads it: what the
                    // log writes down beside the line, for replay to hold it to.
                    time: kibbr_storage::store::effective_ts(&placed.row, &placed.time_column)
                        .unwrap_or(0),
                    placed,
                });
                continue;
            }
            // The template's own lock is poisoned: nothing can say which template the
            // line is, so there is no id to log it under. It used to be counted as
            // ingested and stored nowhere.
            Ok(None) => DetectorErrorKind::NotDurable {
                detail: "the template's lock is poisoned".to_string(),
            },
            // The line parsed and its template could not be made durable. Counted and
            // reported exactly as a line that did not parse: to the client both are
            // "this one is not in the database", and the message says which.
            Err(e @ TrieError::Catalog(_)) => {
                // And counted apart, as a write that failed: a body of nothing but these
                // is sent again, and a record that would not encode is no reason to -
                // it would not encode the next time either.
                if !matches!(e, TrieError::Catalog(CatalogError::Serialize { .. })) {
                    unwritten += 1;
                }
                DetectorErrorKind::NotDurable {
                    detail: error::chain(&e),
                }
            }
            // The trie's lock poisoned, which fails every line after it too.
            Err(e) => return Err(e.into()),
        };
        rejected += 1;
        if apply_errors.len() < MAX_REPORTED_ERRORS {
            apply_errors.push(LineError {
                line,
                msg: kind.to_string(),
            });
        }
    }

    // ── Half three: the log, and then the rows ─────────────────────────────────────
    //
    // With a log: the body's lines durable in one flush, then applied by the log's one
    // applier in the log's order, and back here once both are done. With none - storage
    // is memory - applied here, as it always was: nothing is durable, and the banner
    // says so.
    //
    // **All of the body, or none of it.** A log that cannot take it has none of its
    // lines, so the refusal is of the body: the sender keeps it, and sends it again
    // without storing a line twice.
    let ingested = lines.len() as u64;
    match &server.wal {
        Some(wal) => {
            // A body the log refuses was read and routed, and none of it was taken: its
            // lines are counted as read and as rejected, whichever refusal it was.
            let refused = |refused: wal::Refused| {
                server.lines_read.fetch_add(stats.read, Ordering::Relaxed);
                server
                    .lines_rejected
                    .fetch_add(rejected + ingested, Ordering::Relaxed);
                match refused {
                    wal::Refused::Cannot(why) => IngestError::Log(why),
                    wal::Refused::Busy => IngestError::Busy,
                    wal::Refused::Left => IngestError::Left,
                    wal::Refused::TooLarge { bytes, limit } => {
                        IngestError::TooLarge { bytes, limit }
                    }
                }
            };
            // A log with no room for the body makes it wait for some, for as long as a
            // body may be kept waiting, where it used to be shed at once - after it had
            // been read, inflated, parsed and routed, all of which its sender's retry
            // paid for again a second later.
            let wait = wal::Wait {
                at_most: server.api.ingest_wait,
                left: &place.left,
            };
            let ticket = wal
                .enqueue(lines, db::arrival_nanos(now), wait)
                .map_err(&refused)?;
            // The permit is for the parse and the routing, which are what a core is
            // short of. Held across the flush as well, a flush could never carry more
            // bodies than there are cores - and it is there to carry whatever has queued.
            // What bounds the bodies waiting from here is `ingest_bodies`, at the door.
            //
            // **Kept until the body is in the log's queue, and so across a wait for
            // room.** A body waiting for room is a parsed body held in memory that the
            // log's bound does not count; with its permit kept there are no more of
            // those than there are permits, which is the memory the parse already had.
            // And it is what carries the pressure back: once every permit is held by a
            // body the log has no room for, the door lets nobody in, and every later
            // sender waits there with its body unread.
            drop(permit);
            ticket.wait().map_err(&refused)?;
        }
        None => {
            for line in lines {
                server.held.apply(&server.stores, 0, line.placed);
            }
            drop(permit);
        }
    }

    // **The report is by line number, and it was already.** One pass numbered its
    // rejections in the order it met them, which was line order; two passes meet them in
    // two runs, so the merge has to put them back. Capping each half at
    // `MAX_REPORTED_ERRORS` before the merge changes nothing: an error inside the merged
    // first thousand has fewer than a thousand errors before it overall, so it has fewer
    // than a thousand before it in its own half.
    let mut errors = parse_errors;
    errors.extend(apply_errors);
    errors.sort_by_key(|e| e.line);
    errors.truncate(MAX_REPORTED_ERRORS);

    server.lines_read.fetch_add(stats.read, Ordering::Relaxed);
    server.lines_ingested.fetch_add(ingested, Ordering::Relaxed);
    server.lines_rejected.fetch_add(rejected, Ordering::Relaxed);
    // Past the memory limit the flush runs now, not at its next tick: a burst that filled
    // the buffers in one body should not get half a second more to fill them further.
    if server.stores.over_buffer_limit() {
        server.flush_now.notify_one();
    }

    Ok(IngestReport {
        ingested,
        omitted: rejected - errors.len() as u64,
        errors,
        unwritten,
    })
}
