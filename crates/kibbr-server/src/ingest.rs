//! The ingest path: a body decoded, parsed, logged and placed.

use super::*;

// ── The ingest path ────────────────────────────────────────────────────────────────

/// Why a body could not be turned into NDJSON, before any line of it was read: a fault
/// of the whole body, where a `DetectorError` is one bad line among good ones.
pub(super) enum BodyError {
    /// A `Content-Encoding` this server cannot decode.
    Unsupported(String),
    /// `gzip`, but not gzip: wrong magic, a truncated stream, a failed CRC.
    Corrupt(String),
    /// Valid gzip that inflates past the cap, with the compressed size because the ratio
    /// is what identifies a bomb.
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

/// The body as NDJSON, gunzipped when the client said so, the cap enforced while
/// inflating; `MultiGzDecoder` because a shipper's concatenated members are one stream.
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
        // Every decoder failure arrives here as an `io::Error`, not a panic: a body is
        // attacker-supplied, and a panic in it takes the server, not the body.
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
    /// Of the lines rejected, those that parsed and were refused because the catalog could
    /// not write their record. Not in the JSON: it decides the status (`nothing_taken`).
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
            // "every line went in", not "the request was handled": a client that checks one
            // field should be told that some of its lines are missing.
            "ok": self.errors.is_empty(),
            "ingested": self.ingested,
            "errors": errors,
        });

        // Present only when something was dropped: a field that is almost always zero
        // teaches clients to ignore it.
        if self.omitted > 0 {
            report["errors_omitted"] = json!(self.omitted);
        }
        report
    }

    /// Whether no line was taken and every refusal was a write this server could not make,
    /// so the answer is not a `200` and the sender should send the body again.
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
    /// A lock poisoned mid-body, which fails every line after it too, so the body is
    /// abandoned; a failed catalog append costs only its line (`IngestReport::nothing_taken`).
    Fatal(IngestError),
}

/// An NDJSON body into the database, in the order that is the durability: parsed, routed
/// to a template made durable, logged, and answered once the rows are applied.
pub(super) fn ingest(
    server: &Server,
    body: &[u8],
    now: DateTime<Utc>,
    permit: Permit,
) -> Result<IngestReport, IngestError> {
    // `place` is this body's place between the door and its answer, and what says whether
    // its sender is still there; `permit` is for the parse and the routing.
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
    // Reading a body in memory fails only on a line longer than a line may hold, which a
    // larger `--max-body-bytes` allows: a message, and a 500.
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
            // line is, so there is no id to log it under.
            Ok(None) => DetectorErrorKind::NotDurable {
                detail: "the template's lock is poisoned".to_string(),
            },
            // The line parsed and its template could not be made durable: reported as a
            // line that did not parse, and the message says which.
            Err(e @ TrieError::Catalog(_)) => {
                // Counted apart, as a write that failed and is worth sending again; a record
                // that would not encode would not encode the next time either.
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
    // With a log, the lines are durable in one flush and applied in the log's order; with
    // none, applied here. All of the body or none of it, so a refused sender can resend.
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
            // A log with no room makes the body wait, for as long as a body may be kept
            // waiting: shed at once, its sender's retry pays for the parse again.
            let wait = wal::Wait {
                at_most: server.api.ingest_wait,
                left: &place.left,
            };
            let ticket = wal
                .enqueue(lines, db::arrival_nanos(now), wait)
                .map_err(&refused)?;
            // The permit is for the parse and the routing, so it is not held across the flush;
            // kept across a wait for room, it bounds the parsed bodies held in memory.
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

    // Two passes meet their rejections in two runs, so the merge puts them back in line
    // order; capping each half at `MAX_REPORTED_ERRORS` first loses nothing.
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
