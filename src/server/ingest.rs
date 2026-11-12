// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

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

/// What one `/ingest` body did.
pub(super) struct IngestReport {
    ingested: u64,
    pub(super) errors: Vec<engine::LineError>,
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

/// An NDJSON body into the database (`Database::ingest`), and the door's counts of what it
/// did: the parse permit goes back once the body is in the log's queue.
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
    let mut permit = Some(permit);
    let done = server.db.ingest(
        body,
        now,
        server.api.ingest_wait,
        &place.left,
        // The permit is for the parse and the routing, so it is not held across the flush;
        // kept across a wait for room, it bounds the parsed bodies held in memory.
        || drop(permit.take()),
    );
    let done = match done {
        Ok(done) => done,
        // A body the log refuses was read and routed, and none of it was taken: its lines
        // are counted as read and as rejected, whichever refusal it was.
        Err(refused) => {
            server.lines_read.fetch_add(refused.read, Ordering::Relaxed);
            server
                .lines_rejected
                .fetch_add(refused.rejected, Ordering::Relaxed);
            return Err(refused.error);
        }
    };

    server.lines_read.fetch_add(done.read, Ordering::Relaxed);
    server.lines_ingested.fetch_add(done.ingested, Ordering::Relaxed);
    server.lines_rejected.fetch_add(done.rejected, Ordering::Relaxed);
    // Past the memory limit the flush runs now, not at its next tick: a burst that filled
    // the buffers in one body should not get half a second more to fill them further.
    if done.over_buffer_limit {
        server.flush_now.notify_one();
    }

    Ok(IngestReport {
        ingested: done.ingested,
        omitted: done.omitted,
        errors: done.errors,
        unwritten: done.unwritten,
    })
}
