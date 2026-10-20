// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The handlers: the door, `/ingest`, `/query` and `/load`.

use super::*;

// ── Handlers ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryRequest {
    q: String,
}

/// `{"ok":true,"status":"ok",...}`, or `degraded` while something here cannot write; `ok`
/// stays `true` because this is the liveness check, and a restart does not empty a disk.
/// `version` and `source` are on every answer: where this server's source is (AGPL §13).
pub(super) async fn health(State(server): State<Arc<Server>>) -> Json<Value> {
    let mut answer = match server.degraded() {
        Some(detail) => json!({ "ok": true, "status": "degraded", "detail": detail }),
        None => json!({ "ok": true, "status": "ok" }),
    };
    answer["version"] = json!(VERSION);
    answer["source"] = json!(SOURCE);
    Json(answer)
}

impl Server {
    /// What is wrong with this server's writing, if anything is: the log first, because
    /// while it cannot write `/ingest` refuses every body.
    pub(super) fn degraded(&self) -> Option<String> {
        if let Some(why) = self.wal.as_ref().and_then(Wal::refusal) {
            return Some(format!(
                "the write-ahead log cannot write, and /ingest answers 503 until it can: {why}"
            ));
        }
        if self.write_health.is_degraded() {
            return Some(
                self.write_health
                    .last_error()
                    .unwrap_or_else(|| "the last flush could not write".to_string()),
            );
        }
        match self.held.rows() {
            0 => None,
            rows => Some(format!(
                "{} line(s) are in the write-ahead log and not in the store - the store \
                 refused their rows, or the start could not read them back. The log keeps \
                 them, and the next start tries again",
                commas(rows)
            )),
        }
    }
}

/// The refusal a shipper gets while the write-ahead log cannot write: 503, a
/// `Retry-After` and the write error, since a `200` means the line is in the log.
fn cannot_log(detail: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the write-ahead log cannot write, so nothing of this body was taken - a \
                 200 means the line is durable. Keep the body and send it again: {detail}"
            ),
            "retry_after": RETRY_AFTER_SECONDS,
        })),
    )
        .into_response()
}

/// The answer to a body of which nothing was taken because nothing could be written for
/// it: 503 around the report, since a `200` tells a shipper to drop well-formed lines.
fn nothing_taken(report: &IngestReport) -> Response {
    let first = match report.errors.first() {
        Some(error) => format!("line {}: {}", error.line, error.msg),
        None => "no line's error was kept".to_string(),
    };
    let mut body = report.to_json();
    body["error"] = json!(format!(
        "the catalog could not write the record each line of this body needed, so nothing \
         of it was taken. Keep the body and send it again: {first}"
    ));
    body["retry_after"] = json!(RETRY_AFTER_SECONDS);
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(body),
    )
        .into_response()
}

/// NDJSON in, `{"ok":true,"ingested":N,"errors":[]}` out, optionally gzipped; single
/// tenant, deliberately, as adding an identifier later breaks every shipper's config.
pub(super) async fn post_ingest(
    State(server): State<Arc<Server>>,
    axum::Extension(permit): axum::Extension<Permit>,
    uri: axum::http::Uri,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    // `?received_at=<RFC 3339>`: the arrival time, for a replay or a differential that needs
    // two servers to stamp one body identically. Malformed, the body is refused, not guessed.
    let now = match received_at_of(uri.query()) {
        Ok(Some(at)) => at,
        Ok(None) => Utc::now(),
        Err(text) => {
            return fail(
                StatusCode::BAD_REQUEST,
                &format!("received_at=`{text}` is not an RFC 3339 instant"),
            )
        }
    };
    let body = match body {
        Ok(body) => body,
        // Over the cap is the answer a shipper meets most often, and axum's wording names
        // neither the limit nor the size to split to. A read that died keeps axum's text.
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return fail(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!(
                    "body is larger than the {} byte limit, measured before any \
                     decompression; send a smaller batch",
                    server.api.max_body_bytes
                ),
            )
        }
        Err(rejection) => return fail(rejection.status(), &rejection.body_text()),
    };
    // Non-UTF-8 header bytes reach `decode` as "no encoding", so the body is rejected line
    // by line rather than the request refused on a guess about what the sender meant.
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    let held = Arc::clone(&server);
    // What says the sender has gone, to a body waiting for room in the log: set if this
    // future is dropped before the task below has returned.
    let place = Arc::clone(&permit.body);
    let mut leaving = Leaving {
        server: &server,
        place: &place,
        armed: true,
    };
    // The parse, the trie walk and the wait for the log's flush do not belong on a runtime
    // thread that also answers `/health`; nor does inflating, a gzip bomb being seconds of CPU.
    let done = tokio::task::spawn_blocking(move || {
        // Held from before the body is inflated until its answer is ready: while any body is
        // in its task a query scans on its own thread, so the scan takes no core from the parse.
        let _body = held.fan.as_deref().map(Fan::body);
        let decoded = decode(&body, encoding.as_deref(), held.api.max_decompressed_bytes)
            .map_err(IngestFailure::Body)?;
        ingest(&held, &decoded, now, permit).map_err(IngestFailure::Fatal)
    })
    .await;
    // The task has returned, and its answer is this future's to give: nobody has left.
    leaving.armed = false;

    match done {
        // The one report that is not a 200: no line was taken, and every one was refused for
        // a write the catalog could not make, so the sender keeps the body and comes back.
        Ok(Ok(report)) if report.nothing_taken() => nothing_taken(&report),
        // 200 even with errors in it: part of the body was stored, and a 4xx would tell a
        // shipper to retry the whole batch and duplicate the lines that landed.
        Ok(Ok(report)) => (StatusCode::OK, Json(report.to_json())).into_response(),
        Ok(Err(IngestFailure::Body(e))) => fail(e.status(), &e.message()),
        // The log could not take the body, which is not the client's fault and not a
        // thing a 200 can say: the door's own refusal, for a body that was already past it.
        Ok(Err(IngestFailure::Fatal(IngestError::Log(why)))) => cannot_log(&why),
        // And a shed: the log is behind what arrives, and the body waits at its sender.
        Ok(Err(IngestFailure::Fatal(IngestError::Busy))) => {
            server.door.shed.log.fetch_add(1, Ordering::Relaxed);
            log_busy()
        }
        // The body was let go because its sender had gone, so nobody reads this. Not counted
        // as a shed, which is a sender told to come back.
        Ok(Err(IngestFailure::Fatal(IngestError::Left))) => log_busy(),
        // The sender's to fix, as a body past the wire cap is: no retry of it will fit.
        Ok(Err(IngestFailure::Fatal(e @ IngestError::TooLarge { .. }))) => {
            fail(StatusCode::PAYLOAD_TOO_LARGE, &e.to_string())
        }
        Ok(Err(IngestFailure::Fatal(e))) => {
            fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e))
        }
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("ingest task failed: {join}"),
        ),
    }
}

/// Tells the log when a handler's future is dropped before its task has returned: the
/// sender hung up, and a body waiting for room would hold its permit for nobody.
struct Leaving<'a> {
    server: &'a Arc<Server>,
    place: &'a Arc<Place>,
    /// Cleared once the task has returned: there is then nothing to tell.
    armed: bool,
}

impl Drop for Leaving<'_> {
    fn drop(&mut self) {
        if !self.armed || self.server.wal.is_none() {
            return;
        }
        self.place.left.store(true, Ordering::Release);
        let server = Arc::clone(self.server);
        let wake = move || {
            if let Some(wal) = &server.wal {
                wal.wake_room();
            }
        };
        // The wake-up takes the queue's lock, which the log's writer can keep for a while:
        // not a runtime worker's to wait on.
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => drop(runtime.spawn_blocking(wake)),
            Err(_) => wake(),
        }
    }
}

/// The `received_at` query parameter, or `None` when there is not one. `Err` carries the
/// text that would not parse.
fn received_at_of(query: Option<&str>) -> Result<Option<DateTime<Utc>>, String> {
    let Some(query) = query else {
        return Ok(None);
    };
    for pair in query.split('&') {
        let Some(("received_at", value)) = pair.split_once('=') else {
            continue;
        };
        let text = percent_decode(value);
        return DateTime::parse_from_rfc3339(&text)
            .map(|at| Some(at.with_timezone(&Utc)))
            .map_err(|_| text);
    }
    Ok(None)
}

/// `%XX` escapes and `+` resolved, which is all an RFC 3339 instant in a query string
/// can carry. Anything else is left as it was.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Admission control for `/ingest`: a layer, so a body is refused or kept waiting
/// unread, and 503 not 429 because the server is saturated, not the client over a quota.
pub(super) async fn admit(State(server): State<Arc<Server>>, mut request: Request, next: Next) -> Response {
    // First, and before the permit: a server that cannot log has no use for the slot, and
    // "at capacity" would read as "send fewer". The log, and not the store.
    if let Some(detail) = server.wal.as_ref().and_then(Wal::refusal) {
        return cannot_log(&detail);
    }
    // Nearly every body finds both permits free, and is let in without a clock read or a
    // timer made for it. Only one that does not goes on to where the waits are.
    let permit = match server.admit_now() {
        Some(permit) => permit,
        None => match server.admit_waiting().await {
            Ok(permit) => permit,
            Err(refusal) => return *refusal,
        },
    };
    // Handed to the handler, which hands them to its blocking task: held only here they
    // would go back the moment a client hung up, while the task they cover ran on.
    request.extensions_mut().insert(permit);
    next.run(request).await
}

impl Server {
    /// Both permits, when nothing has to be waited for; this does not jump the line, since
    /// a permit given back goes first to whoever has waited longest.
    pub(super) fn admit_now(&self) -> Option<Permit> {
        if self.stores.buffers_full() {
            return None;
        }
        let parse = Arc::clone(&self.ingest_permits)
            .try_acquire_owned()
            .ok()?;
        // A permit got without a place goes straight back, as this returns.
        let place = Arc::clone(&self.ingest_bodies)
            .try_acquire_owned()
            .ok()?;
        Some(Permit::of(parse, place))
    }

    /// The most bodies that may wait at the door at once: `None` at the default wait, which
    /// bounds itself, and the places once an operator has raised it.
    pub(super) fn line_max(&self) -> Option<usize> {
        let default = Duration::from_millis(crate::settings::DEFAULT_INGEST_WAIT_MS as u64);
        (self.api.ingest_wait > default).then(|| ingest_bodies(&self.api))
    }

    /// Both permits, with every wait before one deadline and the body unread throughout;
    /// `Err` is the refusal, already counted under its cause.
    pub(super) async fn admit_waiting(&self) -> Result<Permit, Box<Response>> {
        if let Some(max) = self.line_max() {
            if self.door.waiting.load(Ordering::Relaxed) >= max as u64 {
                self.door.shed.line.fetch_add(1, Ordering::Relaxed);
                return Err(Box::new(line_full(max)));
            }
        }
        let waiting = Waiting::begin(&self.door);
        let until = waiting.since + self.api.ingest_wait;
        // Whether the wait has run out: from the start, with no wait to give, since a timer
        // set for a moment already here still takes a turn of the runtime's clock.
        let mut spent = self.api.ingest_wait.is_zero();
        let turned_away = || {
            self.door.shed.stopping.fetch_add(1, Ordering::Relaxed);
            Box::new(stopping())
        };
        // The flush is rung once for this body: one that leaves the buffers full has done
        // what a flush can, and the tick will try again.
        let mut rung = false;
        let permit = loop {
            // Past twice their limit the flush is falling behind, and a body taken now is
            // memory the process may not have: it waits for the flush that is on its way.
            loop {
                // Listening before the look, so a flush that ends between the look and
                // the wait is not missed.
                let flushed = self.flushed.notified();
                tokio::pin!(flushed);
                flushed.as_mut().enable();
                if !self.stores.buffers_full() {
                    break;
                }
                if let Some(detail) = self.wal.as_ref().and_then(Wal::refusal) {
                    return Err(Box::new(cannot_log(&detail)));
                }
                // A store that cannot write: nothing leaves the buffers, so a wait would
                // only be the same refusal later. At once, with the store's error in it.
                if self.write_health.is_degraded() {
                    self.door.shed.buffers.fetch_add(1, Ordering::Relaxed);
                    return Err(Box::new(buffers_full(
                        self.stores.buffer_bytes(),
                        self.write_health.last_error(),
                    )));
                }
                if !rung {
                    self.flush_now.notify_one();
                    rung = true;
                }
                // Out of time, and the looks above were made after it ran out: shed, with
                // what the buffers hold at this moment.
                if spent {
                    if self.stopping.is_cancelled() {
                        return Err(turned_away());
                    }
                    self.door.shed.buffers.fetch_add(1, Ordering::Relaxed);
                    return Err(Box::new(buffers_full(self.stores.buffer_bytes(), None)));
                }
                tokio::select! {
                    biased;
                    () = self.stopping.cancelled() => return Err(turned_away()),
                    () = &mut flushed => {}
                    // Not shed from here: a flush brings the buffers down before it rings
                    // `flushed`, so round once more, for the looks a body makes on arriving.
                    () = tokio::time::sleep_until(until) => spent = true,
                }
            }
            // The permit for the parse and then the place, so that running out of places
            // means the bodies inside are waiting on the log. One deadline for both.
            let parse = tokio::select! {
                biased;
                () = self.stopping.cancelled() => return Err(turned_away()),
                got = permit_by(&self.ingest_permits, until, spent) => got,
            };
            // `None` is the deadline: the semaphores are never closed.
            let Some(parse) = parse else {
                return Err(Box::new(self.out_of_time(Wanted::Permit)));
            };
            let place = tokio::select! {
                biased;
                () = self.stopping.cancelled() => return Err(turned_away()),
                got = permit_by(&self.ingest_bodies, until, spent) => got,
            };
            let Some(place) = place else {
                return Err(Box::new(self.out_of_time(Wanted::Place)));
            };
            // The buffers again, because they may have filled while a permit was waited for.
            // Full, the permits go back - a body waiting for a flush holds nothing.
            if !self.stores.buffers_full() {
                break Permit::of(parse, place);
            }
        };
        // The log again, because something was waited for.
        if let Some(detail) = self.wal.as_ref().and_then(Wal::refusal) {
            return Err(Box::new(cannot_log(&detail)));
        }
        Ok(permit)
    }

    /// The refusal of a body whose wait at the door ran out, counted under what there was
    /// none of; a failed or full log is the reason given, whatever was waited for.
    fn out_of_time(&self, wanted: Wanted) -> Response {
        if let Some(detail) = self.wal.as_ref().and_then(Wal::refusal) {
            return cannot_log(&detail);
        }
        let shed = &self.door.shed;
        if self.wal.as_ref().is_some_and(|wal| wal.waiting_for_room() > 0) {
            shed.log.fetch_add(1, Ordering::Relaxed);
            return log_busy();
        }
        match wanted {
            Wanted::Permit => {
                shed.permit.fetch_add(1, Ordering::Relaxed);
                busy(self.api.ingest_concurrency)
            }
            Wanted::Place => {
                shed.place.fetch_add(1, Ordering::Relaxed);
                log_busy()
            }
        }
    }
}

/// One permit of `from` for a body at the door, waited for until `until`; with the wait
/// `spent` it is taken only if free now, not raced against a timer that has rung.
async fn permit_by(
    from: &Arc<Semaphore>,
    until: tokio::time::Instant,
    spent: bool,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    if spent {
        return Arc::clone(from).try_acquire_owned().ok();
    }
    tokio::select! {
        biased;
        got = Arc::clone(from).acquire_owned() => got.ok(),
        () = tokio::time::sleep_until(until) => None,
    }
}

/// What a body at the door was waiting for when its wait ran out.
enum Wanted {
    /// A permit for the parse.
    Permit,
    /// A place among the bodies between the door and their answer.
    Place,
}

/// One body waiting at the door, counted for as long as this lives: however the wait
/// ends, the count goes back and the time it took is added up.
struct Waiting<'a> {
    door: &'a Door,
    since: tokio::time::Instant,
}

impl<'a> Waiting<'a> {
    fn begin(door: &'a Door) -> Self {
        door.waits.fetch_add(1, Ordering::Relaxed);
        door.waiting.fetch_add(1, Ordering::Relaxed);
        Self {
            door,
            since: tokio::time::Instant::now(),
        }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.door.waiting.fetch_sub(1, Ordering::Relaxed);
        let waited = self.since.elapsed().as_micros() as u64;
        self.door.waited_micros.fetch_add(waited, Ordering::Relaxed);
    }
}

/// Shutdown has begun: every body at the door is refused at once and the log told that
/// no body waits for room, so a stop never takes as long as the longest wait.
pub(super) fn begin_stop(server: &Arc<Server>) {
    server.stopping.cancel();
    // A compaction hears of it now, while the requests in flight finish, and is waited
    // for after the final flush: see `join_compactions`.
    cancel_compactions(server);
    if server.wal.is_none() {
        return;
    }
    let held = Arc::clone(server);
    let hurry = move || {
        if let Some(wal) = &held.wal {
            wal.hurry();
        }
    };
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => drop(runtime.spawn_blocking(hurry)),
        Err(_) => hurry(),
    }
}

/// One `/ingest` body's permits, as the door hands them to the handler: `parse` goes
/// back once the body is in the log's queue, `body` when its task returns.
#[derive(Clone)]
pub(super) struct Permit {
    pub(super) parse: Arc<tokio::sync::OwnedSemaphorePermit>,
    pub(super) body: Arc<Place>,
}

impl Permit {
    fn of(
        parse: tokio::sync::OwnedSemaphorePermit,
        place: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            parse: Arc::new(parse),
            body: Arc::new(Place {
                _permit: place,
                left: AtomicBool::new(false),
            }),
        }
    }
}

/// A body's place among those between the door and their answer, and whether anybody is
/// still waiting for that answer.
pub(super) struct Place {
    _permit: tokio::sync::OwnedSemaphorePermit,
    /// Set when the body's sender hung up before its answer was ready, for a body waiting
    /// for room in the log to give the wait up.
    pub(super) left: AtomicBool,
}

/// The refusal of a body that arrives while the server is stopping and would have had to
/// wait: the shed's shape, the body unread.
fn stopping() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the server is stopping, and nothing of this body was taken; keep it and \
                 send it again in {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// The refusal of a body that found as many waiting at the door as may: the shed's shape.
fn line_full(max: usize) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "{max} bodies are already waiting for ingest to have room for them, which \
                 is as many as may; retry in {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// The refusal while the buffers are past twice their limit; with `failing`, the store's
/// last write error, it says the remedy is the disk and not the shippers.
fn buffers_full((held, limit): (u64, u64), failing: Option<String>) -> Response {
    let why = match failing {
        Some(error) => format!(
            "the store cannot write ({error}), so nothing leaves them - every line already \
             acknowledged is in the write-ahead log"
        ),
        None => "the flush is behind what is arriving".to_string(),
    };
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the buffers hold {} bytes of memory, past twice the {} byte limit: {why}; \
                 retry in {RETRY_AFTER_SECONDS}s",
                commas(held),
                commas(limit)
            ),
        })),
    )
        .into_response()
}

/// The shed of a body the write-ahead log had no room for: the log's disk or the store
/// is behind what arrives, and nothing of the body is in the log.
fn log_busy() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "the write-ahead log has more waiting to be written or stored than it may \
                 hold: its disk or the store is behind what is arriving; retry in \
                 {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// The shed response: 503, a `Retry-After`, and the same error shape as everything else.
fn busy(permits: usize) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS.to_string())],
        Json(json!({
            "ok": false,
            "error": format!(
                "ingest is at capacity ({permits} concurrent bodies); retry in \
                 {RETRY_AFTER_SECONDS}s"
            ),
        })),
    )
        .into_response()
}

/// `{"q":"<trieql>"}` in, `query::result_json` out; the body is decoded here so a
/// malformed one is answered in the same `{"ok":false,"error":...}` shape.
pub(super) async fn post_query(State(server): State<Arc<Server>>, body: Bytes) -> Response {
    let request: QueryRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(e) => {
            return fail(
                StatusCode::BAD_REQUEST,
                &format!("expected {{\"q\": \"…\"}}: {e}"),
            )
        }
    };

    // Parsed before the blocking task, so a bad query costs no thread: it is a string
    // and a grammar, and the answer is a 400 either way.
    let query = match query::from_trieql(&request.q, Utc::now(), server.limits) {
        Ok(query) => query,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &error::chain(&e)),
    };

    // Set when this future is dropped before the task below has returned - a timeout or
    // a shutdown's deadline in `track`, or a closed connection. Counted when the scan sees it.
    let stop = {
        let (server, began, asked) = (Arc::clone(&server), Instant::now(), shortened(&request.q));
        Stop::watched(move || {
            server.queries_stopped.fetch_add(1, Ordering::Relaxed);
            log_warn(&format!(
                "query stopped - {:.2}s after it began, its answer no longer wanted (a \
                 request timeout, a client that left or a shutdown): {asked}",
                began.elapsed().as_secs_f64()
            ));
        })
    };
    let mut unwanted = Unwanted {
        stop: &stop,
        armed: true,
    };
    let held = Arc::clone(&server);
    let told = stop.clone();
    let done = tokio::task::spawn_blocking(move || {
        // The scan and then the answer put into JSON both ask the stop as they go.
        let body = engine::engine::execute_until(&held.trie, &held.stores, &query, &told).and_then(
            |result| {
                query::result_json_until(&query, &result, &told)
                    .ok_or(engine::engine::ExecError::Store(StoreError::Stopped))
            },
        )?;
        held.queries.fetch_add(1, Ordering::Relaxed);
        Ok::<Value, engine::engine::ExecError>(body)
    })
    .await;
    // The task has returned: there is no scan left to tell.
    unwanted.armed = false;

    match done {
        Ok(Ok(body)) => (StatusCode::OK, Json(body)).into_response(),
        // Read by nobody, as a rule: what set the stop also dropped this future or has
        // answered in its place. Said all the same, for a caller that is not `track`.
        Ok(Err(e @ engine::engine::ExecError::Store(StoreError::Stopped))) => {
            fail(StatusCode::SERVICE_UNAVAILABLE, &error::chain(&e))
        }
        // A bucket count over the cap is the client's to fix, so it is a 400 beside the parse
        // errors above; a storage failure is the server's and stays a 500.
        Ok(Err(e @ engine::engine::ExecError::Buckets(_))) => {
            fail(StatusCode::BAD_REQUEST, &error::chain(&e))
        }
        Ok(Err(e)) => fail(StatusCode::INTERNAL_SERVER_ERROR, &error::chain(&e)),
        Err(join) => fail(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("query task failed: {join}"),
        ),
    }
}

/// Tells a query's scan to stop when the handler's future is dropped before its task
/// has returned: see `post_query`. What `Leaving` is to an `/ingest` body.
pub(super) struct Unwanted<'a> {
    pub(super) stop: &'a Stop,
    /// Cleared once the task has returned: there is then nothing to tell.
    pub(super) armed: bool,
}

impl Drop for Unwanted<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.stop.set();
        }
    }
}

/// A query's text for a line of the log: its first 120 characters, on one line.
pub(super) fn shortened(q: &str) -> String {
    let mut out: String = q
        .chars()
        .take(120)
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if q.chars().nth(120).is_some() {
        out.push_str("...");
    }
    out
}

/// `POST /load`: what `kibbr load` sends when this server holds the data directory.
#[derive(Debug, Deserialize)]
struct LoadRequest {
    /// Files, already expanded from any directory by the client, each with the file name
    /// the load id hashes.
    inputs: Vec<LoadInput>,
    #[serde(default)]
    replace_day: Option<String>,
    #[serde(default)]
    max_mb_per_sec: Option<f64>,
    #[serde(default)]
    threads: Option<usize>,
    /// The loader's own bound on the rows it holds, not the live door's: see
    /// `load::DEFAULT_LOAD_BUFFER_BYTES`.
    #[serde(default)]
    load_buffer_bytes: Option<usize>,
    #[serde(default)]
    received_at: Option<String>,
    #[serde(default)]
    proceed: bool,
    #[serde(default)]
    max_arrival_share: Option<f64>,
    /// The data directory the client meant, canonical: `LOCK` can still name the last
    /// holder's address in the moment before a new holder announces its own.
    #[serde(default)]
    data_dir: Option<std::path::PathBuf>,
}

#[derive(Debug, Deserialize)]
struct LoadInput {
    path: std::path::PathBuf,
    name: String,
}

/// Parse and check a load request into the loader's options, or say what is wrong with it.
fn load_options(request: LoadRequest) -> Result<load::Options, String> {
    if request.inputs.is_empty() {
        return Err("inputs is empty: nothing to load".to_string());
    }
    let mut inputs = Vec::with_capacity(request.inputs.len());
    for input in request.inputs {
        // Absolute, because it is this process's working directory that resolves it and
        // not the client's.
        if !input.path.is_absolute() {
            return Err(format!("{}: not an absolute path", input.path.display()));
        }
        match std::fs::metadata(&input.path) {
            Ok(meta) if meta.is_file() => {}
            Ok(_) => return Err(format!("{}: not a file", input.path.display())),
            Err(e) => return Err(format!("{}: {e}", input.path.display())),
        }
        inputs.push(load::Input {
            path: input.path,
            name: input.name,
        });
    }
    let replace_day = match request.replace_day.as_deref() {
        None => None,
        Some(text) => Some(
            crate::storage::store::disk::day_of_name(text)
                .ok_or_else(|| format!("replace_day `{text}` is not a YYYY-MM-DD day"))?,
        ),
    };
    let received_at = match request.received_at.as_deref() {
        None => Utc::now(),
        Some(text) => DateTime::parse_from_rfc3339(text)
            .map_err(|e| format!("received_at `{text}` is not RFC 3339: {e}"))?
            .with_timezone(&Utc),
    };
    let share = request
        .max_arrival_share
        .unwrap_or(load::DEFAULT_MAX_ARRIVAL_SHARE);
    if !(share.is_finite() && share >= 0.0) {
        return Err(format!("max_arrival_share {share} is not a percentage"));
    }
    let load_buffer_bytes = request
        .load_buffer_bytes
        .unwrap_or(load::DEFAULT_LOAD_BUFFER_BYTES);
    if load_buffer_bytes == 0 {
        return Err("load_buffer_bytes 0: a load has to hold at least one row".to_string());
    }
    Ok(load::Options {
        inputs,
        replace_day,
        max_bytes_per_sec: request.max_mb_per_sec.map(|mb| mb * 1_000_000.0),
        threads: request.threads.unwrap_or_else(load::server_threads).max(1),
        load_buffer_bytes,
        received_at,
        proceed: request.proceed,
        max_arrival_share: share,
    })
}

/// Start a load job and answer at once with its number: a request held open for the
/// load would hold up a graceful shutdown, and a client that gave up would not stop it.
pub(super) async fn post_load(State(server): State<Arc<Server>>, headers: HeaderMap, body: Bytes) -> Response {
    if server.stores.disk_store().is_none() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            &load::LoadError::NoDisk.to_string(),
        );
    }
    if let Some(refused) = load_refused(&server, &headers) {
        return refused;
    }
    let request: LoadRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &format!("load request: {e}")),
    };
    if let Some(refused) = wrong_directory(&server, request.data_dir.as_deref(), "loaded") {
        return refused;
    }
    let options = match load_options(request) {
        Ok(o) => o,
        Err(message) => return fail(StatusCode::BAD_REQUEST, &message),
    };
    let mut loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((id, _)) = loads.jobs.iter().find(|(_, job)| {
        matches!(
            *job.state.lock().unwrap_or_else(|e| e.into_inner()),
            LoadState::Running
        )
    }) {
        return fail(
            StatusCode::CONFLICT,
            &format!("load job {id} is still running; one load runs at a time"),
        );
    }
    // Reap the last one's thread, which has finished: its state says so.
    if let Some(done) = loads.running.take() {
        let _ = done.join();
    }
    loads.next += 1;
    let id = loads.next;
    let job = Arc::new(LoadJob {
        control: load::Control::default(),
        state: std::sync::Mutex::new(LoadState::Running),
    });
    loads.jobs.insert(id, Arc::clone(&job));
    while loads.jobs.len() > LOAD_JOBS_KEPT {
        let oldest = *loads.jobs.keys().next().expect("more than kept");
        loads.jobs.remove(&oldest);
    }
    let held = Arc::clone(&server);
    let spawned = std::thread::Builder::new()
        .name(format!("load-{id}"))
        .spawn(move || {
            log(&format!(
                "load job {id} - {} input(s){}",
                options.inputs.len(),
                options
                    .replace_day
                    .map(|d| format!(", replacing {}", crate::storage::store::disk::day_name(d)))
                    .unwrap_or_default()
            ));
            // A panic is a failed job and not a job running for ever, which would refuse every
            // later load with a 409. Its staging stays for the next start to finish or remove.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                load::run(
                    &held.trie,
                    &held.stores,
                    &options,
                    &job.control,
                    &mut |_| {},
                )
            }));
            let state = match result {
                Ok(Ok(report)) => {
                    log(&format!("load job {id} - {}", report.summary()));
                    LoadState::Done(Box::new(report))
                }
                Ok(Err(e)) => {
                    log(&format!("load job {id} - failed: {}", error::chain(&e)));
                    LoadState::Failed(error::chain(&e))
                }
                Err(_) => {
                    let why = "the load failed on a bug in this build (a panic); anything it \
                               staged is finished or removed by the next start";
                    log(&format!("load job {id} - {why}"));
                    LoadState::Failed(why.to_string())
                }
            };
            *job.state.lock().unwrap_or_else(|e| e.into_inner()) = state;
        });
    match spawned {
        Ok(handle) => {
            loads.running = Some(handle);
            (StatusCode::ACCEPTED, Json(json!({"ok": true, "job": id}))).into_response()
        }
        Err(e) => {
            loads.jobs.remove(&id);
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("starting the load job: {e}"),
            )
        }
    }
}

/// `GET /load/<job>`: running, done with its report, or failed with why.
pub(super) async fn get_load(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<u64>,
) -> Response {
    if let Some(refused) = load_refused(&server, &headers) {
        return refused;
    }
    let job = {
        let loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
        loads.jobs.get(&id).map(Arc::clone)
    };
    let Some(job) = job else {
        return fail(StatusCode::NOT_FOUND, &format!("no load job {id}"));
    };
    let state = job.state.lock().unwrap_or_else(|e| e.into_inner());
    let body = match &*state {
        LoadState::Running => json!({
            "ok": true, "job": id, "state": "running",
            "lines": job.control.lines.load(Ordering::Relaxed),
        }),
        LoadState::Done(report) => {
            json!({"ok": true, "job": id, "state": "done", "report": report})
        }
        LoadState::Failed(error) => {
            json!({"ok": true, "job": id, "state": "failed", "error": error})
        }
    };
    Json(body).into_response()
}

/// Stop a running load before it commits, and wait for its thread; one already inside
/// its commit finishes it, and one before it removes its staging directory.
pub(super) fn stop_loads(server: &Server) {
    let (running, jobs): (Option<std::thread::JoinHandle<()>>, Vec<Arc<LoadJob>>) = {
        let mut loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
        (loads.running.take(), loads.jobs.values().cloned().collect())
    };
    let Some(running) = running else {
        return;
    };
    for job in &jobs {
        job.control.cancel.store(true, Ordering::Relaxed);
    }
    if !running.is_finished() {
        log("waiting for the running load job to stop");
    }
    let _ = running.join();
}

/// Whether a load job is running: the scheduler leaves compaction until it is not.
pub(super) fn load_running(server: &Server) -> bool {
    let loads = server.loads.lock().unwrap_or_else(|e| e.into_inner());
    loads.jobs.values().any(|job| {
        matches!(
            *job.state.lock().unwrap_or_else(|e| e.into_inner()),
            LoadState::Running
        )
    })
}
