// Copyright (C) 2026 Pratyaydeep Ghanta
// SPDX-License-Identifier: AGPL-3.0-only

//! The viewer, `/templates`, `/stats`, the layers over every request, and the router.

use super::*;

/// The log viewer, one HTML file; `no-store`, because a cached viewer against a newer
/// server is a debugging tool lying about what the database answered.
async fn get_viewer() -> Response {
    // The footer's two placeholders, filled once: the page is a file and the version is not in it.
    static PAGE: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
        VIEWER
            .replace("{{version}}", VERSION)
            .replace("{{source}}", SOURCE)
    });
    (
        [(axum::http::header::CACHE_CONTROL, "no-store")],
        Html(PAGE.as_str()),
    )
        .into_response()
}

/// Every template with its path, which is its identity: the columns alone do not say
/// which names were promoted into the fingerprint.
async fn get_templates(State(server): State<Arc<Server>>) -> Response {
    let stores = &server.stores;
    let mut templates = Vec::new();

    for (path, leaf) in server.trie.leaf_paths() {
        // A leaf whose meta will not read is a poisoned lock: skipped rather than faked, and
        // `count` below is the length of what was readable.
        let Ok(slot) = leaf.leaf_meta.read() else {
            continue;
        };
        let Some(meta) = slot.as_ref() else {
            continue;
        };

        let columns: Vec<Value> = meta
            .columns
            .iter()
            .map(|col| {
                json!({
                    "name": col.name,
                    // `null`, not "String": a column that has only ever held nulls has no
                    // type yet, and that is a different fact from having widened to text.
                    "type": col.col_type.map(|t| t.as_str()),
                    "widened_at": col.widened_at.map(rfc3339_at),
                })
            })
            .collect();

        templates.push(json!({
            "template_id": meta.template_id,
            "path": path,
            "columns": columns,
            // A template with no store is one that exists and holds no rows. Zero, not
            // absent - the two empty answers stay apart here as they do in the engine.
            "rows": stores.lookup(meta.template_id).map_or(0, |s| s.len()),
            "first_seen": rfc3339_at(meta.first_seen),
            "last_seen": rfc3339_at(meta.last_seen),
            "storage_path": meta.storage_path,
            // `column:<name>` or `arrival`, as the catalog spells it.
            "time_source": meta.time_source.as_record(),
        }));
    }

    Json(json!({ "count": templates.len(), "templates": templates })).into_response()
}

/// Throughput, template count, storage sizes, and the two numbers only visible here:
/// `buffered_rows`, and `write_failures` for a disk gone away while `/ingest` answers 200.
pub(super) async fn get_stats(State(server): State<Arc<Server>>) -> Response {
    let stores = &server.stores;
    let (open_segments, max_open_segments) = stores.open_files();
    let (buffered_bytes, max_buffer_bytes) = stores.buffer_bytes();
    let wal = server.wal.as_ref().map(Wal::stats);
    let uptime = server.started.elapsed().as_secs_f64();
    let ingested = server.lines_ingested.load(Ordering::Relaxed);
    let queries = server.queries.load(Ordering::Relaxed);
    let shed: serde_json::Map<String, Value> = server
        .door
        .shed
        .each()
        .into_iter()
        .map(|(cause, count)| (cause.to_string(), json!(count)))
        .collect();

    Json(json!({
        "uptime_seconds": uptime,
        "started_at": rfc3339_at(server.started_at),
        "storage": {
            "kind": stores.kind(),
            "durable": server.storage.is_durable(),
            "note": if server.storage.is_durable() {
"a line is written to the write-ahead log under wal/ and flushed before /ingest acknowledges it, so an unclean exit loses no acknowledged line: the next start puts back every line no row group holds, once; one shared data file per UTC day, named in manifest.log; a row group closes at 8,192 rows, 8MB or the row group timeout, at most max_open_segments day files are open for writing at once and a row for a day with none open past that seals the least recently written and opens its own day's, and a day is rebuilt into full row groups in finished files the day after its last live row, once all of it is behind the log's checkpoint"
            } else {
                "in-memory only - every row is lost when this process exits"
            },
            "base_path": stores.base_path().map(|p| p.display().to_string()),
            "trie_file": server
                .storage
                .trie_file
                .as_ref()
                .map(|p| p.display().to_string()),
            // The file templates are rebuilt from. `trie_file` is the readable snapshot
            // nothing loads while this exists.
            "catalog": server.trie.catalog_path().map(|p| p.display().to_string()),
            "flush_interval_ms": server.storage.flush_interval.as_millis() as u64,
            // How long a row group may stay open: what a quiet template's rows wait in
            // the log and in memory before they reach a file.
            "row_group_timeout_ms": server.storage.row_group_timeout.as_millis() as u64,
            // The write-ahead log; null when storage is memory. `healthy` false is /ingest
            // answering 503, and `replay` is what this start read back.
            "wal": wal.as_ref().map(|wal| wal_stats(&server, wal)),
            "stores_open": stores.open(),
            // Day files held open for writing, and the cap they are held under. Past it the
            // least recently written one is sealed.
            "open_segments": open_segments,
            "max_open_segments": max_open_segments,
            "segment_max_bytes": server.storage.segment_max_bytes,
            "rows": stores.rows(),
            "buffered_rows": stores.buffered(),
            // Estimated memory the buffered rows hold, against the limit past which the
            // largest are closed early.
            "buffered_bytes": buffered_bytes,
            "max_buffer_bytes": max_buffer_bytes,
            // 0 is never. `compaction` is read from the jobs' own lock: this does not wait
            // on a flush.
            "compact_interval_ms": server.storage.compact_interval.as_millis() as u64,
            "compaction": server
                .compactions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .stats(),
            "forced_closes": stores.forced_closes(),
            "write_failures": stores.write_failures(),
            "last_write_error": stores.last_write_error(),
            // True while the last flush tick failed, which is also while /health says
            // degraded; /ingest goes on answering 200 meanwhile, its lines being in the log.
            "degraded": stores.is_degraded(),
            // Estimated either way: on disk the segments' bytes plus what the buffer would
            // add, in memory payload bytes. The log's segments count: this database holds them.
            "approx_bytes": stores.approx_bytes() + wal.as_ref().map_or(0, |wal| wal.bytes),
        },
        "trie": {
            "templates": server.trie.template_count(),
            "nodes": server.trie.node_count(),
            "tokens": server.trie.token_count(),
        },
        "ingest": {
            "lines_read": server.lines_read.load(Ordering::Relaxed),
            "lines_ingested": ingested,
            "lines_rejected": server.lines_rejected.load(Ordering::Relaxed),
            // Averaged over uptime, not sampled over a window: an idle server reports a
            // falling rate rather than a stale spike.
            "lines_per_second": rate(ingested, uptime),
            // Nonzero means the server was telling shippers to slow down, which is the design
            // working. A body shed at the door was never read, so no line counter has it.
            "requests_shed": server.door.shed.total(),
            "ingest_wait_ms": server.api.ingest_wait.as_millis() as u64,
            // 0 is for as long as they take.
            "shutdown_deadline_ms": server
                .api
                .shutdown_deadline
                .map_or(0, |deadline| deadline.as_millis() as u64),
            // 0 is for as long as it takes.
            "request_timeout_ms": server
                .api
                .request_timeout
                .map_or(0, |timeout| timeout.as_millis() as u64),
            "requests_timed_out": server.timed_out.load(Ordering::Relaxed),
            "ingest_concurrency": server.api.ingest_concurrency,
            "ingest_bodies": ingest_bodies(&server.api),
            // Permits for the parse in use, and places between the door and an answer in
            // use: the second less the first is the bodies waiting on the log.
            "parsing": server
                .api
                .ingest_concurrency
                .saturating_sub(server.ingest_permits.available_permits()),
            "bodies_inside": ingest_bodies(&server.api)
                .saturating_sub(server.ingest_bodies.available_permits()),
            // `line_max` is the most that may wait at the door at once, and null while the
            // line has no bound.
            "door": {
                "waiting": server.door.waiting.load(Ordering::Relaxed),
                "waits": server.door.waits.load(Ordering::Relaxed),
                "waited_ms": server.door.waited_micros.load(Ordering::Relaxed) / 1000,
                "line_max": server.line_max(),
            },
            // `requests_shed`, by what there was none of.
            "shed": shed,
        },
        "queries": {
            "served": queries,
            // Scans told to stop, which did: a request that timed out, a client that
            // left, a shutdown. Not in `served`.
            "stopped": server.queries_stopped.load(Ordering::Relaxed),
            "per_second": rate(queries, uptime),
            // Runs of a day's row groups chosen on more threads than the query's own. It stays
            // where it is while bodies are ingested: a query then runs on its own thread.
            "scan_fan_outs": server.fan.as_ref().map_or(0, |fan| fan.fanned()),
            // What this server resolved: without them a count of 0 above reads the same for
            // one thread as for four with nothing worth sharing.
            "scan_threads": scan_threads(&server),
            "scan_fan_rows": server.storage.scan_fan_rows,
        },
    }))
    .into_response()
}

/// `/stats`'s `storage.wal`: where the log has got to, and what is waiting on it. Two
/// objects joined, because one `json!` holds only so many keys before it stops expanding.
fn wal_stats(server: &Server, wal: &wal::Stats) -> Value {
    let mut stats = json!({
        "dir": server.wal.as_ref().map(|w| w.dir().display().to_string()),
        "stream": format!("{:016x}", wal.stream),
        "segments": wal.segments,
        "first_segment": wal.first_segment,
        "bytes": wal.bytes,
        "last_seq": wal.last_seq,
        "applied": wal.applied,
        "checkpoint": wal.checkpoint,
        "flushes": wal.flushes,
        "healthy": wal.healthy,
        "last_error": wal.last_error,
        "segment_bytes": server.storage.wal_segment_bytes,
        "batch_size": server.storage.wal_batch_lines,
        "held_rows": server.held.rows(),
        "replay": {
            "records": server.replayed.records,
            "applied": server.replayed.applied,
            "covered": server.replayed.covered,
            "held": server.replayed.held,
            "replaced": server.replayed.replaced,
            "behind_checkpoint": server.replayed.behind,
            "torn": server.replayed.torn,
            "corrupt": server.replayed.corrupt,
            "seconds": server.replayed.seconds,
        },
    });
    let queue = json!({
        // The two counts are each exact and are not read together, so their sum can miss the
        // one body the writer is taking at that instant, and never counts one twice.
        "queue_max_bytes": server.storage.wal_queue_bytes,
        "queue_bytes": wal.queue_bytes,
        "unapplied_bytes": wal.unapplied_bytes,
        "queue_peak_bytes": wal.queue_peak_bytes,
        // `queue_sheds` are bodies whose wait for room ran out, `queue_left` those let go
        // because their sender hung up while they waited.
        "queue_waiting": wal.queue_waiting,
        "queue_waits": wal.queue_waits,
        "queue_sheds": wal.queue_sheds,
        "queue_left": wal.queue_left,
    });
    if let (Value::Object(stats), Value::Object(queue)) = (&mut stats, queue) {
        stats.extend(queue);
    }
    stats
}

/// Zero for the first instant of uptime, rather than an infinity that serialises to
/// `null` and reads as "unknown".
fn rate(n: u64, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        0.0
    } else {
        n as f64 / seconds
    }
}

/// Every failure this surface reports, in one shape: `{"ok":false,"error":"..."}`.
pub(super) fn fail(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "ok": false, "error": message }))).into_response()
}

/// Counts a request as in flight, and answers it here at the shutdown deadline (503) or
/// its own timeout (408 unread, 504 read) - never an `/ingest` body the log may have taken.
async fn track(State(server): State<Arc<Server>>, request: Request, next: Next) -> Response {
    let _guard = InFlight::enter(&server);
    let noted = tracing::enabled!(tracing::Level::DEBUG).then(|| {
        (
            request.method().clone(),
            request.uri().path().to_string(),
            Instant::now(),
        )
    });
    let arrived = request.extensions().get::<Arrived>().cloned();
    let ingest = request.uri().path() == "/ingest";
    let timeout = server.api.request_timeout;
    let mut handled = std::pin::pin!(next.run(request));
    // One timer a request, made when this is first polled; with no timeout, none.
    let mut clock = std::pin::pin!(async {
        match timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending().await,
        }
    });
    // Cleared when the time runs out on a request it does not end.
    let mut timed = true;
    let answer = loop {
        tokio::select! {
            answer = &mut handled => break answer,
            () = server.cut_off.cancelled() => break cut_off(),
            () = &mut clock, if timed => {
                let whole = arrived
                    .as_ref()
                    .is_none_or(|body| body.0.load(Ordering::Relaxed));
                if whole && ingest {
                    timed = false;
                    continue;
                }
                server.timed_out.fetch_add(1, Ordering::Relaxed);
                break timed_out(whole, timeout.unwrap_or_default());
            }
        }
    };
    if let Some((method, path, since)) = noted {
        let status = answer.status().as_u16();
        tracing::debug!(
            method = %method,
            path = %path,
            status,
            micros = since.elapsed().as_micros() as u64,
            "{method} {path} - {status}"
        );
    }
    answer
}

/// The answer of a request still in flight when the shutdown deadline passed.
fn cut_off() -> Response {
    fail(
        StatusCode::SERVICE_UNAVAILABLE,
        "the server is stopping, and its shutdown deadline passed before this request \
         was answered",
    )
}

/// The answer of a request `api.request_timeout` ran out on: `whole` is whether its body
/// had been read to its end. See `track`.
fn timed_out(whole: bool, timeout: Duration) -> Response {
    let given = crate::settings::duration_words(timeout);
    if whole {
        fail(
            StatusCode::GATEWAY_TIMEOUT,
            &format!(
                "not answered within {given} (--request-timeout): a query's scan was \
                 stopped and nothing of its answer was kept. Ask for less - a narrower \
                 time range, more of the pattern, a LIMIT - or give requests longer"
            ),
        )
    } else {
        fail(
            StatusCode::REQUEST_TIMEOUT,
            &format!(
                "the request's body had not arrived {given} after its headers \
                 (--request-timeout); nothing of it was taken"
            ),
        )
    }
}

/// `Connection: close` on every answer sent with the request's body unread (RFC 9112
/// section 9.6): one layer, so it covers the answers axum builds as well.
async fn say_close(request: Request, next: Next) -> Response {
    // A request with no body has nothing left unread, whatever answers it.
    if request.body().is_end_stream() {
        return next.run(request).await;
    }
    let version = request.version();
    let ended = Arc::new(AtomicBool::new(false));
    let mut request = request.map(|inner| {
        Body::new(Watched {
            inner,
            ended: Arc::clone(&ended),
        })
    });
    // For `track`, which asks the same question when a request's time runs out.
    request.extensions_mut().insert(Arrived(Arc::clone(&ended)));
    let mut response = next.run(request).await;
    if !ended.load(Ordering::Relaxed) {
        // `insert`, so an answer that came with a `Connection` header leaves with this one
        // and not with two.
        response
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        // In the client's own version: an HTTP/1.1 answer to an HTTP/1.0 client that asked to
        // keep its connection is given `keep-alive` on its way out, over the header above.
        *response.version_mut() = version;
    }
    response
}

/// Whether a request's body has been read to its end, as `Watched` records it: put on
/// the request by `say_close`, and absent from one that came with no body.
#[derive(Clone)]
struct Arrived(Arc<AtomicBool>);

/// A request body that records whether it was read to its end: see `say_close`.
struct Watched {
    inner: Body,
    ended: Arc<AtomicBool>,
}

impl HttpBody for Watched {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        // The end is `None`, or a frame with nothing after it: a body with a `Content-Length`
        // ends on its last data frame, and a reader that stops there has read all of it.
        let ended = match &polled {
            Poll::Ready(None) => true,
            Poll::Ready(Some(Ok(_))) => self.inner.is_end_stream(),
            _ => false,
        };
        if ended {
            self.ended.store(true, Ordering::Relaxed);
        }
        polled
    }

    // Both as the body underneath has them, so the body limit and the `Bytes` extractor
    // see what they saw before this was put around it.
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

pub(super) fn router(server: Arc<Server>) -> Router {
    Router::new()
        .route("/", get(get_viewer))
        .route("/health", get(health))
        .route("/stats", get(get_stats))
        .route("/templates", get(get_templates))
        .route("/query", post(post_query))
        // A load names its files, not their lines: a list of absolute paths, which for a
        // directory of months of hourly files passes axum's 2MB default.
        .route(
            "/load",
            post(post_load).layer(DefaultBodyLimit::max(MAX_LOAD_REQUEST_BYTES)),
        )
        .route("/load/{job}", get(get_load))
        .route("/compact", post(post_compact))
        .route("/compact/{job}", get(get_compact))
        // Order matters: the *last* layer added is outermost, so `admit` runs first and a shed
        // body never reaches the extractor that would have buffered it.
        .route(
            "/ingest",
            post(post_ingest)
                .layer(DefaultBodyLimit::max(server.api.max_body_bytes))
                .layer(axum::middleware::from_fn_with_state(
                    Arc::clone(&server),
                    admit,
                )),
        )
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&server),
            track,
        ))
        // Last, so outermost of all: it has to see the body before anything can refuse it or
        // stop reading it, to know that the answer went out with the body unread.
        .layer(axum::middleware::from_fn(say_close))
        .with_state(server)
}
