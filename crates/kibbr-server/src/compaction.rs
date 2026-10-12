//! Compaction: `/compact`, the loop that looks for due days, and their stop.

use super::*;

// ── Compaction ─────────────────────────────────────────────────────────────────────

/// A compaction as `kibbr compact` sends it.
#[derive(Debug, Deserialize)]
struct CompactRequest {
    /// The UTC day to compact now, YYYY-MM-DD. Absent or null, every day that is due.
    #[serde(default)]
    day: Option<String>,
    #[serde(default)]
    threads: Option<usize>,
    /// The data directory the client meant, canonical: see `wrong_directory`.
    #[serde(default)]
    data_dir: Option<std::path::PathBuf>,
}

/// Start a compaction job and answer at once with its number; `GET /compact/<job>` says how
/// it went. Not held open for the compaction, for the reasons `post_load` is not.
///
/// **One at a time, and apart from the load slot.** A second compaction is a 409. A load
/// beside one is not: the two meet only under the store's own lock, and a load that
/// replaces the day being compacted wins it - the compaction finds its inputs retired and
/// removes what it rebuilt.
pub(super) async fn post_compact(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if server.stores.disk_store().is_none() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            compact::NO_DISK,
        );
    }
    if let Some(refused) = compact_refused(&server, &headers) {
        return refused;
    }
    let request: CompactRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => return fail(StatusCode::BAD_REQUEST, &format!("compact request: {e}")),
    };
    if let Some(refused) = wrong_directory(&server, request.data_dir.as_deref(), "compacted") {
        return refused;
    }
    let day = match request.day.as_deref() {
        None => None,
        Some(text) => match kibbr_storage::store::disk::day_of_name(text) {
            Some(day) => Some(day),
            None => {
                return fail(
                    StatusCode::BAD_REQUEST,
                    &format!("day `{text}` is not a YYYY-MM-DD day"),
                )
            }
        },
    };
    let threads = request.threads.unwrap_or_else(load::server_threads).max(1);

    let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    if jobs.is_stopping() {
        return fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "this server is shutting down; nothing was compacted",
        );
    }
    let Some(id) = jobs.start() else {
        return fail(
            StatusCode::CONFLICT,
            &match jobs.running() {
                Some(id) => {
                    format!("compaction job {id} is still running; one compaction runs at a time")
                }
                None => "a compaction is still running; one runs at a time".to_string(),
            },
        );
    };
    // Reap the last one's thread, which has finished: no job was running, and a job's
    // state is the last thing its thread touches here.
    if let Some(done) = jobs.asked.take() {
        let _ = done.join();
    }
    let held = Arc::clone(&server);
    let spawned = std::thread::Builder::new()
        .name(format!("compact-{id}"))
        .spawn(move || compaction(&held, day, threads, Some(id)));
    match spawned {
        Ok(handle) => {
            jobs.asked = Some(handle);
            (StatusCode::ACCEPTED, Json(json!({"ok": true, "job": id}))).into_response()
        }
        Err(e) => {
            jobs.forget(id);
            fail(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("starting the compaction job: {e}"),
            )
        }
    }
}

/// `GET /compact/<job>`: running, done with its report, or failed with why - and with the
/// days it had finished before it failed.
pub(super) async fn get_compact(
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<u64>,
) -> Response {
    if let Some(refused) = compact_refused(&server, &headers) {
        return refused;
    }
    let state = server
        .compactions
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .state(id);
    let body = match state {
        None => return fail(StatusCode::NOT_FOUND, &format!("no compaction job {id}")),
        Some(compact::State::Running) => json!({"ok": true, "job": id, "state": "running"}),
        Some(compact::State::Done(report)) => {
            json!({"ok": true, "job": id, "state": "done", "report": report})
        }
        Some(compact::State::Failed(error, report)) => {
            json!({"ok": true, "job": id, "state": "failed", "error": error, "report": report})
        }
    };
    Json(body).into_response()
}

/// A compaction thread, from its first look to its job's last word: which days, each of
/// them in turn, and how the job ended.
///
/// `job` is the number a request was answered with. The scheduler's thread comes with
/// none, and **takes one only once it has a day to try**: a look that finds nothing due,
/// or only days still waiting out a failure, is not a job - it would fill the kept jobs
/// with empty ones and refuse a request that arrived while it looked.
///
/// **The due days are listed here, on this thread.** The listing takes the flush's lock,
/// which a flush holds across its writes; taken on the runtime it would park a worker
/// behind the disk.
///
/// **A failure here is not a write failure.** It is logged, kept for `/stats` and
/// remembered by the scheduler; the store's health, which `/ingest` and `/health` answer
/// from, is the flush's alone and is not touched.
fn compaction(server: &Server, day: Option<i64>, threads: usize, job: Option<u64>) {
    let jobs = || server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    let every = server.storage.compact_interval;
    let scheduled = job.is_none();
    // Held from the moment there is a job, so that nothing which ends this thread - the
    // panic caught below, or one that is not - leaves the job `running`.
    let ending = |id| compact::Ending::of(&server.compactions, id);
    let asked = job.map(ending);
    let Some(store) = server.stores.disk_store() else {
        if let Some(asked) = asked {
            let why = compact::NO_DISK.to_string();
            let state = compact::State::Failed(why, compact::Report::default());
            asked.finish(state, false);
        }
        return;
    };
    let mut days = compact::plan(store, day);
    let (id, ending) = match job.zip(asked) {
        Some(asked) => asked,
        None => {
            // A day that failed and has nothing left to compact - a load replaced it - is
            // never tried again, so its failure is dropped here or never. Listed before
            // the jobs' lock is taken: the two locks are never held together.
            let compactable = store.compactable_days();
            let mut jobs = jobs();
            jobs.forget_except(&compactable);
            days.retain(|planned| jobs.wait(*planned, every).is_none());
            if days.is_empty() {
                return;
            }
            // None while a request's job runs, or once shutdown began: the next look.
            let Some(id) = jobs.start() else {
                return;
            };
            (id, ending(id))
        }
    };
    log(&format!(
        "compaction job {id} - {}",
        match day {
            Some(day) => kibbr_storage::store::disk::day_name(day),
            None => format!("{} day(s) due", days.len()),
        }
    ));
    let opts = CompactOptions {
        threads,
        cancel: Arc::clone(&server.compact_cancel),
    };
    let mut noted = 0;
    // A panic is a failed job and not a job running for ever, as for a load. Whatever it
    // had written is unnamed, and the next start removes it.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        compact::run(store, &days, &opts, &mut |planned, result| {
            noted += 1;
            let wait = {
                let mut jobs = jobs();
                jobs.note(planned, result);
                jobs.wait(planned, every).filter(|_| scheduled)
            };
            match result {
                Ok(done) => log(&format!("compaction job {id} - {}", compact::line(done))),
                Err(e @ (StoreError::Cancelled { .. } | StoreError::InputsChanged { .. })) => {
                    log(&format!("compaction job {id} - {}", error::chain(e)))
                }
                Err(e) => log_warn(&format!(
                    "compaction job {id} - {}{}",
                    error::chain(e),
                    wait.map_or_else(String::new, |left| format!(
                        "; the day is left for {}s, unless its files change first",
                        left.as_secs().max(1)
                    ))
                )),
            }
        })
    }));
    let state = match result {
        Ok(ran) => match ran.failed.first() {
            None => compact::State::Done(ran.report),
            Some(first) => {
                let mut why = error::chain(first);
                if ran.failed.len() > 1 {
                    why.push_str(&format!(
                        "; and {} more day(s) failed, each in this server's log",
                        ran.failed.len() - 1
                    ));
                }
                compact::State::Failed(why, ran.report)
            }
        },
        Err(_) => {
            let why = "the compaction failed on a bug in this build (a panic); a file it wrote \
                       and had not named is removed by the next start";
            log_warn(&format!("compaction job {id} - {why}"));
            // The day it was on, remembered as failed: a look every interval would
            // otherwise walk into the same panic every interval.
            if let Some(planned) = days.get(noted) {
                let failed = Err(StoreError::CompactFailed {
                    day: kibbr_storage::store::disk::day_name(planned.0),
                    source: Box::new(StoreError::Io {
                        template_id: 0,
                        source: std::io::Error::other(why),
                    }),
                });
                jobs().note(*planned, &failed);
            }
            compact::State::Failed(why.to_string(), compact::Report::default())
        }
    };
    ending.finish(state, !days.is_empty());
}

/// Look for days that are due, every interval, until the token is cancelled.
///
/// **This task keeps the clock and nothing else.** It watches the token, asks whether a
/// load or a compaction is running, and starts a thread; the thread lists the due days and
/// compacts them (`compaction`). Nothing here takes the store's flush lock, so a slow
/// flush cannot park a runtime worker behind it.
///
/// Not part of `flush_loop`: a compaction awaited there would hold up every tick and the
/// memory relief with it.
///
/// A look is skipped while a load runs - a load that replaces a day would make a rebuild
/// of it wasted work, and the two would share the cores - and while the last flush tick
/// could not write, since a rebuild needs the day's size again on a disk that is already
/// refusing.
pub(super) async fn compact_loop(server: Arc<Server>, token: CancellationToken) {
    let every = server.storage.compact_interval;
    // Zero is "never look", and is returned on before it reaches an interval, which
    // panics on a period of zero.
    if !server.storage.is_durable() || every.is_zero() {
        return;
    }
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick completes immediately, and is consumed: nothing is compacted the
    // moment a server starts, so one that a compaction brought down comes back up and
    // serves for an interval before it tries again.
    tick.tick().await;
    loop {
        tokio::select! {
            () = token.cancelled() => return,
            _ = tick.tick() => {}
        }
        look_for_due_days(&server);
    }
}

/// Whether this look is left to the next tick: a load is running, or the last flush tick
/// could not write. Why each is in `compact_loop`.
pub(super) fn look_waits(server: &Server) -> bool {
    server.stores.is_degraded() || load_running(server)
}

/// One look: start the scheduler's thread, unless the look waits (`look_waits`), a
/// compaction is running, shutdown has begun, or the last look's thread is still at it.
pub(super) fn look_for_due_days(server: &Arc<Server>) {
    if look_waits(server) {
        return;
    }
    let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    if jobs.is_stopping() || jobs.running().is_some() {
        return;
    }
    // Joined only once it has finished: a thread still looking may be waiting for the
    // lock held here.
    match jobs.scheduled.take() {
        Some(last) if !last.is_finished() => {
            jobs.scheduled = Some(last);
            return;
        }
        Some(last) => {
            let _ = last.join();
        }
        None => {}
    }
    let held = Arc::clone(server);
    let spawned = std::thread::Builder::new()
        .name("compact-due".to_string())
        .spawn(move || compaction(&held, None, load::server_threads(), None));
    match spawned {
        Ok(handle) => jobs.scheduled = Some(handle),
        Err(e) => log_warn(&format!("compaction - starting the look for due days: {e}")),
    }
}

/// Shutdown has begun: tell the running compaction to stop, and start no other.
///
/// Called the moment the signal arrives, while the last requests are still draining,
/// rather than when the compaction is waited for: the earlier it hears, the less there is
/// to wait for. The flag is raised under the jobs' lock, which both ways of starting a job
/// take their number under, so none starts after it.
pub(super) fn cancel_compactions(server: &Server) {
    let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
    jobs.stop();
    server.compact_cancel.store(true, Ordering::Relaxed);
}

/// Wait for the compaction threads: after the final flush, before the lock is let go.
///
/// **After the final flush and not before it.** A process told to stop may be killed
/// before it has finished, and a kill before the flush costs the next start a replay of
/// every row still buffered: waiting here first would put a compaction's last file between
/// a SIGTERM and the write that saves it. After it, a kill during this wait costs
/// nothing: a rebuilt file the manifest does not name is removed by the next start. Waiting
/// later is safe because a compaction adds no row and no template: it and the flush meet
/// only under the store's lock, one after the other.
///
/// **Before the lock is released**, because a thread still writing the directory after it
/// would be writing beside whichever process took the lock next.
pub(super) fn join_compactions(server: &Server) {
    cancel_compactions(server);
    let threads = {
        let mut jobs = server.compactions.lock().unwrap_or_else(|e| e.into_inner());
        [jobs.asked.take(), jobs.scheduled.take()]
    };
    for thread in threads.into_iter().flatten() {
        if !thread.is_finished() {
            log("waiting for the running compaction to stop");
        }
        let _ = thread.join();
    }
}
