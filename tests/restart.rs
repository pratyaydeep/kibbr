//! What the *next* process sees.
//!
//! Every test here ingests under one server, stops it, and starts another on the same
//! directory. None of it is observable from inside a single process: a store that never
//! reopened its files can hold the right answer in memory and have written nothing.
//!
//! Four questions, one per test - do the rows come back, does a damaged `trie.json` warn
//! rather than crash, does a cursor issued before the restart still name the same row
//! after it, and are ids that a directory already holds ever handed out twice.
//!
//! HTTP over a bare `TcpStream`, as in `serve.rs` and for the same reason: the contract is
//! a status line and a JSON body.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, ChildStderr, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

/// Lines in the ingest body. No corruptions: what a bad line does is `serve.rs`'s
/// subject, and here it would only make the expected counts arithmetic.
const LINES: usize = 500;

// ── A database, and the servers started on it ──────────────────────────────────────

/// A data directory that outlives every server started on it.
///
/// In `serve.rs` the directory belongs to the `Server` and dies with it, which is right
/// there and wrong here: these tests need the bytes to still be on disk when the process
/// that wrote them is gone. So the directory is the fixture and the servers are borrowed.
struct Db {
    dir: PathBuf,
}

impl Db {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "triedb-restart-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        Self { dir }
    }

    fn start(&self) -> Server {
        self.start_with(&[])
    }

    fn start_with(&self, extra: &[&str]) -> Server {
        let mut child = Command::new(env!("CARGO_BIN_EXE_triedb"))
            .args(["serve", "--port", "0"])
            .args(["--data-dir", &self.dir.display().to_string()])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn triedb serve");

        let mut stderr = BufReader::new(child.stderr.take().expect("stderr was not piped"));
        let mut port = None;
        let mut banner = String::new();
        for _ in 0..40 {
            let mut line = String::new();
            if stderr.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            banner.push_str(&line);
            if let Some(at) = line.find("http://127.0.0.1:") {
                port = line[at + "http://127.0.0.1:".len()..].trim().parse().ok();
            }
            if line.contains("shutdown ") {
                break;
            }
        }
        let port = port.unwrap_or_else(|| panic!("no listening line in banner:\n{banner}"));

        Server {
            child,
            port,
            stderr,
            banner,
        }
    }

    fn trie_json(&self) -> PathBuf {
        self.dir.join("trie.json")
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct Server {
    child: Child,
    port: u16,
    stderr: BufReader<ChildStderr>,
    /// Kept because a restart's startup lines are the assertion in two of these tests -
    /// whether the snapshot was restored, and what it said when it could not be.
    banner: String,
}

impl Server {
    fn post(&self, path: &str, body: &[u8]) -> (u16, String) {
        request(self.port, "POST", path, body)
    }

    fn get(&self, path: &str) -> (u16, String) {
        request(self.port, "GET", path, b"")
    }

    fn query(&self, q: &str) -> serde_json::Value {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = self.post("/query", body.as_bytes());
        assert_eq!(status, 200, "query `{q}` answered {status}: {text}");
        json(&text)
    }

    /// A query that is expected not to be answered, kept as its status and body.
    fn query_raw(&self, q: &str) -> (u16, serde_json::Value) {
        let body = serde_json::json!({ "q": q }).to_string();
        let (status, text) = self.post("/query", body.as_bytes());
        (status, json(&text))
    }

    fn ingest(&self, body: &[u8]) -> serde_json::Value {
        let (status, text) = self.post("/ingest", body);
        assert_eq!(status, 200, "ingest answered {status}: {text}");
        json(&text)
    }

    /// SIGTERM, then wait for the exit - the graceful path, which is the one that writes
    /// `trie.json` and runs the final flush. Surviving `kill -9` is the WAL's job, not
    /// this file's subject.
    fn shutdown(mut self) -> String {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("failed to run kill")
            .success();
        assert!(ok, "kill -TERM failed");

        let status = self.child.wait().expect("failed to wait for triedb serve");
        let mut log = String::new();
        let _ = self.stderr.read_to_string(&mut log);
        assert_eq!(
            status.code(),
            Some(0),
            "a graceful shutdown did not exit 0:\n{log}"
        );
        // Already reaped; stop `Drop` from killing a pid that may have been reused.
        self.child = Command::new("true").spawn().expect("failed to spawn true");
        let _ = self.child.wait();
        log
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── Wire ───────────────────────────────────────────────────────────────────────────

fn request(port: u16, method: &str, path: &str, body: &[u8]) -> (u16, String) {
    let mut socket = TcpStream::connect(("127.0.0.1", port)).expect("failed to connect");
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).expect("failed to write");
    let _ = socket.write_all(body);
    let _ = socket.flush();

    let mut response = Vec::new();
    socket
        .read_to_end(&mut response)
        .expect("failed to read the response");
    let split = response
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body split in the response");
    let headers = String::from_utf8_lossy(&response[..split]).into_owned();
    let status = headers
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response:\n{headers}"));
    (
        status,
        String::from_utf8_lossy(&response[split + 4..]).into_owned(),
    )
}

fn json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("not JSON ({e}): {text}"))
}

/// `LINES` generated lines, seeded so two runs of this file ingest the same bytes.
fn corpus() -> Vec<u8> {
    let out = Command::new(env!("CARGO_BIN_EXE_triedb"))
        .args(["gen-logs", "--lines", &LINES.to_string(), "--seed", "42"])
        .output()
        .expect("failed to run gen-logs");
    assert!(out.status.success(), "gen-logs failed");
    out.stdout
}

fn rows_as_text(result: &serde_json::Value) -> Vec<String> {
    result["rows"]
        .as_array()
        .expect("rows is an array")
        .iter()
        .map(ToString::to_string)
        .collect()
}

/// The queries compared across a restart.
///
/// Chosen to read through different paths rather than to be many: a count that never
/// materialises a row, a full scan that materialises every one in order, a filter that
/// selects, a grouped aggregate, and a bucketed one that touches the timestamp column.
/// An answer that survived a restart in one of these and not the others is a store that
/// wrote some of its columns.
const BATTERY: [&str; 5] = [
    "COUNT LOG {}",
    "MATCH LOG {} LIMIT 100000",
    r#"MATCH LOG { "level": "error" } LIMIT 100000"#,
    r#"COUNT LOG {} GROUP BY "level""#,
    "COUNT LOG {} GROUP BY time(1h)",
];

/// Everything about an answer that a restart must not change.
///
/// `meta` is deliberately excluded: it carries the duration and the files opened, which
/// differ between a store answering from its write buffer and the same store answering
/// from the files it flushed - and differing there is the correct behaviour, not a
/// regression.
fn answer(server: &Server, q: &str) -> String {
    let full = server.query(q);
    serde_json::json!({
        "exists": full["exists"],
        "count": full["count"],
        "rows": full["rows"],
        "groups": full["groups"],
    })
    .to_string()
}

// ── The rows come back ─────────────────────────────────────────────────────────────

/// Ingest, stop, start, ask again.
#[test]
fn a_restart_answers_every_query_identically() {
    let db = Db::new();

    let first = db.start();
    let report = first.ingest(&corpus());
    assert_eq!(report["ingested"], LINES as u64);
    let before: Vec<String> = BATTERY.iter().map(|q| answer(&first, q)).collect();
    // A corpus that answered nothing would make every assertion below vacuous.
    assert_eq!(first.query("COUNT LOG {}")["count"], LINES as u64);
    let templates = json(&first.get("/templates").1)["count"].clone();
    assert!(templates.as_u64().unwrap_or(0) > 0, "no templates learned");

    let log = first.shutdown();
    assert!(
        log.contains("trie.json - ") && !log.contains("WARN trie.json"),
        "the snapshot was not written on a graceful exit:\n{log}"
    );

    let second = db.start();
    assert!(
        second.banner.contains("restored"),
        "the restart did not restore the snapshot:\n{}",
        second.banner
    );
    assert_eq!(
        json(&second.get("/templates").1)["count"],
        templates,
        "the restart learned a different number of templates"
    );

    let after: Vec<String> = BATTERY.iter().map(|q| answer(&second, q)).collect();
    for (q, (before, after)) in BATTERY.iter().zip(before.iter().zip(after.iter())) {
        assert_eq!(before, after, "`{q}` answered differently after a restart");
    }
}

// ── A damaged snapshot ─────────────────────────────────────────────────────────────

/// Truncated by hand, which is what a machine losing power mid-write leaves behind.
///
/// A WARN and an empty trie, never a hard failure. What that rules out is a crash loop: a
/// process that cannot start because of a file it wrote itself, and that an operator
/// cannot fix without knowing the format. Starting empty costs the learned templates and
/// relearns them; refusing to start costs the whole database until someone reads the
/// source.
#[test]
fn a_truncated_trie_json_warns_and_starts_clean() {
    let db = Db::new();

    let first = db.start();
    first.ingest(&corpus());
    first.shutdown();

    let whole = fs::read(db.trie_json()).expect("no trie.json after a graceful exit");
    assert!(whole.len() > 40, "trie.json is too small to truncate");
    fs::write(db.trie_json(), &whole[..whole.len() / 2]).expect("failed to truncate");

    let second = db.start();
    assert!(
        second.banner.contains("WARN trie.json"),
        "a truncated snapshot did not warn:\n{}",
        second.banner
    );
    assert!(
        second.banner.contains("starting with an empty trie"),
        "the warning does not say what it did instead:\n{}",
        second.banner
    );

    // Started, not merely warned: it is serving, which is the half a crash loop fails.
    let (status, body) = second.get("/health");
    assert_eq!(status, 200);
    assert_eq!(json(&body), serde_json::json!({ "ok": true }));
    assert_eq!(json(&second.get("/templates").1)["count"], 0);

    // The leaf directories the lost snapshot described are still on disk, and the server
    // says so rather than leaving an operator to find them with `du`.
    assert!(
        second.banner.contains("WARN storage -") && second.banner.contains("no template"),
        "the orphaned leaf directories were not reported:\n{}",
        second.banner
    );

    // And it relearns. The rows ingested now are queryable, so the empty start is a
    // recovery and not a server that is up but permanently useless.
    let report = second.ingest(&corpus());
    assert_eq!(report["ingested"], LINES as u64);
    // `LINES`, not twice it: the relearned templates take fresh ids and so fresh
    // directories, leaving the orphans unread. Losing the snapshot loses the rows it
    // described - which is the documented cost of starting empty, and is what makes the
    // alternative in `a_relearned_template_never_reuses_a_leaf_directory` unacceptable.
    assert_eq!(second.query("COUNT LOG {}")["count"], LINES as u64);

    // The next graceful exit leaves a snapshot that parses: the damage does not persist.
    second.shutdown();
    let third = db.start();
    assert!(
        !third.banner.contains("WARN trie.json"),
        "the damage survived a clean shutdown:\n{}",
        third.banner
    );
}

// ── A cursor across a restart ──────────────────────────────────────────────────────

/// Cursors are **durable**, not session-scoped: all three components are properties of the
/// row rather than positions in a structure a restart rebuilds, so a cursor must resolve to
/// the same row in the next process. The reasoning is at the `Cursor` definition.
///
/// This is the test that decision has to earn. It rules out the failure that produces
/// plausible wrong answers rather than errors: a cursor resolving *nearly* right after a
/// restart silently skips or repeats rows, and the caller's answer looks complete.
#[test]
fn a_cursor_issued_before_a_restart_resolves_identically_after_one() {
    let db = Db::new();

    let first = db.start_with(&["--max-rows", "50"]);
    first.ingest(&corpus());

    let page = first.query("MATCH LOG {}");
    assert_eq!(page["meta"]["truncated"], true);
    let cursor = page["meta"]["next_cursor"]
        .as_str()
        .expect("a backstopped page carried no cursor")
        .to_string();

    // What the rest of the answer is, read without a restart in the way. Paging to
    // completion here rather than taking one page: a cursor that resolves to the right
    // row and then reads the partitions in a different order is still wrong.
    let uninterrupted = page_to_completion(&first, &cursor);
    assert!(!uninterrupted.is_empty(), "the cursor resumed nothing");

    first.shutdown();
    let second = db.start_with(&["--max-rows", "50"]);

    let resumed = page_to_completion(&second, &cursor);
    assert_eq!(
        resumed, uninterrupted,
        "the same cursor named a different answer after a restart"
    );

    // The whole answer, as the sum of what the cursor handed back on each side. Paging
    // that agrees with itself but not with the unpaged query is agreement on a wrong
    // answer.
    let mut whole_by_paging = rows_as_text(&page);
    whole_by_paging.extend(resumed);
    assert_eq!(
        whole_by_paging,
        rows_as_text(&second.query("MATCH LOG {} LIMIT 100000")),
        "paging across a restart did not reconstruct the whole answer"
    );
}

/// Pages from `cursor` to the end, returning the rows in the order they were handed out.
fn page_to_completion(server: &Server, cursor: &str) -> Vec<String> {
    let mut rows = Vec::new();
    let mut cursor = Some(cursor.to_string());
    for _ in 0..40 {
        let Some(c) = cursor.take() else { break };
        let page = server.query(&format!(r#"MATCH LOG {{}} AFTER "{c}""#));
        rows.extend(rows_as_text(&page));
        cursor = page["meta"]["next_cursor"].as_str().map(str::to_string);
    }
    assert_eq!(cursor, None, "paging did not finish");
    rows
}

/// A cursor is still read against the query that issued it, restart or not.
///
/// The durability decision widens what a cursor may outlive; it does not widen what one
/// may be pointed at. Kept here because the guard is easy to lose exactly when the cursor
/// stops being session state - there is no session left to check it against.
#[test]
fn a_cursor_read_against_another_query_is_refused_after_a_restart() {
    let db = Db::new();

    let first = db.start_with(&["--max-rows", "50"]);
    first.ingest(&corpus());
    let cursor = first.query("MATCH LOG {}")["meta"]["next_cursor"]
        .as_str()
        .expect("a backstopped page carried no cursor")
        .to_string();
    first.shutdown();

    let second = db.start_with(&["--max-rows", "50"]);
    let (status, body) = second.query_raw(&format!(
        r#"MATCH LOG {{ "level": "error" }} AFTER "{cursor}""#
    ));
    assert_eq!(
        status, 400,
        "a cursor from another query was accepted after a restart: {body}"
    );
}

// ── Ids a lost snapshot leaves spent ───────────────────────────────────────────────

/// A relearned template must never be handed a template id that a leaf directory already
/// holds.
///
/// Ids come from a counter, and leaf directories are *named* by id, so a lost `trie.json`
/// restarts the counter over directories that still exist. Whether that collides depends
/// on the order lines arrive in: relearn the same key sets in a different order and
/// template *n* is now a different key set from the one whose rows sit in `leaf_n`.
///
/// The symptom is the reason this is a test rather than a comment. The two key sets merge
/// into one directory, so `MATCH LOG {}` still returns every row and looks entirely
/// correct, while a *filtered* query prunes to the template the trie names and misses the
/// rows in the directory it actually opened - a plausible wrong answer, with no error
/// anywhere. Both halves are asserted below, because only the second one ever failed.
#[test]
fn a_relearned_template_never_reuses_a_leaf_directory() {
    let db = Db::new();

    // Two shapes, ingested in one order and then the other. Hand-written rather than
    // generated: the whole mechanism is which key set is *seen first*, and that is
    // exactly what a seeded generator holds fixed.
    let alpha_then_beta: &[u8] = b"{\"ts\":\"2024-01-15T10:00:00Z\",\"alpha\":\"a1\"}\n\
                                   {\"ts\":\"2024-01-15T10:00:01Z\",\"beta\":42}\n";
    let beta_then_alpha: &[u8] = b"{\"ts\":\"2024-01-15T10:00:02Z\",\"beta\":43}\n\
                                   {\"ts\":\"2024-01-15T10:00:03Z\",\"alpha\":\"a2\"}\n";

    let first = db.start();
    assert_eq!(first.ingest(alpha_then_beta)["ingested"], 2);
    let before: Vec<u64> = template_ids(&first);
    assert_eq!(before.len(), 2, "the two shapes did not make two templates");
    first.shutdown();

    fs::remove_file(db.trie_json()).expect("no trie.json to remove");

    let second = db.start();
    assert_eq!(second.ingest(beta_then_alpha)["ingested"], 2);

    // Fresh ids. This is the fix: nothing relearned may land on a directory that the
    // templates from before the loss are still using.
    let after = template_ids(&second);
    assert_eq!(after.len(), 2);
    for id in &after {
        assert!(
            !before.contains(id),
            "relearned template {id} reused an id from before the snapshot was lost \
             (before {before:?}, after {after:?})"
        );
    }

    // The filtered query, which is where reuse used to go quiet: `a2` was ingested by
    // this server and must be found by a query that names its column.
    let hit = second.query(r#"MATCH LOG { "alpha": "a2" } LIMIT 100"#);
    assert_eq!(hit["count"], 1, "a filtered query missed a row it ingested");

    // And the unfiltered one agrees with it: two rows, this server's, with neither of the
    // orphaned leaves read back in.
    let all = second.query("MATCH LOG {} LIMIT 100");
    assert_eq!(all["count"], 2, "rows from the orphaned leaves leaked in");

    // Each row still carries its own key set. A directory holding two of them shows up
    // here as a row with the other shape's column on it, present and null.
    for row in all["rows"].as_array().expect("rows is an array") {
        let row = row.as_object().expect("a row is an object");
        assert!(
            row.contains_key("alpha") != row.contains_key("beta"),
            "a row carries both key sets, so two templates share a directory: {row:?}"
        );
    }
}

/// The template ids the server currently knows, ascending.
fn template_ids(server: &Server) -> Vec<u64> {
    let mut ids: Vec<u64> = json(&server.get("/templates").1)["templates"]
        .as_array()
        .expect("templates is an array")
        .iter()
        .map(|t| t["template_id"].as_u64().expect("a template has an id"))
        .collect();
    ids.sort_unstable();
    ids
}
