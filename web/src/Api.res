// The four endpoints the console reads, typed. Every field is decoded on its own and is an
// `option`: a field this build does not know about is ignored, and one that is missing or
// of another type shows as absent rather than stopping the page.

// ── Reading JSON leniently ──────────────────────────────────────────────────────────

module Read = {
  let obj = (json: JSON.t) =>
    switch json {
    | JSON.Object(dict) => Some(dict)
    | _ => None
    }

  let field = (dict: dict<JSON.t>, name) => dict->Dict.get(name)

  let child = (dict, name) => dict->field(name)->Option.flatMap(obj)

  let float = (dict, name) =>
    switch dict->field(name) {
    | Some(JSON.Number(n)) => Some(n)
    | _ => None
    }

  let int = (dict, name) => dict->float(name)->Option.map(Float.toInt)

  let bool = (dict, name) =>
    switch dict->field(name) {
    | Some(JSON.Boolean(b)) => Some(b)
    | _ => None
    }

  let string = (dict, name) =>
    switch dict->field(name) {
    | Some(JSON.String(s)) => Some(s)
    | _ => None
    }

  let array = (dict, name) =>
    switch dict->field(name) {
    | Some(JSON.Array(items)) => items
    | _ => []
    }

  // A time stamp as a real date, or `None`: text that is not a date never reaches a page
  // as one, where it would show as "NaN" or sort out of place.
  let date = (dict, name) =>
    dict
    ->string(name)
    ->Option.flatMap(text => {
      let date = Date.fromString(text)
      Float.isNaN(date->Date.getTime) ? None : Some(date)
    })

  let strings = (dict, name) =>
    dict
    ->array(name)
    ->Array.filterMap(item =>
      switch item {
      | JSON.String(s) => Some(s)
      | _ => None
      }
    )
}

// ── GET /health ─────────────────────────────────────────────────────────────────────

type health = {
  // "ok" or "degraded".
  status: option<string>,
  detail: option<string>,
}

let decodeHealth = json =>
  switch Read.obj(json) {
  | Some(d) => {status: d->Read.string("status"), detail: d->Read.string("detail")}
  | None => {status: None, detail: None}
  }

// ── GET /stats ──────────────────────────────────────────────────────────────────────

type wal = {
  healthy: option<bool>,
  queueBytes: option<float>,
  queueMaxBytes: option<float>,
  queuePeakBytes: option<float>,
  unappliedBytes: option<float>,
  lastSeq: option<float>,
  checkpoint: option<float>,
  segments: option<float>,
  walBytes: option<float>,
  flushes: option<float>,
}

type compactedDay = {
  day: option<string>,
  dayRows: option<float>,
  filesIn: option<float>,
  filesOut: option<float>,
  rowGroupsIn: option<float>,
  rowGroupsOut: option<float>,
  bytesIn: option<float>,
  bytesOut: option<float>,
}

type compaction = {
  running: option<bool>,
  runs: option<float>,
  daysCompacted: option<float>,
  // The last day of the last run, when there was one.
  lastDay: option<compactedDay>,
  lastError: option<string>,
}

type storage = {
  kind: option<string>,
  durable: option<bool>,
  basePath: option<string>,
  trieFile: option<string>,
  flushIntervalMs: option<float>,
  rowGroupTimeoutMs: option<float>,
  // `None` when storage is in memory: the log is `null` then.
  wal: option<wal>,
  openSegments: option<float>,
  maxOpenSegments: option<float>,
  segmentMaxBytes: option<float>,
  rows: option<float>,
  bufferedRows: option<float>,
  bufferedBytes: option<float>,
  maxBufferBytes: option<float>,
  compactIntervalMs: option<float>,
  compaction: option<compaction>,
  forcedCloses: option<float>,
  writeFailures: option<float>,
  approxBytes: option<float>,
}

type ingest = {
  linesIngested: option<float>,
  linesRejected: option<float>,
  linesPerSecond: option<float>,
  requestsShed: option<float>,
  // Each cause and its count, in the order the server lists them.
  shed: array<(string, float)>,
  ingestWaitMs: option<float>,
  shutdownDeadlineMs: option<float>,
  requestTimeoutMs: option<float>,
  requestsTimedOut: option<float>,
  ingestConcurrency: option<float>,
  bodiesInside: option<float>,
  parsing: option<float>,
  doorWaiting: option<float>,
  doorWaits: option<float>,
  doorWaitedMs: option<float>,
}

type queries = {
  served: option<float>,
  stopped: option<float>,
  perSecond: option<float>,
  scanFanOuts: option<float>,
  scanThreads: option<float>,
}

type stats = {
  uptimeSeconds: option<float>,
  startedAt: option<Date.t>,
  storage: option<storage>,
  templates: option<float>,
  trieNodes: option<float>,
  ingest: option<ingest>,
  queries: option<queries>,
}

let decodeWal = (d: dict<JSON.t>) => {
  healthy: d->Read.bool("healthy"),
  queueBytes: d->Read.float("queue_bytes"),
  queueMaxBytes: d->Read.float("queue_max_bytes"),
  queuePeakBytes: d->Read.float("queue_peak_bytes"),
  unappliedBytes: d->Read.float("unapplied_bytes"),
  lastSeq: d->Read.float("last_seq"),
  checkpoint: d->Read.float("checkpoint"),
  segments: d->Read.float("segments"),
  walBytes: d->Read.float("bytes"),
  flushes: d->Read.float("flushes"),
}

let decodeDay = (d: dict<JSON.t>) => {
  day: d->Read.string("day"),
  dayRows: d->Read.float("rows"),
  filesIn: d->Read.float("files_in"),
  filesOut: d->Read.float("files_out"),
  rowGroupsIn: d->Read.float("row_groups_in"),
  rowGroupsOut: d->Read.float("row_groups_out"),
  bytesIn: d->Read.float("bytes_in"),
  bytesOut: d->Read.float("bytes_out"),
}

let decodeCompaction = (d: dict<JSON.t>) => {
  running: d->Read.bool("running"),
  runs: d->Read.float("runs"),
  daysCompacted: d->Read.float("days_compacted"),
  lastDay: d
  ->Read.child("last")
  ->Option.flatMap(last => last->Read.array("days")->Array.filterMap(Read.obj)->Array.at(-1))
  ->Option.map(decodeDay),
  lastError: d->Read.string("last_error"),
}

let decodeStorage = (d: dict<JSON.t>) => {
  kind: d->Read.string("kind"),
  durable: d->Read.bool("durable"),
  basePath: d->Read.string("base_path"),
  trieFile: d->Read.string("trie_file"),
  flushIntervalMs: d->Read.float("flush_interval_ms"),
  rowGroupTimeoutMs: d->Read.float("row_group_timeout_ms"),
  wal: d->Read.child("wal")->Option.map(decodeWal),
  openSegments: d->Read.float("open_segments"),
  maxOpenSegments: d->Read.float("max_open_segments"),
  segmentMaxBytes: d->Read.float("segment_max_bytes"),
  rows: d->Read.float("rows"),
  bufferedRows: d->Read.float("buffered_rows"),
  bufferedBytes: d->Read.float("buffered_bytes"),
  maxBufferBytes: d->Read.float("max_buffer_bytes"),
  compactIntervalMs: d->Read.float("compact_interval_ms"),
  compaction: d->Read.child("compaction")->Option.map(decodeCompaction),
  forcedCloses: d->Read.float("forced_closes"),
  writeFailures: d->Read.float("write_failures"),
  approxBytes: d->Read.float("approx_bytes"),
}

let decodeIngest = (d: dict<JSON.t>) => {
  let door = d->Read.child("door")
  {
    linesIngested: d->Read.float("lines_ingested"),
    linesRejected: d->Read.float("lines_rejected"),
    linesPerSecond: d->Read.float("lines_per_second"),
    requestsShed: d->Read.float("requests_shed"),
    shed: switch d->Read.child("shed") {
    | Some(causes) =>
      causes
      ->Dict.toArray
      ->Array.filterMap(((cause, count)) =>
        switch count {
        | JSON.Number(n) => Some((cause, n))
        | _ => None
        }
      )
    | None => []
    },
    ingestWaitMs: d->Read.float("ingest_wait_ms"),
    shutdownDeadlineMs: d->Read.float("shutdown_deadline_ms"),
    requestTimeoutMs: d->Read.float("request_timeout_ms"),
    requestsTimedOut: d->Read.float("requests_timed_out"),
    ingestConcurrency: d->Read.float("ingest_concurrency"),
    bodiesInside: d->Read.float("bodies_inside"),
    parsing: d->Read.float("parsing"),
    doorWaiting: door->Option.flatMap(door => door->Read.float("waiting")),
    doorWaits: door->Option.flatMap(door => door->Read.float("waits")),
    doorWaitedMs: door->Option.flatMap(door => door->Read.float("waited_ms")),
  }
}

let decodeQueries = (d: dict<JSON.t>) => {
  served: d->Read.float("served"),
  stopped: d->Read.float("stopped"),
  perSecond: d->Read.float("per_second"),
  scanFanOuts: d->Read.float("scan_fan_outs"),
  scanThreads: d->Read.float("scan_threads"),
}

let decodeStats = json => {
  let d = Read.obj(json)->Option.getOr(Dict.make())
  let trie = d->Read.child("trie")
  {
    uptimeSeconds: d->Read.float("uptime_seconds"),
    startedAt: d->Read.date("started_at"),
    storage: d->Read.child("storage")->Option.map(decodeStorage),
    templates: trie->Option.flatMap(t => t->Read.float("templates")),
    trieNodes: trie->Option.flatMap(t => t->Read.float("nodes")),
    ingest: d->Read.child("ingest")->Option.map(decodeIngest),
    queries: d->Read.child("queries")->Option.map(decodeQueries),
  }
}

// ── GET /templates ──────────────────────────────────────────────────────────────────

type column = {name: string, columnType: option<string>}

type template = {
  id: int,
  // The trie path: promoted `key=value` tokens first, then the other keys.
  path: array<string>,
  columns: array<column>,
  rows: float,
  firstSeen: option<Date.t>,
  lastSeen: option<Date.t>,
  // "column:ts" when the template's time comes from a column; anything else otherwise.
  timeSource: option<string>,
}

let decodeTemplate = json =>
  switch Read.obj(json) {
  | None => None
  | Some(d) =>
    d
    ->Read.int("template_id")
    ->Option.map(id => {
      id,
      path: d->Read.strings("path"),
      columns: d
      ->Read.array("columns")
      ->Array.filterMap(Read.obj)
      ->Array.filterMap(c =>
        c->Read.string("name")->Option.map(name => {name, columnType: c->Read.string("type")})
      ),
      rows: d->Read.float("rows")->Option.getOr(0.0),
      firstSeen: d->Read.date("first_seen"),
      lastSeen: d->Read.date("last_seen"),
      timeSource: d->Read.string("time_source"),
    })
  }

let decodeTemplates = json =>
  switch Read.obj(json) {
  | Some(d) => d->Read.array("templates")->Array.filterMap(decodeTemplate)
  | None => []
  }

// ── POST /query ─────────────────────────────────────────────────────────────────────

type group = {
  // `None` for a row that could not be placed in time, or a bucket that is not a date.
  bucket: option<Date.t>,
  // The group's column keys and their values, e.g. `level`.
  keys: dict<string>,
  count: float,
}

type meta = {
  templatesMatched: option<float>,
  templateIds: array<int>,
  filesOpened: option<float>,
  rowsScanned: option<float>,
  durationMs: option<float>,
  truncated: option<bool>,
  nextCursor: option<string>,
}

type answer = {
  exists: option<bool>,
  count: option<float>,
  // Each row is the object as it was ingested, keys in the order the API returned them.
  rows: array<dict<JSON.t>>,
  groups: array<group>,
  meta: meta,
}

let decodeGroup = (d: dict<JSON.t>) => {
  bucket: d->Read.date("bucket"),
  keys: switch d->Read.child("keys") {
  | Some(keys) =>
    keys
    ->Dict.toArray
    ->Array.map(((k, v)) => (
      k,
      switch v {
      | JSON.String(s) => s
      | JSON.Number(n) => Float.toString(n)
      | JSON.Boolean(b) => b ? "true" : "false"
      | _ => ""
      },
    ))
    ->Dict.fromArray
  | None => Dict.make()
  },
  count: d->Read.float("count")->Option.getOr(0.0),
}

let decodeAnswer = json => {
  let d = Read.obj(json)->Option.getOr(Dict.make())
  let m = d->Read.child("meta")->Option.getOr(Dict.make())
  {
    exists: d->Read.bool("exists"),
    count: d->Read.float("count"),
    rows: d->Read.array("rows")->Array.filterMap(Read.obj),
    groups: d->Read.array("groups")->Array.filterMap(Read.obj)->Array.map(decodeGroup),
    meta: {
      templatesMatched: m->Read.float("templates_matched"),
      templateIds: m
      ->Read.array("template_ids")
      ->Array.filterMap(id =>
        switch id {
        | JSON.Number(n) => Some(Float.toInt(n))
        | _ => None
        }
      ),
      filesOpened: m->Read.float("files_opened"),
      rowsScanned: m->Read.float("rows_scanned"),
      durationMs: m->Read.float("duration_ms"),
      truncated: m->Read.bool("truncated"),
      nextCursor: m->Read.string("next_cursor"),
    },
  }
}

// ── Requests ────────────────────────────────────────────────────────────────────────

type response
@val external fetch: (string, {..}) => promise<response> = "fetch"
@get external ok: response => bool = "ok"
@get external status: response => int = "status"
@send external json: response => promise<JSON.t> = "json"

// The body as JSON, or an error saying what came back instead.
let body = async (response: response) =>
  try Ok(await json(response)) catch {
  | _ => Error(`HTTP ${Int.toString(status(response))}`)
  }

// A failure the server described (`{"ok":false,"error":"..."}`), or the status.
let failure = (json: JSON.t, response) =>
  Error(
    Read.obj(json)
    ->Option.flatMap(d => d->Read.string("error"))
    ->Option.getOr(`HTTP ${Int.toString(status(response))}`),
  )

let unreachable = Error("the server did not answer")

// Each endpoint is called by its own literal path: tests/serve.rs asserts the page asks
// this server and nothing else, by finding these calls in it.

let health = async () =>
  switch await fetch("/health", {"cache": "no-store"}) {
  | response =>
    switch await body(response) {
    | Ok(json) => ok(response) ? Ok(decodeHealth(json)) : failure(json, response)
    | Error(e) => Error(e)
    }
  | exception _ => unreachable
  }

let stats = async () =>
  switch await fetch("/stats", {"cache": "no-store"}) {
  | response =>
    switch await body(response) {
    | Ok(json) => ok(response) ? Ok(decodeStats(json)) : failure(json, response)
    | Error(e) => Error(e)
    }
  | exception _ => unreachable
  }

let templates = async () =>
  switch await fetch("/templates", {"cache": "no-store"}) {
  | response =>
    switch await body(response) {
    | Ok(json) => ok(response) ? Ok(decodeTemplates(json)) : failure(json, response)
    | Error(e) => Error(e)
    }
  | exception _ => unreachable
  }

// A `400` is the parser's message, which the page shows as it is.
let query = async (q: string) =>
  switch await fetch(
    "/query",
    {
      "method": "POST",
      "headers": {"content-type": "application/json"},
      "body": JSON.stringify(JSON.Object(Dict.fromArray([("q", JSON.String(q))]))),
    },
  ) {
  | response =>
    switch await body(response) {
    | Ok(json) => ok(response) ? Ok(decodeAnswer(json)) : failure(json, response)
    | Error(e) => Error(e)
    }
  | exception _ => unreachable
  }
