// How the server is doing, from what `/stats` and `/health` report
// (screens/status/spec.md). The page reads both when it opens and on Refresh.

let text = React.string

type snapshot = {
  health: option<Api.health>,
  stats: option<Api.stats>,
  at: float,
  error: option<string>,
}

// Lines ingested at one refresh, for the rate between refreshes.
type sample = {at: float, lines: float}

let join = parts => parts->Array.filterMap(x => x)->Array.join(" · ")
let commas = n => n->Option.map(Format.commas)
let bytes = n => n->Option.map(Format.bytes)
let duration = n => n->Option.map(Format.duration)
let arrow = (a, b, format) =>
  switch (a, b) {
  | (Some(a), Some(b)) => Some(format(a) ++ " → " ++ format(b))
  | _ => None
  }

module F = DataDisplay.FieldRow

// A value, or nothing when /stats did not give it.
let value = (v: option<string>) => text(v->Option.getOr(""))

let metered = (used, limit, format) =>
  switch (used, limit) {
  | (Some(u), Some(l)) =>
    <DataDisplay.Metered used={format(u)} limit={format(l)} usedValue=u limitValue=l />
  | (Some(u), None) => text(format(u))
  | _ => React.null
  }

@val @scope("window") external openTab: (string, string) => unit = "open"

@react.component
let make = (~server as _: Server.t) => {
  let (snapshot, setSnapshot) = React.useState(() => None)
  let (samples, setSamples) = React.useState(() => [])

  let refresh = () => {
    let _ = (
      async () => {
        let health = await Api.health()
        let stats = await Api.stats()
        let at = Browser.now()
        let error = switch (health, stats) {
        | (Error(e), _) | (_, Error(e)) => Some(e)
        | _ => None
        }
        setSnapshot(previous => Some({
          health: switch health {
          | Ok(h) => Some(h)
          | Error(_) => previous->Option.flatMap(p => p.health)
          },
          stats: switch stats {
          | Ok(s) => Some(s)
          | Error(_) => previous->Option.flatMap(p => p.stats)
          },
          at,
          error,
        }))
        switch stats {
        | Ok(s) =>
          switch s.ingest->Option.flatMap(i => i.linesIngested) {
          | Some(lines) => setSamples(list => list->Array.concat([{at, lines}]))
          | None => ()
          }
        | Error(_) => ()
        }
      }
    )()
  }
  React.useEffect0(() => {
    refresh()
    None
  })

  let stats = snapshot->Option.flatMap(s => s.stats)
  let storage = stats->Option.flatMap(s => s.storage)
  let ingest = stats->Option.flatMap(s => s.ingest)
  let queries = stats->Option.flatMap(s => s.queries)
  let wal = storage->Option.flatMap(s => s.wal)
  let durable = storage->Option.flatMap(s => s.durable)
  let compaction = storage->Option.flatMap(s => s.compaction)
  let lastDay = compaction->Option.flatMap(c => c.lastDay)
  let healthy = switch snapshot->Option.flatMap(s => s.health) {
  | Some({status: Some("degraded")}) => false
  | _ => true
  }

  // Lines a second between this page's refreshes; drawn once there are two of them.
  let rates =
    samples
    ->Array.mapWithIndex((s, i) =>
      i == 0
        ? None
        : {
            let before = samples->Array.getUnsafe(i - 1)
            let seconds = (s.at -. before.at) /. 1000.0
            seconds > 0.0 ? Some(Math.max(0.0, (s.lines -. before.lines) /. seconds)) : None
          }
    )
    ->Array.filterMap(x => x)

  let shed = ingest->Option.mapOr([], i => i.shed)->Array.filter(((_, n)) => n > 0.0)
  let rejected = ingest->Option.flatMap(i => i.linesRejected)
  let compactInterval = storage->Option.flatMap(s => s.compactIntervalMs)
  let running = compaction->Option.flatMap(c => c.running)
  let writeFailures = storage->Option.flatMap(s => s.writeFailures)
  let version = Browser.meta("kibbr-version")
  let source = Browser.meta("kibbr-source")

  <div className="status">
    <div className="status-head">
      <h1 className="type-page-title"> {text("Status")} </h1>
      {snapshot == None ? React.null : <DataDisplay.Pill healthy />}
      <span className="status-since type-mono-sm">
        {text(
          join([
            stats->Option.flatMap(s => s.uptimeSeconds)->Option.map(u => "up " ++ Format.uptime(u)),
            stats
            ->Option.flatMap(s => s.startedAt)
            ->Option.map(t => "since " ++ Format.isoMinuteUtc(Date.fromString(t)) ++ " UTC"),
          ]),
        )}
      </span>
      <span className="status-asof type-caption">
        {text(snapshot->Option.mapOr("", s => "as of " ++ Format.clockSeconds(Date.fromTime(s.at))))}
      </span>
      <Controls.Button className="is-small" icon=Icon.refreshSvg onClick={_ => refresh()}>
        {text("Refresh")}
      </Controls.Button>
      <Controls.Button className="is-small is-mono" onClick={_ => openTab("/stats", "_blank")}>
        {text("GET /stats")}
      </Controls.Button>
    </div>
    {switch snapshot->Option.flatMap(s => s.error) {
    | Some(e) => <div className="status-error type-caption" role="alert"> {text(e)} </div>
    | None => React.null
    }}
    <div className="tiles">
      <DataDisplay.StatTile
        label="Ingest rate"
        value={ingest->Option.flatMap(i => i.linesPerSecond)->Option.mapOr("", Format.rate)}
        caption="average since start"
        spark=?{Array.length(rates) >= 2 ? Some(rates) : None}
      />
      <DataDisplay.StatTile
        label="Lines ingested"
        value={ingest->Option.flatMap(i => i.linesIngested)->Option.mapOr("", Format.commas)}
        caption="since start"
      />
      <DataDisplay.StatTile
        label="Lines rejected"
        value={rejected->Option.mapOr("", Format.commas)}
        caption="since start"
        danger={rejected->Option.mapOr(false, r => r > 0.0)}
      />
      <DataDisplay.StatTile
        label="Requests shed"
        value={ingest->Option.flatMap(i => i.requestsShed)->Option.mapOr("", Format.commas)}
        caption={"by cause: " ++ (
          Array.length(shed) == 0
            ? "none"
            : shed->Array.map(((cause, n)) => cause ++ " " ++ Format.commas(n))->Array.join(" · ")
        )}
      />
      <DataDisplay.StatTile
        label="Templates"
        value={stats->Option.flatMap(s => s.templates)->Option.mapOr("", Format.commas)}
        caption={stats
        ->Option.flatMap(s => s.trieNodes)
        ->Option.mapOr("", n => Format.commas(n) ++ " trie nodes")}
      />
      <DataDisplay.StatTile
        label="On disk"
        value={storage->Option.flatMap(s => s.approxBytes)->Option.mapOr("", b => "≈ " ++ Format.bytes(b))}
        caption={`estimate · data and log`}
      />
    </div>
    <div className="status-cards">
      <div className="status-column">
        <DataDisplay.Card title="Live door" caption={`POST /ingest · NDJSON, gzip accepted`}>
          <F label={`Bodies inside · parsing · waiting`} field="ingest.bodies_inside · parsing · door.waiting">
            {value(
              Some(
                join([
                  commas(ingest->Option.flatMap(i => i.bodiesInside)),
                  commas(ingest->Option.flatMap(i => i.parsing)),
                  commas(ingest->Option.flatMap(i => i.doorWaiting)),
                ]),
              ),
            )}
          </F>
          <F label="Concurrency" field="ingest.ingest_concurrency">
            {value(ingest->Option.flatMap(i => i.ingestConcurrency)->Option.map(n => Format.commas(n) ++ " bodies"))}
          </F>
          <F label="Ingest wait" field="ingest.ingest_wait_ms">
            {value(duration(ingest->Option.flatMap(i => i.ingestWaitMs)))}
          </F>
          <F label="Waited at the door" field="ingest.door.waits · waited_ms">
            {value(
              Some(
                join([
                  ingest->Option.flatMap(i => i.doorWaits)->Option.map(n => Format.commas(n) ++ " bodies"),
                  duration(ingest->Option.flatMap(i => i.doorWaitedMs)),
                ]),
              ),
            )}
          </F>
          <F label="Rows held for a full row group" field="storage.buffered_rows">
            {value(commas(storage->Option.flatMap(s => s.bufferedRows)))}
          </F>
        </DataDisplay.Card>
        <DataDisplay.Card title="Write-ahead log" caption={`wal/ · flushed before /ingest answers`}>
          {switch (wal, durable) {
          | (None, Some(false)) =>
            <div className="card-none type-label"> {text("none: storage is in memory")} </div>
          | (None, _) => React.null
          | (Some(w), _) =>
            <>
              <F
                label="Healthy"
                field="storage.wal.healthy"
                tone={w.healthy == Some(false) ? Danger : Success}>
                {value(w.healthy->Option.map(h => h ? "yes" : "no"))}
              </F>
              <F label="Queue" field="storage.wal.queue_bytes / queue_max_bytes">
                {metered(w.queueBytes, w.queueMaxBytes, Format.bytes)}
              </F>
              <F label="Queue peak" field="storage.wal.queue_peak_bytes">
                {value(bytes(w.queuePeakBytes))}
              </F>
              <F label="Unapplied" field="storage.wal.unapplied_bytes">
                {value(bytes(w.unappliedBytes))}
              </F>
              <F label={`Last · checkpoint`} field="storage.wal.last_seq · checkpoint">
                {value(Some(join([commas(w.lastSeq), commas(w.checkpoint)])))}
              </F>
              <F label={`Segments · size`} field="storage.wal.segments · bytes">
                {value(Some(join([commas(w.segments), bytes(w.walBytes)])))}
              </F>
              <F label="Flushes" field="storage.wal.flushes · storage.flush_interval_ms">
                {value(
                  Some(
                    join([
                      commas(w.flushes),
                      duration(storage->Option.flatMap(s => s.flushIntervalMs))->Option.map(d => "every " ++ d),
                    ]),
                  ),
                )}
              </F>
            </>
          }}
        </DataDisplay.Card>
      </div>
      <div className="status-column">
        <DataDisplay.Card
          title="Storage"
          caption={join([
            storage->Option.flatMap(s => s.kind),
            durable->Option.map(d => d ? "durable" : "not durable"),
          ])}>
          <F
            label="Data directory"
            field="storage.base_path"
            tone={storage->Option.flatMap(s => s.basePath) == None ? Faint : Plain}>
            {value(Some(storage->Option.flatMap(s => s.basePath)->Option.getOr("none")))}
          </F>
          <F label="Rows stored" field="storage.rows">
            {value(commas(storage->Option.flatMap(s => s.rows)))}
          </F>
          <F label="Open day files" field="storage.open_segments / max_open_segments">
            {metered(
              storage->Option.flatMap(s => s.openSegments),
              storage->Option.flatMap(s => s.maxOpenSegments),
              Format.commas,
            )}
          </F>
          <F label="Buffered" field="storage.buffered_bytes / max_buffer_bytes">
            {metered(
              storage->Option.flatMap(s => s.bufferedBytes),
              storage->Option.flatMap(s => s.maxBufferBytes),
              Format.bytes,
            )}
          </F>
          <F label="Segment max" field="storage.segment_max_bytes">
            {value(bytes(storage->Option.flatMap(s => s.segmentMaxBytes)))}
          </F>
          <F
            label={`Forced closes · write failures`}
            field="storage.forced_closes · write_failures"
            tone={writeFailures->Option.mapOr(false, n => n > 0.0) ? Danger : Plain}>
            {value(Some(join([commas(storage->Option.flatMap(s => s.forcedCloses)), commas(writeFailures)])))}
          </F>
          <F
            label="Trie snapshot"
            field="storage.trie_file"
            tone={storage->Option.flatMap(s => s.trieFile) == None ? Faint : Plain}>
            {value(Some(storage->Option.flatMap(s => s.trieFile)->Option.getOr("none")))}
          </F>
        </DataDisplay.Card>
        <DataDisplay.Card title="Compaction" caption="a day is rebuilt the day after its last live row">
          <F
            label="State"
            field="storage.compaction.running · storage.compact_interval_ms"
            tone={running == Some(true) ? Plain : Success}>
            {value(
              switch running {
              | Some(true) => Some("running")
              | Some(false) =>
                Some(
                  switch compactInterval {
                  | Some(ms) if ms > 0.0 => "idle · looks every " ++ Format.duration(ms)
                  | _ => "idle"
                  },
                )
              | None => None
              },
            )}
          </F>
          <F label={`Runs · days rebuilt`} field="storage.compaction.runs · days_compacted">
            {value(
              Some(
                join([
                  commas(compaction->Option.flatMap(c => c.runs)),
                  commas(compaction->Option.flatMap(c => c.daysCompacted)),
                ]),
              ),
            )}
          </F>
          <F
            label="Last day"
            field="storage.compaction.last.days[].day · rows"
            tone={lastDay == None ? Faint : Plain}>
            {value(
              Some(
                switch lastDay {
                | Some(d) => join([d.day, d.dayRows->Option.map(r => Format.commas(r) ++ " rows")])
                | None => "none"
                },
              ),
            )}
          </F>
          {switch lastDay {
          | Some(d) =>
            <>
              <F label="Row groups" field="storage.compaction.last.days[].row_groups_in → row_groups_out">
                {value(arrow(d.rowGroupsIn, d.rowGroupsOut, Format.commas))}
              </F>
              <F
                label={`Files · bytes`}
                field="storage.compaction.last.days[].files_in → files_out · bytes_in → bytes_out">
                {value(
                  Some(
                    join([
                      arrow(d.filesIn, d.filesOut, Format.commas),
                      arrow(d.bytesIn, d.bytesOut, Format.bytes),
                    ]),
                  ),
                )}
              </F>
            </>
          | None => React.null
          }}
          {
            let lastError = compaction->Option.flatMap(c => c.lastError)
            <F
              label="Last error"
              field="storage.compaction.last_error"
              tone={lastError == None ? Faint : Danger}>
              {value(Some(lastError->Option.getOr("none")))}
            </F>
          }
        </DataDisplay.Card>
      </div>
      <div className="status-column">
        <DataDisplay.Card title="Queries" caption="since start">
          <F label={`Served · per second`} field="queries.served · per_second">
            {value(
              Some(
                join([
                  commas(queries->Option.flatMap(q => q.served)),
                  queries->Option.flatMap(q => q.perSecond)->Option.map(r => Float.toFixed(r, ~digits=2)),
                ]),
              ),
            )}
          </F>
          <F label="Stopped" field="queries.stopped">
            {value(commas(queries->Option.flatMap(q => q.stopped)))}
          </F>
          <F label="Requests timed out" field="ingest.requests_timed_out">
            {value(commas(ingest->Option.flatMap(i => i.requestsTimedOut)))}
          </F>
          <F label="Scan threads" field="queries.scan_threads">
            {value(commas(queries->Option.flatMap(q => q.scanThreads)))}
          </F>
          <F label="Shared-out scans" field="queries.scan_fan_outs">
            {value(commas(queries->Option.flatMap(q => q.scanFanOuts)))}
          </F>
        </DataDisplay.Card>
        <DataDisplay.Card title="Settings in effect" caption={`read-only · what /stats reports`}>
          <F label="Request timeout" field="ingest.request_timeout_ms">
            {value(duration(ingest->Option.flatMap(i => i.requestTimeoutMs)))}
          </F>
          <F label="Shutdown deadline" field="ingest.shutdown_deadline_ms">
            {value(duration(ingest->Option.flatMap(i => i.shutdownDeadlineMs)))}
          </F>
          <F label="Flush interval" field="storage.flush_interval_ms">
            {value(duration(storage->Option.flatMap(s => s.flushIntervalMs)))}
          </F>
          <F label="Row group timeout" field="storage.row_group_timeout_ms">
            {value(duration(storage->Option.flatMap(s => s.rowGroupTimeoutMs)))}
          </F>
          <F label="Compact interval" field="storage.compact_interval_ms">
            {value(duration(compactInterval))}
          </F>
          <div className="card-note type-caption">
            {text("Change with a flag, a KIBBR__ variable or --config, then restart.")}
          </div>
        </DataDisplay.Card>
      </div>
    </div>
    // AGPL §13: the source of this server, offered to whoever uses it over the network.
    <footer className="source-offer type-caption">
      {text("kibbr " ++ version->Option.getOr("") ++ " · ")}
      <a href=?source> {text("source")} </a>
    </footer>
  </div>
}
