// The strip across the top when the server is not plainly healthy
// (components/server-banner.md). It cannot be dismissed; it goes when the state clears.

@react.component
let make = (~server: Server.t) =>
  switch Server.state(server) {
  | Healthy => React.null
  | Degraded(detail) =>
    <div className="banner is-degraded type-label" role="status">
      <Icon svg=Icon.warningSvg />
      <span className="banner-message">
        <span className="banner-lead"> {React.string("Degraded: the store cannot write.")} </span>
        {React.string(
          " Lines are safe in the write-ahead log and /ingest still answers; they are written when it recovers.",
        )}
      </span>
      <span className="banner-detail type-mono-sm"> {React.string("/health: " ++ detail)} </span>
    </div>
  | InMemory =>
    <div className="banner is-memory type-label" role="status">
      <Icon svg=Icon.warningSvg />
      <span className="banner-message">
        <span className="banner-lead"> {React.string("Storage is in memory.")} </span>
        {React.string(
          " Everything ingested is lost when this process exits. A restart starts an empty database.",
        )}
      </span>
      <span className="banner-detail type-mono-sm">
        {React.string("started with --storage memory")}
      </span>
    </div>
  }
