// The pieces that show values (components/data-display.md).

let text = React.string

// ── Info strip ──────────────────────────────────────────────────────────────────────

// ingest · templates · rejected, from /stats. A value /stats did not give is left out.
module InfoStrip = {
  @react.component
  let make = (~stats: option<Api.stats>, ~home=false) => {
    let ingest = stats->Option.flatMap(s => s.ingest)
    let pair = (label, value, ~danger=false) =>
      <span key=label className="info-pair">
        <span className="info-label type-caption"> {text(label)} </span>
        <span className={"info-value type-mono-md" ++ (danger ? " is-danger" : "")}>
          {text(value)}
        </span>
      </span>
    let pairs = [
      ingest->Option.flatMap(i => i.linesPerSecond)->Option.map(r => pair("ingest", Format.rate(r))),
      home
        ? None
        : stats->Option.flatMap(s => s.templates)->Option.map(t => pair("templates", Format.compact(t))),
      ingest
      ->Option.flatMap(i => i.linesRejected)
      ->Option.map(r => pair("rejected", Format.commas(r), ~danger=r > 0.0)),
    ]->Array.filterMap(x => x)
    <div className={"info-strip" ++ (home ? " is-home" : "")}> {React.array(pairs)} </div>
  }
}

// ── Recent and Saved ────────────────────────────────────────────────────────────────

module ListRow = {
  @react.component
  let make = (~query: string, ~aside: string, ~star=false) =>
    <a className="list-row" href={Route.toHash(Results(query))}>
      <span className="list-query type-mono"> {text((star ? "★ " : "") ++ query)} </span>
      <span className="list-aside type-caption"> {text(aside)} </span>
    </a>
}

// ── Field rows, meters, tiles, cards, pills (Status) ────────────────────────────────

module Meter = {
  @react.component
  let make = (~used: float, ~limit: float) => {
    let share = limit > 0.0 ? Math.min(1.0, Math.max(0.0, used /. limit)) : 0.0
    <span className="meter" ariaHidden=true>
      <span className="meter-fill" style={{width: Float.toString(share *. 100.0) ++ "%"}} />
    </span>
  }
}

type tone =
  | Plain
  | Success
  | Faint
  | Danger
  | Muted

let toneClass = tone =>
  switch tone {
  | Plain => ""
  | Success => " is-success"
  | Faint => " is-faint"
  | Danger => " is-danger"
  | Muted => " is-muted"
  }

module FieldRow = {
  // `field` names where the value comes from in /stats, as the row's tooltip.
  @react.component
  let make = (~label: string, ~field: string, ~tone=Plain, ~children: React.element) =>
    <div className="field-row" title=field>
      <span className="field-label type-label"> {text(label)} </span>
      <span className={"field-value type-mono" ++ toneClass(tone)}> children </span>
    </div>
}

// "3.1 MiB / 256 MiB" and its meter.
module Metered = {
  @react.component
  let make = (~used: string, ~limit: string, ~usedValue: float, ~limitValue: float) =>
    <span className="field-metered">
      <span> {text(used)} </span>
      <span className="field-of"> {text("/ " ++ limit)} </span>
      <Meter used=usedValue limit=limitValue />
    </span>
}

module Sparkline = {
  @react.component
  let make = (~points: array<float>) => {
    let n = Array.length(points)
    let top = points->Array.reduce(0.0, Math.max)
    let low = points->Array.reduce(top, Math.min)
    let spread = top -. low > 0.0 ? top -. low : 1.0
    let path =
      points
      ->Array.mapWithIndex((v, i) => {
        let x = n > 1 ? Int.toFloat(i) /. Int.toFloat(n - 1) *. 64.0 : 0.0
        let y = 17.0 -. (v -. low) /. spread *. 16.0
        `${i == 0 ? "M" : "L"}${Float.toFixed(x, ~digits=1)} ${Float.toFixed(y, ~digits=1)}`
      })
      ->Array.join(" ")
    <svg className="sparkline" viewBox="0 0 64 18" ariaHidden=true>
      <path d=path fill="none" stroke="currentColor" strokeWidth="1.6" />
    </svg>
  }
}

module StatTile = {
  @react.component
  let make = (
    ~label: string,
    ~value: string,
    ~caption: string,
    ~danger=false,
    ~spark: option<array<float>>=?,
  ) =>
    <div className="tile">
      <div className="tile-top">
        <span className="tile-label type-caption"> {text(label)} </span>
        {switch spark {
        | Some(points) => <Sparkline points />
        | None => React.null
        }}
      </div>
      <div className={"tile-value type-stat" ++ (danger ? " is-danger" : "")}> {text(value)} </div>
      <div className="tile-caption type-caption"> {text(caption)} </div>
    </div>
}

module Card = {
  @react.component
  let make = (~title: string, ~caption: string, ~children: React.element) =>
    <section className="card" ariaLabel=title>
      <div className="card-head">
        <h2 className="card-title type-panel-title"> {text(title)} </h2>
        <span className="card-caption type-caption"> {text(caption)} </span>
      </div>
      children
    </section>
}

module Pill = {
  @react.component
  let make = (~healthy: bool) =>
    <span className={"pill" ++ (healthy ? " is-healthy" : " is-degraded")}>
      <span className="pill-dot" />
      {text(healthy ? "Healthy" : "Degraded")}
    </span>
}

module CountPill = {
  @react.component
  let make = (~label: string) => <span className="pill is-count"> {text(label)} </span>
}

// ── Matches over time ───────────────────────────────────────────────────────────────

type levelBar = {info: float, warn: float, error: float}

// Lines that are not warn or error stack in the bottom layer, so every bar's height is
// its bucket's whole count.
let layerOf = (level: string) =>
  switch level->String.toLowerCase {
  | "warn" | "warning" => #warn
  | "error" => #error
  | _ => #info
  }

// The chart answer's groups as one bar per bucket, in time order.
let bars = (groups: array<Api.group>) => {
  let byBucket: dict<levelBar> = Dict.make()
  let order = []
  groups->Array.forEach(g =>
    switch g.bucket {
    | None => ()
    | Some(bucket) =>
      let current = switch byBucket->Dict.get(bucket) {
      | Some(bar) => bar
      | None =>
        order->Array.push(bucket)
        {info: 0.0, warn: 0.0, error: 0.0}
      }
      let level = g.keys->Dict.get("level")->Option.getOr("")
      let next = switch layerOf(level) {
      | #warn => {...current, warn: current.warn +. g.count}
      | #error => {...current, error: current.error +. g.count}
      | #info => {...current, info: current.info +. g.count}
      }
      byBucket->Dict.set(bucket, next)
    }
  )
  order
  ->Array.toSorted((a, b) => Date.fromString(a)->Date.getTime -. Date.fromString(b)->Date.getTime)
  ->Array.filterMap(bucket => byBucket->Dict.get(bucket))
}

module Chart = {
  @react.component
  let make = (~total: option<float>, ~groups: array<Api.group>) => {
    let bars = bars(groups)
    let n = Array.length(bars)
    let tallest = bars->Array.reduce(0.0, (top, b) => Math.max(top, b.info +. b.warn +. b.error))
    let slot = n > 0 ? 1000.0 /. Int.toFloat(n) : 0.0
    let width = slot *. 0.89
    let rect = (x, y, h, className, key) =>
      <rect
        key
        className
        x={Float.toFixed(x, ~digits=2)}
        y={Float.toFixed(y, ~digits=2)}
        width={Float.toFixed(width, ~digits=2)}
        height={Float.toFixed(h, ~digits=2)}
      />
    let scale = v => tallest > 0.0 ? v /. tallest *. 52.0 : 0.0
    <>
      <div className="chart-total">
        <span className="chart-count">
          {text(total->Option.mapOr("", t => Format.commas(t) ++ " lines"))}
        </span>
        <span className="chart-legend type-caption">
          <span> <span className="swatch-info"> {text("■")} </span> {text(" info")} </span>
          <span> <span className="swatch-warn"> {text("■")} </span> {text(" warn")} </span>
          <span> <span className="swatch-error"> {text("■")} </span> {text(" error")} </span>
        </span>
      </div>
      <svg className="chart" viewBox="0 0 1000 52" preserveAspectRatio="none" ariaHidden=true>
        {bars
        ->Array.mapWithIndex((b, i) => {
          let x = Int.toFloat(i) *. slot +. (slot -. width) /. 2.0
          let info = scale(b.info)
          let warn = scale(b.warn)
          let error = scale(b.error)
          let k = Int.toString(i)
          <g key=k>
            {rect(x, 52.0 -. info, info, "bar-info", "i")}
            {rect(x, 52.0 -. info -. warn, warn, "bar-warn", "w")}
            {rect(x, 52.0 -. info -. warn -. error, error, "bar-error", "e")}
          </g>
        })
        ->React.array}
      </svg>
    </>
  }
}

// ── Rows ────────────────────────────────────────────────────────────────────────────

// What the query asked for: each key with the literal value it named, if any.
type asked = array<(string, option<string>)>

let keyAsked = (asked: asked, path) => asked->Array.some(((k, _)) => k == path)

let valueAsked = (asked: asked, path, value: JSON.t) => {
  let literal = switch value {
  | JSON.String(s) => Some(s)
  | JSON.Number(n) => Some(Float.toString(n))
  | JSON.Boolean(b) => Some(b ? "true" : "false")
  | _ => None
  }
  switch literal {
  | Some(v) => asked->Array.some(((k, want)) => k == path && want == Some(v))
  | None => false
  }
}

let quoted = s => JSON.stringify(JSON.String(s))

// One ingested line as JSON, keys in the order the API gave them, each `"key":value`
// kept on one line and the row wrapping between them.
module RawRow = {
  let rec value = (v: JSON.t, path: string, asked: asked) =>
    switch v {
    | JSON.Object(dict) => object(dict, path ++ ".", asked)
    | JSON.Array(items) =>
      <>
        <span className="j-punct"> {text("[")} </span>
        {items
        ->Array.mapWithIndex((item, i) =>
          <React.Fragment key={Int.toString(i)}>
            {i > 0 ? <span className="j-punct"> {text(",")} </span> : React.null}
            {value(item, path, asked)}
          </React.Fragment>
        )
        ->React.array}
        <span className="j-punct"> {text("]")} </span>
      </>
    | JSON.String(s) =>
      <span className={valueAsked(asked, path, v) ? "j-match" : "j-string"}> {text(quoted(s))} </span>
    | other =>
      <span className={valueAsked(asked, path, v) ? "j-match" : "j-number"}>
        {text(JSON.stringify(other))}
      </span>
    }
  and object = (dict: dict<JSON.t>, prefix: string, asked: asked) => {
    let entries = dict->Dict.toArray
    <>
      <span className="j-punct"> {text("{")} </span>
      {entries
      ->Array.mapWithIndex(((key, v), i) => {
        let path = prefix ++ key
        let last = i == Array.length(entries) - 1
        <React.Fragment key>
          <span className="raw-pair">
            <span className={keyAsked(asked, path) ? "j-match" : "j-key"}> {text(quoted(key))} </span>
            <span className="j-punct"> {text(":")} </span>
            {value(v, path, asked)}
            {last ? React.null : <span className="j-punct"> {text(",")} </span>}
          </span>
          {last ? React.null : <wbr />}
        </React.Fragment>
      })
      ->React.array}
      <span className="j-punct"> {text("}")} </span>
    </>
  }

  @react.component
  let make = (~row: dict<JSON.t>, ~asked: asked) =>
    <div className="raw-row type-mono"> {object(row, "", asked)} </div>
}

// A row's fields flattened with dots, as the Columns view shows them.
let rec flatten = (dict: dict<JSON.t>, prefix: string, out: array<(string, JSON.t)>) =>
  dict
  ->Dict.toArray
  ->Array.forEach(((key, v)) =>
    switch v {
    | JSON.Object(inner) => flatten(inner, prefix ++ key ++ ".", out)
    | _ => out->Array.push((prefix ++ key, v))
    }
  )

let cellText = (v: JSON.t) =>
  switch v {
  | JSON.String(s) => s
  | JSON.Null => "null"
  | other => JSON.stringify(other)
  }

let levelClass = level =>
  switch level->String.toLowerCase {
  | "error" => "level-error"
  | "warn" | "warning" => "level-warn"
  | _ => "level-other"
  }

// The `ts` field as a clock time, or as it is when it does not read as a time.
let timeCell = (v: JSON.t) => Format.parseTime(v)->Option.mapOr(cellText(v), Format.clock)

// One column per field: ts, level, then the query's keys, then every other key in the
// order it first appears. Widths fit the longest value, in characters.
module ColumnsTable = {
  @react.component
  let make = (~rows: array<dict<JSON.t>>, ~asked: asked) => {
    let flat = rows->Array.map(row => {
      let out = []
      flatten(row, "", out)
      Dict.fromArray(out)
    })
    let seen = []
    let add = key =>
      if !(seen->Array.includes(key)) {
        seen->Array.push(key)
      }
    add("ts")
    add("level")
    asked->Array.forEach(((k, _)) => add(k))
    flat->Array.forEach(row => row->Dict.keysToArray->Array.forEach(add))
    let shown = (key, row) =>
      switch row->Dict.get(key) {
      | Some(v) => key == "ts" ? timeCell(v) : cellText(v)
      | None => ""
      }
    let maxWidth = 48
    let widths = seen->Array.map(key => {
      let longest =
        flat->Array.reduce(String.length(key), (w, row) => Math.Int.max(w, String.length(shown(key, row))))
      Math.Int.min(longest, maxWidth)
    })
    let template = widths->Array.map(w => Int.toString(w) ++ "ch")->Array.join(" ")
    <div className="cols type-mono" role="table" ariaLabel="Log rows as columns">
      <div className="cols-row is-head" role="row" style={{gridTemplateColumns: template}}>
        {seen
        ->Array.mapWithIndex((key, i) =>
          <span key className={"cols-cell" ++ (i == 0 ? " is-sticky" : "")} role="columnheader">
            <span className={"cols-key" ++ (keyAsked(asked, key) ? " is-match" : "")}>
              {text(key)}
            </span>
          </span>
        )
        ->React.array}
      </div>
      {flat
      ->Array.mapWithIndex((row, r) =>
        <div
          key={Int.toString(r)} className="cols-row" role="row" style={{gridTemplateColumns: template}}>
          {seen
          ->Array.mapWithIndex((key, i) => {
            let value = shown(key, row)
            let className = switch key {
            | "ts" => "cols-cell is-sticky is-faint"
            | "level" => "cols-cell " ++ levelClass(value)
            | _ => "cols-cell"
            }
            <span key className={i == 0 ? className ++ " is-sticky" : className} role="cell" title=value>
              {text(value)}
            </span>
          })
          ->React.array}
        </div>
      )
      ->React.array}
    </div>
  }
}

// ── Templates matched, Explain ──────────────────────────────────────────────────────

module MatchedRow = {
  @react.component
  let make = (~id: int, ~path: string, ~rows: option<float>, ~onClick) =>
    <button type_="button" className="matched-row" onClick>
      <span className="matched-id type-mono-sm"> {text(Int.toString(id))} </span>
      <span className="matched-path type-mono"> {text(path)} </span>
      <span className="matched-count"> {text(rows->Option.mapOr("", Format.compact))} </span>
    </button>
}

module Phase = {
  @react.component
  let make = (~number: int, ~title: string, ~rows: array<(string, string)>) =>
    <div className="phase">
      <div className="phase-head">
        <span className={"phase-badge type-badge is-" ++ Int.toString(number)}>
          {text(Int.toString(number))}
        </span>
        <span className="phase-title"> {text(title)} </span>
      </div>
      {rows
      ->Array.map(((label, value)) =>
        <div key=label className="phase-row">
          <span className="phase-label"> {text(label)} </span>
          <span className="phase-value"> {text(value)} </span>
        </div>
      )
      ->React.array}
    </div>
}

// ── Templates ───────────────────────────────────────────────────────────────────────

module TreeRow = {
  @react.component
  let make = (
    ~depth: int,
    ~label: string,
    ~count: string,
    ~leaf=false,
    ~open_=false,
    ~selected=false,
    ~onClick,
  ) =>
    <button
      type_="button"
      className={"tree-row" ++ (selected ? " is-selected" : "")}
      style={{paddingLeft: `calc(var(--space-5) + ${Int.toString(depth)} * var(--space-9))`}}
      ariaExpanded=?{leaf ? None : Some(open_)}
      ariaCurrent=?{selected ? Some(#"true") : None}
      onClick>
      <span className="tree-disclosure" ariaHidden=true>
        {text(leaf ? "" : open_ ? "▾" : "▸")}
      </span>
      <span className={"tree-label type-mono" ++ (leaf ? " is-leaf" : "")}> {text(label)} </span>
      <span className="tree-count"> {text(count)} </span>
    </button>
}

// 15:10:02.114 · ERROR · http.request · method=GET path=/v1/refund …
module LatestRow = {
  @react.component
  let make = (~row: dict<JSON.t>, ~timeKey: string) => {
    let out = []
    flatten(row, "", out)
    let time = row->Dict.get(timeKey)->Option.mapOr("", timeCell)
    let level = row->Dict.get("level")->Option.mapOr("", cellText)
    let event = row->Dict.get("event")->Option.mapOr("", cellText)
    let rest = out->Array.filter(((k, _)) => k != timeKey && k != "level" && k != "event")
    <div className="latest-row type-mono">
      <span className="latest-time"> {text(time)} </span>
      <span className={levelClass(level)}> {text(level->String.toUpperCase)} </span>
      <span className="latest-event"> {text(event)} </span>
      <span className="latest-fields">
        {rest
        ->Array.mapWithIndex(((k, v), i) =>
          <React.Fragment key=k>
            {text((i > 0 ? " " : "") ++ k ++ "=")}
            <span className="latest-value"> {text(cellText(v))} </span>
          </React.Fragment>
        )
        ->React.array}
      </span>
    </div>
  }
}

module Fact = {
  @react.component
  let make = (~label: string, ~value: string) =>
    <div className="fact">
      <span className="fact-label"> {text(label)} </span>
      <span className="fact-value type-stat-sans"> {text(value)} </span>
    </div>
}
