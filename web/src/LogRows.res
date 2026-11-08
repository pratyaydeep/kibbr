// Log rows in the Raw view (components/log-rows.md): each row rebuilt as the nested object
// it was ingested as, on one line, wrapped, or opened as indented JSON, with the time and
// level in their own columns and the query's path highlighted.

let text = React.string

// ── The object ──────────────────────────────────────────────────────────────────────

// A flattened key's member names: split on unescaped dots only, `\.` being a literal dot
// and `\\` a literal backslash (the form kibbr-core's `KeyPath` renders).
let splitKey = (key: string) => {
  let segments = [""]
  let n = String.length(key)
  let append = s => {
    let last = Array.length(segments) - 1
    segments->Array.setUnsafe(last, segments->Array.getUnsafe(last) ++ s)
  }
  let rec go = i =>
    if i < n {
      switch key->String.charAt(i) {
      | "\\" if i + 1 < n && (key->String.charAt(i + 1) == "." || key->String.charAt(i + 1) == "\\") =>
        append(key->String.charAt(i + 1))
        go(i + 2)
      | "." =>
        segments->Array.push("")
        go(i + 1)
      | c =>
        append(c)
        go(i + 1)
      }
    }
  go(0)
  segments
}

// One member name as it appears in a flattened key.
let escapeSegment = (segment: string) =>
  segment->String.replaceAll("\\", "\\\\")->String.replaceAll(".", "\\.")

let joinPath = (prefix, segment) =>
  prefix == "" ? escapeSegment(segment) : prefix ++ "." ++ escapeSegment(segment)

// A member: its name, its full flattened path (the form the API and TrieQL use), and what
// it holds. Arrays arrive whole and stay leaves.
type rec node =
  | Leaf(JSON.t)
  | Branch(array<entry>)
and entry = {key: string, path: string, node: node}

let rec fromJson = (value: JSON.t, path: string) =>
  switch value {
  | JSON.Object(dict) =>
    Branch(
      dict
      ->Dict.toArray
      ->Array.map(((key, v)) => {
        let path = joinPath(path, key)
        {key, path, node: fromJson(v, path)}
      }),
    )
  | other => Leaf(other)
  }

// The row as a real nested object, members in the order the API returned them.
let nest = (row: dict<JSON.t>): array<entry> => {
  let root = []
  row
  ->Dict.toArray
  ->Array.forEach(((flat, value)) => {
    let segments = splitKey(flat)
    let last = Array.length(segments) - 1
    let rec place = (entries: array<entry>, i, prefix) => {
      let key = segments->Array.getUnsafe(i)
      let path = joinPath(prefix, key)
      if i == last {
        entries->Array.push({key, path, node: fromJson(value, path)})
      } else {
        switch entries->Array.find(e => e.key == key) {
        | Some({node: Branch(children)}) => place(children, i + 1, path)
        | Some(_) =>
          // A leaf already holds this name: keep the rest of the key flat beside it.
          entries->Array.push({
            key: segments->Array.slice(~start=i)->Array.join("."),
            path: flat,
            node: Leaf(value),
          })
        | None =>
          let children = []
          entries->Array.push({key, path, node: Branch(children)})
          place(children, i + 1, path)
        }
      }
    }
    place(root, 0, "")
  })
  root
}

let rec toJson = (entries: array<entry>) =>
  JSON.Object(
    entries
    ->Array.map(e => (
      e.key,
      switch e.node {
      | Leaf(v) => v
      | Branch(children) => toJson(children)
      },
    ))
    ->Dict.fromArray,
  )

// ── Time and level ──────────────────────────────────────────────────────────────────

// The field every matched template takes its time from, when they all agree on one
// column; otherwise there is no time to show (until the server sends each row's).
let timeField = (templates: array<Api.template>, ids: array<int>) => {
  let sources = ids->Array.map(id =>
    templates->Array.find(t => t.id == id)->Option.flatMap(t => t.timeSource)
  )
  switch sources->Array.get(0) {
  | Some(Some(first)) if first->String.startsWith("column:") && sources->Array.every(s => s == Some(first)) =>
    Some(first->String.slice(~start=String.length("column:")))
  | _ => None
  }
}

let rowTime = (row: dict<JSON.t>, field: option<string>) =>
  field->Option.flatMap(f => row->Dict.get(f))->Option.flatMap(Format.parseTime)

let scalarText = (v: JSON.t) =>
  switch v {
  | JSON.String(s) => Some(s)
  | JSON.Number(_) | JSON.Boolean(_) => Some(JSON.stringify(v))
  | JSON.Null => Some("null")
  | _ => None
  }

// The first of the top-level keys `level`, `severity`, `s` the row has.
let rowLevel = (row: dict<JSON.t>) =>
  ["level", "severity", "s"]->Array.reduce(None, (found, key) =>
    switch found {
    | Some(_) => found
    | None => row->Dict.get(key)->Option.flatMap(scalarText)
    }
  )

let levelTone = level =>
  switch level->String.toLowerCase {
  | "e" | "error" => " is-error"
  | "w" | "warn" | "warning" => " is-warn"
  | _ => ""
  }

let weekdays = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
let monthNames = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
]

// "Wednesday 22 March 2023", in the browser's time zone.
let dayLabel = (date: Date.t) =>
  `${weekdays->Array.getUnsafe(date->Date.getDay)} ${Int.toString(date->Date.getDate)} ${monthNames->Array.getUnsafe(
      date->Date.getMonth,
    )} ${Int.toString(date->Date.getFullYear)}`

let dayKey = (date: Date.t) =>
  `${Int.toString(date->Date.getFullYear)}-${Int.toString(date->Date.getMonth)}-${Int.toString(
      date->Date.getDate,
    )}`

// The browser's short time-zone name, e.g. IST.
let zone: string = %raw(`(() => {
  try {
    const part = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" })
      .formatToParts(new Date())
      .find((p) => p.type === "timeZoneName");
    return part ? part.value : "";
  } catch (_) {
    return "";
  }
})()`)

// ── Highlighting ────────────────────────────────────────────────────────────────────

// What the query asked for: each key with the literal value it named, if any.
type asked = array<(string, option<string>)>

let keyClass = (asked: asked, path: string) =>
  if asked->Array.some(((k, _)) => k == path) {
    "lj-kh"
  } else if asked->Array.some(((k, _)) => k->String.startsWith(path ++ ".")) {
    "lj-kp"
  } else {
    "lj-k"
  }

let valueAsked = (asked: asked, path: string, v: JSON.t) =>
  switch scalarText(v) {
  | Some(literal) => asked->Array.some(((k, want)) => k == path && want == Some(literal))
  | None => false
  }

let scalarClass = (v: JSON.t) =>
  switch v {
  | JSON.String(_) => "lj-s"
  | JSON.Number(_) => "lj-n"
  | _ => "lj-b"
  }

// ── Rendering ───────────────────────────────────────────────────────────────────────

let punct = s => <span className="lj-p"> {text(s)} </span>

let quoted = s => JSON.stringify(JSON.String(s))

// Inside an array: plain syntax colours, nothing highlighted or clickable.
let rec plain = (v: JSON.t) =>
  switch v {
  | JSON.Object(dict) =>
    let items = dict->Dict.toArray
    <>
      {punct("{")}
      {items
      ->Array.mapWithIndex(((k, item), i) =>
        <React.Fragment key={Int.toString(i)}>
          {i > 0 ? <> {punct(",")} <wbr /> </> : React.null}
          <span className="lj-k"> {text(quoted(k))} </span>
          {punct(":")}
          {plain(item)}
        </React.Fragment>
      )
      ->React.array}
      {punct("}")}
    </>
  | JSON.Array(items) =>
    <>
      {punct("[")}
      {items
      ->Array.mapWithIndex((item, i) =>
        <React.Fragment key={Int.toString(i)}>
          {i > 0 ? <> {punct(",")} <wbr /> </> : React.null}
          {plain(item)}
        </React.Fragment>
      )
      ->React.array}
      {punct("]")}
    </>
  | other => <span className={scalarClass(other)}> {text(JSON.stringify(other))} </span>
  }

type onAdd = (string, JSON.t) => unit

// A leaf's value: scalars coloured by kind (and highlighted when the query names them).
// Hovering a scalar shows it as a chip with a + that adds it to the query.
let leaf = (v: JSON.t, path: string, asked: asked, onAdd: onAdd) =>
  switch v {
  | JSON.Object(_) | JSON.Array(_) => plain(v)
  | scalar =>
    let shown = JSON.stringify(scalar)
    let label = `Add ${path} = ${scalarText(scalar)->Option.getOr(shown)} to the query`
    <span className="lj-val">
      <span className={valueAsked(asked, path, scalar) ? "lj-vh" : scalarClass(scalar)}>
        {text(shown)}
      </span>
      <button
        type_="button"
        className="lj-add"
        ariaLabel=label
        title=label
        tabIndex={-1}
        onClick={event => {
          ReactEvent.Mouse.stopPropagation(event)
          onAdd(path, scalar)
        }}>
        {text("+")}
      </button>
    </span>
  }

// The whole object on one line (which the row may wrap).
let rec inline = (entries: array<entry>, asked: asked, onAdd: onAdd) =>
  <>
    {punct("{")}
    {entries
    ->Array.mapWithIndex((e, i) =>
      <React.Fragment key={Int.toString(i)}>
        {i > 0 ? <> {punct(",")} <wbr /> </> : React.null}
        <span className={keyClass(asked, e.path)}> {text(quoted(e.key))} </span>
        {punct(":")}
        <wbr />
        {switch e.node {
        | Leaf(v) => leaf(v, e.path, asked, onAdd)
        | Branch(children) => inline(children, asked, onAdd)
        }}
      </React.Fragment>
    )
    ->React.array}
    {punct("}")}
  </>

// Pretty-printed: one member per line, nested objects indented a step per level.
let opened = (entries: array<entry>, asked: asked, onAdd: onAdd) => {
  let lines = []
  let line = (depth, content) =>
    lines->Array.push(
      <div
        key={Int.toString(Array.length(lines))}
        className="lj-line"
        style={{paddingLeft: `calc(${Int.toString(depth)} * var(--space-9))`}}>
        content
      </div>,
    )
  let rec members = (entries: array<entry>, depth) =>
    entries->Array.forEachWithIndex((e, i) => {
      let comma = i < Array.length(entries) - 1 ? punct(",") : React.null
      let key = <> <span className={keyClass(asked, e.path)}> {text(quoted(e.key))} </span> {punct(": ")} </>
      switch e.node {
      | Branch([]) => line(depth, <> key {punct("{}")} comma </>)
      | Branch(children) =>
        line(depth, <> key {punct("{")} </>)
        members(children, depth + 1)
        line(depth, <> {punct("}")} comma </>)
      | Leaf(v) => line(depth, <> key {leaf(v, e.path, asked, onAdd)} comma </>)
      }
    })
  line(0, punct("{"))
  members(entries, 1)
  line(0, punct("}"))
  React.array(lines)
}

// ── A row ───────────────────────────────────────────────────────────────────────────

type shown = {
  id: int,
  row: dict<JSON.t>,
  entries: array<entry>,
  time: option<Date.t>,
  level: option<string>,
}

// `sent` is the time the server ranked the row by, when it says; otherwise the interim
// rule reads the matched templates' shared time field.
let shownOf = (row: dict<JSON.t>, id: int, ~sent: option<option<Date.t>>, ~field: option<string>) => {
  id,
  row,
  entries: nest(row),
  time: switch sent {
  | Some(time) => time
  | None => rowTime(row, field)
  },
  level: rowLevel(row),
}

type rowAction =
  | Wrap
  | Open
  | Close
  | Select

module RowButton = {
  @react.component
  let make = (~label: string, ~svg: string, ~pressed: option<bool>=?, ~className="", ~onClick) =>
    <button
      type_="button"
      className={"row-button " ++ className}
      ariaLabel=label
      title=label
      ariaPressed=?{pressed->Option.map(p => p ? #"true" : #"false")}
      tabIndex={-1}
      onClick={event => {
        ReactEvent.Mouse.stopPropagation(event)
        onClick()
      }}>
      <Icon svg />
    </button>
}

module Row = {
  @react.component
  let make = (
    ~shown: shown,
    ~asked: asked,
    ~wrapped: bool,
    ~pinned: bool,
    ~isOpen: bool,
    ~selected: bool,
    ~onAction: (int, rowAction) => unit,
    ~onAdd: onAdd,
  ) => {
    let classes =
      [
        "logrow type-log",
        isOpen ? "is-open" : "",
        pinned ? "is-pinned" : "",
        selected ? "is-selected" : "",
      ]
      ->Array.filter(c => c != "")
      ->Array.join(" ")
    <div className=classes onClick={_ => onAction(shown.id, Select)}>
      <span className="logrow-time">
        {switch shown.time {
        | Some(date) =>
          <>
            {text(Format.clockSeconds(date))}
            <span className="logrow-ms"> {text("." ++ Format.pad(date->Date.getMilliseconds, 3))} </span>
          </>
        | None => <span className="logrow-ms"> {text(`—`)} </span>
        }}
      </span>
      <span
        className={"logrow-level" ++ shown.level->Option.mapOr("", levelTone)}
        title=?{shown.level}>
        {text(shown.level->Option.getOr(""))}
      </span>
      <div
        className={"logrow-json " ++ (isOpen ? "is-open" : wrapped ? "is-wrapped" : "is-line")}>
        {isOpen ? opened(shown.entries, asked, onAdd) : inline(shown.entries, asked, onAdd)}
      </div>
      <span className="logrow-actions">
        {isOpen
          ? <RowButton
              label="Close"
              svg=Icon.chevronDownSvg
              className="is-close"
              onClick={() => onAction(shown.id, Close)}
            />
          : <>
              <RowButton
                label={wrapped ? "Show on one line" : "Wrap this line"}
                svg=Icon.wrapLinesSvg
                pressed=wrapped
                onClick={() => onAction(shown.id, Wrap)}
              />
              <RowButton
                label="Open as JSON" svg=Icon.openJsonSvg onClick={() => onAction(shown.id, Open)}
              />
            </>}
      </span>
    </div>
  }
  let make = React.memo(make)
}

// ── The list ────────────────────────────────────────────────────────────────────────

// Under the panel header: the columns' names and the highlight's legend.
module Head = {
  @react.component
  let make = () =>
    <div className="logrows-head type-section-label" ariaHidden=true>
      <span> {text("Time" ++ (zone == "" ? "" : ` · ${zone}`))} </span>
      <span> {text("Level")} </span>
      <span> {text("JSON, as ingested")} </span>
      <span className="logrows-legend">
        <span className="logrows-swatch" />
        {text("query")}
      </span>
    </div>
}

// The rows in the order shown, with a date divider above the first and wherever the day
// changes.
module Rows = {
  @react.component
  let make = (
    ~shown: array<shown>,
    ~asked: asked,
    ~wrapAll: bool,
    ~overrides: Map.t<int, bool>,
    ~opened: Set.t<int>,
    ~selected: option<int>,
    ~onAction: (int, rowAction) => unit,
    ~onAdd: onAdd,
  ) => {
    let lastDay = ref(None)
    shown
    ->Array.map(s => {
      let divider = switch s.time {
      | Some(date) if lastDay.contents != Some(dayKey(date)) =>
        lastDay := Some(dayKey(date))
        <div className="logrows-day"> {text(dayLabel(date))} </div>
      | _ => React.null
      }
      let override = overrides->Map.get(s.id)
      <React.Fragment key={Int.toString(s.id)}>
        divider
        <Row
          shown=s
          asked
          wrapped={override->Option.getOr(wrapAll)}
          pinned={override == Some(true)}
          isOpen={opened->Set.has(s.id)}
          selected={selected == Some(s.id)}
          onAction
          onAdd
        />
      </React.Fragment>
    })
    ->React.array
  }
}

// ── Panel header controls ───────────────────────────────────────────────────────────

module SortButton = {
  @react.component
  let make = (~oldest: bool, ~onFlip) =>
    <button
      type_="button"
      className="head-button"
      ariaLabel="Sort by time"
      title="Sort by time"
      onClick={_ => onFlip()}>
      {text("Time")}
      <span className="head-button-value">
        {text(oldest ? `Oldest first ↑` : `Newest first ↓`)}
      </span>
    </button>
}

module WrapSwitch = {
  @react.component
  let make = (~on: bool, ~onChange) =>
    <button
      type_="button"
      className="head-button"
      role="switch"
      ariaChecked={on ? #"true" : #"false"}
      onClick={_ => onChange(!on)}>
      <span className="switch" ariaHidden=true> <span className="switch-knob" /> </span>
      {text("Wrap lines")}
    </button>
}
