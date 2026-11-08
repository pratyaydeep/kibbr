// The tools that make Logs results quicker to use (components/query-tools.md): the Save
// star, the Time range menu, the toast, and the keyboard hint strip and list.

let text = React.string

@send external contains: (Dom.element, {..}) => bool = "contains"
@send external focus: Dom.element => unit = "focus"
@send external select: Dom.element => unit = "select"

// A menu closes on a click outside `ref`. (Esc is the page's, which knows what is open.)
let useOutside = (ref: React.ref<Nullable.t<Dom.element>>, ~open_: bool, ~onClose) =>
  React.useEffect1(() =>
    if open_ {
      let onDown = event =>
        switch ref.current->Nullable.toOption {
        | Some(el) if !(el->contains(Browser.eventTarget(event))) => onClose()
        | _ => ()
        }
      Browser.addDocumentListener("mousedown", onDown)
      Some(() => Browser.removeDocumentListener("mousedown", onDown))
    } else {
      None
    }
  , [open_])

// ── Time range ──────────────────────────────────────────────────────────────────────

let quick = [
  ("Last 5 min", "5m"),
  ("Last 15 min", "15m"),
  ("Last 1 hour", "1h"),
  ("Last 6 hours", "6h"),
  ("Last 24 hours", "24h"),
  ("Last 7 days", "7d"),
]

// "Last 15 min" for `SINCE 15m`, and so on for any duration.
let sinceLabel = (d: string) =>
  switch quick->Array.find(((_, q)) => q == d) {
  | Some((label, _)) => label
  | None =>
    let n = d->String.slice(~start=0, ~end=-1)
    let many = n != "1"
    let unit = switch d->String.slice(~start=-1) {
    | "s" => "sec"
    | "m" => "min"
    | "h" => many ? "hours" : "hour"
    | "d" => many ? "days" : "day"
    | other => other
    }
    `Last ${n} ${unit}`
  }

// The button's label: the quick range, the BETWEEN range in local time, or "Any time".
let rangeLabel = (parsed: option<Trieql.parsed>) =>
  switch parsed->Option.flatMap(p => p.time) {
  | Some(Between(from, to_)) =>
    `${Format.clockSeconds(Date.fromTime(from))} → ${Format.clockSeconds(Date.fromTime(to_))}`
  | Some(Since(_)) => parsed->Option.flatMap(Trieql.sinceText)->Option.mapOr("Any time", sinceLabel)
  | None => "Any time"
  }

// 2023-03-22 14:21:30, in the browser's time zone; and back.
let localText = (ms: float) => {
  let d = Date.fromTime(ms)
  `${Int.toString(d->Date.getFullYear)}-${Format.pad(d->Date.getMonth + 1, 2)}-${Format.pad(
      d->Date.getDate,
      2,
    )} ${Format.clockSeconds(d)}`
}

let parseLocal = (s: string) => {
  let num = s => Int.fromString(s)
  switch s->String.trim->String.replace("T", " ")->String.split(" ")->Array.filter(p => p != "") {
  | [day, time] =>
    let d = day->String.split("-")->Array.map(num)
    let t = time->String.split(":")->Array.map(num)
    let t = Array.length(t) == 2 ? t->Array.concat([Some(0)]) : t
    switch (d, t) {
    | ([Some(year), Some(month), Some(date)], [Some(hours), Some(minutes), Some(seconds)]) =>
      let ms =
        Date.makeWithYMDHMS(~year, ~month=month - 1, ~day=date, ~hours, ~minutes, ~seconds)->Date.getTime
      Float.isNaN(ms) ? None : Some(ms)
    | _ => None
    }
  | _ => None
  }
}

module TimeMenu = {
  @react.component
  let make = (~parsed: option<Trieql.parsed>, ~onClause: string => unit) => {
    let current = parsed->Option.flatMap(Trieql.sinceText)
    // The fields start at the query's own range, or the last fifteen minutes.
    let (initialFrom, initialTo) = switch parsed->Option.flatMap(p => p.time) {
    | Some(Between(from, to_)) => (from, to_)
    | Some(Since(s)) => (Browser.now() -. s *. 1000.0, Browser.now())
    | None => (Browser.now() -. 900_000.0, Browser.now())
    }
    let (from, setFrom) = React.useState(() => localText(initialFrom))
    let (to_, setTo) = React.useState(() => localText(initialTo))
    let range = switch (parseLocal(from), parseLocal(to_)) {
    | (Some(f), Some(t)) if f < t => Some((f, t))
    | _ => None
    }
    <div className="menu time-menu" role="dialog" ariaLabel="Time range">
      <div className="menu-heading"> {text("Quick")} </div>
      {quick
      ->Array.map(((label, d)) =>
        <button
          key=d
          type_="button"
          className={"menu-item" ++ (current == Some(d) ? " is-current" : "")}
          onClick={_ => onClause("SINCE " ++ d)}>
          <span> {text(label)} </span>
          <span className="menu-item-aside"> {text("SINCE " ++ d)} </span>
        </button>
      )
      ->React.array}
      <div className="menu-rule" />
      <div className="menu-heading"> {text("Between")} </div>
      <label className="menu-field">
        <span className="menu-label"> {text("From")} </span>
        <input
          className="menu-input is-mono"
          value=from
          spellCheck=false
          onChange={e => setFrom(_ => Controls.inputValue(e))}
        />
      </label>
      <label className="menu-field">
        <span className="menu-label"> {text("To")} </span>
        <input
          className="menu-input is-mono"
          value=to_
          spellCheck=false
          onChange={e => setTo(_ => Controls.inputValue(e))}
        />
      </label>
      <button
        type_="button"
        className="button is-primary menu-apply"
        disabled={range == None}
        onClick={_ =>
          switch range {
          | Some((f, t)) => onClause(Trieql.betweenClause(f, t))
          | None => ()
          }}>
        {text("Apply range")}
      </button>
      <div className="menu-note">
        {text(`Times in ${LogRows.zone}. Drag across the chart to pick a range too.`)}
      </div>
    </div>
  }
}

module TimeButton = {
  @react.component
  let make = (~parsed: option<Trieql.parsed>, ~open_: bool, ~onToggle, ~onClose, ~onClause) => {
    let ref = React.useRef(Nullable.null)
    useOutside(ref, ~open_, ~onClose)
    <div className="tool" ref={ReactDOM.Ref.domRef(ref)}>
      <button
        type_="button"
        className="button type-button tool-time"
        ariaExpanded=open_
        ariaHaspopup=#dialog
        onClick={_ => onToggle()}>
        <Icon svg=Icon.clockSvg className="tool-clock" />
        {text(rangeLabel(parsed))}
        <Icon svg=Icon.chevronDownSvg className="tool-chevron" />
      </button>
      {open_ ? <TimeMenu parsed onClause /> : React.null}
    </div>
  }
}

// ── Save ────────────────────────────────────────────────────────────────────────────

module SavePopover = {
  @react.component
  let make = (~initial: string, ~onCancel, ~onSave: string => unit) => {
    let (name, setName) = React.useState(() => initial)
    let input = React.useRef(Nullable.null)
    React.useEffect0(() => {
      input.current->Nullable.forEach(el => {
        el->focus
        el->select
      })
      None
    })
    let save = () => name->String.trim == "" ? () : onSave(name->String.trim)
    <div className="menu save-menu" role="dialog" ariaLabel="Save this query">
      <div className="menu-title"> {text("Save this query")} </div>
      <label className="menu-field">
        <span className="menu-label"> {text("Name")} </span>
        <input
          ref={ReactDOM.Ref.domRef(input)}
          className="menu-input is-tall"
          value=name
          spellCheck=false
          onChange={e => setName(_ => Controls.inputValue(e))}
          onKeyDown={e =>
            if ReactEvent.Keyboard.key(e) == "Enter" {
              ReactEvent.Keyboard.preventDefault(e)
              save()
            }}
        />
      </label>
      <div className="menu-note"> {text("It appears under Saved on Logs home.")} </div>
      <div className="menu-actions">
        <button type_="button" className="button type-button menu-button" onClick={_ => onCancel()}>
          {text("Cancel")}
        </button>
        <button
          type_="button" className="button type-button is-primary menu-button" onClick={_ => save()}>
          {text("Save")}
        </button>
      </div>
    </div>
  }
}

module SaveStar = {
  @react.component
  let make = (~q: string, ~savedName: option<string>, ~open_: bool, ~onToggle, ~onClose, ~onSave) => {
    let ref = React.useRef(Nullable.null)
    useOutside(ref, ~open_, ~onClose)
    let label = savedName->Option.mapOr("Save this query", n => `Saved as "${n}"`)
    <div className="tool" ref={ReactDOM.Ref.domRef(ref)}>
      <button
        type_="button"
        className={"tool-star" ++ (savedName == None ? "" : " is-saved")}
        ariaLabel=label
        title=label
        ariaExpanded=open_
        ariaHaspopup=#dialog
        onClick={_ => onToggle()}>
        <Icon svg=Icon.starSvg />
      </button>
      {open_
        ? <SavePopover
            initial={savedName->Option.getOr(q->String.slice(~start=0, ~end=40))} onCancel=onClose onSave
          />
        : React.null}
    </div>
  }
}

// ── Toast ───────────────────────────────────────────────────────────────────────────

type toast = {id: int, message: string, undo: option<unit => unit>}

module Toast = {
  @react.component
  let make = (~toast: toast, ~onDone) => {
    React.useEffect1(() => {
      let timer = Browser.setTimeout(() => onDone(), 4000)
      Some(() => Browser.clearTimeout(timer))
    }, [toast.id])
    <div className="toast" role="status">
      <span> {text(toast.message)} </span>
      {switch toast.undo {
      | Some(undo) =>
        <button
          type_="button"
          className="toast-undo"
          onClick={_ => {
            undo()
            onDone()
          }}>
          {text("Undo")}
        </button>
      | None => React.null
      }}
    </div>
  }
}

// ── Keyboard ────────────────────────────────────────────────────────────────────────

module Key = {
  @react.component
  let make = (~label: string) => <kbd className="key"> {text(label)} </kbd>
}

module HintStrip = {
  @react.component
  let make = (~onShortcuts) => {
    let hint = (keys, word) =>
      <span key=word className="hint">
        {keys->Array.map(k => <Key key=k label=k />)->React.array}
        <span> {text(word)} </span>
      </span>
    <div className="hints">
      {hint(["j", "k"], "move")}
      {hint([`↵`], "open")}
      {hint(["w"], "wrap")}
      {hint(["c"], "copy")}
      {hint(["/"], "query")}
      {hint(["?"], "all keys")}
      <button type_="button" className="hints-all" onClick={_ => onShortcuts()}>
        {text("Keyboard shortcuts")}
      </button>
    </div>
  }
}

let keyRows = [
  ("Next row", ["j"]),
  ("Previous row", ["k"]),
  ("Open or close the row as JSON", [`↵`]),
  ("Wrap or unwrap the row", ["w"]),
  ("Copy the row as JSON", ["c"]),
  ("Go to the query", ["/"]),
  ("Run the query", [Browser.isMac ? `⌘` : "Ctrl", `↵`]),
  ("Close", ["Esc"]),
  ("Show these keys", ["?"]),
]

module KeyboardList = {
  @react.component
  let make = (~onClose) => {
    let ref = React.useRef(Nullable.null)
    useOutside(ref, ~open_=true, ~onClose)
    <div className="menu keys-menu" role="dialog" ariaLabel="Keyboard" ref={ReactDOM.Ref.domRef(ref)}>
      <div className="menu-title"> {text("Keyboard")} </div>
      {keyRows
      ->Array.map(((label, keys)) =>
        <div key=label className="keys-row">
          <span> {text(label)} </span>
          <span className="keys-chips"> {keys->Array.map(k => <Key key=k label=k />)->React.array} </span>
        </div>
      )
      ->React.array}
      <div className="menu-note">
        {text("Row keys work when the rows have focus: click a row or press Esc in the query.")}
      </div>
    </div>
  }
}
