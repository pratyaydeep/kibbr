// Buttons, the Raw/Columns toggle, the search box, the query bar, chips and the filter
// field (components/controls.md).

@get external scrollLeft: Dom.element => float = "scrollLeft"
@set external setScrollLeft: (Dom.element, float) => unit = "scrollLeft"

let inputValue = event => ReactEvent.Form.target(event)["value"]

module Button = {
  @react.component
  let make = (
    ~primary=false,
    ~className="",
    ~icon: option<string>=?,
    ~title: option<string>=?,
    ~onClick,
    ~children: React.element,
  ) =>
    <button
      type_="button"
      className={"button type-button" ++ (primary ? " is-primary" : "") ++ " " ++ className}
      ?title
      onClick>
      {switch icon {
      | Some(svg) => <Icon svg />
      | None => React.null
      }}
      children
    </button>
}

// "Run ⌘↵", or "Run Ctrl↵" away from Apple machines.
module Run = {
  @react.component
  let make = (~onRun) =>
    <Button primary=true onClick={_ => onRun()}>
      {React.string("Run " ++ Browser.runShortcut)}
    </Button>
}

type view =
  | Raw
  | Columns

// Raw / Columns, for Log rows.
module RowsToggle = {
  @react.component
  let make = (~view: view, ~onView: view => unit) => {
    let segment = (label, value, tooltip) =>
      <button
        type_="button"
        className="segment"
        title=tooltip
        ariaPressed={view == value ? #"true" : #"false"}
        onClick={_ => onView(value)}>
        {React.string(label)}
      </button>
    <div className="segmented" role="group" ariaLabel="Show rows as">
      {segment(
        "Raw",
        Raw,
        "Each line as it was ingested, with the query's keys and values highlighted",
      )}
      {segment("Columns", Columns, "Each field in its own column")}
    </div>
  }
}

let isRunKey = event =>
  ReactEvent.Keyboard.key(event) == "Enter" &&
    (ReactEvent.Keyboard.metaKey(event) || ReactEvent.Keyboard.ctrlKey(event) ||
    !ReactEvent.Keyboard.shiftKey(event))

// The search box on Logs home: one line of TrieQL, run with Enter or ⌘↵ / Ctrl↵.
module SearchBox = {
  @react.component
  let make = (~value, ~onChange, ~onRun) =>
    <div className="search">
      <span className="search-prompt type-query-home" ariaHidden=true> {React.string("›")} </span>
      <label className="search-field">
        <span className="visually-hidden"> {React.string("TrieQL query")} </span>
        <input
          className="search-input type-query-home"
          value
          autoFocus=true
          spellCheck=false
          autoComplete="off"
          placeholder={`MATCH LOG {"event": "…"} SINCE 15m`}
          onChange={event => onChange(inputValue(event))}
          onKeyDown={event =>
            if isRunKey(event) {
              ReactEvent.Keyboard.preventDefault(event)
              onRun()
            }}
        />
      </label>
      <Run onRun />
    </div>
}

// The query's coloured runs cut further where a marked range starts or ends, each piece
// keeping its colour and taking the classes of the ranges over it.
let paintRuns = (value: string, marks: array<(int, int, string)>) => {
  let out = []
  let at = ref(0)
  Trieql.colourise(value)->Array.forEach(segment => {
    let start = at.contents
    let end_ = start + String.length(segment.text)
    let cuts =
      marks
      ->Array.flatMap(((a, b, _)) => [a, b])
      ->Array.filter(c => c > start && c < end_)
      ->Array.toSorted((x, y) => Int.toFloat(x - y))
    let bounds = [start]->Array.concat(cuts)->Array.concat([end_])
    bounds->Array.forEachWithIndex((from, i) =>
      switch bounds->Array.get(i + 1) {
      | Some(to_) if to_ > from =>
        let classes =
          [
            switch segment.colour {
            | Keyword => "tok-keyword"
            | Value => "tok-value"
            | Plain => ""
            },
          ]
          ->Array.concat(marks->Array.filterMap(((a, b, c)) => from >= a && to_ <= b ? Some(c) : None))
          ->Array.filter(c => c != "")
          ->Array.join(" ")
        out->Array.push((value->String.slice(~start=from, ~end=to_), classes))
      | _ => ()
      }
    )
    at := end_
  })
  out
}

// The query bar on Logs results: the query editable in place, its keywords, values and
// durations coloured underneath the (transparent) text being edited. `added` marks a part
// just added to the query, `mistake` where the server says the query goes wrong.
module QueryBar = {
  @react.component
  let make = (
    ~value,
    ~onChange,
    ~onRun,
    ~onExplain,
    ~tools: React.element=React.null,
    ~added: option<(int, int)>=?,
    ~mistake: option<int>=?,
    ~invalid=false,
    ~inputRef: option<React.ref<Nullable.t<Dom.element>>>=?,
    ~onEscape: option<unit => unit>=?,
  ) => {
    let paint = React.useRef(Nullable.null)
    let follow = (input: Dom.element) =>
      paint.current->Nullable.forEach(el => el->setScrollLeft(scrollLeft(input)))
    let target = event => ReactEvent.Synthetic.target(event)->Obj.magic
    // The mistake runs from its position to the next space, or the end.
    let underline = mistake->Option.map(pos => {
      let rest = value->String.slice(~start=pos)
      let space = rest->String.indexOf(" ")
      (pos, space < 0 ? String.length(value) : pos + Math.Int.max(space, 1), "tok-mistake")
    })
    let marks =
      [added->Option.map(((a, b)) => (a, b, "tok-added")), underline]->Array.filterMap(x => x)
    <div className={"querybar" ++ (invalid ? " is-invalid" : "")}>
      <span className="querybar-prompt" ariaHidden=true> {React.string("›")} </span>
      <div className="querybar-field">
        <div className="querybar-paint type-query" ariaHidden=true ref={ReactDOM.Ref.domRef(paint)}>
          {paintRuns(value, marks)
          ->Array.mapWithIndex(((run, className), i) =>
            <span key={Int.toString(i)} className=?{className == "" ? None : Some(className)}>
              {React.string(run)}
            </span>
          )
          ->React.array}
        </div>
        <label>
          <span className="visually-hidden"> {React.string("TrieQL query")} </span>
          <input
            ref=?{inputRef->Option.map(ReactDOM.Ref.domRef)}
            className="querybar-input type-query"
            value
            spellCheck=false
            autoComplete="off"
            ariaInvalid=?{invalid ? Some(#"true") : None}
            onChange={event => {
              onChange(inputValue(event))
              follow(target(event))
            }}
            onScroll={event => follow(target(event))}
            onSelect={event => follow(target(event))}
            onKeyUp={event => follow(target(event))}
            onKeyDown={event =>
              if isRunKey(event) {
                ReactEvent.Keyboard.preventDefault(event)
                onRun()
              } else if ReactEvent.Keyboard.key(event) == "Escape" {
                onEscape->Option.forEach(f => {
                  ReactEvent.Keyboard.preventDefault(event)
                  f()
                })
              }}
          />
        </label>
      </div>
      tools
      <Button onClick={_ => onExplain()}> {React.string("Explain")} </Button>
      <Run onRun />
    </div>
  }
}

module Chip = {
  @react.component
  let make = (~label, ~onClick) =>
    <button type_="button" className="chip type-mono" onClick> {React.string(label)} </button>
}

// The filter field on Templates: filters as you type.
module Filter = {
  @react.component
  let make = (~value, ~onChange) =>
    <label className="filter">
      <span className="visually-hidden"> {React.string("Filter templates")} </span>
      <input
        className="filter-input type-label"
        value
        spellCheck=false
        autoComplete="off"
        placeholder={`Filter keys or values…`}
        onChange={event => onChange(inputValue(event))}
      />
    </label>
}
