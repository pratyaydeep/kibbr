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

// The query bar on Logs results: the query editable in place, its keywords, values and
// durations coloured underneath the (transparent) text being edited.
module QueryBar = {
  @react.component
  let make = (~value, ~onChange, ~onRun, ~onExplain) => {
    let paint = React.useRef(Nullable.null)
    let follow = (input: Dom.element) =>
      paint.current->Nullable.forEach(el => el->setScrollLeft(scrollLeft(input)))
    let target = event => ReactEvent.Synthetic.target(event)->Obj.magic
    <div className="querybar">
      <span className="querybar-prompt" ariaHidden=true> {React.string("›")} </span>
      <div className="querybar-field">
        <div className="querybar-paint type-query" ariaHidden=true ref={ReactDOM.Ref.domRef(paint)}>
          {Trieql.colourise(value)
          ->Array.mapWithIndex((segment, i) =>
            <span
              key={Int.toString(i)}
              className=?{switch segment.colour {
              | Keyword => Some("tok-keyword")
              | Value => Some("tok-value")
              | Plain => None
              }}>
              {React.string(segment.text)}
            </span>
          )
          ->React.array}
        </div>
        <label>
          <span className="visually-hidden"> {React.string("TrieQL query")} </span>
          <input
            className="querybar-input type-query"
            value
            spellCheck=false
            autoComplete="off"
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
              }}
          />
        </label>
      </div>
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
