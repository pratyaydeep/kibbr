// Where the console opens: one search box, suggestions, Recent and Saved
// (screens/logs-home/spec.md).

let text = React.string

// Up to six `key=value` suggestions: the templates grouped by their first promoted pair,
// most rows first.
let suggestions = (templates: array<Api.template>) => {
  let rows: dict<float> = Dict.make()
  let pairs: dict<(string, string)> = Dict.make()
  templates->Array.forEach(t =>
    switch Tmpl.promoted(t)->Array.get(0) {
    | Some((key, value) as pair) =>
      let label = key ++ "=" ++ value
      rows->Dict.set(label, rows->Dict.get(label)->Option.getOr(0.0) +. t.rows)
      pairs->Dict.set(label, pair)
    | None => ()
    }
  )
  rows
  ->Dict.toArray
  ->Array.toSorted(((_, a), (_, b)) => b -. a)
  ->Array.slice(~start=0, ~end=6)
  ->Array.filterMap(((label, _)) =>
    pairs
    ->Dict.get(label)
    ->Option.map(((key, value)) => (
      label,
      `MATCH LOG {${Tmpl.quoted(key)}: ${Tmpl.quoted(value)}} SINCE 15m`,
    ))
  )
}

@react.component
let make = (~server: Server.t) => {
  let (query, setQuery) = React.useState(() => "")
  let (templates, setTemplates) = React.useState(() => [])
  React.useEffect0(() => {
    let alive = ref(true)
    let _ = (
      async () =>
        switch await Api.templates() {
        | Ok(list) => alive.contents ? setTemplates(_ => list) : ()
        | Error(_) => ()
        }
    )()
    Some(() => alive := false)
  })

  let run = q =>
    if q->String.trim != "" {
      Route.go(Results(q->String.trim))
    }

  // ⌘↵ / Ctrl↵ runs the box from anywhere on the page.
  React.useEffect1(() => {
    let onKey = event =>
      if (
        Browser.eventKey(event) == "Enter" &&
          (Browser.eventMetaKey(event) || Browser.eventCtrlKey(event)) &&
          !Browser.typingIn(event)
      ) {
        Browser.preventDefault(event)
        run(query)
      }
    Browser.addDocumentListener("keydown", onKey)
    Some(() => Browser.removeDocumentListener("keydown", onKey))
  }, [query])

  let now = Browser.now()
  let recent = Storage.recent()->Array.slice(~start=0, ~end=4)
  let saved = Storage.saved()
  let templateCount = server.stats->Option.flatMap(s => s.templates)
  let newest = templates->Array.reduce(0.0, (top, t) => Math.max(top, Tmpl.lastSeenMs(t)))

  <div className="home">
    <div className="home-column">
      <Controls.SearchBox value=query onChange={q => setQuery(_ => q)} onRun={() => run(query)} />
      <div className="home-helper type-mono">
        {[
          templateCount->Option.map(n =>
            <React.Fragment key="templates">
              <b> {text(Format.commas(n))} </b>
              {text(" templates")}
            </React.Fragment>
          ),
          newest > 0.0
            ? Some(
                <React.Fragment key="last">
                  {text("last line ")}
                  <b> {text(Format.age(newest, now))} </b>
                  {text(" ago")}
                </React.Fragment>,
              )
            : None,
          Some(
            <React.Fragment key="run">
              <b> {text(Browser.runShortcut)} </b>
              {text(" to run")}
            </React.Fragment>,
          ),
        ]
        ->Array.filterMap(x => x)
        ->Array.mapWithIndex((part, i) =>
          <React.Fragment key={Int.toString(i)}>
            {i > 0 ? text(" · ") : React.null}
            part
          </React.Fragment>
        )
        ->React.array}
      </div>
      <div className="chips">
        {suggestions(templates)
        ->Array.map(((label, q)) => <Controls.Chip key=label label onClick={_ => run(q)} />)
        ->React.array}
      </div>
      <div className="home-lists">
        <div>
          <h2 className="list-heading"> {text("Recent")} </h2>
          {recent
          ->Array.map(r =>
            <DataDisplay.ListRow key=r.q query=r.q aside={Format.relative(r.at, now)} />
          )
          ->React.array}
        </div>
        {Array.length(saved) > 0
          ? <div>
              <h2 className="list-heading"> {text("Saved")} </h2>
              {saved
              ->Array.map(s => <DataDisplay.ListRow key=s.q query=s.q aside=s.name star=true />)
              ->React.array}
            </div>
          : React.null}
      </div>
      <div className="home-foot">
        <DataDisplay.InfoStrip stats=server.stats home=true />
      </div>
    </div>
  </div>
}
