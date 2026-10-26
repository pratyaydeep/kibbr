// A query's answer: matches over time, the lines, how it ran, and the templates it touched
// (screens/logs-results/spec.md).

let text = React.string

type panel =
  | Matches
  | Rows
  | Explain
  | Matched

let titleOf = panel =>
  switch panel {
  | Matches => "Matches over time"
  | Rows => "Log rows"
  | Explain => "Explain"
  | Matched => "Templates matched"
  }

// Raw or Columns stays chosen while new queries run in this tab.
let chosenView = ref(Controls.Raw)

type rowsState = {
  answer: option<Api.answer>,
  rows: array<dict<JSON.t>>,
  nextCursor: option<string>,
  error: option<string>,
  loading: bool,
  loadingMore: bool,
}

let initialRows = {
  answer: None,
  rows: [],
  nextCursor: None,
  error: None,
  loading: true,
  loadingMore: false,
}

// The next page loads once the list is scrolled to within this many pixels of its end.
let nearEnd = 400.0

@get external scrollTop: Dom.element => float = "scrollTop"
@get external scrollHeight: Dom.element => float = "scrollHeight"
@get external clientHeight: Dom.element => float = "clientHeight"

@react.component
let make = (~q: string, ~server: Server.t) => {
  let (draft, setDraft) = React.useState(() => q)
  let (runs, setRuns) = React.useState(() => 0)
  let (rows, setRows) = React.useState(() => initialRows)
  let (groups, setGroups) = React.useState(() => [])
  let (templates, setTemplates) = React.useState(() => [])
  let (view, setView) = React.useState(() => chosenView.contents)
  let (minimized, setMinimized) = React.useState(() => [])
  let (maximized, setMaximized) = React.useState(() => None)
  let parsed = React.useMemo1(() => Trieql.parse(q), [q])

  // The bar shows the query of the page: a new route replaces what was typed.
  React.useEffect1(() => {
    setDraft(_ => q)
    None
  }, [q])

  // Rows and chart, each time the query (or Run) asks again.
  React.useEffect2(() => {
    let alive = ref(true)
    Storage.addRecent(q)
    setRows(previous => {...previous, loading: true, error: None})
    let rowsQuery = parsed->Option.mapOr(q, p => Trieql.rowsQuery(p, ~after=None))
    let _ = (
      async () => {
        let answer = await Api.query(rowsQuery)
        if alive.contents {
          setRows(_ =>
            switch answer {
            | Ok(a) => {
                answer: Some(a),
                rows: a.rows,
                nextCursor: a.meta.nextCursor,
                error: None,
                loading: false,
                loadingMore: false,
              }
            | Error(message) => {...initialRows, loading: false, error: Some(message)}
            }
          )
        }
      }
    )()
    let _ = (
      async () =>
        switch parsed->Option.flatMap(Trieql.chartQuery) {
        | Some(chartQuery) =>
          switch await Api.query(chartQuery) {
          | Ok(a) => alive.contents ? setGroups(_ => a.groups) : ()
          | Error(_) => alive.contents ? setGroups(_ => []) : ()
          }
        | None => setGroups(_ => [])
        }
    )()
    let _ = (
      async () =>
        switch await Api.templates() {
        | Ok(list) => alive.contents ? setTemplates(_ => list) : ()
        | Error(_) => ()
        }
    )()
    Some(() => alive := false)
  }, (q, runs))

  let loadMore = () =>
    switch (rows.nextCursor, parsed, rows.loadingMore, rows.loading) {
    | (Some(cursor), Some(p), false, false) =>
      setRows(previous => {...previous, loadingMore: true})
      let _ = (
        async () =>
          switch await Api.query(Trieql.rowsQuery(p, ~after=Some(cursor))) {
          | Ok(a) =>
            setRows(previous => {
              ...previous,
              rows: previous.rows->Array.concat(a.rows),
              nextCursor: a.meta.nextCursor,
              loadingMore: false,
            })
          | Error(message) =>
            setRows(previous => {...previous, loadingMore: false, nextCursor: None, error: Some(message)})
          }
      )()
    | _ => ()
    }

  let run = () => {
    let next = draft->String.trim
    if next == "" {
      ()
    } else if next == q {
      setRuns(n => n + 1)
    } else {
      Route.go(Results(next))
    }
  }

  // ⌘↵ / Ctrl↵ runs the query from anywhere on the page.
  React.useEffect2(() => {
    let onKey = event =>
      if (
        Browser.eventKey(event) == "Enter" &&
          (Browser.eventMetaKey(event) || Browser.eventCtrlKey(event)) &&
          !Browser.typingIn(event)
      ) {
        Browser.preventDefault(event)
        run()
      }
    Browser.addDocumentListener("keydown", onKey)
    Some(() => Browser.removeDocumentListener("keydown", onKey))
  }, (draft, q))

  let size = panel => Some({
    Panel.minimized: minimized->Array.includes(panel),
    maximized: maximized == Some(panel),
    onMinimize: () =>
      setMinimized(list =>
        list->Array.includes(panel) ? list->Array.filter(p => p != panel) : list->Array.concat([panel])
      ),
    onMaximize: () => setMaximized(current => current == Some(panel) ? None : Some(panel)),
  })

  let asked = parsed->Option.mapOr([], p => p.pairs)
  let answer = rows.answer
  let meta = answer->Option.map(a => a.meta)
  let count = answer->Option.flatMap(a => a.count)
  let range = parsed->Option.flatMap(Trieql.rangeSeconds)

  let matchesPanel =
    <Panel
      key="matches"
      title={titleOf(Matches)}
      subtitle=?{range->Option.map(r =>
        `${Format.duration(Trieql.bucketSeconds(r) *. 1000.0)} buckets · ${Format.duration(
            r *. 1000.0,
          )}`
      )}
      size=?{size(Matches)}
      className="chart-panel">
      <DataDisplay.Chart total=count groups />
    </Panel>

  let rowsBody = switch (rows.rows, view) {
  | ([], _) if !rows.loading && rows.error == None =>
    <div className="panel-empty type-label"> {text("No lines match.")} </div>
  | (list, Controls.Raw) =>
    <>
      <div className="rows-label type-section-label">
        <span> {text("JSON line, as ingested")} </span>
        <span className="rows-legend">
          <span className="rows-legend-swatch" />
          {text("in the query")}
        </span>
      </div>
      <div
        className="rows-scroll"
        onScroll={event => {
          let el: Dom.element = ReactEvent.UI.target(event)->Obj.magic
          if scrollHeight(el) -. scrollTop(el) -. clientHeight(el) < nearEnd {
            loadMore()
          }
        }}>
        {list
        ->Array.mapWithIndex((row, i) => <DataDisplay.RawRow key={Int.toString(i)} row asked />)
        ->React.array}
      </div>
    </>
  | (list, Controls.Columns) =>
    <div
      className="rows-scroll"
      onScroll={event => {
        let el: Dom.element = ReactEvent.UI.target(event)->Obj.magic
        if scrollHeight(el) -. scrollTop(el) -. clientHeight(el) < nearEnd {
          loadMore()
        }
      }}>
      <DataDisplay.ColumnsTable rows=list asked />
    </div>
  }

  let rowsPanel =
    <Panel
      key="rows"
      title={titleOf(Rows)}
      subtitle=?{count->Option.map(c => `newest first · ${Format.commas(c)} lines`)}
      controls={<>
        <Controls.RowsToggle
          view
          onView={v => {
            chosenView := v
            setView(_ => v)
          }}
        />
        <span className="control-divider" ariaHidden=true />
      </>}
      size=?{size(Rows)}
      flex=true>
      rowsBody
    </Panel>

  let explainPanel =
    <Panel key="explain" title={titleOf(Explain)} subtitle="two phases" size=?{size(Explain)}>
      <div className="phases">
        <DataDisplay.Phase
          number=1
          title={`Trie walk · in memory`}
          rows={[
            meta
            ->Option.flatMap(m => m.templatesMatched)
            ->Option.map(n => ("templates matched", Format.commas(n))),
          ]->Array.filterMap(x => x)}
        />
        <DataDisplay.Phase
          number=2
          title={`Column scan · on disk`}
          rows={[
            meta->Option.flatMap(m => m.filesOpened)->Option.map(n => ("files opened", Format.commas(n))),
            meta->Option.flatMap(m => m.rowsScanned)->Option.map(n => ("rows scanned", Format.compact(n))),
            meta->Option.flatMap(m => m.durationMs)->Option.map(ms => ("time", Format.duration(ms))),
          ]->Array.filterMap(x => x)}
        />
      </div>
    </Panel>

  let byId = templates->Array.map(t => (Int.toString(t.Api.id), t))->Dict.fromArray
  let matched =
    meta
    ->Option.mapOr([], m => m.templateIds)
    ->Array.map(id => (id, byId->Dict.get(Int.toString(id))))
    ->Array.toSorted(((_, a), (_, b)) =>
      b->Option.mapOr(0.0, t => t.rows) -. a->Option.mapOr(0.0, t => t.rows)
    )
  let total = server.stats->Option.flatMap(s => s.templates)
  let matchedCount =
    meta->Option.flatMap(m => m.templatesMatched)->Option.getOr(Int.toFloat(Array.length(matched)))

  let matchedPanel =
    <Panel
      key="matched"
      title={titleOf(Matched)}
      subtitle=?{meta->Option.map(_ =>
        Format.commas(matchedCount) ++ total->Option.mapOr("", t => " of " ++ Format.commas(t))
      )}
      size=?{size(Matched)}
      flex=true>
      <div className="matched-list">
        {matched
        ->Array.map(((id, t)) =>
          <DataDisplay.MatchedRow
            key={Int.toString(id)}
            id
            path={t->Option.mapOr("", Tmpl.name)}
            rows={t->Option.map(t => t.rows)}
            onClick={_ => Route.go(Templates(Some(id)))}
          />
        )
        ->React.array}
      </div>
    </Panel>

  <div className="results">
    <Controls.QueryBar
      value=draft
      onChange={v => setDraft(_ => v)}
      onRun=run
      onExplain={() => setMaximized(_ => Some(Explain))}
    />
    {switch rows.error {
    | Some(message) => <div className="query-error type-caption" role="alert"> {text(message)} </div>
    | None => React.null
    }}
    <DataDisplay.InfoStrip stats=server.stats />
    {switch maximized {
    | Some(panel) =>
      <div className={"results-grid is-maximized" ++ (rows.loading ? " is-loading" : "")}>
        {switch panel {
        | Matches => matchesPanel
        | Rows => rowsPanel
        | Explain => explainPanel
        | Matched => matchedPanel
        }}
      </div>
    | None =>
      <div className={"results-grid" ++ (rows.loading ? " is-loading" : "")}>
        <div className="results-main"> matchesPanel rowsPanel </div>
        <div className="results-side"> explainPanel matchedPanel </div>
      </div>
    }}
  </div>
}
