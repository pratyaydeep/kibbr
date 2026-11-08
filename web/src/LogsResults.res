// A query's answer: matches over time, the lines, how it ran, and the templates it touched
// (screens/logs-results/spec.md).

let text = React.string

type panel =
  | Matches
  | Rows
  | Explain
  | Matched

let panels = [Matches, Rows, Explain, Matched]

let titleOf = panel =>
  switch panel {
  | Matches => "Matches over time"
  | Rows => "Log rows"
  | Explain => "Explain"
  | Matched => "Templates matched"
  }

let panelOf = title => panels->Array.find(p => titleOf(p) == title)

// Raw or Columns stays chosen while new queries run in this tab.
let chosenView = ref(Controls.Raw)

// Why the rows did not come: the message, whether the query itself is wrong (a 400), and
// where in the query as typed, when the server says.
type mistake = {message: string, invalid: bool, at: option<int>}

type rowsState = {
  answer: option<Api.answer>,
  rows: array<dict<JSON.t>>,
  // Each loaded row's time from the server, at the same index; shorter than `rows` when
  // the server does not send them.
  times: array<option<Date.t>>,
  nextCursor: option<string>,
  error: option<mistake>,
  loading: bool,
  loadingMore: bool,
}

let initialRows = {
  answer: None,
  rows: [],
  times: [],
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
@send external focusElement: Dom.element => unit = "focus"
@send external scrollIntoView: (Dom.element, {..}) => unit = "scrollIntoView"
@val external writeClipboard: string => promise<unit> = "navigator.clipboard.writeText"

// A UTF-8 byte offset (what the server counts) as an index into the JS string.
let indexOfByte: (string, int) => int = %raw(`(s, b) => {
  const enc = new TextEncoder();
  let bytes = 0, i = 0;
  for (const ch of s) {
    if (bytes >= b) return i;
    bytes += enc.encode(ch).length;
    i += ch.length;
  }
  return i;
}`)

type menu =
  | TimeMenu
  | SaveMenu
  | KeysMenu

@react.component
let make = (~q: string, ~server: Server.t) => {
  let (draft, setDraft) = React.useState(() => q)
  let (runs, setRuns) = React.useState(() => 0)
  let (rows, setRows) = React.useState(() => initialRows)
  let (groups, setGroups) = React.useState(() => [])
  let (templates, setTemplates) = React.useState(() => [])
  let (view, setView) = React.useState(() => chosenView.contents)
  // The panel layout, as this browser last left it (kibbr.layout).
  let (minimized, setMinimized) = React.useState(() =>
    Storage.layout().minimized->Array.filterMap(panelOf)
  )
  let (maximized, setMaximized) = React.useState(() =>
    Storage.layout().maximized->Option.flatMap(panelOf)
  )
  React.useEffect2(() => {
    Storage.setLayout({
      maximized: maximized->Option.map(titleOf),
      minimized: minimized->Array.map(titleOf),
    })
    None
  }, (minimized, maximized))
  let (selected, setSelected) = React.useState((): option<int> => None)
  let (menu, setMenu) = React.useState((): option<menu> => None)
  let (toast, setToast) = React.useState((): option<QueryTools.toast> => None)
  // The part of the query just added by +, for the query it was added to.
  let (added, setAdded) = React.useState((): option<(string, int, int)> => None)
  let (zoomRange, setZoomRange) = React.useState((): option<(float, float)> => None)
  let (saved, setSaved) = React.useState(() => Storage.saved())
  let queryInput = React.useRef(Nullable.null)
  let rowsList = React.useRef(Nullable.null)
  let orderedRef: React.ref<array<LogRows.shown>> = React.useRef([])
  let pointerFocus = React.useRef(false)
  let toastId = React.useRef(0)
  let showToast = (message, ~undo=?) => {
    toastId.current = toastId.current + 1
    setToast(_ => Some({QueryTools.id: toastId.current, message, undo}))
  }
  let parsed = React.useMemo1(() => Trieql.parse(q), [q])
  // Raw view: the Wrap lines default, per-row wrap choices, rows opened as JSON, the order.
  let (wrapAll, setWrapAll) = React.useState(() => false)
  let (overrides, setOverrides) = React.useState((): Map.t<int, bool> => Map.make())
  let (opened, setOpened) = React.useState((): Set.t<int> => Set.make())
  // Oldest first is the server's to sort (`ORDER BY time ASC`); a query that says so starts
  // that way.
  let (oldest, setOldest) = React.useState(() => parsed->Option.mapOr(false, p => p.oldest))

  // The bar shows the query of the page: a new route replaces what was typed.
  React.useEffect1(() => {
    setDraft(_ => q)
    setOldest(_ => parsed->Option.mapOr(false, p => p.oldest))
    setOverrides(_ => Map.make())
    setOpened(_ => Set.make())
    setSelected(_ => None)
    None
  }, [q])

  // Rows and chart, each time the query (or Run) asks again.
  React.useEffect3(() => {
    let alive = ref(true)
    Storage.addRecent(q)
    setRows(previous => {...previous, loading: true, error: None})
    let rowsQuery = parsed->Option.mapOr(q, p => Trieql.rowsQuery(p, ~after=None, ~oldest))
    let _ = (
      async () => {
        let answer = await Api.queryChecked(rowsQuery)
        if alive.contents {
          setRows(_ =>
            switch answer {
            | Ok(a) => {
                answer: Some(a),
                rows: a.rows,
                times: a.times,
                nextCursor: a.meta.nextCursor,
                error: None,
                loading: false,
                loadingMore: false,
              }
            | Error(f) =>
              // The position counts bytes of the query sent, carried back to the one typed.
              let at = f.pos->Option.flatMap(pos => {
                let i = indexOfByte(rowsQuery, pos)
                parsed->Option.mapOr(Some(i), p => Trieql.rowsOffsetToTyped(p, i))
              })
              {...initialRows, loading: false, error: Some({message: f.message, invalid: f.invalid, at})}
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
  }, (q, runs, oldest))

  let loadMore = () =>
    switch (rows.nextCursor, parsed, rows.loadingMore, rows.loading) {
    | (Some(cursor), Some(p), false, false) =>
      setRows(previous => {...previous, loadingMore: true})
      let _ = (
        async () =>
          switch await Api.query(Trieql.rowsQuery(p, ~after=Some(cursor), ~oldest)) {
          | Ok(a) =>
            setRows(previous => {
              ...previous,
              rows: previous.rows->Array.concat(a.rows),
              times: previous.times->Array.concat(a.times),
              nextCursor: a.meta.nextCursor,
              loadingMore: false,
            })
          | Error(message) =>
            setRows(previous => {
              ...previous,
              loadingMore: false,
              nextCursor: None,
              error: Some({message, invalid: false, at: None}),
            })
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
    onMaximize: () => {
      setMaximized(current => current == Some(panel) ? None : Some(panel))
      // Restoring a panel also expands it.
      setMinimized(list => list->Array.filter(p => p != panel))
    },
  })

  let asked = React.useMemo1(() => parsed->Option.mapOr([], p => p.pairs), [parsed])
  let templateIds = rows.answer->Option.mapOr([], a => a.meta.templateIds)
  let timeField = React.useMemo2(() => LogRows.timeField(templates, templateIds), (templates, templateIds))
  // The server's time for each row when it sends one for every row loaded, or else the
  // matched templates' shared time field.
  let shown = React.useMemo3(
    () => {
      let sent = Array.length(rows.times) == Array.length(rows.rows)
      rows.rows->Array.mapWithIndex((row, i) =>
        LogRows.shownOf(row, i, ~sent=sent ? rows.times->Array.get(i) : None, ~field=timeField)
      )
    },
    (rows.rows, rows.times, timeField),
  )
  // The rows come in the order asked for: the server sorts both ways.
  let ordered = shown
  orderedRef.current = ordered
  let flipSort = () => setOldest(o => !o)

  let wrapAllRef = React.useRef(wrapAll)
  wrapAllRef.current = wrapAll
  let onAction = React.useCallback0((id, action: LogRows.rowAction) =>
    switch action {
    | Wrap =>
      setOverrides(previous => {
        let next = Map.fromArray(previous->Map.entries->Iterator.toArray)
        // The row's current state, flipped: the override, or else the switch's default.
        next->Map.set(id, !(previous->Map.get(id)->Option.getOr(wrapAllRef.current)))
        next
      })
    | Open =>
      setOpened(previous => {
        let next = Set.fromArray(previous->Set.values->Iterator.toArray)
        next->Set.add(id)
        next
      })
    | Close =>
      setOpened(previous => {
        let next = Set.fromArray(previous->Set.values->Iterator.toArray)
        next->Set.delete(id)->ignore
        next
      })
    | Select => setSelected(_ => Some(id))
    }
  )

  // A query change from a tool: run it, or run it again when it is the same.
  let goTo = next => next == q ? setRuns(n => n + 1) : Route.go(Results(next))

  // + on a value: `"key": "value"` joins the filter, highlighted, with an Undo.
  let qRef = React.useRef(q)
  qRef.current = q
  let onAdd = React.useCallback0((path: string, value: JSON.t) => {
    let q = qRef.current
    switch (Trieql.parse(q), LogRows.scalarText(value)) {
    | (Some(p), Some(literal)) =>
      switch Trieql.addPair(q, p, ~key=path, ~value=literal) {
      | Some((next, start, end_)) =>
        setAdded(_ => Some((next, start, end_)))
        Route.go(Results(next))
        showToast(
          `Added ${next->String.slice(~start, ~end=end_)} to the query`,
          ~undo=() => {
            setAdded(_ => None)
            Route.go(Results(q))
          },
        )
      | None => ()
      }
    | _ => ()
    }
  })

  // The query the time tools change: what is typed, if it reads, or else the page's.
  let timeTarget = () =>
    switch Trieql.parse(draft) {
    | Some(p) => Some((draft, p))
    | None => parsed->Option.map(p => (q, p))
    }
  let setClause = clause => {
    setMenu(_ => None)
    timeTarget()->Option.forEach(((src, p)) => goTo(Trieql.setTime(src, p, clause)))
  }

  let savedName = saved->Array.find(s => s.q == q)->Option.map(s => s.name)
  let save = name => {
    Storage.addSaved(q, name)
    setSaved(_ => Storage.saved())
    setMenu(_ => None)
    showToast(`Saved as "${name}"`)
  }

  // ── Keyboard ──
  let move = step => {
    let list = orderedRef.current
    let n = Array.length(list)
    if n > 0 {
      setSelected(current => {
        let i = switch current {
        | Some(id) => list->Array.findIndex((s: LogRows.shown) => s.id == id)
        | None => -1
        }
        let next = i < 0 ? 0 : Math.Int.max(0, Math.Int.min(n - 1, i + step))
        list->Array.get(next)->Option.map(s => s.id)
      })
    }
  }
  let copyRow = id =>
    orderedRef.current
    ->Array.find((s: LogRows.shown) => s.id == id)
    ->Option.forEach(s => {
      let _ =
        writeClipboard(JSON.stringify(LogRows.toJson(s.entries)))->Promise.catch(_ => Promise.resolve())
      showToast("Copied the row as JSON")
    })
  let onRowsKey = event => {
    let key = ReactEvent.Keyboard.key(event)
    let plain =
      !ReactEvent.Keyboard.metaKey(event) && !ReactEvent.Keyboard.ctrlKey(event) &&
      !ReactEvent.Keyboard.altKey(event)
    let act = f => {
      ReactEvent.Keyboard.preventDefault(event)
      f()
    }
    switch (plain, key, selected) {
    | (true, "j" | "ArrowDown", _) => act(() => move(1))
    | (true, "k" | "ArrowUp", _) => act(() => move(-1))
    | (true, "Enter", Some(id)) => act(() => onAction(id, opened->Set.has(id) ? Close : Open))
    | (true, "w", Some(id)) => act(() => onAction(id, Wrap))
    | (true, "c", Some(id)) => act(() => copyRow(id))
    | _ => ()
    }
  }
  let focusRows = () => rowsList.current->Nullable.forEach(focusElement)

  // The selected row stays in view.
  React.useEffect1(() => {
    if selected != None {
      switch Browser.querySelector(".logrow.is-selected") {
      | Some(el) => scrollIntoView(el->Obj.magic, {"block": "nearest"})
      | None => ()
      }
    }
    None
  }, [selected])

  // `/`, `?` and Esc on the whole page.
  React.useEffect3(() => {
    let onKey = event => {
      let typing = Browser.typingIn(event)
      let plain = !Browser.eventMetaKey(event) && !Browser.eventCtrlKey(event) && !Browser.eventAltKey(event)
      switch Browser.eventKey(event) {
      | "Escape" =>
        switch (menu, selected) {
        | (Some(_), _) => setMenu(_ => None)
        | (None, Some(id)) if !typing && opened->Set.has(id) => onAction(id, Close)
        | _ => ()
        }
      | "/" if !typing && plain =>
        Browser.preventDefault(event)
        queryInput.current->Nullable.forEach(focusElement)
      | "?" if !typing && !Browser.eventMetaKey(event) && !Browser.eventCtrlKey(event) =>
        Browser.preventDefault(event)
        setMenu(m => m == Some(KeysMenu) ? None : Some(KeysMenu))
      | _ => ()
      }
    }
    Browser.addDocumentListener("keydown", onKey)
    Some(() => Browser.removeDocumentListener("keydown", onKey))
  }, (menu, selected, opened))

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
          )} · drag across to zoom`
      )}
      controls=?{zoomRange->Option.map(r =>
        <span className="chart-range type-caption"> {text(DataDisplay.rangeText(r))} </span>
      )}
      size=?{size(Matches)}
      className="chart-panel">
      <DataDisplay.Chart
        total=count
        groups
        bucketMs=?{range->Option.map(r => Trieql.bucketSeconds(r) *. 1000.0)}
        onSelect={r => setZoomRange(_ => r)}
        onZoom={(from, to_) => setClause(Trieql.betweenClause(from, to_))}
      />
    </Panel>

  let onRowsScroll = event => {
    let el: Dom.element = ReactEvent.UI.target(event)->Obj.magic
    if scrollHeight(el) -. scrollTop(el) -. clientHeight(el) < nearEnd {
      loadMore()
    }
  }

  let rowsBody = switch (rows.rows, view) {
  | ([], _) if !rows.loading && rows.error == None =>
    <div className="panel-empty type-label"> {text("No lines match.")} </div>
  | (_, Controls.Raw) =>
    <div className="logrows">
      <LogRows.Head />
      <div
        className="logrows-scroll"
        ref={ReactDOM.Ref.domRef(rowsList)}
        tabIndex=0
        role="region"
        ariaLabel="Log rows"
        onScroll=onRowsScroll
        onKeyDown=onRowsKey
        onMouseDown={_ => pointerFocus.current = true}
        onFocus={event =>
          // Focus from the keyboard, with nothing selected, selects the first row.
          if (
            ReactEvent.Focus.target(event) == ReactEvent.Focus.currentTarget(event) &&
            !pointerFocus.current &&
            selected == None
          ) {
            move(1)
          }}
        onBlur={_ => pointerFocus.current = false}>
        <LogRows.Rows shown=ordered asked wrapAll overrides opened selected onAction onAdd />
      </div>
      <QueryTools.HintStrip onShortcuts={() => setMenu(_ => Some(KeysMenu))} />
    </div>
  | (_, Controls.Columns) =>
    <div className="rows-scroll" onScroll=onRowsScroll>
      <DataDisplay.ColumnsTable
        rows={ordered->Array.map(s => s.row)}
        asked
        onSortTime=flipSort
      />
    </div>
  }

  let rowsPanel =
    <Panel
      key="rows"
      title={titleOf(Rows)}
      subtitle=?{count->Option.map(c => `${Format.commas(c)} lines`)}
      controls={<>
        <LogRows.SortButton oldest onFlip=flipSort />
        {view == Controls.Raw
          ? <LogRows.WrapSwitch
              on=wrapAll
              onChange={on => {
                setWrapAll(_ => on)
                setOverrides(_ => Map.make())
              }}
            />
          : React.null}
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
      overlay={<>
        {menu == Some(KeysMenu) ? <QueryTools.KeyboardList onClose={() => setMenu(_ => None)} /> : React.null}
        {switch toast {
        | Some(t) =>
          <QueryTools.Toast
            key={Int.toString(t.id)}
            toast=t
            onDone={() =>
              setToast(c => c->Option.map(x => x.QueryTools.id) == Some(t.id) ? None : c)}
          />
        | None => React.null
        }}
      </>}
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

  // A mistake shows until the query is edited.
  let mistake = draft == q ? rows.error : None

  <div className="results">
    <Controls.QueryBar
      value=draft
      onChange={v => setDraft(_ => v)}
      onRun=run
      onExplain={() => setMaximized(_ => Some(Explain))}
      inputRef=queryInput
      onEscape=focusRows
      added=?{added->Option.flatMap(((forQ, a, b)) => forQ == draft ? Some((a, b)) : None)}
      invalid={mistake->Option.mapOr(false, m => m.invalid)}
      mistake=?{mistake->Option.flatMap(m => m.at)}
      tools={<>
        <QueryTools.SaveStar
          q
          savedName
          open_={menu == Some(SaveMenu)}
          onToggle={() => setMenu(m => m == Some(SaveMenu) ? None : Some(SaveMenu))}
          onClose={() => setMenu(m => m == Some(SaveMenu) ? None : m)}
          onSave=save
        />
        <QueryTools.TimeButton
          parsed={timeTarget()->Option.map(((_, p)) => p)}
          open_={menu == Some(TimeMenu)}
          onToggle={() => setMenu(m => m == Some(TimeMenu) ? None : Some(TimeMenu))}
          onClose={() => setMenu(m => m == Some(TimeMenu) ? None : m)}
          onClause=setClause
        />
      </>}
    />
    {switch mistake {
    | Some(m) =>
      // The parser's first line is the message; the lines after it quote the query with a
      // caret under the mistake, kept in mono so the caret lines up. Once the server says
      // where the mistake is, the underline and the character number take their place.
      let lines = m.message->String.split("\n")
      let first = lines->Array.get(0)->Option.getOr("")
      let excerpt = lines->Array.slice(~start=1)->Array.join("\n")
      <div className="query-error" role="alert">
        <div className="query-error-line">
          <Icon svg=Icon.errorXSvg className="query-error-icon" />
          <span> {text(m.at == None ? first : first->String.replaceRegExp(%re("/ at byte \d+$/"), ""))} </span>
          {switch m.at {
          | Some(i) =>
            <span className="query-error-at"> {text(` · character ${Int.toString(i + 1)}`)} </span>
          | None => React.null
          }}
        </div>
        {m.at == None && excerpt != ""
          ? <pre className="query-error-excerpt"> {text(excerpt)} </pre>
          : React.null}
      </div>
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
