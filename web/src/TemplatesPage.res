// Every shape of line kibbr has seen, as a tree, and one template's columns and latest
// lines (screens/templates/spec.md).

let text = React.string

// The tree's levels for one template: each promoted pair is a key level and a value level
// below it; the template itself is the leaf. A template with nothing promoted is a leaf at
// the top.
let levels = (t: Api.template) =>
  Tmpl.promoted(t)->Array.flatMap(((key, value)) => [key, value])

type rec node = {
  label: string,
  // The levels from the top to here, joined: the node's identity.
  id: string,
  depth: int,
  children: array<node>,
  leaves: array<Api.template>,
}

let separator = "\u0000"

// The templates as a tree, branches and leaves each in order of rows, most first.
let build = (templates: array<Api.template>) => {
  let rows = (t: Api.template) => t.rows
  let rec grow = (list: array<Api.template>, depth: int, prefix: string, label: string) => {
    let here = list->Array.filter(t => Array.length(levels(t)) == depth)
    let deeper = list->Array.filter(t => Array.length(levels(t)) > depth)
    let groups: dict<array<Api.template>> = Dict.make()
    let order = []
    deeper->Array.forEach(t => {
      let key = levels(t)->Array.getUnsafe(depth)
      switch groups->Dict.get(key) {
      | Some(members) => members->Array.push(t)
      | None =>
        order->Array.push(key)
        groups->Dict.set(key, [t])
      }
    })
    let total = (n: node) => {
      let rec sum = (n: node) =>
        n.leaves->Array.reduce(0.0, (s, t) => s +. rows(t)) +.
          n.children->Array.reduce(0.0, (s, c) => s +. sum(c))
      sum(n)
    }
    let children =
      order
      ->Array.map(key =>
        grow(
          groups->Dict.get(key)->Option.getOr([]),
          depth + 1,
          prefix == "" ? key : prefix ++ separator ++ key,
          key,
        )
      )
      ->Array.toSorted((a, b) => total(b) -. total(a))
    {
      label,
      id: prefix,
      depth,
      children,
      leaves: here->Array.toSorted((a, b) => rows(b) -. rows(a)),
    }
  }
  grow(templates, 0, "", "")
}

let rec templatesIn = (n: node) =>
  Array.length(n.leaves) + n.children->Array.reduce(0, (s, c) => s + templatesIn(c))

let rec rowsIn = (n: node) =>
  n.leaves->Array.reduce(0.0, (s, t) => s +. t.rows) +.
    n.children->Array.reduce(0.0, (s, c) => s +. rowsIn(c))

// The branches above a template, by id.
let ancestors = (t: Api.template) => {
  let ls = levels(t)
  ls->Array.mapWithIndex((_, i) => ls->Array.slice(~start=0, ~end=i + 1)->Array.join(separator))
}

// Whether the filter text is in the template's path or columns.
let matches = (t: Api.template, needle: string) =>
  needle == "" ||
    t.path->Array.some(p => p->String.toLowerCase->String.includes(needle)) ||
    t.columns->Array.some(c => c.name->String.toLowerCase->String.includes(needle))

// Up to three distinct values of `key` in the rows, as shown.
let samples = (rows: array<dict<JSON.t>>, key: string) => {
  let out = []
  rows->Array.forEach(row => {
    let flat = []
    DataDisplay.flatten(row, "", flat)
    switch flat->Array.find(((k, _)) => k == key) {
    | Some((_, v)) =>
      let shown = DataDisplay.cellText(v)
      if Array.length(out) < 3 && !(out->Array.includes(shown)) {
        out->Array.push(shown)
      }
    | None => ()
    }
  })
  out
}

module Detail = {
  @react.component
  let make = (~template as t: Api.template) => {
    let (latest, setLatest) = React.useState(() => [])
    React.useEffect1(() => {
      let alive = ref(true)
      setLatest(_ => [])
      let _ = (
        async () =>
          switch await Api.query(`MATCH LOG ${Tmpl.filter(t)} SINCE 24h LIMIT 50`) {
          | Ok(a) => alive.contents ? setLatest(_ => a.rows) : ()
          | Error(_) => ()
          }
      )()
      Some(() => alive := false)
    }, [t.id])
    let now = Browser.now()
    let filter = Tmpl.filter(t)
    let timeKey = Tmpl.timeColumn(t)->Option.getOr("ts")
    let columns = Array.length(t.columns)
    <div className="detail">
      <div className="detail-crumb type-mono">
        {text(levels(t)->Array.concat([`template ${Int.toString(t.id)}`])->Array.join(" › "))}
      </div>
      <div className="detail-title">
        <h1 className="detail-name type-heading-mono"> {text(Tmpl.name(t))} </h1>
        <DataDisplay.CountPill
          label={`${Int.toString(columns)} ${columns == 1 ? "column" : "columns"}`}
        />
      </div>
      <div className="facts">
        <DataDisplay.Fact label="Rows" value={Format.compact(t.rows)} />
        {switch t.firstSeen {
        | Some(s) => <DataDisplay.Fact label="First seen" value={Format.dayMonth(Date.fromString(s))} />
        | None => React.null
        }}
        {switch t.lastSeen {
        | Some(_) => <DataDisplay.Fact label="Last seen" value={Format.relative(Tmpl.lastSeenMs(t), now)} />
        | None => React.null
        }}
      </div>
      <div className="detail-actions">
        <Controls.Button
          primary=true onClick={_ => Route.go(Results(`MATCH LOG ${filter} SINCE 15m`))}>
          {text("Query this template")}
        </Controls.Button>
        <Controls.Button
          onClick={_ => Route.go(Results(`COUNT LOG ${filter} SINCE 15m GROUP BY time(10s)`))}>
          {text("Count by time")}
        </Controls.Button>
      </div>
      <Panel title="Columns">
        <table className="data-table">
          <thead>
            <tr>
              <th> {text("key")} </th>
              <th> {text("type")} </th>
              <th> {text("sample values")} </th>
            </tr>
          </thead>
          <tbody>
            {t.columns
            ->Array.map(c =>
              <tr key=c.name>
                <td className="is-key"> {text(c.name)} </td>
                <td> {text(c.columnType->Option.getOr("—"))} </td>
                <td className="is-samples"> {text(samples(latest, c.name)->Array.join(" · "))} </td>
              </tr>
            )
            ->React.array}
          </tbody>
        </table>
      </Panel>
      <Panel title="Latest lines" flex=true>
        {latest
        ->Array.mapWithIndex((row, i) => <DataDisplay.LatestRow key={Int.toString(i)} row timeKey />)
        ->React.array}
      </Panel>
    </div>
  }
}

@react.component
let make = (~selected: option<int>) => {
  let (templates, setTemplates) = React.useState(() => [])
  let (needle, setNeedle) = React.useState(() => "")
  let (opened, setOpened) = React.useState(() => [])
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

  // With nothing selected, the template seen most recently.
  let current = switch selected {
  | Some(id) => templates->Array.find(t => t.id == id)
  | None =>
    templates->Array.reduce(None, (best, t) =>
      switch best {
      | Some(b) if Tmpl.lastSeenMs(b) >= Tmpl.lastSeenMs(t) => best
      | _ => Some(t)
      }
    )
  }

  // The selected template's branches open when it changes.
  React.useEffect1(() => {
    switch current {
    | Some(t) =>
      setOpened(list =>
        list->Array.concat(ancestors(t)->Array.filter(a => !(list->Array.includes(a))))
      )
    | None => ()
    }
    None
  }, [current->Option.map(t => t.id)])

  let filtering = needle->String.trim != ""
  let lowered = needle->String.trim->String.toLowerCase
  let tree = build(templates->Array.filter(t => matches(t, lowered)))
  let isOpen = (n: node) => filtering || opened->Array.includes(n.id)
  let toggle = (n: node) =>
    setOpened(list =>
      list->Array.includes(n.id) ? list->Array.filter(id => id != n.id) : list->Array.concat([n.id])
    )

  let rec rowsOf = (n: node) => {
    let leaves =
      n.leaves->Array.map(t =>
        <DataDisplay.TreeRow
          key={"t" ++ Int.toString(t.id)}
          depth=n.depth
          label={Tmpl.leafLabel(t)}
          count={Format.compact(t.rows)}
          leaf=true
          selected={current->Option.map(c => c.id) == Some(t.id)}
          onClick={_ => Route.go(Templates(Some(t.id)))}
        />
      )
    let branches = n.children->Array.flatMap(child => {
      let open_ = isOpen(child)
      let count =
        child.depth == 1
          ? `${Format.commasInt(templatesIn(child))} templates`
          : Format.compact(rowsIn(child))
      [
        <DataDisplay.TreeRow
          key={"b" ++ child.id}
          depth=n.depth
          label=child.label
          count
          open_
          onClick={_ => toggle(child)}
        />,
      ]->Array.concat(open_ ? rowsOf(child) : [])
    })
    branches->Array.concat(leaves)
  }

  <div className="templates">
    <aside className="tree" ariaLabel="Template tree">
      <div className="tree-head">
        <h2 className="type-panel-title"> {text("Template tree")} </h2>
        <Controls.Filter value=needle onChange={v => setNeedle(_ => v)} />
      </div>
      <div className="tree-body"> {rowsOf(tree)->React.array} </div>
    </aside>
    {switch current {
    | Some(template) => <Detail key={Int.toString(template.id)} template />
    | None => <div className="detail" />
    }}
  </div>
}
