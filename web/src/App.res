// The frame: the server banner across the top when needed, the sidebar, and the page.

// The sidebar's width, shared by every tab through `kibbr.rail.collapsed`.
let useRail = () => {
  let (collapsed, setCollapsed) = React.useState(Storage.railCollapsed)
  React.useEffect0(() => {
    let follow = event =>
      if Browser.eventStorageKey(event)->Nullable.toOption == Some(Storage.railKey) {
        setCollapsed(_ => Storage.railCollapsed())
      }
    Browser.addWindowListener("storage", follow)
    Some(() => Browser.removeWindowListener("storage", follow))
  })
  let set = collapsed => {
    Storage.setRailCollapsed(collapsed)
    setCollapsed(_ => collapsed)
  }
  (collapsed, set)
}

@react.component
let make = () => {
  let route = Route.useRoute()
  let server = Server.useServer()
  let (theme, setTheme) = Theme.useTheme()
  let (collapsed, setCollapsed) = useRail()
  // Logs goes back to the results that are open, if a query is.
  let (lastQuery, setLastQuery) = React.useState(() => None)
  React.useEffect1(() => {
    switch route {
    | Results(q) => setLastQuery(_ => Some(q))
    | Home => setLastQuery(_ => None)
    | _ => ()
    }
    None
  }, [route])

  // 1–3 move between pages when focus is not in something typed into.
  React.useEffect1(() => {
    let onKey = event =>
      if (
        !Browser.typingIn(event) &&
        !Browser.eventMetaKey(event) &&
        !Browser.eventCtrlKey(event) &&
        !Browser.eventAltKey(event)
      ) {
        switch Browser.eventKey(event) {
        | "1" => Route.go(lastQuery->Option.mapOr(Route.Home, q => Route.Results(q)))
        | "2" => Route.go(Templates(None))
        | "3" => Route.go(Status)
        | _ => ()
        }
      }
    Browser.addDocumentListener("keydown", onKey)
    Some(() => Browser.removeDocumentListener("keydown", onKey))
  }, [lastQuery])

  let logsHref = Route.toHash(lastQuery->Option.mapOr(Route.Home, q => Route.Results(q)))

  <div className="app">
    <Banner server />
    <div className="app-body">
      <Sidebar route logsHref collapsed onCollapse=setCollapsed theme onTheme=setTheme server />
      <main className="page">
        {switch route {
        | Home => <LogsHome server />
        | Results(q) => <LogsResults key="results" q server />
        | Templates(selected) => <TemplatesPage selected />
        | Status => <StatusPage server />
        }}
      </main>
    </div>
  </div>
}
