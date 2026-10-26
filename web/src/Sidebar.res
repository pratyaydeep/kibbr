// The rail: two widths, the menu, and the theme, server status and collapse rows
// (components/sidebar.md). Collapsing hides the words only; every icon stays where it is.

type page =
  | Logs
  | Templates
  | Status

let pageOf = (route: Route.t) =>
  switch route {
  | Home | Results(_) => Logs
  | Templates(_) => Templates
  | Status => Status
  }

// The console's own port: what the healthy status row shows on its right.
let port = () =>
  switch Browser.locationPort {
  | "" => Browser.locationProtocol == "https:" ? "443" : "80"
  | port => port
  }

module Item = {
  @react.component
  let make = (~href, ~icon, ~label, ~shortcut, ~active, ~collapsed) =>
    <a
      href
      className={"rail-item type-nav-item" ++ (active ? " is-active" : "")}
      ariaCurrent=?{active ? Some(#page) : None}
      ariaLabel=?{collapsed ? Some(label) : None}
      title=?{collapsed ? Some(label) : None}>
      <Icon svg=icon />
      <span className="rail-label"> {React.string(label)} </span>
      <span className="rail-key type-shortcut"> {React.string(shortcut)} </span>
    </a>
}

@react.component
let make = (
  ~route: Route.t,
  ~logsHref: string,
  ~collapsed: bool,
  ~onCollapse: bool => unit,
  ~theme: [#light | #dark],
  ~onTheme: [#light | #dark] => unit,
  ~server: Server.t,
) => {
  let page = pageOf(route)
  let kind =
    server.stats
    ->Option.flatMap(s => s.storage)
    ->Option.flatMap(s => s.kind)
    ->Option.getOr("")
  let (dot, label, right, rightClass, tooltip) = switch Server.state(server) {
  | Healthy => (
      "is-ok",
      "kibbr · " ++ kind,
      ":" ++ port(),
      "is-port type-mono-sm",
      `kibbr · ${kind} · durable · healthy · :${port()}`,
    )
  | Degraded(_) => (
      "is-degraded",
      "degraded",
      "writes failing",
      "is-degraded",
      "kibbr · degraded: writes failing, queries still answered",
    )
  | InMemory => (
      "is-memory",
      "in memory",
      "not durable",
      "is-memory",
      "kibbr · in memory: everything is lost when the process exits",
    )
  }
  let alert = dot != "is-ok"
  let nextTheme = theme == #dark ? #light : #dark
  let themeLabel = theme == #dark ? "Light mode" : "Dark mode"
  let themeAria = theme == #dark ? "Switch to light mode" : "Switch to dark mode"

  <aside className={"rail" ++ (collapsed ? " is-collapsed" : "")} ariaLabel="Main menu">
    <div className="rail-head">
      <Icon.Mark />
      <span className="rail-word type-wordmark"> {React.string("kibbr")} </span>
    </div>
    <nav className="rail-nav">
      <Item
        href=logsHref icon=Icon.logsSvg label="Logs" shortcut="1" active={page == Logs} collapsed
      />
      <Item
        href={Route.toHash(Templates(None))}
        icon=Icon.templatesSvg
        label="Templates"
        shortcut="2"
        active={page == Templates}
        collapsed
      />
      <Item
        href={Route.toHash(Status)}
        icon=Icon.statusSvg
        label="Status"
        shortcut="3"
        active={page == Status}
        collapsed
      />
    </nav>
    <div className="rail-foot">
      <button
        type_="button"
        className="rail-item type-nav-item"
        ariaLabel=themeAria
        title=themeAria
        onClick={_ => onTheme(nextTheme)}>
        <Icon svg={theme == #dark ? Icon.themeSunSvg : Icon.themeMoonSvg} />
        <span className="rail-label"> {React.string(themeLabel)} </span>
      </button>
      <a href={Route.toHash(Status)} className="rail-item rail-status type-nav-item" title=tooltip>
        <span className="rail-server">
          <Icon svg=Icon.serverSvg />
          <span className={"rail-dot " ++ dot} />
        </span>
        <span className={"rail-label rail-status-label" ++ (alert ? " is-alert" : "")}>
          {React.string(label)}
        </span>
        <span className={"rail-status-right type-caption " ++ rightClass}>
          {React.string(right)}
        </span>
      </a>
      <button
        type_="button"
        className="rail-item type-nav-item"
        ariaLabel={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        onClick={_ => onCollapse(!collapsed)}>
        <Icon svg={collapsed ? Icon.expandSvg : Icon.collapseSvg} />
        <span className="rail-label"> {React.string("Collapse")} </span>
      </button>
    </div>
  </aside>
}
