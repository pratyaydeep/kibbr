// The container for every block of data on Logs results and Templates
// (components/panel.md): a titled section, optionally minimised to its header or
// maximised to the whole results area.

type size = {
  minimized: bool,
  maximized: bool,
  onMinimize: unit => unit,
  onMaximize: unit => unit,
}

@react.component
let make = (
  ~title: string,
  ~subtitle: option<string>=?,
  ~controls: option<React.element>=?,
  ~size: option<size>=?,
  ~flex=false,
  ~className="",
  ~children: React.element,
) => {
  let minimized = size->Option.mapOr(false, s => s.minimized && !s.maximized)
  let maximized = size->Option.mapOr(false, s => s.maximized)
  let classes =
    ["panel", flex || maximized ? "is-flex" : "is-fixed", minimized ? "is-minimized" : "", className]
    ->Array.filter(c => c != "")
    ->Array.join(" ")
  <section className=classes ariaLabel={maximized ? title ++ " (maximized)" : title}>
    <div className="panel-head">
      <h2 className="panel-title type-panel-title"> {React.string(title)} </h2>
      {switch subtitle {
      | Some(text) => <span className="panel-subtitle type-caption"> {React.string(text)} </span>
      | None => React.null
      }}
      {switch (controls, size) {
      | (None, None) => React.null
      | _ =>
        <div className="panel-controls">
          {controls->Option.getOr(React.null)}
          {switch size {
          | None => React.null
          | Some(s) if s.maximized =>
            <IconButton
              label={"Restore " ++ title} svg=Icon.panelRestoreSvg onClick={_ => s.onMaximize()}
            />
          | Some(s) =>
            <>
              {s.minimized
                ? <IconButton
                    label={"Expand " ++ title} svg=Icon.panelExpandSvg onClick={_ => s.onMinimize()}
                  />
                : <IconButton
                    label={"Minimize " ++ title}
                    svg=Icon.panelMinimizeSvg
                    onClick={_ => s.onMinimize()}
                  />}
              <IconButton
                label={"Maximize " ++ title} svg=Icon.panelMaximizeSvg onClick={_ => s.onMaximize()}
              />
            </>
          }}
        </div>
      }}
    </div>
    {minimized ? React.null : <div className="panel-body"> children </div>}
  </section>
}
