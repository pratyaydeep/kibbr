// Every icon and logo of the design, as drawn (src/assets, copied from the design handoff
// with only the provenance metadata removed). Inlined as markup, so `currentColor` takes the
// colour of the text around it.

@module("./assets/icons/collapse.svg?raw") external collapseSvg: string = "default"
@module("./assets/icons/expand.svg?raw") external expandSvg: string = "default"
@module("./assets/icons/logs.svg?raw") external logsSvg: string = "default"
@module("./assets/icons/panel-expand.svg?raw") external panelExpandSvg: string = "default"
@module("./assets/icons/panel-maximize.svg?raw") external panelMaximizeSvg: string = "default"
@module("./assets/icons/panel-minimize.svg?raw") external panelMinimizeSvg: string = "default"
@module("./assets/icons/panel-restore.svg?raw") external panelRestoreSvg: string = "default"
@module("./assets/icons/refresh.svg?raw") external refreshSvg: string = "default"
@module("./assets/icons/server.svg?raw") external serverSvg: string = "default"
@module("./assets/icons/status.svg?raw") external statusSvg: string = "default"
@module("./assets/icons/templates.svg?raw") external templatesSvg: string = "default"
@module("./assets/icons/theme-moon.svg?raw") external themeMoonSvg: string = "default"
@module("./assets/icons/theme-sun.svg?raw") external themeSunSvg: string = "default"
@module("./assets/icons/warning.svg?raw") external warningSvg: string = "default"
@module("./assets/logo/mark-dark.svg?raw") external markDarkSvg: string = "default"
@module("./assets/logo/mark-light.svg?raw") external markLightSvg: string = "default"

@react.component
let make = (~svg: string, ~className="") =>
  <span
    className={"icon " ++ className} ariaHidden=true dangerouslySetInnerHTML={{"__html": svg}}
  />

// The mark for each theme; CSS shows the one that matches.
module Mark = {
  @react.component
  let make = () =>
    <span className="rail-mark">
      <span
        className="icon only-light" ariaHidden=true dangerouslySetInnerHTML={{"__html": markLightSvg}}
      />
      <span
        className="icon only-dark" ariaHidden=true dangerouslySetInnerHTML={{"__html": markDarkSvg}}
      />
    </span>
}
