// A 28×28 button holding only an icon; its label is the `aria-label` and the tooltip
// (components/controls.md).

@react.component
let make = (~label: string, ~svg: string, ~onClick) =>
  <button type_="button" className="icon-button" ariaLabel=label title=label onClick>
    <Icon svg />
  </button>
