// Light or dark: the stored choice (`kibbr.theme`), or else the system's. viewer.html sets
// it before the first paint; this keeps it current.

type t = [#light | #dark]

let systemQuery = "(prefers-color-scheme: dark)"

let system = (): t =>
  try Browser.matchMedia(systemQuery)->Browser.mediaMatches ? #dark : #light catch {
  | _ => #light
  }

let current = (): t => Storage.theme()->Option.getOr(system())

let apply = (theme: t) =>
  Browser.setTheme(
    switch theme {
    | #dark => "dark"
    | #light => "light"
    },
  )

let useTheme = () => {
  let (theme, setTheme) = React.useState(current)
  React.useEffect1(() => {
    apply(theme)
    None
  }, [theme])
  React.useEffect0(() => {
    // With no stored choice the page follows the system as it changes; a choice in another
    // tab is followed too.
    let follow = _ => setTheme(_ => current())
    let media = try Some(Browser.matchMedia(systemQuery)) catch {
    | _ => None
    }
    media->Option.forEach(m => m->Browser.addMediaListener("change", follow))
    Browser.addWindowListener("storage", follow)
    Some(
      () => {
        media->Option.forEach(m => m->Browser.removeMediaListener("change", follow))
        Browser.removeWindowListener("storage", follow)
      },
    )
  })
  let choose = (theme: t) => {
    Storage.setTheme(theme)
    setTheme(_ => theme)
  }
  (theme, choose)
}
