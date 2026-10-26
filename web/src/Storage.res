// Browser storage, under the four keys DESIGN.md lists. Every read and write is wrapped:
// storage can be off (a private window, a blocked site), full, or hold something another
// version wrote, and none of that may stop the page.

@val external getItem: string => Nullable.t<string> = "localStorage.getItem"
@val external setItem: (string, string) => unit = "localStorage.setItem"
@val external removeItem: string => unit = "localStorage.removeItem"

let railKey = "kibbr.rail.collapsed"
let themeKey = "kibbr.theme"
let recentKey = "kibbr.recent"
let savedKey = "kibbr.saved"

let get = key =>
  try getItem(key)->Nullable.toOption catch {
  | _ => None
  }

let set = (key, value) =>
  try setItem(key, value) catch {
  | _ => ()
  }

let remove = key =>
  try removeItem(key) catch {
  | _ => ()
  }

// The sidebar: "1" collapsed, "0" (or nothing) expanded.
let railCollapsed = () => get(railKey) == Some("1")
let setRailCollapsed = collapsed => set(railKey, collapsed ? "1" : "0")

// The theme the user chose, if they chose one.
let theme = () =>
  switch get(themeKey) {
  | Some("dark") => Some(#dark)
  | Some("light") => Some(#light)
  | _ => None
  }
let setTheme = theme =>
  set(
    themeKey,
    switch theme {
    | #dark => "dark"
    | #light => "light"
    },
  )

// A JSON array read leniently: an entry of the wrong shape is skipped, not fatal.
let entries = (key, decode) =>
  switch get(key) {
  | None => []
  | Some(text) =>
    switch try Some(JSON.parseOrThrow(text)) catch {
    | _ => None
    } {
    | Some(JSON.Array(items)) => items->Array.filterMap(decode)
    | _ => []
    }
  }

let field = (dict, name) =>
  switch dict->Dict.get(name) {
  | Some(JSON.String(s)) => Some(s)
  | _ => None
  }

type recent = {q: string, at: float}

// Queries run from this browser, newest first, at most twenty.
let recentMax = 20

let recent = () =>
  entries(recentKey, item =>
    switch item {
    | JSON.Object(dict) =>
      switch (field(dict, "q"), dict->Dict.get("at")) {
      | (Some(q), Some(JSON.Number(at))) => Some({q, at})
      | _ => None
      }
    | _ => None
    }
  )

// The query moves to the top when it is run again, rather than being listed twice.
let addRecent = q => {
  let kept = recent()->Array.filter(r => r.q != q)
  let next = [{q, at: Browser.now()}]->Array.concat(kept)->Array.slice(~start=0, ~end=recentMax)
  let json = next->Array.map(r => {
    let dict = Dict.make()
    dict->Dict.set("q", JSON.String(r.q))
    dict->Dict.set("at", JSON.Number(r.at))
    JSON.Object(dict)
  })
  set(recentKey, JSON.stringify(JSON.Array(json)))
}

type saved = {q: string, name: string}

let saved = () =>
  entries(savedKey, item =>
    switch item {
    | JSON.Object(dict) =>
      switch (field(dict, "q"), field(dict, "name")) {
      | (Some(q), Some(name)) => Some({q, name})
      | _ => None
      }
    | _ => None
    }
  )
