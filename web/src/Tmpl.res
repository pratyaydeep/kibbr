// A template's path read the way the trie writes it (crates/kibbr-core/src/flatten.rs):
// the promoted `key=value` tokens first, split at the first unescaped `=`, then a token for
// each other key.

let splitToken = (token: string) => {
  let rec find = at =>
    if at >= String.length(token) {
      None
    } else {
      switch token->String.charAt(at) {
      | "\\" => find(at + 2)
      | "=" => Some(at)
      | _ => find(at + 1)
      }
    }
  find(0)->Option.map(at => (
    token->String.slice(~start=0, ~end=at)->String.replaceAll("\\=", "="),
    token->String.slice(~start=at + 1),
  ))
}

// The promoted keys and their values, in path order.
let promoted = (t: Api.template) => t.path->Array.filterMap(splitToken)

// The tokens that are keys only: what a leaf of the tree shows.
let keyTokens = (t: Api.template) => t.path->Array.filter(token => splitToken(token) == None)

// The template as one line: its promoted `key=value`s, or its keys when it has none.
let name = (t: Api.template) => {
  let pairs = t.path->Array.filter(token => splitToken(token) != None)
  Array.length(pairs) > 0 ? pairs->Array.join(" · ") : keyTokens(t)->Array.join(" · ")
}

let leafLabel = (t: Api.template) => keyTokens(t)->Array.join(" · ")

let quoted = s => JSON.stringify(JSON.String(s))

// The column the template takes its time from, when that is a column.
let timeColumn = (t: Api.template) =>
  switch t.timeSource {
  | Some(source) if source->String.startsWith("column:") => Some(source->String.slice(~start=7))
  | _ => None
  }

// The path as a filter that walks to this template: its promoted pairs. A template with
// nothing promoted cannot be named that way, so the filter asks for one of its columns to
// be present (`*`) instead, one that is not its time.
let filter = (t: Api.template) => {
  let pairs = promoted(t)->Array.map(((k, v)) => quoted(k) ++ ": " ++ quoted(v))
  let pairs = if Array.length(pairs) > 0 {
    pairs
  } else {
    let time = timeColumn(t)
    switch t.columns->Array.find(c => Some(c.name) != time) {
    | Some(c) => [quoted(c.name) ++ ": *"]
    | None => []
    }
  }
  "{" ++ pairs->Array.join(", ") ++ "}"
}

let lastSeenMs = (t: Api.template) =>
  t.lastSeen->Option.map(s => Date.fromString(s)->Date.getTime)->Option.getOr(0.0)
