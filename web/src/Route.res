// Hash routes, so no page path can collide with an API path: `GET /templates` is JSON.
//
//   #/                 Logs home
//   #/?q=<query>       Logs results
//   #/templates        Templates, the most recently seen template selected
//   #/templates/<id>   Templates, that template selected
//   #/status           Status

type t =
  | Home
  | Results(string)
  | Templates(option<int>)
  | Status

@val external encodeURIComponent: string => string = "encodeURIComponent"
@val external decodeURIComponent: string => string = "decodeURIComponent"

let decode = text =>
  try decodeURIComponent(text) catch {
  | _ => text
  }

// The value of `q` in a query string, `+` read as a space as a form would write it.
let queryParam = (search: string) =>
  search
  ->String.split("&")
  ->Array.findMap(pair =>
    switch pair->String.indexOf("=") {
    | -1 => None
    | at =>
      pair->String.slice(~start=0, ~end=at) == "q"
        ? Some(pair->String.slice(~start=at + 1)->String.replaceAll("+", " ")->decode)
        : None
    }
  )

let parse = (hash: string) => {
  let path = hash->String.startsWith("#") ? hash->String.slice(~start=1) : hash
  let (path, search) = switch path->String.indexOf("?") {
  | -1 => (path, "")
  | at => (path->String.slice(~start=0, ~end=at), path->String.slice(~start=at + 1))
  }
  switch path {
  | "" | "/" =>
    switch queryParam(search) {
    | Some(q) if q->String.trim != "" => Results(q)
    | _ => Home
    }
  | "/templates" | "/templates/" => Templates(None)
  | "/status" => Status
  | _ =>
    switch path->String.startsWith("/templates/") {
    | true => Templates(path->String.slice(~start=11)->Int.fromString)
    | false => Home
    }
  }
}

let toHash = route =>
  switch route {
  | Home => "#/"
  | Results(q) => "#/?q=" ++ encodeURIComponent(q)
  | Templates(None) => "#/templates"
  | Templates(Some(id)) => "#/templates/" ++ Int.toString(id)
  | Status => "#/status"
  }

let current = () => parse(Browser.locationHash)

let go = route => Browser.goToHash(toHash(route))

// The route, kept current as the hash changes.
let useRoute = () => {
  let (route, setRoute) = React.useState(current)
  React.useEffect0(() => {
    let onChange = _ => setRoute(_ => current())
    Browser.addWindowListener("hashchange", onChange)
    Some(() => Browser.removeWindowListener("hashchange", onChange))
  })
  route
}
