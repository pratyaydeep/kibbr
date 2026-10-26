// What the console knows about the server it is served from: `/health` and `/stats`,
// asked when the page opens, every ten seconds, and when the window regains focus. The
// sidebar's status row, the banners and the info strips read this.

type t = {
  health: option<Api.health>,
  stats: option<Api.stats>,
}

let empty = {health: None, stats: None}

type state =
  | Healthy
  | Degraded(string)
  | InMemory

// Degraded wins over in memory: the banner and the status row show one state.
let state = (server: t) =>
  switch server.health {
  | Some({status: Some("degraded"), detail}) => Degraded(detail->Option.getOr(""))
  | _ =>
    switch server.stats->Option.flatMap(s => s.storage)->Option.flatMap(s => s.durable) {
    | Some(false) => InMemory
    | _ => Healthy
    }
  }

let refreshEveryMs = 10_000

let useServer = () => {
  let (server, setServer) = React.useState(() => empty)
  React.useEffect0(() => {
    let alive = ref(true)
    let refresh = () => {
      let _ = (
        async () => {
          let health = await Api.health()
          let stats = await Api.stats()
          if alive.contents {
            // A failed request keeps what was known: one dropped poll is not a state.
            setServer(previous => {
              health: switch health {
              | Ok(h) => Some(h)
              | Error(_) => previous.health
              },
              stats: switch stats {
              | Ok(s) => Some(s)
              | Error(_) => previous.stats
              },
            })
          }
        }
      )()
    }
    refresh()
    let timer = Browser.setInterval(refresh, refreshEveryMs)
    let onFocus = _ => refresh()
    Browser.addWindowListener("focus", onFocus)
    Some(
      () => {
        alive := false
        Browser.clearInterval(timer)
        Browser.removeWindowListener("focus", onFocus)
      },
    )
  })
  server
}
