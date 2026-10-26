// Number and time formats, as data-display.md's table gives them.

@send external toLocaleString: (float, string, {..}) => string = "toLocaleString"

// 412,880,114. Always en-US separators: the design is written with them.
let commas = (n: float) => n->Math.round->toLocaleString("en-US", {"maximumFractionDigits": 0})
let commasInt = (n: int) => commas(Int.toFloat(n))

// One decimal, dropped when it is .0 or when the number has three digits already.
let shortNumber = (v: float) =>
  if v >= 100.0 {
    Float.toFixed(v, ~digits=0)
  } else {
    let text = Float.toFixed(v, ~digits=1)
    text->String.endsWith(".0") ? text->String.slice(~start=0, ~end=-2) : text
  }

// 41.2M · 204K · 912K. Under a thousand, the number itself.
let compact = (n: float) => {
  let abs = Math.abs(n)
  if abs >= 1e9 {
    shortNumber(n /. 1e9) ++ "B"
  } else if abs >= 1e6 {
    shortNumber(n /. 1e6) ++ "M"
  } else if abs >= 1e3 {
    shortNumber(n /. 1e3) ++ "K"
  } else {
    commas(n)
  }
}

// 1,240/s in full under 10,000; 118k/s above it.
let rate = (perSecond: float) =>
  if perSecond < 10_000.0 {
    commas(perSecond) ++ "/s"
  } else if perSecond < 1e6 {
    shortNumber(perSecond /. 1e3) ++ "k/s"
  } else {
    shortNumber(perSecond /. 1e6) ++ "M/s"
  }

// 3.1 MiB · 18.4 GiB · 0 B: binary units, one decimal.
let bytes = (n: float) => {
  let units = ["KiB", "MiB", "GiB", "TiB", "PiB"]
  if n < 1024.0 {
    commas(n) ++ " B"
  } else {
    let rec step = (value, i) =>
      value >= 1024.0 && i < Array.length(units) - 1 ? step(value /. 1024.0, i + 1) : (value, i)
    let (value, i) = step(n /. 1024.0, 0)
    let text = Float.isFinite(value) && Math.floor(value) == value ? Float.toFixed(value, ~digits=0) : shortNumber(value)
    text ++ " " ++ units->Array.getUnsafe(i)
  }
}

// 100 ms · 30 s · 5 min: the largest unit the value fills.
let duration = (ms: float) =>
  if ms < 1.0 && ms > 0.0 {
    Float.toFixed(ms, ~digits=1) ++ " ms"
  } else if ms < 1000.0 {
    shortNumber(ms) ++ " ms"
  } else if ms < 60_000.0 {
    shortNumber(ms /. 1000.0) ++ " s"
  } else if ms < 3_600_000.0 {
    shortNumber(ms /. 60_000.0) ++ " min"
  } else {
    shortNumber(ms /. 3_600_000.0) ++ " h"
  }

// 4d 2h 13m: the larger units only once they are reached.
let uptime = (seconds: float) => {
  let total = Math.floor(seconds /. 60.0)
  let days = Math.floor(total /. 1440.0)
  let hours = Math.floor((total -. days *. 1440.0) /. 60.0)
  let minutes = total -. days *. 1440.0 -. hours *. 60.0
  let part = (n, unit) => Float.toFixed(n, ~digits=0) ++ unit
  if days > 0.0 {
    `${part(days, "d")} ${part(hours, "h")} ${part(minutes, "m")}`
  } else if hours > 0.0 {
    `${part(hours, "h")} ${part(minutes, "m")}`
  } else {
    part(minutes, "m")
  }
}

let pad = (n: int, width) => Int.toString(n)->String.padStart(width, "0")

// A time stamp as a Date, if it reads as one: RFC 3339 text, or epoch seconds or
// milliseconds as numbers.
let parseTime = (value: JSON.t) =>
  switch value {
  | JSON.String(text) =>
    let date = Date.fromString(text)
    Float.isNaN(date->Date.getTime) ? None : Some(date)
  | JSON.Number(n) =>
    // Ten digits are seconds, thirteen milliseconds: the same ladder the parser climbs.
    let ms = n < 1e11 ? n *. 1000.0 : n
    Some(Date.fromTime(ms))
  | _ => None
  }

// 15:10:02.114, in the browser's time zone.
let clock = (date: Date.t) =>
  `${pad(date->Date.getHours, 2)}:${pad(date->Date.getMinutes, 2)}:${pad(
      date->Date.getSeconds,
      2,
    )}.${pad(date->Date.getMilliseconds, 3)}`

// 15:10:02, for "as of".
let clockSeconds = (date: Date.t) =>
  `${pad(date->Date.getHours, 2)}:${pad(date->Date.getMinutes, 2)}:${pad(date->Date.getSeconds, 2)}`

// 2026-10-10, UTC.
let isoDay = (date: Date.t) =>
  `${Int.toString(date->Date.getUTCFullYear)}-${pad(date->Date.getUTCMonth + 1, 2)}-${pad(
      date->Date.getUTCDate,
      2,
    )}`

// 2026-10-06 22:41, UTC, for "since … UTC".
let isoMinuteUtc = (date: Date.t) =>
  `${isoDay(date)} ${pad(date->Date.getUTCHours, 2)}:${pad(date->Date.getUTCMinutes, 2)}`

let months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

// 12 Mar, in the browser's time zone.
let dayMonth = (date: Date.t) =>
  `${Int.toString(date->Date.getDate)} ${months->Array.getUnsafe(date->Date.getMonth)}`

// "2 s" · "2 min" · "1 h": how long ago, without the "ago".
let age = (thenMs: float, nowMs: float) => {
  let seconds = Math.max(0.0, (nowMs -. thenMs) /. 1000.0)
  if seconds < 60.0 {
    Float.toFixed(Math.floor(seconds), ~digits=0) ++ " s"
  } else if seconds < 3600.0 {
    Float.toFixed(Math.floor(seconds /. 60.0), ~digits=0) ++ " min"
  } else {
    Float.toFixed(Math.floor(seconds /. 3600.0), ~digits=0) ++ " h"
  }
}

// 2 s ago · 2 min ago · 1 h ago · yesterday, then the day itself.
let relative = (thenMs: float, nowMs: float) => {
  let hours = (nowMs -. thenMs) /. 3_600_000.0
  if hours < 24.0 {
    age(thenMs, nowMs) ++ " ago"
  } else if hours < 48.0 {
    "yesterday"
  } else {
    isoDay(Date.fromTime(thenMs))
  }
}
