// Just enough TrieQL to take a typed query apart: the server's lexer, mirrored
// (crates/kibbr-engine/src/trieql.rs), and its clauses in their order -
//
//   MATCH|COUNT LOG {pattern} [SINCE d | BETWEEN t AND t] [GROUP BY …] [LIMIT n] [AFTER "c"]
//
// The console builds the Rows and Chart requests from the parts. The server stays the
// judge of what is valid: a query this cannot take apart is sent as it was typed, and the
// parser's message comes back.

type kind =
  | Open
  | Close
  | LParen
  | RParen
  | Colon
  | Comma
  | Star
  // A quoted string, unescaped.
  | Str(string)
  // A bare run: a keyword, a duration, a datetime, a group key.
  | Word(string)

type token = {kind: kind, start: int, end_: int}

let delims = ["{", "}", "(", ")", ",", "\"", "*"]
let isSpace = c => c == " " || c == "\t" || c == "\n" || c == "\r"

// The string body from just after its opening quote; `None` when it never closes.
let readString = (src, from) => {
  let rec go = (at, acc) =>
    if at >= String.length(src) {
      None
    } else {
      switch src->String.charAt(at) {
      | "\"" => Some((acc, at + 1))
      | "\\" =>
        switch src->String.charAt(at + 1) {
        | "n" => go(at + 2, acc ++ "\n")
        | "t" => go(at + 2, acc ++ "\t")
        | "r" => go(at + 2, acc ++ "\r")
        | "u" =>
          let hex = src->String.slice(~start=at + 2, ~end=at + 6)
          switch Int.fromString(hex, ~radix=16) {
          | Some(code) => go(at + 6, acc ++ String.fromCharCode(code))
          | None => None
          }
        | "" => None
        | c => go(at + 2, acc ++ c)
        }
      | c => go(at + 1, acc ++ c)
      }
    }
  go(from, "")
}

// Tokens with their source offsets, or `None` when the text does not lex.
let lex = (src: string) => {
  let out = []
  let rec go = at =>
    if at >= String.length(src) {
      Some(out)
    } else {
      let c = src->String.charAt(at)
      let single = kind => {
        out->Array.push({kind, start: at, end_: at + 1})
        go(at + 1)
      }
      switch c {
      | c if isSpace(c) => go(at + 1)
      | "{" => single(Open)
      | "}" => single(Close)
      | "(" => single(LParen)
      | ")" => single(RParen)
      | ":" => single(Colon)
      | "," => single(Comma)
      | "*" => single(Star)
      | "\"" =>
        switch readString(src, at + 1) {
        | Some((value, next)) =>
          out->Array.push({kind: Str(value), start: at, end_: next})
          go(next)
        | None => None
        }
      | _ =>
        // `:` does not end a word: an RFC 3339 datetime holds two of them. A word that
        // starts with one is the pattern's colon.
        let rec word = i =>
          if i >= String.length(src) {
            i
          } else {
            let ch = src->String.charAt(i)
            isSpace(ch) || delims->Array.includes(ch) || (i == at && ch == ":") ? i : word(i + 1)
          }
        let next = word(at)
        out->Array.push({kind: Word(src->String.slice(~start=at, ~end=next)), start: at, end_: next})
        go(next)
      }
    }
  go(0)
}

let isKeyword = (tok: token, kw) =>
  switch tok.kind {
  | Word(w) => w->String.toUpperCase == kw
  | _ => false
  }

// Seconds in a TrieQL duration: a whole number and one of s, m, h, d.
let durationSeconds = (text: string) => {
  let unit = text->String.slice(~start=-1)
  let n = text->String.slice(~start=0, ~end=-1)
  let digits = n != "" && n->String.split("")->Array.every(c => c >= "0" && c <= "9")
  switch (digits ? Float.fromString(n) : None, unit) {
  | (Some(n), "s") => Some(n)
  | (Some(n), "m") => Some(n *. 60.0)
  | (Some(n), "h") => Some(n *. 3600.0)
  | (Some(n), "d") => Some(n *. 86400.0)
  | _ => None
  }
}

type time =
  | Since(float)
  | Between(float, float)

type parsed = {
  verb: string,
  // The pattern as typed, braces included.
  pattern: string,
  // Each key in the pattern, with its value; `None` for `*`.
  pairs: array<(string, option<string>)>,
  time: option<time>,
  // The time clause as typed, e.g. `SINCE 15m`.
  timeText: option<string>,
  groupText: option<string>,
  limit: option<int>,
}

let parse = (src: string): option<parsed> =>
  switch lex(src) {
  | None => None
  | Some(toks) =>
    let at = ref(0)
    let peek = () => toks->Array.get(at.contents)
    let bump = () => at := at.contents + 1
    let eatKeyword = kw =>
      switch peek() {
      | Some(t) if isKeyword(t, kw) =>
        bump()
        true
      | _ => false
      }
    let sliceFrom = (startTok: token) => {
      let last = toks->Array.getUnsafe(at.contents - 1)
      src->String.slice(~start=startTok.start, ~end=last.end_)
    }
    let verb = switch peek() {
    | Some(t) if isKeyword(t, "MATCH") || isKeyword(t, "COUNT") =>
      bump()
      switch t.kind {
      | Word(w) => Some(w->String.toUpperCase)
      | _ => None
      }
    | _ => None
    }
    switch (verb, eatKeyword("LOG"), peek()) {
    | (Some(verb), true, Some({kind: Open} as open_)) =>
      bump()
      // The pattern: quoted keys, each with a quoted value or `*`.
      let pairs = []
      let rec pattern = () =>
        switch peek() {
        | Some({kind: Close}) =>
          bump()
          true
        | Some({kind: Str(key)}) =>
          bump()
          switch (peek(), toks->Array.get(at.contents + 1)) {
          | (Some({kind: Colon}), Some({kind: Str(value)})) =>
            bump()
            bump()
            pairs->Array.push((key, Some(value)))
            afterPair()
          | (Some({kind: Colon}), Some({kind: Star})) =>
            bump()
            bump()
            pairs->Array.push((key, None))
            afterPair()
          | _ => false
          }
        | _ => false
        }
      and afterPair = () =>
        switch peek() {
        | Some({kind: Comma}) =>
          bump()
          pattern()
        | Some({kind: Close}) =>
          bump()
          true
        | _ => false
        }
      if !pattern() {
        None
      } else {
        let patternText = sliceFrom(open_)
        // The time clause.
        let timeStart = peek()
        let time = if eatKeyword("SINCE") {
          switch peek() {
          | Some({kind: Word(d)}) =>
            bump()
            durationSeconds(d)->Option.map(s => Since(s))
          | _ => None
          }
        } else if eatKeyword("BETWEEN") {
          switch peek() {
          | Some({kind: Word(from)}) =>
            bump()
            if eatKeyword("AND") {
              switch peek() {
              | Some({kind: Word(to_)}) =>
                bump()
                let from = Date.fromString(from)->Date.getTime
                let to_ = Date.fromString(to_)->Date.getTime
                Float.isNaN(from) || Float.isNaN(to_) ? None : Some(Between(from, to_))
              | _ => None
              }
            } else {
              None
            }
          | _ => None
          }
        } else {
          None
        }
        let timeText = switch (time, timeStart) {
        | (Some(_), Some(t)) => Some(sliceFrom(t))
        | _ => None
        }
        // GROUP BY runs to LIMIT, AFTER or the end.
        let groupStart = peek()
        let groupText = if eatKeyword("GROUP") {
          let rec skip = () =>
            switch peek() {
            | Some(t) if !isKeyword(t, "LIMIT") && !isKeyword(t, "AFTER") =>
              bump()
              skip()
            | _ => ()
            }
          skip()
          groupStart->Option.map(sliceFrom)
        } else {
          None
        }
        let limit = if eatKeyword("LIMIT") {
          switch peek() {
          | Some({kind: Word(n)}) =>
            bump()
            Int.fromString(n)
          | _ => None
          }
        } else {
          None
        }
        // An `AFTER` the user typed is dropped: paging writes its own.
        if eatKeyword("AFTER") {
          bump()
        }
        // A clause that did not parse leaves tokens over: the query goes as typed.
        let clean = at.contents == Array.length(toks)
        let timeOk = timeStart->Option.mapOr(true, t =>
          !isKeyword(t, "SINCE") && !isKeyword(t, "BETWEEN") || time != None
        )
        clean && timeOk
          ? Some({verb, pattern: patternText, pairs, time, timeText, groupText, limit})
          : None
      }
    | _ => None
    }
  }

// The range in seconds, when the query names one.
let rangeSeconds = (p: parsed) =>
  switch p.time {
  | Some(Since(s)) => Some(s)
  | Some(Between(from, to_)) => Some((to_ -. from) /. 1000.0)
  | None => None
  }

// The chart's bucket: the range in ninety, rounded up to a whole second.
let bucketSeconds = (range: float) => Math.max(1.0, Math.ceil(range /. 90.0))

let pageSize = 200

// Rows: `MATCH LOG {filter} {time} LIMIT 200`, any GROUP BY dropped; a next page adds the
// previous answer's cursor.
let rowsQuery = (p: parsed, ~after: option<string>) => {
  let time = p.timeText->Option.mapOr("", t => " " ++ t)
  let after = after->Option.mapOr("", c => " AFTER " ++ JSON.stringify(JSON.String(c)))
  `MATCH LOG ${p.pattern}${time} LIMIT ${Int.toString(pageSize)}${after}`
}

// Chart: `COUNT LOG {filter} {time} GROUP BY time({bucket}), level`.
let chartQuery = (p: parsed) =>
  switch (p.timeText, rangeSeconds(p)) {
  | (Some(time), Some(range)) =>
    let bucket = bucketSeconds(range)
    Some(`COUNT LOG ${p.pattern} ${time} GROUP BY time(${Float.toFixed(bucket, ~digits=0)}s), level`)
  | _ => None
  }

// The keys and the literal values of the pattern: what the rows highlight.
let highlights = (p: parsed) => (
  p.pairs->Array.map(((k, _)) => k),
  p.pairs->Array.filterMap(((_, v)) => v),
)

// ── Syntax colours for the query bar ────────────────────────────────────────────────

type colour =
  | Keyword
  | Value
  | Plain

type segment = {text: string, colour: colour}

let keywords = ["MATCH", "COUNT", "LOG", "SINCE", "BETWEEN", "AND", "GROUP", "BY", "LIMIT", "AFTER"]

// The source cut into coloured runs, whitespace kept, so the runs laid end to end are the
// query exactly. Text that does not lex is one plain run.
let colourise = (src: string) =>
  switch lex(src) {
  | None => [{text: src, colour: Plain}]
  | Some(toks) =>
    let out = []
    let push = (text, colour) =>
      if text != "" {
        switch out->Array.at(-1) {
        // Neighbouring runs of one colour merge, so a keyword pair reads as one.
        | Some(last) if last.colour == colour =>
          out->Array.setUnsafe(Array.length(out) - 1, {text: last.text ++ text, colour})
        | _ => out->Array.push({text, colour})
        }
      }
    let cursor = ref(0)
    toks->Array.forEachWithIndex((tok, i) => {
      let gap = src->String.slice(~start=cursor.contents, ~end=tok.start)
      let text = src->String.slice(~start=tok.start, ~end=tok.end_)
      let previous = i > 0 ? toks->Array.get(i - 1) : None
      let colour = switch tok.kind {
      | Word(w) if keywords->Array.includes(w->String.toUpperCase) => Keyword
      | Word(w) if durationSeconds(w) != None => Value
      // A string after a colon is a value; a string before one is a key.
      | Str(_) =>
        switch previous {
        | Some({kind: Colon}) => Value
        | _ => Plain
        }
      | _ => Plain
      }
      // The space between two keywords takes their colour, so "MATCH LOG" is one run.
      let gapColour = switch (out->Array.at(-1), colour) {
      | (Some({colour: Keyword}), Keyword) => Keyword
      | _ => Plain
      }
      push(gap, gapColour)
      push(text, colour)
      cursor := tok.end_
    })
    push(src->String.slice(~start=cursor.contents), Plain)
    out
  }
