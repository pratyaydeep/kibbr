# Look at it in a browser

```
kibbr serve --data-dir kibbr-data     # then open http://127.0.0.1:7700/
```

`GET /` is a log viewer: the first real client of TrieQL and of the cursor.

- **One file**: `crates/kibbr-server/src/viewer.html`, inline CSS and vanilla JS, compiled
  into the binary with `include_str!`. Nothing to deploy beside the binary, no build step,
  and no request to anywhere but this server - `tests/serve.rs` asserts the last of those.
- Served `no-store`: a viewer cached against a newer server is a debugging tool lying about
  what the database answered.
- **Deliberately small**: no saved queries, no dashboards, no auto-refresh, no auth, no
  persistence of any kind.
- **Rows are the JSON lines they were ingested as**, not a table. `MATCH LOG {}` spans 870
  templates whose columns barely overlap; the table that used to union them was 56 columns
  of mostly "this template has no such column".
- **The keys the query named are highlighted**, and their values too when the query asked
  for a value and the row carries it: a highlight always means "this is why the row is here".
- **Only the lines on screen are in the DOM.** A page is up to `max_rows`; spacers stand in
  for the rest, so a 10,000-row answer paints in about 30ms instead of blocking the tab for
  ten seconds. The cost: the browser's Ctrl+F searches the rendered lines only - ask the
  database instead.
- **The chart is the same query, counted**: `GROUP BY time(...)` at a width chosen to land
  near 60 bars. Clicking a bar narrows the range to that bucket and re-runs. Empty buckets
  are bars of zero, because the engine zero-fills them.
- **The viewer never injects a `LIMIT`.** A query naming none meets `max_rows` and the
  banner says so: this is the client most likely to trip the backstop, and hiding it here
  would hide it everywhere.
- **The range control is authoritative.** It opens on the newest ten minutes of whatever is
  stored, found by asking the engine. Clearing both boxes runs the query with no time clause.
