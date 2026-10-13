# Things to try

The type ladder, by hand. Each command is followed by what this build prints.

```
echo '{"event":"login_failed","user":{"id":"u42","email":"u42@corp.example.com"},"order_id":1700000000,"tags":["a","b"],"note":"","deleted_at":null}' | kibbr parse -
```

Parses a line you wrote: `order_id` comes out `Datetime 1700000000 (= 2023-11-14T22:13:20Z)`.

- "Exactly ten digits" sits above "parses as i64": a timestamp misread as an integer turns
  every time-range query into a full scan, and an order ID called a timestamp is the price.
- Change the value to `170000000` and it is `Int64`.

```
kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - | grep -c "(= "
```

How often the ladder re-read a value across a run: `905`.

```
kibbr gen-logs --lines 1000 --seed 42 | kibbr parse - | awk '/^  / && $1 != "fingerprint" {print $2}' | sort | uniq -c | sort -rn
```

The types that show up: 3295 String, 1573 Int64, 1519 Datetime, 488 Float64, 403 Ip, 397 Uuid, 271 Bool, 245 Null.

```
kibbr gen-logs --lines 1000 --templates 12 --seed 42 | kibbr parse - | tail -1
```

The template count: `1000 lines, 356 templates`. Twelve shapes are not twelve fingerprints -
an optional field present on one line and absent on the next is a different key set. That
number is what the schema-merging bucket bound has to survive.

```
kibbr gen-logs --lines 3 --templates 1 --seed 1 | kibbr parse -
```

Template 0 is a fixed kitchen sink: nested objects, nulls, empty strings, unicode, arrays, a real timestamp and a fake one on every line.

```
diff <(kibbr gen-logs --lines 5000 --templates 40 --seed 99) \
     <(kibbr gen-logs --lines 5000 --templates 40 --seed 99)
```

Reproducibility: prints nothing and exits 0.

```
echo '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":1}}}}}}}}}' | kibbr parse -
echo 'not json' | kibbr parse -
echo '[1,2,3]' | kibbr parse -
```

Breaks it on purpose; each line is refused by number and the run goes on:

```
kibbr: line 1: nesting deeper than 8 levels at key `a.b.c.d.e.f.g.h`
kibbr: line 1: invalid JSON at offset 1: expected ident
kibbr: line 1: top-level JSON value is an array, expected an object
```

each followed by `0 lines, 0 templates, 1 lines rejected` and `kibbr: 1 of 1 lines rejected`.

```
kibbr gen-logs --lines 5 --seed 3 | kibbr parse - --json | jq .
```

One JSON record a line instead of the table: `template_id`, `fingerprint`, `keys`, `values`
(raw text), `typed_values` (decoded - timestamps as Unix nanoseconds), `types`,
`is_new_template`, `raw_line`. Key order is stable run to run, so it diffs cleanly.

```
kibbr parse testdata/fixture.ndjson
```

The fixture: hand-written values, each labelled with the type it must get - the ten-digit
cases, `"NaN"`, `"TRUE"`, `"9223372036854775808"`, `[1700000000]` and the rest.
[`testdata/FIXTURE.md`](../testdata/FIXTURE.md) says why each is there. Add a line in the
same shape and the test suite checks your case too.
