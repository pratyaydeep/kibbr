# `fixture.ndjson`

Hand-written. Each line carries one value under `v` and the type the type ladder is
required to give it under `expect`. `tests/fixture.rs` runs the real binary over the
file and compares the printed type against the label - the outer surface, no
mocks.

The cases that matter, and why:

| Value | Label | Why it is here |
|---|---|---|
| `1000000001` | `Datetime` | A ten-digit **order ID**. It names 2001-09-09, inside the epoch window, so rule 2 still calls it a timestamp. This is the accepted trade the ladder's ordering makes, and the single case most worth staring at. |
| `1700000123` | `Datetime` | Another order ID, naming 2023. Still a wrong guess, by design: the window narrows the mistake, and the column join corrects the rest the first time an id of another shape arrives. |
| `4500000120` / `9876543210` | `Int64` | An order ID naming 2112 and a phone number naming 2282. Ten digits outside the window fall through rule 2 - not refused, not text - and rule 6 reads them as the integers they are. |
| `1705312200` | `Datetime` | A real timestamp, 2024-01-15. The window costs it nothing. |
| `"0946684799"` / `"0946684800"` / `4102444799` / `4102444800` | `Int64` / `Int64` / `Datetime` / `Int64` | The window in seconds, `[2000-01-01, 2100-01-01)`: the last second before it opens, the first inside, the last inside, the first after it closes. The end is not included. The start cannot be reached by rule 2 at all: every ten-digit value naming a moment before 2001-09-09 has a leading zero, and a leading zero is not an epoch (below), so `"0946684800"` is the integer 946684800 rather than 2000-01-01. A window starting later than 2001-09-09 is tested in `tests/fixture.rs`. |
| `"0946684799999"` / `"0946684800000"` / `4102444799999` / `4102444800000` | `Int64` / `Int64` / `Datetime` / `Int64` | The same four edges in milliseconds, thirteen digits. |
| `"0999999999"` / `"1000000000"` | `Int64` / `Datetime` | Either side of the leading zero. `"0999999999"` names 2001-09-09T01:46:39Z, inside the window, and was taken for that date. Seconds since 1970 carry no leading zero, so ten digits that start with one are an id kept at a fixed width, and read as an epoch they come back as a date nobody wrote; rule 6 has them. `"1000000000"` is the first ten digits without one. |
| `"0999999999999"` / `"1000000000000"` | `Int64` / `Datetime` | The same pair in milliseconds, thirteen digits. |
| `"0"` | `Int64` | One digit, so rule 2 never looks at it and the leading-zero rule does not touch it. |
| `9999999999` | `Int64` | Ten digits naming 2286, outside the window. A window reaching that far still would not make it a `Datetime`: nanoseconds in an `i64` run out in 2262, and a moment the type cannot hold is not one, so it falls through to rule 6 rather than being clamped to the last instant the type has. |
| `"0123456789"` | `Int64` | Ten digits with a leading zero: not an epoch, whatever moment it names, so it lands on rule 6. |
| `"1970-01-01T00:00:00Z"` | `Datetime` | Rule 1 has no window. Only bare digits need one, because only bare digits are ambiguous. |
| `"2262-04-11T23:47:16.854775807Z"` / `"2262-04-11T23:47:16.854775808Z"` | `Datetime` / `String` | A `Datetime` is `i64` nanoseconds since 1970, which run out here. The last instant it holds is one; a nanosecond later is text. Rule 1 does not clamp: a value the type cannot hold exactly is not a member of it. |
| `"1677-09-21T00:12:43.145224192Z"` / `"1677-09-21T00:12:43.145224191Z"` | `Datetime` / `String` | The other end: `i64::MIN` nanoseconds is a `Datetime`, a nanosecond before it is text. |
| `"9999-12-31T23:59:59Z"` / `"0001-01-01T00:00:00Z"` | `String` | RFC 3339 reaches year 0 and year 9999, far past both ends. Clamped, these came back as 2262 and 1677 - another value, not another spelling. |
| `"2024-01-15T10:30:59Z"` / `"2024-01-15T10:30:60Z"` / `"2024-01-15T10:30:61Z"` | `Datetime` / `String` / `String` | Seconds run 00 to 59. The parser accepts `:60` as a leap second and holds it as a full second past `:59`, so it came back as `10:31:00` - on a day with no leap second at all. `:61` never parsed. |
| `"2016-12-31T23:59:59.999999999Z"` / `"2016-12-31T23:59:60Z"` / `"2016-12-31T23:59:60.999999999Z"` / `"1990-12-31T15:59:60-08:00"` | `Datetime` / `String` / `String` / `String` | The last nanosecond before a leap second, then real leap seconds: the third with a fraction and the fourth with an offset. The parser holds a leap second as a sub-second part of a full second or more, and a nanosecond count has no instant for it, so each rolled forward into the next minute - here, the next year. They are text, whatever the offset; the nanosecond before stays a `Datetime`. |
| `"2024-01-15T10:30:00.123456789Z"` / `"2024-01-15T10:30:00.1234567891Z"` / `"2024-01-15T10:30:00.1234567890Z"` | `Datetime` / `String` / `String` | Nine fraction digits is nanoseconds, the type's resolution. The parser reads more and drops them, which stores another instant unless every dropped digit is a zero. The rule counts digits rather than reading them, so a tenth digit is text even when it is `0`: whether a value is a `Datetime` never turns on which digit would have been lost. |
| `170000000` / `12345678901` / `170000000000` | `Int64` | Nine, eleven and twelve digits - one digit either side of rule 2 and the whole verdict changes. |
| `-1700000000` | `Int64` | The minus sign is not an ASCII digit, so rule 2 does not fire on a value that is otherwise a timestamp. |
| `"0000000000"` | `Int64` | Ten digits including leading zeros, so not an epoch: the integer 0. |
| `"1700000000"` | `Datetime` | Same verdict reached from a JSON string rather than a JSON number. Rule 2 reads text, not types. |
| `"2024-01-15"` | `String` | A date is not RFC 3339 (no time, no offset) and nothing below rule 1 catches it. Real, and a genuine gap. |
| `"550e8400e29b…"` | `String` | Thirty-two hex digits with the hyphens removed is not the UUID pattern. |
| `"127.0.0.1:8080"` | `String` | `host:port` is not an `IpAddr`. |
| `"1.5"` | `Float64` | Reaches rule 7 only because rule 4 rejects it - dotted-quad shapes and floats overlap by eye, not by parser. |
| `"9223372036854775808"` | `Float64` | `i64::MAX + 1`: rule 6 fails on overflow and rule 7 takes it, losing precision. Nineteen digits, so rule 2 never sees it. |
| `"NaN"` / `"inf"` | `String` | Rust's `f64` parser accepts both. `typed_values` is JSON and JSON has no encoding for either, so rule 7 is held to finite values and these fall to rule 9. |
| `"TRUE"` | `String` | Rule 8 lists exactly four spellings. `TRUE` is not one. |
| `"+5"` | `Int64` | Rust's `i64` parser accepts a leading `+`. |
| `" 42"` | `String` | It does not: leading whitespace is not trimmed anywhere in the ladder. |
| `""` | `String` | An empty string is a value, not an absence. Printed as `""` so it cannot be mistaken for one. |
| `null` | `Null` | An explicit null carries no evidence about the column's type, so the ladder is not run - otherwise it would come back `String` and pin the column to the text `"null"`. |
| `["auth","login"]` | `String` | Arrays are never flattened; the raw JSON text is the value. |
| `[1700000000]` | `String` | And a one-element array of a timestamp stays text - the brackets keep rule 2 away. |
| `"u42@auth.example.com"` | `String` | Rule 5. Nothing containing `@` could reach a later rung anyway, which is why the rule is documentation as much as behaviour. |
