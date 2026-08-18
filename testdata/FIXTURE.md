# `fixture.ndjson`

Hand-written. Each line carries one value under `v` and the type the type ladder is
required to give it under `expect`. `tests/fixture.rs` runs the real binary over the
file and compares the printed type against the label - the outer surface, no
mocks.

The cases that matter, and why:

| Value | Label | Why it is here |
|---|---|---|
| `1000000001` | `Datetime` | A ten-digit **order ID**. Rule 2 calls it a timestamp. This is the accepted trade the ladder's ordering makes, and the single case most worth staring at. |
| `9999999999` | `Datetime` | Ten digits, but 9 999 999 999 seconds in nanoseconds overflows `i64` (nanos run out in 2262). The type call stands; the instant saturates. |
| `170000000` / `12345678901` / `170000000000` | `Int64` | Nine, eleven and twelve digits - one digit either side of rule 2 and the whole verdict changes. |
| `-1700000000` | `Int64` | The minus sign is not an ASCII digit, so rule 2 does not fire on a value that is otherwise a timestamp. |
| `"0000000000"` | `Datetime` | Ten digits including leading zeros; epoch zero is still a timestamp. |
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
