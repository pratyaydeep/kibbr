"""TrieDB's frontend, ported to Python: one JSON line -> tokens, fingerprint, typed keys.

The binary has `parse --json`, and `00_frontend_agree.py` uses it as the oracle. The port
exists anyway because script 04 has to change which keys are promoted, and the binary's
promoted-key list is a `const` - so the rule is a parameter here, defaulting to what
`src/flatten.rs` compiles in.

Ported from src/flatten.rs (flattening, escaping, promotion, template id),
src/key.rs (KeyPath rendering) and src/infer.rs (the type ladder). Section numbers refer
to .dev/TRIEDB.md.

The one deliberate divergence: the depth limits are checked against the parsed value
rather than by re-scanning the raw bytes. The binary scans bytes first so that a
100,000-deep array is rejected by TrieDB's rule instead of serde_json's recursion limit.
Nothing in this lab feeds hostile input, and `json.loads` has its own ceiling well below
the point where the distinction matters.
"""

from __future__ import annotations

import ipaddress
import json
import math
import re
from dataclasses import dataclass, field
from typing import Callable, Iterable

# --- src/flatten.rs constants -------------------------------------------------

MAX_DEPTH = 8  # objects, outside arrays
MAX_ARRAY_DEPTH = 64
DEFAULT_PROMOTED_KEYS = ("event",)
MAX_PROMOTED_LEN = 40  # bytes, not characters - `raw.len()` in Rust

# Unicode White_Space, which is what Rust's `char::is_whitespace` tests. Python's
# `str.isspace()` is a different set - it also takes U+001C..U+001F - and promotion
# rejects values containing whitespace, so the set has to be the one Rust uses.
_RUST_WHITESPACE = frozenset(
    chr(c)
    for c in (
        list(range(0x09, 0x0E))
        + [0x20, 0x85, 0xA0, 0x1680]
        + list(range(0x2000, 0x200B))
        + [0x2028, 0x2029, 0x202F, 0x205F, 0x3000]
    )
)


def _has_whitespace(s: str) -> bool:
    return any(ch in _RUST_WHITESPACE for ch in s)


# --- column types (src/types.rs) ----------------------------------------------

STRING = "String"
INT64 = "Int64"
FLOAT64 = "Float64"
DATETIME = "Datetime"
BOOL = "Bool"
UUID = "Uuid"
IP = "Ip"

RFC3339 = "rfc3339"
EPOCH_S = "epoch_s"
EPOCH_MS = "epoch_ms"


def join_types(a, b):
    """The column type join of section 3.2 - commutative, associative, monotonic.

        None  join T        = T
        T     join T        = T
        Int64 join Float64  = Float64
        _     join _        = String

    A column never narrows, so this is the whole rule: the order lines arrived in must
    not decide the column's type.
    """
    if a is None:
        return b
    if b is None:
        return a
    if a == b:
        return a
    if {a, b} == {INT64, FLOAT64}:
        return FLOAT64
    return STRING


def fold_types(types: Iterable):
    acc = None
    for t in types:
        acc = join_types(acc, t)
    return acc


# --- the type ladder (src/infer.rs) -------------------------------------------

# Strict RFC 3339, which is what `chrono::DateTime::parse_from_rfc3339` accepts: a full
# date, a full time, and a mandatory offset. Python's `fromisoformat` is far looser - a
# bare date parses - so it cannot stand in here.
_RFC3339 = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(\.\d+)?"
    r"([Zz]|[+-]\d{2}:\d{2})$"
)
_MONTH_DAYS = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _days_from_civil(y: int, m: int, d: int) -> int:
    """Howard Hinnant's days_from_civil, the inverse of the one in src/gen.rs."""
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def _parse_rfc3339(raw: str):
    """Unix nanoseconds, or None if this is not RFC 3339.

    Saturating at both ends the way `infer` does: i64 nanoseconds cover 1677..2262 while
    RFC 3339 covers year 0..9999, so an out-of-range year is pinned by the sign of its
    seconds rather than both ends landing on i64::MAX.
    """
    m = _RFC3339.match(raw)
    if not m:
        return None
    year, month, day, hh, mm, ss = (int(m.group(i)) for i in range(1, 7))
    if not 1 <= month <= 12 or hh > 23 or mm > 59:
        return None
    if ss > 60:  # chrono accepts a leap second as :60
        return None
    dmax = _MONTH_DAYS[month - 1]
    if month == 2 and (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)):
        dmax = 29
    if not 1 <= day <= dmax:
        return None

    frac, offset = m.group(7), m.group(8)
    secs = _days_from_civil(year, month, day) * 86400 + hh * 3600 + mm * 60 + min(ss, 59)
    if offset not in ("Z", "z"):
        sign = 1 if offset[0] == "+" else -1
        secs -= sign * (int(offset[1:3]) * 3600 + int(offset[4:6]) * 60)
    nanos = int((frac[1:] + "0" * 9)[:9]) if frac else 0
    total = secs * 1_000_000_000 + nanos
    return max(-(2**63), min(2**63 - 1, total))


def _is_epoch_digits(raw: str) -> bool:
    return len(raw) in (10, 13) and all("0" <= c <= "9" for c in raw)


def _is_uuid(raw: str) -> bool:
    """8-4-4-4-12 hex. Version and variant nibbles go unchecked, matching the binary."""
    b = raw.encode("utf-8", "surrogatepass")
    if len(b) != 36:
        return False
    for i, ch in enumerate(b):
        if i in (8, 13, 18, 23):
            if ch != 0x2D:
                return False
        elif not (0x30 <= ch <= 0x39 or 0x41 <= ch <= 0x46 or 0x61 <= ch <= 0x66):
            return False
    return True


def _is_ip(raw: str) -> bool:
    """`IpAddr::from_str`. No zone id: Rust's std rejects `fe80::1%eth0`, Python takes it."""
    if "%" in raw or "/" in raw or _has_whitespace(raw):
        return False
    try:
        ipaddress.ip_address(raw)
        return True
    except ValueError:
        return False


def _is_email(raw: str) -> bool:
    if "@" not in raw:
        return False
    local, _, domain = raw.partition("@")
    if not local or "@" in domain or "." not in domain:
        return False
    if _has_whitespace(raw):
        return False
    return all(label for label in domain.split("."))


_I64 = re.compile(r"^[+-]?[0-9]+$")
# Rust's f64 FromStr grammar. Python's `float()` is wider - it takes underscores
# ("1_0"), surrounding whitespace and unicode digits, none of which Rust accepts.
_F64 = re.compile(r"^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")


def _parse_i64(raw: str):
    if not _I64.match(raw):
        return None
    v = int(raw)
    return v if -(2**63) <= v <= 2**63 - 1 else None


def _parse_f64(raw: str):
    if not _F64.match(raw):
        return None
    try:
        v = float(raw)
    except ValueError:
        return None
    # Rung 7 is held to finite values: inf and NaN have no JSON encoding, so they fall
    # through to rung 9.
    return v if math.isfinite(v) else None


@dataclass
class Typed:
    ty: object
    value: object
    dt_form: object = None

    @property
    def label(self) -> str:
        return self.ty if self.ty is not None else "Null"


def infer(raw: str) -> Typed:
    """Section 3.2's ladder. First match wins; the order is the contract."""
    ns = _parse_rfc3339(raw)
    if ns is not None:  # 1. RFC 3339
        return Typed(DATETIME, ns, RFC3339)

    if _is_epoch_digits(raw):  # 2. ten or thirteen ASCII digits - above rung 6
        per_unit, form = (
            (1_000_000_000, EPOCH_S) if len(raw) == 10 else (1_000_000, EPOCH_MS)
        )
        # `saturating_mul`: 9_999_999_999 seconds in nanoseconds overflows i64.
        return Typed(DATETIME, min(int(raw) * per_unit, 2**63 - 1), form)

    if _is_uuid(raw):  # 3
        return Typed(UUID, raw)
    if _is_ip(raw):  # 4
        return Typed(IP, raw)
    if _is_email(raw):  # 5
        return Typed(STRING, raw)

    i = _parse_i64(raw)
    if i is not None:  # 6
        return Typed(INT64, i)
    f = _parse_f64(raw)
    if f is not None:  # 7
        return Typed(FLOAT64, f)
    if raw in ("true", "True", "false", "False"):  # 8
        return Typed(BOOL, raw in ("true", "True"))
    return Typed(STRING, raw)  # 9


# --- flattening (src/flatten.rs, src/key.rs) ----------------------------------


def render_key(segments) -> str:
    r"""`KeyPath`'s Display. Inside a segment `.` becomes `\.` and `\` becomes `\\`, so an
    unescaped `.` occurs only at a segment boundary. That makes the rendering reversible,
    which is what keeps `{"a.b":1}` and `{"a":{"b":2}}` two different keys.
    """
    out = []
    for i, seg in enumerate(segments):
        if i:
            out.append(".")
        for ch in seg:
            if ch in (".", "\\"):
                out.append("\\")
            out.append(ch)
    return "".join(out)


def _fmt_number(v) -> str:
    """serde_json's rendering of a JSON number, which is what `raw_text` returns.

    Integers keep their value; floats go through the shortest round-tripping repr, which
    is what both ryu (Rust) and Python's repr produce. Exponent spelling is the one place
    the two can differ, so `00_frontend_agree.py` is what certifies this rather than the
    comment.
    """
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if not math.isfinite(v):
        return "null"  # serde_json cannot encode these; unreachable from valid JSON
    r = repr(float(v))
    if "e" in r:
        # Python writes 1e+20 and 1e-07; ryu writes 1e20 and 1e-7.
        mant, _, exp = r.partition("e")
        sign = "-" if exp.startswith("-") else ""
        return f"{mant}e{sign}{int(exp.lstrip('+-'))}"
    return r


def raw_text(v) -> str:
    """The raw text of a leaf. Strings are their own contents, not re-quoted; everything
    else is its JSON encoding, which is how an array survives intact as one value."""
    if isinstance(v, str):
        return v
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return _fmt_number(v)
    # Arrays are never flattened. `separators` matches serde_json's compact form and
    # `ensure_ascii=False` keeps UTF-8 as UTF-8.
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


class DetectorError(Exception):
    def __init__(self, kind: str, key: str = ""):
        super().__init__(f"{kind}{f' at {key}' if key else ''}")
        self.kind = kind
        self.key = key


def flatten(root):
    """Nested objects to sorted (key path, leaf value) pairs. Arrays are leaves.

    Sorted on the *rendered* key, not on the segments: the two are different orders, and
    rendered is the order `keys` is emitted in, so a consumer re-sorting that array lands
    back on the same one.
    """
    if not isinstance(root, dict):
        raise DetectorError("NotAnObject")
    out = []
    _walk(root, [], 1, out)
    out.sort(key=lambda pair: render_key(pair[0]))
    return out


def _walk(obj: dict, path: list, depth: int, out: list) -> None:
    for key, value in obj.items():
        path.append(key)
        if isinstance(value, dict):
            if depth + 1 > MAX_DEPTH:
                raise DetectorError("NestingTooDeep", render_key(path))
            _walk(value, path, depth + 1, out)
        else:
            if isinstance(value, list):
                _check_array_depth(value, 1, path)
            out.append((list(path), value))
        path.pop()


def _check_array_depth(value, depth: int, path: list) -> None:
    if depth > MAX_ARRAY_DEPTH:
        raise DetectorError("ArrayNestingTooDeep", render_key(path))
    if isinstance(value, list):
        for item in value:
            _check_array_depth(item, depth + 1, path)
    elif isinstance(value, dict):
        for item in value.values():
            _check_array_depth(item, depth + 1, path)


# --- promotion ----------------------------------------------------------------

PromotionRule = Callable[[str, Typed, str], bool]


def promoted_keys_rule(keys: Iterable) -> PromotionRule:
    """The binary's rule with a different key list. Everything else - String only, non
    empty, at most 40 bytes, no `|`, no whitespace - is unchanged, because those guards
    are about the fingerprint staying readable and splittable, not about which keys.
    """
    allowed = frozenset(keys)

    def rule(leaf: str, typed: Typed, raw: str) -> bool:
        return (
            leaf in allowed
            and typed.ty == STRING
            and raw != ""
            and len(raw.encode("utf-8", "surrogatepass")) <= MAX_PROMOTED_LEN
            and "|" not in raw
            and not _has_whitespace(raw)
        )

    return rule


DEFAULT_RULE = promoted_keys_rule(DEFAULT_PROMOTED_KEYS)
PROMOTE_NOTHING = promoted_keys_rule(())


# --- template id (src/flatten.rs) ---------------------------------------------

_FNV_OFFSET = 0xCBF2_9CE4_8422_2325
_FNV_PRIME = 0x0000_0100_0000_01B3
_MASK = 0xFFFF_FFFF_FFFF_FFFF


def _fnv1a(h: int, data: bytes) -> int:
    for b in data:
        h ^= b
        h = (h * _FNV_PRIME) & _MASK
    return h


def _feed(h: int, text: str) -> int:
    b = text.encode("utf-8", "surrogatepass")
    h = _fnv1a(h, len(b).to_bytes(8, "little"))
    return _fnv1a(h, b)


def template_id(tokens) -> int:
    """FNV-1a over the token list of `(promoted, key, value)` triples.

    Over the tokens, never over the joined fingerprint. `" | "` is not a sequence a key
    is obliged to stay out of, so hashing the join gives one id to `{"a | b":1}` and
    `{"a":1,"b":1}`. A length prefix makes a boundary counted rather than searched for,
    and the kind tag separates a literal key `event=login` from a promoted one.
    """
    h = _FNV_OFFSET
    for is_promoted, key, value in tokens:
        h = _fnv1a(h, b"\x01" if is_promoted else b"\x00")
        h = _feed(h, key)
        if is_promoted:
            h = _feed(h, value)
    return h


# --- the whole frontend -------------------------------------------------------


@dataclass
class ParsedLog:
    template_id: int
    fingerprint: str
    keys: list  # sorted, flattened, promoted keys included
    values: dict  # raw text of every value
    typed: dict  # only the keys that stayed values
    promoted: list = field(default_factory=list)

    @property
    def types(self) -> dict:
        """The `types` map `parse --json` prints: value keys only, `Null` for a JSON null."""
        return {k: t.label for k, t in self.typed.items()}

    def value_keys(self) -> list:
        promoted = set(self.promoted)
        return [k for k in self.keys if k not in promoted]

    def column_type(self, key: str):
        """What this line contributes to `key`'s column-type join.

        `None` where the value was JSON null: the ladder is not run on a null and it
        claims nothing. A promoted key carries its value on the path instead of in
        `typed`, and promotion required the ladder to say String, so that is what it
        contributes - the key is still on the path, and the path is the key set.
        """
        t = self.typed.get(key)
        if t is not None:
            return t.ty
        return STRING if key in self.promoted else None


def parse_obj(obj, rule: PromotionRule = DEFAULT_RULE) -> ParsedLog:
    """Flatten, type, promote, fingerprint - sections 3.1 and 3.2 over a parsed object."""
    pairs = flatten(obj)
    keys, values, typed, promoted, tokens, parts = [], {}, {}, [], [], []

    for path, leaf in pairs:
        key = render_key(path)
        raw = raw_text(leaf)
        # An explicit JSON null carries no evidence about the column's type, so the
        # ladder is not run on it - it would come back String and pin the column to the
        # four-character text "null".
        t = Typed(None, None, None) if leaf is None else infer(raw)

        # The leaf segment, not the rendered key: splitting the rendered key on `.`
        # would read `{"a.event":"login"}` as a nested path and promote it.
        if rule(path[-1] if path else "", t, raw):
            tokens.append((True, key, raw))
            promoted.append(key)
            parts.append(f"{key}={raw}")
        else:
            tokens.append((False, key, ""))
            typed[key] = t
            parts.append(key)

        values[key] = raw
        keys.append(key)

    return ParsedLog(
        template_id=template_id(tokens),
        fingerprint=" | ".join(parts),
        keys=keys,
        values=values,
        typed=typed,
        promoted=promoted,
    )


def parse_line(line, rule: PromotionRule = DEFAULT_RULE) -> ParsedLog:
    if isinstance(line, bytes):
        line = line.decode("utf-8")
    return parse_obj(json.loads(line), rule)
