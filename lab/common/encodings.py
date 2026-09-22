"""Column encodings, each producing real bytes so the sizes are counted and not modelled.

The baseline is the `.col` plain chunk, exactly: a null bitmap when any row is absent,
then Str as [u32 len][bytes], I64/F64/DT as 8 bytes, Bool as 1 byte. Every other encoder
here is measured against that, and every one can be run with zstd on top - which is the
only speed figure in this lab's storage scripts, because zstd is a C library.

An encoder returns None when it does not apply to the column, rather than a large number.
"Not applicable" and "applicable and bad" are different facts and BEST_PER_CHUNK has to
tell them apart.
"""

from __future__ import annotations

import math
import re
import struct
from dataclasses import dataclass

import zstandard as zstd

from .frontend import BOOL, DATETIME, FLOAT64, INT64, IP, STRING, UUID

_ZSTD: dict = {}


def zstd_bytes(data: bytes, level: int = 3) -> bytes:
    c = _ZSTD.get(level)
    if c is None:
        c = _ZSTD[level] = zstd.ZstdCompressor(level=level)
    return c.compress(data)


# --- primitives ------------------------------------------------------------------


def bitmap(present: list) -> bytes:
    """The `.col` null bitmap: bit i set means row i is PRESENT, lowest bit first."""
    out = bytearray(math.ceil(len(present) / 8))
    for i, p in enumerate(present):
        if p:
            out[i >> 3] |= 1 << (i & 7)
    return bytes(out)


def varint(n: int) -> bytes:
    """LEB128 over a zigzagged signed value, so a small negative delta stays small."""
    z = (n << 1) ^ (n >> 63) if n < 0 else (n << 1)
    out = bytearray()
    while True:
        b = z & 0x7F
        z >>= 7
        if z:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _index_width(n_distinct: int) -> int:
    if n_distinct <= 256:
        return 1
    if n_distinct <= 65_536:
        return 2
    return 4


def _pack_indices(idx: list, width: int) -> bytes:
    fmt = {1: "B", 2: "<H", 4: "<I"}[width]
    return b"".join(struct.pack(fmt, i) for i in idx)


# --- encoders ---------------------------------------------------------------------
# Each takes the column's values (None for absent) and its type, and returns bytes or
# None. `values` is one chunk.


def enc_plain(values: list, ty) -> bytes:
    present = [v is not None for v in values]
    out = bytearray()
    if not all(present):
        out += bitmap(present)
    for v in values:
        if v is None:
            continue
        if ty in (INT64, DATETIME):
            out += struct.pack("<q", _clamp(v))
        elif ty == FLOAT64:
            out += struct.pack("<d", float(v))
        elif ty == BOOL:
            out += b"\x01" if v else b"\x00"
        else:
            b = str(v).encode("utf-8", "surrogatepass")
            out += struct.pack("<I", len(b)) + b
    return bytes(out)


def _clamp(v: int) -> int:
    return max(-(2**63), min(2**63 - 1, int(v)))


def enc_const(values: list, ty) -> bytes | None:
    """One value on every row, stored once. Applies only when that is true."""
    seen = [v for v in values if v is not None]
    if not seen or any(v != seen[0] for v in seen):
        return None
    present = [v is not None for v in values]
    head = b"" if all(present) else bitmap(present)
    return head + struct.pack("<I", len(values)) + enc_plain([seen[0]], ty)


def enc_dict(values: list, ty) -> bytes | None:
    """A word list plus one small number per row.

    Not applied where it cannot pay: a column whose values are nearly all distinct
    stores every value once in the word list AND an index per row, which is strictly
    worse than plain.
    """
    present = [v is not None for v in values]
    seen = [v for v in values if v is not None]
    if not seen:
        return None
    order: dict = {}
    for v in seen:
        if v not in order:
            order[v] = len(order)
    if len(order) == len(seen) and len(seen) > 4:
        return None
    width = _index_width(len(order))
    words = enc_plain(list(order.keys()), ty)
    idx = _pack_indices([order[v] for v in seen], width)
    head = b"" if all(present) else bitmap(present)
    return head + struct.pack("<IB", len(order), width) + words + idx


def enc_delta_varint(values: list, ty) -> bytes | None:
    """Timestamps as differences in variable-length bytes. Datetime and Int64 only."""
    if ty not in (DATETIME, INT64):
        return None
    present = [v is not None for v in values]
    seen = [_clamp(v) for v in values if v is not None]
    if not seen:
        return None
    out = bytearray(b"" if all(present) else bitmap(present))
    out += varint(seen[0])
    prev = seen[0]
    for v in seen[1:]:
        out += varint(v - prev)
        prev = v
    return bytes(out)


def enc_varint(values: list, ty) -> bytes | None:
    """Integers in variable-length bytes, no delta."""
    if ty not in (INT64, DATETIME):
        return None
    present = [v is not None for v in values]
    seen = [_clamp(v) for v in values if v is not None]
    if not seen:
        return None
    out = bytearray(b"" if all(present) else bitmap(present))
    for v in seen:
        out += varint(v)
    return bytes(out)


def enc_bits(values: list, ty) -> bytes | None:
    """Booleans as bits - one eighth of the plain chunk's byte per row."""
    if ty != BOOL:
        return None
    present = [v is not None for v in values]
    seen = [v for v in values if v is not None]
    out = bytearray(b"" if all(present) else bitmap(present))
    out += bitmap([bool(v) for v in seen])
    return bytes(out)


_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                      r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def enc_uuid16(values: list, ty) -> bytes | None:
    """A UUID as its 16 bytes rather than its 36 characters."""
    if ty != UUID:
        return None
    present = [v is not None for v in values]
    out = bytearray(b"" if all(present) else bitmap(present))
    for v in values:
        if v is None:
            continue
        s = str(v)
        if not _UUID_RE.match(s):
            return None
        out += bytes.fromhex(s.replace("-", ""))
    return bytes(out)


def enc_ip_bin(values: list, ty) -> bytes | None:
    """An IP as its 4 or 16 bytes, with one tag byte saying which."""
    if ty != IP:
        return None
    import ipaddress

    present = [v is not None for v in values]
    out = bytearray(b"" if all(present) else bitmap(present))
    for v in values:
        if v is None:
            continue
        try:
            addr = ipaddress.ip_address(str(v))
        except ValueError:
            return None
        out += bytes([4 if addr.version == 4 else 6]) + addr.packed
    return bytes(out)


ENCODERS = [
    ("plain", enc_plain),
    ("const", enc_const),
    ("dict", enc_dict),
    ("delta_varint", enc_delta_varint),
    ("varint", enc_varint),
    ("bits", enc_bits),
    ("uuid16", enc_uuid16),
    ("ip_bin", enc_ip_bin),
]


# --- the CLP idea -----------------------------------------------------------------

# Numbers, hex runs, IPs and UUIDs, found by simple patterns. Deliberately simple: the
# question here is only whether the shape is worth a closer look, not what a tuned
# tokenizer would score.
_VAR = re.compile(
    r"(?P<uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    r"|(?P<ip>\b\d{1,3}(?:\.\d{1,3}){3}\b)"
    r"|(?P<hex>\b(?:0x)?[0-9a-fA-F]{6,}\b)"
    r"|(?P<num>-?\d+(?:\.\d+)?)"
)


@dataclass
class ClpSplit:
    fixed: bytes  # the dictionary of fixed parts, plus one index per row
    variables: list  # one encoded column per variable slot
    slots: int

    @property
    def total(self) -> int:
        return len(self.fixed) + sum(len(v) for v in self.variables)


def clp_split(values: list) -> ClpSplit | None:
    """Split each value into a fixed part and its changing parts.

    The fixed part goes through a word list; each changing part becomes its own column,
    encoded as whatever is smallest for it. Values with different slot counts keep their
    own fixed part, so the word list carries the arity - no padding and no lost text.
    """
    seen = [str(v) for v in values if v is not None]
    if not seen:
        return None
    fixed_parts = []
    slot_values: dict = {}
    max_slots = 0
    for s in seen:
        parts = []
        last = 0
        vals = []
        for m in _VAR.finditer(s):
            parts.append(s[last : m.start()])
            vals.append(m.group(0))
            last = m.end()
        parts.append(s[last:])
        fixed_parts.append("\x00".join(parts))
        for i, v in enumerate(vals):
            slot_values.setdefault(i, []).append(v)
        max_slots = max(max_slots, len(vals))

    if max_slots == 0:
        return None
    fixed = enc_dict(fixed_parts, STRING) or enc_plain(fixed_parts, STRING)
    variables = []
    for i in range(max_slots):
        col = slot_values.get(i, [])
        best = min(
            (
                b
                for b in (
                    enc_plain(col, STRING),
                    enc_dict(col, STRING),
                    _maybe_numeric(col),
                )
                if b is not None
            ),
            key=len,
        )
        variables.append(best)
    return ClpSplit(fixed, variables, max_slots)


def _maybe_numeric(col: list) -> bytes | None:
    """A slot whose every value is an integer stores as deltas, not as text."""
    try:
        ints = [int(v) for v in col]
    except (TypeError, ValueError):
        return None
    return enc_delta_varint(ints, INT64)


# --- running a column through everything -------------------------------------------


def encode_all(values: list, ty, zlevel: int = 3) -> dict:
    """Every applicable encoder, raw and with zstd on top. Keys are encoder names."""
    out: dict = {}
    for name, fn in ENCODERS:
        blob = fn(values, ty)
        if blob is None:
            continue
        out[name] = len(blob)
        out[f"{name}+zstd{zlevel}"] = len(zstd_bytes(blob, zlevel))
    return out
