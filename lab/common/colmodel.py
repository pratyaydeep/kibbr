"""A byte model of the `.col` segment format, exact to the byte.

Every field is written out rather than folded into a constant, because the point of the
model is that a later reading can check it against the format and against script 03's
calibration run of the real binary.

    Header   [4] magic  [1] version  [3] reserved

    Row group
             [4] payload_len  [4] crc
             payload: [8] row_count [8] min_ts [8] max_ts [2] column_count
                      per column: [2] name_len [N] name [1] col_type [1] dt_form
                                  [1] encoding [4] chunk_len
                      chunks: encoding 0 -> ceil(rows/8) null bitmap, then values
                              encoding 1 -> no bitmap, then values
                      Str [4 len][bytes]   I64/F64/DT [8]   Bool [1]

    Footer   [8] total_rows [2] column_count
             per column: [2] name_len [N] name [1] col_type [1] dt_form
             [4] row_group_count
             per row group: [8] row_count [8] min_ts [8] max_ts
                            per column: [8] offset [4] length
             [4] footer_len  [4] magic
"""

from __future__ import annotations

import math

from .frontend import BOOL, DATETIME, FLOAT64, INT64

FILE_HEADER = 8
RG_FRAME = 4 + 4  # payload_len + crc
RG_FIXED = 8 + 8 + 8 + 2  # row_count, min_ts, max_ts, column_count
RG_PER_COL_FIXED = 2 + 1 + 1 + 1 + 4  # name_len, col_type, dt_form, encoding, chunk_len
FOOTER_FIXED = 8 + 2 + 4 + 4 + 4  # total_rows, column_count, rg_count, footer_len, magic
FOOTER_PER_COL_FIXED = 2 + 1 + 1  # name_len, col_type, dt_form
FOOTER_PER_RG_FIXED = 8 + 8 + 8  # row_count, min_ts, max_ts
FOOTER_PER_RG_PER_COL = 8 + 4  # offset, length

BLOCK = 4096

# Fixed-width types. Str carries its own 4-byte length prefix per present row; Uuid and
# Ip are Str on disk in this format - script 05 is where 16 raw bytes gets tested.
_WIDTH = {INT64: 8, FLOAT64: 8, DATETIME: 8, BOOL: 1}


def value_bytes(ty, raw) -> int:
    """What one present value costs in a plain chunk."""
    if raw is None:
        return 0
    w = _WIDTH.get(ty)
    if w is not None:
        return w
    return 4 + len(raw.encode("utf-8", "surrogatepass"))


def rg_column_header(name: str) -> int:
    return RG_PER_COL_FIXED + len(name.encode("utf-8"))


def chunk_bytes(rows: int, present: int, data: int) -> tuple:
    """`(chunk length, encoding)` for one column of one row group.

    Encoding 1 - plain, every row present - drops the bitmap entirely, which is why the
    null share of a merged template is a byte question and not only a schema one.
    """
    if present == rows:
        return data, 1
    return math.ceil(rows / 8) + data, 0


def rowgroup_bytes(rows: int, columns: list) -> tuple:
    """`(total, overhead, data)` for one row group.

    `columns` is `(name, present_count, data_bytes)` per column, in the segment's fixed
    column order - including the columns this row group holds no value for, which cost a
    header and an all-zero bitmap and nothing else.
    """
    header = RG_FRAME + RG_FIXED
    data_total = 0
    chunk_total = 0
    for name, present, data in columns:
        header += rg_column_header(name)
        length, _enc = chunk_bytes(rows, present, data)
        chunk_total += length
        data_total += data
    total = header + chunk_total
    return total, total - data_total, data_total


def footer_bytes(names: list, n_row_groups: int) -> int:
    total = FOOTER_FIXED
    for name in names:
        total += FOOTER_PER_COL_FIXED + len(name.encode("utf-8"))
    total += n_row_groups * (FOOTER_PER_RG_FIXED + len(names) * FOOTER_PER_RG_PER_COL)
    return total


def file_bytes(names: list, row_groups: list) -> tuple:
    """`(total, overhead, data, footer)` for one segment file."""
    data = sum(rg[2] for rg in row_groups)
    body = sum(rg[0] for rg in row_groups)
    foot = footer_bytes(names, len(row_groups))
    total = FILE_HEADER + body + foot
    return total, total - data, data, foot


def blocks(total: int) -> int:
    """What the filesystem charges: every file rounds up to a 4 KiB allocation block."""
    return math.ceil(total / BLOCK) * BLOCK if total else 0
