"""Schema merging, prototyped - section 5 with the knobs turned into parameters.

TrieDB does not merge yet; `LinearMergeIndex` and `BucketedMergeIndex` are section 5.2's
plan, not code on disk. So this is a clean-room prototype whose only job is to say what
each rule would do to the template count, the null share and the rows a search has to
scan.

Faithful to section 5.4 in the part that matters: the decision is made per line, in
arrival order, so order dependence is measured rather than defined away. The bucket bound
of section 5.1 is used as written - a template with T keys can only match a candidate
with C keys when T is in [floor(C*t), ceil(C/t)], which is provably exact and not a
heuristic.

The one liberty taken for speed: a shape's decision is cached until a template enters its
bucket range. Templates are only ever added, and a template's *original* key set never
changes, so a cached best match can only be beaten by a newcomer - the cache returns the
same answer the uncached search would, and the tests below say so.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field

from .frontend import STRING, join_types


@dataclass
class Group:
    """One merged template - what would become one leaf."""

    gid: int
    original_keys: frozenset  # the key set it was created from; never changes
    grown_keys: set  # the union it has grown to
    types: dict  # key -> folded ColumnType, or None
    promoted: tuple  # the promoted (key, value) pairs of its creator
    promoted_seen: set = field(default_factory=set)
    rows: list = field(default_factory=list)
    # Every row-level type this column has seen, unioned across the templates merged in.
    # Judging widening on the folded type instead misses a column that widened inside a
    # single template - by then the folded type is already String and the evidence is gone.
    evidence: dict = field(default_factory=dict)
    n_cells_present: int = 0
    is_leftover: bool = False

    @property
    def n(self) -> int:
        return len(self.rows)


@dataclass
class Result:
    rule: str
    groups: dict
    assign: list  # group id per row index
    note: str = ""

    def stats(self, n_rows: int) -> dict:
        cells = 0
        present = 0
        widened = 0
        columns = 0
        widened_keys: set = set()
        widened_rows = 0
        for g in self.groups.values():
            k = len(g.grown_keys)
            cells += g.n * k
            columns += k
            present += g.n_cells_present
            for key in g.grown_keys:
                if g.types.get(key) != STRING:
                    continue
                if any(t is not None and t != STRING for t in g.evidence.get(key, ())):
                    widened += 1
                    widened_keys.add(key)
                    widened_rows += g.n
        counts = sorted(g.n for g in self.groups.values())
        return {
            "templates": len(self.groups),
            "rows_per_template_min": counts[0] if counts else 0,
            "rows_per_template_median": counts[len(counts) // 2] if counts else 0,
            "rows_per_template_p90": counts[min(len(counts) - 1, int(0.9 * len(counts)))]
            if counts
            else 0,
            "rows_per_template_max": counts[-1] if counts else 0,
            "null_cell_share_pct": 100.0 * (cells - present) / cells if cells else 0.0,
            # The raw count is (template, column) pairs, so it falls as merging cuts the
            # template count - which reads as merging *helping*, when the join is
            # monotonic and a merge can only ever widen more. The three below are what
            # can actually be compared across rules.
            "columns_widened_to_string": widened,
            "columns_widened_share_pct": 100.0 * widened / columns if columns else 0.0,
            "distinct_keys_widened": len(widened_keys),
            "rows_under_a_widened_column": widened_rows,
            "mixed_promoted_groups": sum(
                1 for g in self.groups.values() if len(g.promoted_seen) > 1
            ),
            "rows_in_mixed_promoted_group": sum(
                g.n for g in self.groups.values() if len(g.promoted_seen) > 1
            ),
        }


def _bucket_range(c: int, threshold: float) -> tuple:
    """Section 5.1. Templates outside this range cannot match, whatever their keys."""
    if threshold <= 0:
        return 0, 10**9
    return math.floor(c * threshold), math.ceil(c / threshold)


def run(
    P,
    threshold=None,
    *,
    match_grown: bool = False,
    respect_promoted: bool = True,
    per_event: bool = False,
    cap: int | None = None,
    rule_name: str = "",
    order: list | None = None,
) -> Result:
    """One merge rule over one parsed dataset.

    `threshold=None` is NONE - no merging, one group per template.
    `per_event` is one group per set of promoted values, whatever the keys.
    `cap` sends every line that would create a template past the cap into one shared
    leftover group, which is never itself a merge target - it is a bucket of last resort,
    and letting it match would pull the whole tail into it by accident rather than by rule.
    """
    order = order if order is not None else list(range(P.n_rows))
    groups: dict = {}
    assign = [0] * P.n_rows
    next_gid = 0
    by_count: dict = defaultdict(set)  # matchable key count -> gids
    count_of: dict = {}  # gid -> the count it is currently filed under
    ver: dict = defaultdict(int)  # bumped whenever a bucket's membership changes
    cache: dict = {}
    leftover = None

    def create(keys, promoted, matchable: bool):
        nonlocal next_gid
        g = _new(next_gid, keys, promoted)
        groups[g.gid] = g
        next_gid += 1
        if matchable:
            k = len(g.grown_keys if match_grown else g.original_keys)
            by_count[k].add(g.gid)
            count_of[g.gid] = k
            ver[k] += 1
        return g

    for r in order:
        t = P.templates[P.tid[r]]
        keys = frozenset(t.keys)
        promoted = t.promoted

        if per_event:
            # One template per set of promoted values, whatever the keys are.
            gid = cache.get(promoted)
            if gid is None:
                gid = create(keys, promoted, False).gid
                cache[promoted] = gid

        elif threshold is None:
            # NONE: a group per distinct shape, which is one per template id.
            shape = (keys, promoted)
            gid = cache.get(shape)
            if gid is None:
                gid = create(keys, promoted, False).gid
                cache[shape] = gid

        else:
            shape = (keys, promoted if respect_promoted else None)
            lo, hi = _bucket_range(len(keys), threshold)
            stamp = sum(ver[k] for k in range(lo, hi + 1))
            hit = cache.get(shape)
            if hit is not None and hit[0] == stamp:
                gid = hit[1]
            else:
                gid = _search(
                    groups, by_count, keys, promoted, threshold, lo, hi,
                    match_grown, respect_promoted,
                )
                if gid is None:
                    if cap is not None and len(groups) >= cap:
                        if leftover is None:
                            leftover = create(keys, promoted, False)
                            leftover.is_leftover = True
                        gid = leftover.gid
                    else:
                        gid = create(keys, promoted, True).gid
                cache[shape] = (stamp, gid)

        g = groups[gid]
        before = len(g.grown_keys)
        _absorb(g, P, t, r)
        if match_grown and len(g.grown_keys) != before and gid in count_of:
            # Its match range moved, so every cached decision that could see it is stale.
            old = count_of[gid]
            by_count[old].discard(gid)
            new = len(g.grown_keys)
            by_count[new].add(gid)
            count_of[gid] = new
            ver[old] += 1
            ver[new] += 1
        assign[r] = gid

    return Result(rule_name, groups, assign)


def _new(gid: int, keys: frozenset, promoted: tuple) -> Group:
    return Group(gid, keys, set(keys), {}, promoted)


def _search(
    groups, by_count, keys, promoted, threshold, lo, hi, match_grown, respect_promoted
):
    """Best match wins; ties go to the template created first.

    Measured against the template's ORIGINAL keys by default. That is the rule worth
    testing, because matching against the grown set is how a template eats the tail: each
    merge widens it, which makes the next merge easier, which widens it again.
    """
    best_gid = None
    best_ratio = 0.0
    for k in range(lo, hi + 1):
        for gid in by_count.get(k, ()):
            g = groups[gid]
            if g.is_leftover:
                continue
            if respect_promoted and g.promoted != promoted:
                continue
            other = g.grown_keys if match_grown else g.original_keys
            shared = len(keys & other)
            union = len(keys | other)
            ratio = shared / union if union else 1.0
            if ratio >= threshold and (
                ratio > best_ratio or (ratio == best_ratio and gid < (best_gid or 1 << 62))
            ):
                best_ratio = ratio
                best_gid = gid
    return best_gid


def _absorb(g: Group, P, t, r: int) -> None:
    g.rows.append(r)
    g.promoted_seen.add(t.promoted)
    for i, key in enumerate(t.keys):
        ty = t.types[key]
        if key not in g.grown_keys:
            g.grown_keys.add(key)
        g.types[key] = join_types(g.types.get(key), ty)
        slot = g.evidence.setdefault(key, set())
        slot |= t.evidence.get(key, {ty})
        if P.raws[r][i] is not None:
            g.n_cells_present += 1
