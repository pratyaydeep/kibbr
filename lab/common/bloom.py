"""A bloom filter per row group, sized in bits per value.

Double hashing over one 64-bit xxhash - `pos_i = (h1 + i*h2) mod m` - which is
Kirsch-Mitzenmacher: two hash values give k independent-looking positions, so the filter
costs one hash per value rather than k. The false positive rate is the thing being
measured, so it comes out of the probes below rather than out of the formula.

Probing is vectorised over the probe set, not over row groups: a query asks one value of
many row groups, so the loop that has to be cheap is the one over row groups, and inside
it every probe is checked in one numpy operation.
"""

from __future__ import annotations

import math

import numpy as np
import xxhash

# k = (m/n) ln 2 is the count that minimises the false positive rate for a given size.
_K = {8: 6, 10: 7, 16: 11}


def optimal_k(bits_per_value: int) -> int:
    return _K.get(bits_per_value, max(1, round(bits_per_value * math.log(2))))


def hash_pair(value: str) -> tuple:
    h = xxhash.xxh64(value.encode("utf-8", "surrogatepass")).intdigest()
    h1 = h & 0xFFFFFFFF
    h2 = (h >> 32) | 1  # odd, so it is coprime with any power-of-two m
    return h1, h2


class Filter:
    __slots__ = ("bits", "m", "k", "n")

    def __init__(self, values, bits_per_value: int):
        self.n = len(values)
        self.m = max(8, bits_per_value * max(1, self.n))
        self.k = optimal_k(bits_per_value)
        arr = np.zeros(self.m, dtype=bool)
        for v in values:
            h1, h2 = hash_pair(v)
            for i in range(self.k):
                arr[(h1 + i * h2) % self.m] = True
        self.bits = arr

    @property
    def nbytes(self) -> int:
        """What the filter costs on disk: the bit array, rounded up to whole bytes."""
        return math.ceil(self.m / 8)

    def probe_many(self, h1: np.ndarray, h2: np.ndarray) -> np.ndarray:
        """True where the filter says "maybe present", for every probe at once."""
        hit = np.ones(len(h1), dtype=bool)
        for i in range(self.k):
            hit &= self.bits[(h1 + i * h2) % self.m]
        return hit


def hashes(values) -> tuple:
    h1 = np.empty(len(values), dtype=np.int64)
    h2 = np.empty(len(values), dtype=np.int64)
    for i, v in enumerate(values):
        a, b = hash_pair(v)
        h1[i] = a
        h2[i] = b
    return h1, h2
