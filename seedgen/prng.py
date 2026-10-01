"""Deterministic pseudo-random source.

The generator never uses :mod:`random`, :mod:`secrets`, wall-clock values or
any other ambient entropy.  Every draw is a pure function of
``(definition_id, algorithm_version, stream_name, seed, row_index, purpose,
counter)`` derived with SHA-256, so output is byte-stable across processes,
machines and Python interpreter versions.

A fresh :class:`DeterministicRNG` is created per row; rows are therefore
independent and order-stable: reading only row 1000 yields the same row as
reading rows 0..1000.
"""

from __future__ import annotations

import hashlib
import struct

_DOMAIN_PREFIX = b"seedgen-rng/v1/"


class DeterministicRNG:
    """SHA-256 based, per-draw deterministic random stream for one row."""

    def __init__(self, base_key: bytes, row_index: int):
        self._base = hashlib.sha256(_DOMAIN_PREFIX + base_key).digest()
        self._row_index = row_index
        self._counter = 0

    @classmethod
    def for_row(cls, domain_key: bytes, row_index: int) -> "DeterministicRNG":
        return cls(domain_key, row_index)

    def draw_bytes(self, length: int, purpose: str) -> bytes:
        out = bytearray()
        while len(out) < length:
            block = hashlib.sha256(
                self._base
                + struct.pack(">Q", self._row_index)
                + purpose.encode("utf-8")
                + b"\0"
                + struct.pack(">Q", self._counter)
            ).digest()
            self._counter += 1
            out.extend(block)
        return bytes(out[:length])

    def randbelow(self, n: int, purpose: str = "int") -> int:
        """Uniform integer in ``range(n)`` via rejection sampling."""
        if n <= 0:
            raise ValueError("randbelow() requires n > 0")
        size = (n.bit_length() + 7) // 8
        mask = (1 << (size * 8)) - 1
        threshold = (mask // n) * n
        while True:
            value = int.from_bytes(self.draw_bytes(size, purpose), "big") & mask
            if value < threshold:
                return value % n

    def randrange(self, lo: int, hi: int, purpose: str = "int") -> int:
        if hi <= lo:
            raise ValueError("randrange() requires hi > lo")
        return lo + self.randbelow(hi - lo, purpose)

    def random(self, purpose: str = "float") -> float:
        """Uniform float in ``[0.0, 1.0)`` with 53 bits of entropy."""
        raw = int.from_bytes(self.draw_bytes(8, purpose), "big")
        return (raw >> 11) / (1 << 53)

    def pick(self, seq, purpose: str = "pick"):
        seq = list(seq)
        if not seq:
            raise ValueError("cannot pick from an empty sequence")
        return seq[self.randbelow(len(seq), purpose)]

    def chance(self, p: float, purpose: str = "chance") -> bool:
        if not 0.0 <= p <= 1.0:
            raise ValueError("chance probability must be in [0, 1]")
        return self.random(purpose) < p
