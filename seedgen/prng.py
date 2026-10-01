"""Deterministic pseudo-random number generation.

The generator is a pure-Python SplitMix64 counter-based PRNG.  Seeds are
derived with SHA-256 domain separation, so output depends exclusively on
(seed, stream name, record index) -- never on process state, time or
environment randomization.

SplitMix64 is used in its standard, frozen form; the algorithm is fixed
and will not change between releases.
"""
from __future__ import annotations

MASK64 = (1 << 64) - 1


def splitmix64(state: int) -> int:
    """Advance the SplitMix64 state and return a 64-bit value."""
    state = (state + 0x9E3779B97F4A7C15) & MASK64
    z = state
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
    return z ^ (z >> 31)


class SplitMix64:
    """Minimal deterministic PRNG.

    Instances are cheap and independent: every record constructs its own
    generator from a derived seed, so generating record *i* never mutates
    shared state and records can be produced in any order.
    """

    __slots__ = ("_state",)

    def __init__(self, seed: int) -> None:
        self._state = seed & MASK64

    def next_u64(self) -> int:
        self._state = (self._state + 0x9E3779B97F4A7C15) & MASK64
        z = self._state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
        return z ^ (z >> 31)

    def bounded(self, bound: int) -> int:
        """Return a value in ``[0, bound)`` using modulo reduction."""
        if bound <= 0:
            raise ValueError("bound must be positive")
        return self.next_u64() % bound

    def integer(self, low: int, high: int) -> int:
        """Return an integer in the inclusive range ``[low, high]``."""
        if high < low:
            raise ValueError("high must be >= low")
        return low + self.bounded(high - low + 1)

    def boolean(self) -> bool:
        return (self.next_u64() & 1) == 1

    def choice(self, options: list) -> object:
        if not options:
            raise ValueError("options must not be empty")
        return options[self.bounded(len(options))]
