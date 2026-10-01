"""Generation algorithm versions.

Each algorithm version is a frozen, immutable implementation.  Definitions
record which version produced them so that results can be reproduced
forever.  New versions are added as separate callables; they never change
the output of existing versions.

Version policy:
* ``v1`` is the original, frozen algorithm.  Its output must never change.
* ``v2`` is an independent algorithm with its own domain separation.
* New definitions bind to the configured default version unless they
  explicitly request a specific version.  Bumping the default never
  rewrites existing definitions: they keep reproducing with the version
  they were created with.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Callable

from .prng import SplitMix64

# ---------------------------------------------------------------------------
# Stream spec validation
# ---------------------------------------------------------------------------

KINDS = ("sequence", "integers", "booleans", "choice", "string")


def validate_spec(spec: dict) -> None:
    """Validate a stream specification, raising ValueError on problems."""
    if not isinstance(spec, dict):
        raise ValueError("stream spec must be an object")
    kind = spec.get("kind")
    if kind not in KINDS:
        raise ValueError(f"unknown stream kind: {kind!r} (expected one of {KINDS})")
    count = spec.get("count")
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise ValueError("stream count must be a non-negative integer")
    params = spec.get("params", {})
    if not isinstance(params, dict):
        raise ValueError("stream params must be an object")
    if kind == "sequence":
        for key in ("start", "step"):
            if key not in params or not isinstance(params[key], int):
                raise ValueError(f"sequence params.{key} must be an integer")
    elif kind == "integers":
        for key in ("low", "high"):
            if key not in params or not isinstance(params[key], int):
                raise ValueError(f"integers params.{key} must be an integer")
        if params["high"] < params["low"]:
            raise ValueError("integers params.high must be >= low")
    elif kind == "choice":
        options = params.get("options")
        if not isinstance(options, list) or not options:
            raise ValueError("choice params.options must be a non-empty list")
        for opt in options:
            if not isinstance(opt, (str, int, bool)) or opt is None:
                raise ValueError("choice options must be strings, integers or booleans")
    elif kind == "string":
        alphabet = params.get("alphabet")
        if not isinstance(alphabet, str) or not alphabet:
            raise ValueError("string params.alphabet must be a non-empty string")
        length = params.get("length")
        if not isinstance(length, int) or isinstance(length, bool) or length < 0:
            raise ValueError("string params.length must be a non-negative integer")


# ---------------------------------------------------------------------------
# Record generation
# ---------------------------------------------------------------------------

def _record_rng(domain: bytes, stream_seed: int, index: int) -> SplitMix64:
    """Build the per-record RNG with algorithm-specific domain separation."""
    h = hashlib.sha256()
    h.update(domain)
    h.update(stream_seed.to_bytes(8, "little"))
    h.update(index.to_bytes(8, "little", signed=False))
    return SplitMix64(int.from_bytes(h.digest()[:8], "little"))


def _draw(kind: str, params: dict, index: int, rng: SplitMix64) -> Any:
    if kind == "sequence":
        return params["start"] + index * params["step"]
    if kind == "integers":
        return rng.integer(params["low"], params["high"])
    if kind == "booleans":
        return rng.boolean()
    if kind == "choice":
        return rng.choice(params["options"])
    if kind == "string":
        alphabet = params["alphabet"]
        length = params["length"]
        return "".join(alphabet[rng.bounded(len(alphabet))] for _ in range(length))
    raise ValueError(f"unknown stream kind: {kind!r}")  # pragma: no cover


def generate_record_v1(spec: dict, stream_seed: int, index: int) -> Any:
    """Generate one record with the frozen v1 algorithm."""
    rng = _record_rng(b"seedgen.engine.v1", stream_seed, index)
    return _draw(spec["kind"], spec.get("params", {}), index, rng)


def generate_record_v2(spec: dict, stream_seed: int, index: int) -> Any:
    """Generate one record with the v2 algorithm.

    v2 uses a distinct domain separation from v1; outputs are deliberately
    different from v1 for the same seed and stream.
    """
    rng = _record_rng(b"seedgen.engine.v2", stream_seed, index)
    return _draw(spec["kind"], spec.get("params", {}), index, rng)


# ---------------------------------------------------------------------------
# Version registry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AlgorithmVersion:
    name: str
    record_fn: Callable[[dict, int, int], Any]
    frozen: bool
    note: str


ALGORITHM_VERSIONS: dict[str, AlgorithmVersion] = {
    "v1": AlgorithmVersion(
        name="v1",
        record_fn=generate_record_v1,
        frozen=True,
        note="Original frozen algorithm. Output is immutable.",
    ),
    "v2": AlgorithmVersion(
        name="v2",
        record_fn=generate_record_v2,
        frozen=True,
        note="Second algorithm version with independent domain separation.",
    ),
}


class UnknownAlgorithmVersion(ValueError):
    """Raised when a definition references an algorithm version that does not exist."""


def get_version(name: str) -> AlgorithmVersion:
    try:
        return ALGORITHM_VERSIONS[name]
    except KeyError:
        raise UnknownAlgorithmVersion(
            f"unknown algorithm version: {name!r}; "
            f"available: {sorted(ALGORITHM_VERSIONS)}"
        ) from None


def generate_record(algo_version: str, spec: dict, stream_seed: int, index: int) -> Any:
    return get_version(algo_version).record_fn(spec, stream_seed, index)


# ---------------------------------------------------------------------------
# JSONL output encoding
# ---------------------------------------------------------------------------

def encode_record(index: int, value: Any) -> bytes:
    """Encode one record as a canonical JSON line.

    Keys are sorted and separators fixed so output is byte-identical on
    every platform and Python implementation.
    """
    import json

    return (
        json.dumps(
            {"i": index, "v": value},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def generate_window(
    algo_version: str,
    spec: dict,
    stream_seed: int,
    offset: int,
    limit: int,
) -> bytes:
    """Generate exactly ``limit`` records starting at ``offset``.

    Counter-based generation means the window is produced independently
    of any other window or request order.
    """
    parts = []
    for i in range(offset, offset + limit):
        value = generate_record(algo_version, spec, stream_seed, i)
        parts.append(encode_record(i, value))
    return b"".join(parts)


def iter_chunks(
    algo_version: str,
    spec: dict,
    stream_seed: int,
    chunk_records: int,
    delay: float = 0.0,
):
    """Yield ``(start_index, chunk_bytes)`` tuples for a whole stream.

    Each chunk is generated independently; cancelling the iteration leaves
    no state behind.  *delay* is a test-only hook.
    """
    import time

    count = spec["count"]
    start = 0
    while start < count:
        end = min(start + chunk_records, count)
        chunk = generate_window(algo_version, spec, stream_seed, start, end - start)
        if delay:
            time.sleep(delay)
        yield start, chunk
        start = end
