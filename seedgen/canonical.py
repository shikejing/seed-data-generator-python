"""Stable canonical serialization and content fingerprints.

Ordering is always explicit (``sort_keys``), so dict/set traversal order and
insertion order can never influence output or fingerprints.  The encoding is
fixed to UTF-8, compact JSON with ``ensure_ascii=False``; tuples and lists are
indistinguishable on the wire.  NaN/Infinity are rejected so there is no
ambiguous text representation.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def _convert(obj: Any) -> Any:
    if isinstance(obj, tuple):
        return [_convert(v) for v in obj]
    if isinstance(obj, list):
        return [_convert(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): _convert(v) for k, v in obj.items()}
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, (str, int, float)):
        if isinstance(obj, float):
            # Reject NaN / infinities: their repr is not stable JSON.
            if obj != obj or obj in (float("inf"), float("-inf")):
                raise ValueError("non-finite floats cannot be canonicalized")
        return obj
    if hasattr(obj, "__dict__"):
        return _convert(vars(obj))
    raise TypeError(f"object of type {type(obj).__name__!r} is not serializable")


def canonical_json(obj: Any) -> bytes:
    """Return the unique byte encoding of ``obj``."""
    converted = _convert(obj)
    return json.dumps(
        converted,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def fingerprint(obj: Any) -> str:
    """SHA-256 hex digest of the canonical encoding."""
    return hashlib.sha256(canonical_json(obj)).hexdigest()


def domain_seed(definition_name: str, algorithm_version: str,
                stream_name: str, seed: str) -> bytes:
    """Domain-separation prefix that binds randomness to one stream only.

    The domain deliberately excludes the definition id (which is a fingerprint
    of the *whole* document, including sibling streams) and any other stream.
    It binds only stable, named identity -- definition name, pinned algorithm
    version, stream name and the seed -- so adding or reordering streams can
    never perturb an existing stream's bytes.
    """
    return canonical_json([definition_name, algorithm_version, stream_name, seed])
