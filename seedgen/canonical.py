"""Canonical JSON serialization and content hashing.

All content-addressed identifiers are derived from *canonical* JSON:
keys are sorted, separators are fixed, so dict ordering can never
influence the result.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# Domain separation prefixes so that hashes for different purposes never
# collide even if their inputs happen to be identical.
_DOMAIN_ROOT = b"seedgen.root.v1"
_DOMAIN_STREAM = b"seedgen.stream.v1"
_DOMAIN_RECORD = b"seedgen.record.v1"
_DOMAIN_DEF = b"seedgen.definition.v1"
_DOMAIN_JOB = b"seedgen.job.v1"
_DOMAIN_MANIFEST = b"seedgen.manifest.v1"


def canonical_dumps(obj: Any) -> bytes:
    """Serialize *obj* to canonical, deterministic JSON bytes.

    Keys are sorted, separators are fixed and output is UTF-8 encoded.
    The same object always produces byte-identical output, regardless of
    dict insertion order or platform.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_obj(obj: Any) -> str:
    """Return the SHA-256 hex digest of the canonical JSON of *obj*."""
    return sha256_hex(canonical_dumps(obj))


def _domain_hash(domain: bytes, *parts: bytes) -> bytes:
    h = hashlib.sha256()
    h.update(domain)
    for p in parts:
        # Length-prefixed framing so part boundaries are unambiguous.
        h.update(len(p).to_bytes(8, "little"))
        h.update(p)
    return h.digest()


def derive_root_key(seed: str | int) -> bytes:
    """Derive the 256-bit root key from a user-provided seed."""
    if isinstance(seed, int):
        seed_bytes = b"int:" + str(seed).encode("ascii")
    else:
        seed_bytes = b"str:" + seed.encode("utf-8")
    return _domain_hash(_DOMAIN_ROOT, seed_bytes)


def derive_stream_seed(root_key: bytes, stream_name: str) -> int:
    """Derive a 64-bit seed for a named stream.

    Streams are cryptographically separated: knowing one stream's seed
    reveals nothing about another stream's seed.
    """
    digest = _domain_hash(_DOMAIN_STREAM, root_key, stream_name.encode("utf-8"))
    return int.from_bytes(digest[:8], "little")


def derive_record_seed(stream_seed: int, record_index: int) -> int:
    """Derive the 64-bit seed for an individual record.

    Record generation is counter-based: record *i* never depends on records
    ``0..i-1``, which makes pagination and streaming independent of
    request order.
    """
    digest = _domain_hash(
        _DOMAIN_RECORD,
        stream_seed.to_bytes(8, "little"),
        record_index.to_bytes(8, "little", signed=False),
    )
    return int.from_bytes(digest[:8], "little")


def definition_id(algo_version: str, seed: str | int, streams: dict) -> str:
    """Content-addressed identifier of a generation definition."""
    payload = {"algo_version": algo_version, "seed": seed, "streams": streams}
    return "def_" + sha256_hex(_domain_hash(_DOMAIN_DEF, canonical_dumps(payload)))


def job_id(def_id: str, streams: list[str] | None, force: bool) -> str:
    """Content-addressed identifier of a generation job."""
    payload = {
        "def": def_id,
        "streams": sorted(streams) if streams else None,
        "force": force,
    }
    return "job_" + sha256_hex(_domain_hash(_DOMAIN_JOB, canonical_dumps(payload)))


def manifest_id(def_id: str, manifest: dict) -> str:
    """Checksum over a manifest's canonical contents."""
    return sha256_hex(
        _domain_hash(_DOMAIN_MANIFEST, def_id.encode("ascii"), canonical_dumps(manifest))
    )
