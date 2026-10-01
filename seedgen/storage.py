"""Durable, atomic, checksummed storage for generated stream data.

On-disk layout per stream::

    <root>/<definition_id>/<stream_name>.jsonl
    <root>/<definition_id>/<stream_name>.jsonl.meta.json

Data is newline-delimited JSON (canonical encoding), streamed one row at a
time -- large results are never buffered whole.  Writes go to a hidden temp
file and are ``fsync``-ed and atomically renamed into place only after every
row has been written; readers therefore see either the previous complete
file or the new complete file, never a half-written one.

The ``.meta.json`` records row count, the SHA-256 checksum of the data file
and a fingerprint of the generating specification.  On read, all three are
re-verified; corruption or a stale/missing cache is reported rather than
silently served.
"""

from __future__ import annotations

import dataclasses
import hashlib
import itertools
import json
import os
import threading
from pathlib import Path

from .canonical import canonical_json
from .errors import CacheCorruptedError
from .models import GenerationDefinition, StreamSpec
from .canonical import fingerprint


@dataclasses.dataclass(frozen=True)
class StoredStream:
    path: Path
    meta_path: Path
    row_count: int
    checksum: str
    spec_fingerprint: str


def spec_fingerprint_for(definition: GenerationDefinition,
                         stream: StreamSpec) -> str:
    """Fingerprint of everything that determines this stream's bytes.

    It contains only stable, per-stream inputs (definition name, pinned
    algorithm version, seed, stream name, row count and fields) -- never the
    whole-document definition id or sibling streams -- so a cache remains
    valid when unrelated streams are added.
    """
    return fingerprint({
        "definition_name": definition.name,
        "algorithm_version": definition.algorithm_version,
        "seed": definition.seed,
        "stream": stream.name,
        "row_count": stream.row_count,
        "fields": [
            {"name": f.name, "kind": f.kind, "params": f.params}
            for f in stream.fields
        ],
    })


class _StreamWriter:
    def __init__(self, data_tmp: Path, meta_tmp: Path,
                 data_final: Path, meta_final: Path):
        self._data_tmp = data_tmp
        self._meta_tmp = meta_tmp
        self._data_final = data_final
        self._meta_final = meta_final
        self._fh = open(data_tmp, "wb")
        self._hasher = hashlib.sha256()
        self.row_count = 0
        self._closed = False

    def write_row(self, row: dict) -> None:
        if self._closed:
            raise RuntimeError("writer is closed")
        chunk = canonical_json(row) + b"\n"
        self._fh.write(chunk)
        self._hasher.update(chunk)
        self.row_count += 1

    def _commit(self, spec_fp: str) -> StoredStream:
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        checksum = self._hasher.hexdigest()
        meta = {
            "row_count": self.row_count,
            "checksum_sha256": checksum,
            "spec_fingerprint": spec_fp,
        }
        with open(self._meta_tmp, "wb") as mf:
            mf.write(json.dumps(meta, sort_keys=True).encode("utf-8"))
            mf.flush()
            os.fsync(mf.fileno())
        os.replace(self._data_tmp, self._data_final)
        os.replace(self._meta_tmp, self._meta_final)
        self._closed = True
        return StoredStream(self._data_final, self._meta_final,
                            self.row_count, checksum, spec_fp)

    def _abort(self) -> None:
        if not self._closed:
            try:
                self._fh.close()
            except OSError:
                pass
            self._closed = True
        for p in (self._data_tmp, self._meta_tmp):
            try:
                p.unlink()
            except FileNotFoundError:
                pass


class StreamStore:

    _tmp_counter = itertools.count()

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _dir(self, definition_id: str) -> Path:
        d = self.root / definition_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def stream_path(self, definition_id: str, stream_name: str) -> Path:
        return self._dir(definition_id) / f"{stream_name}.jsonl"

    def _meta_path(self, definition_id: str, stream_name: str) -> Path:
        return self._dir(definition_id) / f"{stream_name}.jsonl.meta.json"

    def exists(self, definition_id: str, stream_name: str) -> bool:
        return (self.stream_path(definition_id, stream_name).exists()
                and self._meta_path(definition_id, stream_name).exists())

    def open_writer(self, definition_id: str, stream_name: str) -> _StreamWriter:
        d = self._dir(definition_id)
        data_final = d / f"{stream_name}.jsonl"
        meta_final = d / f"{stream_name}.jsonl.meta.json"
        token = f"{os.getpid()}.{threading.get_ident()}.{next(self._tmp_counter)}"
        data_tmp = d / f".{stream_name}.jsonl.tmp.{token}"
        meta_tmp = d / f".{stream_name}.jsonl.meta.json.tmp.{token}"
        return _StreamWriter(data_tmp, meta_tmp, data_final, meta_final)

    def commit_writer(self, writer: _StreamWriter, *, spec_fp: str) -> StoredStream:
        return writer._commit(spec_fp)

    def abort_writer(self, writer: _StreamWriter) -> None:
        writer._abort()

    def get_meta(self, definition_id: str, stream_name: str) -> StoredStream:
        data_path = self.stream_path(definition_id, stream_name)
        meta_path = self._meta_path(definition_id, stream_name)
        if not data_path.exists() or not meta_path.exists():
            raise CacheCorruptedError(
                f"cached stream {stream_name!r} is missing",
                code="cache_missing",
            )
        try:
            with open(meta_path, "rb") as fh:
                meta = json.loads(fh.read().decode("utf-8"))
            return StoredStream(
                data_path, meta_path,
                int(meta["row_count"]),
                str(meta["checksum_sha256"]),
                str(meta["spec_fingerprint"]),
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CacheCorruptedError(
                f"meta for stream {stream_name!r} is unreadable: {exc}",
                code="cache_meta_corrupted",
            ) from exc

    def _verified_checksum(self, stored: StoredStream) -> None:
        hasher = hashlib.sha256()
        count = 0
        with open(stored.path, "rb") as fh:
            for line in fh:
                hasher.update(line)
                count += 1
        if hasher.hexdigest() != stored.checksum:
            raise CacheCorruptedError(
                "data file checksum mismatch",
                code="cache_checksum_mismatch",
            )
        if count != stored.row_count:
            raise CacheCorruptedError(
                f"row count mismatch: meta={stored.row_count} data={count}",
                code="cache_row_count_mismatch",
            )

    def verify(self, definition_id: str, stream_name: str,
               expected_spec_fp: str | None = None) -> StoredStream:
        stored = self.get_meta(definition_id, stream_name)
        self._verified_checksum(stored)
        if expected_spec_fp is not None and stored.spec_fingerprint != expected_spec_fp:
            raise CacheCorruptedError(
                "cached stream was generated from a different definition",
                code="cache_spec_mismatch",
            )
        return stored

    def iter_rows(self, definition_id: str, stream_name: str,
                  offset: int = 0, limit: int | None = None):
        stored = self.get_meta(definition_id, stream_name)
        hasher = hashlib.sha256()
        count = 0
        end = None if limit is None else offset + limit
        with open(stored.path, "rb") as fh:
            for idx, line in enumerate(fh):
                hasher.update(line)
                count += 1
                if idx < offset:
                    continue
                if end is not None and idx >= end:
                    continue
                try:
                    yield json.loads(line.decode("utf-8"))
                except ValueError as exc:
                    raise CacheCorruptedError(
                        f"unreadable row in stream {stream_name!r}",
                        code="cache_row_corrupted",
                    ) from exc
        if hasher.hexdigest() != stored.checksum or count != stored.row_count:
            raise CacheCorruptedError(
                "data file failed verification on read",
                code="cache_checksum_mismatch",
            )

    def delete(self, definition_id: str, stream_name: str) -> None:
        for p in (self.stream_path(definition_id, stream_name),
                  self._meta_path(definition_id, stream_name)):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
