"""Orchestration: idempotent generation, streaming reads, cache integrity.

Concurrency model: a single in-process :class:`threading.Lock` is assigned
per ``(definition, stream)`` key (the map itself is guarded).  Concurrent or
duplicate submissions for the same stream serialize; the first generates and
commits atomically, the rest observe the committed result and never
regenerate.  Different streams use different locks, preserving isolation and
parallelism.

Cancellation/timeout: generation checks a cancel event between rows; on
cancellation the temp file is aborted, so no half-written artifact is ever
visible.  On every read the stored checksum, row count and definition
fingerprint are verified; corruption or a definition change is detected and
the stream is rebuilt rather than served.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Callable

from .audit import AuditLog, utc_now_iso
from .auth import Authorizer
from .errors import (
    CacheCorruptedError,
    DefinitionNotFoundError,
    GenerationCancelledError,
    IdempotencyConflictError,
    StreamNotFoundError,
    ValidationError,
)
from .generator import iter_stream
from .models import (
    GenerationDefinition,
    StreamSpec,
    validate_field_kinds,
)
from .repository import DefinitionRepository, compute_definition_id
from .storage import StreamStore, spec_fingerprint_for
from . import versions


class GenerationManager:
    def __init__(self, root: str | Path, *, authorizer: Authorizer | None = None,
                 clock: Callable[[], str] = utc_now_iso):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.repo = DefinitionRepository(self.root / "definitions")
        self.store = StreamStore(self.root / "streams")
        self.audit = AuditLog(self.root / "audit.log", clock=clock)
        self.authorizer = authorizer or Authorizer()
        self._clock = clock
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._create_lock = threading.Lock()

    # ------------------------------------------------------------------ locks
    def _stream_lock(self, definition_id: str, stream_name: str) -> threading.Lock:
        key = f"{definition_id}|{stream_name}"
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    # ------------------------------------------------------------ definitions
    def create_definition(self, *, principal, name, seed, streams,
                          algorithm_version=None, description="",
                          shared_with=(), idempotency_key=None):
        # Role gate applies before any persistence or audit of success.
        self.authorizer.require_generate(principal)
        if not isinstance(name, str) or not name:
            raise ValidationError("name must be a non-empty string")
        if not isinstance(seed, str) or not seed:
            raise ValidationError("seed must be a non-empty string")
        version = algorithm_version or versions.DEFAULT_VERSION
        versions.require_version(version)
        parsed_streams = tuple(StreamSpec.from_dict(s) for s in streams)
        if len({s.name for s in parsed_streams}) != len(parsed_streams):
            raise ValidationError("duplicate stream names")

        payload = {
            "name": name,
            "seed": seed,
            "streams": [
                {
                    "name": s.name,
                    "row_count": s.row_count,
                    "fields": [
                        {"name": f.name, "kind": f.kind, "params": f.params}
                        for f in s.fields
                    ],
                }
                for s in parsed_streams
            ],
            "algorithm_version": version,
            "description": description,
            "created_by": principal.user_id,
            "shared_with": tuple(shared_with),
        }
        definition_id = compute_definition_id(payload)
        with self._create_lock:
            if self.repo.exists(definition_id):
                existing = self.repo.get(definition_id)
                self.audit.record(principal.user_id, "create", definition_id,
                                  "duplicate", "idempotent_replay")
                return existing, False

            # Same name but different author content: if the client tagged this
            # submission with an idempotency key, this is a conflicting replay.
            same_name = self.repo.find_by_name(name)
            if same_name is not None:
                if idempotency_key is not None:
                    raise IdempotencyConflictError(
                        "idempotency key was already used for different content",
                    )
                raise ValidationError(
                    f"definition named {name!r} already exists with different "
                    "content",
                )

            definition = GenerationDefinition(
                definition_id=definition_id,
                name=name,
                seed=seed,
                streams=parsed_streams,
                algorithm_version=version,
                created_by=principal.user_id,
                created_at=self._clock(),
                description=description,
                shared_with=tuple(shared_with),
            )
            validate_field_kinds(definition)
            self.repo.save(definition)
        self.audit.record(principal.user_id, "create", definition_id, "allowed",
                          f"version={version}")
        return definition, True

    def get_definition(self, *, principal, definition_id) -> GenerationDefinition:
        definition = self._load(definition_id)
        if not self.authorizer.can_read(principal, definition):
            self.audit.record(principal.user_id, "read", definition_id,
                              "denied", "not_creator_or_owner")
            self.authorizer.require_read(principal, definition)
        self.audit.record(principal.user_id, "read", definition_id, "allowed")
        return definition

    def list_definitions(self, *, principal) -> list[GenerationDefinition]:
        out = []
        for definition in self.repo.list_all():
            if self.authorizer.can_read(principal, definition):
                out.append(definition)
        return out

    def _load(self, definition_id: str) -> GenerationDefinition:
        if not self.repo.exists(definition_id):
            raise DefinitionNotFoundError(
                f"definition {definition_id!r} not found")
        return self.repo.get(definition_id)

    # -------------------------------------------------------------- generation
    def ensure_stream(self, *, principal, definition_id, stream_name,
                      cancel_event=None) -> StoredStream:
        definition = self._load(definition_id)
        self.authorizer.require_read(principal, definition)
        stream = definition.get_stream(stream_name)
        if stream is None:
            self.audit.record(principal.user_id, "generate",
                              f"{definition_id}/{stream_name}", "denied",
                              "stream_not_found")
            raise StreamNotFoundError(
                f"stream {stream_name!r} does not exist in {definition_id!r}")

        lock = self._stream_lock(definition_id, stream_name)
        with lock:
            stored = self._validated_or_none(definition, stream)
            if stored is not None:
                self.audit.record(principal.user_id, "generate",
                                  f"{definition_id}/{stream_name}", "cache_hit")
                return stored
            self.audit.record(principal.user_id, "generate",
                              f"{definition_id}/{stream_name}", "started")
            self._generate_locked(definition, stream, cancel_event)
            stored = self.store.verify(
                definition_id, stream_name,
                spec_fingerprint_for(definition, stream))
            self.audit.record(principal.user_id, "generate",
                              f"{definition_id}/{stream_name}", "committed",
                              f"rows={stored.row_count}")
            return stored

    def _validated_or_none(self, definition: GenerationDefinition,
                           stream: StreamSpec):
        if not self.store.exists(definition.definition_id, stream.name):
            return None
        try:
            return self.store.verify(
                definition.definition_id, stream.name,
                spec_fingerprint_for(definition, stream))
        except CacheCorruptedError:
            # Detected corruption: remove and rebuild below.
            self.audit.record(definition.created_by or "system", "cache.verify",
                              f"{definition.definition_id}/{stream.name}",
                              "corrupt", "will_rebuild")
            self.store.delete(definition.definition_id, stream.name)
            return None

    def _generate_locked(self, definition: GenerationDefinition,
                         stream: StreamSpec, cancel_event) -> None:
        writer = self.store.open_writer(definition.definition_id, stream.name)
        try:
            for row in iter_stream(definition, stream):
                if cancel_event is not None and cancel_event.is_set():
                    raise GenerationCancelledError("generation cancelled")
                writer.write_row(row)
            self.store.commit_writer(
                writer, spec_fp=spec_fingerprint_for(definition, stream))
        except BaseException:
            self.store.abort_writer(writer)
            raise

    # ------------------------------------------------------------------ reads
    def read_page(self, *, principal, definition_id, stream_name,
                  offset=0, limit=100):
        definition = self._load(definition_id)
        self.authorizer.require_read(principal, definition)
        stream = definition.get_stream(stream_name)
        if stream is None:
            raise StreamNotFoundError(
                f"stream {stream_name!r} does not exist")
        # Reading must never rebuild under the caller implicitly; ensure_stream
        # handles integrity + regeneration explicitly.
        stored = self._validated_or_none(definition, stream)
        if stored is None:
            raise CacheCorruptedError(
                f"stream {stream_name!r} is not generated yet",
                code="cache_missing",
            )
        self.audit.record(principal.user_id, "read",
                          f"{definition_id}/{stream_name}", "allowed",
                          f"offset={offset},limit={limit}")
        rows = list(self.store.iter_rows(definition_id, stream_name,
                                         offset=offset, limit=limit))
        return {
            "definition_id": definition_id,
            "stream": stream_name,
            "offset": offset,
            "limit": limit,
            "row_count": stored.row_count,
            "rows": rows,
        }

    def stream_rows(self, *, principal, definition_id, stream_name):
        """Generator yielding verified rows lazily (no full buffering).

        Integrity (checksum + row count + spec fingerprint) is verified
        *before* the first row is emitted, so a corrupted/missing cache is
        reported as an error instead of streaming partial data.
        """
        definition = self._load(definition_id)
        self.authorizer.require_read(principal, definition)
        stream = definition.get_stream(stream_name)
        if stream is None:
            raise StreamNotFoundError(
                f"stream {stream_name!r} does not exist")
        self.store.verify(definition_id, stream_name,
                          spec_fingerprint_for(definition, stream))
        self.audit.record(principal.user_id, "stream",
                          f"{definition_id}/{stream_name}", "allowed")
        yield from self.store.iter_rows(definition_id, stream_name)

    # ------------------------------------------------------------- operations
    def versions_affected_by_switch(self, new_version: str) -> dict:
        """Report which definitions would be affected by switching default.

        Existing definitions are pinned, so none are mutated; this is a
        read-only impact analysis.
        """
        versions.require_version(new_version)
        affected = [d.definition_id for d in self.repo.list_all()
                    if d.algorithm_version != new_version]
        return {"new_version": new_version,
                "affected_definition_ids": sorted(affected),
                "count": len(affected)}

    def verify_cache(self, definition_id: str) -> dict:
        definition = self._load(definition_id)
        report = {}
        for stream in definition.streams:
            try:
                stored = self.store.verify(
                    definition_id, stream.name,
                    spec_fingerprint_for(definition, stream))
                report[stream.name] = {"ok": True, "rows": stored.row_count}
            except CacheCorruptedError as exc:
                report[stream.name] = {"ok": False, "reason": exc.code}
        return report
