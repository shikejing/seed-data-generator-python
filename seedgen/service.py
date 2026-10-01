"""High-level facade tying definitions, generation, jobs and storage."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import HTTPException

from .auth import (
    AuditLogger,
    Authenticator,
    User,
    can_read,
    require_admin,
    require_create,
)
from .canonical import definition_id, derive_root_key, derive_stream_seed
from .config import Config
from .engines import ALGORITHM_VERSIONS, generate_window, get_version, validate_spec
from .jobs import JobManager, generate_streams_sync
from .storage import ResultStore, validate_stream_name


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class SeedGenService:
    """Application service used by the API layer."""

    def __init__(self, config: Config) -> None:
        self.cfg = config
        self.store = ResultStore(config)
        self.jobs = JobManager(config, self.store)
        self.authenticator = Authenticator(config.users)
        self.audit = AuditLogger(config.audit_file)
        self.jobs.recover_on_startup()

    # -- helpers -----------------------------------------------------------
    def _load_definition(self, def_id: str) -> dict:
        try:
            return self.store.load_definition(def_id)
        except KeyError:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "reason": f"unknown definition {def_id}"},
            ) from None

    def _authorize_read(self, user: User, definition: dict) -> None:
        if not can_read(user, definition):
            from .auth import NOT_AUTHORIZED, NOT_OWNER

            reason = (
                NOT_AUTHORIZED
                if definition.get("owner") not in (None, user.name)
                else NOT_OWNER
            )
            raise HTTPException(
                status_code=403,
                detail={"error": reason, "reason": "read access denied"},
            )

    # -- definitions -------------------------------------------------------
    def create_definition(
        self,
        user: User,
        *,
        name: str,
        seed: str | int,
        streams: dict,
        algo_version: str | None,
        acl_users: list[str],
        acl_roles: list[str],
    ) -> dict:
        require_create(user)
        if not isinstance(streams, dict) or not streams:
            raise HTTPException(
                status_code=400,
                detail={"error": "bad_request", "reason": "streams must be a non-empty object"},
            )
        for stream_name, spec in streams.items():
            validate_stream_name(stream_name)
            try:
                validate_spec(spec)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail={"error": "bad_request", "reason": f"stream {stream_name!r}: {exc}"},
                ) from exc
        version = algo_version or self.cfg.default_algo_version
        try:
            get_version(version)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": "bad_request", "reason": str(exc)},
            ) from exc

        def_id = definition_id(version, seed, streams)
        try:
            existing = self.store.load_definition(def_id)
        except KeyError:
            existing = None
        if existing is not None:
            self.audit.log(
                user=user,
                action="create_definition",
                resource=def_id,
                result="ok",
                detail={"idempotent": True},
            )
            return existing

        record = {
            "def_id": def_id,
            "name": name,
            "seed": seed,
            "algo_version": version,
            "streams": streams,
            "owner": user.name,
            "acl": {"users": sorted(set(acl_users)), "roles": sorted(set(acl_roles))},
            "created_at": _now(),
        }
        self.store.save_definition(record)
        self.audit.log(
            user=user,
            action="create_definition",
            resource=def_id,
            result="ok",
            detail={"algo_version": version, "streams": sorted(streams)},
        )
        return record

    def list_definitions(self, user: User) -> list[dict]:
        visible = []
        for def_id in self.store.list_definition_ids():
            definition = self.store.load_definition(def_id)
            if can_read(user, definition):
                visible.append(definition)
        self.audit.log(
            user=user, action="list_definitions", resource="*", result="ok",
            detail={"count": len(visible)},
        )
        return visible

    def get_definition(self, user: User, def_id: str) -> dict:
        definition = self._load_definition(def_id)
        self._authorize_read(user, definition)
        self.audit.log(
            user=user, action="read_definition", resource=def_id, result="ok"
        )
        return definition

    # -- results -----------------------------------------------------------
    def _ensure_stream_valid(self, definition: dict, stream_name: str) -> None:
        """Verify a stream's cached result, rebuilding if needed."""
        status, _detail = self.store.verify_stream(definition["def_id"], stream_name)
        if status == "ok":
            return
        if status == "manifest_missing":
            # Without a manifest nothing can be trusted: rebuild all streams.
            streams = sorted(definition["streams"])
        else:
            streams = [stream_name]
        staging = self.store.staging_dir_for(
            f"rebuild-{definition['def_id']}-{'-'.join(streams)}"
        )
        generate_streams_sync(self.store, definition["def_id"], definition, streams, staging)

    def read_stream_page(
        self,
        user: User,
        def_id: str,
        stream_name: str,
        offset: int,
        limit: int,
    ) -> dict:
        definition = self._load_definition(def_id)
        self._authorize_read(user, definition)
        if stream_name not in definition["streams"]:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "reason": f"unknown stream {stream_name!r}"},
            )
        self._ensure_stream_valid(definition, stream_name)
        spec = definition["streams"][stream_name]
        total = spec["count"]
        offset = max(0, offset)
        limit = max(0, min(limit, 10_000))
        if offset >= total or limit == 0:
            records: list = []
        else:
            root = derive_root_key(definition["seed"])
            stream_seed = derive_stream_seed(root, stream_name)
            end = min(offset + limit, total)
            window = generate_window(
                definition["algo_version"], spec, stream_seed, offset, end - offset
            )
            records = [json.loads(line) for line in window.splitlines() if line]
        self.audit.log(
            user=user,
            action="read_stream",
            resource=f"{def_id}/{stream_name}",
            result="ok",
            detail={"offset": offset, "limit": limit, "returned": len(records)},
        )
        return {
            "def_id": def_id,
            "stream": stream_name,
            "offset": offset,
            "limit": limit,
            "total": total,
            "records": records,
        }

    def iter_stream_chunks(
        self,
        user: User,
        def_id: str,
        stream_name: str,
    ):
        """Yield JSONL chunks for a stream.

        Verification and any rebuild happen before the first chunk is
        yielded; the API layer is responsible for stopping early if the
        client disconnects.
        """
        definition = self._load_definition(def_id)
        self._authorize_read(user, definition)
        if stream_name not in definition["streams"]:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "reason": f"unknown stream {stream_name!r}"},
            )
        self._ensure_stream_valid(definition, stream_name)
        spec = definition["streams"][stream_name]
        root = derive_root_key(definition["seed"])
        stream_seed = derive_stream_seed(root, stream_name)
        from .engines import iter_chunks

        for _start, chunk in iter_chunks(
            definition["algo_version"],
            spec,
            stream_seed,
            self.cfg.gen_chunk_records,
        ):
            yield chunk
        self.audit.log(
            user=user,
            action="read_stream",
            resource=f"{def_id}/{stream_name}",
            result="ok",
            detail={"streamed": True},
        )

    # -- regeneration ------------------------------------------------------
    async def regenerate(
        self,
        user: User,
        def_id: str,
        streams: list[str] | None,
        force: bool,
    ) -> dict:
        definition = self._load_definition(def_id)
        if not (user.is_admin or definition.get("owner") == user.name):
            from .auth import NOT_OWNER

            raise HTTPException(
                status_code=403,
                detail={"error": NOT_OWNER, "reason": "only the owner or an admin may regenerate"},
            )
        job = await self.jobs.submit(def_id, streams, force, user.name)
        self.audit.log(
            user=user,
            action="regenerate",
            resource=def_id,
            result="ok",
            detail={"job_id": job["job_id"], "force": force, "status": job["status"]},
        )
        return job

    def get_job(self, user: User, job_id: str) -> dict:
        job = self.jobs.load_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "reason": f"unknown job {job_id}"},
            )
        definition = self._load_definition(job["def_id"])
        self._authorize_read(user, definition)
        return job

    # -- verification ------------------------------------------------------
    def verify(self, user: User, def_id: str, rebuild: bool) -> dict:
        definition = self._load_definition(def_id)
        self._authorize_read(user, definition)
        report = self.store.verify_all(def_id)
        rebuilt = []
        if rebuild:
            bad = [name for name, (status, _) in report.items() if status != "ok"]
            if bad:
                staging = self.store.staging_dir_for(f"rebuild-{def_id}")
                generate_streams_sync(
                    self.store,
                    def_id,
                    definition,
                    bad,
                    staging,
                )
                rebuilt = bad
                report = self.store.verify_all(def_id)
        self.audit.log(
            user=user,
            action="verify",
            resource=def_id,
            result="ok",
            detail={"rebuilt": rebuilt},
        )
        return {
            "def_id": def_id,
            "rebuilt": rebuilt,
            "streams": {
                name: {"status": status, "detail": detail}
                for name, (status, detail) in report.items()
            },
        }

    # -- versions ----------------------------------------------------------
    def list_versions(self, user: User) -> dict:
        require_admin(user)
        counts: dict[str, int] = {}
        for def_id in self.store.list_definition_ids():
            definition = self.store.load_definition(def_id)
            counts[definition["algo_version"]] = counts.get(definition["algo_version"], 0) + 1
        return {
            "default": self.cfg.default_algo_version,
            "versions": [
                {
                    "name": name,
                    "frozen": version.frozen,
                    "note": version.note,
                    "definitions": counts.get(name, 0),
                }
                for name, version in ALGORITHM_VERSIONS.items()
            ],
        }

    def set_default_version(self, user: User, version: str) -> dict:
        require_admin(user)
        try:
            get_version(version)
        except Exception as exc:
            raise HTTPException(
                status_code=400, detail={"error": "bad_request", "reason": str(exc)}
        ) from exc
        previous = self.cfg.default_algo_version
        if version == previous:
            return {"default": version, "changed": False}
        self.cfg.default_algo_version = version
        counts: dict[str, int] = {}
        for def_id in self.store.list_definition_ids():
            definition = self.store.load_definition(def_id)
            counts[definition["algo_version"]] = counts.get(definition["algo_version"], 0) + 1
        self.audit.log(
            user=user,
            action="set_default_version",
            resource="default_algo_version",
            result="ok",
            detail={"from": previous, "to": version},
        )
        return {
            "default": version,
            "changed": True,
            "previous": previous,
            "affected_definitions": counts,
            "note": (
                "Existing definitions stay pinned to their recorded version; "
                "only definitions created after this switch use the new default."
            ),
        }
