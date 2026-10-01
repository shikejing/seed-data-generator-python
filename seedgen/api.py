"""FastAPI application exposing the seeded data generation service."""

from __future__ import annotations

import threading
from typing import Any

from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import versions
from .config import Settings, load_settings
from .errors import (
    CacheCorruptedError,
    DefinitionNotFoundError,
    ForbiddenError,
    GenerationCancelledError,
    IdempotencyConflictError,
    SeedGenError,
    StreamNotFoundError,
    UnauthorizedError,
    ValidationError as SeedValidationError,
)
from .manager import GenerationManager

_ERROR_STATUS = {
    "validation_error": 400,
    "idempotency_conflict": 409,
    "definition_not_found": 404,
    "stream_not_found": 404,
    "version_not_found": 400,
    "unauthorized": 401,
    "forbidden": 403,
    "generation_cancelled": 499,
    "cache_corrupted": 500,
    "cache_missing": 409,
}


class FieldIn(BaseModel):
    name: str
    kind: str
    params: dict[str, Any] = Field(default_factory=dict)


class StreamIn(BaseModel):
    name: str
    row_count: int = 0
    fields: list[FieldIn] = Field(default_factory=list)


class DefinitionIn(BaseModel):
    name: str
    seed: str
    streams: list[StreamIn] = Field(default_factory=list)
    algorithm_version: str | None = None
    description: str = ""
    shared_with: list[str] = Field(default_factory=list)
    idempotency_key: str | None = None


def _definition_out(d) -> dict:
    return {
        "definition_id": d.definition_id,
        "name": d.name,
        "seed": d.seed,
        "algorithm_version": d.algorithm_version,
        "created_by": d.created_by,
        "created_at": d.created_at,
        "description": d.description,
        "shared_with": list(d.shared_with),
        "streams": [
            {"name": s.name, "row_count": s.row_count,
             "fields": [{"name": f.name, "kind": f.kind, "params": f.params}
                        for f in s.fields]}
            for s in d.streams
        ],
    }


def create_app(manager: GenerationManager | None = None,
               settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    if manager is None:
        manager = GenerationManager(settings.data_dir)

    app = FastAPI(title="Seeded Data Generator", version="0.1.0")

    def principal(authorization: str | None):
        token = None
        if authorization:
            scheme, _, raw = authorization.partition(" ")
            if scheme.lower() == "bearer":
                token = raw.strip() or None
        return manager.authorizer.principal_from_token(token)

    @app.exception_handler(SeedGenError)
    async def _seed_error_handler(request, exc: SeedGenError):
        code = getattr(exc, "code", "seedgen_error")
        reason = getattr(exc, "reason", None)
        status = _ERROR_STATUS.get(code, 400)
        body: dict[str, Any] = {"error": code, "message": str(exc)}
        if reason:
            body["reason"] = reason
        return JSONResponse(status_code=status, content=body)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/versions")
    def list_versions():
        return {
            "default": versions.DEFAULT_VERSION,
            "current": versions.CURRENT_VERSION,
            "versions": [
                {"version": v, "description": versions.describe(v)}
                for v in versions.all_versions()
            ],
        }

    @app.post("/definitions", status_code=201)
    def create_definition(payload: DefinitionIn,
                          authorization: str | None = Header(default=None)):
        p = principal(authorization)
        streams = [s.model_dump() for s in payload.streams]
        definition, created = manager.create_definition(
            principal=p,
            name=payload.name,
            seed=payload.seed,
            streams=streams,
            algorithm_version=payload.algorithm_version,
            description=payload.description,
            shared_with=tuple(payload.shared_with),
            idempotency_key=payload.idempotency_key,
        )
        return JSONResponse(
            status_code=201 if created else 200,
            content={"created": created, "definition": _definition_out(definition)},
        )

    @app.get("/definitions")
    def list_definitions(authorization: str | None = Header(default=None)):
        p = principal(authorization)
        return {"definitions": [_definition_out(d)
                                for d in manager.list_definitions(principal=p)]}

    @app.get("/definitions/{definition_id}")
    def get_definition(definition_id: str,
                       authorization: str | None = Header(default=None)):
        p = principal(authorization)
        d = manager.get_definition(principal=p, definition_id=definition_id)
        return _definition_out(d)

    @app.post("/definitions/{definition_id}/streams/{stream_name}/generate")
    def generate(definition_id: str, stream_name: str,
                 timeout: float = Query(default=0.0, ge=0.0),
                 authorization: str | None = Header(default=None)):
        p = principal(authorization)
        cancel_event = threading.Event()
        limit = timeout if timeout > 0 else settings.generation_timeout_seconds

        box: dict[str, Any] = {}

        def work():
            try:
                box["stored"] = manager.ensure_stream(
                    principal=p, definition_id=definition_id,
                    stream_name=stream_name, cancel_event=cancel_event)
            except BaseException as exc:  # propagated back to caller
                box["error"] = exc

        worker = threading.Thread(target=work, daemon=True)
        worker.start()
        worker.join(limit)
        if worker.is_alive():
            cancel_event.set()
            worker.join()
            return JSONResponse(
                status_code=499,
                content={"error": "generation_timeout",
                         "message": f"generation exceeded {limit}s and was cancelled; "
                                    "no partial result is visible"},
            )
        if "error" in box:
            raise box["error"]
        stored = box["stored"]
        return {"status": "ready", "definition_id": definition_id,
                "stream": stream_name, "row_count": stored.row_count,
                "checksum_sha256": stored.checksum}

    @app.get("/definitions/{definition_id}/streams/{stream_name}/rows")
    def read_rows(definition_id: str, stream_name: str,
                  offset: int = Query(default=0, ge=0),
                  limit: int = Query(default=None, ge=1),
                  authorization: str | None = Header(default=None)):
        p = principal(authorization)
        page_limit = settings.default_page_size if limit is None else min(limit, settings.max_page_size)
        result = manager.read_page(principal=p, definition_id=definition_id,
                                   stream_name=stream_name, offset=offset,
                                   limit=page_limit)
        return result

    @app.get("/definitions/{definition_id}/streams/{stream_name}/stream")
    def stream_rows(definition_id: str, stream_name: str,
                    authorization: str | None = Header(default=None)):
        p = principal(authorization)

        def gen():
            import json
            for row in manager.stream_rows(principal=p, definition_id=definition_id,
                                           stream_name=stream_name):
                yield json.dumps(row, sort_keys=True) + "\n"

        return StreamingResponse(gen(), media_type="application/x-ndjson")

    @app.get("/definitions/{definition_id}/verify")
    def verify(definition_id: str,
               authorization: str | None = Header(default=None)):
        p = principal(authorization)
        d = manager.get_definition(principal=p, definition_id=definition_id)
        return {"definition_id": definition_id, "streams": manager.verify_cache(d.definition_id)}

    @app.get("/versions/{new_version}/impact")
    def version_impact(new_version: str,
                       authorization: str | None = Header(default=None)):
        principal(authorization)  # any authenticated user may inspect
        return manager.versions_affected_by_switch(new_version)

    return app


def create_default_app() -> FastAPI:
    return create_app()
