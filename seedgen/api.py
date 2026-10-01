"""FastAPI HTTP API for the seed data generator."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .config import Config
from .service import SeedGenService


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class StreamSpecIn(BaseModel):
    kind: str
    count: int = Field(..., ge=0)
    params: dict[str, Any] = Field(default_factory=dict)


class DefinitionIn(BaseModel):
    name: str = ""
    seed: str | int
    algo_version: str | None = None
    streams: dict[str, StreamSpecIn]
    acl_users: list[str] = Field(default_factory=list)
    acl_roles: list[str] = Field(default_factory=list)


class RegenerateIn(BaseModel):
    streams: list[str] | None = None
    force: bool = False


class DefaultVersionIn(BaseModel):
    version: str


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(config: Config | None = None) -> FastAPI:
    config = config or Config()
    service = SeedGenService(config)

    app = FastAPI(title="seed-data-generator", version="0.1.0")

    def _user(request: Request):
        return service.authenticator.resolve(request)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    # -- audit denials ----------------------------------------------------
    def _infer_action(request: Request) -> str:
        path = request.url.path
        method = request.method
        if path.endswith("/regenerate"):
            return "regenerate"
        if path.endswith("/verify"):
            return "verify"
        if "/streams/" in path and (path.endswith("/results") or path.endswith("/stream")):
            return "read_stream"
        if path.startswith("/jobs/"):
            return "read_job"
        if path.startswith("/definitions") and method == "POST":
            return "create_definition"
        if path.startswith("/definitions") and method == "GET":
            return "read_definition"
        if path.startswith("/admin/versions") and method == "POST":
            return "set_default_version"
        return "access"

    @app.exception_handler(HTTPException)
    async def audit_denials(request: Request, exc: HTTPException):
        if exc.status_code in (401, 403):
            user = None
            try:
                user = service.authenticator.resolve(request)
            except HTTPException:
                pass
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            service.audit.log(
                user=user,
                action=_infer_action(request),
                resource=request.url.path,
                result="denied",
                reason=detail.get("error"),
                detail={"reason": detail.get("reason")},
            )
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    # -- definitions -------------------------------------------------------
    @app.post("/definitions", status_code=201)
    def create_definition(payload: DefinitionIn, request: Request):
        user = _user(request)
        streams = {name: spec.model_dump() for name, spec in payload.streams.items()}
        return service.create_definition(
            user,
            name=payload.name,
            seed=payload.seed,
            streams=streams,
            algo_version=payload.algo_version,
            acl_users=payload.acl_users,
            acl_roles=payload.acl_roles,
        )

    @app.get("/definitions")
    def list_definitions(request: Request):
        user = _user(request)
        return {"definitions": service.list_definitions(user)}

    @app.get("/definitions/{def_id}")
    def get_definition(def_id: str, request: Request):
        user = _user(request)
        return service.get_definition(user, def_id)

    # -- results -----------------------------------------------------------
    @app.get("/definitions/{def_id}/streams/{stream_name}/results")
    def read_results(
        def_id: str,
        stream_name: str,
        request: Request,
        offset: int = 0,
        limit: int = 100,
    ):
        user = _user(request)
        return service.read_stream_page(user, def_id, stream_name, offset, limit)

    @app.get("/definitions/{def_id}/streams/{stream_name}/stream")
    async def stream_results(def_id: str, stream_name: str, request: Request):
        user = _user(request)
        generator = service.iter_stream_chunks(user, def_id, stream_name)

        async def response_stream():
            for chunk in generator:
                if await request.is_disconnected():
                    break
                yield chunk

        return StreamingResponse(response_stream(), media_type="application/x-ndjson")

    # -- jobs --------------------------------------------------------------
    @app.post("/definitions/{def_id}/regenerate")
    async def regenerate(def_id: str, payload: RegenerateIn, request: Request):
        user = _user(request)
        return await service.regenerate(user, def_id, payload.streams, payload.force)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str, request: Request):
        user = _user(request)
        return service.get_job(user, job_id)

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str, request: Request):
        user = _user(request)
        job = service.jobs.load_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "not_found", "reason": f"unknown job {job_id}"},
            )
        definition = service._load_definition(job["def_id"])
        if not (user.is_admin or definition.get("owner") == user.name):
            raise HTTPException(
                status_code=403,
                detail={"error": "not_owner", "reason": "only the owner or an admin may cancel"},
            )
        result = service.jobs.cancel(job_id)
        service.audit.log(
            user=user, action="cancel_job", resource=job_id, result="ok"
        )
        return result

    # -- verification ------------------------------------------------------
    @app.post("/definitions/{def_id}/verify")
    def verify(def_id: str, request: Request, rebuild: bool = False):
        user = _user(request)
        return service.verify(user, def_id, rebuild)

    # -- admin: versions ---------------------------------------------------
    @app.get("/admin/versions")
    def list_versions(request: Request):
        user = _user(request)
        return service.list_versions(user)

    @app.post("/admin/versions/default")
    def set_default_version(payload: DefaultVersionIn, request: Request):
        user = _user(request)
        return service.set_default_version(user, payload.version)

    return app
