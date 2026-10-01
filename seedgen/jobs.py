"""Idempotent, cross-process job submission and execution.

Jobs are keyed by the hash of their request (definition, stream subset,
force flag).  An in-process asyncio lock plus a file lock (fcntl)
serializes concurrent submissions; repeated or concurrent submissions
converge to a single job and a single generation.

Generation runs in a dedicated worker *thread* (not an asyncio task), so
it is unaffected by request-lifecycle cancellation in the server or test
harness.  Cancellation and timeout set a flag that is honoured between
chunks, after which the staging area is removed.  Nothing half-written is
ever published.
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from .canonical import job_id
from .storage import ResultStore, _GenerationCancelled, validate_stream_name


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class JobManager:
    """Manages job submission, execution and persistence."""

    def __init__(self, config, store: ResultStore) -> None:
        self.cfg = config
        self.store = store
        # In-process locks are threading locks (not asyncio locks) so they
        # work regardless of which event loop a request runs on -- the test
        # harness uses a loop per thread while a server shares one loop.
        self._in_process_locks: dict[str, threading.Lock] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._cancel_reasons: dict[str, str] = {}
        self._watchdogs: dict[str, threading.Timer] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="seedgen-job"
        )

    # -- persistence -------------------------------------------------------
    def job_path(self, job_id: str) -> Path:
        return self.cfg.jobs_dir / f"{job_id}.json"

    def load_job(self, jid: str) -> dict | None:
        path = self.job_path(jid)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def save_job(self, job: dict) -> None:
        self.cfg.jobs_dir.mkdir(parents=True, exist_ok=True)
        final = self.job_path(job["job_id"])
        tmp = final.with_name(final.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(job, sort_keys=True, separators=(",", ":")))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, final)

    def list_jobs(self) -> list[dict]:
        jobs = []
        for p in sorted(self.cfg.jobs_dir.glob("job_*.json")):
            job = self.load_job(p.stem)
            if job is not None:
                jobs.append(job)
        return jobs

    # -- locking -----------------------------------------------------------
    def _in_process_lock(self, key: str) -> threading.Lock:
        lock = self._in_process_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            self._in_process_locks[key] = lock
        return lock

    class _FileLock:
        """Non-blocking file lock.

        ``fcntl.flock`` blocks the calling thread, which would deadlock
        the shared event loop.  We poll with ``LOCK_NB`` and yield to the
        loop between attempts.
        """

        def __init__(self, path: Path) -> None:
            self.path = path
            self.fd = None

        async def __aenter__(self):
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.fd = open(self.path, "w")
            while True:
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return self
                except BlockingIOError:
                    await asyncio.sleep(0.01)

        async def __aexit__(self, *exc):
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()

    # -- submission --------------------------------------------------------
    async def submit(
        self,
        def_id: str,
        streams: list[str] | None,
        force: bool,
        submitted_by: str,
    ) -> dict:
        """Submit a generation job, returning the existing job if possible.

        Repeated submissions of the same request never regenerate: a done
        job is returned as-is and a running job is returned for polling.
        """
        definition = self.store.load_definition(def_id)  # raises KeyError if missing
        requested = self._resolve_streams(definition, streams)
        key = job_id(def_id, requested, force)

        # The in-process lock is a threading lock: it is loop-independent
        # and serializes submissions within this process.
        with self._in_process_lock(key):
            async with self._FileLock(self.cfg.locks_dir / f"{key}.lock"):
                existing = self.load_job(key)
                if existing is not None and existing["status"] in ("done", "running", "queued"):
                    return existing
                # No job, or a terminal state: (re)create and run.
                job = {
                    "job_id": key,
                    "def_id": def_id,
                    "streams": requested,
                    "force": force,
                    "status": "queued",
                    "created_at": _now(),
                    "started_at": None,
                    "finished_at": None,
                    "error": None,
                    "results": {},
                    "submitted_by": submitted_by,
                    "attempt": (existing or {}).get("attempt", 0) + 1,
                }
                self.save_job(job)
                self._start(job["job_id"])
                return job

    def _resolve_streams(self, definition: dict, streams: list[str] | None) -> list[str]:
        available = sorted(definition["streams"])
        if streams is None:
            return available
        for name in streams:
            validate_stream_name(name)
            if name not in definition["streams"]:
                raise KeyError(name)
        return sorted(set(streams))

    # -- cancellation ------------------------------------------------------
    def cancel(self, jid: str) -> dict | None:
        """Request cancellation of a running job. Returns the job or None."""
        job = self.load_job(jid)
        if job is None or job["status"] not in ("queued", "running"):
            return job
        event = self._cancel_events.get(jid)
        if event is not None:
            self._cancel_reasons[jid] = "cancelled"
            event.set()
        return job

    # -- execution ---------------------------------------------------------
    def _start(self, jid: str) -> None:
        cancel_event = threading.Event()
        self._cancel_events[jid] = cancel_event
        self._cancel_reasons[jid] = "timeout"  # watchdog fires first if it fires
        self._executor.submit(self._run_sync, jid, cancel_event)
        if self.cfg.job_timeout_seconds > 0:
            timer = threading.Timer(self.cfg.job_timeout_seconds, self._on_timeout, args=(jid,))
            timer.daemon = True
            self._watchdogs[jid] = timer
            timer.start()

    def _on_timeout(self, jid: str) -> None:
        event = self._cancel_events.get(jid)
        if event is not None:
            self._cancel_reasons[jid] = "timeout"
            event.set()

    def _run_sync(self, jid: str, cancel_event: threading.Event) -> None:
        """Thread target: run generation to completion or cancellation."""
        job = self.load_job(jid)
        job["status"] = "running"
        job["started_at"] = _now()
        self.save_job(job)
        try:
            self._execute_sync(jid, cancel_event)
            job = self.load_job(jid)
            if job["status"] == "running":
                job["status"] = "done"
                job["finished_at"] = _now()
                self.save_job(job)
        except _GenerationInterrupted:
            self._cleanup(jid)
            job = self.load_job(jid)
            reason = self._cancel_reasons.get(jid, "cancelled")
            job["status"] = "cancelled" if reason == "cancelled" else "failed"
            job["finished_at"] = _now()
            job["error"] = reason
            self.save_job(job)
        except Exception as exc:  # noqa: BLE001
            self._cleanup(jid)
            job = self.load_job(jid)
            job["status"] = "failed"
            job["finished_at"] = _now()
            job["error"] = f"{type(exc).__name__}: {exc}"
            self.save_job(job)
        finally:
            watchdog = self._watchdogs.pop(jid, None)
            if watchdog is not None:
                watchdog.cancel()
            self._cancel_events.pop(jid, None)
            self._cancel_reasons.pop(jid, None)

    def _execute_sync(self, jid: str, cancel_event: threading.Event) -> None:
        """Synchronous generation body, running in a worker thread."""
        job = self.load_job(jid)
        definition = self.store.load_definition(job["def_id"])
        staging = self.store.staging_dir_for(jid)
        staging.mkdir(parents=True, exist_ok=True)
        metas = generate_streams_sync(
            self.store,
            job["def_id"],
            definition,
            job["streams"],
            staging,
            cancel_event,
        )
        job = self.load_job(jid)
        job["results"] = metas
        self.save_job(job)

    def _cleanup(self, jid: str) -> None:
        self.store.remove_staging(jid)

    # -- restart -----------------------------------------------------------
    def recover_on_startup(self) -> None:
        """Sweep staging areas and mark jobs interrupted by a crash."""
        self.store.sweep_staging()
        for job in self.list_jobs():
            if job["status"] in ("queued", "running"):
                job["status"] = "interrupted"
                job["finished_at"] = _now()
                job["error"] = "process restarted before completion"
                self.save_job(job)


class _GenerationInterrupted(Exception):
    """Internal signal: generation was cancelled or timed out."""


def merge_manifest(store: ResultStore, definition: dict, metas: dict[str, dict]) -> None:
    """Merge stream metas into the manifest and persist it."""
    def_id = definition["def_id"]
    existing = store.load_manifest(def_id)
    if existing is None:
        existing = {
            "def_id": def_id,
            "algo_version": definition["algo_version"],
            "seed": definition["seed"],
            "streams": {},
        }
    for name, meta in metas.items():
        existing["streams"][name] = meta
    store.save_manifest(def_id, existing)


def generate_streams_sync(
    store: ResultStore,
    def_id: str,
    definition: dict,
    streams: list[str],
    staging_dir: Path,
    cancel_event: threading.Event | None = None,
) -> dict[str, dict]:
    """Stage, publish and manifest one or more streams synchronously.

    This is the single generation primitive used by both jobs and
    cache rebuilds.  Returns the per-stream result metadata.
    """
    metas: dict[str, dict] = {}
    try:
        for stream_name in streams:
            if cancel_event is not None and cancel_event.is_set():
                raise _GenerationInterrupted()
            staged = store.stage_stream(
                definition,
                stream_name,
                staging_dir,
                should_cancel=cancel_event.is_set if cancel_event else None,
            )
            metas[stream_name] = store.publish_stream(def_id, stream_name, staged)
        if cancel_event is not None and cancel_event.is_set():
            raise _GenerationInterrupted()
        merge_manifest(store, definition, metas)
    except (_GenerationInterrupted, _GenerationCancelled):
        store.remove_staging(staging_dir.name)
        raise _GenerationInterrupted()
    # Always remove the staging area: on success the files have been
    # atomically renamed into the result store, so nothing is lost.
    store.remove_staging(staging_dir.name)
    return metas
