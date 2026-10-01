"""Content-addressed result storage with manifests and verification.

Results are generated into a staging directory and only atomically
renamed into their final location once complete -- a crash or
cancellation never leaves a visible half-written result.

A manifest records the per-stream SHA-256 of every published file.  On
access the digest is verified: a missing or corrupted file is detected
and rebuilt from the definition and seed (which is deterministic, so the
rebuilt bytes are identical to the original).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from typing import Callable

from . import engines
from .canonical import canonical_dumps, derive_root_key, derive_stream_seed

STREAM_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def validate_stream_name(name: str) -> None:
    if not isinstance(name, str) or not STREAM_NAME_RE.match(name):
        raise ValueError(
            f"invalid stream name {name!r}: must match {STREAM_NAME_RE.pattern}"
        )


def _atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON atomically: temp file + fsync + rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(canonical_dumps(payload).decode("utf-8"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class ResultStore:
    """File-backed store for definitions, results and manifests."""

    def __init__(self, config) -> None:
        self.cfg = config
        self.cfg.ensure_dirs()

    # -- paths -------------------------------------------------------------
    def definition_path(self, def_id: str) -> Path:
        return self.cfg.definitions_dir / f"{def_id}.json"

    def manifest_path(self, def_id: str) -> Path:
        return self.cfg.results_dir / def_id / "manifest.json"

    def stream_path(self, def_id: str, stream_name: str) -> Path:
        validate_stream_name(stream_name)
        return self.cfg.results_dir / def_id / f"{stream_name}.jsonl"

    def staging_dir_for(self, job_id: str) -> Path:
        return self.cfg.staging_dir / job_id

    # -- definitions -------------------------------------------------------
    def save_definition(self, record: dict) -> None:
        _atomic_write_json(self.definition_path(record["def_id"]), record)

    def load_definition(self, def_id: str) -> dict:
        path = self.definition_path(def_id)
        if not path.exists():
            raise KeyError(def_id)
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def list_definition_ids(self) -> list[str]:
        if not self.cfg.definitions_dir.exists():
            return []
        return sorted(p.stem for p in self.cfg.definitions_dir.glob("def_*.json"))

    # -- manifests ---------------------------------------------------------
    def load_manifest(self, def_id: str) -> dict | None:
        """Return the manifest, or None if it is missing or unreadable."""
        path = self.manifest_path(def_id)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def save_manifest(self, def_id: str, manifest: dict) -> None:
        _atomic_write_json(self.manifest_path(def_id), manifest)

    # -- staging / publishing ---------------------------------------------
    def stage_stream(
        self,
        definition: dict,
        stream_name: str,
        staging_dir: Path,
        should_cancel: Callable[[], bool] | None = None,
    ) -> Path:
        """Generate one stream into *staging_dir* and return the temp file.

        The file is fully written and fsynced before this returns.  If
        *should_cancel* returns True, generation aborts and the partial
        file is removed.
        """
        validate_stream_name(stream_name)
        spec = definition["streams"][stream_name]
        root = derive_root_key(definition["seed"])
        stream_seed = derive_stream_seed(root, stream_name)
        staging_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = staging_dir / f"{stream_name}.jsonl.tmp"
        try:
            with open(tmp_path, "wb") as f:
                for _start, chunk in engines.iter_chunks(
                    definition["algo_version"],
                    spec,
                    stream_seed,
                    self.cfg.gen_chunk_records,
                    delay=self.cfg.gen_chunk_delay,
                ):
                    if should_cancel is not None and should_cancel():
                        raise _GenerationCancelled()
                    f.write(chunk)
                f.flush()
                os.fsync(f.fileno())
        except _GenerationCancelled:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        return tmp_path

    def publish_stream(self, def_id: str, stream_name: str, staged_path: Path) -> dict:
        """Atomically move a staged stream into the result store.

        Returns the stream metadata (records, bytes, sha256).
        """
        validate_stream_name(stream_name)
        dest = self.stream_path(def_id, stream_name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_path, dest)
        data = dest.read_bytes()
        spec = self.load_definition(def_id)["streams"][stream_name]
        return {
            "file": f"{stream_name}.jsonl",
            "records": spec["count"],
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    def remove_staging(self, job_id: str) -> None:
        shutil.rmtree(self.staging_dir_for(job_id), ignore_errors=True)

    def sweep_staging(self) -> None:
        """Remove all staging directories (left over from crashed runs)."""
        if not self.cfg.staging_dir.exists():
            return
        for child in self.cfg.staging_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # -- verification ------------------------------------------------------
    def verify_stream(self, def_id: str, stream_name: str) -> tuple[str, str | None]:
        """Check one stream against its manifest.

        Returns (status, detail) where status is one of:
        ``ok``, ``missing``, ``corrupt``, ``manifest_missing``.
        """
        validate_stream_name(stream_name)
        manifest = self.load_manifest(def_id)
        path = self.stream_path(def_id, stream_name)
        if manifest is None:
            return "manifest_missing", "manifest is missing or unreadable"
        entry = manifest.get("streams", {}).get(stream_name)
        if entry is None:
            return "corrupt", "stream absent from manifest"
        if not path.exists():
            return "missing", f"result file {path} is missing"
        actual = _sha256_file(path)
        if actual != entry.get("sha256"):
            return (
                "corrupt",
                f"sha256 mismatch: expected {entry.get('sha256')}, got {actual}",
            )
        return "ok", None

    def verify_all(self, def_id: str) -> dict[str, tuple[str, str | None]]:
        definition = self.load_definition(def_id)
        return {name: self.verify_stream(def_id, name) for name in sorted(definition["streams"])}


class _GenerationCancelled(Exception):
    """Internal signal: generation was cancelled mid-stream."""
