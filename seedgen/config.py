"""Configuration for the seedgen service.

Configuration only affects *where* data is stored and service-level behaviour.
Generation output is NEVER influenced by configuration or environment
variables: it is fully determined by the generation definition and its seed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Config:
    """Service configuration.

    Attributes:
        data_dir: root directory for definitions, results, jobs and audit logs.
        default_algo_version: algorithm version bound to definitions that do
            not specify one explicitly.  New algorithm versions never take
            effect automatically; this must be switched deliberately.
        job_timeout_seconds: wall-clock budget for a single generation job.
        gen_chunk_records: number of records per generated chunk.
        gen_chunk_delay: test-only hook: seconds slept per generated chunk.
        users: mapping of user name -> {"roles": [...]}.
    """

    data_dir: Path = field(default_factory=lambda: Path("data"))
    default_algo_version: str = "v1"
    job_timeout_seconds: float = 300.0
    gen_chunk_records: int = 1024
    gen_chunk_delay: float = 0.0
    users: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "admin": {"roles": ["admin"]},
            "alice": {"roles": ["creator"]},
            "bob": {"roles": ["viewer"]},
        }
    )

    # -- derived paths -----------------------------------------------------
    @property
    def definitions_dir(self) -> Path:
        return self.data_dir / "definitions"

    @property
    def results_dir(self) -> Path:
        return self.data_dir / "results"

    @property
    def jobs_dir(self) -> Path:
        return self.data_dir / "jobs"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def locks_dir(self) -> Path:
        return self.data_dir / "locks"

    @property
    def audit_file(self) -> Path:
        return self.data_dir / "audit" / "audit.jsonl"

    def ensure_dirs(self) -> None:
        for d in (
            self.definitions_dir,
            self.results_dir,
            self.jobs_dir,
            self.staging_dir,
            self.locks_dir,
            self.audit_file.parent,
        ):
            d.mkdir(parents=True, exist_ok=True)
