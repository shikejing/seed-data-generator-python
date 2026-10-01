"""Persistent repository of generation definitions.

Each definition is one canonical JSON document written atomically
(temp-file + ``os.replace``).  Definition ids are deterministic fingerprints
of the author-controlled content (name, seed, streams, pinned version,
ACL...) -- never of the wall clock -- so the same submission resolves to the
same id everywhere.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .canonical import canonical_json, fingerprint
from .errors import DefinitionNotFoundError, IdempotencyConflictError
from .models import GenerationDefinition


def compute_definition_id(payload: dict) -> str:
    """Stable id derived solely from author-controlled definition content."""
    content = {
        "name": payload["name"],
        "seed": payload["seed"],
        "streams": payload["streams"],
        "algorithm_version": payload.get("algorithm_version", ""),
        "description": payload.get("description", ""),
        "created_by": payload.get("created_by", ""),
        "shared_with": list(payload.get("shared_with", ())),
    }
    return "def_" + fingerprint(content)[:16]


class DefinitionRepository:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, definition_id: str) -> Path:
        return self.root / f"{definition_id}.json"

    def save(self, definition: GenerationDefinition) -> None:
        tmp = self.root / f".{definition.definition_id}.json.tmp.{os.getpid()}"
        with open(tmp, "wb") as fh:
            fh.write(canonical_json(definition.to_dict()))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._path(definition.definition_id))

    def get(self, definition_id: str) -> GenerationDefinition:
        path = self._path(definition_id)
        if not path.exists():
            raise DefinitionNotFoundError(
                f"definition {definition_id!r} not found")
        with open(path, "rb") as fh:
            return GenerationDefinition.from_dict(json.loads(fh.read()))

    def find_by_name(self, name: str) -> GenerationDefinition | None:
        for definition in self.list_all():
            if definition.name == name:
                return definition
        return None

    def list_all(self) -> list[GenerationDefinition]:
        out = []
        for path in sorted(self.root.glob("def_*.json")):
            with open(path, "rb") as fh:
                out.append(GenerationDefinition.from_dict(json.loads(fh.read())))
        return out

    def list_by_algorithm_version(self, version: str) -> list[GenerationDefinition]:
        return [d for d in self.list_all() if d.algorithm_version == version]

    def exists(self, definition_id: str) -> bool:
        return self._path(definition_id).exists()
