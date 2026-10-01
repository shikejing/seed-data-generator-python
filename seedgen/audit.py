"""Append-only, tamper-evident audit trail for access and generation.

Each entry is a single canonical JSON line containing a UTC ISO-8601
timestamp, the actor, action, resource, result and a free-form reason.
Entries are chained: every record stores the SHA-256 of the previous record,
so deleting or altering a line is detectable.  The clock is injectable so
tests are deterministic; production code uses real UTC time -- the clock
never feeds generated data.
"""

from __future__ import annotations

import dataclasses
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .canonical import canonical_json


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclasses.dataclass(frozen=True)
class AuditEntry:
    timestamp: str
    actor: str
    action: str
    resource: str
    result: str
    reason: str = ""
    prev_hash: str = ""
    entry_hash: str = ""

    def to_record(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "actor": self.actor,
            "action": self.action,
            "resource": self.resource,
            "result": self.result,
            "reason": self.reason,
            "prev_hash": self.prev_hash,
            "entry_hash": self.entry_hash,
        }

    @staticmethod
    def from_record(rec: dict) -> "AuditEntry":
        return AuditEntry(
            timestamp=str(rec["timestamp"]),
            actor=str(rec["actor"]),
            action=str(rec["action"]),
            resource=str(rec["resource"]),
            result=str(rec["result"]),
            reason=str(rec.get("reason", "")),
            prev_hash=str(rec.get("prev_hash", "")),
            entry_hash=str(rec.get("entry_hash", "")),
        )


class AuditLog:
    def __init__(self, path: str | Path, clock: Callable[[], str] = utc_now_iso):
        self.path = Path(path)
        self._clock = clock
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _last_hash(self) -> str:
        if not self.path.exists():
            return ""
        last = ""
        with open(self.path, "rb") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    import json
                    last = str(json.loads(line).get("entry_hash", ""))
        return last

    def record(self, actor: str, action: str, resource: str,
               result: str, reason: str = "") -> AuditEntry:
        entry = AuditEntry(
            timestamp=self._clock(),
            actor=actor,
            action=action,
            resource=resource,
            result=result,
            reason=reason,
            prev_hash=self._last_hash(),
        )
        rec = entry.to_record()
        body = dict(rec)
        body.pop("entry_hash")
        entry = dataclasses.replace(
            entry, entry_hash=hashlib.sha256(canonical_json(body)).hexdigest())
        with open(self.path, "ab") as fh:
            fh.write(canonical_json(entry.to_record()) + b"\n")
            fh.flush()
            os.fsync(fh.fileno())
        return entry

    def read_all(self) -> list[AuditEntry]:
        if not self.path.exists():
            return []
        out = []
        with open(self.path, "rb") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    import json
                    out.append(AuditEntry.from_record(json.loads(line)))
        return out

    def verify_chain(self) -> bool:
        """Return True iff the log is intact and the hash chain is valid."""
        import json

        prev = ""
        with open(self.path, "rb") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                rec = json.loads(raw)
                claimed = rec.pop("entry_hash")
                if rec.get("prev_hash", "") != prev:
                    return False
                if hashlib.sha256(canonical_json(rec)).hexdigest() != claimed:
                    return False
                prev = claimed
        return True
