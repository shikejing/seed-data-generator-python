"""Authentication, role-based authorization and audit logging.

Deny reasons are deliberately distinguishable so callers can tell
*why* access was refused:

* ``unauthenticated`` -- no (or unknown) user identity was presented.
* ``insufficient_role`` -- the user's role does not permit the action.
* ``not_owner`` -- the user is not the definition's owner.
* ``not_authorized`` -- the user is not in the definition's ACL.

Every generation and access attempt is recorded in an append-only audit
log.
"""
from __future__ import annotations

import fcntl
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from fastapi import HTTPException, Request

# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

UNAUTHENTICATED = "unauthenticated"
INSUFFICIENT_ROLE = "insufficient_role"
NOT_OWNER = "not_owner"
NOT_AUTHORIZED = "not_authorized"


@dataclass(frozen=True)
class User:
    name: str
    roles: tuple[str, ...]

    @property
    def is_admin(self) -> bool:
        return "admin" in self.roles

    @property
    def is_creator(self) -> bool:
        return "creator" in self.roles or self.is_admin


class Authenticator:
    """Resolves users from the ``X-User`` request header."""

    def __init__(self, users: dict[str, dict]) -> None:
        self._users = users

    def resolve(self, request: Request) -> User:
        name = request.headers.get("X-User")
        if not name:
            raise HTTPException(
                status_code=401,
                detail={
                    "error": UNAUTHENTICATED,
                    "reason": "missing X-User header",
                },
            )
        record = self._users.get(name)
        if record is None:
            raise HTTPException(
                status_code=401,
                detail={
                    "error": UNAUTHENTICATED,
                    "reason": f"unknown user {name!r}",
                },
            )
        return User(name=name, roles=tuple(sorted(record.get("roles", []))))


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------

def can_create(user: User) -> bool:
    return user.is_creator


def can_read(user: User, definition: dict) -> bool:
    if user.is_admin:
        return True
    if definition.get("owner") == user.name:
        return True
    acl = definition.get("acl", {})
    if user.name in acl.get("users", []):
        return True
    if any(role in acl.get("roles", []) for role in user.roles):
        return True
    return False


def can_regenerate(user: User, definition: dict) -> bool:
    return user.is_admin or definition.get("owner") == user.name


def require_create(user: User) -> None:
    if not can_create(user):
        raise HTTPException(
            status_code=403,
            detail={
                "error": INSUFFICIENT_ROLE,
                "reason": "only creators and admins may create definitions",
            },
        )


def require_read(user: User, definition: dict) -> None:
    if not can_read(user, definition):
        if definition.get("owner") is not None and definition["owner"] != user.name:
            reason = NOT_AUTHORIZED
            message = "user is not the owner and is not in the definition's ACL"
        else:
            reason = NOT_OWNER
            message = "user is not the owner of this definition"
        raise HTTPException(
            status_code=403,
            detail={"error": reason, "reason": message},
        )


def require_regenerate(user: User, definition: dict) -> None:
    if not can_regenerate(user, definition):
        raise HTTPException(
            status_code=403,
            detail={
                "error": NOT_OWNER,
                "reason": "only the owner or an admin may regenerate results",
            },
        )


def require_admin(user: User) -> None:
    if not user.is_admin:
        raise HTTPException(
            status_code=403,
            detail={
                "error": INSUFFICIENT_ROLE,
                "reason": "this action requires the admin role",
            },
        )


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

class AuditLogger:
    """Append-only JSON-lines audit log with cross-process file locking."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(
        self,
        *,
        user: User | None,
        action: str,
        resource: str,
        result: str,
        reason: str | None = None,
        detail: dict | None = None,
    ) -> None:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "user": user.name if user else None,
            "roles": list(user.roles) if user else [],
            "action": action,
            "resource": resource,
            "result": result,
        }
        if reason is not None:
            entry["reason"] = reason
        if detail:
            entry["detail"] = detail
        line = json.dumps(entry, sort_keys=True, separators=(",", ":")) + "\n"
        with open(self.path, "a", encoding="utf-8") as f:
            for attempt in range(100):
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    import time

                    time.sleep(0.01)
            else:
                # Extremely unlikely; do not lose the audit entry.
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
