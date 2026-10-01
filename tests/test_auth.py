"""Authentication, authorization and audit tests.

Only the creator or authorized roles can read and regenerate.  Denial
reasons are distinguishable, and every access is recorded in the audit log.
"""
from __future__ import annotations

import json

from .conftest import ADMIN, ALICE, BOB, auth

STREAMS = {"s": {"kind": "booleans", "count": 5, "params": {}}}


def _create(client, user=ALICE, seed="auth-seed", acl_users=None, acl_roles=None):
    r = client.post(
        "/definitions",
        json={
            "seed": seed,
            "streams": STREAMS,
            "acl_users": acl_users or [],
            "acl_roles": acl_roles or [],
        },
        headers=auth(user),
    )
    assert r.status_code == 201, r.text
    return r.json()["def_id"]


def test_missing_header_is_401(client):
    r = client.post("/definitions", json={"seed": "x", "streams": STREAMS})
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "unauthenticated"


def test_unknown_user_is_401(client):
    r = client.post(
        "/definitions", json={"seed": "x", "streams": STREAMS},
        headers={"X-User": "nobody"},
    )
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "unauthenticated"


def test_viewer_cannot_create(client):
    r = client.post(
        "/definitions", json={"seed": "x", "streams": STREAMS},
        headers=auth(BOB),
    )
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "insufficient_role"


def test_owner_can_read(client):
    def_id = _create(client)
    r = client.get(f"/definitions/{def_id}", headers=auth(ALICE))
    assert r.status_code == 200


def test_non_owner_without_acl_denied(client):
    def_id = _create(client)
    r = client.get(f"/definitions/{def_id}", headers=auth(BOB))
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "not_authorized"


def test_acl_user_can_read(client):
    def_id = _create(client, acl_users=[BOB])
    r = client.get(f"/definitions/{def_id}", headers=auth(BOB))
    assert r.status_code == 200


def test_acl_role_can_read(client):
    def_id = _create(client, acl_roles=["viewer"])
    r = client.get(f"/definitions/{def_id}", headers=auth(BOB))
    assert r.status_code == 200


def test_non_owner_cannot_regenerate(client):
    def_id = _create(client)
    r = client.post(f"/definitions/{def_id}/regenerate", json={}, headers=auth(BOB))
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "not_owner"


def test_admin_can_read_anything(client):
    def_id = _create(client)
    r = client.get(f"/definitions/{def_id}", headers=auth(ADMIN))
    assert r.status_code == 200


def test_list_definitions_filters_by_access(client):
    _create(client, seed="alice-private")
    r = client.get("/definitions", headers=auth(BOB))
    assert r.status_code == 200
    # Bob sees only definitions he is authorized for (none here).
    assert all(d["owner"] != ALICE for d in r.json()["definitions"])


def test_audit_log_records_denials(client, config):
    def_id = _create(client)
    # Bob attempts to read (denied).
    client.get(f"/definitions/{def_id}", headers=auth(BOB))
    # Bob attempts to regenerate (denied).
    client.post(f"/definitions/{def_id}/regenerate", json={}, headers=auth(BOB))
    audit_file = config.audit_file
    assert audit_file.exists()
    entries = [json.loads(line) for line in audit_file.read_text().splitlines()]
    denied = [e for e in entries if e["result"] == "denied"]
    assert len(denied) >= 2
    reasons = {e["reason"] for e in denied}
    assert "not_authorized" in reasons
    assert "not_owner" in reasons
    # Every entry has a user and action.
    for e in entries:
        assert e["user"] is not None
        assert e["action"] in (
            "create_definition", "read_definition", "read_stream",
            "regenerate", "verify", "list_definitions", "cancel_job",
            "set_default_version",
        )
