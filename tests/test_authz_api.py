"""Requirement 6: creator/authorized-role access, distinguishable denials,
and full auditability of generation and access.
"""

from fastapi.testclient import TestClient

from seedgen.config import Settings
from seedgen.manager import GenerationManager
from seedgen.api import create_app

ALICE = {"Authorization": "Bearer token-alice"}
BOB = {"Authorization": "Bearer token-bob"}
ADMIN = {"Authorization": "Bearer token-admin"}


def _client(tmp_path):
    settings = Settings(data_dir=str(tmp_path))
    m = GenerationManager(tmp_path)
    return TestClient(create_app(m, settings)), m


def _def(shared=None):
    return {
        "name": "sensitive", "seed": "s",
        "shared_with": shared or [],
        "streams": [{"name": "users", "row_count": 5,
                     "fields": [{"name": "id", "kind": "int"}]}],
    }


def test_missing_and_bad_token_are_unauthorized(tmp_path):
    client, _ = _client(tmp_path)
    client.post("/definitions", json=_def())
    assert client.post("/definitions", json=_def()).status_code == 401
    assert client.get("/definitions").status_code == 401
    assert client.get("/definitions",
                      headers={"Authorization": "Bearer nope"}).status_code == 401


def test_creator_admin_and_granted_user_can_access(tmp_path):
    client, m = _client(tmp_path)
    r = client.post("/definitions", json=_def(), headers=ALICE)
    did = r.json()["definition"]["definition_id"]

    assert client.get(f"/definitions/{did}", headers=ALICE).status_code == 200
    assert client.get(f"/definitions/{did}", headers=ADMIN).status_code == 200

    # bob denied with distinguishable reason
    denied = client.get(f"/definitions/{did}", headers=BOB)
    assert denied.status_code == 403
    assert denied.json()["error"] == "forbidden"
    assert denied.json()["reason"] == "not_creator_or_owner"

    # bob cannot read rows or stream either
    rows = client.get(f"/definitions/{did}/streams/users/rows", headers=BOB)
    assert rows.status_code == 403
    # and cannot trigger generation
    g = client.post(f"/definitions/{did}/streams/users/generate", headers=BOB)
    assert g.status_code == 403

    # grant bob explicitly on a new definition
    shared = _def(shared=["user:bob"])
    shared["name"] = "shared-one"
    did2 = client.post("/definitions", json=shared, headers=ALICE
                       ).json()["definition"]["definition_id"]
    assert client.get(f"/definitions/{did2}", headers=BOB).status_code == 200
    assert client.post(f"/definitions/{did2}/streams/users/generate",
                       headers=BOB).status_code == 200


def test_role_based_grant(tmp_path):
    client, _ = _client(tmp_path)
    payload = _def(shared=["role:auditor"])
    payload["name"] = "role-shared"
    did = client.post("/definitions", json=payload, headers=ALICE
                      ).json()["definition"]["definition_id"]
    # register a token for an auditor
    from seedgen.auth import Authorizer, Principal
    m_auth = client.app.state  # not used; manager captured via dependency below
    # Easier: construct a fresh client with custom authorizer.
    settings = Settings(data_dir=str(tmp_path))
    mgr = GenerationManager(
        tmp_path,
        authorizer=Authorizer({
            "token-alice": Principal("alice", ("generator",)),
            "token-aud": Principal("audrey", ("auditor",)),
        }),
    )
    c2 = TestClient(create_app(mgr, settings))
    assert c2.get(f"/definitions/{did}",
                  headers={"Authorization": "Bearer token-aud"}).status_code == 200


def test_list_definitions_is_filtered_by_permission(tmp_path):
    client, _ = _client(tmp_path)
    client.post("/definitions", json=_def(), headers=ALICE)
    bob_view = client.get("/definitions", headers=BOB).json()["definitions"]
    assert bob_view == []
    alice_view = client.get("/definitions", headers=ALICE).json()["definitions"]
    assert len(alice_view) == 1


def test_audit_log_records_access_generation_and_denials(tmp_path):
    client, m = _client(tmp_path)
    did = client.post("/definitions", json=_def(), headers=ALICE
                      ).json()["definition"]["definition_id"]
    client.post(f"/definitions/{did}/streams/users/generate", headers=ALICE)
    client.get(f"/definitions/{did}/streams/users/rows", headers=ALICE)
    client.get(f"/definitions/{did}", headers=BOB)  # denied

    entries = m.audit.read_all()
    actions = [(e.action, e.result, e.actor) for e in entries]
    assert ("create", "allowed", "alice") in actions
    assert any(a == "generate" and r == "committed" for a, r, _ in actions)
    assert any(a == "read" and r == "allowed" for a, r, _ in actions)
    assert any(r == "denied" and actor == "bob" for a, r, actor in actions)
    assert m.audit.verify_chain()


def test_non_generator_cannot_create_definitions(tmp_path):
    client, m = _client(tmp_path)
    from seedgen.auth import Authorizer, Principal
    settings = Settings(data_dir=str(tmp_path))
    mgr = GenerationManager(
        tmp_path,
        authorizer=Authorizer({
            "token-reader": Principal("rita", ("reader",)),
        }),
    )
    c2 = TestClient(create_app(mgr, settings))
    r = c2.post("/definitions", json=_def(),
                headers={"Authorization": "Bearer token-reader"})
    assert r.status_code == 403
    assert r.json()["reason"] == "generator_role_required"
