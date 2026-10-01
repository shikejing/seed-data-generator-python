"""Algorithm versioning tests.

Definitions record the algorithm version that produced them; new versions
never take effect unless explicitly selected, and switching the default
leaves existing definitions untouched.
"""
from __future__ import annotations

import hashlib

from seedgen.canonical import derive_root_key, derive_stream_seed
from seedgen.engines import ALGORITHM_VERSIONS, generate_window

from .conftest import ADMIN, ALICE, BOB, auth

STREAMS = {"s": {"kind": "integers", "count": 8, "params": {"low": 0, "high": 99}}}


def _create(client, seed="v-seed", algo_version=None):
    body = {"seed": seed, "streams": STREAMS}
    if algo_version:
        body["algo_version"] = algo_version
    r = client.post("/definitions", json=body, headers=auth(ALICE))
    assert r.status_code == 201, r.text
    return r.json()


def _stream_bytes(client, def_id):
    r = client.get(f"/definitions/{def_id}/streams/s/stream", headers=auth(ALICE))
    assert r.status_code == 200, r.text
    return r.content


def test_v1_and_v2_differ():
    root = derive_root_key("v-seed")
    ss = derive_stream_seed(root, "s")
    v1 = generate_window("v1", STREAMS["s"], ss, 0, 8)
    v2 = generate_window("v2", STREAMS["s"], ss, 0, 8)
    assert v1 != v2


def test_definition_records_version(client):
    record = _create(client)
    assert record["algo_version"] == "v1"  # default
    record_v2 = _create(client, algo_version="v2")
    assert record_v2["algo_version"] == "v2"
    assert record_v2["def_id"] != record["def_id"]


def test_explicit_version_is_honored(client):
    record_v2 = _create(client, algo_version="v2")
    root = derive_root_key("v-seed")
    ss = derive_stream_seed(root, "s")
    expected = generate_window("v2", STREAMS["s"], ss, 0, 8)
    assert _stream_bytes(client, record_v2["def_id"]) == expected


def test_unknown_version_rejected(client):
    r = client.post(
        "/definitions",
        json={"seed": "x", "streams": STREAMS, "algo_version": "v9"},
        headers=auth(ALICE),
    )
    assert r.status_code == 400
    assert "unknown algorithm version" in r.json()["detail"]["reason"]


def test_default_switch_affects_only_new_definitions(client):
    old = _create(client)  # v1
    versions = client.get("/admin/versions", headers=auth(ADMIN)).json()
    assert versions["default"] == "v1"
    # Switch default to v2.
    r = client.post(
        "/admin/versions/default", json={"version": "v2"}, headers=auth(ADMIN)
    )
    assert r.status_code == 200, r.text
    assert r.json()["changed"] is True
    # Existing definition stays on v1.
    old_fetched = client.get(f"/definitions/{old['def_id']}", headers=auth(ALICE)).json()
    assert old_fetched["algo_version"] == "v1"
    # New definitions use v2.
    new = _create(client, seed="v-seed-2")
    assert new["algo_version"] == "v2"
    # Switching back is a no-op change report.
    r2 = client.post(
        "/admin/versions/default", json={"version": "v2"}, headers=auth(ADMIN)
    )
    assert r2.json()["changed"] is False


def test_version_switch_requires_admin(client):
    r = client.post(
        "/admin/versions/default", json={"version": "v2"}, headers=auth(BOB)
    )
    assert r.status_code == 403
    assert r.json()["detail"]["error"] == "insufficient_role"


def test_versions_report_definition_counts(client):
    _create(client, seed="a")
    _create(client, seed="b", algo_version="v2")
    report = client.get("/admin/versions", headers=auth(ADMIN)).json()
    counts = {v["name"]: v["definitions"] for v in report["versions"]}
    assert counts["v1"] >= 1
    assert counts["v2"] >= 1
