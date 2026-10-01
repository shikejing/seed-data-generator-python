"""Determinism tests: same definition + seed must produce byte-identical
output across runs, processes and app instances."""
from __future__ import annotations

import hashlib

from seedgen.canonical import derive_root_key, derive_stream_seed
from seedgen.engines import generate_window

from .conftest import ADMIN, ALICE, auth

ANCHOR_SEED = "anchor-seed-v1"
ANCHOR_STREAMS = {
    "alpha": {"kind": "integers", "count": 10, "params": {"low": 0, "high": 99}},
    "beta": {"kind": "choice", "count": 8, "params": {"options": ["red", "green", "blue"]}},
    "gamma": {"kind": "booleans", "count": 5, "params": {}},
}
# Frozen v1 anchor: if v1 ever changes, this test fails.
ANCHOR_SHA256 = "149a085855ea070ce0995ab48b9359a5361fab017bc3ffe28dc6a5acaf78e394"


def _collect(client, def_id):
    out = {}
    for name in sorted(ANCHOR_STREAMS):
        r = client.get(
            f"/definitions/{def_id}/streams/{name}/results",
            headers=auth(ALICE),
            params={"offset": 0, "limit": 100},
        )
        assert r.status_code == 200, r.text
        out[name] = r.json()["records"]
    return out


def _create(client, seed=ANCHOR_SEED, streams=None, algo_version=None):
    body = {"seed": seed, "streams": streams or ANCHOR_STREAMS}
    if algo_version:
        body["algo_version"] = algo_version
    r = client.post("/definitions", json=body, headers=auth(ALICE))
    assert r.status_code == 201, r.text
    return r.json()["def_id"]


def test_v1_anchor_never_changes(client):
    """The frozen v1 algorithm must reproduce the exact anchor bytes."""
    root = derive_root_key(ANCHOR_SEED)
    h = hashlib.sha256()
    for name in sorted(ANCHOR_STREAMS):
        spec = ANCHOR_STREAMS[name]
        ss = derive_stream_seed(root, name)
        h.update(generate_window("v1", spec, ss, 0, spec["count"]))
    assert h.hexdigest() == ANCHOR_SHA256


def test_same_definition_same_bytes(client):
    def_id = _create(client)
    first = _collect(client, def_id)
    second = _collect(client, def_id)
    assert first == second


def test_independent_app_instances_agree(client, client2):
    """Two app instances with separate in-memory state produce identical
    results (simulates cross-process determinism)."""
    def1 = _create(client)
    def2 = _create(client2)
    assert def1 == def2
    assert _collect(client, def1) == _collect(client2, def2)


def test_different_seeds_differ(client):
    d1 = _create(client, seed="seed-one")
    d2 = _create(client, seed="seed-two")
    assert d1 != d2
    assert _collect(client, d1) != _collect(client, d2)


def test_pagination_order_independence(client):
    """Requesting pages in any order yields the same full stream."""
    def_id = _create(client, streams={
        "s": {"kind": "integers", "count": 20, "params": {"low": 0, "high": 999}}
    })
    forward = client.get(
        f"/definitions/{def_id}/streams/s/results",
        headers=auth(ALICE), params={"offset": 0, "limit": 20},
    ).json()["records"]
    page1 = client.get(
        f"/definitions/{def_id}/streams/s/results",
        headers=auth(ALICE), params={"offset": 0, "limit": 7},
    ).json()["records"]
    page2 = client.get(
        f"/definitions/{def_id}/streams/s/results",
        headers=auth(ALICE), params={"offset": 7, "limit": 7},
    ).json()["records"]
    page3 = client.get(
        f"/definitions/{def_id}/streams/s/results",
        headers=auth(ALICE), params={"offset": 14, "limit": 7},
    ).json()["records"]
    assert page1 + page2 + page3 == forward


def test_streaming_matches_pagination(client):
    def_id = _create(client, streams={
        "s": {"kind": "integers", "count": 15, "params": {"low": 0, "high": 999}}
    })
    streamed = client.get(
        f"/definitions/{def_id}/streams/s/stream", headers=auth(ALICE)
    )
    assert streamed.status_code == 200
    lines = [ln for ln in streamed.text.splitlines() if ln]
    page = client.get(
        f"/definitions/{def_id}/streams/s/results",
        headers=auth(ALICE), params={"offset": 0, "limit": 15},
    ).json()["records"]
    assert len(lines) == len(page)
    import json

    assert [json.loads(ln) for ln in lines] == page


def test_seed_types_are_stable(client):
    d1 = _create(client, seed=42)
    d2 = _create(client, seed=42)
    assert d1 == d2
    # String "42" differs from integer 42.
    d3 = _create(client, seed="42")
    assert d3 != d1
