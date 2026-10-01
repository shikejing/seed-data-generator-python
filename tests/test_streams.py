"""Multi-stream isolation tests.

Requesting or regenerating one stream must never affect another stream's
output, and adding new streams must not change existing ones.
"""
from __future__ import annotations

import hashlib

from seedgen.canonical import derive_root_key, derive_stream_seed
from seedgen.engines import generate_window

from .conftest import ALICE, auth

STREAMS = {
    "alpha": {"kind": "integers", "count": 12, "params": {"low": 0, "high": 999}},
    "beta": {"kind": "choice", "count": 10, "params": {"options": ["x", "y", "z"]}},
    "gamma": {"kind": "string", "count": 8, "params": {"alphabet": "abc", "length": 5}},
}


def _create(client, streams=None, seed="isolation-seed"):
    r = client.post(
        "/definitions",
        json={"seed": seed, "streams": streams or STREAMS},
        headers=auth(ALICE),
    )
    assert r.status_code == 201, r.text
    return r.json()["def_id"]


def _stream_bytes(client, def_id, name):
    r = client.get(
        f"/definitions/{def_id}/streams/{name}/stream", headers=auth(ALICE)
    )
    assert r.status_code == 200, r.text
    return r.content


def test_streams_are_independent(client):
    """Stream seeds are domain-separated: outputs differ across streams."""
    root = derive_root_key("isolation-seed")
    seeds = {name: derive_stream_seed(root, name) for name in STREAMS}
    assert len(set(seeds.values())) == len(STREAMS)
    outputs = set()
    for name, spec in STREAMS.items():
        out = generate_window("v1", spec, seeds[name], 0, spec["count"])
        outputs.add(out)
    assert len(outputs) == len(STREAMS)


def test_requesting_one_stream_keeps_others(client):
    def_id = _create(client)
    before = {name: _stream_bytes(client, def_id, name) for name in STREAMS}
    # Request each stream in turn, then re-check all of them.
    for name in sorted(STREAMS):
        r = client.get(
            f"/definitions/{def_id}/streams/{name}/results",
            headers=auth(ALICE), params={"offset": 0, "limit": 5},
        )
        assert r.status_code == 200
    after = {name: _stream_bytes(client, def_id, name) for name in STREAMS}
    assert before == after


def test_regenerating_one_stream_keeps_others(client):
    def_id = _create(client)
    before = {name: _stream_bytes(client, def_id, name) for name in STREAMS}
    r = client.post(
        f"/definitions/{def_id}/regenerate",
        json={"streams": ["alpha"], "force": True},
        headers=auth(ALICE),
    )
    assert r.status_code == 200, r.text
    import time

    for _ in range(50):
        job = client.get(f"/jobs/{r.json()['job_id']}", headers=auth(ALICE)).json()
        if job["status"] == "done":
            break
        time.sleep(0.05)
    assert job["status"] == "done", job
    after = {name: _stream_bytes(client, def_id, name) for name in STREAMS}
    assert before == after


def test_adding_stream_keeps_existing(client):
    def_id = _create(client, streams={"alpha": STREAMS["alpha"]})
    alpha_before = _stream_bytes(client, def_id, "alpha")
    # A definition with an added stream is a different definition id...
    def_id2 = _create(
        client,
        streams={"alpha": STREAMS["alpha"], "beta": STREAMS["beta"]},
    )
    assert def_id2 != def_id
    # ...but the existing stream's bytes are unchanged.
    assert _stream_bytes(client, def_id2, "alpha") == alpha_before


def test_stream_hash_stable_under_bulk_generation(client):
    """Generating all streams together vs one at a time: identical bytes."""
    def_id = _create(client)
    root = derive_root_key("isolation-seed")
    for name, spec in STREAMS.items():
        expected = generate_window(
            "v1", spec, derive_stream_seed(root, name), 0, spec["count"]
        )
        assert hashlib.sha256(_stream_bytes(client, def_id, name)).hexdigest() == hashlib.sha256(
            expected
        ).hexdigest()
