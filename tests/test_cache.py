"""Cache verification and rebuild tests.

Corruption or loss of a result file or manifest must be detected on access
and rebuilt from the definition and seed.  After a restart, interrupted
jobs are recovered and marked.
"""
from __future__ import annotations

import json
import time

from fastapi.testclient import TestClient

from seedgen.api import create_app
from seedgen.config import Config
from seedgen.service import SeedGenService

from .conftest import ALICE, auth

STREAMS = {"s": {"kind": "integers", "count": 20, "params": {"low": 0, "high": 999}}}


def _create(client, seed="cache-seed"):
    r = client.post(
        "/definitions", json={"seed": seed, "streams": STREAMS}, headers=auth(ALICE)
    )
    assert r.status_code == 201, r.text
    return r.json()["def_id"]


def _stream_bytes(client, def_id):
    r = client.get(f"/definitions/{def_id}/streams/s/stream", headers=auth(ALICE))
    assert r.status_code == 200, r.text
    return r.content


def test_missing_result_file_is_rebuilt(client, config):
    def_id = _create(client)
    original = _stream_bytes(client, def_id)
    # Delete the result file.
    result_file = config.results_dir / def_id / "s.jsonl"
    result_file.unlink()
    # Next access detects the loss and rebuilds transparently.
    rebuilt = _stream_bytes(client, def_id)
    assert rebuilt == original


def test_corrupt_result_file_is_rebuilt(client, config):
    def_id = _create(client)
    original = _stream_bytes(client, def_id)
    # Corrupt the result file.
    result_file = config.results_dir / def_id / "s.jsonl"
    data = result_file.read_bytes()
    result_file.write_bytes(data[:-1] + (b"0" if data[-1:] != b"0" else b"1"))
    # Verify detects corruption.
    verify = client.post(f"/definitions/{def_id}/verify", headers=auth(ALICE)).json()
    assert verify["streams"]["s"]["status"] == "corrupt"
    # Access triggers rebuild.
    rebuilt = _stream_bytes(client, def_id)
    assert rebuilt == original
    # Verify now passes.
    verify = client.post(f"/definitions/{def_id}/verify", headers=auth(ALICE)).json()
    assert verify["streams"]["s"]["status"] == "ok"


def test_missing_manifest_rebuilds_all(client, config):
    def_id = _create(client)
    original = _stream_bytes(client, def_id)
    # Delete the manifest.
    manifest = config.results_dir / def_id / "manifest.json"
    manifest.unlink()
    # Access rebuilds everything.
    rebuilt = _stream_bytes(client, def_id)
    assert rebuilt == original
    # Manifest is recreated.
    assert manifest.exists()


def test_verify_endpoint_rebuild_flag(client, config):
    def_id = _create(client)
    original = _stream_bytes(client, def_id)
    # Corrupt the file.
    result_file = config.results_dir / def_id / "s.jsonl"
    result_file.write_bytes(b"corrupted")
    # verify with rebuild=True fixes it.
    verify = client.post(
        f"/definitions/{def_id}/verify?rebuild=true", headers=auth(ALICE)
    ).json()
    assert verify["streams"]["s"]["status"] == "ok"
    assert _stream_bytes(client, def_id) == original


def test_restart_marks_interrupted_jobs(config):
    """After a restart, jobs that were running are marked interrupted."""
    slow_config = Config(
        data_dir=config.data_dir,
        gen_chunk_records=10,
        gen_chunk_delay=0.1,
        job_timeout_seconds=30,
    )
    app1 = create_app(slow_config)
    c1 = TestClient(app1)
    r = c1.post(
        "/definitions",
        json={"seed": "restart", "streams": {"s": {"kind": "integers", "count": 1000, "params": {"low": 0, "high": 999}}}},
        headers=auth(ALICE),
    )
    def_id = r.json()["def_id"]
    r = c1.post(f"/definitions/{def_id}/regenerate", json={}, headers=auth(ALICE))
    jid = r.json()["job_id"]
    # Immediately "restart": create a new service on the same data dir.
    service2 = SeedGenService(slow_config)
    job = service2.jobs.load_job(jid)
    assert job["status"] == "interrupted", job
    assert "restart" in job["error"]
