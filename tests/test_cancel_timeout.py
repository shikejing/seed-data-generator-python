"""Cancellation and timeout tests.

Cancelling or timing out a job must leave no visible half-written
artifacts: the staging area is removed and no result file is published.
"""
from __future__ import annotations

import time

import pytest

from seedgen.api import create_app
from seedgen.config import Config

from .conftest import ALICE, auth


@pytest.fixture
def slow_config(tmp_path):
    return Config(
        data_dir=tmp_path / "data",
        gen_chunk_records=10,
        gen_chunk_delay=0.05,
        job_timeout_seconds=0.1,
    )


@pytest.fixture
def slow_client(slow_config):
    from fastapi.testclient import TestClient

    return TestClient(create_app(slow_config))


def _create(client, count=1000, seed="cancel-seed"):
    r = client.post(
        "/definitions",
        json={"seed": seed, "streams": {"s": {"kind": "integers", "count": count, "params": {"low": 0, "high": 999}}}},
        headers=auth(ALICE),
    )
    assert r.status_code == 201, r.text
    return r.json()["def_id"]


def test_timeout_marks_job_failed_and_cleans_staging(slow_client, slow_config):
    def_id = _create(slow_client, count=1000)
    r = slow_client.post(
        f"/definitions/{def_id}/regenerate", json={}, headers=auth(ALICE)
    )
    jid = r.json()["job_id"]
    # Wait for the watchdog to fire.
    deadline = time.time() + 5
    while time.time() < deadline:
        job = slow_client.get(f"/jobs/{jid}", headers=auth(ALICE)).json()
        if job["status"] in ("done", "failed", "cancelled"):
            break
        time.sleep(0.05)
    assert job["status"] == "failed", job
    assert job["error"] == "timeout", job
    # Staging cleaned; no result published.
    assert not any(slow_config.staging_dir.iterdir()) if slow_config.staging_dir.exists() else True
    assert not (slow_config.results_dir / def_id / "s.jsonl").exists()


def test_cancel_marks_job_cancelled_and_cleans_staging(slow_client, slow_config):
    def_id = _create(slow_client, count=100)
    r = slow_client.post(
        f"/definitions/{def_id}/regenerate", json={}, headers=auth(ALICE)
    )
    jid = r.json()["job_id"]
    # Cancel immediately.
    r2 = slow_client.post(f"/jobs/{jid}/cancel", headers=auth(ALICE))
    assert r2.status_code == 200, r2.text
    deadline = time.time() + 5
    while time.time() < deadline:
        job = slow_client.get(f"/jobs/{jid}", headers=auth(ALICE)).json()
        if job["status"] in ("done", "failed", "cancelled"):
            break
        time.sleep(0.05)
    assert job["status"] == "cancelled", job
    assert job["error"] == "cancelled", job
    assert not any(slow_config.staging_dir.iterdir()) if slow_config.staging_dir.exists() else True


def test_cancel_unknown_job_404(slow_client):
    r = slow_client.post("/jobs/job_nonexistent/cancel", headers=auth(ALICE))
    assert r.status_code == 404


def test_streaming_disconnect_leaves_no_staging(slow_client, slow_config):
    """Disconnecting mid-stream must not leave staging artifacts."""
    def_id = _create(slow_client, count=1000)
    # First ensure results exist (streaming reads generate on the fly).
    # Stream and disconnect after reading a small amount.
    with slow_client.stream(
        "GET",
        f"/definitions/{def_id}/streams/s/stream",
        headers=auth(ALICE),
    ) as response:
        assert response.status_code == 200
        chunks = response.iter_bytes(100)
        first = next(chunks)
        assert first  # got some data
    # After disconnect, no staging dirs should linger.
    time.sleep(0.2)
    staging_entries = list(slow_config.staging_dir.iterdir()) if slow_config.staging_dir.exists() else []
    assert staging_entries == [], f"staging left behind: {staging_entries}"
