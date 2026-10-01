"""Concurrency and idempotency tests.

Concurrent or repeated submissions of the same request must converge to a
single job and a single generation; no contradictory results and no
half-written artifacts.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from .conftest import ALICE, auth

STREAMS = {"s": {"kind": "integers", "count": 50, "params": {"low": 0, "high": 9999}}}


def _create(client, seed="job-seed"):
    r = client.post(
        "/definitions", json={"seed": seed, "streams": STREAMS}, headers=auth(ALICE)
    )
    assert r.status_code == 201, r.text
    return r.json()["def_id"]


def _wait_done(client, job_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}", headers=auth(ALICE)).json()
        if job["status"] in ("done", "failed", "cancelled", "interrupted"):
            return job
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish: {job}")


def test_repeated_submission_is_idempotent(client):
    def_id = _create(client)
    r1 = client.post(f"/definitions/{def_id}/regenerate", json={}, headers=auth(ALICE))
    j1 = r1.json()
    _wait_done(client, j1["job_id"])
    # Submit the same request again: must return the same job, no regeneration.
    r2 = client.post(f"/definitions/{def_id}/regenerate", json={}, headers=auth(ALICE))
    j2 = r2.json()
    assert j2["job_id"] == j1["job_id"]
    assert j2["status"] == "done"
    assert j2["attempt"] == 1
    # Results unchanged.
    job = client.get(f"/jobs/{j1['job_id']}", headers=auth(ALICE)).json()
    assert job["results"]["s"]["records"] == 50


def test_concurrent_submissions_converge(client):
    def_id = _create(client)
    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(
                client.post,
                f"/definitions/{def_id}/regenerate",
                json={},
                headers=auth(ALICE),
            )
            for _ in range(8)
        ]
        for f in futures:
            results.append(f.result().json())
    job_ids = {r["job_id"] for r in results}
    assert len(job_ids) == 1, f"concurrent submissions diverged: {job_ids}"
    job = _wait_done(client, results[0]["job_id"])
    assert job["status"] == "done"
    assert job["attempt"] == 1


def test_force_regenerates_new_job(client):
    def_id = _create(client)
    r1 = client.post(f"/definitions/{def_id}/regenerate", json={}, headers=auth(ALICE))
    j1 = _wait_done(client, r1.json()["job_id"])
    # Force regeneration: new job, same content.
    r2 = client.post(
        f"/definitions/{def_id}/regenerate", json={"force": True}, headers=auth(ALICE)
    )
    j2 = _wait_done(client, r2.json()["job_id"])
    assert j2["job_id"] != j1["job_id"]
    assert j2["attempt"] == 1
    # Output bytes identical (deterministic rebuild).
    assert j1["results"]["s"]["sha256"] == j2["results"]["s"]["sha256"]


def test_different_streams_independent_jobs(client):
    streams = {
        "a": {"kind": "booleans", "count": 10, "params": {}},
        "b": {"kind": "booleans", "count": 10, "params": {}},
    }
    r = client.post(
        "/definitions", json={"seed": "two", "streams": streams}, headers=auth(ALICE)
    )
    def_id = r.json()["def_id"]
    ja = client.post(
        f"/definitions/{def_id}/regenerate", json={"streams": ["a"]}, headers=auth(ALICE)
    ).json()
    jb = client.post(
        f"/definitions/{def_id}/regenerate", json={"streams": ["b"]}, headers=auth(ALICE)
    ).json()
    assert ja["job_id"] != jb["job_id"]
    _wait_done(client, ja["job_id"])
    _wait_done(client, jb["job_id"])
    # Both streams present and valid.
    verify = client.post(f"/definitions/{def_id}/verify", headers=auth(ALICE)).json()
    assert all(v["status"] == "ok" for v in verify["streams"].values())


def test_job_not_found(client):
    r = client.get("/jobs/job_nonexistent", headers=auth(ALICE))
    assert r.status_code == 404


def test_results_visible_only_after_completion(client, config):
    """While a job runs, no result file may be visible (staging only)."""
    big = {"big": {"kind": "integers", "count": 100000, "params": {"low": 0, "high": 10**9}}}
    r = client.post(
        "/definitions", json={"seed": "big", "streams": big}, headers=auth(ALICE)
    )
    big_id = r.json()["def_id"]
    client.post(f"/definitions/{big_id}/regenerate", json={}, headers=auth(ALICE))
    # While running, the results dir for big_id must not exist yet.
    results_dir = config.results_dir / big_id
    # Poll until done; verify no partial file was ever left behind.
    job = None
    for _ in range(100):
        job = client.get(
            f"/jobs/{client.post(f'/definitions/{big_id}/regenerate', json={}, headers=auth(ALICE)).json()['job_id']}",
            headers=auth(ALICE),
        ).json()
        if job["status"] == "done":
            break
        assert not results_dir.exists() or not any(
            f.suffix == ".jsonl" for f in results_dir.iterdir()
        ), "partial result file visible during generation"
        time.sleep(0.05)
    assert job["status"] == "done", job
    # After completion, the result file exists and staging is cleaned.
    assert results_dir.exists()
    assert (results_dir / "big.jsonl").exists()
    assert not any(config.staging_dir.iterdir()) if config.staging_dir.exists() else True
