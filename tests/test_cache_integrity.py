"""Requirement 7: restart verification, corruption/loss detection and rebuild.

The service never serves wrong data silently: checksum, row count and
specification fingerprint are verified; corruption or loss triggers a rebuild
that reproduces identical bytes.
"""

import json

import pytest

from seedgen.auth import Principal
from seedgen.errors import CacheCorruptedError
from seedgen.manager import GenerationManager

ALICE = Principal("alice", ("generator",))
STREAMS = [{"name": "users", "row_count": 30,
            "fields": [{"name": "id", "kind": "int"},
                       {"name": "email", "kind": "email"}]},
           {"name": "orders", "row_count": 12,
            "fields": [{"name": "oid", "kind": "uuid"}]}]


@pytest.fixture()
def generated(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="demo", seed="cache-seed",
                               streams=STREAMS)
    for name in ("users", "orders"):
        m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                        stream_name=name)
    users_bytes = m.store.stream_path(d.definition_id, "users").read_bytes()
    return tmp_path, d.definition_id, users_bytes


def test_verification_passes_for_intact_cache(generated):
    tmp, did, _ = generated
    m = GenerationManager(tmp)
    report = m.verify_cache(did)
    assert all(v["ok"] for v in report.values())


def test_appended_row_corruption_is_detected_and_rebuilt(generated):
    tmp, did, original = generated
    m = GenerationManager(tmp)
    p = m.store.stream_path(did, "users")
    p.write_bytes(p.read_bytes() + b'{"id": 1, "email": "x@y.z"}\n')
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.store.stream_path(did, "users").read_bytes() == original


def test_byte_flip_corruption_is_detected_and_rebuilt(generated):
    tmp, did, original = generated
    m = GenerationManager(tmp)
    p = m.store.stream_path(did, "users")
    raw = bytearray(p.read_bytes())
    raw[10] = ord("Z") if raw[10] != ord("Z") else ord("Y")
    p.write_bytes(bytes(raw))
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.store.stream_path(did, "users").read_bytes() == original
    assert m.verify_cache(did)["users"]["ok"]


def test_truncation_is_detected_and_rebuilt(generated):
    tmp, did, original = generated
    m = GenerationManager(tmp)
    p = m.store.stream_path(did, "users")
    raw = p.read_bytes().splitlines(keepends=True)
    p.write_bytes(b"".join(raw[:-3]))  # drop last 3 rows
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.store.stream_path(did, "users").read_bytes() == original


def test_data_file_loss_is_rebuilt(generated):
    tmp, did, original = generated
    m = GenerationManager(tmp)
    m.store.stream_path(did, "users").unlink()
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.store.stream_path(did, "users").read_bytes() == original


def test_meta_loss_is_detected_and_rebuilt(generated):
    tmp, did, original = generated
    m = GenerationManager(tmp)
    m.store._meta_path(did, "users").unlink()
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.store.stream_path(did, "users").read_bytes() == original


def test_corrupted_meta_is_detected(generated):
    tmp, did, original = generated
    m = GenerationManager(tmp)
    mp = m.store._meta_path(did, "users")
    mp.write_bytes(b"{not valid json")
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.store.stream_path(did, "users").read_bytes() == original


def test_verify_cache_reports_status_without_raising(generated):
    tmp, did, _ = generated
    m = GenerationManager(tmp)
    m.store.stream_path(did, "orders").unlink()
    status = m.verify_cache(did)
    assert status["users"]["ok"] is True
    assert status["orders"]["ok"] is False
    assert status["orders"]["reason"] == "cache_missing"


def test_spec_change_invalidates_and_rebuilds(generated):
    # If the definition changed (e.g. seed edited on a copied deployment) the
    # stale fingerprint mismatch must be detected rather than served.
    tmp, did, _ = generated
    m = GenerationManager(tmp)
    definition = m.repo.get(did)
    stream = definition.get_stream("users")
    other_fp = "0" * 64
    meta_path = m.store._meta_path(did, "users")
    meta = json.loads(meta_path.read_text())
    meta["spec_fingerprint"] = other_fp
    meta_path.write_text(json.dumps(meta, sort_keys=True))
    with pytest.raises(CacheCorruptedError) as ei:
        m.store.verify(did, "users",
                       __import__("seedgen.storage", fromlist=["spec_fingerprint_for"])
                       .spec_fingerprint_for(definition, stream))
    assert ei.value.code == "cache_spec_mismatch"
    # ensure_stream detects the stale cache and rebuilds it
    m.ensure_stream(principal=ALICE, definition_id=did, stream_name="users")
    assert m.verify_cache(did)["users"]["ok"]
