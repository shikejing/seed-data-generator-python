"""Requirement 5: streaming/paging, no half artifacts on cancel/timeout."""

import json
import threading

import pytest
from fastapi.testclient import TestClient

from seedgen.auth import Principal
from seedgen.config import Settings
from seedgen.errors import GenerationCancelledError
from seedgen.manager import GenerationManager
from seedgen.api import create_app

ALICE = Principal("alice", ("generator",))
BIG = [{"name": "big", "row_count": 100_000,
        "fields": [{"name": "i", "kind": "int"}, {"name": "u", "kind": "uuid"}]}]


def test_pre_set_cancellation_leaves_no_artifact(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="big", seed="s",
                               streams=BIG)
    ev = threading.Event()
    ev.set()
    with pytest.raises(GenerationCancelledError):
        m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                        stream_name="big", cancel_event=ev)
    assert not m.store.exists(d.definition_id, "big")
    assert not list((m.store.root / d.definition_id).glob(".big*"))


def test_cancellation_midway_leaves_no_partial_file(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="big", seed="s",
                               streams=BIG)
    ev = threading.Event()
    real_generate = m._generate_locked

    def cancelling(definition, stream, cancel_event):
        def fire_after_delay():
            # Let some rows be written, then cancel.
            cancel_event.set()
        threading.Timer(0.01, fire_after_delay).start()
        return real_generate(definition, stream, cancel_event)

    m._generate_locked = cancelling
    with pytest.raises(GenerationCancelledError):
        m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                        stream_name="big", cancel_event=ev)
    assert not m.store.exists(d.definition_id, "big")
    assert not list((m.store.root / d.definition_id).glob(".big*"))

    # After cancellation the stream can still be (re)generated successfully.
    m._generate_locked = real_generate
    stored = m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                             stream_name="big")
    assert stored.row_count == 100_000


def test_paging_does_not_buffer_whole_stream(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="big", seed="s",
                               streams=BIG)
    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="big")
    # Pages from arbitrary offsets are correct vs independent recomputation.
    from seedgen.generator import generate_page
    definition = m.repo.get(d.definition_id)
    spec = definition.get_stream("big")
    for offset, limit in [(0, 10), (99_990, 10), (50_000, 100)]:
        page = m.read_page(principal=ALICE, definition_id=d.definition_id,
                           stream_name="big", offset=offset, limit=limit)
        assert page["rows"] == generate_page(definition, spec, offset, limit)
        assert page["row_count"] == 100_000


def test_streaming_endpoint_emits_all_rows_and_verifies_first(tmp_path):
    settings = Settings(data_dir=str(tmp_path))
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(
        principal=ALICE, name="big", seed="s",
        streams=[{"name": "big", "row_count": 500,
                  "fields": [{"name": "i", "kind": "int"}]}])
    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="big")
    app = create_app(m, settings)
    client = TestClient(app)
    resp = client.get(
        f"/definitions/{d.definition_id}/streams/big/stream",
        headers={"Authorization": "Bearer token-alice"})
    assert resp.status_code == 200
    rows = [json.loads(line) for line in resp.text.strip().splitlines()]
    assert len(rows) == 500


def test_timeout_cancels_generation(tmp_path):
    settings = Settings(data_dir=str(tmp_path),
                        generation_timeout_seconds=0.05)
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="big", seed="s",
                               streams=BIG)
    app = create_app(m, settings)
    client = TestClient(app)
    resp = client.post(
        f"/definitions/{d.definition_id}/streams/big/generate",
        headers={"Authorization": "Bearer token-alice"})
    assert resp.status_code == 499
    assert resp.json()["error"] == "generation_timeout"
    # no half artifact
    assert not m.store.exists(d.definition_id, "big")


def test_read_page_clamps_to_max_page_size(tmp_path):
    settings = Settings(data_dir=str(tmp_path), default_page_size=5,
                        max_page_size=50)
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(
        principal=ALICE, name="p", seed="s",
        streams=[{"name": "big", "row_count": 100,
                  "fields": [{"name": "i", "kind": "int"}]}])
    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="big")
    app = create_app(m, settings)
    client = TestClient(app)
    hdr = {"Authorization": "Bearer token-alice"}
    assert len(client.get(
        f"/definitions/{d.definition_id}/streams/big/rows?limit=9999",
        headers=hdr).json()["rows"]) == 50
    assert len(client.get(
        f"/definitions/{d.definition_id}/streams/big/rows",
        headers=hdr).json()["rows"]) == 5
