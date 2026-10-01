"""Edge cases: ungenerated reads, offset past end, empty streams, unknown."""

import pytest
from fastapi.testclient import TestClient

from seedgen.auth import Principal
from seedgen.config import Settings
from seedgen.errors import CacheCorruptedError, StreamNotFoundError
from seedgen.manager import GenerationManager
from seedgen.api import create_app

ALICE = Principal("alice", ("generator",))


def test_reading_not_yet_generated_stream_is_distinct_error(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(
        principal=ALICE, name="n", seed="s",
        streams=[{"name": "users", "row_count": 3,
                  "fields": [{"name": "id", "kind": "int"}]}])
    with pytest.raises(CacheCorruptedError) as ei:
        m.read_page(principal=ALICE, definition_id=d.definition_id,
                    stream_name="users")
    assert ei.value.code == "cache_missing"


def test_offset_past_end_returns_empty_page(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(
        principal=ALICE, name="n", seed="s",
        streams=[{"name": "users", "row_count": 3,
                  "fields": [{"name": "id", "kind": "int"}]}])
    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="users")
    page = m.read_page(principal=ALICE, definition_id=d.definition_id,
                       stream_name="users", offset=100, limit=10)
    assert page["rows"] == [] and page["row_count"] == 3


def test_empty_stream_is_committed_and_readable(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(
        principal=ALICE, name="n", seed="s",
        streams=[{"name": "users", "row_count": 0,
                  "fields": [{"name": "id", "kind": "int"}]}])
    stored = m.ensure_stream(principal=ALICE,
                             definition_id=d.definition_id, stream_name="users")
    assert stored.row_count == 0
    page = m.read_page(principal=ALICE, definition_id=d.definition_id,
                       stream_name="users")
    assert page["rows"] == [] and page["row_count"] == 0


def test_unknown_stream_and_definition(tmp_path):
    settings = Settings(data_dir=str(tmp_path))
    m = GenerationManager(tmp_path)
    app = create_app(m, settings)
    c = TestClient(app)
    hdr = {"Authorization": "Bearer token-alice"}
    r = c.post("/definitions", headers=hdr, json={
        "name": "n", "seed": "s",
        "streams": [{"name": "users", "row_count": 1,
                     "fields": [{"name": "id", "kind": "int"}]}]})
    did = r.json()["definition"]["definition_id"]
    assert c.post(f"/definitions/{did}/streams/ghost/generate",
                  headers=hdr).status_code == 404
    assert c.get("/definitions/def_doesnotexist", headers=hdr).status_code == 404
    with pytest.raises(StreamNotFoundError):
        m.read_page(principal=ALICE, definition_id=did, stream_name="ghost")
