"""Requirement 3: algorithm version pinning and explicit switching."""

import pytest

from seedgen import versions
from seedgen.auth import Principal
from seedgen.errors import VersionNotFoundError
from seedgen.manager import GenerationManager

ALICE = Principal("alice", ("generator",))

STREAMS = [{"name": "users", "row_count": 10,
            "fields": [{"name": "id", "kind": "int"}]}]


def test_definition_pins_creation_version_by_default(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="demo", seed="s",
                               streams=STREAMS)
    assert d.algorithm_version == versions.DEFAULT_VERSION == "1.0.0"


def test_explicit_version_selection(tmp_path):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="demo2", seed="s",
                               streams=STREAMS, algorithm_version="1.1.0")
    assert d.algorithm_version == "1.1.0"
    # persisted pin survives restart
    m2 = GenerationManager(tmp_path)
    assert m2.repo.get(d.definition_id).algorithm_version == "1.1.0"


def test_unknown_version_rejected(tmp_path):
    m = GenerationManager(tmp_path)
    with pytest.raises(VersionNotFoundError):
        m.create_definition(principal=ALICE, name="demo3", seed="s",
                            streams=STREAMS, algorithm_version="9.9.9")


def test_new_version_default_does_not_change_old_definitions(tmp_path, monkeypatch):
    m = GenerationManager(tmp_path)
    d, _ = m.create_definition(principal=ALICE, name="legacy", seed="s",
                               streams=STREAMS)
    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="users")
    before = m.store.stream_path(d.definition_id, "users").read_bytes()

    # Simulate a future release bumping the default version.
    monkeypatch.setattr(versions, "DEFAULT_VERSION", "1.1.0")
    m2 = GenerationManager(tmp_path)
    m2.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                     stream_name="users")
    assert m2.store.stream_path(d.definition_id, "users").read_bytes() == before

    new_d, created = m2.create_definition(principal=ALICE, name="brand-new",
                                          seed="s", streams=STREAMS)
    assert created and new_d.algorithm_version == "1.1.0"


def test_version_impact_is_read_only_and_lists_affected(tmp_path):
    m = GenerationManager(tmp_path)
    d_old, _ = m.create_definition(principal=ALICE, name="old", seed="s",
                                   streams=STREAMS)  # pinned 1.0.0
    d_new, _ = m.create_definition(principal=ALICE, name="new", seed="s2",
                                   streams=STREAMS, algorithm_version="1.1.0")
    impact = m.versions_affected_by_switch("1.1.0")
    assert impact["new_version"] == "1.1.0"
    assert d_old.definition_id in impact["affected_definition_ids"]
    assert d_new.definition_id not in impact["affected_definition_ids"]
    assert impact["count"] == 1
    # read-only: pins are untouched
    assert m.repo.get(d_old.definition_id).algorithm_version == "1.0.0"

    with pytest.raises(VersionNotFoundError):
        m.versions_affected_by_switch("4.0.0")
