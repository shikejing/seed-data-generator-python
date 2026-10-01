"""Requirement 4: concurrent and duplicate submissions converge."""

import threading
from unittest.mock import patch

from seedgen.auth import Principal
from seedgen.manager import GenerationManager

ALICE = Principal("alice", ("generator",))
STREAMS = [{"name": "big", "row_count": 500,
            "fields": [{"name": "i", "kind": "int",
                        "params": {"min": 0, "max": 1_000_000}},
                       {"name": "u", "kind": "uuid"}]}]


def _make(manager, name="demo", seed="conc-seed", **kw):
    d, _ = manager.create_definition(principal=ALICE, name=name, seed=seed,
                                     streams=STREAMS, **kw)
    return d


def test_concurrent_generation_converges_to_single_result(tmp_path):
    m = GenerationManager(tmp_path)
    d = _make(m)
    checksums, errors = [], []
    n = 24
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        try:
            stored = m.ensure_stream(principal=ALICE,
                                    definition_id=d.definition_id,
                                    stream_name="big")
            checksums.append(stored.checksum)
        except BaseException as exc:  # pragma: no cover - debugging aid
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(checksums) == n
    assert len(set(checksums)) == 1
    assert len(list(m.store.iter_rows(d.definition_id, "big"))) == 500


def test_generation_runs_once_for_duplicate_requests(tmp_path):
    m = GenerationManager(tmp_path)
    d = _make(m)
    calls = {"n": 0}
    original = m._generate_locked

    def counting(definition, stream, cancel_event):
        calls["n"] += 1
        return original(definition, stream, cancel_event)

    with patch.object(m, "_generate_locked", side_effect=counting):
        m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                        stream_name="big")
        for _ in range(10):
            m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                            stream_name="big")
    assert calls["n"] == 1


def test_duplicate_creation_is_idempotent(tmp_path):
    m = GenerationManager(tmp_path)
    d1, created1 = m.create_definition(principal=ALICE, name="demo", seed="s",
                                       streams=STREAMS, idempotency_key="k-7")
    d2, created2 = m.create_definition(principal=ALICE, name="demo", seed="s",
                                       streams=STREAMS, idempotency_key="k-7")
    assert d1 == d2 and created1 is True and created2 is False


def test_concurrent_identical_creation_is_idempotent(tmp_path):
    m = GenerationManager(tmp_path)
    results = []
    n = 12
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        d, created = m.create_definition(
            principal=ALICE, name="race", seed="s", streams=STREAMS)
        results.append((d.definition_id, created))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ids = {r[0] for r in results}
    assert len(ids) == 1
    assert sum(1 for _, created in results if created) == 1
