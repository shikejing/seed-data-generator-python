"""Requirement 2: named streams are independent.

Requesting stream B, changing request order, or adding a new stream never
changes the bytes of an existing stream.
"""

from seedgen.auth import Principal
from seedgen.manager import GenerationManager

ALICE = Principal("alice", ("generator",))

USERS = [{"name": "id", "kind": "int", "params": {"min": 0, "max": 1000}},
         {"name": "email", "kind": "email"}]
ORDERS = [{"name": "oid", "kind": "uuid"}]
EVENTS = [{"name": "eid", "kind": "uuid"},
          {"name": "ts", "kind": "timestamp",
           "params": {"start": "2020-01-01", "end": "2020-12-31"}}]


def _create(manager, streams, name="demo", seed="isolation-seed"):
    d, _ = manager.create_definition(
        principal=ALICE, name=name, seed=seed, streams=streams)
    return d


def test_requesting_other_stream_does_not_change_existing(tmp_path):
    m = GenerationManager(tmp_path)
    d = _create(m, [{"name": "users", "row_count": 30, "fields": USERS},
                    {"name": "orders", "row_count": 20, "fields": ORDERS}])
    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="users")
    users_before = m.store.stream_path(d.definition_id, "users").read_bytes()

    m.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                    stream_name="orders")
    assert m.store.stream_path(d.definition_id, "users").read_bytes() == users_before


def test_request_order_does_not_change_bytes(tmp_path):
    # Two directories, opposite request order; resulting files must match.
    dirs = [tmp_path / "a", tmp_path / "b"]
    outputs = []
    for base in dirs:
        m = GenerationManager(base)
        d = _create(m, [{"name": "users", "row_count": 25, "fields": USERS},
                        {"name": "orders", "row_count": 15, "fields": ORDERS},
                        {"name": "events", "row_count": 10, "fields": EVENTS}])
        outputs.append(d.definition_id)
    m1, m2 = GenerationManager(dirs[0]), GenerationManager(dirs[1])
    did = outputs[0]
    for name in ("users", "orders", "events"):
        m1.ensure_stream(principal=ALICE, definition_id=did, stream_name=name)
    for name in ("events", "users", "orders"):
        m2.ensure_stream(principal=ALICE, definition_id=did, stream_name=name)
    for name in ("users", "orders", "events"):
        assert (m1.store.stream_path(did, name).read_bytes()
                == m2.store.stream_path(did, name).read_bytes())


def test_independent_row_access_matches_bulk(tmp_path):
    from seedgen.generator import generate_row, iter_stream
    m = GenerationManager(tmp_path)
    d = _create(m, [{"name": "users", "row_count": 40, "fields": USERS}])
    definition = m.repo.get(d.definition_id)
    spec = definition.get_stream("users")
    bulk = list(iter_stream(definition, spec))
    # Request only a handful of specific rows, out of order.
    for i in (0, 17, 39, 5):
        assert generate_row(definition, spec, i) == bulk[i]


def test_adding_a_stream_keeps_existing_stream_bytes(tmp_path):
    m = GenerationManager(tmp_path)
    d1 = _create(m, [{"name": "users", "row_count": 20, "fields": USERS}],
                 name="growing")
    m.ensure_stream(principal=ALICE, definition_id=d1.definition_id,
                    stream_name="users")
    before = m.store.stream_path(d1.definition_id, "users").read_bytes()

    # A new definition version (different id) that adds a stream must not
    # perturb the users stream given the same seed/algorithm.
    m2 = GenerationManager(tmp_path / "v2")
    d2, _ = m2.create_definition(
        principal=ALICE, name="growing", seed="isolation-seed",
        streams=[{"name": "users", "row_count": 20, "fields": USERS},
                 {"name": "events", "row_count": 20, "fields": EVENTS}])
    # Same definition content for users -> same definition id prefix.
    for name in ("users", "events"):
        m2.ensure_stream(principal=ALICE, definition_id=d2.definition_id,
                         stream_name=name)
    assert m2.store.stream_path(d2.definition_id, "users").read_bytes() == before
