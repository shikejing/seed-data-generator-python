"""Requirement 1: results depend only on definition + seed.

Byte-identical across runs, "restarts" (new manager on same directory),
dict/set traversal order and are independent of wall clock and environment.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from seedgen.generator import generate_row, generate_page, iter_stream
from seedgen.manager import GenerationManager
from seedgen.auth import Principal
from seedgen.models import FieldSpec, GenerationDefinition, StreamSpec

ALICE = Principal("alice", ("generator",))

FIELDS = [
    {"name": "id", "kind": "int", "params": {"min": 1, "max": 1_000_000}},
    {"name": "score", "kind": "float", "params": {"decimals": 4}},
    {"name": "level", "kind": "choice",
     "params": {"options": ["bronze", "silver", "gold"]}},
    {"name": "active", "kind": "bool"},
    {"name": "uid", "kind": "uuid"},
    {"name": "bio", "kind": "text", "params": {"length": 6}},
    {"name": "full_name", "kind": "name"},
    {"name": "email", "kind": "email"},
    {"name": "day", "kind": "date"},
    {"name": "tags", "kind": "sequence",
     "params": {"length": 3, "of": {"kind": "choice",
                                    "params": {"options": ["x", "y", "z"]}}}},
]


def _definition(seed="the-seed", version="1.0.0", did="def-abc", extra_streams=()):
    streams = (StreamSpec("users", 50, tuple(FieldSpec.from_dict(f) for f in FIELDS)),)
    streams = streams + tuple(extra_streams)
    return GenerationDefinition(
        definition_id=did, name="demo", seed=seed, streams=streams,
        algorithm_version=version, created_by="alice")


def test_row_by_row_equals_full_iteration():
    d = _definition()
    s = d.get_stream("users")
    full = list(iter_stream(d, s))
    assert [generate_row(d, s, i) for i in range(s.row_count)] == full


def test_pagination_is_contiguous_and_ordered():
    d = _definition()
    s = d.get_stream("users")
    pages = generate_page(d, s, 0, 10) + generate_page(d, s, 10, 10) + \
        generate_page(d, s, 20, 30)
    assert pages == list(iter_stream(d, s))
    # random access to a late page equals the corresponding slice
    assert generate_page(d, s, 45, 5) == list(iter_stream(d, s))[45:50]


def test_seed_changes_output_but_structure_is_stable():
    a = _definition(seed="seed-a")
    b = _definition(seed="seed-b")
    sa, sb = a.get_stream("users"), b.get_stream("users")
    rows_a = list(iter_stream(a, sa))
    rows_b = list(iter_stream(b, sb))
    assert rows_a != rows_b
    assert sorted(rows_a[0].keys()) == sorted(rows_b[0].keys())


def test_named_identity_and_version_participate_in_output():
    # Algorithm version changes output...
    d1 = _definition(version="1.0.0")
    dv = _definition(version="1.1.0")
    s = d1.get_stream("users")
    assert generate_row(dv, s, 0) != generate_row(d1, s, 0)

    # ...as does the stable definition name (namespace), while the internal
    # document id -- a fingerprint of sibling streams too -- deliberately does
    # not, preserving stream isolation.
    d_other_name = _definition(did="def-zzz")
    object.__setattr__(d_other_name, "name", "different-namespace")
    assert generate_row(d_other_name, s, 0) != generate_row(d1, s, 0)

    same_ns = _definition(did="def-completely-different-id")
    assert generate_row(same_ns, s, 0) == generate_row(d1, s, 0)


def test_field_declaration_order_does_not_change_field_values():
    d = _definition()
    original = list(iter_stream(d, d.get_stream("users")))
    reordered_specs = [FIELDS[i] for i in (2, 0, 4, 1, 3, 5, 9, 6, 7, 8)]
    s2 = StreamSpec("users", 50, tuple(FieldSpec.from_dict(f) for f in reordered_specs))
    d2 = GenerationDefinition(definition_id="def-abc", name="demo",
                              seed="the-seed", streams=(s2,),
                              algorithm_version="1.0.0", created_by="alice")
    moved = list(iter_stream(d2, s2))
    for i in range(50):
        for fname in ("id", "email", "tags", "uid"):
            assert moved[i][fname] == original[i][fname]


def test_persisted_bytes_survive_restart(tmp_path):
    mgr = GenerationManager(tmp_path)
    d, _ = mgr.create_definition(
        principal=ALICE, name="demo", seed="the-seed",
        streams=[{"name": "users", "row_count": 50, "fields": FIELDS}])
    mgr.ensure_stream(principal=ALICE, definition_id=d.definition_id,
                      stream_name="users")
    first = mgr.store.stream_path(d.definition_id, "users").read_bytes()

    # "restart": a fresh manager over the same directory
    mgr2 = GenerationManager(tmp_path)
    second = mgr2.store.stream_path(d.definition_id, "users").read_bytes()
    assert first == second


def test_cross_process_byte_equality(tmp_path):
    # A genuinely separate interpreter process must reproduce identical bytes.
    import os
    repo_root = Path(__file__).resolve().parent.parent
    fields_json = json.dumps(FIELDS)
    script = tmp_path / "gen.py"
    script.write_text(
        "import sys\n"
        "from seedgen.manager import GenerationManager\n"
        "from seedgen.auth import Principal\n"
        "import json\n"
        "fields = json.loads(sys.argv[2])\n"
        "m = GenerationManager(sys.argv[1])\n"
        "d, _ = m.create_definition(\n"
        "    principal=Principal('alice', ('generator',)), name='demo',\n"
        "    seed='the-seed',\n"
        "    streams=[{'name': 'users', 'row_count': 40, 'fields': fields}])\n"
        "m.ensure_stream(principal=Principal('alice', ('generator',)),\n"
        "                definition_id=d.definition_id, stream_name='users')\n"
        "sys.stdout.write(m.store.stream_path(d.definition_id, 'users').read_text())\n"
    )
    env = dict(os.environ, PYTHONPATH=str(repo_root))
    d1, d2 = tmp_path / "run1", tmp_path / "run2"
    args_base = [sys.executable, str(script)]
    r1 = subprocess.run(args_base + [str(d1), fields_json],
                        capture_output=True, text=True, env=env)
    r2 = subprocess.run(args_base + [str(d2), fields_json],
                        capture_output=True, text=True, env=env)
    assert r1.returncode == 0, r1.stderr
    assert r2.returncode == 0, r2.stderr
    assert r1.stdout == r2.stdout


def test_output_independent_of_python_hash_seed(tmp_path):
    # Dict/set traversal order varies across hash randomization seeds.
    # Generated bytes must be identical regardless.
    import os
    repo_root = Path(__file__).resolve().parent.parent
    fields_json = json.dumps(FIELDS)
    script = tmp_path / "genhs.py"
    script.write_text(
        "import sys, json\n"
        "from seedgen.manager import GenerationManager\n"
        "from seedgen.auth import Principal\n"
        "fields = json.loads(sys.argv[2])\n"
        "m = GenerationManager(sys.argv[1])\n"
        "d, _ = m.create_definition(principal=Principal('alice', ('generator',)),\n"
        "                           name='demo', seed='the-seed',\n"
        "                           streams=[{'name': 'users', 'row_count': 30,\n"
        "                                    'fields': fields}])\n"
        "m.ensure_stream(principal=Principal('alice', ('generator',)),\n"
        "                definition_id=d.definition_id, stream_name='users')\n"
        "sys.stdout.write(m.store.stream_path(d.definition_id, 'users').read_text())\n"
    )
    outs = []
    for seed in ("0", "1", "12345", "random"):
        env = dict(os.environ, PYTHONPATH=str(repo_root))
        if seed == "random":
            env.pop("PYTHONHASHSEED", None)
        else:
            env["PYTHONHASHSEED"] = seed
        out_dir = tmp_path / f"hs_{seed}"
        r = subprocess.run([sys.executable, str(script), str(out_dir), fields_json],
                           capture_output=True, text=True, env=env)
        assert r.returncode == 0, r.stderr
        outs.append(r.stdout)
    assert all(o == outs[0] for o in outs)
