# seed-data-generator

A reproducible, seeded data generation service.  **The same generation
definition with the same seed always produces byte-identical output** —
across runs, process restarts, and machines.

## Reproducibility semantics

### Guarantee

A generation definition is content-addressed: its identifier is the
SHA-256 of the canonical JSON of `(seed, algo_version, streams)`.  Given
the same definition and seed, every result is byte-identical regardless
of:

| Source of non-determinism | How it is eliminated |
|---|---|
| System time / wall clock | Never used in generation; only in metadata |
| Environment variables | Never read by the generation path |
| Dict / set iteration order | Definitions are serialized as canonical JSON (sorted keys, fixed separators) before hashing |
| Python hash randomization (`PYTHONHASHSEED`) | No `hash()` of strings is used for output |
| Process / machine differences | The PRNG is a pure-Python SplitMix64 with a frozen implementation |

### How seeds are derived

```
root_key   = SHA-256("seedgen.root.v1" || seed)
stream_seed = SHA-256("seedgen.stream.v1" || root_key || stream_name)  (first 8 bytes)
record_seed = SHA-256("seedgen.record.v1" || stream_seed || index)      (first 8 bytes)
```

Each record is generated from its own derived seed in **counter mode**:
record `i` never depends on records `0..i-1`.  This means:

- Pagination and streaming produce the same bytes in any order.
- Requesting one stream never affects another stream.
- Adding a new stream never changes existing streams.

### Output format

Results are JSON Lines (`.jsonl`), one record per line:

```json
{"i":0,"v":42}
{"i":1,"v":7}
```

Keys are sorted and separators are fixed, so the output is
platform-independent.

### Stream kinds

| Kind | Parameters | Value |
|---|---|---|
| `sequence` | `start`, `step` | `start + i * step` |
| `integers` | `low`, `high` | random integer in `[low, high]` |
| `booleans` | — | `true` / `false` |
| `choice` | `options` | one of the given options |
| `string` | `alphabet`, `length` | random string |

## Version strategy

Definitions record which algorithm version produced them.  This
guarantees that a definition can always reproduce its original output,
even as the algorithm evolves.

### Policy

- **`v1` is frozen.** Its implementation is immutable.  A regression test
  pins the exact v1 output for a fixed definition; any accidental change
  fails the build.
- **New versions are opt-in.** Adding `v2` does not change any existing
  definition.  New definitions bind to the configured default version
  (initially `v1`) unless they explicitly set `algo_version`.
- **Switching the default** affects only definitions created after the
  switch.  Existing definitions stay pinned to their recorded version.
- **Explicit selection** is always possible: pass `algo_version` when
  creating a definition.

### API

- `GET /admin/versions` — list algorithm versions and how many
  definitions use each.
- `POST /admin/versions/default` — set the default version for new
  definitions (admin only).  The response reports which definitions are
  affected.

### Example: pinning to v2

```bash
curl -X POST http://localhost:8000/definitions \
  -H 'X-User: alice' \
  -H 'Content-Type: application/json' \
  -d '{"seed": "my-seed", "algo_version": "v2", "streams": {...}}'
```

## Configuration

Configuration only affects *where* data is stored and service-level
behaviour.  Generation output is never influenced by configuration.

| Config field | Env var | Default | Description |
|---|---|---|---|
| `data_dir` | `SEEDGEN_DATA_DIR` | `data` | Root directory for all persistent state |
| `default_algo_version` | — | `v1` | Algorithm version for new definitions |
| `job_timeout_seconds` | — | `300` | Wall-clock budget for a generation job |
| `gen_chunk_records` | — | `1024` | Records per generated chunk |
| `gen_chunk_delay` | — | `0.0` | Test-only delay between chunks |
| `users` | — | see below | User registry with roles |

### Users and roles

Users are identified by the `X-User` request header.  The default
registry:

| User | Roles |
|---|---|
| `admin` | `admin` |
| `alice` | `creator` |
| `bob` | `viewer` |

Roles:

- **admin** — full access, can switch algorithm versions.
- **creator** — can create definitions and regenerate their own.
- **viewer** — can read definitions they are authorized for.

### Authorization

- Creating definitions requires `creator` or `admin`.
- Reading results requires the owner, an ACL user/role, or admin.
- Regenerating requires the owner or admin.
- Denial reasons are distinguishable: `unauthenticated`,
  `insufficient_role`, `not_owner`, `not_authorized`.

### Audit log

Every generation and access attempt is recorded in
`data/audit/audit.jsonl` (append-only, fsynced).  Each entry records the
timestamp, user, action, resource, result, and reason.

## API overview

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Health check |
| `POST` | `/definitions` | Create a definition (idempotent by content) |
| `GET` | `/definitions` | List visible definitions |
| `GET` | `/definitions/{id}` | Get definition metadata |
| `GET` | `/definitions/{id}/streams/{s}/results` | Paginated results (JSON) |
| `GET` | `/definitions/{id}/streams/{s}/stream` | Streaming results (JSONL) |
| `POST` | `/definitions/{id}/regenerate` | Regenerate (idempotent job) |
| `GET` | `/jobs/{id}` | Job status |
| `POST` | `/jobs/{id}/cancel` | Cancel a running job |
| `POST` | `/definitions/{id}/verify?rebuild=true` | Verify / rebuild results |
| `GET` | `/admin/versions` | Algorithm versions |
| `POST` | `/admin/versions/default` | Set default version |

## Concurrency and idempotency

- **Definitions** are content-addressed: creating the same definition
  twice returns the same identifier and record.
- **Jobs** are keyed by `(def_id, streams, force)`.  Concurrent or
  repeated submissions converge to a single job.  A done job is returned
  as-is; a running job is returned for polling.
- **Cross-process safety** is provided by a non-blocking `fcntl` file
  lock.
- **No half-written artifacts**: results are generated into a staging
  directory and atomically renamed into place only when complete.
  Cancellation or timeout removes the staging area.

## Cache verification and rebuild

- Each published stream has a manifest entry with its SHA-256.
- On access, the digest is verified.  A missing or corrupted file is
  rebuilt transparently from the definition and seed.
- A missing manifest triggers a full rebuild.
- After a restart, jobs that were running are marked `interrupted` and
  can be resubmitted.

## Running

```bash
uv run python main.py
# or
SEEDGEN_DATA_DIR=/data uvicorn seedgen.api:create_app --factory
```

## Tests

```bash
uv run pytest tests/ -v
```

Test coverage:

- `test_determinism.py` — byte-identical output across runs and app
  instances, pagination order independence, streaming vs. pagination,
  frozen v1 anchor.
- `test_streams.py` — stream isolation, independent regeneration,
  adding streams without affecting existing ones.
- `test_versions.py` — v1/v2 differences, version recording, explicit
  selection, default switch behaviour.
- `test_jobs.py` — concurrent submission convergence, idempotent
  resubmission, force regeneration, job polling.
- `test_cancel_timeout.py` — timeout and cancellation cleanup,
  streaming disconnect.
- `test_cache.py` — missing/corrupt file rebuild, manifest loss,
  restart recovery.
- `test_auth.py` — authentication, RBAC, denial reasons, audit trail.
