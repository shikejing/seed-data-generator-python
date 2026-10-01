"""Pure, deterministic row/page generation.

Every generated value is a function of the definition (including its pinned
algorithm version), the stream name, the zero-based row index and the field
specification.  Nothing here reads the wall clock, the environment, container
traversal order, :mod:`random` or :mod:`secrets`.

Field value rules (algorithm 1.0.0):

* ``int(min=0,max=2**31-1)``, ``float(min=0.0,max=1.0,decimals=None)``
* ``choice(options=[...])`` – element selection
* ``bool(true_prob=0.5)``
* ``sequence(length, ...)`` – builds ``length`` independent sub-values
* ``uuid`` – deterministic ``uuid5`` style hex string
* ``text(words, length)`` – words drawn from a built-in fixed word list
* ``name`` / ``email(domain="example.com")``
* ``date(start="1970-01-01", end="2100-01-01")`` – ISO date
* ``timestamp(start, end)`` – Unix epoch seconds; fixed if ``value`` is given
"""

from __future__ import annotations

import hashlib
from datetime import date, timedelta

from .canonical import domain_seed
from .errors import ValidationError
from .models import FieldSpec, GenerationDefinition, StreamSpec
from .prng import DeterministicRNG

_FIRST_NAMES = (
    "alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi",
    "ivan", "judy", "kim", "leo", "mallory", "nina", "oscar", "peggy",
)
_LAST_NAMES = (
    "anderson", "brown", "clark", "davis", "evans", "foster", "garcia",
    "harris", "ibsen", "jones", "king", "lee", "miller", "nelson",
)
_WORD_LIST = (
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
    "hotel", "india", "juliet", "kilo", "lima", "mike", "november",
    "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform",
)
_EPOCH_START = date(1970, 1, 1)


def _as_int(params: dict, key: str, default: int) -> int:
    value = params.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"param {key!r} must be an integer")
    return value


def _as_float(params: dict, key: str, default: float) -> float:
    value = params.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"param {key!r} must be a number")
    return float(value)


def _gen_int(rng: DeterministicRNG, spec: FieldSpec):
    lo = _as_int(spec.params, "min", 0)
    hi = _as_int(spec.params, "max", 2**31 - 1)
    if hi <= lo:
        raise ValidationError(f"int field {spec.name!r}: max must be > min")
    return rng.randrange(lo, hi + 1, "int")


def _gen_float(rng: DeterministicRNG, spec: FieldSpec):
    lo = _as_float(spec.params, "min", 0.0)
    hi = _as_float(spec.params, "max", 1.0)
    if hi <= lo:
        raise ValidationError(f"float field {spec.name!r}: max must be > min")
    value = lo + (hi - lo) * rng.random("float")
    decimals = spec.params.get("decimals")
    if decimals is not None:
        if isinstance(decimals, bool) or not isinstance(decimals, int) or decimals < 0:
            raise ValidationError("float 'decimals' must be a non-negative integer")
        value = round(value, decimals)
    return value


def _gen_choice(rng: DeterministicRNG, spec: FieldSpec):
    options = spec.params.get("options", [])
    if not isinstance(options, (list, tuple)) or not options:
        raise ValidationError(f"choice field {spec.name!r} needs non-empty 'options'")
    return rng.pick(list(options), "choice")


def _gen_bool(rng: DeterministicRNG, spec: FieldSpec):
    p = _as_float(spec.params, "true_prob", 0.5)
    if not 0.0 <= p <= 1.0:
        raise ValidationError("bool 'true_prob' must be within [0, 1]")
    return rng.chance(p, "bool")


def _gen_sequence(definition: GenerationDefinition, stream: StreamSpec,
                  row_index: int, spec: FieldSpec, rng: DeterministicRNG):
    length = _as_int(spec.params, "length", 1)
    if length < 0:
        raise ValidationError("sequence 'length' must be >= 0")
    inner_params = spec.params.get("of", {"kind": "int"})
    if not isinstance(inner_params, dict) or "kind" not in inner_params:
        raise ValidationError("sequence field needs an 'of' sub-spec with 'kind'")
    inner = FieldSpec(name=spec.name + "[]", kind=inner_params["kind"],
                      params=dict(inner_params.get("params", {})))
    base = domain_seed(definition.name, definition.algorithm_version,
                       stream.name, definition.seed)
    out = []
    for i in range(length):
        # Independent, domain-separated sub-RNG: element i cannot be
        # influenced by other fields or by the length actually drawn.
        element_key = (base + b"\x00seq\x00"
                       + spec.name.encode("utf-8") + b"\x00"
                       + str(i).encode("ascii"))
        sub_rng = DeterministicRNG.for_row(element_key, row_index)
        out.append(generate_field(sub_rng, inner, definition, stream, row_index))
    return out


def _gen_uuid(rng: DeterministicRNG, spec: FieldSpec):
    raw = rng.draw_bytes(16, "uuid")
    h = raw.hex()
    return f"{h[0:8]}-{h[8:12]}-4{h[13:16]}-8{h[17:20]}-{h[20:32]}"


def _gen_text(rng: DeterministicRNG, spec: FieldSpec):
    length = _as_int(spec.params, "length", 4)
    if length <= 0:
        return ""
    words = list(spec.params.get("words") or _WORD_LIST)
    return " ".join(rng.pick(words, "text") for _ in range(length))


def _gen_name(rng: DeterministicRNG, spec: FieldSpec):
    first = rng.pick(_FIRST_NAMES, "name.first")
    last = rng.pick(_LAST_NAMES, "name.last")
    return f"{first} {last}"


def _gen_email(rng: DeterministicRNG, spec: FieldSpec):
    local = rng.pick(_FIRST_NAMES, "email.local")
    domain = spec.params.get("domain", "example.com")
    if not isinstance(domain, str) or not domain:
        raise ValidationError("email 'domain' must be a non-empty string")
    suffix = rng.randbelow(100_000, "email.num")
    return f"{local}.{suffix}@{domain}"


def _parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"invalid ISO date: {value!r}") from exc


def _gen_date(rng: DeterministicRNG, spec: FieldSpec):
    start = _parse_iso_date(spec.params.get("start", "1970-01-01"))
    end = _parse_iso_date(spec.params.get("end", "2100-01-01"))
    if end < start:
        raise ValidationError("date 'end' must be >= 'start'")
    span = (end - start).days
    if span == 0:
        return start.isoformat()
    return (start + timedelta(days=rng.randbelow(span + 1, "date"))).isoformat()


def _gen_timestamp(rng: DeterministicRNG, spec: FieldSpec):
    if "value" in spec.params:
        value = _as_int(spec.params, "value", 0)
        return value
    start = _parse_iso_date(spec.params.get("start", "1970-01-01"))
    end = _parse_iso_date(spec.params.get("end", "2100-01-01"))
    if end < start:
        raise ValidationError("timestamp 'end' must be >= 'start'")
    start_s = (start - _EPOCH_START).days * 86400
    end_s = (end - _EPOCH_START).days * 86400
    if end_s == start_s:
        return start_s
    return rng.randrange(start_s, end_s, "timestamp")


_GENERATORS = {
    "int": _gen_int,
    "float": _gen_float,
    "choice": _gen_choice,
    "bool": _gen_bool,
    "uuid": _gen_uuid,
    "text": _gen_text,
    "name": _gen_name,
    "email": _gen_email,
    "date": _gen_date,
    "timestamp": _gen_timestamp,
}


def generate_field(rng: DeterministicRNG, spec: FieldSpec,
                   definition: GenerationDefinition, stream: StreamSpec,
                   row_index: int):
    if spec.kind == "sequence":
        return _gen_sequence(definition, stream, row_index, spec, rng)
    handler = _GENERATORS.get(spec.kind)
    if handler is None:
        raise ValidationError(f"unsupported field kind {spec.kind!r}")
    return handler(rng, spec)


def generate_row(definition: GenerationDefinition, stream: StreamSpec,
                 row_index: int) -> dict:
    base = domain_seed(definition.name, definition.algorithm_version,
                       stream.name, definition.seed)
    row = {}
    for field_spec in stream.fields:
        # Each field gets its own domain-separated RNG, so a field's value is
        # independent of sibling-field ordering and of how many random draws
        # sibling fields consume.
        field_key = (base + b"\x00field\x00"
                     + field_spec.name.encode("utf-8"))
        rng = DeterministicRNG.for_row(field_key, row_index)
        row[field_spec.name] = generate_field(
            rng, field_spec, definition, stream, row_index)
    return row


def iter_stream(definition: GenerationDefinition, stream: StreamSpec):
    """Yield rows lazily; never buffers the whole stream in memory."""
    for row_index in range(stream.row_count):
        yield generate_row(definition, stream, row_index)


def generate_page(definition: GenerationDefinition, stream: StreamSpec,
                  offset: int, limit: int) -> list[dict]:
    if offset < 0:
        raise ValidationError("offset must be >= 0")
    if limit < 0:
        raise ValidationError("limit must be >= 0")
    end = min(stream.row_count, offset + limit)
    return [generate_row(definition, stream, i) for i in range(offset, end)]
