"""Domain models: generation definitions, streams and fields."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict

from .errors import ValidationError

#: Field kinds supported by algorithm version 1.0.0.
SUPPORTED_FIELD_KINDS = frozenset({
    "int", "float", "choice", "bool", "sequence",
    "uuid", "text", "name", "email", "date", "timestamp",
})


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str
    params: dict = field(default_factory=dict)

    @staticmethod
    def from_dict(data: dict) -> "FieldSpec":
        if not isinstance(data, dict):
            raise ValidationError("field must be an object")
        name = data.get("name")
        kind = data.get("kind")
        if not isinstance(name, str) or not name:
            raise ValidationError("field.name must be a non-empty string")
        if not isinstance(kind, str) or not kind:
            raise ValidationError("field.kind must be a non-empty string")
        params = data.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ValidationError("field.params must be an object")
        return FieldSpec(name=name, kind=kind, params=dict(params))


@dataclass(frozen=True)
class StreamSpec:
    name: str
    row_count: int
    fields: tuple[FieldSpec, ...] = ()

    @staticmethod
    def from_dict(data: dict) -> "StreamSpec":
        if not isinstance(data, dict):
            raise ValidationError("stream must be an object")
        name = data.get("name")
        if not isinstance(name, str) or not name:
            raise ValidationError("stream.name must be a non-empty string")
        row_count = data.get("row_count", 0)
        if not isinstance(row_count, int) or isinstance(row_count, bool):
            raise ValidationError("stream.row_count must be an integer")
        if row_count < 0:
            raise ValidationError("stream.row_count must be >= 0")
        raw_fields = data.get("fields", [])
        if not isinstance(raw_fields, (list, tuple)):
            raise ValidationError("stream.fields must be a list")
        fields_ = tuple(FieldSpec.from_dict(f) for f in raw_fields)
        names = [f.name for f in fields_]
        if len(set(names)) != len(names):
            raise ValidationError(f"duplicate field names in stream {name!r}")
        return StreamSpec(name=name, row_count=row_count, fields=fields_)


@dataclass(frozen=True)
class GenerationDefinition:
    """A named, versioned generation definition.

    ``algorithm_version`` is part of the persisted pin: re-running this
    definition years later uses the same algorithm and yields the same bytes.
    """

    definition_id: str
    name: str
    seed: str
    streams: tuple[StreamSpec, ...] = ()
    algorithm_version: str = ""
    created_by: str = ""
    created_at: str = ""
    description: str = ""
    #: Explicit ACL: user ids ("user:alice") or role ids ("role:auditor")
    #: that may read/regenerate in addition to the creator and admins.
    shared_with: tuple[str, ...] = ()

    def get_stream(self, stream_name: str) -> StreamSpec | None:
        for stream in self.streams:
            if stream.name == stream_name:
                return stream
        return None

    def stream_names(self) -> tuple[str, ...]:
        return tuple(s.name for s in self.streams)

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "GenerationDefinition":
        if not isinstance(data, dict):
            raise ValidationError("definition must be an object")
        raw_streams = data.get("streams", [])
        if not isinstance(raw_streams, (list, tuple)):
            raise ValidationError("streams must be a list")
        streams = tuple(StreamSpec.from_dict(s) for s in raw_streams)
        names = [s.name for s in streams]
        if len(set(names)) != len(names):
            raise ValidationError("duplicate stream names")
        shared = data.get("shared_with", ())
        if not isinstance(shared, (list, tuple)):
            raise ValidationError("shared_with must be a list")
        shared = tuple(str(x) for x in shared)
        return GenerationDefinition(
            definition_id=str(data["definition_id"]),
            name=str(data["name"]),
            seed=str(data["seed"]),
            streams=streams,
            algorithm_version=str(data.get("algorithm_version", "")),
            created_by=str(data.get("created_by", "")),
            created_at=str(data.get("created_at", "")),
            description=str(data.get("description", "")),
            shared_with=shared,
        )


def validate_field_kinds(definition: GenerationDefinition) -> None:
    """Validate against the field kinds a pinned version understands."""
    for stream in definition.streams:
        for f in stream.fields:
            if f.kind not in SUPPORTED_FIELD_KINDS:
                raise ValidationError(
                    f"field {f.name!r} uses unsupported kind {f.kind!r}",
                )
