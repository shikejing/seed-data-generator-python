"""Algorithm version registry.

Every definition pins the algorithm version it was created with; new
versions never silently change existing output.  A new generator release
adds a registry entry here but ``DEFAULT_VERSION`` only moves when the
project consciously opts in, and stored definitions are unaffected either way
because their pin is persisted.
"""

from __future__ import annotations

from .errors import VersionNotFoundError

#: Version used for newly created definitions unless explicitly overridden.
DEFAULT_VERSION = "1.0.0"

#: The newest version shipped with this build.
CURRENT_VERSION = "1.1.0"

_REGISTRY: dict[str, str] = {
    "1.0.0": "Initial deterministic generators: int, float, choice, bool, "
             "sequence, uuid, text, name, email, date.",
    # 1.1.0 is intentionally a forward-looking example: no definition is
    # bound to it until explicitly requested.
    "1.1.0": "Reserved for future generators; selecting it is explicit only.",
}


def register_version(version: str, *, description: str = "") -> None:
    if not isinstance(version, str) or not version:
        raise ValueError("version must be a non-empty string")
    _REGISTRY[version] = description


def version_exists(version: str) -> bool:
    return version in _REGISTRY


def require_version(version: str) -> str:
    if not version_exists(version):
        raise VersionNotFoundError(
            f"algorithm version {version!r} is not registered",
            code="version_not_found",
        )
    return version


def all_versions() -> list[str]:
    return sorted(_REGISTRY)


def describe(version: str) -> str:
    require_version(version)
    return _REGISTRY[version]
