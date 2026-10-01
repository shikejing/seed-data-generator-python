"""Domain error types with stable, distinguishable codes."""


class SeedGenError(Exception):
    code = "seedgen_error"

    def __init__(self, message: str = "", *, code: str | None = None):
        super().__init__(message)
        if code is not None:
            self.code = code


class DefinitionNotFoundError(SeedGenError):
    code = "definition_not_found"


class StreamNotFoundError(SeedGenError):
    code = "stream_not_found"


class ValidationError(SeedGenError):
    code = "validation_error"


class VersionNotFoundError(SeedGenError):
    code = "version_not_found"


class UnauthorizedError(SeedGenError):
    code = "unauthorized"


class ForbiddenError(SeedGenError):
    code = "forbidden"

    def __init__(self, message: str = "access denied", *, reason: str = "forbidden"):
        super().__init__(message)
        self.reason = reason


class GenerationCancelledError(SeedGenError):
    code = "generation_cancelled"


class CacheCorruptedError(SeedGenError):
    code = "cache_corrupted"


class IdempotencyConflictError(SeedGenError):
    code = "idempotency_conflict"
