"""Custom exception hierarchy for eGon validation framework."""


class EgonValidationError(Exception):
    """Base exception for all eGon validation framework errors."""

    pass


class DatabaseConnectionError(EgonValidationError):
    """Raised when database connection fails or times out."""

    pass


class RuleExecutionError(EgonValidationError):
    """Raised when rule execution fails."""

    pass


class ConfigurationError(EgonValidationError):
    """Raised when configuration is invalid or missing."""

    pass


class PermissionDeniedError(EgonValidationError):
    """Raised when database permissions are insufficient."""

    pass


class ValidationTimeoutError(EgonValidationError):
    """Raised when validation operation times out."""

    pass


class RuleRegistrationError(EgonValidationError):
    """Raised when rule registration fails (e.g., duplicate rules)."""

    pass


class RunIdCollisionError(EgonValidationError):
    """Raised when run ID already exists and cannot be overwritten."""

    pass


class ResultAggregationError(EgonValidationError):
    """Raised when result aggregation fails."""

    pass
