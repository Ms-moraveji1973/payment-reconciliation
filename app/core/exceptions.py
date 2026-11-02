"""Core Application Exceptions.

Provides the base hierarchy for application, domain, and infrastructure exceptions.
Follows Python official exception hierarchy guidelines and OWASP error handling recommendations.
"""


class AppException(Exception):
    """Base exception for all application-defined exceptions."""

    def __init__(self, message: str = "", *args, **kwargs):
        self.message = message
        super().__init__(message, *args)

    def __str__(self) -> str:
        return self.message or super().__str__()


class DomainException(AppException):
    """Base exception for all business logic and domain rule violations."""
    pass


class EntityNotFoundError(DomainException):
    """Raised when an expected domain entity/resource does not exist."""
    pass


class EntityAlreadyExistsError(DomainException):
    """Raised when an entity or resource already exists violating uniqueness constraints."""
    pass


class DomainValidationError(DomainException):
    """Raised when domain data fails domain validation or invariant checks."""
    pass


class InfrastructureException(AppException):
    """Base exception for external infrastructure errors (DB, Redis, network, etc.)."""
    pass


class DatabaseError(InfrastructureException):
    """Raised when an unhandled or unexpected database error occurs."""
    pass
