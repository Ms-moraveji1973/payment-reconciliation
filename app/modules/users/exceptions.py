"""Users Domain Exceptions."""

from app.core.exceptions import (
    DomainException,
    EntityAlreadyExistsError,
    EntityNotFoundError,
)


class UserAlreadyExistsError(EntityAlreadyExistsError):
    """Raised when attempting to create a user that already exists."""

    def __init__(self, message: str = "User already exists"):
        super().__init__(message)


class UsernameAlreadyExistsError(EntityAlreadyExistsError):
    """Raised when attempting to register with a username that is already taken."""

    def __init__(self, message: str = "Username already exists"):
        super().__init__(message)


class UserNotFoundError(EntityNotFoundError):
    """Raised when a requested user is not found."""

    def __init__(self, message: str = "User not found"):
        super().__init__(message)


class RefreshTokenAlreadyExistsError(EntityAlreadyExistsError):
    """Raised when attempting to create a duplicate refresh token record."""

    def __init__(self, message: str = "Refresh token already exists"):
        super().__init__(message)


class TokenRevocationError(DomainException):
    """Raised when revoking refresh tokens fails due to database errors."""

    def __init__(self, message: str = "Error revoking token"):
        super().__init__(message)
