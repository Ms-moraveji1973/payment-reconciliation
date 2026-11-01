"""Order and Payment Domain Exceptions.

Defines custom exceptions for order processing, payment intents,
SMS webhook transactions, outbox events, and worker task control.
"""

from app.core.exceptions import (
    AppException,
    DomainException,
    EntityNotFoundError,
    EntityAlreadyExistsError,
    DomainValidationError,
    InfrastructureException,
)


# =====================================================================
# Order & Payment Domain Exceptions
# =====================================================================

class OrderNotFoundError(EntityNotFoundError):
    """Raised when an order cannot be found for the given ID and/or user."""

    def __init__(self, message: str = "Order not found"):
        super().__init__(message)


class NoFreeAmountAvailableError(DomainException):
    """Raised when no unique amount is available in the pool for a new order."""

    def __init__(self, message: str = "No free amount is available"):
        super().__init__(message)


class DuplicateUniqueAmountError(EntityAlreadyExistsError):
    """Raised when the generated exact amount conflicts with an existing pending order."""

    def __init__(self, message: str = "Duplicate unique amount"):
        super().__init__(message)


class PaymentIntentNotFoundError(EntityNotFoundError):
    """Raised when no matching pending payment intent is found for the given amount or ID."""

    def __init__(self, message: str = "Payment intent not found"):
        super().__init__(message)


class DuplicatePaymentIntentError(EntityAlreadyExistsError):
    """Raised when multiple pending payment intents match the same exact amount."""

    def __init__(self, message: str = "Duplicate payment intents found"):
        super().__init__(message)


# =====================================================================
# SMS Transaction & Outbox Domain Exceptions
# =====================================================================

class InvalidSMSDataError(DomainValidationError):
    """Raised when SMS webhook payload is missing required fields or has invalid values."""

    def __init__(self, message: str = "The transaction data is incomplete"):
        super().__init__(message)


class DuplicateSMSTransactionError(EntityAlreadyExistsError):
    """Raised when an incoming SMS transaction has already been recorded."""

    def __init__(self, message: str = "The transaction data already exists"):
        super().__init__(message)


class DuplicateOutboxEventError(EntityAlreadyExistsError):
    """Raised when an outbox order event for the transaction already exists."""

    def __init__(self, message: str = "The event data already exists"):
        super().__init__(message)


class OutboxEventNotFoundError(EntityNotFoundError, LookupError):
    """Raised when an outbox event is not found by ID.

    Inherits from LookupError to maintain backward compatibility.
    """

    def __init__(self, message: str = "Outbox event not found"):
        super().__init__(message)


class InvalidOutboxPayload(DomainValidationError):
    """Raised when an outbox event payload structure or amount is invalid."""

    def __init__(self, message: str = "Invalid outbox payload"):
        super().__init__(message)


# =====================================================================
# Infrastructure Exceptions
# =====================================================================

class OutBoxEventRedisException(InfrastructureException):
    """Raised when publishing an outbox event to Redis fails."""

    def __init__(self, message: str = "Outbox event Redis failure"):
        super().__init__(message)


# =====================================================================
# Worker Flow Control Exceptions
# =====================================================================

class WorkerException(AppException):
    """Base exception for worker task execution and flow control."""
    pass


class IgnoreAndAckMessage(WorkerException):
    """Signal that a message cannot/should not be processed further,

    but should be acknowledged (ACK) and cleared from the stream (no retry/DLQ).
    """

    def __init__(self, message: str = "Message ignored and acknowledged"):
        super().__init__(message)


class CriticalRejectMessage(WorkerException):
    """Signal that message processing encountered a critical failure.

    Triggers failure handling (retry count check, DLQ migration upon exhaustion).
    """

    def __init__(self, message: str = "Message critically rejected"):
        super().__init__(message)
