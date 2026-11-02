"""Unit tests for Custom Exceptions hierarchy and behavior."""

import pytest
from app.core.exceptions import (
    AppException,
    DomainException,
    EntityNotFoundError,
    EntityAlreadyExistsError,
    DomainValidationError,
    InfrastructureException,
    DatabaseError,
)
from app.modules.order.exceptions import (
    OrderNotFoundError,
    NoFreeAmountAvailableError,
    DuplicateUniqueAmountError,
    PaymentIntentNotFoundError,
    DuplicatePaymentIntentError,
    InvalidSMSDataError,
    DuplicateSMSTransactionError,
    DuplicateOutboxEventError,
    OutboxEventNotFoundError,
    InvalidOutboxPayload,
    OutBoxEventRedisException,
    WorkerException,
    IgnoreAndAckMessage,
    CriticalRejectMessage,
)
from app.modules.users.exceptions import (
    UserAlreadyExistsError,
    UsernameAlreadyExistsError,
    UserNotFoundError,
    RefreshTokenAlreadyExistsError,
    TokenRevocationError,
)
import app.modules.order.execptions as compat_execptions


def test_core_exception_hierarchy():
    """Verify core exception inheritance and base behavior."""
    assert issubclass(DomainException, AppException)
    assert issubclass(InfrastructureException, AppException)
    assert issubclass(EntityNotFoundError, DomainException)
    assert issubclass(EntityAlreadyExistsError, DomainException)
    assert issubclass(DomainValidationError, DomainException)
    assert issubclass(DatabaseError, InfrastructureException)

    exc = AppException("Custom base error")
    assert str(exc) == "Custom base error"
    assert exc.message == "Custom base error"


def test_order_domain_exception_hierarchy():
    """Verify order and payment domain exceptions inherit from correct base types."""
    assert issubclass(OrderNotFoundError, EntityNotFoundError)
    assert issubclass(NoFreeAmountAvailableError, DomainException)
    assert issubclass(DuplicateUniqueAmountError, EntityAlreadyExistsError)
    assert issubclass(PaymentIntentNotFoundError, EntityNotFoundError)
    assert issubclass(DuplicatePaymentIntentError, EntityAlreadyExistsError)

    # Check default error messages
    assert str(OrderNotFoundError()) == "Order not found"
    assert str(NoFreeAmountAvailableError()) == "No free amount is available"
    assert str(DuplicateUniqueAmountError()) == "Duplicate unique amount"
    assert str(PaymentIntentNotFoundError()) == "Payment intent not found"
    assert str(DuplicatePaymentIntentError()) == "Duplicate payment intents found"


def test_sms_and_outbox_exception_hierarchy():
    """Verify SMS and outbox domain exceptions."""
    assert issubclass(InvalidSMSDataError, DomainValidationError)
    assert issubclass(DuplicateSMSTransactionError, EntityAlreadyExistsError)
    assert issubclass(DuplicateOutboxEventError, EntityAlreadyExistsError)
    assert issubclass(OutboxEventNotFoundError, EntityNotFoundError)
    assert issubclass(OutboxEventNotFoundError, LookupError)  # backward-compatibility
    assert issubclass(InvalidOutboxPayload, DomainValidationError)
    assert issubclass(OutBoxEventRedisException, InfrastructureException)

    # Check default messages
    assert str(InvalidSMSDataError()) == "The transaction data is incomplete"
    assert str(DuplicateSMSTransactionError()) == "The transaction data already exists"
    assert str(DuplicateOutboxEventError()) == "The event data already exists"
    assert str(OutboxEventNotFoundError()) == "Outbox event not found"
    assert str(InvalidOutboxPayload()) == "Invalid outbox payload"


def test_worker_exception_hierarchy():
    """Verify worker flow-control exceptions."""
    assert issubclass(WorkerException, AppException)
    assert issubclass(IgnoreAndAckMessage, WorkerException)
    assert issubclass(CriticalRejectMessage, WorkerException)

    ignore_exc = IgnoreAndAckMessage("1520000 not found")
    assert "1520000 not found" in str(ignore_exc)
    assert isinstance(ignore_exc, WorkerException)
    assert isinstance(ignore_exc, AppException)

    reject_exc = CriticalRejectMessage("Duplicate found")
    assert "Duplicate found" in str(reject_exc)
    assert isinstance(reject_exc, WorkerException)
    assert isinstance(reject_exc, AppException)


def test_users_exception_hierarchy():
    """Verify user domain exceptions."""
    assert issubclass(UserAlreadyExistsError, EntityAlreadyExistsError)
    assert issubclass(UsernameAlreadyExistsError, EntityAlreadyExistsError)
    assert issubclass(UserNotFoundError, EntityNotFoundError)
    assert issubclass(RefreshTokenAlreadyExistsError, EntityAlreadyExistsError)
    assert issubclass(TokenRevocationError, DomainException)

    assert str(UserAlreadyExistsError()) == "User already exists"
    assert str(UsernameAlreadyExistsError()) == "Username already exists"
    assert str(TokenRevocationError()) == "Error revoking token"


def test_backward_compatibility_module():
    """Verify app.modules.order.execptions re-exports the identical classes."""
    assert compat_execptions.WorkerException is WorkerException
    assert compat_execptions.IgnoreAndAckMessage is IgnoreAndAckMessage
    assert compat_execptions.CriticalRejectMessage is CriticalRejectMessage
    assert compat_execptions.OutBoxEventRedisException is OutBoxEventRedisException
    assert compat_execptions.InvalidOutboxPayload is InvalidOutboxPayload
    assert compat_execptions.PaymentIntentNotFoundError is PaymentIntentNotFoundError
    assert compat_execptions.DuplicatePaymentIntentError is DuplicatePaymentIntentError
