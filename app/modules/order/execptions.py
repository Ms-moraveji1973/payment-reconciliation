"""Backward-compatibility module for misspelled 'execptions.py'.

Prefer importing from 'app.modules.order.exceptions'.
"""

from app.modules.order.exceptions import (
    WorkerException,
    IgnoreAndAckMessage,
    CriticalRejectMessage,
    OutBoxEventRedisException,
    InvalidOutboxPayload,
    OrderNotFoundError,
    NoFreeAmountAvailableError,
    DuplicateUniqueAmountError,
    PaymentIntentNotFoundError,
    DuplicatePaymentIntentError,
    InvalidSMSDataError,
    DuplicateSMSTransactionError,
    DuplicateOutboxEventError,
    OutboxEventNotFoundError,
)

__all__ = [
    "WorkerException",
    "IgnoreAndAckMessage",
    "CriticalRejectMessage",
    "OutBoxEventRedisException",
    "InvalidOutboxPayload",
    "OrderNotFoundError",
    "NoFreeAmountAvailableError",
    "DuplicateUniqueAmountError",
    "PaymentIntentNotFoundError",
    "DuplicatePaymentIntentError",
    "InvalidSMSDataError",
    "DuplicateSMSTransactionError",
    "DuplicateOutboxEventError",
    "OutboxEventNotFoundError",
]
