import pytest
from unittest.mock import AsyncMock, MagicMock
from app.modules.order.handlers import handle_payment
from app.modules.order.exceptions import (
    IgnoreAndAckMessage,
    CriticalRejectMessage,
    PaymentIntentNotFoundError,
    DuplicatePaymentIntentError,
)
from app.modules.order.queue_manager import Message


@pytest.mark.asyncio
async def test_handle_payment_success(mocker, fake_redis):
    mock_process = mocker.patch("app.modules.order.handlers.process_pending_payment", new_callable=AsyncMock)
    mock_session = AsyncMock()
    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock()
    mocker.patch("app.modules.order.handlers.SessionLocal", mock_session_local)

    # Setup fake_redis with pending_orders
    await fake_redis.sadd("pending_orders", "1520000")

    fake_message = Message(
        id="100-0",
        stream="pending_orders_queue",
        data={"amount": 1520000, "type": "payment"}
    )

    result = await handle_payment(fake_message, fake_redis)

    assert result is True
    mock_process.assert_awaited_once_with(1520000, mock_session)
    mock_session.commit.assert_awaited_once()

    # Verify amount was moved to free_amounts in redis
    is_in_pending = await fake_redis.sismember("pending_orders", "1520000")
    is_in_free = await fake_redis.sismember("free_amounts", "1520000")
    assert not is_in_pending
    assert bool(is_in_free)


@pytest.mark.asyncio
async def test_handle_payment_no_result_found(mocker, fake_redis):
    mocker.patch(
        "app.modules.order.handlers.process_pending_payment",
        side_effect=PaymentIntentNotFoundError("not found in DB")
    )
    mock_session = AsyncMock()
    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    mocker.patch("app.modules.order.handlers.SessionLocal", mock_session_local)

    fake_message = Message(
        id="101-0",
        stream="pending_orders_queue",
        data={"amount": 1520000}
    )

    with pytest.raises(IgnoreAndAckMessage, match="not found in DB"):
        await handle_payment(fake_message, fake_redis)

    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_payment_multiple_results_found(mocker, fake_redis):
    mocker.patch(
        "app.modules.order.handlers.process_pending_payment",
        side_effect=DuplicatePaymentIntentError("Duplicate amounts")
    )
    mock_session = AsyncMock()
    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    mocker.patch("app.modules.order.handlers.SessionLocal", mock_session_local)

    fake_message = Message(
        id="102-0",
        stream="pending_orders_queue",
        data={"amount": 1520000}
    )

    with pytest.raises(CriticalRejectMessage, match="Duplicate amounts found"):
        await handle_payment(fake_message, fake_redis)

    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_payment_unexpected_exception(mocker, fake_redis):
    mocker.patch(
        "app.modules.order.handlers.process_pending_payment",
        side_effect=RuntimeError("Database connection lost")
    )
    mock_session = AsyncMock()
    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    mocker.patch("app.modules.order.handlers.SessionLocal", mock_session_local)

    fake_message = Message(
        id="103-0",
        stream="pending_orders_queue",
        data={"amount": 1520000}
    )

    with pytest.raises(CriticalRejectMessage, match="Unexpected error"):
        await handle_payment(fake_message, fake_redis)

    mock_session.rollback.assert_awaited_once()
