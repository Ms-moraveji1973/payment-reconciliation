import pytest
from unittest.mock import AsyncMock, MagicMock
from redis.exceptions import RedisError
from datetime import datetime, timezone, timedelta

from app.modules.order.tasks import (
    validate_event_data,
    process_single_outbox_event,
    update_outbox_event,
    claim_process_events,
    process_outbox_event,
    MAX_OUTBOX_RETRIES,
)
from app.modules.order.execptions import InvalidOutboxPayload
from app.modules.order.models import OutBoxOrderEvent, EventStatus


def test_validate_event_data_valid():
    data = {"amount": 1500000, "trace_id": "tx_123"}
    assert validate_event_data(data) == data

    # Float amount
    float_data = {"amount": 1500000.50}
    assert validate_event_data(float_data) == float_data


def test_validate_event_data_not_dict():
    with pytest.raises(InvalidOutboxPayload, match="Payload must be a dictionary"):
        validate_event_data("not_a_dict")

    with pytest.raises(InvalidOutboxPayload, match="Payload must be a dictionary"):
        validate_event_data([123])


def test_validate_event_data_missing_amount():
    with pytest.raises(InvalidOutboxPayload, match="Missing amount"):
        validate_event_data({"trace_id": "tx_no_amount"})


def test_validate_event_data_amount_boolean():
    with pytest.raises(InvalidOutboxPayload, match="Amount must be numeric"):
        validate_event_data({"amount": True})


def test_validate_event_data_amount_not_numeric():
    with pytest.raises(InvalidOutboxPayload, match="Amount must be numeric"):
        validate_event_data({"amount": "invalid_string"})


def test_validate_event_data_amount_non_positive():
    with pytest.raises(InvalidOutboxPayload, match="Amount must be positive"):
        validate_event_data({"amount": 0})

    with pytest.raises(InvalidOutboxPayload, match="Amount must be positive"):
        validate_event_data({"amount": -500})


@pytest.mark.asyncio
async def test_process_single_outbox_event_success(mocker, fake_redis):
    mocker_update = mocker.patch("app.modules.order.tasks.update_outbox_event", new_callable=AsyncMock)
    event = {
        "id": 101,
        "data": {"amount": 1510000, "trace_id": "tr_1"},
        "retry_count": 0
    }

    await process_single_outbox_event(fake_redis, event)

    # Queue should contain the message
    stream_len = await fake_redis.xlen("pending_orders_queue")
    assert stream_len == 1

    # Should update event to PUBLISHED
    mocker_update.assert_awaited_once_with(
        101,
        status=EventStatus.PUBLISHED,
        last_error=None
    )


@pytest.mark.asyncio
async def test_process_single_outbox_event_invalid_payload(mocker, fake_redis):
    mocker_update = mocker.patch("app.modules.order.tasks.update_outbox_event", new_callable=AsyncMock)
    event = {
        "id": 102,
        "data": {"amount": -10},  # Invalid payload
        "retry_count": 0
    }

    await process_single_outbox_event(fake_redis, event)

    # Message should be sent to dead letter stream
    dlq_len = await fake_redis.xlen("pending_orders_queue:dead")
    assert dlq_len == 1

    # Should update event to FAILED
    mocker_update.assert_awaited_once()
    call_args = mocker_update.await_args.kwargs
    assert call_args["status"] == EventStatus.FAILED
    assert "Amount must be positive" in call_args["last_error"]


@pytest.mark.asyncio
async def test_process_single_outbox_event_redis_error_retry(mocker, fake_redis):
    mocker_update = mocker.patch("app.modules.order.tasks.update_outbox_event", new_callable=AsyncMock)
    mocker.patch("app.modules.order.queue_manager.MessageQueue.enqueue", side_effect=RedisError("Redis down"))

    event = {
        "id": 103,
        "data": {"amount": 1520000},
        "retry_count": 1
    }

    await process_single_outbox_event(fake_redis, event)

    # Should stay PENDING with incremented retry_count
    mocker_update.assert_awaited_once_with(
        103,
        status=EventStatus.PENDING,
        retry_count=2,
        last_error="Redis down"
    )


@pytest.mark.asyncio
async def test_process_single_outbox_event_redis_error_max_retries(mocker, fake_redis):
    mocker_update = mocker.patch("app.modules.order.tasks.update_outbox_event", new_callable=AsyncMock)
    mocker.patch("app.modules.order.queue_manager.MessageQueue.enqueue", side_effect=RedisError("Redis still down"))

    event = {
        "id": 104,
        "data": {"amount": 1520000},
        "retry_count": MAX_OUTBOX_RETRIES - 1  # 4 attempts -> next is 5 >= MAX
    }

    await process_single_outbox_event(fake_redis, event)

    # Should transition to FAILED
    mocker_update.assert_awaited_once()
    call_args = mocker_update.await_args.kwargs
    assert call_args["status"] == EventStatus.FAILED
    assert call_args["retry_count"] == MAX_OUTBOX_RETRIES
    assert "Max retries" in call_args["last_error"]


@pytest.mark.asyncio
async def test_claim_process_events(mocker):
    fake_event = OutBoxOrderEvent(
        id=7,
        status=EventStatus.PENDING,
        data={"amount": 1530000},
        retry_count=0
    )
    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_event]
    mock_session.execute.return_value = mock_result

    # Mock session.begin() as an async context manager
    mock_session.begin = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()

    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock()

    mocker.patch("app.modules.order.tasks.SessionLocal", mock_session_local)

    events = await claim_process_events(limit_s=10)

    assert len(events) == 1
    assert events[0]["id"] == 7
    assert events[0]["data"]["amount"] == 1530000
    assert fake_event.status == EventStatus.PROCESSING
    assert fake_event.locked_at is not None


@pytest.mark.asyncio
async def test_update_outbox_event(mocker):
    fake_event = OutBoxOrderEvent(id=1, status=EventStatus.PROCESSING)
    mock_session = AsyncMock()
    mock_session.get.return_value = fake_event

    mock_session.begin = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()

    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock()
    mocker.patch("app.modules.order.tasks.SessionLocal", mock_session_local)

    await update_outbox_event(
        1,
        status=EventStatus.PUBLISHED,
        retry_count=2,
        last_error="test_error"
    )

    assert fake_event.status == EventStatus.PUBLISHED
    assert fake_event.retry_count == 2
    assert fake_event.last_error == "test_error"


@pytest.mark.asyncio
async def test_update_outbox_event_not_found(mocker):
    mock_session = AsyncMock()
    mock_session.get.return_value = None

    mock_session.begin = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock(return_value=False)

    mock_session_local = MagicMock()
    mock_session_local.return_value.__aenter__.return_value = mock_session
    mock_session_local.return_value.__aexit__ = AsyncMock(return_value=False)
    mocker.patch("app.modules.order.tasks.SessionLocal", mock_session_local)

    with pytest.raises(LookupError, match="Outbox event not found: 999"):
        await update_outbox_event(999, status=EventStatus.FAILED)
