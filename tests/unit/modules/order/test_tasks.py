import pytest
from unittest.mock import AsyncMock, MagicMock
from redis.exceptions import RedisError

from app.modules.order.tasks import (
    validate_event_data,
    process_single_outbox_event,
    claim_process_events,
    process_outbox_updates,
    process_outbox_event,
    MAX_OUTBOX_RETRIES,
    LOCK_TIMEOUT_MINUTES,
)
from app.modules.order.exceptions import InvalidOutboxPayload
from app.modules.order.models import EventStatus


# =====================================================================
# Validation Unit Tests
# =====================================================================

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


# =====================================================================
# process_single_outbox_event Unit Tests
# =====================================================================

@pytest.mark.asyncio
async def test_process_single_outbox_event_success():
    mock_queue = AsyncMock()
    event = {
        "id": 101,
        "data": {"amount": 1510000, "trace_id": "tr_1"},
        "retry_count": 0,
    }

    result = await process_single_outbox_event(mock_queue, event)

    mock_queue.enqueue.assert_awaited_once_with({"amount": 1510000, "trace_id": "tr_1"})
    assert result == {
        "id": 101,
        "status": EventStatus.PUBLISHED,
        "retry_count": 0,
        "last_error": None,
    }


@pytest.mark.asyncio
async def test_process_single_outbox_event_none_retry_count():
    mock_queue = AsyncMock()
    event = {
        "id": 102,
        "data": {"amount": 1510000},
        "retry_count": None,
    }

    result = await process_single_outbox_event(mock_queue, event)

    mock_queue.enqueue.assert_awaited_once()
    assert result["retry_count"] == 0
    assert result["status"] == EventStatus.PUBLISHED


@pytest.mark.asyncio
async def test_process_single_outbox_event_invalid_payload_dlq_success():
    mock_queue = AsyncMock()
    event = {
        "id": 103,
        "data": {"amount": -10},
        "retry_count": 0,
    }

    result = await process_single_outbox_event(mock_queue, event)

    mock_queue.enqueue_dead_letter.assert_awaited_once_with(
        data={"amount": -10},
        event_id=103,
        reason="Amount must be positive",
    )
    assert result == {
        "id": 103,
        "status": EventStatus.FAILED,
        "retry_count": 0,
        "last_error": "Amount must be positive",
    }


@pytest.mark.asyncio
async def test_process_single_outbox_event_invalid_payload_dlq_redis_error_retry():
    mock_queue = AsyncMock()
    mock_queue.enqueue_dead_letter.side_effect = RedisError("DLQ failure")
    event = {
        "id": 104,
        "data": {"amount": -10},
        "retry_count": 1,
    }

    result = await process_single_outbox_event(mock_queue, event)

    assert result["status"] == EventStatus.PENDING
    assert result["retry_count"] == 2
    assert "DLQ Redis error" in result["last_error"]


@pytest.mark.asyncio
async def test_process_single_outbox_event_invalid_payload_dlq_redis_error_max_retries():
    mock_queue = AsyncMock()
    mock_queue.enqueue_dead_letter.side_effect = RedisError("DLQ failure")
    event = {
        "id": 105,
        "data": {"amount": -10},
        "retry_count": MAX_OUTBOX_RETRIES - 1,
    }

    result = await process_single_outbox_event(mock_queue, event)

    assert result["status"] == EventStatus.FAILED
    assert result["retry_count"] == MAX_OUTBOX_RETRIES
    assert "DLQ Redis error after max retries" in result["last_error"]


@pytest.mark.asyncio
async def test_process_single_outbox_event_redis_error_retry():
    mock_queue = AsyncMock()
    mock_queue.enqueue.side_effect = RedisError("Redis connection lost")
    event = {
        "id": 106,
        "data": {"amount": 1520000},
        "retry_count": 1,
    }

    result = await process_single_outbox_event(mock_queue, event)

    assert result == {
        "id": 106,
        "status": EventStatus.PENDING,
        "retry_count": 2,
        "last_error": "Redis connection lost",
    }


@pytest.mark.asyncio
async def test_process_single_outbox_event_redis_error_max_retries():
    mock_queue = AsyncMock()
    mock_queue.enqueue.side_effect = RedisError("Redis connection lost")
    event = {
        "id": 107,
        "data": {"amount": 1520000},
        "retry_count": MAX_OUTBOX_RETRIES - 1,
    }

    result = await process_single_outbox_event(mock_queue, event)

    assert result["status"] == EventStatus.FAILED
    assert result["retry_count"] == MAX_OUTBOX_RETRIES
    assert f"Max retries ({MAX_OUTBOX_RETRIES}) reached" in result["last_error"]


# =====================================================================
# claim_process_events Unit Tests
# =====================================================================

@pytest.mark.asyncio
async def test_claim_process_events():
    mock_session = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()

    mock_row_1 = MagicMock()
    mock_row_1._mapping = {"id": 1, "data": {"amount": 1000}, "retry_count": 0}
    mock_row_2 = MagicMock()
    mock_row_2._mapping = {"id": 2, "data": {"amount": 2000}, "retry_count": 1}

    mock_result = MagicMock()
    mock_result.all.return_value = [mock_row_1, mock_row_2]
    mock_session.execute = AsyncMock(return_value=mock_result)

    claimed = await claim_process_events(mock_session, limit_s=50)

    assert len(claimed) == 2
    assert claimed[0]["id"] == 1
    assert claimed[0]["data"]["amount"] == 1000
    assert claimed[1]["id"] == 2
    mock_session.execute.assert_awaited_once()


# =====================================================================
# process_outbox_updates Unit Tests
# =====================================================================

@pytest.mark.asyncio
async def test_process_outbox_updates_empty():
    mock_session = MagicMock()
    mock_session.execute = AsyncMock()
    await process_outbox_updates(mock_session, [])
    mock_session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_process_outbox_updates_published_only():
    mock_session = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()
    mock_session.execute = AsyncMock()

    updates = [
        {"id": 1, "status": EventStatus.PUBLISHED, "retry_count": 0, "last_error": None},
        {"id": 2, "status": EventStatus.PUBLISHED, "retry_count": 0, "last_error": None},
    ]

    await process_outbox_updates(mock_session, updates)

    assert mock_session.execute.await_count == 1


@pytest.mark.asyncio
async def test_process_outbox_updates_errors_only():
    mock_session = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()
    mock_session.execute = AsyncMock()

    updates = [
        {"id": 10, "status": EventStatus.PENDING, "retry_count": 2, "last_error": "err1"},
        {"id": 20, "status": EventStatus.FAILED, "retry_count": 5, "last_error": "err2"},
    ]

    await process_outbox_updates(mock_session, updates)

    assert mock_session.execute.await_count == 1
    call_args = mock_session.execute.await_args[0]
    # Verify parameter list passed to batch update
    assert call_args[1] == updates


@pytest.mark.asyncio
async def test_process_outbox_updates_mixed():
    mock_session = MagicMock()
    mock_session.begin.return_value.__aenter__ = AsyncMock()
    mock_session.begin.return_value.__aexit__ = AsyncMock()
    mock_session.execute = AsyncMock()

    updates = [
        {"id": 1, "status": EventStatus.PUBLISHED, "retry_count": 0, "last_error": None},
        {"id": 2, "status": EventStatus.FAILED, "retry_count": 5, "last_error": "Invalid"},
    ]

    await process_outbox_updates(mock_session, updates)

    assert mock_session.execute.await_count == 2


# =====================================================================
# process_outbox_event Orchestration Unit Tests
# =====================================================================

@pytest.mark.asyncio
async def test_process_outbox_event_no_events(mocker):
    mock_redis = AsyncMock()
    mocker.patch(
        "app.modules.order.tasks.claim_process_events",
        new_callable=AsyncMock,
        return_value=[],
    )

    mock_session = AsyncMock()
    mock_get_session = MagicMock()
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock()
    mocker.patch("app.modules.order.tasks.get_session", mock_get_session)

    count = await process_outbox_event(mock_redis)

    assert count == 0


@pytest.mark.asyncio
async def test_process_outbox_event_success_flow(mocker):
    mock_redis = AsyncMock()
    events = [
        {"id": 1, "data": {"amount": 1000}, "retry_count": 0},
        {"id": 2, "data": {"amount": 2000}, "retry_count": 0},
    ]

    mocker.patch(
        "app.modules.order.tasks.claim_process_events",
        new_callable=AsyncMock,
        return_value=events,
    )

    update_1 = {"id": 1, "status": EventStatus.PUBLISHED, "retry_count": 0, "last_error": None}
    update_2 = {"id": 2, "status": EventStatus.PUBLISHED, "retry_count": 0, "last_error": None}
    mocker.patch(
        "app.modules.order.tasks.process_single_outbox_event",
        new_callable=AsyncMock,
        side_effect=[update_1, update_2],
    )

    mock_updates = mocker.patch(
        "app.modules.order.tasks.process_outbox_updates",
        new_callable=AsyncMock,
    )

    mock_session = AsyncMock()
    mock_get_session = MagicMock()
    mock_get_session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    mock_get_session.return_value.__aexit__ = AsyncMock()
    mocker.patch("app.modules.order.tasks.get_session", mock_get_session)

    count = await process_outbox_event(mock_redis)

    assert count == 2
    mock_updates.assert_awaited_once_with(mock_session, [update_1, update_2])
