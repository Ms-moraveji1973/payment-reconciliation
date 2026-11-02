import json
import pytest
from datetime import datetime, timezone, timedelta

from app.modules.order.tasks import process_outbox_event
from app.modules.order.models import OutBoxOrderEvent, EventStatus
from app.modules.order.queue_manager import MessageQueue


@pytest.mark.asyncio
async def test_outbox_relay_pipeline_success(db_session, real_redis):
    """
    Integration Test: Outbox Relay Pipeline (Happy Path)
    Flow: PostgreSQL (OutBoxOrderEvent) -> Outbox Relay Worker -> Redis Streams
    
    Validates:
    1. Persisting an event in PostgreSQL with PENDING status.
    2. Claiming and locking via atomic CTE + UPDATE ... RETURNING.
    3. Publishing message payload into real Redis Streams (pending_orders_queue).
    4. Batch updating status to PUBLISHED and clearing locked_at in PostgreSQL.
    """
    # 1. Arrange: Persist a real OutBoxOrderEvent in PostgreSQL
    event_payload = {
        "amount": 1520000,
        "trace_id": "tx_relay_integ_001",
        "type": "payment"
    }
    event = OutBoxOrderEvent(
        status=EventStatus.PENDING,
        data=event_payload,
        retry_count=0
    )
    db_session.add(event)
    await db_session.commit()
    event_id = event.id

    # 2. Act: Execute the actual Outbox Relay pipeline without mocks
    processed_count = await process_outbox_event(real_redis)

    # 3. Assert (PostgreSQL): Verify atomic state transition to PUBLISHED
    assert processed_count == 1
    await db_session.refresh(event)
    assert event.status == EventStatus.PUBLISHED
    assert event.locked_at is None  # Lock is cleared upon publication
    assert event.last_error is None
    assert event.retry_count == 0

    # 4. Assert (Redis Streams): Verify the message exists in the real Redis stream
    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    stream_len = await real_redis.xlen(queue.stream_name)
    assert stream_len == 1

    messages = await real_redis.xrange(queue.stream_name)
    assert len(messages) == 1

    msg_id, fields = messages[0]
    stream_payload = json.loads(fields["data"])

    assert fields["attempts"] == "0"
    assert stream_payload["amount"] == 1520000
    assert stream_payload["trace_id"] == "tx_relay_integ_001"
    assert stream_payload["type"] == "payment"


@pytest.mark.asyncio
async def test_outbox_relay_invalid_payload_moves_to_dlq(db_session, real_redis):
    """
    Integration Test: Outbox Relay DLQ Failure Path
    Flow: Invalid Payload in DB -> Relay Validation Failure -> Dead Letter Queue
    
    Validates:
    1. Event with invalid amount (non-positive) is detected by domain validator.
    2. Event is forwarded to real Redis DLQ (pending_orders_queue:dead).
    3. Main queue remains clean (0 messages).
    4. Event in PostgreSQL is updated to FAILED with error message and lock released.
    """
    # 1. Arrange: Persist invalid event in DB
    invalid_payload = {
        "amount": -500,
        "trace_id": "tx_bad_payload",
        "type": "payment"
    }
    event = OutBoxOrderEvent(
        status=EventStatus.PENDING,
        data=invalid_payload,
        retry_count=0
    )
    db_session.add(event)
    await db_session.commit()

    # 2. Act: Process relay
    processed_count = await process_outbox_event(real_redis)

    # 3. Assert (PostgreSQL)
    assert processed_count == 1
    await db_session.refresh(event)
    assert event.status == EventStatus.FAILED
    assert "Amount must be positive" in (event.last_error or "")
    assert event.locked_at is None

    # 4. Assert (Redis): Main stream is empty, DLQ stream has 1 entry
    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    assert await real_redis.xlen(queue.stream_name) == 0
    assert await real_redis.xlen(queue.dead_letter_stream) == 1

    dlq_entries = await real_redis.xrange(queue.dead_letter_stream)
    _, dlq_fields = dlq_entries[0]
    assert dlq_fields["event_id"] == str(event.id)
    assert dlq_fields["reason"] == "Amount must be positive"
    assert dlq_fields["original_stream"] == queue.stream_name


@pytest.mark.asyncio
async def test_outbox_relay_stale_lock_recovery(db_session, real_redis):
    """
    Integration Test: Stale Lock Recovery
    Flow: Zombie Event (PROCESSING with expired locked_at) -> Re-claimed -> PUBLISHED
    
    Validates:
    Events that were stuck in PROCESSING state due to a worker crash or timeout (>5 mins)
    are successfully reclaimed and published.
    """
    # 1. Arrange: Stale processing event from 6 minutes ago
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=6)
    event = OutBoxOrderEvent(
        status=EventStatus.PROCESSING,
        data={"amount": 1750000, "trace_id": "tx_stale_recovered", "type": "payment"},
        locked_at=stale_time,
        retry_count=1
    )
    db_session.add(event)
    await db_session.commit()

    # 2. Act
    processed_count = await process_outbox_event(real_redis)

    # 3. Assert (PostgreSQL): Stale lock was broken and transitioned to PUBLISHED
    assert processed_count == 1
    await db_session.refresh(event)
    assert event.status == EventStatus.PUBLISHED
    assert event.locked_at is None
    assert event.last_error is None

    # 4. Assert (Redis): Message arrived in queue
    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    assert await real_redis.xlen(queue.stream_name) == 1


@pytest.mark.asyncio
async def test_outbox_relay_batch_mixed_updates(db_session, real_redis):
    """
    Integration Test: Batch Processing and Heterogeneous Status Updates
    Flow: Multiple Events (Valid + Invalid) -> Batched Execution & Database Updates
    
    Validates:
    New batch update mechanism (process_outbox_updates) correctly partitions
    published events and error events in a single batch transaction.
    """
    # 1. Arrange: 2 valid events and 1 invalid event
    event_valid_1 = OutBoxOrderEvent(
        status=EventStatus.PENDING,
        data={"amount": 1000000, "trace_id": "tx_batch_1", "type": "payment"},
        retry_count=0
    )
    event_invalid = OutBoxOrderEvent(
        status=EventStatus.PENDING,
        data={"amount": 0, "trace_id": "tx_batch_2", "type": "payment"},  # Invalid
        retry_count=0
    )
    event_valid_2 = OutBoxOrderEvent(
        status=EventStatus.PENDING,
        data={"amount": 2000000, "trace_id": "tx_batch_3", "type": "payment"},
        retry_count=0
    )
    db_session.add_all([event_valid_1, event_invalid, event_valid_2])
    await db_session.commit()

    # 2. Act
    processed_count = await process_outbox_event(real_redis)

    # 3. Assert (PostgreSQL): Correct statuses per event
    assert processed_count == 3
    await db_session.refresh(event_valid_1)
    await db_session.refresh(event_invalid)
    await db_session.refresh(event_valid_2)

    assert event_valid_1.status == EventStatus.PUBLISHED
    assert event_valid_1.locked_at is None

    assert event_invalid.status == EventStatus.FAILED
    assert "Amount must be positive" in (event_invalid.last_error or "")
    assert event_invalid.locked_at is None

    assert event_valid_2.status == EventStatus.PUBLISHED
    assert event_valid_2.locked_at is None

    # 4. Assert (Redis): 2 messages in main queue, 1 in DLQ
    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    assert await real_redis.xlen(queue.stream_name) == 2
    assert await real_redis.xlen(queue.dead_letter_stream) == 1
