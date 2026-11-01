import json
import pytest
from app.modules.order.tasks import process_outbox_event
from app.modules.order.models import OutBoxOrderEvent, EventStatus
from app.modules.order.queue_manager import MessageQueue


@pytest.mark.asyncio
async def test_outbox_relay_pipeline_success(db_session, real_redis):
    """
    Integration Test: Outbox Relay Pipeline
    Flow: PostgreSQL (OutBoxOrderEvent) -> Outbox Relay Worker -> Redis Streams
    
    Validates:
    1. Persisting an event in PostgreSQL with PENDING status.
    2. Claiming and locking the event with row-level locks (FOR UPDATE SKIP LOCKED).
    3. Transitioning the event to PROCESSING and assigning locked_at.
    4. Publishing the event payload into real Redis Streams (pending_orders_queue).
    5. Finalizing the event status to PUBLISHED in PostgreSQL with no errors.
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

    # 2. Act: Execute the actual Outbox Relay pipeline without any mocks
    await process_outbox_event(real_redis)

    # 3. Assert (PostgreSQL): Verify the event status was updated in the database
    # Expire or query independently to bypass any in-memory identity map caching
    await db_session.refresh(event)
    assert event.status == EventStatus.PUBLISHED
    assert event.locked_at is not None
    assert event.last_error is None
    assert event.retry_count == 0

    # 4. Assert (Redis Streams): Verify the message was published to the real Redis stream
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
