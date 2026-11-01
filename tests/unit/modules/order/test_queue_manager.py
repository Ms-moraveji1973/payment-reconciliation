import pytest
import json
import time
from unittest.mock import AsyncMock, MagicMock
import redis.asyncio as redis

from app.modules.order.queue_manager import (
    Message,
    MessageQueue,
    ConsumerGroup,
)


def test_message_dataclass_post_init():
    msg = Message(id="1-0", stream="orders", data={"amount": 100})
    assert msg.id == "1-0"
    assert msg.stream == "orders"
    assert msg.data == {"amount": 100}
    assert msg.attempts == 0
    assert msg.created_at is not None
    assert isinstance(msg.created_at, float)


@pytest.mark.asyncio
async def test_message_queue_enqueue(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="test_order_queue")
    msg_id = await queue.enqueue({"amount": 1500000, "trace_id": "tx_abc"})

    assert msg_id is not None
    stream_len = await fake_redis.xlen("test_order_queue")
    assert stream_len == 1

    messages = await fake_redis.xrange("test_order_queue")
    stored_id, fields = messages[0]
    assert stored_id == msg_id
    assert fields["attempts"] == "0"
    assert json.loads(fields["data"]) == {"amount": 1500000, "trace_id": "tx_abc"}


@pytest.mark.asyncio
async def test_message_queue_enqueue_dead_letter(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="test_order_queue")
    dlq_id = await queue.enqueue_dead_letter(
        data={"amount": -50},
        event_id=12,
        reason="Amount must be positive"
    )

    assert dlq_id is not None
    dlq_len = await fake_redis.xlen(queue.dead_letter_stream)
    assert dlq_len == 1

    messages = await fake_redis.xrange(queue.dead_letter_stream)
    _, fields = messages[0]
    assert fields["event_id"] == "12"
    assert fields["reason"] == "Amount must be positive"
    assert fields["original_stream"] == "test_order_queue"


@pytest.mark.asyncio
async def test_consumer_group_creation_and_busygroup_handling(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="stream_for_group")
    # First creation -> creates group
    group = await ConsumerGroup.create(queue, group_name="group1", consumer_name="worker1")
    assert group.group_name == "group1"
    assert group.consumer_name == "worker1"

    # Second creation with same group_name -> handles BUSYGROUP cleanly without exception
    group2 = await ConsumerGroup.create(queue, group_name="group1", consumer_name="worker2")
    assert group2.consumer_name == "worker2"


@pytest.mark.asyncio
async def test_consumer_group_read_and_acknowledge(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="stream_rw")
    group = await ConsumerGroup.create(queue, group_name="grp", consumer_name="c1")

    # Empty read
    empty_messages = await group.read_messages(count=5, block_ms=10)
    assert empty_messages == []

    # Enqueue a message and read
    await queue.enqueue({"amount": 12345})
    messages = await group.read_messages(count=5, block_ms=10)
    assert len(messages) == 1
    assert messages[0].data["amount"] == 12345

    # Acknowledge single
    ack_res = await group.acknowledge(messages[0].id)
    assert ack_res is True

    # Acknowledge batch
    batch_ack = await group.acknowledge_batch([])
    assert batch_ack == 0


@pytest.mark.asyncio
async def test_consumer_group_claim_stale_message(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="stale_stream")
    group1 = await ConsumerGroup.create(queue, group_name="stale_grp", consumer_name="worker_dead")
    group2 = await ConsumerGroup.create(queue, group_name="stale_grp", consumer_name="worker_alive")

    # Enqueue and read with worker_dead so it enters PEL
    await queue.enqueue({"amount": 9999})
    msgs = await group1.read_messages(count=1, block_ms=10)
    assert len(msgs) == 1

    # Fake redis claim with min_idle_time=0 for immediate claim
    claimed = await group2.claim_stale_message(min_idle_time=0)
    assert len(claimed) == 1
    assert claimed[0].data["amount"] == 9999
