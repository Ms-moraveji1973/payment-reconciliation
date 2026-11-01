import pytest
import json
from unittest.mock import AsyncMock, MagicMock

from app.modules.admin.dlq_manager import DeadLetterQueue
from app.modules.order.queue_manager import MessageQueue


@pytest.mark.asyncio
async def test_get_dead_letters_empty(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="test_queue")
    dlq = DeadLetterQueue(queue)

    result = await dlq.get_dead_letters(count=5)
    assert result["messages"] == []
    assert result["next_cursor"] is None


@pytest.mark.asyncio
async def test_get_dead_letters_with_messages_and_pagination(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="test_queue")
    dlq = DeadLetterQueue(queue)

    # Enqueue 3 dead letters
    for i in range(3):
        await fake_redis.xadd(
            dlq.dead_letter_stream,
            {
                "data": json.dumps({"amount": 1000 + i}),
                "original_id": f"orig-{i}",
                "attempts": str(i + 1),
                "failed_at": "1730000000.0"
            }
        )

    # Read count=2 -> should return 2 messages and next_cursor
    page1 = await dlq.get_dead_letters(count=2)
    assert len(page1["messages"]) == 2
    assert page1["next_cursor"] is not None
    assert page1["messages"][0]["data"]["amount"] == 1000
    assert page1["messages"][0]["attempts"] == 1

    # Read rest with cursor
    page2 = await dlq.get_dead_letters(count=2, cursor=page1["next_cursor"])
    assert len(page2["messages"]) == 1
    assert page2["next_cursor"] is None
    assert page2["messages"][0]["data"]["amount"] == 1002


@pytest.mark.asyncio
async def test_replay_message_not_found(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="test_queue")
    dlq = DeadLetterQueue(queue)

    result = await dlq.replay_message("1730000000000-0")
    assert result is None


@pytest.mark.asyncio
async def test_replay_message_success(fake_redis):
    queue = MessageQueue(fake_redis, stream_name="orders_stream")
    dlq = DeadLetterQueue(queue)

    # Add a dead letter
    dead_msg_id = await fake_redis.xadd(
        dlq.dead_letter_stream,
        {
            "data": json.dumps({"amount": 55555}),
            "attempts": "3",
            "failed_at": "1730000000.0"
        }
    )

    replay_result = await dlq.replay_message(dead_msg_id)
    assert replay_result["success"] is True
    assert replay_result["new_msg_id"] is not None

    # Message should be removed from dead letter stream
    remaining_dlq = await fake_redis.xlen(dlq.dead_letter_stream)
    assert remaining_dlq == 0

    # Message should now be in the main orders_stream
    main_len = await fake_redis.xlen(queue.stream_name)
    assert main_len == 1

    # Check fields in main stream
    messages = await fake_redis.xrange(queue.stream_name)
    new_id, fields = messages[0]
    assert fields["attempts"] == "0"
    assert json.loads(fields["data"]) == {"amount": 55555}
