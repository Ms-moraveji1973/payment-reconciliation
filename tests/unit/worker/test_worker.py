import pytest
import json
from unittest.mock import AsyncMock, MagicMock
from app.worker import Worker
from app.modules.order.queue_manager import MessageQueue, Message
from app.modules.order.execptions import IgnoreAndAckMessage, CriticalRejectMessage


@pytest.fixture
def fake_queue(fake_redis):
    return MessageQueue(fake_redis, stream_name="test_worker_queue")


def test_worker_handler_registration(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    handler_payment = MagicMock()
    handler_default = MagicMock()

    worker.register_handler("payment", handler_payment)
    worker.set_default_handler(handler_default)

    assert worker._get_handler({"type": "payment"}) == handler_payment
    assert worker._get_handler({"type": "unknown_type"}) == handler_default
    assert worker._get_handler({}) == handler_default


@pytest.mark.asyncio
async def test_worker_process_message_no_handler(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    msg = Message(id="1-0", stream=fake_queue.stream_name, data={"type": "unregistered"})

    result = await worker._process_message(msg)
    assert result is False


@pytest.mark.asyncio
async def test_worker_process_message_success(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    mock_handler = AsyncMock(return_value=True)
    worker.register_handler("payment", mock_handler)

    msg = Message(id="2-0", stream=fake_queue.stream_name, data={"type": "payment", "amount": 1000})
    result = await worker._process_message(msg)

    assert result is True
    mock_handler.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_worker_process_message_ignore_and_ack(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    mock_handler = AsyncMock(side_effect=IgnoreAndAckMessage("Not found"))
    worker.register_handler("payment", mock_handler)

    msg = Message(id="3-0", stream=fake_queue.stream_name, data={"type": "payment"})
    result = await worker._process_message(msg)

    assert result is True


@pytest.mark.asyncio
async def test_worker_process_message_critical_reject(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    mock_handler = AsyncMock(side_effect=CriticalRejectMessage("Duplicate"))
    worker.register_handler("payment", mock_handler)

    msg = Message(id="4-0", stream=fake_queue.stream_name, data={"type": "payment"})
    result = await worker._process_message(msg)

    assert result is False


@pytest.mark.asyncio
async def test_worker_process_message_generic_exception(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    mock_handler = AsyncMock(side_effect=RuntimeError("Unexpected error"))
    worker.register_handler("payment", mock_handler)

    msg = Message(id="5-0", stream=fake_queue.stream_name, data={"type": "payment"})
    result = await worker._process_message(msg)

    assert result is False


@pytest.mark.asyncio
async def test_worker_handle_failure_below_max_retries(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1", max_retries=3)
    worker.consumer = AsyncMock()
    worker._move_to_dead_letter = AsyncMock()

    msg = Message(id="6-0", stream=fake_queue.stream_name, data={"amount": 100}, attempts=1)
    await worker._handle_failure(msg)

    # Below max retries: should NOT move to dead letter or acknowledge yet
    worker._move_to_dead_letter.assert_not_called()
    worker.consumer.acknowledge.assert_not_called()


@pytest.mark.asyncio
async def test_worker_handle_failure_max_retries_reached(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1", max_retries=3)
    worker.consumer = AsyncMock()

    msg = Message(id="7-0", stream=fake_queue.stream_name, data={"amount": 500}, attempts=3)
    await worker._handle_failure(msg)

    # Reached max retries: message should be in dead letter stream and acknowledged
    worker.consumer.acknowledge.assert_awaited_once_with("7-0")

    dlq_len = await fake_queue.redis.xlen(fake_queue.dead_letter_stream)
    assert dlq_len == 1

    messages = await fake_queue.redis.xrange(fake_queue.dead_letter_stream)
    _, fields = messages[0]
    assert fields["original_id"] == "7-0"
    assert fields["attempts"] == "3"
    assert json.loads(fields["data"]) == {"amount": 500}


@pytest.mark.asyncio
async def test_worker_move_to_dead_letter(fake_queue):
    worker = Worker(fake_queue, group_name="test_grp", worker_id="w1")
    msg = Message(id="8-0", stream=fake_queue.stream_name, data={"amount": 999}, attempts=2)

    await worker._move_to_dead_letter(msg)

    dlq_len = await fake_queue.redis.xlen(fake_queue.dead_letter_stream)
    assert dlq_len == 1

    messages = await fake_queue.redis.xrange(fake_queue.dead_letter_stream)
    _, fields = messages[0]
    assert fields["original_id"] == "8-0"
    assert fields["attempts"] == "2"
