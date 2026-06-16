import asyncio
import json
import time
import redis.asyncio as redis
from redis.exceptions import TimeoutError
from functools import partial
from typing import Dict, Callable, Optional
from app.modules.order.queue_manager import MessageQueue, ConsumerGroup, Message
from app.core.config import get_settings
from app.modules.order.handlers import handle_payment
from app.modules.order.execptions import IgnoreAndAckMessage, CriticalRejectMessage
from app.core.logger import log
config = get_settings()

class Worker:
    def __init__(self, queue: MessageQueue, group_name: str,
                worker_id: str = None, max_retries: int = 3,
                retry_delay_ms: int = 5000):
        self.queue = queue
        self.group_name = group_name
        self.worker_id = worker_id
        self.redis = queue.redis
        self.consumer = None
        self.handlers: Dict[str, Callable] = {}
        self.default_handler: Optional[Callable] = None
        self.retry_delay_ms = retry_delay_ms
        self.running = False
        self.max_retries = max_retries


    def register_handler(self, message_type: str, handler: Callable[[dict], bool]):
        self.handlers[message_type] = handler

    def set_default_handler(self, handler: Callable[[dict], bool]):
        self.default_handler = handler

    def _get_handler(self, data:dict):
        msg_type = data.get('type')
        if msg_type and msg_type in self.handlers:
            return self.handlers[msg_type]
        return self.default_handler


    async def _process_message(self, message:Message):
        trace_id = message.data.get("trace_id", f"gen-{message.id}")
        context = {
            "trace_id": trace_id,
            "message_id": message.id,
            "amount": message.data.get("amount"),
            "attempts": message.attempts
        }
        log.info("starting message processing", context=context)
        handler = self._get_handler(message.data)
        if not handler:
            log.warning(f"No handler for message: {message.id}",context=context)
            return True
        try:
            result = await handler(message)
            return result if result is not None else True

        except IgnoreAndAckMessage:
            return True

        except CriticalRejectMessage:
            log.error(f"Duplicate amounts in message : {message.id}", context=context)
            return False

        except Exception as e:
            log.error(f"Error processing {message.id} {e}", context=context)
            return False


    async def _handle_failure(self, message:Message):
        trace_id = message.data.get("trace_id", f"gen-{message.id}")
        context = {
            "trace_id": trace_id,
            "message_id": message.id,
            "amount": message.data.get("amount"),
            "current_attempt": message.attempts,
            "max_retries": self.max_retries
        }
        worker_log = log.bind(trace_id=trace_id, message_id=message.id)
        if message.attempts >= self.max_retries:
            log.critical("Executing DLQ migration", context=context)
            try:
                await self._move_to_dead_letter(message)
                await self.consumer.acknowledge(message.id)
                worker_log.warning(f"Message moved to dead letter queue", context=context)
            except Exception as dlq_err:
                worker_log.critical(f"Failed to move message to DLQ! Redis might be down: {dlq_err}", context=context)
        else:
            worker_log.info(f"Message {message.id} will be retried",
                            next_attempt=message.attempts + 1,
                            max_retries=self.max_retries,
                            context=context)


    async def _move_to_dead_letter(self, message: Message):
        await self.queue.redis.xadd(
            self.queue.dead_letter_stream,
            {
                'data': json.dumps(message.data),
                'original_id': message.id,
                'attempts': str(message.attempts),
                'failed_at': str(time.time())
            }
        )


    async def _process_batch(self):
        stale = await self.consumer.claim_stale_message(min_idle_time=6000)
        for stale_message in stale:
            if await self._process_message(stale_message):
                await self.consumer.acknowledge(stale_message.id)
            else:
                await self._handle_failure(stale_message)

        messages = await self.consumer.read_messages(count=10, block_ms=5000)
        for message in messages:
            amount = message.data.get("amount")
            trace_id = message.data.get("trace_id")
            if not amount:
                log.warning(f"Message {trace_id} has no amount field.")
                await self.consumer.acknowledge(message.id)
                continue
            if await self._process_message(message):
                await self.consumer.acknowledge(message.id)
            else:
                await self._handle_failure(message)

    async def start(self):
        self.running = True
        self.consumer = await ConsumerGroup.create(queue=self.queue,
                                                    group_name=self.group_name,
                                                    consumer_name=self.worker_id)
        log.info(f"Worker {self.consumer.consumer_name} starting...")
        await self._run()

    async def _run(self):
        while self.running:
            try:
                await self._process_batch()
            except TimeoutError:
                pass
            except Exception as e:
                log.error(f"Worker error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        log.info(f"Worker {self.consumer.consumer_name} stopped")



async def main():
    redis_client = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=0, decode_responses=True)
    queue = MessageQueue(redis_client,stream_name="pending_orders_queue")
    worker = Worker(queue, group_name='orders_processors', worker_id='worker-1')
    dependencies_handler = partial(handle_payment, redis_client=redis_client)
    worker.register_handler("payment",dependencies_handler)
    try:
        await worker.start()
    except asyncio.CancelledError:
        await worker.stop()
    finally:
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())