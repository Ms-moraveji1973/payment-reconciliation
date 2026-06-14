import logging
import asyncio
import redis.asyncio as redis
from redis.exceptions import TimeoutError
from app.db.database import SessionLocal
from app.modules.order.queue_manager import MessageQueue, ConsumerGroup
from app.modules.order.service import process_pending_payment
from app.core.config import get_settings

config = get_settings()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, queue: MessageQueue, group_name: str,
                worker_id: str = None,
                retry_delay_ms: int = 5000):
        self.queue = queue
        self.group_name = group_name
        self.worker_id = worker_id
        self.redis = queue.redis
        self.consumer = None
        self.retry_delay_ms = retry_delay_ms
        self.running = False


    async def _process_batch(self):
        messages = await self.consumer.read_messages(count=10, block_ms=5000)
        for message in messages:
            amount = message.data.get("amount")
            if not amount:
                logger.warning(f"Message {message.id} has no amount field.")
                await self.consumer.acknowledge(message.id)
                continue

            async with SessionLocal() as session:
                try:
                    await process_pending_payment(amount, session)
                    await session.commit()
                    await self.redis.smove("pending_orders","free_amounts",str(amount))
                    await self.consumer.acknowledge(message.id)
                    logger.info(f"Message {message.id} processed successfully.")

                except ValueError as e:
                    await session.rollback()
                    error_message = str(e)

                    if error_message == "NoResultFound":
                        await self.consumer.acknowledge(message.id)
                        logger.warning(f"{message.id} has been acknowledged")

                    elif error_message == "MultipleResultsFound":
                        logger.critical(f"There are the same {amount} in DB --> {message.id}")

                except Exception:
                    await session.rollback()
                    logger.critical(f"Unexpected error in worker for message {message.id}:")



    async def start(self):
        self.running = True
        self.consumer = await ConsumerGroup.create(queue=self.queue,
                                                    group_name=self.group_name,
                                                    consumer_name=self.worker_id)
        logger.info(f"Worker {self.consumer.consumer_name} starting...")
        await self._run()

    async def _run(self):
        while self.running:
            try:
                await self._process_batch()
            except TimeoutError:
                pass
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        self.running = False
        logger.info(f"Worker {self.consumer.consumer_name} stopped")

async def main():
    redis_client = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=0, decode_responses=True)
    queue = MessageQueue(redis_client,stream_name="pending_orders_queue")
    worker = Worker(queue, group_name='orders_processors', worker_id='worker-1')

    try:
        await worker.start()
    except asyncio.CancelledError:
        await worker.stop()
    finally:
        await redis_client.aclose()

if __name__ == "__main__":
    asyncio.run(main())