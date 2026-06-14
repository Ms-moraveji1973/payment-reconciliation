import redis.asyncio as redis
import json
import time
import logging
import uuid
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



@dataclass
class Message:
    id: str
    stream: str
    data: dict
    attempts: int = 0
    created_at: float = None
    processed_at: float = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


class MessageQueue:
    def __init__(self, redis_client:redis.Redis,
                 stream_name: str = 'pending_orders_queue',
                 max_len: int = 100000):
        self.redis = redis_client
        self.stream_name = stream_name
        self.max_len = max_len

    async def enqueue(self, data:dict):
        payload = {
            "data" : json.dumps(data),
            "created_at" : str(time.time()),
            "attempts" : "0"
        }
        msg_id = await self.redis.xadd(self.stream_name, payload, id="*", maxlen=self.max_len, approximate=True)
        logging.info(f"Enqueued message: {msg_id}")
        return msg_id


class ConsumerGroup:
    def __init__(self, queue: MessageQueue, group_name:str, consumer_name:str):
        self.queue = queue
        self.redis = queue.redis
        self.stream_name = queue.stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name or f"consumer-{uuid.uuid4().hex[:8]}"

    @classmethod
    async def create(cls, queue:MessageQueue, group_name: str, consumer_name: str):
        self = cls(queue, group_name, consumer_name)
        await self._ensure_group()
        return self

    async def _ensure_group(self):
        try:
            await self.redis.xgroup_create(
                self.stream_name,
                self.group_name,
                id='0',
                mkstream=True
            )
            logger.info(f"Created consumer group: {self.group_name}")

        except redis.ResponseError as e:
            if 'BUSYGROUP' not in str(e):
                raise
            logger.debug(f"Consumer group already exists: {self.group_name}")


    async def read_messages(self, count:int = 10, block_ms:int = 5000):
        result = await self.redis.xreadgroup(
            self.group_name,
            self.consumer_name,
            {self.stream_name: '>'},
            count=count,
            block=block_ms
        )

        if not result:
            return []

        messages = []
        for stream, stream_messages in result:
            for msg_id, fields in stream_messages:
                messages.append(Message(
                    id=msg_id,
                    stream=stream,
                    data=json.loads(fields.get('data', '{}')),
                    attempts=int(fields.get('attempts', 0)),
                    created_at=float(fields.get('created_at', time.time()))
                ))

        return messages


    async def acknowledge(self, message_id: int) -> int :
        result = await self.redis.xack(self.stream_name, self.group_name, message_id)
        if result:
            logger.debug(f"Acknowledged: {message_id}")
        return result > 0


    async def acknowledge_batch(self, message_ids: list[str]) -> int:
        if not message_ids:
            return 0
        return await self.redis.xack(self.stream_name, self.group_name, *message_ids)