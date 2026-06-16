import redis.asyncio as redis
import json
import time
import uuid
from dataclasses import dataclass
from app.core.logger import log as logger

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
        self.dead_letter_stream = f"{self.stream_name}:dead"

    async def enqueue(self, data:dict):
        payload = {
            "data" : json.dumps(data),
            "created_at" : str(time.time()),
            "attempts" : "0"
        }
        msg_id = await self.redis.xadd(self.stream_name, payload, id="*", maxlen=self.max_len, approximate=True)
        logger.info(f"Enqueued message: {msg_id}")
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
        instance = cls(queue, group_name, consumer_name)
        await instance._ensure_group()
        return instance

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
            logger.info(f"Consumer group already exists: {self.group_name}")


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
            logger.info(f"Acknowledged: {message_id}")
        return result > 0


    async def acknowledge_batch(self, message_ids: list[str]) -> int:
        if not message_ids:
            return 0
        return await self.redis.xack(self.stream_name, self.group_name, *message_ids)


    async def get_pending(self, count: int = 100):
        pending = await self.redis.xpending_range(self.stream_name, self.group_name, "-", "+", count=count)
        return pending

    async def get_pending_summary(self):
        info = await self.redis.xpending(self.stream_name, self.group_name)
        return {
            'count': info['pending'],
            'min_id': info['min'],
            'max_id': info['max'],
            'by_consumer': info['consumers']
        }


    async def claim_stale_message(self, min_idle_time=6000, count: int = 10):
        pending = await self.redis.xpending_range(self.stream_name, self.group_name, "-", "+", count=count)
        claimed = []
        for entry in pending:
            if entry["time_since_delivered"] >= min_idle_time:
                real_delivery_count = entry['times_delivered']
                result = await self.redis.xclaim(self.stream_name, self.group_name,
                                                self.consumer_name, min_idle_time,
                                                [entry['message_id']])
                for msg_id, fields in result:
                    claimed.append(Message(
                        id=msg_id,
                        stream=self.stream_name,
                        data=json.loads(fields.get('data', '{}')),
                        attempts=real_delivery_count,
                        created_at=float(fields.get('created_at', time.time()))
                    ))
        if claimed:
            logger.info(f" Claimed {len(claimed)} stale message")

        return claimed