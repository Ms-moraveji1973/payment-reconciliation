from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from redis.exceptions import RedisError
import redis.asyncio as redis

from app.core.logger import log
from app.db.database import SessionLocal
from .models import OutBoxOrderEvent, EventStatus
from .queue_manager import MessageQueue
from .execptions import OutBoxEventRedisException

async def process_outbox_event(redis_client: redis.Redis):
    async with SessionLocal() as session:
        try:
            query = select(OutBoxOrderEvent).where(OutBoxOrderEvent.status == EventStatus.PENDING).limit(100).with_for_update(skip_locked=True)
            result = await session.execute(query)
            events = result.scalars().all()
            for event in events:
                events_data = event.data
                log.info(f"outbox event data found {events_data}")
                process_pending_queue = MessageQueue(redis_client, stream_name="pending_orders_queue")
                await process_pending_queue.enqueue(events_data)
                log.info(f"the message order :{events_data['amount']} was sent to queue successfully ")
                event.status = EventStatus.PUBLISHED
                await session.commit()
                log.info(f"the message order :{events_data['amount']} changed status to PUBLISHED")


        except RedisError as re:
            redis_error = str(re)
            log.error(f"redis error at sending message to pending order queue : {redis_error}")
            raise OutBoxEventRedisException("redis_error")


        except Exception as e:
            error_msg = str(e)
            log.error(error_msg)
            raise ValueError("unknown_error")
