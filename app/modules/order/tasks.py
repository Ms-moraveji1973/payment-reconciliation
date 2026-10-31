from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from redis.exceptions import RedisError
import redis.asyncio as redis
import random
from datetime import datetime, timedelta, timezone


from app.core.logger import log
from app.db.database import SessionLocal
from .models import OutBoxOrderEvent, EventStatus
from .queue_manager import MessageQueue
from .execptions import OutBoxEventRedisException, InvalidOutboxPayload



async def claim_process_events(limit_s: int = 100) -> list[dict]:
    async with SessionLocal() as session:
        async with session.begin():
            now = datetime.now(timezone.utc)
            query = select(OutBoxOrderEvent).where( (OutBoxOrderEvent.status == EventStatus.PENDING) | ( (OutBoxOrderEvent.status == EventStatus.PROCESSING) & (OutBoxOrderEvent.locked_at < now - timedelta(minutes=5) ) ) ).limit(limit_s).with_for_update(skip_locked=True)
            result = await session.execute(query)
            events = result.scalars().all()

            for event in events:
                now = datetime.now(timezone.utc)
                events_data = event.data
                log.info(f"outbox events data found {events_data}")
                event.status = EventStatus.PROCESSING
                event.locked_at = now
                log.info(f"the message order :{events_data['amount']} changed status to PROCESSING")

            return [
                {
                    "id": event.id,
                    "data": event.data,
                    "retry_count": event.retry_count if event.retry_count >= 0 else None
                }
                for event in events
            ]



def validate_event_data(data: object) -> dict:
    if not isinstance(data, dict):
        raise InvalidOutboxPayload("Payload must be a dictionary")

    if "amount" not in data:
        raise InvalidOutboxPayload("Missing amount")

    amount = data["amount"]

    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise InvalidOutboxPayload("Amount must be numeric")

    if amount <= 0:
        raise InvalidOutboxPayload("Amount must be positive")

    return data


async def update_outbox_event(event_id, *, status, retry_count=None, last_error=None):
    async with SessionLocal() as session:
        async with session.begin():
            event = await session.get(OutBoxOrderEvent, event_id)

            if event is None:
                raise LookupError(f"Outbox event not found: {event_id}")

            event.status = status

            if retry_count is not None:
                event.retry_count = retry_count

            event.last_error = last_error

MAX_OUTBOX_RETRIES = 5
async def process_single_outbox_event(redis_client, event):
    event_id = event["id"]
    data = event["data"]
    retry_count = event["retry_count"]
    queue = MessageQueue(redis_client)

    try:

        data = validate_event_data(data)

    except InvalidOutboxPayload as exc:
        await queue.enqueue_dead_letter(
            data=data,
            event_id=event_id,
            reason=str(exc),
        )

        await update_outbox_event(
            event_id,
            status=EventStatus.FAILED,
            last_error=str(exc),
        )
        return

    try:
        await queue.enqueue(data)

    except RedisError as exc:
        attempts = (retry_count or 0) + 1
        if attempts >= MAX_OUTBOX_RETRIES :
            log.critical(f"Outbox event {event_id} exceeded max retries ({MAX_OUTBOX_RETRIES}). Moving to FAILED.", event_id=event_id, error=str(exc))
            await update_outbox_event(
                event_id,
                status=EventStatus.FAILED,
                retry_count=attempts,
                last_error=f"Max retries ({MAX_OUTBOX_RETRIES}) reached. Last error: {exc}",
            )

        else:
            await update_outbox_event(
                event_id,
                status=EventStatus.PENDING,
                retry_count=attempts,
                last_error=str(exc),
            )
        return

    await update_outbox_event(
        event_id,
        status=EventStatus.PUBLISHED,
        last_error=None,
    )

async def process_outbox_event(redis_client: redis.Redis):
    events = await claim_process_events(100)
    for event in events:
        await process_single_outbox_event(redis_client, event)