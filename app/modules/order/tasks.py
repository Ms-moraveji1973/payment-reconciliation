from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as redis
from redis.exceptions import RedisError
from sqlalchemy import bindparam, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import log
from app.db.database import get_session
from .exceptions import InvalidOutboxPayload
from .models import EventStatus, OutBoxOrderEvent
from .queue_manager import MessageQueue

MAX_OUTBOX_RETRIES = 5
LOCK_TIMEOUT_MINUTES = 5


async def claim_process_events(
    session: AsyncSession,
    limit_s: int = 100,
) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    stale_lock = now - timedelta(minutes=LOCK_TIMEOUT_MINUTES)

    async with session.begin():
        cte_query = (
            select(OutBoxOrderEvent.id)
            .where(
                (OutBoxOrderEvent.status == EventStatus.PENDING)
                | (
                    (OutBoxOrderEvent.status == EventStatus.PROCESSING)
                    & (OutBoxOrderEvent.locked_at < stale_lock)
                )
            )
            .order_by(OutBoxOrderEvent.created_at.asc())
            .limit(limit_s)
            .with_for_update(skip_locked=True)
            .cte("claimed_cte")
        )

        stmt = (
            update(OutBoxOrderEvent)
            .where(OutBoxOrderEvent.id == cte_query.c.id)
            .values(
                status=EventStatus.PROCESSING,
                locked_at=now,
            )
            .returning(
                OutBoxOrderEvent.id,
                OutBoxOrderEvent.data,
                OutBoxOrderEvent.retry_count,
            )
        )

        result = await session.execute(stmt)
        return [dict(row._mapping) for row in result.all()]


def validate_event_data(data: object) -> dict[str, Any]:
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


async def process_single_outbox_event(
    queue: MessageQueue,
    event: dict[str, Any],
) -> dict[str, Any]:
    event_id = event["id"]
    data = event["data"]
    retry_count = event["retry_count"] or 0

    try:
        validated_data = validate_event_data(data)
    except InvalidOutboxPayload as exc:
        try:
            await queue.enqueue_dead_letter(
                data=data,
                event_id=event_id,
                reason=str(exc),
            )
        except RedisError as redis_exc:
            log.error(
                "Failed to enqueue dead letter event",
                event_id=event_id,
                error=str(redis_exc),
            )
            attempts = retry_count + 1
            if attempts >= MAX_OUTBOX_RETRIES:
                return {
                    "id": event_id,
                    "status": EventStatus.FAILED,
                    "retry_count": attempts,
                    "last_error": f"DLQ Redis error after max retries: {redis_exc}",
                }
            return {
                "id": event_id,
                "status": EventStatus.PENDING,
                "retry_count": attempts,
                "last_error": f"Validation failed ({exc}) + DLQ Redis error: {redis_exc}",
            }

        return {
            "id": event_id,
            "status": EventStatus.FAILED,
            "retry_count": retry_count,
            "last_error": str(exc),
        }

    try:
        await queue.enqueue(validated_data)
    except RedisError as exc:
        attempts = retry_count + 1
        if attempts >= MAX_OUTBOX_RETRIES:
            log.critical(
                f"Outbox event {event_id} exceeded max retries ({MAX_OUTBOX_RETRIES}). Moving to FAILED.",
                event_id=event_id,
                error=str(exc),
            )
            return {
                "id": event_id,
                "status": EventStatus.FAILED,
                "retry_count": attempts,
                "last_error": f"Max retries ({MAX_OUTBOX_RETRIES}) reached. Last error: {exc}",
            }

        return {
            "id": event_id,
            "status": EventStatus.PENDING,
            "retry_count": attempts,
            "last_error": str(exc),
        }

    return {
        "id": event_id,
        "status": EventStatus.PUBLISHED,
        "retry_count": retry_count,
        "last_error": None,
    }


async def process_outbox_updates(
    session: AsyncSession,
    updates: list[dict[str, Any]],
) -> None:
    if not updates:
        return

    published_ids = [
        item["id"]
        for item in updates
        if item["status"] == EventStatus.PUBLISHED
    ]
    error_updates = [
        item
        for item in updates
        if item["status"] != EventStatus.PUBLISHED
    ]

    async with session.begin():
        if published_ids:
            publish_stmt = (
                update(OutBoxOrderEvent)
                .where(OutBoxOrderEvent.id.in_(published_ids))
                .values(
                    status=EventStatus.PUBLISHED,
                    locked_at=None,
                    last_error=None,
                )
            )
            await session.execute(publish_stmt)

        if error_updates:
            error_stmt = (
                update(OutBoxOrderEvent)
                .values(
                    status=bindparam("status"),
                    retry_count=bindparam("retry_count"),
                    last_error=bindparam("last_error"),
                    locked_at=None,
                )
            )
            await session.execute(error_stmt, error_updates)


async def process_outbox_event(redis_client: redis.Redis) -> int:
    async with get_session() as session:
        events = await claim_process_events(session, limit_s=100)

    if not events:
        return 0

    queue = MessageQueue(redis_client)
    updates: list[dict[str, Any]] = []

    for event in events:
        result = await process_single_outbox_event(queue, event)
        updates.append(result)

    async with get_session() as session:
        await process_outbox_updates(session, updates)

    return len(events)