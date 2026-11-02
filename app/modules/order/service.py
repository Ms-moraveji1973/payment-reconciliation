from pydantic import BaseModel
from typing import Annotated
from enum import Enum
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, MultipleResultsFound, NoResultFound
from sqlalchemy.orm import joinedload
import redis.asyncio as redis
from redis.exceptions import RedisError
from sqlalchemy import select, text
import random
import re
import uuid
# internal
from .models import Order, PaymentIntent, OrderStatus, SMSTransaction, OutBoxOrderEvent
from app.modules.users.models import User
from app.core.logger import log
from app.core.exceptions import DatabaseError
from .queue_manager import MessageQueue
from .exceptions import (
    OutBoxEventRedisException,
    OrderNotFoundError,
    DuplicateUniqueAmountError,
    PaymentIntentNotFoundError,
    DuplicatePaymentIntentError,
    InvalidSMSDataError,
    DuplicateSMSTransactionError,
    DuplicateOutboxEventError,
    DuplicatePaymentTransactionError
)


class AmountSource(str, Enum):
    REDIS = "REDIS"
    POSTGRES = "POSTGRES"


class AllocatedAmount(int):
    source: AmountSource

    def __new__(cls, value: int, source: AmountSource = AmountSource.REDIS):
        instance = super().__new__(cls, value)
        instance.source = source
        return instance


async def create_order_service(
    user: User,
    amount: int,
    session: AsyncSession,
    redis_client: redis.Redis,
    max_retries: int = 3
):
    for attempt in range(max_retries):
        unique_amount = await get_unique_amount(session, redis_client, base_amount=amount)
        log.info(f"unique amount equals : {unique_amount}")
        if not unique_amount:
            try:
                amounts_count = await redis_client.scard("amounts")
                free_amounts_count = await redis_client.scard("free_amounts")
                log.info(f"amounts count: {amounts_count}")
                log.info(f" free_amounts count: {free_amounts_count}")
            except Exception:
                log.warning("could not retrieve redis amounts statistics")
            return None

        source = getattr(unique_amount, "source", AmountSource.REDIS)

        try:
            async with session.begin_nested():
                new_order = Order(
                    user_id=user.id,
                    amount=amount,
                    payment_intent=PaymentIntent(
                        status=OrderStatus.PENDING,
                        base_amount=amount,
                        exact_amount=int(unique_amount)
                    )
                )
                session.add(new_order)
                await session.flush()
            return new_order

        except IntegrityError as exc:
            # Compensate only if amount came from Redis
            if source == AmountSource.REDIS:
                try:
                    await redis_client.smove("pending_orders", "amounts", unique_amount)
                    await redis_client.sadd("free_amounts", unique_amount)
                except Exception as r_err:
                    log.error(f"failed to compensate redis: {r_err}")
                raise DuplicateUniqueAmountError("Duplicate unique amount") from exc

            # PostgreSQL fallback path: retry on collision if attempts remain
            log.warning(
                f"Conflict on exact_amount {unique_amount} from postgres fallback (attempt {attempt + 1}/{max_retries})"
            )
            if attempt < max_retries - 1:
                continue
            raise DuplicateUniqueAmountError("Duplicate unique amount") from exc

        except Exception as exc:
            if source == AmountSource.REDIS:
                try:
                    await redis_client.smove("pending_orders", "amounts", unique_amount)
                    await redis_client.sadd("free_amounts", unique_amount)
                except Exception as r_err:
                    log.error(f"failed to compensate redis: {r_err}")
            raise DatabaseError("Database error occurred") from exc

    return None



async def get_order_service(order_id:int,user_id:int,session:AsyncSession):
    get_order = select(Order).where(Order.id == order_id , Order.user_id == user_id)
    result = await session.execute(get_order)
    order = result.scalars().first()
    return order


async def delete_order_service(order_id:int,user_id:int,session:AsyncSession):
    get_order_by_user = select(Order).where(Order.id == order_id , Order.user_id == user_id)
    result = await session.execute(get_order_by_user)
    order = result.scalars().first()
    if not order:
        raise OrderNotFoundError("Order not found")
    await session.delete(order)



async def get_all_orders(current_user:User, session:AsyncSession) -> list[Order]:
    stmt = select(Order).options(joinedload(Order.payment_intent)).where(Order.user_id == current_user.id)
    result = await session.execute(stmt)
    orders = result.scalars().all()
    return orders



async def get_all_pending_orders(session:AsyncSession, limit: int = 100):
    last_order = 0
    sort_order = select(PaymentIntent).where(PaymentIntent.status == OrderStatus.PENDING).order_by(PaymentIntent.id)
    while True:
        stmt = sort_order.where(PaymentIntent.id > last_order)
        result = await session.execute(stmt.limit(limit))
        orders = result.scalars().all()
        if not orders:
            break
        last_order = orders[-1].id
        yield orders

async def get_pending_orders(session:AsyncSession, limit: int, cursor:int):
    stmt = select(PaymentIntent).options(
                                    joinedload(PaymentIntent.order).joinedload(Order.user)
                                        ).where(PaymentIntent.status == OrderStatus.PENDING).order_by(PaymentIntent.id)
    if cursor:
        stmt = stmt.where(PaymentIntent.id > cursor)
    result = await session.execute(stmt.limit(limit))
    payment_intents = result.scalars().all()
    formatted_orders = []
    for pi in payment_intents:
        formatted_orders.append({
            "id": pi.order.id,
            "status": pi.order.status,
            "user": pi.order.user,
            "created_at": pi.order.created_at,
            "amount": pi.order.amount,
            "payment": pi
        })
    next_cursor = payment_intents[-1].id if payment_intents else None
    return {
        "next_cursor" : next_cursor,
        "orders" : formatted_orders
    }


async def get_unique_amount(
    session: AsyncSession,
    redis_client: redis.Redis,
    base_amount: int | None = None
) -> AllocatedAmount | None:
    try:
        given_amount = await redis_client.spop('free_amounts')
        log.info(f"{given_amount} was taken from free_amounts")
        if given_amount:
            await redis_client.smove("amounts", 'pending_orders', given_amount)
            return AllocatedAmount(int(given_amount), AmountSource.REDIS)
    except RedisError:
        fallback_amount = await get_unique_amount_from_postgres(session, base_amount=base_amount)
        log.warning("fallback is called ")
        if fallback_amount is not None:
            return AllocatedAmount(fallback_amount, AmountSource.POSTGRES)
        return None
    return None



async def get_unique_amount_from_postgres(
    session: AsyncSession,
    base_amount: int | None = None,
    window: int = 500
) -> int | None:
    if base_amount is None:
        start_val, end_val = 1500000, 1550000
    else:
        start_val, end_val = base_amount + 1, base_amount + window

    query = text(
        """
        SELECT candidate_amount
        FROM generate_series(CAST(:start_val AS integer), CAST(:end_val AS integer)) AS candidate_amount
        WHERE NOT EXISTS (
            SELECT 1
            FROM payment_intent
            WHERE status = 'PENDING'
              AND exact_amount = candidate_amount
        )
        ORDER BY random()
        LIMIT 1;
        """
    )
    result = await session.execute(query, {"start_val": start_val, "end_val": end_val})
    unique_amount = result.scalar_one_or_none()
    if unique_amount is not None:
        log.info(f"unique amount from postgres is : {unique_amount}")
        return int(unique_amount)
    return None



async def process_pending_payment(amount:int, sms_transaction_id: int, session: AsyncSession):
    if not sms_transaction_id:
        raise InvalidSMSDataError("transaction_id is required for payment processing")

    existing_stmt = select(PaymentIntent).where(PaymentIntent.sms_transaction_id == sms_transaction_id)
    existing_result = await session.execute(existing_stmt)
    already_processed = existing_result.scalar_one_or_none()
    if already_processed :
        log.info(f"SMS {sms_transaction_id} was already processed for order {already_processed.order_id}. Skipping.")
        return already_processed, False

    stmt = select(PaymentIntent).with_for_update(nowait=False).options(joinedload(PaymentIntent.order, innerjoin=True)).where(PaymentIntent.exact_amount == amount, PaymentIntent.status == OrderStatus.PENDING)
    payment_result = await session.execute(stmt)
    try:
        payment = payment_result.scalar_one()
        payment.status = OrderStatus.PAID
        payment.order.status = OrderStatus.PAID
        payment.sms_transaction_id = sms_transaction_id
        await session.flush()
        log.info("the transaction amount has been found and changed the status to PAID",status="transaction_found")
        return payment, True

    except NoResultFound as exc:
        raise PaymentIntentNotFoundError(f"Payment intent for amount {amount} not found") from exc

    except MultipleResultsFound as exc:
        raise DuplicatePaymentIntentError(f"Multiple payment intents found for amount {amount}") from exc

    except IntegrityError as exc:
        log.warning(f"Integrity conflict on SMS {sms_transaction_id}: already processed concurrently.")
        raise DuplicatePaymentTransactionError(f"SMS {sms_transaction_id} was processed concurrently") from exc

async def create_transaction_service(sms_data: dict, session:AsyncSession):
    amount = sms_data.get("sms_amount")
    inventory = sms_data.get("sms_inventory")
    date = sms_data.get("sms_date")
    time = sms_data.get("sms_time")
    webhook_payload = sms_data.get("webhook_payload")
    if None in (amount, inventory, date, time, webhook_payload):
        raise InvalidSMSDataError("error_missing_fields")
    new_transaction = SMSTransaction(sms_amount=amount, sms_inventory=inventory, sms_date=date, sms_time=time, webhook_payload=webhook_payload)
    session.add(new_transaction)
    try:
        await session.flush()
        return new_transaction
    except IntegrityError as exc:
        raise DuplicateSMSTransactionError("error_duplicate_transaction") from exc


async def create_outbox_event_service(session:AsyncSession, transaction_data: dict):
    if not transaction_data:
        raise InvalidSMSDataError("error_missing")
    new_event = OutBoxOrderEvent(data=transaction_data)
    session.add(new_event)
    try:
        await session.flush()
        return new_event
    except IntegrityError as exc:
        raise DuplicateOutboxEventError("error_duplicate_event") from exc



async def handle_transaction_service(sms_data: dict, session:AsyncSession, redis_client: redis.Redis):
    try:
        transaction = await create_transaction_service(sms_data, session)
        amount = transaction.sms_amount
        trace_id = sms_data["trace_id"]
        transaction_id = transaction.id
        transaction_data = {
            "transaction_id": transaction_id,
            "trace_id":trace_id,
            "amount":amount,
            "type":"payment"
            }
        await create_outbox_event_service(session, transaction_data)
        await session.commit()
        log.info("transaction and order event committed to database successfully")
        # a background job like celery should send message to redis queue from outbox_order_event

        '''
        process_pending_queue = MessageQueue(redis_client, stream_name="pending_orders_queue")
        message_id = await process_pending_queue.enqueue(transaction_data)
        '''

        return {
            "status" : "accepted",
            "transaction_id": transaction_id
        }

    except (InvalidSMSDataError, DuplicateSMSTransactionError, DuplicateOutboxEventError):
        await session.rollback()
        raise

    except IntegrityError as exc:
        await session.rollback()
        log.error("transaction and order event failed due to database constraint")
        raise DatabaseError("database_error") from exc




def parse_sms_transaction(content: str):
    persian_to_eng = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
    clean_text = content.translate(persian_to_eng)
    pattern = (
        r"(?P<amount>[\d,]+)\s*ریال به حساب شما نشست\..*?"
        r"موجودی:\s*(?P<inventory>[\d,]+)\s*ریال.*?"
        r"(?P<time>\d{2}:\d{2}).*?"
        r"(?P<date>\d{4}\.\d{2}\.\d{2})"
    )
    match = re.search(pattern, clean_text, re.DOTALL)
    if not match:
        return None
    data = match.groupdict()
    amount = int(data['amount'].replace(',', ''))
    inventory = int(data['inventory'].replace(',', ''))
    raw_time = data['time']
    raw_date = data['date']
    trace_id = f"tx_{uuid.uuid4().hex[:12]}"

    return {
        "trace_id":trace_id,
        "sms_amount": amount,
        "sms_inventory": inventory,
        "sms_date": raw_date,
        "sms_time": raw_time,
        "webhook_payload": content
    }