from pydantic import BaseModel
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError, MultipleResultsFound, NoResultFound
from sqlalchemy.orm import joinedload
import redis.asyncio as redis
from redis.exceptions import RedisError
from sqlalchemy import select
import random
import re
# internal
from .models import Order, PaymentIntent, OrderStatus, SMSTransaction
from app.modules.users.models import User
from .queue_manager import MessageQueue


async def create_order_service(user:User, amount:int, session: AsyncSession, redis_client: redis.Redis):
    unique_amount = await get_unique_amount(session, redis_client)
    print("-------------- unique amount equals : ", unique_amount)
    if not unique_amount :
        amounts_count = await redis_client.scard("amounts")
        free_amounts_count = await redis_client.scard("free_amounts")
        print(f"--------------------- amounts count: {amounts_count}")
        print(f"--------------------- free_amounts count: {free_amounts_count}")
        return None
    try:
        new_order = Order(user_id=user.id, amount=amount,
                        payment_intent=PaymentIntent(status=OrderStatus.PENDING,
                                                    base_amount=amount,
                                                    exact_amount=unique_amount ))
        session.add(new_order)
        try :
            await session.flush()
            return new_order
        except IntegrityError :
            await redis_client.smove("pending_orders","amounts",unique_amount)
            await redis_client.sadd("free_amounts",unique_amount)
            raise ValueError("Duplicate unique amount")
    except Exception:
        await redis_client.smove("pending_orders","amounts",unique_amount)
        await redis_client.sadd("free_amounts",unique_amount)
        raise ValueError("Database error occurred ")



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
        raise ValueError("Order not found")
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


async def get_unique_amount(session: AsyncSession, redis_client:redis.Redis) -> int | None :
    try:
        given_amount = await redis_client.spop('free_amounts')
        print(f"-------- {given_amount} was taken from free_amounts")
        if given_amount :
            await redis_client.smove("amounts",'pending_orders',given_amount)
            return int(given_amount)
    except RedisError :
        fallback_amount = await get_unique_amount_from_postgres(session)
        print("------------- fallback is called ------------")
        return fallback_amount
    return None



async def get_unique_amount_from_postgres(session: AsyncSession):
    pending_orders = set()
    async for orders in get_all_pending_orders(session) :
        for order in orders:
            pending_orders.add(order.exact_amount)
    possible_range = set(range(min(pending_orders), max(pending_orders)+200))
    available_amounts = list(possible_range - pending_orders)
    unique_amount = random.choice(available_amounts)
    if unique_amount:
        print("--------------------- unique amount from postgres is :", unique_amount)
        return unique_amount
    return None



async def process_pending_payment(amount:int, session: AsyncSession):
    stmt = select(PaymentIntent).with_for_update(nowait=False).options(joinedload(PaymentIntent.order, innerjoin=True)).where(PaymentIntent.exact_amount == amount, PaymentIntent.status == OrderStatus.PENDING)
    payment_result = await session.execute(stmt)
    try:
        payment = payment_result.scalar_one()
        payment.status = OrderStatus.PAID
        payment.order.status = OrderStatus.PAID
        await session.flush()
        print("---------------- the transaction amount has been found and changed the status to PAID ----------")
        return payment

    except NoResultFound:
        raise ValueError("NoResultFound")

    except MultipleResultsFound:
        raise ValueError("MultipleResultsFound")


async def create_transaction_service(sms_data: dict, session:AsyncSession):
    amount = sms_data["sms_amount"]
    inventory = sms_data["sms_inventory"]
    date = sms_data["sms_date"]
    time = sms_data["sms_time"]
    webhook_payload = sms_data["webhook_payload"]
    if None in (amount, inventory, date, time, webhook_payload):
        raise ValueError("error_missing_fields")
    new_transaction = SMSTransaction(sms_amount=amount, sms_inventory=inventory, sms_date=date, sms_time=time, webhook_payload=webhook_payload)
    session.add(new_transaction)
    try:
        await session.flush()
        return new_transaction
    except IntegrityError:
        raise ValueError("error_duplicate_transaction")


async def handle_transaction_service(sms_data: dict, session:AsyncSession, redis_client: redis.Redis):
    transaction = await create_transaction_service(sms_data, session)
    await session.commit()
    amount = transaction.sms_amount
    transaction_id = transaction.id
    transaction_amount = {"amount":amount}
    process_pending_queue = MessageQueue(redis_client, stream_name="pending_orders_queue")
    message_id = await process_pending_queue.enqueue(transaction_amount)
    #get_pending_payment = await process_pending_payment(amount, session)
    #await session.commit()
    print(f"------------- {amount} was moved to free_amount --------------- ")
    #return get_pending_payment
    return {
        "status" : "accepted",
        "redis_message_id" : message_id,
        "transaction_id":transaction_id
    }





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

    return {
        "sms_amount": amount,
        "sms_inventory": inventory,
        "sms_date": raw_date,
        "sms_time": raw_time,
        "webhook_payload": content
    }