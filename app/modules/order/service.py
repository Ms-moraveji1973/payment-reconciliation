from pydantic import BaseModel
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload
import redis.asyncio as redis
from sqlalchemy import select

# internal
from .models import Order, PaymentIntent, OrderStatus
from app.modules.users.models import User


async def create_order_service(user:User, amount:int, session: AsyncSession, redis_client: redis.Redis):
    unique_amount = await get_unique_amount(session, redis_client)
    print("-------------- unique amount equals : ", unique_amount)
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
            raise ValueError("Something has been occurred")
    except Exception:
        await redis_client.smove("pending_orders","amounts",unique_amount)
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
    given_amount = await redis_client.spop('free_amounts')
    if given_amount :
        await redis_client.smove("amounts",'pending_orders',given_amount)
        return int(given_amount)
    return None
