from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, HTTPException
from typing import Annotated
import redis.asyncio as redis
from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy.orm import joinedload

from app.db.redis_db import get_redis
from app.modules.users.models import User
from app.modules.order.models import PaymentIntent
from app.modules.users.security import get_current_user
from app.modules.order.queue_manager import MessageQueue
from .dlq_manager import DeadLetterQueue
from app.core.logger import log
from app.modules.order.schema import OrderStatus


async def get_dead_letter_queue(redis_client:Annotated[redis.Redis,Depends(get_redis)]):
    queue = MessageQueue(redis_client)
    dlq = DeadLetterQueue(queue=queue)
    return dlq



async def get_current_admin_user(current_user:Annotated[User,Depends(get_current_user)]):
    if current_user and current_user.admin is True :
        return current_user
    raise HTTPException(status_code=403, detail="You can't access to the admin panel")


async def handle_pending_payment(order_id:int, payment_id:int, session: AsyncSession):
    stmt = select(PaymentIntent).with_for_update(nowait=False).options(joinedload(PaymentIntent.order, innerjoin=True)).where(PaymentIntent.id == payment_id, PaymentIntent.order_id == order_id,
                                                                                                                              PaymentIntent.status.in_([OrderStatus.PENDING, OrderStatus.FAILED]))
    payment_result = await session.execute(stmt)
    try:
        payment = payment_result.scalar_one()
        payment.status = OrderStatus.PAID
        payment.order.status = OrderStatus.PAID
        await session.flush()
        log.info("The order status has been changed to PAID by admin",status="PAID")
        return payment

    except NoResultFound:
        raise ValueError("NoResultFound")


