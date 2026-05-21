from pydantic import BaseModel
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# internal
from .models import Order
from app.modules.users.models import User


async def create_order_service(user:User,amount:float,session: AsyncSession):
    order = Order(user_id=user.id,amount=amount)
    session.add(order)
    return order



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



async def get_all_orders(session:AsyncSession) -> list[Order]:
    stmt = select(Order)
    result = await session.execute(stmt)
    orders = result.scalars().all()
    return orders