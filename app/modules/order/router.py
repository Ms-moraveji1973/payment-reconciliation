from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis
# internal
from app.db.database import get_db
from app.modules.users.models import User
from app.modules.users.security import get_current_user
from app.db.redis_db import get_redis

from .schema import OrderResponseSchema, OrderSchema
from .service import (
    create_order_service,
    delete_order_service,
    get_all_orders,
    get_order_service,
)

router = APIRouter(prefix="/orders", tags=["orders"])

@router.post("/create-order",response_model=OrderResponseSchema ,status_code=status.HTTP_201_CREATED)
async def create_order(order_data:OrderSchema, current_user: Annotated[User, Depends(get_current_user)], session:AsyncSession = Depends(get_db), redis_client: redis.Redis = Depends(get_redis)):
    try :
        create_order = await create_order_service(current_user, order_data.amount, session, redis_client)
        if not create_order :
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="No free amount is available ")

        await session.commit()
        await session.refresh(create_order, attribute_names=['payment_intent'])
        return create_order

    except ValueError:
        await session.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Duplicate unique amount")

    except Exception :
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error"
        )


@router.get("/get-order/{order_id}",response_model=OrderResponseSchema,status_code=status.HTTP_200_OK)
async def get_order(current_user: Annotated[User, Depends(get_current_user)],order_id:int,session:AsyncSession = Depends(get_db)):
    order = await get_order_service(order_id,current_user.id,session)
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="Order not found")
    return order



@router.get("/all-orders",response_model=list[OrderResponseSchema],status_code=status.HTTP_200_OK)
async def get_orders(current_user: Annotated[User, Depends(get_current_user)], session:AsyncSession = Depends(get_db)):
    orders = await get_all_orders(current_user, session)
    if not orders:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="There is no orders")
    return orders

@router.delete("/delete/{order_id}",status_code=status.HTTP_204_NO_CONTENT)
async def delete_order(current_user: Annotated[User, Depends(get_current_user)],order_id:int,session:AsyncSession = Depends(get_db)):
    try :
        await delete_order_service(order_id,current_user.id,session)
        await session.commit()
    except ValueError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="Order not found")
    return None
