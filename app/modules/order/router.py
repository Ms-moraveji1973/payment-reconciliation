from fastapi import APIRouter , Depends , status , HTTPException
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
# internal
from app.modules.users.service import get_user_by_telegram_id
from app.db.database import get_db
from .schema import OrderSchema , OrderResponseSchema
from .service import create_order_service , get_all_orders , delete_order_service , get_order_service

router = APIRouter(prefix="/orders", tags=["orders"])

@router.post("/create-order",response_model=OrderResponseSchema,status_code=status.HTTP_201_CREATED)
async def create_order(order_data:OrderSchema,session:AsyncSession = Depends(get_db)):
    user = await get_user_by_telegram_id(order_data.telegram_id,session)
    if not user :
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="User not found")
    try :
        order = await create_order_service(user,order_data.amount,session)
        await session.commit()
        await session.refresh(order)
        return order

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail="Internal server error")


@router.get("/get-order/{order_id}",response_model=OrderResponseSchema,status_code=status.HTTP_200_OK)
async def get_order(order_id:int,user_id:int,session:AsyncSession = Depends(get_db)):
    order = await get_order_service(order_id,user_id,session)
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="Order not found")
    return order



@router.get("/all-orders",response_model=list[OrderResponseSchema],status_code=status.HTTP_200_OK)
async def get_orders(session:AsyncSession = Depends(get_db)):
    orders = await get_all_orders(session)
    return orders

@router.delete("/delete/{order_id}",status_code=status.HTTP_204_NO_CONTENT)
async def delete_order(order_id:int,user_id:int,session:AsyncSession = Depends(get_db)):
    try :
        await delete_order_service(order_id,user_id,session)
        await session.commit()
    except ValueError as v:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,detail="Order not found")
    return None
