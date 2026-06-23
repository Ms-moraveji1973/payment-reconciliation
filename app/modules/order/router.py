from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis
from fastapi.concurrency import run_in_threadpool

# internal
from app.db.database import get_db
from app.modules.users.models import User
from app.modules.users.security import get_current_user
from app.db.redis_db import get_redis
from app.core.logger import log
from .schema import (OrderResponseSchema,
                        OrderSchema,
                        SmsWebhookPayload,
                        SmsWebhookPayloadResponse,
                    )

from .service import (
    create_order_service,
    delete_order_service,
    get_all_orders,
    get_order_service,
    parse_sms_transaction,
    handle_transaction_service
)
from app.modules.users.security import oauth2_scheme
from app.core.config import get_settings

config = get_settings()

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
    except HTTPException:
        raise
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



@router.post("/transaction", response_model=SmsWebhookPayloadResponse, status_code=status.HTTP_200_OK)
async def receive_transaction(sms_transaction:SmsWebhookPayload, token:Annotated[str, Depends(oauth2_scheme)], session:AsyncSession = Depends(get_db), redis_client: redis.Redis = Depends(get_redis)):
    if token == config.TOKEN_VERIFICATION :
        request_log = log.bind(payload=sms_transaction.model_dump())
        request_log.info("transaction")
        content = sms_transaction.content
        if not content:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="The content is required")
        sms_data = await run_in_threadpool(parse_sms_transaction,content)
        if not sms_data :
            request_log.warning("missing_content")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="The sms format isn't valid")
        try:
            result = await handle_transaction_service(sms_data, session, redis_client)
            return result
        except ValueError as v:
            await session.rollback()
            error_message = str(v)
            if error_message == "error_duplicate_transaction":
                return {
                        "status": "already_exists",
                        "detail": "The transaction data already exists"
            }

            elif error_message == "error_missing_fields":
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="The transaction data is incomplete")

            else:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal Error")
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="The TOKEN is invalid")


"""        elif error_message == "NoResultFound":
            raise HTTPException(status_code=status.HTTP_204_NO_CONTENT, detail="No transaction amount matched")

        elif error_message == "MultipleResultsFound":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="There are the same amounts in DB")
"""
