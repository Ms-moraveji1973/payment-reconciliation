from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated, Optional

# internal package
from app.db.database import get_db
from app.modules.users.models import User
from app.modules.order.service import get_pending_orders
from .service import get_current_admin_user, get_dead_letter_queue, handle_pending_payment
from .dlq_manager import DeadLetterQueue
from .schema import PaginatedOrdersResponse
from app.core.logger import log


router = APIRouter(prefix='/admin', tags=["admin"])



@router.get('/dlq-dashboard')
async def dlq_dashboard(current_user:Annotated[User,Depends(get_current_admin_user)], dlq:Annotated[DeadLetterQueue, Depends(get_dead_letter_queue)],cursor:str = Query('-')):
    messages = await dlq.get_dead_letters(cursor=cursor)
    return {
        "messages":messages['messages'],
        "next_cursor": messages['next_cursor']
        }



@router.post('/replay-dlq/{message_id}')
async def replay_dlq_dashboard(message_id:str,current_user:Annotated[User,Depends(get_current_admin_user)], dlq:Annotated[DeadLetterQueue, Depends(get_dead_letter_queue)]):
    replay_message = await dlq.replay_message(message_id)
    if not replay_message:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="There is no result")
    return replay_message


@router.get('/users-orders', response_model=PaginatedOrdersResponse, status_code=status.HTTP_200_OK)
async def get_all_users_orders(session: Annotated[AsyncSession, Depends(get_db)], current_user: Annotated[User, Depends(get_current_admin_user)], cursor: Optional[int] = Query(None), limit : int = Query(20, ge=1, le=100)):
    orders = await get_pending_orders(session, limit, cursor)
    return orders


@router.patch('/order/{order_id}/payment/{payment_id}/confirm', status_code=status.HTTP_200_OK)
async def confirm_payment(order_id: int , payment_id: int, session: Annotated[AsyncSession, Depends(get_db)],current_user: Annotated[User, Depends(get_current_admin_user)]):
    try :
        update_payment = await handle_pending_payment(order_id, payment_id, session)
        await session.commit()
        return update_payment
    except ValueError as v:
        error = str(v)
        await session.rollback()

        if error == "NoResultFound":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=error)

    except Exception:
        await session.rollback()
        log.error("Unexpected error in payment")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
