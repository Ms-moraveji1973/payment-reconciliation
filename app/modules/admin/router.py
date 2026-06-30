from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated

# internal package
from app.db.database import get_db
from app.modules.users.models import User
from .service import get_current_admin_user, get_dead_letter_queue
from .dlq_manager import DeadLetterQueue



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