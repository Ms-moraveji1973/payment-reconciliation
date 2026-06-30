from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, HTTPException
from typing import Annotated
import redis.asyncio as redis

from app.db.redis_db import get_redis
from app.modules.users.models import User
from app.modules.users.security import get_current_user
from app.modules.order.queue_manager import MessageQueue
from .dlq_manager import DeadLetterQueue



async def get_dead_letter_queue(redis_client:Annotated[redis.Redis,Depends(get_redis)]):
    queue = MessageQueue(redis_client)
    dlq = DeadLetterQueue(queue=queue)
    return dlq



async def get_current_admin_user(current_user:Annotated[User,Depends(get_current_user)]):
    if current_user and current_user.admin is True :
        return current_user
    raise HTTPException(status_code=403, detail="You can't access to the admin panel")


