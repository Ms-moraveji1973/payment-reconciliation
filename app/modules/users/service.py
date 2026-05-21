from jinja2.lexer import OptionalLStrip
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from typing import Optional

from .models import User
from .schema import UserTelegramSchema

async def create_telegram_user_service(user_data:UserTelegramSchema,session:AsyncSession) -> User:
    user = User(telegram_id=user_data.telegram_id,username=user_data.username,name=user_data.name)
    session.add(user)
    try :
        await session.flush()
    except IntegrityError:
        raise ValueError("User already exists")
    return user

async def get_user_by_telegram_id(telegram_id:int,session:AsyncSession) -> User | None:
    stmt = select(User).where(User.telegram_id == telegram_id)
    result =  await session.execute(stmt)
    user = result.scalar_one_or_none()
    return user


async def register_user_service(username:str, password:str, name:str, session:AsyncSession) -> User:
    from .security import hash_password
    hashed_password = hash_password(password)
    new_user = User(username=username,hashed_password=hashed_password,name=name)
    session.add(new_user)
    try :
        await session.flush()
    except IntegrityError:
        raise ValueError("username already exists")
    return new_user


async def get_user_by_username(username:str,session:AsyncSession) -> User | None:
    stmt = select(User).where(User.username == username)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()
    return user


async def get_all_users(cursor: Optional[int], limit: int, session: AsyncSession) -> tuple[list[User],Optional[int],bool]:
    stmt = select(User).order_by(User.id)
    if cursor is not None :
        stmt = stmt.where(User.id > cursor)
    result = await session.execute(stmt.limit(limit + 1))
    users = result.scalars().all()
    if has_more := len(users) > limit:
        users = users[:limit]
    next_cursor = users[-1].id if has_more else None
    return users, next_cursor, has_more



