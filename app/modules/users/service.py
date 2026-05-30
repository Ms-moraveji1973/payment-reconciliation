from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete
from sqlalchemy.exc import IntegrityError
from typing import Optional
from datetime import datetime, timedelta, timezone
import uuid

from .models import User , RefreshToken
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



async def create_refresh_token_record_service(session:AsyncSession,user_id:int, jti:str, expired_at:datetime, family_id: uuid.UUID | None = None) -> RefreshToken:
    refresh_token = RefreshToken(user_id=user_id, token_hash=jti, expires_at=expired_at, family_id=family_id or uuid.uuid4())
    session.add(refresh_token)
    try :
        await session.flush()
        return refresh_token
    except IntegrityError:
        return None


async def validate_and_revoke_refresh_token_service(session:AsyncSession,jti:str):
    stmt = (update(RefreshToken).where(RefreshToken.token_hash == jti,RefreshToken.expires_at > datetime.now(timezone.utc),
                                       RefreshToken.is_revoked == False)
                                .values(is_revoked = True)
                                .returning(RefreshToken))
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_refresh_token(session:AsyncSession,jti:str):
    stmt = select(RefreshToken).where(RefreshToken.token_hash == jti)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()



async def revoke_refresh_token(session:AsyncSession,jti:str):
    stmt = update(RefreshToken).where(RefreshToken.token_hash == jti).values(is_revoked=True)
    result = await session.execute(stmt)
    try :
        await session.flush()
    except IntegrityError:
        raise ValueError("Error at revoking refresh token")
    return result


async def revoke_family_token(session : AsyncSession, family_id : uuid.UUID):
    stmt = update(RefreshToken).where(RefreshToken.family_id == family_id).values(is_revoked=True)
    revoke_rt = await session.execute(stmt)
    try :
        await session.flush()
    except IntegrityError:
        raise ValueError("Error at revoking family_id refresh token ")
    return revoke_rt


async def revoke_all_tokens(session : AsyncSession, user_id : int):
    stmt = delete(RefreshToken).where(RefreshToken.user_id == user_id)
    result = await session.execute(stmt)
    try :
        await session.flush()
    except IntegrityError:
        raise ValueError("Error at deleting all refresh tokens")
    return result