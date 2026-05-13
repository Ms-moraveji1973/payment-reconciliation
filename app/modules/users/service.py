from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .models import User
from .schema import UserSchema

async def create_user_service(user_data:UserSchema,session:AsyncSession):
    get_user = await get_user_by_telegram_id(user_data.telegram_id,session)
    if get_user:
        raise ValueError("User already exists")
    user = User(telegram_id=user_data.telegram_id,username=user_data.username,name=user_data.name)
    session.add(user)
    await session.commit()
    await session.refresh(user)
    print('The user has been created successfully ')
    return user

async def get_user_by_telegram_id(telegram_id:int,session:AsyncSession):
    stmt = select(User).where(User.telegram_id == telegram_id)
    result =  await session.execute(stmt)
    user = result.scalar_one_or_none()
    return user


async def get_all_users(session:AsyncSession):
    stmt = select(User)
    result = await session.execute(stmt)
    users = result.scalars().all()
    return users