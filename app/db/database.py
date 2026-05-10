from sqlalchemy import text
from collections.abc import AsyncGenerator
from typing_extensions import Annotated
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession )

from ..core.config import get_settings

def get_database():
    setting = get_settings()
    return setting.DATABASE_URL

DATABASE_URL= get_database()
engine = create_async_engine(DATABASE_URL,echo=False,pool_pre_ping=True)

SessionLocal = async_sessionmaker(bind=engine,class_=AsyncSession,expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        try:
            yield session
        except:
            await session.rollback()
            raise
        finally:
            await session.close()


async def test_async_connection() -> bool:
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            value = result.scalar()
            print(f"✅ Database connection test passed (result: {value})")
            return True
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False