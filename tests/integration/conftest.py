"""
Integration Test Fixtures and Infrastructure Configuration.

Provides real PostgreSQL and real Redis connections for integration tests,
schema initialization, transaction isolation, and deterministic cleanup.
"""
import os
import asyncio
import pytest
import pytest_asyncio
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.pool import NullPool
from sqlalchemy import text

from app.db.base import Base
from app.db.database import SessionLocal
# Ensure all models are registered with Base.metadata
import app.modules.users.models
import app.modules.order.models

# Integration test environment settings (fallback to standard local test instances)
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://muhammad:Qwertyuiop@localhost:5432/payment_test"
)
TEST_REDIS_HOST = os.getenv("TEST_REDIS_HOST", "localhost")
TEST_REDIS_PORT = int(os.getenv("TEST_REDIS_PORT", 6379))
TEST_REDIS_DB = int(os.getenv("TEST_REDIS_DB", 9))  # DB 9 reserved for integration tests


@pytest.fixture(scope="session")
def test_engine():
    """
    Session-scoped SQLAlchemy engine bound to the dedicated test database.
    Uses NullPool to avoid connection pooling issues across per-test event loops.
    Reconfigures the application's global SessionLocal to target this engine.
    """
    engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool, echo=False)
    
    # Reconfigure the application's global SessionLocal factory so worker tasks
    # and background handlers automatically interact with the real test database.
    SessionLocal.configure(bind=engine)
    
    # Initialize database tables once per test session
    async def _init_schema():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    asyncio.run(_init_schema())
    
    yield engine
    
    asyncio.run(engine.dispose())


@pytest_asyncio.fixture
async def db_session(test_engine):
    """
    Function-scoped AsyncSession for test setup and assertions.
    Ensures table truncation before and after every test for complete isolation.
    """
    truncate_sql = text(
        "TRUNCATE TABLE payment_intent, orders, outbox_order_event, "
        "sms_transactions, refresh_tokens, users CASCADE;"
    )
    
    # Clean database before running the test
    async with test_engine.begin() as conn:
        await conn.execute(truncate_sql)
        
    session_factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    async with session_factory() as session:
        yield session

    # Clean database after running the test
    async with test_engine.begin() as conn:
        await conn.execute(truncate_sql)


@pytest_asyncio.fixture
async def real_redis():
    """
    Function-scoped real Redis client connected to the dedicated test Redis DB.
    Flushes the test database before and after each test for deterministic state.
    """
    client = redis.Redis(
        host=TEST_REDIS_HOST,
        port=TEST_REDIS_PORT,
        db=TEST_REDIS_DB,
        decode_responses=True
    )
    
    # Flush test DB before test
    await client.flushdb()
    
    yield client
    
    # Flush and close connection after test
    await client.flushdb()
    await client.aclose()
