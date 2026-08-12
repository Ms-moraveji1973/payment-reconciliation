import pytest
import fakeredis.aioredis
import pytest_asyncio

from unittest.mock import AsyncMock, MagicMock
from app.modules.users.models import User

@pytest_asyncio.fixture
async def fake_redis():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)

    yield client

    await client.flushall()
    await client.aclose()


@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.add = MagicMock()
    return session

@pytest.fixture
def fake_user_data():
    return User(
        id=1241,
        telegram_id=182614114,
        name="mohammad reza",
        is_active=True
    )
