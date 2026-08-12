import pytest
import asyncio
from unittest.mock import AsyncMock
from sqlalchemy.exc import IntegrityError
from app.modules.order.service import get_unique_amount, create_order_service, OrderStatus


@pytest.mark.asyncio
async def test_get_unique_amount_concurrency(fake_redis, mock_session):

    free_amounts = [str(1000 + i) for i in range(30)]
    await fake_redis.sadd('free_amounts', *free_amounts)
    await fake_redis.sadd('amounts', *free_amounts)

    tasks = [get_unique_amount(mock_session, fake_redis) for _ in range(50)]
    results = await asyncio.gather(*tasks)

    successful_amounts = [amount for amount in results if amount is not None]
    failed_requests = [amount for amount in results if amount is None]

    assert len(successful_amounts) == 30

    assert len(failed_requests) == 20

    assert len(set(successful_amounts)) == 30

    remaining_in_free = await fake_redis.scard('free_amounts')
    assert remaining_in_free == 0

    pending_count = await fake_redis.scard('pending_orders')
    assert pending_count == 30



@pytest.mark.asyncio
async def test_create_order(fake_redis, mock_session, fake_user_data, amount=1510000):
    exact_amount = 1520001
    await fake_redis.sadd('free_amounts', str(exact_amount))
    await fake_redis.sadd('amounts', str(exact_amount))

    order = await create_order_service(fake_user_data, amount, mock_session, fake_redis)
    assert order.user_id == fake_user_data.id
    assert order.payment_intent.exact_amount
    assert order.amount == amount
    assert order.payment_intent.status == OrderStatus.PENDING

    is_in_free = await fake_redis.sismember("free_amounts", str(exact_amount))
    is_in_pending = await fake_redis.sismember("pending_orders", str(exact_amount))

    assert not is_in_free
    assert bool(is_in_pending)


@pytest.mark.asyncio
async def test_create_order_duplicate(fake_redis, mock_session, fake_user_data, amount=1510000):
    exact_amount = 1520001
    await fake_redis.sadd('free_amounts', str(exact_amount))
    await fake_redis.sadd('amounts', str(exact_amount))

    mock_session.flush = AsyncMock(side_effect=IntegrityError("stmt","params",Exception("DB constraint violated")))

    with pytest.raises(ValueError, match=r"Duplicate unique amount"):
        await create_order_service(fake_user_data, amount, mock_session, fake_redis)

    is_in_free = await fake_redis.sismember("free_amounts", str(exact_amount))
    is_in_pending = await fake_redis.sismember("pending_orders", str(exact_amount))

    assert is_in_free
    assert not is_in_pending
