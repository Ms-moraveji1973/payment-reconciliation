import asyncio
import pytest
from unittest.mock import AsyncMock
from redis.exceptions import RedisError
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.modules.order.service import create_order_service, OrderStatus
from app.modules.order.models import Order, PaymentIntent
from app.modules.order.exceptions import DuplicateUniqueAmountError
from app.modules.users.models import User


@pytest.mark.asyncio
async def test_fallback_postgres_allocation_real_db(db_session, test_engine):
    """
    Validates that when Redis is unavailable, create_order_service uses PostgreSQL
    generate_series to allocate a valid unique exact_amount and saves the order.
    """
    user = User(telegram_id=12345678, name="Test Fallback User", is_active=True)
    db_session.add(user)
    await db_session.flush()

    # Mock Redis to simulate outage
    failing_redis = AsyncMock()
    failing_redis.spop.side_effect = RedisError("Redis connection lost")

    base_amount = 1520000
    order = await create_order_service(
        user=user,
        amount=base_amount,
        session=db_session,
        redis_client=failing_redis
    )
    await db_session.commit()

    assert order is not None
    assert order.id is not None
    assert order.amount == base_amount
    assert order.payment_intent is not None
    assert order.payment_intent.status == OrderStatus.PENDING
    assert order.payment_intent.base_amount == base_amount
    # Exact amount should be within base_amount + [1, 500]
    assert base_amount + 1 <= order.payment_intent.exact_amount <= base_amount + 500

    # Ensure Redis was not called for compensation
    failing_redis.smove.assert_not_called()
    failing_redis.sadd.assert_not_called()


@pytest.mark.asyncio
async def test_fallback_postgres_high_concurrency_no_collisions(db_session, test_engine):
    """
    Concurrently executes 25 orders with the identical base_amount during a Redis outage.
    Verifies that:
    1. Every order succeeds without error.
    2. Every order obtains a strictly unique exact_amount from PostgreSQL.
    3. The partial unique index 'idx_pending_price' is never violated.
    4. All 25 records are committed in PostgreSQL.
    """
    user = User(telegram_id=99988877, name="Concurrent User", is_active=True)
    db_session.add(user)
    await db_session.commit()

    base_amount = 1520000
    concurrency_count = 25

    session_factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    failing_redis = AsyncMock()
    failing_redis.spop.side_effect = RedisError("Connection refused")

    async def create_single_order():
        async with session_factory() as session:
            order = await create_order_service(
                user=user,
                amount=base_amount,
                session=session,
                redis_client=failing_redis,
                max_retries=5
            )
            assert order is not None
            await session.commit()
            return order.payment_intent.exact_amount

    # Execute all 25 requests concurrently
    tasks = [create_single_order() for _ in range(concurrency_count)]
    exact_amounts = await asyncio.gather(*tasks)

    # All amounts must be unique
    assert len(exact_amounts) == concurrency_count
    assert len(set(exact_amounts)) == concurrency_count

    # Verify directly in PostgreSQL storage
    async with session_factory() as verify_session:
        stmt = select(PaymentIntent.exact_amount).where(
            PaymentIntent.status == OrderStatus.PENDING,
            PaymentIntent.base_amount == base_amount
        )
        result = await verify_session.execute(stmt)
        stored_amounts = result.scalars().all()

        assert len(stored_amounts) == concurrency_count
        assert len(set(stored_amounts)) == concurrency_count


@pytest.mark.asyncio
async def test_fallback_postgres_savepoint_rollback_and_retry(db_session, test_engine, mocker):
    """
    Simulates a race condition collision on the PostgreSQL partial unique index:
    1. Pre-inserts a PENDING order with exact_amount = 1520010.
    2. Forces the first fallback call to pick 1520010 (causing an intentional IntegrityError).
    3. Verifies that the nested transaction (Savepoint) rolls back cleanly without breaking
       the main session.
    4. Second attempt picks a valid free amount and successfully commits.
    """
    user = User(telegram_id=55544433, name="Retry User", is_active=True)
    db_session.add(user)
    await db_session.flush()

    existing_order = Order(user_id=user.id, amount=1520000)
    existing_intent = PaymentIntent(
        order=existing_order,
        status=OrderStatus.PENDING,
        base_amount=1520000,
        exact_amount=1520010
    )
    db_session.add(existing_order)
    db_session.add(existing_intent)
    await db_session.commit()

    failing_redis = AsyncMock()
    failing_redis.spop.side_effect = RedisError("Redis unavailable")

    from app.modules.order import service

    real_pg_func = service.get_unique_amount_from_postgres
    call_counter = 0

    async def mock_pg_with_forced_collision(session, base_amount=None, window=500):
        nonlocal call_counter
        call_counter += 1
        if call_counter == 1:
            # Force collision with existing_intent
            return 1520010
        # Subsequent calls use the real PostgreSQL query
        return await real_pg_func(session, base_amount, window)

    mocker.patch(
        "app.modules.order.service.get_unique_amount_from_postgres",
        side_effect=mock_pg_with_forced_collision
    )

    session_factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    async with session_factory() as session:
        new_order = await create_order_service(
            user=user,
            amount=1520000,
            session=session,
            redis_client=failing_redis,
            max_retries=3
        )
        await session.commit()

        assert new_order is not None
        assert new_order.payment_intent.exact_amount != 1520010
        assert call_counter == 2
