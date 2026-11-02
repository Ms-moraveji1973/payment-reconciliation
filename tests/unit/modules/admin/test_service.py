import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException
from sqlalchemy.exc import NoResultFound

from app.modules.admin.service import (
    get_current_admin_user,
    handle_pending_payment,
    get_dead_letter_queue,
)
from app.modules.admin.dlq_manager import DeadLetterQueue
from app.modules.users.models import User
from app.modules.order.models import Order, PaymentIntent, OrderStatus
from app.modules.order.exceptions import PaymentIntentNotFoundError


@pytest.mark.asyncio
async def test_get_current_admin_user_success():
    admin_user = User(id=1, username="boss", admin=True)
    result = await get_current_admin_user(admin_user)
    assert result == admin_user


@pytest.mark.asyncio
async def test_get_current_admin_user_forbidden():
    regular_user = User(id=2, username="regular", admin=False)
    with pytest.raises(HTTPException) as exc_info:
        await get_current_admin_user(regular_user)
    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == "You can't access to the admin panel"

    with pytest.raises(HTTPException) as exc_info_none:
        await get_current_admin_user(None)
    assert exc_info_none.value.status_code == 403


@pytest.mark.asyncio
async def test_handle_pending_payment_success(mock_session, fake_redis):
    order = Order(id=10, user_id=1, status=OrderStatus.PENDING, amount=1520000)
    payment = PaymentIntent(
        id=20,
        order_id=10,
        status=OrderStatus.PENDING,
        base_amount=1520000,
        exact_amount=1520005,
        order=order
    )
    mock_result = MagicMock()
    mock_result.scalar_one.return_value = payment
    mock_session.execute.return_value = mock_result

    # Add to pending_orders in fake_redis
    await fake_redis.sadd("pending_orders", "1520005")

    updated = await handle_pending_payment(
        order_id=10,
        payment_id=20,
        session=mock_session,
        redis_client=fake_redis
    )

    assert updated.status == OrderStatus.PAID
    assert updated.order.status == OrderStatus.PAID
    mock_session.flush.assert_awaited_once()

    # Verify moved from pending_orders to free_amounts in redis
    is_in_pending = await fake_redis.sismember("pending_orders", "1520005")
    is_in_free = await fake_redis.sismember("free_amounts", "1520005")
    assert not is_in_pending
    assert bool(is_in_free)


@pytest.mark.asyncio
async def test_handle_pending_payment_not_found(mock_session, fake_redis):
    mock_result = MagicMock()
    mock_result.scalar_one.side_effect = NoResultFound("No row found")
    mock_session.execute.return_value = mock_result

    with pytest.raises(PaymentIntentNotFoundError):
        await handle_pending_payment(
            order_id=99,
            payment_id=99,
            session=mock_session,
            redis_client=fake_redis
        )


@pytest.mark.asyncio
async def test_get_dead_letter_queue(fake_redis):
    dlq = await get_dead_letter_queue(fake_redis)
    assert isinstance(dlq, DeadLetterQueue)
    assert dlq.dead_letter_stream == "pending_orders_queue:dead"
