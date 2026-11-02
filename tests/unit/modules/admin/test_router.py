"""Unit tests for Admin Router error handling."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException, status

from app.modules.admin.router import confirm_payment
from app.modules.order.exceptions import PaymentIntentNotFoundError
from app.modules.users.models import User
from app.modules.order.models import Order, PaymentIntent, OrderStatus


@pytest.mark.asyncio
async def test_confirm_payment_success(mocker, mock_session, fake_redis):
    admin_user = User(id=1, username="admin", admin=True)
    fake_order = Order(id=10, user_id=2, status=OrderStatus.PENDING, amount=1500000)
    fake_payment = PaymentIntent(id=20, order_id=10, status=OrderStatus.PENDING, exact_amount=1510000, order=fake_order)

    mocker.patch(
        "app.modules.admin.router.handle_pending_payment",
        new_callable=AsyncMock,
        return_value=fake_payment
    )

    result = await confirm_payment(
        order_id=10,
        payment_id=20,
        session=mock_session,
        current_user=admin_user,
        redis_client=fake_redis
    )

    assert result == fake_payment
    mock_session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_confirm_payment_not_found(mocker, mock_session, fake_redis):
    admin_user = User(id=1, username="admin", admin=True)
    mocker.patch(
        "app.modules.admin.router.handle_pending_payment",
        new_callable=AsyncMock,
        side_effect=PaymentIntentNotFoundError("Payment intent 99 for order 99 not found")
    )

    with pytest.raises(HTTPException) as exc_info:
        await confirm_payment(
            order_id=99,
            payment_id=99,
            session=mock_session,
            current_user=admin_user,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert "not found" in exc_info.value.detail
    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_confirm_payment_unexpected_error(mocker, mock_session, fake_redis):
    admin_user = User(id=1, username="admin", admin=True)
    mocker.patch(
        "app.modules.admin.router.handle_pending_payment",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Redis connection dropped")
    )

    with pytest.raises(HTTPException) as exc_info:
        await confirm_payment(
            order_id=10,
            payment_id=20,
            session=mock_session,
            current_user=admin_user,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    mock_session.rollback.assert_awaited_once()
