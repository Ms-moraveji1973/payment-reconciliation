"""Unit tests for Order Router HTTP exception handling and response mapping."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException, status

from app.modules.order.router import (
    create_order,
    get_order,
    delete_order,
    receive_transaction,
)
from app.modules.order.schema import OrderSchema, SmsWebhookPayload
from app.modules.order.exceptions import (
    DuplicateUniqueAmountError,
    OrderNotFoundError,
    DuplicateSMSTransactionError,
    InvalidSMSDataError,
    DuplicateOutboxEventError,
)
from app.modules.order.models import Order, PaymentIntent, OrderStatus
from app.core.config import get_settings


@pytest.mark.asyncio
async def test_create_order_success(mocker, fake_user_data, mock_session, fake_redis):
    order_data = OrderSchema(amount=1510000)
    fake_order = Order(
        id=1,
        user_id=fake_user_data.id,
        amount=1510000,
        payment_intent=PaymentIntent(status=OrderStatus.PENDING, base_amount=1510000, exact_amount=1520001)
    )

    mocker.patch(
        "app.modules.order.router.create_order_service",
        new_callable=AsyncMock,
        return_value=fake_order
    )

    result = await create_order(
        order_data=order_data,
        current_user=fake_user_data,
        session=mock_session,
        redis_client=fake_redis
    )

    assert result == fake_order
    mock_session.commit.assert_awaited_once()
    mock_session.refresh.assert_awaited_once_with(fake_order, attribute_names=['payment_intent'])


@pytest.mark.asyncio
async def test_create_order_no_free_amount_conflict(mocker, fake_user_data, mock_session, fake_redis):
    order_data = OrderSchema(amount=1510000)
    mocker.patch(
        "app.modules.order.router.create_order_service",
        new_callable=AsyncMock,
        return_value=None
    )

    with pytest.raises(HTTPException) as exc_info:
        await create_order(
            order_data=order_data,
            current_user=fake_user_data,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail == "No free amount is available "


@pytest.mark.asyncio
async def test_create_order_duplicate_unique_amount_error(mocker, fake_user_data, mock_session, fake_redis):
    order_data = OrderSchema(amount=1510000)
    mocker.patch(
        "app.modules.order.router.create_order_service",
        new_callable=AsyncMock,
        side_effect=DuplicateUniqueAmountError("Duplicate unique amount")
    )

    with pytest.raises(HTTPException) as exc_info:
        await create_order(
            order_data=order_data,
            current_user=fake_user_data,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_409_CONFLICT
    assert exc_info.value.detail == "Duplicate unique amount"
    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_order_unexpected_internal_error(mocker, fake_user_data, mock_session, fake_redis):
    order_data = OrderSchema(amount=1510000)
    mocker.patch(
        "app.modules.order.router.create_order_service",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Database connection lost")
    )

    with pytest.raises(HTTPException) as exc_info:
        await create_order(
            order_data=order_data,
            current_user=fake_user_data,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert exc_info.value.detail == "Internal Server Error"
    mock_session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_order_not_found(mocker, fake_user_data, mock_session):
    mocker.patch(
        "app.modules.order.router.delete_order_service",
        new_callable=AsyncMock,
        side_effect=OrderNotFoundError("Order not found")
    )

    with pytest.raises(HTTPException) as exc_info:
        await delete_order(
            current_user=fake_user_data,
            order_id=999,
            session=mock_session
        )

    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert exc_info.value.detail == "Order not found"


@pytest.mark.asyncio
async def test_receive_transaction_invalid_token(fake_redis, mock_session):
    payload = SmsWebhookPayload(from_number="+989120000000", content="valid raw content", timestamp=1700000000)
    with pytest.raises(HTTPException) as exc_info:
        await receive_transaction(
            sms_transaction=payload,
            token="invalid_token_123",
            session=mock_session,
            redis_client=fake_redis
        )
    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "The TOKEN is invalid"


@pytest.mark.asyncio
async def test_receive_transaction_empty_content(fake_redis, mock_session):
    config = get_settings()
    payload = SmsWebhookPayload(from_number="+989120000000", content="", timestamp=1700000000)
    with pytest.raises(HTTPException) as exc_info:
        await receive_transaction(
            sms_transaction=payload,
            token=config.TOKEN_VERIFICATION,
            session=mock_session,
            redis_client=fake_redis
        )
    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == "The content is required"


@pytest.mark.asyncio
async def test_receive_transaction_duplicate_sms_error(mocker, fake_redis, mock_session):
    config = get_settings()
    payload = SmsWebhookPayload(from_number="+989120000000", content="dummy content", timestamp=1700000000)
    mocker.patch("app.modules.order.router.parse_sms_transaction", return_value={"trace_id": "tx_1"})
    mocker.patch(
        "app.modules.order.router.handle_transaction_service",
        new_callable=AsyncMock,
        side_effect=DuplicateSMSTransactionError("The transaction data already exists")
    )

    with pytest.raises(HTTPException) as exc_info:
        await receive_transaction(
            sms_transaction=payload,
            token=config.TOKEN_VERIFICATION,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == "The transaction data already exists"


@pytest.mark.asyncio
async def test_receive_transaction_invalid_sms_data_error(mocker, fake_redis, mock_session):
    config = get_settings()
    payload = SmsWebhookPayload(from_number="+989120000000", content="dummy content", timestamp=1700000000)
    mocker.patch("app.modules.order.router.parse_sms_transaction", return_value={"trace_id": "tx_2"})
    mocker.patch(
        "app.modules.order.router.handle_transaction_service",
        new_callable=AsyncMock,
        side_effect=InvalidSMSDataError("The transaction data is incomplete")
    )

    with pytest.raises(HTTPException) as exc_info:
        await receive_transaction(
            sms_transaction=payload,
            token=config.TOKEN_VERIFICATION,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == "The transaction data is incomplete"


@pytest.mark.asyncio
async def test_receive_transaction_duplicate_outbox_event_error(mocker, fake_redis, mock_session):
    config = get_settings()
    payload = SmsWebhookPayload(from_number="+989120000000", content="dummy content", timestamp=1700000000)
    mocker.patch("app.modules.order.router.parse_sms_transaction", return_value={"trace_id": "tx_3"})
    mocker.patch(
        "app.modules.order.router.handle_transaction_service",
        new_callable=AsyncMock,
        side_effect=DuplicateOutboxEventError("The event data already exists")
    )

    with pytest.raises(HTTPException) as exc_info:
        await receive_transaction(
            sms_transaction=payload,
            token=config.TOKEN_VERIFICATION,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == "The event data already exists"


@pytest.mark.asyncio
async def test_receive_transaction_unexpected_internal_error(mocker, fake_redis, mock_session):
    config = get_settings()
    payload = SmsWebhookPayload(from_number="+989120000000", content="dummy content", timestamp=1700000000)
    mocker.patch("app.modules.order.router.parse_sms_transaction", return_value={"trace_id": "tx_4"})
    mocker.patch(
        "app.modules.order.router.handle_transaction_service",
        new_callable=AsyncMock,
        side_effect=RuntimeError("Redis connection failure")
    )

    with pytest.raises(HTTPException) as exc_info:
        await receive_transaction(
            sms_transaction=payload,
            token=config.TOKEN_VERIFICATION,
            session=mock_session,
            redis_client=fake_redis
        )

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert exc_info.value.detail == "Internal Error"
