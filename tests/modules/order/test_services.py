import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.exc import IntegrityError, NoResultFound, MultipleResultsFound
from app.modules.order.service import get_unique_amount, create_order_service, OrderStatus, parse_sms_transaction, delete_order_service, process_pending_payment, handle_transaction_service


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


@pytest.mark.parametrize("sms, expected_type",
    [
        pytest.param(
            "",
            type(None),
            id="none_type_test"
            ),

        pytest.param(
            "بلو \nواریز پول\nمحمد عزیز\n1760021ریال به حساب شما نشست.\nموجودی: ۱۲۴۱۲۴۲۲۱۰ ریال\n۱۱:۴۸\n۱۴۰۵.۰۳.۱۲",
            dict,
            id="dict_type_test"
            )
    ]
)
def test_parse_sms_transaction(sms, expected_type):
    parse_sms = parse_sms_transaction(sms)
    assert isinstance(parse_sms, expected_type)


@pytest.mark.asyncio
async def test_delete_order(mock_session, fake_user_data, order_id:int=129):
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = None
    mock_session.execute.return_value = mock_result

    with pytest.raises(ValueError, match=r"Order not found"):
        await delete_order_service(order_id, fake_user_data.id, mock_session)

    mock_session.execute.assert_called_once()
    mock_session.delete.assert_not_called()




@pytest.mark.asyncio
async def test_processing_pending_payment(mock_session, amount=1219518):
    mock_result = MagicMock()
    mock_result.scalar_one.side_effect = NoResultFound("stmt","params",Exception("DB constraint violated"))
    mock_session.execute.return_value = mock_result


    with pytest.raises(ValueError, match=r"NoResultFound"):
        await process_pending_payment(amount, mock_session)


@pytest.mark.asyncio
async def test_handle_transaction(mocker, mock_session, fake_redis):
    fake_sms_data = {"trace_id":"398xmm38b3jx-12382"}
    mocker_create_tra = mocker.patch("app.modules.order.service.create_transaction_service", new_callable=AsyncMock)
    fake_transaction = MagicMock()
    fake_transaction.id = 131
    fake_transaction.sms_amount = 2319512
    mocker_create_tra.return_value = fake_transaction

    mocker_msg_queue = mocker.patch("app.modules.order.service.MessageQueue")
    mocker_msg_queue_instance = mocker_msg_queue.return_value
    mocker_msg_queue_instance.enqueue = AsyncMock(return_value="msg-125")

    handle_transaction = await handle_transaction_service(fake_sms_data, mock_session, fake_redis)

    assert handle_transaction['status'] == 'accepted'
    assert handle_transaction['redis_message_id'] == 'msg-125'
    assert handle_transaction['transaction_id'] == 131

    mocker_msg_queue_instance.enqueue.assert_awaited_once_with({
    "trace_id": "398xmm38b3jx-12382",
    "amount": 2319512,
    "type": "payment"
    })

    mock_session.commit.assert_awaited_once()
