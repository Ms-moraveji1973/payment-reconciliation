import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from sqlalchemy.exc import IntegrityError, NoResultFound, MultipleResultsFound
from redis.exceptions import RedisError

from app.modules.order.service import (get_unique_amount, create_order_service, OrderStatus,
                                    parse_sms_transaction, delete_order_service, process_pending_payment,
                                    handle_transaction_service, get_pending_orders, create_transaction_service,
                                    create_outbox_event_service)


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
async def test_get_unique_amount_fallback(fake_redis, mock_session, mocker):
    fake_redis.spop = AsyncMock(side_effect=RedisError())
    mock_unique_amount = mocker.patch("app.modules.order.service.get_unique_amount_from_postgres")
    mock_unique_amount.return_value = 1247981

    amount = await get_unique_amount(mock_session, fake_redis)

    assert amount == 1247981
    fake_redis.spop.assert_awaited_once()
    mock_unique_amount.assert_awaited_once()


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
    fake_transaction.trace_id = fake_sms_data["trace_id"]
    mocker_create_tra.return_value = fake_transaction

    transaction_data =  {
                "trace_id":fake_transaction.trace_id,
                "amount":fake_transaction.sms_amount,
                "type":"payment"
                        }

    mocker_outbox = mocker.patch("app.modules.order.service.create_outbox_event_service", new_callable=AsyncMock)
    mocker_outbox_instance = MagicMock()
    mocker_outbox_instance.data = transaction_data
    mocker_outbox.return_value = mocker_outbox_instance


    handle_transaction = await handle_transaction_service(fake_sms_data, mock_session, fake_redis)

    assert handle_transaction['status'] == 'accepted'
    assert handle_transaction['transaction_id'] == 131

    mock_session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_pending_orders(mock_session, limit=1):
    fake_payment = MagicMock()
    fake_payment.id = 12
    fake_payment.status = "PENDING"

    fake_order = MagicMock()
    fake_order.id = 16
    fake_order.amount = 2319512
    fake_order.status = "PENDING"
    fake_order.created_at = "2026/08/1"
    fake_order.user = "luis hamilton"

    fake_payment.order = fake_order

    mock_result = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_result.scalars.return_value.all.return_value = [fake_payment]
    pendign_orders = await get_pending_orders(mock_session, limit, 0)

    assert pendign_orders['next_cursor'] == 12
    assert len(pendign_orders['orders']) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("sms_data, expected",
                         [
                             pytest.param({"sms_amount":None, "sms_time":2341, "sms_date":"sakldjfa", "sms_inventory":2312, "webhook_payload": "alsdkfjasldfj"},
                                          pytest.raises(ValueError, match="error_missing_fields"),
                                          id="none_amount"),
                            pytest.param({"sms_amount":2341, "sms_time":None, "sms_date":"sakldjfa", "sms_inventory":2312, "webhook_payload": "alsdkfjasldfj"},
                                         pytest.raises(ValueError, match="error_missing_fields"),
                                         id="none_payload")
                         ])
async def test_create_transaction_service(mock_session,sms_data, expected):
    with expected:
        await create_transaction_service(sms_data, mock_session)

    mock_session.add.assert_not_called()


@pytest.mark.asyncio
async def test_create_order_no_free_amount(fake_redis, mock_session, fake_user_data):
    # Empty redis sets -> unique_amount returns None
    order = await create_order_service(fake_user_data, 1510000, mock_session, fake_redis)
    assert order is None


@pytest.mark.asyncio
async def test_get_order_service_found(mock_session):
    from app.modules.order.service import get_order_service
    fake_order = MagicMock()
    fake_order.id = 10
    fake_order.user_id = 1

    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = fake_order
    mock_session.execute.return_value = mock_result

    order = await get_order_service(order_id=10, user_id=1, session=mock_session)
    assert order == fake_order


@pytest.mark.asyncio
async def test_get_order_service_not_found(mock_session):
    from app.modules.order.service import get_order_service
    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = None
    mock_session.execute.return_value = mock_result

    order = await get_order_service(order_id=999, user_id=1, session=mock_session)
    assert order is None


@pytest.mark.asyncio
async def test_delete_order_success(mock_session):
    fake_order = MagicMock()
    fake_order.id = 5
    fake_order.user_id = 2

    mock_result = MagicMock()
    mock_result.scalars.return_value.first.return_value = fake_order
    mock_session.execute.return_value = mock_result

    await delete_order_service(order_id=5, user_id=2, session=mock_session)
    mock_session.delete.assert_awaited_once_with(fake_order)


@pytest.mark.asyncio
async def test_get_all_orders(mock_session, fake_user_data):
    from app.modules.order.service import get_all_orders
    fake_order1 = MagicMock()
    fake_order2 = MagicMock()

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [fake_order1, fake_order2]
    mock_session.execute.return_value = mock_result

    orders = await get_all_orders(fake_user_data, mock_session)
    assert len(orders) == 2
    assert orders == [fake_order1, fake_order2]


@pytest.mark.asyncio
async def test_get_unique_amount_from_postgres_empty_pending(mocker, mock_session):
    from app.modules.order.service import get_unique_amount_from_postgres

    async def empty_generator(*args, **kwargs):
        if False:
            yield []

    mocker.patch("app.modules.order.service.get_all_pending_orders", side_effect=empty_generator)

    amount = await get_unique_amount_from_postgres(mock_session)
    assert amount is not None
    assert 1500000 <= amount <= 1550000


@pytest.mark.asyncio
async def test_process_pending_payment_multiple_results_found(mock_session):
    mock_result = MagicMock()
    mock_result.scalar_one.side_effect = MultipleResultsFound("Duplicate rows")
    mock_session.execute.return_value = mock_result

    with pytest.raises(ValueError, match=r"MultipleResultsFound"):
        await process_pending_payment(1520000, mock_session)


@pytest.mark.asyncio
async def test_process_pending_payment_success(mock_session):
    payment = MagicMock()
    payment.status = OrderStatus.PENDING
    payment.order = MagicMock()
    payment.order.status = OrderStatus.PENDING

    mock_result = MagicMock()
    mock_result.scalar_one.return_value = payment
    mock_session.execute.return_value = mock_result

    result = await process_pending_payment(1520000, mock_session)
    assert result.status == OrderStatus.PAID
    assert result.order.status == OrderStatus.PAID
    mock_session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_outbox_event_service_success(mock_session):
    data = {"amount": 1500000, "trace_id": "tx_xyz"}
    event = await create_outbox_event_service(mock_session, data)

    mock_session.add.assert_called_once()
    mock_session.flush.assert_awaited_once()
    assert event.data == data


@pytest.mark.asyncio
async def test_create_outbox_event_service_missing_data(mock_session):
    with pytest.raises(ValueError, match="error_missing"):
        await create_outbox_event_service(mock_session, {})


@pytest.mark.asyncio
async def test_create_outbox_event_service_integrity_error(mock_session):
    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("DB constraint violated"))
    with pytest.raises(ValueError, match="error_duplicate_event"):
        await create_outbox_event_service(mock_session, {"amount": 100})


@pytest.mark.asyncio
async def test_create_transaction_service_success(mock_session):
    valid_sms_data = {
        "sms_amount": 1500000,
        "sms_inventory": 25000000,
        "sms_date": "1405.03.12",
        "sms_time": "12:30",
        "webhook_payload": "valid raw content"
    }
    tx = await create_transaction_service(valid_sms_data, mock_session)

    mock_session.add.assert_called_once()
    mock_session.flush.assert_awaited_once()
    assert tx.sms_amount == 1500000
    assert tx.sms_date == "1405.03.12"


@pytest.mark.asyncio
async def test_create_transaction_service_duplicate_integrity_error(mock_session):
    mock_session.flush.side_effect = IntegrityError("stmt", "params", Exception("Duplicate SMS constraint"))
    valid_sms_data = {
        "sms_amount": 1500000,
        "sms_inventory": 25000000,
        "sms_date": "1405.03.12",
        "sms_time": "12:30",
        "webhook_payload": "valid raw content"
    }
    with pytest.raises(ValueError, match="error_duplicate_transaction"):
        await create_transaction_service(valid_sms_data, mock_session)
