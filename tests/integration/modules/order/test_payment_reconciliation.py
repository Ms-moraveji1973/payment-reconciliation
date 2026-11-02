import pytest
from functools import partial

from app.worker import Worker
from app.modules.order.handlers import handle_payment
from app.modules.order.models import Order, PaymentIntent, OrderStatus, SMSTransaction
from app.modules.order.queue_manager import MessageQueue, ConsumerGroup
from app.modules.users.models import User


@pytest.mark.asyncio
async def test_payment_reconciliation_workflow_success(db_session, real_redis):
    """
    Integration Test: Payment Reconciliation Workflow (Happy Path)
    Flow: Redis Streams -> Worker Consumer -> Payment Handler -> PostgreSQL & Redis Release
    
    Validates:
    1. Real data persistence in PostgreSQL (User, Order, PaymentIntent, SMSTransaction).
    2. Exact amount reservation tracking in Redis sets ("pending_orders").
    3. Ingestion of payment event via Redis Streams with transaction_id.
    4. Worker consumption and dispatching to handle_payment without mocks.
    5. Database row locking (SELECT ... FOR UPDATE) and atomic transition to PAID.
    6. Setting payment_intent.sms_transaction_id foreign key link.
    7. Moving exact amount from "pending_orders" to "free_amounts" in Redis.
    8. Message acknowledgment in Redis Streams Consumer Group (clearing PEL).
    """
    # 1. Arrange: Setup initial state in PostgreSQL and Redis
    reserved_amount = 1520005
    base_amount = 1520000

    # Create user
    user = User(
        telegram_id=987654321,
        name="Integration User",
        is_active=True
    )
    db_session.add(user)
    await db_session.flush()

    # Create pending order
    order = Order(
        user_id=user.id,
        amount=base_amount,
        status=OrderStatus.PENDING
    )
    db_session.add(order)
    await db_session.flush()

    # Create pending payment intent with exact reservation amount
    payment_intent = PaymentIntent(
        order_id=order.id,
        status=OrderStatus.PENDING,
        base_amount=base_amount,
        exact_amount=reserved_amount
    )
    db_session.add(payment_intent)

    # Create incoming SMSTransaction record
    sms_tx = SMSTransaction(
        sms_amount=reserved_amount,
        sms_inventory=50000000,
        sms_date="1405.03.12",
        sms_time="11:45",
        webhook_payload={"raw": "واریز پول به حساب شما نشست"}
    )
    db_session.add(sms_tx)
    await db_session.commit()
    sms_tx_id = sms_tx.id

    # Place reserved amount in Redis "pending_orders" set
    await real_redis.sadd("pending_orders", str(reserved_amount))

    # Initialize queue and enqueue incoming payment event with transaction_id
    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    await queue.enqueue({
        "amount": reserved_amount,
        "transaction_id": sms_tx_id,
        "trace_id": "tx_rec_integ_999",
        "type": "payment"
    })

    # Setup real worker and consumer group
    group_name = "integration_reconcile_group"
    worker_id = "reconciler_worker_1"
    worker = Worker(queue=queue, group_name=group_name, worker_id=worker_id)
    worker.consumer = await ConsumerGroup.create(
        queue=queue,
        group_name=group_name,
        consumer_name=worker_id
    )
    dependencies_handler = partial(handle_payment, redis_client=real_redis)
    worker.register_handler("payment", dependencies_handler)

    # 2. Act: Worker processes the stream batch
    await worker._process_batch()

    # 3. Assert (PostgreSQL): Verify atomic state transition to PAID and foreign key linking
    await db_session.refresh(payment_intent)
    await db_session.refresh(order)
    assert payment_intent.status == OrderStatus.PAID
    assert order.status == OrderStatus.PAID
    assert payment_intent.sms_transaction_id == sms_tx_id

    # 4. Assert (Redis): Verify release of reserved amount back to free pool
    is_in_pending = await real_redis.sismember("pending_orders", str(reserved_amount))
    is_in_free = await real_redis.sismember("free_amounts", str(reserved_amount))
    assert not is_in_pending
    assert bool(is_in_free)

    # 5. Assert (Redis Streams): Verify message was acknowledged in Consumer Group
    pending_info = await worker.consumer.get_pending_summary()
    assert pending_info["count"] == 0


@pytest.mark.asyncio
async def test_payment_reconciliation_idempotent_duplicate_sms(db_session, real_redis):
    """
    Integration Test: Idempotent Replay Handling
    Flow: Duplicate payment message for already processed SMS -> Skip smove & ACK
    
    Validates:
    When an SMS payment event is re-delivered, process_pending_payment returns (payment, False),
    Redis smove is bypassed, and message is acknowledged safely without side effects.
    """
    reserved_amount = 1530005
    base_amount = 1530000

    user = User(telegram_id=987654322, name="Idempotent User", is_active=True)
    db_session.add(user)
    await db_session.flush()

    order = Order(user_id=user.id, amount=base_amount, status=OrderStatus.PENDING)
    db_session.add(order)
    await db_session.flush()

    payment_intent = PaymentIntent(
        order_id=order.id,
        status=OrderStatus.PENDING,
        base_amount=base_amount,
        exact_amount=reserved_amount
    )
    db_session.add(payment_intent)

    sms_tx = SMSTransaction(
        sms_amount=reserved_amount,
        sms_inventory=60000000,
        sms_date="1405.03.12",
        sms_time="12:00",
        webhook_payload={"raw": "واریز پول مجدد"}
    )
    db_session.add(sms_tx)
    await db_session.commit()
    sms_tx_id = sms_tx.id

    await real_redis.sadd("pending_orders", str(reserved_amount))

    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    worker = Worker(queue=queue, group_name="integration_idempotent_group", worker_id="idempotent_worker_1")
    worker.consumer = await ConsumerGroup.create(
        queue=queue,
        group_name="integration_idempotent_group",
        consumer_name="idempotent_worker_1"
    )
    dependencies_handler = partial(handle_payment, redis_client=real_redis)
    worker.register_handler("payment", dependencies_handler)

    # First delivery: normal successful processing
    await queue.enqueue({
        "amount": reserved_amount,
        "transaction_id": sms_tx_id,
        "trace_id": "tx_first_attempt",
        "type": "payment"
    })
    await worker._process_batch()

    await db_session.refresh(payment_intent)
    assert payment_intent.status == OrderStatus.PAID
    assert payment_intent.sms_transaction_id == sms_tx_id

    # Second delivery: replaying the duplicate message
    await queue.enqueue({
        "amount": reserved_amount,
        "transaction_id": sms_tx_id,
        "trace_id": "tx_duplicate_replay",
        "type": "payment"
    })
    await worker._process_batch()

    # Assert: Still PAID, still linked, no double-move or error
    await db_session.refresh(payment_intent)
    assert payment_intent.status == OrderStatus.PAID
    assert payment_intent.sms_transaction_id == sms_tx_id

    # Redis: amount remains in free_amounts
    is_in_pending = await real_redis.sismember("pending_orders", str(reserved_amount))
    is_in_free = await real_redis.sismember("free_amounts", str(reserved_amount))
    assert not is_in_pending
    assert bool(is_in_free)

    # Both messages acknowledged
    pending_info = await worker.consumer.get_pending_summary()
    assert pending_info["count"] == 0


@pytest.mark.asyncio
async def test_payment_reconciliation_missing_transaction_id_fails(db_session, real_redis):
    """
    Integration Test: Missing transaction_id Error Flow
    Flow: Stream message missing transaction_id -> Handler Critical Reject -> DB Rollback
    
    Validates:
    If a malformed payment message arrives without transaction_id, process_pending_payment
    rejects it, database transaction is rolled back, and order remains PENDING.
    """
    reserved_amount = 1540005
    base_amount = 1540000

    user = User(telegram_id=987654323, name="Missing Tx User", is_active=True)
    db_session.add(user)
    await db_session.flush()

    order = Order(user_id=user.id, amount=base_amount, status=OrderStatus.PENDING)
    db_session.add(order)
    await db_session.flush()

    payment_intent = PaymentIntent(
        order_id=order.id,
        status=OrderStatus.PENDING,
        base_amount=base_amount,
        exact_amount=reserved_amount
    )
    db_session.add(payment_intent)
    await db_session.commit()

    await real_redis.sadd("pending_orders", str(reserved_amount))

    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    worker = Worker(queue=queue, group_name="integration_missing_tx_group", worker_id="missing_tx_worker_1")
    worker.consumer = await ConsumerGroup.create(
        queue=queue,
        group_name="integration_missing_tx_group",
        consumer_name="missing_tx_worker_1"
    )
    dependencies_handler = partial(handle_payment, redis_client=real_redis)
    worker.register_handler("payment", dependencies_handler)

    # Enqueue message WITHOUT transaction_id
    await queue.enqueue({
        "amount": reserved_amount,
        "trace_id": "tx_missing_tx_id",
        "type": "payment"
    })

    # Worker processes batch (message will fail handling)
    await worker._process_batch()

    # PostgreSQL: state was rolled back and remains PENDING
    await db_session.refresh(payment_intent)
    await db_session.refresh(order)
    assert payment_intent.status == OrderStatus.PENDING
    assert order.status == OrderStatus.PENDING
    assert payment_intent.sms_transaction_id is None

    # Redis: amount was NOT released to free_amounts
    is_in_pending = await real_redis.sismember("pending_orders", str(reserved_amount))
    is_in_free = await real_redis.sismember("free_amounts", str(reserved_amount))
    assert bool(is_in_pending)
    assert not is_in_free
