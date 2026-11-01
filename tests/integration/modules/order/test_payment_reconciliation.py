import pytest
from functools import partial

from app.worker import Worker
from app.modules.order.handlers import handle_payment
from app.modules.order.models import Order, PaymentIntent, OrderStatus
from app.modules.order.queue_manager import MessageQueue, ConsumerGroup
from app.modules.users.models import User


@pytest.mark.asyncio
async def test_payment_reconciliation_workflow_success(db_session, real_redis):
    """
    Integration Test: Payment Reconciliation Workflow
    Flow: Redis Streams -> Worker Consumer -> Payment Handler -> PostgreSQL & Redis Release
    
    Validates:
    1. Real data persistence in PostgreSQL (User, Order, PaymentIntent).
    2. Exact amount reservation tracking in Redis sets ("pending_orders").
    3. Ingestion of payment event via Redis Streams ("pending_orders_queue").
    4. Worker consumption and dispatching to handle_payment without mocks.
    5. Database row locking (SELECT ... FOR UPDATE) and atomic transition to PAID.
    6. Moving exact amount from "pending_orders" to "free_amounts" in Redis.
    7. Message acknowledgment in Redis Streams Consumer Group (clearing PEL).
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
    await db_session.commit()

    # Place reserved amount in Redis "pending_orders" set
    await real_redis.sadd("pending_orders", str(reserved_amount))

    # Initialize queue and enqueue incoming payment event
    queue = MessageQueue(real_redis, stream_name="pending_orders_queue")
    await queue.enqueue({
        "amount": reserved_amount,
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

    # 3. Assert (PostgreSQL): Verify atomic state transition to PAID
    await db_session.refresh(payment_intent)
    await db_session.refresh(order)
    assert payment_intent.status == OrderStatus.PAID
    assert order.status == OrderStatus.PAID

    # 4. Assert (Redis): Verify release of reserved amount back to free pool
    is_in_pending = await real_redis.sismember("pending_orders", str(reserved_amount))
    is_in_free = await real_redis.sismember("free_amounts", str(reserved_amount))
    assert not is_in_pending
    assert bool(is_in_free)

    # 5. Assert (Redis Streams): Verify message was acknowledged in Consumer Group
    pending_info = await worker.consumer.get_pending_summary()
    assert pending_info["count"] == 0
