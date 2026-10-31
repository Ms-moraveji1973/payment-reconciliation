from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.modules.order.tasks import process_outbox_event

def setup_scheduler(redis_client) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        process_outbox_event,
        'interval',
        minutes=1,
        id='order_outbox_relay',
        kwargs={"redis_client": redis_client},
        replace_existing=True
    )

    return scheduler