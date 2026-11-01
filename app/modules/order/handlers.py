from app.db.database import SessionLocal
from app.modules.order.service import process_pending_payment
from app.modules.order.exceptions import (
    PaymentIntentNotFoundError,
    DuplicatePaymentIntentError,
    IgnoreAndAckMessage,
    CriticalRejectMessage,
)
from app.core.logger import log as logger

async def handle_payment(message, redis_client):
    async with SessionLocal() as session:
        try:
            amount = message.data.get("amount")
            await process_pending_payment(amount, session)
            await session.commit()
            await redis_client.smove("pending_orders","free_amounts",str(amount))
            logger.info(f" {amount} was moved to | redis free_amount")
            logger.info(f"Message {message.id} processed successfully.")
            return True

        except PaymentIntentNotFoundError:
            await session.rollback()
            logger.warning(f"{message.id} has been acknowledged")
            raise IgnoreAndAckMessage(f"{amount} not found in DB")

        except DuplicatePaymentIntentError:
            await session.rollback()
            logger.critical(f"There are the same {amount} in DB --> {message.id}")
            raise CriticalRejectMessage(f"Duplicate amounts found for {amount}")

        except Exception as error:
            await session.rollback()
            logger.critical(f"Unexpected error: {error}")
            raise CriticalRejectMessage(f"Unexpected error {error}")