from app.db.database import SessionLocal
from app.modules.order.service import process_pending_payment
from app.modules.order.exceptions import (
    PaymentIntentNotFoundError,
    DuplicatePaymentIntentError,
    IgnoreAndAckMessage,
    CriticalRejectMessage,
    DuplicatePaymentTransactionError
)
from app.core.logger import log as logger

async def handle_payment(message, redis_client):
    async with SessionLocal() as session:
        try:
            logger.info(f"the message {message}")
            amount = message.data.get("amount")
            transaction_id = message.data.get("transaction_id")
            payment, is_new = await process_pending_payment(amount, transaction_id, session)
            await session.commit()
            if is_new:
                await redis_client.smove("pending_orders","free_amounts",str(amount))
                logger.info(f" {amount} was moved to | redis free_amount")
            else :
                logger.info(f"Duplicate payment for SMS {transaction_id}. Redis smove skipped.")

            logger.info(f"Message {message.id} processed successfully.")
            return True

        except DuplicatePaymentTransactionError as exc:
            await session.rollback()
            logger.info(f"Message {message.id} ignored and acknowledged due to concurrent resolution: {exc}")
            raise IgnoreAndAckMessage(str(exc))

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