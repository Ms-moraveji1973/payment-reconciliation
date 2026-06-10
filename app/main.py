from fastapi import FastAPI , Depends
from typing import Annotated
from contextlib import asynccontextmanager
import redis.asyncio as redis
from redis.exceptions import RedisError

# internal package
from .modules.order import router as order
from .modules.users import router as users

from .db.database import test_async_connection, SessionLocal
from .db.redis_db import test_async_redis_connection, AmountSeeder
from .core.config import get_settings
from .modules.order.service import get_all_pending_orders

config = get_settings()

@asynccontextmanager
async def lifespan(app : FastAPI):
    app.state.redis = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=0, decode_responses=True)
    # await app.state.redis.delete('amounts')
    amount_seeder = AmountSeeder(app.state.redis)
    await amount_seeder.is_exist_range_amount(1500000, 1550000)
    async with SessionLocal() as session:
        await app.state.redis.delete("pending_orders")
        REDIS_BUFFER = 1000
        buffer = []
        async for orders in get_all_pending_orders(session):
            if orders :
                orders_amount = [order.exact_amount for order in orders]
                buffer.extend(orders_amount)
                if len(buffer) >= REDIS_BUFFER:
                    await app.state.redis.sadd("pending_orders",*buffer)
                    buffer.clear()
        if buffer:
            await app.state.redis.sadd("pending_orders",*buffer)
            buffer.clear()

    await app.state.redis.sdiffstore('free_amounts',['amounts','pending_orders'])

    yield
    await app.state.redis.aclose()




app = FastAPI(
    lifespan=lifespan,
    title="Payment",
    version="0.0.1",
    contact={
        "name": "Order a payment",
    }
)



app.include_router(order.router, tags=["order"])
app.include_router(users.router, tags=["users"])


@app.get("/test-connection")
async def test_connection():
    result = await test_async_connection()
    return result

@app.get("/test-redis-connection")
async def test_redis_connection():
    result = await test_async_redis_connection()
    return result