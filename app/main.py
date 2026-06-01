from fastapi import FastAPI , Depends
from typing import Annotated
from contextlib import asynccontextmanager
import redis.asyncio as redis

# internal package
from .modules.order import router as order
from .modules.users import router as users

from .db.database import test_async_connection
from .db.redis_db import test_async_redis_connection
from .core.config import get_settings

config = get_settings()

@asynccontextmanager
async def lifespan(app : FastAPI):
    app.state.redis = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=0)
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