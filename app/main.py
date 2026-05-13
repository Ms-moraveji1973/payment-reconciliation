from fastapi import FastAPI , Depends
from typing import Annotated
from contextlib import asynccontextmanager
# internal package
from .modules.order import router as order
from .modules.users import router as users

from .db.database import test_async_connection
from .core import config

app = FastAPI(
    title="ChimichangApp",
    summary="Deadpool's favorite app. Nuff said.",
    version="0.0.1",
    terms_of_service="http://example.com/terms/",
    contact={
        "name": "Order a payment",
        "url": "http://x-force.example.com/contact/",
        "email": "order-force.example.com",
    },
    license_info={
        "name": "Apache 2.0",
        "identifier": "MIT",
    },
)

app.include_router(order.router, tags=["order"])
app.include_router(users.router, tags=["users"])


@app.get("/test-connection")
async def test_connection():
    result = await test_async_connection()
    return result