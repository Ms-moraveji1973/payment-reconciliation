import json
import redis.asyncio as redis
from fastapi import Request
from app.core.config import get_settings


config = get_settings()

async def get_redis(request: Request):
    return request.app.state.redis

async def add_token_to_grace_period(redis_client: redis.Redis, jti: str, access_token:str, refresh_token:str):
    payload = {
        "access_token" : access_token,
        "refresh_token" : refresh_token,
        "token_type" : "bearer"
    }
    await redis_client.set(name=jti, value=json.dumps(payload), ex=30)

async def token_in_grace_period(redis_client: redis.Redis, jti: str):
    result = await redis_client.get(jti)
    if result :
        return json.loads(result)
    return None




async def test_async_redis_connection():
    try:
        client = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=0)
        print(f"Ping : {await client.ping()}")
        await client.aclose()
        return True
    except Exception as e:
        print(f"Redis connection test failed: {e}")
        return False
