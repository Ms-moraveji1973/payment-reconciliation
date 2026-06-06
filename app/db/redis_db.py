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


class AmountSeeder:
    all_amount_key = "amounts"
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def is_exist_range_amount(self, start:int, end:int, batch: int = 10000):
        if await self.redis.exists(self.all_amount_key):
            print("--------- It's already existed ---------")
            return
        print('----------- created new amount range set ------------')
        amount_range = [str(i) for i in range(start, end+1)]
        for i in range(0, len(amount_range), batch):
            batch_range = amount_range[i: i+batch]
            await self.redis.sadd(self.all_amount_key, *batch_range)





async def test_async_redis_connection():
    try:
        client = redis.Redis(host=config.REDIS_HOST,port=config.REDIS_PORT,db=0)
        print(f"Ping : {await client.ping()}")
        await client.aclose()
        return True
    except Exception as e:
        print(f"Redis connection test failed: {e}")
        return False
