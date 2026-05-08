import asyncio
import os

import redis.asyncio as aioredis


async def test_redis():
    url = "redis://192.168.32.1:6379"
    print(f"Testing Redis at {url}")
    try:
        r = aioredis.from_url(url)
        keys = await r.keys("*")
        print(f"Total keys: {len(keys)}")
        for k in keys[:10]:
            val = await r.get(k)
            print(f"Key: {k.decode()}, Value: {val.decode() if val else 'None'}")
        await r.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_redis())
