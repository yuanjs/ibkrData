import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")


async def check():
    try:
        conn = await asyncpg.connect(DB_URL)
        count = await conn.fetchval("SELECT count(*) FROM daily_bars")
        rows = await conn.fetch("SELECT * FROM daily_bars ORDER BY time DESC LIMIT 5")
        print(f"Total rows in daily_bars: {count}")
        for r in rows:
            print(dict(r))
        await conn.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(check())
