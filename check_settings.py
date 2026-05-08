import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")


async def check_settings():
    try:
        conn = await asyncpg.connect(DB_URL)
        rows = await conn.fetch("SELECT key, value FROM settings")
        print("Database settings table:")
        for r in rows:
            print(f"{r['key']}: {r['value']}")
        await conn.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(check_settings())
