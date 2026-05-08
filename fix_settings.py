import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")


async def run():
    try:
        conn = await asyncpg.connect(DB_URL)
        # 强制设置正确的端口和主机
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('ib_port', '4001') ON CONFLICT (key) DO UPDATE SET value = '4001'"
        )
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('ib_host', '127.0.0.1') ON CONFLICT (key) DO UPDATE SET value = '127.0.0.1'"
        )
        print("Updated database settings: ib_port=4001, ib_host=127.0.0.1")
        await conn.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(run())
