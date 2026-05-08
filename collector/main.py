import asyncio
import signal
import sys

# Workaround for Python 3.12+ (since eventkit calls get_event_loop on import)
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import json
import logging

import asyncpg
import redis.asyncio as aioredis
from aiohttp import web
from config import DB_URL, IB_CLIENT_ID, IB_HOST, IB_PORT, REDIS_URL
from data_writer import DataWriter
from ibkr_client import IBKRClient
from publisher import Publisher
from tick_aggregator import TickAggregator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def load_settings(pool):
    rows = await pool.fetch("SELECT key, value FROM settings")
    return {r["key"]: r["value"] for r in rows}


async def load_subscriptions(pool):
    rows = await pool.fetch(
        "SELECT symbol, sec_type, exchange, currency FROM subscriptions WHERE active=true"
    )
    return [dict(r) for r in rows]


async def tick_loop(client, pub):
    """Publish bid/ask/volume snapshots for the quote table (no DB write)."""
    while True:
        await asyncio.sleep(1)
        try:
            snapshots = client.get_snapshots()
            if snapshots:
                for symbol, data in snapshots.items():
                    await pub.publish_market(symbol, data)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Tick loop error: {e}")


async def aggregator_flush_loop(aggregator):
    """Flush completed 1-second OHLC bars from the in-memory aggregator to DB."""
    while True:
        await asyncio.sleep(0.2)  # Check every 200ms
        try:
            await aggregator.flush_expired()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Aggregator flush error: {e}")


async def account_loop(client, writer, pub, interval):
    while True:
        await asyncio.sleep(interval)
        try:
            accounts = await client.get_account_summary()
            positions = client.get_positions()
            await writer.write_account(accounts)
            await writer.write_positions(positions)
            await pub.publish_account({"accounts": accounts, "positions": positions})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Account loop error: {e}")


async def settings_listener(redis_client):
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("settings:update")
    try:
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                logger.info("Settings updated")
    except asyncio.CancelledError:
        await pubsub.unsubscribe("settings:update")
        raise


async def backfill_daily_bars(client, writer, pool, duration="100 D"):
    """Backfill daily bars for all active subscriptions on startup."""
    try:
        symbols = await load_subscriptions(pool)
        for s in symbols:
            symbol = s["symbol"]
            logger.info(f"Backfilling daily bars for {symbol} ({duration})...")
            bars = await client.get_historical_daily_bars(symbol, duration=duration)
            if bars:
                await writer.upsert_daily_bars(bars)
        logger.info("Daily bar backfill completed")
    except Exception as e:
        logger.error(f"Daily bar backfill error: {e}")


async def daily_bar_refresh_loop(client, writer, pool):
    """Periodically refresh daily bars for all active subscriptions."""
    # Run first backfill immediately
    await backfill_daily_bars(client, writer, pool, duration="100 D")

    while True:
        await asyncio.sleep(4 * 3600)  # Refresh every 4 hours
        try:
            # For periodic refresh, we can fetch a shorter period (e.g., 5 days) to keep it light
            await backfill_daily_bars(client, writer, pool, duration="5 D")
            logger.info("Periodic daily bar refresh completed")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Daily bar refresh error: {e}")


async def health(request):
    return web.Response(text="ok")


def _on_task_done(task: asyncio.Task):
    """Log exceptions from fire-and-forget tasks."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        logger.error(f"Background task failed: {exc}", exc_info=exc)


async def main():
    pool = await asyncpg.create_pool(DB_URL)
    redis_client = aioredis.from_url(REDIS_URL)

    settings = await load_settings(pool)
    account_interval = int(settings.get("account_refresh_interval", "30"))
    health_port = int(settings.get("health_port", "8001"))

    # IBKR 连接参数直接从 .env 读取，不从数据库覆盖
    client = IBKRClient(IB_HOST, IB_PORT, IB_CLIENT_ID)
    writer = DataWriter(pool)
    pub = Publisher(redis_client)
    aggregator = TickAggregator(writer)

    # Register tick-by-tick callbacks:
    # 1) Feed each tick into the 1-second OHLC aggregator (for DB persistence)
    # 2) Publish each tick in real-time via Redis (for frontend live display)
    def on_trade_tick(symbol, price, size, tick_time):
        # Synchronous call to aggregator (accumulates in memory)
        aggregator.on_tick(symbol, price, size, tick_time)
        # Async publish for real-time frontend (fire-and-forget)
        t = asyncio.ensure_future(pub.publish_tick(symbol, price, size, tick_time))
        t.add_done_callback(_on_task_done)

    client.register_tick_handler(on_trade_tick)

    await client.connect_with_retry()

    for s in await load_subscriptions(pool):
        await client.subscribe(s["symbol"], s["sec_type"], s["exchange"], s["currency"])

    def on_order(trade):
        t = asyncio.ensure_future(writer.upsert_order(trade))
        t.add_done_callback(_on_task_done)
        t2 = asyncio.ensure_future(
            pub.publish_order(
                {"order_id": trade.order.orderId, "status": trade.orderStatus.status}
            )
        )
        t2.add_done_callback(_on_task_done)

    def on_exec(trade, fill):
        t = asyncio.ensure_future(writer.write_execution(trade, fill))
        t.add_done_callback(_on_task_done)
        t2 = asyncio.ensure_future(
            pub.publish_order({"type": "execution", "symbol": trade.contract.symbol})
        )
        t2.add_done_callback(_on_task_done)

    client.register_order_handlers(on_order, on_exec)

    # Health check endpoint
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", health_port).start()
    logger.info(f"Health endpoint listening on :{health_port}")

    # Graceful shutdown
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Run main loops as tasks
    tasks = [
        asyncio.create_task(tick_loop(client, pub), name="tick_loop"),
        asyncio.create_task(aggregator_flush_loop(aggregator), name="aggregator_flush"),
        asyncio.create_task(
            account_loop(client, writer, pub, account_interval), name="account_loop"
        ),
        asyncio.create_task(settings_listener(redis_client), name="settings_listener"),
        asyncio.create_task(
            daily_bar_refresh_loop(client, writer, pool), name="daily_bar_refresh"
        ),
    ]

    # Wait for shutdown signal
    await shutdown_event.wait()
    logger.info("Shutting down gracefully...")

    # Cancel all tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Flush any remaining aggregated bars before exit
    await aggregator.flush_all()
    logger.info("Flushed remaining OHLC bars")

    # Cleanup resources
    await runner.cleanup()
    if client.is_connected:
        client.ib.disconnect()
        logger.info("Disconnected from IB Gateway")
    await pool.close()
    logger.info("Database pool closed")
    await redis_client.aclose()
    logger.info("Redis connection closed")
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
