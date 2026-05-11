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
from config import (
    ACCOUNT_REFRESH_INTERVAL,
    DB_URL,
    DEFAULT_SUBSCRIPTIONS,
    HEALTH_PORT,
    IB_CLIENT_ID,
    IB_HOST,
    IB_PORT,
    REDIS_URL,
)
from daily_tracker import DailyBarTracker
from data_writer import DataWriter
from ibkr_client import IBKRClient
from publisher import Publisher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TickBuffer:
    """Buffers raw ticks and flushes them to the DB in batches."""

    def __init__(self, writer, batch_size=1000):
        self.writer = writer
        self.batch_size = batch_size
        self._buffer = []
        self._lock = asyncio.Lock()

    def add_tick(self, symbol, price, size, tick_time):
        """Synchronous add to buffer (called from IB callback)."""
        # (time, symbol, last, volume, open, high, low, close)
        self._buffer.append(
            (tick_time, symbol, price, size, price, price, price, price)
        )

    async def flush(self):
        """Async flush to database."""
        async with self._lock:
            if not self._buffer:
                return
            rows = list(self._buffer)
            self._buffer.clear()

        if rows:
            await self.writer.write_raw_ticks(rows)


async def load_subscriptions(pool):
    """Load subscriptions from DB; fall back to .env SYMBOLS if table is empty or missing."""
    try:
        rows = await pool.fetch(
            "SELECT symbol, sec_type, exchange, currency FROM subscriptions WHERE active=true"
        )
        if rows:
            return [dict(r) for r in rows]
    except Exception:
        logger.warning("Failed to load subscriptions from DB, using .env SYMBOLS")
    logger.info(f"Using {len(DEFAULT_SUBSCRIPTIONS)} symbols from .env SYMBOLS")
    return DEFAULT_SUBSCRIPTIONS


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


async def tick_flush_loop(tick_buffer):
    """Periodically flush raw ticks from the buffer to DB."""
    while True:
        await asyncio.sleep(0.5)  # Flush every 500ms
        try:
            await tick_buffer.flush()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Tick buffer flush error: {e}")


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
                await writer.upsert_daily_bars(bars, update_open=True)
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


async def trading_days_refresh_loop(client, daily_tracker):
    """Refresh trading days cache daily so holiday data stays current."""
    while True:
        await asyncio.sleep(24 * 3600)
        try:
            await client.refresh_trading_days()
            daily_tracker.trading_days = client._trading_days
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Trading days refresh error: {e}")


async def daily_bar_flush_loop(tracker, writer):
    """Flush real-time daily bars from the tick tracker to DB every 5 seconds."""
    while True:
        await asyncio.sleep(5)
        try:
            for bar in tracker.get_dirty_bars():
                await writer.upsert_daily_bars([bar], update_open=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Daily bar flush error: {e}")


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

    # IBKR 连接参数和其他配置直接从 .env 读取
    client = IBKRClient(IB_HOST, IB_PORT, IB_CLIENT_ID)
    writer = DataWriter(pool)
    pub = Publisher(redis_client)
    tick_buffer = TickBuffer(writer)
    daily_tracker = DailyBarTracker()

    # Load the most recent daily bars from DB so tracker preserves OHLC across restarts
    symbols = await load_subscriptions(pool)
    await daily_tracker.load_from_db(pool, symbols)

    # Register tick-by-tick callbacks:
    # 1) Feed each tick into the buffer (for full DB persistence)
    # 2) Track today's daily OHLCV from real-time ticks
    # 3) Publish each tick in real-time via Redis (for frontend live display)
    def on_trade_tick(symbol, price, size, tick_time):
        # Buffer the raw tick for batch DB write
        tick_buffer.add_tick(symbol, price, size, tick_time)
        # Track today's daily OHLCV from real-time ticks
        daily_tracker.on_tick(symbol, price, size, tick_time)
        # Async publish for real-time frontend (fire-and-forget)
        t = asyncio.ensure_future(pub.publish_tick(symbol, price, size, tick_time))
        t.add_done_callback(_on_task_done)

    client.register_tick_handler(on_trade_tick)

    await client.connect_with_retry()

    for s in symbols:
        await client.subscribe(s["symbol"], s["sec_type"], s["exchange"], s["currency"])

    # Share trading days from IBKRClient with the tracker (populated during subscribe)
    daily_tracker.trading_days = client._trading_days

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
    await web.TCPSite(runner, "0.0.0.0", HEALTH_PORT).start()
    logger.info(f"Health endpoint listening on :{HEALTH_PORT}")

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
        asyncio.create_task(tick_flush_loop(tick_buffer), name="tick_flush"),
        asyncio.create_task(
            account_loop(client, writer, pub, ACCOUNT_REFRESH_INTERVAL),
            name="account_loop",
        ),
        asyncio.create_task(settings_listener(redis_client), name="settings_listener"),
        asyncio.create_task(
            daily_bar_refresh_loop(client, writer, pool), name="daily_bar_refresh"
        ),
        asyncio.create_task(
            daily_bar_flush_loop(daily_tracker, writer), name="daily_bar_flush"
        ),
        asyncio.create_task(
            trading_days_refresh_loop(client, daily_tracker), name="trading_days_refresh"
        ),
    ]

    # Wait for shutdown signal
    await shutdown_event.wait()
    logger.info("Shutting down gracefully...")

    # Cancel all tasks
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Flush any remaining ticks before exit
    await tick_buffer.flush()
    logger.info("Flushed remaining raw ticks")

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
