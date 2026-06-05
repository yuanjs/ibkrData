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
    HAS_PAPER,
)
from daily_tracker import DailyBarTracker
from data_writer import DataWriter
from ibkr_client import IBKRClient
from publisher import Publisher

# ====== monkey-patch: 捕获 tickType 45 (LAST_TIMESTAMP) 交易所时间戳 ======
# ib_insync 的 Wrapper.tickString 没有处理 tickType 45，
# 导致 CASH/FX 产品的交易所秒级时间戳被丢弃。
# 这里在运行时添加 lastTimestamp 字段 + 补丁处理器。
from ib_insync.wrapper import Wrapper
from ib_insync.ticker import Ticker
from datetime import datetime, timezone

Ticker.lastTimestamp = None  # type: ignore[attr-defined]

_orig_tickString = Wrapper.tickString
def _patched_tickString(self, reqId, tickType, value):
    if tickType == 45:
        ticker = self.reqId2Ticker.get(reqId)
        if ticker:
            ticker.lastTimestamp = datetime.fromtimestamp(int(value), timezone.utc)
    return _orig_tickString(self, reqId, tickType, value)
Wrapper.tickString = _patched_tickString
# =======================================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mapping of order_id -> close_id for correlating close order status updates
_close_id_maps: dict[str, dict[int, str]] = {
    "live": {},
    "paper": {},
}
_paper_tasks: set[asyncio.Task] = set()


async def _update_gateway_map(redis, gateway: str, accounts: list[dict]):
    """发布 gateway→account_id 映射，排除 "All"（IBKR 虚拟聚合账户，无有效数据）。"""
    key = "gateway:account_map"
    raw = await redis.get(key)
    mapping: dict[str, list[str]] = json.loads(raw) if raw else {}
    ids = [a["account_id"] for a in accounts if a["account_id"] not in ("All", "", None)]
    mapping[gateway] = ids
    await redis.set(key, json.dumps(mapping))
    await redis.publish("gateway:map:update", json.dumps(mapping))


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


async def account_loop(client, writer, pub, interval, gateway="live", redis=None):
    first_fetch = True
    while True:
        await asyncio.sleep(interval)
        try:
            if not client.is_connected:
                continue
            accounts = await client.get_account_summary()
            positions = client.get_positions()
            await writer.write_account(accounts)
            await writer.write_positions(positions)
            await pub.publish_account({"accounts": accounts, "positions": positions})
            if first_fetch and redis and accounts:
                await _update_gateway_map(redis, gateway, accounts)
                first_fetch = False
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Account loop ({gateway}) error: {e}")


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


async def order_command_listener(client, pub, channel="order:command:live"):
    """监听 Redis order 通道，执行平仓指令。"""
    gateway = "paper" if "paper" in channel else "live"
    close_map = _close_id_maps[gateway]

    redis = aioredis.from_url(REDIS_URL)
    pubsub = redis.pubsub()

    # 同时监听旧通道(向后兼容)和新通道
    if channel == "order:command:live":
        await pubsub.subscribe("order:command", "order:command:live")
    else:
        await pubsub.subscribe(channel)

    logger.info(f"Order command listener started, subscribed to {channel}")
    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            try:
                data = json.loads(msg["data"])
                symbol = data["symbol"]
                close_id = data["close_id"]
                logger.info(f"Close position command received: {symbol} (close_id={close_id})")

                # 1. 取消该品种所有待成交订单
                cancelled_ids = client.cancel_orders_for_symbol(symbol)

                # 2. 下市价平仓单
                order_id, status = client.place_market_order(
                    symbol, data["side"], data["quantity"],
                    data["sec_type"], data["exchange"], data["currency"],
                )

                # Track close_id for subsequent on_order callbacks
                close_map[order_id] = close_id

                # 3. 发布带 close_id 的订单状态（供前端匹配回执）
                await pub.publish_order({
                    "close_id": close_id,
                    "order_id": order_id,
                    "symbol": symbol,
                    "side": data["side"],
                    "quantity": data["quantity"],
                    "status": status,
                    "cancelled_orders": cancelled_ids,
                })
            except Exception as e:
                logger.error(f"order_command_listener ({channel}) error: {e}")
    except asyncio.CancelledError:
        logger.info(f"Order command listener ({channel}) cancelled, cleaning up...")
        raise
    finally:
        await pubsub.unsubscribe(channel)
        await redis.aclose()


async def backfill_daily_bars(client, writer, pool, duration="100 D", daily_tracker=None):
    """Backfill daily bars for all active subscriptions on startup."""
    try:
        symbols = await load_subscriptions(pool)
        for s in symbols:
            symbol = s["symbol"]
            logger.info(f"Backfilling daily bars for {symbol} ({duration})...")
            bars = await client.get_historical_daily_bars(symbol, duration=duration)
            if bars:
                await writer.upsert_daily_bars(bars, update_open=True)
                # Update the tracker with the latest bar date from backfill,
                # so _effective_date_str can use it as an anchor for holiday
                # detection (e.g., Memorial Day where trade date skips ahead).
                if daily_tracker is not None:
                    latest = max(b["date_str"] for b in bars)
                    daily_tracker.update_latest_bar_date(symbol, latest)


        logger.info("Daily bar backfill completed")
    except Exception as e:
        logger.error(f"Daily bar backfill error: {e}")


async def daily_bar_refresh_loop(client, writer, pool, daily_tracker):
    """Periodically refresh daily bars for all active subscriptions."""
    # Run first backfill immediately
    await backfill_daily_bars(client, writer, pool, duration="100 D", daily_tracker=daily_tracker)

    while True:
        await asyncio.sleep(4 * 3600)  # Refresh every 4 hours
        try:
            # For periodic refresh, we can fetch a shorter period (e.g., 5 days) to keep it light
            await backfill_daily_bars(client, writer, pool, duration="5 D", daily_tracker=daily_tracker)
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
    """Flush real-time daily bars from the tick tracker to DB every 5 seconds.

    Also deletes stale future-date bars that should not appear on the chart
    (e.g., from a previous session's post-rollhour data after a restart).
    """
    while True:
        await asyncio.sleep(5)
        try:
            for bar in tracker.get_dirty_bars():
                await writer.upsert_daily_bars([bar], update_open=False)
            # Clean up stale future-date bars flagged by the tracker
            stale = tracker.get_stale_bars()
            if stale:
                await writer.delete_daily_bars(stale)
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


async def init_paper(pool, redis_client, writer, pub):
    try:
        from config import PAPER_IB_HOST, PAPER_IB_PORT, PAPER_IB_CLIENT_ID, ACCOUNT_REFRESH_INTERVAL
        paper_client = IBKRClient(PAPER_IB_HOST, PAPER_IB_PORT, PAPER_IB_CLIENT_ID)
        logger.info(f"Paper gateway connecting to {PAPER_IB_HOST}:{PAPER_IB_PORT}...")
        await paper_client.connect_with_retry()

        # 同步历史成交（连接前已完成但未入库的记录）
        try:
            fills = await paper_client.ib.reqExecutionsAsync()
            await writer.sync_executions(fills)
            logger.info(f"Synced {len(fills)} historical executions for paper account")
        except Exception as e:
            logger.warning(f"Failed to sync paper execution history: {e}")

        def on_paper_order(trade):
            t = asyncio.ensure_future(writer.upsert_order(trade))
            t.add_done_callback(_on_task_done)
            payload: dict = {"order_id": trade.order.orderId, "status": trade.orderStatus.status}
            # Attach close_id if this order was initiated by a close command
            oid = trade.order.orderId
            paper_map = _close_id_maps["paper"]
            if oid in paper_map:
                payload["close_id"] = paper_map[oid]
                if trade.orderStatus.status in ("Filled", "Cancelled", "Inactive"):
                    del paper_map[oid]
            t2 = asyncio.ensure_future(pub.publish_order(payload))
            t2.add_done_callback(_on_task_done)

        def on_paper_exec(trade, fill):
            t = asyncio.ensure_future(writer.write_execution(trade, fill))
            t.add_done_callback(_on_task_done)
            t2 = asyncio.ensure_future(
                pub.publish_order({"type": "execution", "symbol": trade.contract.symbol})
            )
            t2.add_done_callback(_on_task_done)

        paper_client.register_order_handlers(on_paper_order, on_paper_exec)

        task1 = asyncio.create_task(
            account_loop(paper_client, writer, pub, ACCOUNT_REFRESH_INTERVAL,
                         gateway="paper", redis=redis_client),
            name="paper_account_loop",
        )
        _paper_tasks.add(task1)
        task1.add_done_callback(_paper_tasks.discard)
        task2 = asyncio.create_task(
            order_command_listener(paper_client, pub, channel="order:command:paper"),
            name="paper_order_listener",
        )
        _paper_tasks.add(task2)
        task2.add_done_callback(_paper_tasks.discard)
        logger.info("Paper gateway initialized successfully")
    except Exception as e:
        logger.error(f"Paper gateway init failed (will retry): {e}")
        await asyncio.sleep(30)
        asyncio.create_task(init_paper(pool, redis_client, writer, pub))


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
        payload: dict = {"order_id": trade.order.orderId, "status": trade.orderStatus.status}
        # Attach close_id if this order was initiated by a close command
        oid = trade.order.orderId
        if oid in _close_id_maps["live"]:
            payload["close_id"] = _close_id_maps["live"][oid]
            # Clean up map when order reaches terminal state
            if trade.orderStatus.status in ("Filled", "Cancelled", "Inactive"):
                del _close_id_maps["live"][oid]
        t2 = asyncio.ensure_future(pub.publish_order(payload))
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
            account_loop(client, writer, pub, ACCOUNT_REFRESH_INTERVAL,
                         gateway="live", redis=redis_client),
            name="live_account_loop",
        ),
        asyncio.create_task(settings_listener(redis_client), name="settings_listener"),
        asyncio.create_task(
            daily_bar_refresh_loop(client, writer, pool, daily_tracker), name="daily_bar_refresh"
        ),
        asyncio.create_task(
            daily_bar_flush_loop(daily_tracker, writer), name="daily_bar_flush"
        ),
        asyncio.create_task(
            trading_days_refresh_loop(client, daily_tracker), name="trading_days_refresh"
        ),
        asyncio.create_task(
            order_command_listener(client, pub, channel="order:command:live"),
            name="live_order_listener",
        ),
    ]

    # Paper Gateway 后台初始化（不阻塞 Live）
    if HAS_PAPER:
        paper_task = asyncio.create_task(
            init_paper(pool, redis_client, writer, pub),
            name="init_paper",
        )
        _paper_tasks.add(paper_task)
        paper_task.add_done_callback(_paper_tasks.discard)

    # Wait for shutdown signal
    await shutdown_event.wait()
    logger.info("Shutting down gracefully...")

    # Cancel paper tasks
    for task in list(_paper_tasks):
        task.cancel()

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
