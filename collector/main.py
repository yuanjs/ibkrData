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
from zoneinfo import ZoneInfo
from config import (
    ACCOUNT_REFRESH_INTERVAL,
    DB_URL,
    DEFAULT_SUBSCRIPTIONS,
    FUTURES_ROLL_CALENDAR_AFTER_SESSION_MINUTES,
    FUTURES_ROLL_CALENDAR_COMMODITY_SAFETY_DAYS,
    FUTURES_ROLL_CALENDAR_CONFIRM_DAYS,
    FUTURES_ROLL_CALENDAR_ENABLED,
    FUTURES_ROLL_CALENDAR_INDEX_SAFETY_DAYS,
    FUTURES_ROLL_CALENDAR_INTERVAL_SECONDS,
    HEALTH_PORT,
    IB_CLIENT_ID,
    IB_HOST,
    IB_PORT,
    PRODUCT_ROLL_CONFIG,
    REDIS_URL,
    HAS_PAPER,
)
from daily_tracker import DailyBarTracker
from data_writer import DataWriter
from ibkr_client import IBKRClient
from publisher import Publisher
from backfiller.roll_calendar import RollCalendarGenerator

# ====== monkey-patch: 捕获 tickType 45 (LAST_TIMESTAMP) 交易所时间戳 ======
# ib_insync 的 Wrapper.tickString 没有处理 tickType 45，
# 导致 CASH/FX 产品的交易所秒级时间戳被丢弃。
# 这里在运行时添加 lastTimestamp 字段 + 补丁处理器。
from ib_insync.wrapper import Wrapper
from ib_insync.ticker import Ticker
from datetime import date, datetime, timedelta, timezone

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
# 平仓成交后立即唤醒 account_loop 刷新仓位
_account_refresh_events: dict[str, asyncio.Event] = {
    "live": asyncio.Event(),
    "paper": asyncio.Event(),
}
_paper_tasks: set[asyncio.Task] = set()

COMMODITY_ROLL_SYMBOLS = {"HG", "ZC"}
ROLL_CALENDAR_LOCK_KEY = 817_260_611_001


async def _update_gateway_map(redis, gateway: str, accounts: list[dict]):
    """发布 gateway→account_id 映射，排除 "All"（IBKR 虚拟聚合账户，无有效数据）。"""
    key = "gateway:account_map"
    raw = await redis.get(key)
    mapping: dict[str, list[str]] = json.loads(raw) if raw else {}
    ids = [a["account_id"] for a in accounts if a["account_id"] not in ("All", "", None)]
    mapping[gateway] = ids
    await redis.set(key, json.dumps(mapping))
    await redis.publish("gateway:map:update", json.dumps(mapping))


def _roll_calendar_safety_days(symbol: str) -> int:
    if symbol in COMMODITY_ROLL_SYMBOLS:
        return FUTURES_ROLL_CALENDAR_COMMODITY_SAFETY_DAYS
    return FUTURES_ROLL_CALENDAR_INDEX_SAFETY_DAYS


def _roll_calendar_ready_session_date(
    symbol: str,
    now_utc: datetime,
) -> tuple[date, bool]:
    config = PRODUCT_ROLL_CONFIG.get(symbol)
    if not config:
        return now_utc.date(), True

    local_now = now_utc.astimezone(ZoneInfo(config["timezone"]))
    boundary = local_now.replace(
        hour=config["roll_hour"],
        minute=config["roll_minute"],
        second=0,
        microsecond=0,
    ) + timedelta(minutes=FUTURES_ROLL_CALENDAR_AFTER_SESSION_MINUTES)

    if local_now >= boundary:
        return local_now.date(), True
    return (local_now.date() - timedelta(days=1)), True


async def _load_active_futures_subscription_symbols(pool) -> list[str]:
    subscriptions = await load_subscriptions(pool)
    symbols = {
        str(s["symbol"]).upper()
        for s in subscriptions
        if s.get("sec_type") == "FUT" and s.get("symbol")
    }
    return sorted(symbols)


class TickBuffer:
    """Buffers raw ticks and flushes them to the DB in batches."""

    def __init__(self, writer, batch_size=1000):
        self.writer = writer
        self.batch_size = batch_size
        self._buffer = []
        self._futures_buffer = []
        self._futures_minute_bars = {}
        self._lock = asyncio.Lock()

    def add_tick(self, symbol, price=None, size=None, tick_time=None):
        """Synchronous add to buffer (called from IB callback)."""
        if isinstance(symbol, dict):
            self.add_futures_tick(symbol)
            return
        # (time, symbol, last, volume, open, high, low, close)
        self._buffer.append(
            (tick_time, symbol, price, size, price, price, price, price)
        )

    def add_futures_tick(self, tick: dict):
        """Synchronous add of a real-contract futures tick."""
        price = tick.get("last", tick.get("price"))
        normalized = {
            **tick,
            "last": price,
            "volume": tick.get("volume", tick.get("size")),
            "open": tick.get("open", price),
            "high": tick.get("high", price),
            "low": tick.get("low", price),
            "close": tick.get("close", price),
        }
        self._futures_buffer.append(normalized)
        self._update_futures_minute_bar(normalized)

    def _update_futures_minute_bar(self, tick: dict):
        price = tick.get("last", tick.get("price"))
        tick_time = tick.get("time")
        con_id = tick.get("con_id")
        if price is None or tick_time is None or con_id is None:
            return
        bucket = tick_time.replace(second=0, microsecond=0)
        key = (tick["symbol"], int(con_id), bucket)
        size = tick.get("volume", tick.get("size")) or 0
        bar = self._futures_minute_bars.get(key)
        if bar is None:
            self._futures_minute_bars[key] = {
                "time": bucket,
                "symbol": tick["symbol"],
                "con_id": int(con_id),
                "local_symbol": tick.get("local_symbol"),
                "trading_class": tick.get("trading_class"),
                "contract_month": tick.get("contract_month"),
                "last_trade_date": tick.get("last_trade_date"),
                "exchange": tick.get("exchange"),
                "currency": tick.get("currency"),
                "multiplier": tick.get("multiplier"),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
                "bar_count": 1,
            }
            return

        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["close"] = price
        bar["volume"] = (bar.get("volume") or 0) + size
        bar["bar_count"] = (bar.get("bar_count") or 0) + 1

    async def flush(self):
        """Async flush to database."""
        async with self._lock:
            if (
                not self._buffer
                and not self._futures_buffer
                and not self._futures_minute_bars
            ):
                return
            rows = list(self._buffer)
            futures_rows = list(self._futures_buffer)
            futures_minute_rows = list(self._futures_minute_bars.values())
            self._buffer.clear()
            self._futures_buffer.clear()
            self._futures_minute_bars.clear()

        if rows:
            await self.writer.write_raw_ticks(rows)
        if futures_rows:
            await self.writer.write_futures_ticks(futures_rows)
        if futures_minute_rows:
            await self.writer.upsert_futures_minute_bars_from_live(futures_minute_rows)


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


async def _load_latest_raw_futures_contract(pool, symbol: str) -> dict | None:
    """Return the newest raw futures contract available in local storage."""
    try:
        raw_row = await pool.fetchrow(
            """
            WITH raw AS (
                SELECT
                    symbol,
                    con_id,
                    contract_month,
                    local_symbol,
                    trading_class,
                    exchange,
                    currency,
                    multiplier,
                    last_trade_date,
                    MAX(time) AS latest_time
                FROM (
                    SELECT
                        symbol,
                        con_id,
                        contract_month,
                        local_symbol,
                        trading_class,
                        exchange,
                        currency,
                        multiplier,
                        last_trade_date,
                        time
                    FROM futures_minute_bars
                    WHERE symbol = $1

                    UNION ALL

                    SELECT
                        symbol,
                        con_id,
                        contract_month,
                        local_symbol,
                        trading_class,
                        exchange,
                        currency,
                        multiplier,
                        last_trade_date,
                        time
                    FROM futures_daily_bars
                    WHERE symbol = $1
                ) x
                GROUP BY symbol, con_id, contract_month, local_symbol,
                         trading_class, exchange, currency, multiplier, last_trade_date
                ORDER BY contract_month DESC NULLS LAST, con_id DESC, latest_time DESC
                LIMIT 1
            )
            SELECT * FROM raw
            """,
            symbol,
        )
    except Exception as e:
        logger.warning(
            "Failed to load latest raw futures contract for %s: %s",
            symbol,
            e,
        )
        return None

    if not raw_row:
        return None

    data = dict(raw_row)
    if not data.get("con_id"):
        return None
    return data


async def load_active_futures_contract(pool, symbol: str) -> dict | None:
    """Return the live active futures contract identity from DB roll state."""

    try:
        row = await pool.fetchrow(
            "SELECT * FROM active_futures_contract_asof($1, $2)",
            symbol,
            datetime.now(timezone.utc),
        )
    except Exception as e:
        logger.warning(
            "Failed to load active futures contract for %s, falling back to IBKR resolution: %s",
            symbol,
            e,
        )
        return None

    if not row:
        logger.warning(
            "No active futures contract found for %s, falling back to IBKR resolution",
            symbol,
        )
        return None

    data = dict(row)
    if not data.get("con_id"):
        logger.warning(
            "Active futures contract for %s has no con_id, falling back to IBKR resolution",
            symbol,
        )
        return None
    last_trade_date = data.get("last_trade_date")
    config = PRODUCT_ROLL_CONFIG.get(symbol)
    current_date = datetime.now(timezone.utc).date()
    if config:
        try:
            current_date = datetime.now(timezone.utc).astimezone(
                ZoneInfo(config["timezone"])
            ).date()
        except Exception:
            pass
    if last_trade_date is not None and last_trade_date < current_date:
        logger.warning(
            "Active futures contract for %s is expired (%s), falling back to latest raw contract",
            symbol,
            last_trade_date,
        )
        raw_data = await _load_latest_raw_futures_contract(pool, symbol)
        if raw_data:
            logger.warning(
                "Using latest raw contract for %s live subscription: conId=%s month=%s localSymbol=%s",
                symbol,
                raw_data.get("con_id"),
                raw_data.get("contract_month"),
                raw_data.get("local_symbol"),
            )
            return raw_data
        return None
    return data


def _same_contract(left: dict | None, right: dict | None) -> bool:
    if not left or not right:
        return False
    return str(left.get("con_id")) == str(right.get("con_id"))


async def futures_roll_state_loop(
    client,
    pub,
    pool,
    symbols: list[dict],
    active_contracts: dict[str, dict],
    interval: int = 60,
):
    """Switch live futures market-data subscriptions when local roll state changes."""
    futures_symbols = [s for s in symbols if s.get("sec_type") == "FUT"]
    if not futures_symbols:
        return

    while True:
        await asyncio.sleep(interval)
        try:
            if not client.is_connected:
                continue

            for sub in futures_symbols:
                symbol = sub["symbol"]
                current = active_contracts.get(symbol)
                latest = await load_active_futures_contract(pool, symbol)
                if latest is None or _same_contract(current, latest):
                    continue

                logger.info(
                    "Futures active contract changed for %s: %s -> %s",
                    symbol,
                    current.get("con_id") if current else None,
                    latest.get("con_id"),
                )
                client.unsubscribe(symbol)
                await client.subscribe(
                    symbol,
                    sub["sec_type"],
                    sub["exchange"],
                    sub["currency"],
                    contract_identity=latest,
                )
                active_contracts[symbol] = latest
                await pub.publish_futures_roll_state(
                    symbol,
                    {
                        "symbol": symbol,
                        "previous": current,
                        "active": latest,
                        "roll_event_id": latest.get("roll_event_id"),
                        "effective_from": latest.get("effective_from"),
                        "time": datetime.now(timezone.utc),
                    },
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Futures roll state loop error: {e}")


async def futures_roll_calendar_loop(
    pool,
    interval: int = FUTURES_ROLL_CALENDAR_INTERVAL_SECONDS,
):
    """Generate as-of futures roll events for active FUT subscriptions.

    The collector owns scheduling only.  Roll rules are delegated to
    RollCalendarGenerator so backtest and live roll selection stay aligned.
    """
    if not FUTURES_ROLL_CALENDAR_ENABLED:
        logger.info("Futures roll calendar loop disabled")
        return

    generator = RollCalendarGenerator(pool)
    while True:
        try:
            now = datetime.now(timezone.utc)
            symbols = await _load_active_futures_subscription_symbols(pool)
            if not symbols:
                await asyncio.sleep(interval)
                continue

            async with pool.acquire() as lock_conn:
                async with lock_conn.transaction():
                    locked = await lock_conn.fetchval(
                        "SELECT pg_try_advisory_xact_lock($1)",
                        ROLL_CALENDAR_LOCK_KEY,
                    )
                    if not locked:
                        logger.info(
                            "Skipping futures roll calendar generation; another collector holds the lock"
                        )
                    else:
                        for symbol in symbols:
                            session_date, ready = _roll_calendar_ready_session_date(
                                symbol,
                                now,
                            )
                            if not ready:
                                continue
                            safety_days = _roll_calendar_safety_days(symbol)
                            try:
                                events = await generator.generate_asof(
                                    symbol,
                                    safety_days_before_expiry=safety_days,
                                    min_confirm_days=FUTURES_ROLL_CALENDAR_CONFIRM_DAYS,
                                    replace=False,
                                    dry_run=False,
                                )
                            except Exception as e:
                                logger.error(
                                    "Failed to generate futures roll calendar for %s: %s",
                                    symbol,
                                    e,
                                )
                                continue

                            logger.info(
                                "Generated as-of roll calendar for %s: %s events, session=%s, safety=%sbd",
                                symbol,
                                len(events),
                                session_date,
                                safety_days,
                            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Futures roll calendar loop error: {e}")

        await asyncio.sleep(interval)


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
        try:
            if not client.is_connected:
                await asyncio.sleep(interval)
                continue
            accounts = await client.get_account_summary()
            positions = client.get_positions()
            await writer.write_account(accounts)
            await writer.write_positions(positions, account_ids=[a["account_id"] for a in accounts])
            await pub.publish_account({"accounts": accounts, "positions": positions})
            if first_fetch and redis and accounts:
                await _update_gateway_map(redis, gateway, accounts)
                first_fetch = False
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Account loop ({gateway}) error: {e}")

        # 等待 interval 或被 Event 唤醒（平仓成交后立即刷新仓位）
        evt = _account_refresh_events[gateway]
        try:
            await asyncio.wait_for(evt.wait(), timeout=interval)
            evt.clear()
        except asyncio.TimeoutError:
            pass


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

                contract_identity = {
                    key: data.get(key)
                    for key in (
                        "con_id",
                        "local_symbol",
                        "contract_month",
                        "trading_class",
                        "multiplier",
                        "exchange",
                        "currency",
                    )
                    if data.get(key) is not None
                }

                # 1. 取消该合约所有待成交订单；旧命令没有合约身份时按 symbol 兼容。
                cancelled_ids = client.cancel_orders_for_symbol(
                    symbol,
                    con_id=contract_identity.get("con_id"),
                    local_symbol=contract_identity.get("local_symbol"),
                )

                # 2. 下市价平仓单
                order_id, status = await client.place_market_order(
                    symbol, data["side"], data["quantity"],
                    data["sec_type"], data["exchange"], data["currency"],
                    data.get("account_id"),
                    contract_identity=contract_identity or None,
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
    """Backfill daily bars for non-futures active subscriptions on startup."""
    try:
        symbols = await load_subscriptions(pool)
        for s in symbols:
            if s.get("sec_type") == "FUT":
                continue
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
    """Periodically refresh daily bars for non-futures active subscriptions."""
    # Run first backfill immediately
    await backfill_daily_bars(client, writer, pool, duration="100 D", daily_tracker=daily_tracker)

    while True:
        await asyncio.sleep(4 * 3600)  # Refresh every 4 hours
        try:
            # Refresh a wider window so late IBKR daily settlement/CONTFUT
            # revisions overwrite any live-tick partial bars saved earlier.
            await backfill_daily_bars(client, writer, pool, duration="30 D", daily_tracker=daily_tracker)
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
                    # 平仓成交后立即唤醒 account_loop 刷新仓位
                    if trade.orderStatus.status == "Filled":
                        _account_refresh_events["paper"].set()
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
    futures_symbol_set = {s["symbol"] for s in symbols if s.get("sec_type") == "FUT"}

    # Register tick-by-tick callbacks:
    # 1) Feed each tick into the buffer (for full DB persistence)
    # 2) Track today's daily OHLCV from real-time ticks
    # 3) Publish each tick in real-time via Redis (for frontend live display)
    def on_trade_tick(*args):
        if len(args) == 1 and isinstance(args[0], dict):
            payload = args[0]
            symbol = payload["symbol"]
            price = payload.get("last", payload.get("price"))
            size = payload.get("volume", payload.get("size", 0))
            tick_time = payload["time"]
            tick_buffer.add_futures_tick(payload)
        else:
            symbol, price, size, tick_time = args
            # Buffer the raw tick for batch DB write
            tick_buffer.add_tick(symbol, price, size, tick_time)

        # Buffer the raw tick for batch DB write
        # Track today's daily OHLCV from real-time ticks for non-futures only.
        if symbol not in futures_symbol_set:
            daily_tracker.on_tick(symbol, price, size, tick_time)
        # Async publish for real-time frontend (fire-and-forget)
        t = asyncio.ensure_future(pub.publish_tick(symbol, price, size, tick_time))
        t.add_done_callback(_on_task_done)

    client.register_tick_handler(on_trade_tick)

    await client.connect_with_retry()

    active_futures_contracts: dict[str, dict] = {}
    for s in symbols:
        contract_identity = None
        if s["sec_type"] == "FUT":
            contract_identity = await load_active_futures_contract(pool, s["symbol"])
            if contract_identity:
                active_futures_contracts[s["symbol"]] = contract_identity
        await client.subscribe(
            s["symbol"],
            s["sec_type"],
            s["exchange"],
            s["currency"],
            contract_identity=contract_identity,
        )

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
                # 平仓成交后立即唤醒 account_loop 刷新仓位
                if trade.orderStatus.status == "Filled":
                    _account_refresh_events["live"].set()
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
            futures_roll_state_loop(
                client,
                pub,
                pool,
                symbols,
                active_futures_contracts,
            ),
            name="futures_roll_state",
        ),
        asyncio.create_task(
            futures_roll_calendar_loop(pool),
            name="futures_roll_calendar",
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
