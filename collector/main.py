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
from dataclasses import dataclass

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
    FUTURES_LIVE_CONTRACT_REFRESH_SECONDS,
    FUTURES_LIVE_DAILY_REFRESH_SECONDS,
    FUTURES_MINUTE_COMPLETE_DELAY_SECONDS,
    FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS,
    HEALTH_PORT,
    IB_CLIENT_ID,
    IB_HOST,
    IB_PORT,
    ORDER_SYNC_INTERVAL,
    PRODUCT_ROLL_CONFIG,
    REDIS_URL,
    HAS_PAPER,
    STARTUP_MINUTE_BACKFILL_ENABLED,
    STARTUP_MINUTE_BACKFILL_FUTURES_MIN_SESSION_MINUTES,
    STARTUP_MINUTE_BACKFILL_GAP_THRESHOLD_MINUTES,
    STARTUP_MINUTE_BACKFILL_LOOKBACK_DAYS,
    STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL,
    STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS,
    STARTUP_MINUTE_BACKFILL_STABLE_DELAY_MINUTES,
    DAILY_BAR_BACKFILL_REQUEST_INTERVAL_SECONDS,
)
from daily_tracker import DailyBarTracker
from data_writer import DataWriter
from futures_runtime import LiveFuturesRuntime
from ibkr_client import IBKRClient
from publisher import Publisher
from backfiller.contract import resolve_contract_async, resolve_what_to_show
from backfiller.db_writer import MinuteBarWriter
from backfiller.roll_calendar import RollCalendarGenerator

# ====== monkey-patch: 捕获 tickType 45 (LAST_TIMESTAMP) 交易所时间戳 ======
# ib_insync 的 Wrapper.tickString 没有处理 tickType 45，
# 导致 CASH/FX 产品的交易所秒级时间戳被丢弃。
# 这里在运行时添加 lastTimestamp 字段 + 补丁处理器。
from ib_insync import Future
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


@dataclass
class ProductConfig:
    symbol: str
    sec_type: str
    exchange: str
    currency: str

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
LIVE_TICK_PUBLISH_QUEUE_MAX = 5000


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
        self._minute_bars = {}
        self._futures_buffer = []
        self._futures_minute_bars = {}
        self._futures_minute_complete_bars = {}
        self._futures_minute_published_revisions = {}
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
        self._update_cash_minute_bar(symbol, price, size, tick_time)

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
        self._update_futures_minute_complete_bar(normalized)

    def _update_futures_minute_bar(self, tick: dict):
        self._update_minute_bar_store(self._futures_minute_bars, tick)

    def _update_futures_minute_complete_bar(self, tick: dict):
        self._update_minute_bar_store(self._futures_minute_complete_bars, tick)

    def _update_cash_minute_bar(self, symbol, price, size, tick_time):
        if price is None or tick_time is None:
            return
        bucket = tick_time.replace(second=0, microsecond=0)
        key = (symbol, bucket)
        size = size or 0
        bar = self._minute_bars.get(key)
        if bar is None:
            self._minute_bars[key] = {
                "time": bucket,
                "symbol": symbol,
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

    def _update_minute_bar_store(self, store: dict, tick: dict):
        price = tick.get("last", tick.get("price"))
        tick_time = tick.get("time")
        con_id = tick.get("con_id")
        if price is None or tick_time is None or con_id is None:
            return
        bucket = tick_time.replace(second=0, microsecond=0)
        key = (tick["symbol"], int(con_id), bucket)
        size = tick.get("volume", tick.get("size")) or 0
        bar = store.get(key)
        if bar is None:
            store[key] = {
                "time": bucket,
                "symbol": tick["symbol"],
                "con_id": int(con_id),
                "role": tick.get("role"),
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
                "_revision": 1,
            }
            return

        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["close"] = price
        bar["volume"] = (bar.get("volume") or 0) + size
        bar["bar_count"] = (bar.get("bar_count") or 0) + 1
        bar["_revision"] = (bar.get("_revision") or 0) + 1

    def _minute_complete_payload(self, bar: dict, *, final: bool) -> dict:
        payload = {
            key: value
            for key, value in bar.items()
            if not key.startswith("_")
        }
        payload["bar_start"] = bar["time"]
        payload["bar_end"] = bar["time"] + timedelta(seconds=59, microseconds=999000)
        payload["final"] = final
        payload["status"] = "final" if final else "provisional"
        payload["revision"] = int(bar.get("_revision") or 0)
        return payload

    def pop_completed_futures_minute_bars(
        self,
        reference_time: datetime | None = None,
        finalization_delay: timedelta | None = None,
    ) -> list[dict]:
        """Return bars whose minute has remained stable for the configured delay."""
        if not self._futures_minute_complete_bars:
            return []

        if reference_time is None:
            reference_time = datetime.now(timezone.utc)
        if finalization_delay is None:
            finalization_delay = timedelta(seconds=FUTURES_MINUTE_COMPLETE_DELAY_SECONDS)
        completed = []
        for key, bar in list(self._futures_minute_complete_bars.items()):
            bar_end = bar["time"] + timedelta(seconds=59, microseconds=999000)
            if bar_end + finalization_delay <= reference_time:
                completed.append(self._minute_complete_payload(bar, final=True))
                del self._futures_minute_complete_bars[key]
                self._futures_minute_published_revisions.pop(key, None)

        completed.sort(key=lambda item: (item["symbol"], item["con_id"], item["time"]))
        return completed

    def pop_publishable_futures_minute_bars(
        self,
        reference_time: datetime | None = None,
        provisional_delay: timedelta | None = None,
        finalization_delay: timedelta | None = None,
    ) -> list[dict]:
        """Return provisional revisions quickly, then one final revision later."""
        if not self._futures_minute_complete_bars:
            return []

        if reference_time is None:
            reference_time = datetime.now(timezone.utc)
        if provisional_delay is None:
            provisional_delay = timedelta(seconds=FUTURES_MINUTE_COMPLETE_DELAY_SECONDS)
        if finalization_delay is None:
            finalization_delay = timedelta(seconds=FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS)

        publishable = []
        for key, bar in list(self._futures_minute_complete_bars.items()):
            bar_end = bar["time"] + timedelta(seconds=59, microseconds=999000)
            revision = int(bar.get("_revision") or 0)

            if bar_end + finalization_delay <= reference_time:
                publishable.append(self._minute_complete_payload(bar, final=True))
                del self._futures_minute_complete_bars[key]
                self._futures_minute_published_revisions.pop(key, None)
                continue

            if bar_end + provisional_delay <= reference_time:
                published_revision = self._futures_minute_published_revisions.get(key, 0)
                if revision > published_revision:
                    publishable.append(self._minute_complete_payload(bar, final=False))
                    self._futures_minute_published_revisions[key] = revision

        publishable.sort(
            key=lambda item: (item["symbol"], item["con_id"], item["time"], item["revision"])
        )
        return publishable

    async def flush(self):
        """Async flush to database."""
        async with self._lock:
            if (
                not self._buffer
                and not self._minute_bars
                and not self._futures_buffer
                and not self._futures_minute_bars
            ):
                return
            rows = list(self._buffer)
            minute_rows = list(self._minute_bars.values())
            futures_rows = list(self._futures_buffer)
            futures_minute_rows = list(self._futures_minute_bars.values())
            self._buffer.clear()
            self._minute_bars.clear()
            self._futures_buffer.clear()
            self._futures_minute_bars.clear()

        if rows:
            await self.writer.write_raw_ticks(rows)
        if minute_rows:
            await self.writer.upsert_minute_bars_from_live(minute_rows)
        if futures_rows:
            await self.writer.write_futures_ticks(futures_rows)
        if futures_minute_rows:
            await self.writer.upsert_futures_minute_bars_from_live(futures_minute_rows)


def should_publish_live_tick(payload_or_symbol) -> bool:
    """Only expose active futures ticks on the shared symbol-level tick channel."""
    if not isinstance(payload_or_symbol, dict):
        return True
    if payload_or_symbol.get("sec_type") != "FUT":
        return True
    return payload_or_symbol.get("role", "active") == "active"


def should_publish_futures_minute_complete(bar: dict) -> bool:
    """Only expose active-contract futures minute bars on the symbol-level channel."""
    if not isinstance(bar, dict):
        return True
    return bar.get("role", "active") == "active"


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


def _startup_backfill_cutoff(now: datetime | None = None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)
    return (
        now
        - timedelta(minutes=STARTUP_MINUTE_BACKFILL_STABLE_DELAY_MINUTES)
    ).replace(second=0, microsecond=0)


def _ib_end_datetime(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y%m%d %H:%M:%S UTC")


def _ib_duration(start: datetime, end: datetime) -> str:
    seconds = max(60, int((end - start).total_seconds()) + 60)
    if seconds >= 86400:
        return f"{max(1, (seconds + 86399) // 86400)} D"
    return f"{seconds} S"


def _product_from_subscription(sub: dict) -> ProductConfig:
    return ProductConfig(
        symbol=sub["symbol"],
        sec_type=sub["sec_type"],
        exchange=sub["exchange"],
        currency=sub["currency"],
    )


async def _detect_cash_startup_minute_gaps(
    pool,
    symbol: str,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime]]:
    threshold = timedelta(minutes=STARTUP_MINUTE_BACKFILL_GAP_THRESHOLD_MINUTES)
    rows = await pool.fetch(
        """
        WITH ordered AS (
            SELECT time,
                   LEAD(time) OVER (ORDER BY time) AS next_time
            FROM minute_bars
            WHERE symbol = $1
              AND time >= $2
              AND time <= $3
        )
        SELECT time AS gap_start, next_time AS gap_end
        FROM ordered
        WHERE next_time IS NOT NULL
          AND next_time - time > $4
        ORDER BY time
        """,
        symbol,
        start,
        end,
        threshold,
    )
    return [(r["gap_start"], r["gap_end"]) for r in rows]


async def _repair_cash_startup_minute_gaps(
    ib,
    pool,
    writer: MinuteBarWriter,
    product: ProductConfig,
    start: datetime,
    end: datetime,
) -> int:
    min_ts, max_ts, count = await writer.get_range(product.symbol, product.sec_type)
    ranges: list[tuple[datetime, datetime]] = []

    if count == 0 or max_ts is None:
        ranges.append((start, end))
    else:
        if min_ts is not None and min_ts > start:
            ranges.append((start, min(min_ts, end)))
        ranges.extend(
            await _detect_cash_startup_minute_gaps(
                pool,
                product.symbol,
                start,
                end,
            )
        )
        stale_threshold = timedelta(
            minutes=STARTUP_MINUTE_BACKFILL_GAP_THRESHOLD_MINUTES
        )
        if max_ts < end - stale_threshold:
            ranges.append((max(max_ts, start), end))

    if not ranges:
        logger.info("%s: no startup minute gaps found", product.symbol)
        return 0

    ranges.sort(key=lambda item: item[1], reverse=True)

    contract = await resolve_contract_async(
        ib,
        product.symbol,
        product.sec_type,
        product.exchange,
        product.currency,
    )
    if contract is None:
        logger.warning("%s: cannot resolve contract for startup backfill", product.symbol)
        return 0

    repaired = 0
    for gap_start, gap_end in ranges[:STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL]:
        if gap_end <= gap_start:
            continue
        logger.info(
            "%s: startup minute backfill %s..%s",
            product.symbol,
            gap_start,
            gap_end,
        )
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=_ib_end_datetime(gap_end),
            durationStr=_ib_duration(gap_start, gap_end),
            barSizeSetting="1 min",
            whatToShow=resolve_what_to_show(product.sec_type),
            useRTH=False,
            formatDate=1,
        )
        repaired += await writer.upsert_bars(product.symbol, bars)
        await asyncio.sleep(STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS)

    skipped = len(ranges) - STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL
    if skipped > 0:
        logger.warning(
            "%s: skipped %d startup minute gaps due to max-gaps limit",
            product.symbol,
            skipped,
        )
    return repaired


_STARTUP_FUTURES_QUARTERLY_MONTHS = frozenset({"03", "06", "09", "12"})
_STARTUP_FUTURES_ROLL_CONTRACT_MONTHS = {
    "SPI": _STARTUP_FUTURES_QUARTERLY_MONTHS,
    "MYM": _STARTUP_FUTURES_QUARTERLY_MONTHS,
    "MNQ": _STARTUP_FUTURES_QUARTERLY_MONTHS,
    "MES": _STARTUP_FUTURES_QUARTERLY_MONTHS,
    "N225M": _STARTUP_FUTURES_QUARTERLY_MONTHS,
    "10Y": _STARTUP_FUTURES_QUARTERLY_MONTHS,
    "ZC": frozenset({"03", "05", "07", "09", "12"}),
}


def _is_startup_roll_contract(symbol: str, contract) -> bool:
    months = _STARTUP_FUTURES_ROLL_CONTRACT_MONTHS.get(symbol)
    if not months:
        return True
    exp = (getattr(contract, "lastTradeDateOrContractMonth", None) or "0000")
    return len(exp) >= 6 and exp[4:6] in months


async def _resolve_startup_futures_contracts(ib, product: ProductConfig) -> list:
    try:
        details = await ib.reqContractDetailsAsync(
            Future(
                product.symbol,
                exchange=product.exchange,
                includeExpired=True,
            )
        )
    except Exception as exc:
        logger.warning("%s: failed to list futures contracts: %s", product.symbol, exc)
        return []

    contracts = [
        d.contract for d in details
        if _is_startup_roll_contract(product.symbol, d.contract)
    ]
    contracts.sort(key=lambda c: getattr(c, "lastTradeDateOrContractMonth", "") or "")
    return contracts


async def _repair_futures_startup_session_gaps(
    ib,
    writer: MinuteBarWriter,
    product: ProductConfig,
    start: datetime,
    end: datetime,
) -> None:
    gaps = await writer.detect_futures_session_gaps(
        product.symbol,
        start_date=start.date(),
        end_date=end.date(),
        min_minutes=STARTUP_MINUTE_BACKFILL_FUTURES_MIN_SESSION_MINUTES,
    )
    if not gaps:
        logger.info("%s: no startup futures session gaps found", product.symbol)
        return

    contracts = await _resolve_startup_futures_contracts(ib, product)
    contracts_by_con_id = {
        int(c.conId): c for c in contracts if getattr(c, "conId", None)
    }
    repaired = 0
    for gap in gaps[:STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL]:
        contract = contracts_by_con_id.get(int(gap["con_id"]))
        if contract is None:
            logger.warning(
                "%s: cannot resolve conId=%s for startup session repair",
                product.symbol,
                gap["con_id"],
            )
            continue
        contract.includeExpired = True
        logger.info(
            "%s %s conId=%s: startup session repair %s (%s..%s)",
            product.symbol,
            gap["local_symbol"],
            gap["con_id"],
            gap["session_date"],
            gap["session_start"],
            gap["session_end"],
        )
        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime=_ib_end_datetime(gap["session_end"]),
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow=resolve_what_to_show(product.sec_type),
            useRTH=False,
            formatDate=1,
        )
        await writer.upsert_futures_bars(product.symbol, contract, bars)
        repaired += 1
        await asyncio.sleep(STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS)

    skipped = len(gaps) - STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL
    if skipped > 0:
        logger.warning(
            "%s: skipped %d startup futures session gaps due to max-gaps limit",
            product.symbol,
            skipped,
        )
    logger.info(
        "%s: attempted startup repair for %d futures sessions",
        product.symbol,
        repaired,
    )


async def _repair_futures_startup_minute_gaps(
    ib,
    writer: MinuteBarWriter,
    product: ProductConfig,
    start: datetime,
    end: datetime,
) -> int:
    repaired = 0
    _, max_ts, count = await writer.get_range(product.symbol, product.sec_type)
    stale_threshold = timedelta(minutes=STARTUP_MINUTE_BACKFILL_GAP_THRESHOLD_MINUTES)
    if count == 0 or max_ts is None or max_ts < end - stale_threshold:
        contract = await resolve_contract_async(
            ib,
            product.symbol,
            product.sec_type,
            product.exchange,
            product.currency,
        )
        if contract is not None:
            gap_start = start if max_ts is None else max(max_ts, start)
            logger.info(
                "%s: startup active-futures minute backfill %s..%s",
                product.symbol,
                gap_start,
                end,
            )
            bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=_ib_end_datetime(end),
                durationStr=_ib_duration(gap_start, end),
                barSizeSetting="1 min",
                whatToShow=resolve_what_to_show(product.sec_type),
                useRTH=False,
                formatDate=1,
            )
            repaired += await writer.upsert_futures_bars(product.symbol, contract, bars)
            await asyncio.sleep(STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS)
        else:
            logger.warning("%s: cannot resolve active futures contract", product.symbol)
    else:
        logger.info("%s: no startup active-futures tail gap found", product.symbol)

    await _repair_futures_startup_session_gaps(ib, writer, product, start, end)
    return repaired


async def startup_minute_gap_backfill(client: IBKRClient, pool, symbols: list[dict]):
    if not STARTUP_MINUTE_BACKFILL_ENABLED:
        logger.info("Startup minute gap backfill disabled")
        return

    end = _startup_backfill_cutoff()
    start = end - timedelta(days=STARTUP_MINUTE_BACKFILL_LOOKBACK_DAYS)
    if end <= start:
        logger.warning("Startup minute gap backfill skipped: invalid time window")
        return

    writer = MinuteBarWriter(pool)
    logger.info(
        "Startup minute gap backfill scanning %s..%s for %d symbols",
        start,
        end,
        len(symbols),
    )
    ordered_symbols = sorted(
        symbols,
        key=lambda item: 1 if item.get("sec_type") == "FUT" else 0,
    )
    for sub in ordered_symbols:
        product = _product_from_subscription(sub)
        try:
            if product.sec_type == "FUT":
                await _repair_futures_startup_minute_gaps(
                    client.ib,
                    writer,
                    product,
                    start,
                    end,
                )
            else:
                await _repair_cash_startup_minute_gaps(
                    client.ib,
                    pool,
                    writer,
                    product,
                    start,
                    end,
                )
        except Exception as exc:
            logger.warning(
                "%s: startup minute gap backfill failed: %s",
                product.symbol,
                exc,
            )


async def reconnect_backfill_worker(event, client, pool, symbols):
    """重连成功后回填 minute gap，复用启动期的 gap 检测逻辑。

    用 asyncio.Event 驱动：Event 天然合并 gateway 半开风暴期的多次触发；
    若回填在途时又有新触发，下一轮 wait() 会立即返回再跑一次，不丢事件、不重叠。
    覆盖 gateway 断线重连与 10197 数据恢复两条路径（均经 _fire_reconnect_callbacks）。
    注：startup_minute_gap_backfill 内含 _repair_futures_startup_minute_gaps，
    因此期货 minute gap 也在此补齐；期货 live 重订阅仍由 LiveFuturesRuntime 负责。
    历史请求经 client._historical_lock 串行化，避免与周期性 daily 刷新循环并发触发 pacing。
    """
    while True:
        await event.wait()
        event.clear()
        try:
            if client.is_connected:
                logger.info("Reconnect detected: scanning for minute gaps to backfill")
                async with client._historical_lock:
                    await startup_minute_gap_backfill(client, pool, symbols)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Reconnect backfill error: {e}")


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

    generator = RollCalendarGenerator(pool, contract_source="live_contracts")
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


async def live_futures_contract_loop(runtime, symbols, interval: int):
    """Refresh live futures contract chain and maintain market data subscriptions."""
    while True:
        try:
            if runtime.client.is_connected:
                await runtime.refresh_contracts(symbols)
                await runtime.ensure_market_data()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Live futures contract loop error: {e}")
        await asyncio.sleep(interval)


async def live_futures_daily_loop(runtime, interval: int):
    """Refresh per-contract IBKR daily bars for live futures roll decisions."""
    while True:
        try:
            if runtime.client.is_connected:
                async with runtime.client._historical_lock:
                    await runtime.refresh_daily_bars()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Live futures daily loop error: {e}")
        await asyncio.sleep(interval)


async def live_futures_subscription_loop(runtime, interval: int = 60):
    """Ensure pending roll events can promote candidate subscriptions promptly."""
    while True:
        try:
            if runtime.client.is_connected:
                await runtime.ensure_market_data()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Live futures subscription loop error: {e}")
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


async def futures_minute_complete_loop(tick_buffer, pub):
    """Publish quick provisional bars, then final revisions after a grace window."""
    provisional_delay = timedelta(seconds=FUTURES_MINUTE_COMPLETE_DELAY_SECONDS)
    finalization_delay = timedelta(seconds=FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS)
    while True:
        await asyncio.sleep(1)
        try:
            completed_bars = tick_buffer.pop_publishable_futures_minute_bars(
                provisional_delay=provisional_delay,
                finalization_delay=finalization_delay,
            )
            for bar in completed_bars:
                if should_publish_futures_minute_complete(bar):
                    await pub.publish_futures_minute_complete(bar["symbol"], bar)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Futures minute-complete loop error: {e}")


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


async def order_sync_loop(client, writer, interval, refresh_event, gateway="live"):
    """周期性对账订单/成交，兜住实时事件收不到的部分。

    IBKR 的 execDetails/openOrder 实时事件只推给下单的那个 client（除非 IB Gateway
    配置了 Master API client ID），断线重连后也不会补推。reqExecutions 与
    reqAllOpenOrders 不受 clientId 限制，能拿到账户下所有程序的记录。

    注意 reqAllOpenOrders 只返回当前未完成的订单，两次轮询之间开平的短命订单抓不到；
    订单表的完整性仍然依赖 Master API client ID 带来的实时事件。成交表不受此限。
    """
    while True:
        try:
            if client.is_connected:
                fills = await client.ib.reqExecutionsAsync()
                await writer.sync_executions(fills)
                trades = await client.ib.reqAllOpenOrdersAsync()
                await writer.sync_orders(trades)
                if fills or trades:
                    logger.info(
                        f"Order sync ({gateway}): {len(fills)} fills, {len(trades)} open orders"
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Order sync loop ({gateway}) error: {e}")

        # 等待 interval 或被重连事件唤醒
        try:
            await asyncio.wait_for(refresh_event.wait(), timeout=interval)
            refresh_event.clear()
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
            
            # Rate limit backfills to prevent hitting IB API pacing limits and pinning disk I/O
            await asyncio.sleep(DAILY_BAR_BACKFILL_REQUEST_INTERVAL_SECONDS)


        logger.info("Daily bar backfill completed")
    except Exception as e:
        logger.error(f"Daily bar backfill error: {e}")


async def daily_bar_refresh_loop(client, writer, pool, daily_tracker):
    """Periodically refresh daily bars for non-futures active subscriptions."""
    # Run first backfill immediately
    async with client._historical_lock:
        await backfill_daily_bars(client, writer, pool, duration="100 D", daily_tracker=daily_tracker)

    while True:
        await asyncio.sleep(4 * 3600)  # Refresh every 4 hours
        try:
            # Refresh a wider window so late IBKR daily settlement/CONTFUT
            # revisions overwrite any live-tick partial bars saved earlier.
            async with client._historical_lock:
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


async def live_tick_publish_loop(queue: asyncio.Queue, pub: Publisher):
    while True:
        symbol, price, size, tick_time = await queue.get()
        try:
            await pub.publish_tick(symbol, price, size, tick_time)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Live tick publish error: {e}")
        finally:
            queue.task_done()


async def init_paper(pool, redis_client, writer, pub):
    try:
        from config import (
            PAPER_IB_HOST,
            PAPER_IB_PORT,
            PAPER_IB_CLIENT_ID,
            ACCOUNT_REFRESH_INTERVAL,
            ORDER_SYNC_INTERVAL,
        )
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
        paper_order_sync_event = asyncio.Event()
        paper_client.register_reconnect_handler(paper_order_sync_event.set)
        task3 = asyncio.create_task(
            order_sync_loop(
                paper_client, writer, ORDER_SYNC_INTERVAL,
                paper_order_sync_event, gateway="paper",
            ),
            name="paper_order_sync",
        )
        _paper_tasks.add(task3)
        task3.add_done_callback(_paper_tasks.discard)
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
    live_tick_queue: asyncio.Queue = asyncio.Queue(maxsize=LIVE_TICK_PUBLISH_QUEUE_MAX)
    live_tick_queue_drops = 0

    # Load the most recent daily bars from DB so tracker preserves OHLC across restarts
    symbols = await load_subscriptions(pool)
    await daily_tracker.load_from_db(pool, symbols)
    futures_symbol_set = {s["symbol"] for s in symbols if s.get("sec_type") == "FUT"}

    # Register tick-by-tick callbacks:
    # 1) Feed each tick into the buffer (for full DB persistence)
    # 2) Track today's daily OHLCV from real-time ticks
    # 3) Publish each tick in real-time via Redis (for frontend live display)
    def on_trade_tick(*args):
        nonlocal live_tick_queue_drops
        publish_live_tick = True
        if len(args) == 1 and isinstance(args[0], dict):
            payload = args[0]
            symbol = payload["symbol"]
            price = payload.get("last", payload.get("price"))
            size = payload.get("volume", payload.get("size", 0))
            tick_time = payload["time"]
            publish_live_tick = should_publish_live_tick(payload)
            tick_buffer.add_futures_tick(payload)
            completed_bars = tick_buffer.pop_completed_futures_minute_bars(tick_time)
            for bar in completed_bars:
                if should_publish_futures_minute_complete(bar):
                    t_complete = asyncio.ensure_future(
                        pub.publish_futures_minute_complete(bar["symbol"], bar)
                    )
                    t_complete.add_done_callback(_on_task_done)
        else:
            symbol, price, size, tick_time = args
            # Buffer the raw tick for batch DB write
            tick_buffer.add_tick(symbol, price, size, tick_time)

        # Buffer the raw tick for batch DB write
        # Track today's daily OHLCV from real-time ticks for non-futures only.
        if symbol not in futures_symbol_set:
            daily_tracker.on_tick(symbol, price, size, tick_time)
        # Async publish for real-time frontend (fire-and-forget)
        if publish_live_tick:
            item = (symbol, price, size, tick_time)
            try:
                live_tick_queue.put_nowait(item)
            except asyncio.QueueFull:
                try:
                    live_tick_queue.get_nowait()
                    live_tick_queue.task_done()
                except asyncio.QueueEmpty:
                    pass
                live_tick_queue.put_nowait(item)
                live_tick_queue_drops += 1
                if live_tick_queue_drops % 1000 == 1:
                    logger.warning(
                        "Dropped %s live tick publish events due to Redis backpressure",
                        live_tick_queue_drops,
                    )

    client.register_tick_handler(on_trade_tick)

    await client.connect_with_retry()

    # 同步历史成交（连接前已完成但未入库的记录），避免订单页/盈亏页落后于 IBKR。
    try:
        fills = await client.ib.reqExecutionsAsync()
        await writer.sync_executions(fills)
        logger.info(f"Synced {len(fills)} historical executions for live account")
    except Exception as e:
        logger.warning(f"Failed to sync live execution history: {e}")

    futures_runtime = LiveFuturesRuntime(client, writer, pool, pub)
    await futures_runtime.refresh_contracts(symbols)
    await startup_minute_gap_backfill(client, pool, symbols)
    await futures_runtime.ensure_market_data()
    await futures_runtime.refresh_daily_bars()

    # 重连回填：注册晚于首次 startup_minute_gap_backfill，确保启动期回填与
    # 重连回填不并发（首连时回调尚未注册，_has_connected_once 作冗余第二道闸）。
    reconnect_backfill_event = asyncio.Event()
    client.register_reconnect_handler(reconnect_backfill_event.set)

    # 重连后立即对账订单/成交：断线期间其他程序的成交不会被补推
    order_sync_event = asyncio.Event()
    client.register_reconnect_handler(order_sync_event.set)

    for s in symbols:
        if s["sec_type"] == "FUT":
            # Futures are managed by LiveFuturesRuntime so active/next concrete
            # contracts can coexist during roll windows.
            continue
        await client.subscribe(
            s["symbol"],
            s["sec_type"],
            s["exchange"],
            s["currency"],
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
        asyncio.create_task(
            live_tick_publish_loop(live_tick_queue, pub),
            name="live_tick_publish",
        ),
        asyncio.create_task(tick_loop(client, pub), name="tick_loop"),
        asyncio.create_task(tick_flush_loop(tick_buffer), name="tick_flush"),
        asyncio.create_task(
            futures_minute_complete_loop(tick_buffer, pub),
            name="futures_minute_complete",
        ),
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
            live_futures_contract_loop(
                futures_runtime,
                symbols,
                FUTURES_LIVE_CONTRACT_REFRESH_SECONDS,
            ),
            name="live_futures_contracts",
        ),
        asyncio.create_task(
            live_futures_daily_loop(
                futures_runtime,
                FUTURES_LIVE_DAILY_REFRESH_SECONDS,
            ),
            name="live_futures_daily",
        ),
        asyncio.create_task(
            live_futures_subscription_loop(futures_runtime),
            name="live_futures_subscriptions",
        ),
        asyncio.create_task(
            futures_roll_calendar_loop(pool),
            name="futures_roll_calendar",
        ),
        asyncio.create_task(
            order_command_listener(client, pub, channel="order:command:live"),
            name="live_order_listener",
        ),
        asyncio.create_task(
            reconnect_backfill_worker(
                reconnect_backfill_event, client, pool, symbols
            ),
            name="reconnect_backfill",
        ),
        asyncio.create_task(
            order_sync_loop(
                client, writer, ORDER_SYNC_INTERVAL, order_sync_event, gateway="live"
            ),
            name="live_order_sync",
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
