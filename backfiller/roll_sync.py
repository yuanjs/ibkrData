from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from backfiller.roll_calendar import RollCalendarGenerator
from collector.config import (
    FUTURES_ROLL_CALENDAR_AFTER_SESSION_MINUTES,
    FUTURES_ROLL_CALENDAR_COMMODITY_SAFETY_DAYS,
    FUTURES_ROLL_CALENDAR_CONFIRM_DAYS,
    FUTURES_ROLL_CALENDAR_INDEX_SAFETY_DAYS,
    PRODUCT_ROLL_CONFIG,
)

logger = logging.getLogger(__name__)

COMMODITY_ROLL_SYMBOLS = {"HG", "ZC"}
ROLL_CALENDAR_LOCK_KEY = 817_260_611_001
_LAST_SYNCED_SESSION: dict[str, date] = {}


async def sync_futures_contracts_from_daily_bars(pool, symbol: str) -> None:
    """Backfill futures_contracts metadata from historical daily bars."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO futures_contracts (
                symbol, con_id, local_symbol, trading_class, contract_month,
                last_trade_date, exchange, currency, multiplier, source,
                first_seen_at, last_seen_at
            )
            SELECT
                symbol,
                con_id,
                (array_agg(local_symbol ORDER BY time DESC))[1],
                (array_agg(trading_class ORDER BY time DESC))[1],
                contract_month,
                MAX(last_trade_date),
                (array_agg(exchange ORDER BY time DESC))[1],
                (array_agg(currency ORDER BY time DESC))[1],
                (array_agg(multiplier ORDER BY time DESC))[1],
                'historical_daily',
                MIN(created_at),
                MAX(created_at)
            FROM futures_daily_bars
            WHERE symbol = $1
            GROUP BY symbol, con_id, contract_month
            ON CONFLICT (symbol, con_id) DO UPDATE SET
                local_symbol = COALESCE(
                    futures_contracts.local_symbol,
                    EXCLUDED.local_symbol
                ),
                trading_class = COALESCE(
                    futures_contracts.trading_class,
                    EXCLUDED.trading_class
                ),
                contract_month = COALESCE(
                    futures_contracts.contract_month,
                    EXCLUDED.contract_month
                ),
                last_trade_date = COALESCE(
                    futures_contracts.last_trade_date,
                    EXCLUDED.last_trade_date
                ),
                exchange = COALESCE(
                    futures_contracts.exchange,
                    EXCLUDED.exchange
                ),
                currency = COALESCE(
                    futures_contracts.currency,
                    EXCLUDED.currency
                ),
                multiplier = COALESCE(
                    futures_contracts.multiplier,
                    EXCLUDED.multiplier
                ),
                last_seen_at = GREATEST(
                    futures_contracts.last_seen_at,
                    EXCLUDED.last_seen_at
                )
            """,
            symbol,
        )


def roll_calendar_safety_days(symbol: str) -> int:
    if symbol in COMMODITY_ROLL_SYMBOLS:
        return FUTURES_ROLL_CALENDAR_COMMODITY_SAFETY_DAYS
    return FUTURES_ROLL_CALENDAR_INDEX_SAFETY_DAYS


def roll_calendar_ready_session_date(
    symbol: str,
    now_utc: datetime,
) -> tuple[date | None, bool]:
    config = PRODUCT_ROLL_CONFIG.get(symbol)
    if not config:
        return None, False

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


async def ensure_futures_roll_calendar(
    pool,
    symbol: str,
    *,
    as_of: datetime | None = None,
) -> bool:
    symbol = str(symbol).upper()
    now = as_of or datetime.now(timezone.utc)
    session_date, ready = roll_calendar_ready_session_date(symbol, now)
    if not ready or session_date is None:
        return False

    if _LAST_SYNCED_SESSION.get(symbol) == session_date:
        return False

    generator = RollCalendarGenerator(pool, contract_source="live_contracts")
    safety_days = roll_calendar_safety_days(symbol)

    async with pool.acquire() as conn:
        async with conn.transaction():
            locked = await conn.fetchval(
                "SELECT pg_try_advisory_xact_lock($1)",
                ROLL_CALENDAR_LOCK_KEY,
            )
            if not locked:
                logger.info(
                    "Skipping on-demand futures roll calendar generation for %s; lock is held elsewhere",
                    symbol,
                )
                return False

            await sync_futures_contracts_from_daily_bars(pool, symbol)
            if symbol == "HG":
                logger.info(
                    "Skipping on-demand HG roll calendar generation; using curated safety-day events",
                )
                _LAST_SYNCED_SESSION[symbol] = session_date
                return False

            await generator.generate_asof(
                symbol,
                safety_days_before_expiry=safety_days,
                min_confirm_days=FUTURES_ROLL_CALENDAR_CONFIRM_DAYS,
                replace=False,
                dry_run=False,
            )

    _LAST_SYNCED_SESSION[symbol] = session_date
    logger.info(
        "Ensured as-of futures roll calendar for %s before serving API data (session=%s)",
        symbol,
        session_date,
    )
    return True
