"""Database writer for minute-bar data (TimescaleDB).

Provides the :class:`MinuteBarWriter` class which handles upserting raw
ib_insync ``Bar`` objects into the ``minute_bars`` hypertable, querying
the stored range, and detecting time gaps for gap-filling logic.

For futures, raw history is written to ``futures_minute_bars`` keyed by
``(symbol, con_id, time)``.  Keeping contract identity in the key prevents
different contract months from being flattened into an unauditable product
stream.
"""

import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import asyncpg
from ib_insync import Contract

logger = logging.getLogger(__name__)


def _clean_num(val):
    """Return *val* as float, or None if NaN/Inf/None."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


def _clean_int(val):
    """Return *val* as int, or None if NaN/Inf/None."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return int(f)
    except (ValueError, TypeError):
        return None

_INSERT_SQL = """\
INSERT INTO minute_bars (time, symbol, open, high, low, close, volume, bar_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (symbol, time) DO NOTHING\
"""

_FUTURES_INSERT_SQL = """\
INSERT INTO futures_minute_bars (
    time, symbol, con_id, local_symbol, trading_class, contract_month,
    last_trade_date, exchange, currency, multiplier,
    open, high, low, close, volume, bar_count
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16
)
ON CONFLICT (symbol, con_id, time) DO NOTHING\
"""


def _contract_month(contract: Contract) -> Optional[str]:
    raw = contract.lastTradeDateOrContractMonth or None
    return raw[:6] if raw and len(raw) >= 6 else raw


def _last_trade_date(contract: Contract) -> Optional[date]:
    raw = contract.lastTradeDateOrContractMonth or ""
    if len(raw) < 8:
        return None
    try:
        return date.fromisoformat(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    except ValueError:
        return None


class MinuteBarWriter:
    """Persist minute-bar OHLCV data to TimescaleDB.

    Parameters
    ----------
    pool:
        An open :class:`asyncpg.Pool` connected to the target database.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # factory
    # ------------------------------------------------------------------

    @staticmethod
    async def create_pool(dsn: str) -> asyncpg.Pool:
        """Create and return a new connection pool from *dsn*.

        Typical usage::

            pool = await MinuteBarWriter.create_pool("postgresql://...")
            writer = MinuteBarWriter(pool)
        """
        return await asyncpg.create_pool(dsn)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    async def upsert_bars(self, symbol: str, bars: list) -> int:
        """Insert bars from an ib_insync ``BarList`` into ``minute_bars``.

        Each element in *bars* should be an ib_insync ``Bar`` object with
        attributes ``date``, ``open``, ``high``, ``low``, ``close``,
        ``volume``, and ``barCount``.

        Timezone handling
        -----------------
        If ``bar.date`` is timezone-naive (no ``tzinfo``), UTC is assumed
        and the timestamp is coerced via
        ``bar.date.replace(tzinfo=timezone.utc)``.

        Idempotency
        -----------
        ``ON CONFLICT (symbol, time) DO NOTHING`` is used so that re-running
        the same bars does not produce duplicates.

        Parameters
        ----------
        symbol:
            The product symbol (e.g. ``"SPI"``, ``"USD.JPY"``).
        bars:
            Iterable of ib_insync ``Bar`` objects.

        Returns
        -------
        int
            Number of bar records prepared for insert (may over-count when
            rows hit the ``ON CONFLICT`` no-op path — use for progress
            tracking rather than exact counts).
        """
        records: list[tuple] = []
        for bar in bars:
            ts: datetime = bar.date
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            _open = _clean_num(bar.open)
            _high = _clean_num(bar.high)
            _low = _clean_num(bar.low)
            _close = _clean_num(bar.close)
            _volume = _clean_int(bar.volume)
            records.append((
                ts,
                symbol,
                _open,
                _high,
                _low,
                _close,
                _volume,
                bar.barCount,
            ))

        if not records:
            logger.debug("upsert_bars(%s): no bars to insert", symbol)
            return 0

        async with self._pool.acquire() as conn:
            await conn.executemany(_INSERT_SQL, records)

        logger.info("upsert_bars(%s): attempted %d bars", symbol, len(records))
        return len(records)

    async def upsert_futures_bars(
        self, symbol: str, contract: Contract, bars: list
    ) -> int:
        """Insert raw futures bars with individual contract identity.

        ``conId`` is part of the database key, so overlapping contract months
        can coexist.  This is the raw source needed for later continuous
        futures roll selection and back-adjustment.
        """
        con_id = getattr(contract, "conId", None)
        if con_id is None:
            raise ValueError(f"{symbol}: futures contract is missing conId")

        records: list[tuple] = []
        for bar in bars:
            ts: datetime = bar.date
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            records.append((
                ts,
                symbol,
                int(con_id),
                getattr(contract, "localSymbol", None) or None,
                getattr(contract, "tradingClass", None) or None,
                _contract_month(contract),
                _last_trade_date(contract),
                getattr(contract, "exchange", None) or None,
                getattr(contract, "currency", None) or None,
                getattr(contract, "multiplier", None) or None,
                _clean_num(bar.open),
                _clean_num(bar.high),
                _clean_num(bar.low),
                _clean_num(bar.close),
                _clean_int(bar.volume),
                _clean_int(getattr(bar, "barCount", None)),
            ))

        if not records:
            logger.debug(
                "upsert_futures_bars(%s conId=%s): no bars to insert",
                symbol,
                con_id,
            )
            return 0

        async with self._pool.acquire() as conn:
            await conn.executemany(_FUTURES_INSERT_SQL, records)

        logger.info(
            "upsert_futures_bars(%s conId=%s exp=%s): attempted %d bars",
            symbol,
            con_id,
            contract.lastTradeDateOrContractMonth,
            len(records),
        )
        return len(records)

    async def get_range(
        self, symbol: str, sec_type: Optional[str] = None
    ) -> tuple[Optional[datetime], Optional[datetime], int]:
        """Return summary time range and row count for *symbol*.

        Returns
        -------
        tuple[datetime | None, datetime | None, int]
            ``(min_time, max_time, row_count)``.
            If no data exists both datetimes are ``None``.
        """
        table = "futures_minute_bars" if sec_type == "FUT" else "minute_bars"
        async with self._pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    f"SELECT MIN(time) AS min, MAX(time) AS max, COUNT(*) AS cnt "
                    f"FROM {table} WHERE symbol = $1",
                    symbol,
                )
            except asyncpg.UndefinedTableError:
                row = None

        if row is None:
            return None, None, 0

        min_time: Optional[datetime] = row["min"]
        max_time: Optional[datetime] = row["max"]
        count: int = row["cnt"]
        return min_time, max_time, count

    async def detect_gaps(
        self,
        symbol: str,
        threshold_minutes: int = 3,
        sec_type: Optional[str] = None,
    ) -> list[dict]:
        """Find gaps between consecutive bars exceeding *threshold_minutes*.

        Uses a window-function approach (``LEAD``) to compare each bar's
        timestamp with the next one for the given symbol.

        Parameters
        ----------
        symbol:
            Product symbol to inspect.
        threshold_minutes:
            Minimum gap duration (in minutes) to report. Defaults to 3.

        Returns
        -------
        list[dict]
            Each dict has keys ``gap_start``, ``gap_end`` (``datetime``),
            and ``diff_minutes`` (``float``).  Empty list means no gaps
            exceeding the threshold were found.
        """
        threshold = timedelta(minutes=threshold_minutes)

        table = "futures_minute_bars" if sec_type == "FUT" else "minute_bars"
        partition = "PARTITION BY con_id " if sec_type == "FUT" else ""
        async with self._pool.acquire() as conn:
            try:
                rows = await conn.fetch(
                    f"""
                WITH gaps AS (
                    SELECT time AS gap_start,
                           LEAD(time) OVER ({partition}ORDER BY time) AS gap_end
                    FROM {table}
                    WHERE symbol = $1
                )
                SELECT gap_start, gap_end,
                       EXTRACT(EPOCH FROM (gap_end - gap_start)) / 60
                           AS diff_minutes
                FROM gaps
                WHERE gap_end IS NOT NULL
                  AND (gap_end - gap_start) > $2
                ORDER BY gap_start
                """,
                    symbol,
                    threshold,
                )
            except asyncpg.UndefinedTableError:
                rows = []

        return [
            {
                "gap_start": r["gap_start"],
                "gap_end": r["gap_end"],
                "diff_minutes": float(r["diff_minutes"]),
            }
            for r in rows
        ]
