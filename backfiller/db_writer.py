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
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional

import asyncpg
from ib_insync import Contract

from backfiller.roll_calendar import session_start_time_utc


FUTURES_DAY_SESSION_WINDOWS = {
    "SPI": ("Australia/Sydney", time(10, 0), time(16, 10)),
    "N225M": ("Asia/Tokyo", time(9, 0), time(15, 24)),
}

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

_DAILY_INSERT_SQL = """\
INSERT INTO daily_bars (symbol, date_str, time, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (symbol, date_str) DO UPDATE SET
    time = EXCLUDED.time,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume\
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

_FUTURES_DAILY_INSERT_SQL = """\
INSERT INTO futures_daily_bars (
    symbol, con_id, date_str, time, local_symbol, trading_class,
    contract_month, last_trade_date, exchange, currency, multiplier,
    open, high, low, close, volume, bar_count
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17
)
ON CONFLICT (symbol, con_id, date_str) DO UPDATE SET
    time = EXCLUDED.time,
    local_symbol = EXCLUDED.local_symbol,
    trading_class = EXCLUDED.trading_class,
    contract_month = EXCLUDED.contract_month,
    last_trade_date = EXCLUDED.last_trade_date,
    exchange = EXCLUDED.exchange,
    currency = EXCLUDED.currency,
    multiplier = EXCLUDED.multiplier,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    bar_count = EXCLUDED.bar_count\
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


def _parse_daily_bar_date(val) -> Optional[tuple[str, datetime]]:
    """Return (YYYYMMDD, UTC midnight timestamp) for an IBKR daily bar date."""
    if isinstance(val, datetime):
        d = val.astimezone(timezone.utc).date() if val.tzinfo else val.date()
    elif isinstance(val, date):
        d = val
    elif isinstance(val, str):
        s = val.strip()
        if len(s) >= 8 and s[:8].isdigit():
            try:
                d = date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
            except ValueError:
                return None
        else:
            return None
    else:
        return None
    return d.strftime("%Y%m%d"), datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _next_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _previous_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


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

    async def upsert_daily_bars(self, symbol: str, bars: list) -> int:
        """Insert daily bars for a non-contract-specific product.

        This uses the existing ``daily_bars`` table keyed by
        ``(symbol, date_str)``.  Futures should use
        :meth:`upsert_futures_daily_bars` so each contract month remains
        auditable.
        """
        records: list[tuple] = []
        for bar in bars:
            parsed = _parse_daily_bar_date(bar.date)
            if parsed is None:
                continue
            date_str, ts = parsed
            records.append((
                symbol,
                date_str,
                ts,
                _clean_num(bar.open),
                _clean_num(bar.high),
                _clean_num(bar.low),
                _clean_num(bar.close),
                _clean_int(bar.volume),
            ))

        if not records:
            logger.debug("upsert_daily_bars(%s): no bars to insert", symbol)
            return 0

        async with self._pool.acquire() as conn:
            await conn.executemany(_DAILY_INSERT_SQL, records)

        logger.info("upsert_daily_bars(%s): attempted %d bars", symbol, len(records))
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

    async def has_futures_window_coverage(
        self,
        symbol: str,
        con_id: int,
        window_start: str,
        window_end: str,
    ) -> bool:
        """True when DB rows span the requested futures window dates.

        This is used only to bootstrap checkpoints for data downloaded before
        futures checkpointing existed.  If a window is only partially present,
        it returns False so the request is retried.
        """
        start_date = date.fromisoformat(window_start)
        end_date = date.fromisoformat(window_end)

        async with self._pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    SELECT MIN(time)::date AS min_date,
                           MAX(time)::date AS max_date,
                           COUNT(*) AS cnt
                    FROM futures_minute_bars
                    WHERE symbol = $1
                      AND con_id = $2
                      AND time::date BETWEEN $3 AND $4
                    """,
                    symbol,
                    con_id,
                    start_date,
                    end_date,
                )
            except asyncpg.UndefinedTableError:
                return False

        if row is None or row["cnt"] == 0:
            return False
        if not (row["min_date"] <= start_date and row["max_date"] >= end_date):
            return False

        session_gaps = await self.detect_futures_session_gaps(
            symbol,
            start_date=start_date,
            end_date=end_date,
            con_id=con_id,
        )
        return not session_gaps

    async def has_daily_window_coverage(
        self,
        symbol: str,
        window_start: str,
        window_end: str,
    ) -> bool:
        """True when ``daily_bars`` spans the requested date window."""
        start_date = _next_weekday(date.fromisoformat(window_start))
        end_date = _previous_weekday(date.fromisoformat(window_end))
        if start_date > end_date:
            return True

        async with self._pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    SELECT MIN(time)::date AS min_date,
                           MAX(time)::date AS max_date,
                           COUNT(*) AS cnt
                    FROM daily_bars
                    WHERE symbol = $1
                      AND time::date BETWEEN $2 AND $3
                    """,
                    symbol,
                    start_date,
                    end_date,
                )
            except asyncpg.UndefinedTableError:
                return False

        if row is None or row["cnt"] == 0:
            return False
        return row["min_date"] <= start_date and row["max_date"] >= end_date

    async def has_futures_daily_window_coverage(
        self,
        symbol: str,
        con_id: int,
        window_start: str,
        window_end: str,
    ) -> bool:
        """True when ``futures_daily_bars`` spans a contract date window."""
        start_date = _next_weekday(date.fromisoformat(window_start))
        end_date = _previous_weekday(date.fromisoformat(window_end))
        if start_date > end_date:
            return True

        async with self._pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    SELECT MIN(time)::date AS min_date,
                           MAX(time)::date AS max_date,
                           COUNT(*) AS cnt
                    FROM futures_daily_bars
                    WHERE symbol = $1
                      AND con_id = $2
                      AND time::date BETWEEN $3 AND $4
                    """,
                    symbol,
                    con_id,
                    start_date,
                    end_date,
                )
            except asyncpg.UndefinedTableError:
                return False

        if row is None or row["cnt"] == 0:
            return False
        return row["min_date"] <= start_date and row["max_date"] >= end_date

    async def upsert_futures_daily_bars(
        self, symbol: str, contract: Contract, bars: list
    ) -> int:
        """Insert daily bars for one futures contract."""
        con_id = getattr(contract, "conId", None)
        if con_id is None:
            raise ValueError(f"{symbol}: futures contract is missing conId")

        records: list[tuple] = []
        for bar in bars:
            parsed = _parse_daily_bar_date(bar.date)
            if parsed is None:
                continue
            date_str, ts = parsed
            records.append((
                symbol,
                int(con_id),
                date_str,
                ts,
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
                "upsert_futures_daily_bars(%s conId=%s): no bars to insert",
                symbol,
                con_id,
            )
            return 0

        async with self._pool.acquire() as conn:
            await conn.executemany(_FUTURES_DAILY_INSERT_SQL, records)

        logger.info(
            "upsert_futures_daily_bars(%s conId=%s exp=%s): attempted %d bars",
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

    async def detect_futures_session_gaps(
        self,
        symbol: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        con_id: Optional[int] = None,
        min_minutes: int = 300,
    ) -> list[dict]:
        """Find futures sessions whose minute bars are clearly incomplete.

        Timestamp-only gap checks miss products whose sessions cross UTC dates.
        This uses normalized daily futures bars as the expected-session source
        and counts minute bars in the matching exchange session window.

        For products where backtests depend on the local day session, it also
        checks the strategy-critical day window.  This catches the common
        failure mode where the overnight bars exist but the local day session
        is missing.
        """
        filters = ["symbol = $1"]
        params: list[object] = [symbol]
        if start_date is not None:
            params.append(start_date)
            filters.append(f"session_date >= ${len(params)}")
        if end_date is not None:
            params.append(end_date)
            filters.append(f"session_date <= ${len(params)}")
        if con_id is not None:
            params.append(con_id)
            filters.append(f"con_id = ${len(params)}")

        where = " AND ".join(filters)
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT session_date, con_id, local_symbol, contract_month,
                       volume, bar_count
                FROM futures_daily_bars_session_normalized
                WHERE {where}
                  AND (COALESCE(volume, 0) > 0 OR COALESCE(bar_count, 0) > 0)
                ORDER BY con_id, session_date
                """,
                *params,
            )

            gaps: list[dict] = []
            for row in rows:
                session_date = row["session_date"]
                session_start = session_start_time_utc(symbol, session_date)
                session_end = session_start + timedelta(days=1)
                minute_row = await conn.fetchrow(
                    """
                    SELECT MIN(time) AS min_time,
                           MAX(time) AS max_time,
                           COUNT(*) AS minute_count,
                           COALESCE(SUM(volume), 0) AS minute_volume,
                           COALESCE(SUM(bar_count), 0) AS minute_bar_count
                    FROM futures_minute_bars
                    WHERE symbol = $1
                      AND con_id = $2
                      AND time >= $3
                      AND time < $4
                    """,
                    symbol,
                    row["con_id"],
                    session_start,
                    session_end,
                )
                minute_count = int(minute_row["minute_count"] or 0)
                day_session_count = None
                day_session_start = None
                day_session_end = None
                day_window = FUTURES_DAY_SESSION_WINDOWS.get(symbol)
                if day_window is not None:
                    tz_name, start_time, end_time = day_window
                    day_row = await conn.fetchrow(
                        """
                        SELECT COUNT(*) AS minute_count,
                               MIN(time) AS min_time,
                               MAX(time) AS max_time
                        FROM futures_minute_bars
                        WHERE symbol = $1
                          AND con_id = $2
                          AND (time AT TIME ZONE $3)::date = $4
                          AND (time AT TIME ZONE $3)::time >= $5
                          AND (time AT TIME ZONE $3)::time <= $6
                        """,
                        symbol,
                        row["con_id"],
                        tz_name,
                        session_date,
                        start_time,
                        end_time,
                    )
                    day_session_count = int(day_row["minute_count"] or 0)
                    day_session_start = day_row["min_time"]
                    day_session_end = day_row["max_time"]

                is_session_complete = minute_count >= min_minutes
                is_day_session_complete = (
                    day_session_count is None
                    or day_session_count >= min_minutes
                )
                if is_session_complete and is_day_session_complete:
                    continue
                gaps.append(
                    {
                        "symbol": symbol,
                        "con_id": row["con_id"],
                        "local_symbol": row["local_symbol"],
                        "contract_month": row["contract_month"],
                        "session_date": session_date,
                        "session_start": session_start,
                        "session_end": session_end,
                        "minute_count": minute_count,
                        "minute_min_time": minute_row["min_time"],
                        "minute_max_time": minute_row["max_time"],
                        "day_session_count": day_session_count,
                        "day_session_min_time": day_session_start,
                        "day_session_max_time": day_session_end,
                        "daily_volume": row["volume"],
                        "daily_bar_count": row["bar_count"],
                        "minute_volume": minute_row["minute_volume"],
                        "minute_bar_count": minute_row["minute_bar_count"],
                    }
                )
        return gaps
