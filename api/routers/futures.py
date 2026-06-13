from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from auth import require_auth
from dateutil import parser
from fastapi import APIRouter, Depends, HTTPException, Query

from db import get_pool
from backfiller.roll_sync import ensure_futures_roll_calendar
from collector.config import PRODUCT_ROLL_CONFIG

router = APIRouter(prefix="/api/futures", dependencies=[Depends(require_auth)])

_DAILY_ADJUSTMENTS = {"raw", "back_adjusted", "ratio_adjusted"}
_MINUTE_MODES = {"active_raw", "adjusted"}


def _parse_datetime(value: str, name: str) -> datetime:
    try:
        parsed = parser.isoparse(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {name}: {exc}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_optional_datetime(value: str | None, name: str) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    return _parse_datetime(value, name)


def _next_weekday(d: date) -> date:
    current = d + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _effective_futures_session_date(symbol: str, ts: datetime) -> date:
    config = PRODUCT_ROLL_CONFIG.get(symbol)
    if not config:
        return ts.astimezone(timezone.utc).date()

    local_dt = ts.astimezone(ZoneInfo(config["timezone"]))
    if local_dt.weekday() >= 5:
        return _next_weekday(local_dt.date() - timedelta(days=1))
    if (
        local_dt.hour > config["roll_hour"]
        or (
            local_dt.hour == config["roll_hour"]
            and local_dt.minute >= config["roll_minute"]
        )
    ):
        return _next_weekday(local_dt.date())
    return local_dt.date()


def _daily_time(session_date: date) -> datetime:
    return datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        12,
        tzinfo=timezone.utc,
    )


async def _append_live_partial_daily(
    pool,
    rows: list[dict],
    *,
    symbol: str,
    start_date: date,
    as_of: datetime,
    as_of_session_date: date,
) -> list[dict]:
    """Append missing current-session daily bars from live futures minutes.

    This is display/API-only compensation for the current incomplete session.
    Roll volume still comes from IBKR per-contract daily bars in
    futures_daily_bars.
    """
    active = await pool.fetchrow(
        "SELECT * FROM active_futures_contract_asof($1, $2)",
        symbol,
        as_of,
    )
    if active is None:
        return rows
    active = dict(active)
    if active.get("con_id") is None:
        return rows

    con_id = int(active["con_id"])
    existing_sessions = {
        r["session_date"]
        for r in rows
        if r.get("session_date") is not None
    }

    daily_rows = await pool.fetch(
        """
        SELECT time, date_str, session_date, symbol, open, high, low, close,
               volume, bar_count, con_id, contract_month, local_symbol,
               trading_class, exchange, currency, multiplier
        FROM futures_daily_bars_session_normalized
        WHERE symbol = $1
          AND con_id = $2
          AND session_date >= $3
          AND session_date <= $4
        ORDER BY session_date
        """,
        symbol,
        con_id,
        start_date,
        as_of_session_date,
    )
    appended_daily = []
    for raw_record in daily_rows:
        record = dict(raw_record)
        session_date = record.get("session_date")
        if session_date is None or session_date in existing_sessions:
            continue
        existing_sessions.add(session_date)
        appended_daily.append(
            {
                **record,
                "source_con_id": record.get("con_id"),
                "roll_event_id": active.get("roll_event_id"),
                "roll_time": None,
                "known_at": None,
                "decision_session_date": None,
                "price_session_date": None,
                "segment_start": None,
                "segment_end": None,
                "is_roll_date": False,
                "adjustment_value": 0,
                "adjustment_ratio": 1,
                "adjustment_method": "live_daily_asof",
                "is_live_partial": True,
            }
        )

    minute_start = as_of - timedelta(days=7)
    minute_rows = await pool.fetch(
        """
        SELECT time, open, high, low, close, volume, bar_count,
               con_id, contract_month, local_symbol, trading_class,
               exchange, currency, multiplier
        FROM futures_minute_bars
        WHERE symbol = $1
          AND con_id = $2
          AND time >= $3
          AND time <= $4
        ORDER BY time
        """,
        symbol,
        con_id,
        minute_start,
        as_of,
    )
    if not minute_rows and not appended_daily:
        return rows
    partials: dict[date, dict] = {}
    for raw_record in minute_rows:
        record = dict(raw_record)
        if (
            record.get("con_id") is None
            or int(record["con_id"]) != con_id
            or record.get("time") is None
            or record.get("open") is None
            or record.get("close") is None
        ):
            continue
        session_date = _effective_futures_session_date(symbol, record["time"])
        if session_date < start_date or session_date > as_of_session_date:
            continue
        if session_date in existing_sessions:
            continue

        bar = partials.get(session_date)
        volume = int(record["volume"] or 0)
        bar_count = int(record.get("bar_count") or 0)
        if bar is None:
            partials[session_date] = {
                "time": _daily_time(session_date),
                "date_str": session_date.strftime("%Y%m%d"),
                "session_date": session_date,
                "symbol": symbol,
                "open": record["open"],
                "high": record["high"],
                "low": record["low"],
                "close": record["close"],
                "volume": volume,
                "bar_count": bar_count,
                "source_con_id": record["con_id"],
                "contract_month": record["contract_month"],
                "local_symbol": record["local_symbol"],
                "trading_class": record["trading_class"],
                "exchange": record["exchange"],
                "currency": record["currency"],
                "multiplier": record["multiplier"],
                "roll_event_id": active.get("roll_event_id"),
                "roll_time": None,
                "known_at": None,
                "decision_session_date": None,
                "price_session_date": None,
                "segment_start": None,
                "segment_end": None,
                "is_roll_date": False,
                "adjustment_value": 0,
                "adjustment_ratio": 1,
                "adjustment_method": "live_partial_minute",
                "is_live_partial": True,
            }
            continue

        bar["high"] = max(bar["high"], record["high"])
        bar["low"] = min(bar["low"], record["low"])
        bar["close"] = record["close"]
        bar["volume"] = (bar.get("volume") or 0) + volume
        bar["bar_count"] = (bar.get("bar_count") or 0) + bar_count

    if not partials:
        if not appended_daily:
            return rows
        merged = [*rows, *appended_daily]
        merged.sort(key=lambda r: r["session_date"])
        return merged

    merged = [*rows, *appended_daily, *partials.values()]
    merged.sort(key=lambda r: r["session_date"])
    return merged


@router.get("/{symbol}/active-contract")
async def get_active_contract(symbol: str, as_of: str | None = None):
    dt_as_of = _parse_optional_datetime(as_of, "as_of")

    pool = await get_pool()
    await ensure_futures_roll_calendar(pool, symbol, as_of=dt_as_of)
    row = await pool.fetchrow(
        "SELECT * FROM active_futures_contract_asof($1, $2)",
        symbol,
        dt_as_of,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"No active contract for {symbol}")
    return dict(row)


@router.get("/{symbol}/daily")
async def get_futures_daily(
    symbol: str,
    start: str,
    as_of: str | None = None,
    adjustment: str = "back_adjusted",
    include_live_partial: bool = False,
    limit: int | None = Query(default=None, ge=1, le=5000),
):
    if adjustment not in _DAILY_ADJUSTMENTS:
        raise HTTPException(status_code=400, detail=f"Invalid adjustment: {adjustment}")

    dt_start = _parse_datetime(start, "start")
    dt_as_of = _parse_optional_datetime(as_of, "as_of")
    if dt_start > dt_as_of:
        raise HTTPException(status_code=400, detail="start must be before as_of")

    pool = await get_pool()
    await ensure_futures_roll_calendar(pool, symbol, as_of=dt_as_of)
    if limit is None:
        rows = await pool.fetch(
            "SELECT * FROM continuous_futures_daily_asof($1, $2, $3, $4)",
            symbol,
            dt_start.date(),
            dt_as_of.date(),
            adjustment,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT *
            FROM (
                SELECT *
                FROM continuous_futures_daily_asof($1, $2, $3, $4)
                ORDER BY session_date DESC
                LIMIT $5
            ) limited_daily
            ORDER BY session_date
            """,
            symbol,
            dt_start.date(),
            dt_as_of.date(),
            adjustment,
            limit,
        )
    result = [dict(r) for r in rows]
    if include_live_partial:
        result = await _append_live_partial_daily(
            pool,
            result,
            symbol=symbol,
            start_date=dt_start.date(),
            as_of=dt_as_of,
            as_of_session_date=_effective_futures_session_date(symbol, dt_as_of),
        )
    if limit is not None and len(result) > limit:
        result = result[-limit:]
    return result


@router.get("/{symbol}/minute")
async def get_futures_minute(
    symbol: str,
    start: str,
    end: str,
    mode: str = "active_raw",
    as_of: str | None = None,
):
    if mode not in _MINUTE_MODES:
        raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}")

    dt_start = _parse_datetime(start, "start")
    dt_end = _parse_datetime(end, "end")
    dt_as_of = _parse_optional_datetime(as_of, "as_of")
    if dt_start >= dt_end:
        raise HTTPException(status_code=400, detail="start must be before end")

    pool = await get_pool()
    await ensure_futures_roll_calendar(pool, symbol, as_of=dt_as_of)
    if mode == "active_raw":
        rows = await pool.fetch(
            "SELECT * FROM continuous_futures_minute_asof_raw($1, $2, $3)",
            symbol,
            dt_start,
            dt_end,
        )
    else:
        rows = await pool.fetch(
            "SELECT * FROM continuous_futures_minute_asof_adjusted($1, $2, $3, $4, $5)",
            symbol,
            dt_start,
            dt_end,
            dt_as_of,
            "back_adjusted",
        )
    return [dict(r) for r in rows]


@router.get("/{symbol}/roll-events")
async def get_roll_events(symbol: str, start: str | None = None, end: str | None = None):
    dt_start = _parse_datetime(start, "start") if start is not None else None
    dt_end = _parse_datetime(end, "end") if end is not None else None
    if dt_start is not None and dt_end is not None and dt_start >= dt_end:
        raise HTTPException(status_code=400, detail="start must be before end")

    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT *
        FROM futures_roll_events_asof
        WHERE symbol = $1
          AND ($2::timestamptz IS NULL OR effective_roll_time >= $2)
          AND ($3::timestamptz IS NULL OR effective_roll_time < $3)
        ORDER BY effective_roll_time
        """,
        symbol,
        dt_start,
        dt_end,
    )
    return [dict(r) for r in rows]


@router.get("/{symbol}/roll-state")
async def get_roll_state(symbol: str):
    now = datetime.now(timezone.utc)
    pool = await get_pool()
    await ensure_futures_roll_calendar(pool, symbol, as_of=now)

    active = await pool.fetchrow(
        "SELECT * FROM active_futures_contract_asof($1, $2)",
        symbol,
        now,
    )
    if active is None:
        raise HTTPException(status_code=404, detail=f"No active contract for {symbol}")

    previous_roll = await pool.fetchrow(
        """
        SELECT *
        FROM futures_roll_events_asof
        WHERE symbol = $1
          AND known_at <= $2
          AND effective_roll_time <= $2
        ORDER BY effective_roll_time DESC, known_at DESC, id DESC
        LIMIT 1
        """,
        symbol,
        now,
    )
    next_roll = await pool.fetchrow(
        """
        SELECT *
        FROM futures_roll_events_asof
        WHERE symbol = $1
          AND known_at <= $2
          AND effective_roll_time > $2
        ORDER BY effective_roll_time ASC, known_at DESC, id ASC
        LIMIT 1
        """,
        symbol,
        now,
    )

    return {
        "symbol": symbol,
        "as_of": now,
        "active": dict(active),
        "previous_roll": dict(previous_roll) if previous_roll else None,
        "next_roll": dict(next_roll) if next_roll else None,
    }
