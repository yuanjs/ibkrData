from datetime import datetime, timezone

from auth import require_auth
from dateutil import parser
from fastapi import APIRouter, Depends, HTTPException, Query

from db import get_pool
from backfiller.roll_sync import ensure_futures_roll_calendar

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
    return [dict(r) for r in rows]


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
