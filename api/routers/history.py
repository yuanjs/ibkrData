import csv
import io
from datetime import datetime, timedelta, timezone

from auth import require_auth
from dateutil import parser
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from db import get_pool

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])

# Safe mapping of interval values to SQL time_bucket strings
_BUCKET_MAP = {
    "1s": timedelta(seconds=1),
    "5s": timedelta(seconds=5),
    "10s": timedelta(seconds=10),
    "1m": timedelta(minutes=1),
    "1min": timedelta(minutes=1),
    "2m": timedelta(minutes=2),
    "3m": timedelta(minutes=3),
    "5m": timedelta(minutes=5),
    "5min": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
    "1w": timedelta(weeks=1),
}

_MINUTE_BAR_INTERVALS = {
    "1m",
    "1min",
    "2m",
    "3m",
    "5m",
    "5min",
    "15m",
    "1h",
    "4h",
}


async def _fetch_tick_bars(pool, bucket: timedelta, symbol: str, start: datetime, end: datetime):
    if start > end:
        return []
    return await pool.fetch(
        "SELECT time_bucket($1, time) AS time, "
        "first(last,time) AS open, max(last) AS high, min(last) AS low, "
        "last(last,time) AS close, sum(volume) AS volume "
        "FROM ticks WHERE symbol=$2 AND time BETWEEN $3 AND $4 "
        "GROUP BY 1 ORDER BY 1",
        bucket,
        symbol,
        start,
        end,
    )


def _merge_ohlcv_rows(rows):
    merged = {}
    for row in rows:
        item = dict(row)
        key = item["time"]
        existing = merged.get(key)
        if existing is None:
            merged[key] = item
            continue

        existing["high"] = max(existing["high"], item["high"])
        existing["low"] = min(existing["low"], item["low"])
        existing["close"] = item["close"]
        existing["volume"] = (existing.get("volume") or 0) + (item.get("volume") or 0)

    return [merged[key] for key in sorted(merged)]


@router.get("/history/{symbol}")
async def get_history(symbol: str, start: str, end: str, interval: str = "1min"):
    # Robustly convert ISO strings to UTC datetime objects for asyncpg
    try:
        dt_start = parser.isoparse(start)
        dt_end = parser.isoparse(end)

        # Ensure they have timezone info (asyncpg requires it for TIMESTAMPTZ)
        if dt_start.tzinfo is None:
            dt_start = dt_start.replace(tzinfo=timezone.utc)
        if dt_end.tzinfo is None:
            dt_end = dt_end.replace(tzinfo=timezone.utc)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")

    pool = await get_pool()
    bucket = _BUCKET_MAP.get(interval)
    if bucket is None:
        raise HTTPException(status_code=400, detail=f"Invalid interval: {interval}")

    if interval == "1d":
        # Direct fetch from daily_bars table for 1-day interval
        rows = await pool.fetch(
            "SELECT time, open, high, low, close, volume "
            "FROM daily_bars WHERE symbol=$1 AND time >= $2 AND time <= $3 "
            "ORDER BY time",
            symbol,
            dt_start,
            dt_end,
        )
    elif interval == "1w":
        # Aggregate weekly bars from daily_bars aligned to Monday
        # 2000-01-03 was a Monday, used as alignment origin
        rows = await pool.fetch(
            "SELECT time_bucket('7 days', time, origin => '2000-01-03'::timestamptz) AS time, "
            "first(open, time) AS open, max(high) AS high, min(low) AS low, "
            "last(close, time) AS close, sum(volume) AS volume "
            "FROM daily_bars WHERE symbol=$1 AND time >= $2 AND time <= $3 "
            "GROUP BY 1 ORDER BY 1",
            symbol,
            dt_start,
            dt_end,
        )
    elif interval in _MINUTE_BAR_INTERVALS:
        bounds = await pool.fetchrow(
            "SELECT min(time) AS first_time, max(time) AS last_time "
            "FROM minute_bars WHERE symbol=$1 AND time BETWEEN $2 AND $3",
            symbol,
            dt_start,
            dt_end,
        )

        if interval in {"1m", "1min"}:
            rows = await pool.fetch(
                "SELECT time, open, high, low, close, volume "
                "FROM minute_bars WHERE symbol=$1 AND time >= $2 AND time <= $3 "
                "ORDER BY time",
                symbol,
                dt_start,
                dt_end,
            )
        else:
            rows = await pool.fetch(
                "SELECT time_bucket($1, time) AS time, "
                "first(open,time) AS open, max(high) AS high, min(low) AS low, "
                "last(close,time) AS close, sum(volume) AS volume "
                "FROM minute_bars WHERE symbol=$2 AND time BETWEEN $3 AND $4 "
                "GROUP BY 1 ORDER BY 1",
                bucket,
                symbol,
                dt_start,
                dt_end,
            )

        first_time = bounds["first_time"] if bounds else None
        last_time = bounds["last_time"] if bounds else None
        if first_time is None or last_time is None:
            rows = await _fetch_tick_bars(pool, bucket, symbol, dt_start, dt_end)
        else:
            rows = list(rows)
            if dt_start < first_time:
                rows.extend(await _fetch_tick_bars(pool, bucket, symbol, dt_start, first_time - timedelta(microseconds=1)))
            if last_time < dt_end:
                rows.extend(await _fetch_tick_bars(pool, bucket, symbol, last_time + timedelta(minutes=1), dt_end))
            rows = _merge_ohlcv_rows(rows)
    else:
        # Second-level intervals still aggregate directly from raw ticks.
        rows = await _fetch_tick_bars(pool, bucket, symbol, dt_start, dt_end)
    return [dict(r) for r in rows]


@router.get("/history/{symbol}/export")
async def export_history(symbol: str, start: str, end: str, interval: str = "1min"):
    rows = await get_history(symbol, start, end, interval)
    buf = io.StringIO()
    w = csv.DictWriter(
        buf, fieldnames=["time", "open", "high", "low", "close", "volume"]
    )
    w.writeheader()
    w.writerows([{k: str(v) for k, v in r.items()} for r in rows])
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={symbol}.csv"},
    )


@router.get("/status")
async def get_status():
    pool = await get_pool()
    row = await pool.fetchrow("SELECT time FROM ticks ORDER BY time DESC LIMIT 1")
    return {"last_tick": str(row["time"]) if row else None}
