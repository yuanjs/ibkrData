"""Backfill cash minute_bars from raw ticks.

This is intended for CASH products such as USD.JPY and AUD.USD where live
collection historically wrote raw ticks but did not maintain minute_bars.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta, timezone

import asyncpg
from dateutil import parser as date_parser


DEFAULT_DB_URL = "postgresql://ibkr:password@localhost:5432/ibkrdata"


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    dt = date_parser.isoparse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_execute_count(status: str) -> int:
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


async def load_cash_symbols(conn: asyncpg.Connection, only: list[str]) -> list[str]:
    if only:
        return sorted({symbol.upper() for symbol in only})
    rows = await conn.fetch(
        "SELECT symbol FROM subscriptions "
        "WHERE active=true AND sec_type='CASH' ORDER BY symbol"
    )
    return [row["symbol"] for row in rows]


async def tick_bounds(
    conn: asyncpg.Connection,
    symbol: str,
    start: datetime | None,
    end: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    row = await conn.fetchrow(
        "SELECT min(time) AS first_time, max(time) AS last_time "
        "FROM ticks WHERE symbol=$1 "
        "AND ($2::timestamptz IS NULL OR time >= $2) "
        "AND ($3::timestamptz IS NULL OR time <= $3)",
        symbol,
        start,
        end,
    )
    return row["first_time"], row["last_time"]


async def backfill_chunk(
    conn: asyncpg.Connection,
    symbol: str,
    start: datetime,
    end: datetime,
    dry_run: bool,
) -> int:
    if dry_run:
        return max(0, int((end - start).total_seconds() // 60))

    status = await conn.execute(
        "INSERT INTO minute_bars (time, symbol, open, high, low, close, volume, bar_count) "
        "SELECT time_bucket('1 minute', time) AS minute_time, "
        "symbol, first(last, time), max(last), min(last), last(last, time), "
        "sum(volume), count(*) "
        "FROM ticks "
        "WHERE symbol=$1 AND time >= $2 AND time < $3 "
        "GROUP BY minute_time, symbol "
        "ON CONFLICT (symbol, time) DO UPDATE SET "
        "open=EXCLUDED.open,"
        "high=EXCLUDED.high,"
        "low=EXCLUDED.low,"
        "close=EXCLUDED.close,"
        "volume=EXCLUDED.volume,"
        "bar_count=EXCLUDED.bar_count",
        symbol,
        start,
        end,
    )
    return parse_execute_count(status)


async def backfill_symbol(
    conn: asyncpg.Connection,
    symbol: str,
    start: datetime | None,
    end: datetime | None,
    chunk_hours: int,
    dry_run: bool,
) -> int:
    if start is not None and end is not None:
        range_start = start
        range_end = end
    else:
        first_tick, last_tick = await tick_bounds(conn, symbol, start, end)
        if first_tick is None or last_tick is None:
            print(f"{symbol}: no ticks in requested range")
            return 0
        range_start = max(first_tick, start) if start else first_tick
        range_end = min(last_tick, end) if end else last_tick
    range_start = range_start.replace(second=0, microsecond=0)
    range_end = (range_end + timedelta(minutes=1)).replace(second=0, microsecond=0)
    chunk_size = timedelta(hours=chunk_hours)
    total = 0
    current = range_start

    print(f"{symbol}: {range_start.isoformat()} -> {range_end.isoformat()}")
    while current < range_end:
        chunk_end = min(current + chunk_size, range_end)
        count = await backfill_chunk(conn, symbol, current, chunk_end, dry_run)
        total += count
        print(f"{symbol}: {current.isoformat()} -> {chunk_end.isoformat()} rows={count}")
        current = chunk_end

    print(f"{symbol}: total rows={total}")
    return total


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill CASH minute_bars from raw ticks."
    )
    parser.add_argument("--db-url", default=os.getenv("DB_URL", DEFAULT_DB_URL))
    parser.add_argument("--only", nargs="*", default=[])
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--chunk-hours", type=int, default=24)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.chunk_hours <= 0:
        raise SystemExit("--chunk-hours must be positive")

    start = parse_dt(args.start)
    end = parse_dt(args.end)
    if start and end and start >= end:
        raise SystemExit("--start must be earlier than --end")

    if args.dry_run and args.only and start is not None and end is not None:
        symbols = sorted({symbol.upper() for symbol in args.only})
        grand_total = 0
        for symbol in symbols:
            grand_total += await backfill_symbol(
                None,
                symbol,
                start,
                end,
                args.chunk_hours,
                args.dry_run,
            )
        print(f"grand total rows={grand_total}")
        return 0

    conn = await asyncpg.connect(args.db_url)
    try:
        symbols = await load_cash_symbols(conn, args.only)
        if not symbols:
            print("No CASH symbols to backfill")
            return 0
        grand_total = 0
        for symbol in symbols:
            grand_total += await backfill_symbol(
                conn,
                symbol,
                start,
                end,
                args.chunk_hours,
                args.dry_run,
            )
        print(f"grand total rows={grand_total}")
    finally:
        await conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
