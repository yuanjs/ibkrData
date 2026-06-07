"""Generate futures roll events from raw contract-level minute bars."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Optional

import asyncpg


@dataclass(frozen=True)
class ContractSummary:
    symbol: str
    con_id: int
    contract_month: str
    local_symbol: str
    last_trade_date: date
    min_time: datetime
    max_time: datetime


@dataclass(frozen=True)
class RollEvent:
    symbol: str
    from_con_id: int
    to_con_id: int
    from_contract_month: str
    to_contract_month: str
    from_local_symbol: str
    to_local_symbol: str
    roll_time: datetime
    roll_rule: str
    price_gap: Decimal
    ratio: Decimal
    old_price: Decimal
    new_price: Decimal
    old_volume: int
    new_volume: int
    old_bar_count: int
    new_bar_count: int


def subtract_trading_days(d: date, days: int) -> date:
    current = d
    remaining = days
    while remaining > 0:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def default_fallback_roll_date(last_trade_date: date, days_before: int) -> date:
    return subtract_trading_days(last_trade_date, days_before)


def choose_roll_candidate(
    rows: Iterable[dict],
    *,
    min_confirm_days: int,
) -> Optional[dict]:
    """Return first overlap day where the new contract is more active.

    A day is considered active enough when either new volume or new bar_count
    exceeds the old contract.  The condition must hold for
    ``min_confirm_days`` consecutive overlap days to avoid one-off spikes.
    """
    streak = 0
    first_in_streak: Optional[dict] = None

    for row in rows:
        new_volume = row["new_volume"] or 0
        old_volume = row["old_volume"] or 0
        new_bar_count = row["new_bar_count"] or 0
        old_bar_count = row["old_bar_count"] or 0
        is_new_more_active = (
            new_volume > old_volume or new_bar_count > old_bar_count
        )

        if is_new_more_active:
            if streak == 0:
                first_in_streak = row
            streak += 1
            if streak >= min_confirm_days:
                return first_in_streak
        else:
            streak = 0
            first_in_streak = None

    return None


class RollCalendarGenerator:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def generate(
        self,
        symbol: str,
        *,
        fallback_days_before_expiry: int = 5,
        min_confirm_days: int = 2,
        replace: bool = False,
        dry_run: bool = True,
    ) -> list[RollEvent]:
        contracts = await self._load_contracts(symbol)
        events: list[RollEvent] = []

        for old, new in zip(contracts, contracts[1:]):
            event = await self._generate_pair(
                old,
                new,
                fallback_days_before_expiry=fallback_days_before_expiry,
                min_confirm_days=min_confirm_days,
            )
            if event is not None:
                events.append(event)

        if not dry_run:
            await self._save_events(symbol, events, replace=replace)

        return events

    async def _load_contracts(self, symbol: str) -> list[ContractSummary]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, con_id, contract_month, local_symbol,
                       last_trade_date, MIN(time) AS min_time,
                       MAX(time) AS max_time
                FROM futures_minute_bars
                WHERE symbol = $1
                GROUP BY symbol, con_id, contract_month, local_symbol,
                         last_trade_date
                ORDER BY contract_month
                """,
                symbol,
            )

        return [
            ContractSummary(
                symbol=r["symbol"],
                con_id=r["con_id"],
                contract_month=r["contract_month"],
                local_symbol=r["local_symbol"],
                last_trade_date=r["last_trade_date"],
                min_time=r["min_time"],
                max_time=r["max_time"],
            )
            for r in rows
        ]

    async def _generate_pair(
        self,
        old: ContractSummary,
        new: ContractSummary,
        *,
        fallback_days_before_expiry: int,
        min_confirm_days: int,
    ) -> Optional[RollEvent]:
        overlap_rows = await self._load_overlap_days(old, new)
        if not overlap_rows:
            return None

        candidate = choose_roll_candidate(
            overlap_rows,
            min_confirm_days=min_confirm_days,
        )
        roll_rule = (
            f"volume_or_bar_count_{min_confirm_days}d_confirm"
            if candidate is not None
            else f"fallback_{fallback_days_before_expiry}bd_before_expiry"
        )

        if candidate is None:
            fallback_date = default_fallback_roll_date(
                old.last_trade_date,
                fallback_days_before_expiry,
            )
            candidate = self._first_on_or_after(overlap_rows, fallback_date)
            if candidate is None:
                return None

        prices = await self._load_roll_prices(
            old.con_id,
            new.con_id,
            candidate["session_date"],
        )
        if prices is None:
            return None

        old_price = prices["old_price"]
        new_price = prices["new_price"]
        if old_price is None or old_price == 0 or new_price is None:
            return None

        price_gap = new_price - old_price
        ratio = new_price / old_price
        roll_time = datetime.combine(
            candidate["session_date"],
            time.min,
            tzinfo=timezone.utc,
        )

        return RollEvent(
            symbol=old.symbol,
            from_con_id=old.con_id,
            to_con_id=new.con_id,
            from_contract_month=old.contract_month,
            to_contract_month=new.contract_month,
            from_local_symbol=old.local_symbol,
            to_local_symbol=new.local_symbol,
            roll_time=roll_time,
            roll_rule=roll_rule,
            price_gap=price_gap,
            ratio=ratio,
            old_price=old_price,
            new_price=new_price,
            old_volume=candidate["old_volume"] or 0,
            new_volume=candidate["new_volume"] or 0,
            old_bar_count=candidate["old_bar_count"] or 0,
            new_bar_count=candidate["new_bar_count"] or 0,
        )

    async def _load_overlap_days(
        self,
        old: ContractSummary,
        new: ContractSummary,
    ) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH old_daily AS (
                    SELECT time::date AS session_date,
                           SUM(COALESCE(volume, 0)) AS old_volume,
                           SUM(COALESCE(bar_count, 0)) AS old_bar_count
                    FROM futures_minute_bars
                    WHERE symbol = $1 AND con_id = $2
                    GROUP BY time::date
                ),
                new_daily AS (
                    SELECT time::date AS session_date,
                           SUM(COALESCE(volume, 0)) AS new_volume,
                           SUM(COALESCE(bar_count, 0)) AS new_bar_count
                    FROM futures_minute_bars
                    WHERE symbol = $1 AND con_id = $3
                    GROUP BY time::date
                )
                SELECT o.session_date, o.old_volume, n.new_volume,
                       o.old_bar_count, n.new_bar_count
                FROM old_daily o
                JOIN new_daily n USING (session_date)
                ORDER BY o.session_date
                """,
                old.symbol,
                old.con_id,
                new.con_id,
            )

        return [dict(r) for r in rows]

    async def _load_roll_prices(
        self,
        old_con_id: int,
        new_con_id: int,
        session_date: date,
    ) -> Optional[dict]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH old_bar AS (
                    SELECT close AS old_price
                    FROM futures_minute_bars
                    WHERE con_id = $1 AND time::date = $3
                    ORDER BY time DESC
                    LIMIT 1
                ),
                new_bar AS (
                    SELECT close AS new_price
                    FROM futures_minute_bars
                    WHERE con_id = $2 AND time::date = $3
                    ORDER BY time DESC
                    LIMIT 1
                )
                SELECT old_price, new_price
                FROM old_bar CROSS JOIN new_bar
                """,
                old_con_id,
                new_con_id,
                session_date,
            )

        return dict(row) if row else None

    @staticmethod
    def _first_on_or_after(rows: list[dict], fallback_date: date) -> Optional[dict]:
        for row in rows:
            if row["session_date"] >= fallback_date:
                return row
        return None

    async def _save_events(
        self,
        symbol: str,
        events: list[RollEvent],
        *,
        replace: bool,
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if replace:
                    await conn.execute(
                        "DELETE FROM futures_roll_events WHERE symbol = $1",
                        symbol,
                    )
                await conn.executemany(
                    """
                    INSERT INTO futures_roll_events (
                        symbol, from_con_id, to_con_id, roll_time, roll_rule,
                        price_gap, ratio
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (symbol, from_con_id, to_con_id, roll_time)
                    DO UPDATE SET
                        roll_rule = EXCLUDED.roll_rule,
                        price_gap = EXCLUDED.price_gap,
                        ratio = EXCLUDED.ratio
                    """,
                    [
                        (
                            e.symbol,
                            e.from_con_id,
                            e.to_con_id,
                            e.roll_time,
                            e.roll_rule,
                            e.price_gap,
                            e.ratio,
                        )
                        for e in events
                    ],
                )
