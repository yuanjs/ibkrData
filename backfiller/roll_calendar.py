"""Generate futures roll events from raw contract-level minute bars."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

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


@dataclass(frozen=True)
class AsOfRollEvent:
    symbol: str
    from_con_id: int
    to_con_id: int
    from_contract_month: str
    to_contract_month: str
    from_local_symbol: str
    to_local_symbol: str
    effective_roll_time: datetime
    known_at: datetime
    decision_session_date: date
    price_session_date: date
    roll_rule: str
    price_gap: Decimal
    ratio: Decimal
    old_price: Decimal
    new_price: Decimal
    old_volume: int
    new_volume: int
    old_bar_count: int
    new_bar_count: int

    @property
    def roll_time(self) -> datetime:
        return self.effective_roll_time


@dataclass(frozen=True)
class AsOfRollCandidate:
    decision_row: dict
    known_row: dict
    rule_source: str


@dataclass(frozen=True)
class SessionBoundary:
    timezone_name: str
    roll_time: time


SESSION_BOUNDARIES = {
    "SPI": SessionBoundary("Australia/Sydney", time(17, 10)),
    "MYM": SessionBoundary("America/Chicago", time(16, 0)),
    "MES": SessionBoundary("America/Chicago", time(16, 0)),
    "MNQ": SessionBoundary("America/Chicago", time(16, 0)),
    "10Y": SessionBoundary("America/Chicago", time(16, 0)),
    "ZC": SessionBoundary("America/Chicago", time(16, 0)),
    "N225M": SessionBoundary("Asia/Tokyo", time(16, 30)),
    "HG": SessionBoundary("America/New_York", time(17, 0)),
}

QUARTERLY_MONTHS = frozenset({"03", "06", "09", "12"})
ROLL_CONTRACT_MONTHS = {
    "SPI": QUARTERLY_MONTHS,
    "MYM": QUARTERLY_MONTHS,
    "MNQ": QUARTERLY_MONTHS,
    "MES": QUARTERLY_MONTHS,
    "N225M": QUARTERLY_MONTHS,
    "10Y": QUARTERLY_MONTHS,
    "ZC": frozenset({"03", "05", "07", "09", "12"}),
}


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


def next_weekday(d: date) -> date:
    current = d + timedelta(days=1)
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def session_start_time_utc(symbol: str, session_date: date) -> datetime:
    boundary = SESSION_BOUNDARIES.get(
        symbol,
        SessionBoundary("UTC", time.min),
    )
    local_date = session_date - timedelta(days=1)
    local_dt = datetime.combine(
        local_date,
        boundary.roll_time,
        tzinfo=ZoneInfo(boundary.timezone_name),
    )
    return local_dt.astimezone(timezone.utc)


def is_roll_contract_month(symbol: str, contract_month: str | None) -> bool:
    months = ROLL_CONTRACT_MONTHS.get(symbol)
    if not months:
        return True
    if not contract_month or len(contract_month) < 6:
        return False
    return contract_month[4:6] in months


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


def choose_volume_safety_candidate(
    rows: list[dict],
    *,
    min_confirm_days: int,
    safety_date: date,
) -> tuple[Optional[dict], str]:
    """Choose roll date using volume crossover capped by a safety date.

    The primary signal is the first day where the new contract's daily volume
    exceeds the old contract's daily volume for ``min_confirm_days``
    consecutive overlap days.  The safety date is a latest acceptable roll
    boundary.  If volume confirmation happens after that boundary, or never
    happens, the first overlap day on/after the safety date is used instead.
    """
    volume_candidate = _first_volume_confirmed_candidate(
        rows,
        min_confirm_days=min_confirm_days,
    )
    safety_candidate = _first_on_or_after(rows, safety_date)

    if volume_candidate is None:
        return safety_candidate, "safety"
    if safety_candidate is None:
        return volume_candidate, "volume"

    if volume_candidate["session_date"] <= safety_candidate["session_date"]:
        return volume_candidate, "volume"
    return safety_candidate, "safety"


def choose_volume_safety_candidate_asof(
    rows: list[dict],
    *,
    min_confirm_days: int,
    safety_date: date,
) -> Optional[AsOfRollCandidate]:
    """Choose an auditable as-of roll candidate.

    ``decision_row`` is the first day in the volume confirmation streak.
    ``known_row`` is the day where the confirmation becomes knowable.  For
    safety rolls both rows are the safety candidate.
    """
    volume_candidate = _first_volume_confirmed_candidate_asof(
        rows,
        min_confirm_days=min_confirm_days,
    )
    safety_candidate = _first_on_or_after(rows, safety_date)

    if volume_candidate is None:
        if safety_candidate is None:
            return None
        return AsOfRollCandidate(
            decision_row=safety_candidate,
            known_row=safety_candidate,
            rule_source="safety",
        )

    if safety_candidate is None:
        decision_row, known_row = volume_candidate
        return AsOfRollCandidate(
            decision_row=decision_row,
            known_row=known_row,
            rule_source="volume",
        )

    decision_row, known_row = volume_candidate
    if known_row["session_date"] <= safety_candidate["session_date"]:
        return AsOfRollCandidate(
            decision_row=decision_row,
            known_row=known_row,
            rule_source="volume",
        )
    return AsOfRollCandidate(
        decision_row=safety_candidate,
        known_row=safety_candidate,
        rule_source="safety",
    )


def _first_volume_confirmed_candidate(
    rows: Iterable[dict],
    *,
    min_confirm_days: int,
) -> Optional[dict]:
    streak = 0
    first_in_streak: Optional[dict] = None

    for row in rows:
        new_volume = row["new_volume"] or 0
        old_volume = row["old_volume"] or 0
        is_new_more_active = new_volume > old_volume

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


def _first_volume_confirmed_candidate_asof(
    rows: Iterable[dict],
    *,
    min_confirm_days: int,
) -> Optional[tuple[dict, dict]]:
    streak = 0
    first_in_streak: Optional[dict] = None

    for row in rows:
        new_volume = row["new_volume"] or 0
        old_volume = row["old_volume"] or 0
        is_new_more_active = new_volume > old_volume

        if is_new_more_active:
            if streak == 0:
                first_in_streak = row
            streak += 1
            if streak >= min_confirm_days:
                return first_in_streak, row
        else:
            streak = 0
            first_in_streak = None

    return None


def _first_on_or_after(rows: list[dict], fallback_date: date) -> Optional[dict]:
    for row in rows:
        if row["session_date"] >= fallback_date:
            return row
    return None


def _first_after(rows: list[dict], after_date: date) -> Optional[dict]:
    for row in rows:
        if row["session_date"] > after_date:
            return row
    return None


class RollCalendarGenerator:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        contract_source: str = "historical_bars",
    ) -> None:
        self._pool = pool
        self._contract_source = contract_source

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

    async def generate_volume_safety(
        self,
        symbol: str,
        *,
        safety_days_before_expiry: int,
        min_confirm_days: int = 2,
        replace: bool = False,
        dry_run: bool = True,
    ) -> list[RollEvent]:
        contracts = await self._load_contracts(symbol)
        events: list[RollEvent] = []

        for old, new in zip(contracts, contracts[1:]):
            event = await self._generate_volume_safety_pair(
                old,
                new,
                safety_days_before_expiry=safety_days_before_expiry,
                min_confirm_days=min_confirm_days,
            )
            if event is not None:
                events.append(event)

        if not dry_run:
            await self._save_volume_safety_events(
                symbol,
                events,
                replace=replace,
            )

        return events

    async def generate_asof(
        self,
        symbol: str,
        *,
        safety_days_before_expiry: int,
        min_confirm_days: int = 2,
        replace: bool = False,
        dry_run: bool = True,
    ) -> list[AsOfRollEvent]:
        contracts = await self._load_contracts(symbol)
        events: list[AsOfRollEvent] = []

        for old, new in zip(contracts, contracts[1:]):
            event = await self._generate_asof_pair(
                old,
                new,
                safety_days_before_expiry=safety_days_before_expiry,
                min_confirm_days=min_confirm_days,
            )
            if event is not None:
                events.append(event)

        if not dry_run:
            await self._save_asof_events(symbol, events, replace=replace)

        return events

    async def _load_contracts(self, symbol: str) -> list[ContractSummary]:
        if self._contract_source == "live_contracts":
            return await self._load_live_contracts(symbol)

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
            if is_roll_contract_month(r["symbol"], r["contract_month"])
        ]

    async def _load_live_contracts(self, symbol: str) -> list[ContractSummary]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    c.symbol,
                    c.con_id,
                    c.contract_month,
                    c.local_symbol,
                    c.last_trade_date,
                    COALESCE(MIN(d.time), c.first_seen_at) AS min_time,
                    COALESCE(MAX(d.time), c.last_seen_at) AS max_time
                FROM futures_contracts c
                LEFT JOIN futures_daily_bars d
                  ON d.symbol = c.symbol
                 AND d.con_id = c.con_id
                WHERE c.symbol = $1
                GROUP BY c.symbol, c.con_id, c.contract_month, c.local_symbol,
                         c.last_trade_date, c.first_seen_at, c.last_seen_at
                ORDER BY c.contract_month
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
            if is_roll_contract_month(r["symbol"], r["contract_month"])
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

    async def _generate_volume_safety_pair(
        self,
        old: ContractSummary,
        new: ContractSummary,
        *,
        safety_days_before_expiry: int,
        min_confirm_days: int,
    ) -> Optional[RollEvent]:
        overlap_rows = await self._load_overlap_days(old, new)
        if not overlap_rows:
            return None

        safety_date = default_fallback_roll_date(
            old.last_trade_date,
            safety_days_before_expiry,
        )
        candidate, rule_source = choose_volume_safety_candidate(
            overlap_rows,
            min_confirm_days=min_confirm_days,
            safety_date=safety_date,
        )
        if candidate is None:
            return None

        roll_rule = (
            f"volume_{min_confirm_days}d_confirm_safety_"
            f"{safety_days_before_expiry}bd"
            if rule_source == "volume"
            else f"safety_{safety_days_before_expiry}bd_before_expiry"
        )

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

    async def _generate_asof_pair(
        self,
        old: ContractSummary,
        new: ContractSummary,
        *,
        safety_days_before_expiry: int,
        min_confirm_days: int,
    ) -> Optional[AsOfRollEvent]:
        overlap_rows = await self._load_overlap_days(old, new)
        if not overlap_rows:
            return None

        safety_date = default_fallback_roll_date(
            old.last_trade_date,
            safety_days_before_expiry,
        )
        candidate = choose_volume_safety_candidate_asof(
            overlap_rows,
            min_confirm_days=min_confirm_days,
            safety_date=safety_date,
        )
        if candidate is None:
            return None

        known_session_date = candidate.known_row["session_date"]
        price_session_date = known_session_date
        effective_row = _first_after(overlap_rows, known_session_date)
        effective_session_date = (
            effective_row["session_date"]
            if effective_row is not None
            else next_weekday(known_session_date)
        )

        roll_rule = (
            f"volume_{min_confirm_days}d_confirm_asof_safety_"
            f"{safety_days_before_expiry}bd"
            if candidate.rule_source == "volume"
            else f"safety_{safety_days_before_expiry}bd_before_expiry_asof"
        )

        prices = await self._load_roll_prices(
            old.con_id,
            new.con_id,
            price_session_date,
        )
        if prices is None:
            return None

        old_price = prices["old_price"]
        new_price = prices["new_price"]
        if old_price is None or old_price == 0 or new_price is None:
            return None

        price_gap = new_price - old_price
        ratio = new_price / old_price
        effective_roll_time = session_start_time_utc(
            old.symbol,
            effective_session_date,
        )
        known_at = effective_roll_time

        return AsOfRollEvent(
            symbol=old.symbol,
            from_con_id=old.con_id,
            to_con_id=new.con_id,
            from_contract_month=old.contract_month,
            to_contract_month=new.contract_month,
            from_local_symbol=old.local_symbol,
            to_local_symbol=new.local_symbol,
            effective_roll_time=effective_roll_time,
            known_at=known_at,
            decision_session_date=candidate.decision_row["session_date"],
            price_session_date=price_session_date,
            roll_rule=roll_rule,
            price_gap=price_gap,
            ratio=ratio,
            old_price=old_price,
            new_price=new_price,
            old_volume=candidate.known_row["old_volume"] or 0,
            new_volume=candidate.known_row["new_volume"] or 0,
            old_bar_count=candidate.known_row["old_bar_count"] or 0,
            new_bar_count=candidate.known_row["new_bar_count"] or 0,
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
                    SELECT session_date,
                           COALESCE(volume, 0) AS old_volume,
                           COALESCE(bar_count, 0) AS old_bar_count
                    FROM futures_daily_bars_session_normalized
                    WHERE symbol = $1 AND con_id = $2
                ),
                new_daily AS (
                    SELECT session_date,
                           COALESCE(volume, 0) AS new_volume,
                           COALESCE(bar_count, 0) AS new_bar_count
                    FROM futures_daily_bars_session_normalized
                    WHERE symbol = $1 AND con_id = $3
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
                    FROM futures_daily_bars_session_normalized
                    WHERE con_id = $1 AND session_date = $3
                ),
                new_bar AS (
                    SELECT close AS new_price
                    FROM futures_daily_bars_session_normalized
                    WHERE con_id = $2 AND session_date = $3
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
        return _first_on_or_after(rows, fallback_date)

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

    async def _save_volume_safety_events(
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
                        """
                        DELETE FROM futures_roll_events_volume_safety
                        WHERE symbol = $1
                        """,
                        symbol,
                    )
                await conn.executemany(
                    """
                    INSERT INTO futures_roll_events_volume_safety (
                        symbol, from_con_id, to_con_id,
                        from_contract_month, to_contract_month,
                        from_local_symbol, to_local_symbol,
                        roll_time, roll_rule, price_gap, ratio,
                        old_price, new_price, old_volume, new_volume,
                        old_bar_count, new_bar_count
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14, $15, $16, $17
                    )
                    ON CONFLICT (symbol, from_con_id, to_con_id, roll_time)
                    DO UPDATE SET
                        from_contract_month = EXCLUDED.from_contract_month,
                        to_contract_month = EXCLUDED.to_contract_month,
                        from_local_symbol = EXCLUDED.from_local_symbol,
                        to_local_symbol = EXCLUDED.to_local_symbol,
                        roll_rule = EXCLUDED.roll_rule,
                        price_gap = EXCLUDED.price_gap,
                        ratio = EXCLUDED.ratio,
                        old_price = EXCLUDED.old_price,
                        new_price = EXCLUDED.new_price,
                        old_volume = EXCLUDED.old_volume,
                        new_volume = EXCLUDED.new_volume,
                        old_bar_count = EXCLUDED.old_bar_count,
                        new_bar_count = EXCLUDED.new_bar_count
                    """,
                    [
                        (
                            e.symbol,
                            e.from_con_id,
                            e.to_con_id,
                            e.from_contract_month,
                            e.to_contract_month,
                            e.from_local_symbol,
                            e.to_local_symbol,
                            e.roll_time,
                            e.roll_rule,
                            e.price_gap,
                            e.ratio,
                            e.old_price,
                            e.new_price,
                            e.old_volume,
                            e.new_volume,
                            e.old_bar_count,
                            e.new_bar_count,
                        )
                        for e in events
                    ],
                )

    async def _save_asof_events(
        self,
        symbol: str,
        events: list[AsOfRollEvent],
        *,
        replace: bool,
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if replace:
                    await conn.execute(
                        """
                        DELETE FROM futures_roll_events_asof
                        WHERE symbol = $1
                        """,
                        symbol,
                    )
                await conn.executemany(
                    """
                    INSERT INTO futures_roll_events_asof (
                        symbol, from_con_id, to_con_id,
                        from_contract_month, to_contract_month,
                        from_local_symbol, to_local_symbol,
                        effective_roll_time, known_at,
                        decision_session_date, price_session_date,
                        roll_rule, price_gap, ratio,
                        old_price, new_price, old_volume, new_volume,
                        old_bar_count, new_bar_count
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9,
                        $10, $11, $12, $13, $14, $15, $16,
                        $17, $18, $19, $20
                    )
                    ON CONFLICT (symbol, from_con_id, to_con_id, effective_roll_time)
                    DO UPDATE SET
                        from_contract_month = EXCLUDED.from_contract_month,
                        to_contract_month = EXCLUDED.to_contract_month,
                        from_local_symbol = EXCLUDED.from_local_symbol,
                        to_local_symbol = EXCLUDED.to_local_symbol,
                        known_at = EXCLUDED.known_at,
                        decision_session_date = EXCLUDED.decision_session_date,
                        price_session_date = EXCLUDED.price_session_date,
                        roll_rule = EXCLUDED.roll_rule,
                        price_gap = EXCLUDED.price_gap,
                        ratio = EXCLUDED.ratio,
                        old_price = EXCLUDED.old_price,
                        new_price = EXCLUDED.new_price,
                        old_volume = EXCLUDED.old_volume,
                        new_volume = EXCLUDED.new_volume,
                        old_bar_count = EXCLUDED.old_bar_count,
                        new_bar_count = EXCLUDED.new_bar_count
                    """,
                    [
                        (
                            e.symbol,
                            e.from_con_id,
                            e.to_con_id,
                            e.from_contract_month,
                            e.to_contract_month,
                            e.from_local_symbol,
                            e.to_local_symbol,
                            e.effective_roll_time,
                            e.known_at,
                            e.decision_session_date,
                            e.price_session_date,
                            e.roll_rule,
                            e.price_gap,
                            e.ratio,
                            e.old_price,
                            e.new_price,
                            e.old_volume,
                            e.new_volume,
                            e.old_bar_count,
                            e.new_bar_count,
                        )
                        for e in events
                    ],
                )
