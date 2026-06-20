import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from config import (
    FUTURES_LIVE_DAILY_LOOKAHEAD_TRADING_DAYS,
    FUTURES_LIVE_DAILY_REFRESH_DAYS,
    FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS,
)

logger = logging.getLogger(__name__)

ACTIVE_STALE_PROMOTION_SECONDS = 300

QUARTERLY_MONTHS = frozenset({"03", "06", "09", "12"})
LIVE_CONTRACT_MONTHS = {
    "SPI": QUARTERLY_MONTHS,
    "MYM": QUARTERLY_MONTHS,
    "MNQ": QUARTERLY_MONTHS,
    "MES": QUARTERLY_MONTHS,
    "N225M": QUARTERLY_MONTHS,
    "10Y": QUARTERLY_MONTHS,
    "ZC": frozenset({"03", "05", "07", "09", "12"}),
}


def _contract_month(contract) -> str | None:
    raw = getattr(contract, "lastTradeDateOrContractMonth", None)
    return raw[:6] if raw and len(raw) >= 6 else raw


def _last_trade_date(contract) -> date | None:
    raw = getattr(contract, "lastTradeDateOrContractMonth", None) or ""
    if len(raw) < 8:
        return None
    try:
        return date.fromisoformat(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    except ValueError:
        return None


def _subtract_weekdays(d: date, days: int) -> date:
    current = d
    remaining = days
    while remaining > 0:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _is_live_roll_contract(symbol: str, identity: dict) -> bool:
    months = LIVE_CONTRACT_MONTHS.get(symbol)
    if not months:
        return True
    contract_month = identity.get("contract_month") or ""
    return len(contract_month) >= 6 and contract_month[4:6] in months


@dataclass
class LiveFuturesState:
    symbol: str
    exchange: str
    currency: str
    contracts: list[dict] = field(default_factory=list)
    active: dict | None = None
    next: dict | None = None
    subscribed: dict[int, str] = field(default_factory=dict)


class LiveFuturesRuntime:
    """Manage live futures contract discovery, daily refresh, and subscriptions."""

    def __init__(self, client, writer, pool, pub) -> None:
        self.client = client
        self.writer = writer
        self.pool = pool
        self.pub = pub
        self.states: dict[str, LiveFuturesState] = {}

    @staticmethod
    def identity_from_contract(symbol: str, contract) -> dict:
        con_id = getattr(contract, "conId", None)
        return {
            "symbol": symbol,
            "con_id": int(con_id) if con_id else None,
            "local_symbol": getattr(contract, "localSymbol", None) or None,
            "trading_class": getattr(contract, "tradingClass", None) or None,
            "contract_month": _contract_month(contract),
            "last_trade_date": _last_trade_date(contract),
            "exchange": getattr(contract, "exchange", None) or None,
            "currency": getattr(contract, "currency", None) or None,
            "multiplier": getattr(contract, "multiplier", None) or None,
        }

    async def refresh_contracts(self, subscriptions: list[dict]) -> None:
        for sub in subscriptions:
            if sub.get("sec_type") != "FUT":
                continue
            symbol = sub["symbol"]
            try:
                contracts = await self.client.list_futures_contracts(
                    symbol,
                    sub["exchange"],
                    sub["currency"],
                )
            except Exception as e:
                logger.warning("Failed to discover futures contracts for %s: %s", symbol, e)
                continue

            identities = [
                self.identity_from_contract(symbol, c)
                for c in contracts
                if getattr(c, "conId", None)
            ]
            identities = [
                {
                    **i,
                    "exchange": i.get("exchange") or sub["exchange"],
                    "currency": i.get("currency") or sub["currency"],
                }
                for i in identities
                if _is_live_roll_contract(symbol, i)
            ]
            identities.sort(
                key=lambda i: (
                    i.get("last_trade_date") or date.max,
                    i.get("contract_month") or "",
                    int(i.get("con_id") or 0),
                )
            )
            await self.writer.upsert_futures_contracts(identities)

            state = self.states.setdefault(
                symbol,
                LiveFuturesState(symbol, sub["exchange"], sub["currency"]),
            )
            state.contracts = identities
            state.active = await self._load_active(symbol) or self._first_live_contract(identities)
            state.next = self._next_after_active(state)

    async def ensure_market_data(self) -> None:
        for state in self.states.values():
            if not state.active:
                continue
            await self._ensure_subscribed(state, state.active, "active")

            latest = await self._load_active(state.symbol)
            if latest and state.active and latest.get("con_id") != state.active.get("con_id"):
                await self._promote_active(state, latest)

            if state.next and await self._should_subscribe_next(state):
                await self._ensure_subscribed(state, state.next, "candidate")
                await self._promote_next_if_active_stale(state)

    async def refresh_daily_bars(self) -> None:
        for state in self.states.values():
            contracts = [c for c in (state.active, state.next) if c]
            for identity in contracts:
                if not self._within_daily_lookahead(state, identity):
                    continue
                try:
                    bars = await self.client.request_futures_daily_bars(
                        identity,
                        duration=f"{FUTURES_LIVE_DAILY_REFRESH_DAYS} D",
                    )
                    rows = self.writer.futures_daily_bar_rows_from_identity(
                        identity,
                        bars,
                    )
                    await self.writer.upsert_futures_daily_bars_from_live(rows)
                except Exception as e:
                    logger.warning(
                        "Failed to refresh futures daily bars for %s conId=%s: %s",
                        state.symbol,
                        identity.get("con_id"),
                        e,
                    )

    async def _load_active(self, symbol: str) -> dict | None:
        row = await self.pool.fetchrow(
            "SELECT * FROM active_futures_contract_asof($1, $2)",
            symbol,
            datetime.now(timezone.utc),
        )
        return dict(row) if row else None

    def _first_live_contract(self, contracts: list[dict]) -> dict | None:
        today = datetime.now(timezone.utc).date()
        for contract in contracts:
            last_trade_date = contract.get("last_trade_date")
            if last_trade_date is None or last_trade_date >= today:
                return contract
        return contracts[-1] if contracts else None

    def _next_after_active(self, state: LiveFuturesState) -> dict | None:
        if not state.active:
            return None
        active_month = state.active.get("contract_month")
        active_con_id = int(state.active.get("con_id") or 0)
        for contract in state.contracts:
            con_id = int(contract.get("con_id") or 0)
            month = contract.get("contract_month")
            if active_month and month and month > active_month:
                return contract
            if not active_month and con_id != active_con_id:
                return contract
        return None

    async def _ensure_subscribed(
        self,
        state: LiveFuturesState,
        identity: dict,
        role: str,
    ) -> None:
        con_id = int(identity["con_id"])
        if (
            state.subscribed.get(con_id) == role
            and self.client.is_futures_contract_subscribed(
                state.symbol,
                con_id,
                role=role,
            )
        ):
            return
        subscribed = await self.client.subscribe_futures_contract(
            state.symbol,
            state.exchange,
            state.currency,
            identity,
            role=role,
        )
        if subscribed:
            state.subscribed[con_id] = role

    async def _promote_active(self, state: LiveFuturesState, latest: dict) -> None:
        old = state.active
        await self._ensure_subscribed(state, latest, "active")
        if old and old.get("con_id") != latest.get("con_id"):
            old_con_id = int(old["con_id"])
            self.client.unsubscribe_futures_contract(state.symbol, old_con_id)
            state.subscribed.pop(old_con_id, None)
        state.active = latest
        state.next = self._next_after_active(state)
        await self.pub.publish_futures_roll_state(
            state.symbol,
            {
                "symbol": state.symbol,
                "previous": old,
                "active": latest,
                "roll_event_id": latest.get("roll_event_id"),
                "effective_from": latest.get("effective_from"),
                "time": datetime.now(timezone.utc),
            },
        )

    async def _promote_next_if_active_stale(self, state: LiveFuturesState) -> None:
        """Promote next contract on expiry day when the active contract stops ticking.

        Some contracts stop trading intraday while their last_trade_date is still
        today's UTC date.  The as-of roll state can therefore lag until the next
        calendar day; live ticks should follow the contract that is actually
        producing current market data.
        """
        if not state.active or not state.next:
            return

        active_last_trade_date = state.active.get("last_trade_date")
        today = datetime.now(timezone.utc).date()
        if active_last_trade_date is None or active_last_trade_date > today:
            return

        active_con_id = int(state.active.get("con_id") or 0)
        next_con_id = int(state.next.get("con_id") or 0)
        if not active_con_id or not next_con_id:
            return

        row = await self.pool.fetchrow(
            """
            SELECT
                (SELECT max(time) FROM futures_ticks WHERE symbol = $1 AND con_id = $2) AS active_last_tick,
                (SELECT max(time) FROM futures_ticks WHERE symbol = $1 AND con_id = $3) AS next_last_tick
            """,
            state.symbol,
            active_con_id,
            next_con_id,
        )
        if not row:
            return

        active_last_tick = row.get("active_last_tick")
        next_last_tick = row.get("next_last_tick")
        if next_last_tick is None:
            return

        now = datetime.now(timezone.utc)
        if next_last_tick.tzinfo is None:
            next_last_tick = next_last_tick.replace(tzinfo=timezone.utc)
        if (now - next_last_tick).total_seconds() > ACTIVE_STALE_PROMOTION_SECONDS:
            return

        active_is_stale = active_last_tick is None
        if active_last_tick is not None:
            if active_last_tick.tzinfo is None:
                active_last_tick = active_last_tick.replace(tzinfo=timezone.utc)
            active_is_stale = (
                (now - active_last_tick).total_seconds() > ACTIVE_STALE_PROMOTION_SECONDS
                and next_last_tick > active_last_tick
            )

        if not active_is_stale:
            return

        logger.info(
            "Promoting %s next contract %s because active conId=%s is stale "
            "(active_last_tick=%s, next_last_tick=%s)",
            state.symbol,
            state.next.get("con_id"),
            active_con_id,
            active_last_tick,
            next_last_tick,
        )
        await self._promote_active(state, state.next)

    async def _should_subscribe_next(self, state: LiveFuturesState) -> bool:
        if not state.active or not state.next:
            return False
        if self._within_tick_overlap(state.active):
            return True
        next_con_id = int(state.next.get("con_id") or 0)
        latest = await self._load_active(state.symbol)
        if latest and int(latest.get("con_id") or 0) == next_con_id:
            return True
        pending = await self.pool.fetchrow(
            """
            SELECT 1
            FROM futures_roll_events_asof
            WHERE symbol = $1
              AND to_con_id = $2
              AND known_at <= $3
              AND effective_roll_time > $3
            ORDER BY effective_roll_time ASC
            LIMIT 1
            """,
            state.symbol,
            next_con_id,
            datetime.now(timezone.utc),
        )
        return pending is not None

    def _within_tick_overlap(self, active: dict) -> bool:
        last_trade_date = active.get("last_trade_date")
        if last_trade_date is None:
            return False
        start = _subtract_weekdays(
            last_trade_date,
            FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS,
        )
        return datetime.now(timezone.utc).date() >= start

    def _within_daily_lookahead(self, state: LiveFuturesState, identity: dict) -> bool:
        if state.active and identity.get("con_id") == state.active.get("con_id"):
            return True
        active_ltd = state.active.get("last_trade_date") if state.active else None
        if active_ltd is None:
            return True
        start = _subtract_weekdays(
            active_ltd,
            FUTURES_LIVE_DAILY_LOOKAHEAD_TRADING_DAYS,
        )
        return datetime.now(timezone.utc).date() >= start
