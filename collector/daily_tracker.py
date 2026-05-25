import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import PRODUCT_ROLL_CONFIG

logger = logging.getLogger(__name__)


def _parse_trading_days_str(trading_hours: str) -> set[str]:
    """Parse IBKR tradingHours string into a set of trading date strings (YYYYMMDD)."""
    days = set()
    for segment in trading_hours.split(";"):
        segment = segment.strip()
        if not segment or "CLOSED" in segment:
            continue
        date_part = segment.split(":")[0]
        if len(date_part) == 8:
            days.add(date_part)
    return days


def _next_trading_day(dt: datetime, trading_days: set[str] | None) -> str:
    d = dt + timedelta(days=1)
    while True:
        ds = d.strftime("%Y%m%d")
        if d.weekday() < 5 and (trading_days is None or ds in trading_days):
            return ds
        d += timedelta(days=1)


def _effective_date_str(bar_time, symbol: str, trading_days: set[str] | None = None,
                         latest_bar_date: str | None = None) -> str:
    """Adjust bar date based on product roll time, skipping weekends and holidays.

    If latest_bar_date is provided and the clock-based result is BEFORE it,
    the latest_bar_date is used instead. This handles cases where the first
    trading day after a holiday has already started (e.g., Memorial Day),
    meaning ticks belong to the next session, not the holiday calendar date.
    """
    # IBKR formatDate=1 returns date-only objects (no time → no roll adjustment needed)
    if isinstance(bar_time, date) and not isinstance(bar_time, datetime):
        return bar_time.strftime("%Y%m%d")

    config = PRODUCT_ROLL_CONFIG.get(symbol)
    if not config:
        return bar_time.strftime("%Y%m%d")

    tz = ZoneInfo(config["timezone"])
    local_dt = bar_time.astimezone(tz) if bar_time.tzinfo is not None else bar_time.replace(tzinfo=timezone.utc).astimezone(tz)

    if local_dt.weekday() >= 5:
        return _next_trading_day(local_dt - timedelta(days=1), trading_days)

    if (local_dt.hour > config["roll_hour"]
            or (local_dt.hour == config["roll_hour"] and local_dt.minute >= config["roll_minute"])):
        return _next_trading_day(local_dt, trading_days)
    result = local_dt.strftime("%Y%m%d")
    # If the latest backfill bar date is AHEAD of the clock-based result,
    # use it instead. This handles holiday sessions (e.g., Memorial Day)
    # where CME assigns the trade date to the next business day.
    if latest_bar_date is not None and result < latest_bar_date:
        return latest_bar_date
    return result


def _bucket_time(date_str: str) -> datetime:
    """Return UTC noon of a date_str (YYYYMMDD).

    Noon UTC ensures correct date display in all browser timezones (±12h).
    """
    return datetime.strptime(date_str, "%Y%m%d").replace(hour=12, tzinfo=timezone.utc)


class DailyBarTracker:
    """Tracks today's daily OHLCV from real-time tick data.

    Flushed periodically to daily_bars so the history API always has
    today's partial bar, which the frontend chart merges with live ticks.
    """

    def __init__(self):
        self._bars = {}  # symbol -> dict
        self.trading_days: dict[str, set[str]] = {}  # symbol -> set of YYYYMMDD, set by IBKRClient
        self._stale_date_strs: dict[str, str] = {}  # symbol -> stale date_str to delete from DB
        self._latest_bar_dates: dict[str, str] = {}  # symbol -> latest date_str from backfill

    def update_latest_bar_date(self, symbol: str, date_str: str):
        """Update the latest known bar date for a symbol (from backfill)."""
        current = self._latest_bar_dates.get(symbol)
        if current is None or date_str > current:
            self._latest_bar_dates[symbol] = date_str
            logger.info(f"Updated latest bar date for {symbol}: {date_str}")

    def on_tick(self, symbol: str, price: float, size: float, tick_time: datetime):
        date_str = _effective_date_str(
            tick_time, symbol, self.trading_days.get(symbol),
            self._latest_bar_dates.get(symbol),
        )
        bar = self._bars.get(symbol)

        if bar is None:
            self._bars[symbol] = {
                "symbol": symbol,
                "date_str": date_str,
                "time": _bucket_time(date_str),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": float(size),
                "_dirty": True,
            }
        elif bar["date_str"] == date_str:
            bar["high"] = max(bar["high"], price)
            bar["low"] = min(bar["low"], price)
            bar["close"] = price
            bar["volume"] += float(size)
            bar["_dirty"] = True
        else:
            old_date = bar["date_str"]
            latest = self._latest_bar_dates.get(symbol)
            if latest is None:
                # latest_bar_date not yet populated (startup before backfill
                # completes). _effective_date_str may return a clock-time
                # fallback that's wrong for holiday sessions. Keep the loaded
                # bar and continue updating it until backfill provides data.
                bar["high"] = max(bar["high"], price)
                bar["low"] = min(bar["low"], price)
                bar["close"] = price
                bar["volume"] += float(size)
                bar["_dirty"] = True
                return
            # New trading day — reset
            if old_date > date_str or old_date < latest:
                # old_date > date_str: stale future bar (loaded from previous session)
                # old_date < latest: old bar is behind the backfill-anchored date
                #                     (e.g., clock returned "20260525" but backfill
                #                      says latest is "20260526")
                if old_date < date_str and old_date < latest:
                    # Transitioning from a holiday bar to the correct later date
                    self._stale_date_strs[symbol] = old_date
                elif old_date > date_str:
                    # Transitioning from a future bar to the correct earlier date
                    self._stale_date_strs[symbol] = old_date
            bar["symbol"] = symbol
            bar["date_str"] = date_str
            bar["time"] = _bucket_time(date_str)
            bar["open"] = price
            bar["high"] = price
            bar["low"] = price
            bar["close"] = price
            bar["volume"] = float(size)
            bar["_dirty"] = True

    async def load_from_db(self, pool, symbols: list[str]):
        """Load the most recent daily bar from DB for each symbol on startup.

        This preserves the correct open/high/low/close across collector restarts,
        so the tracker continues from where it left off instead of resetting.

        Stale future-date bars (from a previous session's post-rollhour data)
        are handled by on_tick: when a real tick arrives with an earlier date,
        the tracker transitions and flags the stale bar for DB cleanup.
        """
        for s in symbols:
            try:
                sym = s["symbol"] if isinstance(s, dict) else s
                row = await pool.fetchrow(
                    "SELECT symbol, date_str, time, open, high, low, close, volume "
                    "FROM daily_bars WHERE symbol=$1 ORDER BY date_str DESC LIMIT 1",
                    sym,
                )
                if row and row["date_str"]:
                    self._bars[sym] = {
                        "symbol": sym,
                        "date_str": row["date_str"],
                        "time": row["time"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": float(row["volume"]),
                        "_dirty": False,  # Don't re-flush; wait for new ticks
                    }
                    self.update_latest_bar_date(sym, row["date_str"])
                    logger.info(
                        f"Loaded bar for {sym} from DB: date={row['date_str']} "
                        f"O={row['open']} H={row['high']} L={row['low']} C={row['close']}"
                    )
            except Exception as e:
                symbol = s["symbol"] if isinstance(s, dict) else s
                logger.warning(f"Failed to load bar for {symbol} from DB: {e}")

    def get_dirty_bars(self) -> list[dict]:
        """Return bars that have changed since last flush, and mark them clean."""
        result = []
        for bar in self._bars.values():
            if bar.get("_dirty"):
                result.append({
                    "symbol": bar["symbol"],
                    "date_str": bar["date_str"],
                    "time": bar["time"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                })
                bar["_dirty"] = False
        return result

    def get_stale_bars(self) -> list[dict]:
        """Return (symbol, date_str) pairs for stale future-date bars that
        should be deleted from the database.

        These are generated when on_tick transitions from a future-date bar
        (e.g., loaded from a previous session's post-rollhour data) to the
        correct earlier date because the actual tick time falls before rollhour.
        """
        result = []
        stale = list(self._stale_date_strs.items())
        self._stale_date_strs.clear()
        for symbol, date_str in stale:
            result.append({"symbol": symbol, "date_str": date_str})
        return result
