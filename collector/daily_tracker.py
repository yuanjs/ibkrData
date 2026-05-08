import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import PRODUCT_ROLL_CONFIG

logger = logging.getLogger(__name__)


def _effective_date_str(bar_time, symbol: str) -> str:
    """Adjust bar date based on product roll time."""
    # IBKR formatDate=1 returns date-only objects (no time → no roll adjustment needed)
    if isinstance(bar_time, date) and not isinstance(bar_time, datetime):
        return bar_time.strftime("%Y%m%d")

    config = PRODUCT_ROLL_CONFIG.get(symbol)
    if not config:
        return bar_time.strftime("%Y%m%d")

    tz = ZoneInfo(config["timezone"])
    local_dt = bar_time.astimezone(tz) if bar_time.tzinfo is not None else bar_time.replace(tzinfo=tz)

    if (local_dt.hour > config["roll_hour"]
            or (local_dt.hour == config["roll_hour"] and local_dt.minute >= config["roll_minute"])):
        next_day = local_dt + timedelta(days=1)
        return next_day.strftime("%Y%m%d")
    return local_dt.strftime("%Y%m%d")


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

    def on_tick(self, symbol: str, price: float, size: float, tick_time: datetime):
        date_str = _effective_date_str(tick_time, symbol)
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
            # New trading day — reset
            bar["symbol"] = symbol
            bar["date_str"] = date_str
            bar["time"] = _bucket_time(date_str)
            bar["open"] = price
            bar["high"] = price
            bar["low"] = price
            bar["close"] = price
            bar["volume"] = float(size)
            bar["_dirty"] = True

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
