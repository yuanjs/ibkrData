"""
1-second OHLC bar aggregator.

Accumulates individual ticks in memory and flushes completed 1-second bars
to the database. This gives us accurate OHLC data (matching TWS) while
keeping database writes at a manageable 1-row-per-second-per-symbol rate.
"""
import asyncio
import logging
import math
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class BarAccumulator:
    """Tracks OHLC state for a single 1-second window."""

    __slots__ = ("open", "high", "low", "close", "volume", "tick_count", "bucket_time")

    def __init__(self, bucket_time: datetime, price: float, size: float):
        self.bucket_time = bucket_time
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.volume = size
        self.tick_count = 1

    def update(self, price: float, size: float):
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.tick_count += 1


class TickAggregator:
    """
    Collects tick-by-tick trade data and aggregates into 1-second OHLC bars.

    Usage:
        aggregator = TickAggregator(data_writer)
        aggregator.on_tick("SPI", price=8780.0, size=1, tick_time=datetime.now(UTC))
        # Call flush_expired() periodically (e.g. every 100ms) to write completed bars
        await aggregator.flush_expired()
    """

    def __init__(self, writer):
        self.writer = writer
        # symbol -> BarAccumulator for the current (incomplete) second
        self._bars: dict[str, BarAccumulator] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _truncate_to_second(dt: datetime) -> datetime:
        """Truncate a datetime to the start of its second."""
        return dt.replace(microsecond=0)

    def on_tick(self, symbol: str, price: float, size: float, tick_time: datetime):
        """
        Process a single trade tick. Called from the ib_insync event callback.

        If the tick belongs to a new second, the previous bar is queued for
        flushing (handled by flush_expired).
        """
        if price is None or (isinstance(price, float) and math.isnan(price)):
            return
        if price <= 0:
            return

        bucket = self._truncate_to_second(tick_time)
        bar = self._bars.get(symbol)

        if bar is None:
            # First tick for this symbol
            self._bars[symbol] = BarAccumulator(bucket, price, size)
        elif bar.bucket_time == bucket:
            # Same second — update the running bar
            bar.update(price, size)
        else:
            # New second — the old bar will be flushed by flush_expired()
            # We can safely replace it since flush_expired runs on a timer
            # and we need to capture the boundary correctly
            self._pending_flush(symbol, bar)
            self._bars[symbol] = BarAccumulator(bucket, price, size)

    def _pending_flush(self, symbol: str, bar: BarAccumulator):
        """Schedule a completed bar for async DB write (fire-and-forget)."""
        task = asyncio.ensure_future(
            self.writer.write_ohlc_bar(
                time=bar.bucket_time,
                symbol=symbol,
                open_=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=int(bar.volume),
                tick_count=bar.tick_count,
            )
        )
        task.add_done_callback(self._log_error)

    @staticmethod
    def _log_error(task: asyncio.Task):
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(f"Failed to flush OHLC bar: {exc}", exc_info=exc)

    async def flush_expired(self):
        """
        Flush all bars whose bucket_time is older than the current second.
        Call this periodically (e.g. every 100-200ms).
        """
        now = datetime.now(timezone.utc).replace(microsecond=0)
        to_flush = []

        for symbol, bar in list(self._bars.items()):
            if bar.bucket_time < now:
                to_flush.append((symbol, bar))

        for symbol, bar in to_flush:
            # Only flush if the bar hasn't been replaced by a newer tick
            current = self._bars.get(symbol)
            if current is bar:
                del self._bars[symbol]
            await self.writer.write_ohlc_bar(
                time=bar.bucket_time,
                symbol=symbol,
                open_=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=int(bar.volume),
                tick_count=bar.tick_count,
            )

    async def flush_all(self):
        """Flush all bars (used during shutdown)."""
        for symbol, bar in list(self._bars.items()):
            await self.writer.write_ohlc_bar(
                time=bar.bucket_time,
                symbol=symbol,
                open_=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=int(bar.volume),
                tick_count=bar.tick_count,
            )
        self._bars.clear()
