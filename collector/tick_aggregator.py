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

    async def flush_expired(self):
        """
        Periodically called to flush bars that have completed their second.
        This ensures data is written to the DB even if no new ticks arrive for a symbol.
        """
        now_bucket = self._truncate_to_second(datetime.now(timezone.utc))
        
        async with self._lock:
            symbols_to_remove = []
            for symbol, bar in self._bars.items():
                # If the bar's second is in the past, it's safe to flush
                if bar.bucket_time < now_bucket:
                    self._pending_flush(symbol, bar)
                    symbols_to_remove.append(symbol)
            
            for symbol in symbols_to_remove:
                del self._bars[symbol]

    def on_tick(self, symbol: str, price: float, size: float, tick_time: datetime):
        """
        Process a single trade tick. Called from the ib_insync event callback.
        """
        if price is None or (isinstance(price, float) and math.isnan(price)):
            return
        if price <= 0:
            return

        # Ensure tick_time is UTC aware
        if tick_time.tzinfo is None:
            tick_time = tick_time.replace(tzinfo=timezone.utc)

        bucket = self._truncate_to_second(tick_time)
        
        # Note: We use a simple dict update here. 
        # Concurrent access with flush_expired is protected by the lock in flush_expired.
        bar = self._bars.get(symbol)

        if bar is None:
            self._bars[symbol] = BarAccumulator(bucket, price, size)
        elif bar.bucket_time == bucket:
            bar.update(price, size)
        elif bar.bucket_time < bucket:
            # New second arrived - flush the old one immediately
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
            )
        self._bars.clear()
