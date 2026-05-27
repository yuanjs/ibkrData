"""
Monkey-patch ib_insync Wrapper to use IBKR-provided exchange timestamps.

Problem: ib_insync's Wrapper ignores the exchange timestamps that the
IBKR API sends and uses `self.lastTime` (collector's datetime.now(utc))
for all TickData and tickByTick creation. This means timestamps vary
across devices due to clock drift and network latency.

Patch: Replace the relevant Wrapper methods so that, for tick-by-tick
data (`reqTickByTickData`), the IBKR API `time` parameter is used
instead of `self.lastTime`.

When reqMktData is used (current setup), RTVolume (tickType 48/77)
already provides the Gateway timestamp via ticker.rtTime — that is
handled separately in ibkr_client.py by preferring rtTime over ticker.time.

Usage in main.py:
    import ibkr_patch
    ibkr_patch.apply()
"""

import logging

logger = logging.getLogger(__name__)

def apply():
    """Apply monkey-patches to ib_insync wrapper after import."""
    try:
        from ib_insync import wrapper as wrp
        from ib_insync.objects import TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint

        # Store original methods
        orig_tickByTickAllLast = wrp.Wrapper.tickByTickAllLast
        orig_tickByTickBidAsk = wrp.Wrapper.tickByTickBidAsk
        orig_tickByTickMidPoint = wrp.Wrapper.tickByTickMidPoint

        def patched_tickByTickAllLast(self, reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions):
            """Use IBKR-provided `time` as the exchange timestamp."""
            ticker = self.reqId2Ticker.get(reqId)
            if not ticker:
                logger.error(f'tickByTickAllLast: Unknown reqId: {reqId}')
                return
            if price != ticker.last:
                ticker.prevLast = ticker.last
                ticker.last = price
            if size != ticker.lastSize:
                ticker.prevLastSize = ticker.lastSize
                ticker.lastSize = size
            # Convert IBKR epoch millis to datetime
            import datetime as dt
            exchange_time = dt.datetime.fromtimestamp(time / 1000, tz=dt.timezone.utc)
            tick = TickByTickAllLast(tickType, exchange_time, price, size, tickAttribLast, exchange, specialConditions)
            ticker.tickByTicks.append(tick)
            self.pendingTickers.add(ticker)

        def patched_tickByTickBidAsk(self, reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
            """Use IBKR-provided `time` as the exchange timestamp."""
            ticker = self.reqId2Ticker.get(reqId)
            if not ticker:
                logger.error(f'tickByTickBidAsk: Unknown reqId: {reqId}')
                return
            if bidPrice != ticker.bid:
                ticker.prevBid = ticker.bid
                ticker.bid = bidPrice
            if bidSize != ticker.bidSize:
                ticker.prevBidSize = ticker.bidSize
                ticker.bidSize = bidSize
            if askPrice != ticker.ask:
                ticker.prevAsk = ticker.ask
                ticker.ask = askPrice
            if askSize != ticker.askSize:
                ticker.prevAskSize = ticker.askSize
                ticker.askSize = askSize
            import datetime as dt
            exchange_time = dt.datetime.fromtimestamp(time / 1000, tz=dt.timezone.utc)
            tick = TickByTickBidAsk(exchange_time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk)
            ticker.tickByTicks.append(tick)
            self.pendingTickers.add(ticker)

        def patched_tickByTickMidPoint(self, reqId, time, midPoint):
            """Use IBKR-provided `time` as the exchange timestamp."""
            ticker = self.reqId2Ticker.get(reqId)
            if not ticker:
                logger.error(f'tickByTickMidPoint: Unknown reqId: {reqId}')
                return
            import datetime as dt
            exchange_time = dt.datetime.fromtimestamp(time / 1000, tz=dt.timezone.utc)
            tick = TickByTickMidPoint(exchange_time, midPoint)
            ticker.tickByTicks.append(tick)
            self.pendingTickers.add(ticker)

        wrp.Wrapper.tickByTickAllLast = patched_tickByTickAllLast
        wrp.Wrapper.tickByTickBidAsk = patched_tickByTickBidAsk
        wrp.Wrapper.tickByTickMidPoint = patched_tickByTickMidPoint

        logger.info("Applied ib_insync timestamp patch: using exchange timestamps for tick-by-tick data")

    except ImportError:
        logger.warning("ib_insync not available, timestamp patch skipped")
    except Exception as e:
        logger.error(f"Failed to apply ib_insync timestamp patch: {e}")
