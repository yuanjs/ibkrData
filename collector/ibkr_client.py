import asyncio
import logging
import math

from ib_insync import IB, Contract, Stock, Ticker

from daily_tracker import _effective_date_str as _get_effective_date_str, _bucket_time

logger = logging.getLogger(__name__)


class IBKRClient:
    def __init__(self, host: str, port: int, client_id: int):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()
        self._tickers: dict[str, Ticker] = {}
        self._tick_tickers: dict[str, Ticker] = {}  # tick-by-tick tickers
        self._symbol_map: dict[int, str] = {}  # conId -> symbol mapping
        self._tick_callbacks = []  # (symbol, price, size, time) callbacks
        self._retry = 0
        self._subscriptions = {}

        self.ib.disconnectedEvent += self._on_disconnect
        self.ib.connectedEvent += self._on_connect
        self.ib.errorEvent += self._on_error

        self._reconnect_task = None
        self._data_suspended = False

    def _on_error(self, reqId, errorCode, errorString, contract):
        if errorCode == 10197:
            logger.error(
                f"Error 10197: Market data disconnected (likely competing login). Suspending data..."
            )
            self._data_suspended = True
            if self._reconnect_task is None or self._reconnect_task.done():
                self._reconnect_task = asyncio.create_task(
                    self._auto_reconnect_market_data()
                )

    async def _auto_reconnect_market_data(self):
        while self._data_suspended:
            await asyncio.sleep(15)
            logger.info("Auto-reconnecting market data (Code: 10197 recovery)...")
            await self._resubscribe_all()

    def register_tick_handler(self, callback):
        """Register a callback for individual trade ticks: callback(symbol, price, size, time)"""
        self._tick_callbacks.append(callback)

    async def connect(self):
        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
        self._retry = 0
        logger.info("Connected to IB Gateway")

    async def connect_with_retry(self):
        while True:
            try:
                await self.connect()
                return
            except Exception as e:
                wait = min(2**self._retry, 60)
                logger.warning(f"Connection failed: {e}. Retrying in {wait}s")
                self._retry += 1
                await asyncio.sleep(wait)

    def _on_disconnect(self):
        logger.warning("Disconnected from IB Gateway")
        asyncio.ensure_future(self.connect_with_retry())

    def _on_connect(self):
        logger.info("Connected to IB Gateway")
        asyncio.ensure_future(self._resubscribe_all())

    async def _resubscribe_all(self):
        if not self._subscriptions:
            return

        logger.info(f"Re-subscribing to {len(self._subscriptions)} symbols...")
        # Clear existing tickers as they are bound to the old connection
        self._tickers.clear()
        self._tick_tickers.clear()
        self._symbol_map.clear()

        # Deep copy the subscriptions to avoid mutation during iteration
        subs = list(self._subscriptions.values())
        for params in subs:
            try:
                await self.subscribe(**params)
            except Exception as e:
                logger.error(f"Failed to re-subscribe to {params.get('symbol')}: {e}")

    async def subscribe(
        self, symbol: str, sec_type="STK", exchange="SMART", currency="USD"
    ):
        # Record subscription parameters for re-subscription on disconnect
        self._subscriptions[symbol] = {
            "symbol": symbol,
            "sec_type": sec_type,
            "exchange": exchange,
            "currency": currency,
        }

        if symbol in self._tickers:
            return

        # For Forex (CASH), we might use a symbol like 'USD.JPY' in our system
        # but IBKR expects the base currency (e.g. 'USD') as the contract symbol.
        contract_symbol = symbol
        if sec_type == "CASH" and "." in symbol:
            contract_symbol = symbol.split(".")[0]

        contract = Contract(
            symbol=contract_symbol,
            secType=sec_type,
            exchange=exchange,
            currency=currency,
        )

        # For futures without a specific expiry, use CONTFUT to find the rolling active contract
        if sec_type == "FUT" and not contract.lastTradeDateOrContractMonth:
            logger.info(f"Resolving active rolling future for {symbol} via CONTFUT...")

            # Step 1: Use CONTFUT to identify which contract IBKR considers active
            cont_contract = Contract(
                secType="CONTFUT", symbol=symbol, exchange=exchange, currency=currency
            )
            cont_details = await self.ib.reqContractDetailsAsync(cont_contract)

            if cont_details:
                # CONTFUT returns the currently active rolling contract
                resolved = cont_details[0].contract
                # Build a real FUT contract from the CONTFUT result
                contract = Contract(
                    secType="FUT",
                    symbol=resolved.symbol,
                    exchange=resolved.exchange,
                    currency=resolved.currency,
                    lastTradeDateOrContractMonth=resolved.lastTradeDateOrContractMonth,
                    tradingClass=resolved.tradingClass,
                    multiplier=resolved.multiplier,
                )
                logger.info(
                    f"CONTFUT resolved {symbol} -> {contract.localSymbol} (Exp: {contract.lastTradeDateOrContractMonth})"
                )
            else:
                logger.warning(
                    f"CONTFUT not available for {symbol}, falling back to earliest expiry..."
                )
                cds = await self.ib.reqContractDetailsAsync(contract)
                if cds:
                    sorted_cds = sorted(
                        cds, key=lambda x: x.contract.lastTradeDateOrContractMonth
                    )
                    contract = sorted_cds[0].contract
                    logger.info(
                        f"Fallback resolved {symbol} to {contract.localSymbol} ({contract.lastTradeDateOrContractMonth})"
                    )
                else:
                    logger.error(f"Failed to resolve future contract for {symbol}")
                    return

        qualified = await self.ib.qualifyContractsAsync(contract)
        if qualified:
            contract = qualified[0]

        # Store conId -> symbol mapping for event callbacks
        self._symbol_map[contract.conId] = symbol

        # reqMktData for bid/ask/volume/daily OHLC (kept for quote table)
        ticker = self.ib.reqMktData(contract, "", False, False)
        self._tickers[symbol] = ticker

        def _on_mkt_data_update(ticker, symbol=symbol):
            if self._data_suspended:
                logger.info(
                    f"Market data snapshot received for {symbol}. Stopping auto-reconnect."
                )
                self._data_suspended = False

        ticker.updateEvent += _on_mkt_data_update

        # reqTickByTickData for accurate trade-by-trade streaming
        try:
            # Forex (CASH) doesn't have 'AllLast' (Trades), use 'BidAsk' instead
            tick_type = "BidAsk" if sec_type == "CASH" else "AllLast"
            tick_ticker = self.ib.reqTickByTickData(contract, tick_type)
            self._tick_tickers[symbol] = tick_ticker

            # Wire up the event callback
            def _on_tick_update(ticker, symbol=symbol):
                """Called on each tick-by-tick trade or bid/ask update."""
                if self._data_suspended:
                    logger.info(
                        f"Real-time tick data received for {symbol}. Stopping auto-reconnect."
                    )
                    self._data_suspended = False

                ticks = ticker.tickByTicks
                if not ticks:
                    return
                # Process the latest tick(s)
                for tick in ticks:
                    p = None
                    s = 0.0

                    if hasattr(tick, "price"):
                        # Trade tick (AllLast)
                        p = tick.price
                        s = float(tick.size or 0)
                    elif hasattr(tick, "bidPrice") and hasattr(tick, "askPrice"):
                        # BidAsk tick - use midpoint as a representative price
                        p = (tick.bidPrice + tick.askPrice) / 2
                        s = float((tick.bidSize or 0) + (tick.askSize or 0))

                    if (
                        p is not None
                        and not (isinstance(p, float) and math.isnan(p))
                        and p > 0
                    ):
                        for cb in self._tick_callbacks:
                            try:
                                cb(symbol, p, s, tick.time)
                            except Exception as e:
                                logger.error(f"Tick callback error: {e}")

            tick_ticker.updateEvent += _on_tick_update
            logger.info(
                f"Subscribed tick-by-tick ({tick_type}): {symbol} (Local: {contract.localSymbol}, ConId: {contract.conId})"
            )
        except Exception as e:
            logger.warning(
                f"reqTickByTickData failed for {symbol}: {e}. Falling back to snapshot-only mode."
            )

    async def get_historical_daily_bars(self, symbol: str, duration: str = "1 Y"):
        """Fetch historical daily bars from IBKR."""
        if symbol not in self._subscriptions:
            logger.warning(
                f"Symbol {symbol} not in subscriptions, cannot fetch historical data"
            )
            return []

        params = self._subscriptions[symbol]
        sec_type = params["sec_type"]
        contract_symbol = symbol
        if sec_type == "CASH" and "." in symbol:
            contract_symbol = symbol.split(".")[0]

        contract = Contract(
            symbol=contract_symbol,
            secType=sec_type,
            exchange=params["exchange"],
            currency=params["currency"],
        )

        if sec_type == "FUT":
            # Use CONTFUT directly for historical data to get continuous prices
            # that automatically roll across contract months. Resolving to a
            # specific FUT contract would only return data for that contract's
            # active period (a few months), not the full continuous history.
            contract = Contract(
                secType="CONTFUT",
                symbol=symbol,
                exchange=params["exchange"],
                currency=params["currency"],
            )
        else:
            qualified = await self.ib.qualifyContractsAsync(contract)
            if not qualified:
                logger.error(f"Failed to qualify contract for {symbol}")
                return []
            contract = qualified[0]

        # Use MIDPOINT for CASH (Forex), TRADES for others
        what_to_show = "MIDPOINT" if sec_type == "CASH" else "TRADES"

        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting="1 day",
                whatToShow=what_to_show,
                useRTH=False,
                formatDate=1,
            )
            result = []
            for b in bars:
                ds = _get_effective_date_str(b.date, symbol)
                result.append({
                    "symbol": symbol,
                    "date_str": ds,
                    "time": _bucket_time(ds),
                    "open": b.open,
                    "high": b.high,
                    "low": b.low,
                    "close": b.close,
                    "volume": int(b.volume) if b.volume > 0 else 0,
                })
            return result
        except Exception as e:
            logger.error(f"Error fetching historical bars for {symbol}: {e}")
            return []

    def unsubscribe(self, symbol: str):
        self._subscriptions.pop(symbol, None)
        ticker = self._tickers.pop(symbol, None)
        if ticker:
            self.ib.cancelMktData(ticker.contract)

        tick_ticker = self._tick_tickers.pop(symbol, None)
        if tick_ticker:
            self.ib.cancelTickByTickData(tick_ticker.contract, "AllLast")

    def get_snapshots(self) -> dict:
        result = {}
        for symbol, ticker in self._tickers.items():
            result[symbol] = {
                "bid": ticker.bid,
                "ask": ticker.ask,
                "last": ticker.last,
                "volume": ticker.volume,
                "open": ticker.open,
                "high": ticker.high,
                "low": ticker.low,
                "close": ticker.close,
            }
        return result

    async def get_account_summary(self) -> list[dict]:
        values = await self.ib.accountSummaryAsync()
        by_account: dict[str, dict] = {}
        for v in values:
            acc = by_account.setdefault(v.account, {"account_id": v.account})
            key_map = {
                "NetLiquidation": "net_liquidation",
                "TotalCashValue": "total_cash",
                "AvailableFunds": "available_funds",
                "ExcessLiquidity": "excess_liquidity",
                "InitMarginReq": "init_margin_req",
                "MaintMarginReq": "maint_margin_req",
                "DailyPnL": "daily_pnl",
                "UnrealizedPnL": "unrealized_pnl",
                "RealizedPnL": "realized_pnl",
            }
            if v.tag in key_map:
                acc[key_map[v.tag]] = float(v.value) if v.value else None
        return list(by_account.values())

    def get_positions(self) -> list[dict]:
        return [
            {
                "account_id": p.account,
                "symbol": p.contract.symbol,
                "sec_type": p.contract.secType,
                "quantity": float(p.position),
                "avg_cost": float(p.avgCost),
                "market_value": float(p.marketValue)
                if hasattr(p, "marketValue")
                else None,
                "unrealized_pnl": float(p.unrealizedPNL)
                if hasattr(p, "unrealizedPNL")
                else None,
                "realized_pnl": float(p.realizedPNL)
                if hasattr(p, "realizedPNL")
                else None,
            }
            for p in self.ib.positions()
        ]

    def register_order_handlers(self, on_order, on_exec):
        self.ib.openOrderEvent += on_order
        self.ib.orderStatusEvent += on_order
        self.ib.execDetailsEvent += on_exec

    @property
    def is_connected(self) -> bool:
        return self.ib.isConnected()
