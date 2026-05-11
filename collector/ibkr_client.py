import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from config import BARK_KEY, BARK_SERVER, NOTIFY_THRESHOLD_SECONDS, PRODUCT_ROLL_CONFIG
from daily_tracker import _bucket_time
from daily_tracker import _effective_date_str as _get_effective_date_str
from daily_tracker import _parse_trading_days_str
from ib_insync import IB, Contract, Stock, Ticker
from notifier import BarkNotifier


logger = logging.getLogger(__name__)


class IBKRClient:
    def __init__(self, host: str, port: int, client_id: int):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()
        self._tickers: dict[str, Ticker] = {}
        self._symbol_map: dict[int, str] = {}  # conId -> symbol mapping
        self._tick_callbacks = []  # (symbol, price, size, time) callbacks
        self._retry = 0
        self._subscriptions = {}

        self._trading_days: dict[str, set[str]] = {}  # symbol -> set of YYYYMMDD trading days

        self._notifier = BarkNotifier(BARK_SERVER, BARK_KEY)
        self._first_fail_time = None
        self._alert_sent = False

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
        if self._alert_sent:
            loop = asyncio.get_event_loop()
            duration = int(loop.time() - self._first_fail_time)
            asyncio.create_task(
                self._notifier.send_notification(
                    "✅ IBKR 连接已恢复", f"连接已成功建立，故障持续 {duration} 秒。"
                )
            )
        self._first_fail_time = None
        self._alert_sent = False
        self._retry = 0
        logger.info("Connected to IB Gateway")

    async def connect_with_retry(self):
        while True:
            try:
                await self.connect()
                return
            except Exception as e:
                loop = asyncio.get_event_loop()
                now = loop.time()

                if self._first_fail_time is None:
                    self._first_fail_time = now

                elapsed = now - self._first_fail_time
                if elapsed >= NOTIFY_THRESHOLD_SECONDS and not self._alert_sent:
                    asyncio.create_task(
                        self._notifier.send_notification(
                            "🚨 IBKR 连接故障",
                            f"错误详情: {e}\n重试次数: {self._retry}\n持续时间: {int(elapsed)} 秒",
                        )
                    )
                    self._alert_sent = True

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

        # Fetch contract details to extract tradingHours
        try:
            details = await self.ib.reqContractDetailsAsync(contract)
            if details and details[0].tradingHours:
                self._trading_days[symbol] = _parse_trading_days_str(details[0].tradingHours)
                logger.info(f"Cached {len(self._trading_days[symbol])} trading days for {symbol}")
        except Exception as e:
            logger.warning(f"Failed to fetch tradingHours for {symbol}: {e}")

        # Store conId -> symbol mapping for event callbacks
        self._symbol_map[contract.conId] = symbol

        # reqMktData for bid/ask/volume/daily OHLC and real-time trade ticks
        ticker = self.ib.reqMktData(contract, "", False, False)
        self._tickers[symbol] = ticker

        def _on_mkt_data_update(ticker, symbol=symbol):
            if self._data_suspended:
                logger.info(
                    f"Market data received for {symbol}. Stopping auto-reconnect."
                )
                self._data_suspended = False

            price = ticker.last if hasattr(ticker, "last") else None
            size = ticker.lastSize if hasattr(ticker, "lastSize") else 0.0

            if (
                price is not None
                and not (isinstance(price, float) and math.isnan(price))
                and price > 0
            ):
                for cb in self._tick_callbacks:
                    try:
                        cb(symbol, float(price), float(size or 0), ticker.time)
                    except Exception as e:
                        logger.error(f"Tick callback error: {e}")

        ticker.updateEvent += _on_mkt_data_update

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
                # Shift weekend dates to the following Monday
                d = datetime.strptime(ds, "%Y%m%d")
                if d.weekday() == 5:  # Saturday -> Monday
                    d += timedelta(days=2)
                elif d.weekday() == 6:  # Sunday -> Monday
                    d += timedelta(days=1)
                ds = d.strftime("%Y%m%d")

                # Skip the current incomplete bar when roll hour shifts its date.
                # IBKR's current daily bar has OHLC covering the full trading session.
                # After roll hour, _effective_date_str correctly reassigns the date to the
                # next trading day, but the OHLC values still include pre-roll-hour data.
                # The DailyBarTracker correctly tracks post-roll-hour data from live ticks.
                if isinstance(b.date, datetime):
                    config = PRODUCT_ROLL_CONFIG.get(symbol)
                    if config:
                        tz = ZoneInfo(config["timezone"])
                        local_dt = b.date.astimezone(tz) if b.date.tzinfo is not None else b.date.replace(tzinfo=timezone.utc).astimezone(tz)
                        if local_dt.weekday() < 5:  # Weekday check (weekend handled above)
                            is_after_roll = (local_dt.hour > config["roll_hour"] or
                                             (local_dt.hour == config["roll_hour"] and local_dt.minute >= config["roll_minute"]))
                            if is_after_roll:
                                logger.info(f"Skipping current incomplete bar for {symbol}: time {local_dt} rolled from {local_dt.strftime('%Y%m%d')} to {ds}")
                                continue

                existing = next((r for r in result if r["date_str"] == ds), None)
                if existing:
                    existing["high"] = max(existing["high"], b.high)
                    existing["low"] = min(existing["low"], b.low)
                    existing["close"] = b.close
                    existing["volume"] += int(b.volume) if b.volume > 0 else 0
                else:
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

    async def refresh_trading_days(self):
        """Refresh tradingHours cache for all subscribed symbols."""
        for symbol, params in self._subscriptions.items():
            try:
                contract_symbol = symbol
                if params["sec_type"] == "CASH" and "." in symbol:
                    contract_symbol = symbol.split(".")[0]
                contract = Contract(
                    symbol=contract_symbol,
                    secType=params["sec_type"],
                    exchange=params["exchange"],
                    currency=params["currency"],
                )
                details = await self.ib.reqContractDetailsAsync(contract)
                if details and details[0].tradingHours:
                    self._trading_days[symbol] = _parse_trading_days_str(details[0].tradingHours)
            except Exception as e:
                logger.warning(f"Failed to refresh tradingHours for {symbol}: {e}")

    def unsubscribe(self, symbol: str):
        self._subscriptions.pop(symbol, None)
        ticker = self._tickers.pop(symbol, None)
        if ticker:
            self.ib.cancelMktData(ticker.contract)

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
