import logging
from datetime import date, datetime, timezone

import asyncpg

logger = logging.getLogger(__name__)


import math


def _clean_num(val):
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except:
        return None


def _clean_int(val):
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return int(f)
    except:
        return None


def _contract_month_from_ib_contract(contract):
    raw = getattr(contract, "lastTradeDateOrContractMonth", None)
    return raw[:6] if raw and len(raw) >= 6 else raw


def _last_trade_date_from_ib_contract(contract):
    raw = getattr(contract, "lastTradeDateOrContractMonth", None) or ""
    if len(raw) < 8:
        return None
    try:
        return date.fromisoformat(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    except ValueError:
        return None


def _parse_daily_bar_date(val):
    if isinstance(val, datetime):
        d = val.astimezone(timezone.utc).date() if val.tzinfo else val.date()
    elif isinstance(val, date):
        d = val
    elif isinstance(val, str):
        s = val.strip()
        if len(s) < 8 or not s[:8].isdigit():
            return None
        try:
            d = date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")
        except ValueError:
            return None
    else:
        return None
    return d.strftime("%Y%m%d"), datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


class DataWriter:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    def _futures_tick_record(self, row):
        if not isinstance(row, dict):
            return row
        price = _clean_num(row.get("last", row.get("price")))
        size = _clean_int(row.get("volume", row.get("size")))
        return (
            row["time"],
            row["symbol"],
            int(row["con_id"]),
            row.get("local_symbol"),
            row.get("trading_class"),
            row.get("contract_month"),
            row.get("last_trade_date"),
            row.get("exchange"),
            row.get("currency"),
            row.get("multiplier"),
            _clean_num(row.get("bid")),
            _clean_num(row.get("ask")),
            price,
            size,
            _clean_num(row.get("open", price)),
            _clean_num(row.get("high", price)),
            _clean_num(row.get("low", price)),
            _clean_num(row.get("close", price)),
            row.get("source", "IBKR"),
        )

    def _futures_minute_bar_record(self, row):
        if not isinstance(row, dict):
            return row
        return (
            row["time"],
            row["symbol"],
            int(row["con_id"]),
            row.get("local_symbol"),
            row.get("trading_class"),
            row.get("contract_month"),
            row.get("last_trade_date"),
            row.get("exchange"),
            row.get("currency"),
            row.get("multiplier"),
            _clean_num(row.get("open")),
            _clean_num(row.get("high")),
            _clean_num(row.get("low")),
            _clean_num(row.get("close")),
            _clean_int(row.get("volume")),
            _clean_int(row.get("bar_count")),
        )

    def _futures_contract_record(self, row):
        if not isinstance(row, dict):
            return row
        return (
            row["symbol"],
            int(row["con_id"]),
            row.get("local_symbol"),
            row.get("trading_class"),
            row.get("contract_month"),
            row.get("last_trade_date"),
            row.get("exchange"),
            row.get("currency"),
            row.get("multiplier"),
            row.get("source", "live_collector"),
        )

    def _futures_daily_bar_record(self, row):
        if not isinstance(row, dict):
            return row
        return (
            row["symbol"],
            int(row["con_id"]),
            row["date_str"],
            row["time"],
            row.get("local_symbol"),
            row.get("trading_class"),
            row.get("contract_month"),
            row.get("last_trade_date"),
            row.get("exchange"),
            row.get("currency"),
            row.get("multiplier"),
            _clean_num(row.get("open")),
            _clean_num(row.get("high")),
            _clean_num(row.get("low")),
            _clean_num(row.get("close")),
            _clean_int(row.get("volume")),
            _clean_int(row.get("bar_count")),
        )

    @staticmethod
    def futures_contract_identity(symbol: str, contract) -> dict:
        con_id = getattr(contract, "conId", None)
        return {
            "symbol": symbol,
            "con_id": int(con_id) if con_id else None,
            "local_symbol": getattr(contract, "localSymbol", None) or None,
            "trading_class": getattr(contract, "tradingClass", None) or None,
            "contract_month": _contract_month_from_ib_contract(contract),
            "last_trade_date": _last_trade_date_from_ib_contract(contract),
            "exchange": getattr(contract, "exchange", None) or None,
            "currency": getattr(contract, "currency", None) or None,
            "multiplier": getattr(contract, "multiplier", None) or None,
        }

    @staticmethod
    def futures_daily_bar_rows(symbol: str, contract, bars: list) -> list[dict]:
        identity = DataWriter.futures_contract_identity(symbol, contract)
        return DataWriter.futures_daily_bar_rows_from_identity(identity, bars)

    @staticmethod
    def futures_daily_bar_rows_from_identity(identity: dict, bars: list) -> list[dict]:
        if not identity.get("con_id"):
            return []
        rows = []
        for bar in bars:
            parsed = _parse_daily_bar_date(getattr(bar, "date", None))
            if parsed is None:
                continue
            date_str, ts = parsed
            rows.append(
                {
                    **identity,
                    "date_str": date_str,
                    "time": ts,
                    "open": getattr(bar, "open", None),
                    "high": getattr(bar, "high", None),
                    "low": getattr(bar, "low", None),
                    "close": getattr(bar, "close", None),
                    "volume": getattr(bar, "volume", None),
                    "bar_count": getattr(bar, "barCount", None),
                }
            )
        return rows

    async def write_raw_ticks(self, rows: list[tuple]):
        """Batch insert raw trade ticks into the ticks table."""
        if not rows:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO ticks(time,symbol,last,volume,open,high,low,close) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                    rows,
                )
        except Exception as e:
            logger.error(f"write_raw_ticks error: {e}")

    async def write_futures_ticks(self, rows: list[dict | tuple]):
        """Batch insert raw futures ticks keyed by real contract identity."""
        if not rows:
            return
        try:
            records = [self._futures_tick_record(r) for r in rows]
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO futures_ticks("
                    "time,symbol,con_id,local_symbol,trading_class,contract_month,"
                    "last_trade_date,exchange,currency,multiplier,bid,ask,last,"
                    "volume,open,high,low,close,source"
                    ") VALUES("
                    "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19"
                    ")",
                    records,
                )
        except Exception as e:
            logger.error(f"write_futures_ticks error: {e}")

    async def upsert_futures_minute_bars_from_live(self, rows: list[dict | tuple]):
        """Upsert real-time futures minute bars into the raw futures bar table."""
        if not rows:
            return
        try:
            records = [self._futures_minute_bar_record(r) for r in rows]
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO futures_minute_bars("
                    "time,symbol,con_id,local_symbol,trading_class,contract_month,"
                    "last_trade_date,exchange,currency,multiplier,"
                    "open,high,low,close,volume,bar_count"
                    ") VALUES("
                    "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16"
                    ") ON CONFLICT (symbol, con_id, time) DO UPDATE SET "
                    "local_symbol=EXCLUDED.local_symbol,"
                    "trading_class=EXCLUDED.trading_class,"
                    "contract_month=EXCLUDED.contract_month,"
                    "last_trade_date=EXCLUDED.last_trade_date,"
                    "exchange=EXCLUDED.exchange,"
                    "currency=EXCLUDED.currency,"
                    "multiplier=EXCLUDED.multiplier,"
                    "open=EXCLUDED.open,"
                    "high=EXCLUDED.high,"
                    "low=EXCLUDED.low,"
                    "close=EXCLUDED.close,"
                    "volume=EXCLUDED.volume,"
                    "bar_count=EXCLUDED.bar_count",
                    records,
                )
        except Exception as e:
            logger.error(f"upsert_futures_minute_bars_from_live error: {e}")

    async def upsert_futures_contracts(self, rows: list[dict | tuple]):
        """Upsert live-discovered futures contract metadata."""
        if not rows:
            return
        try:
            records = [self._futures_contract_record(r) for r in rows if r]
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO futures_contracts("
                    "symbol,con_id,local_symbol,trading_class,contract_month,"
                    "last_trade_date,exchange,currency,multiplier,source"
                    ") VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) "
                    "ON CONFLICT (symbol, con_id) DO UPDATE SET "
                    "local_symbol=EXCLUDED.local_symbol,"
                    "trading_class=EXCLUDED.trading_class,"
                    "contract_month=EXCLUDED.contract_month,"
                    "last_trade_date=EXCLUDED.last_trade_date,"
                    "exchange=EXCLUDED.exchange,"
                    "currency=EXCLUDED.currency,"
                    "multiplier=EXCLUDED.multiplier,"
                    "source=EXCLUDED.source,"
                    "last_seen_at=NOW()",
                    records,
                )
        except Exception as e:
            logger.error(f"upsert_futures_contracts error: {e}")

    async def upsert_futures_daily_bars_from_live(self, rows: list[dict | tuple]):
        """Upsert IBKR daily bars for real futures contracts."""
        if not rows:
            return
        try:
            records = [self._futures_daily_bar_record(r) for r in rows]
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO futures_daily_bars("
                    "symbol,con_id,date_str,time,local_symbol,trading_class,"
                    "contract_month,last_trade_date,exchange,currency,multiplier,"
                    "open,high,low,close,volume,bar_count"
                    ") VALUES("
                    "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17"
                    ") ON CONFLICT (symbol, con_id, date_str) DO UPDATE SET "
                    "time=EXCLUDED.time,"
                    "local_symbol=EXCLUDED.local_symbol,"
                    "trading_class=EXCLUDED.trading_class,"
                    "contract_month=EXCLUDED.contract_month,"
                    "last_trade_date=EXCLUDED.last_trade_date,"
                    "exchange=EXCLUDED.exchange,"
                    "currency=EXCLUDED.currency,"
                    "multiplier=EXCLUDED.multiplier,"
                    "open=EXCLUDED.open,"
                    "high=EXCLUDED.high,"
                    "low=EXCLUDED.low,"
                    "close=EXCLUDED.close,"
                    "volume=EXCLUDED.volume,"
                    "bar_count=EXCLUDED.bar_count",
                    records,
                )
        except Exception as e:
            logger.error(f"upsert_futures_daily_bars_from_live error: {e}")

    async def write_account(self, accounts: list[dict]):
        now = datetime.now(timezone.utc)
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO account_snapshots(time,account_id,net_liquidation,total_cash,"
                    "available_funds,excess_liquidity,init_margin_req,maint_margin_req,"
                    "daily_pnl,unrealized_pnl,realized_pnl) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    [
                        (
                            now,
                            a["account_id"],
                            _clean_num(a.get("net_liquidation")),
                            _clean_num(a.get("total_cash")),
                            _clean_num(a.get("available_funds")),
                            _clean_num(a.get("excess_liquidity")),
                            _clean_num(a.get("init_margin_req")),
                            _clean_num(a.get("maint_margin_req")),
                            _clean_num(a.get("daily_pnl")),
                            _clean_num(a.get("unrealized_pnl")),
                            _clean_num(a.get("realized_pnl")),
                        )
                        for a in accounts
                    ],
                )
        except Exception as e:
            logger.error(f"write_account error: {e}")

    async def write_positions(self, positions: list[dict], account_ids: list[str] | None = None):
        now = datetime.now(timezone.utc)
        try:
            async with self.pool.acquire() as conn:
                # 写入当前仓位
                if positions:
                    await conn.executemany(
                        "INSERT INTO positions(time,account_id,symbol,con_id,local_symbol,"
                        "contract_month,trading_class,exchange,currency,multiplier,sec_type,"
                        "quantity,avg_cost,market_value,unrealized_pnl,realized_pnl) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
                        [
                            (
                                now,
                                p["account_id"],
                                p["symbol"],
                                _clean_int(p.get("con_id")),
                                p.get("local_symbol"),
                                p.get("contract_month"),
                                p.get("trading_class"),
                                p.get("exchange"),
                                p.get("currency"),
                                p.get("multiplier"),
                                p["sec_type"],
                                _clean_num(p["quantity"]),
                                _clean_num(p["avg_cost"]),
                                _clean_num(p.get("market_value")),
                                _clean_num(p.get("unrealized_pnl")),
                                _clean_num(p.get("realized_pnl")),
                            )
                            for p in positions
                        ],
                    )

                # 若指定了 account_ids，找出当前仓位列表中缺失的 symbol 并写入 quantity=0
                # 用于处理 ib.positions() 不返回零仓位的情形
                if account_ids:
                    current_symbols = {
                        (
                            p["account_id"],
                            p["symbol"],
                            _clean_int(p.get("con_id")),
                            p.get("local_symbol"),
                        )
                        for p in positions
                    }
                    for aid in account_ids:
                        held = await conn.fetch(
                            "SELECT DISTINCT ON (symbol, con_id, local_symbol) "
                            "symbol, con_id, local_symbol, contract_month, trading_class, "
                            "exchange, currency, multiplier, sec_type "
                            "FROM positions "
                            "WHERE account_id = $1 AND quantity != 0 "
                            "ORDER BY symbol, con_id, local_symbol, time DESC",
                            aid,
                        )
                        for r in held:
                            key = (
                                aid,
                                r["symbol"],
                                _clean_int(r["con_id"]),
                                r["local_symbol"],
                            )
                            if key not in current_symbols:
                                await conn.execute(
                                    "INSERT INTO positions(time, account_id, symbol, con_id, "
                                    "local_symbol, contract_month, trading_class, exchange, "
                                    "currency, multiplier, sec_type, quantity) "
                                    "VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 0)",
                                    now,
                                    aid,
                                    r["symbol"],
                                    r["con_id"],
                                    r["local_symbol"],
                                    r["contract_month"],
                                    r["trading_class"],
                                    r["exchange"],
                                    r["currency"],
                                    r["multiplier"],
                                    r["sec_type"],
                                )
        except Exception as e:
            logger.error(f"write_positions error: {e}")

    async def upsert_order(self, trade):
        """Insert or update an order using proper ON CONFLICT with the order_id PRIMARY KEY."""
        o = trade.order
        s = trade.orderStatus
        now = datetime.now(timezone.utc)
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO orders(order_id,account_id,symbol,con_id,local_symbol,"
                    "contract_month,trading_class,exchange,currency,multiplier,action,order_type,"
                    "quantity,limit_price,status,filled_qty,avg_fill_price,created_at,updated_at) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19) "
                    "ON CONFLICT(order_id) DO UPDATE SET "
                    "status=EXCLUDED.status, filled_qty=EXCLUDED.filled_qty, "
                    "avg_fill_price=EXCLUDED.avg_fill_price, updated_at=EXCLUDED.updated_at, "
                    "con_id=COALESCE(EXCLUDED.con_id, orders.con_id), "
                    "local_symbol=COALESCE(EXCLUDED.local_symbol, orders.local_symbol), "
                    "contract_month=COALESCE(EXCLUDED.contract_month, orders.contract_month), "
                    "trading_class=COALESCE(EXCLUDED.trading_class, orders.trading_class), "
                    "exchange=COALESCE(EXCLUDED.exchange, orders.exchange), "
                    "currency=COALESCE(EXCLUDED.currency, orders.currency), "
                    "multiplier=COALESCE(EXCLUDED.multiplier, orders.multiplier)",
                    o.orderId,
                    o.account,
                    trade.contract.symbol,
                    _clean_int(getattr(trade.contract, "conId", None)),
                    getattr(trade.contract, "localSymbol", None) or None,
                    _contract_month_from_ib_contract(trade.contract),
                    getattr(trade.contract, "tradingClass", None) or None,
                    getattr(trade.contract, "exchange", None) or None,
                    getattr(trade.contract, "currency", None) or None,
                    getattr(trade.contract, "multiplier", None) or None,
                    o.action,
                    o.orderType,
                    _clean_num(o.totalQuantity),
                    _clean_num(o.lmtPrice) if o.lmtPrice else None,
                    s.status,
                    _clean_num(s.filled),
                    _clean_num(s.avgFillPrice) if s.avgFillPrice else None,
                    now,
                    now,
                )
        except Exception as e:
            logger.error(f"upsert_order error: {e}")

    async def write_execution(self, trade, fill):
        now = datetime.now(timezone.utc)
        e = fill.execution
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO executions(time,exec_id,order_id,account_id,symbol,con_id,"
                    "local_symbol,contract_month,trading_class,exchange,currency,multiplier,"
                    "side,quantity,price,commission) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) "
                    "ON CONFLICT(exec_id) DO NOTHING",
                    now,
                    e.execId,
                    e.orderId,
                    e.acctNumber,
                    trade.contract.symbol,
                    _clean_int(getattr(trade.contract, "conId", None)),
                    getattr(trade.contract, "localSymbol", None) or None,
                    _contract_month_from_ib_contract(trade.contract),
                    getattr(trade.contract, "tradingClass", None) or None,
                    getattr(trade.contract, "exchange", None) or None,
                    getattr(trade.contract, "currency", None) or None,
                    getattr(trade.contract, "multiplier", None) or None,
                    e.side,
                    float(e.shares),
                    float(e.price),
                    float(fill.commissionReport.commission)
                    if fill.commissionReport
                    else None,
                )
        except Exception as e:
            logger.error(f"write_execution error: {e}")

    async def sync_executions(self, fills: list):
        """批量写入历史成交记录（来自 reqExecutionsAsync）。"""
        if not fills:
            return
        try:
            async with self.pool.acquire() as conn:
                for fill in fills:
                    e = fill.execution
                    await conn.execute(
                        "INSERT INTO executions(time,exec_id,order_id,account_id,symbol,con_id,"
                        "local_symbol,contract_month,trading_class,exchange,currency,multiplier,"
                        "side,quantity,price,commission) "
                        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) "
                        "ON CONFLICT(exec_id) DO NOTHING",
                        e.time if hasattr(e, 'time') else datetime.now(timezone.utc),
                        e.execId,
                        e.orderId,
                        e.acctNumber,
                        fill.contract.symbol,
                        _clean_int(getattr(fill.contract, "conId", None)),
                        getattr(fill.contract, "localSymbol", None) or None,
                        _contract_month_from_ib_contract(fill.contract),
                        getattr(fill.contract, "tradingClass", None) or None,
                        getattr(fill.contract, "exchange", None) or None,
                        getattr(fill.contract, "currency", None) or None,
                        getattr(fill.contract, "multiplier", None) or None,
                        e.side,
                        float(e.shares),
                        float(e.price),
                        float(fill.commissionReport.commission)
                        if fill.commissionReport
                        else None,
                    )
        except Exception as e:
            logger.error(f"sync_executions error: {e}")

    async def upsert_daily_bars(self, bars: list[dict], update_open: bool = True):
        if not bars:
            return
        try:
            async with self.pool.acquire() as conn:
                update_cols = "high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, time=EXCLUDED.time"
                if update_open:
                    update_cols = f"open=EXCLUDED.open, {update_cols}"
                await conn.executemany(
                    f"INSERT INTO daily_bars(symbol, date_str, time, open, high, low, close, volume) "
                    "VALUES($1, $2, $3, $4, $5, $6, $7, $8) "
                    f"ON CONFLICT(symbol, date_str) DO UPDATE SET {update_cols}",
                    [
                        (
                            b["symbol"],
                            b["date_str"],
                            b["time"],
                            b["open"],
                            b["high"],
                            b["low"],
                            b["close"],
                            b["volume"],
                        )
                        for b in bars
                    ],
                )
        except Exception as e:
            logger.error(f"upsert_daily_bars error: {e}")

    async def delete_daily_bars(self, bars: list[dict]):
        """Delete stale daily bars by (symbol, date_str) pairs."""
        if not bars:
            return
        try:
            async with self.pool.acquire() as conn:
                for b in bars:
                    await conn.execute(
                        "DELETE FROM daily_bars WHERE symbol=$1 AND date_str=$2",
                        b["symbol"],
                        b["date_str"],
                    )
                    logger.info(f"Deleted stale daily bar: {b['symbol']} date={b['date_str']}")
        except Exception as e:
            logger.error(f"delete_daily_bars error: {e}")
