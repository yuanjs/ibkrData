import logging
from datetime import datetime, timezone

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


class DataWriter:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def write_ticks(self, snapshots: dict):
        now = datetime.now(timezone.utc)
        rows = []
        for symbol, d in snapshots.items():
            if d.get("last") is not None and not math.isnan(d.get("last")):
                rows.append(
                    (
                        now,
                        symbol,
                        _clean_num(d.get("bid")),
                        _clean_num(d.get("ask")),
                        _clean_num(d.get("last")),
                        _clean_int(d.get("volume")),
                        _clean_num(d.get("open")),
                        _clean_num(d.get("high")),
                        _clean_num(d.get("low")),
                        _clean_num(d.get("close")),
                    )
                )

        if not rows:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO ticks(time,symbol,bid,ask,last,volume,open,high,low,close) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    rows,
                )
        except Exception as e:
            logger.error(f"write_ticks error: {e}")

    async def write_ohlc_bar(
        self,
        time,
        symbol: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
    ):
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ticks(time,symbol,last,open,high,low,close,volume) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                    time,
                    symbol,
                    close,
                    open_,
                    high,
                    low,
                    close,
                    volume,
                )
        except Exception as e:
            logger.error(f"write_ohlc_bar error: {e}")

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

    async def write_positions(self, positions: list[dict]):
        now = datetime.now(timezone.utc)
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO positions(time,account_id,symbol,sec_type,quantity,avg_cost,"
                    "market_value,unrealized_pnl,realized_pnl) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    [
                        (
                            now,
                            p["account_id"],
                            p["symbol"],
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
                    "INSERT INTO orders(order_id,account_id,symbol,action,order_type,"
                    "quantity,limit_price,status,filled_qty,avg_fill_price,created_at,updated_at) "
                    "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) "
                    "ON CONFLICT(order_id) DO UPDATE SET "
                    "status=EXCLUDED.status, filled_qty=EXCLUDED.filled_qty, "
                    "avg_fill_price=EXCLUDED.avg_fill_price, updated_at=EXCLUDED.updated_at",
                    o.orderId,
                    o.account,
                    trade.contract.symbol,
                    o.action,
                    o.orderType,
                    float(o.totalQuantity),
                    float(o.lmtPrice) if o.lmtPrice else None,
                    s.status,
                    float(s.filled),
                    float(s.avgFillPrice) if s.avgFillPrice else None,
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
                    "INSERT INTO executions(time,exec_id,order_id,account_id,symbol,side,"
                    "quantity,price,commission) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) "
                    "ON CONFLICT(exec_id) DO NOTHING",
                    now,
                    e.execId,
                    e.orderId,
                    e.acctNumber,
                    trade.contract.symbol,
                    e.side,
                    float(e.shares),
                    float(e.price),
                    float(fill.commissionReport.commission)
                    if fill.commissionReport
                    else None,
                )
        except Exception as e:
            logger.error(f"write_execution error: {e}")

    async def upsert_daily_bars(self, bars: list[dict]):
        if not bars:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO daily_bars(symbol, date_str, time, open, high, low, close, volume) "
                    "VALUES($1, $2, $3, $4, $5, $6, $7, $8) "
                    "ON CONFLICT(symbol, date_str) DO UPDATE SET "
                    "open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, "
                    "close=EXCLUDED.close, volume=EXCLUDED.volume, time=EXCLUDED.time",
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
