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

                # 若指定了 account_ids，找出当前仓位列表中缺失的 symbol 并写入 quantity=0
                # 用于处理 ib.positions() 不返回零仓位的情形
                if account_ids:
                    current_symbols = {
                        (p["account_id"], p["symbol"]) for p in positions
                    }
                    for aid in account_ids:
                        held = await conn.fetch(
                            "SELECT DISTINCT ON (symbol) symbol FROM positions "
                            "WHERE account_id = $1 AND quantity != 0 "
                            "ORDER BY symbol, time DESC",
                            aid,
                        )
                        for r in held:
                            if (aid, r["symbol"]) not in current_symbols:
                                await conn.execute(
                                    "INSERT INTO positions(time, account_id, symbol, quantity) "
                                    "VALUES($1, $2, $3, 0)",
                                    now, aid, r["symbol"],
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

    async def sync_executions(self, fills: list):
        """批量写入历史成交记录（来自 reqExecutionsAsync）。"""
        if not fills:
            return
        try:
            async with self.pool.acquire() as conn:
                for fill in fills:
                    e = fill.execution
                    await conn.execute(
                        "INSERT INTO executions(time,exec_id,order_id,account_id,symbol,side,"
                        "quantity,price,commission) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) "
                        "ON CONFLICT(exec_id) DO NOTHING",
                        e.time if hasattr(e, 'time') else datetime.now(timezone.utc),
                        e.execId,
                        e.orderId,
                        e.acctNumber,
                        fill.contract.symbol,
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
