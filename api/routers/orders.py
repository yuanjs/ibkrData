from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from db import get_pool
from auth import require_auth
from pydantic import BaseModel
from uuid import uuid4
import io
import csv
import json
import redis.asyncio as aioredis
from config import REDIS_URL

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])

# Allowed status filter values
_OPEN_STATUSES = ("Filled", "Cancelled", "Inactive")


async def _gateway_account_ids(gateway: str) -> list[str]:
    """从 Redis 获取指定 gateway 的 account_id 列表。"""
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        mapping = json.loads(raw)
        return mapping.get(gateway, [])
    return []


async def _resolve_gateway(account_id: str) -> str:
    """根据 account_id 查找对应的 gateway 名称。"""
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        mapping = json.loads(raw)
        for gw, ids in mapping.items():
            if account_id in ids:
                return gw
    return "live"  # fallback


class ClosePositionRequest(BaseModel):
    symbol: str
    gateway: str = "live"


@router.get("/orders")
async def get_orders(status: str = "all", start: Optional[datetime] = None,
                     end: Optional[datetime] = None, gateway: Optional[str] = None):
    pool = await get_pool()
    where = []
    args = []
    if status == "open":
        args.append(list(_OPEN_STATUSES))
        where.append(f"status != ALL(${len(args)})")
    if start:
        args.append(start)
        where.append(f"updated_at >= ${len(args)}")
    if end:
        args.append(end)
        where.append(f"updated_at <= ${len(args)}")
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            where.append(f"account_id = ANY(${len(args)})")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = await pool.fetch(f"SELECT * FROM orders {clause} ORDER BY updated_at DESC LIMIT 500", *args)
    return [dict(r) for r in rows]


@router.get("/trades")
async def get_trades(start: Optional[datetime] = None, end: Optional[datetime] = None,
                     symbol: Optional[str] = None, gateway: Optional[str] = None):
    pool = await get_pool()
    where, args = [], []
    if start:
        args.append(start)
        where.append(f"time >= ${len(args)}")
    if end:
        args.append(end)
        where.append(f"time <= ${len(args)}")
    if symbol:
        args.append(symbol)
        where.append(f"symbol = ${len(args)}")
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            where.append(f"account_id = ANY(${len(args)})")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = await pool.fetch(f"SELECT * FROM executions {clause} ORDER BY time DESC LIMIT 1000", *args)
    return [dict(r) for r in rows]


@router.get("/trades/export")
async def export_trades(start: Optional[datetime] = None, end: Optional[datetime] = None,
                        symbol: Optional[str] = None, gateway: Optional[str] = None):
    rows = await get_trades(start, end, symbol, gateway)
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows([{k: str(v) for k, v in r.items()} for r in rows])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=trades.csv"})


@router.get("/pnl")
async def get_pnl(gateway: Optional[str] = None):
    pool = await get_pool()
    query = """
        SELECT symbol,
               sum(quantity * price * CASE WHEN side='BOT' THEN -1 ELSE 1 END) AS realized_pnl,
               count(*) AS trade_count
        FROM executions
    """
    args = []
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            query += " WHERE account_id = ANY($1)"
    query += " GROUP BY symbol ORDER BY realized_pnl"
    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]


@router.post("/positions/close")
async def close_position(req: ClosePositionRequest):
    pool = await get_pool()
    close_id = str(uuid4())

    # 按 gateway 过滤对应 account_ids，确保只查该 gateway 下的持仓
    ids = await _gateway_account_ids(req.gateway)
    if not ids:
        from fastapi import HTTPException
        raise HTTPException(400, f"{req.gateway} 网关未就绪，无法查询持仓")

    row = await pool.fetchrow(
        "SELECT DISTINCT ON (symbol) * FROM positions "
        "WHERE symbol = $1 AND account_id = ANY($2) "
        "ORDER BY symbol, time DESC",
        req.symbol, ids
    )
    if not row or row["quantity"] == 0:
        from fastapi import HTTPException
        raise HTTPException(400, f"{req.symbol} 无持仓")

    # 自动计算平仓方向
    side = "SELL" if row["quantity"] > 0 else "BUY"
    qty = int(abs(row["quantity"]))

    # 从 subscriptions 表获取品种参数
    sub = await pool.fetchrow(
        "SELECT sec_type, exchange, currency FROM subscriptions WHERE symbol = $1",
        req.symbol
    )
    sec_type = sub["sec_type"] if sub else row.get("sec_type", "STK")
    exchange = sub["exchange"] if sub else "SMART"
    currency = sub["currency"] if sub else "USD"

    # 使用请求中明确的 gateway 路由，不依赖 account_id 推测
    channel = f"order:command:{req.gateway}"

    r = aioredis.from_url(REDIS_URL)
    await r.publish(channel, json.dumps({
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "sec_type": sec_type,
        "exchange": exchange,
        "currency": currency,
        "account_id": row["account_id"],
    }))
    await r.aclose()

    return {
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "message": "平仓指令已发送",
    }
