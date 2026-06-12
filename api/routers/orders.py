from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
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
    con_id: Optional[int] = None
    local_symbol: Optional[str] = None


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
        raise HTTPException(400, f"{req.gateway} 网关未就绪，无法查询持仓")

    rows = await pool.fetch(
        """
        SELECT *
        FROM (
            SELECT DISTINCT ON (account_id, symbol, con_id, local_symbol) *
            FROM positions
            WHERE symbol = $1
              AND account_id = ANY($2)
              AND ($3::bigint IS NULL OR con_id = $3)
              AND ($4::text IS NULL OR local_symbol = $4)
            ORDER BY account_id, symbol, con_id, local_symbol, time DESC
        ) latest
        WHERE quantity != 0
        ORDER BY time DESC
        """,
        req.symbol,
        ids,
        req.con_id,
        req.local_symbol,
    )
    if not rows:
        raise HTTPException(400, f"{req.symbol} 无持仓")
    if len(rows) > 1 and req.con_id is None and req.local_symbol is None:
        raise HTTPException(
            400,
            f"{req.symbol} 存在多个未平期货合约，请指定 con_id 或 local_symbol",
        )
    position = dict(rows[0])

    # 自动计算平仓方向
    side = "SELL" if position["quantity"] > 0 else "BUY"
    qty = int(abs(position["quantity"]))

    # 从 subscriptions 表获取品种参数
    sub = await pool.fetchrow(
        "SELECT sec_type, exchange, currency FROM subscriptions WHERE symbol = $1",
        req.symbol
    )
    sec_type = sub["sec_type"] if sub else position.get("sec_type", "STK")
    exchange = sub["exchange"] if sub else "SMART"
    currency = sub["currency"] if sub else "USD"
    warning = None

    contract_payload = {}
    if sec_type == "FUT":
        if position.get("con_id") or position.get("local_symbol"):
            contract_payload = {
                "con_id": position.get("con_id"),
                "local_symbol": position.get("local_symbol"),
                "contract_month": position.get("contract_month"),
                "trading_class": position.get("trading_class"),
                "multiplier": position.get("multiplier"),
            }
            exchange = position.get("exchange") or exchange
            currency = position.get("currency") or currency
        else:
            active_contract = await pool.fetchrow(
                "SELECT * FROM active_futures_contract_asof($1, $2)",
                req.symbol,
                datetime.now(timezone.utc),
            )
            if active_contract:
                contract_payload = {
                    "con_id": active_contract["con_id"],
                    "local_symbol": active_contract["local_symbol"],
                    "contract_month": active_contract["contract_month"],
                    "trading_class": active_contract["trading_class"],
                    "multiplier": active_contract["multiplier"],
                }
                exchange = active_contract["exchange"] or exchange
                currency = active_contract["currency"] or currency
                warning = (
                    f"{req.symbol} 持仓缺少合约身份，已 fallback 到 active futures contract"
                )
            else:
                warning = (
                    f"{req.symbol} 未找到 active futures contract，"
                    "平仓命令将按 symbol/exchange/currency 兼容路径发送"
                )

    # 使用请求中明确的 gateway 路由，不依赖 account_id 推测
    channel = f"order:command:{req.gateway}"

    r = aioredis.from_url(REDIS_URL)
    command = {
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "sec_type": sec_type,
        "exchange": exchange,
        "currency": currency,
        "account_id": position["account_id"],
        **contract_payload,
    }
    await r.publish(channel, json.dumps(command))
    await r.aclose()

    response = {
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "message": "平仓指令已发送",
    }
    if warning:
        response["warning"] = warning
    return response
