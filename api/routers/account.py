from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends
from db import get_pool
from auth import require_auth
import json
import redis.asyncio as aioredis
from config import REDIS_URL

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])


async def _gateway_account_ids(gateway: str) -> list[str]:
    """从 Redis 获取指定 gateway 的 account_id 列表。"""
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        mapping = json.loads(raw)
        return mapping.get(gateway, [])
    return []


@router.get("/account")
async def get_account(gateway: str | None = None):
    pool = await get_pool()
    query = "SELECT DISTINCT ON (account_id) * FROM account_snapshots"
    args = []
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            query += " WHERE account_id = ANY($1)"
    query += " ORDER BY account_id, time DESC"
    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]


@router.get("/account/history")
async def get_account_history(start: datetime, end: datetime, gateway: str | None = None):
    pool = await get_pool()
    args = [start, end]
    where = "WHERE time BETWEEN $1 AND $2"
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            where += " AND account_id = ANY($3)"
    rows = await pool.fetch(
        "WITH latest AS ("
        "  SELECT DISTINCT ON (bucket, account_id) "
        "         bucket, account_id, net_liquidation, daily_pnl "
        "  FROM ("
        "    SELECT time_bucket('1 day', time) AS bucket, time, account_id, net_liquidation, daily_pnl "
        f"    FROM account_snapshots {where}"
        "  ) snapshots "
        "  ORDER BY bucket, account_id, time DESC"
        ") "
        "SELECT bucket AS time, sum(net_liquidation) AS net_liquidation, sum(daily_pnl) AS daily_pnl "
        "FROM latest GROUP BY bucket ORDER BY bucket",
        *args
    )
    return [dict(r) for r in rows]


@router.get("/positions")
async def get_positions(gateway: str | None = None):
    pool = await get_pool()
    query = "SELECT DISTINCT ON (account_id, symbol) * FROM positions"
    args = []
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            query += " WHERE account_id = ANY($1)"
    query += " ORDER BY account_id, symbol, time DESC"
    # 子查询：先取每 (account_id, symbol) 最新的行，再过滤 quantity=0
    query = f"SELECT * FROM ({query}) sub WHERE quantity != 0"
    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]
