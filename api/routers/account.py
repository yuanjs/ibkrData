from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends
from db import get_pool
from auth import require_auth

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])


@router.get("/account")
async def get_account():
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT ON (account_id) * FROM account_snapshots ORDER BY account_id, time DESC"
    )
    return [dict(r) for r in rows]


@router.get("/account/history")
async def get_account_history(start: datetime, end: datetime):
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT time, account_id, net_liquidation, daily_pnl FROM account_snapshots "
        "WHERE time BETWEEN $1 AND $2 ORDER BY time",
        start, end
    )
    return [dict(r) for r in rows]


@router.get("/positions")
async def get_positions():
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT DISTINCT ON (account_id, symbol) * FROM positions ORDER BY account_id, symbol, time DESC"
    )
    return [dict(r) for r in rows]
