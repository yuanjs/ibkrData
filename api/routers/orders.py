from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from db import get_pool
from auth import require_auth
import io
import csv

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])

# Allowed status filter values
_OPEN_STATUSES = ("Filled", "Cancelled", "Inactive")


@router.get("/orders")
async def get_orders(status: str = "all", start: Optional[datetime] = None, end: Optional[datetime] = None):
    pool = await get_pool()
    where = []
    args = []
    if status == "open":
        # Use parameterized ANY to avoid f-string SQL
        args.append(list(_OPEN_STATUSES))
        where.append(f"status != ALL(${len(args)})")
    if start:
        args.append(start)
        where.append(f"updated_at >= ${len(args)}")
    if end:
        args.append(end)
        where.append(f"updated_at <= ${len(args)}")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = await pool.fetch(f"SELECT * FROM orders {clause} ORDER BY updated_at DESC LIMIT 500", *args)
    return [dict(r) for r in rows]


@router.get("/trades")
async def get_trades(start: Optional[datetime] = None, end: Optional[datetime] = None, symbol: Optional[str] = None):
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
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = await pool.fetch(f"SELECT * FROM executions {clause} ORDER BY time DESC LIMIT 1000", *args)
    return [dict(r) for r in rows]


@router.get("/trades/export")
async def export_trades(start: Optional[datetime] = None, end: Optional[datetime] = None, symbol: Optional[str] = None):
    rows = await get_trades(start, end, symbol)
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows([{k: str(v) for k, v in r.items()} for r in rows])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=trades.csv"})


@router.get("/pnl")
async def get_pnl():
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT symbol, sum(quantity * price * CASE WHEN side='BOT' THEN -1 ELSE 1 END) AS realized_pnl, "
        "count(*) AS trade_count FROM executions GROUP BY symbol ORDER BY realized_pnl"
    )
    return [dict(r) for r in rows]
