from fastapi import APIRouter, Depends
from db import get_pool
from auth import require_auth
from models import SymbolCreate

router = APIRouter(prefix="/api/symbols", dependencies=[Depends(require_auth)])


@router.get("")
async def list_symbols():
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM subscriptions WHERE active=true ORDER BY symbol")
    return [dict(r) for r in rows]


@router.post("")
async def add_symbol(body: SymbolCreate):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO subscriptions(symbol,sec_type,exchange,currency) VALUES($1,$2,$3,$4) "
        "ON CONFLICT(symbol) DO UPDATE SET active=true",
        body.symbol, body.sec_type, body.exchange, body.currency
    )
    return {"ok": True}


@router.delete("/{symbol}")
async def remove_symbol(symbol: str):
    pool = await get_pool()
    await pool.execute("UPDATE subscriptions SET active=false WHERE symbol=$1", symbol)
    return {"ok": True}
