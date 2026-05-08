from fastapi import APIRouter, Depends
from db import get_pool
from auth import require_auth
from models import AlertCreate, SettingsUpdate
import redis.asyncio as aioredis
from config import REDIS_URL

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])


@router.get("/settings")
async def get_settings():
    pool = await get_pool()
    rows = await pool.fetch("SELECT key, value FROM settings ORDER BY key")
    return {r["key"]: r["value"] for r in rows}


@router.put("/settings")
async def update_settings(body: SettingsUpdate):
    pool = await get_pool()
    data = body.model_dump(exclude_unset=True)
    async with pool.acquire() as conn:
        for key, value in data.items():
            await conn.execute(
                "INSERT INTO settings(key,value,updated_at) VALUES($1,$2,NOW()) "
                "ON CONFLICT(key) DO UPDATE SET value=$2, updated_at=NOW()",
                key, str(value)
            )
    r = aioredis.from_url(REDIS_URL)
    await r.publish("settings:update", "updated")
    await r.aclose()
    return {"ok": True}


@router.get("/alerts")
async def get_alerts():
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM alerts WHERE active=true ORDER BY id")
    return [dict(r) for r in rows]


@router.post("/alerts")
async def create_alert(body: AlertCreate):
    pool = await get_pool()
    row = await pool.fetchrow(
        "INSERT INTO alerts(symbol,alert_type,threshold) VALUES($1,$2,$3) RETURNING id",
        body.symbol, body.alert_type, body.threshold
    )
    return {"id": row["id"]}


@router.delete("/alerts/{alert_id}")
async def delete_alert(alert_id: int):
    pool = await get_pool()
    await pool.execute("UPDATE alerts SET active=false WHERE id=$1", alert_id)
    return {"ok": True}
