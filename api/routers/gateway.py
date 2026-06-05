import json

import redis.asyncio as aioredis
from auth import require_auth
from config import REDIS_URL
from fastapi import APIRouter, Depends

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])


@router.get("/gateway/map")
async def get_gateway_map():
    """Return gateway->account_id mapping, format: {"live": ["U123456"], "paper": ["DU987654"]}"""
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        return json.loads(raw)
    return {}
