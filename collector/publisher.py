import json
import math
import redis.asyncio as aioredis


def _sanitize(obj):
    """Replace NaN/Infinity/-1 (IBKR 'no data' sentinel) with None for valid JSON."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj) or obj == -1.0):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj



class Publisher:
    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    async def publish_market(self, symbol: str, data: dict):
        payload = _sanitize({"symbol": symbol, **data})
        await self.redis.publish(f"market:{symbol}", json.dumps(payload))

    async def publish_tick(self, symbol: str, price: float, size: float, tick_time):
        """Publish a single trade tick for real-time frontend consumption."""
        payload = _sanitize({
            "symbol": symbol,
            "price": price,
            "size": size,
            "time": tick_time.isoformat() if hasattr(tick_time, 'isoformat') else str(tick_time),
        })
        await self.redis.publish(f"tick:{symbol}", json.dumps(payload))

    async def publish_account(self, data: dict):
        await self.redis.publish("account:update", json.dumps(_sanitize(data)))

    async def publish_order(self, data: dict):
        await self.redis.publish("order:update", json.dumps(_sanitize(data)))
