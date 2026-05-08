import asyncio
import json
import logging

import redis.asyncio as aioredis
from config import JWT_TOKEN, REDIS_URL
from fastapi import Query, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, channel: str, ws: WebSocket):
        await ws.accept()
        self._connections.setdefault(channel, []).append(ws)

    def disconnect(self, channel: str, ws: WebSocket):
        conns = self._connections.get(channel, [])
        try:
            conns.remove(ws)
        except ValueError:
            pass  # already removed

    async def broadcast(self, channel: str, message: str):
        for ws in list(self._connections.get(channel, [])):
            try:
                await ws.send_text(message)
            except Exception:
                self.disconnect(channel, ws)


manager = ConnectionManager()


async def redis_forwarder(channel: str):
    """Subscribe to Redis and forward messages to WebSocket clients, with auto-reconnect."""
    while True:
        try:
            r = aioredis.from_url(REDIS_URL)
            pubsub = r.pubsub()
            await pubsub.psubscribe(f"{channel}*")
            logger.info(f"Redis forwarder subscribed: {channel}*")
            async for msg in pubsub.listen():
                if msg["type"] in ("message", "pmessage"):
                    await manager.broadcast(channel, msg["data"].decode())
        except Exception as e:
            logger.error(
                f"Redis forwarder error ({channel}): {e}, reconnecting in 3s..."
            )
            await asyncio.sleep(3)


def _verify_token(token: str | None) -> bool:
    """Verify the bearer token from WebSocket query param."""
    logger.info(f"Verifying WS token: received='{token}', expected='{JWT_TOKEN}'")
    return token is not None and token == JWT_TOKEN


async def ws_market(ws: WebSocket, token: str = Query(default="")):
    logger.info(f"WS market connection attempt with token: {token}")
    if not _verify_token(token):
        logger.warning("WS market: Unauthorized")
        await ws.close(code=4001, reason="Unauthorized")
        return
    await manager.connect("market:", ws)
    # ... rest of function ...
    try:
        while True:
            data = await ws.receive_text()
            msg = json.loads(data)
            # client sends {"subscribe": ["AAPL"]} - handled by collector
    except WebSocketDisconnect:
        manager.disconnect("market:", ws)


async def ws_account(ws: WebSocket, token: str = Query(default="")):
    if not _verify_token(token):
        await ws.close(code=4001, reason="Unauthorized")
        return
    await manager.connect("account:update", ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect("account:update", ws)


async def ws_orders(ws: WebSocket, token: str = Query(default="")):
    if not _verify_token(token):
        await ws.close(code=4001, reason="Unauthorized")
        return
    await manager.connect("order:update", ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect("order:update", ws)


async def ws_tick(ws: WebSocket, token: str = Query(default="")):
    """WebSocket endpoint for real-time tick-by-tick trade data."""
    logger.info(f"WS tick connection attempt with token: {token}")
    if not _verify_token(token):
        logger.warning("WS tick: Unauthorized")
        await ws.close(code=4001, reason="Unauthorized")
        return
    await manager.connect("tick:", ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect("tick:", ws)
