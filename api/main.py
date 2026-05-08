from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from db import get_pool, close_pool
from websocket import manager, redis_forwarder, ws_market, ws_account, ws_orders, ws_tick
from routers import symbols, history, account, orders, settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()

    # Start Redis forwarders as background tasks with error logging
    forwarder_tasks = [
        asyncio.create_task(redis_forwarder("market:"), name="fwd_market"),
        asyncio.create_task(redis_forwarder("tick:"), name="fwd_tick"),
        asyncio.create_task(redis_forwarder("account:update"), name="fwd_account"),
        asyncio.create_task(redis_forwarder("order:update"), name="fwd_orders"),
    ]

    yield

    # Cancel forwarders on shutdown
    for task in forwarder_tasks:
        task.cancel()
    await asyncio.gather(*forwarder_tasks, return_exceptions=True)
    await close_pool()
    logger.info("API shutdown complete")


app = FastAPI(title="IBKR Data API", lifespan=lifespan)

# CORS: restrict to known frontend origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
    ],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

app.include_router(symbols.router)
app.include_router(history.router)
app.include_router(account.router)
app.include_router(orders.router)
app.include_router(settings.router)

app.add_api_websocket_route("/ws/market", ws_market)
app.add_api_websocket_route("/ws/account", ws_account)
app.add_api_websocket_route("/ws/orders", ws_orders)
app.add_api_websocket_route("/ws/tick", ws_tick)
