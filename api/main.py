from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from backfiller.roll_sync import ensure_futures_roll_calendar
from db import get_pool, close_pool
from websocket import manager, redis_forwarder, ws_market, ws_account, ws_orders, ws_tick, ws_gateway_map, ws_futures_roll_state, ws_futures_minute_complete
from routers import symbols, history, futures, account, orders, settings, gateway

logger = logging.getLogger(__name__)

ROLL_CALENDAR_WARMUP_INTERVAL_SECONDS = 1800


async def warm_futures_roll_calendars():
    """Generate the as-of roll calendars ahead of the first chart request.

    ensure_futures_roll_calendar() only runs once per symbol per session date,
    but that one run costs 0.3-0.8s and is otherwise paid by whoever loads the
    first daily chart of the day. Re-running on an interval keeps the next
    session date warm too.
    """
    while True:
        try:
            pool = await get_pool()
            rows = await pool.fetch(
                "SELECT symbol FROM subscriptions WHERE sec_type = 'FUT' AND active"
            )
            for row in rows:
                await ensure_futures_roll_calendar(pool, row["symbol"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Futures roll calendar warmup failed")
        await asyncio.sleep(ROLL_CALENDAR_WARMUP_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()

    # Start Redis forwarders as background tasks with error logging
    forwarder_tasks = [
        asyncio.create_task(redis_forwarder("market:"), name="fwd_market"),
        asyncio.create_task(redis_forwarder("tick:"), name="fwd_tick"),
        asyncio.create_task(redis_forwarder("account:update"), name="fwd_account"),
        asyncio.create_task(redis_forwarder("order:update"), name="fwd_orders"),
        asyncio.create_task(redis_forwarder("gateway:map:update"), name="fwd_gateway_map"),
        asyncio.create_task(redis_forwarder("futures:roll-state:"), name="fwd_futures_roll_state"),
        asyncio.create_task(redis_forwarder("futures:minute-complete:"), name="fwd_futures_minute_complete"),
        asyncio.create_task(warm_futures_roll_calendars(), name="warm_roll_calendars"),
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
app.include_router(futures.router)
app.include_router(account.router)
app.include_router(orders.router)
app.include_router(settings.router)
app.include_router(gateway.router)

app.add_api_websocket_route("/ws/market", ws_market)
app.add_api_websocket_route("/ws/account", ws_account)
app.add_api_websocket_route("/ws/orders", ws_orders)
app.add_api_websocket_route("/ws/tick", ws_tick)
app.add_api_websocket_route("/ws/gateway/map", ws_gateway_map)
app.add_api_websocket_route("/ws/futures/roll-state", ws_futures_roll_state)
app.add_api_websocket_route("/ws/futures/minute-complete", ws_futures_minute_complete)
