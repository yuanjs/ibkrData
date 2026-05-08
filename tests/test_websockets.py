import pytest
import asyncio
import websockets
from websockets.exceptions import InvalidStatus, InvalidHandshake
import os

@pytest.mark.asyncio
async def test_ws_market_connection(api_base_url):
    ws_url = api_base_url.replace("http://", "ws://") + "/ws/market"
    token = os.getenv("JWT_TOKEN", "yuanjs666")
    
    # 1. Test Unauthorized
    # Catch any handshake/status failure
    with pytest.raises((InvalidStatus, InvalidHandshake)):
        async with websockets.connect(f"{ws_url}?token=wrong"):
            pass

    # 2. Test Authorized
    async with websockets.connect(f"{ws_url}?token={token}") as ws:
        await asyncio.sleep(0.2)
        assert ws.state.name == "OPEN"

@pytest.mark.asyncio
async def test_ws_account_connection(api_base_url):
    ws_url = api_base_url.replace("http://", "ws://") + "/ws/account"
    token = os.getenv("JWT_TOKEN", "yuanjs666")
    
    async with websockets.connect(f"{ws_url}?token={token}") as ws:
        await asyncio.sleep(0.2)
        assert ws.state.name == "OPEN"

@pytest.mark.asyncio
async def test_ws_orders_connection(api_base_url):
    ws_url = api_base_url.replace("http://", "ws://") + "/ws/orders"
    token = os.getenv("JWT_TOKEN", "yuanjs666")
    
    async with websockets.connect(f"{ws_url}?token={token}") as ws:
        await asyncio.sleep(0.2)
        assert ws.state.name == "OPEN"
