import pytest
import httpx
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_account_endpoints(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 1. Accounts Snapshot
        resp = await client.get("/api/account")
        assert resp.status_code == 200
        accounts = resp.json()
        assert isinstance(accounts, list)
        if accounts:
            assert "account_id" in accounts[0]
            assert "net_liquidation" in accounts[0]

        # 2. Account History
        end = datetime.now()
        start = end - timedelta(days=1)
        params = {"start": start.isoformat(), "end": end.isoformat()}
        resp = await client.get("/api/account/history", params=params)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        # 3. Positions
        resp = await client.get("/api/positions")
        assert resp.status_code == 200
        positions = resp.json()
        assert isinstance(positions, list)
        if positions:
            assert "symbol" in positions[0]
            assert "quantity" in positions[0]
