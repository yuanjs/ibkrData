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


@pytest.mark.asyncio
async def test_account_gateway_filter(api_base_url, auth_headers):
    """测试 gateway 过滤参数对 account 和 positions 端点的影响。"""
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # gateway=live — 不应报错
        resp = await client.get("/api/account", params={"gateway": "live"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        # gateway=paper — 可能无数据但不报错
        resp = await client.get("/api/account", params={"gateway": "paper"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        # positions 也支持 gateway 过滤
        resp = await client.get("/api/positions", params={"gateway": "live"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
