import pytest
import httpx

@pytest.mark.asyncio
async def test_order_endpoints(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 1. Orders
        resp = await client.get("/api/orders", params={"status": "all"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        # 2. Trades (Executions)
        resp = await client.get("/api/trades")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        # 3. Trades Export
        resp = await client.get("/api/trades/export")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")

        # 4. PnL Summary
        resp = await client.get("/api/pnl")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        if len(data) > 0:
            assert "realized_pnl" in data[0]
