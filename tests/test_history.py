import pytest
import httpx
from datetime import datetime, timedelta

@pytest.mark.asyncio
async def test_history_endpoints(api_base_url, auth_headers, active_spi_contract):
    symbol = active_spi_contract.symbol
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # Test Status
        resp = await client.get("/api/status")
        if resp.status_code != 200:
            print(f"\nStatus Error: {resp.text}")
        assert resp.status_code == 200
        assert "last_tick" in resp.json()

        # Test History Query
        end = datetime.now()
        start = end - timedelta(hours=1)
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "interval": "1min"
        }
        resp = await client.get(f"/api/history/{symbol}", params=params)
        if resp.status_code != 200:
            print(f"\nHistory Error: {resp.text}")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        # Test Export
        resp = await client.get(f"/api/history/{symbol}/export", params=params)
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        assert "time,open,high,low,close,volume" in resp.text

@pytest.mark.asyncio
async def test_history_invalid_interval(api_base_url, auth_headers, active_spi_contract):
    symbol = active_spi_contract.symbol
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        params = {
            "start": datetime.now().isoformat(),
            "end": datetime.now().isoformat(),
            "interval": "invalid"
        }
        resp = await client.get(f"/api/history/{symbol}", params=params)
        assert resp.status_code == 400
