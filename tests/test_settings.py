import pytest
import httpx

@pytest.mark.asyncio
async def test_settings_endpoints(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 1. Get Settings
        resp = await client.get("/api/settings")
        assert resp.status_code == 200
        settings = resp.json()
        assert "ib_host" in settings

        # 2. Update Settings
        payload = {"ui_language": "en"} # Change from default 'zh'
        resp = await client.put("/api/settings", json=payload)
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        # Verify change
        resp = await client.get("/api/settings")
        assert resp.json()["ui_language"] == "en"

@pytest.mark.asyncio
async def test_alerts_endpoints(api_base_url, auth_headers, active_spi_contract):
    symbol = active_spi_contract.symbol
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 1. Create Alert
        payload = {
            "symbol": symbol,
            "alert_type": "price_above",
            "threshold": 10000.0
        }
        resp = await client.post("/api/alerts", json=payload)
        assert resp.status_code == 200
        alert_id = resp.json()["id"]

        # 2. List Alerts
        resp = await client.get("/api/alerts")
        assert resp.status_code == 200
        alerts = resp.json()
        assert any(a["id"] == alert_id for a in alerts)

        # 3. Delete Alert
        resp = await client.delete(f"/api/alerts/{alert_id}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
