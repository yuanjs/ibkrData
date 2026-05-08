import pytest
import httpx

@pytest.mark.asyncio
async def test_symbol_lifecycle(api_base_url, auth_headers, active_spi_contract):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 1. Add SPI Symbol
        payload = {
            "symbol": active_spi_contract.symbol,
            "sec_type": active_spi_contract.secType,
            "exchange": active_spi_contract.exchange,
            "currency": active_spi_contract.currency
        }
        resp = await client.post("/api/symbols", json=payload)
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        # 2. List Symbols and check if present
        resp = await client.get("/api/symbols")
        assert resp.status_code == 200
        symbols = resp.json()
        assert any(s["symbol"] == active_spi_contract.symbol for s in symbols)

        # 3. Test symbol removal (soft delete in this project)
        resp = await client.delete(f"/api/symbols/{active_spi_contract.symbol}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        # 4. Re-add for other tests
        await client.post("/api/symbols", json=payload)

@pytest.mark.asyncio
async def test_auth_failure(api_base_url):
    async with httpx.AsyncClient(base_url=api_base_url) as client:
        resp = await client.get("/api/symbols")
        assert resp.status_code == 403 # HTTPBearer returns 403 if no header
