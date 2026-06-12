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


@pytest.mark.asyncio
async def test_close_position_endpoint(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 调用平仓端点（如果数据库没有真实持仓，会返回 400）
        resp = await client.post("/api/positions/close", json={"symbol": "MES"})

        # 两种可能结果：
        # 1) 200 — 平仓指令已发送（有持仓）
        # 2) 400 — 无持仓（测试环境可能无数据）
        assert resp.status_code in (200, 400), f"Unexpected status: {resp.status_code}"

        if resp.status_code == 200:
            data = resp.json()
            assert "close_id" in data
            assert data["symbol"] == "MES"
            assert data["side"] in ("BUY", "SELL")
            assert data["quantity"] > 0
            assert "平仓" in data["message"]
        else:
            data = resp.json()
            detail = data.get("detail", "")
            assert any(
                text in detail
                for text in ("无持仓", "网关未就绪", "多个未平期货合约")
            )
