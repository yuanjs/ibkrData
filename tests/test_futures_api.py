import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))

from routers import futures  # noqa: E402


class FakePool:
    def __init__(self, fetch_rows=None, fetchrow_rows=None):
        self.fetch_rows = fetch_rows or []
        self.fetchrow_rows = list(fetchrow_rows or [])
        self.fetch_calls = []
        self.fetchrow_calls = []

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return self.fetch_rows

    async def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        if self.fetchrow_rows:
            return self.fetchrow_rows.pop(0)
        return None


async def fake_get_pool(pool):
    return pool


@pytest.fixture
def futures_app():
    app = FastAPI()
    app.include_router(futures.router)

    async def auth_override():
        return "test-token"

    app.dependency_overrides[futures.require_auth] = auth_override
    return app


@pytest.mark.asyncio
async def test_futures_routes_are_callable(monkeypatch, futures_app):
    now = datetime(2026, 6, 12, tzinfo=timezone.utc)
    pool = FakePool(
        fetch_rows=[
            {
                "time": now,
                "symbol": "SPI",
                "open": 1,
                "high": 2,
                "low": 1,
                "close": 2,
                "volume": 10,
            }
        ],
        fetchrow_rows=[
            {
                "symbol": "SPI",
                "con_id": 123,
                "contract_month": "202606",
                "local_symbol": "APM6",
                "trading_class": "AP",
                "exchange": "SNFE",
                "currency": "AUD",
                "multiplier": "25",
                "last_trade_date": None,
                "effective_from": now,
                "roll_event_id": 1,
            },
            {
                "symbol": "SPI",
                "con_id": 123,
                "contract_month": "202606",
                "local_symbol": "APM6",
                "trading_class": "AP",
                "exchange": "SNFE",
                "currency": "AUD",
                "multiplier": "25",
                "last_trade_date": None,
                "effective_from": now,
                "roll_event_id": 1,
            },
            {"id": 1, "symbol": "SPI", "effective_roll_time": now},
            None,
        ],
    )

    async def get_pool_override():
        return await fake_get_pool(pool)

    monkeypatch.setattr(futures, "get_pool", get_pool_override)

    transport = httpx.ASGITransport(app=futures_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        params = {"as_of": now.isoformat()}
        resp = await client.get("/api/futures/SPI/active-contract", params=params)
        assert resp.status_code == 200
        assert resp.json()["con_id"] == 123

        resp = await client.get(
            "/api/futures/SPI/daily",
            params={"start": "2026-06-01", "as_of": now.isoformat()},
        )
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        resp = await client.get(
            "/api/futures/SPI/minute",
            params={
                "start": "2026-06-12T00:00:00Z",
                "end": "2026-06-12T01:00:00Z",
                "as_of": now.isoformat(),
                "mode": "active_raw",
            },
        )
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
        assert any(
            "continuous_futures_minute_asof_raw" in query
            for query, _args in pool.fetch_calls
        )

        resp = await client.get(
            "/api/futures/SPI/roll-events",
            params={"start": "2026-06-01T00:00:00Z", "end": "2026-07-01T00:00:00Z"},
        )
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        resp = await client.get("/api/futures/SPI/roll-state")
        assert resp.status_code == 200
        assert resp.json()["active"]["con_id"] == 123


@pytest.mark.asyncio
async def test_futures_parameter_errors(futures_app):
    transport = httpx.ASGITransport(app=futures_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/futures/SPI/daily",
            params={"start": "not-a-date", "as_of": "2026-06-12T00:00:00Z"},
        )
        assert resp.status_code == 400

        resp = await client.get(
            "/api/futures/SPI/daily",
            params={
                "start": "2026-06-01",
                "as_of": "2026-06-12T00:00:00Z",
                "adjustment": "bad",
            },
        )
        assert resp.status_code == 400

        resp = await client.get(
            "/api/futures/SPI/minute",
            params={
                "start": "2026-06-12T01:00:00Z",
                "end": "2026-06-12T00:00:00Z",
                "mode": "active_raw",
            },
        )
        assert resp.status_code == 400

        resp = await client.get(
            "/api/futures/SPI/minute",
            params={
                "start": "2026-06-12T00:00:00Z",
                "end": "2026-06-12T01:00:00Z",
                "mode": "bad",
            },
        )
        assert resp.status_code == 400

        resp = await client.get("/api/futures/SPI/daily")
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_active_contract_not_found(monkeypatch, futures_app):
    pool = FakePool(fetchrow_rows=[None])

    async def get_pool_override():
        return await fake_get_pool(pool)

    monkeypatch.setattr(futures, "get_pool", get_pool_override)

    transport = httpx.ASGITransport(app=futures_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/futures/UNKNOWN/active-contract",
            params={"as_of": "2026-06-12T00:00:00Z"},
        )
        assert resp.status_code == 404
