import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "api"))

from routers import futures  # noqa: E402


class FakePool:
    def __init__(self, fetch_rows=None, fetchrow_rows=None, fetch_results=None):
        self.fetch_rows = fetch_rows or []
        self.fetch_results = list(fetch_results or [])
        self.fetchrow_rows = list(fetchrow_rows or [])
        self.fetch_calls = []
        self.fetchrow_calls = []

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
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
    roll_sync_calls = []

    async def ensure_roll_calendar_override(pool_arg, symbol, *, as_of=None):
        roll_sync_calls.append((pool_arg, symbol, as_of))
        return True

    monkeypatch.setattr(
        futures,
        "ensure_futures_roll_calendar",
        ensure_roll_calendar_override,
    )

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
        daily_query, daily_args = next(
            (query, args)
            for query, args in pool.fetch_calls
            if "continuous_futures_daily_asof" in query
            and "LIMIT $5" not in query
        )
        assert "ORDER BY session_date DESC" not in daily_query
        assert daily_args == ("SPI", now.date().replace(day=1), now.date(), "back_adjusted")
        assert len(pool.fetchrow_calls) == 1

        resp = await client.get(
            "/api/futures/SPI/daily",
            params={
                "start": "2026-05-01",
                "as_of": now.isoformat(),
                "adjustment": "back_adjusted",
                "limit": "30",
            },
        )
        assert resp.status_code == 200
        limited_daily_query, limited_daily_args = next(
            (query, args)
            for query, args in pool.fetch_calls
            if "continuous_futures_daily_asof" in query
            and "LIMIT $5" in query
        )
        assert "ORDER BY session_date DESC" in limited_daily_query
        assert "LIMIT $5" in limited_daily_query
        assert limited_daily_args == (
            "SPI",
            now.date().replace(month=5, day=1),
            now.date(),
            "back_adjusted",
            30,
        )
        assert len(pool.fetchrow_calls) == 1

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
        assert [symbol for _pool, symbol, _as_of in roll_sync_calls] == [
            "SPI",
            "SPI",
            "SPI",
            "SPI",
            "SPI",
        ]


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
            "/api/futures/SPI/daily",
            params={
                "start": "2026-06-01",
                "as_of": "2026-06-12T00:00:00Z",
                "limit": "0",
            },
        )
        assert resp.status_code == 422

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
async def test_futures_daily_appends_live_partial_next_session(monkeypatch, futures_app):
    friday_after_roll = datetime(2026, 6, 12, 8, 0, tzinfo=timezone.utc)
    monday_as_of = datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)
    pool = FakePool(
        fetch_results=[
            [
                {
                    "time": datetime(2026, 6, 12, 12, tzinfo=timezone.utc),
                    "date_str": "20260612",
                    "session_date": datetime(2026, 6, 12, tzinfo=timezone.utc).date(),
                    "symbol": "SPI",
                    "open": 7000,
                    "high": 7010,
                    "low": 6990,
                    "close": 7005,
                    "volume": 100,
                    "bar_count": 50,
                }
            ],
            [],
            [
                {
                    "time": friday_after_roll,
                    "open": 7020,
                    "high": 7025,
                    "low": 7018,
                    "close": 7022,
                    "volume": 10,
                    "bar_count": 1,
                    "con_id": 123,
                    "contract_month": "202606",
                    "local_symbol": "APM6",
                    "trading_class": "AP",
                    "exchange": "SNFE",
                    "currency": "AUD",
                    "multiplier": "25",
                },
                {
                    "time": datetime(2026, 6, 12, 8, 1, tzinfo=timezone.utc),
                    "open": 7023,
                    "high": 7028,
                    "low": 7021,
                    "close": 7027,
                    "volume": 20,
                    "bar_count": 1,
                    "con_id": 123,
                    "contract_month": "202606",
                    "local_symbol": "APM6",
                    "trading_class": "AP",
                    "exchange": "SNFE",
                    "currency": "AUD",
                    "multiplier": "25",
                },
            ],
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
                "effective_from": None,
                "roll_event_id": None,
            }
        ],
    )

    async def get_pool_override():
        return pool

    async def ensure_roll_calendar_override(pool_arg, symbol, *, as_of=None):
        return True

    monkeypatch.setattr(futures, "get_pool", get_pool_override)
    monkeypatch.setattr(
        futures,
        "ensure_futures_roll_calendar",
        ensure_roll_calendar_override,
    )

    transport = httpx.ASGITransport(app=futures_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/futures/SPI/daily",
            params={
                "start": "2026-06-01",
                "as_of": monday_as_of.isoformat(),
                "adjustment": "back_adjusted",
                "include_live_partial": "true",
            },
        )

    assert resp.status_code == 200
    rows = resp.json()
    assert [r["date_str"] for r in rows] == ["20260612", "20260615"]
    partial = rows[-1]
    assert partial["is_live_partial"] is True
    assert partial["open"] == 7020
    assert partial["high"] == 7028
    assert partial["low"] == 7018
    assert partial["close"] == 7027
    assert partial["volume"] == 30


@pytest.mark.asyncio
async def test_futures_daily_appends_next_session_daily_bar_on_weekend(
    monkeypatch,
    futures_app,
):
    saturday_as_of = datetime(2026, 6, 13, 6, 0, tzinfo=timezone.utc)
    pool = FakePool(
        fetch_results=[
            [
                {
                    "time": datetime(2026, 6, 12, 12, tzinfo=timezone.utc),
                    "date_str": "20260612",
                    "session_date": datetime(2026, 6, 12, tzinfo=timezone.utc).date(),
                    "symbol": "SPI",
                    "open": 7000,
                    "high": 7010,
                    "low": 6990,
                    "close": 7005,
                    "volume": 100,
                    "bar_count": 50,
                }
            ],
            [
                {
                    "time": datetime(2026, 6, 15, 12, tzinfo=timezone.utc),
                    "date_str": "20260615",
                    "session_date": datetime(2026, 6, 15, tzinfo=timezone.utc).date(),
                    "symbol": "SPI",
                    "open": 7020,
                    "high": 7030,
                    "low": 7010,
                    "close": 7025,
                    "volume": 200,
                    "bar_count": 80,
                    "con_id": 123,
                    "contract_month": "202606",
                    "local_symbol": "APM6",
                    "trading_class": "AP",
                    "exchange": "SNFE",
                    "currency": "AUD",
                    "multiplier": "25",
                }
            ],
            [],
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
                "effective_from": None,
                "roll_event_id": None,
            }
        ],
    )

    async def get_pool_override():
        return pool

    async def ensure_roll_calendar_override(pool_arg, symbol, *, as_of=None):
        return True

    monkeypatch.setattr(futures, "get_pool", get_pool_override)
    monkeypatch.setattr(
        futures,
        "ensure_futures_roll_calendar",
        ensure_roll_calendar_override,
    )

    transport = httpx.ASGITransport(app=futures_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/futures/SPI/daily",
            params={
                "start": "2026-06-01",
                "as_of": saturday_as_of.isoformat(),
                "adjustment": "back_adjusted",
                "include_live_partial": "true",
            },
        )

    assert resp.status_code == 200
    rows = resp.json()
    assert [r["date_str"] for r in rows] == ["20260612", "20260615"]
    assert rows[-1]["adjustment_method"] == "live_daily_asof"
    assert rows[-1]["is_live_partial"] is True


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
