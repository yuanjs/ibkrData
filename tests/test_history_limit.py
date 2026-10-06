import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "api"))

from routers import history  # noqa: E402


class FakePool:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, query, *args):
        return self.rows


@pytest.fixture
def history_app():
    app = FastAPI()
    app.include_router(history.router)

    async def auth_override():
        return "test-token"

    app.dependency_overrides[history.require_auth] = auth_override
    return app


@pytest.mark.asyncio
async def test_history_limit_returns_latest_rows(monkeypatch, history_app):
    base = datetime(2026, 6, 12, tzinfo=timezone.utc)
    pool = FakePool(
        [
            {
                "time": base + timedelta(seconds=i),
                "open": 100 + i,
                "high": 100 + i,
                "low": 100 + i,
                "close": 100 + i,
                "volume": 1,
            }
            for i in range(4)
        ]
    )

    async def get_pool_override():
        return pool

    monkeypatch.setattr(history, "get_pool", get_pool_override)

    transport = httpx.ASGITransport(app=history_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/history/USD.JPY",
            params={
                "start": base.isoformat(),
                "end": (base + timedelta(minutes=1)).isoformat(),
                "interval": "1s",
                "limit": 2,
            },
        )

    assert resp.status_code == 200
    assert [row["close"] for row in resp.json()] == [102, 103]


@pytest.mark.asyncio
async def test_history_limit_must_be_positive(history_app):
    transport = httpx.ASGITransport(app=history_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/api/history/USD.JPY",
            params={
                "start": "2026-06-12T00:00:00Z",
                "end": "2026-06-12T01:00:00Z",
                "interval": "1m",
                "limit": 0,
            },
        )

    assert resp.status_code == 422
