import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))

from routers import orders  # noqa: E402
from fastapi import HTTPException  # noqa: E402


class FakePool:
    def __init__(self, fetch_rows, fetchrow_rows):
        self.fetch_rows = list(fetch_rows)
        self.fetchrow_rows = list(fetchrow_rows)

    async def fetch(self, query, *args):
        if self.fetch_rows:
            return self.fetch_rows.pop(0)
        return []

    async def fetchrow(self, query, *args):
        if self.fetchrow_rows:
            return self.fetchrow_rows.pop(0)
        return None


class FakeRedis:
    def __init__(self):
        self.published = []

    async def publish(self, channel, payload):
        self.published.append((channel, json.loads(payload)))

    async def aclose(self):
        pass


@pytest.mark.asyncio
async def test_close_position_futures_command_includes_active_contract(monkeypatch):
    redis = FakeRedis()
    pool = FakePool(
        fetch_rows=[
            [
                {
                    "account_id": "U123",
                    "symbol": "MES",
                    "sec_type": "FUT",
                    "quantity": 1,
                    "con_id": 98765,
                    "local_symbol": "MESM6",
                    "contract_month": "202606",
                    "trading_class": "MES",
                    "exchange": "CME",
                    "currency": "USD",
                    "multiplier": "5",
                }
            ]
        ],
        fetchrow_rows=[
            {"sec_type": "FUT", "exchange": "CME", "currency": "USD"},
        ],
    )

    async def get_pool_override():
        return pool

    async def account_ids_override(gateway):
        return ["U123"]

    monkeypatch.setattr(orders, "get_pool", get_pool_override)
    monkeypatch.setattr(orders, "_gateway_account_ids", account_ids_override)
    monkeypatch.setattr(
        orders.aioredis,
        "from_url",
        lambda _url: redis,
    )

    resp = await orders.close_position(orders.ClosePositionRequest(symbol="MES"))

    assert resp["symbol"] == "MES"
    assert redis.published
    channel, payload = redis.published[0]
    assert channel == "order:command:live"
    assert payload["sec_type"] == "FUT"
    assert payload["con_id"] == 98765
    assert payload["local_symbol"] == "MESM6"
    assert payload["contract_month"] == "202606"
    assert payload["side"] == "SELL"


@pytest.mark.asyncio
async def test_close_position_requires_contract_when_multiple_futures_positions(monkeypatch):
    pool = FakePool(
        fetch_rows=[
            [
                {
                    "account_id": "U123",
                    "symbol": "MES",
                    "sec_type": "FUT",
                    "quantity": 1,
                    "con_id": 111,
                    "local_symbol": "MESH6",
                },
                {
                    "account_id": "U123",
                    "symbol": "MES",
                    "sec_type": "FUT",
                    "quantity": -1,
                    "con_id": 222,
                    "local_symbol": "MESM6",
                },
            ]
        ],
        fetchrow_rows=[],
    )

    async def get_pool_override():
        return pool

    async def account_ids_override(gateway):
        return ["U123"]

    monkeypatch.setattr(orders, "get_pool", get_pool_override)
    monkeypatch.setattr(orders, "_gateway_account_ids", account_ids_override)

    with pytest.raises(HTTPException) as exc:
        await orders.close_position(orders.ClosePositionRequest(symbol="MES"))

    assert exc.value.status_code == 400
    assert "指定 con_id" in exc.value.detail
