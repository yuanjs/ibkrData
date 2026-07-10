import os
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.modules.pop("config", None)

if "aiohttp" not in sys.modules:
    aiohttp = types.ModuleType("aiohttp")
    aiohttp.web = types.SimpleNamespace(
        Response=object,
        Application=object,
        AppRunner=object,
        TCPSite=object,
    )
    sys.modules["aiohttp"] = aiohttp
    sys.modules["aiohttp.web"] = aiohttp.web

import main


class FakePool:
    def __init__(self, rows=None):
        self.rows = rows or []

    async def fetch(self, *args):
        return self.rows


class FakeWriter:
    def __init__(self, min_ts, max_ts, bars):
        self.min_ts = min_ts
        self.max_ts = max_ts
        self.bars = bars
        self.upserted = []

    async def get_range(self, symbol, sec_type=None):
        return self.min_ts, self.max_ts, 10

    async def upsert_bars(self, symbol, bars):
        self.upserted.append((symbol, bars))
        return len(bars)


class FakeIB:
    def __init__(self, bars):
        self.bars = bars
        self.requests = []

    async def reqHistoricalDataAsync(self, contract, **kwargs):
        self.requests.append((contract, kwargs))
        return self.bars


@pytest.mark.asyncio
async def test_startup_backfill_repairs_cash_tail_gap(monkeypatch):
    end = datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)
    start = end - timedelta(days=1)
    max_ts = end - timedelta(minutes=10)
    bars = [SimpleNamespace(date=max_ts, open=1, high=1, low=1, close=1, volume=1, barCount=1)]
    writer = FakeWriter(start, max_ts, bars)
    ib = FakeIB(bars)
    product = main.ProductConfig("AUD.USD", "CASH", "IDEALPRO", "USD")

    async def fake_resolve_contract_async(*args):
        return SimpleNamespace(symbol="AUD", secType="CASH")

    monkeypatch.setattr(main, "resolve_contract_async", fake_resolve_contract_async)
    monkeypatch.setattr(main, "STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS", 0)

    repaired = await main._repair_cash_startup_minute_gaps(
        ib,
        FakePool(),
        writer,
        product,
        start,
        end,
    )

    assert repaired == 1
    assert writer.upserted == [("AUD.USD", bars)]
    assert ib.requests[0][1]["barSizeSetting"] == "1 min"
    assert ib.requests[0][1]["whatToShow"] == "MIDPOINT"
    assert ib.requests[0][1]["endDateTime"] == "20260102 00:00:00 UTC"


@pytest.mark.asyncio
async def test_startup_backfill_prioritizes_recent_cash_gaps(monkeypatch):
    end = datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)
    start = end - timedelta(days=1)
    older_start = start + timedelta(hours=1)
    older_end = older_start + timedelta(minutes=10)
    recent_start = end - timedelta(hours=1)
    recent_end = recent_start + timedelta(minutes=10)
    bars = [SimpleNamespace(date=recent_end, open=1, high=1, low=1, close=1, volume=1, barCount=1)]
    writer = FakeWriter(start, end, bars)
    ib = FakeIB(bars)
    product = main.ProductConfig("USD.JPY", "CASH", "IDEALPRO", "JPY")
    rows = [
        {"gap_start": older_start, "gap_end": older_end},
        {"gap_start": recent_start, "gap_end": recent_end},
    ]

    async def fake_resolve_contract_async(*args):
        return SimpleNamespace(symbol="USD", secType="CASH")

    monkeypatch.setattr(main, "resolve_contract_async", fake_resolve_contract_async)
    monkeypatch.setattr(main, "STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(main, "STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL", 1)

    repaired = await main._repair_cash_startup_minute_gaps(
        ib,
        FakePool(rows),
        writer,
        product,
        start,
        end,
    )

    assert repaired == 1
    assert ib.requests[0][1]["endDateTime"] == "20260101 23:10:00 UTC"
