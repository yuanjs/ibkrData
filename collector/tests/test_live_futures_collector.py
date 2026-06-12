import os
import sys
import types
from datetime import date, datetime, timezone

import pytest

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

from data_writer import DataWriter
from main import TickBuffer


class FakeWriter:
    def __init__(self):
        self.raw_rows = []
        self.futures_rows = []
        self.futures_minute_rows = []

    async def write_raw_ticks(self, rows):
        self.raw_rows.extend(rows)

    async def write_futures_ticks(self, rows):
        self.futures_rows.extend(rows)

    async def upsert_futures_minute_bars_from_live(self, rows):
        self.futures_minute_rows.extend(rows)


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConn:
    def __init__(self):
        self.sql = None
        self.records = None

    async def executemany(self, sql, records):
        self.sql = sql
        self.records = records


class FakePool:
    def __init__(self):
        self.conn = FakeConn()

    def acquire(self):
        return FakeAcquire(self.conn)


@pytest.mark.asyncio
async def test_tick_buffer_flushes_raw_and_futures_ticks_separately():
    writer = FakeWriter()
    buffer = TickBuffer(writer)
    tick_time = datetime(2026, 6, 12, tzinfo=timezone.utc)

    buffer.add_tick("AAPL", 100.0, 2.0, tick_time)
    buffer.add_futures_tick({
        "symbol": "SPI",
        "sec_type": "FUT",
        "con_id": 12345,
        "local_symbol": "APM6",
        "contract_month": "202606",
        "trading_class": "AP",
        "exchange": "SNFE",
        "currency": "AUD",
        "multiplier": "25",
        "price": 7000.0,
        "size": 1.0,
        "time": tick_time,
    })

    await buffer.flush()

    assert writer.raw_rows == [
        (tick_time, "AAPL", 100.0, 2.0, 100.0, 100.0, 100.0, 100.0)
    ]
    assert len(writer.futures_rows) == 1
    assert writer.futures_rows[0]["symbol"] == "SPI"
    assert writer.futures_rows[0]["con_id"] == 12345
    assert writer.futures_rows[0]["last"] == 7000.0
    assert writer.futures_rows[0]["volume"] == 1.0
    assert len(writer.futures_minute_rows) == 1
    assert writer.futures_minute_rows[0]["symbol"] == "SPI"
    assert writer.futures_minute_rows[0]["con_id"] == 12345
    assert writer.futures_minute_rows[0]["open"] == 7000.0
    assert writer.futures_minute_rows[0]["close"] == 7000.0
    assert writer.futures_minute_rows[0]["bar_count"] == 1


@pytest.mark.asyncio
async def test_write_futures_ticks_uses_futures_ticks_shape():
    pool = FakePool()
    writer = DataWriter(pool)
    tick_time = datetime(2026, 6, 12, tzinfo=timezone.utc)

    await writer.write_futures_ticks([
        {
            "time": tick_time,
            "symbol": "MES",
            "con_id": 98765,
            "local_symbol": "MESM6",
            "trading_class": "MES",
            "contract_month": "202606",
            "last_trade_date": date(2026, 6, 19),
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "5",
            "bid": 5400.0,
            "ask": 5400.25,
            "price": 5400.25,
            "size": 3,
        }
    ])

    assert "INSERT INTO futures_ticks" in pool.conn.sql
    assert len(pool.conn.records) == 1
    record = pool.conn.records[0]
    assert len(record) == 19
    assert record[:10] == (
        tick_time,
        "MES",
        98765,
        "MESM6",
        "MES",
        "202606",
        date(2026, 6, 19),
        "CME",
        "USD",
        "5",
    )
    assert record[10:19] == (
        5400.0,
        5400.25,
        5400.25,
        3,
        5400.25,
        5400.25,
        5400.25,
        5400.25,
        "IBKR",
    )


@pytest.mark.asyncio
async def test_write_positions_persists_contract_identity():
    pool = FakePool()
    writer = DataWriter(pool)

    await writer.write_positions([
        {
            "account_id": "U123",
            "symbol": "MES",
            "con_id": 98765,
            "local_symbol": "MESM6",
            "contract_month": "202606",
            "trading_class": "MES",
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "5",
            "sec_type": "FUT",
            "quantity": 1,
            "avg_cost": 5400.25,
            "market_value": 27001.25,
            "unrealized_pnl": 12.5,
            "realized_pnl": 0,
        }
    ])

    assert "INSERT INTO positions" in pool.conn.sql
    record = pool.conn.records[0]
    assert len(record) == 16
    assert record[1:11] == (
        "U123",
        "MES",
        98765,
        "MESM6",
        "202606",
        "MES",
        "CME",
        "USD",
        "5",
        "FUT",
    )
