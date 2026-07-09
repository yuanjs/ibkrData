import os
import sys
import types
from datetime import date, datetime, timedelta, timezone
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

from data_writer import DataWriter
from futures_runtime import LiveFuturesRuntime, LiveFuturesState
from main import (
    IBKRClient,
    TickBuffer,
    should_publish_futures_minute_complete,
    should_publish_live_tick,
)


class FakeWriter:
    def __init__(self):
        self.raw_rows = []
        self.minute_rows = []
        self.futures_rows = []
        self.futures_minute_rows = []
        self.futures_contract_rows = []

    async def write_raw_ticks(self, rows):
        self.raw_rows.extend(rows)

    async def upsert_minute_bars_from_live(self, rows):
        self.minute_rows.extend(rows)

    async def write_futures_ticks(self, rows):
        self.futures_rows.extend(rows)

    async def upsert_futures_minute_bars_from_live(self, rows):
        self.futures_minute_rows.extend(rows)

    async def upsert_futures_contracts(self, rows):
        self.futures_contract_rows.extend(rows)


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
        self.fetchrow_result = None
        self.fetchrow_results = []

    async def executemany(self, sql, records):
        self.sql = sql
        self.records = records

    async def fetchrow(self, *args):
        if self.fetchrow_results:
            return self.fetchrow_results.pop(0)
        return self.fetchrow_result


class FakePool:
    def __init__(self):
        self.conn = FakeConn()

    def acquire(self):
        return FakeAcquire(self.conn)

    async def fetchrow(self, *args):
        return await self.conn.fetchrow(*args)


@pytest.mark.asyncio
async def test_resubscribe_all_cancels_old_market_data_before_replacing_tickers():
    class FakeIB:
        def __init__(self):
            self.cancelled = []

        def cancelMktData(self, contract):
            self.cancelled.append(contract)

    client = IBKRClient.__new__(IBKRClient)
    client.ib = FakeIB()
    client._tickers = {
        "AUD.USD": SimpleNamespace(contract="cash-contract"),
        ("SPI", 123): SimpleNamespace(contract="futures-contract"),
    }
    client._ticker_roles = {("SPI", 123): "active"}
    client._symbol_map = {123: "SPI"}
    client._last_trade_prices = {"AUD.USD": 0.65, ("SPI", 123): 7100.0}
    client._subscriptions = {
        "AUD.USD": {"symbol": "AUD.USD"},
    }
    resubscribed = []

    async def fake_subscribe(**params):
        resubscribed.append(params)

    client.subscribe = fake_subscribe

    await client._resubscribe_all()

    assert client.ib.cancelled == ["cash-contract", "futures-contract"]
    assert client._tickers == {}
    assert client._ticker_roles == {}
    assert client._symbol_map == {}
    assert client._last_trade_prices == {}
    assert resubscribed == [{"symbol": "AUD.USD"}]


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
    assert len(writer.minute_rows) == 1
    assert writer.minute_rows[0] == {
        "time": tick_time,
        "symbol": "AAPL",
        "open": 100.0,
        "high": 100.0,
        "low": 100.0,
        "close": 100.0,
        "volume": 2.0,
        "bar_count": 1,
    }
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
async def test_tick_buffer_aggregates_cash_ticks_into_minute_bars():
    writer = FakeWriter()
    buffer = TickBuffer(writer)
    t1 = datetime(2026, 6, 12, 10, 0, 10, tzinfo=timezone.utc)
    t2 = datetime(2026, 6, 12, 10, 0, 45, tzinfo=timezone.utc)
    t3 = datetime(2026, 6, 12, 10, 1, 2, tzinfo=timezone.utc)

    buffer.add_tick("USD.JPY", 158.10, 1, t1)
    buffer.add_tick("USD.JPY", 158.15, 2, t2)
    buffer.add_tick("USD.JPY", 158.12, 3, t3)

    await buffer.flush()

    assert len(writer.raw_rows) == 3
    assert writer.minute_rows == [
        {
            "time": datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc),
            "symbol": "USD.JPY",
            "open": 158.10,
            "high": 158.15,
            "low": 158.10,
            "close": 158.15,
            "volume": 3,
            "bar_count": 2,
        },
        {
            "time": datetime(2026, 6, 12, 10, 1, tzinfo=timezone.utc),
            "symbol": "USD.JPY",
            "open": 158.12,
            "high": 158.12,
            "low": 158.12,
            "close": 158.12,
            "volume": 3,
            "bar_count": 1,
        },
    ]


def test_should_publish_live_tick_filters_candidate_futures_only():
    assert should_publish_live_tick("AAPL") is True
    assert should_publish_live_tick({
        "symbol": "AAPL",
        "sec_type": "STK",
    }) is True
    assert should_publish_live_tick({
        "symbol": "MYM",
        "sec_type": "FUT",
        "role": "active",
    }) is True
    assert should_publish_live_tick({
        "symbol": "MYM",
        "sec_type": "FUT",
        "role": "candidate",
    }) is False


def test_should_publish_futures_minute_complete_filters_candidate_only():
    assert should_publish_futures_minute_complete({
        "symbol": "MYM",
        "role": "active",
    }) is True
    assert should_publish_futures_minute_complete({
        "symbol": "MYM",
        "role": "candidate",
    }) is False


def test_tick_buffer_emits_completed_minute_bars_on_minute_rollover():
    writer = FakeWriter()
    buffer = TickBuffer(writer)
    t1 = datetime(2026, 6, 12, 10, 0, 10, tzinfo=timezone.utc)
    t2 = datetime(2026, 6, 12, 10, 0, 45, tzinfo=timezone.utc)
    t3 = datetime(2026, 6, 12, 10, 1, 2, tzinfo=timezone.utc)

    buffer.add_futures_tick({
        "symbol": "SPI",
        "con_id": 12345,
        "role": "active",
        "local_symbol": "APM6",
        "contract_month": "202606",
        "trading_class": "AP",
        "exchange": "SNFE",
        "currency": "AUD",
        "multiplier": "25",
        "price": 7000.0,
        "size": 1.0,
        "time": t1,
    })
    buffer.add_futures_tick({
        "symbol": "SPI",
        "con_id": 12345,
        "role": "active",
        "local_symbol": "APM6",
        "contract_month": "202606",
        "trading_class": "AP",
        "exchange": "SNFE",
        "currency": "AUD",
        "multiplier": "25",
        "price": 7003.0,
        "size": 2.0,
        "time": t2,
    })
    buffer.add_futures_tick({
        "symbol": "SPI",
        "con_id": 12345,
        "role": "active",
        "local_symbol": "APM6",
        "contract_month": "202606",
        "trading_class": "AP",
        "exchange": "SNFE",
        "currency": "AUD",
        "multiplier": "25",
        "price": 7005.0,
        "size": 1.0,
        "time": t3,
    })

    completed = buffer.pop_completed_futures_minute_bars(t3, finalization_delay=timedelta(0))
    assert len(completed) == 1
    bar = completed[0]
    assert bar["symbol"] == "SPI"
    assert bar["role"] == "active"
    assert bar["time"] == datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc)
    assert bar["bar_start"] == datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc)
    assert bar["open"] == 7000.0
    assert bar["high"] == 7003.0
    assert bar["low"] == 7000.0
    assert bar["close"] == 7003.0
    assert bar["volume"] == 3.0
    assert bar["bar_count"] == 2
    assert bar["final"] is True
    assert bar["status"] == "final"
    assert bar["revision"] == 2


def test_tick_buffer_publishes_provisional_revisions_then_final_bar():
    writer = FakeWriter()
    buffer = TickBuffer(writer)
    t1 = datetime(2026, 6, 12, 10, 0, 10, tzinfo=timezone.utc)
    t2 = datetime(2026, 6, 12, 10, 0, 45, tzinfo=timezone.utc)
    t3 = datetime(2026, 6, 12, 10, 0, 50, tzinfo=timezone.utc)

    for tick_time, price in ((t1, 7000.0), (t2, 7003.0)):
        buffer.add_futures_tick({
            "symbol": "SPI",
            "con_id": 12345,
            "role": "active",
            "local_symbol": "APM6",
            "contract_month": "202606",
            "trading_class": "AP",
            "exchange": "SNFE",
            "currency": "AUD",
            "multiplier": "25",
            "price": price,
            "size": 1.0,
            "time": tick_time,
        })

    provisional_delay = timedelta(seconds=5)
    finalization_delay = timedelta(seconds=75)
    before_delay = datetime(2026, 6, 12, 10, 1, 4, tzinfo=timezone.utc)
    after_delay = datetime(2026, 6, 12, 10, 1, 5, tzinfo=timezone.utc)

    assert buffer.pop_publishable_futures_minute_bars(
        before_delay,
        provisional_delay=provisional_delay,
        finalization_delay=finalization_delay,
    ) == []

    provisional = buffer.pop_publishable_futures_minute_bars(
        after_delay,
        provisional_delay=provisional_delay,
        finalization_delay=finalization_delay,
    )
    assert len(provisional) == 1
    assert provisional[0]["time"] == datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc)
    assert provisional[0]["close"] == 7003.0
    assert provisional[0]["final"] is False
    assert provisional[0]["status"] == "provisional"
    assert provisional[0]["revision"] == 2

    assert buffer.pop_publishable_futures_minute_bars(
        after_delay,
        provisional_delay=provisional_delay,
        finalization_delay=finalization_delay,
    ) == []

    buffer.add_futures_tick({
        "symbol": "SPI",
        "con_id": 12345,
        "role": "active",
        "local_symbol": "APM6",
        "contract_month": "202606",
        "trading_class": "AP",
        "exchange": "SNFE",
        "currency": "AUD",
        "multiplier": "25",
        "price": 7006.0,
        "size": 1.0,
        "time": t3,
    })

    revised = buffer.pop_publishable_futures_minute_bars(
        datetime(2026, 6, 12, 10, 1, 6, tzinfo=timezone.utc),
        provisional_delay=provisional_delay,
        finalization_delay=finalization_delay,
    )
    assert len(revised) == 1
    assert revised[0]["close"] == 7006.0
    assert revised[0]["final"] is False
    assert revised[0]["status"] == "provisional"
    assert revised[0]["revision"] == 3

    final = buffer.pop_publishable_futures_minute_bars(
        datetime(2026, 6, 12, 10, 2, 15, tzinfo=timezone.utc),
        provisional_delay=provisional_delay,
        finalization_delay=finalization_delay,
    )
    assert len(final) == 1
    assert final[0]["close"] == 7006.0
    assert final[0]["final"] is True
    assert final[0]["status"] == "final"
    assert final[0]["revision"] == 3


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
async def test_upsert_minute_bars_from_live_merges_existing_cash_bar():
    pool = FakePool()
    writer = DataWriter(pool)

    await writer.upsert_minute_bars_from_live([
        {
            "time": datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc),
            "symbol": "USD.JPY",
            "open": 158.10,
            "high": 158.15,
            "low": 158.08,
            "close": 158.12,
            "volume": 3,
            "bar_count": 2,
        }
    ])

    assert "INSERT INTO minute_bars" in pool.conn.sql
    assert "GREATEST(COALESCE(minute_bars.high, EXCLUDED.high), EXCLUDED.high)" in pool.conn.sql
    assert "LEAST(COALESCE(minute_bars.low, EXCLUDED.low), EXCLUDED.low)" in pool.conn.sql
    assert "COALESCE(minute_bars.volume, 0) + COALESCE(EXCLUDED.volume, 0)" in pool.conn.sql
    assert "COALESCE(minute_bars.bar_count, 0) + COALESCE(EXCLUDED.bar_count, 0)" in pool.conn.sql
    assert pool.conn.records == [
        (
            datetime(2026, 6, 12, 10, 0, tzinfo=timezone.utc),
            "USD.JPY",
            158.10,
            158.15,
            158.08,
            158.12,
            3,
            2,
        )
    ]


@pytest.mark.asyncio
async def test_upsert_futures_minute_bars_from_live_merges_existing_bar():
    pool = FakePool()
    writer = DataWriter(pool)

    await writer.upsert_futures_minute_bars_from_live([
        {
            "time": datetime(2026, 6, 16, 5, 54, tzinfo=timezone.utc),
            "symbol": "SPI",
            "con_id": 749811513,
            "local_symbol": "APM6",
            "trading_class": "AP",
            "contract_month": "202606",
            "last_trade_date": date(2026, 6, 19),
            "exchange": "SNFE",
            "currency": "AUD",
            "multiplier": "25",
            "open": 8915,
            "high": 8916,
            "low": 8915,
            "close": 8916,
            "volume": 2,
            "bar_count": 2,
        }
    ])

    assert "GREATEST(COALESCE(futures_minute_bars.high, EXCLUDED.high), EXCLUDED.high)" in pool.conn.sql
    assert "LEAST(COALESCE(futures_minute_bars.low, EXCLUDED.low), EXCLUDED.low)" in pool.conn.sql
    assert "COALESCE(futures_minute_bars.volume, 0) + COALESCE(EXCLUDED.volume, 0)" in pool.conn.sql
    assert "COALESCE(futures_minute_bars.bar_count, 0) + COALESCE(EXCLUDED.bar_count, 0)" in pool.conn.sql
    assert len(pool.conn.records) == 1
    assert pool.conn.records[0][:16] == (
        datetime(2026, 6, 16, 5, 54, tzinfo=timezone.utc),
        "SPI",
        749811513,
        "APM6",
        "AP",
        "202606",
        date(2026, 6, 19),
        "SNFE",
        "AUD",
        "25",
        8915.0,
        8916.0,
        8915.0,
        8916.0,
        2,
        2,
    )


@pytest.mark.asyncio
async def test_upsert_futures_contracts_uses_live_metadata_shape():
    pool = FakePool()
    writer = DataWriter(pool)

    await writer.upsert_futures_contracts([
        {
            "symbol": "MES",
            "con_id": 98765,
            "local_symbol": "MESM6",
            "trading_class": "MES",
            "contract_month": "202606",
            "last_trade_date": date(2026, 6, 19),
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "5",
        }
    ])

    assert "INSERT INTO futures_contracts" in pool.conn.sql
    assert pool.conn.records == [
        (
            "MES",
            98765,
            "MESM6",
            "MES",
            "202606",
            date(2026, 6, 19),
            "CME",
            "USD",
            "5",
            "live_collector",
        )
    ]


@pytest.mark.asyncio
async def test_upsert_futures_daily_bars_from_live_uses_daily_shape():
    pool = FakePool()
    writer = DataWriter(pool)
    rows = DataWriter.futures_daily_bar_rows_from_identity(
        {
            "symbol": "MES",
            "con_id": 98765,
            "local_symbol": "MESM6",
            "trading_class": "MES",
            "contract_month": "202606",
            "last_trade_date": date(2026, 6, 19),
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "5",
        },
        [
            SimpleNamespace(
                date="20260612",
                open=5400,
                high=5410,
                low=5390,
                close=5405,
                volume=12345,
                barCount=321,
            )
        ],
    )

    await writer.upsert_futures_daily_bars_from_live(rows)

    assert "INSERT INTO futures_daily_bars" in pool.conn.sql
    assert len(pool.conn.records) == 1
    assert pool.conn.records[0][:11] == (
        "MES",
        98765,
        "20260612",
        datetime(2026, 6, 12, tzinfo=timezone.utc),
        "MESM6",
        "MES",
        "202606",
        date(2026, 6, 19),
        "CME",
        "USD",
        "5",
    )
    assert pool.conn.records[0][11:] == (5400.0, 5410.0, 5390.0, 5405.0, 12345, 321)


@pytest.mark.asyncio
async def test_runtime_subscribes_next_when_pending_roll_exists_before_tick_window():
    class FakeClient:
        async def subscribe_futures_contract(self, symbol, exchange, currency, identity, *, role):
            return {**identity, "symbol": symbol, "role": role}

        def is_futures_contract_subscribed(self, symbol, con_id, *, role=None):
            return False

    pool = FakePool()
    pool.conn.fetchrow_result = {"exists": 1}
    runtime = LiveFuturesRuntime(FakeClient(), FakeWriter(), pool, SimpleNamespace())
    state = LiveFuturesState("MES", "CME", "USD")
    state.active = {
        "symbol": "MES",
        "con_id": 1,
        "contract_month": "202606",
        "last_trade_date": datetime.now(timezone.utc).date() + timedelta(days=30),
    }
    state.next = {
        "symbol": "MES",
        "con_id": 2,
        "contract_month": "202609",
        "last_trade_date": datetime.now(timezone.utc).date() + timedelta(days=120),
    }

    assert await runtime._should_subscribe_next(state) is True


@pytest.mark.asyncio
async def test_runtime_resubscribes_when_client_lost_futures_ticker_after_reconnect():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def is_futures_contract_subscribed(self, symbol, con_id, *, role=None):
            return False

        async def subscribe_futures_contract(self, symbol, exchange, currency, identity, *, role):
            self.calls.append((symbol, exchange, currency, identity["con_id"], role))
            return {**identity, "symbol": symbol, "role": role}

    client = FakeClient()
    runtime = LiveFuturesRuntime(client, FakeWriter(), FakePool(), SimpleNamespace())
    state = LiveFuturesState("N225M", "OSE.JPN", "JPY")
    state.active = {
        "symbol": "N225M",
        "con_id": 123,
        "contract_month": "202609",
        "last_trade_date": datetime.now(timezone.utc).date() + timedelta(days=90),
    }
    state.subscribed[123] = "active"

    await runtime._ensure_subscribed(state, state.active, "active")

    assert client.calls == [("N225M", "OSE.JPN", "JPY", 123, "active")]
    assert state.subscribed[123] == "active"


@pytest.mark.asyncio
async def test_runtime_filters_spi_to_quarterly_contracts_for_next_selection():
    class FakeClient:
        async def list_futures_contracts(self, symbol, exchange, currency):
            return [
                SimpleNamespace(
                    conId=1,
                    localSymbol="APM6",
                    tradingClass="AP",
                    lastTradeDateOrContractMonth="20260618",
                    exchange="SNFE",
                    currency="AUD",
                    multiplier="25",
                ),
                SimpleNamespace(
                    conId=2,
                    localSymbol="APN6",
                    tradingClass="AP",
                    lastTradeDateOrContractMonth="20260716",
                    exchange="SNFE",
                    currency="AUD",
                    multiplier="25",
                ),
                SimpleNamespace(
                    conId=3,
                    localSymbol="APU6",
                    tradingClass="AP",
                    lastTradeDateOrContractMonth="20260917",
                    exchange="SNFE",
                    currency="AUD",
                    multiplier="25",
                ),
            ]

    pool = FakePool()
    pool.conn.fetchrow_result = {
        "symbol": "SPI",
        "con_id": 1,
        "contract_month": "202606",
        "local_symbol": "APM6",
        "last_trade_date": date(2026, 6, 18),
    }
    runtime = LiveFuturesRuntime(FakeClient(), FakeWriter(), pool, SimpleNamespace())

    await runtime.refresh_contracts([
        {"symbol": "SPI", "sec_type": "FUT", "exchange": "SNFE", "currency": "AUD"}
    ])

    state = runtime.states["SPI"]
    assert [c["contract_month"] for c in state.contracts] == ["202606", "202609"]
    assert state.next["local_symbol"] == "APU6"


@pytest.mark.asyncio
async def test_runtime_promotes_next_when_expiry_day_active_ticks_are_stale():
    class FakeClient:
        def __init__(self):
            self.roles = {}
            self.unsubscribed = []

        def is_futures_contract_subscribed(self, symbol, con_id, *, role=None):
            current = self.roles.get((symbol, con_id))
            return current == role if role else current is not None

        async def subscribe_futures_contract(self, symbol, exchange, currency, identity, *, role):
            self.roles[(symbol, identity["con_id"])] = role
            return {**identity, "symbol": symbol, "role": role}

        def unsubscribe_futures_contract(self, symbol, con_id):
            self.unsubscribed.append((symbol, con_id))
            self.roles.pop((symbol, con_id), None)

    class FakePub:
        def __init__(self):
            self.roll_states = []

        async def publish_futures_roll_state(self, symbol, data):
            self.roll_states.append((symbol, data))

    pool = FakePool()
    now = datetime.now(timezone.utc)
    pool.conn.fetchrow_results = [
        {
            "active_last_tick": now - timedelta(minutes=30),
            "next_last_tick": now - timedelta(seconds=10),
        }
    ]
    client = FakeClient()
    pub = FakePub()
    runtime = LiveFuturesRuntime(client, FakeWriter(), pool, pub)
    state = LiveFuturesState("SPI", "SNFE", "AUD")
    state.active = {
        "symbol": "SPI",
        "con_id": 1,
        "contract_month": "202606",
        "local_symbol": "APM6",
        "last_trade_date": now.date(),
    }
    state.next = {
        "symbol": "SPI",
        "con_id": 2,
        "contract_month": "202609",
        "local_symbol": "APU6",
        "last_trade_date": now.date() + timedelta(days=90),
    }
    state.subscribed[1] = "active"
    state.subscribed[2] = "candidate"
    client.roles[("SPI", 1)] = "active"
    client.roles[("SPI", 2)] = "candidate"

    await runtime._promote_next_if_active_stale(state)

    assert state.active["con_id"] == 2
    assert client.roles[("SPI", 2)] == "active"
    assert client.unsubscribed == [("SPI", 1)]
    assert pub.roll_states[0][0] == "SPI"


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
