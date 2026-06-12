"""Tests for backfiller.scheduler — pure logic only, no IBKR connection."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date, datetime, timezone
from pathlib import Path

from backfiller.scheduler import (
    PullScheduler,
    split_date_windows,
    split_windows,
    subtract_trading_days,
)
from backfiller.config import AppConfig, ProductConfig, load_config
from backfiller.db_writer import MinuteBarWriter


@pytest.fixture
def mock_config():
    return AppConfig(
        products=[
            ProductConfig(
                symbol="USD.JPY", sec_type="CASH", exchange="IDEALPRO",
                currency="JPY"
            ),
        ],
        start="2024-01-01",
        end="2024-01-05",
        ib_host="127.0.0.1",
        ib_port=4002,
        ib_client_id=99,
    )


@pytest.fixture
def mock_writer():
    writer = MagicMock(spec=MinuteBarWriter)
    writer.get_range = AsyncMock(return_value=(None, None, 0))
    writer.upsert_bars = AsyncMock(return_value=0)
    writer.upsert_futures_bars = AsyncMock(return_value=0)
    writer.upsert_daily_bars = AsyncMock(return_value=0)
    writer.upsert_futures_daily_bars = AsyncMock(return_value=0)
    writer.has_daily_window_coverage = AsyncMock(return_value=False)
    writer.has_futures_daily_window_coverage = AsyncMock(return_value=False)
    writer.has_futures_window_coverage = AsyncMock(return_value=False)
    writer.detect_futures_session_gaps = AsyncMock(return_value=[])
    return writer


# ------------------------------------------------------------------
# split_windows  — kept from the original test file
# ------------------------------------------------------------------


def test_split_windows_basic():
    windows = split_windows(date(2024, 1, 1), date(2024, 1, 2))
    assert len(windows) == 1
    assert windows[0] == ("2024-01-01", "2024-01-02")


def test_split_windows_multiple():
    windows = split_windows(date(2024, 1, 1), date(2024, 1, 5))
    assert len(windows) == 3
    assert windows[0] == ("2024-01-01", "2024-01-02")


def test_split_windows_single_day():
    windows = split_windows(date(2024, 1, 1), date(2024, 1, 1))
    assert len(windows) == 1
    assert windows[0] == ("2024-01-01", "2024-01-01")


def test_split_date_windows_uses_requested_size():
    windows = split_date_windows(date(2024, 1, 1), date(2024, 1, 10), 4)

    assert windows == [
        ("2024-01-01", "2024-01-04"),
        ("2024-01-05", "2024-01-08"),
        ("2024-01-09", "2024-01-10"),
    ]


def test_subtract_trading_days_skips_weekends():
    assert subtract_trading_days(date(2024, 3, 18), 1) == date(2024, 3, 15)
    assert subtract_trading_days(date(2024, 3, 18), 5) == date(2024, 3, 11)


# ------------------------------------------------------------------
# PullScheduler — mock-based unit tests
# ------------------------------------------------------------------


def test_request_stop(mock_config, mock_writer, tmp_path):
    """request_stop flips the _should_stop flag."""
    scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
    assert scheduler._should_stop is False
    scheduler.request_stop()
    assert scheduler._should_stop is True


def test_disconnect_connected(mock_config, mock_writer, tmp_path):
    """disconnect on a connected client calls _ib.disconnect()."""
    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler.disconnect()
        ib_instance.disconnect.assert_called_once()


def test_disconnect_already_disconnected(mock_config, mock_writer, tmp_path):
    """disconnect on an already-disconnected client does nothing."""
    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = False
        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler.disconnect()
        ib_instance.disconnect.assert_not_called()


def test_disconnect_error_handled(mock_config, mock_writer, tmp_path):
    """disconnect swallows exceptions from _ib.disconnect()."""
    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.disconnect.side_effect = RuntimeError("test error")
        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler.disconnect()  # must not raise


@pytest.mark.asyncio
async def test_ensure_connected_success(mock_config, mock_writer, tmp_path):
    """Reconnect succeeds when IB Gateway becomes available after a brief
    disconnection.  The warm-up sleep is suppressed so the test is fast."""
    with (
        patch("backfiller.scheduler.IB") as MockIB,
        patch("backfiller.scheduler.HMDS_WARMUP_SECONDS", 0),
    ):
        ib_instance = MockIB.return_value
        ib_instance.isConnected.side_effect = [False, True]
        ib_instance.connectAsync = AsyncMock()

        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        result = await scheduler.ensure_connected()
        assert result is True
        ib_instance.connectAsync.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_connected_reconnect_exhausted(
    mock_config, mock_writer, tmp_path,
):
    """When the IB Gateway stays unreachable and a stop has been requested,
    ensure_connected gives up and returns False without blocking."""
    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = False
        ib_instance.connectAsync = AsyncMock(
            side_effect=ConnectionError("refused"),
        )

        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._should_stop = True  # break reconnect loop immediately
        result = await scheduler.ensure_connected()
        assert result is False


def test_check_new_products(mock_config, mock_writer, tmp_path):
    """_check_new_products re-reads the config YAML and returns every product
    whose symbol is not yet tracked in _known_symbols."""
    scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
    scheduler._known_symbols = set()
    new_products = scheduler._check_new_products()
    assert len(new_products) == len(load_config().products)


@pytest.mark.asyncio
async def test_compute_windows_no_db_data(mock_config, mock_writer, tmp_path):
    """DB中没有数据 → 返回全部窗口"""
    with patch('backfiller.scheduler.IB'):
        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        # mock_writer.get_range 默认返回 (None, None, 0)，在 fixture 中已设置
        windows = await scheduler._compute_windows(mock_config.products[0])
        # 2024-01-01 ~ 2024-01-05 = 3 个窗口 (2+2+1 天)
        assert len(windows) == 3


@pytest.mark.asyncio
async def test_compute_windows_partial_db_data(mock_config, mock_writer, tmp_path):
    """DB中已有部分数据 → 只返回未覆盖的窗口"""
    mock_writer.get_range = AsyncMock(return_value=(
        datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),  # min
        datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc),  # max
        1440,  # count
    ))
    with patch('backfiller.scheduler.IB'):
        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        windows = await scheduler._compute_windows(mock_config.products[0])
        # 2024-01-01 ~ 2024-01-02 已有数据，应只剩 2024-01-03 ~ 2024-01-05
        assert len(windows) == 2


@pytest.mark.asyncio
async def test_pull_product_basic_flow(mock_config, mock_writer, tmp_path):
    """验证 _pull_product 的正常流程：resolve → request → upsert → mark_completed"""
    mock_writer.get_range = AsyncMock(return_value=(None, None, 0))

    with (patch('backfiller.scheduler.IB') as MockIB,
          patch('backfiller.scheduler.resolve_contract_async') as mock_resolve,
          patch('backfiller.scheduler.ProgressStore') as MockStore):

        # Mock IB 实例
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.RequestTimeout = 60

        # Mock contract 解析
        mock_contract = MagicMock()
        mock_resolve.return_value = mock_contract

        # Mock reqHistoricalDataAsync 返回空 list
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[])

        # Mock store
        store_instance = MockStore.return_value
        store_instance.load.return_value = []  # 无 checkpoint，触发 _compute_windows

        # 避免 request_interval_seconds 带来的 25s 等待
        mock_config.request_interval_seconds = 0

        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        # 跳过 ensure_connected（isConnected 返回 True）
        scheduler._known_symbols = {p.symbol for p in mock_config.products}

        await scheduler._pull_product(mock_config.products[0])

        # 验证流程：
        # 1. resolve_contract_async 被调用
        mock_resolve.assert_called_once()
        # 2. reqHistoricalDataAsync 被调用（至少一次）
        assert ib_instance.reqHistoricalDataAsync.call_count >= 1
        # 3. upsert_bars 被调用
        mock_writer.upsert_bars.assert_called()
        # 4. store.save 被调用（保存窗口）
        store_instance.save.assert_called()


@pytest.mark.asyncio
async def test_pull_product_contract_failure(mock_config, mock_writer, tmp_path):
    """合约解析失败 → 跳过该产品"""
    mock_writer.get_range = AsyncMock(return_value=(None, None, 0))

    with (patch('backfiller.scheduler.IB') as MockIB,
          patch('backfiller.scheduler.resolve_contract_async',
                return_value=None),
          patch('backfiller.scheduler.ProgressStore') as MockStore):

        store_instance = MockStore.return_value
        store_instance.load.return_value = []

        scheduler = PullScheduler(mock_config, mock_writer, tmp_path)
        await scheduler._pull_product(mock_config.products[0])

        # 合约解析失败，不应调用 reqHistoricalDataAsync
        MockIB.return_value.reqHistoricalDataAsync.assert_not_called()
        # compute_windows 已执行并保存（save 发生在 resolve_contract 之前）
        store_instance.save.assert_called_once()
        # 不应标记任何窗口为 completed
        store_instance.mark_completed.assert_not_called()


@pytest.mark.asyncio
async def test_pull_fut_writes_contract_level_raw_bars(mock_writer, tmp_path):
    """期货回填必须保留 conId，写入 futures_minute_bars 专用路径。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-03-01",
        end="2024-03-02",
        request_interval_seconds=0,
    )

    mock_contract = MagicMock()
    mock_contract.conId = 123456
    mock_contract.lastTradeDateOrContractMonth = "20240315"
    mock_contract.includeExpired = False

    mock_bar = MagicMock()
    mock_bar.date = datetime(2024, 3, 1, 0, 0, tzinfo=timezone.utc)
    mock_bar.open = 1
    mock_bar.high = 2
    mock_bar.low = 0.5
    mock_bar.close = 1.5
    mock_bar.volume = 10
    mock_bar.barCount = 3

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[mock_bar])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(
            return_value=[mock_contract],
        )

        await scheduler._pull_product(product)

    mock_writer.upsert_futures_bars.assert_awaited()
    called_symbol, called_contract, called_bars = (
        mock_writer.upsert_futures_bars.await_args.args
    )
    assert called_symbol == "MES"
    assert called_contract is mock_contract
    assert called_bars == [mock_bar]
    mock_writer.upsert_bars.assert_not_awaited()
    assert mock_contract.includeExpired is True


@pytest.mark.asyncio
async def test_pull_fut_uses_overlap_before_previous_expiry(
    mock_writer, tmp_path,
):
    """后续期货合约应从上一合约到期日前 N 个交易日开始下载。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-01-01",
        end="2024-06-30",
        futures_overlap_trading_days=5,
        request_interval_seconds=0,
    )

    first_contract = MagicMock()
    first_contract.conId = 1
    first_contract.lastTradeDateOrContractMonth = "20240315"

    second_contract = MagicMock()
    second_contract.conId = 2
    second_contract.lastTradeDateOrContractMonth = "20240621"

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(
            return_value=[first_contract, second_contract],
        )

        await scheduler._pull_product(product)

    second_contract_windows = [
        call.kwargs["endDateTime"]
        for call in ib_instance.reqHistoricalDataAsync.await_args_list
        if call.args[0] is second_contract
    ]
    assert second_contract_windows[0] == "20240308-23:59:59"


@pytest.mark.asyncio
async def test_pull_fut_resumes_from_contract_checkpoint(
    mock_writer, tmp_path,
):
    """已有期货 checkpoint 时只请求剩余窗口，不重复下载完成窗口。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-03-01",
        end="2024-03-06",
        request_interval_seconds=0,
    )

    contract = MagicMock()
    contract.conId = 123456
    contract.lastTradeDateOrContractMonth = "20240315"

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        mock_bar = MagicMock()
        mock_bar.date = datetime(2024, 3, 5, 12, 0, tzinfo=timezone.utc)
        mock_bar.open = 1
        mock_bar.high = 2
        mock_bar.low = 0.5
        mock_bar.close = 1.5
        mock_bar.volume = 10
        mock_bar.barCount = 3
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[mock_bar])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(return_value=[contract])
        scheduler._store.save_task_windows(
            "MES",
            "FUT:123456:202403",
            [("2024-03-05", "2024-03-06")],
        )

        await scheduler._pull_product(product)

    requested_end_times = [
        call.kwargs["endDateTime"]
        for call in ib_instance.reqHistoricalDataAsync.await_args_list
    ]
    assert requested_end_times == ["20240306-23:59:59"]
    assert scheduler._store.load_task_windows("MES", "FUT:123456:202403") == []


@pytest.mark.asyncio
async def test_pull_fut_keeps_checkpoint_when_window_returns_no_bars(
    mock_writer, tmp_path,
):
    """IBKR 空返回不能被当作完成窗口。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-03-05",
        end="2024-03-06",
        request_interval_seconds=0,
    )

    contract = MagicMock()
    contract.conId = 123456
    contract.lastTradeDateOrContractMonth = "20240315"

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(return_value=[contract])

        await scheduler._pull_product(product)

    assert scheduler._store.load_task_windows("MES", "FUT:123456:202403") == [
        ("2024-03-05", "2024-03-05"),
        ("2024-03-06", "2024-03-06"),
    ]


@pytest.mark.asyncio
async def test_pull_fut_keeps_checkpoint_when_window_still_has_gaps(
    mock_writer, tmp_path,
):
    """写库后复查仍有缺口时，窗口必须保留以便后续补拉。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-03-05",
        end="2024-03-06",
        request_interval_seconds=0,
    )

    contract = MagicMock()
    contract.conId = 123456
    contract.lastTradeDateOrContractMonth = "20240315"

    mock_bar = MagicMock()
    mock_bar.date = datetime(2024, 3, 5, 12, 0, tzinfo=timezone.utc)
    mock_bar.open = 1
    mock_bar.high = 2
    mock_bar.low = 0.5
    mock_bar.close = 1.5
    mock_bar.volume = 10
    mock_bar.barCount = 3
    mock_writer.detect_futures_session_gaps = AsyncMock(
        return_value=[
            {
                "session_date": date(2024, 3, 5),
                "minute_count": 960,
                "day_session_count": 60,
                "minute_min_time": datetime(2024, 3, 5, tzinfo=timezone.utc),
                "minute_max_time": datetime(2024, 3, 5, 23, 59, tzinfo=timezone.utc),
            }
        ]
    )

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[mock_bar])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(return_value=[contract])

        await scheduler._pull_product(product)

    assert scheduler._store.load_task_windows("MES", "FUT:123456:202403") == [
        ("2024-03-05", "2024-03-05"),
        ("2024-03-06", "2024-03-06"),
    ]


@pytest.mark.asyncio
async def test_pull_fut_skips_windows_already_covered_in_db(
    mock_writer, tmp_path,
):
    """首次创建 checkpoint 时，已完整覆盖的窗口不再请求。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-03-01",
        end="2024-03-04",
        request_interval_seconds=0,
    )

    contract = MagicMock()
    contract.conId = 123456
    contract.lastTradeDateOrContractMonth = "20240315"

    async def _coverage(_symbol, _con_id, window_start, _window_end):
        return window_start in {"2024-03-01", "2024-03-02", "2024-03-03"}

    mock_writer.has_futures_window_coverage = AsyncMock(side_effect=_coverage)

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(return_value=[contract])

        await scheduler._pull_product(product)

    requested_end_times = [
        call.kwargs["endDateTime"]
        for call in ib_instance.reqHistoricalDataAsync.await_args_list
    ]
    assert requested_end_times == ["20240304-23:59:59"]


@pytest.mark.asyncio
async def test_pull_fut_daily_writes_contract_level_daily_bars(
    mock_writer, tmp_path,
):
    """期货日K必须保留 conId，写入 futures_daily_bars 专用路径。"""
    product = ProductConfig(
        symbol="MES", sec_type="FUT", exchange="CME", currency="USD",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-04-01",
        end="2024-06-21",
        request_interval_seconds=0,
    )

    contract = MagicMock()
    contract.conId = 123456
    contract.lastTradeDateOrContractMonth = "20240621"
    contract.includeExpired = False

    mock_bar = MagicMock()
    mock_bar.date = "20240301"
    mock_bar.open = 1
    mock_bar.high = 2
    mock_bar.low = 0.5
    mock_bar.close = 1.5
    mock_bar.volume = 10
    mock_bar.barCount = 3

    with patch("backfiller.scheduler.IB") as MockIB:
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[mock_bar])

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance
        scheduler._resolve_fut_contracts = AsyncMock(return_value=[contract])

        await scheduler.run_daily()

    mock_writer.upsert_futures_daily_bars.assert_awaited()
    called_symbol, called_contract, called_bars = (
        mock_writer.upsert_futures_daily_bars.await_args.args
    )
    assert called_symbol == "MES"
    assert called_contract is contract
    assert called_bars == [mock_bar]
    mock_writer.upsert_daily_bars.assert_not_awaited()
    assert contract.includeExpired is True

    request = ib_instance.reqHistoricalDataAsync.await_args
    assert request.kwargs["endDateTime"] == "20240621-23:59:59"
    assert request.kwargs["durationStr"] == "113 D"
    assert request.kwargs["barSizeSetting"] == "1 day"


@pytest.mark.asyncio
async def test_pull_daily_non_fut_uses_existing_daily_table(
    mock_writer, tmp_path,
):
    """非期货日K写入 daily_bars，并提前 31 天请求。"""
    product = ProductConfig(
        symbol="USD.JPY", sec_type="CASH", exchange="IDEALPRO",
        currency="JPY",
    )
    cfg = AppConfig(
        products=[product],
        start="2024-04-01",
        end="2024-04-10",
        request_interval_seconds=0,
    )

    mock_contract = MagicMock()
    mock_bar = MagicMock()
    mock_bar.date = "20240301"
    mock_bar.open = 1
    mock_bar.high = 2
    mock_bar.low = 0.5
    mock_bar.close = 1.5
    mock_bar.volume = 10

    with (
        patch("backfiller.scheduler.IB") as MockIB,
        patch("backfiller.scheduler.resolve_contract_async") as mock_resolve,
    ):
        ib_instance = MockIB.return_value
        ib_instance.isConnected.return_value = True
        ib_instance.reqHistoricalDataAsync = AsyncMock(return_value=[mock_bar])
        mock_resolve.return_value = mock_contract

        scheduler = PullScheduler(cfg, mock_writer, tmp_path)
        scheduler._ib = ib_instance

        await scheduler.run_daily()

    mock_writer.upsert_daily_bars.assert_awaited_once_with(
        "USD.JPY",
        [mock_bar],
    )
    mock_writer.upsert_futures_daily_bars.assert_not_awaited()
    request = ib_instance.reqHistoricalDataAsync.await_args
    assert request.kwargs["endDateTime"] == "20240410-23:59:59"
    assert request.kwargs["durationStr"] == "41 D"
    assert request.kwargs["barSizeSetting"] == "1 day"
