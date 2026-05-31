"""Tests for backfiller.scheduler — pure logic only, no IBKR connection."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date, datetime, timezone
from pathlib import Path

from backfiller.scheduler import PullScheduler, split_windows
from backfiller.config import AppConfig, ProductConfig
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
    # The project's config.yaml defines 8 products
    assert len(new_products) == 8


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
