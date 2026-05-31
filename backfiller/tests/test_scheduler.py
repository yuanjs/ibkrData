"""Tests for backfiller.scheduler — pure logic only, no IBKR connection."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import date
from pathlib import Path

from backfiller.scheduler import PullScheduler, split_windows
from backfiller.config import AppConfig, ProductConfig
from backfiller.db_writer import MinuteBarWriter


@pytest.fixture
def mock_config():
    return AppConfig(
        products=[
            ProductConfig(
                symbol="SPI", sec_type="FUT", exchange="SNFE", currency="AUD"
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
