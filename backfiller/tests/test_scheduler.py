"""Tests for backfiller.scheduler — pure logic only, no IBKR connection."""

from datetime import date

from backfiller.scheduler import split_windows


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
