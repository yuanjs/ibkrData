"""Tests for holiday-aware daily bar date assignment."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo

import pytest
from daily_tracker import _parse_trading_days_str, _next_trading_day, _effective_date_str, DailyBarTracker


# ── helpers ──────────────────────────────────────────────────────────────────

def dt(year, month, day, hour, minute=0, tz="America/Chicago"):
    return datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(tz))


# MYM rolls at 16:00 Chicago time
MYM_TRADING_DAYS = {
    "20251222", "20251223", "20251224",  # Mon-Wed before Christmas
    "20251226",                           # Thu (Christmas=25 is CLOSED)
    "20251229", "20251230", "20251231",
    "20260102",                           # Fri (New Year's Day=1 is CLOSED)
}


# ── _parse_trading_days_str ───────────────────────────────────────────────────

class TestParseTradingDays:
    def test_normal_days_included(self):
        s = "20251222:0830-20251222:1500;20251223:0830-20251223:1500"
        result = _parse_trading_days_str(s)
        assert "20251222" in result
        assert "20251223" in result

    def test_closed_days_excluded(self):
        s = "20251224:0830-20251224:1500;20251225:CLOSED;20251226:0830-20251226:1500"
        result = _parse_trading_days_str(s)
        assert "20251225" not in result
        assert "20251224" in result
        assert "20251226" in result

    def test_empty_string(self):
        assert _parse_trading_days_str("") == set()

    def test_all_closed(self):
        s = "20251225:CLOSED;20260101:CLOSED"
        assert _parse_trading_days_str(s) == set()


# ── _next_trading_day ─────────────────────────────────────────────────────────

class TestNextTradingDay:
    def test_normal_weekday_no_holidays(self):
        # Monday → next is Tuesday
        d = dt(2025, 12, 22, 10)
        assert _next_trading_day(d, None) == "20251223"

    def test_skips_weekend(self):
        # Friday → next is Monday
        d = dt(2025, 12, 19, 10)
        assert _next_trading_day(d, None) == "20251222"

    def test_skips_holiday_with_trading_days(self):
        # Dec 24 → next should be Dec 26 (Dec 25 = Christmas, CLOSED)
        d = dt(2025, 12, 24, 10)
        assert _next_trading_day(d, MYM_TRADING_DAYS) == "20251226"

    def test_skips_new_years_day(self):
        # Dec 31 → next should be Jan 2 (Jan 1 = New Year, CLOSED)
        d = dt(2025, 12, 31, 10)
        assert _next_trading_day(d, MYM_TRADING_DAYS) == "20260102"

    def test_skips_weekend_and_holiday(self):
        # If holiday falls on Monday after a weekend, skip to Tuesday
        # Simulate: Fri Dec 19 → Sat 20, Sun 21, Mon 22 (mark as holiday), Tue 23
        trading_days = MYM_TRADING_DAYS - {"20251222"}
        d = dt(2025, 12, 19, 10)
        assert _next_trading_day(d, trading_days) == "20251223"


# ── _effective_date_str ───────────────────────────────────────────────────────

class TestEffectiveDateStr:
    def test_before_roll_same_day(self):
        # 15:00 Chicago < 16:00 roll → same day
        t = dt(2025, 12, 22, 15, 0)
        assert _effective_date_str(t, "MYM") == "20251222"

    def test_after_roll_next_trading_day_no_holidays(self):
        # 16:30 Chicago > 16:00 roll, next day is normal Tuesday
        t = dt(2025, 12, 22, 16, 30)
        assert _effective_date_str(t, "MYM") == "20251223"

    def test_after_roll_skips_holiday(self):
        # Dec 24 16:30 → roll → next trading day should be Dec 26 (not Dec 25)
        t = dt(2025, 12, 24, 16, 30)
        result = _effective_date_str(t, "MYM", MYM_TRADING_DAYS)
        assert result == "20251226"

    def test_after_roll_skips_holiday_no_trading_days_fallback(self):
        # Without trading_days, still skips weekend but not holidays
        # Dec 26 is Friday, so Dec 24 16:30 → Dec 25 (holiday, but we don't know)
        t = dt(2025, 12, 24, 16, 30)
        result = _effective_date_str(t, "MYM", None)
        assert result == "20251225"  # no holiday info → lands on Christmas

    def test_weekend_tick_shifts_to_next_trading_day(self):
        # Saturday tick → Monday
        t = dt(2025, 12, 20, 10, 0)  # Saturday
        assert _effective_date_str(t, "MYM") == "20251222"

    def test_weekend_tick_skips_holiday_monday(self):
        # Saturday tick, but Monday is a holiday → Tuesday
        trading_days = MYM_TRADING_DAYS - {"20251222"}
        t = dt(2025, 12, 20, 10, 0)  # Saturday
        assert _effective_date_str(t, "MYM", trading_days) == "20251223"

    def test_date_only_input_unchanged(self):
        # formatDate=1 returns date objects — no roll adjustment
        d = date(2025, 12, 25)
        assert _effective_date_str(d, "MYM") == "20251225"

    def test_unknown_symbol_no_adjustment(self):
        t = dt(2025, 12, 22, 17, 0)
        assert _effective_date_str(t, "UNKNOWN") == "20251222"

    def test_exact_roll_minute(self):
        # Exactly at roll_hour:roll_minute → belongs to next day
        t = dt(2025, 12, 22, 16, 0)  # exactly 16:00
        assert _effective_date_str(t, "MYM") == "20251223"

    def test_one_minute_before_roll(self):
        t = dt(2025, 12, 22, 15, 59)
        assert _effective_date_str(t, "MYM") == "20251222"


# ── DailyBarTracker ───────────────────────────────────────────────────────────

class TestDailyBarTracker:
    def test_tick_before_roll_assigned_to_today(self):
        tracker = DailyBarTracker()
        t = dt(2025, 12, 22, 15, 0)
        tracker.on_tick("MYM", 100.0, 1.0, t)
        bars = tracker.get_dirty_bars()
        assert bars[0]["date_str"] == "20251222"

    def test_tick_after_roll_assigned_to_next_trading_day(self):
        tracker = DailyBarTracker()
        tracker.trading_days["MYM"] = MYM_TRADING_DAYS
        t = dt(2025, 12, 24, 16, 30)
        tracker.on_tick("MYM", 100.0, 1.0, t)
        bars = tracker.get_dirty_bars()
        assert bars[0]["date_str"] == "20251226"

    def test_new_day_resets_bar(self):
        tracker = DailyBarTracker()
        tracker.on_tick("MYM", 100.0, 1.0, dt(2025, 12, 22, 10))
        tracker.get_dirty_bars()  # mark clean
        tracker.on_tick("MYM", 200.0, 2.0, dt(2025, 12, 23, 10))
        bars = tracker.get_dirty_bars()
        assert bars[0]["date_str"] == "20251223"
        assert bars[0]["open"] == 200.0

    def test_ohlcv_accumulation(self):
        tracker = DailyBarTracker()
        t = dt(2025, 12, 22, 10)
        tracker.on_tick("MYM", 100.0, 1.0, t)
        tracker.on_tick("MYM", 110.0, 2.0, t)
        tracker.on_tick("MYM", 95.0, 1.0, t)
        bars = tracker.get_dirty_bars()
        assert bars[0]["open"] == 100.0
        assert bars[0]["high"] == 110.0
        assert bars[0]["low"] == 95.0
        assert bars[0]["close"] == 95.0
        assert bars[0]["volume"] == 4.0

    def test_dirty_flag_cleared_after_get(self):
        tracker = DailyBarTracker()
        tracker.on_tick("MYM", 100.0, 1.0, dt(2025, 12, 22, 10))
        tracker.get_dirty_bars()
        assert tracker.get_dirty_bars() == []

    def test_dirty_flag_set_on_new_tick(self):
        tracker = DailyBarTracker()
        tracker.on_tick("MYM", 100.0, 1.0, dt(2025, 12, 22, 10))
        tracker.get_dirty_bars()
        tracker.on_tick("MYM", 101.0, 1.0, dt(2025, 12, 22, 11))
        assert len(tracker.get_dirty_bars()) == 1
