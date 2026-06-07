"""Tests for futures roll calendar generation helpers."""

from datetime import date

from backfiller.roll_calendar import (
    choose_roll_candidate,
    default_fallback_roll_date,
    subtract_trading_days,
)


def test_choose_roll_candidate_requires_consecutive_confirm_days():
    rows = [
        {
            "session_date": date(2026, 3, 1),
            "old_volume": 100,
            "new_volume": 90,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
        {
            "session_date": date(2026, 3, 2),
            "old_volume": 100,
            "new_volume": 120,
            "old_bar_count": 50,
            "new_bar_count": 45,
        },
        {
            "session_date": date(2026, 3, 3),
            "old_volume": 100,
            "new_volume": 80,
            "old_bar_count": 50,
            "new_bar_count": 55,
        },
    ]

    selected = choose_roll_candidate(rows, min_confirm_days=2)

    assert selected is rows[1]


def test_choose_roll_candidate_resets_streak():
    rows = [
        {
            "session_date": date(2026, 3, 1),
            "old_volume": 100,
            "new_volume": 120,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
        {
            "session_date": date(2026, 3, 2),
            "old_volume": 100,
            "new_volume": 80,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
        {
            "session_date": date(2026, 3, 3),
            "old_volume": 100,
            "new_volume": 130,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
        {
            "session_date": date(2026, 3, 4),
            "old_volume": 100,
            "new_volume": 140,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
    ]

    selected = choose_roll_candidate(rows, min_confirm_days=2)

    assert selected is rows[2]


def test_choose_roll_candidate_returns_none_without_confirmation():
    rows = [
        {
            "session_date": date(2026, 3, 1),
            "old_volume": 100,
            "new_volume": 120,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
        {
            "session_date": date(2026, 3, 2),
            "old_volume": 100,
            "new_volume": 80,
            "old_bar_count": 50,
            "new_bar_count": 40,
        },
    ]

    assert choose_roll_candidate(rows, min_confirm_days=2) is None


def test_fallback_roll_date_skips_weekends():
    assert subtract_trading_days(date(2026, 3, 16), 1) == date(2026, 3, 13)
    assert default_fallback_roll_date(date(2026, 3, 16), 5) == date(2026, 3, 9)
