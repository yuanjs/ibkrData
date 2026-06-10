"""Tests for futures roll calendar generation helpers."""

from datetime import date, datetime, timezone

from backfiller.roll_calendar import (
    choose_volume_safety_candidate_asof,
    choose_roll_candidate,
    choose_volume_safety_candidate,
    default_fallback_roll_date,
    next_weekday,
    session_start_time_utc,
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


def test_choose_volume_safety_uses_volume_confirmation_before_safety():
    rows = [
        {"session_date": date(2026, 3, 1), "old_volume": 100, "new_volume": 90},
        {"session_date": date(2026, 3, 2), "old_volume": 100, "new_volume": 120},
        {"session_date": date(2026, 3, 3), "old_volume": 100, "new_volume": 130},
        {"session_date": date(2026, 3, 4), "old_volume": 100, "new_volume": 80},
    ]

    selected, rule_source = choose_volume_safety_candidate(
        rows,
        min_confirm_days=2,
        safety_date=date(2026, 3, 4),
    )

    assert selected is rows[1]
    assert rule_source == "volume"


def test_choose_volume_safety_caps_late_confirmation_at_safety_date():
    rows = [
        {"session_date": date(2026, 3, 1), "old_volume": 100, "new_volume": 90},
        {"session_date": date(2026, 3, 2), "old_volume": 100, "new_volume": 80},
        {"session_date": date(2026, 3, 3), "old_volume": 100, "new_volume": 120},
        {"session_date": date(2026, 3, 4), "old_volume": 100, "new_volume": 130},
    ]

    selected, rule_source = choose_volume_safety_candidate(
        rows,
        min_confirm_days=2,
        safety_date=date(2026, 3, 2),
    )

    assert selected is rows[1]
    assert rule_source == "safety"


def test_choose_volume_safety_skips_when_safety_not_reached_without_volume():
    rows = [
        {"session_date": date(2026, 3, 1), "old_volume": 100, "new_volume": 90},
        {"session_date": date(2026, 3, 2), "old_volume": 100, "new_volume": 80},
    ]

    selected, rule_source = choose_volume_safety_candidate(
        rows,
        min_confirm_days=2,
        safety_date=date(2026, 3, 10),
    )

    assert selected is None
    assert rule_source == "safety"


def test_choose_volume_safety_asof_returns_decision_and_known_rows():
    rows = [
        {"session_date": date(2026, 3, 1), "old_volume": 100, "new_volume": 90},
        {"session_date": date(2026, 3, 2), "old_volume": 100, "new_volume": 120},
        {"session_date": date(2026, 3, 3), "old_volume": 100, "new_volume": 130},
        {"session_date": date(2026, 3, 4), "old_volume": 100, "new_volume": 80},
    ]

    selected = choose_volume_safety_candidate_asof(
        rows,
        min_confirm_days=2,
        safety_date=date(2026, 3, 5),
    )

    assert selected is not None
    assert selected.decision_row is rows[1]
    assert selected.known_row is rows[2]
    assert selected.rule_source == "volume"


def test_choose_volume_safety_asof_uses_safety_when_volume_known_after_safety():
    rows = [
        {"session_date": date(2026, 3, 1), "old_volume": 100, "new_volume": 90},
        {"session_date": date(2026, 3, 2), "old_volume": 100, "new_volume": 120},
        {"session_date": date(2026, 3, 3), "old_volume": 100, "new_volume": 130},
        {"session_date": date(2026, 3, 4), "old_volume": 100, "new_volume": 140},
    ]

    selected = choose_volume_safety_candidate_asof(
        rows,
        min_confirm_days=3,
        safety_date=date(2026, 3, 3),
    )

    assert selected is not None
    assert selected.decision_row is rows[2]
    assert selected.known_row is rows[2]
    assert selected.rule_source == "safety"


def test_next_weekday_skips_weekends():
    assert next_weekday(date(2026, 3, 6)) == date(2026, 3, 9)
    assert next_weekday(date(2026, 3, 9)) == date(2026, 3, 10)


def test_session_start_time_utc_uses_exchange_timezone():
    assert session_start_time_utc("SPI", date(2024, 6, 19)) == datetime(
        2024,
        6,
        18,
        7,
        10,
        tzinfo=timezone.utc,
    )
