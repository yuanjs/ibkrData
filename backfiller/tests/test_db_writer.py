"""Pure helper tests for backfiller.db_writer."""

from datetime import date, datetime, timezone

from backfiller.db_writer import (
    _count_weekdays,
    _covers_expected_dates,
    _parse_daily_bar_date,
)


def test_parse_daily_bar_date_from_ibkr_string():
    parsed = _parse_daily_bar_date("20240315")

    assert parsed == (
        "20240315",
        datetime(2024, 3, 15, tzinfo=timezone.utc),
    )


def test_parse_daily_bar_date_from_date():
    parsed = _parse_daily_bar_date(date(2024, 3, 15))

    assert parsed == (
        "20240315",
        datetime(2024, 3, 15, tzinfo=timezone.utc),
    )


def test_parse_daily_bar_date_rejects_invalid_value():
    assert _parse_daily_bar_date("not-a-date") is None


def test_count_weekdays_excludes_weekends():
    assert _count_weekdays(date(2024, 3, 25), date(2024, 3, 31)) == 5


def test_covers_expected_dates_rejects_internal_gap():
    assert not _covers_expected_dates(
        start_date=date(2024, 1, 22),
        end_date=date(2024, 1, 26),
        min_date=date(2024, 1, 22),
        max_date=date(2024, 1, 26),
        observed_count=2,
        expected_open_days=5,
    )


def test_covers_expected_dates_accepts_calendar_coverage():
    assert _covers_expected_dates(
        start_date=date(2024, 1, 22),
        end_date=date(2024, 1, 26),
        min_date=date(2024, 1, 22),
        max_date=date(2024, 1, 26),
        observed_count=5,
        expected_open_days=5,
    )


def test_covers_expected_dates_falls_back_to_weekdays_when_calendar_missing():
    assert _covers_expected_dates(
        start_date=date(2024, 3, 25),
        end_date=date(2024, 3, 29),
        min_date=date(2024, 3, 25),
        max_date=date(2024, 3, 29),
        observed_count=5,
        expected_open_days=0,
    )
