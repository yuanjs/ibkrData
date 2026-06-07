"""Pure helper tests for backfiller.db_writer."""

from datetime import date, datetime, timezone

from backfiller.db_writer import _parse_daily_bar_date


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
