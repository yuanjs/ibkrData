"""Tests for exchange calendar generation used by daily normalization."""

from datetime import date

from backfiller.exchange_calendar import (
    au_asx_holidays,
    generate_calendar,
    jp_ose_holidays,
    us_cme_holidays,
)


def test_au_asx_holidays_include_easter_and_christmas():
    holidays = au_asx_holidays(2025)

    assert holidays[date(2025, 4, 18)] == "good_friday"
    assert holidays[date(2025, 4, 21)] == "easter_monday"
    assert holidays[date(2025, 12, 25)] == "christmas"
    assert holidays[date(2025, 12, 26)] == "boxing_day"


def test_jp_ose_holidays_include_golden_week_and_year_end():
    holidays = jp_ose_holidays(2025)

    assert holidays[date(2025, 5, 5)] == "childrens_day"
    assert date(2025, 5, 6) in holidays
    assert holidays[date(2025, 12, 31)] == "new_year_market_closure"


def test_us_cme_holidays_include_good_friday_and_christmas():
    holidays = us_cme_holidays(2025)

    assert holidays[date(2025, 4, 18)] == "good_friday"
    assert holidays[date(2025, 12, 25)] == "christmas"


def test_generate_calendar_marks_weekends_and_holidays_closed():
    days = generate_calendar("AU_ASX", date(2025, 4, 18), date(2025, 4, 22))
    by_date = {d.trading_date: d for d in days}

    assert by_date[date(2025, 4, 18)].is_open is False
    assert by_date[date(2025, 4, 19)].reason == "weekend"
    assert by_date[date(2025, 4, 21)].is_open is False
    assert by_date[date(2025, 4, 22)].is_open is True
