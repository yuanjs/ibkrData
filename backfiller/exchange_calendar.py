"""Seed exchange trading-day calendars used by normalized daily views."""

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, timedelta

import asyncpg


@dataclass(frozen=True)
class CalendarDay:
    exchange_code: str
    trading_date: date
    is_open: bool
    reason: str | None


def observed(d: date) -> date:
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    return d + timedelta(days=7 * (n - 1))


def last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def japanese_equinox(year: int, kind: str) -> date:
    if kind == "spring":
        day = int(20.8431 + 0.242194 * (year - 1980) - ((year - 1980) // 4))
        return date(year, 3, day)
    day = int(23.2488 + 0.242194 * (year - 1980) - ((year - 1980) // 4))
    return date(year, 9, day)


def add_japan_observed(holidays: dict[date, str], d: date, reason: str) -> None:
    holidays[d] = reason
    if d.weekday() == 6:
        obs = d + timedelta(days=1)
        while obs in holidays:
            obs += timedelta(days=1)
        holidays[obs] = f"{reason}_observed"


def au_asx_holidays(year: int) -> dict[date, str]:
    easter = easter_sunday(year)
    holidays = {
        observed(date(year, 1, 1)): "new_year",
        observed(date(year, 1, 26)): "australia_day",
        easter - timedelta(days=2): "good_friday",
        easter + timedelta(days=1): "easter_monday",
        observed(date(year, 4, 25)): "anzac_day",
        nth_weekday(year, 6, 0, 2): "kings_birthday",
    }
    christmas = observed(date(year, 12, 25))
    boxing = observed(date(year, 12, 26))
    if boxing == christmas:
        boxing += timedelta(days=1)
    while boxing.weekday() >= 5 or boxing == christmas:
        boxing += timedelta(days=1)
    holidays[christmas] = "christmas"
    holidays[boxing] = "boxing_day"
    return holidays


def us_cme_holidays(year: int) -> dict[date, str]:
    easter = easter_sunday(year)
    return {
        observed(date(year, 1, 1)): "new_year",
        nth_weekday(year, 1, 0, 3): "martin_luther_king_day",
        nth_weekday(year, 2, 0, 3): "presidents_day",
        easter - timedelta(days=2): "good_friday",
        last_weekday(year, 5, 0): "memorial_day",
        observed(date(year, 6, 19)): "juneteenth",
        observed(date(year, 7, 4)): "independence_day",
        nth_weekday(year, 9, 0, 1): "labor_day",
        nth_weekday(year, 11, 3, 4): "thanksgiving",
        observed(date(year, 12, 25)): "christmas",
    }


def jp_ose_holidays(year: int) -> dict[date, str]:
    holidays: dict[date, str] = {}
    for d in (date(year, 1, 1), date(year, 1, 2), date(year, 1, 3), date(year, 12, 31)):
        holidays[d] = "new_year_market_closure"
    fixed = [
        (date(year, 2, 11), "national_foundation_day"),
        (date(year, 2, 23), "emperor_birthday"),
        (date(year, 4, 29), "showa_day"),
        (date(year, 5, 3), "constitution_memorial_day"),
        (date(year, 5, 4), "greenery_day"),
        (date(year, 5, 5), "childrens_day"),
        (date(year, 8, 11), "mountain_day"),
        (date(year, 11, 3), "culture_day"),
        (date(year, 11, 23), "labor_thanksgiving_day"),
    ]
    for d, reason in fixed:
        holidays[d] = reason
    for d, reason in fixed:
        if d.weekday() == 6:
            obs = d + timedelta(days=1)
            while obs in holidays:
                obs += timedelta(days=1)
            holidays[obs] = f"{reason}_observed"
    for d, reason in [
        (nth_weekday(year, 1, 0, 2), "coming_of_age_day"),
        (japanese_equinox(year, "spring"), "vernal_equinox_day"),
        (nth_weekday(year, 7, 0, 3), "marine_day"),
        (nth_weekday(year, 9, 0, 3), "respect_for_the_aged_day"),
        (japanese_equinox(year, "autumn"), "autumnal_equinox_day"),
        (nth_weekday(year, 10, 0, 2), "sports_day"),
    ]:
        holidays[d] = reason
    return holidays


CALENDAR_BUILDERS = {
    "AU_ASX": au_asx_holidays,
    "US_CME": us_cme_holidays,
    "JP_OSE": jp_ose_holidays,
}

SYMBOL_CALENDARS = {
    "SPI": "AU_ASX",
    "MYM": "US_CME",
    "MNQ": "US_CME",
    "MES": "US_CME",
    "HG": "US_CME",
    "ZC": "US_CME",
    "10Y": "US_CME",
    "N225M": "JP_OSE",
}


def generate_calendar(exchange_code: str, start: date, end: date) -> list[CalendarDay]:
    builder = CALENDAR_BUILDERS[exchange_code]
    holidays: dict[date, str] = {}
    for year in range(start.year - 1, end.year + 2):
        holidays.update(builder(year))

    days: list[CalendarDay] = []
    d = start
    while d <= end:
        weekend = d.weekday() >= 5
        reason = "weekend" if weekend else holidays.get(d)
        days.append(CalendarDay(exchange_code, d, not weekend and d not in holidays, reason))
        d += timedelta(days=1)
    return days


async def seed_calendars(dsn: str, start: date, end: date) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO futures_daily_symbol_calendars (symbol, exchange_code)
                VALUES ($1, $2)
                ON CONFLICT (symbol) DO UPDATE SET
                    exchange_code = EXCLUDED.exchange_code
                """,
                list(SYMBOL_CALENDARS.items()),
            )
            records = []
            for exchange_code in CALENDAR_BUILDERS:
                records.extend(generate_calendar(exchange_code, start, end))
            await conn.executemany(
                """
                INSERT INTO exchange_trading_days (
                    exchange_code, trading_date, is_open, reason
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (exchange_code, trading_date) DO UPDATE SET
                    is_open = EXCLUDED.is_open,
                    reason = EXCLUDED.reason
                """,
                [
                    (r.exchange_code, r.trading_date, r.is_open, r.reason)
                    for r in records
                ],
            )
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed exchange trading calendars")
    parser.add_argument("--db-url", default="postgresql://ibkr:password@localhost:5432/ibkrdata")
    parser.add_argument("--start", default="2023-01-01")
    parser.add_argument("--end", default="2027-12-31")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(
        seed_calendars(
            args.db_url,
            date.fromisoformat(args.start),
            date.fromisoformat(args.end),
        )
    )


if __name__ == "__main__":
    main()
