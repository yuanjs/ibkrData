import json
import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# 从项目根目录加载 .env（自动向上级目录搜索）
load_dotenv(find_dotenv())

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1"))
DB_URL = os.getenv("DB_URL", "postgresql://ibkr:password@localhost:5432/ibkrdata")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8001"))
ACCOUNT_REFRESH_INTERVAL = int(os.getenv("ACCOUNT_REFRESH_INTERVAL", "30"))
DEFAULT_SUBSCRIPTIONS = json.loads(os.getenv("SYMBOLS", "[]"))

# Futures roll event generation.  Symbols are intentionally not configured
# here; the collector derives them from active FUT subscriptions.
FUTURES_ROLL_CALENDAR_ENABLED = os.getenv(
    "FUTURES_ROLL_CALENDAR_ENABLED",
    "YES",
).upper() in ("1", "YES", "TRUE", "ON")
FUTURES_ROLL_CALENDAR_INTERVAL_SECONDS = int(
    os.getenv("FUTURES_ROLL_CALENDAR_INTERVAL_SECONDS", "1800")
)
FUTURES_ROLL_CALENDAR_AFTER_SESSION_MINUTES = int(
    os.getenv("FUTURES_ROLL_CALENDAR_AFTER_SESSION_MINUTES", "30")
)
FUTURES_ROLL_CALENDAR_CONFIRM_DAYS = int(
    os.getenv("FUTURES_ROLL_CALENDAR_CONFIRM_DAYS", "2")
)
FUTURES_ROLL_CALENDAR_INDEX_SAFETY_DAYS = int(
    os.getenv("FUTURES_ROLL_CALENDAR_INDEX_SAFETY_DAYS", "2")
)
FUTURES_ROLL_CALENDAR_COMMODITY_SAFETY_DAYS = int(
    os.getenv("FUTURES_ROLL_CALENDAR_COMMODITY_SAFETY_DAYS", "5")
)
FUTURES_LIVE_DAILY_LOOKAHEAD_TRADING_DAYS = int(
    os.getenv("FUTURES_LIVE_DAILY_LOOKAHEAD_TRADING_DAYS", "15")
)
FUTURES_LIVE_DAILY_REFRESH_DAYS = int(
    os.getenv("FUTURES_LIVE_DAILY_REFRESH_DAYS", "10")
)
FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS = int(
    os.getenv("FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS", "5")
)
FUTURES_LIVE_CONTRACT_REFRESH_SECONDS = int(
    os.getenv("FUTURES_LIVE_CONTRACT_REFRESH_SECONDS", "3600")
)
FUTURES_LIVE_DAILY_REFRESH_SECONDS = int(
    os.getenv("FUTURES_LIVE_DAILY_REFRESH_SECONDS", "1800")
)
FUTURES_MINUTE_COMPLETE_DELAY_SECONDS = int(
    os.getenv("FUTURES_MINUTE_COMPLETE_DELAY_SECONDS", "5")
)
FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS = int(
    os.getenv("FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS", "75")
)

# Startup minute-bar gap repair.  This is intentionally bounded so the live
# collector does not spend startup doing a full historical backfill.
STARTUP_MINUTE_BACKFILL_ENABLED = os.getenv(
    "STARTUP_MINUTE_BACKFILL_ENABLED",
    "YES",
).upper() in ("1", "YES", "TRUE", "ON")
STARTUP_MINUTE_BACKFILL_LOOKBACK_DAYS = int(
    os.getenv("STARTUP_MINUTE_BACKFILL_LOOKBACK_DAYS", "3")
)
STARTUP_MINUTE_BACKFILL_GAP_THRESHOLD_MINUTES = int(
    os.getenv("STARTUP_MINUTE_BACKFILL_GAP_THRESHOLD_MINUTES", "3")
)
STARTUP_MINUTE_BACKFILL_STABLE_DELAY_MINUTES = int(
    os.getenv("STARTUP_MINUTE_BACKFILL_STABLE_DELAY_MINUTES", "2")
)
STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL = int(
    os.getenv("STARTUP_MINUTE_BACKFILL_MAX_GAPS_PER_SYMBOL", "5")
)
STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS = int(
    os.getenv("STARTUP_MINUTE_BACKFILL_REQUEST_INTERVAL_SECONDS", "5")
)
STARTUP_MINUTE_BACKFILL_FUTURES_MIN_SESSION_MINUTES = int(
    os.getenv("STARTUP_MINUTE_BACKFILL_FUTURES_MIN_SESSION_MINUTES", "300")
)

# Bark Notification
BARK_SERVER = os.getenv("BARK_SERVER", "https://api.day.app")
BARK_KEY = os.getenv("BARK_KEY", "")
NOTIFY_THRESHOLD_SECONDS = int(os.getenv("NOTIFY_THRESHOLD_SECONDS", "120"))

# Product rollover times for daily bar date adjustment.
# After rollHour:rollMinute (local exchange time), the bar belongs to the next trading day.
PRODUCT_ROLL_CONFIG = {
    "SPI": {"timezone": "Australia/Sydney", "roll_hour": 17, "roll_minute": 10},
    "MYM": {"timezone": "America/Chicago", "roll_hour": 16, "roll_minute": 0},
    "N225M": {"timezone": "Asia/Tokyo", "roll_hour": 16, "roll_minute": 30},
    "USD.JPY": {"timezone": "America/New_York", "roll_hour": 17, "roll_minute": 0},
    "AUD.USD": {"timezone": "America/New_York", "roll_hour": 17, "roll_minute": 0},
    "10Y": {"timezone": "America/Chicago", "roll_hour": 16, "roll_minute": 0},
    "ZC": {"timezone": "America/Chicago", "roll_hour": 16, "roll_minute": 0},
    "HG": {"timezone": "America/New_York", "roll_hour": 17, "roll_minute": 0},
    "MNQ": {"timezone": "America/Chicago", "roll_hour": 16, "roll_minute": 0},
    "MES": {"timezone": "America/Chicago", "roll_hour": 16, "roll_minute": 0},
}

# Paper Gateway (optional — leave PAPER_IB_HOST empty to disable)
PAPER_IB_HOST = os.getenv("PAPER_IB_HOST", "")
PAPER_IB_PORT = int(os.getenv("PAPER_IB_PORT", "4002"))
PAPER_IB_CLIENT_ID = int(os.getenv("PAPER_IB_CLIENT_ID", "99"))

HAS_PAPER = bool(PAPER_IB_HOST)
