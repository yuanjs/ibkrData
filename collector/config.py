import json
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

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

# Product rollover times for daily bar date adjustment.
# After rollHour:rollMinute (local exchange time), the bar belongs to the next trading day.
PRODUCT_ROLL_CONFIG = {
    'SPI':    {'timezone': 'Australia/Sydney',  'roll_hour': 17, 'roll_minute': 10},
    'MYM':    {'timezone': 'America/Chicago',    'roll_hour': 16, 'roll_minute': 0},
    'N225M':  {'timezone': 'Asia/Tokyo',         'roll_hour': 16, 'roll_minute': 30},
    'USD.JPY': {'timezone': 'America/New_York',  'roll_hour': 17, 'roll_minute': 0},
}
