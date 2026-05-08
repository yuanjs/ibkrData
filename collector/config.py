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
