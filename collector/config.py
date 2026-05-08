import os
from dotenv import load_dotenv

load_dotenv() # 这会自动向上级目录寻找 .env 文件

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1"))
DB_URL = os.getenv("DB_URL", "postgresql://ibkr:password@localhost:5432/ibkrdata")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
