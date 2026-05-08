import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL", "postgresql://ibkr:password@localhost:5432/ibkrdata")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
JWT_TOKEN = os.getenv("JWT_TOKEN", "dev-token")
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production")
API_PORT = int(os.getenv("API_PORT", "8002"))
