"""Configuration loader for backfiller.

Priority: .env > config.yaml > defaults.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

load_dotenv()  # search from cwd


@dataclass
class ProductConfig:
    symbol: str
    sec_type: str
    exchange: str
    currency: str


@dataclass
class AppConfig:
    products: list[ProductConfig]
    start: str           # "YYYY-MM-DD"
    end: str             # "YYYY-MM-DD"
    futures_overlap_trading_days: int = 30
    request_interval_seconds: int = 25
    ib_host: str = "127.0.0.1"
    ib_port: int = 4002
    ib_client_id: int = 99
    db_url: str = "postgresql://ibkr:password@localhost:5432/ibkrdata"


def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    return int(val) if val is not None else default


def load_config(yaml_path: Optional[str] = None) -> AppConfig:
    if yaml_path is None:
        yaml_path = str(Path(__file__).parent / "config.yaml")

    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    products = [ProductConfig(**p) for p in raw.get("products", [])]

    return AppConfig(
        products=products,
        start=raw.get("start", "2024-01-01"),
        end=raw.get("end", "2026-05-31"),
        futures_overlap_trading_days=raw.get(
            "futures_overlap_trading_days", 30,
        ),
        request_interval_seconds=raw.get("request_interval_seconds", 25),
        ib_host=_env_str("IB_HOST", raw.get("ib_host", "127.0.0.1")),
        ib_port=_env_int("IB_PORT", raw.get("ib_port", 4002)),
        # NOTE: ib_client_id 不从 .env 读取，避免与 collector 的 IB_CLIENT_ID 冲突
        ib_client_id=raw.get("ib_client_id", 99),
        db_url=_env_str("DB_URL", raw.get("db_url",
                        "postgresql://ibkr:password@localhost:5432/ibkrdata")),
    )
