"""Pydantic request/response models for API input validation."""
from pydantic import BaseModel, Field
from typing import Optional


class SymbolCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    sec_type: str = Field(default="STK", max_length=10)
    exchange: str = Field(default="SMART", max_length=20)
    currency: str = Field(default="USD", max_length=5)


class AlertCreate(BaseModel):
    symbol: Optional[str] = None
    alert_type: str = Field(..., pattern=r"^(price_above|price_below|daily_loss)$")
    threshold: float


class SettingsUpdate(BaseModel):
    """Accepts arbitrary key-value pairs for settings."""
    model_config = {"extra": "allow"}
