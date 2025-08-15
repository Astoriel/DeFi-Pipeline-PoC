"""
Configuration — loads environment variables and validates settings.
"""
from __future__ import annotations

import os
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import field_validator
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    # API keys
    etherscan_api_key: str = ""
    dune_api_key: str = ""
    coingecko_api_key: str = ""

    # Database
    database_url: str = "postgresql://pipeline:pipeline_secret@localhost:5432/defi_pipeline"

    # Pipeline behaviour
    log_level: str = "INFO"
    extract_start_date: str = "2024-01-01"
    max_retries: int = 3
    request_timeout: int = 30

    # Contracts of interest
    uniswap_v3_router: str = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
    aave_v3_pool: str = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"

    # Tokens to track
    tracked_tokens: list[str] = [
        "ethereum", "wrapped-ether", "uniswap", "aave",
        "usd-coin", "tether", "dai",
    ]

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR"}
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
