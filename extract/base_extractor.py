"""
BaseExtractor — abstract base class for all data source extractors.

Provides:
- Retry logic with exponential backoff (via tenacity)
- Rate limiting
- Structured logging (via loguru)
- Abstract interface: extract() → pd.DataFrame
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import settings


class BaseExtractor(ABC):
    """Abstract base class for all DeFi data extractors."""

    name: str = "base"
    api_base_url: str = ""
    target_table: str = ""
    rate_limit_rps: float = 2.0  # requests per second

    def __init__(self) -> None:
        self._session = self._build_session()
        self._last_request_time: float = 0.0
        logger.info(f"Initialized extractor: {self.name}")

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "DeFi-Revenue-Attribution-Pipeline/1.0",
                "Accept": "application/json",
            }
        )
        return session

    def _throttle(self) -> None:
        """Enforce rate limiting between requests."""
        min_interval = 1.0 / self.rate_limit_rps
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.monotonic()

    @retry(
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(settings.max_retries),
        reraise=True,
    )
    def _make_request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list:
        """Make an HTTP GET request with retry and rate limiting."""
        self._throttle()
        url = f"{self.api_base_url}{endpoint}" if not endpoint.startswith("http") else endpoint
        try:
            response = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=settings.request_timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            logger.error(f"[{self.name}] HTTP {response.status_code} for {url}: {e}")
            raise
        except Exception as e:
            logger.error(f"[{self.name}] Request failed for {url}: {e}")
            raise

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from source and return a clean DataFrame."""
        ...

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basic validation — override in subclasses for custom rules."""
        if df.empty:
            logger.warning(f"[{self.name}] Extraction returned empty DataFrame")
        else:
            logger.info(f"[{self.name}] Extracted {len(df):,} rows")
        return df

    def run(self, loader=None) -> int:
        """Extract, validate, optionally load. Returns number of rows processed."""
        started_at = datetime.utcnow()
        try:
            df = self.extract()
            df = self.validate(df)
            if loader is not None and not df.empty:
                rows_loaded = loader.upsert(df, self.target_table)
                logger.success(f"[{self.name}] Loaded {rows_loaded:,} rows → {self.target_table}")
                return rows_loaded
            return len(df)
        except Exception as e:
            logger.error(f"[{self.name}] Extractor failed: {e}")
            raise
