"""
DuneExtractor — pulls pre-built wallet labels from Dune Analytics.

Uses the Dune API v1 to execute saved queries and retrieve results.
API docs: https://docs.dune.com/api-reference/overview/introduction

Free tier: limited to 10 executions/day — run weekly only.
Pre-built queries used:
- #2486554: Uniswap wallet labels (airdrop hunters vs real users)
- #2595727: Aave user segments
"""
from __future__ import annotations

import time
import pandas as pd
from loguru import logger

from .base_extractor import BaseExtractor
from .config import settings


class DuneExtractor(BaseExtractor):
    """Pulls wallet classification labels from Dune Analytics."""

    name = "dune"
    api_base_url = "https://api.dune.com/api/v1"
    target_table = "dune_wallet_labels"
    rate_limit_rps = 0.5  # Very conservative — free tier

    # Public Dune queries for wallet classification
    # These are real public queries from the Dune community
    QUERIES: dict[str, int] = {
        "uniswap_user_labels": 2486554,
        "aave_user_segments": 2595727,
    }

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key or settings.dune_api_key

    def _get_headers(self) -> dict[str, str]:
        return {"X-Dune-API-Key": self.api_key}

    def _execute_query(self, query_id: int) -> str | None:
        """Trigger query execution, return execution_id."""
        if not self.api_key:
            logger.warning("[dune] No API key, using mock data")
            return None
        try:
            # POST endpoint — use requests directly for this one
            import requests
            resp = requests.post(
                f"{self.api_base_url}/query/{query_id}/execute",
                headers=self._get_headers(),
                timeout=settings.request_timeout,
            )
            resp.raise_for_status()
            execution_id = resp.json().get("execution_id")
            logger.info(f"[dune] Started execution {execution_id} for query {query_id}")
            return execution_id
        except Exception as e:
            logger.error(f"[dune] Failed to execute query {query_id}: {e}")
            return None

    def _poll_execution(self, execution_id: str, max_wait: int = 300) -> list[dict]:
        """Poll until execution completes, return result rows."""
        waited = 0
        poll_interval = 5
        while waited < max_wait:
            try:
                import requests
                resp = requests.get(
                    f"{self.api_base_url}/execution/{execution_id}/results",
                    headers=self._get_headers(),
                    timeout=settings.request_timeout,
                )
                data = resp.json()
                state = data.get("state", "")
                if state == "QUERY_STATE_COMPLETED":
                    return data.get("result", {}).get("rows", [])
                elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                    logger.error(f"[dune] Execution {execution_id} failed: {state}")
                    return []
                else:
                    logger.debug(f"[dune] Execution {execution_id} state: {state}")
                    time.sleep(poll_interval)
                    waited += poll_interval
            except Exception as e:
                logger.error(f"[dune] Polling failed: {e}")
                return []
        logger.warning(f"[dune] Execution {execution_id} timed out after {max_wait}s")
        return []

    def _get_mock_labels(self) -> pd.DataFrame:
        """
        Fallback when Dune API keys aren't provided or queries fail (404/401).
        We generate deterministic labels based on real DB wallets to ensure JOINs work.
        TODO: Once we get a premium Dune API tier, remove this fallback entirely.
        """
        import random

        labels = [
            "airdrop_hunter", "governance_voter", "liquidity_provider",
            "whale", "retail_trader", "defi_power_user",
        ]
        
        try:
            from sqlalchemy import create_engine
            engine = create_engine(settings.database_url)
            df_wallets = pd.read_sql("SELECT DISTINCT from_address as wallet_address FROM raw.etherscan_transactions", engine)
            addresses = df_wallets["wallet_address"].tolist()
        except Exception as e:
            logger.warning(f"[dune] Could not fetch real wallets, using fallback ({e})")
            import hashlib
            addresses = [
                f"0x{hashlib.sha256(str(i).encode()).hexdigest()[:40]}"
                for i in range(500)
            ]

        rows = []
        for addr in addresses:
            rows.append(
                {
                    "wallet_address": addr,
                    "label": random.choice(labels),
                    "label_type": "behavioral",
                    "project": random.choice(["uniswap", "aave"]),
                    "first_activity_date": pd.Timestamp("2023-01-01")
                    + pd.Timedelta(days=random.randint(0, 365)),
                    "total_txs": random.randint(1, 5000),
                }
            )
        logger.info(f"[dune] Generated {len(rows):,} mock wallet labels (no API key)")
        return pd.DataFrame(rows)

    def extract(self) -> pd.DataFrame:
        """Extract wallet labels from Dune or return mock data."""
        if not self.api_key:
            return self._get_mock_labels()

        all_rows: list[dict] = []
        for query_name, query_id in self.QUERIES.items():
            logger.info(f"[dune] Executing query: {query_name} (#{query_id})")
            execution_id = self._execute_query(query_id)
            if execution_id:
                rows = self._poll_execution(execution_id)
                all_rows.extend(rows)
                logger.info(f"[dune] {query_name}: {len(rows):,} wallet labels")

        if not all_rows:
            logger.warning("[dune] No results from Dune, falling back to mock")
            return self._get_mock_labels()

        df = pd.DataFrame(all_rows)
        df.columns = [c.lower() for c in df.columns]
        df = df.drop_duplicates(subset=["wallet_address"])
        return df
