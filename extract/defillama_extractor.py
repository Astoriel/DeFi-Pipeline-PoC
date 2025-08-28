"""
DeFiLlamaExtractor — pulls TVL history and protocol fees from DeFiLlama.

No API key required. Covers Uniswap V3 and Aave V3.

API docs: https://defillama.com/docs/api
"""
from __future__ import annotations

from datetime import datetime, date

import pandas as pd
from loguru import logger

from .base_extractor import BaseExtractor


class DeFiLlamaExtractor(BaseExtractor):
    """Extracts TVL and revenue data from DeFiLlama (no API key needed)."""

    name = "defillama"
    api_base_url = "https://api.llama.fi"
    target_table = "defillama_tvl"
    rate_limit_rps = 2.0  # Be polite to the free API

    PROTOCOLS: list[dict] = [
        {"slug": "uniswap-v3", "name": "Uniswap V3", "chain": "ethereum"},
        {"slug": "aave-v3",    "name": "Aave V3",    "chain": "ethereum"},
    ]

    def _fetch_tvl_history(self, protocol_slug: str) -> list[dict]:
        """Fetch historical TVL for a protocol."""
        try:
            data = self._make_request(f"/protocol/{protocol_slug}")
            # DeFiLlama returns tvl as list of {date: unix_timestamp, totalLiquidityUSD: float}
            tvl_history = data.get("tvl", [])
            return tvl_history
        except Exception as e:
            logger.error(f"[defillama] Failed to fetch TVL for {protocol_slug}: {e}")
            return []

    def _fetch_protocol_fees(self, protocol_slug: str) -> list[dict]:
        """Fetch daily fees and revenue for a protocol."""
        try:
            data = self._make_request(
                f"https://api.llama.fi/summary/fees/{protocol_slug}",
                params={"dataType": "dailyFees"},
            )
            return data.get("totalDataChartBreakdown", []) or data.get("totalDataChart", [])
        except Exception as e:
            logger.warning(f"[defillama] Could not fetch fees for {protocol_slug}: {e}")
            return []

    def extract_tvl(self) -> pd.DataFrame:
        """Extract TVL history for all tracked protocols."""
        rows = []
        for protocol in self.PROTOCOLS:
            slug = protocol["slug"]
            logger.info(f"[defillama] Fetching TVL for {slug}...")
            history = self._fetch_tvl_history(slug)

            for entry in history:
                # date is Unix timestamp in DeFiLlama
                ts = entry.get("date", 0)
                tvl = entry.get("totalLiquidityUSD", 0)
                rows.append(
                    {
                        "protocol_slug": slug,
                        "protocol_name": protocol["name"],
                        "chain": protocol["chain"],
                        "date": date.fromtimestamp(int(ts)),
                        "tvl_usd": round(float(tvl), 2) if tvl else None,
                    }
                )

            logger.info(
                f"[defillama] {slug}: {len(history):,} daily TVL records"
            )

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates(subset=["protocol_slug", "chain", "date"])
        return df

    def extract_fees(self) -> pd.DataFrame:
        """Extract daily fees/revenue for all tracked protocols."""
        rows = []
        for protocol in self.PROTOCOLS:
            slug = protocol["slug"]
            logger.info(f"[defillama] Fetching fees for {slug}...")

            # Try the fees endpoint
            try:
                data = self._make_request(
                    f"https://api.llama.fi/summary/fees/{slug}"
                )
                chart = data.get("totalDataChart", [])
                for entry in chart:
                    if isinstance(entry, list) and len(entry) == 2:
                        ts, value = entry
                        rows.append(
                            {
                                "protocol_slug": slug,
                                "date": date.fromtimestamp(int(ts)),
                                "total_fees_usd": round(float(value), 2),
                                "revenue_usd": None,  # Revenue requires premium endpoint
                            }
                        )
            except Exception as e:
                logger.warning(f"[defillama] Fees not available for {slug}: {e}")
                continue

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.drop_duplicates(subset=["protocol_slug", "date"])
        return df

    def extract(self) -> pd.DataFrame:
        """Extract TVL (primary target table)."""
        return self.extract_tvl()
