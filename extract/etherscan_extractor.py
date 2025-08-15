"""
EtherscanExtractor — pulls wallet transactions from DeFi protocol contracts.

Targets:
- Uniswap V3 Router (0xE592...)
- Aave V3 Lending Pool (0x8787...)

API docs: https://docs.etherscan.io/api-endpoints/accounts
Rate limit: 5 req/sec on free tier
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from loguru import logger

from .base_extractor import BaseExtractor
from .config import settings


class EtherscanExtractor(BaseExtractor):
    """Extracts transaction history from Ethereum DeFi protocol contracts."""

    name = "etherscan"
    api_base_url = "https://api.etherscan.io/v2/api"
    target_table = "etherscan_transactions"
    rate_limit_rps = 4.0  # Stay safely under 5 req/sec limit

    # Known DeFi protocols and their contract addresses
    PROTOCOLS: dict[str, dict[str, str]] = {
        "uniswap_v3": {
            "address": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
            "name": "Uniswap V3",
        },
        "aave_v3": {
            "address": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
            "name": "Aave V3",
        },
    }

    # Method signatures for known DeFi operations
    METHOD_NAMES: dict[str, str] = {
        "0x414bf389": "exactInputSingle",       # Uniswap swap
        "0xc04b8d59": "exactInput",              # Uniswap multi-hop swap
        "0x617ba037": "supply",                  # Aave supply
        "0xa415bcad": "borrow",                  # Aave borrow
        "0x573ade81": "repay",                   # Aave repay
        "0x69328dec": "withdraw",                # Aave withdraw
    }

    def __init__(self, api_key: str | None = None) -> None:
        super().__init__()
        self.api_key = api_key or settings.etherscan_api_key
        if not self.api_key:
            logger.warning(
                "[etherscan] No API key provided. "
                "Using demo mode with very low rate limits."
            )

    def extract_transactions(
        self,
        contract_address: str,
        protocol_name: str,
        start_block: int = 0,
        end_block: int = 99999999,
        page_size: int = 10000,
    ) -> pd.DataFrame:
        """
        Paginated extraction of normal transactions for a contract address.
        
        Returns a DataFrame with standardised column names.
        """
        all_transactions: list[dict] = []
        current_block = start_block
        page = 1

        while True:
            logger.debug(
                f"[etherscan] Fetching page {page} for {protocol_name} "
                f"from block {current_block}"
            )
            params = {
                "chainid": "1",
                "module": "account",
                "action": "txlist",
                "address": contract_address,
                "startblock": current_block,
                "endblock": end_block,
                "page": page,
                "offset": page_size,
                "sort": "asc",
                "apikey": self.api_key or "YourApiKeyToken",
            }

            try:
                data = self._make_request("", params=params)
            except Exception as e:
                logger.error(f"[etherscan] Failed to fetch page {page}: {e}")
                break

            # Handle API errors
            if data.get("status") != "1":
                message = data.get("message", "")
                if "No transactions found" in message or not data.get("result"):
                    logger.info(f"[etherscan] No more transactions for {protocol_name}")
                    break
                logger.warning(f"[etherscan] API message: {message}")
                break

            results = data.get("result", [])
            if not results:
                break

            all_transactions.extend(results)
            logger.info(
                f"[etherscan] Fetched {len(results):,} txs for {protocol_name} "
                f"(total: {len(all_transactions):,})"
            )

            # If we got fewer results than page_size, we're done
            if len(results) < page_size:
                break

            # Next page starts at the last block + 1
            current_block = int(results[-1]["blockNumber"]) + 1
            page += 1

        if not all_transactions:
            return pd.DataFrame()

        return self._parse_transactions(all_transactions, protocol_name)

    def _parse_transactions(
        self,
        raw_transactions: list[dict[str, Any]],
        protocol_name: str,
    ) -> pd.DataFrame:
        """Parse raw Etherscan API response into a clean DataFrame."""
        rows = []
        for tx in raw_transactions:
            method_id = tx.get("methodId", "")[:10]
            rows.append(
                {
                    "tx_hash": tx.get("hash", ""),
                    "block_number": int(tx.get("blockNumber", 0)),
                    "block_timestamp": datetime.utcfromtimestamp(
                        int(tx.get("timeStamp", 0))
                    ),
                    "from_address": tx.get("from", "").lower(),
                    "to_address": tx.get("to", "").lower(),
                    "contract_address": tx.get("contractAddress", "").lower() or None,
                    "value_wei": str(tx.get("value", "0") or "0"),
                    "gas_used": int(tx.get("gasUsed", "0") or 0),
                    "gas_price_wei": str(tx.get("gasPrice", "0") or "0"),
                    "method_id": method_id,
                    "function_name": (
                        tx.get("functionName", "").split("(")[0]
                        or self.METHOD_NAMES.get(method_id, "unknown")
                    ),
                    "is_error": tx.get("isError", "0") == "1",
                    "protocol_name": protocol_name,
                    "chain": "ethereum",
                }
            )

        df = pd.DataFrame(rows)
        # Deduplicate by tx_hash
        df = df.drop_duplicates(subset=["tx_hash"])
        return df

    def extract(self) -> pd.DataFrame:
        """Extract transactions for all tracked protocols."""
        all_dfs = []
        for slug, protocol in self.PROTOCOLS.items():
            logger.info(
                f"[etherscan] Starting extraction for {protocol['name']}..."
            )
            df = self.extract_transactions(
                contract_address=protocol["address"],
                protocol_name=protocol["name"],
            )
            if not df.empty:
                all_dfs.append(df)
                logger.info(
                    f"[etherscan] {protocol['name']}: {len(df):,} transactions"
                )

        if not all_dfs:
            return pd.DataFrame()

        combined = pd.concat(all_dfs, ignore_index=True)
        return combined
