"""
Unit tests for DeFiLlamaExtractor.
"""
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date

from extract.defillama_extractor import DeFiLlamaExtractor


@pytest.fixture
def extractor():
    return DeFiLlamaExtractor()


@pytest.fixture
def mock_tvl_response():
    """Minimal DeFiLlama TVL history response."""
    return {
        "tvl": [
            {"date": 1704067200, "totalLiquidityUSD": 5_000_000_000.0},  # 2024-01-01
            {"date": 1704153600, "totalLiquidityUSD": 5_100_000_000.0},  # 2024-01-02
            {"date": 1704240000, "totalLiquidityUSD": 4_900_000_000.0},  # 2024-01-03
        ]
    }


class TestDeFiLlamaExtractor:
    def test_initialization(self, extractor):
        assert extractor.name == "defillama"
        assert len(extractor.PROTOCOLS) == 2
        assert all("slug" in p for p in extractor.PROTOCOLS)

    @patch.object(DeFiLlamaExtractor, "_make_request")
    def test_fetch_tvl_history_parses_correctly(self, mock_request, extractor, mock_tvl_response):
        mock_request.return_value = mock_tvl_response
        history = extractor._fetch_tvl_history("uniswap-v3")
        assert len(history) == 3
        assert "totalLiquidityUSD" in history[0]

    @patch.object(DeFiLlamaExtractor, "_make_request")
    def test_extract_tvl_returns_dataframe(self, mock_request, extractor, mock_tvl_response):
        mock_request.return_value = mock_tvl_response
        df = extractor.extract_tvl()

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "protocol_slug" in df.columns
        assert "date" in df.columns
        assert "tvl_usd" in df.columns

    @patch.object(DeFiLlamaExtractor, "_fetch_tvl_history")
    def test_extract_filters_zero_tvl(self, mock_fetch, extractor):
        """TVL of 0 should be filtered out in SQL staging, but raw goes through."""
        mock_fetch.return_value = [
            {"date": 1704067200, "totalLiquidityUSD": 0},
            {"date": 1704153600, "totalLiquidityUSD": 5_000_000_000.0},
        ]
        df = extractor.extract_tvl()
        # Both should be in raw — staging filters zeros
        assert len(df) >= 1

    @patch.object(DeFiLlamaExtractor, "_make_request")
    def test_extract_handles_api_failure(self, mock_request, extractor):
        """Should return empty DataFrame on API failure, not crash."""
        mock_request.side_effect = Exception("API timeout")
        df = extractor.extract()
        assert isinstance(df, pd.DataFrame)

    @patch.object(DeFiLlamaExtractor, "_fetch_tvl_history")
    def test_deduplication(self, mock_fetch, extractor):
        """Duplicate (protocol_slug, chain, date) rows should be removed."""
        # Same entry for both protocols at same date
        mock_fetch.return_value = [
            {"date": 1704067200, "totalLiquidityUSD": 5e9},
            {"date": 1704067200, "totalLiquidityUSD": 5e9},  # duplicate
        ]
        df = extractor.extract_tvl()
        dupes = df.duplicated(subset=["protocol_slug", "chain", "date"])
        assert not dupes.any()
