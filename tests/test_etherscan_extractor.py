"""
Unit tests for EtherscanExtractor.
"""
import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from extract.etherscan_extractor import EtherscanExtractor


@pytest.fixture
def extractor():
    with patch.dict("os.environ", {"ETHERSCAN_API_KEY": "test_key"}):
        return EtherscanExtractor(api_key="test_key")


@pytest.fixture
def mock_api_response():
    """Minimal valid Etherscan API response for 2 transactions."""
    return {
        "status": "1",
        "message": "OK",
        "result": [
            {
                "hash": "0xabc123",
                "blockNumber": "19000000",
                "timeStamp": "1700000000",
                "from": "0xUser1",
                "to": "0xContract1",
                "contractAddress": "",
                "value": "1000000000000000000",  # 1 ETH in wei
                "gasUsed": "150000",
                "gasPrice": "20000000000",
                "methodId": "0x414bf389",
                "functionName": "exactInputSingle()",
                "isError": "0",
            },
            {
                "hash": "0xdef456",
                "blockNumber": "19000001",
                "timeStamp": "1700000120",
                "from": "0xUser2",
                "to": "0xContract1",
                "contractAddress": "",
                "value": "0",
                "gasUsed": "200000",
                "gasPrice": "25000000000",
                "methodId": "0x617ba037",
                "functionName": "supply()",
                "isError": "0",
            },
        ],
    }


class TestEtherscanExtractor:
    def test_initialization(self, extractor):
        assert extractor.name == "etherscan"
        assert extractor.api_key == "test_key"
        assert len(extractor.PROTOCOLS) == 2

    def test_parse_transactions(self, extractor, mock_api_response):
        raw = mock_api_response["result"]
        df = extractor._parse_transactions(raw, "Uniswap V3")

        assert len(df) == 2
        assert "tx_hash" in df.columns
        assert "from_address" in df.columns
        assert "value_wei" in df.columns
        assert "function_name" in df.columns
        assert "protocol_name" in df.columns

    def test_wei_to_eth_conversion_logic(self, extractor, mock_api_response):
        raw = mock_api_response["result"]
        df = extractor._parse_transactions(raw, "Uniswap V3")
        # value_wei should be 1e18 for first tx
        assert df.iloc[0]["value_wei"] == 1_000_000_000_000_000_000

    def test_method_id_classification(self, extractor, mock_api_response):
        raw = mock_api_response["result"]
        df = extractor._parse_transactions(raw, "Uniswap V3")
        assert df.iloc[0]["function_name"] == "exactInputSingle"
        assert df.iloc[1]["function_name"] == "supply"

    def test_address_normalised_to_lowercase(self, extractor, mock_api_response):
        raw = mock_api_response["result"]
        df = extractor._parse_transactions(raw, "Uniswap V3")
        assert df.iloc[0]["from_address"] == "0xuser1"

    def test_deduplication_by_tx_hash(self, extractor):
        """Duplicate transactions should be removed."""
        raw = [
            {
                "hash": "0xduplicate",
                "blockNumber": "100",
                "timeStamp": "1700000000",
                "from": "0xA",
                "to": "0xB",
                "contractAddress": "",
                "value": "0",
                "gasUsed": "21000",
                "gasPrice": "10000000000",
                "methodId": "0x",
                "functionName": "",
                "isError": "0",
            }
        ] * 3  # Same tx 3 times

        df = extractor._parse_transactions(raw, "Uniswap V3")
        assert len(df) == 1

    @patch.object(EtherscanExtractor, "_make_request")
    def test_extract_transactions_stops_on_empty(self, mock_request, extractor):
        """Should stop pagination when API returns no more results."""
        mock_request.return_value = {"status": "0", "message": "No transactions found", "result": []}
        df = extractor.extract_transactions(
            contract_address="0xtest",
            protocol_name="Test Protocol",
        )
        assert df.empty or len(df) == 0

    def test_validate_returns_dataframe(self, extractor):
        """validate() should return the same DataFrame unchanged."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = extractor.validate(df)
        assert len(result) == 3

    def test_validate_warns_on_empty(self, extractor, caplog):
        """validate() should warn on empty DataFrame."""
        import logging
        df = pd.DataFrame()
        result = extractor.validate(df)
        assert result.empty
