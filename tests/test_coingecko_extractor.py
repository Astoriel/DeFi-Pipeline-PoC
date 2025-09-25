import pytest
import pandas as pd
from extract.coingecko_extractor import CoinGeckoExtractor

@pytest.fixture
def extractor():
    return CoinGeckoExtractor(api_key="test_key")

def test_extract_price_history_success(extractor, mocker):
    mock_data = {
        "prices": [[1672531200000, 1200.50], [1672617600000, 1210.75]],
        "market_caps": [[1672531200000, 150000000000.0], [1672617600000, 151000000000.0]],
        "total_volumes": [[1672531200000, 5000000000.0], [1672617600000, 5100000000.0]],
    }
    mocker.patch.object(extractor, '_make_request', return_value=mock_data)

    df = extractor.extract_price_history("ethereum", days=2)

    assert not df.empty
    assert len(df) == 2
    assert "price_usd" in df.columns
    assert "market_cap_usd" in df.columns
    assert "token_symbol" in df.columns
    assert df.iloc[0]["price_usd"] == 1200.50
    assert df.iloc[0]["token_symbol"] == "ETH"

def test_extract_price_history_api_failure(extractor, mocker):
    mocker.patch.object(extractor, '_make_request', side_effect=Exception("API Error"))

    df = extractor.extract_price_history("ethereum", days=2)

    assert df.empty
    assert isinstance(df, pd.DataFrame)

def test_extract_success(extractor, mocker):
    extractor.tokens = ["ethereum", "uniswap"]
    
    mock_df_eth = pd.DataFrame([{
        "token_id": "ethereum",
        "token_symbol": "ETH",
        "date": pd.Timestamp("2023-01-01"),
        "price_usd": 1200.0,
        "market_cap_usd": 150e9,
        "volume_24h_usd": 5e9
    }])

    mock_df_uni = pd.DataFrame([{
        "token_id": "uniswap",
        "token_symbol": "UNI",
        "date": pd.Timestamp("2023-01-01"),
        "price_usd": 5.5,
        "market_cap_usd": 4e9,
        "volume_24h_usd": 1e8
    }])

    mocker.patch.object(extractor, 'extract_price_history', side_effect=[mock_df_eth, mock_df_uni])

    df = extractor.extract()

    assert not df.empty
    assert len(df) == 2
    assert set(df["token_id"].unique()) == {"ethereum", "uniswap"}

def test_extract_all_empty(extractor, mocker):
    extractor.tokens = ["ethereum", "uniswap"]
    mocker.patch.object(extractor, 'extract_price_history', return_value=pd.DataFrame())

    df = extractor.extract()

    assert df.empty
    assert isinstance(df, pd.DataFrame)
