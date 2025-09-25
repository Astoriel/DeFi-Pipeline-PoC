import pytest
import pandas as pd
from extract.dune_extractor import DuneExtractor

@pytest.fixture
def extractor():
    return DuneExtractor(api_key="test_key")

def test_get_headers(extractor):
    headers = extractor._get_headers()
    assert headers == {"X-Dune-API-Key": "test_key"}

def test_execute_query_success(extractor, mocker):
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"execution_id": "test_exec_123"}
    mocker.patch('requests.post', return_value=mock_response)
    mocker.patch.object(extractor, '_make_request') # Mocking the base class _make_request which is called but its result is discarded in the method

    execution_id = extractor._execute_query(1234)

    assert execution_id == "test_exec_123"

def test_execute_query_failure(extractor, mocker):
    mocker.patch('requests.post', side_effect=Exception("Network Error"))
    mocker.patch.object(extractor, '_make_request')

    execution_id = extractor._execute_query(1234)

    assert execution_id is None

def test_poll_execution_success(extractor, mocker):
    mock_response = mocker.Mock()
    mock_response.json.return_value = {
        "state": "QUERY_STATE_COMPLETED",
        "result": {
            "rows": [{"wallet_address": "0x123", "label": "whale"}]
        }
    }
    mocker.patch('requests.get', return_value=mock_response)

    rows = extractor._poll_execution("test_exec_123")

    assert len(rows) == 1
    assert rows[0]["wallet_address"] == "0x123"
    assert rows[0]["label"] == "whale"

def test_poll_execution_failed_state(extractor, mocker):
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"state": "QUERY_STATE_FAILED"}
    mocker.patch('requests.get', return_value=mock_response)

    rows = extractor._poll_execution("test_exec_123")

    assert rows == []

def test_get_mock_labels(extractor):
    df = extractor._get_mock_labels()
    
    assert not df.empty
    assert len(df) == 500
    assert "wallet_address" in df.columns
    assert "label" in df.columns

def test_extract_with_api_key_success(extractor, mocker):
    extractor.QUERIES = {"test_query": 111}
    mocker.patch.object(extractor, '_execute_query', return_value="test_exec_111")
    mocker.patch.object(extractor, '_poll_execution', return_value=[{"wallet_address": "0xabc", "label": "test_label"}])

    df = extractor.extract()

    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["wallet_address"] == "0xabc"
    assert df.iloc[0]["label"] == "test_label"

def test_extract_without_api_key(mocker):
    # If no API key is provided, it should return mock labels directly
    no_key_extractor = DuneExtractor(api_key=None)
    no_key_extractor.api_key = None # ensure it's None, config might load one
    
    mocker.patch.object(no_key_extractor, '_get_mock_labels', return_value=pd.DataFrame([{"wallet_address": "0xmock"}]))

    df = no_key_extractor.extract()

    assert not df.empty
    assert len(df) == 1
    assert df.iloc[0]["wallet_address"] == "0xmock"
    no_key_extractor._get_mock_labels.assert_called_once()
