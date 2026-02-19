import pytest
from unittest.mock import patch, MagicMock
from src.spacenews_ingestion.api_client import SpaceNewsApiClient
from src.spacenews_ingestion.logging import JsonLogger

@patch("requests.Session.get")
def test_get_paginated_success(mock_get):
    logger = JsonLogger()
    client = SpaceNewsApiClient("https://api.spaceflightnewsapi.net/v4", logger=logger)
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"results": [{"id": 1}], "next": None}
    pages = list(client.get_paginated("/articles"))
    assert len(pages) == 1
    assert pages[0]["results"][0]["id"] == 1

@patch("requests.Session.get")
def test_get_paginated_retries_on_429(mock_get):
    logger = JsonLogger()
    client = SpaceNewsApiClient("https://api.spaceflightnewsapi.net/v4", max_retries=2, logger=logger)
    # First call returns 429, second call returns 200
    mock_get.side_effect = [
        MagicMock(status_code=429, reason="Too Many Requests", json=lambda: {}),
        MagicMock(status_code=200, json=lambda: {"results": [], "next": None})
    ]
    pages = list(client.get_paginated("/articles"))
    assert len(pages) == 1
