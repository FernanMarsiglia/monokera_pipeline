import pytest
from unittest.mock import MagicMock
from src.spacenews_ingestion.extractors import extract_articles
from src.spacenews_ingestion.api_client import SpaceNewsApiClient
from src.spacenews_ingestion.dedup import Deduplicator
from src.spacenews_ingestion.checkpoint import CheckpointManager

@pytest.fixture
def mock_client():
    client = MagicMock(spec=SpaceNewsApiClient)
    client.get_paginated.return_value = iter([
        {"results": [{"id": 1, "updatedAt": "2024-01-01T00:00:00Z"}]}
    ])
    return client

@pytest.fixture
def mock_checkpoint_mgr():
    mgr = MagicMock(spec=CheckpointManager)
    mgr.load.return_value = None
    return mgr

def test_extract_articles_yields_records(mock_client, mock_checkpoint_mgr):
    dedup = Deduplicator()
    records = list(extract_articles(
        client=mock_client,
        checkpoint_mgr=mock_checkpoint_mgr,
        dedup=dedup,
        ingest_date="2024-01-01"
    ))
    assert len(records) == 1
    assert records[0]["id"] == 1
