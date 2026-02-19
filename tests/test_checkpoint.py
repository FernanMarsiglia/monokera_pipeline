import pytest
from unittest.mock import patch, MagicMock
from src.spacenews_ingestion.checkpoint import CheckpointManager

@patch("boto3.client")
def test_checkpoint_load_and_save(mock_boto_client):
    s3 = MagicMock()
    mock_boto_client.return_value = s3
    mgr = CheckpointManager("bucket", "prefix")
    # Simulate no checkpoint
    s3.get_object.side_effect = s3.exceptions.NoSuchKey = Exception
    assert mgr.load("articles") is None
    # Simulate save
    s3.get_object.side_effect = None
    s3.put_object.return_value = None
    mgr.save("articles", {"last_updated_at": "2024-01-01T00:00:00Z"})
    s3.put_object.assert_called()
