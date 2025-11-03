"""Unit tests for the data pipeline."""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.invezgo_client import InvezgoClient
from src.bigquery_loader import BigQueryLoader


class TestInvezgoClient(unittest.TestCase):
    """Test cases for InvezgoClient."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.client = InvezgoClient(
            base_url="https://api.invezgo.com/",
            api_token="test_token"
        )
    
    def test_normalize_response_dict(self):
        """Test normalizing dict response."""
        data = {"id": 1, "name": "test"}
        result = self.client._normalize_response(data)
        self.assertEqual(result, [{"id": 1, "name": "test"}])
    
    def test_normalize_response_list(self):
        """Test normalizing list response."""
        data = [{"id": 1}, {"id": 2}]
        result = self.client._normalize_response(data)
        self.assertEqual(result, data)
    
    def test_normalize_response_with_data_key(self):
        """Test normalizing response with data key."""
        data = {"data": [{"id": 1}, {"id": 2}]}
        result = self.client._normalize_response(data)
        self.assertEqual(result, [{"id": 1}, {"id": 2}])


class TestBigQueryLoader(unittest.TestCase):
    """Test cases for BigQueryLoader."""
    
    @patch('src.bigquery_loader.bigquery.Client')
    def test_initialization(self, mock_client):
        """Test BigQueryLoader initialization."""
        loader = BigQueryLoader(project_id="test-project")
        self.assertEqual(loader.project_id, "test-project")
        mock_client.assert_called_once_with(project="test-project")


if __name__ == '__main__':
    unittest.main()
