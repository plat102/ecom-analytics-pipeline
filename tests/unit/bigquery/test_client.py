"""
Tests for BigQuery client utilities.

Tests ensure that client initialization works correctly.
"""

import pytest
from unittest.mock import patch, MagicMock
from common.bigquery.client import get_client


class TestGetClient:
    """Tests for get_client function."""

    @patch('common.bigquery.client.bigquery.Client')
    def test_client_initialization(self, mock_client_class):
        """Test that client is initialized with project ID."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        client = get_client("test-project-id")

        mock_client_class.assert_called_once_with(project="test-project-id")
        assert client == mock_client

    @patch('common.bigquery.client.bigquery.Client')
    def test_client_with_different_project_ids(self, mock_client_class):
        """Test that different project IDs are passed correctly."""
        project_ids = ["project-1", "project-2", "ecom-analytics-tp"]

        for project_id in project_ids:
            mock_client_class.reset_mock()
            get_client(project_id)
            mock_client_class.assert_called_once_with(project=project_id)

    @patch('common.bigquery.client.bigquery.Client')
    def test_client_return_type(self, mock_client_class):
        """Test that get_client returns a Client instance."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        client = get_client("test-project")

        assert client is not None
        assert client == mock_client
