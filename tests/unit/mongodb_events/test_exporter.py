"""
Unit tests for MongoDB events exporter

Tests the export_events function with mocked MongoDB and GCS dependencies.
"""

from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from bson import ObjectId

import pytest

from ingestion.sources.mongodb.events.exporter import export_events


class TestExportEvents:
    """Test export_events function"""

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    @patch('ingestion.sources.mongodb.events.exporter.write_and_upload_jsonl_gz')
    @patch('ingestion.sources.mongodb.events.exporter.save_checkpoint')
    @patch('ingestion.sources.mongodb.events.exporter.clear_checkpoint')
    def test_export_events_full_mode_single_batch(
        self, mock_clear_checkpoint, mock_save_checkpoint,
        mock_write_upload, mock_get_client
    ):
        """Should export all collections in full mode with single batch"""
        # Setup mock MongoDB client
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_collection.return_value = mock_collection
        mock_get_client.return_value = mock_client

        # Mock documents (less than BATCH_SIZE)
        mock_docs = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "collection": "view_product_detail",
                "timestamp": datetime(2024, 3, 15, 10, 0, 0),
                "user_id": "user123"
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "collection": "add_to_cart",
                "timestamp": datetime(2024, 3, 15, 11, 0, 0),
                "user_id": "user456"
            }
        ]

        mock_collection.count_documents.return_value = 2
        mock_collection.find.return_value.batch_size.return_value = iter(mock_docs)

        # Mock successful upload
        mock_write_upload.return_value = {
            'success': True,
            'records': 2,
            'uncompressed_bytes': 1000,
            'compressed_bytes': 300,
            'compression_ratio': 30.0,
            'gcs_uri': 'gs://test_bucket/raw/events/events_20240315_part001.jsonl.gz'
        }

        # Execute
        result = export_events(mode="full", date_str="20240315")

        # Verify success
        assert result is True

        # Verify MongoDB query was correct (empty query for full mode)
        mock_collection.find.assert_called_once()
        query_arg = mock_collection.find.call_args[0][0]
        assert query_arg == {}

        # Verify upload was called
        assert mock_write_upload.call_count == 1

        # Verify checkpoint was cleared on success
        mock_clear_checkpoint.assert_called_once()

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    @patch('ingestion.sources.mongodb.events.exporter.write_and_upload_jsonl_gz')
    @patch('ingestion.sources.mongodb.events.exporter.save_checkpoint')
    def test_export_events_filter_mode_specific_collections(
        self, mock_save_checkpoint, mock_write_upload, mock_get_client
    ):
        """Should filter specific collections in filter mode"""
        # Setup mocks
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_collection.return_value = mock_collection
        mock_get_client.return_value = mock_client

        mock_docs = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "collection": "checkout_success",
                "amount": 99.99
            }
        ]

        mock_collection.count_documents.return_value = 1
        mock_collection.find.return_value.batch_size.return_value = iter(mock_docs)

        mock_write_upload.return_value = {
            'success': True,
            'records': 1,
            'uncompressed_bytes': 500,
            'compressed_bytes': 150,
            'compression_ratio': 30.0,
            'gcs_uri': 'gs://bucket/path.jsonl.gz'
        }

        # Execute with filter mode
        result = export_events(
            mode="filter",
            collections=["checkout_success", "view_product_detail"]
        )

        # Verify success
        assert result is True

        # Verify MongoDB query includes collection filter
        query_arg = mock_collection.find.call_args[0][0]
        assert "collection" in query_arg
        assert query_arg["collection"] == {"$in": ["checkout_success", "view_product_detail"]}

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    @patch('ingestion.sources.mongodb.events.exporter.load_checkpoint')
    @patch('ingestion.sources.mongodb.events.exporter.write_and_upload_jsonl_gz')
    def test_export_events_resume_from_checkpoint(
        self, mock_write_upload, mock_load_checkpoint, mock_get_client
    ):
        """Should resume from checkpoint with last_id filter"""
        # Setup mocks
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_collection.return_value = mock_collection
        mock_get_client.return_value = mock_client

        # Mock checkpoint data
        mock_load_checkpoint.return_value = {
            'part_number': 5,
            'last_id': '507f1f77bcf86cd799439011'
        }

        mock_docs = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "collection": "test_event"
            }
        ]

        mock_collection.count_documents.return_value = 1
        mock_collection.find.return_value.batch_size.return_value = iter(mock_docs)

        mock_write_upload.return_value = {
            'success': True,
            'records': 1,
            'uncompressed_bytes': 100,
            'compressed_bytes': 30,
            'compression_ratio': 30.0,
            'gcs_uri': 'gs://bucket/path.jsonl.gz'
        }

        # Execute with resume=True
        result = export_events(mode="full", resume=True)

        # Verify checkpoint was loaded
        mock_load_checkpoint.assert_called_once()

        # Verify query includes _id filter to resume from last_id
        query_arg = mock_collection.find.call_args[0][0]
        assert "_id" in query_arg
        assert "$gt" in query_arg["_id"]

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    def test_export_events_filter_mode_requires_collections(self, mock_get_client):
        """Should return False if filter mode without collections"""
        result = export_events(mode="filter", collections=None)

        assert result is False

        # Should not connect to MongoDB
        mock_get_client.assert_not_called()

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    @patch('ingestion.sources.mongodb.events.exporter.write_and_upload_jsonl_gz')
    def test_export_events_handles_upload_failure(
        self, mock_write_upload, mock_get_client
    ):
        """Should return False and stop if upload fails"""
        # Setup mocks
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_collection.return_value = mock_collection
        mock_get_client.return_value = mock_client

        mock_docs = [{"_id": ObjectId(), "test": "data"}]
        mock_collection.count_documents.return_value = 1
        mock_collection.find.return_value.batch_size.return_value = iter(mock_docs)

        # Mock upload failure
        mock_write_upload.return_value = {'success': False}

        # Execute
        result = export_events(mode="full")

        # Should return False on failure
        assert result is False

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    def test_export_events_handles_exception(self, mock_get_client):
        """Should return False and log exception on error"""
        # Mock exception during MongoDB connection
        mock_get_client.side_effect = Exception("Connection failed")

        # Execute
        result = export_events(mode="full")

        # Should return False on exception
        assert result is False

    @patch('ingestion.sources.mongodb.events.exporter.get_mongodb_client')
    def test_export_events_closes_client_in_finally(self, mock_get_client):
        """Should close MongoDB client even if exception occurs"""
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        # Mock exception during execution
        mock_client.get_collection.side_effect = Exception("Test error")

        # Execute
        export_events(mode="full")

        # Verify client.close() was called
        mock_client.close.assert_called_once()
