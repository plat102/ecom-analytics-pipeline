"""
Unit tests for ingestion.shared.mongodb_utils module

Tests MongoDB-specific JSON serialization utilities.
"""

import json
from datetime import datetime, timezone
from bson import ObjectId

from ingestion.shared.mongodb_utils import MongoJSONEncoder


class TestMongoJSONEncoder:
    """Test MongoJSONEncoder for MongoDB document serialization"""

    def test_encode_objectid_to_string(self):
        """Should convert ObjectId to string"""
        object_id = ObjectId("507f1f77bcf86cd799439011")
        doc = {"_id": object_id, "name": "test"}

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        assert decoded["_id"] == "507f1f77bcf86cd799439011"
        assert isinstance(decoded["_id"], str)

    def test_encode_datetime_to_isoformat(self):
        """Should convert datetime to ISO 8601 string"""
        dt = datetime(2024, 3, 15, 10, 30, 45, tzinfo=timezone.utc)
        doc = {"created_at": dt, "name": "test"}

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        assert decoded["created_at"] == "2024-03-15T10:30:45+00:00"
        assert isinstance(decoded["created_at"], str)

    def test_encode_datetime_without_timezone(self):
        """Should handle naive datetime (without timezone)"""
        dt = datetime(2024, 3, 15, 10, 30, 45)
        doc = {"timestamp": dt}

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        # Should produce ISO format string
        assert "2024-03-15T10:30:45" in decoded["timestamp"]
        assert isinstance(decoded["timestamp"], str)

    def test_encode_document_with_multiple_types(self):
        """Should handle document with mixed types including ObjectId and datetime"""
        doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "created": datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc),
            "updated": datetime(2024, 3, 16, 15, 30, 0, tzinfo=timezone.utc),
            "user_id": ObjectId("507f191e810c19729de860ea"),
            "name": "Test Product",
            "price": 99.99,
            "active": True,
            "tags": ["tag1", "tag2"]
        }

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        # Check ObjectIds converted to strings
        assert decoded["_id"] == "507f1f77bcf86cd799439011"
        assert decoded["user_id"] == "507f191e810c19729de860ea"

        # Check datetimes converted to ISO strings
        assert decoded["created"] == "2024-03-15T12:00:00+00:00"
        assert decoded["updated"] == "2024-03-16T15:30:00+00:00"

        # Check other types preserved
        assert decoded["name"] == "Test Product"
        assert decoded["price"] == 99.99
        assert decoded["active"] is True
        assert decoded["tags"] == ["tag1", "tag2"]

    def test_encode_nested_document(self):
        """Should handle nested documents with ObjectId and datetime"""
        doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "metadata": {
                "created_by": ObjectId("507f191e810c19729de860ea"),
                "created_at": datetime(2024, 3, 15, 10, 0, 0, tzinfo=timezone.utc)
            },
            "items": [
                {
                    "item_id": ObjectId("507f1f77bcf86cd799439012"),
                    "timestamp": datetime(2024, 3, 15, 11, 0, 0, tzinfo=timezone.utc)
                }
            ]
        }

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        # Check nested ObjectIds and datetimes
        assert decoded["metadata"]["created_by"] == "507f191e810c19729de860ea"
        assert decoded["metadata"]["created_at"] == "2024-03-15T10:00:00+00:00"
        assert decoded["items"][0]["item_id"] == "507f1f77bcf86cd799439012"
        assert decoded["items"][0]["timestamp"] == "2024-03-15T11:00:00+00:00"

    def test_encode_list_of_documents(self):
        """Should handle list of documents with MongoDB types"""
        docs = [
            {
                "_id": ObjectId("507f1f77bcf86cd799439011"),
                "created": datetime(2024, 3, 15, tzinfo=timezone.utc)
            },
            {
                "_id": ObjectId("507f1f77bcf86cd799439012"),
                "created": datetime(2024, 3, 16, tzinfo=timezone.utc)
            }
        ]

        result = json.dumps(docs, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        assert len(decoded) == 2
        assert decoded[0]["_id"] == "507f1f77bcf86cd799439011"
        assert decoded[1]["_id"] == "507f1f77bcf86cd799439012"
        assert "2024-03-15" in decoded[0]["created"]
        assert "2024-03-16" in decoded[1]["created"]

    def test_encode_standard_types_unchanged(self):
        """Should not modify standard JSON-serializable types"""
        doc = {
            "string": "text",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "dict": {"key": "value"}
        }

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        assert decoded == doc

    def test_encode_empty_document(self):
        """Should handle empty document"""
        doc = {}

        result = json.dumps(doc, cls=MongoJSONEncoder)
        decoded = json.loads(result)

        assert decoded == {}

    def test_encode_preserves_json_options(self):
        """Should work with standard json.dumps options like indent"""
        doc = {
            "_id": ObjectId("507f1f77bcf86cd799439011"),
            "created": datetime(2024, 3, 15, tzinfo=timezone.utc)
        }

        result = json.dumps(doc, cls=MongoJSONEncoder, indent=2)

        # Should be formatted with indentation
        assert "\n" in result
        assert "  " in result

        # Should still decode correctly
        decoded = json.loads(result)
        assert decoded["_id"] == "507f1f77bcf86cd799439011"
