"""
MongoDB Utilities

Shared utilities for working with MongoDB data in ingestion pipelines.
"""

import json
from datetime import datetime
from bson import ObjectId


class MongoJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for MongoDB document serialization.

    Handles MongoDB-specific types:
    - ObjectId -> string
    - datetime -> ISO 8601 string

    Usage:
        >>> doc = {"_id": ObjectId("..."), "created": datetime.now()}
        >>> json.dumps(doc, cls=MongoJSONEncoder)
    """
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
