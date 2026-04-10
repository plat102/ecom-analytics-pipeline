"""
Database clients and utilities.

This module provides utilities for interacting with various databases.

Currently supports:
- MongoDB
"""

# Re-export commonly used functions from submodules
from common.database.mongodb import (
    get_mongodb_client,
    MongoDBClient,
    MongoJSONEncoder,
)

__all__ = [
    "get_mongodb_client",
    "MongoDBClient",
    "MongoJSONEncoder",
]
