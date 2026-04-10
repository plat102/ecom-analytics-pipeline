"""
Cloud storage utilities.

This module provides utilities for interacting with cloud storage services.

Currently supports:
- Google Cloud Storage (GCS)
"""

# Re-export commonly used functions from submodules
from common.storage.gcs import (
    upload_to_gcs,
    download_from_gcs,
    list_blobs,
    write_and_upload_jsonl_gz,
)

__all__ = [
    "upload_to_gcs",
    "download_from_gcs",
    "list_blobs",
    "write_and_upload_jsonl_gz",
]
