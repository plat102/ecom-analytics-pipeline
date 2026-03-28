"""
Storage clients for object storage (Data Lake).

Currently supports:
- Google Cloud Storage (GCS)
"""

from common.storage.gcs_client import upload_to_gcs, download_from_gcs

__all__ = ["upload_to_gcs", "download_from_gcs"]
